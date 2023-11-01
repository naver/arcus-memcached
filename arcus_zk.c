/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
   Arcus incorporates memcached instance as a ephemeral entity
   Here's how memcached participates in Arcus cluster

    1. get local IP address
    2. connect to Zookeeper ensemble
    3. sync "/arcus/cache_server_mapping"
         for latest provisioned cache server
    4. get child of "/arcus/cache_server_mapping/{ip}:{port}" znode
         to get service code
         this service code is a string that indicates which service this
         cache server belongs to

         ie. /arcus/cache_server_mapping/10.0.0.1:11211/{svc}

    5. create an ephemeral "/arcus/cache_list/{svc}/{ip}:{port}-{hostname}" znode
         to indicate that this server is participating in Arcus {svc} cluster
         {port} is the memcached listen port
         hostname is a redudant information to easily identify the server

         the memcached memory capacity is stored in the znode's data

    6. create "/arcus/cache_server_log/{date}/{ip}:{port}-{hostname}-" sequential znode
         this is to log join and leave activity for later debugging or post-mortem

    In global watcher callback routine, we chose not to restart/reconnect to
    Zookeeper ensemble (when session expires), because this may create stale cache
    entry access. We need to restart the whole memcached process to flush its content
    before rejoin the Arcus cluster. We need to force restart of memcached (/etc/inittab)
*/


#include <config.h>

#ifdef ENABLE_ZK_INTEGRATION

#include <assert.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <zookeeper.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <sysexits.h>
#include <limits.h>
#include "arcus_zk.h"
#ifdef ENABLE_CLUSTER_AWARE
#include "cluster_config.h"
#endif

/* zookeeper.h unfortunately includes a lot of other headers.  One of
 * them (recordio.h) unconditionally defines htonll.  memcached.h also
 * includes so many headers.  util.h defines htonll, which conflicts
 * with the one in zookeeper.h.  As a workaround, define HAVE_HTONLL
 * here to prevent util.h from defining htonll again.
 *
 * All we need from memcached.h is ADMIN_CLIENT_IP...
 */
#define HAVE_HTONLL 1
#include "memcached.h"

/* Kill server if disconnected from zookeeper for too long */
//#define ENABLE_SUICIDE_UPON_DISCONNECT 1

/* Recv. timeout governs ZK heartbeat period, reconnect timeout
 * as well as ZK session timeout
 * Below sets 30 sec. session timeout, 20 sec Zookeeper ensemble timeout,
 * and 10 second heartbeat period
 */
#define DEFAULT_ZK_TO 30000 /* ZK session timeout in msec (server tick time is 2 sec, min is 4 sec) */
#define MAX_ZK_TO     2000  /* Max allowable session timeout in sec */
#define MIN_ZK_TO     10    /* Min 10 seconds, in line with the current heartbeat timeout */

#define ZK_WATCH      1
#define ZK_NOWATCH    0
#define SYSLOGD_PORT  514     /* UDP syslog port # */

#define MAX_SERVICECODE_LENGTH  128
#define MAX_HOSTNAME_LENGTH     128

#ifdef ENABLE_ZK_RECONFIG
/* The maximum config data size per ZK server is 600.
 * Therefore, config data of about 26 ZK servers can be stored.
 */
#define MAX_ZK_CONFIG_DATA_LENGTH  (16 * 1024)
struct _zk_reconfig {
    char data_buffer[MAX_ZK_CONFIG_DATA_LENGTH];
    char host_buffer[MAX_ZK_CONFIG_DATA_LENGTH/2]; /* half space is enough */
    int64_t version; /* current ZK config version */
    bool    enabled; /* dynamic reconfig is enabled in ZK ? */
    bool    needed;  /* dynamic reconfig is needed by arcus ? */
} zk_reconfig;
#endif

static const char *zk_root = NULL;
static const char *zk_map_dir = "cache_server_mapping";
static const char *zk_log_dir = "cache_server_log";
static const char *zk_cache_dir = "cache_list";

typedef struct {
    char       *ensemble_list; /* ZK ensemble IP:port list */
    zhandle_t  *zh;            /* ZK handle */
    clientid_t  myid;
} zk_info_t;

static zk_info_t zk_info;
static zk_info_t *main_zk=NULL;
static int        last_rc=ZOK;

#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
static bool zk_connected = false;
#endif

/* this is to indicate if we get shutdown signal during ZK initialization
 * ZK shutdown is done at the end of arcus_zk_init() to simplify synchronization
 */
volatile sig_atomic_t arcus_zk_shutdown=0;

/* placeholder for zookeeper and memcached settings */
typedef struct {
    char    *svc;               // Service code name
    char    *mc_ipport;         // this memcached ip:port string
    char    *mc_hostnameport;   // this memcached hostname:port string
    char    *hostip;            // localhost server IP
    int     port;               // memcached port number
    bool    zk_failstop;        // memcached automatic failstop on/off
    int     zk_timeout;         // Zookeeper session timeout
    bool    auto_scrub;         // automactic scrub_stale
    char   *znode_name;         // Ephemeral ZK node name for this mc identification
    int     znode_ver;          // Ephemeral ZK node version
    bool    znode_created;      // Ephemeral ZK node is created ?
    int     verbose;            // verbose output
    size_t  maxbytes;           // mc -M option
    EXTENSION_LOGGER_DESCRIPTOR *logger; // mc logger
    union {
        ENGINE_HANDLE *v0;
        ENGINE_HANDLE_V1 *v1;
    } engine;                   // mc engine
    char   *cluster_path;       // cluster path for this memcached
#ifdef PROXY_SUPPORT
    char   *proxy;              // proxy server ip:port
#endif
#ifdef ENABLE_CLUSTER_AWARE
    struct cluster_config *ch;  // cluster configuration handle
#endif
    pthread_mutex_t lock;
    bool    init;               // is this structure initialized?
} arcus_zk_config;

arcus_zk_config arcus_conf = {
    .svc            = NULL,
    .mc_ipport      = NULL,
    .mc_hostnameport    = NULL,
    .hostip         = NULL,
    .zk_failstop    = true,
    .zk_timeout     = DEFAULT_ZK_TO,
    .auto_scrub     = true,
    .znode_name     = NULL,
    .znode_ver      = -1,
    .znode_created  = false,
    .verbose        = -1,
    .port           = -1,
    .maxbytes       = -1,
    .logger         = NULL,
    .cluster_path   = NULL,
#ifdef PROXY_SUPPORT
    .proxy          = NULL,
#endif
#ifdef ENABLE_CLUSTER_AWARE
    .ch             = NULL,
#endif
    .lock           = PTHREAD_MUTEX_INITIALIZER,
    .init           = false
};

/* Arcus ZK stats */
arcus_zk_stats azk_stat;

/* static forward declaration */
static void arcus_zk_watcher(zhandle_t *wzh, int type, int state,
                             const char *path, void *cxt);
#ifdef ENABLE_ZK_RECONFIG
static int  arcus_check_zk_reconfig_enabled(zhandle_t *zh);
static void arcus_zkconfig_watcher(zhandle_t *zh, int type, int state,
                                   const char *path, void *ctx);
#endif
static void arcus_cache_list_watcher(zhandle_t *zh, int type, int state,
                                     const char *path, void *ctx);
static void arcus_zk_sync_cb(int rc, const char *name, const void *data);
static void arcus_zk_log(zhandle_t *zh, const char *);
static void arcus_exit(zhandle_t *zh, int err);

/* State machine thread.
 * It performs all blocking ZK operations including cluster_config.
 * We don't do blocking operations in the context of watcher callback
 * any more.
 */
/* sm request structure */
struct sm_request {
#ifdef ENABLE_ZK_RECONFIG
    bool update_zkconfig;
#endif
    bool update_cache_list;
};

/* sm structure */
struct sm {
    /* sm requests by other threads */
    struct sm_request request;

    /* Cache of the latest version we pulled from ZK */
    struct String_vector sv_cache_list; /* from /cache_list/{svc} */

    /* Current # of nodes in cluster */
    int cluster_node_count;

    /* the time a new node was added to the cluster */
    volatile uint64_t node_added_time;

    volatile bool mc_pause;

    /* Used to wake up the thread */
    pthread_mutex_t lock;
    pthread_cond_t cond;
    bool notification;

    volatile bool state_running;
#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
    volatile bool timer_running;
#endif

    pthread_t state_tid;
#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
    /* A timer thread to fail-stop the server
     * when it is disconnected from the ZK ensemble.
     */
    pthread_t timer_tid;
#endif
} sm_info;

/* sm functions */
static int  sm_init(void);
static void sm_lock(void);
static void sm_unlock(void);
static void sm_wakeup(bool locked);

/* async routine synchronization */
static pthread_cond_t  azk_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t azk_mtx  = PTHREAD_MUTEX_INITIALIZER;
static int             azk_count;

/* zookeeper close synchronization */
static pthread_mutex_t zk_lock = PTHREAD_MUTEX_INITIALIZER;

static int
arcus_memcached_scrub_stale(void)
{
    ENGINE_ERROR_CODE ret;
    ret = arcus_conf.engine.v1->scrub_stale(arcus_conf.engine.v0);
    return ret == ENGINE_SUCCESS ? 0 : -1;
}

/* Some znode names use '^' as a delimiter.
 * Return pointers to the starting and ending ('^') characters.
 */
static int
breakup_string(char *str, char *start[], char *end[], int vec_len)
{
    char *c = str;
    int i = 0;
    while (i < vec_len) {
        start[i] = c;
        while (*c != '\0') {
            if (*c == '^')
                break;
            c++;
        }
        if (*c == '\0') {
            end[i] = c;
            i++;
            break;
        }
        if (start[i] == c)
            break; /* empty */
        end[i] = c++;
        i++;
    }
    if (*c != '\0') {
        /* There are leftover characters. Ignore them. */
    }
    return i; /* i <= vec_len */
}

/* mutex for async operations */
static void inc_count(int delta)
{
    pthread_mutex_lock(&azk_mtx);
    azk_count += delta;
    pthread_cond_broadcast(&azk_cond);
    pthread_mutex_unlock(&azk_mtx);
}

static void clear_count(void)
{
    pthread_mutex_lock(&azk_mtx);
    azk_count = 0;
    pthread_mutex_unlock(&azk_mtx);
}

static int wait_count(int timeout)
{
    struct timeval  tv;
    struct timespec ts;
    int rc = 0;
    pthread_mutex_lock(&azk_mtx);
    while (azk_count > 0 && rc == 0) {
        if (timeout == 0) {
            rc = pthread_cond_wait(&azk_cond, &azk_mtx);
        } else {
            gettimeofday(&tv, NULL);
            ts.tv_sec  = tv.tv_sec;
            ts.tv_nsec = tv.tv_usec*1000;
            //clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += timeout/1000; /* timeout: msec */
            rc = pthread_cond_timedwait(&azk_cond, &azk_mtx, &ts);
        }
    }
    pthread_mutex_unlock(&azk_mtx);
    return rc;
}

static int
arcus_zk_client_init(zk_info_t *zinfo)
{
    inc_count(1);
    zinfo->zh = zookeeper_init(zinfo->ensemble_list, arcus_zk_watcher,
                               arcus_conf.zk_timeout, &zinfo->myid, zinfo, 0);
    if (!zinfo->zh) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "zookeeper_init() failed: %s\n", strerror(errno));
        return EX_PROTOCOL;
    }
    /* wait until above init callback is called
     * We need to wait until ZOO_CONNECTED_STATE
     */
    if (wait_count(arcus_conf.zk_timeout) != 0) {
        /* zoo_state(zh) != ZOO_CONNECTED_STATE */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                 "cannot to be ZOO_CONNECTED_STATE\n");
        zookeeper_close(zinfo->zh);
        zinfo->zh = NULL;
        inc_count(-1);
        return EX_PROTOCOL;
    }
    // "recv" timeout is actually the session timeout
    // ZK client ping period is recv_timeout / 3.
    if (arcus_conf.zk_timeout != zoo_recv_timeout(zinfo->zh)) {
        arcus_conf.zk_timeout = zoo_recv_timeout(zinfo->zh);
    }
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
            "ZooKeeper client initialized. (ZK session timeout=%d sec)\n",
            arcus_conf.zk_timeout/1000);
    return 0;
}

// Arcus zookeeper global watch callback routine
static void
arcus_zk_watcher(zhandle_t *wzh, int type, int state, const char *path, void *cxt)
{
    if (type != ZOO_SESSION_EVENT) {
        return;
    }

    if (state == ZOO_CONNECTED_STATE) {
        const clientid_t *id = zoo_client_id(wzh);
        zk_info_t *zinfo = cxt;
        if (arcus_conf.verbose > 1) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                   "ZK ensemble connected. session id: 0x%llx\n",
                                   (long long) zinfo->myid.client_id);
        }
        if (zinfo->myid.client_id == 0 || zinfo->myid.client_id != id->client_id) {
            if (arcus_conf.verbose > 1)
                 arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                         "A old session id: 0x%llx\n", (long long) zinfo->myid.client_id);
             zinfo->myid = *id;
        }

#ifdef ENABLE_ZK_RECONFIG
        if (arcus_conf.init && zk_reconfig.needed && !zk_reconfig.enabled) {
            /* check if ZK dynamic reconfig is enabled ? */
            if (arcus_check_zk_reconfig_enabled(zinfo->zh) < 0) {
                /* zoo_getconfig API failed.. (rarely)
                 * Instead of retry checking, set zk_reconfig to true to make it more safe.
                 */
                zk_reconfig.enabled = true;
            }
            if (zk_reconfig.enabled) {
                /* Wake up the state machine thread and update zkconfig. */
                sm_lock();
                sm_info.request.update_zkconfig = true;
                sm_wakeup(true);
                sm_unlock();
            }
        }
#endif

        // finally connected to one of ZK ensemble. signal go.
        inc_count(-1);
#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
        zk_connected = true;
#endif
    }
    else if (state == ZOO_AUTH_FAILED_STATE) {
        // authorization failure
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Auth failure. shutting down\n");
        arcus_exit(wzh, EX_NOPERM);
    }
    else if (state == ZOO_EXPIRED_SESSION_STATE) {
        if (arcus_conf.zk_failstop) {
            // very likely that memcached process exited and restarted within
            // session timeout
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                                   "Expired state. shutting down\n");
            // send SMS here??
            arcus_exit(wzh, EX_TEMPFAIL);
        } else {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                                   "Expired state. pausing memcached (zk_failstop: off)\n");
            sm_lock();
            sm_info.mc_pause = true;
            sm_wakeup(true);
            sm_unlock();
        }
    }
    else if (state == ZOO_ASSOCIATING_STATE || state == ZOO_CONNECTING_STATE) {
        /* we get these when connection to Ensemble is dropped and retrying
         * this happens emsemble failover as well
         * since this is not memcached operation issue, we will allow reconnecting
         * but it is possible that app_ping fails and zk server may have disconnected
         * and if that's the case, we let heartbeat timeout algorithm decide
         * what to do.
         */
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "CONNECTING.... \n");
#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
        /* The server is disconnected from ZK.  But we do not know how much
         * time has elapsed from the last successful ZK ping.  A connection
         * timeout (session timeout * 2/3) might have occurred.  Or, the
         * TCP connection might have reset/closed (immediate).
         * See sm_timer_thread.  For now, it is being conservative and dies
         * in session timeout * 1/3.  Perhaps we should do our own ping, not
         * rely on ZK session notifications.  FIXME
         */
        zk_connected = false;
#endif
    }
}

#ifdef ENABLE_ZK_RECONFIG
/* config zk watcher */
static void
arcus_zkconfig_watcher(zhandle_t *zh, int type, int state, const char *path, void *ctx)
{
    if (path != NULL && strcmp(path, ZOO_CONFIG_NODE) == 0) {
        /* The ZK library has two threads of its own.  The completion thread
         * calls watcher functions.
         *
         * Do not do operations that may block or fail in the watcher context.
         */
        arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                "EVENT from ZK config: state=%d, path=%s\n",
                state, (path ? path : "null"));
    }

    /* Just wake up the sm thread and update the zk server list.
     * This may be a false positive (session event or others).
     * But it is harmless.
     */
    sm_lock();
    sm_info.request.update_zkconfig = true;
    sm_wakeup(true);
    sm_unlock();
}
#endif

/* cache_list zk watcher */
static void
arcus_cache_list_watcher(zhandle_t *zh, int type, int state, const char *path, void *ctx)
{
    if (type == ZOO_CHILD_EVENT) {
        /* The ZK library has two threads of its own.  The completion thread
         * calls watcher functions.
         *
         * Do not do operations that may block or fail in the watcher context.
         */
        arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                "ZOO_CHILD_EVENT from ZK cache list: state=%d, path=%s\n",
                state, (path ? path : "null"));
    } else {
        /* an unexpected event has been occurred */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Unexpected event(%d) gotten by cache_list_watcher: state=%d path=%s\n",
                type, state, (path ? path : "null"));
    }

    /* Just wake up the sm thread and update the hash ring.
     * This may be a false positive (session event or others).
     * But it is harmless.
     *
     * Should we do this only in ZOO_CHILD_EVENT ? FIXME.
     */
    sm_lock();
    sm_info.request.update_cache_list = true;
    sm_wakeup(true);
    sm_unlock();
}

static bool
check_my_znode_existence(void)
{
    if (cluster_config_get_self_id(arcus_conf.ch) == -1) {
        return false;
    }
    return true;
}

#ifdef ENABLE_CLUSTER_AWARE
/* update cluster config, that is ketama hash ring. */
static int
update_cluster_config(struct String_vector *strv, int *num_added, int *num_removed)
{
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                           "update cluster config. (count=%d)\n", (int)strv->count);
    if (arcus_conf.verbose > 0) {
        for (int i = 0; i < strv->count; i++) {
            arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                                   "server[%d] = %s\n", i, strv->data[i]);
        }
    }

    /* reconfigure arcus-memcached cluster */
    if (cluster_config_reconfigure(arcus_conf.ch, strv->data, strv->count,
                                   num_added, num_removed) < 0) {
        return -1;
    }
    sm_info.cluster_node_count = strv->count;
    return 0;
}
#endif

/* callback for sync command */
static void
arcus_zk_sync_cb(int rc, const char *name, const void *data)
{
    if (arcus_conf.verbose > 1) {
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "arcus_zk_sync cb\n");
    }
    // signal cond var
    inc_count(-1);
    last_rc = rc;
}

/* Log memcached join/leave acvitivity for debugging purpose
 * We cannot log in case of memcached crash
 */
static void
arcus_zk_log(zhandle_t *zh, const char *action)
{
    int     rc;
    char    zpath[200] = "";
    char    rcbuf[200] = "";
    char    sbuf[128] = "";

    struct timeval now;
    struct tm      *ltm;
    struct Stat    zstat;

    if (!zh || zoo_state(zh) != ZOO_CONNECTED_STATE || !arcus_conf.init)
        return;

    gettimeofday(&now, 0);
    ltm = localtime (&now.tv_sec);
    strftime(sbuf, sizeof(sbuf), "%Y-%m-%d", ltm);
    snprintf(zpath, sizeof(zpath), "%s/%s/%s", zk_root, zk_log_dir, sbuf);

    rc = zoo_exists(zh, zpath, ZK_NOWATCH, &zstat);
    if (rc == ZNONODE) {
        // if this "date" directory does not exist, create one
        rc = zoo_create(zh, zpath, NULL, 0, &ZOO_OPEN_ACL_UNSAFE,
                        0, rcbuf, sizeof(rcbuf));
        if (rc == ZOK && arcus_conf.verbose > 1) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "znode %s does not exist. created\n", rcbuf);
        }
    }
    if (rc == ZOK || rc == ZNODEEXISTS) {
        // if this znode already exist or the creation was successful
        snprintf(zpath, sizeof(zpath), "%s/%s/%s/%s-",
                 zk_root, zk_log_dir, sbuf, arcus_conf.znode_name);
        snprintf(sbuf,  sizeof(sbuf), "%s", action);
        rc = zoo_create(zh, zpath, sbuf, strlen(sbuf), &ZOO_OPEN_ACL_UNSAFE,
                        ZOO_SEQUENCE, rcbuf, sizeof(rcbuf));
        if (arcus_conf.verbose > 1) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "znode \"%s\" created\n", rcbuf);
        }
    }
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                "cannot log this acvitiy. error=%d(%s)\n", rc, zerror(rc));
    }
}

static void
arcus_exit(zhandle_t *zh, int err)
{
    if (zh)
        zookeeper_close(zh);
    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "shutting down\n");
    exit(err);
}

static int arcus_build_znode_name(char *ensemble_list)
{
    int                 sock, rc;
    socklen_t           socklen=16;
    struct in_addr      inaddr;
    struct sockaddr_in  saddr, myaddr;
    struct hostent      *zkhost, *hp;
    char                *zip, *hlist;
    char                *sep1=",";
    char                *sep2=":";
    char                myip[50];
    char                rcbuf[512];

    // Need to figure out local IP. first create a dummy udp socket
    if ((sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP)) == -1) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "socket() failed: %s\n", strerror(errno));
        return EX_OSERR;
    }

    saddr.sin_family = AF_INET;
    saddr.sin_port   = htons(SYSLOGD_PORT); // what if this is not open? XXX

    // loop through all ensemble IP in case ensemble failure
    zip = strtok_r(ensemble_list, sep1, &hlist); // separate IP:PORT tuple
    while (zip)
    {
        zip = strtok(zip, sep2); // extract the first IP

        // try to convert IP -> network IP format
        if (inet_aton(zip, &inaddr)) {
            // just use the IP
            saddr.sin_addr = inaddr;
            if (arcus_conf.verbose > 1) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "Trying ensemble IP: %s\n", zip);
            }
        } else {
            // Must be hostname, not IP. Convert hostname to IP first
            zkhost = gethostbyname(zip);
            if (!zkhost) {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Invalid IP/hostname in arg: %s\n", zip);
                // get next token: separate IP:PORT tuple
                zip = strtok_r(NULL, sep1, &hlist);
                continue;
            }
            memcpy((char *)&saddr.sin_addr,
                   zkhost->h_addr_list[0], zkhost->h_length);
            inet_ntop(AF_INET, &saddr.sin_addr, rcbuf, sizeof(rcbuf));
            if (arcus_conf.verbose > 1) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "Trying converted IP of hostname(%s): %s\n", zip, rcbuf);
            }
        }

        // try to connect to ensemble' arcus_conf.logger->logd UDP port
        // this is dummy code that simply is used to get local IP address
        int flags = fcntl(sock, F_GETFL, 0);
        fcntl(sock, F_SETFL, flags | O_NONBLOCK);
        rc = connect(sock, (struct sockaddr*) &saddr, sizeof(struct sockaddr_in));
        fcntl(sock, F_SETFL, flags);
        if (rc == 0) { // connect immediately
            if (arcus_conf.verbose > 1) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "connected to ensemble \"%s\" syslogd UDP port\n", zip);
            }
            break;
        }

        // if cannot connect immediately, try other ensemble
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Cannot connect to ensemble %s: %s\n", zip, strerror(errno));
        zip = strtok_r(NULL, sep1, &hlist); // get next token: separate IP:PORT tuple
    }
    if (!zip) { // failed to connect to all ensemble host. fail
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "not able to connect any Ensemble server\n");
        close(sock); sock = -1;
        return EX_UNAVAILABLE;
    }

    // finally, what's my local IP?
    if (getsockname(sock, (struct sockaddr *) &myaddr, &socklen)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "getsockname failed: %s\n", strerror(errno));
        close(sock); sock = -1;
        return EX_NOHOST;
    }
    close(sock); sock = -1;

    // Finally local IP
    inet_ntop(AF_INET, &myaddr.sin_addr, myip, sizeof(myip));
    arcus_conf.hostip = strdup(myip);
    arcus_conf.logger->log(EXTENSION_LOG_DETAIL, NULL, "local IP: %s\n", myip);

    if (getenv("ARCUS_CACHE_PUBLIC_IP") != NULL) {
        free(arcus_conf.hostip);
        arcus_conf.hostip = strdup(getenv("ARCUS_CACHE_PUBLIC_IP"));
        arcus_conf.logger->log(EXTENSION_LOG_DETAIL, NULL, "local public IP: %s\n",
                               arcus_conf.hostip);
    }

#ifdef PROXY_SUPPORT
    if (arcus_conf.proxy) {
        struct hostent  *proxy_host;
        char            *proxy_ip, *proxy_port;
        struct in_addr   inaddr;
        char             buf[50];

        proxy_ip = strtok(arcus_conf.proxy, sep2);
        proxy_port = strtok(NULL, sep2);

        if (inet_aton(proxy_ip, &inaddr)) {
            if (arcus_conf.verbose > 1) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "Proxy IP: %s\n", proxy_ip);
            }
        } else {
            // Must be hostname, not IP. Convert hostname to IP first
            proxy_host = gethostbyname(proxy_ip);
            if (!proxy_host) {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Invalid proxy IP/hostname in arg: %s\n", proxy_ip);
                return EX_UNAVAILABLE;
            }
            memcpy((char *)&inaddr,
                   proxy_host->h_addr_list[0], proxy_host->h_length);
            inet_ntop(AF_INET, &inaddr, buf, sizeof(buf));
            proxy_ip = buf;
        }

        proxy_host = gethostbyaddr((char*)&inaddr, sizeof(inaddr), AF_INET);

        if (!proxy_host) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "cannot get hostname: %s\n", strerror(errno));
            return EX_NOHOST;
        }

        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                               "proxy hostname: %s\n", proxy_host->h_name);

        snprintf(rcbuf, sizeof(rcbuf), "%s:%s", proxy_ip, proxy_port);
        arcus_conf.mc_ipport = strdup(rcbuf);
        snprintf(rcbuf, sizeof(rcbuf), "%s:%s", proxy_host->h_name, proxy_port);
        arcus_conf.mc_hostnameport = strdup(rcbuf);
        snprintf(rcbuf, sizeof(rcbuf), "%s-%s", arcus_conf.mc_ipport, proxy_host->h_name);
        arcus_conf.znode_name = strdup(rcbuf);
        return 0; // EX_OK
    }
#endif

    if (!arcus_conf.znode_name) {
        char *hostp=NULL;
        char  hostbuf[256];
        // Also get local hostname.
        // We want IP and hostname to better identify this cache
        hp = gethostbyaddr((char*)&myaddr.sin_addr.s_addr,
                            sizeof(myaddr.sin_addr.s_addr), AF_INET);
        if (hp) {
            hostp = hp->h_name;
        } else {
            // if gethostbyaddr() doesn't work, try gethostname
            if (gethostname((char *)&hostbuf, sizeof(hostbuf))) {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "cannot get hostname: %s\n", strerror(errno));
                return EX_NOHOST;
            }
            hostp = hostbuf;
        }
        if (strlen(hostp) > MAX_HOSTNAME_LENGTH) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Too long hostname. hostname=%s\n", hostp);
            return EX_DATAERR;
        }
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                               "local hostname: %s\n", hostp);

        // we need to keep ip:port-hostname tuple for later user (restart)
        snprintf(rcbuf, sizeof(rcbuf), "%s:%d", arcus_conf.hostip, arcus_conf.port);
        arcus_conf.mc_ipport = strdup(rcbuf);
        snprintf(rcbuf, sizeof(rcbuf), "%s:%d", hostp, arcus_conf.port);
        arcus_conf.mc_hostnameport = strdup(rcbuf);
        snprintf(rcbuf, sizeof(rcbuf), "%s-%s", arcus_conf.mc_ipport, hostp);
        arcus_conf.znode_name = strdup(rcbuf);
    }
    return 0; // EX_OK
}

static int arcus_parse_server_mapping(char *znode)
{
    char *start[4], *end[4]; /* enough pointers for future extension */
    int max_substrs = 4;
    int i, count;

    count = breakup_string(znode, start, end, max_substrs);
    /* Null-terminate substrings */
    for (i = 0; i < count; i++) {
        *end[i] = '\0';
    }

    /* get service code */
    arcus_conf.svc = strdup(start[0]);
    if (strlen(arcus_conf.svc) > MAX_SERVICECODE_LENGTH) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Too long service code. service_code=%s\n", arcus_conf.svc);
        return -1;
    }
    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Found the valid server mapping. service_code=%s\n",
            arcus_conf.svc);
    return 0;
}

#ifdef ENABLE_ZK_RECONFIG
static int arcus_check_zk_reconfig_enabled(zhandle_t *zh)
{
    int len = 256;
    char buf[256];
    struct Stat zstat;

    int rc = zoo_getconfig(zh, ZK_NOWATCH, buf, &len, &zstat);
    if (rc == ZOK && len > 0) {
        zk_reconfig.enabled = true;
        zk_reconfig.version = -1;
        arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                "Zookeeper dynamic reconfiguration is enabled.\n");
    } else if (rc == ZNONODE) {
        zk_reconfig.enabled = false;
        arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                "Zookeeper dynamic reconfiguration is disabled.\n");
    } else {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "zoo_getconfig() failed. error=%d(%s).\n", rc, zerror(rc));
        return -1;
    }
    return 0;
}
#endif

static int arcus_check_server_mapping(zhandle_t *zh, const char *root)
{
    struct String_vector strv = {0, NULL};
    char zpath[256];
    int  rc;

    /* sync map path */
    snprintf(zpath, sizeof(zpath), "%s/%s", root, zk_map_dir);
    inc_count(1);
    rc = zoo_async(zh, zpath, arcus_zk_sync_cb, NULL);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "zoo_async() failed: error=%d(%s)\n", rc, zerror(rc));
        return -1;
    }
    /* wait until above sync callback is called */
    wait_count(0);
    rc = last_rc;
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to synchronize zpath=%s: error=%d(%s)\n",
                zpath, rc, zerror(rc));
        return -1;
    }

    /* check the cache_server_mapping info. */
    do {
        /* 1st check: get children of "/cache_server_mapping/ip:port" */
        snprintf(zpath, sizeof(zpath), "%s/%s/%s",
                 root, zk_map_dir, arcus_conf.mc_ipport);
        rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
        if (rc != ZNONODE) break;

        /* 2nd check: get children of "/cache_server_mapping/hostname:port" */
        snprintf(zpath, sizeof(zpath), "%s/%s/%s",
                 root, zk_map_dir, arcus_conf.mc_hostnameport);
        rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
        if (rc != ZNONODE) break;

#ifdef PROXY_SUPPORT
        if (arcus_conf.proxy) {
            break; /* skip the below third checking */
        }
#endif
        /* 3rd check: get children of "/cache_server_mapping/ip" */
        snprintf(zpath, sizeof(zpath), "%s/%s/%s",
                 root, zk_map_dir, arcus_conf.hostip);
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Recheck the server mapping without port number. zpath=%s\n",
                zpath);
        rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
    } while(0);

    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to read the server mapping. zpath=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
        return -1;
    }

    if (strv.count == 1) {
        rc = arcus_parse_server_mapping(strv.data[0]);
        if (rc < 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to parse server mapping znode.\n");
        }
    } else {
        /* zoo_get_children returns ZOK even if no child exist */
        /* We assume only one server mapping znode. We do not care that
         * we have more than one (meaning one server participating in
         * more than one service in the cluster: not likely though)
         */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "The server mapping exists but has %d(!= 1) mapping znodes."
                " zpath=%s\n", strv.count, zpath);
        rc = -1;
    }
    deallocate_String_vector(&strv);
    return rc;
}

static int arcus_create_ephemeral_znode(zhandle_t *zh)
{
    int         rc;
    char        zpath[512];
    char        value[200];
    struct Stat zstat;

#ifdef PROXY_SUPPORT
    if (arcus_conf.proxy) {
        arcus_conf.znode_created = true;
        return 0;
    }
#endif

    snprintf(zpath, sizeof(zpath), "%s/%s",
             arcus_conf.cluster_path, arcus_conf.znode_name);
    snprintf(value, sizeof(value), "%ldMB", (long)arcus_conf.maxbytes/1024/1024);
    rc = zoo_create(zh, zpath, value, strlen(value), &ZOO_OPEN_ACL_UNSAFE,
                    ZOO_EPHEMERAL, NULL, 0);
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "cannot create znode %s: error=%d(%s)\n", zpath, rc, zerror(rc));
        if (rc == ZNODEEXISTS) {
            arcus_conf.logger->log( EXTENSION_LOG_DETAIL, NULL,
                    "old session still exist. Wait a bit\n");
        }
        return -1;
    }

    rc = zoo_exists(zh, zpath, 0, &zstat);
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "cannot find znode %s just created: error=%d(%s)\n",
                zpath, rc, zerror(rc));
        return -1;
    }

    /* store this version number, just in case we restart */
    arcus_conf.znode_ver = zstat.version;
    arcus_conf.znode_created = true;
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
            "Joined Arcus cloud at %s(ver=%d)\n", zpath, arcus_conf.znode_ver);

    /* log this join activity in /arcus/cache_server_log */
    arcus_zk_log(main_zk->zh, sm_info.mc_pause? "rejoin" : "join");
    return 0;
}

static int arcus_register_cache_instance(zhandle_t *zh)
{
    /* create "/cache_list/{svc}/ip:port-hostname" ephemeral znode */
    if (arcus_create_ephemeral_znode(zh) != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "arcus_create_ephemeral_znode() failed.\n");
        return -1;
    }
    return 0;
}

static char *arcus_get_self_name(void)
{
    return arcus_conf.mc_ipport;
}

static bool sm_check_mc_paused(void)
{
    bool paused = false;
    pthread_mutex_lock(&zk_lock);
    if (sm_info.mc_pause == true) {
        if (main_zk->zh != NULL) {
            zookeeper_close(main_zk->zh);
            main_zk->zh = NULL;
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "zk connection closed by mc_pause.\n");
        }
        paused = true;
    }
    pthread_mutex_unlock(&zk_lock);
    return paused;
}

static inline int
sm_retry_msec_by_zoo_error(int rc, int default_msec)
{
    int retry_msec = default_msec; /* default 100 ms */

    switch(rc) {
    case ZCONNECTIONLOSS:   /* Connection to the server has been lost */
    case ZSESSIONEXPIRED:   /* The session has been expired by the server */
        retry_msec = 1000;
        break;
    case ZOPERATIONTIMEOUT: /* Operation timeout */
    case ZSESSIONMOVED:     /* Session moved to another server, so operation is ignored */
    case ZNOTHING:          /* (not error) no server responses to process */
        retry_msec = 100;
        break;
    case ZNONODE:           /* Node does not exist */
        retry_msec = 2000;
        break;
    }
    return retry_msec;
}

static inline void sm_set_retry_ms(int *final_retry_ms, int local_retry_ms)
{
    assert(local_retry_ms > 0);
    if (*final_retry_ms <= 0 || *final_retry_ms > local_retry_ms) {
        *final_retry_ms = local_retry_ms;
    }
}

static void sm_check_and_scrub_stale(int *retry_ms)
{
    assert(sm_info.node_added_time > 0);
    int elapsed = time(NULL) - sm_info.node_added_time;
    if (elapsed >= (arcus_conf.zk_timeout/1000)) {
       /* remove stale items after zk_timeout have passed
        * since a new node is added to the cluster
        */
        if (arcus_memcached_scrub_stale() == 0) {
            sm_info.node_added_time = 0;
        } else {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to start scrub stale task.\n");
            sm_set_retry_ms(retry_ms, 1000); /* Do retry in 1 second */
        }
    } else {
        int wait_msec = arcus_conf.zk_timeout;
        if (elapsed > 0) {
            wait_msec -= (elapsed * 1000);
        }
        sm_set_retry_ms(retry_ms, wait_msec); /* Do retry */
    }
}

static int sm_reload_cache_list_znode(zhandle_t *zh, int *retry_ms)
{
    struct String_vector strv_cache_list = {0, NULL};
    bool znode_deleted = false;

    int rc = zoo_wget_children(zh, arcus_conf.cluster_path,
                                   arcus_cache_list_watcher, NULL,
                                   &strv_cache_list);
    if (rc != ZOK) {
      arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
              "Failed to reload cache_list in zpath=%s: error=%d(%s)\n",
              arcus_conf.cluster_path, rc, zerror(rc));
      if (rc == ZNONODE) { /* the node does not exist */
        znode_deleted = true;
      } else {
        int rc_retry_ms = sm_retry_msec_by_zoo_error(rc, 100);
        sm_set_retry_ms(retry_ms, rc_retry_ms); /* Do retry */
        sm_lock();
        sm_info.request.update_cache_list = true;
        sm_unlock();
      }
    } else {
        /* Remember the latest cache list */
        deallocate_String_vector(&sm_info.sv_cache_list);
        sm_info.sv_cache_list = strv_cache_list;

#ifdef ENABLE_CLUSTER_AWARE
        /* update cluster config */
        int num_added, num_removed;
        if (update_cluster_config(&sm_info.sv_cache_list,
                                  &num_added, &num_removed) != 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to update cluster config. Will check later.\n");
        } else { /* cluster config is updated */
            if (arcus_conf.znode_created && !check_my_znode_existence()) {
                znode_deleted = true;
            }
            else if (arcus_conf.auto_scrub && num_added > 0) {
                /* a new node added */
                sm_info.node_added_time = time(NULL);
                sm_set_retry_ms(retry_ms, arcus_conf.zk_timeout); /* Do retry */
            }
        }
#endif
    }

    if (znode_deleted) {
#ifdef PROXY_SUPPORT
        if (arcus_conf.proxy) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Proxy server down. shutting down\n");
            return -1;
        }
#endif
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "My ephemeral znode in cache_list is deleted. "
                "Please check and rejoin.");
        sm_lock();
        sm_info.mc_pause = true;
        sm_unlock();
        sm_set_retry_ms(retry_ms, 100); /* Do retry */
    }
    return 0;
}

#ifdef ENABLE_ZK_RECONFIG
static int get_zk_client_ipport(char *startp, char *endp, char *buf)
{
    char *clientp, *ip_addrp, *portp, *colonp;
    int ip_addr_len, port_len;
    int length = 0;

    clientp = memchr(startp, ';', endp - startp);
    if (!clientp) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Invalid ZK config string. missing client port in zoo.cfg.dynamic file.\n");
        return -1;
    }
    clientp++;

    colonp = memchr(clientp, ':', endp - clientp);
    if (colonp && strncmp(clientp, "0.0.0.0", colonp - clientp) != 0) {
        memcpy(buf, clientp, endp - clientp);
        return endp - clientp;
    }
    portp = colonp ? (colonp + 1) : clientp;
    port_len = endp - portp;

    ip_addrp = memchr(startp, '=', clientp - startp);
    if (!ip_addrp) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Invalid ZK config string. missing server ip in zoo.cfg.dynamic file.\n");
        return -1;
    }
    do {
        ip_addrp++;
    } while (ip_addrp[0] == ' ' && ip_addrp < clientp);

    colonp = memchr(ip_addrp, ':', clientp - ip_addrp);
    if (!colonp) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Invalid ZK config string. missing zk port in zoo.cfg.dynamic file.\n");
        return -1;
    }
    ip_addr_len = colonp - ip_addrp + 1;  // ip_addr + colon(:)

    memcpy(buf + length, ip_addrp, ip_addr_len);
    length += ip_addr_len;
    memcpy(buf + length, portp, port_len);
    length += port_len;

    return length;
}

static int get_client_config_data(char *data_buf, int data_len,
                                  char *host_buf, int64_t *version)
{
    char *startp, *endp, *versionp, *versionerrp;
    int len;
    int host_len = 0;

    startp = &data_buf[0];
    data_buf[data_len] = '\0';
    while ((endp = memchr(startp, '\n', data_len)) != NULL) {
        len = get_zk_client_ipport(startp, endp, host_buf + host_len);
        if (len <= 0) {
            return -1;
        }
        host_len += len;
        memcpy(host_buf + host_len, ",", 1);
        host_len++;

        /* next server id */
        data_len -= (endp - startp) + 1;
        startp = endp + 1;
    }

    /* [host_len-1] is last comma character */
    if (host_len > 0) {
        host_buf[host_len-1] = '\0';
    }

    /* make config version */
    versionp = memchr(startp, '=', data_len);
    if (!versionp) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Invalid ZK config string. missing version in zoo.cfg.dynamic file.\n");
        return -1;
    }
    versionp++;
    *version = 0;
    *version = strtoll(versionp, &versionerrp, 16);
    if ((errno == ERANGE && (*version == LLONG_MAX || *version == LLONG_MIN)) ||
        (errno != 0 && *version == 0)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Invalid ZK config string. invalid version in zoo.cfg.dynamic file.\n");
        return -1;
    }
    return 0;
}

static int arcus_zoo_set_servers(const char *hosts)
{
    int ret = 0;

    pthread_mutex_lock(&zk_lock);
    do {
        if (main_zk->zh == NULL) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to change the ZooKeeper ensemble list. Invalid ZK handle.\n");
            ret = -1; break;
        }
        char *copy = strdup(hosts);
        if (!copy) {
            /* Should not happen unless the system is really short of memory. */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to copy the ensemble list. list=%s\n", hosts);
            ret = -1; break;
        }
        /* To avoid mass client migration at the same time,
        * sleep a random short period of time before zoo_set_servers().
        */
        srand(time(NULL));
        usleep(rand() % 1000); /* 0~1000 usec. */
        int rc = zoo_set_servers(main_zk->zh, hosts);
        if (rc != ZOK) {
            free(copy);
            /* Some internal errors may occur, retry at next event.
            * If need more complete error handling,
            * save the list of servers and try again.
            */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to update ZK servers: error=%d(%s)\n",
                    rc, zerror(rc));
            ret = -1; break;
        }
        char *old_ensemble = main_zk->ensemble_list;
        main_zk->ensemble_list = copy;
        free(old_ensemble);
    } while (0);
    pthread_mutex_unlock(&zk_lock);
    return ret;
}

static void sm_reload_ZK_config(zhandle_t *zh, int *retry_ms)
{
    struct Stat zstat;
    char *data_buf = zk_reconfig.data_buffer;
    int   data_len = MAX_ZK_CONFIG_DATA_LENGTH;
    char *host_buf = zk_reconfig.host_buffer;
    int64_t version;

    int rc = zoo_wgetconfig(zh, arcus_zkconfig_watcher, NULL,
                                data_buf, &data_len, &zstat);
    if (rc != ZOK) {
      arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
              "Failed to reload /zookeeper/config znode: error=%d(%s)\n",
              rc, zerror(rc));
      if (rc == ZNONODE) {
        /* arcus_zkconfig_watcher won't be triggered. */
        zk_reconfig.enabled = false;
        arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                "Zookeeper dynamic reconfiguration is disabled.\n");
      } else {
        int rc_retry_ms = sm_retry_msec_by_zoo_error(rc, 100);
        sm_set_retry_ms(retry_ms, rc_retry_ms); /* Do retry */
        sm_lock();
        sm_info.request.update_zkconfig = true;
        sm_unlock();
      }
    } else {
        /* zookeeper config format generated by the zk server.
         * server.1=127.0.0.1:2888:3888:participant;0.0.0.0:2181\n
         * server.2=127.0.0.1:2889:3889:participant;0.0.0.0:2182\n
         * version=10000000d
         */
        if (data_len <= 0 || data_len >= MAX_ZK_CONFIG_DATA_LENGTH) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Invalid ZK config data length(%d).\n", data_len);
            return;
        }
        if (get_client_config_data(data_buf, data_len, host_buf, &version) < 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Invalid ZK config data found.\n");
            return;
        }

        if (zk_reconfig.version == -1) {
            zk_reconfig.version = version;
        } else if (version != 0 && version > zk_reconfig.version) {
            /* version will be greater than 0 if ZK config data has been synced. */
            arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                    "Updated ZK servers... ZK servers[%s], version[%" PRIx64 "]\n",
                    host_buf, version);

            if (arcus_zoo_set_servers(host_buf) != 0) {
                return;
            }
            zk_reconfig.version = version;
        } else if (version < zk_reconfig.version) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Unexpected ZK config version. "
                    "zk_reconfig.version=%" PRIx64 ", version=%" PRIx64 "\n",
                    zk_reconfig.version, version);
        }
    }
}
#endif

#ifdef ENABLE_ZK_RECONFIG
static int arcus_check_domain_name(char *ensemble_list)
{
    char *copy, *token, *save_ptr;
    struct hostent *zkhost;
    int ret;

    copy = strdup(ensemble_list);
    if (copy == NULL) {
        return -1;
    }

    do {
        if (strchr(copy, ',')) {
            ret = 0; break;
        }

        token = strtok_r(ensemble_list, ":", &save_ptr);
        if (inet_aton(token, NULL)) {
            ret = 0; break;
        }

        zkhost = gethostbyname(token);
        if (!zkhost) {
            ret = 0; break;
        }

        int addr_cnt = 0;
        while (zkhost->h_addr_list[addr_cnt]) {
            addr_cnt++;
        }
        ret = (addr_cnt >= 3) ? 1 : 0;
    } while (0);

    free(copy);
    return ret;
}
#endif

void arcus_zk_init(char *ensemble_list, int zk_to,
                   EXTENSION_LOGGER_DESCRIPTOR *logger,
                   int verbose, size_t maxbytes, int port,
#ifdef PROXY_SUPPORT
                   char *proxy,
#endif
                   ENGINE_HANDLE_V1 *engine)
{
    int             rc;
    char            zpath[256] = "";
    struct timeval  start_time, end_time;
    long            difftime_us;

    assert(logger);
    assert(engine);
    if (!ensemble_list) { // Arcus Zookeeper Ensemble IP list
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    " -z{ensemble_list} must not be empty\n");
        arcus_exit(NULL, EX_USAGE);
    }
    logger->log(EXTENSION_LOG_INFO, NULL,
                "arcus_zk_init(ensemble=%s, zk_timeout=%d)\n",
                ensemble_list, zk_to);

    memset(&zk_info, 0, sizeof(zk_info_t));
    main_zk = &zk_info;
    main_zk->ensemble_list = strdup(ensemble_list);
    assert(main_zk->ensemble_list);

    /* save these for later use (restart) */
    if (!arcus_conf.init) {
        arcus_conf.port      = port;    // memcached listen port
        arcus_conf.logger    = logger;
        arcus_conf.engine.v1 = engine;
        arcus_conf.verbose   = verbose;
        arcus_conf.maxbytes  = maxbytes;
#ifdef PROXY_SUPPORT
        arcus_conf.proxy     = proxy;
#endif
        /* Use the user specified timeout only if it falls within [MIN, MAX].
         * Otherwise, silently ignore it and use the default value.
         */
        if (zk_to >= MIN_ZK_TO && zk_to <= MAX_ZK_TO) {
            arcus_conf.zk_timeout = zk_to*1000; // msec conversion
        }
    }

    /* initialize Arcus ZK stats */
    memset(&azk_stat, 0, sizeof(azk_stat));

    /* make znode name while getting local ip and hostname */
    rc = arcus_build_znode_name(ensemble_list);
    if (rc != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Failed to build znode name.\n");
        arcus_exit(NULL, rc);
    }

    if (strncmp("syslog", arcus_conf.logger->get_name(), 7) == 0) {
        zoo_forward_logs_to_syslog("memcached", 1);
    }

    if (arcus_conf.verbose > 1) {        // -vv, -vvv
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    } else if (arcus_conf.verbose > 0) { // -v
        zoo_set_debug_level(ZOO_LOG_LEVEL_INFO);
    } else {                             // default
        zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
    }

    gettimeofday(&start_time, 0);

    rc = arcus_zk_client_init(main_zk);
    if (rc != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Failed to initialize zk client\n");
        arcus_exit(NULL, rc);
    }

    /* check zk root directory and get the service code */
    if (zk_root == NULL) {
        zk_root = "/arcus"; /* set zk root directory */
        if (arcus_check_server_mapping(main_zk->zh, zk_root) != 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to check server mapping for this cache node. "
                    "(zk_root=%s)\n", zk_root);
            arcus_exit(main_zk->zh, EX_PROTOCOL);
        }
    }

    snprintf(zpath, sizeof(zpath), "%s/%s/%s", zk_root, zk_cache_dir, arcus_conf.svc);
    arcus_conf.cluster_path = strdup(zpath);
    assert(arcus_conf.cluster_path);

    /* Initialize the state machine. */
    if (sm_init() != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to initialize the state machine. Terminating.\n");
        arcus_exit(main_zk->zh, EX_CONFIG);
    }

#ifdef ENABLE_ZK_RECONFIG
    /* Initialize zk_reconfig */
    int is_domain_name = arcus_check_domain_name(main_zk->ensemble_list);
    if (is_domain_name < 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to initialize the zk reconfig. Terminating.\n");
        arcus_exit(main_zk->zh, EX_SOFTWARE);
    }
    zk_reconfig.needed = (is_domain_name == 0);
    zk_reconfig.enabled = false;
    zk_reconfig.version = -1;
    if (zk_reconfig.needed) {
        /* check if ZK dynamic reconfig is enabled ? */
        if (arcus_check_zk_reconfig_enabled(main_zk->zh) != 0) {
            arcus_exit(main_zk->zh, EX_PROTOCOL);
        }
    }
#endif

    arcus_conf.init = true;

    /* register cache instance in ZK */
    if (arcus_register_cache_instance(main_zk->zh) != 0) {
        arcus_exit(main_zk->zh, EX_PROTOCOL);
    }

    gettimeofday(&end_time, 0);
    difftime_us = (end_time.tv_sec*1000000 + end_time.tv_usec) -
                  (start_time.tv_sec*1000000 + start_time.tv_usec);

    /* We have finished registering this memcached instance to Arcus cluster */
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
            "Memcached joined Arcus cache cloud for \"%s\" service "
            "(took %ld microsec)\n", arcus_conf.svc, difftime_us);

#ifdef ENABLE_CLUSTER_AWARE
    const char *self_name = arcus_get_self_name();
    arcus_conf.ch = cluster_config_init(self_name,
                                        arcus_conf.logger, arcus_conf.verbose);
    assert(arcus_conf.ch);

    struct String_vector strv = { 0, NULL };
    rc = zoo_get_children(main_zk->zh, arcus_conf.cluster_path, ZK_NOWATCH, &strv);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to read cache list in zpath=%s: error=%d(%s) Terminating...\n",
                arcus_conf.cluster_path, rc, zerror(rc));
        arcus_exit(main_zk->zh, EX_CONFIG);
    }
    if (update_cluster_config(&strv, NULL, NULL) != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to update cluster config. Terminating...\n");
        arcus_exit(main_zk->zh, EX_CONFIG);
    }
    deallocate_String_vector(&strv);

    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Memcached is watching Arcus cache cloud for \"%s\"\n",
            arcus_conf.cluster_path);
#endif

    /* Wake up the state machine thread.
     * Tell it to refresh the hash ring (cluster_config).
     */
    sm_lock();
#ifdef ENABLE_ZK_RECONFIG
    if (zk_reconfig.enabled) {
        sm_info.request.update_zkconfig = true;
    }
#endif
    /* Don't care if we just read the list above.  Do it again. */
    sm_info.request.update_cache_list = true;
    sm_wakeup(true);
    sm_unlock();

    /* Either got SIG* or memcached shutdown process finished */
    if (arcus_zk_shutdown) {
        arcus_zk_final("Interrupted");
        arcus_zk_destroy();
        arcus_exit(main_zk->zh, EX_OSERR);
    }
}

/* Close the ZK connection and die. Do not do anything that may
 * block here such as deleting the ephemeral znode.
 * It will be deleted, after closing the ZK connection(or session).
 */
void arcus_zk_final(const char *msg)
{
    assert(arcus_zk_shutdown == 1);
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "arcus_zk_final(%s)\n", msg);

    pthread_mutex_lock(&zk_lock);
    if (main_zk->zh) {
        /* wake up the SM state thread if it's sleeping */
        sm_wakeup(false);

        /* wait a maximum of 1000 msec */
        int elapsed_msec = 0;
        while (elapsed_msec <= 1000) {
            if (elapsed_msec == 0) {
                /* the first check: go below */
            } else {
                usleep(10000); // 10ms wait
                elapsed_msec += 10;
            }
            /* Do not call zookeeper_close (arcus_exit) and zoo_wget_children at
             * the same time.  Otherwise, this thread (main thread) and the watcher
             * thread (zoo_wget_children) may hang forever until the user manually
             * kills the process.  The root cause is within the zookeeper library.
             * Its handling of concurrent API calls like zoo_wget_children and
             * zookeeper_close still has holes...
             */
            /* If the watcher thread (zoo_wget_children) is running now, wait for
             * one second.  Either it completes, or it at least registers
             * sync_completion and blocks.  In the latter case, zookeeper_close
             * aborts all sync_completion's, which then wakes up the blocking
             * watcher thread.  zoo_wget_children returns an error.
             */
            if (sm_info.state_running)
                continue;
#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
            if (sm_info.timer_running)
                continue;
#endif
            arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "zk threads terminated\n");
            break;
        }

        /* close zk connection */
        zookeeper_close(main_zk->zh);
        main_zk->zh = NULL;
        free(main_zk->ensemble_list);
        main_zk->ensemble_list = NULL;
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "zk connection closed\n");
    }
    pthread_mutex_unlock(&zk_lock);
}

void arcus_zk_destroy(void)
{
    assert(arcus_zk_shutdown == 1);
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "arcus_zk_destroy\n");

    pthread_mutex_lock(&zk_lock);
#ifdef ENABLE_CLUSTER_AWARE
    if (arcus_conf.ch != NULL) {
        cluster_config_final(arcus_conf.ch);
        arcus_conf.ch = NULL;
    }
#endif
    pthread_mutex_unlock(&zk_lock);
}

int arcus_zk_set_ensemble(char *ensemble_list)
{
    int ret = 0;

    pthread_mutex_lock(&zk_lock);
    do {
        if (main_zk->zh == NULL) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to change the ZooKeeper ensemble list. Invalid ZK handle.\n");
            ret = -1; break;
        }
        char *copy = strdup(ensemble_list);
        if (!copy) {
            /* Should not happen unless the system is really short of memory. */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to copy the ensemble list. list=%s\n", ensemble_list);
            ret = -1; break;
        }
        int rc = zookeeper_change_ensemble(main_zk->zh, ensemble_list);
        if (rc != ZOK) {
            free(copy);
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to change the ZooKeeper ensemble list. error=%d(%s)\n",
                    rc, zerror(rc));
            ret = -1; break;
        }
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Successfully changed the ZooKeeper ensemble list. list=%s\n",
                ensemble_list);
        char *old_ensemble = main_zk->ensemble_list;
        main_zk->ensemble_list = copy;
        free(old_ensemble);
    } while(0);
    pthread_mutex_unlock(&zk_lock);
    return ret;
}

int arcus_zk_get_ensemble(char *buf, int size)
{
    int ret = 0;
    pthread_mutex_lock(&zk_lock);
    if (main_zk->zh) {
        int rc = zookeeper_get_ensemble_string(main_zk->zh, buf, size);
        if (rc != ZOK) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to get the ZooKeeper ensemble list. error=%d(%s)\n",
                    rc, zerror(rc));
            ret = -1;
        }
    } else {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to get the ZooKeeper ensemble list. Invalid ZK handle.\n");
        ret = -1;
    }
    pthread_mutex_unlock(&zk_lock);
    return ret;
}

int arcus_zk_rejoin_ensemble(void)
{
    int ret = 0;

    clear_count();

    pthread_mutex_lock(&zk_lock);

    if (main_zk->zh != NULL) {
        pthread_mutex_unlock(&zk_lock);
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to rejoin ensemble. It's already a member of cloud.\n");
        return -1;
    }

    assert(sm_info.mc_pause == true);

    do {
        struct timeval  start_time, end_time;
        long            difftime_us;

        /* initialize Arcus ZK stats */
        memset(&azk_stat, 0, sizeof(azk_stat));
        memset(&main_zk->myid, 0, sizeof(clientid_t));
        assert(main_zk->ensemble_list);

        gettimeofday(&start_time, 0);
        if (arcus_zk_client_init(main_zk) != 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                                   "Failed to initialize zk client.\n");
            ret = -1; break;
        }

        /* create "/cache_list/{svc}/ip:port-hostname" ephemeral znode */
        if (arcus_create_ephemeral_znode(main_zk->zh) != 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                                   "arcus_create_ephemeral_znode() failed.\n");
            ret = -1; break;
        }
        gettimeofday(&end_time, 0);
        difftime_us = (end_time.tv_sec*1000000 + end_time.tv_usec) -
                      (start_time.tv_sec*1000000 + start_time.tv_usec);

        /* We have finished registering this memcached instance to Arcus cluster */
        arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                "Memcached rejoined Arcus cache cloud for \"%s\" service "
                "(took %ld microsec)\n", arcus_conf.svc, difftime_us);

        assert(arcus_conf.ch);
        struct String_vector strv = { 0, NULL };
        int rc = zoo_get_children(main_zk->zh, arcus_conf.cluster_path, ZK_NOWATCH, &strv);
        if (rc != ZOK) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to read cache list in zpath=%s: error=%d(%s)\n",
                    arcus_conf.cluster_path, rc, zerror(rc));
            ret = -1; break;
        }
        if (update_cluster_config(&strv, NULL, NULL) != 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to update cluster config.\n");
            ret = -1; break;
        }
        deallocate_String_vector(&strv);

        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Memcached is watching Arcus cache cloud for \"%s\"\n",
                arcus_conf.cluster_path);

        /* Wake up the state machine thread.
         * Tell it to refresh the hash ring (cluster_config).
         */
        sm_lock();
        sm_info.mc_pause = false;
        /* Don't care if we just read the list above.  Do it again. */
        sm_info.request.update_cache_list = true;
        sm_wakeup(true);
        sm_unlock();
    } while(0);

    if (ret != 0) {
        if (main_zk->zh) {
          zookeeper_close(main_zk->zh);
          main_zk->zh = NULL;
        }
    }
    pthread_mutex_unlock(&zk_lock);

    return ret;
}

void arcus_zk_set_failstop(bool failstop)
{
    pthread_mutex_lock(&arcus_conf.lock);
    arcus_conf.zk_failstop = failstop;
    pthread_mutex_unlock(&arcus_conf.lock);
}

bool arcus_zk_get_failstop(void)
{
    return arcus_conf.zk_failstop;
}

void arcus_zk_get_confs(arcus_zk_confs *confs)
{
    confs->zk_libversion = zoo_version_str();
    confs->zk_timeout = arcus_conf.zk_timeout;
    confs->zk_failstop = arcus_conf.zk_failstop;
}

void arcus_zk_get_stats(arcus_zk_stats *stats)
{
    stats->zk_connected = (main_zk != NULL && main_zk->zh != NULL) ? true : false;
    stats->zk_ready = arcus_conf.znode_created && !sm_info.mc_pause;
#ifdef ENABLE_ZK_RECONFIG
    stats->zk_reconfig_needed = zk_reconfig.needed;
    stats->zk_reconfig_enabled = zk_reconfig.enabled;
    stats->zk_reconfig_version = zk_reconfig.version;
#endif
}

#ifdef ENABLE_CLUSTER_AWARE
int arcus_key_is_mine(const char *key, size_t nkey, bool *mine)
{
    int      ret;
    uint32_t key_id, self_id;

    ret = cluster_config_key_is_mine(arcus_conf.ch, key, nkey, mine,
                                     &key_id, &self_id);
    if (ret == 0) {
        if (arcus_conf.verbose > 1) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "key=%s, self_id=%d, key_id=%d, mine=%s\n",
                    key, self_id, key_id, ((*mine) ? "true" : "false"));
        }
    }
    return ret;
}
#endif

static void sm_get_timespec(struct timespec *ts, int msec)
{
  struct timeval tv;
  uint64_t now;

  gettimeofday(&tv, NULL);
  now = ((uint64_t)tv.tv_sec * 1000) + ((uint64_t)tv.tv_usec / 1000);
  now += msec;
  ts->tv_sec = now / 1000;
  ts->tv_nsec = (now % 1000) * 1000000;
}

static void *sm_state_thread(void *arg)
{
    struct sm_request smreq;
    struct timespec ts;
    int sm_retry_ms = 0;
    bool shutdown_by_me = false;

    sm_info.mc_pause = false;
    sm_info.state_running = true;

    while (!arcus_zk_shutdown)
    {
        sm_lock();
        while (!sm_info.notification && !arcus_zk_shutdown) {
            /* Poll if requested.
             * Otherwise, wait till the ZK watcher wakes us up.
             */
            if (sm_retry_ms > 0) {
                /* Wake up in 100 ms.  Magic number.  FIXME */
                sm_get_timespec(&ts, sm_retry_ms);
                pthread_cond_timedwait(&sm_info.cond, &sm_info.lock, &ts);
                sm_retry_ms = 0;
                break;
            }
            pthread_cond_wait(&sm_info.cond, &sm_info.lock);
        }
        sm_info.notification = false;
        smreq = sm_info.request;
        memset(&sm_info.request, 0, sizeof(struct sm_request));
        sm_unlock();

        if (arcus_zk_shutdown)
            break;

        if (sm_info.mc_pause == true) {
            if (sm_check_mc_paused()) {
                continue; /* zk paused */
            }
        }

        if (sm_info.node_added_time != 0) {
            sm_check_and_scrub_stale(&sm_retry_ms);
        }

#ifdef ENABLE_ZK_RECONFIG
        if (smreq.update_zkconfig) {
            sm_reload_ZK_config(main_zk->zh, &sm_retry_ms);
        }
#endif

        /* Read the latest hash ring */
        if (smreq.update_cache_list) {
            if (sm_reload_cache_list_znode(main_zk->zh, &sm_retry_ms) < 0) {
                shutdown_by_me = true; break;
            }
            if (sm_info.mc_pause) continue;
        }
    }
    sm_info.state_running = false;

    deallocate_String_vector(&sm_info.sv_cache_list);
    if (shutdown_by_me) {
        arcus_zk_shutdown = 1;
        arcus_zk_final("SM state failure");
        arcus_zk_destroy();
        exit(0);
    }
    return NULL;
}

#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
static void *sm_timer_thread(void *arg)
{
    struct timeval tv;
    uint64_t start_msec = 0, now_msec;
    bool shutdown_by_me = false;

    /* Run forever.
     * When the ZK watcher says it lost connection to ZK, start running
     * the timer.  If the timer expires, fail-stop.  If the ZK watcher
     * says it re-connected to ZK, cancel the timer.
     */
    sm_info.timer_running = true;
    while (!arcus_zk_shutdown)
    {
        /* Keep the logic simple.  Poll once every 100msec. */
        usleep(100000);

        if (arcus_zk_shutdown)
            break;

        if (!zk_connected) {
            gettimeofday(&tv, NULL);
            now_msec = ((uint64_t)tv.tv_sec*1000 + (uint64_t)tv.tv_usec/1000);

            if (start_msec == 0)
                start_msec = now_msec;
            if ((now_msec - start_msec) >= arcus_conf.zk_timeout/3 - 1000) {
                /* We need to die slightly before our session times out
                 * in ZK ensemble.  Otherwise, we might end up with multiple
                 * masters.  It is very fragile...  FIXME
                 */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Disconnected from ZK for too long. Terminating... "
                        "duration_msec=%"PRIu64"\n", now_msec - start_msec);
                shutdown_by_me = true;
                break;
                /* Do we have to kill the slave too?  Or, only the master?
                 * Also, make this fail-stop behavior a runtime option.
                 * We may not want to kill the master even if it's partitioned
                 * from ZK?  FIXME
                 */
            }
        } else {
            /* Still connected to ZK */
            start_msec = 0;
        }
    }
    sm_info.timer_running = false;

    if (shutdown_by_me) {
        arcus_zk_shutdown = 1;
        arcus_zk_final("SM timer failure");
        arcus_zk_destroy();
        exit(0);
    }
    return NULL;
}
#endif

static int sm_init(void)
{
    pthread_attr_t attr;
    int ret;

    /* clear sm_info structure */
    memset(&sm_info, 0, sizeof(sm_info));

    pthread_mutex_init(&sm_info.lock, NULL);
    pthread_cond_init(&sm_info.cond, NULL);

    pthread_attr_init(&attr);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);

    /* sm state thread */
    ret = pthread_create(&sm_info.state_tid, &attr, sm_state_thread, NULL);
    if (ret != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Cannot create sm state thread: %s\n", strerror(ret));
        return -1;
    }
#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
    /* sm timer thread */
    ret = pthread_create(&sm_info.timer_tid, &attr, sm_timer_thread, NULL);
    if (ret != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Cannot create sm timer thread: %s\n", strerror(ret));
        return -1;
    }
#endif
    return 0;
}

static void sm_lock(void)
{
    pthread_mutex_lock(&sm_info.lock);
}

static void sm_unlock(void)
{
    pthread_mutex_unlock(&sm_info.lock);
}

static void sm_wakeup(bool locked)
{
    /* 'locked' is mostly to force the user to think about locking. */
    if (!locked)
        sm_lock();
    if (!sm_info.notification) {
        sm_info.notification = true;
        pthread_cond_signal(&sm_info.cond);
    }
    if (!locked)
        sm_unlock();
}

#endif /* ENABLE_ZK_INTEGRATION */
