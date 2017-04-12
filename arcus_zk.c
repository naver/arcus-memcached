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

// Recv. timeout governs ZK heartbeat period, reconnect timeout
// as well as ZK session timeout
//
// Below sets 30 sec. session timeout, 20 sec Zookeeper ensemble timeout,
// and 10 second heartbeat period
#define DEFAULT_ZK_TO   30000   // ZK session timeout in msec (server tick time is 2 sec, min is 4 sec)
#define MAX_ZK_TO       300     // Max allowable session timeout in sec
#define MIN_ZK_TO       10      // Min 10 seconds, in line with the current heartbeat timeout

#define ZK_WATCH        1
#define ZK_NOWATCH      0
#define SYSLOGD_PORT    514     // UDP syslog port #

/* One heartbeat every 3 seconds. */
#define HEART_BEAT_PERIOD       3 /* sec */
/* If consecutive heartbeats fail, consider memcached dead and commit
 * suicide.  If the accumulated latency of consecutive heartbeats is
 * over the following HEART_BEAT_FAILSTOP value, it does fail-stop.
 */
#define HEART_BEAT_MIN_FAILSTOP 3000  /* msec */
#define HEART_BEAT_DFT_FAILSTOP 30000 /* msec */
#define HEART_BEAT_MAX_FAILSTOP 60000 /* msec */
/* If hearbeat takes more than timeout msec, consider it failed.  */
#define HEART_BEAT_MIN_TIMEOUT  50    /* msec */
#define HEART_BEAT_DFT_TIMEOUT  10000 /* msec */
#define HEART_BEAT_MAX_TIMEOUT  HEART_BEAT_MAX_FAILSTOP /* msec */

#define MAX_SERVICECODE_LENGTH  32

static const char *zk_root = NULL;
static const char *zk_map_dir = "cache_server_mapping";
static const char *zk_log_dir = "cache_server_log";
static const char *zk_cache_dir = "cache_list";
static const char *mc_hb_command = "set arcus:zk-ping 1 0 1\r\n1\r\n";

#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
static bool zk_connected = false;
#endif

static zhandle_t *zh=NULL;
static clientid_t myid;
static int        last_rc=ZOK;

// this is to indicate if we get shutdown signal
// during ZK initialization
// ZK shutdown is done at the end of arcus_zk_init()
// to simplify synchronization
volatile sig_atomic_t arcus_zk_shutdown=0;
// This flag is now used to indicate graceful shutdown.  See hb_thread for
// more comment.

// placeholder for zookeeper and memcached settings
typedef struct {
    char    *ensemble_list;     // ZK ensemble IP:port list
    char    *svc;               // Service code name
    char    *mc_ipport;         // this memcached ip:port string
    char    *hostip;            // localhost server IP
    int     port;               // memcached port number
    int     hb_failstop;        // memcached heartbeat failstop
    int     hb_timeout;         // memcached heartbeat timeout
    int     zk_timeout;         // Zookeeper session timeout
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
#ifdef ENABLE_CLUSTER_AWARE
    struct cluster_config *ch;  // cluster configuration handle
#endif
    pthread_mutex_t lock;
    bool    init;               // is this structure initialized?
} arcus_zk_conf;

arcus_zk_conf arcus_conf = {
    .ensemble_list  = NULL,
    .svc            = NULL,
    .mc_ipport      = NULL,
    .hostip         = NULL,
    .hb_failstop    = HEART_BEAT_DFT_FAILSTOP,
    .hb_timeout     = HEART_BEAT_DFT_TIMEOUT,
    .zk_timeout     = DEFAULT_ZK_TO,
    .znode_name     = NULL,
    .znode_ver      = -1,
    .znode_created  = false,
    .verbose        = -1,
    .port           = -1,
    .maxbytes       = -1,
    .logger         = NULL,
    .cluster_path   = NULL,
#ifdef ENABLE_CLUSTER_AWARE
    .ch             = NULL,
#endif
    .lock           = PTHREAD_MUTEX_INITIALIZER,
    .init           = false
};

/* Arcus ZK stats */
arcus_zk_stats azk_stat;

// context struct to pass to app_ping
typedef struct {
    struct sockaddr_in  addr; // cache this for use in L7 ping routine
    struct timeval      to;   // mc_hb may block for up to this long (give or take)
} app_ping_t;

static app_ping_t mc_ping_context;

// static declaration
static void arcus_zk_watcher(zhandle_t *wzh, int type, int state,
                             const char *path, void *cxt);
static void arcus_cache_list_watcher(zhandle_t *zh, int type, int state,
                                     const char *path, void *ctx);
static void arcus_zk_sync_cb(int rc, const char *name, const void *data);
static void arcus_zk_log(zhandle_t *zh, const char *);
static void arcus_exit(zhandle_t *zh, int err);

int  mc_hb(zhandle_t* zh, void *context);     // memcached self-heartbeat

/* State machine thread.
 * It performs all blocking ZK operations including cluster_config.
 * We don't do blocking operations in the context of watcher callback
 * any more.
 */
/* sm request structure */
struct sm_request {
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
    /* A timer thread to fail-stop the server when it is disconnected from
     * the ZK ensemble.
     */
    pthread_t timer_tid;
#endif
} sm_info;

/* sm functions */
static int  sm_init(void);
static void sm_lock(void);
static void sm_unlock(void);
static void sm_wakeup(bool locked);

// async routine synchronization
static pthread_cond_t  azk_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t azk_mtx  = PTHREAD_MUTEX_INITIALIZER;
static int             azk_count;

// zookeeper close synchronization
static pthread_mutex_t zk_final_lock = PTHREAD_MUTEX_INITIALIZER;

// memcached heartheat thread */
static volatile bool hb_thread_running = false;
static bool hb_thread_sleep = false;
static pthread_mutex_t hb_thread_lock;
static pthread_cond_t  hb_thread_cond;
static void *hb_thread(void *arg);
static int start_hb_thread(app_ping_t *data);

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
        /* There are leftover characters.
         * Ignore them.
         */
    }
    return i; /* i <= vec_len */
}

// mutex for async operations
static void inc_count(int delta)
{
    pthread_mutex_lock(&azk_mtx);
    azk_count += delta;
    pthread_cond_broadcast(&azk_cond);
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

// Arcus zookeeper global watch callback routine
static void
arcus_zk_watcher(zhandle_t *wzh, int type, int state, const char *path, void *cxt)
{
    if (type != ZOO_SESSION_EVENT) {
        if (arcus_conf.verbose > 2)
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "arcus_zk_watch not session event (type=%d)\n", type);
        return;
    }

    if (state == ZOO_CONNECTED_STATE) {
        const clientid_t *id = zoo_client_id(wzh);

#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
        zk_connected = true;
#endif
        if (arcus_conf.verbose > 2) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "ZK ensemble connected\n");
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                            "session id: 0x%llx\n", (long long) myid.client_id);
        }
        if (myid.client_id == 0 || myid.client_id != id->client_id) {
            if (arcus_conf.verbose > 2)
                 arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                         "a old session id: 0x%llx\n", (long long) myid.client_id);
             myid = *id;
        }

        // finally connected to one of ZK ensemble. signal go.
        inc_count(-1);
    }
    else if (state == ZOO_AUTH_FAILED_STATE) {
        // authorization failure
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "Auth failure. shutting down\n");
        arcus_exit(wzh, EX_NOPERM);
    }
    else if (state == ZOO_EXPIRED_SESSION_STATE) {
        // very likely that memcached process exited and restarted within
        // session timeout
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "Expired state. shutting down\n");

        // send SMS here??
        arcus_exit(wzh, EX_TEMPFAIL);

        // We chose not to restart here.
        // All memcached start/restart should start from scratch (cold cache)
        // to avoid stale cache data access (rehashing already happened)
        //
        //arcus_zk_init(arcus_conf.ensemble_list, settings );
    }
    else if (state == ZOO_ASSOCIATING_STATE || state == ZOO_CONNECTING_STATE) {
        // we get these when connection to Ensemble is dropped and retrying
        // this happens emsemble failover as well
        // since this is not memcached operation issue, we will allow reconnecting
        // but it is possible that app_ping fails and zk server may have disconnected
        // and if that's the case, we let heartbeat timeout algorithm decide
        // what to do.
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

static int
arcus_read_ZK_children(const char *zpath, watcher_fn watcher,
                       struct String_vector *strv)
{
    int rc = zoo_wget_children(zh, zpath, watcher, NULL, strv);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to read children(znode list) of zpath=%s: "
            "error=%d(%s)\n", zpath, rc, zerror(rc));
        return (rc == ZNONODE ? 0 : -1);
    }
    return 1; /* The caller must free strv */
}

#ifdef ENABLE_CLUSTER_AWARE
/* update cluster config, that is ketama hash ring. */
static int
update_cluster_config(struct String_vector *strv)
{
    if (strv->count == 0) { /* cache_list can be empty. */
        sm_info.cluster_node_count = 0;
        return 0;
    }

    if (arcus_conf.verbose > 0) {
        for (int i = 0; i < strv->count; i++) {
            arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "server[%d] = %s\n",
                                   i, strv->data[i]);
        }
    }
    /* reconfigure arcus-memcached cluster */
    if (cluster_config_reconfigure(arcus_conf.ch, strv->data, strv->count) < 0) {
        return -1;
    }
    sm_info.cluster_node_count = strv->count;
    return 0;
}
#endif

// callback for sync command
static void
arcus_zk_sync_cb(int rc, const char *name, const void *data)
{
    if (arcus_conf.verbose > 2)
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "arcus_zk_sync cb\n");

    // signal cond var
    inc_count(-1);
    last_rc = rc;
}

// Log memcached join/leave acvitivity
// this is for debugging purpose
// We cannot log in case of memcached crash
//
static void
arcus_zk_log(zhandle_t *zh, const char *action)
{
    int     rc;
    char    zpath[200] = "";
    char    rcbuf[200] = "";
    char    sbuf[200] = "";

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
        if (rc == ZOK && arcus_conf.verbose > 2) {
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
        if (arcus_conf.verbose > 2)
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "znode \"%s\" created\n", rcbuf);
    }
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                "cannot log this acvitiy (%s error)\n", zerror(rc));
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

static void arcus_adjust_ping_timeout(app_ping_t *ping_data, int timeout)
{
    /* +50 msec so that the caller can detect the timeout */
    uint64_t usec = timeout * 1000 + 50000;
    ping_data->to.tv_sec = usec / 1000000;
    ping_data->to.tv_usec = usec % 1000000;
}

static void arcus_prepare_ping_context(app_ping_t *ping_data, int port)
{
    /* prepare app_ping context data:
     * sockaddr for memcached connection in app ping
     */
    memset(ping_data, 0, sizeof(app_ping_t));
    ping_data->addr.sin_family = AF_INET;
    ping_data->addr.sin_port   = htons(port);
    /* Use the admin ip (currently localhost) to avoid competing with
     * regular clients for connections.
     */
    ping_data->addr.sin_addr.s_addr = inet_addr(ADMIN_CLIENT_IP);

    arcus_adjust_ping_timeout(ping_data, arcus_conf.hb_timeout);
}

// this is L7 application ping callback
// only one app ping per ZK ping period if successful
// In this case, we make a TCP connection to self memcached port, and
// try to set a key.
//
// make sure that successful app heartbeat completes in 2/3 of recv timeout, otherwise
// it is very possible have expired state in the end.
// this retries 2 twice at every 1/3 of ZK recv timeout
int
mc_hb(zhandle_t *zh, void *context)
{
    app_ping_t      *data = (app_ping_t *) context;
    struct linger   linger;
    struct timeval  tv_timeo = data->to;
    int             flags;
    int             sock;
    char            buf[100];
    int             err=0;

    // make a tcp connection to this memcached itself,
    // and try "set arcus:zk-ping".
    // if any of these failed, we return success to allow ZK ping.

    sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    if (sock == -1) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                            "mc_hb: cannot create a socket (%s error)\n", strerror(errno));
        return 0; // Allow ZK ping by returning 0 even if socket() fails.
    }

    flags = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags)); // fast port recycle
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));

    // send()/recv() could be blocked forever when all memcached worker threads are deadlocked
    // while the acceptor thread is still alive.
    // Blocked in L7 health check, ZK ping fails, then ZK ensemble gives up the connection.
    // However, as the blocked ZK ping runs on ZK client's I/O thread
    // the client cannot receive the events from ZK ensemble. So memcached fails to fail-stop.
    // To prevent this situation we set timeouts to send()/recv().
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo)); // recv timeout
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo)); // send timeout

    linger.l_onoff  = 1;
    linger.l_linger = 0;        // flush buffers upon close() and send TCP RST
    setsockopt(sock, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));

    arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "mc_hb: connecting to memcached at %s:%d...\n",
                           inet_ntoa(data->addr.sin_addr), arcus_conf.port);

    err = connect(sock, (struct sockaddr *) &data->addr, sizeof(struct sockaddr));
    if (err) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "mc_hb: cannot connect to local memcached (%s error)\n", strerror(errno));
        close(sock);
        return 0; // Allow ZK ping by returning 0 even if connect() fails.
    }

    // try to set a key "arcus:zk-ping"
    // we need to be careful here. Since we can make a connection, basic
    // memcached event loop works and system resources are enough. if we can an
    // error during send()/recv(), it may not be a memcached failure at all.
    // We may get slab memory shortage for slab class 0 for above key.
    // For now, we simply return here without ping error or intentional delay
    err = send(sock, mc_hb_command, 28, 0);
    if (err > 0) {
        // expects "STORED\r\n"
        err = recv(sock, buf, 8, 0);
        if (err < 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "mc_hb: recv failed (%s error)\n", strerror(errno));
        }
    } else {
        if (err < 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "mc_hb: send failed (%s error)\n", strerror(errno));
        }
    }

    close(sock);
    return 0;
}

static void
hb_wakeup(void)
{
  pthread_mutex_lock(&hb_thread_lock);
  if (hb_thread_sleep == true)
      pthread_cond_signal(&hb_thread_cond);
  pthread_mutex_unlock(&hb_thread_lock);
}

static void *
hb_thread(void *arg)
{
    app_ping_t *ping_data = arg;
    int cur_hb_failstop = arcus_conf.hb_failstop;
    int cur_hb_timeout = arcus_conf.hb_timeout;
    int acc_hb_latency = 0;
    bool shutdown_by_me = false;
    struct timeval  tv;
    struct timespec ts;
    struct timeval start_time;
    struct timeval end_time;
    uint64_t elapsed_msec, start_msec, end_msec;

    hb_thread_running = true;

    /* Always print this to help the admin. */
    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Heartbeat thread is running.\n");

    /* We consider 3 types of shutdowns.
     *
     * 1. control-c
     * The user wants a graceful shutdown.  The main thread wakes up all
     * the worker threads and wait for them to terminate.  It also calls
     * the engine destroy function.  In this case, heartbeat should just stop.
     * arcus_zk_shutdown = 1 indicates this case.  So, we check that flag
     * in this function.
     *
     * 2. kill -KILL
     * Something is very wrong, and the user wants to kill the process right
     * away.  For example, control-c/graceful shutdown might hang due to bugs.
     * And, the user wants to forcefully kill the process.  There is nothing
     * we can do here.
     *
     * 3. Heartbeat failure
     * memcached is not working properly.  We attempt to close the ZK session
     * and then forcefully terminate.  This is NOT a graceful shutdown.  We
     * do not wait for worker threads to terminate.  We do not call the engine
     * destroy function.
     */

    while (hb_thread_running && !arcus_zk_shutdown) {
        /* If the last hearbeat timed out, do not wait and try another
         * right away.
         */
        if (acc_hb_latency == 0) {
            /* sleep HEART_BEAT_PERIOD seconds */
            gettimeofday(&tv, NULL);
            ts.tv_sec  = tv.tv_sec + HEART_BEAT_PERIOD;
            ts.tv_nsec = tv.tv_usec*1000;
            /* sleep with spurious wakeups checking */
            while (tv.tv_sec < ts.tv_sec && !arcus_zk_shutdown) {
                pthread_mutex_lock(&hb_thread_lock);
                hb_thread_sleep = true;
                pthread_cond_timedwait(&hb_thread_cond, &hb_thread_lock, &ts);
                hb_thread_sleep = false;
                pthread_mutex_unlock(&hb_thread_lock);
                gettimeofday(&tv, NULL);
            }
        }

        /* Graceful shutdown was requested while sleeping. */
        if (arcus_zk_shutdown)
            break;

        /* check if hb_failstop is changed */
        if (cur_hb_failstop != arcus_conf.hb_failstop) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat failstop has changed. old(%d) => new(%d)\n",
                cur_hb_failstop, arcus_conf.hb_failstop);
            cur_hb_failstop = arcus_conf.hb_failstop;
            acc_hb_latency = 0; /* reset accumulated hb latency */
        }
        /* check if hb_timeout is changed */
        if (cur_hb_timeout != arcus_conf.hb_timeout) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat timeout has changed. old(%d) => new(%d)\n",
                cur_hb_timeout, arcus_conf.hb_timeout);
            cur_hb_timeout = arcus_conf.hb_timeout;
            acc_hb_latency = 0; /* reset accumulated hb latency */
            /* adjust ping timeout */
            arcus_adjust_ping_timeout(ping_data, cur_hb_timeout);
        }

        /* Do one ping and measure how long it takes. */
        gettimeofday(&start_time, NULL);
        mc_hb((zhandle_t *)NULL, (void *)ping_data);
        gettimeofday(&end_time, NULL);

        /* Graceful shutdown was requested while doing heartbeat.
         * When worker threads die, there is no one to process heartbeat
         * request, and we would see a timeout.  Ignore the false alarm.
         */
        if (arcus_zk_shutdown)
            break;

        /* Paranoid.  Check if the clock had gone backwards. */
        start_msec = start_time.tv_sec * 1000 + start_time.tv_usec / 1000;
        end_msec = end_time.tv_sec * 1000 + end_time.tv_usec / 1000;
        if (end_msec >= start_msec) {
            elapsed_msec = end_msec - start_msec;
        } else {
            elapsed_msec = 0; /* Ignore this failure */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "hb_thread: Clock has gone backwards? start_msec=%"PRIu64
                " end_msec=%"PRIu64"\n", start_msec, end_msec);
        }

        azk_stat.hb_count += 1;
        azk_stat.hb_latency += elapsed_msec;

        if (elapsed_msec > cur_hb_timeout) {
            acc_hb_latency += elapsed_msec;
            /* Print a message for every failure to help debugging, postmortem
             * analysis, etc.
             */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat failure. hb_timeout=%d hb_latency=%"PRIu64
                " accumulated_hb_latency=%d\n", cur_hb_timeout,
                elapsed_msec, acc_hb_latency);

            if (acc_hb_latency > cur_hb_failstop) {
                /* Do not bother calling memcached.c:shutdown_server.
                 * Call arcus_zk_final directly.  shutdown_server just sets
                 * memcached_shutdown = arcus_zk_shutdown = 1 and calls
                 * arcus_zk_final.  These flags are used for graceful shutdown.
                 * We do not need them here.  Besides, arcus_zk_final kills
                 * the process right away, assuming the zookeeper library
                 * still functions.
                 */
                /*
                  SERVER_HANDLE_V1 *api = arcus_conf.get_server_api();
                  api->core->shutdown();
                */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "consecutive heartbeat failures. Shutting down...\n");
                shutdown_by_me = true;
                break;
            }
        } else {
            /* Reset the acc_hb_latency */
            acc_hb_latency = 0;
        }
    }
    hb_thread_running = false;
    if (shutdown_by_me) {
        arcus_zk_shutdown = 1;
        arcus_zk_final("Heartbeat failure");
        arcus_zk_destroy();
        exit(0);
    }
    return NULL;
}

static int
start_hb_thread(app_ping_t *data)
{
    pthread_t tid;
    int ret;
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_mutex_init(&hb_thread_lock, NULL);
    pthread_cond_init(&hb_thread_cond, NULL);

    if ((ret = pthread_create(&tid, &attr, hb_thread, (void *)data)) != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Cannot create hb thread: %s\n", strerror(ret));
    }
    return ret;
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
    char                rcbuf[200];

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
            if (arcus_conf.verbose > 2) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "Trying ensemble IP: %s\n", zip);
            }
        } else {
            // Must be hostname, not IP. Convert hostname to IP first
            zkhost = gethostbyname(zip);
            if (!zkhost) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "Invalid IP/hostname in arg: %s\n", zip);
                // get next token: separate IP:PORT tuple
                zip = strtok_r(NULL, sep1, &hlist);
                continue;
            }
            memcpy((char *)&saddr.sin_addr,
                   zkhost->h_addr_list[0], zkhost->h_length);
            inet_ntop(AF_INET, &saddr.sin_addr, rcbuf, sizeof(rcbuf));
            if (arcus_conf.verbose > 2) {
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
            if (arcus_conf.verbose > 2) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                        "connected to ensemble \"%s\" syslogd UDP port\n", zip);
            }
            break;
        }

        // if cannot connect immediately, try other ensemble
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Cannot connect to ensemble %s (%s error)\n", zip, strerror(errno));
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

    if (!arcus_conf.znode_name) {
        char *hostp=NULL;
        char  hostbuf[100];
        // Also get local hostname.
        // We want IP and hostname to better identify this cache
        hp = gethostbyaddr((char*)&myaddr.sin_addr.s_addr,
                            sizeof(myaddr.sin_addr.s_addr), AF_INET);
        if (hp) {
            hostp = strdup(hp->h_name);
        } else {
            // if gethostbyaddr() doesn't work, try gethostname
            if (gethostname((char *)&hostbuf, sizeof(hostbuf))) {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "cannot get hostname: %s\n", strerror(errno));
                return EX_NOHOST;
            }
            hostp = hostbuf;
        }
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                               "local hostname: %s\n", hostp);

        // we need to keep ip:port-hostname tuple for later user (restart)
        snprintf(rcbuf, sizeof(rcbuf), "%s:%d", arcus_conf.hostip, arcus_conf.port);
        arcus_conf.mc_ipport = strdup(rcbuf);
        snprintf(rcbuf, sizeof(rcbuf), "%s-%s", arcus_conf.mc_ipport, hostp);
        arcus_conf.znode_name = strdup(rcbuf);
    }
    return 0; // EX_OK
}

static int arcus_parse_server_mapping(const char *root, char *znode)
{
    char *start[4], *end[4]; /* enough pointers for future extension */
    int num_substrs = 1; /* sevice code */
    int i, count;

    count = breakup_string(znode, start, end, num_substrs);
    if (count < num_substrs) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "The server mapping znode seems to be invalid. znode=%s\n", znode);
        return -1;
    }
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

static int arcus_check_server_mapping(zhandle_t *zh, const char *root)
{
    struct String_vector strv = {0, NULL};
    char zpath[200];
    int  rc;

    // sync map path
    snprintf(zpath, sizeof(zpath), "%s/%s", root, zk_map_dir);
    inc_count(1);
    rc = zoo_async(zh, zpath, arcus_zk_sync_cb, NULL);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "zoo_async() failed: %s\n", zerror(rc));
        return -1;
    }
    // wait until above sync callback is called
    wait_count(0);
    rc = last_rc;
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to synchronize zpath. zpath=%s error=%s\n", zpath, zerror(rc));
        return -1;
    }

    // First check: get children of "/cache_server_mapping/ip:port"
    snprintf(zpath, sizeof(zpath), "%s/%s/%s",
             root, zk_map_dir, arcus_conf.mc_ipport);
    rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
    if (rc == ZNONODE) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Cannot find the server mapping. zpath=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));

        // Second check: get children of "/cache_server_mapping/ip"
        snprintf(zpath, sizeof(zpath), "%s/%s/%s",
                 root, zk_map_dir, arcus_conf.hostip);
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Recheck the server mapping without the port number."
                " zpath=%s\n", zpath);
        rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
    }
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to read the server mapping. zpath=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
        return -1;
    }

    // zoo_get_children returns ZOK even if no child exist
    if (strv.count == 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "The server mapping exists but has no service code."
                " zpath=%s\n", zpath);
        return -1;
    }
    /* We assume only one server mapping znode.
     * We do not care that we have more than one (meaning one server participating in
     * more than one service in the cluster: not likely though)
     */
    if (strv.count > 1) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "The server mapping exists but has %d( > 1) mapping znodes."
                " zpath=%s\n", strv.count, zpath);
        return -1;
    }
    if (arcus_parse_server_mapping(root, strv.data[0]) < 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to parse server mapping znode.\n");
        return -1;
    }

    deallocate_String_vector(&strv);
    return 0;
}

static int arcus_create_ephemeral_znode(zhandle_t *zh, const char *root)
{
    int         rc;
    char        zpath[200];
    char        value[200];
    struct Stat zstat;

    snprintf(zpath, sizeof(zpath), "%s/%s",
             arcus_conf.cluster_path, arcus_conf.znode_name);
    snprintf(value, sizeof(value), "%ldMB", (long)arcus_conf.maxbytes/1024/1024);
    rc = zoo_create(zh, zpath, value, strlen(value), &ZOO_OPEN_ACL_UNSAFE,
                    ZOO_EPHEMERAL, NULL, 0);
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "cannot create znode %s (%s error)\n", zpath, zerror(rc));
        if (rc == ZNODEEXISTS) {
            arcus_conf.logger->log( EXTENSION_LOG_DETAIL, NULL,
                    "old session still exist. Wait a bit\n");
        }
        return -1;
    }

    rc = zoo_exists(zh, zpath, 0, &zstat);
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "cannot find %s just created (%s error)\n", zpath, zerror(rc));
        return -1;
    }

    // store this version number, just in case we restart
    arcus_conf.znode_ver = zstat.version;
    arcus_conf.znode_created = true;
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
            "Joined Arcus cloud at %s(ver=%d)\n", zpath, arcus_conf.znode_ver);
    return 0;
}

void arcus_zk_init(char *ensemble_list, int zk_to,
                   EXTENSION_LOGGER_DESCRIPTOR *logger,
                   int verbose, size_t maxbytes, int port,
                   ENGINE_HANDLE_V1 *engine)
{
    int             rc;
    char            zpath[200] = "";
    struct timeval  start_time, end_time;
    long            difftime_us;

    assert(logger);
    assert(engine);
    if (!ensemble_list) { // Arcus Zookeeper Ensemble IP list
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    " -z{ensemble_list} must not be empty\n");
        arcus_exit(zh, EX_USAGE);
    }

    if (arcus_conf.verbose > 2) {
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                               "arcus_zk_init(%s)\n", ensemble_list);
    }

    // save these for later use (restart)
    if (!arcus_conf.init) {
        arcus_conf.port          = port;    // memcached listen port
        arcus_conf.logger        = logger;
        arcus_conf.engine.v1     = engine;
        arcus_conf.verbose       = verbose;
        arcus_conf.maxbytes      = maxbytes;
        arcus_conf.ensemble_list = strdup(ensemble_list);
        assert(arcus_conf.ensemble_list);
        // Use the user specified timeout only if it falls within
        // [MIN, MAX).  Otherwise, silently ignore it and use
        // the default value.
        if (zk_to >= MIN_ZK_TO && zk_to < MAX_ZK_TO)
            arcus_conf.zk_timeout = zk_to*1000; // msec conversion

        memset( &myid, 0, sizeof(myid) );
    }

    /* initialize Arcus ZK stats */
    memset(&azk_stat, 0, sizeof(azk_stat));

    /* make znode name while getting local ip and hostname */
    rc = arcus_build_znode_name(ensemble_list);
    if (rc != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Failed to build znode name.\n");
        arcus_exit(zh, rc);
    }

    gettimeofday(&start_time, 0);

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

    if (arcus_conf.verbose > 2)
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "zookeeper_init()\n");

    inc_count(1);
    zh = zookeeper_init(arcus_conf.ensemble_list, arcus_zk_watcher,
                        arcus_conf.zk_timeout, &myid, 0, 0);
    if (!zh) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "zookeeper_init() failed (%s error)\n", strerror(errno));
        arcus_exit(zh, EX_PROTOCOL);
    }
    // wait until above init callback is called
    // We need to wait until ZOO_CONNECTED_STATE
    if (wait_count(arcus_conf.zk_timeout) != 0) {
        /* zoo_state(zh) != ZOO_CONNECTED_STATE */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                 "cannot to be ZOO_CONNECTED_STATE\n");
        arcus_exit(zh, EX_PROTOCOL);
    }

    /* check zk root directory and get the serice code */
    if (zk_root == NULL) {
        zk_root = "/arcus"; /* set zk root directory */
        if (arcus_check_server_mapping(zh, zk_root) != 0) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                     "Failed to check server mapping for this cache node. "
                     "(zk_root=%s)\n", zk_root);
            arcus_exit(zh, EX_PROTOCOL);
        }
    }

    snprintf(zpath, sizeof(zpath), "%s/%s/%s", zk_root, zk_cache_dir, arcus_conf.svc);
    arcus_conf.cluster_path = strdup(zpath);
    assert(arcus_conf.cluster_path);

    /* Initialize the state machine. */
    if (sm_init() != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to initialize the state machine. Terminating.\n");
        arcus_exit(zh, EX_CONFIG);
    }

    arcus_conf.init = true;

    /* create "/cache_list/{svc}/ip:port-hostname" ephemeral znode */
    if (arcus_create_ephemeral_znode(zh, zk_root) != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "arcus_create_ephemeral_znode() failed.\n");
        arcus_exit(zh, EX_PROTOCOL);
    }

    // log this join activity in /arcus/cache_server_log
    arcus_zk_log(zh, "join");

    gettimeofday(&end_time, 0);
    difftime_us = (end_time.tv_sec*1000000 + end_time.tv_usec) -
                  (start_time.tv_sec*1000000 + start_time.tv_usec);
    // We have finished registering this memcached instance to Arcus cluster
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
            "Memcached joined Arcus cache cloud for \"%s\" service (took %ld microsec)\n",
            arcus_conf.svc, difftime_us);

    // "recv" timeout is actually the session timeout
    // ZK client ping period is recv_timeout / 3.
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
            "ZooKeeper session timeout: %d sec\n", zoo_recv_timeout(zh)/1000);

#ifdef ENABLE_CLUSTER_AWARE
    const char *nodename = arcus_conf.mc_ipport;
    arcus_conf.ch = cluster_config_init(nodename,
                                        arcus_conf.logger, arcus_conf.verbose);
    assert(arcus_conf.ch);

    struct String_vector strv = { 0, NULL };
    /* 2nd argument, NULL means no watcher */
    if (arcus_read_ZK_children(arcus_conf.cluster_path, NULL, &strv) <= 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to read cache list from ZK. Terminating...\n");
        arcus_exit(zh, EX_CONFIG);
    }
    if (update_cluster_config(&strv) != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to update cluster config. Terminating...\n");
        arcus_exit(zh, EX_CONFIG);
    }
    deallocate_String_vector(&strv);

    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Memcached is watching Arcus cache cloud for \"%s\"\n", arcus_conf.cluster_path);
#endif

    /* Wake up the state machine thread.
     * Tell it to refresh the hash ring (cluster_config).
     */
    sm_lock();
    /* Don't care if we just read the list above.  Do it again. */
    sm_info.request.update_cache_list = true;
    sm_wakeup(true);
    sm_unlock();

    // start heartbeat thread
    arcus_prepare_ping_context(&mc_ping_context, arcus_conf.port);
    if (start_hb_thread(&mc_ping_context) != 0) {
        /* We need normal shutdown at this time */
        arcus_zk_shutdown = 1;
    }

    // Either got SIG* or memcached shutdown process finished
    if (arcus_zk_shutdown) {
        arcus_zk_final("Interrupted");
        arcus_zk_destroy();
        arcus_exit(zh, EX_OSERR);
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

    pthread_mutex_lock(&zk_final_lock);
    if (zh) {
        /* hb_thread is probably sleeping.  And, if it is in the middle of
         * doing a ping, it would block for a long time because worker threads
         * are likely all dead at this point.
         */
        /* wake up the heartbeat thread if it's sleeping */
        hb_wakeup();
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
            if (hb_thread_running)
                continue;
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
        zookeeper_close(zh);
        zh = NULL;
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "zk connection closed\n");
    }
    pthread_mutex_unlock(&zk_final_lock);
}

void arcus_zk_destroy(void)
{
    assert(arcus_zk_shutdown == 1);
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "arcus_zk_destroy\n");

    pthread_mutex_lock(&zk_final_lock);
#ifdef ENABLE_CLUSTER_AWARE
    if (arcus_conf.ch != NULL) {
        cluster_config_final(arcus_conf.ch);
        arcus_conf.ch = NULL;
    }
#endif
    if (arcus_conf.ensemble_list) {
        free(arcus_conf.ensemble_list);
        arcus_conf.ensemble_list = NULL;
    }
    pthread_mutex_unlock(&zk_final_lock);
}

int arcus_zk_set_ensemble(char *ensemble_list)
{
    int rc;
    if (zh) {
        char *copy = strdup(ensemble_list);
        if (!copy) {
            /* Should not happen unless the system is really short of memory. */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to copy the ensemble list. list=%s\n", ensemble_list);
            return -1;
        }
        rc = zookeeper_change_ensemble(zh, ensemble_list);
        if (rc == ZOK) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Successfully changed the ZooKeeper ensemble list. list=%s\n", ensemble_list);
            /* arcus_conf.ensemble_list is not used after init.
             * Nothing uses it.  So it is okay to just replace the pointer.
             */
            free(arcus_conf.ensemble_list);
            arcus_conf.ensemble_list = copy;
            return 0;
        }
        free(copy);
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to change the ZooKeeper ensemble list. error=%d(%s)\n", rc, zerror(rc));
    } else {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to change the ZooKeeper ensemble list. The handle is invalid.\n");
    }
    return -1;
}

int arcus_zk_get_ensemble(char *buf, int size)
{
    int rc;
    if (zh) {
        rc = zookeeper_get_ensemble_string(zh, buf, size);
        if (rc == ZOK)
            return 0;
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the ZooKeeper ensemble list. error=%d(%s)\n", rc, zerror(rc));
    } else {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the ZooKeeper ensemble list. The handle is invalid.\n");
    }
    return -1;
}

int arcus_zk_set_hbtimeout(int hbtimeout)
{
    int ret=0;

    if (hbtimeout < HEART_BEAT_MIN_TIMEOUT ||
        hbtimeout > HEART_BEAT_MAX_TIMEOUT)
        return -1;

    pthread_mutex_lock(&arcus_conf.lock);
    do {
        /* Check: heartbeat timeout <= heartbeat failstop */
        if (hbtimeout > arcus_conf.hb_failstop) {
            ret = -1; break;
        }
        if (hbtimeout != arcus_conf.hb_timeout) {
            arcus_conf.hb_timeout = hbtimeout;
        }
    } while(0);
    pthread_mutex_unlock(&arcus_conf.lock);

    return ret;
}

int arcus_zk_get_hbtimeout(void)
{
    assert(arcus_conf.hb_timeout >= HEART_BEAT_MIN_TIMEOUT &&
           arcus_conf.hb_timeout <= HEART_BEAT_MAX_TIMEOUT);
    return arcus_conf.hb_timeout;
}

int arcus_zk_set_hbfailstop(int hbfailstop)
{
    int ret=0;

    if (hbfailstop < HEART_BEAT_MIN_FAILSTOP ||
        hbfailstop > HEART_BEAT_MAX_FAILSTOP)
        return -1;

    pthread_mutex_lock(&arcus_conf.lock);
    do {
        /* Check: heartbeat failstop >= heartbeat timeout */
        if (hbfailstop < arcus_conf.hb_timeout) {
            ret = -1; break;
        }
        if (hbfailstop != arcus_conf.hb_failstop) {
            arcus_conf.hb_failstop = hbfailstop;
        }
    } while(0);
    pthread_mutex_unlock(&arcus_conf.lock);

    return ret;
}

int arcus_zk_get_hbfailstop(void)
{
    assert(arcus_conf.hb_failstop >= HEART_BEAT_MIN_FAILSTOP &&
           arcus_conf.hb_failstop <= HEART_BEAT_MAX_FAILSTOP );
    return arcus_conf.hb_failstop;
}

void arcus_zk_get_stats(arcus_zk_stats *stats)
{
    stats->zk_timeout = arcus_conf.zk_timeout;
    stats->hb_timeout = arcus_conf.hb_timeout;
    stats->hb_failstop = arcus_conf.hb_failstop;
    stats->hb_count = azk_stat.hb_count;
    stats->hb_latency = azk_stat.hb_latency;
}

#ifdef ENABLE_CLUSTER_AWARE
int arcus_key_is_mine(const char *key, size_t nkey, bool *mine)
{
    int      ret;
    uint32_t key_id, self_id;

    ret = cluster_config_key_is_mine(arcus_conf.ch, key, nkey, mine,
                                     &key_id, &self_id);
    if (ret == 0) {
        if (arcus_conf.verbose > 2) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "key=%s, self_id=%d, key_id=%d, mine=%s\n",
                    key, self_id, key_id, ((*mine) ? "true" : "false"));
        }
    }
    return ret;
}

int arcus_ketama_hslice(const char *key, size_t nkey, uint32_t *hvalue)
{
    return cluster_config_ketama_hslice(arcus_conf.ch, key, nkey, hvalue);
}
#endif

static void *
sm_state_thread(void *arg)
{
    struct sm_request smreq;
    struct timeval  tv;
    struct timespec ts;
    bool sm_retry = false;
    bool shutdown_by_me = false;

    sm_info.state_running = true;

    while (!arcus_zk_shutdown)
    {
        sm_lock();
        while (!sm_info.notification && !arcus_zk_shutdown) {
            /* Poll if requested.
             * Otherwise, wait till the ZK watcher wakes us up.
             */
            if (sm_retry) {
                gettimeofday(&tv, NULL);
                tv.tv_usec += 100000; /* Wake up in 100 ms.  Magic number.  FIXME */
                if (tv.tv_usec >= 1000000) {
                    tv.tv_sec += 1;
                    tv.tv_usec -= 1000000;
                }
                ts.tv_sec = tv.tv_sec;
                ts.tv_nsec = tv.tv_usec * 1000;
                pthread_cond_timedwait(&sm_info.cond, &sm_info.lock, &ts);
                sm_retry = false;
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

        /* Read the latest hash ring */
        if (smreq.update_cache_list) {
            struct String_vector strv_cache_list = {0, NULL};
            int zresult = arcus_read_ZK_children(arcus_conf.cluster_path,
                                                 arcus_cache_list_watcher,
                                                 &strv_cache_list);
            if (zresult < 0) {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to read cache list from ZK.  Retry...\n");
                sm_retry = true;
                sm_lock();
                sm_info.request.update_cache_list = true;
                sm_unlock();
                /* ZK operations can fail.  For example, when we are
                 * disconnected from ZK, operations fail with connectionloss.
                 * Or, we would see operation timeout.
                 */
            } else if (zresult == 0) { /* NO znode */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Cannot read cache list from ZK.  No znode.\n");
                /* Do not retry */
            } else {
                /* Remember the latest cache list */
                deallocate_String_vector(&sm_info.sv_cache_list);
                sm_info.sv_cache_list = strv_cache_list;

#ifdef ENABLE_CLUSTER_AWARE
                /* update cluster config */
                if (update_cluster_config(&sm_info.sv_cache_list) != 0) {
                    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                            "Failed to update cluster config. Will check later.\n");
                }
#endif
            }
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
static void *
sm_timer_thread(void *arg)
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
            if ((now_msec - start_msec) >= DEFAULT_ZK_TO/3 - 1000) {
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

static int
sm_init(void)
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

static void
sm_lock(void)
{
    pthread_mutex_lock(&sm_info.lock);
}

static void
sm_unlock(void)
{
    pthread_mutex_unlock(&sm_info.lock);
}

static void
sm_wakeup(bool locked)
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

#endif  // ENABLE_ZK_INTEGRATION
