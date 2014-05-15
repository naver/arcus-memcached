/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
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
#include "memcached/extension_loggers.h"
#include "memcached/engine.h"
#ifdef ENABLE_CLUSTER_AWARE
#include "cluster_config.h"
#endif
#include "arcus_zk.h"

/* Kill server if disconnected from zookeeper for too long */
#define ENABLE_SUICIDE_UPON_DISCONNECT 0

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

/* Use a heart heat thread instead of ZK app_ping */
#define ENABLE_HEART_BEAT_THREAD 1

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
#define MAX_HB_RETRY    (2*3)   // we want to retry 2 heartbeats every 1/3 of ZK session timeout

static const char *zk_map_path   = "cache_server_mapping";
static const char *zk_log_path   = "cache_server_log";
static const char *zk_cache_path = "cache_list";
static const char *zk_group_path = "cache_server_group";
static const char *mc_hb_cmd     = "set arcus-zk5:ping 1 0 1\r\n1\r\n";

/* For 1.6: /arcus
 * For 1.7: /arcus_1_7
 */
static const char *zk_root = NULL;
static bool is_1_7;
static bool zk_connected = false;

static zhandle_t    *zh=NULL;
static clientid_t   myid;
static int          last_rc=ZOK;

// this is to indicate if we get shutdown signal
// during ZK initialization
// ZK shutdown is done at the end of arcus_zk_init()
// to simplify synchronization
volatile sig_atomic_t  arcus_zk_shutdown=false;
// This flag is now used to indicate graceful shutdown.  See hb_thread for
// more comment.

// placeholder for zookeeper and memcached settings
typedef struct {
    char    *ensemble_list;     // ZK ensemble IP:port list
    char    *svc;               // Service code name
    char    *hostip;            // localhost server IP
    int     port;               // memcached port number
    int     zk_timeout;         // Zookeeper session timeout
    char    *zk_path;           // Ephemeral ZK path for this mc identification
    int     zk_path_ver;        // Zookeeper path version
    int     verbose;            // verbose output
    size_t  maxbytes;           // mc -M option
    EXTENSION_LOGGER_DESCRIPTOR *logger; // mc logger
    char   *cluster_path;       // cache_list/svc
#ifdef ENABLE_CLUSTER_AWARE
    char   *self_hostport;      // host:port string for this memcached
    struct cluster_config *ch;  // cluster configuration handle
#endif
    bool    init;               // is this structure initialized?
    /* 1.7 */
    char *mc_ipport_str;        // This server's memcached ip:port
    char *groupname;
    char *listen_addr_str;      // Replication ip:port
    struct sockaddr_in listen_addr;
    union {
        ENGINE_HANDLE *v0;
        ENGINE_HANDLE_V1 *v1;
    } engine;
} arcus_zk_conf;

arcus_zk_conf arcus_conf = {
    .ensemble_list  = NULL,
    .svc            = NULL,
    .hostip         = NULL,
    .zk_timeout     = DEFAULT_ZK_TO,
    .zk_path        = NULL,
    .zk_path_ver    = -1,
    .verbose        = -1,
    .port           = -1,
    .maxbytes       = -1,
    .logger         = NULL,
#ifdef ENABLE_CLUSTER_AWARE
    .cluster_path   = NULL,
    .self_hostport  = NULL,
    .ch             = NULL,
#endif
    .init           = false
};

// context struct to pass to app_ping
typedef struct {
    struct sockaddr_in  addr; // cache this for use in L7 ping routine
    struct timeval      to;   // mc_hb may block for up to this long (give or take)
} app_ping_t;

// static declaration
static void arcus_zk_watcher(zhandle_t *wzh, int type, int state,
                             const char *path, void *cxt);
static void arcus_zk_sync_cb(int rc, const char *name, const void *data);
static void arcus_zk_log(zhandle_t *zh, const char *);
static void arcus_exit(zhandle_t *zh, int err);
static void inc_count(int delta);
static int  wait_count(int timeout);

#ifdef ENABLE_CLUSTER_AWARE
static void arcus_cache_list_watcher(zhandle_t *zh, int type, int state,
    const char *path, void *ctx);
#endif

// declaration
int  mc_hb(zhandle_t* zh, void *context);     // memcached self-heartbeat
int  arcus_zk_set_ensemble(char *ensemble_list);
int  arcus_zk_get_ensemble_str(char *buf, int size);

/* Util functions used by 1.7 */
static int fill_sockaddr(const char *host, struct sockaddr_in *addr);
static int parse_hostport(const char *addr_str, struct sockaddr_in *addr, char **host_out);
static int breakup_string(char *str, char *start[], char *end[], int vec_len);

/* State machine thread.  It monitors ZK nodes and computes whether this
 * server is the master or the slave.  It also performs all blocking
 * ZK operations, including cluster_config.  We don't do blocking operations
 * in the context of watcher callback any more.
 */
struct sm {
    bool update_cache_list;
    bool watch_cache_list;
    bool update_lock;

    /* The full lock znode path including the sequence number.
     * See sm_create_lock_znode.
     */
    char *lock_znode_path;
    char *lock_dir_path;
    char *slave_name; /* group^S^ip:port-host */
    char *master_name; /* group^M^ip:port-host */

    int state; /* Tracks whether we are the master or the slave */

    /* Cache of the latest version we pulled from ZK */
    struct String_vector cache_list; /* from /cache_list */
    struct String_vector lock_list; /* from /cache_server_group/.../lock */

    /* Used to wake up the thread */
    pthread_mutex_t lock;
    pthread_cond_t cond;
    bool notification;

    bool running;
    pthread_t tid;

    /* A timer thread to fail-stop the server when it is disconnected from
     * the ZK ensemble.
     */
    pthread_t timer_tid;

    /* Remember the master's and the slave's listen address.
     * See sm_get_master_lock_status.
     */
    char slave_laddr[128]; /* FIXME.  Magic number */
    char master_laddr[128]; /* FIXME.  Magic number */
} sm_info;

#define SM_STATE_UNKNOWN        0
#define SM_STATE_BECOME_MASTER  1
#define SM_STATE_MASTER         2
#define SM_STATE_BECOME_SLAVE   3
#define SM_STATE_SLAVE          4
#define SM_STATE_BECOME_UNKNOWN 5
#define SM_STATE_MAX            5
static const char *sm_state_str[] = {
    "UNKNOWN",
    "BECOME_MASTER",
    "MASTER",
    "BECOME_SLAVE",
    "SLAVE",
    "BECOME_UNKNOWN",
};

#define SM_MASTER_LOCK_STATUS_INVALID     0  // Do not use this status
#define SM_MASTER_LOCK_STATUS_OWNER       1  // We own the lock
#define SM_MASTER_LOCK_STATUS_NOT_OWNER   2  // Someone else owns the lock
#define SM_MASTER_LOCK_STATUS_UNKNOWN     3  // Cannot tell who owns the lock
#define SM_MASTER_LOCK_STATUS_MAX         3
static const char *sm_master_lock_status_str[] = {
    "INVALID",
    "OWNER",
    "NOT_OWNER",
    "UNKNOWN",
};

/* Master/slave state machine
 *
 *  +--+          +---+            +--+
 *  |  |          |   |            |  |
 *  |  V          |   V            |  V
 * Unknown ---> Become_master ---> Master
 *   |  ^          |                |
 *   |  |          |                |
 *   |  +--Become_unknwon-----------+
 *   |             |                |
 *   +--------> Become_slave ---> Slave
 *               ^  |             ^  |
 *               |  |             |  |
 *               +--+             +--+
 *
 * Threads:
 * The ZK library has one thread that calls back our watchers.
 *
 * When we lose connection to ZK (CONNECTING), we run our own timer to
 * detect the loss of the master lock.  We do this because session expiration
 * only happens when we actually connect to ZK.  If we cannot connect to ZK
 * due to partition, we never see session expiration events.
 *
 * We should not block in our watcher functions.  Otherwise, we may see
 * session events too late, leading to two servers acting as masters.
 *
 * - ZK thread (created by the ZK library)
 * It only receives events and passes them to the state machine thread.
 * It does not block, at least in our code.
 *
 * - Timer thread
 * It waits for "connection lost" event from the ZK thread.  Then it runs
 * a timer and fail-stops this server.  "connected" event from the ZK thread
 * cancels the timer.  This thread does nothing else.
 *
 * - State machine thread
 * It waits for input events from the ZK thread and memcached threads, and
 * runs the state machine.  It may block.
 */

static int sm_init(void);
static void sm_lock(void);
static void sm_unlock(void);
static void sm_wakeup(bool locked);
static int sm_check_dup_in_lock(void);
static int sm_check_dup_in_cache_list(void);
static int sm_create_lock_znode(void);

/* These functions tell the engine to become master/slave. */
static int
arcus_memcached_slave_mode(const char *saddr, const char *maddr)
{
    ENGINE_ERROR_CODE ret;
    ret = arcus_conf.engine.v1->rp_slave(arcus_conf.engine.v0, saddr, maddr);
    return ret == ENGINE_SUCCESS ? 0 : -1;
}

static int
arcus_memcached_master_mode(const char *addr)
{
    ENGINE_ERROR_CODE ret;
    ret = arcus_conf.engine.v1->rp_master(arcus_conf.engine.v0, addr);
    return ret == ENGINE_SUCCESS ? 0 : -1;
}

static int
arcus_memcached_slave_address(const char *addr)
{
    ENGINE_ERROR_CODE ret;
    ret = arcus_conf.engine.v1->rp_slave_addr(arcus_conf.engine.v0, addr);
    return ret == ENGINE_SUCCESS ? 0 : -1;
}

// async routine synchronization
static pthread_cond_t  azk_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t azk_mtx  = PTHREAD_MUTEX_INITIALIZER;
static int             azk_count;

// zookeeper close synchronization
static pthread_mutex_t zk_close_lock = PTHREAD_MUTEX_INITIALIZER;
static bool            zk_closing = false;
static bool            zk_watcher_running = false;

#ifdef ENABLE_HEART_BEAT_THREAD
/* One heartbeat every 3 seconds. */
#define HEART_BEAT_PERIOD       3 /* sec */
/* If hearbeat takes more than 10 seconds, consider it failed.  */
#define HEART_BEAT_TIMEOUT      10000 /* msec */
/* If heartbeat takes more than 1 second, leave a warning message. */
#define HEART_BEAT_WARN_TIMEOUT 1000 /* msec */

/* If 3 consecutive heartbeats fail, consider memcached dead and commit
 * suicide.  TIMEOUT x TRY_COUNT = 30 seconds.  When we have more data
 * later on, we should adjust both TIMEOUT and TRY_COUNT.
 */
#define HEART_BEAT_TRY_COUNT 3

static volatile bool hb_thread_running = false;
static void *hb_thread(void *arg);
static void start_hb_thread(app_ping_t *data);
#endif // ENABLE_HEART_BEAT_THREAD

// mutex for async operations
void inc_count(int delta) {
    pthread_mutex_lock(&azk_mtx);
    azk_count += delta;
    pthread_cond_broadcast(&azk_cond);
    pthread_mutex_unlock(&azk_mtx);
}

int wait_count(int timeout) {
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

        zk_connected = true; /* 1.7 */

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
        inc_count (-1);

        /* Tell the thread to refresh the lock status, and the hash ring, just to be safe. */
        if (arcus_conf.init) {
            sm_lock();
            sm_info.update_lock = true;
            sm_info.update_cache_list = true;
            /* This flags is strictly for sm_thread and cache_list_watcher. */
            /* sm_info.watch_cache_list = true */
            sm_wakeup(true);
            sm_unlock();
        }
    }
    else if (state == ZOO_AUTH_FAILED_STATE) {

        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "Auth failure. shutting down\n");
        arcus_exit (wzh, EX_NOPERM);

    }
    else if (state == ZOO_EXPIRED_SESSION_STATE) {

        // very likely that memcached process exited and restarted within
        // session timeout
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "Expired state. shutting down\n");

        // send SMS here??
        //
        arcus_exit (wzh, EX_TEMPFAIL);

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

        /* The server is disconnected from ZK.  But we do not know how much
         * time has elapsed from the last successful ZK ping.  A connection
         * timeout (session timeout * 2/3) might have occurred.  Or, the
         * TCP connection might have reset/closed (immediate).
         * See sm_timer_thread.  For now, it is being conservative and dies
         * in session timeout * 1/3.  Perhaps we should do our own ping, not
         * rely on ZK session notifications.  FIXME
         */
        zk_connected = false; /* 1.7 */
    }
}

#ifdef ENABLE_CLUSTER_AWARE
static void
update_cluster_config(struct String_vector *strv)
{
    int i, j, count;
    char **array;

    /* cache_list is empty.  In 1.7, this case is possible either during init
     * or shutdown.
     */
    if (strv->count == 0)
        return;

    if (arcus_conf.verbose > 0) {
        for (i = 0; i < strv->count; i++) {
            arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                "server[%d] = %s\n", i, strv->data[i]);
        }
    }

    if (is_1_7) {
        /* In 1.7, group names appear on the hash ring.
         * Extract them and remove duplicates before passing them
         * to cluster_config.  Actually pass "group-bogushost" to
         * cluster_config.  cluster_config removes "-hostname"...
         */
        char *start[1], *end[1], *s;

        array = malloc(sizeof(char*) * strv->count);
        memset(array, 0, sizeof(char*) * strv->count);
        /* Let it crash if array == NULL.  We cannot continue anyway. */

        count = 0;
        for (i = 0; i < strv->count; i++) {
            if (0 != breakup_string(strv->data[i], start, end, 1)) {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Error while trying to re-compute the cluster hash ring."
                    " An invalid server name in the cache list. znode=%s\n",
                    strv->data[i]);
                return;
            }

            /* group-bogushost */
            s = malloc(end[0] - start[0] + sizeof("-bogushost"));
            memcpy(s, start[0], end[0] - start[0]);
            memcpy(s + (end[0] - start[0]), "-bogushost", sizeof("-bogushost"));
            /* Let it crash if s == NULL */

            /* Check duplicate */
            for (j = 0; j < count; j++) {
                if (0 == strcmp(s, array[j])) {
                    free(s);
                    s = NULL;
                    break;
                }
            }
            if (s)
                array[count++] = s;
        }

        if (arcus_conf.verbose > 0) {
            for (i = 0; i < count; i++) {
                arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                    "1.7 group[%d] = %s\n", i, array[i]);
            }
        }
    }
    else {
        array = strv->data;
        count = strv->count;
    }

    cluster_config_reconfigure(arcus_conf.ch, array, count);

    /* Clean up 1.7 strings */
    if (is_1_7 && array) {
        for (i = 0; i < count; i++)
            free(array[i]);
        free(array);
    }
}
#endif

static void
arcus_cache_list_watcher(zhandle_t *zh, int type, int state, const char *path,
    void *ctx)
{
    if (type == ZOO_CHILD_EVENT) {
        /* arcus_cluster_watch_servers(zh, path, arcus_cluster_watcher); */

        /* The ZK library has two threads of its own.  The completion thread
         * calls watcher functions.
         *
         * Do not do operations that may block or fail in the watcher context.
         */
    } else if (type == ZOO_SESSION_EVENT) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "ZOO_SESSION_EVENT from ZK: state=%d\n", state);
    } else {
        // an unexpected event has been occurred
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "An unexpected event from ZK: type=%d, state=%d\n",
                               type, state);
    }

    /* Just wake up the thread and update the ring.
     * This may be a false positive (session event or others).
     * But it is harmless.
     */
    sm_lock();
    sm_info.update_cache_list = true;
    sm_info.watch_cache_list = true; /* Register a watcher again */
    sm_wakeup(true);
    sm_unlock();
}

static int
read_cache_list(struct String_vector *strv, bool watch)
{
    int rc;

    rc = zoo_wget_children(zh, arcus_conf.cluster_path,
        watch ? arcus_cache_list_watcher : NULL, NULL, strv);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the list of znodes in the cache_list directory."
            " Will try again. path=%s error=%d(%s)\n",
            arcus_conf.cluster_path, rc, zerror(rc));
    }
    /* The caller must free strv */
    return (rc == ZOK ? 0 : -1);
}

// callback for sync command
static void
arcus_zk_sync_cb (int rc, const char *name, const void *data)
{
    if (arcus_conf.verbose > 2)
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "arcus_zk_sync cb\n");

    // signal cond var
    inc_count (-1);
    last_rc = rc;

}

// Log memcached join/leave acvitivity
// this is for debugging purpose
// We cannot log in case of memcached crash
//
void
arcus_zk_log(zhandle_t *zh, const char *action)
{
    int     rc;
    char    zpath[200] = "";
    char    rcbuf[200] = "";
    char    sbuf[200] = "";

    struct timeval now;
    struct tm      *ltm;
    struct Stat    zstat;

    if (!zh || zoo_state(zh) != ZOO_CONNECTED_STATE || !arcus_conf.init || zk_root == NULL)
        return;

    gettimeofday(&now, 0);
    ltm = localtime (&now.tv_sec);
    strftime(sbuf, sizeof(sbuf), "%Y-%m-%d", ltm);
    snprintf(zpath, sizeof(zpath), "%s/%s/%s", zk_root, zk_log_path, sbuf);
    rc = zoo_exists(zh, zpath, ZK_NOWATCH, &zstat);

    // if this "date" directory does not exist, create one
    if (rc == ZNONODE) {
        rc = zoo_create (zh, zpath, NULL, 0, &ZOO_OPEN_ACL_UNSAFE,
                         0, rcbuf, sizeof(rcbuf));
        if (rc == ZOK && arcus_conf.verbose > 2) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG,
                            NULL, "znode %s does not exist. created\n", rcbuf);
        }
    }

    // if this znode already exist or the creation was successful
    if (rc == ZOK || rc == ZNODEEXISTS) {
        snprintf(zpath, sizeof(zpath), "%s/%s/%s/%s-", zk_root, zk_log_path, sbuf, arcus_conf.zk_path);
        snprintf(sbuf,  sizeof(sbuf), "%s", action);
        rc = zoo_create (zh, zpath, sbuf, strlen(sbuf), &ZOO_OPEN_ACL_UNSAFE,
                         ZOO_SEQUENCE, rcbuf, sizeof(rcbuf));
        if (arcus_conf.verbose > 2)
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "znode \"%s\" created\n", rcbuf);
    }

    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "cannot log this acvitiy (%s error)\n", zerror(rc));
    }
}

/* Close the ZK connection and die.  Do not do anything that may
 * block here (e.g. ZK operations).
 */
void
arcus_shutdown(const char *msg)
{
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "\nArcus memcached shutting down - %s\n", msg);

    if (!zh)
        exit (0);
#ifdef ENABLE_HEART_BEAT_THREAD
    /* Just set a flag and do not wait for the thread to die.
     * hb_thread is probably sleeping.  And, if it is in the middle of
     * doing a ping, it would block for a long time because worker threads
     * are likely all dead at this point.
     */
    hb_thread_running = false;
#endif

    /* Do not call zookeeper_close (arcus_exit) and
     * zoo_wget_children (arcus_cluster_watch_servers) at the same time.
     * Otherwise, this thread (main thread) and the watcher thread
     * (zoo_wget_children) may hang forever until the user manually kills
     * the process.  The root cause is within the zookeeper library.  Its
     * handling of concurrent API calls like zoo_wget_children and
     * zookeeper_close still has holes...
     */
    {
        bool watcher_running;
        pthread_mutex_lock(&zk_close_lock);
        zk_closing = true;
        watcher_running = zk_watcher_running;
        pthread_mutex_unlock(&zk_close_lock);

        /* If the watcher thread (zoo_wget_children) is running now, wait for
         * one second.  Either it completes, or it at least registers
         * sync_completion and blocks.  In the latter case, zookeeper_close
         * aborts all sync_completion's, which then wakes up the blocking
         * watcher thread.  zoo_wget_children returns an error.
         */
        if (watcher_running)
            sleep(1);
    }

    arcus_exit (zh, EX_OK);
}

int get_arcus_zk_timeout(void) {
    return (int)arcus_conf.zk_timeout;
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
mc_hb (zhandle_t *zh, void *context)
{
    app_ping_t      *data = (app_ping_t *) context;
    struct linger   linger;
    struct timeval  tv_timeo = data->to;
    int             flags;
    int             sock;
    char            buf[100];
    int             err=0;

    // make a tcp connection to this memcached itself,
    // and try "set arcus-zk5:ping".
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

    err = connect (sock, (struct sockaddr *) &data->addr, sizeof(struct sockaddr));
    if (err) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "mc_hb: cannot connect to local memcached (%s error)\n", strerror(errno));
        close (sock);
        return 0; // Allow ZK ping by returning 0 even if connect() fails.
    }

    // try to set a key "arcus-zk5:ping"
    // we need to be careful here. Since we can make a connection, basic
    // memcached event loop works and system resources are enough. if we can an
    // error during send()/recv(), it may not be a memcached failure at all.
    // We may get slab memory shortage for slab class 0 for above key.
    // For now, we simply return here without ping error or intentional delay
    err = send(sock, mc_hb_cmd, 29, 0);
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

    if (sock)
        close (sock);
    return 0;
}


void
arcus_exit(zhandle_t *zh, int err)
{
    if (zh)
        zookeeper_close(zh);
    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "shutting down\n");
    exit(err);
}

static void
sync_map_path(const char *root)
{
    int rc;
    char zpath[200];

    snprintf(zpath, sizeof(zpath), "%s/%s", root, zk_map_path);
    inc_count(1);
    rc = zoo_async(zh, zpath, arcus_zk_sync_cb, NULL);
    if (rc == ZOK) {
        wait_count(0);
        rc = last_rc; /* arcus_zk_sync_cb */
    }
    /* /arcus_1_7 may not exist.  So ignore ZNONODE */
    if (rc != ZOK && rc != ZNONODE) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to synchronize zpath. zpath=%s error=%s\n", zpath, zerror(rc));
        arcus_exit(zh, EX_PROTOCOL);
    }
}

static char *
get_service_code(const char *root)
{
    int rc;
    char zpath[200];
    struct String_vector strv = { 0, NULL };
    char *svc;

    /* First check: /cache_server_mapping/ip:port */
    snprintf(zpath, sizeof(zpath), "%s/%s/%s:%d", root, zk_map_path,
        arcus_conf.hostip, arcus_conf.port);
    rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
    if (rc == ZNONODE) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Cannot find the server mapping. zpath=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));

        /* Second check: /cache_server_mapping/ip */
        snprintf(zpath, sizeof(zpath), "%s/%s/%s", root, zk_map_path,
            arcus_conf.hostip);
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Checking the server mapping without the port number."
            " zpath=%s\n", zpath);
        rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
    }
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to read the server mapping. zpath=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        return NULL;
    }
    // zoo_get_children returns ZOK even if no child exist
    if (strv.count == 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "The server mapping exists but has no service code. zpath=%s\n",
            zpath);
        arcus_exit(zh, EX_PROTOCOL);
    }
    // get the first one. we could have more than one (meaning one server participating in
    // more than one service in the cluster: not likely though)
    svc = strdup(strv.data[0]);
    if (svc == NULL) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to allocate service code string\n");
        arcus_exit(zh, EX_PROTOCOL);
    }
    deallocate_String_vector(&strv);
    return svc;
}

void
arcus_zk_init(char *ensemble_list, int zk_to, EXTENSION_LOGGER_DESCRIPTOR *logger,
              int verbose, size_t maxbytes, int port,
              ENGINE_HANDLE_V1 *engine)
{
    int     rc;
    char    zpath[200] = "";
    char    rcbuf[200] = "";
    char    sbuf[200] = "";
    char    host[100] = "";
    char    myip[50] = "";
    char    *sep1=",", *sep2=":", *zip, *hlist;
    char    *hostp=NULL;
    char    *service_code;

    struct timeval          start_time, end_time;
    struct tm               *ltm;

    int                 sock;
    socklen_t           socklen=16;
    struct in_addr      inaddr;
    struct sockaddr_in  saddr, myaddr;
    struct hostent      *zkhost, *hp;
    long                difftime_us;
    struct Stat         zstat;
    app_ping_t          *ping_data;

    // save these for later use (restart)
    if (!arcus_conf.init) {
        arcus_conf.port          = port;    // memcached liste port
        arcus_conf.logger        = logger;
        arcus_conf.engine.v1     = engine;
        arcus_conf.verbose       = verbose;
        arcus_conf.maxbytes      = maxbytes;
        arcus_conf.ensemble_list = strdup(ensemble_list);
        // Use the user specified timeout only if it falls within
        // [MIN, MAX).  Otherwise, silently ignore it and use
        // the default value.
        if (zk_to >= MIN_ZK_TO && zk_to < MAX_ZK_TO)
            arcus_conf.zk_timeout = zk_to*1000; // msec conversion

        memset( &myid, 0, sizeof(myid) );
    }

    assert(logger);
    if (arcus_conf.verbose > 2)
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "-> arcus_zk_init\n");

    // Arcus Zookeeper Ensemble IP list
    if (!ensemble_list) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, " -z{ensemble_list} must not be empty\n");
        arcus_exit(zh, EX_USAGE);
    }

    // Need to figure out local IP. first create a dummy udp socket
    if ((sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP)) == -1) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "socket() failed: %s\n", strerror(errno));
        arcus_exit(zh, EX_OSERR);
    }

    saddr.sin_family    = AF_INET;
    saddr.sin_port      = htons(SYSLOGD_PORT); // syslogd port. what if this is not open? XXX

    // loop through all ensemble IP in case ensemble failure
    zip = strtok_r(ensemble_list, sep1, &hlist);  // separate IP:PORT tuple
    while (zip) {

        zip = strtok(zip, sep2);                    // extract the first IP

        // try to convert IP -> network IP format
        if (!inet_aton(zip, &inaddr)) {

            // Must be hostname, not IP. Convert hostame to IP first
            zkhost = gethostbyname(zip);
            if (!zkhost) {
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                "Invalid IP/hostname in arg: %s\n", zip);
                zip = strtok_r(NULL, sep1, &hlist);  // get next token: separate IP:PORT tuple
                continue;
            }

            if (arcus_conf.verbose > 2)
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "Ensemble hostname: %s\n", zip);
            memcpy((char *) &saddr.sin_addr, zkhost->h_addr_list[0], zkhost->h_length);
            inet_ntop(AF_INET, &saddr.sin_addr, rcbuf, sizeof(rcbuf));
            if (arcus_conf.verbose > 2)
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "Trying converted IP: %s\n", rcbuf);
        }
        else {
            // just use the IP
            saddr.sin_addr = inaddr;
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "Trying ensemble IP: %s\n", zip);
        }

        // try to connect to ensemble' arcus_conf.logger->logd UDP port
        // this is dummy code that simply is used to get local IP address
        int flags = fcntl(sock, F_GETFL, 0);

        fcntl(sock, F_SETFL, flags | O_NONBLOCK);
        rc = connect(sock, (struct sockaddr*) &saddr, sizeof(struct sockaddr_in));
        fcntl(sock, F_SETFL, flags);

        if (rc == 0) {
            // connect immediately
            if (arcus_conf.verbose > 2)
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                            "connected to ensemble \"%s\" syslogd UDP port\n", zip);
            break;
        }

        // if cannot connect immediately, try other ensemble
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Cannot connect to ensemble %s (%s error)\n", zip, strerror(errno));
        zip = strtok_r(NULL, sep1, &hlist);  // get next token: separate IP:PORT tuple
    }

    // failed to connect to all ensemble host. fail
    if (!zip) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "not able to connect any Ensemble server\n");
        arcus_exit(zh, EX_UNAVAILABLE);
    }

    // finally, what's my local IP?
    if (getsockname(sock, (struct sockaddr *) &myaddr, &socklen)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "getsockname failed: %s\n", strerror(errno));
        arcus_exit(zh, EX_NOHOST);
    }

    close(sock);
    sock = -1;

    // Finally local IP
    inet_ntop(AF_INET, &myaddr.sin_addr, myip, sizeof(myip));
    arcus_conf.hostip = strdup(myip);
    arcus_conf.logger->log(EXTENSION_LOG_DETAIL, NULL, "local IP is %s\n", myip);


    if (!arcus_conf.zk_path) {

        // Also get local hostname. We want IP and hostname to better identify this cache
        hp = gethostbyaddr( (char*) &myaddr.sin_addr.s_addr, sizeof(myaddr.sin_addr.s_addr), AF_INET );
        if (hp) {
            hostp = strdup (hp->h_name);
        }
        else {
            // if gethostbyaddr() doesn't work, try gethostname
            if (gethostname((char *) &host, sizeof(host))) {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "cannot get hostname: %s\n", strerror(errno));
                arcus_exit(zh, EX_NOHOST);
            }

            hostp = host;
        }

        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "hostname: %s\n", hostp);
    }

    // prepare app_ping context data: sockaddr for memcached connection in app ping
    //
    ping_data = calloc(1, sizeof(app_ping_t));
    assert (ping_data);

    ping_data->addr.sin_family = AF_INET;
    ping_data->addr.sin_port   = htons(arcus_conf.port);
    // Use the admin ip (currently localhost) to avoid competing with regular
    // clients for connections.
    ping_data->addr.sin_addr.s_addr = inet_addr(ADMIN_CLIENT_IP);
#ifdef ENABLE_HEART_BEAT_THREAD
    {
        /* +500 msec so that the caller can detect the timeout */
        uint64_t usec = HEART_BEAT_TIMEOUT * 1000 + 500000;
        ping_data->to.tv_sec = usec / 1000000;
        ping_data->to.tv_usec = usec % 1000000;
    }
#else
    // Just use ZK session timeout in seconds
    ping_data->to.tv_sec = arcus_conf.zk_timeout / 1000;
    ping_data->to.tv_usec = 0;
#endif

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

    /* Do not include the root path (i.e. /arcus or /arcus_1_7).
     * zookeeper_init creates a session with the ensemble, which is an
     * expensive quorum operation.
     * There are no APIs to change the root after zookeeper_init.
     * We would have to close the session and call zookeeper_init again.
     * So, deal with the root path ourselves.  The ZK lib just string-copies
     * and prepends the chroot anyway.  This is just a client-side library
     * trick.  Nothing special.
     */
    snprintf(zpath, sizeof(zpath), "%s", arcus_conf.ensemble_list);

    // connect to ZK ensemble
    if (arcus_conf.verbose > 2)
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "zookeeper_init()\n");

    gettimeofday(&start_time, 0);
    inc_count(1);
    zh = zookeeper_init(zpath, arcus_zk_watcher, arcus_conf.zk_timeout, &myid, 0, 0);
    if (!zh) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "zookeeper_init() failed (%s error)\n",
                                strerror(errno));
        arcus_exit(zh, EX_PROTOCOL);
    }

    // wait until above init callback is called
    // We need to wait until ZOO_CONNECTED_STATE
    if (wait_count(arcus_conf.zk_timeout) != 0) {
        /* zoo_state(zh) != ZOO_CONNECTED_STATE */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "cannot to be ZOO_CONNECTED_STATE\n");
        arcus_exit (zh, EX_PROTOCOL);
    }

    /* Retrieve the service code and figure out if we are in 1.7 or 1.6
     * cluster.
     */
    /* Check 1.7 first */
    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Checking to see if the server belongs to a 1.7 cluster...\n");
    zk_root = "/arcus_1_7";
    /* Do SYNC operation to bring the server up to date with the leader. */
    sync_map_path(zk_root);
    service_code = get_service_code(zk_root);
    if (service_code) {
        char *start[3], *end[3];

        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "The server belongs to a 1.7 cluster.\n");

        /* Parse svc, group, listen address.
         * 1.7 service code={svc}^{group}^{listen_ip:port}^
         */
        memset(start, 0, sizeof(start));
        if (0 != breakup_string(service_code, start, end, 3)) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "The server mapping seems to be invalid. service_code=%s\n",
                service_code);
            arcus_exit(zh, EX_PROTOCOL);
        }
        /* Null-terminate substrings */
        *end[0] = '\0';
        *end[1] = '\0';
        *end[2] = '\0';
        if (0 != parse_hostport(start[2], &arcus_conf.listen_addr, NULL)) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "The listen address seems to be invalid. listen_address=%s\n",
                start[2]);
            arcus_exit(zh, EX_PROTOCOL);
        }
        arcus_conf.svc = strdup(start[0]);
        arcus_conf.groupname = strdup(start[1]);
        arcus_conf.listen_addr_str = strdup(start[2]);
        free(service_code);
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Found the valid server mapping. service_code=%s group=%s"
            " listen_address=%s\n",
            arcus_conf.svc, arcus_conf.groupname, arcus_conf.listen_addr_str);
        is_1_7 = true;
    }
    else {
        /* See if we belong to 1.6 */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Checking to see if the server belongs to a 1.6 cluster...\n");
        zk_root = "/arcus";
        sync_map_path(zk_root);
        service_code = get_service_code(zk_root);
        if (service_code == NULL) {
            /* Cannot find the server mapping in 1.7 or 1.6 */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Cannot find the server mapping. Provision this server in"
                " Arcus cluster.\n");
            arcus_exit(zh, EX_PROTOCOL);
        }
        else {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Found the service code. The server belongs to a 1.6 cluster."
                " service_code=%s\n",
                service_code);
            arcus_conf.svc = service_code;
            is_1_7 = false;
        }
    }

#ifdef ENABLE_HEART_BEAT_THREAD
#else
    // Now register L7 ping
    if (zoo_register_app_ping(zh, &mc_hb, (void *) ping_data)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                 "zoo_register_app_ping() failed (%s error)\n", strerror(errno));
        arcus_exit(zh, EX_PROTOCOL);
    }
#endif

    // we need to keep ip:port-hostname tuple for later user (restart)
    snprintf(rcbuf, sizeof(rcbuf), "%s:%d-%s", arcus_conf.hostip, arcus_conf.port, hostp);
    arcus_conf.zk_path = strdup (rcbuf);

    /* Also save ip:port.  We use it in 1.7 lock directory... */
    snprintf(rcbuf, sizeof(rcbuf), "%s:%d", arcus_conf.hostip, arcus_conf.port);
    arcus_conf.mc_ipport_str = strdup(rcbuf);

    /* Initialize the state machine.  We do not know our role yet. */
    if (0 != sm_init()) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to initialize the state machine. Terminating.\n");
        arcus_exit(zh, EX_CONFIG);
    }

    snprintf(zpath, sizeof(zpath), "%s/%s/%s",
        zk_root, zk_cache_path, arcus_conf.svc);
    arcus_conf.cluster_path = strdup(zpath);

    /* Connected to ZK, all strings are set, and so on...*/
    arcus_conf.init = true;

    /* 1.6: we store the server node in the cache list right away. */
    if (!is_1_7) {
        // create "/cache_list/{svc}/ip:port-hostname" ephemeral znode
        snprintf(zpath, sizeof(zpath), "%s/%s/%s/%s",
            zk_root, zk_cache_path, arcus_conf.svc, arcus_conf.zk_path);
        sprintf(sbuf, "%ldMB", (long) arcus_conf.maxbytes/1024/1024);
        rc = zoo_create (zh, zpath, sbuf, strlen(sbuf), &ZOO_OPEN_ACL_UNSAFE,
            ZOO_EPHEMERAL, rcbuf, sizeof(rcbuf));
        if (rc) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "cannot create znode %s (%s error)\n",
                zpath, zerror(rc));
            if (rc == ZNODEEXISTS) {
                arcus_conf.logger->log( EXTENSION_LOG_DETAIL, NULL,
                    "old session still exist. Wait a bit\n");
            }
            arcus_exit (zh, EX_PROTOCOL);
        }
        else {
            if (arcus_conf.verbose > 2)
                arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "Joined Arcus cloud at %s\n", rcbuf);
        }

        rc = zoo_exists (zh, zpath, 0, &zstat);
        if (rc) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "cannot find %s just created (%s error)\n",
                rcbuf, zerror(rc));
            arcus_exit (zh, EX_PROTOCOL);
        }

        // store this version number, just in case we restart
        arcus_conf.zk_path_ver = zstat.version;
    }
    /* 1.7: we first determine the role (master/slave) and then
     * store the server node in the cache list.
     */

    gettimeofday(&end_time, 0);
    difftime_us = (end_time.tv_sec*1000000+end_time.tv_usec) -
                  (start_time.tv_sec*1000000 + start_time.tv_usec);

    ltm = localtime (&end_time.tv_sec);
    strftime(sbuf, sizeof(sbuf), "%Y-%m-%d %H:%M:%S", ltm);

    // log this join activity in /arcus/cache_server_log
    arcus_zk_log(zh, "join");

    // We have finished registering this memcached instance to Arcus cluster
    arcus_conf.logger->log(
                    EXTENSION_LOG_INFO,
                    NULL,
                    "Memcached joined Arcus cache cloud for \"%s\" service"
                    " (took %ld microsec)\n",
                    arcus_conf.svc,
                    difftime_us);
    // "recv" timeout is actually the session timeout
    // ZK client ping period is recv_timeout / 3.
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "ZooKeeper session timeout: %d sec\n",
                    zoo_recv_timeout(zh)/1000);

    struct String_vector strv = { 0, NULL };
    if (0 != read_cache_list(&strv, false /* do not register watcher */)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Cannot complete the initialization without the cache list. "
            "Terminating...\n");
        arcus_exit(zh, EX_CONFIG);
    }

    if (is_1_7) {
        /* Sanity check ephemeral znodes in lock and cache_list. */
        if (0 != sm_check_dup_in_lock() || 0 != sm_check_dup_in_cache_list())
            arcus_exit(zh, EX_CONFIG);

        /* Create this server's node in the lock directory */
        if (0 != sm_create_lock_znode())
            arcus_exit(zh, EX_CONFIG);
    }

#ifdef ENABLE_CLUSTER_AWARE
    arcus_conf.ch = cluster_config_init(arcus_conf.logger, arcus_conf.verbose);

    char self_hostport[200];
    if (is_1_7) {
        /* In 1.7, the group name appears on the hash ring */
        snprintf(self_hostport, sizeof(self_hostport), "%s",
            arcus_conf.groupname);
    }
    else {
        /* In 1.6, the server name "ip:port " appears on the hash ring */
        snprintf(self_hostport, sizeof(self_hostport), "%s:%u",
            arcus_conf.hostip, arcus_conf.port);
    }
    cluster_config_set_hostport(arcus_conf.ch, self_hostport, 200);
    update_cluster_config(&strv);

    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                           "Memcached is watching Arcus cache cloud for \"%s\"\n",
                           arcus_conf.cluster_path);
#endif
    deallocate_String_vector(&strv);

    /* Wake up the state machine thread.
     * Tell it to refresh the hash ring (cluster_config).
     * It will also figure out master/slave role.
     */
    sm_lock();
    /* Don't care if we just read the list above.  Do it again. */
    sm_info.update_cache_list = true;
    sm_info.watch_cache_list = true;
    sm_info.update_lock = true;
    sm_wakeup(true);
    sm_unlock();

#ifdef ENABLE_HEART_BEAT_THREAD
    start_hb_thread(ping_data);
#endif

    // Either got SIG* or memcached shutdown process finished
    if (arcus_zk_shutdown) {
        arcus_shutdown("Interrupted");
    }

    return;
}

int
arcus_zk_set_ensemble(char *ensemble_list)
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
    }
    else {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to change the ZooKeeper ensemble list. The handle is invalid.\n");
    }
    return -1;
}

int
arcus_zk_get_ensemble_str(char *buf, int size)
{
    int rc;
    if (zh) {
        rc = zookeeper_get_ensemble_string(zh, buf, size);
        if (rc == ZOK)
            return 0;
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the ZooKeeper ensemble list. error=%d(%s)\n", rc, zerror(rc));
    }
    else {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the ZooKeeper ensemble list. The handle is invalid.\n");
    }
    return -1;
}

#ifdef ENABLE_CLUSTER_AWARE
bool arcus_cluster_is_valid()
{
    return cluster_config_is_valid(arcus_conf.ch);
}

bool arcus_key_is_mine(const char *key, size_t nkey)
{
    bool key_is_mine;
    uint32_t key_id, self_id;

    key_is_mine = cluster_config_key_is_mine(arcus_conf.ch, key, nkey,
                                             &key_id, &self_id);

    if (arcus_conf.verbose > 2) {
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                               "key=%s, self_id=%d, key_id=%d, key_is_mine=%s\n",
                               key, self_id, key_id,
                               (key_is_mine)?"true":"false");
    }

    return key_is_mine;
}
#endif

/* Several 1.7 znode names use '^' as a delimiter.
 * Example: svc^group^ip:port^
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
        if (*c == '\0')
            break;
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
    if (i != vec_len) {
        /* There are fewer substrings than expected. */
        return -1;
    }
    return 0;
}

/* This function's copied from util_common.c.  FIXME */
static int
fill_sockaddr(const char *host, struct sockaddr_in *addr)
{
    /* */
    struct addrinfo hints;
    struct addrinfo *res, *info;
    int s;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; /* IPv4 */
    res = NULL;
    s = getaddrinfo(host, NULL, NULL, &res);
    if (s == 0) {
        info = res;
        while (info) {
            if (info->ai_family == AF_INET && info->ai_addr &&
                info->ai_addrlen >= sizeof(*addr)) {
                /* Use the first address we find */
                struct sockaddr_in *in = (struct sockaddr_in*)info->ai_addr;
                const char *ip;
                char buf[INET_ADDRSTRLEN*2];

                addr->sin_family = in->sin_family;
                addr->sin_addr = in->sin_addr;

                ip = inet_ntop(AF_INET, (const void*)&addr->sin_addr, buf, sizeof(buf));
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Address for host. host=%s addr=%s canonicalname=%s\n",
                    host, ip ? ip : "null",
                    info->ai_canonname ? info->ai_canonname : "null");
                break;
            }
            info = info->ai_next;
        }
        if (info == NULL) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "No addresses for the host. host=%s\n", host);
            s = -1;
        }
    }
    else {
        if (s == EAI_SYSTEM) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to get the address of the host. host=%s\n",
                host);
        }
        else {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to get the address of the host. host=%s error=%s\n",
                host, gai_strerror(s));
        }
        s = -1;
    }

    if (res)
        freeaddrinfo(res);
    return s;
}

/* This function's copied from util_common.c.  FIXME */
static int
parse_hostport(const char *addr_str, struct sockaddr_in *addr, char **host_out)
{
    char *host, *c, *dup;
    int port, err = -1;

    dup = strdup(addr_str);
    if (dup == NULL) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "failed to allocate address buffer. len=%ld\n", strlen(addr_str));
        goto exit;
    }
    c = dup;
    host = c;
    while (*c != ':' && *c != '\0')
        c++;
    if (*c == '\0') {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "bad address string %s\n", addr_str);
        goto exit;
    }
    *c++ = '\0';
    errno = 0;
    port = strtol(c, NULL, 10);
    if (errno != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "error while parsing port number (strtol). address=%s\n", addr_str);
        goto exit;
    }
    if (port <= 0 || port >= 64*1024) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "bad port %d in address string %s\n", port, addr_str);
        goto exit;
    }
    memset(addr, 0, sizeof(*addr));
    if (0 != fill_sockaddr(host, addr)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "cannot find the address for host %s\n", host);
        goto exit;
    }
    addr->sin_port = htons(port);
    err = 0;
exit:
    if (err == 0 && host_out != NULL)
        *host_out = host;
    else if (dup)
        free(dup);
    return err;
}

/* This function's copied from util_common.c.  FIXME */
static void
gettime(uint64_t *msec, struct timeval *ptv, struct timespec *pts)
{
    struct timeval tv;
    if (0 != gettimeofday(&tv, NULL)) {
        /* Cannot fix this */
        abort();
    }
    if (msec) {
        *msec = ((uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec/1000);
    }
    if (ptv) {
        *ptv = tv;
    }
    if (pts) {
        pts->tv_sec = tv.tv_sec;
        pts->tv_nsec = (long)tv.tv_usec * 1000;
    }
}

static void
sm_stop_slave(void)
{
    /* FIXME */
}

static void
sm_become_master(bool *retry)
{
    int rc, i, groupname_len, slave_count;
    char zpath[200];
    char *sbuf = "0";
    struct String_vector *cache_list;

    /* 3-step process.
     * 1. Create the master znode in cache_list.
     * 2. Delete the slave znode in cache_list.
     * 3. Tell memcached to start acting as the master.
     *
     * Between (1)-(2), cache_list has two znodes with this server's address,
     * if it is failing over.  (1) and (2) may fail, and we may retry.
     *
     * We create the master znode first so that clients do not delete
     * the group.
     *
     * Between (1)-(3), the server still thinks it's the slave, while clients
     * may see it as the new master.  The server may receive write requests
     * from clients.  These requests would fail.
     */

    sm_info.state = SM_STATE_BECOME_MASTER;

    /* sm_check_dup_in_cache_list already checked that there are no znodes
     * with this server's ip:port.  If we are failing over, there might be
     * the slave znode.  Other than that, there are no other znodes with
     * this server's address.  Aassume no one has created one in
     * the meantime...
     */

    /* Make sure there are no master nodes in cache_list. */
    cache_list = &sm_info.cache_list;
    groupname_len = strlen(arcus_conf.groupname);
    slave_count = 0;
    for (i = 0; i < cache_list->count; i++) {
        char *cur = cache_list->data[i];
        int cur_len = strlen(cur);
        if (strlen(cur) > groupname_len &&
            0 == memcmp(cur, arcus_conf.groupname, groupname_len)) {
            /* Have the same group name */
            cur_len -= groupname_len;
            if (cur_len < 3) {
                /* No ^M^ or ^S^ */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Trying to become the master. Found an invalid znode"
                    " in the cache_list directory."
                    " Terminating... path=%s\n",
                    cache_list->data[i]);
                arcus_exit(zh, EX_OK /* FIXME */);
            }
            cur = cur + groupname_len + 1; /* should point to M/S */
            if (*cur == 'M') {
                /* The master node.  Okay if it is ours.  If not, wait. */
                if (0 != strcmp(cache_list->data[i], sm_info.master_name)) {
                    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Trying to become the master. Found the master znode"
                        " in the cache_list directory that is not the"
                        " current server. Wait till it disappears."
                        " path=%s\n",
                        cache_list->data[i]);
                    /* *retry = true; */
                    return;
                }
            }
            else if (*cur == 'S') {
                /* There may be slave nodes.  Either ours or another
                 * instance's.  Count nodes that are not ours.
                 * There should be at most 1.
                 */
                if (0 != strcmp(cache_list->data[i], sm_info.slave_name))
                    slave_count++;
            }
            else {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Trying to become the master. Found an invalid znode"
                    " in the cache_list directory."
                    " Terminating... path=%s\n",
                    cache_list->data[i]);
                arcus_exit(zh, EX_OK /* FIXME */);
            }
        }
    }
    if (slave_count > 1) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the master. Too many slave znodes"
            " in the cache_list directory. Wait till there is at most one."
            " slave_count=%d\n", slave_count);
        /* *retry = true; */
        return;
    }

    /* Create the znode for this server.
     * name=group^M^ip:port-hostname
     */
    snprintf(zpath, sizeof(zpath), "%s/%s^M^%s", arcus_conf.cluster_path,
        arcus_conf.groupname, arcus_conf.zk_path);
    rc = zoo_create(zh, zpath, sbuf, strlen(sbuf), &ZOO_OPEN_ACL_UNSAFE,
        ZOO_EPHEMERAL, NULL, 0);
    if (rc == ZNODEEXISTS) {
        /* Okay.  We must have created this znode previously, but received
         * an error like operation timeout.
         */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the master. The znode for this server already"
            " exists in the cache_list directory. It is okay."
            " path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
    }
    else if (rc == ZNOTHING || rc == ZSESSIONMOVED || rc == ZSESSIONEXPIRED ||
        rc == ZOPERATIONTIMEOUT || rc == ZCONNECTIONLOSS) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the master. Failed to create the znode"
            " in the cache_list directory. Will try again..."
            " path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        *retry = true;
        return;
    }
    else if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the master. Failed to create the znode"
            " in the cache_list directory with an unexpected error."
            " Terminating... path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        arcus_exit(zh, EX_OK /* FIXME */);
    }

    /* Delete the slave znode if it exists. */
    snprintf(zpath, sizeof(zpath), "%s/%s^S^%s", arcus_conf.cluster_path,
        arcus_conf.groupname, arcus_conf.zk_path);
    rc = zoo_delete(zh, zpath, -1);
    if (rc == ZNONODE) {
        /* Okay */
    }
    else if (rc == ZNOTHING || rc == ZSESSIONMOVED || rc == ZSESSIONEXPIRED ||
        rc == ZOPERATIONTIMEOUT || rc == ZCONNECTIONLOSS) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the master. Failed to delete the slave znode"
            " in the cache_list directory. Will try again..."
            " path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        *retry = true;
        return;
    }
    else if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the master. Failed to delete the slave znode"
            " in the cache_list directory with an unexpected error."
            " Terminating... path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        arcus_exit(zh, EX_OK /* FIXME */);
    }

    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Committed to the master mode.");
    sm_info.state = SM_STATE_MASTER;

    /* Tell memcached to start acting as the master. */
    if (0 != arcus_memcached_master_mode(arcus_conf.listen_addr_str)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to enable the master mode. Terminating...\n");
        arcus_exit(zh, EX_OK);
    }
    if (0 != arcus_memcached_slave_address(sm_info.slave_laddr)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to set the slave's listen address. "
            "Terminating...\n");
        arcus_exit(zh, EX_OK);
    }
}

static void
sm_become_slave(bool *retry)
{
    int rc;
    char zpath[200];
    char *sbuf = "0";

    sm_info.state = SM_STATE_BECOME_SLAVE;

    /* sm_check_dup_in_cache_list already checked that there are no znodes
     * with this server's ip:port.  Assume no one has created one in
     * the meantime...
     */

    /* Create the znode for this server.
     * name=group^S^ip:port-hostname
     */
    snprintf(zpath, sizeof(zpath), "%s/%s", arcus_conf.cluster_path,
        sm_info.slave_name);
    rc = zoo_create(zh, zpath, sbuf, strlen(sbuf), &ZOO_OPEN_ACL_UNSAFE,
        ZOO_EPHEMERAL, NULL, 0);
    if (rc == ZNODEEXISTS) {
        /* Okay.  We must have created this znode previously, but received
         * an error like operation timeout.
         */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the slave. The znode for this server already"
            " exists in the cache_list directory. It is okay."
            " path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
    }
    else if (rc == ZNOTHING || rc == ZSESSIONMOVED || rc == ZSESSIONEXPIRED ||
        rc == ZOPERATIONTIMEOUT || rc == ZCONNECTIONLOSS) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the slave. Failed to create the znode"
            " in the cache_list directory. Will try again..."
            " path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        *retry = true;
        return;
    }
    else if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Trying to become the slave. Failed to create the znode"
            " in the cache_list directory with an unexpected error."
            " Terminating... path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        arcus_exit(zh, EX_OK /* FIXME */);
    }

    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Committed to the slave mode.");
    sm_info.state = SM_STATE_SLAVE;

    /* Tell memcached to start acting as the slave. */
    if (0 != arcus_memcached_slave_mode(arcus_conf.listen_addr_str,
            sm_info.master_laddr)) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to enable the slave mode. Terminating...\n");
        arcus_exit(zh, EX_OK);
    }
}

static int
sm_check_dup_in_cache_list(void)
{
    int i, rc;
    char *zpath = arcus_conf.cluster_path;
    char *start[2], *end[2], *c;
    struct String_vector strv = { 0, NULL };

    rc = zoo_wget_children(zh, zpath, NULL, NULL, &strv);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the list of znodes in the cache_list directory."
            " path=%s error=%d(%s)\n", zpath, rc, zerror(rc));
        return -1;
    }

    rc = 0;
    for (i = 0; i < strv.count; i++) {
        /* lock znode=group^{m,s}^ip:port-hostname */
        if (0 != breakup_string(strv.data[i], start, end, 2)) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Error while checking the cache_list directory."
                " The znode does not include group and/or role."
                " znode=%s\n", strv.data[i]);
            rc = -1;
            break;
        }
        c = end[1];
        while (*c != '\0') {
            if (*c == '-')
                break;
            c++;
        }
        if (*c == '\0' || (c - end[1]) < 2) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Error while checking the cache_list directory."
                " The node does not include ip:port."
                " znode=%s\n", strv.data[i]);
            rc = -1;
            break;
        }
        *c = '\0';
        if (0 == strcmp(arcus_conf.mc_ipport_str, end[1])) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "The cache_list directory includes a znode that has this"
                " server's address. It might be from a previous instance"
                " of this server. znode=%s\n", strv.data[i]);
            rc = -1;
            break;
        }
    }

    deallocate_String_vector(&strv);
    return rc;
}

static int
sm_check_dup_in_lock(void)
{
    int i, rc;
    char *zpath = sm_info.lock_dir_path;
    struct String_vector strv = { 0, NULL };
    char *start[1], *end[1];

    rc = zoo_wget_children(zh, zpath, NULL, NULL, &strv);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the list of znodes in the lock directory."
            " path=%s error=%d(%s)\n", zpath, rc, zerror(rc));
        return -1;
    }

    rc = 0;
    for (i = 0; i < strv.count; i++) {
        /* lock znode=ip:port^ip2:port2^seq */
        if (0 != breakup_string(strv.data[i], start, end, 1)) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Error while checking the lock directory."
                " An invalid znode name. znode=%s\n", strv.data[i]);
            rc = -1;
            break;
        }
        end[0] = '\0';
        if (0 == strcmp(arcus_conf.mc_ipport_str, start[0])) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "The lock directory includes a znode that has this server's"
                " address. It might be from a previous instance of this"
                " server. znode=%s\n", strv.data[i]);
            rc = -1;
            break;
        }
    }

    deallocate_String_vector(&strv);
    return rc;
}

/* Create the lock znode.  It is ephemeral and sequential. */
static int
sm_create_lock_znode(void)
{
    char zpath[256];
    char rcbuf[256];
    char *sbuf = "0";
    int rc;

    /* lock_dir/ip:port^ip2:port2^seq */
    snprintf(zpath, sizeof(zpath), "%s/%s:%d^%s^", sm_info.lock_dir_path,
        arcus_conf.hostip, arcus_conf.port, arcus_conf.listen_addr_str);

    rc = zoo_create(zh, zpath, sbuf, strlen(sbuf), &ZOO_OPEN_ACL_UNSAFE,
        ZOO_EPHEMERAL | ZOO_SEQUENCE, rcbuf, sizeof(rcbuf));
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to create the lock znode. path=%s error=%d(%s)\n",
            zpath, rc, zerror(rc));
        return -1;
    }

    /* Save the actual path for later use.  This includes the sequence number
     * that is automatically generated by ZK.
     */
    sm_info.lock_znode_path = strdup(rcbuf);
    assert(sm_info.lock_znode_path != NULL);

    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
        "Created the lock znode. path=%s\n", sm_info.lock_znode_path);
    return 0;
}

static void
save_listen_address(char *node, char *buf, int buf_len)
{
    char *start[2], *end[2];
    int len;

    if (0 != breakup_string(node, start, end, 2)) {
        /* We've done this sanity check above.  If it fails now, it's
         * a program error...
         */
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Fatal error. line=%d\n", __LINE__);
        arcus_exit(zh, EX_OK);
    }
    len = end[1] - start[1];
    if (len >= buf_len) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "The listen address is too long. length=%d path=%s\n",
            len, node);
    }
    else {
        memcpy(buf, start[1], len);
        buf[len] = '\0';
    }
}

/* From the list of children znodes in the lock directory,
 * figure out if we are the lock owner.
 */
static int
sm_get_master_lock_status(struct String_vector *node_list)
{
    int i, j, len, found, slave_count, slave_idx;
    char *c;
    uint64_t seq[16]; /* There should be at most two, usually */
    char *start[2], *end[2];

    /* Do not assume that node_list is sorted by the sequence number.
     * Find our lock znode and get its sequence number.
     * Then go over the list again and see if there are other znodes with
     * lower sequence number than ours.
     */
    if (node_list->count <= 0)
        return SM_MASTER_LOCK_STATUS_UNKNOWN; // Nothing in the list
    if (node_list->count > 16) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Too many znodes in the lock directory. count=%d\n",
            node_list->count);
        return SM_MASTER_LOCK_STATUS_UNKNOWN;
    }

    found = -1;
    for (i = 0; i < node_list->count; i++) {
        c = node_list->data[i];
        len = strlen(c);
        if (len < 2) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "A znode in the lock directory appears to have an invalid"
                " name. znode=%s\n", c);
            return SM_MASTER_LOCK_STATUS_UNKNOWN;
        }
        /* Scan backwards looking for '^' */
        c = c+len;  /* '\0' */
        while (c >= node_list->data[i]) {
            if (*c == '^')
                break;
            c--;
        }
        if (c < node_list->data[i] ||
            (node_list->data[i]+len - c) < 2 /* too short */) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "A znode in the lock directory has no sequence number."
                " znode=%s\n", node_list->data[i]);
            return SM_MASTER_LOCK_STATUS_UNKNOWN;
            /* Should never have these names.  The admin should look into
             * the directory and see what is going on...
             */
        }
        len = c - node_list->data[i];
        if (len < 1) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "A znode in the lock directory has no server address."
                " znode=%s\n", node_list->data[i]);
            return SM_MASTER_LOCK_STATUS_UNKNOWN;
        }

        /* ip:port^ip2:port2^
         * c points to the last '^'
         * Grab the first ip:port and see if it matches our address.
         */
        start[0] = start[1] = end[0] = end[1] = NULL;
        if (0 != breakup_string(node_list->data[i], start, end, 2)) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "A znode in the lock directory has an invalid name."
                " znode=%s\n", node_list->data[i]);
            return SM_MASTER_LOCK_STATUS_UNKNOWN;
        }
        len = end[0] - start[0];
        if (strlen(arcus_conf.mc_ipport_str) == len &&
            0 == memcmp(start[0], arcus_conf.mc_ipport_str, len)) {
            if (found >= 0) {
                /* Multiple znodes that have our server address
                 * This can happen when memcached dies and then starts
                 * again before ZK times out the old session.
                 */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Multiple znodes in the lock directory have the current"
                    " server's address. prev_znode=%s current_znode=%s\n",
                    node_list->data[found], node_list->data[i]);
                return SM_MASTER_LOCK_STATUS_UNKNOWN;
            }
            else {
                /* Remember the index */
                found = i;
            }
        }

        /* Parse the sequence number.  It is 32-bit decimal (base 10).
         * For example, "0000000019".
         */
        c++;
        seq[i] = 0;
        while (*c != '\0') {
            uint32_t num;
            if (*c >= '0' && *c <= '9')
                num = *c - '0';
            else {
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "An invalid character in the sequence number."
                    " znode=%s\n", node_list->data[i]);
                return SM_MASTER_LOCK_STATUS_UNKNOWN;
            }
            seq[i] = seq[i] * 10 + num;
            c++;
        }

        /* More sanity check.  Any duplicate sequence numbers? */
        for (j = 0; j < i; j++) {
            if (seq[i] == seq[j]) {
                /* ZK is really broken... */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Multiple znodes in the lock directory have the same"
                    " sequence number. prev_znode=%s seq=%llu"
                    " current_znode=%s seq=%llu\n",
                    node_list->data[j], (unsigned long long)seq[j],
                    node_list->data[i], (unsigned long long)seq[i]);
                return SM_MASTER_LOCK_STATUS_UNKNOWN;
            }
        }

        arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
            "Parsed a znode in the lock directory. count=%d znode=%s"
            " seq=%llu\n",
            i, node_list->data[i], (unsigned long long)seq[i]);
    }
    if (found < 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Cannot find the current server's znode in the lock directory."
            " server_ipport=%s\n", arcus_conf.mc_ipport_str);
        return SM_MASTER_LOCK_STATUS_UNKNOWN;
        /* Should not happen unless our ZK session has expired just now */
    }

    for (i = 0; i < node_list->count; i++) {
        if (found != i && seq[found] > seq[i]) {
            /* This znode has a lower sequence number than our znode.
             * Find the master and save its listen address.
             */
            int master_idx = 0;
            for (i = 1; i < node_list->count; i++) {
                if (seq[master_idx] > seq[i])
                    master_idx = i;
            }
            save_listen_address(node_list->data[master_idx],
                sm_info.master_laddr, sizeof(sm_info.master_laddr));
            return SM_MASTER_LOCK_STATUS_NOT_OWNER;
        }
    }

    /* Great, we are the owner.
     * Figure out the slave's listen address, if any.
     */
    memset(sm_info.slave_laddr, 0, sizeof(sm_info.slave_laddr));
    slave_count = 0;
    for (i = 0; i < node_list->count; i++) {
        if (found != i) {
            slave_idx = i;
            slave_count++;
        }
    }
    if (slave_count == 1) {
        save_listen_address(node_list->data[slave_idx], sm_info.slave_laddr,
            sizeof(sm_info.slave_laddr));
    }
    else {
        /* Either too many slaves or no slaves at all.
         * Check again later.
         */
    }
    return SM_MASTER_LOCK_STATUS_OWNER;
}

static void
arcus_lock_watcher(zhandle_t *zh, int type, int state, const char *path,
    void *ctx)
{
    /* Just leave a message and let the sm thread deal with updating
     * lock status.
     */
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
        "Event from the lock directory. type=%d state=%d path=%s\n",
        type, state, path == NULL ? "null" : path);
    sm_lock();
    sm_info.update_lock = true;
    sm_wakeup(true);
    sm_unlock();
}

static int
read_lock_list(struct String_vector *strv, bool watch)
{
    int rc;
    char *zpath = sm_info.lock_dir_path;

    rc = zoo_wget_children(zh, zpath, watch ? arcus_lock_watcher : NULL,
        NULL, strv);
    if (rc != ZOK) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get the list of znodes in the lock directory."
            " path=%s error=%d(%s)\n", zpath, rc, zerror(rc));
    }
    /* The caller must free strv */
    return (rc == ZOK ? 0 : -1);
}

static const char *
sm_state_to_string(int state)
{
    if (state >= 0 && state <= SM_STATE_MAX)
        return sm_state_str[state];
    return "INVALID";
}

static int
sm_run(int master_lock_status, bool *retry)
{
    assert(master_lock_status >= SM_MASTER_LOCK_STATUS_INVALID &&
        master_lock_status <= SM_MASTER_LOCK_STATUS_MAX);
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
        "sm_run enter: state=%d(%s) master_lock_status=%d(%s)\n",
        sm_info.state, sm_state_to_string(sm_info.state),
        master_lock_status, sm_master_lock_status_str[master_lock_status]);

    switch (sm_info.state) {
        case SM_STATE_UNKNOWN:
        {
            if (master_lock_status == SM_MASTER_LOCK_STATUS_OWNER)
                sm_become_master(retry);
            else if (master_lock_status == SM_MASTER_LOCK_STATUS_NOT_OWNER)
                sm_become_slave(retry);
        }
        break;

        case SM_STATE_BECOME_MASTER:
        {
            if (master_lock_status == SM_MASTER_LOCK_STATUS_NOT_OWNER) {
                /* We were transitioning to the master mode, but did not
                 * finish.  Now that we've lost the lock, we cannot become
                 * the master.  We don't support master->slave transition.
                 * So, fail-stop.
                 */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Lost the master lock in ZK while in state=BECOME_MASTER. "
                    "Terminating...\n");
                arcus_exit(zh, EX_OK);
            }
            else {
                sm_become_master(retry);
            }
        }
        break;

        case SM_STATE_MASTER:
        {
            if (master_lock_status == SM_MASTER_LOCK_STATUS_NOT_OWNER) {
                /* FIXME Lost the lock?  Can this happen?
                 * The ephemeral znode in the lock directory does not
                 * disappear unless our ZK session expires.  If the session
                 * expires, we would have fail-stopped by now.
                 */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Lost the master lock in ZK while in state=MASTER. "
                    "Terminating...\n");
                arcus_exit(zh, EX_OK);
            }
            else {
                /* We are the master.  Do sanity checks if necessary.
                 * Pass the slave's listen address to the replication code.
                 * It may be null.  It is okay.
                 */
                if (0 != arcus_memcached_slave_address(sm_info.slave_laddr)) {
                    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to set the slave's listen address. "
                        "Terminating...\n");
                    arcus_exit(zh, EX_OK);
                }
            }
        }
        break;

        case SM_STATE_BECOME_SLAVE:
        {
            if (master_lock_status == SM_MASTER_LOCK_STATUS_OWNER) {
                /* Have not finished transitioning to the slave.
                 * But we've acquired the master lock.  So become the master.
                 */
                sm_become_master(retry);
            }
            else {
                /* Did not finish transition to the slave mode before.
                 * Try again now.
                 */
                sm_become_slave(retry);
            }
        }
        break;

        case SM_STATE_SLAVE:
        {
            if (master_lock_status == SM_MASTER_LOCK_STATUS_OWNER) {
                /* Slave -> master.  This is the only failover case
                 * we support.
                 */
                sm_stop_slave();
                sm_become_master(retry);
            }
            else {
                /* We are the slave.  Do sanity checks in the slave mode
                 * if necessary.  Nothing to do now.
                 */
            }
        }
        break;

        case SM_STATE_BECOME_UNKNOWN:
        {
            // Not used now
        }
        break;

        default:
        {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Program error. sm_run: invalid state. state=%d\n",
                sm_info.state);
            arcus_exit(zh, EX_SOFTWARE);
        }
        break;
    }

    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
        "sm_run exit: state=%d(%s)\n",
        sm_info.state, sm_state_to_string(sm_info.state));
    return 0;
}

static void *
sm_thread(void *arg)
{
    bool update_cache_list;
    bool update_lock;
    bool watch_cache_list;
    bool retry = false;
    int lock_status;
    struct String_vector strv_cache_list, strv_lock;
    bool strv_cache_list_valid;

    // Run forever
    while (sm_info.running) {
        sm_lock();
        while (!sm_info.notification) {
            /* Poll if requested.  Otherwise, wait till the ZK watcher
             * wakes us up.
             */
            if (retry) {
                struct timespec ts;
                uint64_t msec;
                gettime(&msec, NULL, NULL);
                msec += 1000; /* Wake up in 1 second.  Magic number.  FIXME */
                ts.tv_sec = msec / 1000;
                ts.tv_nsec = (msec % 1000) * 1000000;
                pthread_cond_timedwait(&sm_info.cond, &sm_info.lock, &ts);
                retry = false;
                break;
            }
            else {
                pthread_cond_wait(&sm_info.cond, &sm_info.lock);
            }
        }
        sm_info.notification = 0; // Clear
        update_cache_list = sm_info.update_cache_list;
        watch_cache_list = sm_info.watch_cache_list;
        update_lock = sm_info.update_lock;
        sm_info.update_cache_list = false;
        sm_info.watch_cache_list = false;
        sm_info.update_lock = false;
        sm_unlock();

#ifndef ENABLE_CLUSTER_AWARE
        /* Don't need to access cache_list unless we are using 1.7
         * or 1.6+cluster_aware.
         */
        if (!is_1_7)
            update_cache_list = false;
#endif
        /* Read the latest hash ring */
        strv_cache_list.data = NULL;
        strv_cache_list.count = 0;
        strv_cache_list_valid = false;
        if (update_cache_list) {
            if (0 != read_cache_list(&strv_cache_list, watch_cache_list)) {
                retry = true;
                sm_lock();
                sm_info.update_cache_list = true;
                sm_info.watch_cache_list = watch_cache_list;
                /* FIXME.  Don't actually need this watch flag...
                 * We always want to set the watch.
                 * This is the only place where we read cache_list.
                 */
                sm_unlock();
                /* ZK operations can fail.  For example, when we are
                 * disconnected from ZK, operations fail with connectionloss.
                 * Or, we would see operation timeout.
                 */
            }
            else {
                strv_cache_list_valid = true;
#ifdef ENABLE_CLUSTER_AWARE
                update_cluster_config(&strv_cache_list);
#endif
            }
        }
        if (!is_1_7) {
            deallocate_String_vector(&strv_cache_list);
            continue;
        }
        /* Everything below is 1.7 specific */

        /* Remember the latest cache list */
        if (strv_cache_list_valid) {
            deallocate_String_vector(&sm_info.cache_list);
            sm_info.cache_list = strv_cache_list;
            strv_cache_list.count = 0;
            strv_cache_list.data = NULL;
        }

        /* Determine the master lock status */
        lock_status = SM_MASTER_LOCK_STATUS_INVALID;
        if (update_lock) {
            strv_lock.count = 0;
            strv_lock.data = NULL;
            if (0 != read_lock_list(&strv_lock, true)) {
                retry = true;
                sm_info.update_lock = true; /* locking unnecessary */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Cannot determine the master lock status."
                    " Will try again.\n");
            }
            else {
                /* Remember the latest lock list */
                deallocate_String_vector(&sm_info.lock_list);
                sm_info.lock_list = strv_lock;
                strv_lock.count = 0;
                strv_lock.data = NULL;

                /* Compute the latest status */
                lock_status = sm_get_master_lock_status(&sm_info.lock_list);
                arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                    "Current master lock status=%s\n",
                    sm_master_lock_status_str[lock_status]);
            }
        }

        /* Make state ransitions based on the current lock status and
         * the old state.
         */
        sm_run(lock_status, &retry);
    }

    return NULL;
}

#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
static void *
sm_timer_thread(void *arg)
{
    uint64_t start_msec = 0, now_msec;

    /* Run forever.
     * When the ZK watcher says it lost connection to ZK, start running
     * the timer.  If the timer expires, fail-stop.  If the ZK watcher
     * says it re-connected to ZK, cancel the timer.
     */
    while (1) {
        /* Keep the logic simple.  Poll once every 100msec. */
        usleep(100000);
        if (!zk_connected) {
            /* Disconnected */
            gettime(&now_msec, NULL, NULL);
            if (start_msec == 0)
                start_msec = now_msec;
            if ((now_msec - start_msec) >= DEFAULT_ZK_TO/3 - 1000) {
                /* We need to die slightly before our session times out
                 * in ZK ensemble.  Otherwise, we might end up with multiple
                 * masters.  It is very fragile...
                 * FIXME
                 */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Disconnected from ZK for too long. Terminating..."
                    " duration_msec=%llu\n",
                    (long long unsigned)(now_msec - start_msec));
                arcus_exit(zh, EX_TEMPFAIL);
                /* Do we have to kill the slave too?  Or, only the master?
                 * Also, make this fail-stop behavior a runtime option.
                 * We may not want to kill the master even if it's partitioned
                 * from ZK?  FIXME
                 */
            }
        }
        else {
            /* Still connected to ZK */
            start_msec = 0;
        }
    }
    return NULL;
}
#endif /* ENABLE_SUICIDE_UPON_DISCONNECT */

static int
sm_init(void)
{
    pthread_attr_t attr;
    char zpath[200];

    memset(&sm_info, 0, sizeof(sm_info));

    /* Do not know whether this server is the master or the slave. */
    sm_info.state = SM_STATE_UNKNOWN;

    /* /root/cache_server_group/{svc}/{group}/lock */
    snprintf(zpath, sizeof(zpath), "%s/%s/%s/%s/lock",
        zk_root, zk_group_path, arcus_conf.svc, arcus_conf.groupname);
    sm_info.lock_dir_path = strdup(zpath);

    snprintf(zpath, sizeof(zpath), "%s^S^%s",
        arcus_conf.groupname, arcus_conf.zk_path);
    sm_info.slave_name = strdup(zpath);

    snprintf(zpath, sizeof(zpath), "%s^M^%s",
        arcus_conf.groupname, arcus_conf.zk_path);
    sm_info.master_name = strdup(zpath);

    pthread_mutex_init(&sm_info.lock, NULL);
    pthread_cond_init(&sm_info.cond, NULL);
    sm_info.notification = false;
    sm_info.running = true;
    pthread_attr_init(&attr);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&sm_info.tid, &attr, sm_thread, NULL);

#ifdef ENABLE_SUICIDE_UPON_DISCONNECT
    pthread_attr_init(&attr);
    pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
    pthread_create(&sm_info.timer_tid, &attr, sm_timer_thread, NULL);
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

static void
add_master_znode_blocking(void)
{
    bool done = false;
    int rc;
    char zpath[200];
    char *sbuf = "0";

    while (!done) {
        /* Create the znode for this server.
         * name=group^M^ip:port-hostname
         */
        snprintf(zpath, sizeof(zpath), "%s/%s^M^%s", arcus_conf.cluster_path,
            arcus_conf.groupname, arcus_conf.zk_path);
        rc = zoo_create(zh, zpath, sbuf, strlen(sbuf), &ZOO_OPEN_ACL_UNSAFE,
            ZOO_EPHEMERAL, NULL, 0);
        if (rc == ZNODEEXISTS) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Creating the master znode.  The znode for this server already"
                " exists in the cache_list directory. It is okay."
                " path=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
            done = true;
        }
        else if (rc == ZNOTHING || rc == ZSESSIONMOVED ||
            rc == ZSESSIONEXPIRED || rc == ZOPERATIONTIMEOUT ||
            rc == ZCONNECTIONLOSS) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Creating the master znode. Failed to create the znode"
                " in the cache_list directory. Will try again..."
                " path=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
        }
        else if (rc != ZOK) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Creating the master znode. Failed to create the znode"
                " in the cache_list directory with an unexpected error."
                " Terminating... path=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
            arcus_exit(zh, EX_OK /* FIXME */);
        }
        else {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Created the master znode. path=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
            done = true;
        }

        /* If we get here and sleep, clients would start seeing timeouts.
         * So, the above call needs to succeed in the first attempt for
         * graceful failover to be really, well, graceful.
         */
        if (!done)
            usleep(200000); /* Try again 200msec.  FIXME magic number */
    }
}

static void *
add_master_znode_thread(void *arg)
{
    int *complete = arg;
    add_master_znode_blocking();
    *complete = 1;
    return NULL;
}

/* Create the master znode in the cache list.  Do not return until we know
 * for sure that it succeeded or failed.  For error handling, see
 * sm_become_master.  During graceful failover, the slave calls this function
 * to add itself as the new master in the cache list.
 */
void
arcus_zk_add_master_znode(int *async_complete)
{
    if (async_complete) {
        pthread_attr_t attr;
        pthread_t tid;
        pthread_attr_init(&attr);
        pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM);
        pthread_create(&tid, &attr, add_master_znode_thread, async_complete);
    }
    else {
        add_master_znode_blocking();
        *async_complete = 1;
    }
}

/* Remove the master znode from the cache list.  Do not return until we know
 * it succeeded or failed.  During graceful failover, the master calls this
 * function to remove itself from the cache list.
 */
void
arcus_zk_remove_master_znode(void)
{
    bool done = false;
    int rc;
    char zpath[200];

    while (!done) {
        snprintf(zpath, sizeof(zpath), "%s/%s^M^%s", arcus_conf.cluster_path,
            arcus_conf.groupname, arcus_conf.zk_path);
        rc = zoo_delete(zh, zpath, -1);
        if (rc == ZNONODE) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Deleting the master znode. It does not exist. This is okay."
                " path=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
            done = true;
        }
        else if (rc == ZNOTHING || rc == ZSESSIONMOVED ||
            rc == ZSESSIONEXPIRED || rc == ZOPERATIONTIMEOUT ||
            rc == ZCONNECTIONLOSS) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Deleting the master znode. Failed to delete the znode"
                " in the cache_list directory. Will try again..."
                " path=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
        }
        else if (rc != ZOK) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Deleting the master znode. Failed to delete the znode"
                " in the cache_list directory with an unexpected error."
                " Terminating... path=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));
            arcus_exit(zh, EX_OK /* FIXME */);
        }
        else
            done = true;

        if (!done)
            usleep(200000); /* Try again 200msec.  FIXME magic number */
    }
}

#ifdef ENABLE_HEART_BEAT_THREAD
static void *
hb_thread(void *arg)
{
    app_ping_t *ping_data = arg;
    int failed = 0;
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
     * arcus_zk_shutdown = true indicates this case.  So, we check that flag
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
        if (failed == 0)
            sleep(HEART_BEAT_PERIOD);

        /* Graceful shutdown was requested while sleeping. */
        if (arcus_zk_shutdown)
            break;

        /* Do one ping and measure how long it takes. */
        gettimeofday(&start_time, NULL);
        mc_hb((zhandle_t *)NULL, (void *)ping_data);
        gettimeofday(&end_time, NULL);

        /* Paranoid.  Check if the clock had gone backwards. */
        start_msec = start_time.tv_sec * 1000 + start_time.tv_usec / 1000;
        end_msec = end_time.tv_sec * 1000 + end_time.tv_usec / 1000;
        if (end_msec >= start_msec) {
            elapsed_msec = end_msec - start_msec;
        }
        else {
            elapsed_msec = 0; /* Ignore this failure */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "hb_thread: Clock has gone backwards? start_msec=%llu"
                " end_msec=%llu\n", (unsigned long long)start_msec,
                (unsigned long long)end_msec);
        }

        /* If the heartbeat took longer than 1 second, leave a log message.
         * Local heartbeat should take on the order of microseconds.  We want
         * to see how often it takes unexpectedly long, independently of
         * failure detection.
         */
        if (elapsed_msec > HEART_BEAT_WARN_TIMEOUT && !arcus_zk_shutdown) {
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat took longer than expected. elapsed_msec=%llu\n",
                (unsigned long long)elapsed_msec);
        }

        if (elapsed_msec > HEART_BEAT_TIMEOUT) {
            failed++;

            /* Graceful shutdown was requested while doing heartbeat.
             * When worker threads die, there is no one to process heartbeat
             * request, and we would see a timeout.  Ignore the false alarm.
             */
            if (arcus_zk_shutdown)
                break;

            /* Print a message for every failure to help debugging, postmortem
             * analysis, etc.
             */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat failure. elapsed_msec=%llu consecutive_fail=%d\n",
                (unsigned long long)elapsed_msec, failed);

            if (failed >= HEART_BEAT_TRY_COUNT) {
                /* Do not bother calling memcached.c:shutdown_server.
                 * Call arcus_shutdown directly.  shutdown_server just sets
                 * memcached_shutdown = arcus_zk_shutdown = 1 and calls
                 * arcus_shutdown.  These flags are used for graceful shutdown.
                 * We do not need them here.  Besides, arcus_shutdown kills
                 * the process right away, assuming the zookeeper library
                 * still functions.
                 */
                /*
                  SERVER_HANDLE_V1 *api = arcus_conf.get_server_api();
                  api->core->shutdown();
                */
                arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                    "%d consecutive heartbeat failures. Shutting down...\n",
                    failed);
                arcus_shutdown("Heartbeat failures");
                /* Does not return */
            }
        }
        else {
            /* Reset the failure counter */
            failed = 0;
        }
    }
    return NULL;
}

static void
start_hb_thread(app_ping_t *data)
{
    pthread_t tid;
    int ret;
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    if ((ret = pthread_create(&tid, &attr, hb_thread, (void *)data)) != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Cannot create hb thread: %s\n", strerror(ret));
        arcus_shutdown("start_hb_thread");
    }
}
#endif // ENABLE_HEART_BEAT_THREAD
#endif  // ENABLE_ZK_INTEGRATION
