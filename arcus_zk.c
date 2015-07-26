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

static const char *zk_root = NULL;
static const char *zk_map_path   = "cache_server_mapping";
static const char *zk_log_path   = "cache_server_log";
static const char *zk_cache_path = "cache_list";
static const char *mc_hb_cmd     = "set arcus:zk-ping 1 0 1\r\n1\r\n";

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
    char    *mc_ipport;         // this memcached ip:port string
    char    *hostip;            // localhost server IP
    int     port;               // memcached port number
    int     hb_failstop;        // memcached heartbeat failstop
    int     hb_timeout;         // memcached heartbeat timeout
    int     zk_timeout;         // Zookeeper session timeout
    char    *zk_path;           // Ephemeral ZK path for this mc identification
    int     zk_path_ver;        // Zookeeper path version
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
    .zk_path        = NULL,
    .zk_path_ver    = -1,
    .verbose        = -1,
    .port           = -1,
    .maxbytes       = -1,
    .logger         = NULL,
    .cluster_path   = NULL,
#ifdef ENABLE_CLUSTER_AWARE
    .ch             = NULL,
#endif
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
#ifdef ENABLE_CLUSTER_AWARE
static void arcus_cluster_watcher(zhandle_t *zh, int type, int state,
                                  const char *path, void *ctx);
#endif
static void arcus_zk_sync_cb(int rc, const char *name, const void *data);
static void arcus_zk_log(zhandle_t *zh, const char *);
static void arcus_exit(zhandle_t *zh, int err);

int  mc_hb(zhandle_t* zh, void *context);     // memcached self-heartbeat

// async routine synchronization
static pthread_cond_t  azk_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t azk_mtx  = PTHREAD_MUTEX_INITIALIZER;
static int             azk_count;

// zookeeper close synchronization
static pthread_mutex_t zk_close_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile bool   zk_watcher_running = false;

// memcached heartheat thread */
static volatile bool hb_thread_running = false;
static bool hb_thread_sleep = false;
static pthread_mutex_t hb_thread_lock;
static pthread_cond_t  hb_thread_cond;
static void *hb_thread(void *arg);
static int start_hb_thread(app_ping_t *data);

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
    }
}

#ifdef ENABLE_CLUSTER_AWARE
static void
arcus_cluster_watch_servers(zhandle_t *zh, const char *path, watcher_fn fn)
{
    struct String_vector result;
    int i, rc;

    /* Do not call zookeeper API if we are calling
     * zookeeper_close.  See arcus_zk_final.
     */
    if (arcus_zk_shutdown)
        return;

    zk_watcher_running = true;

    // get cache list
    rc = zoo_wget_children(zh, path, arcus_cluster_watcher, NULL, &result);
    if (rc == ZOK) {
        // update the cache list
        if (arcus_conf.verbose > 0) {
            for (i=0; i<result.count; i++) {
                arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                                       "server[%d] = %s\n", i, result.data[i]);
            }
        }
        cluster_config_reconfigure(arcus_conf.ch, result.data, result.count);
        deallocate_String_vector(&result);
    } else {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "Could not get children from ZK. errno=%d\n", rc);
    }

    zk_watcher_running = false;
}

static void
arcus_cluster_watcher(zhandle_t *zh, int type, int state, const char *path, void *ctx)
{
    if (type == ZOO_CHILD_EVENT) {
        arcus_cluster_watch_servers(zh, path, arcus_cluster_watcher);
    }
    else if (type == ZOO_SESSION_EVENT) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "ZOO_SESSION_EVENT from ZK: state=%d\n", state);
    }
    else { // an unexpected event has occurred
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "An unexpected event from ZK: type=%d, state=%d\n", type, state);
    }
}
#endif

// callback for sync command
static void
arcus_zk_sync_cb(int rc, const char *name, const void *data)
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
    snprintf(zpath, sizeof(zpath), "%s/%s/%s", zk_root, zk_log_path, sbuf);
    rc = zoo_exists(zh, zpath, ZK_NOWATCH, &zstat);

    // if this "date" directory does not exist, create one
    if (rc == ZNONODE) {
        rc = zoo_create(zh, zpath, NULL, 0, &ZOO_OPEN_ACL_UNSAFE,
                        0, rcbuf, sizeof(rcbuf));
        if (rc == ZOK && arcus_conf.verbose > 2) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG,
                    NULL, "znode %s does not exist. created\n", rcbuf);
        }
    }

    // if this znode already exist or the creation was successful
    if (rc == ZOK || rc == ZNODEEXISTS) {
        snprintf(zpath, sizeof(zpath), "%s/%s/%s/%s-",
                 zk_root, zk_log_path, sbuf, arcus_conf.zk_path);
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
                "hb_thread: Clock has gone backwards? start_msec=%llu"
                " end_msec=%llu\n", (unsigned long long)start_msec,
                (unsigned long long)end_msec);
        }

        azk_stat.hb_count += 1;
        azk_stat.hb_latency += elapsed_msec;

        if (elapsed_msec > cur_hb_timeout) {
            acc_hb_latency += elapsed_msec;
            /* Print a message for every failure to help debugging, postmortem
             * analysis, etc.
             */
            arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat failure. hb_timeout=%d hb_latency=%llu "
                "accumulated_hb_latency=%d\n", cur_hb_timeout,
                (unsigned long long)elapsed_msec, acc_hb_latency);

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
        arcus_zk_shutdown = true;
        arcus_zk_final("Heartbeat failure");
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
        arcus_conf.logger->log(EXTENSION_LOG_DETAIL, NULL, "local public IP: %s\n", arcus_conf.hostip);
    }

    if (!arcus_conf.zk_path) {
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
        arcus_conf.zk_path = strdup(rcbuf);
    }
    return 0; // EX_OK
}

static int arcus_get_service_code(zhandle_t *zh, const char *root)
{
    struct String_vector strv = {0, NULL};
    char zpath[200];
    int  rc;

    // sync map path
    snprintf(zpath, sizeof(zpath), "%s/%s", root, zk_map_path);
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
             root, zk_map_path, arcus_conf.mc_ipport);
    rc = zoo_get_children(zh, zpath, ZK_NOWATCH, &strv);
    if (rc == ZNONODE) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Cannot find the server mapping. zpath=%s error=%d(%s)\n",
                zpath, rc, zerror(rc));

        // Second check: get children of "/cache_server_mapping/ip"
        snprintf(zpath, sizeof(zpath), "%s/%s/%s",
                 root, zk_map_path, arcus_conf.hostip);
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

    // get the first one. we could have more than one (meaning one server participating in
    // more than one service in the cluster: not likely though)
    arcus_conf.svc = strdup(strv.data[0]);
    if (arcus_conf.svc == NULL) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to allocate service code string\n");
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
    char        rcbuf[200];
    struct Stat zstat;

    snprintf(zpath, sizeof(zpath), "%s/%s",
             arcus_conf.cluster_path, arcus_conf.zk_path);
    snprintf(value, sizeof(value), "%ldMB", (long)arcus_conf.maxbytes/1024/1024);
    rc = zoo_create(zh, zpath, value, strlen(value), &ZOO_OPEN_ACL_UNSAFE,
                    ZOO_EPHEMERAL, rcbuf, sizeof(rcbuf));
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "cannot create znode %s (%s error)\n", zpath, zerror(rc));
        if (rc == ZNODEEXISTS) {
            arcus_conf.logger->log( EXTENSION_LOG_DETAIL, NULL,
                    "old session still exist. Wait a bit\n");
        }
        return -1;
    } else {
        if (arcus_conf.verbose > 2) {
            arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "Joined Arcus cloud at %s\n", rcbuf);
        }
    }

    rc = zoo_exists(zh, zpath, 0, &zstat);
    if (rc) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                "cannot find %s just created (%s error)\n", rcbuf, zerror(rc));
        return -1;
    }

    // store this version number, just in case we restart
    arcus_conf.zk_path_ver = zstat.version;
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
    app_ping_t      *ping_data;

    assert(logger);
    assert(engine);

    if (!ensemble_list) { // Arcus Zookeeper Ensemble IP list
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    " -z{ensemble_list} must not be empty\n");
        arcus_exit(zh, EX_USAGE);
    }

    // save these for later use (restart)
    if (!arcus_conf.init) {
        arcus_conf.port          = port;    // memcached listen port
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

    /* initialize Arcus ZK stats */
    azk_stat.zk_timeout = 0;
    azk_stat.hb_count = 0;
    azk_stat.hb_latency = 0;

    if (arcus_conf.verbose > 2)
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "-> arcus_zk_init\n");

    /* prepare app_ping context data */
    arcus_prepare_ping_context(&mc_ping_context, arcus_conf.port);
    ping_data = &mc_ping_context;

    /* make znode name while getting local ip and hostname */
    rc = arcus_build_znode_name(ensemble_list);
    if (rc != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                               "arcus_build_znode_name() failed\n");
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

    // connect to ZK ensemble
    snprintf(zpath, sizeof(zpath), "%s", arcus_conf.ensemble_list);
    if (arcus_conf.verbose > 2)
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL, "zk ensemble: %s\n", zpath);
    inc_count(1);
    zh = zookeeper_init(zpath, arcus_zk_watcher, arcus_conf.zk_timeout, &myid, 0, 0);
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

    /* zk root node */
    zk_root = "/arcus";

    /* Retrieve the service code for this cache node */
    if (arcus_get_service_code(zh, zk_root) != 0) {
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
                 "Failed to get the service code for this cache node.\n");
        arcus_exit(zh, EX_PROTOCOL);
    }
    assert(arcus_conf.svc != NULL);

    snprintf(zpath, sizeof(zpath), "%s/%s/%s", zk_root, zk_cache_path, arcus_conf.svc);
    arcus_conf.cluster_path = strdup(zpath);
    assert(arcus_conf.cluster_path);

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
    arcus_conf.ch = cluster_config_init(arcus_conf.mc_ipport, strlen(arcus_conf.mc_ipport),
                                        arcus_conf.logger, arcus_conf.verbose);
    assert(arcus_conf.ch);

    // set a watch to the cache list this memcached belongs in
    // (e.g. /arcus/cache_list/a_cluster)
    arcus_cluster_watch_servers(zh, arcus_conf.cluster_path, arcus_cluster_watcher);

    arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL,
        "Memcached is watching Arcus cache cloud for \"%s\"\n", arcus_conf.cluster_path);
#endif

    // start heartbeat thread
    if (start_hb_thread(ping_data) != 0) {
        /* We need normal shutdown at this time */
        arcus_zk_shutdown = true;
    }

    // Either got SIG* or memcached shutdown process finished
    if (arcus_zk_shutdown) {
        arcus_zk_final("Interrupted");
    }
}

/* Close the ZK connection and die. Do not do anything that may
 * block here such as deleting the ephemeral znode.
 * It will be deleted, after closing the ZK connection(or session).
 */
void arcus_zk_final(const char *msg)
{
    arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL,
                           "Arcus memcached shutting down - %s\n", msg);

    assert(arcus_zk_shutdown == true);

    pthread_mutex_lock(&zk_close_lock);
    if (zh) {
        int elapsed_msec;

        /* hb_thread is probably sleeping.  And, if it is in the middle of
         * doing a ping, it would block for a long time because worker threads
         * are likely all dead at this point.
         */
        /* wake up the heartbeat thread if it's sleeping */
        hb_wakeup();

        /* wait a maximum of 1000 msec */
        elapsed_msec = 0;
        while (elapsed_msec <= 1000) {
            if (elapsed_msec == 0) {
                /* go below */
            } else {
                usleep(10000); // 10ms wait
                elapsed_msec += 10;
            }
            /* Do not call zookeeper_close (arcus_exit) and
             * zoo_wget_children (arcus_cluster_watch_servers) at the same time.
             * Otherwise, this thread (main thread) and the watcher thread
             * (zoo_wget_children) may hang forever until the user manually kills
             * the process.  The root cause is within the zookeeper library.  Its
             * handling of concurrent API calls like zoo_wget_children and
             * zookeeper_close still has holes...
             */
            /* If the watcher thread (zoo_wget_children) is running now, wait for
             * one second.  Either it completes, or it at least registers
             * sync_completion and blocks.  In the latter case, zookeeper_close
             * aborts all sync_completion's, which then wakes up the blocking
             * watcher thread.  zoo_wget_children returns an error.
             */
            if (zk_watcher_running)
                continue;

            if (hb_thread_running)
                continue;

            arcus_conf.logger->log(EXTENSION_LOG_INFO, NULL, "zk threads terminated\n");
            break;
        }

        /* close zk connection */
        zookeeper_close(zh);
        zh = NULL;
        arcus_conf.logger->log(EXTENSION_LOG_WARNING, NULL, "zk connection closed\n");
    }
    pthread_mutex_unlock(&zk_close_lock);
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

int arcus_zk_get_ensemble_str(char *buf, int size)
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
    if (hbtimeout < HEART_BEAT_MIN_TIMEOUT ||
        hbtimeout > HEART_BEAT_MAX_TIMEOUT)
        return -1;

    /* Check: heartbeat timeout <= heartbeat failstop */
    if (hbtimeout > arcus_conf.hb_failstop)
        return -1;

    if (hbtimeout != arcus_conf.hb_timeout) {
        arcus_conf.hb_timeout = hbtimeout;
    }
    return 0;
}

int arcus_zk_get_hbtimeout(void)
{
    assert(arcus_conf.hb_timeout >= HEART_BEAT_MIN_TIMEOUT &&
           arcus_conf.hb_timeout <= HEART_BEAT_MAX_TIMEOUT);
    return arcus_conf.hb_timeout;
}

int arcus_zk_set_hbfailstop(int hbfailstop)
{
    if (hbfailstop < HEART_BEAT_MIN_FAILSTOP ||
        hbfailstop > HEART_BEAT_MAX_FAILSTOP)
        return -1;

    /* Check: heartbeat failstop >= heartbeat timeout */
    if (hbfailstop < arcus_conf.hb_timeout)
        return -1;

    if (hbfailstop != arcus_conf.hb_failstop) {
        arcus_conf.hb_failstop = hbfailstop;
    }
    return 0;
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
bool arcus_cluster_is_valid()
{
    return cluster_config_is_valid(arcus_conf.ch);
}

bool arcus_key_is_mine(const char *key, size_t nkey)
{
    uint32_t key_id, self_id;
    bool     mine;

    mine = cluster_config_key_is_mine(arcus_conf.ch, key, nkey,
                                      &key_id, &self_id);
    if (arcus_conf.verbose > 2) {
        arcus_conf.logger->log(EXTENSION_LOG_DEBUG, NULL,
                "key=%s, self_id=%d, key_id=%d, mine=%s\n",
                key, self_id, key_id, (mine)?"true":"false");
    }
    return mine;
}

uint32_t arcus_gen_ketama_hash(const char *key, size_t nkey, int *hashidx)
{
    return cluster_config_ketama_hash(arcus_conf.ch, key, nkey, hashidx);
}
#endif

#endif  // ENABLE_ZK_INTEGRATION
