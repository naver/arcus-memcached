/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015-2021 JaM2in Co., Ltd.
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
#include "config.h"
#include "arcus_hb.h"
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

#ifdef ENABLE_ZK_INTEGRATION

/* admin client ip. It is also defined in memcached.h */
#define ADMIN_CLIENT_IP "127.0.0.1"

/* One heartbeat every 3 seconds. */
#define MC_HB_PERIOD 3 /* sec */

/* If hearbeat takes more than timeout msec, consider it failed.  */
#define MC_HB_TIMEOUT_MIN 50     /* msec */
#define MC_HB_TIMEOUT_MAX 300000 /* msec */
#define MC_HB_TIMEOUT_DFT 10000  /* msec */

/* If consecutive heartbeats fail, consider memcached dead and commit
 * suicide.  If the accumulated latency of consecutive heartbeats is
 * over the following HEART_BEAT_FAILSTOP value, it does fail-stop.
 */
#define MC_HB_FAILSTOP_MIN 3000   /* msec */
#define MC_HB_FAILSTOP_MAX 300000 /* msec */
#define MC_HB_FAILSTOP_DFT 60000  /* msec */

/* context struct to pass to app_ping */
typedef struct {
    struct sockaddr_in addr; /* cache this for use in L7 ping routine */
    struct timeval     to;   /* mc_hb may block for up to this long (give or take) */
} app_ping_t;

typedef struct {
    pthread_mutex_t lock;
    void (*shutdown)(void);
    char *command; /* heartbeat command string */
    int cmdleng;   /* heartbeat command length */
    int port;      /* memcached port */
    int timeout;   /* memcached heartbeat timeout */
    int failstop;  /* memcached heartbeat failstop */
} hb_config_t;

/* Arcus heartbeat config */
static hb_config_t hb_conf;

/* Arcus heartbeat stats */
static arcus_hb_stats hb_stat;

/* memcached heartheat thread */
static volatile bool hb_thread_running = false;
static volatile bool hb_thread_stopreq = false;
static bool hb_thread_sleep = false;
static pthread_mutex_t hb_thread_lock;
static pthread_cond_t  hb_thread_cond;

/* logger */
static EXTENSION_LOGGER_DESCRIPTOR *hb_logger = NULL;

/*
 * Arcus heartbeat static functions
 */
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

    arcus_adjust_ping_timeout(ping_data, hb_conf.timeout);
}

/* This is L7 application ping callback
 * only one app ping per ZK ping period if successful
 * In this case, we make a TCP connection to self memcached port, and
 * try to set a key.
 *
 * Make sure that successful app heartbeat completes in 2/3 of recv timeout,
 * otherwise it is very possible have expired state in the end.
 * This retries 2 twice at every 1/3 of ZK recv timeout
 */
static int mc_hb(void *context)
{
    app_ping_t *data = (app_ping_t *) context;
    struct linger linger;
    struct timeval tv_timeo;
    char buf[32];
    int sock;
    int flags;
    int err=0;

    /* make a tcp connection to this memcached itself,
     * and try "set arcus:zk-ping".
     */
    sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    if (sock == -1) {
        hb_logger->log(EXTENSION_LOG_WARNING, NULL,
            "mc_hb: cannot create a socket (error=%s)\n", strerror(errno));
        return 0; /* Allow ZK ping by returning 0 even if socket() fails. */
    }

    flags = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags)); /* fast port recycle */
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags));

    /* send()/recv() could be blocked forever when all memcached worker threads are deadlocked
     * while the acceptor thread is still alive.
     * Blocked in L7 health check, ZK ping fails, then ZK ensemble gives up the connection.
     * However, as the blocked ZK ping runs on ZK client's I/O thread
     * the client cannot receive the events from ZK ensemble. So memcached fails to fail-stop.
     * To prevent this situation we set timeouts to send()/recv().
     */
    tv_timeo = data->to;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv_timeo, sizeof(tv_timeo));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv_timeo, sizeof(tv_timeo));

    linger.l_onoff  = 1;
    linger.l_linger = 0; /* flush buffers upon close() and send TCP RST */
    setsockopt(sock, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));

    err = connect(sock, (struct sockaddr *) &data->addr, sizeof(struct sockaddr));
    if (err) {
        hb_logger->log(EXTENSION_LOG_WARNING, NULL,
            "mc_hb: cannot connect to local memcached (error=%s)\n",
            strerror(errno));
        close(sock);
        return 0; /* Allow ZK ping by returning 0 even if socket() fails. */
    }

    /* Try to set a key "arcus:zk-ping"
     * we need to be careful here. Since we can make a connection, basic
     * memcached event loop works and system resources are enough. if we can an
     * error during send()/recv(), it may not be a memcached failure at all.
     * We may get slab memory shortage for slab class 0 for above key.
     * For now, we simply return here without ping error or intentional delay
     */
    err = send(sock, hb_conf.command, hb_conf.cmdleng, 0);
    if (err > 0) {
        err = recv(sock, buf, 8, 0); /* expects "STORED\r\n" */
        if (err < 0) {
            hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                "mc_hb: recv failed (error=%s)\n", strerror(errno));
        }
    } else {
        if (err < 0) {
            hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                "mc_hb: send failed (error=%s)\n", strerror(errno));
        }
    }
    close(sock);
    return 0;
}

static void hb_thread_sleep_interval(int interval)
{
    struct timeval  tv;
    struct timespec ts;

    gettimeofday(&tv, NULL);
    ts.tv_sec  = tv.tv_sec + interval;
    ts.tv_nsec = tv.tv_usec * 1000;

    pthread_mutex_lock(&hb_thread_lock);
    hb_thread_sleep = true;
    pthread_cond_timedwait(&hb_thread_cond, &hb_thread_lock, &ts);
    hb_thread_sleep = false;
    pthread_mutex_unlock(&hb_thread_lock);
}

static void hb_thread_wakeup(void)
{
  pthread_mutex_lock(&hb_thread_lock);
  if (hb_thread_sleep == true) {
      pthread_cond_signal(&hb_thread_cond);
  }
  pthread_mutex_unlock(&hb_thread_lock);
}

static void *hb_thread_main(void *arg)
{
    static app_ping_t ping_context;
    int cur_hb_timeout = hb_conf.timeout;
    int cur_hb_failstop = hb_conf.failstop;
    int acc_hb_latency = 0;
    bool shutdown_by_me = false;
    struct timeval bgn_time;
    struct timeval end_time;
    uint64_t bgn_msec, end_msec;
    uint64_t elapsed_msec;

    hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                   "Heartbeat thread is running.\n");

    /* prepare ping context */
    arcus_prepare_ping_context(&ping_context, hb_conf.port);

    /* We consider 3 types of shutdowns.
     *
     * 1. control-c
     * The user wants a graceful shutdown.  The main thread wakes up all
     * the worker threads and wait for them to terminate.  It also calls
     * the engine destroy function.  In this case, heartbeat should just stop.
     * hb_thread_stopreq = true indicates this case.  So, we check that flag
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
    hb_thread_running = true;
    while (hb_thread_stopreq == false) {
        /* If the last hearbeat timed out, do not wait and try right away. */
        if (acc_hb_latency == 0) {
            hb_thread_sleep_interval(MC_HB_PERIOD);
            if (hb_thread_stopreq) break;
        }

        /* check if hb_timeout and hb_failstop are changed */
        if (cur_hb_timeout != hb_conf.timeout) {
            hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat timeout has changed. old(%d) => new(%d)\n",
                cur_hb_timeout, hb_conf.timeout);
            cur_hb_timeout = hb_conf.timeout;
            acc_hb_latency = 0; /* reset accumulated hb latency */
            /* adjust ping timeout */
            arcus_adjust_ping_timeout(&ping_context, cur_hb_timeout);
        }
        if (cur_hb_failstop != hb_conf.failstop) {
            hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat failstop has changed. old(%d) => new(%d)\n",
                cur_hb_failstop, hb_conf.failstop);
            cur_hb_failstop = hb_conf.failstop;
            acc_hb_latency = 0; /* reset accumulated hb latency */
        }

        /* Do one ping and measure how long it takes. */
        gettimeofday(&bgn_time, NULL);
        mc_hb((void *)&ping_context);
        gettimeofday(&end_time, NULL);

        /* Paranoid.  Check if the clock had gone backwards. */
        bgn_msec = bgn_time.tv_sec * 1000 + bgn_time.tv_usec / 1000;
        end_msec = end_time.tv_sec * 1000 + end_time.tv_usec / 1000;
        if (bgn_msec <= end_msec) {
            elapsed_msec = end_msec - bgn_msec;
            hb_stat.count += 1;
            hb_stat.latency += elapsed_msec;
        } else {
            elapsed_msec = 0; /* Ignore this heartbeat */
            hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                "hb_thread: Clock has gone backwards? "
                "bgn_msec=%"PRIu64" end_msec=%"PRIu64"\n",
                bgn_msec, end_msec);
        }

        if (elapsed_msec < cur_hb_timeout) {
            /* Reset the acc_hb_latency */
            acc_hb_latency = 0;
        } else {
            /* Print a message for every failure to help debugging,
             * postmortem analysis, etc.
             */
            hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Heartbeat failure. hb_timeout=%d hb_latency=%"PRIu64
                " accumulated_hb_latency=%d\n", cur_hb_timeout,
                elapsed_msec, acc_hb_latency);

            if (cur_hb_failstop > 0) {
                acc_hb_latency += elapsed_msec;
            } else {
                acc_hb_latency = 0; /* Reset acc_hb_latency */
            }
            if (acc_hb_latency > cur_hb_failstop) {
                hb_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "consecutive heartbeat failures. Shutting down...\n");
                shutdown_by_me = true; break;
            }
        }
    }
    hb_thread_running = false;

    if (shutdown_by_me) {
        /* It calls shutdown_server() in memcached.c */
        hb_conf.shutdown();
    }
    return NULL;
}

/*
 * Arcus heartbeat extern functions
 */
int arcus_hb_init(int port, EXTENSION_LOGGER_DESCRIPTOR *logger,
                  void (*cb_shutdown_server)(void))
{
    pthread_t tid;
    pthread_attr_t attr;
    int ret;

    /* init hb_logger */
    hb_logger = logger;

    /* init hb_config */
    pthread_mutex_init(&hb_conf.lock, NULL);
    hb_conf.shutdown = cb_shutdown_server;
    hb_conf.command = "set arcus:zk-ping 1 0 1\r\n1\r\n";
    hb_conf.cmdleng = strlen(hb_conf.command);
    hb_conf.port = port;
    hb_conf.timeout = MC_HB_TIMEOUT_DFT,
    hb_conf.failstop = MC_HB_FAILSTOP_DFT,

    /* init hb stat */
    memset(&hb_stat, 0, sizeof(hb_stat));

    /* start heartbeat thread */
    pthread_attr_init(&attr);
    pthread_mutex_init(&hb_thread_lock, NULL);
    pthread_cond_init(&hb_thread_cond, NULL);

    ret = pthread_create(&tid, &attr, hb_thread_main, NULL);
    if (ret != 0) {
        hb_logger->log(EXTENSION_LOG_WARNING, NULL,
            "Cannot create mc heartbeart thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void arcus_hb_final(void)
{
    /* hb_thread is probably sleeping.  And, if it is in the middle of
     * doing a ping, it would block for a long time because worker threads
     * are likely all dead at this point.
     */
    hb_thread_stopreq = true;
    hb_thread_wakeup();

    /* wait a maximum of 1000 msec */
    int elapsed_msec = 0;
    while (hb_thread_running) {
        usleep(10000); // 10ms wait
        elapsed_msec += 10;
        if (elapsed_msec > 1000)
            break;
    }
}

int arcus_hb_get_timeout(void)
{
    return hb_conf.timeout;
}

int arcus_hb_set_timeout(int timeout)
{
    int ret=0;

    if (timeout < MC_HB_TIMEOUT_MIN || timeout > MC_HB_TIMEOUT_MAX) {
        return -1;
    }

    pthread_mutex_lock(&hb_conf.lock);
    if (hb_conf.failstop > 0) {
        /* Check: heartbeat timeout <= heartbeat failstop */
        if (timeout > hb_conf.failstop) {
            ret = -1;
        }
    }
    if (ret == 0) {
        if (timeout != hb_conf.timeout) {
            hb_conf.timeout = timeout;
        }
    }
    pthread_mutex_unlock(&hb_conf.lock);

    return ret;
}

int arcus_hb_get_failstop(void)
{
    return hb_conf.failstop;
}

int arcus_hb_set_failstop(int failstop)
{
    int ret=0;

    if (failstop < MC_HB_FAILSTOP_MIN || failstop > MC_HB_FAILSTOP_MAX) {
        if (failstop != 0)
            return -1;
    }

    pthread_mutex_lock(&hb_conf.lock);
    if (failstop > 0) {
        /* Check: heartbeat failstop >= heartbeat timeout */
        if (failstop < hb_conf.timeout) {
            ret = -1;
        }
    }
    if (ret == 0) {
        if (failstop != hb_conf.failstop) {
            hb_conf.failstop = failstop;
        }
    }
    pthread_mutex_unlock(&hb_conf.lock);

    return ret;
}

void arcus_hb_get_stats(arcus_hb_stats *stats)
{
    stats->count = hb_stat.count;
    stats->latency = hb_stat.latency;
}

void arcus_hb_get_confs(arcus_hb_confs *confs)
{
    confs->timeout = hb_conf.timeout;
    confs->failstop = hb_conf.failstop;
}
#endif
