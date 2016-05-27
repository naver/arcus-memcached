/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2015 JaM2in Co., Ltd.
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
 *  memcached - memory caching daemon
 *
 *       http://www.danga.com/memcached/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */
#include "config.h"
#include "memcached.h"
#include "memcached/extension_loggers.h"
#ifdef ENABLE_ZK_INTEGRATION
#include "arcus_zk.h"
#endif

#if defined(ENABLE_SASL) || defined(ENABLE_ISASL)
#define SASL_ENABLED
#endif

#define ZK_CONNECTIONS 1

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <ctype.h>
#include <stdarg.h>
#include <stddef.h>

/* max collection size */
static int ARCUS_COLL_SIZE_LIMIT = 1000000;
static int MAX_LIST_SIZE  = 50000;
static int MAX_SET_SIZE   = 50000;
static int MAX_BTREE_SIZE = 50000;

/* The item must always be called "it" */
#define SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_stats[info.clsid].slab_op++;

#define THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    thread_stats->thread_op++;

#define THREAD_GUTS2(conn, thread_stats, slab_op, thread_op) \
    thread_stats->slab_op++; \
    thread_stats->thread_op++;

#define SLAB_THREAD_GUTS(conn, thread_stats, slab_op, thread_op) \
    SLAB_GUTS(conn, thread_stats, slab_op, thread_op) \
    THREAD_GUTS(conn, thread_stats, slab_op, thread_op)

#define STATS_INCR1(GUTS, conn, slab_op, thread_op, key, nkey) { \
    struct independent_stats *independent_stats = get_independent_stats(conn); \
    struct thread_stats *thread_stats = \
        &independent_stats->thread_stats[conn->thread->index]; \
    topkeys_t *topkeys = independent_stats->topkeys; \
    pthread_mutex_lock(&thread_stats->mutex); \
    GUTS(conn, thread_stats, slab_op, thread_op); \
    pthread_mutex_unlock(&thread_stats->mutex); \
    TK(topkeys, slab_op, key, nkey, current_time); \
}

#define STATS_INCR(conn, op, key, nkey) \
    STATS_INCR1(THREAD_GUTS, conn, op, op, key, nkey)

#define SLAB_INCR(conn, op, key, nkey) \
    STATS_INCR1(SLAB_GUTS, conn, op, op, key, nkey)

#define STATS_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(THREAD_GUTS2, conn, slab_op, thread_op, key, nkey)

#define SLAB_TWO(conn, slab_op, thread_op, key, nkey) \
    STATS_INCR1(SLAB_THREAD_GUTS, conn, slab_op, thread_op, key, nkey)

#define STATS_HIT(conn, op, key, nkey) \
    SLAB_TWO(conn, op##_hits, cmd_##op, key, nkey)

#define STATS_HITS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_hits, cmd_##op, key, nkey)

#define STATS_OKS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_oks, cmd_##op, key, nkey)

#define STATS_ELEM_HITS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_elem_hits, cmd_##op, key, nkey)

#define STATS_NONE_HITS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_none_hits, cmd_##op, key, nkey)

#define STATS_MISS(conn, op, key, nkey) \
    STATS_TWO(conn, op##_misses, cmd_##op, key, nkey)

#define STATS_NOKEY(conn, op) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    pthread_mutex_lock(&thread_stats->mutex); \
    thread_stats->op++; \
    pthread_mutex_unlock(&thread_stats->mutex); \
}

#define STATS_NOKEY2(conn, op1, op2) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    pthread_mutex_lock(&thread_stats->mutex); \
    thread_stats->op1++; \
    thread_stats->op2++; \
    pthread_mutex_unlock(&thread_stats->mutex); \
}

#define STATS_ADD(conn, op, amt) { \
    struct thread_stats *thread_stats = \
        get_thread_stats(conn); \
    pthread_mutex_lock(&thread_stats->mutex); \
    thread_stats->op += amt; \
    pthread_mutex_unlock(&thread_stats->mutex); \
}

#define GET_8ALIGN_SIZE(size) \
    (((size) % 8) == 0 ? (size) : ((size) + (8 - ((size) % 8))))

volatile sig_atomic_t memcached_shutdown=0;

/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;

/** exported globals **/
struct settings settings;
struct mc_stats mc_stats;
EXTENSION_LOGGER_DESCRIPTOR *mc_logger;

static union {
    ENGINE_HANDLE *v0;
    ENGINE_HANDLE_V1 *v1;
} mc_engine;

static time_t process_started;     /* when the process was started */

/* The size of string representing 4 bytes integer is 10. */
static int lenstr_size = 10;

/** file scope variables **/
static conn *listen_conn = NULL;
static struct event_base *main_base;
static struct independent_stats *default_independent_stats;

static struct engine_event_handler *engine_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];

#ifdef ENABLE_ZK_INTEGRATION
static char *arcus_zk_cfg = NULL;
#endif

#ifdef COMMAND_LOGGING
static bool cmdlog_in_use = false;
#endif

#ifdef DETECT_LONG_QUERY
static bool lqdetect_in_use = false;
#endif

/*
 * forward declarations
 */
static int new_socket(struct addrinfo *ai);
static int try_read_command(conn *c);
static inline struct independent_stats *get_independent_stats(conn *c);
static inline struct thread_stats *get_thread_stats(conn *c);

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occured (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

static enum try_read_result try_read_network(conn *c);
static enum try_read_result try_read_udp(conn *c);

/* stats */
static void stats_init(void);
static void server_stats(ADD_STAT add_stats, conn *c, bool aggregate);
static void process_stat_settings(ADD_STAT add_stats, void *c);

/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(const int fd, const short which, void *arg);
static bool update_event(conn *c, const int new_flags);
static void complete_nread(conn *c);
static void process_command(conn *c, char *command, int cmdlen);
static void write_and_free(conn *c, char *buf, int bytes);
static int ensure_iov_space(conn *c);
static int add_iov(conn *c, const void *buf, int len);
static int add_msghdr(conn *c);

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

static enum transmit_result transmit(conn *c);


/* time-sensitive callers can call it by hand with this,
 * outside the normal ever-1-second timer
 */
static void set_current_time(void)
{
    struct timeval timer;

    gettimeofday(&timer, NULL);
    current_time = (rel_time_t) (timer.tv_sec - process_started);
}

static rel_time_t get_current_time(void)
{
    return current_time;
}

#define REALTIME_MAXDELTA 60*60*24*30

/*
 * given time value that's either unix time or delta from current unix time,
 * return unix time. Use the fact that delta can't exceed one month
 * (and real time value can't be that low).
 */
static rel_time_t realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */
#ifdef ENABLE_STICKY_ITEM
    if (exptime == -1) return (rel_time_t)(-1);
#endif

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started)
            return (rel_time_t)1;
        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + current_time);
    }
}

static void stats_init(void) {
    mc_stats.daemon_conns = 0;
    mc_stats.rejected_conns = 0;
    mc_stats.quit_conns = 0;
    mc_stats.curr_conns = mc_stats.total_conns = mc_stats.conn_structs = 0;

    /* make the time we started always be 2 seconds before we really
       did, so time(0) - time.started is never zero.  if so, things
       like 'settings.oldest_live' which act as booleans as well as
       values are now false in boolean context... */
    process_started = time(0) - 2;
    stats_prefix_init();
}

static void stats_reset(const void *cookie) {
    struct conn *conn = (struct conn*)cookie;
    STATS_LOCK();
    mc_stats.rejected_conns = 0;
    mc_stats.quit_conns = 0;
    mc_stats.total_conns = 0;
    stats_prefix_clear();
    STATS_UNLOCK();
    threadlocal_stats_reset(get_independent_stats(conn)->thread_stats);
    mc_engine.v1->reset_stats(mc_engine.v0, cookie);
}

static void settings_init(void) {
    settings.use_cas = true;
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 11211;
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
    settings.maxconns = 1024;         /* to limit connections-related memory to about 5MB */
    settings.sticky_ratio = 0;        /* default: 0 */
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;       /* push old items out of cache when memory runs out */
    settings.socketpath = NULL;       /* by default, not using a unix socket */
    settings.factor = 1.25;
    settings.chunk_size = 48;         /* space for a modest key and value */
    settings.num_threads = 4;         /* N workers */
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.allow_detailed = true;
    settings.reqs_per_event = DEFAULT_REQS_PER_EVENT;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024; /* The famous 1MB upper limit. */
    settings.max_list_size = MAX_LIST_SIZE;
    settings.max_set_size = MAX_SET_SIZE;
    settings.max_btree_size = MAX_BTREE_SIZE;
    settings.topkeys = 0;
    settings.require_sasl = false;
    settings.extensions.logger = get_stderr_logger();
}

/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int add_msghdr(conn *c)
{
    struct msghdr *msg;

    assert(c != NULL);

    if (c->msgsize == c->msgused) {
        msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
        if (! msg)
            return -1;
        c->msglist = msg;
        c->msgsize *= 2;
    }

    msg = c->msglist + c->msgused;

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(msg, 0, sizeof(struct msghdr));

    msg->msg_iov = &c->iov[c->iovused];

    if (c->request_addr_size > 0) {
        msg->msg_name = &c->request_addr;
        msg->msg_namelen = c->request_addr_size;
    }

    c->msgbytes = 0;
    c->msgused++;

    if (IS_UDP(c->transport)) {
        /* Leave room for the UDP header, which we'll fill in later. */
        return add_iov(c, NULL, UDP_HEADER_SIZE);
    }

    return 0;
}

static const char *prot_text(enum protocol prot) {
    char *rv = "unknown";
    switch(prot) {
        case ascii_prot:
            rv = "ascii";
            break;
        case binary_prot:
            rv = "binary";
            break;
        case negotiating_prot:
            rv = "auto-negotiate";
            break;
    }
    return rv;
}

void safe_close(int sfd) {
    if (sfd != -1) {
        int rval;
        while ((rval = close(sfd)) == -1 &&
               (errno == EINTR || errno == EAGAIN)) {
            /* go ahead and retry */
        }

        if (rval == -1) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "Failed to close socket %d (%s)!!\n",
                           (int)sfd, strerror(errno));
        } else {
            STATS_LOCK();
            mc_stats.curr_conns--;
            STATS_UNLOCK();
        }
    }
}

// Register a callback.
static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb, const void *cb_data) {
    struct engine_event_handler *h =
        calloc(sizeof(struct engine_event_handler), 1);

    assert(h);
    h->cb = cb;
    h->cb_data = cb_data;
    h->next = engine_event_handlers[type];
    engine_event_handlers[type] = h;
}

// Perform all callbacks of a given type for the given connection.
static void perform_callbacks(ENGINE_EVENT_TYPE type,
                              const void *data,
                              const void *c) {
    for (struct engine_event_handler *h = engine_event_handlers[type];
         h; h = h->next) {
        h->cb(c, type, data, h->cb_data);
    }
}


/*
 * Free list management for connections.
 */
cache_t *conn_cache;      /* suffix cache */

/**
 * Reset all of the dynamic buffers used by a connection back to their
 * default sizes. The strategy for resizing the buffers is to allocate a
 * new one of the correct size and free the old one if the allocation succeeds
 * instead of using realloc to change the buffer size (because realloc may
 * not shrink the buffers, and will also copy the memory). If the allocation
 * fails the buffer will be unchanged.
 *
 * @param c the connection to resize the buffers for
 * @return true if all allocations succeeded, false if one or more of the
 *         allocations failed.
 */
static bool conn_reset_buffersize(conn *c) {
    bool ret = true;

    if (c->rsize != DATA_BUFFER_SIZE) {
        void *ptr = malloc(DATA_BUFFER_SIZE);
        if (ptr != NULL) {
            free(c->rbuf);
            c->rbuf = ptr;
            c->rsize = DATA_BUFFER_SIZE;
        } else {
            ret = false;
        }
    }

    if (c->wsize != DATA_BUFFER_SIZE) {
        void *ptr = malloc(DATA_BUFFER_SIZE);
        if (ptr != NULL) {
            free(c->wbuf);
            c->wbuf = ptr;
            c->wsize = DATA_BUFFER_SIZE;
        } else {
            ret = false;
        }
    }

    if (c->isize != ITEM_LIST_INITIAL) {
        void *ptr = malloc(sizeof(item *) * ITEM_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->ilist);
            c->ilist = ptr;
            c->isize = ITEM_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->suffixsize != SUFFIX_LIST_INITIAL) {
        void *ptr = malloc(sizeof(char *) * SUFFIX_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->suffixlist);
            c->suffixlist = ptr;
            c->suffixsize = SUFFIX_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->iovsize != IOV_LIST_INITIAL) {
        void *ptr = malloc(sizeof(struct iovec) * IOV_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->iov);
            c->iov = ptr;
            c->iovsize = IOV_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    if (c->msgsize != MSG_LIST_INITIAL) {
        void *ptr = malloc(sizeof(struct msghdr) * MSG_LIST_INITIAL);
        if (ptr != NULL) {
            free(c->msglist);
            c->msglist = ptr;
            c->msgsize = MSG_LIST_INITIAL;
        } else {
            ret = false;
        }
    }

    return ret;
}

/**
 * Constructor for all memory allocations of connection objects. Initialize
 * all members and allocate the transfer buffers.
 *
 * @param buffer The memory allocated by the object cache
 * @param unused1 not used
 * @param unused2 not used
 * @return 0 on success, 1 if we failed to allocate memory
 */
static int conn_constructor(void *buffer, void *unused1, int unused2) {
    (void)unused1; (void)unused2;

    conn *c = buffer;
    memset(c, 0, sizeof(*c));
    MEMCACHED_CONN_CREATE(c);

    if (!conn_reset_buffersize(c)) {
        free(c->rbuf);
        free(c->wbuf);
        free(c->ilist);
        free(c->suffixlist);
        free(c->iov);
        free(c->msglist);
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                       "Failed to allocate buffers for connection\n");
        return 1;
    }

    STATS_LOCK();
    mc_stats.conn_structs++;
    STATS_UNLOCK();

    return 0;
}

/**
 * Destructor for all connection objects. Release all allocated resources.
 *
 * @param buffer The memory allocated by the objec cache
 * @param unused not used
 */
static void conn_destructor(void *buffer, void *unused) {
    (void)unused;
    conn *c = buffer;
    free(c->rbuf);
    free(c->wbuf);
    free(c->ilist);
    free(c->suffixlist);
    free(c->iov);
    free(c->msglist);

    STATS_LOCK();
    mc_stats.conn_structs--;
    STATS_UNLOCK();
}

conn *conn_new(const int sfd, STATE_FUNC init_state,
                const int event_flags,
                const int read_buffer_size, enum network_transport transport,
                struct event_base *base, struct timeval *timeout) {
    conn *c = cache_alloc(conn_cache);

    if (c == NULL) {
        return NULL;
    }

    assert(c->thread == NULL);

    if (c->rsize < read_buffer_size) {
        void *mem = malloc(read_buffer_size);
        if (mem) {
            c->rsize = read_buffer_size;
            free(c->rbuf);
            c->rbuf = mem;
        } else {
            assert(c->thread == NULL);
            cache_free(conn_cache, c);
            return NULL;
        }
    }

    c->transport = transport;
    c->protocol = settings.binding_protocol;

    /* unix socket mode doesn't need this, so zeroed out.  but why
     * is this done for every command?  presumably for UDP
     * mode.  */
    if (!settings.socketpath) {
        c->request_addr_size = sizeof(c->request_addr);
    } else {
        c->request_addr_size = 0;
    }

    if (settings.verbose > 1) {
        if (init_state == conn_listening) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                           "<%d server listening (%s)\n", sfd, prot_text(c->protocol));
        } else if (IS_UDP(transport)) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                           "<%d server listening (udp)\n", sfd);
        } else if (c->protocol == negotiating_prot) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                           "<%d new auto-negotiating client connection\n", sfd);
        } else if (c->protocol == ascii_prot) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                           "<%d new ascii client connection.\n", sfd);
        } else if (c->protocol == binary_prot) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                           "<%d new binary client connection.\n", sfd);
        } else {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                           "<%d new unknown (%d) client connection\n", sfd, c->protocol);
            assert(false);
        }
    }

    c->sfd = sfd;
    c->state = init_state;
    c->cmd = -1;
    c->ascii_cmd = NULL;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->rlbytes = 0;
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;
    c->next = NULL;
    c->conn_prev = NULL;
    c->conn_next = NULL;

#ifdef DETECT_LONG_QUERY
    c->lq_bufcnt = 0;
#endif

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;

    c->coll_strkeys = 0;
    c->coll_eitem = 0;
    c->coll_resps = 0;

    // COMMAND PIPELINING
    c->pipe_state = PIPE_STATE_OFF;
    c->pipe_count = 0;
    c->noreply = false;

    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (event_add(&c->event, timeout) == -1) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                       "Failed to add connection to libevent: %s", strerror(errno));
        assert(c->thread == NULL);
        cache_free(conn_cache, c);
        return NULL;
    }

    STATS_LOCK();
    mc_stats.total_conns++;
    STATS_UNLOCK();

    c->aiostat = ENGINE_SUCCESS;
    c->ewouldblock = false;
    c->io_blocked = false;
    c->premature_notify_io_complete = false;

    /* save client ip address in connection object */
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    if (getpeername(c->sfd, (struct sockaddr*)&addr, &addrlen) != 0) {
        if (init_state == conn_new_cmd) {
            mc_logger->log(EXTENSION_LOG_WARNING, c,
                           "getpeername(fd=%d) has failed: %s\n",
                           c->sfd, strerror(errno));
        }
    }
    snprintf(c->client_ip, 16, "%s", inet_ntoa(addr.sin_addr));

    MEMCACHED_CONN_ALLOCATE(c->sfd);

    perform_callbacks(ON_CONNECT, NULL, c);

    return c;
}

static void conn_coll_eitem_free(conn *c) {
    switch (c->coll_op) {
      case OPERATION_LOP_INSERT:
        mc_engine.v1->list_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
        break;
      case OPERATION_LOP_GET:
        mc_engine.v1->list_elem_release(mc_engine.v0, c, c->coll_eitem, c->coll_ecount);
        free(c->coll_eitem);
        if (c->coll_resps != NULL) {
            free(c->coll_resps); c->coll_resps = NULL;
        }
        break;
      case OPERATION_SOP_INSERT:
        mc_engine.v1->set_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
        break;
      case OPERATION_SOP_DELETE:
      case OPERATION_SOP_EXIST:
        free(c->coll_eitem);
        break;
      case OPERATION_SOP_GET:
        mc_engine.v1->set_elem_release(mc_engine.v0, c, c->coll_eitem, c->coll_ecount);
        free(c->coll_eitem);
        if (c->coll_resps != NULL) {
            free(c->coll_resps); c->coll_resps = NULL;
        }
        break;
      case OPERATION_BOP_INSERT:
      case OPERATION_BOP_UPSERT:
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
        break;
      case OPERATION_BOP_UPDATE:
        if (c->coll_eitem != NULL)
            free(c->coll_eitem);
        break;
      case OPERATION_BOP_GET:
      case OPERATION_BOP_PWG: /* position with get */
      case OPERATION_BOP_GBP: /* get by position */
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, c->coll_eitem, c->coll_ecount);
        free(c->coll_eitem);
        if (c->coll_resps != NULL) {
            free(c->coll_resps); c->coll_resps = NULL;
        }
        break;
#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
#ifdef SUPPORT_BOP_MGET
      case OPERATION_BOP_MGET:
#endif
#ifdef SUPPORT_BOP_SMGET
      case OPERATION_BOP_SMGET:
#endif
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, c->coll_eitem, c->coll_ecount);
        free(c->coll_eitem);
        free(c->coll_strkeys); c->coll_strkeys = NULL;
        break;
#endif
      default:
        assert(0); /* This case must not happen */
    }
    c->coll_eitem = NULL;
}

static void conn_cleanup(conn *c) {
    assert(c != NULL);

    if (c->item) {
        mc_engine.v1->release(mc_engine.v0, c, c->item);
        c->item = 0;
    }

#ifdef DETECT_LONG_QUERY
    if (c->lq_bufcnt != 0) {
        lqdetect_buffer_release(c->lq_bufcnt);
        c->lq_bufcnt = 0;
    }
#endif

    if (c->coll_eitem != NULL) {
        conn_coll_eitem_free(c);
    }

    if (c->ileft != 0) {
        for (; c->ileft > 0; c->ileft--,c->icurr++) {
            mc_engine.v1->release(mc_engine.v0, c, *(c->icurr));
        }
    }

    if (c->suffixleft != 0) {
        for (; c->suffixleft > 0; c->suffixleft--, c->suffixcurr++) {
            cache_free(c->thread->suffix_cache, *(c->suffixcurr));
        }
    }

    if (c->write_and_free) {
        free(c->write_and_free);
        c->write_and_free = 0;
    }

    if (c->sasl_conn) {
        sasl_dispose(&c->sasl_conn);
        c->sasl_conn = NULL;
    }

    c->engine_storage = NULL;
    /* disconnect it from the conn_list of a thread in charge */
    if (c->conn_prev != NULL) {
        c->conn_prev->conn_next = c->conn_next;
    } else {
        assert(c->thread->conn_list == c);
        c->thread->conn_list = c->conn_next;
    }
    if (c->conn_next != NULL) {
        c->conn_next->conn_prev = c->conn_prev;
    }
    c->thread = NULL;
    assert(c->next == NULL);
    c->ascii_cmd = NULL;
    c->sfd = -1;

    c->ewouldblock = false;
    c->io_blocked = false;
    c->premature_notify_io_complete = false;
}

void conn_close(conn *c) {
    assert(c != NULL);

    /* delete the event, the socket and the conn */
    if (c->sfd != -1) {
        MEMCACHED_CONN_RELEASE(c->sfd);
        event_del(&c->event);

        if (settings.verbose > 1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                           "<%d connection closed.\n", c->sfd);
        }
        safe_close(c->sfd);
        c->sfd = -1;
    }

    if (c->ascii_cmd != NULL) {
        c->ascii_cmd->abort(c->ascii_cmd, c);
    }

    assert(c->thread);
    perform_callbacks(ON_DISCONNECT, NULL, c);

    LOCK_THREAD(c->thread);
    /* remove from pending-io list */
    if (settings.verbose > 1 && list_contains(c->thread->pending_io, c)) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                       "Current connection was in the pending-io list.. Nuking it\n");
    }
    c->thread->pending_io = list_remove(c->thread->pending_io, c);
    UNLOCK_THREAD(c->thread);

    conn_cleanup(c);

    /*
     * The contract with the object cache is that we should return the
     * object in a constructed state. Reset the buffers to the default
     * size
     */
    conn_reset_buffersize(c);
    assert(c->thread == NULL);
    cache_free(conn_cache, c);
}

/*
 * Shrinks a connection's buffers if they're too big.  This prevents
 * periodic large "get" requests from permanently chewing lots of server
 * memory.
 *
 * This should only be called in between requests since it can wipe output
 * buffers!
 */
static void conn_shrink(conn *c) {
    assert(c != NULL);

    if (IS_UDP(c->transport))
        return;

    if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
        char *newbuf;

        if (c->rcurr != c->rbuf)
            memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

        newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);

        if (newbuf) {
            c->rbuf = newbuf;
            c->rsize = DATA_BUFFER_SIZE;
        }
        /* TODO check other branch... */
        c->rcurr = c->rbuf;
    }

    if (c->isize > ITEM_LIST_HIGHWAT) {
        item **newbuf = (item**) realloc((void *)c->ilist, ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
        if (newbuf) {
            c->ilist = newbuf;
            c->isize = ITEM_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->msgsize > MSG_LIST_HIGHWAT) {
        struct msghdr *newbuf = (struct msghdr *) realloc((void *)c->msglist, MSG_LIST_INITIAL * sizeof(c->msglist[0]));
        if (newbuf) {
            c->msglist = newbuf;
            c->msgsize = MSG_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->iovsize > IOV_LIST_HIGHWAT) {
        struct iovec *newbuf = (struct iovec *) realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
        if (newbuf) {
            c->iov = newbuf;
            c->iovsize = IOV_LIST_INITIAL;
        }
    /* TODO check return value */
    }
}

/**
 * Convert a state name to a human readable form.
 */
const char *state_text(STATE_FUNC state) {
    if (state == conn_listening) {
        return "conn_listening";
    } else if (state == conn_new_cmd) {
        return "conn_new_cmd";
    } else if (state == conn_waiting) {
        return "conn_waiting";
    } else if (state == conn_read) {
        return "conn_read";
    } else if (state == conn_parse_cmd) {
        return "conn_parse_cmd";
    } else if (state == conn_write) {
        return "conn_write";
    } else if (state == conn_nread) {
        return "conn_nread";
    } else if (state == conn_swallow) {
        return "conn_swallow";
    } else if (state == conn_closing) {
        return "conn_closing";
    } else if (state == conn_mwrite) {
        return "conn_mwrite";
    } else {
        return "Unknown";
    }
}

/*
 * Sets a connection's current state in the state machine. Any special
 * processing that needs to happen on certain state transitions can
 * happen here.
 */
void conn_set_state(conn *c, STATE_FUNC state) {
    assert(c != NULL);

    if (state != c->state) {
        if (settings.verbose > 2 || c->state == conn_closing) {
            mc_logger->log(EXTENSION_LOG_DETAIL, c, "%d: going from %s to %s\n",
                           c->sfd, state_text(c->state), state_text(state));
        }

        c->state = state;

        if (state == conn_write || state == conn_mwrite) {
            MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);
        }
    }
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int ensure_iov_space(conn *c) {
    assert(c != NULL);

    if (c->iovused >= c->iovsize) {
        int i, iovnum;
        struct iovec *new_iov = (struct iovec *)realloc(c->iov,
                                (c->iovsize * 2) * sizeof(struct iovec));
        if (! new_iov)
            return -1;
        c->iov = new_iov;
        c->iovsize *= 2;

        /* Point all the msghdr structures at the new list. */
        for (i = 0, iovnum = 0; i < c->msgused; i++) {
            c->msglist[i].msg_iov = &c->iov[iovnum];
            iovnum += c->msglist[i].msg_iovlen;
        }
    }

    return 0;
}


/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */

static int add_iov(conn *c, const void *buf, int len) {
    struct msghdr *m;
    int leftover;
    bool limit_to_mtu;

    assert(c != NULL);

    do {
        m = &c->msglist[c->msgused - 1];

        /*
         * Limit UDP packets, and the first payloads of TCP replies, to
         * UDP_MAX_PAYLOAD_SIZE bytes.
         */
        limit_to_mtu = IS_UDP(c->transport) || (1 == c->msgused);

        /* We may need to start a new msghdr if this one is full. */
        if (m->msg_iovlen == IOV_MAX ||
            (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
            add_msghdr(c);
            m = &c->msglist[c->msgused - 1];
        }

        if (ensure_iov_space(c) != 0)
            return -1;

        /* If the fragment is too big to fit in the datagram, split it up */
        if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE) {
            leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
            len -= leftover;
        } else {
            leftover = 0;
        }

        m = &c->msglist[c->msgused - 1];
        m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;

        c->msgbytes += len;
        c->iovused++;
        m->msg_iovlen++;

        buf = ((char *)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return 0;
}


/*
 * Constructs a set of UDP headers and attaches them to the outgoing messages.
 */
static int build_udp_headers(conn *c) {
    int i;
    unsigned char *hdr;

    assert(c != NULL);

    if (c->msgused > c->hdrsize) {
        void *new_hdrbuf;
        if (c->hdrbuf)
            new_hdrbuf = realloc(c->hdrbuf, c->msgused * 2 * UDP_HEADER_SIZE);
        else
            new_hdrbuf = malloc(c->msgused * 2 * UDP_HEADER_SIZE);
        if (! new_hdrbuf)
            return -1;
        c->hdrbuf = (unsigned char *)new_hdrbuf;
        c->hdrsize = c->msgused * 2;
    }

    hdr = c->hdrbuf;
    for (i = 0; i < c->msgused; i++) {
        c->msglist[i].msg_iov[0].iov_base = (void*)hdr;
        c->msglist[i].msg_iov[0].iov_len = UDP_HEADER_SIZE;
        *hdr++ = c->request_id / 256;
        *hdr++ = c->request_id % 256;
        *hdr++ = i / 256;
        *hdr++ = i % 256;
        *hdr++ = c->msgused / 256;
        *hdr++ = c->msgused % 256;
        *hdr++ = 0;
        *hdr++ = 0;
        assert((void *) hdr == (caddr_t)c->msglist[i].msg_iov[0].iov_base + UDP_HEADER_SIZE);
    }

    return 0;
}


static void out_string(conn *c, const char *str) {
    size_t len;

    assert(c != NULL);

    len = strlen(str);

    if (settings.verbose > 1) {
        if (c->noreply)
            mc_logger->log(EXTENSION_LOG_DEBUG, c, ">%d NOREPLY %s\n", c->sfd, str);
        else
            mc_logger->log(EXTENSION_LOG_DEBUG, c, ">%d %s\n", c->sfd, str);
    }

    if (c->pipe_state != PIPE_STATE_OFF) {
        if (c->pipe_state == PIPE_STATE_ON) {
            if (c->pipe_count == 0) {
                /* initialize pipe responses */
                /* response header format : "RESPONSE %d\r\n" */
                c->pipe_reslen = 11 + 3; /* 3: max length of count */
                c->pipe_resptr = &c->pipe_response[c->pipe_reslen];
            }
            if ((c->pipe_reslen + (len+2)) < (PIPE_MAX_RES_SIZE-40)) {
                sprintf(c->pipe_resptr, "%s\r\n", str);
                c->pipe_reslen += (len+2);
                c->pipe_resptr = &c->pipe_response[c->pipe_reslen];
                c->pipe_count++;
                if (c->pipe_count >= PIPE_MAX_CMD_COUNT && c->noreply == true) {
                    c->pipe_state = PIPE_STATE_ERR_CFULL; /* pipe count overflow */
                    c->noreply = false; /* stop pipelining */
                }
            } else {
                c->pipe_state = PIPE_STATE_ERR_MFULL; /* pipe memory overflow */
                c->noreply = false; /* stop pipelining */
            }
            if (c->pipe_state == PIPE_STATE_ON) {
                if ((strncmp(str, "CLIENT_ERROR", 12) == 0) ||
                    (strncmp(str, "SERVER_ERROR", 12) == 0) ||
                    (strncmp(str, "ERROR", 5) == 0)) { /* severe error */
                    c->pipe_state = PIPE_STATE_ERR_BAD; /* bad error in pipelining */
                    c->noreply = false; /* stop pipelining */
                }
            }
        } else {
            /* A response message has come here before pipe error is reset.
             * Maybe, clients may not send all the commands of the pipelining.
             * So, force to reset the current pipelining.
             */
            mc_logger->log(EXTENSION_LOG_INFO, c,
                           "%d: response message before pipe error is reset. %s\n",
                           c->sfd, str);
            /* clear pipe_state: the end of pipe */
            c->pipe_state = PIPE_STATE_OFF;
            c->pipe_count = 0;
        }
    }

    if (c->noreply) {
        c->noreply = false;
       /* Clear the ewouldblock so that the next read command from
        * the same connection does not falsely block and time out.
        *
        * It's better not to set the ewouldblock if noreply exists
        * when write operations are performed.
        */
        if (c->ewouldblock)
            c->ewouldblock = false;
        conn_set_state(c, conn_new_cmd);
        return;
    }

    /* Nuke a partial output... */
    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    add_msghdr(c);

    if (c->pipe_state != PIPE_STATE_OFF) {
        if (c->pipe_state == PIPE_STATE_ON) {
            sprintf(c->pipe_resptr, "END\r\n");
            c->pipe_reslen += 5;
        } else {
            if (c->pipe_state == PIPE_STATE_ERR_CFULL) {
                sprintf(c->pipe_resptr, "PIPE_ERROR command overflow\r\n");
                c->pipe_reslen += 29;
            } else if (c->pipe_state == PIPE_STATE_ERR_MFULL) {
                sprintf(c->pipe_resptr, "PIPE_ERROR memory overflow\r\n");
                c->pipe_reslen += 28;
            } else { /* PIPE_STATE_ERR_BAD */
                sprintf(c->pipe_resptr, "PIPE_ERROR bad error\r\n");
                c->pipe_reslen += 22;
            }
        }
        sprintf(&c->pipe_response[0], "RESPONSE %3d", c->pipe_count);
        memcpy(&c->pipe_response[12], "\r\n", 2);

        if (c->pipe_state == PIPE_STATE_ON) {
            /* clear pipe_state: the end of pipe */
            c->pipe_state = PIPE_STATE_OFF;
            c->pipe_count = 0;
        } else {
            /* The pipe_state will be reset
             * after swallowing the remaining data.
             */
        }

        c->wbytes = c->pipe_reslen;
        c->wcurr  = c->pipe_response;

        conn_set_state(c, conn_write);
        c->write_and_go = conn_new_cmd;
        return;
    }

    if ((len + 2) > c->wsize) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }

    memcpy(c->wbuf, str, len);
    memcpy(c->wbuf + len, "\r\n", 2);
    c->wbytes = len + 2;
    c->wcurr = c->wbuf;

    conn_set_state(c, conn_write);
    c->write_and_go = conn_new_cmd;
    return;
}

static inline char *get_item_type_str(uint8_t type) {
    if (type == ITEM_TYPE_KV)          return "kv";
    else if (type == ITEM_TYPE_LIST)   return "list";
    else if (type == ITEM_TYPE_SET)    return "set";
    else if (type == ITEM_TYPE_BTREE)  return "b+tree";
    else                               return "unknown";
}

static inline char *get_ovflaction_str(uint8_t ovflact) {
    if (ovflact == OVFL_HEAD_TRIM)          return "head_trim";
    else if (ovflact == OVFL_TAIL_TRIM)     return "tail_trim";
    else if (ovflact == OVFL_SMALLEST_TRIM) return "smallest_trim";
    else if (ovflact == OVFL_LARGEST_TRIM)  return "largest_trim";
    else if (ovflact == OVFL_SMALLEST_SILENT_TRIM) return "smallest_silent_trim";
    else if (ovflact == OVFL_LARGEST_SILENT_TRIM)  return "largest_silent_trim";
    else if (ovflact == OVFL_ERROR)         return "error";
    else                                    return "unknown";
}

static void
handle_unexpected_errorcode_ascii(conn *c, ENGINE_ERROR_CODE ret)
{
    out_string(c, "SERVER_ERROR internal");
}

/*
 * we get here after reading the value in set/add/replace commands. The command
 * has been stored in c->cmd, and the item is ready in c->item.
 */
static void process_lop_insert_complete(conn *c) {
    assert(c->coll_op == OPERATION_LOP_INSERT);
    assert(c->coll_eitem != NULL);
    eitem *elem = (eitem *)c->coll_eitem;

    eitem_info info;
    mc_engine.v1->get_list_elem_info(mc_engine.v0, c, elem, &info);

    if (strncmp((char*)info.value + info.nbytes - 2, "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool created;
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->list_elem_insert(mc_engine.v0, c,
                                             c->coll_key, c->coll_nkey, c->coll_index, elem,
                                             c->coll_attrp, &created, 0);
        if (ret == ENGINE_EWOULDBLOCK) {
            c->ewouldblock = true;
            ret = ENGINE_SUCCESS;
        }

        if (settings.detail_enabled) {
            stats_prefix_record_lop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

#ifdef DETECT_LONG_QUERY
        /* long query detection */
        if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
            uint32_t overhead = c->coll_index >= 0 ? c->coll_index+1 : -(c->coll_index);
            if (lqdetect_discriminant(overhead)) {
                struct lq_detect_argument argument;
                char *bufptr = argument.range;

                snprintf(bufptr, 16, "%d", c->coll_index);
                argument.overhead = overhead;

                if (! lqdetect_save_cmd(c->client_ip, c->coll_key, LQCMD_LOP_INSERT, &argument)) {
                    lqdetect_in_use = false;
                }
            }
        }
#endif

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, lop_insert, c->coll_key, c->coll_nkey);
            if (created == false) out_string(c, "STORED");
            else                  out_string(c, "CREATED_STORED");
            break;
        case ENGINE_DISCONNECT:
            c->state = conn_closing;
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISS(c, lop_insert, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_NOKEY(c, cmd_lop_insert);
            if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_EOVERFLOW) out_string(c, "OVERFLOWED");
            else if (ret == ENGINE_EINDEXOOR) out_string(c, "OUT_OF_RANGE");
            else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
            else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, ret);
        }
    }

    mc_engine.v1->list_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
    c->coll_eitem = NULL;
}

static void process_sop_insert_complete(conn *c) {
    assert(c->coll_op == OPERATION_SOP_INSERT);
    assert(c->coll_eitem != NULL);
    eitem *elem = (eitem *)c->coll_eitem;

    eitem_info info;
    mc_engine.v1->get_set_elem_info(mc_engine.v0, c, elem, &info);

    if (strncmp((char*)info.value + info.nbytes - 2, "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool created;
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->set_elem_insert(mc_engine.v0, c,
                                            c->coll_key, c->coll_nkey, elem,
                                            c->coll_attrp, &created, 0);
        if (ret == ENGINE_EWOULDBLOCK) {
            c->ewouldblock = true;
            ret = ENGINE_SUCCESS;
        }

        if (settings.detail_enabled) {
            stats_prefix_record_sop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, sop_insert, c->coll_key, c->coll_nkey);
            if (created == false) out_string(c, "STORED");
            else                  out_string(c, "CREATED_STORED");
            break;
        case ENGINE_DISCONNECT:
            c->state = conn_closing;
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISS(c, sop_insert, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_NOKEY(c, cmd_sop_insert);
            if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_EOVERFLOW) out_string(c, "OVERFLOWED");
            else if (ret == ENGINE_ELEM_EEXISTS) out_string(c, "ELEMENT_EXISTS");
            else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
            else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, ret);
        }
    }

    mc_engine.v1->set_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
    c->coll_eitem = NULL;
}

static void process_sop_delete_complete(conn *c) {
    assert(c->coll_op == OPERATION_SOP_DELETE);
    assert(c->coll_eitem != NULL);
    elem_value *elem = (elem_value *)c->coll_eitem;

    if (strncmp(&elem->value[elem->nbytes-2], "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool dropped;
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->set_elem_delete(mc_engine.v0, c,
                                            c->coll_key, c->coll_nkey,
                                            elem->value, elem->nbytes, c->coll_drop,
                                            &dropped, 0);
        if (ret == ENGINE_EWOULDBLOCK) {
            c->ewouldblock = true;
            ret = ENGINE_SUCCESS;
        }

        if (settings.detail_enabled) {
            stats_prefix_record_sop_delete(c->coll_key, c->coll_nkey,
                                           (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_ELEM_HITS(c, sop_delete, c->coll_key, c->coll_nkey);
            if (dropped == false) out_string(c, "DELETED");
            else                  out_string(c, "DELETED_DROPPED");
            break;
        case ENGINE_ELEM_ENOENT:
            STATS_NONE_HITS(c, sop_delete, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND_ELEMENT");
            break;
        case ENGINE_DISCONNECT:
            c->state = conn_closing;
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISS(c, sop_delete, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_NOKEY(c, cmd_sop_delete);
            if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
            else handle_unexpected_errorcode_ascii(c, ret);
        }
    }

    free(c->coll_eitem);
    c->coll_eitem = NULL;
}

static void process_sop_exist_complete(conn *c) {
    assert(c->coll_op == OPERATION_SOP_EXIST);
    assert(c->coll_eitem != NULL);
    elem_value *elem = (elem_value *)c->coll_eitem;

    if (strncmp(&elem->value[elem->nbytes-2], "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool exist;
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->set_elem_exist(mc_engine.v0, c,
                                           c->coll_key, c->coll_nkey,
                                           elem->value, elem->nbytes,
                                           &exist, 0);
        if (settings.detail_enabled) {
            stats_prefix_record_sop_exist(c->coll_key, c->coll_nkey,
                                          (ret==ENGINE_SUCCESS));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, sop_exist, c->coll_key, c->coll_nkey);
            if (exist) out_string(c, "EXIST");
            else       out_string(c, "NOT_EXIST");
            break;
        case ENGINE_DISCONNECT:
            c->state = conn_closing;
            break;
        case ENGINE_KEY_ENOENT:
        case ENGINE_UNREADABLE:
            STATS_MISS(c, sop_exist, c->coll_key, c->coll_nkey);
            if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
            else                          out_string(c, "UNREADABLE");
            break;
        default:
            STATS_NOKEY(c, cmd_sop_exist);
            if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
            else out_string(c, "SERVER_ERROR internal");
        }
    }

    free(c->coll_eitem);
    c->coll_eitem = NULL;
}

static int make_bop_elem_response(char *bufptr, eitem_info *info)
{
    char *tmpptr = bufptr;

    /* bkey */
    if (info->nscore > 0) {
        memcpy(tmpptr, "0x", 2); tmpptr += 2;
        safe_hexatostr(info->score, info->nscore, tmpptr);
        tmpptr += strlen(tmpptr);
    } else {
        sprintf(tmpptr, "%"PRIu64"", *(uint64_t*)info->score);
        tmpptr += strlen(tmpptr);
    }
    /* eflag */
    if (info->neflag > 0) {
        memcpy(tmpptr, " 0x", 3); tmpptr += 3;
        safe_hexatostr(info->eflag, info->neflag, tmpptr);
        tmpptr += strlen(tmpptr);
    }
    /* nbytes */
    sprintf(tmpptr, " %u ", info->nbytes-2);
    tmpptr += strlen(tmpptr);

    return (int)(tmpptr - bufptr);
}

static void process_bop_insert_complete(conn *c) {
    assert(c->coll_op == OPERATION_BOP_INSERT ||
           c->coll_op == OPERATION_BOP_UPSERT);
    assert(c->coll_eitem != NULL);
    eitem *elem = (eitem *)c->coll_eitem;

    eitem_info info;
    mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem, &info);

    if (strncmp((char*)info.value + info.nbytes - 2, "\r\n", 2) != 0) {
        // release the btree element
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
        c->coll_eitem = NULL;
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        eitem_result trim_result; // contain the info of an element trimmed by maxcount overflow
        bool created;
        bool replaced;
        bool replace_if_exist = (c->coll_op == OPERATION_BOP_UPSERT ? true : false);
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->btree_elem_insert(mc_engine.v0, c,
                                              c->coll_key, c->coll_nkey, elem, replace_if_exist,
                                              c->coll_attrp, &replaced, &created,
                                              (c->coll_drop ? &trim_result : NULL), 0);
        if (ret == ENGINE_EWOULDBLOCK) {
            c->ewouldblock = true;
            ret = ENGINE_SUCCESS;
        }

        // release the btree element inserted.
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
        c->coll_eitem = NULL;

        if (settings.detail_enabled) {
            stats_prefix_record_bop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, bop_insert, c->coll_key, c->coll_nkey);
            if (c->coll_drop && trim_result.elems != NULL) { /* getrim flag */
                assert(trim_result.count == 1);
                char  buffer[256];
                char *respptr = &buffer[0];
                int   resplen;

                /* return the trimmed element info to the client */
                sprintf(respptr, "VALUE %u %u\r\n", htonl(trim_result.flags), trim_result.count);
                resplen = strlen(respptr);

                /* get trimmed element info */
                mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, trim_result.elems, &info);
                resplen += make_bop_elem_response(respptr + resplen, &info);

                /* add io vectors */
                if ((add_iov(c, respptr, resplen) != 0) ||
                    (add_iov(c, info.value, info.nbytes) != 0) ||
                    (add_iov(c, "TRIMMED\r\n", strlen("TRIMMED\r\n")) != 0))
                {
                    mc_engine.v1->btree_elem_release(mc_engine.v0, c,
                                                     &trim_result.elems, trim_result.count);
                    if (c->ewouldblock)
                        c->ewouldblock = false;
                    out_string(c, "SERVER_ERROR out of memory writing get response");
                } else {
                    /* prepare for writing response */
                    c->coll_eitem = trim_result.elems;
                    c->coll_ecount = trim_result.count; /* trim_result.count == 1 */
                    conn_set_state(c, conn_mwrite);
                }
            } else {
                /* no getrim flag or no trimmed element */
                if (replaced == false) {
                    if (created == false) out_string(c, "STORED");
                    else                  out_string(c, "CREATED_STORED");
                } else {
                    out_string(c, "REPLACED");
                }
            }
            break;
        case ENGINE_DISCONNECT:
            c->state = conn_closing;
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISS(c, bop_insert, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_NOKEY(c, cmd_bop_insert);
            if (ret == ENGINE_EBADTYPE)          out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_EBADBKEY)     out_string(c, "BKEY_MISMATCH");
            else if (ret == ENGINE_EOVERFLOW)    out_string(c, "OVERFLOWED");
            else if (ret == ENGINE_EBKEYOOR)     out_string(c, "OUT_OF_RANGE");
            else if (ret == ENGINE_ELEM_EEXISTS) out_string(c, "ELEMENT_EXISTS");
            else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
            else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, ret);
        }
    }
}

static void process_bop_update_complete(conn *c)
{
    assert(c->coll_op == OPERATION_BOP_UPDATE);
    assert(c->ewouldblock == false);
    char *new_value;
    int  new_nbytes;
    eflag_update *eupdate_ptr;

    if (c->coll_eitem != NULL) {
        elem_value *elem = (elem_value *)c->coll_eitem;
        if (strncmp(&elem->value[elem->nbytes-2], "\r\n", 2) != 0) {
            out_string(c, "CLIENT_ERROR bad data chunk");
        }
        new_value  = elem->value;
        new_nbytes = elem->nbytes;
    } else {
        new_value  = NULL;
        new_nbytes = 0;
    }

    eupdate_ptr = (c->coll_eupdate.neflag == EFLAG_NULL ? NULL : &c->coll_eupdate);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_update(mc_engine.v0, c,
                                          c->coll_key, c->coll_nkey,
                                          &c->coll_bkrange, eupdate_ptr,
                                          new_value, new_nbytes, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_update(c->coll_key, c->coll_nkey,
                                       (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, bop_update, c->coll_key, c->coll_nkey);
        out_string(c, "UPDATED");
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_update, c->coll_key, c->coll_nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, bop_update, c->coll_key, c->coll_nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_NOKEY(c, cmd_bop_update);
        if (ret == ENGINE_EBADTYPE)       out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY)  out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_EBADEFLAG) out_string(c, "EFLAG_MISMATCH");
        else if (ret == ENGINE_ENOMEM)    out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);
    }

    if (c->coll_eitem != NULL) {
        free((void*)c->coll_eitem);
        c->coll_eitem = NULL;
    }
}

#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
static int tokenize_keys(char *keystr, int length, char delimiter, int keycnt, token_t *tokens) {
    int ntokens = 0;
    char *first, *s, *e;
    bool finish = false;

    assert(keystr != NULL && tokens != NULL && keycnt >= 1);

    s = keystr;
    while (*s == ' ')
        s++;
    first = s;
    for (e = s; ntokens < keycnt; ++e) {
        if (*e == ' ') break;
        if (*e == delimiter) {
            if (s == e) break;
            tokens[ntokens].value = s;
            tokens[ntokens].length = e - s;
            ntokens++;
            s = e + 1;
        } else if (*e == '\r' && *(e+1) == '\n') {
            if (s == e) break;
            tokens[ntokens].value = s;
            tokens[ntokens].length = e - s;
            ntokens++;
            if (ntokens == keycnt && (e-first) == (length-2))
                finish = true;
            break; /* string end */
        }
    }
    if (finish == true) {
        return ntokens;
    } else {
        return -1; /* some errors */
    }
}
#endif

#ifdef SUPPORT_BOP_MGET
static void process_bop_mget_complete(conn *c) {
    assert(c->coll_op == OPERATION_BOP_MGET);
    assert(c->coll_eitem != NULL);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    eitem **elem_array = (eitem **)c->coll_eitem;
    uint32_t tot_elem_count = 0;
    uint32_t tot_access_count = 0;
    char delimiter = ',';
    token_t *key_tokens = (token_t *)((char*)c->coll_strkeys + GET_8ALIGN_SIZE(c->coll_lenkeys));

    if ((strncmp((char*)c->coll_strkeys + c->coll_lenkeys - 2, "\r\n", 2) != 0) ||
        (tokenize_keys((char*)c->coll_strkeys, c->coll_lenkeys,
                       delimiter, c->coll_numkeys, key_tokens) == -1))
    {
        ret = ENGINE_EBADVALUE;
    }
    else /* valid key_tokens */
    {
        uint32_t cur_elem_count = 0;
        uint32_t cur_access_count = 0;
        uint32_t flags, k, e;
        bool trimmed;
        eitem_info info;
        char *resultptr;
        char *valuestrp = (char*)elem_array + (c->coll_numkeys * c->coll_rcount * sizeof(eitem*));
        int   resultlen;
        int   nvaluestr;

        sprintf(valuestrp, "VALUE "); nvaluestr = strlen("VALUE ");
        resultptr = valuestrp + nvaluestr;

        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }

        for (k = 0; k < c->coll_numkeys; k++) {
            ret = mc_engine.v1->btree_elem_get(mc_engine.v0, c,
                                             key_tokens[k].value, key_tokens[k].length,
                                             &c->coll_bkrange,
                                             (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                                             c->coll_roffset, c->coll_rcount,
                                             false, false,
                                             &elem_array[tot_elem_count], &cur_elem_count,
                                             &cur_access_count, &flags, &trimmed, 0);

            if (settings.detail_enabled) {
                stats_prefix_record_bop_get(key_tokens[k].value, key_tokens[k].length,
                                            (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
            }

            if (ret == ENGINE_SUCCESS) {
                sprintf(resultptr, " %s %u %u\r\n",
                        (trimmed==false ? "OK" : "TRIMMED"), htonl(flags), cur_elem_count);
                if ((add_iov(c, valuestrp, nvaluestr) != 0) ||
                    (add_iov(c, key_tokens[k].value, key_tokens[k].length) != 0) ||
                    (add_iov(c, resultptr, strlen(resultptr)) != 0)) {
                    STATS_NOKEY(c, cmd_bop_get);
                    ret = ENGINE_ENOMEM; break;
                }
                resultptr += strlen(resultptr);

                for (e = 0; e < cur_elem_count; e++) {
                    mc_engine.v1->get_btree_elem_info(mc_engine.v0, c,
                                                      elem_array[tot_elem_count+e], &info);
                    sprintf(resultptr, "ELEMENT ");
                    resultlen = strlen(resultptr);
                    resultlen += make_bop_elem_response(resultptr + resultlen, &info);

                    if ((add_iov(c, resultptr, resultlen) != 0) ||
                        (add_iov(c, info.value, info.nbytes) != 0)) {
                        ret = ENGINE_ENOMEM; break;
                    }
                    resultptr += resultlen;
                }
                if (ret == ENGINE_SUCCESS) {
                    STATS_ELEM_HITS(c, bop_get, key_tokens[k].value, key_tokens[k].length);
                    tot_elem_count += cur_elem_count;
                    cur_elem_count = 0;
                    tot_access_count += cur_access_count;
                    cur_access_count = 0;
                } else {
                    STATS_NOKEY(c, cmd_bop_get);
                    // ret == ENGINE_ENOMEM
                    break;
                }
            } else {
                if (ret == ENGINE_ELEM_ENOENT) {
                    STATS_NONE_HITS(c, bop_get,  key_tokens[k].value, key_tokens[k].length);
                    sprintf(resultptr, " %s\r\n", "NOT_FOUND_ELEMENT");
                }
                else if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_EBKEYOOR || ret == ENGINE_UNREADABLE) {
                    STATS_MISS(c, bop_get, key_tokens[k].value, key_tokens[k].length);
                    if (ret == ENGINE_KEY_ENOENT)    sprintf(resultptr, " %s\r\n", "NOT_FOUND");
                    else if (ret == ENGINE_EBKEYOOR) sprintf(resultptr, " %s\r\n", "OUT_OF_RANGE");
                    else                             sprintf(resultptr, " %s\r\n", "UNREADABLE");
                }
                else if (ret == ENGINE_EBADTYPE || ret == ENGINE_EBADBKEY) {
                    STATS_NOKEY(c, cmd_bop_get);
                    if (ret == ENGINE_EBADTYPE) sprintf(resultptr, " %s\r\n", "TYPE_MISMATCH");
                    else                        sprintf(resultptr, " %s\r\n", "BKEY_MISMATCH");
                }
                else {
                    break; // ENGINE_DISCONNECT or SEVERE error
                }

                if ((add_iov(c, valuestrp, nvaluestr) != 0) ||
                    (add_iov(c, key_tokens[k].value, key_tokens[k].length) != 0) ||
                    (add_iov(c, resultptr, strlen(resultptr)) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                resultptr += strlen(resultptr);
            }
        }
        if (k == c->coll_numkeys) {
            ret = ENGINE_SUCCESS;
            sprintf(resultptr, "END\r\n");
            if ((add_iov(c, resultptr, strlen(resultptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM;
            }
        } else {
            // ret != ENGINE_SUCCESS
            tot_elem_count += cur_elem_count;
            cur_elem_count = 0;
            tot_access_count += cur_access_count;
            cur_access_count = 0;
        }
    }

    switch (ret) {
      case ENGINE_SUCCESS:
        STATS_NOKEY2(c, cmd_bop_mget, bop_mget_oks);
        /* Remember this command so we can garbage collect it later */
        /* c->coll_eitem  = (void *)elem_array; */
        c->coll_ecount = tot_elem_count;
        c->coll_op     = OPERATION_BOP_MGET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
        break;
      case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
      case ENGINE_ENOMEM:
        STATS_NOKEY(c, cmd_bop_mget);
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, tot_elem_count);
        out_string(c, "SERVER_ERROR out of memory writing get response");
        break;
      default:
        STATS_NOKEY(c, cmd_bop_mget);
        if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad data chunk");
        else out_string(c, "SERVER_ERROR internal");
    }

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            free((void *)c->coll_strkeys);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

#ifdef SUPPORT_BOP_SMGET
#ifdef JHPARK_NEW_SMGET_INTERFACE
static char *get_smget_miss_response(int res)
{
    if (res == ENGINE_KEY_ENOENT)      return " NOT_FOUND\r\n";
    else if (res == ENGINE_UNREADABLE) return " UNREADABLE\r\n";
    else if (res == ENGINE_EBKEYOOR)   return " OUT_OF_RANGE\r\n";
    else                               return " UNKNOWN\r\n";
}

static int make_smget_trim_response(char *bufptr, eitem_info *info)
{
    char *tmpptr = bufptr;

    /* bkey */
    if (info->nscore > 0) {
        memcpy(tmpptr, " 0x", 3); tmpptr += 3;
        safe_hexatostr(info->score, info->nscore, tmpptr);
        tmpptr += strlen(tmpptr);
    } else {
        sprintf(tmpptr, " %"PRIu64"", *(uint64_t*)info->score);
        tmpptr += strlen(tmpptr);
    }
    sprintf(tmpptr, "\r\n");
    tmpptr += 2;
    return (int)(tmpptr - bufptr);
}
#endif

#if 1 // JHPARK_OLD_SMGET_INTERFACE
static void process_bop_smget_complete_old(conn *c) {
    int i, idx;
    char *vptr = (char*)c->coll_strkeys;
    char delimiter = ',';
    token_t *keys_array = (token_t *)(vptr + GET_8ALIGN_SIZE(c->coll_lenkeys));
    char *respptr;
    int   resplen;
    int smget_count = c->coll_roffset + c->coll_rcount;
    int kmis_array_size = c->coll_numkeys * sizeof(uint32_t);

    uint32_t  kmis_count = 0;
    uint32_t  elem_count = 0;
    eitem   **elem_array = (eitem  **)c->coll_eitem;
    uint32_t *kfnd_array = (uint32_t*)((char*)elem_array + (smget_count*sizeof(eitem*)));
    uint32_t *flag_array = (uint32_t*)((char*)kfnd_array + (smget_count*sizeof(uint32_t)));
    uint32_t *kmis_array = (uint32_t*)((char*)flag_array + (smget_count*sizeof(uint32_t)));
    bool trimmed;
    bool duplicated;

    respptr = ((char*)kmis_array + kmis_array_size);

    ENGINE_ERROR_CODE ret;
    if ((strncmp(vptr + c->coll_lenkeys - 2, "\r\n", 2) != 0) ||
        (tokenize_keys(vptr, c->coll_lenkeys, delimiter, c->coll_numkeys, keys_array) == -1)) {
        ret = ENGINE_EBADVALUE;
    } else {
        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }
        ret = mc_engine.v1->btree_elem_smget_old(mc_engine.v0, c,
                                             keys_array, c->coll_numkeys,
                                             &c->coll_bkrange,
                                             (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                                             c->coll_roffset, c->coll_rcount,
                                             elem_array, kfnd_array, flag_array, &elem_count,
                                             kmis_array, &kmis_count, &trimmed, &duplicated, 0);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info[elem_count+1]; /* elem_count might be 0. */

        do {
            sprintf(respptr, "VALUE %u\r\n", elem_count);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            for (i = 0; i < elem_count; i++) {
                idx = kfnd_array[i];
                if (add_iov(c, keys_array[idx].value, keys_array[idx].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
                mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem_array[i], &info[i]);
                /* flags */
                sprintf(respptr, " %u ", htonl(flag_array[i]));
                resplen = strlen(respptr);
                resplen += make_bop_elem_response(respptr + resplen, &info[i]);
                if ((add_iov(c, respptr, resplen) != 0) ||
                    (add_iov(c, info[i].value, info[i].nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
            if (ret == ENGINE_ENOMEM) break;

            sprintf(respptr, "MISSED_KEYS %u\r\n", kmis_count);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            if (kmis_count > 0) {
                sprintf(respptr, "\r\n"); resplen = 2;
                for (i = 0; i < kmis_count; i++) {
                    /* the last key string does not have delimiter character */
                    idx = kmis_array[i];
                    if ((add_iov(c, keys_array[idx].value, keys_array[idx].length) != 0) ||
                        (add_iov(c, respptr, resplen) != 0)) {
                        ret = ENGINE_ENOMEM; break;
                    }
                }
                respptr += resplen;
                if (ret == ENGINE_ENOMEM) break;
            }

            if (trimmed == true) {
                sprintf(respptr, (duplicated ? "DUPLICATED_TRIMMED\r\n" : "TRIMMED\r\n"));
            } else {
                sprintf(respptr, (duplicated ? "DUPLICATED\r\n" : "END\r\n"));
            }
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_NOKEY2(c, cmd_bop_smget, bop_smget_oks);
            /* Remember this command so we can garbage collect it later */
            /* c->coll_eitem  = (void *)elem_array; */
            c->coll_ecount = elem_count;
            c->coll_op     = OPERATION_BOP_SMGET;
            conn_set_state(c, conn_mwrite);
            c->msgcurr     = 0;
        } else {
            STATS_NOKEY(c, cmd_bop_smget);
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_smget);
        if (ret == ENGINE_EBADVALUE)     out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_EBADATTR) out_string(c, "ATTR_MISMATCH");
        else if (ret == ENGINE_EBKEYOOR) out_string(c, "OUT_OF_RANGE");
        else out_string(c, "SERVER_ERROR internal");
    }

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            free((void *)c->coll_strkeys);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

static void process_bop_smget_complete(conn *c) {
    assert(c->coll_op == OPERATION_BOP_SMGET);
    assert(c->coll_eitem != NULL);
#if 1 // JHPARK_OLD_SMGET_INTERFACE
    if (c->coll_smgmode == 0) {
        process_bop_smget_complete_old(c);
        return;
    }
#endif
    int i, idx;
    char *vptr = (char*)c->coll_strkeys;
    char delimiter = ',';
    token_t *keys_array = (token_t *)(vptr + GET_8ALIGN_SIZE(c->coll_lenkeys));
    char *respptr;
    int   resplen;
#ifdef JHPARK_NEW_SMGET_INTERFACE
    smget_result_t smres;

    smres.elem_array = (eitem **)c->coll_eitem;
    smres.elem_kinfo = (smget_ehit_t *)&smres.elem_array[c->coll_rcount+c->coll_numkeys];
    smres.miss_kinfo = (smget_emis_t *)&smres.elem_kinfo[c->coll_rcount];

    respptr = (char *)&smres.miss_kinfo[c->coll_numkeys];
#else
    int smget_count = c->coll_roffset + c->coll_rcount;
#if 1 // JHPARK_OLD_SMGET_INTERFACE
#else
    int elem_array_size = smget_count * (sizeof(eitem*) + (2*sizeof(uint32_t)));
#endif
    int kmis_array_size = c->coll_numkeys * sizeof(uint32_t);

    uint32_t  kmis_count = 0;
    uint32_t  elem_count = 0;
    eitem   **elem_array = (eitem  **)c->coll_eitem;
    uint32_t *kfnd_array = (uint32_t*)((char*)elem_array + (smget_count*sizeof(eitem*)));
    uint32_t *flag_array = (uint32_t*)((char*)kfnd_array + (smget_count*sizeof(uint32_t)));
    uint32_t *kmis_array = (uint32_t*)((char*)flag_array + (smget_count*sizeof(uint32_t)));
    bool trimmed;
    bool duplicated;

    respptr = ((char*)kmis_array + kmis_array_size);
#endif

    ENGINE_ERROR_CODE ret;
    if ((strncmp(vptr + c->coll_lenkeys - 2, "\r\n", 2) != 0) ||
        (tokenize_keys(vptr, c->coll_lenkeys, delimiter, c->coll_numkeys, keys_array) == -1)) {
        ret = ENGINE_EBADVALUE;
    } else {
        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }
#ifdef JHPARK_NEW_SMGET_INTERFACE
        ret = mc_engine.v1->btree_elem_smget(mc_engine.v0, c,
                                             keys_array, c->coll_numkeys,
                                             &c->coll_bkrange,
                                             (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
#if 1 // JHPARK_OLD_SMGET_INTERFACE
                                             c->coll_roffset, c->coll_rcount,
                                             (c->coll_smgmode == 2 ? true : false),
#else
                                             c->coll_roffset, c->coll_rcount, c->coll_unique,
#endif
                                             &smres, 0);
#else
        ret = mc_engine.v1->btree_elem_smget(mc_engine.v0, c,
                                             keys_array, c->coll_numkeys,
                                             &c->coll_bkrange,
                                             (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                                             c->coll_roffset, c->coll_rcount,
                                             elem_array, kfnd_array, flag_array, &elem_count,
                                             kmis_array, &kmis_count, &trimmed, &duplicated, 0);
#endif
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
#ifdef JHPARK_NEW_SMGET_INTERFACE
        eitem_info info[smres.elem_count+1]; /* elem_count might be 0. */
#else
        eitem_info info[elem_count+1]; /* elem_count might be 0. */
#endif

        do {
#ifdef JHPARK_NEW_SMGET_INTERFACE
            /* Change smget response head string: VALUE => ELEMENTS.
             * It makes incompatible with the clients of lower version.
             */
            sprintf(respptr, "ELEMENTS %u\r\n", smres.elem_count);
#else
            sprintf(respptr, "VALUE %u\r\n", elem_count);
#endif
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

#ifdef JHPARK_NEW_SMGET_INTERFACE
            for (i = 0; i < smres.elem_count; i++) {
                mc_engine.v1->get_btree_elem_info(mc_engine.v0, c,
                                  smres.elem_array[i], &info[i]);
                sprintf(respptr, " %u ", htonl(smres.elem_kinfo[i].flag));
                resplen = strlen(respptr);
                resplen += make_bop_elem_response(respptr + resplen, &info[i]);
                idx = smres.elem_kinfo[i].kidx;
                if ((add_iov(c, keys_array[idx].value, keys_array[idx].length) != 0) ||
                    (add_iov(c, respptr, resplen) != 0) ||
                    (add_iov(c, info[i].value, info[i].nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
#else
            for (i = 0; i < elem_count; i++) {
                idx = kfnd_array[i];
                if (add_iov(c, keys_array[idx].value, keys_array[idx].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
                mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem_array[i], &info[i]);
                /* flags */
                sprintf(respptr, " %u ", htonl(flag_array[i]));
                resplen = strlen(respptr);
                resplen += make_bop_elem_response(respptr + resplen, &info[i]);
                if ((add_iov(c, respptr, resplen) != 0) ||
                    (add_iov(c, info[i].value, info[i].nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
#endif
            if (ret == ENGINE_ENOMEM) break;

#ifdef JHPARK_NEW_SMGET_INTERFACE
            sprintf(respptr, "MISSED_KEYS %u\r\n", smres.miss_count);
#else
            sprintf(respptr, "MISSED_KEYS %u\r\n", kmis_count);
#endif
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

#ifdef JHPARK_NEW_SMGET_INTERFACE
            if (smres.miss_count > 0) {
                char *str = NULL;
                for (i = 0; i < smres.miss_count; i++) {
                    /* the last key string does not have delimiter character */
                    idx = smres.miss_kinfo[i].kidx;
                    str = get_smget_miss_response(smres.miss_kinfo[i].code);
                    if ((add_iov(c, keys_array[idx].value, keys_array[idx].length) != 0) ||
                        (add_iov(c, str, strlen(str)) != 0)) {
                        ret = ENGINE_ENOMEM; break;
                    }
                }
                if (ret == ENGINE_ENOMEM) break;
            }

            sprintf(respptr, "TRIMMED_KEYS %u\r\n", smres.trim_count);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            if (smres.trim_count > 0) {
                eitem_info tinfo;
                for (i = 0; i < smres.trim_count; i++) {
                    mc_engine.v1->get_btree_elem_info(mc_engine.v0, c,
                                                      smres.trim_elems[i], &tinfo);
                    resplen = make_smget_trim_response(respptr, &tinfo);
                    idx = smres.trim_kinfo[i].kidx;
                    if ((add_iov(c, keys_array[idx].value, keys_array[idx].length) != 0) ||
                        (add_iov(c, respptr, resplen) != 0)) {
                        ret = ENGINE_ENOMEM; break;
                    }
                    respptr += resplen;
                }
                if (ret == ENGINE_ENOMEM) break;
            }

            sprintf(respptr, (smres.duplicated ? "DUPLICATED\r\n" : "END\r\n"));
#else
            if (kmis_count > 0) {
                sprintf(respptr, "\r\n"); resplen = 2;
                for (i = 0; i < kmis_count; i++) {
                    /* the last key string does not have delimiter character */
                    idx = kmis_array[i];
                    if ((add_iov(c, keys_array[idx].value, keys_array[idx].length) != 0) ||
                        (add_iov(c, respptr, resplen) != 0)) {
                        ret = ENGINE_ENOMEM; break;
                    }
                }
                respptr += resplen;
                if (ret == ENGINE_ENOMEM) break;
            }

            if (trimmed == true) {
                sprintf(respptr, (duplicated ? "DUPLICATED_TRIMMED\r\n" : "TRIMMED\r\n"));
            } else {
                sprintf(respptr, (duplicated ? "DUPLICATED\r\n" : "END\r\n"));
            }
#endif
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_NOKEY2(c, cmd_bop_smget, bop_smget_oks);
            /* Remember this command so we can garbage collect it later */
            /* c->coll_eitem  = (void *)elem_array; */
#ifdef JHPARK_NEW_SMGET_INTERFACE
            c->coll_ecount = smres.elem_count+smres.trim_count;
#else
            c->coll_ecount = elem_count;
#endif
            c->coll_op     = OPERATION_BOP_SMGET;
            conn_set_state(c, conn_mwrite);
            c->msgcurr     = 0;
        } else {
            STATS_NOKEY(c, cmd_bop_smget);
#ifdef JHPARK_NEW_SMGET_INTERFACE
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, smres.elem_array,
                                             smres.elem_count+smres.trim_count);
#else
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
#endif
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_smget);
        if (ret == ENGINE_EBADVALUE)     out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_EBADATTR) out_string(c, "ATTR_MISMATCH");
#ifdef JHPARK_NEW_SMGET_INTERFACE // JHPARK_SMGET_OFFSET_HANDLING
#else
        else if (ret == ENGINE_EBKEYOOR) out_string(c, "OUT_OF_RANGE");
#endif
        else out_string(c, "SERVER_ERROR internal");
    }

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            free((void *)c->coll_strkeys);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

static void complete_update_ascii(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);

    if (c->coll_eitem != NULL) {
        if (c->coll_op == OPERATION_LOP_INSERT)  process_lop_insert_complete(c);
        else if (c->coll_op == OPERATION_SOP_INSERT) process_sop_insert_complete(c);
        else if (c->coll_op == OPERATION_SOP_DELETE) process_sop_delete_complete(c);
        else if (c->coll_op == OPERATION_SOP_EXIST) process_sop_exist_complete(c);
        else if (c->coll_op == OPERATION_BOP_INSERT ||
                 c->coll_op == OPERATION_BOP_UPSERT) process_bop_insert_complete(c);
        else if (c->coll_op == OPERATION_BOP_UPDATE) process_bop_update_complete(c);
#ifdef SUPPORT_BOP_MGET
        else if (c->coll_op == OPERATION_BOP_MGET) process_bop_mget_complete(c);
#endif
#ifdef SUPPORT_BOP_SMGET
        else if (c->coll_op == OPERATION_BOP_SMGET) process_bop_smget_complete(c);
#endif
        return;
    }

    item *it = c->item;
    item_info info = { .nvalue = 1 };
    if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &info)) {
        mc_engine.v1->release(mc_engine.v0, c, it);
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                       "%d: Failed to get item info\n", c->sfd);
        out_string(c, "SERVER_ERROR failed to get item details");
        return;
    }

    if (memcmp((char*)info.value[0].iov_base + info.nbytes - 2, "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        ENGINE_ERROR_CODE ret;
        ret = mc_engine.v1->store(mc_engine.v0, c, it, &c->cas, c->store_op, 0);

#ifdef ENABLE_DTRACE
        switch (c->store_op) {
        case OPERATION_ADD:
            MEMCACHED_COMMAND_ADD(c->sfd, info.key, info.nkey,
                                  (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
            break;
        case OPERATION_REPLACE:
            MEMCACHED_COMMAND_REPLACE(c->sfd, info.key, info.nkey,
                                      (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
            break;
        case OPERATION_APPEND:
            MEMCACHED_COMMAND_APPEND(c->sfd, info.key, info.nkey,
                                     (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
            break;
        case OPERATION_PREPEND:
            MEMCACHED_COMMAND_PREPEND(c->sfd, info.key, info.nkey,
                                      (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
            break;
        case OPERATION_SET:
            MEMCACHED_COMMAND_SET(c->sfd, info.key, info.nkey,
                                  (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
            break;
        case OPERATION_CAS:
            MEMCACHED_COMMAND_CAS(c->sfd, info.key, info.nkey, info.nbytes, c->cas);
            break;
        }
#endif

        if (ret == ENGINE_EWOULDBLOCK) {
            c->ewouldblock = true;
            ret = ENGINE_SUCCESS;
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            out_string(c, "STORED");
            break;
        case ENGINE_KEY_EEXISTS:
            out_string(c, "EXISTS");
            break;
        case ENGINE_KEY_ENOENT:
            out_string(c, "NOT_FOUND");
            break;
        case ENGINE_NOT_STORED:
            out_string(c, "NOT_STORED");
            break;
        case ENGINE_DISCONNECT:
            c->state = conn_closing;
            break;
        case ENGINE_ENOTSUP:
            out_string(c, "SERVER_ERROR not supported");
            break;
        case ENGINE_PREFIX_ENAME:
            out_string(c, "CLIENT_ERROR invalid prefix name");
            break;
        case ENGINE_ENOMEM:
            out_string(c, "SERVER_ERROR out of memory");
            break;
        case ENGINE_EINVAL:
            out_string(c, "CLIENT_ERROR invalid arguments");
            break;
        case ENGINE_E2BIG:
            out_string(c, "CLIENT_ERROR value too big");
            break;
        case ENGINE_EACCESS:
            out_string(c, "CLIENT_ERROR access control violation");
            break;
        case ENGINE_NOT_MY_VBUCKET:
            out_string(c, "SERVER_ERROR not my vbucket");
            break;
        case ENGINE_EBADTYPE:
            out_string(c, "TYPE_MISMATCH");
            break;
        case ENGINE_FAILED:
            out_string(c, "SERVER_ERROR failure");
            break;
        default:
            handle_unexpected_errorcode_ascii(c, ret);
        }
    }
    SLAB_INCR(c, cmd_set, info.key, info.nkey);

    /* release the c->item reference */
    mc_engine.v1->release(mc_engine.v0, c, c->item);
    c->item = 0;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
static void* binary_get_request(conn *c) {
    char *ret = c->rcurr;
    ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen +
            c->binary_header.request.extlen);

    assert(ret >= c->rbuf);
    return ret;
}

/**
 * get a pointer to the key in this request
 */
static char* binary_get_key(conn *c) {
    return c->rcurr - (c->binary_header.request.keylen);
}

/**
 * Insert a key into a buffer, but replace all non-printable characters
 * with a '.'.
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param prefix string to insert before the data
 * @param client the client we are serving
 * @param from_client set to true if this data is from the client
 * @param key the key to add to the buffer
 * @param nkey the number of bytes in the key
 * @return number of bytes in dest if success, -1 otherwise
 */
static ssize_t key_to_printable_buffer(char *dest, size_t destsz,
                                       int client, bool from_client,
                                       const char *prefix,
                                       const char *key,
                                       size_t nkey)
{
    ssize_t nw = snprintf(dest, destsz, "%c%d %s ", from_client ? '>' : '<',
                          client, prefix);
    if (nw == -1) {
        return -1;
    }

    char *ptr = dest + nw;
    destsz -= nw;
    if (nkey > destsz) {
        nkey = destsz;
    }

    for (ssize_t ii = 0; ii < nkey; ++ii, ++key, ++ptr) {
        if (isgraph(*key)) {
            *ptr = *key;
        } else {
            *ptr = '.';
        }
    }

    *ptr = '\0';
    return ptr - dest;
}

/**
 * Convert a byte array to a text string
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param prefix string to insert before the data
 * @param client the client we are serving
 * @param from_client set to true if this data is from the client
 * @param data the data to add to the buffer
 * @param size the number of bytes in data to print
 * @return number of bytes in dest if success, -1 otherwise
 */
static ssize_t bytes_to_output_string(char *dest, size_t destsz,
                                      int client, bool from_client,
                                      const char *prefix,
                                      const char *data,
                                      size_t size)
{
    ssize_t nw = snprintf(dest, destsz, "%c%d %s", from_client ? '>' : '<',
                          client, prefix);
    if (nw == -1) {
        return -1;
    }
    ssize_t offset = nw;

    for (ssize_t ii = 0; ii < size; ++ii) {
        if (ii % 4 == 0) {
            if ((nw = snprintf(dest + offset, destsz - offset, "\n%c%d  ",
                               from_client ? '>' : '<', client)) == -1) {
                return  -1;
            }
            offset += nw;
        }
        if ((nw = snprintf(dest + offset, destsz - offset,
                           " 0x%02x", (unsigned char)data[ii])) == -1) {
            return -1;
        }
        offset += nw;
    }

    if ((nw = snprintf(dest + offset, destsz - offset, "\n")) == -1) {
        return -1;
    }

    return offset + nw;
}

static void add_bin_header(conn *c, uint16_t err, uint8_t hdr_len, uint16_t key_len, uint32_t body_len) {
    protocol_binary_response_header* header;

    assert(c);

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        /* XXX:  out_string is inappropriate here */
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    header = (protocol_binary_response_header *)c->wbuf;

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = (uint8_t)hdr_len;
    header->response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = c->opaque;
    header->response.cas = htonll(c->cas);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (bytes_to_output_string(buffer, sizeof(buffer), c->sfd, false,
                                   "Writing bin response:",
                                   (const char*)header->bytes,
                                   sizeof(header->bytes)) != -1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s", buffer);
        }
    }

    add_iov(c, c->wbuf, sizeof(header->response));
}

static void write_bin_packet(conn *c, protocol_binary_response_status err, int swallow) {
    ssize_t len;
    char buffer[1024] = { [sizeof(buffer) - 1] = '\0' };

    switch (err) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        len = 0;
        break;
    case PROTOCOL_BINARY_RESPONSE_ENOMEM:
        len = snprintf(buffer, sizeof(buffer), "Out of memory");
        break;
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
        len = snprintf(buffer, sizeof(buffer), "Unknown command");
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
        len = snprintf(buffer, sizeof(buffer), "Not found");
        break;
    case PROTOCOL_BINARY_RESPONSE_EINVAL:
        len = snprintf(buffer, sizeof(buffer), "Invalid arguments");
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
        len = snprintf(buffer, sizeof(buffer), "Data exists for key");
        break;
    case PROTOCOL_BINARY_RESPONSE_E2BIG:
        len = snprintf(buffer, sizeof(buffer), "Too large");
        break;
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
        len = snprintf(buffer, sizeof(buffer),
                       "Non-numeric server-side value for incr or decr");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
        len = snprintf(buffer, sizeof(buffer), "Not stored");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBADTYPE:
        len = snprintf(buffer, sizeof(buffer), "Not supported operation, bad type");
        break;
    case PROTOCOL_BINARY_RESPONSE_EOVERFLOW:
        len = snprintf(buffer, sizeof(buffer), "Data structure full");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBADVALUE:
        len = snprintf(buffer, sizeof(buffer), "Bad value");
        break;
    case PROTOCOL_BINARY_RESPONSE_EINDEXOOR:
        len = snprintf(buffer, sizeof(buffer), "Index out of range");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBKEYOOR:
        len = snprintf(buffer, sizeof(buffer), "Bkey out of range");
        break;
    case PROTOCOL_BINARY_RESPONSE_ELEM_ENOENT:
        len = snprintf(buffer, sizeof(buffer), "Not found element");
        break;
    case PROTOCOL_BINARY_RESPONSE_ELEM_EEXISTS:
        len = snprintf(buffer, sizeof(buffer), "Element already exists");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBADATTR:
        len = snprintf(buffer, sizeof(buffer), "Attribute not found");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBADBKEY:
        len = snprintf(buffer, sizeof(buffer), "Bkey mismatch");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBADEFLAG:
        len = snprintf(buffer, sizeof(buffer), "Eflag mismatch");
        break;
    case PROTOCOL_BINARY_RESPONSE_UNREADABLE:
        len = snprintf(buffer, sizeof(buffer), "Unreadable item");
        break;
    case PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME:
        len = snprintf(buffer, sizeof(buffer), "Invalid prefix name");
        break;
    case PROTOCOL_BINARY_RESPONSE_PREFIX_ENOENT:
        len = snprintf(buffer, sizeof(buffer), "Prefix not found");
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
        len = snprintf(buffer, sizeof(buffer), "Auth failure");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED:
        len = snprintf(buffer, sizeof(buffer), "Not supported");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET:
        len = snprintf(buffer, sizeof(buffer),
                       "I'm not responsible for this vbucket");
        break;

    default:
        len = snprintf(buffer, sizeof(buffer), "UNHANDLED ERROR (%d)", err);
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                       ">%d UNHANDLED ERROR: %d\n", c->sfd, err);
    }

    /* Allow the engine to pass extra error information */
    if (mc_engine.v1->errinfo != NULL) {
        size_t elen = mc_engine.v1->errinfo(mc_engine.v0, c, buffer + len + 2,
                                            sizeof(buffer) - len - 3);

        if (elen > 0) {
            memcpy(buffer + len, ": ", 2);
            len += elen + 2;
        }
    }

    if (err != PROTOCOL_BINARY_RESPONSE_SUCCESS && settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                       ">%d Writing an error: %s\n", c->sfd, buffer);
    }

    add_bin_header(c, err, 0, 0, len);
    if (len > 0) {
        add_iov(c, buffer, len);
    }
    conn_set_state(c, conn_mwrite);
    if (swallow > 0) {
        c->sbytes = swallow;
        c->write_and_go = conn_swallow;
    } else {
        c->write_and_go = conn_new_cmd;
    }
}

/* Form and send a response to a command over the binary protocol */
static void write_bin_response(conn *c, void *d, int hlen, int keylen, int dlen) {
    if (!c->noreply || c->cmd == PROTOCOL_BINARY_CMD_GET ||
        c->cmd == PROTOCOL_BINARY_CMD_GETK) {
        add_bin_header(c, 0, hlen, keylen, dlen);
        if(dlen > 0) {
            add_iov(c, d, dlen);
        }
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
    } else {
        conn_set_state(c, conn_new_cmd);
    }
}


static void complete_incr_bin(conn *c) {
    protocol_binary_response_incr* rsp = (protocol_binary_response_incr*)c->wbuf;
    protocol_binary_request_incr* req = binary_get_request(c);

    assert(c != NULL);
    assert(c->wsize >= sizeof(*rsp));

    /* fix byteorder in the request */
    req->message.body.delta = ntohll(req->message.body.delta);
    req->message.body.initial = ntohll(req->message.body.initial);
    req->message.body.expiration = ntohl(req->message.body.expiration);
    char *key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    bool incr = (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT ||
                 c->cmd == PROTOCOL_BINARY_CMD_INCREMENTQ);

    if (settings.verbose > 1) {
        char buffer[1024];
        ssize_t nw;
        nw = key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                     incr ? "INCR" : "DECR", key, nkey);
        if (nw != -1) {
            if (snprintf(buffer + nw, sizeof(buffer) - nw,
                         " %" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n",
                         (uint64_t)req->message.body.delta,
                         (uint64_t)req->message.body.initial,
                         (uint64_t)req->message.body.expiration) != -1) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s", buffer);
            }
        }
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->arithmetic(mc_engine.v0,
                                   c, key, nkey, incr,
                                   req->message.body.expiration != 0xffffffff,
                                   req->message.body.delta,
                                   req->message.body.initial,
                                   0, /* flags */
                                   req->message.body.expiration,
                                   &c->cas,
                                   &rsp->message.body.value,
                                   c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        if (incr) {
            stats_prefix_record_incr(key, nkey);
        } else {
            stats_prefix_record_decr(key, nkey);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        rsp->message.body.value = htonll(rsp->message.body.value);
        write_bin_response(c, &rsp->message.body, 0, 0,
                           sizeof (rsp->message.body.value));
        if (incr) {
            STATS_HITS(c, incr, key, nkey);
        } else {
            STATS_HITS(c, decr, key, nkey);
        }
        break;
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        if (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
            STATS_MISS(c, incr, key, nkey);
        } else {
            STATS_MISS(c, decr, key, nkey);
        }
        break;
    case ENGINE_PREFIX_ENAME:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        break;
    case ENGINE_EINVAL:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL, 0);
        break;
    case ENGINE_NOT_STORED:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0);
        break;
    case ENGINE_EBADTYPE:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        break;
    default:
        abort();
    }
}

static void complete_update_bin(conn *c) {
    protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    assert(c != NULL);

    item *it = c->item;
    item_info info = { .nvalue = 1 };
    if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &info)) {
        mc_engine.v1->release(mc_engine.v0, c, it);
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                       "%d: Failed to get item info\n", c->sfd);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
        return;
    }
    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    memcpy((char*)info.value[0].iov_base + info.value[0].iov_len - 2, "\r\n", 2);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->store(mc_engine.v0, c, it, &c->cas, c->store_op,
                              c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

#ifdef ENABLE_DTRACE
    switch (c->cmd) {
    case OPERATION_ADD:
        MEMCACHED_COMMAND_ADD(c->sfd, info.key, info.nkey,
                              (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
        break;
    case OPERATION_REPLACE:
        MEMCACHED_COMMAND_REPLACE(c->sfd, info.key, info.nkey,
                                  (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
        break;
    case OPERATION_APPEND:
        MEMCACHED_COMMAND_APPEND(c->sfd, info.key, info.nkey,
                                 (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
        break;
    case OPERATION_PREPEND:
        MEMCACHED_COMMAND_PREPEND(c->sfd, info.key, info.nkey,
                                  (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
        break;
    case OPERATION_SET:
        MEMCACHED_COMMAND_SET(c->sfd, info.key, info.nkey,
                              (ret == ENGINE_SUCCESS) ? info.nbytes : -1, c->cas);
        break;
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    case ENGINE_PREFIX_ENAME:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0);
        break;
    case ENGINE_EBADTYPE:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        break;
    default:
        if (c->store_op == OPERATION_ADD) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        } else if(c->store_op == OPERATION_REPLACE) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            eno = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
        }
        write_bin_packet(c, eno, 0);
    }
    SLAB_INCR(c, cmd_set, info.key, info.nkey);

    /* release the c->item reference */
    mc_engine.v1->release(mc_engine.v0, c, c->item);
    c->item = 0;
}

static void process_bin_get(conn *c) {
    item *it;

    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->wbuf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                    "GET", key, nkey) != -1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s\n", buffer);
        }
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->get(mc_engine.v0, c, &it, key, nkey,
                            c->binary_header.request.vbucket);

    uint16_t keylen;
    uint32_t bodylen;
    item_info info = { .nvalue = 1 };

    switch (ret) {
    case ENGINE_SUCCESS:
        if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &info)) {
            mc_engine.v1->release(mc_engine.v0, c, it);
            mc_logger->log(EXTENSION_LOG_WARNING, c,
                           "%d: Failed to get item info\n", c->sfd);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
            break;
        }

        /* the length has two unnecessary bytes ("\r\n") */
        keylen = 0;
        bodylen = sizeof(rsp->message.body) + (info.nbytes - 2);

        STATS_HIT(c, get, key, nkey);

        if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
            bodylen += nkey;
            keylen = nkey;
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
        rsp->message.header.response.cas = htonll(info.cas);

        // add the flags
        rsp->message.body.flags = info.flags;
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
            add_iov(c, info.key, nkey);
        }

        /* Add the data minus the CRLF */
        add_iov(c, info.value[0].iov_base, info.value[0].iov_len - 2);
        conn_set_state(c, conn_mwrite);
        /* Remember this command so we can garbage collect it later */
        c->item = it;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, get, key, nkey);

        MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);

        if (c->noreply) {
            conn_set_state(c, conn_new_cmd);
        } else {
            if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
                char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
                add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                        0, nkey, nkey);
                memcpy(ofs, key, nkey);
                add_iov(c, ofs, nkey);
                conn_set_state(c, conn_mwrite);
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            }
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0);
        break;
    case ENGINE_EBADTYPE:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        break;
    default:
        /* @todo add proper error handling! */
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                       "Unknown error code: %d\n", ret);
        abort();
    }

    if (settings.detail_enabled && ret != ENGINE_EWOULDBLOCK) {
        stats_prefix_record_get(key, nkey, ret == ENGINE_SUCCESS);
    }
}

static void append_bin_stats(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             conn *c) {
    char *buf = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    uint32_t bodylen = klen + vlen;
    protocol_binary_response_header header = {
        .response.magic = (uint8_t)PROTOCOL_BINARY_RES,
        .response.opcode = PROTOCOL_BINARY_CMD_STAT,
        .response.keylen = (uint16_t)htons(klen),
        .response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES,
        .response.bodylen = htonl(bodylen),
        .response.opaque = c->opaque
    };

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (klen > 0) {
        memcpy(buf, key, klen);
        buf += klen;

        if (vlen > 0) {
            memcpy(buf, val, vlen);
        }
    }

    c->dynamic_buffer.offset += sizeof(header.response) + bodylen;
}

/**
 * Append a key-value pair to the stats output buffer. This function assumes
 * that the output buffer is big enough (it will be if you call it through
 * append_stats)
 */
static void append_ascii_stats(const char *key, const uint16_t klen,
                               const char *val, const uint32_t vlen,
                               conn *c) {
    char *pos = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    uint32_t nbytes = 5; /* "END\r\n" or "STAT " */

    if (klen == 0 && vlen == 0) {
        memcpy(pos, "END\r\n", 5);
    } else {
        memcpy(pos, "STAT ", 5);
        memcpy(pos + nbytes, key, klen);
        nbytes += klen;
        if (vlen != 0) {
            pos[nbytes] = ' ';
            ++nbytes;
            memcpy(pos + nbytes, val, vlen);
            nbytes += vlen;
        }
        memcpy(pos + nbytes, "\r\n", 2);
        nbytes += 2;
    }

    c->dynamic_buffer.offset += nbytes;
}

static bool grow_dynamic_buffer(conn *c, size_t needed) {
    size_t nsize = c->dynamic_buffer.size;
    size_t available = nsize - c->dynamic_buffer.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->dynamic_buffer.buffer == NULL) {
        nsize = 1024;
        available = c->dynamic_buffer.size = c->dynamic_buffer.offset = 0;
    }

    while (needed > available) {
        assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->dynamic_buffer.offset;
    }

    if (nsize != c->dynamic_buffer.size) {
        char *ptr = realloc(c->dynamic_buffer.buffer, nsize);
        if (ptr) {
            c->dynamic_buffer.buffer = ptr;
            c->dynamic_buffer.size = nsize;
        } else {
            rv = false;
        }
    }

    return rv;
}

static void append_stats(const char *key, const uint16_t klen,
                  const char *val, const uint32_t vlen,
                  const void *cookie)
{
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return ;
    }

    conn *c = (conn*)cookie;

    if (c->protocol == binary_prot) {
        size_t needed = vlen + klen + sizeof(protocol_binary_response_header);
        if (!grow_dynamic_buffer(c, needed)) {
            return ;
        }
        append_bin_stats(key, klen, val, vlen, c);
    } else {
        size_t needed = vlen + klen + 10; // 10 == "STAT = \r\n"
        if (!grow_dynamic_buffer(c, needed)) {
            return ;
        }
        append_ascii_stats(key, klen, val, vlen, c);
    }

    assert(c->dynamic_buffer.offset <= c->dynamic_buffer.size);
}

static void process_bin_stat(conn *c) {
    char *subcommand = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                    "STATS", subcommand, nkey) != -1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s\n", buffer);
        }
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if (nkey == 0) {
        /* request all statistics */
        ret = mc_engine.v1->get_stats(mc_engine.v0, c, NULL, 0, append_stats);
        if (ret == ENGINE_SUCCESS) {
            server_stats(&append_stats, c, false);
        }
    } else if (strncmp(subcommand, "reset", 5) == 0) {
        stats_reset(c);
        mc_engine.v1->reset_stats(mc_engine.v0, c);
    } else if (strncmp(subcommand, "settings", 8) == 0) {
        process_stat_settings(&append_stats, c);
    } else if (strncmp(subcommand, "detail", 6) == 0) {
        char *subcmd_pos = subcommand + 6;
        if (settings.allow_detailed) {
            if (strncmp(subcmd_pos, " dump", 5) == 0) {
                int len;
                char *dump_buf = stats_prefix_dump(&len);
                if (dump_buf == NULL || len <= 0) {
                    write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
                    return ;
                } else {
                    append_stats("detailed", strlen("detailed"), dump_buf, len, c);
                    free(dump_buf);
                }
            } else if (strncmp(subcmd_pos, " on", 3) == 0) {
                settings.detail_enabled = 1;
            } else if (strncmp(subcmd_pos, " off", 4) == 0) {
                settings.detail_enabled = 0;
            } else {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
                return;
            }
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
            return;
        }
    } else if (strncmp(subcommand, "aggregate", 9) == 0) {
        server_stats(&append_stats, c, true);
    } else if (strncmp(subcommand, "topkeys", 7) == 0) {
        topkeys_t *tk = get_independent_stats(c)->topkeys;
        if (tk != NULL) {
            topkeys_stats(tk, c, current_time, append_stats);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            return;
        }
    /****** SPEC-OUT FUNCTIONS **********
    } else if (strncmp(subcommand, "prefix", 6) == 0) {
        char *prefix = subcommand + 7;
        int nprefix = nkey - 13;

        if (nprefix < 1 || nprefix > KEY_MAX_LENGTH) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
            return;
        }

        prefix_engine_stats prefix_data;
        ret = mc_engine.v1->get_prefix_stats(mc_engine.v0, c, prefix, nprefix, &prefix_data);

        if (ret == ENGINE_SUCCESS) {
            char buffer[1024];
            char *str = &buffer[0];
            *str = '\0';

            sprintf(str, "PREFIX hash_items=%"PRIu64"\r\n", prefix_data.hash_items);
            sprintf(str+strlen(str), "PREFIX hash_items_bytes=%"PRIu64"\r\n", prefix_data.hash_items_bytes);
            sprintf(str+strlen(str), "PREFIX prefix_items=%u\r\n", prefix_data.prefix_items);
            sprintf(str+strlen(str), "END");

            append_stats("prefix", strlen("prefix"), str, strlen(str), c);
        }
    } else if (strncmp(subcommand, "noprefix", 8) == 0) {
        prefix_engine_stats prefix_data;
        ret = mc_engine.v1->get_prefix_stats(mc_engine.v0, c, NULL, 0, &prefix_data);

        if (ret == ENGINE_SUCCESS) {
            char buffer[1024];
            char *str = &buffer[0];
            *str = '\0';

            sprintf(str, "NOPREFIX hash_items=%"PRIu64"\r\n", prefix_data.hash_items);
            sprintf(str+strlen(str), "NOPREFIX hash_items_bytes=%"PRIu64"\r\n", prefix_data.hash_items_bytes);
            sprintf(str+strlen(str), "NOPREFIX prefix_items=%u\r\n", prefix_data.prefix_items);
            sprintf(str+strlen(str), "NOPREFIX tot_prefix_items=%u\r\n", prefix_data.tot_prefix_items);
            sprintf(str+strlen(str), "END");

            append_stats("noprefix", strlen("noprefix"), str, strlen(str), c);
        }
    ************************************/
    } else {
        ret = mc_engine.v1->get_stats(mc_engine.v0, c, subcommand, nkey, append_stats);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        append_stats(NULL, 0, NULL, 0, c);
        write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
        c->dynamic_buffer.buffer = NULL;
        break;
    case ENGINE_ENOMEM:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    case ENGINE_PREFIX_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENOENT, 0);
        break;
    default:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    }
}

static void bin_read_chunk(conn *c, enum bin_substates next_substate, uint32_t chunk) {
    assert(c);
    c->substate = next_substate;
    c->rlbytes = chunk;

    /* Ok... do we have room for everything in our buffer? */
    ptrdiff_t offset = c->rcurr + sizeof(protocol_binary_request_header) - c->rbuf;
    if (c->rlbytes > c->rsize - offset) {
        size_t nsize = c->rsize;
        size_t size = c->rlbytes + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->rsize) {
            if (settings.verbose > 1) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c,
                    "%d: Need to grow buffer from %lu to %lu\n",
                    c->sfd, (unsigned long)c->rsize, (unsigned long)nsize);
            }
            char *newm = realloc(c->rbuf, nsize);
            if (newm == NULL) {
                if (settings.verbose) {
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                        "%d: Failed to grow buffer. closing connection\n", c->sfd);
                }
                conn_set_state(c, conn_closing);
                return;
            }

            c->rbuf= newm;
            /* rcurr should point to the same offset in the packet */
            c->rcurr = c->rbuf + offset - sizeof(protocol_binary_request_header);
            c->rsize = nsize;
        }
        if (c->rbuf != c->rcurr) {
            memmove(c->rbuf, c->rcurr, c->rbytes);
            c->rcurr = c->rbuf;
            if (settings.verbose > 1) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c,
                               "%d: Repack input buffer\n", c->sfd);
            }
        }
    }

    /* preserve the header in the buffer.. */
    c->ritem = c->rcurr + sizeof(protocol_binary_request_header);
    conn_set_state(c, conn_nread);
}

static void bin_read_key(conn *c, enum bin_substates next_substate, int extra) {
    bin_read_chunk(c, next_substate, c->keylen + extra);
}


/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(conn *c) {
    write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    if (settings.verbose) {
        mc_logger->log(EXTENSION_LOG_INFO, c,
                "%d: Protocol error (opcode %02x), close connection\n",
                c->sfd, c->binary_header.request.opcode);
    }
    c->write_and_go = conn_closing;
}

static void init_sasl_conn(conn *c) {
    assert(c);
    if (!c->sasl_conn) {
        int result=sasl_server_new("memcached",
                                   NULL, NULL, NULL, NULL,
                                   NULL, 0, &c->sasl_conn);
        if (result != SASL_OK) {
            if (settings.verbose) {
                mc_logger->log(EXTENSION_LOG_INFO, c,
                         "%d: Failed to initialize SASL conn.\n", c->sfd);
            }
            c->sasl_conn = NULL;
        }
    }
}

static void get_auth_data(const void *cookie, auth_data_t *data) {
    conn *c = (conn*)cookie;
    if (c->sasl_conn) {
        sasl_getprop(c->sasl_conn, SASL_USERNAME, (void*)&data->username);
#ifdef ENABLE_ISASL
        sasl_getprop(c->sasl_conn, ISASL_CONFIG, (void*)&data->config);
#endif
    }
}

#ifdef SASL_ENABLED
static void bin_list_sasl_mechs(conn *c) {
    init_sasl_conn(c);
    const char *result_string = NULL;
    unsigned int string_length = 0;
    int result=sasl_listmech(c->sasl_conn, NULL,
                             "",   /* What to prepend the string with */
                             " ",  /* What to separate mechanisms with */
                             "",   /* What to append to the string */
                             &result_string, &string_length,
                             NULL);
    if (result != SASL_OK) {
        /* Perhaps there's a better error for this... */
        if (settings.verbose) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                     "%d: Failed to list SASL mechanisms.\n", c->sfd);
        }
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        return;
    }
    write_bin_response(c, (char*)result_string, 0, 0, string_length);
}
#endif

struct sasl_tmp {
    int ksize;
    int vsize;
    char data[]; /* data + ksize == value */
};

static void process_bin_sasl_auth(conn *c) {
    assert(c->binary_header.request.extlen == 0);

    uint32_t nkey = c->binary_header.request.keylen;
    uint32_t vlen = 0;

    if (nkey <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - nkey;
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    if (nkey > MAX_SASL_MECH_LEN) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    char *key = binary_get_key(c);
    assert(key);

    size_t buffer_size = sizeof(struct sasl_tmp) + nkey + vlen + 2;
    struct sasl_tmp *data = calloc(sizeof(struct sasl_tmp) + buffer_size, 1);
    if (!data) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    data->ksize = nkey;
    data->vsize = vlen;
    memcpy(data->data, key, nkey);

    c->item = data;
    c->ritem = data->data + nkey;
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_reading_sasl_auth_data;
}

static void process_bin_complete_sasl_auth(conn *c) {
    const char *out = NULL;
    unsigned int outlen = 0;

    assert(c->item);
    init_sasl_conn(c);

    uint32_t nkey = c->binary_header.request.keylen;
    uint32_t vlen = 0;

    if (nkey <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - nkey;
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    struct sasl_tmp *stmp = c->item;
    char mech[nkey+1];
    memcpy(mech, stmp->data, nkey);
    mech[nkey] = 0x00;

    if (settings.verbose) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                "%d: mech: ``%s'' with %d bytes of data\n", c->sfd, mech, vlen);
    }

    const char *challenge = vlen == 0 ? NULL : (stmp->data + nkey);

    int result=-1;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
        result = sasl_server_start(c->sasl_conn, mech,
                                   challenge, vlen,
                                   &out, &outlen);
        break;
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        result = sasl_server_step(c->sasl_conn,
                                  challenge, vlen,
                                  &out, &outlen);
        break;
    default:
        assert(false); /* CMD should be one of the above */
        /* This code is pretty much impossible, but makes the compiler
           happier */
        if (settings.verbose) {
            mc_logger->log(EXTENSION_LOG_WARNING, c,
                    "%d: Unhandled command %d with challenge %s\n",
                    c->sfd, c->cmd, challenge);
        }
        break;
    }

    free(c->item);
    c->item = NULL;
    c->ritem = NULL;

    if (settings.verbose) {
        mc_logger->log(EXTENSION_LOG_INFO, c,
                       "%d: sasl result code:  %d\n", c->sfd, result);
    }

    switch(result) {
    case SASL_OK:
        write_bin_response(c, "Authenticated", 0, 0, strlen("Authenticated"));
        auth_data_t data;
        get_auth_data(c, &data);
        perform_callbacks(ON_AUTH, (const void*)&data, c);
        STATS_NOKEY(c, auth_cmds);
        break;
    case SASL_CONTINUE:
        add_bin_header(c, PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE, 0, 0, outlen);
        if(outlen > 0) {
            add_iov(c, out, outlen);
        }
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        break;
    default:
        if (settings.verbose) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                           "%d: Unknown sasl response:  %d\n", c->sfd, result);
        }
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        STATS_NOKEY2(c, auth_cmds, auth_errors);
    }
}

static bool authenticated(conn *c) {
    bool rv = false;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
        rv = true;
        break;
    default:
        if (c->sasl_conn) {
            const void *uname = NULL;
            sasl_getprop(c->sasl_conn, SASL_USERNAME, &uname);
            rv = uname != NULL;
        }
    }

    if (settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                "%d: authenticated() in cmd 0x%02x is %s\n",
                c->sfd, c->cmd, rv ? "true" : "false");
    }

    return rv;
}

static void process_bin_lop_create(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_lop_create* req = binary_get_request(c);
    req->message.body.exptime  = ntohl(req->message.body.exptime);
    req->message.body.maxcount = ntohl(req->message.body.maxcount);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d LOP CREATE ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " flags(%u) exptime(%d) maxcount(%d) ovflaction(%s) readable(%d)\n",
                req->message.body.flags, req->message.body.exptime, req->message.body.maxcount,
                get_ovflaction_str(req->message.body.ovflaction), req->message.body.readable);
    }

    item_attr attr_data;
    attr_data.flags   = req->message.body.flags;
    attr_data.exptime = realtime(req->message.body.exptime);
    attr_data.maxcount = req->message.body.maxcount;

    if ((req->message.body.ovflaction > 0) &&
        (req->message.body.ovflaction == OVFL_ERROR ||
         req->message.body.ovflaction == OVFL_HEAD_TRIM ||
         req->message.body.ovflaction == OVFL_TAIL_TRIM)) {
        attr_data.ovflaction = req->message.body.ovflaction;
    } else {
        attr_data.ovflaction = 0;
    }
    attr_data.readable = req->message.body.readable;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->list_struct_create(mc_engine.v0, c, key, nkey, &attr_data,
                                           c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_lop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, lop_create, key, nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_lop_create);
        if (ret == ENGINE_KEY_EEXISTS)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        else if (ret == ENGINE_PREFIX_ENAME)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

static void process_bin_lop_prepare_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd == PROTOCOL_BINARY_CMD_LOP_INSERT);

    char *key = binary_get_key(c);
    uint32_t nkey = c->binary_header.request.keylen;
    uint32_t vlen = 0;

    if (nkey + c->binary_header.request.extlen <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    /* fix byteorder in the request */
    protocol_binary_request_lop_insert* req = binary_get_request(c);
    req->message.body.index = ntohl(req->message.body.index);
    if (req->message.body.create) {
        req->message.body.exptime  = ntohl(req->message.body.exptime);
        req->message.body.maxcount = ntohl(req->message.body.maxcount);
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d LOP INSERT ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " Index(%d) NBytes(%d)", req->message.body.index, vlen);
        if (req->message.body.create) {
            fprintf(stderr, " %s", "Create");
        }
        fprintf(stderr, "\n");
    }

    eitem *elem;
    ENGINE_ERROR_CODE ret;
    if ((vlen + 2) > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        ret = mc_engine.v1->list_elem_alloc(mc_engine.v0, c, key, nkey, vlen+2, &elem);
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        stats_prefix_record_lop_insert(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        mc_engine.v1->get_list_elem_info(mc_engine.v0, c, elem, &info);
        c->ritem   = (char *)info.value;
        c->rlbytes = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = OPERATION_LOP_INSERT;
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        c->coll_index  = req->message.body.index;
        if (req->message.body.create) {
            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
            c->coll_attrp->flags    = req->message.body.flags;
            c->coll_attrp->exptime  = realtime(req->message.body.exptime);
            c->coll_attrp->maxcount = req->message.body.maxcount;
            c->coll_attrp->readable = 1;
        } else {
            c->coll_attrp = NULL;
        }
        conn_set_state(c, conn_nread);
        c->substate = bin_reading_lop_nread_complete;
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_lop_insert);
        if (ret == ENGINE_E2BIG)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_lop_insert_complete(conn *c) {
    assert(c->coll_op == OPERATION_LOP_INSERT);
    assert(c->coll_eitem != NULL);
    eitem *elem = (eitem *)c->coll_eitem;

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    eitem_info info;
    mc_engine.v1->get_list_elem_info(mc_engine.v0, c, elem, &info);
    memcpy((char*)info.value + info.nbytes - 2, "\r\n", 2);

    bool created;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->list_elem_insert(mc_engine.v0, c,
                                   c->coll_key, c->coll_nkey, c->coll_index, elem,
                                   c->coll_attrp, &created,
                                   c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_lop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, lop_insert, c->coll_key, c->coll_nkey);
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, lop_insert, c->coll_key, c->coll_nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_lop_insert);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EOVERFLOW)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EOVERFLOW, 0);
        else if (ret == ENGINE_EINDEXOOR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINDEXOOR, 0);
        else if (ret == ENGINE_PREFIX_ENAME)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    /* release the c->coll_eitem reference */
    mc_engine.v1->list_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
    c->coll_eitem = NULL;
}

static void process_bin_lop_nread_complete(conn *c) {
    //protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    assert(c != NULL);
    process_bin_lop_insert_complete(c);
}

static void process_bin_lop_delete(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_lop_delete* req = binary_get_request(c);
    req->message.body.from_index = ntohl(req->message.body.from_index);
    req->message.body.to_index   = ntohl(req->message.body.to_index);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d LOP DELETE ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " From_Index(%d) To_Index(%d)\n",
                req->message.body.from_index, req->message.body.to_index);
    }

    uint32_t del_count;
    bool     dropped;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->list_elem_delete(mc_engine.v0, c, key, nkey,
                                         req->message.body.from_index,
                                         req->message.body.to_index,
                                         (bool)req->message.body.drop,
                                         &del_count, &dropped,
                                         c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_lop_delete(key, nkey,
                                       (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, lop_delete, key, nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, lop_delete, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINDEXOOR, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, lop_delete, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_lop_delete);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

static void process_bin_lop_get(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    assert(c->cmd == PROTOCOL_BINARY_CMD_LOP_GET);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_lop_get* req = binary_get_request(c);
    int from_index = ntohl(req->message.body.from_index);
    int to_index   = ntohl(req->message.body.to_index);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d LOP GET ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " From_Index(%d) To_Index(%d) Delete(%s)\n",
                from_index, to_index,
                (req->message.body.delete ? "true" : "false"));
    }

    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t flags, i;
    bool     dropped;
    int      est_count;
    int      need_size;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    /* adjust list index */
    if (from_index > MAX_LIST_SIZE)           from_index = MAX_LIST_SIZE;
    else if (from_index < -(MAX_LIST_SIZE+1)) from_index = -(MAX_LIST_SIZE+1);
    if (to_index > MAX_LIST_SIZE)             to_index   = MAX_LIST_SIZE;
    else if (to_index < -(MAX_LIST_SIZE+1))   to_index   = -(MAX_LIST_SIZE+1);

    est_count = MAX_LIST_SIZE;
    if ((from_index >= 0 && to_index >= 0) || (from_index  < 0 && to_index < 0)) {
        est_count = (from_index <= to_index ? to_index - from_index + 1
                                            : from_index - to_index + 1);
        if (est_count > MAX_LIST_SIZE) est_count = MAX_LIST_SIZE;
    }
    need_size = est_count * (sizeof(eitem*)+sizeof(uint32_t));
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        return;
    }
    ret = mc_engine.v1->list_elem_get(mc_engine.v0, c, key, nkey,
                                      from_index, to_index,
                                      (bool)req->message.body.delete,
                                      (bool)req->message.body.drop,
                                      elem_array, &elem_count, &flags, &dropped,
                                      c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_lop_get(key, nkey,
                                    (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        protocol_binary_response_lop_get* rsp = (protocol_binary_response_lop_get*)c->wbuf;
        uint32_t *vlenptr = (uint32_t *)&elem_array[elem_count];
        uint32_t  bodylen;

        eitem_info info[elem_count];
        for (i = 0; i < elem_count; i++) {
            mc_engine.v1->get_list_elem_info(mc_engine.v0, c, elem_array[i], &info[i]);
        }

        bodylen = sizeof(rsp->message.body) + (elem_count * sizeof(uint32_t));
        for (i = 0; i < elem_count; i++) {
             bodylen += (info[i].nbytes - 2);
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), 0, bodylen);

        // add the flags and count
        rsp->message.body.flags = flags;
        rsp->message.body.count = htonl(elem_count);
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        // add value lengths
        for (i = 0; i < elem_count; i++) {
             vlenptr[i] = htonl(info[i].nbytes - 2);
        }
        add_iov(c, (char*)vlenptr, elem_count*sizeof(uint32_t));

        /* Add the data without CRLF */
        for (i = 0; i < elem_count; i++) {
            if (add_iov(c, info[i].value, info[i].nbytes - 2) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
        }

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, lop_get, key, nkey);
            /* Remember this command so we can garbage collect it later */
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_op     = OPERATION_LOP_GET;
            conn_set_state(c, conn_mwrite);
        } else {
            STATS_NOKEY(c, cmd_lop_get);
            mc_engine.v1->list_elem_release(mc_engine.v0, c, elem_array, elem_count);
            if (c->ewouldblock)
                c->ewouldblock = false;
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, lop_get, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINDEXOOR, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, lop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNREADABLE, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_lop_get);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_bin_sop_create(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_sop_create* req = binary_get_request(c);
    req->message.body.exptime  = ntohl(req->message.body.exptime);
    req->message.body.maxcount = ntohl(req->message.body.maxcount);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d SOP CREATE ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " flags(%u) exptime(%d) maxcount(%d) ovflaction(%s) readable(%d)\n",
                req->message.body.flags, req->message.body.exptime, req->message.body.maxcount,
                "error", req->message.body.readable);
    }

    item_attr attr_data;
    attr_data.flags = req->message.body.flags;
    attr_data.exptime = realtime(req->message.body.exptime);
    attr_data.maxcount = req->message.body.maxcount;
    attr_data.readable = req->message.body.readable;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->set_struct_create(mc_engine.v0, c, key, nkey, &attr_data,
                                          c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_sop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, sop_create, key, nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_sop_create);
        if (ret == ENGINE_KEY_EEXISTS)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        else if (ret == ENGINE_PREFIX_ENAME)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

static void process_bin_sop_prepare_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT ||
           c->cmd == PROTOCOL_BINARY_CMD_SOP_DELETE ||
           c->cmd == PROTOCOL_BINARY_CMD_SOP_EXIST);
    char *key = binary_get_key(c);
    uint32_t nkey = c->binary_header.request.keylen;
    uint32_t vlen = 0;

    if (nkey + c->binary_header.request.extlen <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    if (settings.verbose > 1) {
        if (c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT) {
            fprintf(stderr, "<%d SOP INSERT ", c->sfd);
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SOP_DELETE) {
            fprintf(stderr, "<%d SOP DELETE ", c->sfd);
        } else {
            fprintf(stderr, "<%d SOP EXIST ", c->sfd);
        }
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " NBytes(%d)", vlen);
        fprintf(stderr, "\n");
    }

    eitem *elem = NULL;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if ((vlen + 2) > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        if (c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT) {
            ret = mc_engine.v1->set_elem_alloc(mc_engine.v0, c, key, nkey, vlen+2, &elem);
        } else { /* PROTOCOL_BINARY_CMD_SOP_DELETE or PROTOCOL_BINARY_CMD_SOP_EXIST */
            if ((elem = (eitem *)malloc(sizeof(elem_value) + vlen + 2)) == NULL)
                ret = ENGINE_ENOMEM;
            else
                ((elem_value*)elem)->nbytes = vlen + 2;
        }
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        if (c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT)
            stats_prefix_record_sop_insert(key, nkey, false);
        else if (c->cmd == PROTOCOL_BINARY_CMD_SOP_DELETE)
            stats_prefix_record_sop_delete(key, nkey, false);
        else
            stats_prefix_record_sop_exist(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT) {
            eitem_info info;
            mc_engine.v1->get_set_elem_info(mc_engine.v0, c, elem, &info);
            c->ritem   = (char *)info.value;
            c->rlbytes = vlen;
         } else {
            c->ritem   = ((elem_value *)elem)->value;
            c->rlbytes = vlen;
         }
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        if (c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT) {
            protocol_binary_request_sop_insert* req = binary_get_request(c);
            if (req->message.body.create) {
                req->message.body.exptime  = ntohl(req->message.body.exptime);
                req->message.body.maxcount = ntohl(req->message.body.maxcount);
            }
            c->coll_op     = OPERATION_SOP_INSERT;
            if (req->message.body.create) {
                c->coll_attrp = &c->coll_attr_space; /* create if not exist */
                c->coll_attrp->flags    = req->message.body.flags;
                c->coll_attrp->exptime  = realtime(req->message.body.exptime);
                c->coll_attrp->maxcount = req->message.body.maxcount;
                c->coll_attrp->readable = 1;
            } else {
                c->coll_attrp = NULL;
            }
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SOP_DELETE) {
            protocol_binary_request_sop_delete* req = binary_get_request(c);
            c->coll_op     = OPERATION_SOP_DELETE;
            c->coll_drop   = (req->message.body.drop ? true : false);
        } else { /* PROTOCOL_BINARY_CMD_SOP_EXIST */
            c->coll_op     = OPERATION_SOP_EXIST;
        }
        conn_set_state(c, conn_nread);
        c->substate = bin_reading_sop_nread_complete;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        if (c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT) {
            STATS_NOKEY(c, cmd_sop_insert);
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SOP_DELETE) {
            STATS_NOKEY(c, cmd_sop_delete);
        } else {
            STATS_NOKEY(c, cmd_sop_exist);
        }

        if (ret == ENGINE_E2BIG)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_sop_insert_complete(conn *c) {
    assert(c->coll_eitem != NULL);
    eitem *elem = c->coll_eitem;

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    eitem_info info;
    mc_engine.v1->get_set_elem_info(mc_engine.v0, c, elem, &info);
    memcpy((char*)info.value + info.nbytes - 2, "\r\n", 2);

    bool created;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->set_elem_insert(mc_engine.v0, c,
                                  c->coll_key, c->coll_nkey, elem,
                                  c->coll_attrp, &created,
                                  c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_sop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, sop_insert, c->coll_key, c->coll_nkey);
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, sop_insert, c->coll_key, c->coll_nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_sop_insert);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EOVERFLOW)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EOVERFLOW, 0);
        else if (ret == ENGINE_ELEM_EEXISTS)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ELEM_EEXISTS, 0);
        else if (ret == ENGINE_PREFIX_ENAME)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    /* release the c->coll_eitem reference */
    mc_engine.v1->set_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
    c->coll_eitem = NULL;
}

static void process_bin_sop_delete_complete(conn *c) {
    assert(c->coll_eitem != NULL);

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    elem_value *elem = (elem_value *)c->coll_eitem;
    memcpy(elem->value + elem->nbytes - 2, "\r\n", 2);

    bool dropped;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->set_elem_delete(mc_engine.v0, c,
                                        c->coll_key, c->coll_nkey,
                                        elem->value, elem->nbytes,
                                        c->coll_drop, &dropped,
                                        c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_sop_delete(c->coll_key, c->coll_nkey,
                                       (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, sop_delete, c->coll_key, c->coll_nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, sop_delete, c->coll_key, c->coll_nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ELEM_ENOENT, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, sop_delete, c->coll_key, c->coll_nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_sop_delete);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    /* release the c->coll_eitem reference */
    free(c->coll_eitem);
    c->coll_eitem = NULL;
}

static void process_bin_sop_exist_complete(conn *c) {
    assert(c->coll_eitem != NULL);

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    elem_value *elem = (elem_value *)c->coll_eitem;
    memcpy(elem->value + elem->nbytes - 2, "\r\n", 2);

    bool exist;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->set_elem_exist(mc_engine.v0, c,
                                       c->coll_key, c->coll_nkey,
                                       elem->value, elem->nbytes,
                                       &exist, c->binary_header.request.vbucket);

    if (settings.detail_enabled) {
        stats_prefix_record_sop_exist(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        STATS_HITS(c, sop_exist, c->coll_key, c->coll_nkey);

        protocol_binary_response_sop_exist* rsp = (protocol_binary_response_sop_exist*)c->wbuf;
        rsp->message.body.exist = htonl((exist == true) ? 1 : 0);
        write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body));
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, sop_exist, c->coll_key, c->coll_nkey);
        if (ret == ENGINE_KEY_ENOENT)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNREADABLE, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_sop_exist);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    /* release the c->coll_eitem reference */
    free(c->coll_eitem);
    c->coll_eitem = NULL;
}

static void process_bin_sop_nread_complete(conn *c) {
    //protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    assert(c != NULL);

    if (c->cmd == PROTOCOL_BINARY_CMD_SOP_INSERT)
        process_bin_sop_insert_complete(c);
    else if (c->cmd == PROTOCOL_BINARY_CMD_SOP_DELETE)
        process_bin_sop_delete_complete(c);
    else if (c->cmd == PROTOCOL_BINARY_CMD_SOP_EXIST)
        process_bin_sop_exist_complete(c);
}

static void process_bin_sop_get(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    assert(c->cmd == PROTOCOL_BINARY_CMD_SOP_GET);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_sop_get* req = binary_get_request(c);
    uint32_t req_count = ntohl(req->message.body.count);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d SOP GET ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " Count(%d) Delete(%s)\n", req_count,
                (req->message.body.delete ? "true" : "false"));
    }

    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t flags, i;
    bool     dropped;
    int      need_size;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (req_count <= 0 || req_count > MAX_SET_SIZE) req_count = MAX_SET_SIZE;
    need_size = req_count * (sizeof(eitem*)+sizeof(uint32_t));
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        return;
    }

    ret = mc_engine.v1->set_elem_get(mc_engine.v0, c, key, nkey, req_count,
                                     (bool)req->message.body.delete,
                                     (bool)req->message.body.drop,
                                     elem_array, &elem_count, &flags, &dropped,
                                     c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_sop_get(key, nkey,
                                    (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        protocol_binary_response_sop_get* rsp = (protocol_binary_response_sop_get*)c->wbuf;
        uint32_t *vlenptr = (uint32_t *)&elem_array[elem_count];
        uint32_t  bodylen;

        eitem_info info[elem_count];
        for (i = 0; i < elem_count; i++) {
            mc_engine.v1->get_set_elem_info(mc_engine.v0, c, elem_array[i], &info[i]);
        }

        bodylen = sizeof(rsp->message.body) + (elem_count * sizeof(uint32_t));
        for (i = 0; i < elem_count; i++) {
             bodylen += (info[i].nbytes - 2);
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), 0, bodylen);

        // add the flags and count
        rsp->message.body.flags = flags;
        rsp->message.body.count = htonl(elem_count);
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        // add value lengths
        for (i = 0; i < elem_count; i++) {
             vlenptr[i] = htonl(info[i].nbytes - 2);
        }
        add_iov(c, (char*)vlenptr, elem_count*sizeof(uint32_t));

        /* Add the data without CRLF */
        for (i = 0; i < elem_count; i++) {
            if (add_iov(c, info[i].value, info[i].nbytes - 2) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
        }

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, sop_get, key, nkey);
            /* Remember this command so we can garbage collect it later */
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_op     = OPERATION_SOP_GET;
            conn_set_state(c, conn_mwrite);
        } else {
            STATS_NOKEY(c, cmd_sop_get);
            mc_engine.v1->set_elem_release(mc_engine.v0, c, elem_array, elem_count);
            if (c->ewouldblock)
                c->ewouldblock = false;
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, sop_get, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ELEM_ENOENT, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, sop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNREADABLE, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_sop_get);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_bin_bop_create(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_bop_create* req = binary_get_request(c);
    req->message.body.exptime  = ntohl(req->message.body.exptime);
    req->message.body.maxcount = ntohl(req->message.body.maxcount);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d BOP CREATE ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " flags(%u) exptime(%d) maxcount(%d) ovflaction(%s) readable(%d)\n",
                req->message.body.flags, req->message.body.exptime, req->message.body.maxcount,
                get_ovflaction_str(req->message.body.ovflaction), req->message.body.readable);
    }

    item_attr attr_data;
    attr_data.flags = req->message.body.flags;
    attr_data.exptime = realtime(req->message.body.exptime);
    attr_data.maxcount = req->message.body.maxcount;

    if ((req->message.body.ovflaction > 0) &&
        (req->message.body.ovflaction == OVFL_ERROR ||
         req->message.body.ovflaction == OVFL_SMALLEST_TRIM ||
         req->message.body.ovflaction == OVFL_LARGEST_TRIM ||
         req->message.body.ovflaction == OVFL_SMALLEST_SILENT_TRIM ||
         req->message.body.ovflaction == OVFL_LARGEST_SILENT_TRIM)) {
        attr_data.ovflaction = req->message.body.ovflaction;
    } else {
        attr_data.ovflaction = 0;
    }
    attr_data.readable = req->message.body.readable;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_struct_create(mc_engine.v0, c, key, nkey, &attr_data,
                                            c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }
    if (settings.detail_enabled) {
        stats_prefix_record_bop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, bop_create, key, nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_create);
        if (ret == ENGINE_KEY_EEXISTS)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        else if (ret == ENGINE_PREFIX_ENAME)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

static void process_bin_bop_prepare_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd == PROTOCOL_BINARY_CMD_BOP_INSERT ||
           c->cmd == PROTOCOL_BINARY_CMD_BOP_UPSERT);
    char *key = binary_get_key(c);
    uint32_t nkey = c->binary_header.request.keylen;
    uint32_t vlen = 0;

    if (nkey + c->binary_header.request.extlen <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    /* fix byteorder in the request */
    protocol_binary_request_bop_insert* req = binary_get_request(c);
    if (req->message.body.nbkey == 0) {
        uint64_t bkey_temp;
        memcpy((uint8_t*)&bkey_temp, req->message.body.bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(req->message.body.bkey, (uint8_t*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)req->message.body.bkey = ntohll(*(uint64_t*)req->message.body.bkey);
    }
    if (req->message.body.create) {
        req->message.body.exptime  = ntohl(req->message.body.exptime);
        req->message.body.maxcount = ntohl(req->message.body.maxcount);
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d BOP %s ",
                c->sfd, (c->cmd == PROTOCOL_BINARY_CMD_BOP_INSERT ? "INSERT" : "UPSERT"));
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " NBKey(%d) NEFlag(%d) NBytes(%d)",
                req->message.body.nbkey, req->message.body.neflag, vlen);
        if (req->message.body.create) {
            fprintf(stderr, " %s", "Create");
        }
        fprintf(stderr, "\n");
    }

    eitem *elem;

    ENGINE_ERROR_CODE ret;
    if ((vlen + 2) > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        ret = mc_engine.v1->btree_elem_alloc(mc_engine.v0, c, key, nkey,
                                             req->message.body.nbkey,
                                             req->message.body.neflag, vlen+2,
                                             &elem);
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        stats_prefix_record_bop_insert(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem, &info);
        if (info.nscore == 0) {
            memcpy((void*)info.score, req->message.body.bkey, sizeof(uint64_t));
        } else {
            memcpy((void*)info.score, req->message.body.bkey, info.nscore);
        }
        if (info.neflag > 0) {
            memcpy((void*)info.eflag, req->message.body.eflag, info.neflag);
        }

        c->ritem   = (char *)info.value;
        c->rlbytes = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = (c->cmd == PROTOCOL_BINARY_CMD_BOP_INSERT ? OPERATION_BOP_INSERT
                                                                   : OPERATION_BOP_UPSERT);
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        if (req->message.body.create) {
            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
            c->coll_attrp->flags    = req->message.body.flags;
            c->coll_attrp->exptime  = realtime(req->message.body.exptime);
            c->coll_attrp->maxcount = req->message.body.maxcount;
            c->coll_attrp->readable = 1;
        } else {
            c->coll_attrp = NULL;
        }
        conn_set_state(c, conn_nread);
        c->substate = bin_reading_bop_nread_complete;
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_insert);
        if (ret == ENGINE_E2BIG)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_bop_insert_complete(conn *c) {
    assert(c->coll_op == OPERATION_BOP_INSERT ||
           c->coll_op == OPERATION_BOP_UPSERT);
    assert(c->coll_eitem != NULL);
    eitem *elem = (eitem *)c->coll_eitem;

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    eitem_info info;
    mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem, &info);
    memcpy((char*)info.value + info.nbytes - 2, "\r\n", 2);

    bool created;
    bool replaced;
    bool replace_if_exist = (c->coll_op == OPERATION_BOP_UPSERT ? true : false);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_insert(mc_engine.v0, c,
                                   c->coll_key, c->coll_nkey, elem, replace_if_exist,
                                   c->coll_attrp, &replaced, &created, NULL,
                                   c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, bop_insert, c->coll_key, c->coll_nkey);
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, bop_insert, c->coll_key, c->coll_nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_bop_insert);

        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EBADBKEY)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADBKEY, 0);
        else if (ret == ENGINE_EOVERFLOW)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EOVERFLOW, 0);
        else if (ret == ENGINE_EBKEYOOR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBKEYOOR, 0);
        else if (ret == ENGINE_ELEM_EEXISTS)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ELEM_EEXISTS, 0);
        else if (ret == ENGINE_PREFIX_ENAME)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENAME, 0);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    /* release the c->coll_eitem reference */
    mc_engine.v1->btree_elem_release(mc_engine.v0, c, &c->coll_eitem, 1);
    c->coll_eitem = NULL;
}

static void process_bin_bop_nread_complete(conn *c) {
    //protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    assert(c != NULL);
    process_bin_bop_insert_complete(c);
}

static void process_bin_bop_update_complete(conn *c) {
    assert(c->coll_op == OPERATION_BOP_UPDATE);
    char *new_value;
    int  new_nbytes;
    eflag_update *eupdate_ptr;

    if (c->coll_eitem != NULL) {
        elem_value *elem = (elem_value *)c->coll_eitem;
        memcpy(elem->value + elem->nbytes - 2, "\r\n", 2);
        new_value  = elem->value;
        new_nbytes = elem->nbytes;
    } else {
        new_value  = NULL;
        new_nbytes = 0;
    }

    eupdate_ptr = (c->coll_eupdate.neflag == EFLAG_NULL ? NULL : &c->coll_eupdate);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_update(mc_engine.v0, c,
                                       c->coll_key, c->coll_nkey,
                                       &c->coll_bkrange, eupdate_ptr,
                                       new_value, new_nbytes,
                                       c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_update(c->coll_key, c->coll_nkey,
                                       (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, bop_update, c->coll_key, c->coll_nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_update, c->coll_key, c->coll_nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ELEM_ENOENT, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, bop_update, c->coll_key, c->coll_nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_bop_update);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EBADBKEY)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADBKEY, 0);
        else if (ret == ENGINE_EBADEFLAG)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADEFLAG, 0);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    if (c->coll_eitem != NULL) {
        free((void*)c->coll_eitem);
        c->coll_eitem = NULL;
    }
}

static void process_bin_bop_update_prepare_nread(conn *c) {
    assert(c != NULL);
    assert(c->cmd == PROTOCOL_BINARY_CMD_BOP_UPDATE);
    char *key = binary_get_key(c);

    uint32_t nkey = 0;
    uint32_t vlen = 0;
    int  real_nbkey;

    nkey = c->binary_header.request.keylen;
    if (nkey + c->binary_header.request.extlen <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    /* fix byteorder in the request */
    protocol_binary_request_bop_update* req = binary_get_request(c);
    if (req->message.body.nbkey == 0) {
        uint64_t bkey_temp;
        memcpy((uint8_t*)&bkey_temp, req->message.body.bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(req->message.body.bkey, (uint8_t*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)req->message.body.bkey = ntohll(*(uint64_t*)req->message.body.bkey);
        real_nbkey = 8;
    } else {
        real_nbkey = req->message.body.nbkey;
    }
    /* build bkey range */
    memcpy(c->coll_bkrange.from_bkey, req->message.body.bkey, real_nbkey);
    c->coll_bkrange.from_nbkey = req->message.body.nbkey;
    c->coll_bkrange.to_nbkey   = BKEY_NULL;
    /* build elem update */
    c->coll_eupdate.fwhere = req->message.body.fwhere;
    c->coll_eupdate.bitwop = req->message.body.bitwop;
    c->coll_eupdate.neflag = req->message.body.neflag;
    if (req->message.body.neflag > 0) {
        memcpy(c->coll_eupdate.eflag, req->message.body.eflag, req->message.body.neflag);
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d BOP UPDATE", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " NBKey(%d) NEFlag(%d) NBytes(%d)",
                req->message.body.nbkey, req->message.body.neflag, vlen);
        fprintf(stderr, "\n");
    }

    if (req->message.body.novalue) {
        c->coll_eitem  = NULL;
        c->coll_ecount = 0;
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        c->coll_op     = OPERATION_BOP_UPDATE;
        process_bin_bop_update_complete(c);
        return;
    }

    eitem *elem = NULL;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if ((vlen + 2) > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        if ((elem = (eitem *)malloc(sizeof(elem_value) + vlen + 2)) == NULL)
            ret = ENGINE_ENOMEM;
        else
            ((elem_value*)elem)->nbytes = vlen + 2;
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        stats_prefix_record_bop_update(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        c->ritem       = ((elem_value *)elem)->value;
        c->rlbytes     = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        c->coll_op     = OPERATION_BOP_UPDATE;
        conn_set_state(c, conn_nread);
        c->substate = bin_reading_bop_update_nread_complete;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_update);
        if (ret == ENGINE_E2BIG)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_bop_delete(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_bop_delete* req = binary_get_request(c);
    bkey_range   *bkrange = &req->message.body.bkrange;
    eflag_filter *efilter = &req->message.body.efilter;
    if (bkrange->from_nbkey == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->from_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->from_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->from_bkey = ntohll(*(uint64_t*)bkrange->from_bkey);
    }
    if (bkrange->to_nbkey   == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->to_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->to_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->to_bkey   = ntohll(*(uint64_t*)bkrange->to_bkey);
    }
    req->message.body.count = ntohl(req->message.body.count);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d BOP DELETE ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
    }

    uint32_t del_count;
    uint32_t acc_count; /* access count */
    bool     dropped;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_delete(mc_engine.v0, c, key, nkey,
                                          bkrange, efilter, req->message.body.count,
                                          (bool)req->message.body.drop,
                                          &del_count, &acc_count, &dropped,
                                          c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_delete(key, nkey,
                                       (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, bop_delete, key, nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_delete, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ELEM_ENOENT, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, bop_delete, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_bop_delete);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EBADBKEY)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADBKEY, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

static void process_bin_bop_get(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    assert(c->cmd == PROTOCOL_BINARY_CMD_BOP_GET);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_bop_get* req = binary_get_request(c);
    bkey_range   *bkrange = &req->message.body.bkrange;
    eflag_filter *efilter = &req->message.body.efilter;
    if (bkrange->from_nbkey == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->from_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->from_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->from_bkey = ntohll(*(uint64_t*)bkrange->from_bkey);
    }
    if (bkrange->to_nbkey   == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->to_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->to_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->to_bkey   = ntohll(*(uint64_t*)bkrange->to_bkey);
    }
    req->message.body.offset = ntohl(req->message.body.offset);
    req->message.body.count  = ntohl(req->message.body.count);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d BOP GET ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " Offset(%u) Count(%u) Delete(%s)\n",
                req->message.body.offset, req->message.body.count,
                (req->message.body.delete ? "true" : "false"));
    }

    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t access_count;
    uint32_t flags, i;
    bool     dropped_trimmed;
    int      est_count;
    int      need_size;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    est_count = MAX_BTREE_SIZE;
    if (req->message.body.count > 0 && req->message.body.count < MAX_BTREE_SIZE) {
        est_count = req->message.body.count;
        if (est_count % 2) est_count += 1;
    }
    need_size = est_count * (sizeof(eitem*)+MAX_BKEY_LENG+MAX_EFLAG_LENG+sizeof(uint32_t));
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        return;
    }

    ret = mc_engine.v1->btree_elem_get(mc_engine.v0, c, key, nkey,
                                       bkrange, efilter,
                                       req->message.body.offset,
                                       req->message.body.count,
                                       (bool)req->message.body.delete,
                                       (bool)req->message.body.drop,
                                       elem_array, &elem_count, &access_count,
                                       &flags, &dropped_trimmed,
                                       c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_get(key, nkey,
                                    (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        protocol_binary_response_bop_get* rsp = (protocol_binary_response_bop_get*)c->wbuf;
        uint64_t *bkeyptr = ((elem_count % 2) == 0 /* for 8 byte align */
                             ? (uint64_t *)&elem_array[elem_count]
                             : (uint64_t *)&elem_array[elem_count+1]);
        uint32_t *vlenptr = (uint32_t *)((char*)bkeyptr + (sizeof(uint64_t) * elem_count));
        uint32_t  bodylen;

        eitem_info info[elem_count];
        for (i = 0; i < elem_count; i++) {
            mc_engine.v1->get_btree_elem_info(mc_engine.v0, c,
                                                    elem_array[i], &info[i]);
        }

        bodylen = sizeof(rsp->message.body) + (elem_count * (sizeof(uint64_t)+sizeof(uint32_t)));
        for (i = 0; i < elem_count; i++) {
             bodylen += (info[i].nbytes - 2);
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), 0, bodylen);

        // add the flags and count
        rsp->message.body.flags = flags;
        rsp->message.body.count = htonl(elem_count);
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        // add value lengths
        for (i = 0; i < elem_count; i++) {
             bkeyptr[i] = htonll(*(uint64_t*)info[i].score);
             vlenptr[i] = htonl(info[i].nbytes - 2);
        }
        add_iov(c, (char*)bkeyptr, elem_count*(sizeof(uint64_t)+sizeof(uint32_t)));

        /* Add the data without CRLF */
        for (i = 0; i < elem_count; i++) {
            if (add_iov(c, info[i].value, info[i].nbytes - 2) != 0) {
                ret = ENGINE_ENOMEM;
                break;
            }
        }

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_get, key, nkey);
            /* Remember this command so we can garbage collect it later */
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_op     = OPERATION_BOP_GET;
            conn_set_state(c, conn_mwrite);
        } else {
            STATS_NOKEY(c, cmd_bop_get);
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
            if (c->ewouldblock)
                c->ewouldblock = false;
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_get, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ELEM_ENOENT, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_EBKEYOOR:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, bop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        else if (ret == ENGINE_EBKEYOOR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBKEYOOR, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNREADABLE, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_bop_get);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EBADBKEY)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADBKEY, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_bin_bop_count(conn *c) {
    assert(c != NULL);
    assert(c->cmd == PROTOCOL_BINARY_CMD_BOP_COUNT);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    protocol_binary_request_bop_count* req = binary_get_request(c);
    bkey_range   *bkrange = &req->message.body.bkrange;
    eflag_filter *efilter = &req->message.body.efilter;
    if (bkrange->from_nbkey == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->from_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->from_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->from_bkey = ntohll(*(uint64_t*)bkrange->from_bkey);
    }
    if (bkrange->to_nbkey   == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->to_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->to_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->to_bkey   = ntohll(*(uint64_t*)bkrange->to_bkey);
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d BOP COUNT", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, "\n");
    }

    uint32_t elem_count;
    uint32_t access_count;
    uint32_t flags = 0;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_count(mc_engine.v0, c, key, nkey,
                                         bkrange, efilter, &elem_count, &access_count,
                                         c->binary_header.request.vbucket);

    if (settings.detail_enabled) {
        stats_prefix_record_bop_count(key, nkey, (ret==ENGINE_SUCCESS));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, bop_count, key, nkey);
        protocol_binary_response_bop_count* rsp = (protocol_binary_response_bop_count*)c->wbuf;
        // add the flags and count
        rsp->message.body.flags = flags;
        rsp->message.body.count = htonl(elem_count);
        write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body));
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, bop_count, key, nkey);
        if (ret == ENGINE_KEY_ENOENT)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNREADABLE, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_bop_count);
        if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EBADBKEY)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADBKEY, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
static void process_bin_bop_prepare_nread_keys(conn *c) {
    assert(c != NULL);
    assert(c->cmd == PROTOCOL_BINARY_CMD_BOP_MGET || c->cmd == PROTOCOL_BINARY_CMD_BOP_SMGET);

    char *key = binary_get_key(c);
    uint32_t nkey = c->binary_header.request.keylen;
    size_t vlen = 0;

    if (nkey + c->binary_header.request.extlen <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    /* fix byteorder in the request */
    protocol_binary_request_bop_mkeys* req = binary_get_request(c);
    bkey_range   *bkrange = &req->message.body.bkrange;
    if (bkrange->from_nbkey == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->from_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->from_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->from_bkey = ntohll(*(uint64_t*)bkrange->from_bkey);
    }
    if (bkrange->to_nbkey   == 0) {
        uint64_t bkey_temp;
        memcpy((unsigned char*)&bkey_temp, bkrange->to_bkey, sizeof(uint64_t));
        bkey_temp = ntohll(bkey_temp);
        memcpy(bkrange->to_bkey, (unsigned char*)&bkey_temp, sizeof(uint64_t));
        //*(uint64_t*)bkrange->to_bkey   = ntohll(*(uint64_t*)bkrange->to_bkey);
    }
    req->message.body.req_offset = ntohl(req->message.body.req_offset);
    req->message.body.req_count  = ntohl(req->message.body.req_count);
    req->message.body.key_count  = ntohl(req->message.body.key_count);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d BOP %s ", c->sfd, (c->cmd==PROTOCOL_BINARY_CMD_BOP_MGET ? "MGET" : "SMGET"));
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, " ReqOffset(%u) ReqCount(%u) KeyCount(%u)\n",
                req->message.body.req_offset, req->message.body.req_count, req->message.body.key_count);
    }

    eitem *elem=NULL;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    int need_size = 0;
    do {
        /* validation checking on arguments */
        if (req->message.body.key_count < 1 || vlen < 1 || req->message.body.req_count < 1) {
            ret = ENGINE_EBADVALUE; break;
        }
#ifdef SUPPORT_BOP_MGET
        if (c->cmd == PROTOCOL_BINARY_CMD_BOP_MGET) {
            int bmget_count;
            int elem_array_size;

            if (req->message.body.key_count > MAX_BMGET_KEY_COUNT ||
                req->message.body.req_count > MAX_BMGET_ELM_COUNT) {
                ret = ENGINE_EBADVALUE; break;
            }
            bmget_count = req->message.body.key_count * req->message.body.req_count;
            elem_array_size = bmget_count * sizeof(eitem*);

            need_size = elem_array_size;
        }
#endif
#ifdef SUPPORT_BOP_SMGET
        if (c->cmd == PROTOCOL_BINARY_CMD_BOP_SMGET) {
#if 1 // JHPARK_OLD_SMGET_INTERFACE
          if (c->coll_smgmode == 0) {
            int smget_count;
            int elem_array_size; /* elem pointer array where the found elements will be saved */
            int kmis_array_size; /* key index array where the missed key indexes are to be saved */
            int elem_rshdr_size; /* the size of result header about the found elems */
            int kmis_rshdr_size; /* the size of result header about the missed keys */

            if (req->message.body.key_count > MAX_SMGET_KEY_COUNT ||
                (req->message.body.req_offset + req->message.body.req_count) > MAX_SMGET_REQ_COUNT) {
                ret = ENGINE_EBADVALUE; break;
            }
            smget_count = req->message.body.req_offset + req->message.body.req_count;
            elem_array_size = smget_count * (sizeof(eitem*) + (2*sizeof(uint32_t)));
            kmis_array_size = req->message.body.key_count * sizeof(uint32_t);
            elem_rshdr_size = smget_count * (sizeof(uint64_t) + (3*sizeof(uint32_t)));
            kmis_rshdr_size = req->message.body.key_count * sizeof(uint32_t);
            need_size = elem_array_size + kmis_array_size + elem_rshdr_size + kmis_rshdr_size;
          } else {
#endif
#ifdef JHPARK_NEW_SMGET_INTERFACE
            int elem_array_size; /* smget element array size */
            int ehit_array_size; /* smget hitted elem array size */
            int emis_array_size; /* element missed keys array size */
            int elem_rshdr_size; /* the size of result header about the found elems */
            int emis_rshdr_size; /* the size of result header about the missed keys */
#else
            int smget_count;
            int elem_array_size; /* elem pointer array where the found elements will be saved */
            int kmis_array_size; /* key index array where the missed key indexes are to be saved */
            int elem_rshdr_size; /* the size of result header about the found elems */
            int kmis_rshdr_size; /* the size of result header about the missed keys */
#endif

            if (req->message.body.key_count > MAX_SMGET_KEY_COUNT ||
                (req->message.body.req_offset + req->message.body.req_count) > MAX_SMGET_REQ_COUNT) {
                ret = ENGINE_EBADVALUE; break;
            }
#ifdef JHPARK_NEW_SMGET_INTERFACE
            elem_array_size = (req->message.body.req_count + req->message.body.key_count) * sizeof(eitem*);
            ehit_array_size = req->message.body.req_count * sizeof(smget_ehit_t);
            emis_array_size = req->message.body.key_count * sizeof(smget_emis_t);
            elem_rshdr_size = req->message.body.req_count * (sizeof(uint64_t) + (3*sizeof(uint32_t)));
            emis_rshdr_size = req->message.body.key_count * sizeof(uint32_t);
            need_size = elem_array_size + ehit_array_size + emis_array_size
                      + elem_rshdr_size + emis_rshdr_size;
#else
            smget_count = req->message.body.req_offset + req->message.body.req_count;
            elem_array_size = smget_count * (sizeof(eitem*) + (2*sizeof(uint32_t)));
            kmis_array_size = req->message.body.key_count * sizeof(uint32_t);
            elem_rshdr_size = smget_count * (sizeof(uint64_t) + (3*sizeof(uint32_t)));
            kmis_rshdr_size = req->message.body.key_count * sizeof(uint32_t);
            need_size = elem_array_size + kmis_array_size + elem_rshdr_size + kmis_rshdr_size;
#endif
#if 1 // JHPARK_OLD_SMGET_INTERFACE
          }
#endif
        }
#endif
        assert(need_size > 0);

        if ((elem = (eitem *)malloc(need_size)) == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            int kmem_size = GET_8ALIGN_SIZE(vlen+2)
                          + (sizeof(token_t) * req->message.body.key_count);
            if ((c->coll_strkeys = malloc(kmem_size)) == NULL) {
                free((void*)elem);
                ret = ENGINE_ENOMEM;
            } else {
                c->coll_bkrange = req->message.body.bkrange;
                c->coll_efilter = req->message.body.efilter;
                c->coll_roffset = req->message.body.req_offset;
                c->coll_rcount  = req->message.body.req_count;
                c->coll_numkeys = req->message.body.key_count;
                c->coll_lenkeys = vlen + 2;
            }
        }
    } while(0);

    switch (ret) {
    case ENGINE_SUCCESS:
        c->ritem       = (char *)c->coll_strkeys;
        c->rlbytes     = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 0;
        c->coll_op     = (c->cmd==PROTOCOL_BINARY_CMD_BOP_MGET ? OPERATION_BOP_MGET : OPERATION_BOP_SMGET);
        conn_set_state(c, conn_nread);
        c->substate = bin_reading_bop_nread_keys_complete;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        if (ret == ENGINE_EBADVALUE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADVALUE, vlen);
        else if (ret == ENGINE_ENOMEM)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}
#endif

#ifdef SUPPORT_BOP_MGET
static void process_bin_bop_mget_complete(conn *c) {
    assert(c->coll_op == OPERATION_BOP_MGET);
    assert(c->coll_eitem != NULL);

    ENGINE_ERROR_CODE ret = ENGINE_ENOTSUP;
    write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            free((void *)c->coll_strkeys);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

#ifdef SUPPORT_BOP_SMGET
#if 1 // JHPARK_OLD_SMGET_INTERFACE
static void process_bin_bop_smget_complete_old(conn *c) {
    int smget_count = c->coll_roffset + c->coll_rcount;
    int kmis_array_size = c->coll_numkeys * sizeof(uint32_t);
    char *resultptr;
    char *vptr = (char*)c->coll_strkeys;
    char delimiter = ',';
    token_t *keys_array = (token_t*)(vptr + GET_8ALIGN_SIZE(c->coll_lenkeys));

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    memcpy(vptr + c->coll_lenkeys - 2, "\r\n", 2);

    uint32_t  kmis_count = 0;
    uint32_t  elem_count = 0;
    eitem   **elem_array = (eitem  **)c->coll_eitem;
    uint32_t *kfnd_array = (uint32_t*)((char*)elem_array + (smget_count*sizeof(eitem*)));
    uint32_t *flag_array = (uint32_t*)((char*)kfnd_array + (smget_count*sizeof(uint32_t)));
    uint32_t *kmis_array = (uint32_t*)((char*)flag_array + (smget_count*sizeof(uint32_t)));

    resultptr = ((char *)kmis_array + kmis_array_size);

    bool trimmed;
    bool duplicated;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    int ntokens = tokenize_keys(vptr, c->coll_lenkeys, delimiter, c->coll_numkeys, keys_array);
    if (ntokens == -1) {
        ret = ENGINE_EBADVALUE;
    } else {
        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }
        ret = mc_engine.v1->btree_elem_smget_old(mc_engine.v0, c,
                                     keys_array, c->coll_numkeys,
                                     &c->coll_bkrange,
                                     (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                                     c->coll_roffset, c->coll_rcount,
                                     elem_array, kfnd_array, flag_array, &elem_count,
                                     kmis_array, &kmis_count, &trimmed, &duplicated,
                                     c->binary_header.request.vbucket);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        protocol_binary_response_bop_smget* rsp = (protocol_binary_response_bop_smget*)c->wbuf;
        uint32_t real_elem_hdr_size = elem_count * (sizeof(uint64_t) + (3*sizeof(uint32_t)));
        uint32_t real_kmis_hdr_size = kmis_count * sizeof(uint32_t);
        uint32_t bodylen, i;
        uint64_t *bkeyptr;
        uint32_t *vlenptr;
        uint32_t *flagptr;
        uint32_t *klenptr;

        if (((long)resultptr % 8) != 0) /* NOT aligned */
            resultptr += (8 - ((long)resultptr % 8));
        bkeyptr = (uint64_t *)resultptr;
        vlenptr = (uint32_t *)((char*)bkeyptr + (sizeof(uint64_t) * elem_count));
        flagptr = (uint32_t *)((char*)vlenptr + (sizeof(uint32_t) * elem_count));
        klenptr = (uint32_t *)((char*)flagptr + (sizeof(uint32_t) * elem_count));

        eitem_info info[elem_count+1]; /* elem_count might be 0. */
        for (i = 0; i < elem_count; i++) {
            mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem_array[i], &info[i]);
        }

        bodylen = sizeof(rsp->message.body);
        bodylen += (real_elem_hdr_size + real_kmis_hdr_size);
        for (i = 0; i < elem_count; i++) {
             bodylen += (info[i].nbytes - 2);
             bodylen += keys_array[kfnd_array[i]].length;
        }
        for (i = 0; i < kmis_count; i++) {
             bodylen += keys_array[kmis_array[i]].length;
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), 0, bodylen);

        // add the flags and count
        rsp->message.body.elem_count = htonl(elem_count);
#if 1 /* JHPARK_NEW_SMGET_INTERFACE */
        rsp->message.body.miss_count = htonl(kmis_count);
#else
        rsp->message.body.kmis_count = htonl(kmis_count);
#endif
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        // add value lengths
        for (i = 0; i < elem_count; i++) {
             bkeyptr[i] = htonll(*(uint64_t*)info[i].score);
             vlenptr[i] = htonl(info[i].nbytes - 2);
             flagptr[i] = flag_array[i];
             klenptr[i] = htonl(keys_array[kfnd_array[i]].length);
        }
        for (i = 0; i < kmis_count; i++) {
             klenptr[elem_count+i] = htonl(keys_array[kmis_array[i]].length);
        }
        if (add_iov(c, (char*)bkeyptr, real_elem_hdr_size+real_kmis_hdr_size) != 0) {
            ret = ENGINE_ENOMEM;
        }

        /* Add the data without CRLF */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < elem_count; i++) {
                if (add_iov(c, info[i].value, info[i].nbytes - 2) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
        /* Add the found key */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < elem_count; i++) {
                if (add_iov(c, keys_array[kfnd_array[i]].value,
                               keys_array[kfnd_array[i]].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
        /* Add the missed key */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < kmis_count; i++) {
                if (add_iov(c, keys_array[kmis_array[i]].value,
                               keys_array[kmis_array[i]].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }

        if (ret == ENGINE_SUCCESS) {
            /* Remember this command so we can garbage collect it later */
            /* c->coll_eitem  = (void *)elem_array; */
            STATS_NOKEY2(c, cmd_bop_smget, bop_smget_oks);
            c->coll_ecount = elem_count;
            c->coll_op     = OPERATION_BOP_SMGET;
            conn_set_state(c, conn_mwrite);
        } else {
            STATS_NOKEY(c, cmd_bop_smget);
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        }
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_smget);
        if (ret == ENGINE_EBADVALUE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADVALUE, 0);
        else if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EBADBKEY)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADBKEY, 0);
        else if (ret == ENGINE_EBADATTR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADATTR, 0);
        else if (ret == ENGINE_EBKEYOOR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBKEYOOR, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            free((void *)c->coll_strkeys);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

static void process_bin_bop_smget_complete(conn *c) {
    assert(c->coll_eitem != NULL);
#if 1 // JHPARK_OLD_SMGET_INTERFACE
    if (c->coll_smgmode == 0) {
        process_bin_bop_smget_complete_old(c);
        return;
    }
#endif
#ifdef JHPARK_NEW_SMGET_INTERFACE
    smget_result_t smres;
#else
    int smget_count = c->coll_roffset + c->coll_rcount;
#if 1 // JHPARK_OLD_SMGET_INTERFACE
#else
    int elem_array_size = smget_count * (sizeof(eitem*) + (2*sizeof(uint32_t)));
#endif
    int kmis_array_size = c->coll_numkeys * sizeof(uint32_t);
#endif
    char *resultptr;
    char *vptr = (char*)c->coll_strkeys;
    char delimiter = ',';
    token_t *keys_array = (token_t*)(vptr + GET_8ALIGN_SIZE(c->coll_lenkeys));

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    memcpy(vptr + c->coll_lenkeys - 2, "\r\n", 2);

#ifdef JHPARK_NEW_SMGET_INTERFACE
    smres.elem_array = (eitem **)c->coll_eitem;
    smres.elem_kinfo = (smget_ehit_t *)&smres.elem_array[c->coll_rcount+c->coll_numkeys];
    smres.miss_kinfo = (smget_emis_t *)&smres.elem_kinfo[c->coll_rcount];

    resultptr = (char *)&smres.miss_kinfo[c->coll_numkeys];
#else
    uint32_t  kmis_count = 0;
    uint32_t  elem_count = 0;
    eitem   **elem_array = (eitem  **)c->coll_eitem;
    uint32_t *kfnd_array = (uint32_t*)((char*)elem_array + (smget_count*sizeof(eitem*)));
    uint32_t *flag_array = (uint32_t*)((char*)kfnd_array + (smget_count*sizeof(uint32_t)));
    uint32_t *kmis_array = (uint32_t*)((char*)flag_array + (smget_count*sizeof(uint32_t)));

    resultptr = ((char *)kmis_array + kmis_array_size);

    bool trimmed;
    bool duplicated;
#endif

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    int ntokens = tokenize_keys(vptr, c->coll_lenkeys, delimiter, c->coll_numkeys, keys_array);
    if (ntokens == -1) {
        ret = ENGINE_EBADVALUE;
    } else {
        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }
        ret = mc_engine.v1->btree_elem_smget(mc_engine.v0, c,
                                     keys_array, c->coll_numkeys,
                                     &c->coll_bkrange,
                                     (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
#ifdef JHPARK_NEW_SMGET_INTERFACE
#if 1 // JHPARK_OLD_SMGET_INTERFACE
                                     c->coll_roffset, c->coll_rcount,
                                     (c->coll_smgmode == 2 ? true : false), &smres,
#else
                                     c->coll_roffset, c->coll_rcount, c->coll_unique,
                                     &smres,
#endif
#else
                                     c->coll_roffset, c->coll_rcount,
                                     elem_array, kfnd_array, flag_array, &elem_count,
                                     kmis_array, &kmis_count, &trimmed, &duplicated,
#endif
                                     c->binary_header.request.vbucket);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        protocol_binary_response_bop_smget* rsp = (protocol_binary_response_bop_smget*)c->wbuf;
#ifdef JHPARK_NEW_SMGET_INTERFACE
        uint32_t real_elem_hdr_size = smres.elem_count * (sizeof(uint64_t) + (3*sizeof(uint32_t)));
        uint32_t real_emis_hdr_size = (smres.miss_count + smres.trim_count) * sizeof(uint32_t);
        uint32_t bodylen, i;
#else
        uint32_t real_elem_hdr_size = elem_count * (sizeof(uint64_t) + (3*sizeof(uint32_t)));
        uint32_t real_kmis_hdr_size = kmis_count * sizeof(uint32_t);
        uint32_t bodylen, i;
#endif
        uint64_t *bkeyptr;
        uint32_t *vlenptr;
        uint32_t *flagptr;
        uint32_t *klenptr;

        if (((long)resultptr % 8) != 0) /* NOT aligned */
            resultptr += (8 - ((long)resultptr % 8));
        bkeyptr = (uint64_t *)resultptr;
#ifdef JHPARK_NEW_SMGET_INTERFACE
        vlenptr = (uint32_t *)((char*)bkeyptr + (sizeof(uint64_t) * smres.elem_count));
        flagptr = (uint32_t *)((char*)vlenptr + (sizeof(uint32_t) * smres.elem_count));
        klenptr = (uint32_t *)((char*)flagptr + (sizeof(uint32_t) * smres.elem_count));

        eitem_info info[smres.elem_count+1]; /* elem_count might be 0. */
        for (i = 0; i < smres.elem_count; i++) {
            mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, smres.elem_array[i], &info[i]);
        }
#else
        vlenptr = (uint32_t *)((char*)bkeyptr + (sizeof(uint64_t) * elem_count));
        flagptr = (uint32_t *)((char*)vlenptr + (sizeof(uint32_t) * elem_count));
        klenptr = (uint32_t *)((char*)flagptr + (sizeof(uint32_t) * elem_count));

        eitem_info info[elem_count+1]; /* elem_count might be 0. */
        for (i = 0; i < elem_count; i++) {
            mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem_array[i], &info[i]);
        }
#endif

        bodylen = sizeof(rsp->message.body);
#ifdef JHPARK_NEW_SMGET_INTERFACE
        bodylen += (real_elem_hdr_size + real_emis_hdr_size);
        for (i = 0; i < smres.elem_count; i++) {
             bodylen += (info[i].nbytes - 2);
             bodylen += keys_array[smres.elem_kinfo[i].kidx].length;
        }
        for (i = 0; i < smres.miss_count; i++) {
             bodylen += keys_array[smres.miss_kinfo[i].kidx].length;
        }
        for (i = 0; i < smres.trim_count; i++) {
             bodylen += keys_array[smres.trim_kinfo[i].kidx].length;
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), 0, bodylen);

        // add the flags and count
        rsp->message.body.elem_count = htonl(smres.elem_count);
        rsp->message.body.miss_count = htonl(smres.miss_count);
        rsp->message.body.trim_count = htonl(smres.trim_count);
#else
        bodylen += (real_elem_hdr_size + real_kmis_hdr_size);
        for (i = 0; i < elem_count; i++) {
             bodylen += (info[i].nbytes - 2);
             bodylen += keys_array[kfnd_array[i]].length;
        }
        for (i = 0; i < kmis_count; i++) {
             bodylen += keys_array[kmis_array[i]].length;
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), 0, bodylen);

        // add the flags and count
        rsp->message.body.elem_count = htonl(elem_count);
        rsp->message.body.kmis_count = htonl(kmis_count);
#endif
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

#ifdef JHPARK_NEW_SMGET_INTERFACE
        // add value lengths
        for (i = 0; i < smres.elem_count; i++) {
             bkeyptr[i] = htonll(*(uint64_t*)info[i].score);
             vlenptr[i] = htonl(info[i].nbytes - 2);
             flagptr[i] = smres.elem_kinfo[i].flag;
             klenptr[i] = htonl(keys_array[smres.elem_kinfo[i].kidx].length);
        }
        for (i = 0; i < smres.miss_count; i++) {
             klenptr[smres.elem_count+i] = htonl(keys_array[smres.miss_kinfo[i].kidx].length);
        }
        for (i = 0; i < smres.trim_count; i++) {
             klenptr[smres.elem_count+smres.miss_count+i] = htonl(keys_array[smres.trim_kinfo[i].kidx].length);
        }
        if (add_iov(c, (char*)bkeyptr, real_elem_hdr_size+real_emis_hdr_size) != 0) {
            ret = ENGINE_ENOMEM;
        }

        /* Add the data without CRLF */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < smres.elem_count; i++) {
                if (add_iov(c, info[i].value, info[i].nbytes - 2) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
        /* Add the found key */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < smres.elem_count; i++) {
                if (add_iov(c, keys_array[smres.elem_kinfo[i].kidx].value,
                               keys_array[smres.elem_kinfo[i].kidx].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
        /* Add the missed key */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < smres.miss_count; i++) {
                if (add_iov(c, keys_array[smres.miss_kinfo[i].kidx].value,
                               keys_array[smres.miss_kinfo[i].kidx].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
        /* Add the trimmed key */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < smres.trim_count; i++) {
                if (add_iov(c, keys_array[smres.trim_kinfo[i].kidx].value,
                               keys_array[smres.trim_kinfo[i].kidx].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
#else
        // add value lengths
        for (i = 0; i < elem_count; i++) {
             bkeyptr[i] = htonll(*(uint64_t*)info[i].score);
             vlenptr[i] = htonl(info[i].nbytes - 2);
             flagptr[i] = flag_array[i];
             klenptr[i] = htonl(keys_array[kfnd_array[i]].length);
        }
        for (i = 0; i < kmis_count; i++) {
             klenptr[elem_count+i] = htonl(keys_array[kmis_array[i]].length);
        }
        if (add_iov(c, (char*)bkeyptr, real_elem_hdr_size+real_kmis_hdr_size) != 0) {
            ret = ENGINE_ENOMEM;
        }

        /* Add the data without CRLF */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < elem_count; i++) {
                if (add_iov(c, info[i].value, info[i].nbytes - 2) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
        /* Add the found key */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < elem_count; i++) {
                if (add_iov(c, keys_array[kfnd_array[i]].value,
                               keys_array[kfnd_array[i]].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
        /* Add the missed key */
        if (ret == ENGINE_SUCCESS) {
            for (i = 0; i < kmis_count; i++) {
                if (add_iov(c, keys_array[kmis_array[i]].value,
                               keys_array[kmis_array[i]].length) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
        }
#endif

        if (ret == ENGINE_SUCCESS) {
            /* Remember this command so we can garbage collect it later */
            /* c->coll_eitem  = (void *)elem_array; */
            STATS_NOKEY2(c, cmd_bop_smget, bop_smget_oks);
#ifdef JHPARK_NEW_SMGET_INTERFACE
            c->coll_ecount = smres.elem_count+smres.trim_count;
#else
            c->coll_ecount = elem_count;
#endif
            c->coll_op     = OPERATION_BOP_SMGET;
            conn_set_state(c, conn_mwrite);
        } else {
            STATS_NOKEY(c, cmd_bop_smget);
#ifdef JHPARK_NEW_SMGET_INTERFACE
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, smres.elem_array,
                                             smres.elem_count+smres.trim_count);
#else
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
#endif
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        }
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_smget);
        if (ret == ENGINE_EBADVALUE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADVALUE, 0);
        else if (ret == ENGINE_EBADTYPE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADTYPE, 0);
        else if (ret == ENGINE_EBADBKEY)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADBKEY, 0);
        else if (ret == ENGINE_EBADATTR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADATTR, 0);
#ifdef JHPARK_NEW_SMGET_INTERFACE // JHPARK_SMGET_OFFSET_HANDLING
#else
        else if (ret == ENGINE_EBKEYOOR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBKEYOOR, 0);
#endif
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            free((void *)c->coll_strkeys);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
static void process_bin_bop_nread_keys_complete(conn *c) {
    assert(c != NULL);
#ifdef SUPPORT_BOP_MGET
    if (c->coll_op == OPERATION_BOP_MGET)
        process_bin_bop_mget_complete(c);
#endif
#ifdef SUPPORT_BOP_SMGET
    if (c->coll_op == OPERATION_BOP_SMGET)
        process_bin_bop_smget_complete(c);
#endif
}
#endif

static void process_bin_getattr(conn *c) {
    assert(c != NULL);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d GETATTR ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, "\n");
    }

    item_attr attr_data;
    ENGINE_ITEM_ATTR attr_ids[ATTR_END];
    uint32_t attr_count = 0;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->getattr(mc_engine.v0, c, key, nkey,
                                attr_ids, attr_count, &attr_data,
                                c->binary_header.request.vbucket);

    if (settings.detail_enabled) {
        stats_prefix_record_getattr(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        STATS_HITS(c, getattr, key, nkey);

        protocol_binary_response_getattr* rsp = (protocol_binary_response_getattr*)c->wbuf;
        rsp->message.body.flags      = attr_data.flags;
        rsp->message.body.expiretime = htonl(attr_data.exptime);
        rsp->message.body.type       = attr_data.type;
        if (attr_data.type == ITEM_TYPE_LIST || attr_data.type == ITEM_TYPE_SET ||
            attr_data.type == ITEM_TYPE_BTREE) {
            rsp->message.body.count      = htonl(attr_data.count);
            rsp->message.body.maxcount   = htonl(attr_data.maxcount);
            rsp->message.body.ovflaction = attr_data.ovflaction;
            rsp->message.body.readable   = attr_data.readable;
        }
        if (attr_data.type == ITEM_TYPE_BTREE) {
            rsp->message.body.maxbkeyrange.len = attr_data.maxbkeyrange.len;
            if (attr_data.maxbkeyrange.len != BKEY_NULL) {
                if (attr_data.maxbkeyrange.len == 0) {
                    uint64_t bkey_temp;
                    memcpy((unsigned char*)&bkey_temp, attr_data.maxbkeyrange.val, sizeof(uint64_t));
                    bkey_temp = htonll(bkey_temp);
                    memcpy(rsp->message.body.maxbkeyrange.val, (unsigned char*)&bkey_temp, sizeof(uint64_t));
                    //*(uint64_t*)rsp->message.body.maxbkeyrange.val = htonll(*(uint64_t*)attr_data.maxbkeyrange.val);
                } else {
                    memcpy(rsp->message.body.maxbkeyrange.val, attr_data.maxbkeyrange.val, attr_data.maxbkeyrange.len);
                }
            }
        }
        write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body));
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, getattr, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_getattr);
        if (ret == ENGINE_EBADATTR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADATTR, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

static void process_bin_setattr(conn *c) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = binary_get_key(c);
    int  nkey = c->binary_header.request.keylen;

    item_attr attr_data;
    ENGINE_ITEM_ATTR attr_ids[ATTR_END];
    uint32_t attr_count = 0;

    /* fix byteorder in the request */
    protocol_binary_request_setattr* req = binary_get_request(c);

    if (req->message.body.expiretime_f != 0) {
        attr_ids[attr_count++] = ATTR_EXPIRETIME;
        req->message.body.expiretime = ntohl(req->message.body.expiretime);
        attr_data.exptime = realtime(req->message.body.expiretime);
    }
    if (req->message.body.maxcount_f != 0) {
        attr_ids[attr_count++] = ATTR_MAXCOUNT;
        req->message.body.maxcount = ntohl(req->message.body.maxcount);
        attr_data.maxcount = req->message.body.maxcount;
    }
    if (req->message.body.maxbkeyrange.len != BKEY_NULL) {
        attr_ids[attr_count++] = ATTR_MAXBKEYRANGE;
        if (req->message.body.maxbkeyrange.len == 0) {
            uint64_t bkey_temp;
            memcpy((unsigned char*)&bkey_temp, req->message.body.maxbkeyrange.val, sizeof(uint64_t));
            bkey_temp = ntohll(bkey_temp);
            memcpy(attr_data.maxbkeyrange.val, (unsigned char*)&bkey_temp, sizeof(uint64_t));
            //*(uint64_t*)attr_data.maxbkeyrange.val = ntohll(*(uint64_t*)req->message.body.maxbkeyrange.val);
        } else {
            memcpy(attr_data.maxbkeyrange.val, req->message.body.maxbkeyrange.val, req->message.body.maxbkeyrange.len);
        }
        attr_data.maxbkeyrange.len = req->message.body.maxbkeyrange.len;
    }
    if (req->message.body.ovflaction != 0) {
        attr_ids[attr_count++] = ATTR_OVFLACTION;
        attr_data.ovflaction = req->message.body.ovflaction;
    }
    if (req->message.body.readable != 0) {
        attr_ids[attr_count++] = ATTR_READABLE;
        attr_data.readable = req->message.body.readable;
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d SETATTR ", c->sfd);
        for (int ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        for (int ii = 0; ii < attr_count; ii++) {
             if (attr_ids[ii] == ATTR_EXPIRETIME)
                 fprintf(stderr, " expiretime=%d", (int32_t)attr_data.exptime);
             else if (attr_ids[ii] == ATTR_MAXCOUNT)
                 fprintf(stderr, " maxcount=%d", attr_data.maxcount);
             else if (attr_ids[ii] == ATTR_OVFLACTION)
                 fprintf(stderr, " ovflaction=%s", get_ovflaction_str(attr_data.ovflaction));
             else if (attr_ids[ii] == ATTR_READABLE)
                 fprintf(stderr, " readable=%s", (attr_data.readable ? "on" : "off"));
             else if (attr_ids[ii] == ATTR_MAXBKEYRANGE) {
                 char buffer[MAX_BKEY_LENG*2+4];
                 if (attr_data.maxbkeyrange.len == 0) {
                     uint64_t bkey_temp;
                     memcpy((unsigned char*)&bkey_temp, attr_data.maxbkeyrange.val, sizeof(uint64_t));
                     fprintf(stderr, "maxbkeyrange=%"PRIu64, bkey_temp);
                     //fprintf(stderr, "maxbkeyrange=%"PRIu64, *(uint64_t*)attr_data.maxbkeyrange.val);
                 } else {
                     safe_hexatostr(attr_data.maxbkeyrange.val, attr_data.maxbkeyrange.len, buffer);
                     fprintf(stderr, "maxbkeyrange=0x%s", buffer);
                 }
             }
        }
        fprintf(stderr, "\n");
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->setattr(mc_engine.v0, c, key, nkey,
                                attr_ids, attr_count, &attr_data,
                                c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_setattr(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, setattr, key, nkey);
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, setattr, key, nkey);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    default:
        STATS_NOKEY(c, cmd_setattr);
        if (ret == ENGINE_EBADATTR)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADATTR, 0);
        else if (ret == ENGINE_EBADVALUE)
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EBADVALUE, 0);
        else
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
    }
}

static bool binary_response_handler(const void *key, uint16_t keylen,
                                    const void *ext, uint8_t extlen,
                                    const void *body, uint32_t bodylen,
                                    uint8_t datatype, uint16_t status,
                                    uint64_t cas, const void *cookie)
{
    conn *c = (conn*)cookie;
    /* Look at append_bin_stats */
    size_t needed = keylen + extlen + bodylen + sizeof(protocol_binary_response_header);
    if (!grow_dynamic_buffer(c, needed)) {
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                    "<%d ERROR: Failed to allocate memory for response\n", c->sfd);
        }
        return false;
    }

    char *buf = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    protocol_binary_response_header header = {
        .response.magic = (uint8_t)PROTOCOL_BINARY_RES,
        .response.opcode = c->binary_header.request.opcode,
        .response.keylen = (uint16_t)htons(keylen),
        .response.extlen = extlen,
        .response.datatype = datatype,
        .response.status = (uint16_t)htons(status),
        .response.bodylen = htonl(bodylen + keylen + extlen),
        .response.opaque = c->opaque,
        .response.cas = htonll(cas),
    };

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (extlen > 0) {
        memcpy(buf, ext, extlen);
        buf += extlen;
    }

    if (keylen > 0) {
        memcpy(buf, key, keylen);
        buf += keylen;
    }

    if (bodylen > 0) {
        memcpy(buf, body, bodylen);
    }

    c->dynamic_buffer.offset += needed;

    return true;
}

static void process_bin_unknown_packet(conn *c) {
    void *packet = c->rcurr - (c->binary_header.request.bodylen +
                               sizeof(c->binary_header));

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->unknown_command(mc_engine.v0, c, packet,
                                        binary_response_handler);

    switch (ret) {
    case ENGINE_SUCCESS:
        write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
        c->dynamic_buffer.buffer = NULL;
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0);
        break;
    default:
        /* FATAL ERROR, shut down connection */
        conn_set_state(c, conn_closing);
    }
}

static void dispatch_bin_command(conn *c) {
    int protocol_error = 0;

    int extlen = c->binary_header.request.extlen;
    int keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    if (settings.require_sasl && !authenticated(c)) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        c->write_and_go = conn_closing;
        return;
    }

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);
    c->noreply = true;

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (keylen > KEY_MAX_LENGTH) {
        handle_binary_protocol_error(c);
        return;
    }

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SETQ:
        c->cmd = PROTOCOL_BINARY_CMD_SET;
        break;
    case PROTOCOL_BINARY_CMD_ADDQ:
        c->cmd = PROTOCOL_BINARY_CMD_ADD;
        break;
    case PROTOCOL_BINARY_CMD_REPLACEQ:
        c->cmd = PROTOCOL_BINARY_CMD_REPLACE;
        break;
    case PROTOCOL_BINARY_CMD_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_DELETE;
        break;
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_INCREMENT;
        break;
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_DECREMENT;
        break;
    case PROTOCOL_BINARY_CMD_QUITQ:
        c->cmd = PROTOCOL_BINARY_CMD_QUIT;
        break;
    case PROTOCOL_BINARY_CMD_FLUSHQ:
        c->cmd = PROTOCOL_BINARY_CMD_FLUSH;
        break;
    case PROTOCOL_BINARY_CMD_APPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_APPEND;
        break;
    case PROTOCOL_BINARY_CMD_PREPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_PREPEND;
        break;
    case PROTOCOL_BINARY_CMD_GETQ:
        c->cmd = PROTOCOL_BINARY_CMD_GET;
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->cmd = PROTOCOL_BINARY_CMD_GETK;
        break;
    case PROTOCOL_BINARY_CMD_LOP_INSERTQ:
        c->cmd = PROTOCOL_BINARY_CMD_LOP_INSERT;
        break;
    case PROTOCOL_BINARY_CMD_LOP_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_LOP_DELETE;
        break;
    case PROTOCOL_BINARY_CMD_SOP_INSERTQ:
        c->cmd = PROTOCOL_BINARY_CMD_SOP_INSERT;
        break;
    case PROTOCOL_BINARY_CMD_SOP_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_SOP_DELETE;
        break;
    case PROTOCOL_BINARY_CMD_BOP_INSERTQ:
        c->cmd = PROTOCOL_BINARY_CMD_BOP_INSERT;
        break;
    case PROTOCOL_BINARY_CMD_BOP_UPSERTQ:
        c->cmd = PROTOCOL_BINARY_CMD_BOP_UPSERT;
        break;
    case PROTOCOL_BINARY_CMD_BOP_UPDATEQ:
        c->cmd = PROTOCOL_BINARY_CMD_BOP_UPDATE;
        break;
    case PROTOCOL_BINARY_CMD_BOP_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_BOP_DELETE;
        break;
    default:
        c->noreply = false;
    }

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_VERSION:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, VERSION, 0, 0, strlen(VERSION));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
            if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
                bin_read_key(c, bin_read_flush_exptime, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_FLUSH_PREFIX:
            if (keylen > 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
                bin_read_key(c, bin_read_flush_prefix_exptime, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_ADD: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_REPLACE:
            if (extlen == 8 && keylen != 0 && bodylen >= (keylen + 8)) {
                bin_read_key(c, bin_reading_set_header, 8);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_GETQ:  /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GET:   /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETKQ: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETK:
            if (extlen == 0 && bodylen == keylen && keylen > 0) {
                bin_read_key(c, bin_reading_get_key, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
            if (keylen > 0 && extlen == 0 && bodylen == keylen) {
                bin_read_key(c, bin_reading_del_header, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENT:
            if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_incr_header, 20);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_PREPEND:
            if (keylen > 0 && extlen == 0) {
                bin_read_key(c, bin_reading_set_header, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_STAT:
            if (extlen == 0) {
                bin_read_key(c, bin_reading_stat, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_QUIT:
            if (keylen == 0 && extlen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
                c->write_and_go = conn_closing;
                if (c->noreply) {
                    conn_set_state(c, conn_closing);
                }
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_GETATTR:
            if (keylen > 0 && extlen == (sizeof(bkey_t)+24) && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_getattr, (sizeof(bkey_t)+24));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SETATTR:
            if (keylen > 0 && extlen == (sizeof(bkey_t)+12) && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_setattr, (sizeof(bkey_t)+12));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_LOP_CREATE:
            if (keylen > 0 && extlen == 16 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_lop_create, 16);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_LOP_INSERT:
            if (keylen > 0 && extlen == 20 && bodylen > (keylen + extlen)) {
                bin_read_key(c, bin_reading_lop_prepare_nread, 20);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_LOP_DELETE:
            if (keylen > 0 && extlen == 12 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_lop_delete, 12);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_LOP_GET:
            if (keylen > 0 && extlen == 12 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_lop_get, 12);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SOP_CREATE:
            if (keylen > 0 && extlen == 16 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_sop_create, 16);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SOP_INSERT:
            if (keylen > 0 && extlen == 16 && bodylen > (keylen + extlen)) {
                bin_read_key(c, bin_reading_sop_prepare_nread, 16);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SOP_DELETE:
            if (keylen > 0 && extlen == 4 && bodylen > (keylen + extlen)) {
                bin_read_key(c, bin_reading_sop_prepare_nread, 4);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SOP_EXIST:
            if (keylen > 0 && extlen == 0 && bodylen > (keylen + extlen)) {
                bin_read_key(c, bin_reading_sop_prepare_nread, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SOP_GET:
            if (keylen > 0 && extlen == 8 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_sop_get, 8);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_BOP_CREATE:
            if (keylen > 0 && extlen == 16 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_bop_create, 16);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_BOP_INSERT:
        case PROTOCOL_BINARY_CMD_BOP_UPSERT:
            if (keylen > 0 && extlen == (MAX_BKEY_LENG+1+MAX_EFLAG_LENG+1+16) && bodylen > (keylen + extlen)) {
                bin_read_key(c, bin_reading_bop_prepare_nread, (MAX_BKEY_LENG+1+MAX_EFLAG_LENG+1+16));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_BOP_UPDATE:
            if (keylen > 0 && extlen == (MAX_BKEY_LENG+1+MAX_EFLAG_LENG+1+8) && bodylen > (keylen + extlen)) {
                bin_read_key(c, bin_reading_bop_update_prepare_nread, (MAX_BKEY_LENG+1+MAX_EFLAG_LENG+1+8));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_BOP_DELETE:
            if (keylen > 0 && extlen == (sizeof(bkey_range)+sizeof(eflag_filter)+8) && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_bop_delete, (sizeof(bkey_range)+sizeof(eflag_filter)+8));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_BOP_GET:
            if (keylen > 0 && extlen == (sizeof(bkey_range)+sizeof(eflag_filter)+12) && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_bop_get, (sizeof(bkey_range)+sizeof(eflag_filter)+12));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_BOP_COUNT:
            if (keylen > 0 && extlen == (sizeof(bkey_range)+sizeof(eflag_filter)) && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_bop_count, (sizeof(bkey_range)+sizeof(eflag_filter)));
            } else {
                protocol_error = 1;
            }
            break;
#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
#ifdef SUPPORT_BOP_MGET
        case PROTOCOL_BINARY_CMD_BOP_MGET:
#endif
#ifdef SUPPORT_BOP_SMGET
        case PROTOCOL_BINARY_CMD_BOP_SMGET:
#endif
            if (keylen > 0 && extlen == (sizeof(bkey_range)+sizeof(eflag_filter)+12) && bodylen > (keylen + extlen)) {
                bin_read_key(c, bin_reading_bop_prepare_nread_keys, (sizeof(bkey_range)+sizeof(eflag_filter)+12));
            } else {
                protocol_error = 1;
            }
            break;
#endif
#ifdef SASL_ENABLED
        case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                bin_list_sasl_mechs(c);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SASL_AUTH:
        case PROTOCOL_BINARY_CMD_SASL_STEP:
            if (extlen == 0 && keylen != 0) {
                bin_read_key(c, bin_reading_sasl_auth, 0);
            } else {
                protocol_error = 1;
            }
            break;
#endif
        default:
            if (mc_engine.v1->unknown_command == NULL) {
                write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND,
                                bodylen);
            } else {
                bin_read_chunk(c, bin_reading_packet, c->binary_header.request.bodylen);
            }
    }

    if (protocol_error)
        handle_binary_protocol_error(c);
}

static void process_bin_update(conn *c) {
    assert(c != NULL);

    char *key;
    item *it;
    protocol_binary_request_set* req = binary_get_request(c);
    key = binary_get_key(c);
    uint32_t nkey = c->binary_header.request.keylen;
    uint32_t vlen = 0;

    /* fix byteorder in the request */
    req->message.body.flags = req->message.body.flags;
    req->message.body.expiration = ntohl(req->message.body.expiration);

    if (nkey + c->binary_header.request.extlen <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    if (settings.verbose > 1) {
        char buffer[1024];
        const char *prefix;
        if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
            prefix = "ADD";
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            prefix = "SET";
        } else {
            prefix = "REPLACE";
        }

        size_t nw;
        nw = key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                     prefix, key, nkey);

        if (nw != -1) {
            if (snprintf(buffer + nw, sizeof(buffer) - nw,
                         " Value len is %d\n", vlen)) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s", buffer);
            }
        }
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    ENGINE_ERROR_CODE ret;
    item_info info = { .nvalue = 1 };

    ret = mc_engine.v1->allocate(mc_engine.v0, c,
                                 &it, key, nkey, vlen + 2,
                                 req->message.body.flags,
                                 realtime(req->message.body.expiration));
    if (ret == ENGINE_SUCCESS && !mc_engine.v1->get_item_info(mc_engine.v0,
                                                              c, it, &info)) {
        mc_engine.v1->release(mc_engine.v0, c, it);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
        return;
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mc_engine.v1->item_set_cas(mc_engine.v0, c, it,
                                   c->binary_header.request.cas);

        switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
            c->store_op = OPERATION_ADD;
            break;
        case PROTOCOL_BINARY_CMD_SET:
            c->store_op = OPERATION_SET;
            break;
        case PROTOCOL_BINARY_CMD_REPLACE:
            c->store_op = OPERATION_REPLACE;
            break;
        default:
            assert(0);
        }

        if (c->binary_header.request.cas != 0) {
            c->store_op = OPERATION_CAS;
        }

        c->item = it;
        c->ritem = info.value[0].iov_base;
        c->rlbytes = vlen;
        conn_set_state(c, conn_nread);
        c->substate = bin_read_set_value;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        if (ret == ENGINE_E2BIG) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }

        /*
         * Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET (but only if cas matches).
         * Anywhere else too?
         */
        if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            /* @todo fix this for the ASYNC interface! */
            mc_engine.v1->remove(mc_engine.v0, c, key, nkey,
                                 ntohll(req->message.header.request.cas),
                                 c->binary_header.request.vbucket);
        }

        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_append_prepend(conn *c) {
    assert(c != NULL);

    char *key;
    item *it;
    uint32_t nkey = c->binary_header.request.keylen;
    uint32_t vlen = 0;

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;
    if (nkey <= c->binary_header.request.bodylen) {
        vlen = c->binary_header.request.bodylen - nkey;
    } else {
        handle_binary_protocol_error(c);
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    ENGINE_ERROR_CODE ret;
    item_info info = { .nvalue = 1 };

    ret = mc_engine.v1->allocate(mc_engine.v0, c,
                                 &it, key, nkey, vlen + 2, 0, 0);
    if (ret == ENGINE_SUCCESS && !mc_engine.v1->get_item_info(mc_engine.v0,
                                                              c, it, &info)) {
        mc_engine.v1->release(mc_engine.v0, c, it);
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0);
        return;
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mc_engine.v1->item_set_cas(mc_engine.v0, c, it,
                                   c->binary_header.request.cas);

        switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_APPEND:
            c->store_op = OPERATION_APPEND;
            break;
        case PROTOCOL_BINARY_CMD_PREPEND:
            c->store_op = OPERATION_PREPEND;
            break;
        default:
            assert(0);
        }

        c->item = it;
        c->ritem = info.value[0].iov_base;
        c->rlbytes = vlen;
        conn_set_state(c, conn_nread);
        c->substate = bin_read_set_value;
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        if (ret == ENGINE_E2BIG) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }
        /* swallow the data line */
        c->write_and_go = conn_swallow;
    }
}

static void process_bin_flush(conn *c) {
    time_t exptime = 0;
    protocol_binary_request_flush* req = binary_get_request(c);

    if (c->binary_header.request.extlen == sizeof(req->message.body)) {
        exptime = ntohl(req->message.body.expiration);
    }

    if (settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                       "%d: flush %ld", c->sfd, (long)exptime);
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->flush(mc_engine.v0, c, exptime);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (ret == ENGINE_SUCCESS) {
        write_bin_response(c, NULL, 0, 0, 0);
    } else if (ret == ENGINE_ENOTSUP) {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
    } else {
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    }
    STATS_NOKEY(c, cmd_flush);
}

static void process_bin_flush_prefix(conn *c) {
    time_t exptime = 0;

    char *prefix = binary_get_key(c);
    size_t nprefix = c->binary_header.request.keylen;

    protocol_binary_request_flush_prefix *req = binary_get_request(c);

    if (c->binary_header.request.extlen == sizeof(req->message.body)) {
        exptime = ntohl(req->message.body.expiration);
    }

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true, "FLUSH_PREFIX", prefix, nprefix) != -1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s %ld\n", buffer, (long)exptime);
        }
    }

    if (nprefix == 4 && strncmp(prefix, "null", 4) == 0) {
        /* flush null prefix */
        prefix = NULL;
        nprefix = 0;
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->flush_prefix(mc_engine.v0, c, prefix, nprefix, exptime);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        if (ret == ENGINE_SUCCESS || ret == ENGINE_PREFIX_ENOENT) {
            if (stats_prefix_delete(prefix, nprefix) == 0) { /* found */
                ret = ENGINE_SUCCESS;
            }
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_PREFIX_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_PREFIX_ENOENT, 0);
        break;
    case ENGINE_ENOTSUP:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0);
        break;
    default:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
        break;
    }

    STATS_NOKEY(c, cmd_flush_prefix);
    return;
}

static void process_bin_delete(conn *c) {
    protocol_binary_request_delete* req = binary_get_request(c);

    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    assert(c != NULL);

    if (settings.verbose > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer), c->sfd, true,
                                    "DELETE", key, nkey) != -1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s\n", buffer);
        }
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->remove(mc_engine.v0, c, key, nkey,
                               ntohll(req->message.header.request.cas),
                               c->binary_header.request.vbucket);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_KEY_EEXISTS:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case ENGINE_KEY_ENOENT:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0);
        break;
    default:
        write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    }
}

static void complete_nread_binary(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);

    switch(c->substate) {
    case bin_reading_set_header:
        if (c->cmd == PROTOCOL_BINARY_CMD_APPEND ||
                c->cmd == PROTOCOL_BINARY_CMD_PREPEND) {
            process_bin_append_prepend(c);
        } else {
            process_bin_update(c);
        }
        break;
    case bin_read_set_value:
        complete_update_bin(c);
        break;
    case bin_reading_get_key:
        process_bin_get(c);
        break;
    case bin_reading_stat:
        process_bin_stat(c);
        break;
    case bin_reading_del_header:
        process_bin_delete(c);
        break;
    case bin_reading_incr_header:
        complete_incr_bin(c);
        break;
    case bin_read_flush_exptime:
        process_bin_flush(c);
        break;
    case bin_read_flush_prefix_exptime:
        process_bin_flush_prefix(c);
        break;
    case bin_reading_sasl_auth:
        process_bin_sasl_auth(c);
        break;
    case bin_reading_sasl_auth_data:
        process_bin_complete_sasl_auth(c);
        break;
    case bin_reading_getattr:
        process_bin_getattr(c);
        break;
    case bin_reading_setattr:
        process_bin_setattr(c);
        break;
    case bin_reading_lop_create:
        process_bin_lop_create(c);
        break;
    case bin_reading_lop_prepare_nread:
        process_bin_lop_prepare_nread(c);
        break;
    case bin_reading_lop_nread_complete:
        process_bin_lop_nread_complete(c);
        break;
    case bin_reading_lop_delete:
        process_bin_lop_delete(c);
        break;
    case bin_reading_lop_get:
        process_bin_lop_get(c);
        break;
    case bin_reading_sop_create:
        process_bin_sop_create(c);
        break;
    case bin_reading_sop_prepare_nread:
        process_bin_sop_prepare_nread(c);
        break;
    case bin_reading_sop_nread_complete:
        process_bin_sop_nread_complete(c);
        break;
    case bin_reading_sop_get:
        process_bin_sop_get(c);
        break;
    case bin_reading_bop_create:
        process_bin_bop_create(c);
        break;
    case bin_reading_bop_prepare_nread:
        process_bin_bop_prepare_nread(c);
        break;
    case bin_reading_bop_nread_complete:
        process_bin_bop_nread_complete(c);
        break;
    case bin_reading_bop_update_prepare_nread:
        process_bin_bop_update_prepare_nread(c);
        break;
    case bin_reading_bop_update_nread_complete:
        process_bin_bop_update_complete(c);
        break;
    case bin_reading_bop_delete:
        process_bin_bop_delete(c);
        break;
    case bin_reading_bop_get:
        process_bin_bop_get(c);
        break;
    case bin_reading_bop_count:
        process_bin_bop_count(c);
        break;
#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
    case bin_reading_bop_prepare_nread_keys:
        process_bin_bop_prepare_nread_keys(c);
        break;
    case bin_reading_bop_nread_keys_complete:
        process_bin_bop_nread_keys_complete(c);
        break;
#endif
    case bin_reading_packet:
        process_bin_unknown_packet(c);
        break;
    default:
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                "Not handling substate %d\n", c->substate);
        abort();
    }
}

static void reset_cmd_handler(conn *c) {
    c->ascii_cmd = NULL;
    c->cmd = -1;
    c->substate = bin_no_state;
    if(c->item != NULL) {
        mc_engine.v1->release(mc_engine.v0, c, c->item);
        c->item = NULL;
    }
#ifdef DETECT_LONG_QUERY
    if (c->lq_bufcnt != 0) {
        lqdetect_buffer_release(c->lq_bufcnt);
        c->lq_bufcnt = 0;
    }
#endif
    if (c->coll_eitem != NULL) {
        conn_coll_eitem_free(c);
    }
    conn_shrink(c);
    if (c->rbytes > 0) {
        conn_set_state(c, conn_parse_cmd);
    } else {
        conn_set_state(c, conn_waiting);
    }
}

static bool ascii_response_handler(const void *cookie,
                                   int nbytes,
                                   const char *dta)
{
    conn *c = (conn*)cookie;
    if (!grow_dynamic_buffer(c, nbytes)) {
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                    "<%d ERROR: Failed to allocate memory for response\n", c->sfd);
        }
        return false;
    }

    char *buf = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    memcpy(buf, dta, nbytes);
    c->dynamic_buffer.offset += nbytes;

    return true;
}

static void complete_nread_ascii(conn *c) {
    if (c->ascii_cmd != NULL) {
        if (!c->ascii_cmd->execute(c->ascii_cmd->cookie, c, 0, NULL,
                                   ascii_response_handler)) {
            conn_set_state(c, conn_closing);
        } else if (c->dynamic_buffer.buffer != NULL) {
            write_and_free(c, c->dynamic_buffer.buffer,
                           c->dynamic_buffer.offset);
            c->dynamic_buffer.buffer = NULL;
        } else {
            conn_set_state(c, conn_new_cmd);
        }
    } else {
        complete_update_ascii(c);
    }
}

static void complete_nread(conn *c)
{
    assert(c != NULL);
    assert(c->protocol == ascii_prot || c->protocol == binary_prot);

    if (c->protocol == ascii_prot) {
        complete_nread_ascii(c);
    } else {
        complete_nread_binary(c);
    }
}

#define COMMAND_TOKEN 0
#define SUBCOMMAND_TOKEN 1
#define PREFIX_TOKEN 1
#define KEY_TOKEN 1
#define LOP_KEY_TOKEN 2
#define SOP_KEY_TOKEN 2
#define BOP_KEY_TOKEN 2

#define MAX_TOKENS 30

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_command(char *command, int cmdlen, token_t *tokens, const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;
    size_t checked = 0;

    assert(command != NULL && tokens != NULL && max_tokens > 1);

    s = command;
    while (ntokens < max_tokens - 1) {
        e = memchr(s, ' ', cmdlen - checked);
        if (e) {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
                checked = e - command;
            }
            s = (++e);
        } else {
            e = command + cmdlen;
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }

            break; /* string end */
        }
    }

    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    if (*e == '\0') {
        tokens[ntokens].value = NULL;
    } else {
        assert(ntokens == (max_tokens-1));
        tokens[ntokens].value = e;
        /* The next reserved token keeps the length of untokenized command. */
        tokens[ntokens+1].length = cmdlen - (e - command);
    }
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

static void detokenize(token_t *tokens, int ntokens, char **out, int *nbytes) {
    int i, nb;
    char *buf, *p;

    nb = ntokens; // account for spaces, which is ntokens-1, plus the null
    for (i = 0; i < ntokens; ++i) {
        nb += tokens[i].length;
    }

    buf = malloc(nb * sizeof(char));
    if (buf != NULL) {
        p = buf;
        for (i = 0; i < ntokens; ++i) {
            memcpy(p, tokens[i].value, tokens[i].length);
            p += tokens[i].length;
            *p = ' ';
            p++;
        }
        buf[nb - 1] = '\0';
        *nbytes = nb - 1;
        *out = buf;
    }
}


/* set up a connection to write a buffer then free it, used for stats */
static void write_and_free(conn *c, char *buf, int bytes) {
    if (buf) {
        c->write_and_free = buf;
        c->wcurr = buf;
        c->wbytes = bytes;
        conn_set_state(c, conn_write);
        c->write_and_go = conn_new_cmd;
    } else {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    }
}

#ifdef JHPARK_NEW_SMGET_INTERFACE
#if 1 // JHPARK_OLD_SMGET_INTERFACE
static inline int set_smget_mode_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int mode_index = ntokens - 2;
    int mode_value = 0;

    if (tokens[mode_index].value) {
        if (strcmp(tokens[mode_index].value, "duplicate") == 0)
            mode_value = 1;
        else if (strcmp(tokens[mode_index].value, "unique") == 0)
            mode_value = 2;
    }
    return mode_value;
}
#else
static inline bool set_unique_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int unique_index = ntokens - 2;

    if (tokens[unique_index].value
        && strcmp(tokens[unique_index].value, "unique") == 0)
        return true;
    else
        return false;
}
#endif
#endif

static inline bool set_noreply_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int noreply_index = ntokens - 2;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "noreply" option is not reliable anyway, so
      it can't be helped.
    */
    if (tokens[noreply_index].value
        && strcmp(tokens[noreply_index].value, "noreply") == 0) {
        c->noreply = true;
    }
    return c->noreply;
}

static inline bool set_pipe_noreply_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int noreply_index = ntokens - 2;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "noreply" or "pipe" option is not reliable anyway,
      so it can't be helped.
    */
    if (tokens[noreply_index].value) {
        if (strcmp(tokens[noreply_index].value, "noreply") == 0) {
            c->noreply = true;
        } else if (strcmp(tokens[noreply_index].value, "pipe") == 0) {
            c->noreply = true;
            if (unlikely(c->pipe_state == PIPE_STATE_OFF))
                c->pipe_state = PIPE_STATE_ON;
        }
    }
    return c->noreply;
}

static inline bool set_pipe_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int noreply_index = ntokens - 2;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "pipe" option is not reliable anyway,
      so it can't be helped.
    */
    if (tokens[noreply_index].value) {
        if (strcmp(tokens[noreply_index].value, "pipe") == 0) {
            c->noreply = true;
            if (unlikely(c->pipe_state == PIPE_STATE_OFF))
                c->pipe_state = PIPE_STATE_ON;
        }
    }
    return c->noreply;
}

static bool check_and_handle_pipe_state(conn *c, size_t swallow)
{
    if (c->pipe_state == PIPE_STATE_OFF || c->pipe_state == PIPE_STATE_ON) {
        return true;
    } else {
        assert(c->pipe_state == PIPE_STATE_ERR_CFULL ||
               c->pipe_state == PIPE_STATE_ERR_MFULL ||
               c->pipe_state == PIPE_STATE_ERR_BAD);
        if (c->noreply == true) {
            c->noreply = false; /* reset noreply */
        } else  {
            /* The last command of pipelining has come. */
            /* clear pipe_state: the end of pipe */
            c->pipe_state = PIPE_STATE_OFF;
            c->pipe_count = 0;
        }
        if (swallow > 0) {
            c->sbytes = swallow;
            conn_set_state(c, conn_swallow);
        }
        return false;
    }
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    va_list ap;

    assert(name);
    assert(add_stats);
    assert(c);
    assert(fmt);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
    va_end(ap);

    add_stats(name, strlen(name), val_str, vlen, c);
}

inline static void process_stats_detail(conn *c, const char *command) {
    assert(c != NULL);

    if (settings.allow_detailed) {
        if (strcmp(command, "on") == 0) {
            settings.detail_enabled = 1;
            out_string(c, "OK");
        }
        else if (strcmp(command, "off") == 0) {
            settings.detail_enabled = 0;
            out_string(c, "OK");
        }
        else if (strcmp(command, "dump") == 0) {
            int len;
            char *stats = stats_prefix_dump(&len);
            write_and_free(c, stats, len);
        }
        else {
            out_string(c, "CLIENT_ERROR usage: stats detail on|off|dump");
        }
    }
    else {
        out_string(c, "CLIENT_ERROR detailed stats disabled");
    }
}

static void process_stats_prefix(conn *c, const char *prefix, const int nprefix) {
    assert(c != NULL);
    ENGINE_ERROR_CODE ret;

    if (nprefix < 0) {
        char *prefix_data;
        ret = mc_engine.v1->get_prefix_stats(mc_engine.v0, c, prefix, nprefix, &prefix_data);
        switch (ret) {
            case ENGINE_SUCCESS:
                c->write_and_free = prefix_data;
                c->wcurr  = prefix_data + sizeof(uint32_t);
                c->wbytes = *(uint32_t*)prefix_data;
                conn_set_state(c, conn_write);
                c->write_and_go = conn_new_cmd;
                break;
            case ENGINE_DISCONNECT:
                c->state = conn_closing;
                break;
            case ENGINE_ENOMEM:
                out_string(c, "SERVER_ERROR no more memory");
                break;
            default:
                out_string(c, "PREFIX_ERROR");
        }
    } else {
        /****** SPEC-OUT FUNCTIONS **********
        prefix_engine_stats prefix_data;
        if (nprefix > KEY_MAX_LENGTH) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        ret = mc_engine.v1->get_prefix_stats(mc_engine.v0, c, prefix, nprefix, &prefix_data);
        switch (ret) {
            case ENGINE_SUCCESS:
                {
                char buffer[1024];
                char *str = &buffer[0];
                *str = '\0';
                sprintf(str, "PREFIX name=%s\r\n", (prefix == NULL ? "<null>" : prefix));
                sprintf(str+strlen(str), "PREFIX hash_items=%"PRIu64"\r\n", prefix_data.hash_items);
                sprintf(str+strlen(str), "PREFIX hash_items_bytes=%"PRIu64"\r\n", prefix_data.hash_items_bytes);
                sprintf(str+strlen(str), "PREFIX prefix_items=%u\r\n", prefix_data.prefix_items);
                sprintf(str+strlen(str), "PREFIX tot_prefix_items=%u\r\n", prefix_data.tot_prefix_items);
                sprintf(str+strlen(str), "END");
                out_string(c, str);
                }
                break;
            case ENGINE_DISCONNECT:
                c->state = conn_closing;
                break;
            case ENGINE_PREFIX_ENOENT:
                out_string(c, "NOT_FOUND");
                break;
            default:
                out_string(c, "PREFIX_ERROR");
        }
        ************************************/
    }
    return;
}

static void aggregate_callback(void *in, void *out) {
    struct thread_stats *out_thread_stats = out;
    struct independent_stats *in_independent_stats = in;
    threadlocal_stats_aggregate(in_independent_stats->thread_stats,
                                out_thread_stats);
}

/* return server specific stats only */
static void server_stats(ADD_STAT add_stats, conn *c, bool aggregate) {
    pid_t pid = getpid();
    rel_time_t now = current_time;

    struct thread_stats thread_stats;
    threadlocal_stats_clear(&thread_stats);

    if (aggregate && mc_engine.v1->aggregate_stats != NULL) {
        mc_engine.v1->aggregate_stats(mc_engine.v0, (const void *)c,
                                      aggregate_callback, &thread_stats);
    } else {
        threadlocal_stats_aggregate(get_independent_stats(c)->thread_stats,
                                    &thread_stats);
    }

    struct slab_stats slab_stats;
    slab_stats_aggregate(&thread_stats, &slab_stats);

#ifndef __WIN32__
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
#endif

#ifdef ENABLE_ZK_INTEGRATION
    arcus_zk_stats zk_stats;
    arcus_zk_get_stats(&zk_stats);
#endif

    STATS_LOCK();

    APPEND_STAT("pid", "%lu", (long)pid);
    APPEND_STAT("uptime", "%u", now);
    APPEND_STAT("time", "%ld", now + (long)process_started);
    APPEND_STAT("version", "%s", VERSION);
    APPEND_STAT("libevent", "%s", event_get_version());
    APPEND_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));
#ifdef ENABLE_ZK_INTEGRATION
    APPEND_STAT("zk_timeout", "%u", zk_stats.zk_timeout);
    APPEND_STAT("hb_timeout", "%u", zk_stats.hb_timeout);
    APPEND_STAT("hb_failstop", "%u", zk_stats.hb_failstop);
    APPEND_STAT("hb_count", "%"PRIu64, zk_stats.hb_count);
    APPEND_STAT("hb_latency", "%"PRIu64, zk_stats.hb_latency);
#endif

#ifndef __WIN32__
    append_stat("rusage_user", add_stats, c, "%ld.%06ld",
                (long)usage.ru_utime.tv_sec,
                (long)usage.ru_utime.tv_usec);
    append_stat("rusage_system", add_stats, c, "%ld.%06ld",
                (long)usage.ru_stime.tv_sec,
                (long)usage.ru_stime.tv_usec);
#endif

    APPEND_STAT("daemon_connections", "%u", mc_stats.daemon_conns);
    APPEND_STAT("curr_connections", "%u", mc_stats.curr_conns);
    APPEND_STAT("quit_connections", "%u", mc_stats.quit_conns);
    APPEND_STAT("reject_connections", "%u", mc_stats.rejected_conns);
    APPEND_STAT("total_connections", "%u", mc_stats.total_conns);
    APPEND_STAT("connection_structures", "%u", mc_stats.conn_structs);
    APPEND_STAT("cmd_get", "%"PRIu64, thread_stats.cmd_get);
    APPEND_STAT("cmd_set", "%"PRIu64, slab_stats.cmd_set);
    APPEND_STAT("cmd_incr", "%"PRIu64, thread_stats.cmd_incr);
    APPEND_STAT("cmd_decr", "%"PRIu64, thread_stats.cmd_decr);
    APPEND_STAT("cmd_delete", "%"PRIu64, thread_stats.cmd_delete);
    APPEND_STAT("cmd_flush", "%"PRIu64, thread_stats.cmd_flush);
    APPEND_STAT("cmd_flush_prefix", "%"PRIu64, thread_stats.cmd_flush_prefix);
    APPEND_STAT("cmd_lop_create", "%"PRIu64, thread_stats.cmd_lop_create);
    APPEND_STAT("cmd_lop_insert", "%"PRIu64, thread_stats.cmd_lop_insert);
    APPEND_STAT("cmd_lop_delete", "%"PRIu64, thread_stats.cmd_lop_delete);
    APPEND_STAT("cmd_lop_get", "%"PRIu64, thread_stats.cmd_lop_get);
    APPEND_STAT("cmd_sop_create", "%"PRIu64, thread_stats.cmd_sop_create);
    APPEND_STAT("cmd_sop_insert", "%"PRIu64, thread_stats.cmd_sop_insert);
    APPEND_STAT("cmd_sop_delete", "%"PRIu64, thread_stats.cmd_sop_delete);
    APPEND_STAT("cmd_sop_get", "%"PRIu64, thread_stats.cmd_sop_get);
    APPEND_STAT("cmd_sop_exist", "%"PRIu64, thread_stats.cmd_sop_exist);
    APPEND_STAT("cmd_bop_create", "%"PRIu64, thread_stats.cmd_bop_create);
    APPEND_STAT("cmd_bop_insert", "%"PRIu64, thread_stats.cmd_bop_insert);
    APPEND_STAT("cmd_bop_update", "%"PRIu64, thread_stats.cmd_bop_update);
    APPEND_STAT("cmd_bop_delete", "%"PRIu64, thread_stats.cmd_bop_delete);
    APPEND_STAT("cmd_bop_get", "%"PRIu64, thread_stats.cmd_bop_get);
    APPEND_STAT("cmd_bop_count", "%"PRIu64, thread_stats.cmd_bop_count);
    APPEND_STAT("cmd_bop_position", "%"PRIu64, thread_stats.cmd_bop_position);
    APPEND_STAT("cmd_bop_pwg", "%"PRIu64, thread_stats.cmd_bop_pwg);
    APPEND_STAT("cmd_bop_gbp", "%"PRIu64, thread_stats.cmd_bop_gbp);
#ifdef SUPPORT_BOP_MGET
    APPEND_STAT("cmd_bop_mget", "%"PRIu64, thread_stats.cmd_bop_mget);
#endif
#ifdef SUPPORT_BOP_SMGET
    APPEND_STAT("cmd_bop_smget", "%"PRIu64, thread_stats.cmd_bop_smget);
#endif
    APPEND_STAT("cmd_bop_incr", "%"PRIu64, thread_stats.cmd_bop_incr);
    APPEND_STAT("cmd_bop_decr", "%"PRIu64, thread_stats.cmd_bop_decr);
    APPEND_STAT("cmd_getattr", "%"PRIu64, thread_stats.cmd_getattr);
    APPEND_STAT("cmd_setattr", "%"PRIu64, thread_stats.cmd_setattr);
    APPEND_STAT("auth_cmds", "%"PRIu64, thread_stats.auth_cmds);
    APPEND_STAT("auth_errors", "%"PRIu64, thread_stats.auth_errors);
    APPEND_STAT("get_hits", "%"PRIu64, slab_stats.get_hits);
    APPEND_STAT("get_misses", "%"PRIu64, thread_stats.get_misses);
    APPEND_STAT("delete_misses", "%"PRIu64, thread_stats.delete_misses);
    APPEND_STAT("delete_hits", "%"PRIu64, slab_stats.delete_hits);
    APPEND_STAT("incr_misses", "%"PRIu64, thread_stats.incr_misses);
    APPEND_STAT("incr_hits", "%"PRIu64, thread_stats.incr_hits);
    APPEND_STAT("decr_misses", "%"PRIu64, thread_stats.decr_misses);
    APPEND_STAT("decr_hits", "%"PRIu64, thread_stats.decr_hits);
    APPEND_STAT("cas_misses", "%"PRIu64, thread_stats.cas_misses);
    APPEND_STAT("cas_hits", "%"PRIu64, slab_stats.cas_hits);
    APPEND_STAT("cas_badval", "%"PRIu64, slab_stats.cas_badval);
    APPEND_STAT("lop_create_oks", "%"PRIu64, thread_stats.lop_create_oks);
    APPEND_STAT("lop_insert_misses", "%"PRIu64, thread_stats.lop_insert_misses);
    APPEND_STAT("lop_insert_hits", "%"PRIu64, thread_stats.lop_insert_hits);
    APPEND_STAT("lop_delete_misses", "%"PRIu64, thread_stats.lop_delete_misses);
    APPEND_STAT("lop_delete_elem_hits", "%"PRIu64, thread_stats.lop_delete_elem_hits);
    APPEND_STAT("lop_delete_none_hits", "%"PRIu64, thread_stats.lop_delete_none_hits);
    APPEND_STAT("lop_get_misses", "%"PRIu64, thread_stats.lop_get_misses);
    APPEND_STAT("lop_get_elem_hits", "%"PRIu64, thread_stats.lop_get_elem_hits);
    APPEND_STAT("lop_get_none_hits", "%"PRIu64, thread_stats.lop_get_none_hits);
    APPEND_STAT("sop_create_oks", "%"PRIu64, thread_stats.sop_create_oks);
    APPEND_STAT("sop_insert_misses", "%"PRIu64, thread_stats.sop_insert_misses);
    APPEND_STAT("sop_insert_hits", "%"PRIu64, thread_stats.sop_insert_hits);
    APPEND_STAT("sop_delete_misses", "%"PRIu64, thread_stats.sop_delete_misses);
    APPEND_STAT("sop_delete_elem_hits", "%"PRIu64, thread_stats.sop_delete_elem_hits);
    APPEND_STAT("sop_delete_none_hits", "%"PRIu64, thread_stats.sop_delete_none_hits);
    APPEND_STAT("sop_get_misses", "%"PRIu64, thread_stats.sop_get_misses);
    APPEND_STAT("sop_get_elem_hits", "%"PRIu64, thread_stats.sop_get_elem_hits);
    APPEND_STAT("sop_get_none_hits", "%"PRIu64, thread_stats.sop_get_none_hits);
    APPEND_STAT("sop_exist_misses", "%"PRIu64, thread_stats.sop_exist_misses);
    APPEND_STAT("sop_exist_hits", "%"PRIu64, thread_stats.sop_exist_hits);
    APPEND_STAT("bop_create_oks", "%"PRIu64, thread_stats.bop_create_oks);
    APPEND_STAT("bop_insert_misses", "%"PRIu64, thread_stats.bop_insert_misses);
    APPEND_STAT("bop_insert_hits", "%"PRIu64, thread_stats.bop_insert_hits);
    APPEND_STAT("bop_update_misses", "%"PRIu64, thread_stats.bop_update_misses);
    APPEND_STAT("bop_update_elem_hits", "%"PRIu64, thread_stats.bop_update_elem_hits);
    APPEND_STAT("bop_update_none_hits", "%"PRIu64, thread_stats.bop_update_none_hits);
    APPEND_STAT("bop_delete_misses", "%"PRIu64, thread_stats.bop_delete_misses);
    APPEND_STAT("bop_delete_elem_hits", "%"PRIu64, thread_stats.bop_delete_elem_hits);
    APPEND_STAT("bop_delete_none_hits", "%"PRIu64, thread_stats.bop_delete_none_hits);
    APPEND_STAT("bop_get_misses", "%"PRIu64, thread_stats.bop_get_misses);
    APPEND_STAT("bop_get_elem_hits", "%"PRIu64, thread_stats.bop_get_elem_hits);
    APPEND_STAT("bop_get_none_hits", "%"PRIu64, thread_stats.bop_get_none_hits);
    APPEND_STAT("bop_count_misses", "%"PRIu64, thread_stats.bop_count_misses);
    APPEND_STAT("bop_count_hits", "%"PRIu64, thread_stats.bop_count_hits);
    APPEND_STAT("bop_position_misses", "%"PRIu64, thread_stats.bop_position_misses);
    APPEND_STAT("bop_position_elem_hits", "%"PRIu64, thread_stats.bop_position_elem_hits);
    APPEND_STAT("bop_position_none_hits", "%"PRIu64, thread_stats.bop_position_none_hits);
    APPEND_STAT("bop_pwg_misses", "%"PRIu64, thread_stats.bop_pwg_misses);
    APPEND_STAT("bop_pwg_elem_hits", "%"PRIu64, thread_stats.bop_pwg_elem_hits);
    APPEND_STAT("bop_pwg_none_hits", "%"PRIu64, thread_stats.bop_pwg_none_hits);
    APPEND_STAT("bop_gbp_misses", "%"PRIu64, thread_stats.bop_gbp_misses);
    APPEND_STAT("bop_gbp_elem_hits", "%"PRIu64, thread_stats.bop_gbp_elem_hits);
    APPEND_STAT("bop_gbp_none_hits", "%"PRIu64, thread_stats.bop_gbp_none_hits);
#ifdef SUPPORT_BOP_MGET
    APPEND_STAT("bop_mget_oks", "%"PRIu64, thread_stats.bop_mget_oks);
#endif
#ifdef SUPPORT_BOP_SMGET
    APPEND_STAT("bop_smget_oks", "%"PRIu64, thread_stats.bop_smget_oks);
#endif
    APPEND_STAT("bop_incr_elem_hits", "%"PRIu64, thread_stats.bop_incr_elem_hits);
    APPEND_STAT("bop_incr_none_hits", "%"PRIu64, thread_stats.bop_incr_none_hits);
    APPEND_STAT("bop_incr_misses", "%"PRIu64, thread_stats.bop_incr_misses);
    APPEND_STAT("bop_decr_elem_hits", "%"PRIu64, thread_stats.bop_decr_elem_hits);
    APPEND_STAT("bop_decr_none_hits", "%"PRIu64, thread_stats.bop_decr_none_hits);
    APPEND_STAT("bop_decr_misses", "%"PRIu64, thread_stats.bop_decr_misses);
    APPEND_STAT("getattr_misses", "%"PRIu64, thread_stats.getattr_misses);
    APPEND_STAT("getattr_hits", "%"PRIu64, thread_stats.getattr_hits);
    APPEND_STAT("setattr_misses", "%"PRIu64, thread_stats.setattr_misses);
    APPEND_STAT("setattr_hits", "%"PRIu64, thread_stats.setattr_hits);
    APPEND_STAT("bytes_read", "%"PRIu64, thread_stats.bytes_read);
    APPEND_STAT("bytes_written", "%"PRIu64, thread_stats.bytes_written);
    APPEND_STAT("limit_maxbytes", "%"PRIu64, settings.maxbytes);
    APPEND_STAT("threads", "%d", settings.num_threads);
    APPEND_STAT("conn_yields", "%" PRIu64, (unsigned long long)thread_stats.conn_yields);
    STATS_UNLOCK();
}

static void process_stat_settings(ADD_STAT add_stats, void *c) {
    assert(add_stats);
    APPEND_STAT("maxbytes", "%u", (unsigned int)settings.maxbytes);
    APPEND_STAT("maxconns", "%d", settings.maxconns);
    APPEND_STAT("tcpport", "%d", settings.port);
    APPEND_STAT("udpport", "%d", settings.udpport);
    APPEND_STAT("sticky_ratio", "%d", settings.sticky_ratio);
    APPEND_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
    APPEND_STAT("verbosity", "%d", settings.verbose);
    APPEND_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
    APPEND_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
    APPEND_STAT("domain_socket", "%s",
                settings.socketpath ? settings.socketpath : "NULL");
    APPEND_STAT("umask", "%o", settings.access);
    APPEND_STAT("growth_factor", "%.2f", settings.factor);
    APPEND_STAT("chunk_size", "%d", settings.chunk_size);
    APPEND_STAT("num_threads", "%d", settings.num_threads);
    APPEND_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
    APPEND_STAT("detail_enabled", "%s",
                settings.detail_enabled ? "yes" : "no");
    APPEND_STAT("allow_detailed", "%s",
                settings.allow_detailed ? "yes" : "no");
    APPEND_STAT("reqs_per_event", "%d", settings.reqs_per_event);
    APPEND_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
    APPEND_STAT("tcp_backlog", "%d", settings.backlog);
    APPEND_STAT("binding_protocol", "%s",
                prot_text(settings.binding_protocol));
#ifdef SASL_ENABLED
    APPEND_STAT("auth_enabled_sasl", "%s", "yes");
#else
    APPEND_STAT("auth_enabled_sasl", "%s", "no");
#endif

#ifdef ENABLE_ISASL
    APPEND_STAT("auth_sasl_engine", "%s", "isasl");
#elif defined(ENABLE_SASL)
    APPEND_STAT("auth_sasl_engine", "%s", "cyrus");
#else
    APPEND_STAT("auth_sasl_engine", "%s", "none");
#endif
    APPEND_STAT("auth_required_sasl", "%s", settings.require_sasl ? "yes" : "no");
    APPEND_STAT("item_size_max", "%d", settings.item_size_max);
    APPEND_STAT("max_list_size", "%d", settings.max_list_size);
    APPEND_STAT("max_set_size", "%d", settings.max_set_size);
    APPEND_STAT("max_btree_size", "%d", settings.max_btree_size);
    APPEND_STAT("topkeys", "%d", settings.topkeys);

    for (EXTENSION_DAEMON_DESCRIPTOR *ptr = settings.extensions.daemons;
         ptr != NULL;
         ptr = ptr->next) {
        APPEND_STAT("extension", "%s", ptr->get_name());
    }

    APPEND_STAT("logger", "%s", mc_logger->get_name());

    for (EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ptr = settings.extensions.ascii;
         ptr != NULL;
         ptr = ptr->next) {
        APPEND_STAT("ascii_extension", "%s", ptr->get_name(ptr->cookie));
    }
}

static void process_stat(conn *c, token_t *tokens, const size_t ntokens) {
    const char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    assert(c != NULL);

    if (ntokens < 2) {
        out_string(c, "CLIENT_ERROR bad command line");
        return;
    }

    if (ntokens == 2) {
        server_stats(&append_stats, c, false);
        (void)mc_engine.v1->get_stats(mc_engine.v0, c,
                                      NULL, 0, &append_stats);
    } else if (strcmp(subcommand, "reset") == 0) {
        stats_reset(c);
        out_string(c, "RESET");
        return ;
    } else if (strcmp(subcommand, "detail") == 0) {
        /* NOTE: how to tackle detail with binary? */
        if (ntokens < 4)
            process_stats_detail(c, "");  /* outputs the error message */
        else
            process_stats_detail(c, tokens[2].value);
        /* Output already generated */
        return ;
    } else if (strcmp(subcommand, "settings") == 0) {
        process_stat_settings(&append_stats, c);
    } else if (strcmp(subcommand, "cachedump") == 0) {
        char *buf = NULL;
        unsigned int bytes = 0, id, limit = 0;
        bool forward=true, sticky=false;

        if (ntokens < 5 || ntokens > 7) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (!safe_strtoul(tokens[2].value, &id) ||
            !safe_strtoul(tokens[3].value, &limit)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (id > POWER_LARGEST) {
            out_string(c, "CLIENT_ERROR Illegal slab id");
            return;
        }

        if (limit == 0)  limit = 50;
        if (limit > 200) limit = 200;

        if (ntokens >= 6) {
            if (strcmp(tokens[4].value, "forward")==0) forward = true;
            else if (strcmp(tokens[4].value, "backward")==0) forward = false;
            else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        if (ntokens == 7) {
            if (strcmp(tokens[5].value, "sticky")==0) sticky = true;
            else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        buf = mc_engine.v1->cachedump(mc_engine.v0, c, id, limit,
                                      forward, sticky,  &bytes);
        write_and_free(c, buf, bytes);
        return ;
    } else if (strcmp(subcommand, "aggregate") == 0) {
        server_stats(&append_stats, c, true);
    } else if (strcmp(subcommand, "topkeys") == 0) {
        topkeys_t *tk = get_independent_stats(c)->topkeys;
        if (tk != NULL) {
            topkeys_stats(tk, c, current_time, append_stats);
        } else {
            out_string(c, "SERVER_ERROR not supported");
            return;
        }
    } else if (strcmp(subcommand, "prefixes") == 0) {
        process_stats_prefix(c, NULL, -1);
        return;
    /****** SPEC-OUT FUNCTIONS **********
    } else if (strcmp(subcommand, "prefix") == 0) {
        if (ntokens < 4) {
            out_string(c, "CLIENT_ERROR usage: stats prefix <prefix>");
        } else {
            process_stats_prefix(c, tokens[2].value, tokens[2].length);
        }
        return;
    } else if (strcmp(subcommand, "noprefix") == 0) {
        process_stats_prefix(c, NULL, 0);
        return;
    ************************************/
    } else {
        /* getting here means that the subcommand is either engine specific or
           is invalid. query the engine and see. */
        ENGINE_ERROR_CODE ret;
        char *buf = NULL;
        int nb = -1;
        detokenize(&tokens[1], ntokens - 2, &buf, &nb);
        ret = mc_engine.v1->get_stats(mc_engine.v0, c, buf,
                                      nb, append_stats);
        free(buf);

        switch (ret) {
        case ENGINE_SUCCESS:
            append_stats(NULL, 0, NULL, 0, c);
            write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
            c->dynamic_buffer.buffer = NULL;
            break;
        case ENGINE_ENOMEM:
            out_string(c, "SERVER_ERROR out of memory writing stats");
            break;
        case ENGINE_DISCONNECT:
            c->state = conn_closing;
            break;
        case ENGINE_ENOTSUP:
            out_string(c, "SERVER_ERROR not supported");
            break;
        default:
            out_string(c, "ERROR no matching stat");
            break;
        }
        return ;
    }

    /* append terminator and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);

    if (c->dynamic_buffer.buffer == NULL) {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    } else {
        write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
        c->dynamic_buffer.buffer = NULL;
    }
}

/**
 * Get a suffix buffer and insert it into the list of used suffix buffers
 * @param c the connection object
 * @return a pointer to a new suffix buffer or NULL if allocation failed
 */
static char *get_suffix_buffer(conn *c) {
    if (c->suffixleft == c->suffixsize) {
        char **new_suffix_list;
        size_t sz = sizeof(char*) * c->suffixsize * 2;

        new_suffix_list = realloc(c->suffixlist, sz);
        if (new_suffix_list) {
            c->suffixsize *= 2;
            c->suffixlist = new_suffix_list;
        } else {
            if (settings.verbose > 1) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c,
                        "=%d Failed to resize suffix buffer\n", c->sfd);
            }

            return NULL;
        }
    }

    char *suffix = cache_alloc(c->thread->suffix_cache);
    if (suffix != NULL) {
        *(c->suffixlist + c->suffixleft) = suffix;
        ++c->suffixleft;
    }

    return suffix;
}

/* ntokens is overwritten here... shrug.. */
static inline void process_get_command(conn *c, token_t *tokens, size_t ntokens, bool return_cas)
{
    char *key;
    size_t nkey;
    int i = 0;
    item *it;
    token_t *key_token = &tokens[KEY_TOKEN];
    assert(c != NULL);

    do {
        while(key_token->length != 0) {

            key = key_token->value;
            nkey = key_token->length;

            if(nkey > KEY_MAX_LENGTH) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            if (mc_engine.v1->get(mc_engine.v0, c, &it, key, nkey, 0) != ENGINE_SUCCESS) {
                it = NULL;
            }

            if (settings.detail_enabled) {
                stats_prefix_record_get(key, nkey, NULL != it);
            }

            if (it) {
                item_info info = { .nvalue = 1 };
                if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &info)) {
                    mc_engine.v1->release(mc_engine.v0, c, it);
                    out_string(c, "SERVER_ERROR error getting item data");
                    break;
                }

                assert(memcmp((char*)info.value[0].iov_base + info.nbytes - 2, "\r\n", 2) == 0);

                if (i >= c->isize) {
                    item **new_list = realloc(c->ilist, sizeof(item *) * c->isize * 2);
                    if (new_list) {
                        c->isize *= 2;
                        c->ilist = new_list;
                    } else {
                        mc_engine.v1->release(mc_engine.v0, c, it);
                        break;
                    }
                }

                /* Rebuild the suffix */
                char *suffix = get_suffix_buffer(c);
                if (suffix == NULL) {
                    out_string(c, "SERVER_ERROR out of memory rebuilding suffix");
                    mc_engine.v1->release(mc_engine.v0, c, it);
                    return;
                }
                int suffix_len = snprintf(suffix, SUFFIX_SIZE,
                                          " %u %u\r\n", htonl(info.flags),
                                          info.nbytes - 2);

                /*
                 * Construct the response. Each hit adds three elements to the
                 * outgoing data list:
                 *   "VALUE "
                 *   key
                 *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
                 */

                MEMCACHED_COMMAND_GET(c->sfd, info.key, info.nkey,
                                      info.nbytes, info.cas);
                if (return_cas)
                {

                  char *cas = get_suffix_buffer(c);
                  if (cas == NULL) {
                    out_string(c, "SERVER_ERROR out of memory making CAS suffix");
                    mc_engine.v1->release(mc_engine.v0, c, it);
                    return;
                  }
                  int cas_len = snprintf(cas, SUFFIX_SIZE, " %"PRIu64"\r\n",
                                         info.cas);
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, info.key, info.nkey) != 0 ||
                      add_iov(c, suffix, suffix_len - 2) != 0 ||
                      add_iov(c, cas, cas_len) != 0 ||
                      add_iov(c, info.value[0].iov_base, info.value[0].iov_len) != 0)
                      {
                          mc_engine.v1->release(mc_engine.v0, c, it);
                          break;
                      }
                }
                else
                {
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, info.key, info.nkey) != 0 ||
                      add_iov(c, suffix, suffix_len) != 0 ||
                      add_iov(c, info.value[0].iov_base, info.value[0].iov_len) != 0)
                      {
                          mc_engine.v1->release(mc_engine.v0, c, it);
                          break;
                      }
                }


                if (settings.verbose > 1) {
                    mc_logger->log(EXTENSION_LOG_DEBUG, c,
                            ">%d sending key %s\n", c->sfd, (char*)info.key);
                }

                /* item_get() has incremented it->refcount for us */
                STATS_HIT(c, get, key, nkey);
                *(c->ilist + i) = it;
                i++;

            } else {
                STATS_MISS(c, get, key, nkey);
                MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
            }

            key_token++;
        }

        /*
         * If the command string hasn't been fully processed, get the next set
         * of tokens.
         */
        if(key_token->value != NULL) {
            /* The next reserved token has the length of untokenized command. */
            ntokens = tokenize_command(key_token->value, (key_token+1)->length,
                                       tokens, MAX_TOKENS);
            key_token = tokens;
        }

    } while(key_token->value != NULL);

    c->icurr = c->ilist;
    c->ileft = i;
    c->suffixcurr = c->suffixlist;

    if (settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                       ">%d END\n", c->sfd);
    }

    /*
        If the loop was terminated because of out-of-memory, it is not
        reliable to add END\r\n to the buffer, because it might not end
        in \r\n. So we send SERVER_ERROR instead.
    */
    if (key_token->value != NULL || add_iov(c, "END\r\n", 5) != 0
        || (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
        out_string(c, "SERVER_ERROR out of memory writing get response");
    }
    else {
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
    }

    return;
}

static void process_update_command(conn *c, token_t *tokens, const size_t ntokens, ENGINE_STORE_OPERATION store_op, bool handle_cas) {
    char *key;
    size_t nkey;
    unsigned int flags;
    int32_t exptime_int = 0;
    time_t exptime;
    int vlen;
    uint64_t req_cas_id=0;
    item *it;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (! (safe_strtoul(tokens[2].value, (uint32_t *)&flags)
           && safe_strtol(tokens[3].value, &exptime_int)
           && safe_strtol(tokens[4].value, (int32_t *)&vlen))) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    if (vlen < 0 || vlen > (INT_MAX-2)) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    vlen += 2;

    /* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
    exptime = exptime_int;

    // does cas value exist?
    if (handle_cas) {
        if (!safe_strtoull(tokens[5].value, &req_cas_id)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->allocate(mc_engine.v0, c, &it, key, nkey,
                                 vlen, htonl(flags), realtime(exptime));

    item_info info = { .nvalue = 1 };
    switch (ret) {
    case ENGINE_SUCCESS:
        mc_engine.v1->item_set_cas(mc_engine.v0, c, it, req_cas_id);
        if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &info)) {
            mc_engine.v1->release(mc_engine.v0, c, it);
            out_string(c, "SERVER_ERROR error getting item data");
            break;
        }
        c->item = it;
        c->ritem = info.value[0].iov_base;
        c->rlbytes = vlen;
        c->store_op = store_op;
        conn_set_state(c, conn_nread);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_E2BIG:
    case ENGINE_ENOMEM:
        if (ret == ENGINE_E2BIG) {
            out_string(c, "SERVER_ERROR object too large for cache");
        } else {
            out_string(c, "SERVER_ERROR out of memory storing object");
        }
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (store_op == OPERATION_SET) {
            mc_engine.v1->remove(mc_engine.v0, c, key, nkey, 0, 0);
        }
        break;
    default:
        handle_unexpected_errorcode_ascii(c, ret);
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;
    }
}

static void process_arithmetic_command(conn *c, token_t *tokens, const size_t ntokens, const bool incr) {

    uint64_t delta;
    char *key;
    size_t nkey;

    assert(c != NULL);
    assert(c->ewouldblock == false);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (!safe_strtoull(tokens[2].value, &delta)) {
        out_string(c, "CLIENT_ERROR invalid numeric delta argument");
        return;
    }

    bool create = false;
    unsigned int flags;
    int32_t exptime_int = 0;
    uint64_t init_value = 0;

    if (ntokens >= 7) {
        if (! (safe_strtoul(tokens[3].value, (uint32_t *)&flags)
               && safe_strtol(tokens[4].value, &exptime_int)
               && safe_strtoull(tokens[5].value, &init_value))) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        create = true;
    }

    if (settings.detail_enabled) {
        if (incr) {
            stats_prefix_record_incr(key, nkey);
        } else {
            stats_prefix_record_decr(key, nkey);
        }
    }

    ENGINE_ERROR_CODE ret;
    uint64_t cas;
    uint64_t result;

    ret = mc_engine.v1->arithmetic(mc_engine.v0, c, key, nkey,
                                   incr, create, delta,
                                   init_value, htonl(flags), exptime_int,
                                   &cas, &result, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    char temp[INCR_MAX_STORAGE_LEN];
    switch (ret) {
    case ENGINE_SUCCESS:
        if (incr) {
            STATS_HITS(c, incr, key, nkey);
        } else {
            STATS_HITS(c, decr, key, nkey);
        }
        snprintf(temp, sizeof(temp), "%"PRIu64, result);
        out_string(c, temp);
        break;
    case ENGINE_KEY_ENOENT:
        if (incr) {
            STATS_MISS(c, incr, key, nkey);
        } else {
            STATS_MISS(c, decr, key, nkey);
        }
        out_string(c, "NOT_FOUND");
        break;
    case ENGINE_PREFIX_ENAME:
        out_string(c, "CLIENT_ERROR invalid prefix name");

        break;
    case ENGINE_ENOMEM:
        out_string(c, "SERVER_ERROR out of memory");
        break;
    case ENGINE_EINVAL:
        out_string(c, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        break;
    case ENGINE_NOT_STORED:
        out_string(c, "SERVER_ERROR failed to store item");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_ENOTSUP:
        out_string(c, "SERVER_ERROR not supported");
        break;
    case ENGINE_EBADTYPE:
        out_string(c, "TYPE_MISMATCH");
        break;
    default:
        handle_unexpected_errorcode_ascii(c, ret);
    }
}

static void process_delete_command(conn *c, token_t *tokens, const size_t ntokens) {
    char *key;
    size_t nkey;

    assert(c != NULL);
    assert(c->ewouldblock == false);

    if (ntokens > 3) {
        bool hold_is_zero = strcmp(tokens[KEY_TOKEN+1].value, "0") == 0;
        bool sets_noreply = set_noreply_maybe(c, tokens, ntokens);
        bool valid = (ntokens == 4 && (hold_is_zero || sets_noreply))
            || (ntokens == 5 && hold_is_zero && sets_noreply);
        if (!valid) {
            out_string(c, "CLIENT_ERROR bad command line format.  "
                       "Usage: delete <key> [noreply]");
            return;
        }
    }


    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if(nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->remove(mc_engine.v0, c, key, nkey, 0, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    /* For some reason the SLAB_INCR tries to access this... */
    item_info info = { .nvalue = 1 };
    if (ret == ENGINE_SUCCESS) {
        out_string(c, "DELETED");
        //SLAB_INCR(c, delete_hits, key, nkey);
        STATS_HIT(c, delete, key, nkey);
    } else if (ret == ENGINE_KEY_ENOENT) {
        out_string(c, "NOT_FOUND");
        //STATS_INCR(c, delete_misses, key, nkey);
        STATS_MISS(c, delete, key, nkey);
    } else {
        handle_unexpected_errorcode_ascii(c, ret);
    }
}

static void process_flush_command(conn *c, token_t *tokens, const size_t ntokens, bool flush_all)
{
    time_t exptime;

    assert(c->ewouldblock == false);

    set_noreply_maybe(c, tokens, ntokens);

    if (flush_all)
    {
        /* flush_all [<delay>] [noreply]\r\n */
        if (ntokens == (c->noreply ? 3 : 2)) {
            exptime = 0;
        } else {
            exptime = strtol(tokens[1].value, NULL, 10);
            if(errno == ERANGE) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        ENGINE_ERROR_CODE ret;
        ret = mc_engine.v1->flush(mc_engine.v0, c, exptime);
        if (ret == ENGINE_EWOULDBLOCK) {
            c->ewouldblock = true;
            ret = ENGINE_SUCCESS;
        }

        if (ret == ENGINE_SUCCESS) {
            out_string(c, "OK");
        } else if (ret == ENGINE_ENOTSUP) {
            out_string(c, "SERVER_ERROR not supported");
        } else {
            handle_unexpected_errorcode_ascii(c, ret);
        }
        STATS_NOKEY(c, cmd_flush);
    }
    else /* flush_prefix */
    {
        /* flush_prefix <prefix> [<delay>] [noreply]\r\n */
        if (ntokens == (c->noreply ? 4 : 3)) {
            exptime = 0;
        } else {
            exptime = strtol(tokens[2].value, NULL, 10);
            if (errno == ERANGE) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        char *prefix = tokens[PREFIX_TOKEN].value;
        size_t nprefix = tokens[PREFIX_TOKEN].length;
        if (nprefix == 4 && strncmp(prefix, "null", 4) == 0) {
            /* flush null prefix */
            prefix = NULL;
            nprefix = 0;
        }

        ENGINE_ERROR_CODE ret;
        ret = mc_engine.v1->flush_prefix(mc_engine.v0, c, prefix, nprefix, exptime);
        if (ret == ENGINE_EWOULDBLOCK) {
            c->ewouldblock = true;
            ret = ENGINE_SUCCESS;
        }

        if (settings.detail_enabled) {
            if (ret == ENGINE_SUCCESS || ret == ENGINE_PREFIX_ENOENT) {
                if (stats_prefix_delete(prefix, nprefix) == 0) { /* found */
                    ret = ENGINE_SUCCESS;
                }
            }
        }

        if (ret == ENGINE_SUCCESS) {
            out_string(c, "OK");
        } else if (ret == ENGINE_DISCONNECT) {
            c->state = conn_closing;
        } else if (ret == ENGINE_PREFIX_ENOENT) {
            out_string(c, "NOT_FOUND");
        } else if (ret == ENGINE_ENOTSUP) {
            out_string(c, "SERVER_ERROR not supported");
        } else {
            handle_unexpected_errorcode_ascii(c, ret);
        }
        STATS_NOKEY(c, cmd_flush_prefix);
    }
}

static void process_verbosity_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int level;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (!c->noreply && ntokens == 3) {
        char buf[50];
        sprintf(buf, "verbosity %u\r\nEND", settings.verbose);
        out_string(c, buf);

    } else if ((!c->noreply && ntokens == 4) || (c->noreply && ntokens == 5)) {
        if (!safe_strtoul(tokens[COMMAND_TOKEN+2].value, &level)) {
            c->noreply = false;
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (level > MAX_VERBOSITY_LEVEL) {
            c->noreply = false;
            out_string(c, "SERVER_ERROR cannot change the verbosity over the limit");
            return;
        }

        settings.verbose = level;
        perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
        out_string(c, "END");
        mc_engine.v1->set_verbose(mc_engine.v0, c, settings.verbose);
    } else {
        c->noreply = false;
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_memlimit_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int mlimit;
    assert(c != NULL);

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "memlimit %u\r\nEND", (int)(settings.maxbytes / (1024 * 1024)));
        out_string(c, buf);

    } else if (ntokens == 4 && safe_strtoul(tokens[COMMAND_TOKEN+2].value, &mlimit)) {
        ENGINE_ERROR_CODE ret;
        size_t new_maxbytes = (size_t)mlimit * 1024 * 1024;

        ret = mc_engine.v1->set_memlimit(mc_engine.v0, c, new_maxbytes, settings.sticky_ratio);
        if (ret == ENGINE_SUCCESS) {
            settings.maxbytes = new_maxbytes;
            out_string(c, "END");
        } else { /* ENGINE_EBADVALUE */
            out_string(c, "CLIENT_ERROR bad value");
        }
    } else {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_maxconns_command(conn *c, token_t *tokens, const size_t ntokens) {
    int new_max;
    int curr_conns = mc_stats.curr_conns;
    char buf[50];
    struct rlimit rlim;

    if (ntokens == 3) {
        sprintf(buf, "maxconns %d\r\nEND", settings.maxconns);
        out_string(c, buf);

    } else if (ntokens == 4 && safe_strtol(tokens[COMMAND_TOKEN+2].value, &new_max)) {
        int extra_nfiles = ADMIN_MAX_CONNECTIONS + ZK_CONNECTIONS;
        if (settings.port != 0) {
            extra_nfiles += 2;
        }
        if (settings.udpport != 0) {
            extra_nfiles += settings.num_threads * 2;
        }

        if (new_max + extra_nfiles < (int)(curr_conns * 1.1) || new_max + extra_nfiles > 1000000) {
            out_string(c, "CLIENT_ERROR the value is out of range");
            return;
        }
        if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            out_string(c, "SERVER_ERROR failed to get RLIMIT_NOFILE");
            return;
        }
        if ((rlim.rlim_cur != RLIM_INFINITY) && (new_max + extra_nfiles > (int)rlim.rlim_cur)) {
            out_string(c, "SERVER_ERROR cannot change to the maxconns over the soft limit");
            return;
        }

        SETTING_LOCK();
        settings.maxconns = new_max;
        SETTING_UNLOCK();

        out_string(c, "END");
    } else {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#ifdef CONFIG_MAX_COLLECTION_SIZE
static void process_maxcollsize_command(conn *c, token_t *tokens, const size_t ntokens,
                                        int coll_type)
{
    assert(coll_type==ITEM_TYPE_LIST || coll_type==ITEM_TYPE_SET ||
           coll_type==ITEM_TYPE_BTREE);
    assert(c != NULL);
    int32_t maxsize;

    if (ntokens == 3) {
        char buf[50];
        switch (coll_type) {
          case ITEM_TYPE_LIST:
               sprintf(buf, "max_list_size %d\r\nEND", settings.max_list_size);
               break;
          case ITEM_TYPE_SET:
               sprintf(buf, "max_set_size %d\r\nEND", settings.max_set_size);
               break;
          case ITEM_TYPE_BTREE:
               sprintf(buf, "max_btree_size %d\r\nEND", settings.max_btree_size);
               break;
        }
        out_string(c, buf);
    }
    else if (ntokens == 4 && safe_strtol(tokens[COMMAND_TOKEN+2].value, &maxsize)) {
        ENGINE_ERROR_CODE ret;

        SETTING_LOCK();
        ret = mc_engine.v1->set_maxcollsize(mc_engine.v0, c, coll_type, &maxsize);
        if (ret == ENGINE_SUCCESS) {
            switch (coll_type) {
              case ITEM_TYPE_LIST:
                   settings.max_list_size = maxsize;
                   MAX_LIST_SIZE = maxsize;
                   break;
              case ITEM_TYPE_SET:
                   settings.max_set_size = maxsize;
                   MAX_SET_SIZE = maxsize;
                   break;
              case ITEM_TYPE_BTREE:
                   settings.max_btree_size = maxsize;
                   MAX_BTREE_SIZE = maxsize;
                   break;
            }
        }
        SETTING_UNLOCK();
        if (ret == ENGINE_SUCCESS) {
            out_string(c, "END");
        } else { /* ENGINE_EBADVALUE */
            out_string(c, "CLIENT_ERROR bad value");
        }
    }
    else {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}
#endif

#ifdef ENABLE_ZK_INTEGRATION
static void process_hbtimeout_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int hbtimeout;
    assert(c != NULL);

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "hbtimeout %d\r\nEND", arcus_zk_get_hbtimeout());
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(tokens[COMMAND_TOKEN+2].value, &hbtimeout)) {
        if (arcus_zk_set_hbtimeout((int)hbtimeout) == 0)
            out_string(c, "END");
        else
            out_string(c, "CLIENT_ERROR bad value");
    } else {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_hbfailstop_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int hbfailstop;
    assert(c != NULL);

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "hbfailstop %d\r\nEND", arcus_zk_get_hbfailstop());
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(tokens[COMMAND_TOKEN+2].value, &hbfailstop)) {
        if (arcus_zk_set_hbfailstop((int)hbfailstop) == 0)
            out_string(c, "END");
        else
            out_string(c, "CLIENT_ERROR bad value");
    } else {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}
#endif

static void process_config_command(conn *c, token_t *tokens, const size_t ntokens)
{
    if ((ntokens == 3 || ntokens == 4) &&
        (strcmp(tokens[SUBCOMMAND_TOKEN].value, "maxconns") == 0))
    {
        process_maxconns_command(c, tokens, ntokens);
    }
    else if ((ntokens == 3 || ntokens == 4) &&
             (strcmp(tokens[SUBCOMMAND_TOKEN].value, "memlimit") == 0))
    {
        process_memlimit_command(c, tokens, ntokens);
    }
#ifdef CONFIG_MAX_COLLECTION_SIZE
    else if ((ntokens == 3 || ntokens == 4) &&
             (strcmp(tokens[SUBCOMMAND_TOKEN].value, "max_list_size") == 0))
    {
        process_maxcollsize_command(c, tokens, ntokens, ITEM_TYPE_LIST);
    }
    else if ((ntokens == 3 || ntokens == 4) &&
             (strcmp(tokens[SUBCOMMAND_TOKEN].value, "max_set_size") == 0))
    {
        process_maxcollsize_command(c, tokens, ntokens, ITEM_TYPE_SET);
    }
    else if ((ntokens == 3 || ntokens == 4) &&
             (strcmp(tokens[SUBCOMMAND_TOKEN].value, "max_btree_size") == 0))
    {
        process_maxcollsize_command(c, tokens, ntokens, ITEM_TYPE_BTREE);
    }
#endif
#ifdef ENABLE_ZK_INTEGRATION
    else if ((ntokens == 3 || ntokens == 4) &&
             (strcmp(tokens[SUBCOMMAND_TOKEN].value, "hbtimeout") == 0))
    {
        process_hbtimeout_command(c, tokens, ntokens);
    }
    else if ((ntokens == 3 || ntokens == 4) &&
             (strcmp(tokens[SUBCOMMAND_TOKEN].value, "hbfailstop") == 0))
    {
        process_hbfailstop_command(c, tokens, ntokens);
    }
#endif
    else if ((ntokens >= 3 && ntokens <= 5) &&
             (strcmp(tokens[SUBCOMMAND_TOKEN].value, "verbosity") == 0))
    {
        process_verbosity_command(c, tokens, ntokens);
    }
    else
    {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#ifdef ENABLE_ZK_INTEGRATION
static void process_zk_ensemble_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(ntokens == 2 || ntokens == 3);

    if (arcus_zk_cfg == NULL) {
        out_string(c, "ERROR not using ZooKeeper");
        return;
    }
    if (ntokens == 3) { /* set_zk_ensemble */
        /* The ensemble is a comma separated list of host:port addresses.
         * host1:port1,host2:port2,...
         */
        if (arcus_zk_set_ensemble(tokens[COMMAND_TOKEN+1].value) != 0) {
            out_string(c, "ERROR failed to set the new ensemble address (check logs)");
        } else {
            out_string(c, "OK");
        }
    } else { /* ntokens == 2: show_zk_ensemble */
        char buf[1024];

        if (arcus_zk_get_ensemble_str(buf, sizeof(buf)-16) != 0) {
            out_string(c, "ERROR failed to get the ensemble address");
        } else {
            strcat(buf, "\r\n\n");
            out_string(c, buf);
        }
    }
}
#endif

#ifdef JHPARK_KEY_DUMP
static void process_dump_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *opstr;
    char *modestr = NULL;
    char *filepath = NULL;
    char *prefix = NULL;
    int  nprefix = -1; /* all prefixes */

    /* dump ascii command
     * dump start key [<prefix>] filepath\r\n
     * dump stop\r\n
     */
    opstr = tokens[1].value;
    if (ntokens == 3) {
        if (memcmp(opstr, "stop", 4) != 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    } else if (ntokens == 5 || ntokens == 6) {
        modestr = tokens[2].value;
        if (memcmp(opstr, "start", 5) != 0 || memcmp(modestr, "key", 3) != 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (ntokens == 5) {
            filepath = tokens[3].value;
        } else {
            prefix = tokens[3].value;
            nprefix = tokens[3].length;
            if (nprefix == 4 && strncmp(prefix, "null", 4) == 0) { /* null prefix */
                prefix = NULL;
                nprefix = 0;
            }
            filepath = tokens[4].value;
        }
    } else {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->dump(mc_engine.v0, c, opstr, modestr,
                             prefix, nprefix, filepath);
    if (ret == ENGINE_SUCCESS) {
        out_string(c, "OK");
    } else if (ret == ENGINE_DISCONNECT) {
        c->state = conn_closing;
    } else if (ret == ENGINE_ENOTSUP) {
        out_string(c, "SERVER_ERROR not supported");
    } else if (ret == ENGINE_FAILED) {
        out_string(c, "SERVER_ERROR failed. refer to the reason in server log.");
    } else {
        handle_unexpected_errorcode_ascii(c, ret);
    }
}
#endif

static void process_help_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *type = tokens[COMMAND_TOKEN+1].value;

    if (ntokens > 2 && strcmp(type, "kv") == 0) {
        out_string(c,
        "\t" "set|add|replace <key> <flags> <exptime> <bytes> [noreply]\\r\\n<data>\\r\\n" "\n"
        "\t" "append|prepend <key> <flags> <exptime> <bytes> [noreply]\\r\\n<data>\\r\\n" "\n"
        "\t" "cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\\r\\n<data>\\r\\n" "\n"
        "\t" "get <key>[,<key>...]\\r\\n" "\n"
        "\t" "gets <key>[,<key>...]\\r\\n" "\n"
        "\t" "incr|decr <key> <delta> [<flags> <exptime> <initial>] [noreply]\\r\\n" "\n"
        "\t" "delete <key> [<time>] [noreply]\\r\\n" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "list") == 0) {
        out_string(c,
        "\t" "lop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "lop insert <key> <index> <bytes> [create <attributes>] [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "lop delete <key> <index or range> [drop] [noreply|pipe]\\r\\n" "\n"
        "\t" "lop get <key> <index or range> [delete|drop]\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "set") == 0) {
        out_string(c,
        "\t" "sop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "sop insert <key> <bytes> [create <attributes>] [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "sop delete <key> <bytes> [drop] [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "sop get <key> <count> [delete|drop]\\r\\n" "\n"
        "\t" "sop exist <key> <bytes> [pipe]\\r\\n<data>\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "btree") == 0) {
#ifdef JHPARK_NEW_SMGET_INTERFACE
#if 1 // JHPARK_OLD_SMGET_INTERFACE
        out_string(c,
        "\t" "bop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "bop insert|upsert <key> <bkey> [<eflag>] <bytes> [create <attributes>] [noreply|pipe|getrim]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop update <key> <bkey> [<eflag_update>] <bytes> [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop delete <key> <bkey or \"bkey range\"> [<eflag_filter>] [<count>] [drop] [noreply|pipe]\\r\\n" "\n"
        "\t" "bop get <key> <bkey or \"bkey range\"> [<eflag_filter>] [[<offset>] <count>] [delete|drop]\\r\\n" "\n"
        "\t" "bop count <key> <bkey or \"bkey range\"> [<eflag_filter>] \\r\\n" "\n"
        "\t" "bop incr|decr <key> <bkey> <value> [noreply|pipe]\\r\\n" "\n"
        "\t" "bop mget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] [<offset>] <count>\\r\\n<\"comma separated keys\">\\r\\n" "\n"
        "\t" "bop smget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] [<offset>] <count> [duplicate|unique]\\r\\n<\"comma separated keys\">\\r\\n" "\n"
        "\t" "bop position <key> <bkey> <order>\\r\\n" "\n"
        "\t" "bop pwg <key> <bkey> <order> [<count>]\\r\\n" "\n"
        "\t" "bop gbp <key> <order> <position or \"position range\">\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        "\t" "* <eflag_update> : [<fwhere> <bitwop>] <fvalue>" "\n"
        "\t" "* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>" "\n"
        "\t" "                 : <fwhere> [<bitwop> <foperand>] EQ|NE <comma separated fvalue list>" "\n"
        "\t" "* <bitwop> : &, |, ^" "\n"
        "\t" "* <compop> : EQ, NE, LT, LE, GT, GE" "\n"
        );
#else
        out_string(c,
        "\t" "bop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "bop insert|upsert <key> <bkey> [<eflag>] <bytes> [create <attributes>] [noreply|pipe|getrim]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop update <key> <bkey> [<eflag_update>] <bytes> [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop delete <key> <bkey or \"bkey range\"> [<eflag_filter>] [<count>] [drop] [noreply|pipe]\\r\\n" "\n"
        "\t" "bop get <key> <bkey or \"bkey range\"> [<eflag_filter>] [[<offset>] <count>] [delete|drop]\\r\\n" "\n"
        "\t" "bop count <key> <bkey or \"bkey range\"> [<eflag_filter>] \\r\\n" "\n"
        "\t" "bop incr|decr <key> <bkey> <value> [noreply|pipe]\\r\\n" "\n"
        "\t" "bop mget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] [<offset>] <count>\\r\\n<\"comma separated keys\">\\r\\n" "\n"
        "\t" "bop smget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] [<offset>] <count> [unique]\\r\\n<\"comma separated keys\">\\r\\n" "\n"
        "\t" "bop position <key> <bkey> <order>\\r\\n" "\n"
        "\t" "bop pwg <key> <bkey> <order> [<count>]\\r\\n" "\n"
        "\t" "bop gbp <key> <order> <position or \"position range\">\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        "\t" "* <eflag_update> : [<fwhere> <bitwop>] <fvalue>" "\n"
        "\t" "* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>" "\n"
        "\t" "                 : <fwhere> [<bitwop> <foperand>] EQ|NE <comma separated fvalue list>" "\n"
        "\t" "* <bitwop> : &, |, ^" "\n"
        "\t" "* <compop> : EQ, NE, LT, LE, GT, GE" "\n"
        );
#endif
#else
        out_string(c,
        "\t" "bop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "bop insert|upsert <key> <bkey> [<eflag>] <bytes> [create <attributes>] [noreply|pipe|getrim]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop update <key> <bkey> [<eflag_update>] <bytes> [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop delete <key> <bkey or \"bkey range\"> [<eflag_filter>] [<count>] [drop] [noreply|pipe]\\r\\n" "\n"
        "\t" "bop get <key> <bkey or \"bkey range\"> [<eflag_filter>] [[<offset>] <count>] [delete|drop]\\r\\n" "\n"
        "\t" "bop count <key> <bkey or \"bkey range\"> [<eflag_filter>] \\r\\n" "\n"
        "\t" "bop incr|decr <key> <bkey> <value> [noreply|pipe]\\r\\n" "\n"
        "\t" "bop mget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] [<offset>] <count>\\r\\n<\"comma separated keys\">\\r\\n" "\n"
        "\t" "bop smget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] [<offset>] <count>\\r\\n<\"comma separated keys\">\\r\\n" "\n"
        "\t" "bop position <key> <bkey> <order>\\r\\n" "\n"
        "\t" "bop pwg <key> <bkey> <order> [<count>]\\r\\n" "\n"
        "\t" "bop gbp <key> <order> <position or \"position range\">\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        "\t" "* <eflag_update> : [<fwhere> <bitwop>] <fvalue>" "\n"
        "\t" "* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>" "\n"
        "\t" "                 : <fwhere> [<bitwop> <foperand>] EQ|NE <comma separated fvalue list>" "\n"
        "\t" "* <bitwop> : &, |, ^" "\n"
        "\t" "* <compop> : EQ, NE, LT, LE, GT, GE" "\n"
        );
#endif
    } else if (ntokens > 2 && strcmp(type, "attr") == 0) {
        out_string(c,
        "\t" "getattr <key> [<attribute name> ...]\\r\\n" "\n"
        "\t" "setattr <key> <name>=<value> [<name>=value> ...]\\r\\n" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "admin") == 0) {
        out_string(c,
        "\t" "flush_all [<delay>] [noreply]\\r\\n" "\n"
        "\t" "flush_prefix <prefix> [<delay>] [noreply]\\r\\n" "\n"
        "\n"
        "\t" "scrub [stale]\\r\\n" "\n"
        "\n"
        "\t" "stats\\r\\n" "\n"
        "\t" "stats settings\\r\\n" "\n"
        "\t" "stats items\\r\\n" "\n"
        "\t" "stats slabs\\r\\n" "\n"
        "\t" "stats prefixes\\r\\n" "\n"
        "\t" "stats detail [on|off|dump]\\r\\n" "\n"
        "\t" "stats scrub\\r\\n" "\n"
#ifdef JHPARK_KEY_DUMP
        "\t" "stats dump\\r\\n" "\n"
#endif
        "\t" "stats cachedump <slab_clsid> <limit> [forward|backward [sticky]]\\r\\n" "\n"
        "\t" "stats reset\\r\\n" "\n"
#ifdef COMMAND_LOGGING
        "\n"
        "\t" "cmdlog start [<file_path>]\\r\\n" "\n"
        "\t" "cmdlog stop\\r\\n" "\n"
        "\t" "cmdlog stats\\r\\n" "\n"
#endif
#ifdef DETECT_LONG_QUERY
        "\n"
        "\t" "lqdetect start [<detect_standard>]\\r\\n" "\n"
        "\t" "lqdetect stop\\r\\n" "\n"
        "\t" "lqdetect show\\r\\n" "\n"
        "\t" "lqdetect stats\\r\\n" "\n"
#endif
#ifdef JHPARK_KEY_DUMP
        "\n"
        "\t" "dump start key [<prefix>] filepath\\r\\n" "\n"
        "\t" "dump stop\\r\\n" "\n"
#endif
        "\n"
        "\t" "config verbosity [<verbose>]\\r\\n" "\n"
        "\t" "config memlimit [<memsize(MB)>]\\r\\n" "\n"
        "\t" "config maxconns [<maxconn>]\\r\\n" "\n"
#ifdef CONFIG_MAX_COLLECTION_SIZE
        "\t" "config max_list_size [<maxsize>]\\r\\n" "\n"
        "\t" "config max_set_size [<maxsize>]\\r\\n" "\n"
        "\t" "config max_btree_size [<maxsize>]\\r\\n" "\n"
#endif
#ifdef ENABLE_ZK_INTEGRATION
        "\t" "config hbtimeout [<hbtimeout>]\\r\\n" "\n"
        "\t" "config hbfailstop [<hbfailstop>]\\r\\n" "\n"
#endif
        );
    } else {
       out_string(c,
       "\t" "* Usage: help [kv | list | set | btree | attr | admin ]" "\n"
       );
    }
}

static void process_extension_command(conn *c, token_t *tokens, size_t ntokens)
{
    EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *cmd;
    size_t nbytes = 0;
    char *ptr;

    if (ntokens > 0) {
        if (ntokens == MAX_TOKENS) {
            out_string(c, "ERROR too many arguments");
            return;
        }
        if (tokens[ntokens - 1].length == 0) {
            --ntokens;
        }
    }
    /* ntokens must be larger than 0 in order to avoid segfault in the next for statement. */
    if (ntokens <= 0) {
        out_string(c, "ERROR no arguments");
        return;
    }

    for (cmd = settings.extensions.ascii; cmd != NULL; cmd = cmd->next) {
        if (cmd->accept(cmd->cookie, c, ntokens, tokens, &nbytes, &ptr)) {
            break;
        }
    }
    if (cmd == NULL) {
        out_string(c, "ERROR no matching command");
        return;
    }
    if (nbytes == 0) {
        if (!cmd->execute(cmd->cookie, c, ntokens, tokens,
                          ascii_response_handler)) {
            conn_set_state(c, conn_closing);
        } else {
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                conn_set_state(c, conn_new_cmd);
            }
        }
    } else {
        c->rlbytes = nbytes;
        c->ritem = ptr;
        c->ascii_cmd = cmd;
        /* NOT SUPPORTED YET! */
        conn_set_state(c, conn_nread);
    }
}

#ifdef COMMAND_LOGGING
static void get_cmdlog_stats(char* str)
{
    char *stop_cause_str[5] = {"Not started",                     // CMDLOG_NOT_STARTED
                               "stopped by explicit request",     // CMDLOG_EXPLICIT_STOP
                               "stopped by command log overflow", // CMDLOG_OVERFLOW_STOP
                               "stopped by disk flush error",     // CMDLOG_FLUSHERR_STOP
                               "running"};                        // CMDLOG_RUNNING
    struct cmd_log_stats *stats = cmdlog_stats();

    snprintf(str, CMDLOG_INPUT_SIZE,
            "\t" "Command logging stats : %s" "\n"
            "\t" "The last running time : %d_%d ~ %d_%d" "\n"
            "\t" "The number of entered commands : %d" "\n"
            "\t" "The number of skipped commands : %d" "\n"
            "\t" "The number of log files : %d" "\n"
            "\t" "The log file name: %s/command_%d_%d_%d_{n}.log" "\n",
            (stats->stop_cause >= 0 && stats->stop_cause <= 4 ?
             stop_cause_str[stats->stop_cause] : "unknown"),
            stats->bgndate, stats->bgntime, stats->enddate, stats->endtime,
            stats->entered_commands, stats->skipped_commands,
            stats->file_count,
            stats->dirpath, settings.port, stats->bgndate, stats->bgntime);
}

static void process_logging_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *type = tokens[COMMAND_TOKEN+1].value;
    char *fpath = NULL;
    bool already_check = false;
    int ret;

    if (ntokens > 2 && strcmp(type, "start") == 0) {
        if (ntokens > 3) {
            fpath = tokens[COMMAND_TOKEN+2].value;
        }

        ret = cmdlog_start(fpath, &already_check);
        if (already_check) {
            out_string(c, "\tcommand logging already started.\n");
        } else if (! already_check && ret == 0) {
            out_string(c, "\tcommand logging started.\n");
            cmdlog_in_use = true;
        } else {
            out_string(c, "\tcommand logging failed to start.\n");
            cmdlog_in_use = false;
        }
    } else if (ntokens > 2 && strcmp(type, "stop") == 0) {
        cmdlog_stop(&already_check);
        if (already_check) {
            out_string(c, "\tcommand logging already stopped.\n");
        } else {
            out_string(c, "\tcommand logging stopped.\n");
            cmdlog_in_use = false;
        }
    } else if (ntokens > 2 && strcmp(type, "stats") == 0) {
        char *str = malloc(CMDLOG_INPUT_SIZE * sizeof(char));
        if (str) {
            get_cmdlog_stats(str);
            write_and_free(c, str, strlen(str));
        } else {
            out_string(c, "\tcommand logging failed to get stats memory.\n");
        }
    } else {
        out_string(c, "\t* Usage: cmdlog [start [path] | stop | stats]\n");
    }
}
#endif

#ifdef DETECT_LONG_QUERY
static void lqdetect_make_bkeystring(const unsigned char* from_bkey, const unsigned char* to_bkey,
                                     const int from_nbkey, const int to_nbkey,
                                     const eflag_filter *efilter, char *bufptr) {
    char *tmpptr = bufptr;

    /* bkey */
    if (from_nbkey > 0) {
        memcpy(tmpptr, "0x", 2); tmpptr += 2;
        safe_hexatostr(from_bkey, from_nbkey, tmpptr);
        tmpptr += strlen(tmpptr);
        if (to_bkey != NULL) {
            memcpy(tmpptr, "..0x", 4); tmpptr += 4;
            safe_hexatostr(to_bkey, to_nbkey, tmpptr);
            tmpptr += strlen(tmpptr);
        }
    } else {
        sprintf(tmpptr, "%"PRIu64"", *(uint64_t*)from_bkey);
        tmpptr += strlen(tmpptr);
        if (to_bkey != NULL) {
            sprintf(tmpptr, "..%"PRIu64"", *(uint64_t*)to_bkey);
            tmpptr += strlen(tmpptr);
        }
    }
    /* efilter */
    if (efilter != NULL) {
        strcpy(tmpptr, " efilter");
    }
}

static void lqdetect_get_stats(char* str)
{
    char *stop_cause_str[3] = {"stopped by explicit request",     // LONGQ_EXPLICIT_STOP
                               "stopped by long query overflow", // LONGQ_OVERFLOW_STOP
                               "running"}; // LONGQ_RUNNING
    struct lq_detect_stats stats;
    lqdetect_stats(&stats);

    snprintf(str, LONGQ_STAT_STRLEN,
            "\t" "Long query detection stats : %s" "\n"
            "\t" "The last running time : %d_%d ~ %d_%d" "\n"
            "\t" "The number of total long query commands : %d" "\n"
            "\t" "The detection standard : %d" "\n",
            (stats.stop_cause >= 0 && stats.stop_cause <= 2 ?
             stop_cause_str[stats.stop_cause] : "unknown"),
            stats.bgndate, stats.bgntime, stats.enddate, stats.endtime,
            stats.total_lqcmds, stats.standard);
}

static void lqdetect_show(conn *c)
{
    char *shorted_str[LONGQ_COMMAND_NUM] = {
                            "sop get command entered count :",
                            "lop insert command entered count :",
                            "lop delete command entered count :",
                            "lop get command entered count :",
                            "bop delete command entered count :",
                            "bop get command entered count :",
                            "bop count command entered count :",
                            "bop gbp command entered count :"};
    char *data;
    uint32_t length;
    uint32_t cmdcnt;
    int ii, ret = 0;

    /* create detected long query return string */
    for(ii = 0; ii < LONGQ_COMMAND_NUM; ii++) {
        data = lqdetect_buffer_get(ii, &length, &cmdcnt);
        c->lq_bufcnt++;

        char *count = get_suffix_buffer(c);
        if (count == NULL) {
            out_string(c, "SERVER ERROR out of memory wrting show response");
            lqdetect_buffer_release(c->lq_bufcnt);
            c->lq_bufcnt = 0;
            ret = -1; break;
        }
        int count_len = snprintf(count, SUFFIX_SIZE, " %d\n", cmdcnt);

        if (add_iov(c, shorted_str[ii], strlen(shorted_str[ii])) != 0 ||
            add_iov(c, count, count_len) != 0 ||
            add_iov(c, data, length) != 0 ||
            add_iov(c, "\n", 1) != 0)
            {
                out_string(c, "SERVER ERROR out of memory wrting show response");
                lqdetect_buffer_release(c->lq_bufcnt);
                c->lq_bufcnt = 0;
                ret = -1; break;
            }
    }
    c->suffixcurr = c->suffixlist;

    if (ret == 0) {
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
    }
}

static void process_lqdetect_command(conn *c, token_t *tokens, size_t ntokens)
{
    char *type = tokens[COMMAND_TOKEN+1].value;
    bool already_check = false;
    uint32_t standard;
    int ret;

    if (ntokens > 2 && strcmp(type, "start") == 0) {
        if (ntokens == 3) {
            standard = LONGQ_STANDARD_DEFAULT;
        } else {
            if (! safe_strtoul(tokens[COMMAND_TOKEN+2].value, &standard)) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        ret = lqdetect_start(standard, &already_check);
        if (ret == 0) {
            if (already_check) {
                out_string(c, "\tlong query detection already started.\n");
            } else {
                out_string(c, "\tlong query detection started.\n");
                lqdetect_in_use = true;
            }
        } else {
            out_string(c, "\tlong query detection failed to start.\n");
        }
    } else if (ntokens > 2 && strcmp(type, "stop") == 0) {
        lqdetect_stop(&already_check);
        if (already_check) {
            out_string(c, "\tlong query detection already stopped.\n");
        } else {
            out_string(c, "\tlong query detection stopped.\n");
            lqdetect_in_use = false;
        }
    } else if (ntokens > 2 && strcmp(type, "show") == 0) {
        lqdetect_show(c);
    } else if (ntokens > 2 && strcmp(type, "stats") == 0) {
        char str[LONGQ_STAT_STRLEN];
        lqdetect_get_stats(str);
        out_string(c, str);
    } else {
        out_string(c,
        "\t" "* Usage: lqdetect [start [standard] | stop | show | stats]" "\n"
        );
    }
}
#endif

static inline int get_coll_create_attr_from_tokens(token_t *tokens, const int ntokens,
                                                   int coll_type, item_attr *attrp)
{
    assert(coll_type==ITEM_TYPE_LIST || coll_type==ITEM_TYPE_SET ||
           coll_type==ITEM_TYPE_BTREE);
    int32_t exptime_int;

    /* create attributes: flags, exptime, maxcount, ovflaction, unreadable */
    /* support arcus 1.5 backward compatibility. */
    if (ntokens < 1 || ntokens > 5) return -1;
    //if (ntokens < 3 || ntokens > 5) return -1;

    /* flags */
    if (! safe_strtoul(tokens[0].value, &attrp->flags)) return -1;
    attrp->flags = htonl(attrp->flags);

    /* exptime */
    if (ntokens >= 2) {
        if (! safe_strtol(tokens[1].value, &exptime_int)) return -1;
    } else {
        exptime_int = 0; /* default value */
    }
    attrp->exptime = realtime(exptime_int);

    /* maxcount */
    if (ntokens >= 3) {
        if (! safe_strtol(tokens[2].value, &attrp->maxcount)) return -1;
    } else {
        attrp->maxcount = 0; /* default value */
    }

    attrp->ovflaction = 0; /* undefined : will be set to default later */
    attrp->readable   = 1; /* readable = on */

    if (ntokens >= 4) {
        if (strcmp(tokens[3].value, "error") == 0) {
            attrp->ovflaction = OVFL_ERROR;
        } else {
            if (coll_type == ITEM_TYPE_LIST) {
                if (strcmp(tokens[3].value, "head_trim") == 0)
                    attrp->ovflaction = OVFL_HEAD_TRIM;
                else if (strcmp(tokens[3].value, "tail_trim") == 0)
                    attrp->ovflaction = OVFL_TAIL_TRIM;
            }
            else if (coll_type == ITEM_TYPE_BTREE) {
                if (strcmp(tokens[3].value, "smallest_trim") == 0)
                    attrp->ovflaction = OVFL_SMALLEST_TRIM;
                else if (strcmp(tokens[3].value, "largest_trim") == 0)
                    attrp->ovflaction = OVFL_LARGEST_TRIM;
                else if (strcmp(tokens[3].value, "smallest_silent_trim") == 0)
                    attrp->ovflaction = OVFL_SMALLEST_SILENT_TRIM;
                else if (strcmp(tokens[3].value, "largest_silent_trim") == 0)
                    attrp->ovflaction = OVFL_LARGEST_SILENT_TRIM;
            }
        }
        if (attrp->ovflaction != 0) { /* defined */
            if (ntokens == 5) {
                if (strcmp(tokens[4].value, "unreadable") != 0) return -1;
                attrp->readable = 0;
            }
        } else { /* undefined */
            if (ntokens == 5) return -1; /* ovflaction must be defined */
            else { /* ntokens == 4 */
                if (strcmp(tokens[3].value, "unreadable") != 0) return -1;
                attrp->readable = 0;
            }
        }
    }
    return 0;
}

static void process_lop_get(conn *c, char *key, size_t nkey,
                            int32_t from_index, int32_t to_index,
                            bool delete, bool drop_if_empty)
{
    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t flags, i;
    bool     dropped;
    int      est_count;
    int      need_size;

    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    /* adjust list index */
    if (from_index > MAX_LIST_SIZE)           from_index = MAX_LIST_SIZE;
    else if (from_index < -(MAX_LIST_SIZE+1)) from_index = -(MAX_LIST_SIZE+1);
    if (to_index > MAX_LIST_SIZE)             to_index   = MAX_LIST_SIZE;
    else if (to_index < -(MAX_LIST_SIZE+1))   to_index   = -(MAX_LIST_SIZE+1);

    est_count = MAX_LIST_SIZE;
    if ((from_index >= 0 && to_index >= 0) || (from_index < 0 && to_index < 0)) {
        est_count = (from_index <= to_index ? to_index - from_index + 1
                                            : from_index - to_index + 1);
        if (est_count > MAX_LIST_SIZE) est_count = MAX_LIST_SIZE;
    }
    need_size = est_count * sizeof(eitem*);
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    ret = mc_engine.v1->list_elem_get(mc_engine.v0, c, key, nkey,
                                      from_index, to_index, delete, drop_if_empty,
                                      elem_array, &elem_count, &flags, &dropped, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_lop_get(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

#ifdef DETECT_LONG_QUERY
    /* long query detection */
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        uint32_t overhead = elem_count + (from_index >= 0 ? from_index+1 : -(from_index));
        if (lqdetect_discriminant(overhead)) {
            struct lq_detect_argument argument;
            char *bufptr = argument.range;

            snprintf(bufptr, 36, "%d..%d", from_index, to_index);
            argument.overhead = overhead;
            argument.delete_or_drop = 0;
            if (drop_if_empty) {
                argument.delete_or_drop = 2;
            } else if (delete) {
                argument.delete_or_drop = 1;
            }

            if (! lqdetect_save_cmd(c->client_ip, key, LQCMD_LOP_GET, &argument)) {
                lqdetect_in_use = false;
            }
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        char *respbuf; /* response string buffer */
        char *respptr;

        do {
            need_size = ((2*lenstr_size) + 30) /* response head and tail size */
                      + (elem_count * (lenstr_size+2)); /* response body size */
            if ((respbuf = (char*)malloc(need_size)) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr = respbuf;

            sprintf(respptr, "VALUE %u %u\r\n", htonl(flags), elem_count);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            for (i = 0; i < elem_count; i++) {
                mc_engine.v1->get_list_elem_info(mc_engine.v0, c, elem_array[i], &info);
                sprintf(respptr, "%u ", info.nbytes-2);
                if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                    (add_iov(c, info.value, info.nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += strlen(respptr);
            }
            if (ret == ENGINE_ENOMEM) break;

            sprintf(respptr, "%s\r\n",
                    (delete ? (dropped ? "DELETED_DROPPED" : "DELETED") : "END"));
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, lop_get, key, nkey);
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_resps  = respbuf;
            c->coll_op     = OPERATION_LOP_GET;
            conn_set_state(c, conn_mwrite);
            c->msgcurr     = 0;
        } else { /* ENGINE_ENOMEM */
            STATS_NOKEY(c, cmd_lop_get);
            mc_engine.v1->list_elem_release(mc_engine.v0, c, elem_array, elem_count);
            free(respbuf);
            if (c->ewouldblock)
                c->ewouldblock = false;
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, lop_get, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, lop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_NOKEY(c, cmd_lop_get);
        if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, ret);
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_lop_prepare_nread(conn *c, int cmd, size_t vlen,
                                      char *key, size_t nkey, int32_t index) {
    eitem *elem;

    ENGINE_ERROR_CODE ret;
    if (vlen > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        ret = mc_engine.v1->list_elem_alloc(mc_engine.v0, c, key, nkey, vlen, &elem);
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        stats_prefix_record_lop_insert(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        mc_engine.v1->get_list_elem_info(mc_engine.v0, c, elem, &info);
        c->ritem   = (char *)info.value;
        c->rlbytes = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = OPERATION_LOP_INSERT;
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        c->coll_index  = index;
        conn_set_state(c, conn_nread);
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_lop_insert);
        if (ret == ENGINE_E2BIG) out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;
    }
}

static void process_lop_create(conn *c, char *key, size_t nkey, item_attr *attrp) {

    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->list_struct_create(mc_engine.v0, c, key, nkey, attrp, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_lop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, lop_create, key, nkey);
        out_string(c, "CREATED");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_lop_create);
        if (ret == ENGINE_KEY_EEXISTS) out_string(c, "EXISTS");
        else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);
    }
}

static void process_lop_delete(conn *c, char *key, size_t nkey,
                               int32_t from_index, int32_t to_index, bool drop_if_empty)
{
    uint32_t del_count;
    bool     dropped;

    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->list_elem_delete(mc_engine.v0, c, key, nkey,
                                         from_index, to_index, drop_if_empty,
                                         &del_count, &dropped, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_lop_delete(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

#ifdef DETECT_LONG_QUERY
    /* long query detection */
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        uint32_t overhead = del_count + (from_index >= 0 ? from_index+1 : -(from_index));
        if (lqdetect_discriminant(overhead)) {
            struct lq_detect_argument argument;
            char *bufptr = argument.range;

            snprintf(bufptr, 36, "%d..%d", from_index, to_index);
            argument.overhead = overhead;
            argument.delete_or_drop = 0;
            if (drop_if_empty) {
                argument.delete_or_drop = 2;
            }

            if (! lqdetect_save_cmd(c->client_ip, key, LQCMD_LOP_DELETE, &argument)) {
                lqdetect_in_use = false;
            }
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, lop_delete, key, nkey);
        if (dropped == false) out_string(c, "DELETED");
        else                  out_string(c, "DELETED_DROPPED");
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, lop_delete, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, lop_delete, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_NOKEY(c, cmd_lop_delete);
        if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, ret);
    }
}

static inline int get_list_range_from_str(char *str, int32_t *from_index, int32_t *to_index)
{
    char *delimiter = strstr(str, "..");
    if (delimiter != NULL) { /* range */
        *delimiter = '\0';
        if (! (safe_strtol(str, from_index) &&
               safe_strtol(delimiter + 2, to_index))) {
            *delimiter = '.';
            return -1;
        }
        *delimiter = '.';
    } else { /* single index */
        if (! (safe_strtol(str, from_index)))
            return -1;
        *to_index = *from_index;
    }
    return 0;
}

static void process_lop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *key = tokens[LOP_KEY_TOKEN].value;
    size_t nkey = tokens[LOP_KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if ((ntokens >= 6 && ntokens <= 13) && (strcmp(subcommand,"insert") == 0))
    {
        int32_t index, vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if ((! safe_strtol(tokens[LOP_KEY_TOKEN+1].value, &index)) ||
            (! safe_strtol(tokens[LOP_KEY_TOKEN+2].value, &vlen)) || (vlen < 0)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        int read_ntokens = LOP_KEY_TOKEN + 3;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 2) {
            if (strcmp(tokens[read_ntokens].value, "create") != 0) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
            if (get_coll_create_attr_from_tokens(&tokens[read_ntokens+1], rest_ntokens-1,
                                                 ITEM_TYPE_LIST, c->coll_attrp) != 0) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        } else {
            if (rest_ntokens != 0) {
               out_string(c, "CLIENT_ERROR bad command line format");
               return;
            }
            c->coll_attrp = NULL;
        }

        if (check_and_handle_pipe_state(c, vlen)) {
            process_lop_prepare_nread(c, (int)OPERATION_LOP_INSERT, vlen, key, nkey, index);
        }
    }
    else if ((ntokens >= 7 && ntokens <= 10) && (strcmp(subcommand, "create") == 0))
    {
        set_noreply_maybe(c, tokens, ntokens);

        int read_ntokens = LOP_KEY_TOKEN+1;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        c->coll_attrp = &c->coll_attr_space;
        if (get_coll_create_attr_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                             ITEM_TYPE_LIST, c->coll_attrp) != 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_lop_create(c, key, nkey, c->coll_attrp);
    }
    else if ((ntokens >= 5 && ntokens <= 7) && (strcmp(subcommand, "delete") == 0))
    {
        int32_t from_index, to_index;
        bool drop_if_empty = false;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if (ntokens == 7 && c->noreply == 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (get_list_range_from_str(tokens[LOP_KEY_TOKEN+1].value, &from_index, &to_index)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if ((ntokens == 6 && c->noreply == 0) || (ntokens == 7)) {
            if (strcmp(tokens[LOP_KEY_TOKEN+2].value, "drop")==0) {
                drop_if_empty = true;
            } else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        if (check_and_handle_pipe_state(c, 0)) {
            process_lop_delete(c, key, nkey, from_index, to_index, drop_if_empty);
        }
    }
    else if ((ntokens==5 || ntokens==6) && (strcmp(subcommand, "get") == 0))
    {
        int32_t from_index, to_index;
        bool delete = false;
        bool drop_if_empty = false;

        if (get_list_range_from_str(tokens[LOP_KEY_TOKEN+1].value, &from_index, &to_index)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (ntokens == 6) {
            if (strcmp(tokens[LOP_KEY_TOKEN+2].value, "delete")==0) {
                delete = true;
            } else if (strcmp(tokens[LOP_KEY_TOKEN+2].value, "drop")==0) {
                delete = true;
                drop_if_empty = true;
            } else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        process_lop_get(c, key, nkey, from_index, to_index, delete, drop_if_empty);
    }
    else
    {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_sop_get(conn *c, char *key, size_t nkey, uint32_t count,
                            bool delete, bool drop_if_empty)
{
    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t req_count = count;
    uint32_t flags, i;
    bool     dropped;
    int      need_size;

    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (req_count <= 0 || req_count > MAX_SET_SIZE) req_count = MAX_SET_SIZE;
    need_size = req_count * sizeof(eitem*);
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    ret = mc_engine.v1->set_elem_get(mc_engine.v0, c, key, nkey, req_count,
                                     delete, drop_if_empty, elem_array, &elem_count,
                                     &flags, &dropped, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_sop_get(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

#ifdef DETECT_LONG_QUERY
    /* long query detection */
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (lqdetect_discriminant(elem_count)) {
            struct lq_detect_argument argument;
            char *bufptr = argument.range;

            snprintf(bufptr, 16, "%d", count);
            argument.overhead = elem_count;
            argument.count = count;
            argument.delete_or_drop = 0;
            if (drop_if_empty) {
                argument.delete_or_drop = 2;
            } else if (delete) {
                argument.delete_or_drop = 1;
            }

            if (! lqdetect_save_cmd(c->client_ip, key, LQCMD_SOP_GET, &argument)) {
                lqdetect_in_use = false;
            }
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        char *respbuf; /* response string buffer */
        char *respptr;

        do {
            need_size = ((2*lenstr_size) + 30) /* response head and tail size */
                      + (elem_count * (lenstr_size+2)); /* response body size */
            if ((respbuf = (char*)malloc(need_size)) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr = respbuf;

            sprintf(respptr, "VALUE %u %u\r\n", htonl(flags), elem_count);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            for (i = 0; i < elem_count; i++) {
                mc_engine.v1->get_set_elem_info(mc_engine.v0, c, elem_array[i], &info);
                sprintf(respptr, "%u ", info.nbytes-2);
                if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                    (add_iov(c, info.value, info.nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += strlen(respptr);
            }
            if (ret == ENGINE_ENOMEM) break;

            sprintf(respptr, "%s\r\n",
                    (delete ? (dropped ? "DELETED_DROPPED" : "DELETED") : "END"));
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, sop_get, key, nkey);
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_resps  = respbuf;
            c->coll_op     = OPERATION_SOP_GET;
            conn_set_state(c, conn_mwrite);
            c->msgcurr     = 0;
        } else { /* ENGINE_ENOMEM */
            STATS_NOKEY(c, cmd_sop_get);
            mc_engine.v1->set_elem_release(mc_engine.v0, c, elem_array, elem_count);
            free(respbuf);
            if (c->ewouldblock)
                c->ewouldblock = false;
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, sop_get, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, sop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_NOKEY(c, cmd_sop_get);
        if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, ret);
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_sop_prepare_nread(conn *c, int cmd, size_t vlen, char *key, size_t nkey) {
    eitem *elem = NULL;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if (vlen > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        if (cmd == (int)OPERATION_SOP_INSERT) {
            ret = mc_engine.v1->set_elem_alloc(mc_engine.v0, c, key, nkey, vlen, &elem);
        } else { /* OPERATION_SOP_DELETE or OPERATION_SOP_EXIST */
            if ((elem = (eitem *)malloc(sizeof(elem_value) + vlen)) == NULL)
                ret = ENGINE_ENOMEM;
            else
                ((elem_value*)elem)->nbytes = vlen;
        }
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        if (cmd == (int)OPERATION_SOP_INSERT)
            stats_prefix_record_sop_insert(key, nkey, false);
        else if (cmd == (int)OPERATION_SOP_DELETE)
            stats_prefix_record_sop_delete(key, nkey, false);
        else
            stats_prefix_record_sop_exist(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (cmd == (int)OPERATION_SOP_INSERT) {
            eitem_info info;
            mc_engine.v1->get_set_elem_info(mc_engine.v0, c, elem, &info);
            c->ritem   = (char *)info.value;
            c->rlbytes = vlen;
        } else {
            c->ritem   = ((elem_value *)elem)->value;
            c->rlbytes = vlen;
        }
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = cmd;
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        conn_set_state(c, conn_nread);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        if (cmd == (int)OPERATION_SOP_INSERT) {
            STATS_NOKEY(c, cmd_sop_insert);
        } else if (cmd == (int)OPERATION_SOP_DELETE) {
            STATS_NOKEY(c, cmd_sop_delete);
        } else {
            STATS_NOKEY(c, cmd_sop_exist);
        }

        if (ret == ENGINE_E2BIG) out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;
    }
}

static void process_sop_create(conn *c, char *key, size_t nkey, item_attr *attrp) {
    assert(c->ewouldblock == false);
    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->set_struct_create(mc_engine.v0, c, key, nkey, attrp, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_sop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, sop_create, key, nkey);
        out_string(c, "CREATED");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_sop_create);
        if (ret == ENGINE_KEY_EEXISTS) out_string(c, "EXISTS");
        else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);
    }
}

static void process_sop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *key = tokens[SOP_KEY_TOKEN].value;
    size_t nkey = tokens[SOP_KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if ((ntokens >= 5 && ntokens <= 12) && (strcmp(subcommand,"insert") == 0))
    {
        int32_t vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if ((! safe_strtol(tokens[SOP_KEY_TOKEN+1].value, &vlen)) || (vlen < 0)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        int read_ntokens = SOP_KEY_TOKEN + 2;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 2) {
            if (strcmp(tokens[read_ntokens].value, "create") != 0) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
            if (get_coll_create_attr_from_tokens(&tokens[read_ntokens+1], rest_ntokens-1,
                                                 ITEM_TYPE_SET, c->coll_attrp) != 0) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        } else {
            if (rest_ntokens != 0) {
               out_string(c, "CLIENT_ERROR bad command line format");
               return;
            }
            c->coll_attrp = NULL;
        }

        if (check_and_handle_pipe_state(c, vlen)) {
            process_sop_prepare_nread(c, (int)OPERATION_SOP_INSERT, vlen, key, nkey);
        }
    }
    else if ((ntokens >= 7 && ntokens <= 10) && (strcmp(subcommand, "create") == 0))
    {
        set_noreply_maybe(c, tokens, ntokens);

        int read_ntokens = SOP_KEY_TOKEN+1;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        c->coll_attrp = &c->coll_attr_space;
        if (get_coll_create_attr_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                             ITEM_TYPE_SET, c->coll_attrp) != 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_sop_create(c, key, nkey, c->coll_attrp);
    }
    else if ((ntokens >= 5 && ntokens <= 7) && (strcmp(subcommand, "delete") == 0))
    {
        int32_t vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);
        if (ntokens == 7 && c->noreply == 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        c->coll_drop = false;

        if (! safe_strtol(tokens[SOP_KEY_TOKEN+1].value, &vlen) || vlen < 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        if ((ntokens == 6 && c->noreply == 0) || (ntokens == 7)) {
            if (strcmp(tokens[SOP_KEY_TOKEN+2].value, "drop")==0) {
                c->coll_drop = true;
            } else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        if (check_and_handle_pipe_state(c, vlen)) {
            process_sop_prepare_nread(c, (int)OPERATION_SOP_DELETE, vlen, key, nkey);
        }
    }
    else if ((ntokens==5 || ntokens==6) && strcmp(subcommand, "exist") == 0)
    {
        int32_t vlen;

        set_pipe_maybe(c, tokens, ntokens);

        if (! safe_strtol(tokens[SOP_KEY_TOKEN+1].value, &vlen) || vlen < 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        if (check_and_handle_pipe_state(c, vlen)) {
            process_sop_prepare_nread(c, (int)OPERATION_SOP_EXIST, vlen, key, nkey);
        }
    }
    else if ((ntokens==5 || ntokens==6) && (strcmp(subcommand, "get") == 0))
    {
        bool delete = false;
        bool drop_if_empty = false;
        uint32_t count = 0;

        if (! safe_strtoul(tokens[SOP_KEY_TOKEN+1].value, &count)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (ntokens == 6) {
            if (strcmp(tokens[SOP_KEY_TOKEN+2].value, "delete")==0) {
                delete = true;
            } else if (strcmp(tokens[SOP_KEY_TOKEN+2].value, "drop")==0) {
                delete = true;
                drop_if_empty = true;
            } else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        process_sop_get(c, key, nkey, count, delete, drop_if_empty);
    }
    else
    {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_bop_get(conn *c, char *key, size_t nkey,
                            const bkey_range *bkrange, const eflag_filter *efilter,
                            const uint32_t offset, const uint32_t count,
                            const bool delete, const bool drop_if_empty)
{
    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t access_count;
    uint32_t flags, i;
    bool     dropped_trimmed;
    int      est_count;
    int      need_size;

    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    est_count = MAX_BTREE_SIZE;
    if (count > 0 && count < MAX_BTREE_SIZE) {
        est_count = count;
    }
    need_size = est_count * sizeof(eitem*);
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    ret = mc_engine.v1->btree_elem_get(mc_engine.v0, c, key, nkey,
                                       bkrange, efilter, offset, count,
                                       delete, drop_if_empty,
                                       elem_array, &elem_count, &access_count,
                                       &flags, &dropped_trimmed, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_get(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

#ifdef DETECT_LONG_QUERY
    /* long query detection */
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (lqdetect_discriminant(access_count)) {
            struct lq_detect_argument argument;
            char *bufptr = argument.range;

            lqdetect_make_bkeystring(bkrange->from_bkey, bkrange->to_bkey,
                                     bkrange->from_nbkey, bkrange->to_nbkey,
                                     efilter, bufptr);
            argument.overhead = access_count;
            argument.offset = offset;
            argument.count = count;
            argument.delete_or_drop = 0;
            if (drop_if_empty) {
                argument.delete_or_drop = 2;
            } else if (delete) {
                argument.delete_or_drop = 1;
            }

            if (! lqdetect_save_cmd(c->client_ip, key, LQCMD_BOP_GET, &argument)) {
                lqdetect_in_use = false;
            }
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        char *respbuf; /* response string buffer */
        char *respptr;
        int   resplen;

        do {
            need_size = ((2*lenstr_size) + 30) /* response head and tail size */
                      + (elem_count * ((MAX_BKEY_LENG*2+2) + (MAX_EFLAG_LENG*2+2) + lenstr_size+3)); /* response body size */
            if ((respbuf = (char*)malloc(need_size)) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr = respbuf;

            sprintf(respptr, "VALUE %u %u\r\n", htonl(flags), elem_count);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            for (i = 0; i < elem_count; i++) {
                mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem_array[i], &info);
                resplen = make_bop_elem_response(respptr, &info);
                if ((add_iov(c, respptr, resplen) != 0) ||
                    (add_iov(c, info.value, info.nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
            if (ret == ENGINE_ENOMEM) break;

            if (delete) {
                sprintf(respptr, "%s\r\n", (dropped_trimmed ? "DELETED_DROPPED" : "DELETED"));
            } else {
                sprintf(respptr, "%s\r\n", (dropped_trimmed ? "TRIMMED" : "END"));
            }
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_get, key, nkey);
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_resps  = respbuf;
            c->coll_op     = OPERATION_BOP_GET;
            conn_set_state(c, conn_mwrite);
            c->msgcurr     = 0;
        } else { /* ENGINE_ENOMEM */
            STATS_NOKEY(c, cmd_bop_get);
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
            free(respbuf);
            if (c->ewouldblock)
                c->ewouldblock = false;
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_get, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_EBKEYOOR:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, bop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT)    out_string(c, "NOT_FOUND");
        else if (ret == ENGINE_EBKEYOOR) out_string(c, "OUT_OF_RANGE");
        else                             out_string(c, "UNREADABLE");
        break;
    default:
        STATS_NOKEY(c, cmd_bop_get);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, ret);
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_bop_count(conn *c, char *key, size_t nkey,
                              const bkey_range *bkrange, const eflag_filter *efilter)
{
    uint32_t elem_count;
    uint32_t access_count;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_count(mc_engine.v0, c,
                                         key, nkey, bkrange, efilter,
                                         &elem_count, &access_count, 0);

    if (settings.detail_enabled) {
        stats_prefix_record_bop_count(key, nkey, (ret==ENGINE_SUCCESS));
    }

#ifdef DETECT_LONG_QUERY
    /* long query detection */
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (lqdetect_discriminant(access_count)) {
            struct lq_detect_argument argument;
            char *bufptr = argument.range;

            lqdetect_make_bkeystring(bkrange->from_bkey, bkrange->to_bkey,
                                     bkrange->from_nbkey, bkrange->to_nbkey,
                                     efilter, bufptr);
            argument.overhead = access_count;

            if (! lqdetect_save_cmd(c->client_ip, key, LQCMD_BOP_COUNT, &argument)) {
                lqdetect_in_use = false;
            }
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        char buffer[32];
        STATS_HITS(c, bop_count, key, nkey);

        sprintf(buffer, "COUNT=%u", elem_count);
        out_string(c, buffer);
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, bop_count, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_NOKEY(c, cmd_bop_count);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else out_string(c, "SERVER_ERROR internal");
    }
}

static void process_bop_position(conn *c, char *key, size_t nkey,
                                 const bkey_range *bkrange, ENGINE_BTREE_ORDER order)
{
    int position;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_posi_find(mc_engine.v0, c, key, nkey,
                                        bkrange, order, &position, 0);

    if (settings.detail_enabled) {
        stats_prefix_record_bop_position(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        char buffer[32];
        STATS_ELEM_HITS(c, bop_position, key, nkey);

        sprintf(buffer, "POSITION=%d", position);
        out_string(c, buffer);
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_position, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, bop_position, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_NOKEY(c, cmd_bop_position);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else out_string(c, "SERVER_ERROR internal");
    }
}

static void process_bop_pwg(conn *c, char *key, size_t nkey, const bkey_range *bkrange,
                            ENGINE_BTREE_ORDER order, const uint32_t count)
{
    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t elem_index;
    uint32_t flags, i;
    int      position;
    int      need_size;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    need_size = ((count*2) + 1) * sizeof(eitem*);
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    ret = mc_engine.v1->btree_posi_find_with_get(mc_engine.v0, c, key, nkey,
                                                 bkrange, order, count, &position,
                                                 elem_array, &elem_count, &elem_index,
                                                 &flags, 0);

    if (settings.detail_enabled) {
        stats_prefix_record_bop_pwg(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        char *respbuf; /* response string buffer */
        char *respptr;
        int   resplen;

        do {
            need_size = ((4*lenstr_size) + 30) /* response head and tail size */
                      + (elem_count * ((MAX_BKEY_LENG*2+2) + (MAX_EFLAG_LENG*2+2) + lenstr_size+3)); /* result body size */
            if ((respbuf = (char*)malloc(need_size)) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr = respbuf;

            sprintf(respptr, "VALUE %d %u %u %u\r\n", position, htonl(flags), elem_count, elem_index);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            for (i = 0; i < elem_count; i++) {
                mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem_array[i], &info);
                resplen = make_bop_elem_response(respptr, &info);
                if ((add_iov(c, respptr, resplen) != 0) ||
                    (add_iov(c, info.value, info.nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
            if (ret == ENGINE_ENOMEM) break;

            sprintf(respptr, "%s\r\n", "END");
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_pwg, key, nkey);
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_resps  = respbuf;
            c->coll_op     = OPERATION_BOP_PWG;
            conn_set_state(c, conn_mwrite);
            c->msgcurr     = 0;
        } else { /* ENGINE_ENOMEM */
            STATS_NOKEY(c, cmd_bop_pwg);
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
            free(respbuf);
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_pwg, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, bop_pwg, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_NOKEY(c, cmd_bop_pwg);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else out_string(c, "SERVER_ERROR internal");
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_bop_gbp(conn *c, char *key, size_t nkey, ENGINE_BTREE_ORDER order,
                            uint32_t from_posi, uint32_t to_posi)
{
    eitem  **elem_array = NULL;
    uint32_t elem_count;
    uint32_t flags, i;
    int      est_count;
    int      need_size;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (from_posi > MAX_BTREE_SIZE) from_posi = MAX_BTREE_SIZE;
    if (to_posi   > MAX_BTREE_SIZE) to_posi   = MAX_BTREE_SIZE;

    est_count = (from_posi <= to_posi ? (to_posi - from_posi + 1)
                                      : (from_posi - to_posi + 1));
    need_size = est_count * sizeof(eitem*);
    if ((elem_array = (eitem **)malloc(need_size)) == NULL) {
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    ret = mc_engine.v1->btree_elem_get_by_posi(mc_engine.v0, c, key, nkey,
                                               order, from_posi, to_posi,
                                               elem_array, &elem_count, &flags, 0);

    if (settings.detail_enabled) {
        stats_prefix_record_bop_gbp(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

#ifdef DETECT_LONG_QUERY
    /* long query detection */
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (lqdetect_discriminant(elem_count)) {
            struct lq_detect_argument argument;
            char *bufptr = argument.range;

            snprintf(bufptr, 36, "%d..%d", from_posi, to_posi);
            argument.overhead = elem_count;
            argument.asc_or_desc = order == BTREE_ORDER_ASC ? 1 : 2;

            if (! lqdetect_save_cmd(c->client_ip, key, LQCMD_BOP_GBP, &argument)) {
                lqdetect_in_use = false;
            }
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        char *respbuf; /* response string buffer */
        char *respptr;
        int   resplen;

        do {
            need_size = ((2*lenstr_size) + 30) /* response head and tail size */
                      + (elem_count * ((MAX_BKEY_LENG*2+2) + (MAX_EFLAG_LENG*2+2) + lenstr_size+3)); /* result body size */
            if ((respbuf = (char*)malloc(need_size)) == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr = respbuf;

            sprintf(respptr, "VALUE %u %u\r\n", htonl(flags), elem_count);
            if (add_iov(c, respptr, strlen(respptr)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);

            for (i = 0; i < elem_count; i++) {
                mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem_array[i], &info);
                resplen = make_bop_elem_response(respptr, &info);
                if ((add_iov(c, respptr, resplen) != 0) ||
                    (add_iov(c, info.value, info.nbytes) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
            if (ret == ENGINE_ENOMEM) break;

            sprintf(respptr, "%s\r\n", "END");
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_gbp, key, nkey);
            c->coll_eitem  = (void *)elem_array;
            c->coll_ecount = elem_count;
            c->coll_resps  = respbuf;
            c->coll_op     = OPERATION_BOP_GBP;
            conn_set_state(c, conn_mwrite);
            c->msgcurr     = 0;
        } else { /* ENGINE_ENOMEM */
            STATS_NOKEY(c, cmd_bop_gbp);
            mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
            free(respbuf);
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_gbp, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISS(c, bop_gbp, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_NOKEY(c, cmd_bop_gbp);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else out_string(c, "SERVER_ERROR internal");
    }

    if (ret != ENGINE_SUCCESS && elem_array != NULL) {
        free((void *)elem_array);
    }
}

static void process_bop_update_prepare_nread(conn *c, int cmd, char *key, size_t nkey, const int vlen)
{
    assert(cmd == (int)OPERATION_BOP_UPDATE);
    eitem *elem = NULL;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if (vlen > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        if ((elem = (eitem *)malloc(sizeof(elem_value) + vlen)) == NULL)
            ret = ENGINE_ENOMEM;
        else
            ((elem_value*)elem)->nbytes = vlen;
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        stats_prefix_record_bop_update(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        c->ritem   = ((elem_value *)elem)->value;
        c->rlbytes = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = cmd;
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        conn_set_state(c, conn_nread);
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_update);
        if (ret == ENGINE_E2BIG) out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else out_string(c, "SERVER_ERROR internal");

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;
    }
}

static void process_bop_prepare_nread(conn *c, int cmd, char *key, size_t nkey,
                                      const unsigned char *bkey, const int nbkey,
                                      const unsigned char *eflag, const int neflag,
                                      const int vlen)
{
    eitem *elem;

    ENGINE_ERROR_CODE ret;
    if (vlen > MAX_ELEMENT_BYTES) {
        ret = ENGINE_E2BIG;
    } else {
        ret = mc_engine.v1->btree_elem_alloc(mc_engine.v0, c, key, nkey,
                                             nbkey, neflag, vlen, &elem);
    }

    if (settings.detail_enabled && ret != ENGINE_SUCCESS) {
        stats_prefix_record_bop_insert(key, nkey, false);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        eitem_info info;
        mc_engine.v1->get_btree_elem_info(mc_engine.v0, c, elem, &info);
        memcpy((void*)info.score, bkey, (info.nscore==0 ? sizeof(uint64_t) : info.nscore));
        if (info.neflag > 0)
            memcpy((void*)info.eflag, eflag, info.neflag);
        c->ritem   = (char *)info.value;
        c->rlbytes = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = cmd; /* OPERATION_BOP_INSERT | OPERATION_BOP_UPSERT */
        c->coll_key    = key;
        c->coll_nkey   = nkey;
        conn_set_state(c, conn_nread);
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_insert);
        if (ret == ENGINE_E2BIG) out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;
    }
}

#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
static void process_bop_prepare_nread_keys(conn *c, int cmd, uint32_t vlen, uint32_t kcnt)
{
    eitem *elem = NULL;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    int need_size = 0;

#ifdef SUPPORT_BOP_MGET
    if (cmd == OPERATION_BOP_MGET) {
        int bmget_count = c->coll_numkeys * c->coll_rcount;
        int elem_array_size = bmget_count * sizeof(eitem*);
        int respon_hdr_size = c->coll_numkeys * ((lenstr_size*2)+30);
        int respon_bdy_size = bmget_count * ((MAX_BKEY_LENG*2+2)+(MAX_EFLAG_LENG*2+2)+lenstr_size+15);

        need_size = elem_array_size + respon_hdr_size + respon_bdy_size;
    }
#endif
#ifdef SUPPORT_BOP_SMGET
    if (cmd == OPERATION_BOP_SMGET) {
#if 1 // JHPARK_OLD_SMGET_INTERFACE
      if (c->coll_smgmode == 0) {
        int smget_count = c->coll_roffset + c->coll_rcount;
        int elem_array_size; /* elem pointer array where the found elements will be saved */
        int kmis_array_size; /* key index array where the missed key indexes are to be saved */
        int respon_hdr_size; /* the size of response head and tail */
        int respon_bdy_size; /* the size of response body */

        elem_array_size = smget_count * (sizeof(eitem*) + (2*sizeof(uint32_t)));
        kmis_array_size = c->coll_numkeys * sizeof(uint32_t);
        respon_hdr_size = (2*lenstr_size) + 30; /* result head and tail size */
        respon_bdy_size = smget_count * ((MAX_BKEY_LENG*2+2)+(MAX_EFLAG_LENG*2+2)+(lenstr_size*2)+5); /* result body size */

        need_size = elem_array_size + kmis_array_size + respon_hdr_size + respon_bdy_size;
      } else {
#endif
#ifdef JHPARK_NEW_SMGET_INTERFACE
        int elem_array_size; /* smget element array size */
        int ehit_array_size; /* smget hitted elem array size */
        int emis_array_size; /* element missed keys array size */
#else
        int smget_count = c->coll_roffset + c->coll_rcount;
        int elem_array_size; /* elem pointer array where the found elements will be saved */
        int kmis_array_size; /* key index array where the missed key indexes are to be saved */
#endif
        int respon_hdr_size; /* the size of response head and tail */
        int respon_bdy_size; /* the size of response body */

#ifdef JHPARK_NEW_SMGET_INTERFACE
        elem_array_size = (c->coll_rcount + c->coll_numkeys) * sizeof(eitem*);
        ehit_array_size = c->coll_rcount * sizeof(smget_ehit_t);
        emis_array_size = c->coll_numkeys * sizeof(smget_emis_t);
        respon_hdr_size = (3*lenstr_size) + 50; /* result head and tail size */
        respon_bdy_size = (c->coll_rcount * ((MAX_BKEY_LENG*2+2)+(MAX_EFLAG_LENG*2+2)+(lenstr_size*2)+10))
                        + (c->coll_numkeys * ((MAX_EFLAG_LENG*2+2) + 5)); /* result body size */
        need_size = elem_array_size + ehit_array_size + emis_array_size
                  + respon_hdr_size + respon_bdy_size;
#else
        elem_array_size = smget_count * (sizeof(eitem*) + (2*sizeof(uint32_t)));
        kmis_array_size = c->coll_numkeys * sizeof(uint32_t);
        respon_hdr_size = (2*lenstr_size) + 30; /* result head and tail size */
        respon_bdy_size = smget_count * ((MAX_BKEY_LENG*2+2)+(MAX_EFLAG_LENG*2+2)+(lenstr_size*2)+5); /* result body size */

        need_size = elem_array_size + kmis_array_size + respon_hdr_size + respon_bdy_size;
#endif
#if 1 // JHPARK_OLD_SMGET_INTERFACE
     }
#endif
    }
#endif
    assert(need_size > 0);

    if ((elem = (eitem *)malloc(need_size)) == NULL) {
        ret = ENGINE_ENOMEM;
    } else {
        int kmem_size = GET_8ALIGN_SIZE(vlen)
                      + (sizeof(token_t) * c->coll_numkeys);
        if ((c->coll_strkeys = malloc(kmem_size)) == NULL) {
            free((void*)elem);
            ret = ENGINE_ENOMEM;
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        c->ritem       = (char *)c->coll_strkeys;
        c->rlbytes     = vlen;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 0;
        c->coll_op     = cmd;
        conn_set_state(c, conn_nread);
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
#ifdef SUPPORT_BOP_MGET
        if (cmd == OPERATION_BOP_MGET)
            STATS_NOKEY(c, cmd_bop_mget);
#endif
#ifdef SUPPORT_BOP_SMGET
        if (cmd == OPERATION_BOP_SMGET)
            STATS_NOKEY(c, cmd_bop_smget);
#endif
        if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else out_string(c, "SERVER_ERROR internal");

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;
    }
}
#endif

static void process_bop_create(conn *c, char *key, size_t nkey, item_attr *attrp) {

    assert(c->ewouldblock == false);
    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_struct_create(mc_engine.v0, c, key, nkey, attrp, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, bop_create, key, nkey);
        out_string(c, "CREATED");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    default:
        STATS_NOKEY(c, cmd_bop_create);
        if (ret == ENGINE_KEY_EEXISTS) out_string(c, "EXISTS");
        else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);
    }
}

static void process_bop_delete(conn *c, char *key, size_t nkey,
                               bkey_range *bkrange, eflag_filter *efilter,
                               uint32_t count, bool drop_if_empty)
{
    uint32_t del_count;
    uint32_t acc_count; /* access count */
    bool     dropped;

    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_delete(mc_engine.v0, c, key, nkey,
                                          bkrange, efilter, count, drop_if_empty,
                                          &del_count, &acc_count, &dropped, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_bop_delete(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
    }

#ifdef DETECT_LONG_QUERY
    /* long query detection */
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (lqdetect_discriminant(acc_count)) {
            struct lq_detect_argument argument;
            char *bufptr = argument.range;

            lqdetect_make_bkeystring(bkrange->from_bkey, bkrange->to_bkey,
                                     bkrange->from_nbkey, bkrange->to_nbkey,
                                     efilter, bufptr);
            argument.overhead = acc_count;
            argument.count = count;
            argument.delete_or_drop = 0;
            if (drop_if_empty) {
                argument.delete_or_drop = 2;
            }

            if (! lqdetect_save_cmd(c->client_ip, key, LQCMD_BOP_DELETE, &argument)) {
                lqdetect_in_use = false;
            }
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, bop_delete, key, nkey);
        if (dropped == false) out_string(c, "DELETED");
        else                  out_string(c, "DELETED_DROPPED");
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_delete, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, bop_delete, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_NOKEY(c, cmd_bop_delete);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, ret);
    }
}

static void process_bop_arithmetic(conn *c, char *key, size_t nkey, bkey_range *bkrange,
                                   const bool incr, const bool create,
                                   const uint64_t delta, const uint64_t initial, const eflag_t *eflagp)
{
    ENGINE_ERROR_CODE ret;
    uint64_t result;
    char temp[INCR_MAX_STORAGE_LEN];

    assert(c->ewouldblock == false);

    ret = mc_engine.v1->btree_elem_arithmetic(mc_engine.v0, c, key, nkey, bkrange, incr, create,
                                              delta, initial, eflagp, &result, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        if (incr) {
            stats_prefix_record_bop_incr(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
        } else {
            stats_prefix_record_bop_decr(key, nkey, (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT));
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (incr) {
            STATS_ELEM_HITS(c, bop_incr, key, nkey);
        } else {
            STATS_ELEM_HITS(c, bop_decr, key, nkey);
        }
        snprintf(temp, sizeof(temp), "%"PRIu64, result);
        out_string(c, temp);
        break;
    case ENGINE_KEY_ENOENT:
        if (incr) {
            STATS_MISS(c, bop_incr, key, nkey);
        } else {
            STATS_MISS(c, bop_decr, key, nkey);
        }
        out_string(c, "NOT_FOUND");
        break;
    case ENGINE_ELEM_ENOENT:
        if (incr) {
            STATS_NONE_HITS(c, bop_incr, key, nkey);
        } else {
            STATS_NONE_HITS(c, bop_decr, key, nkey);
        }
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_EINVAL:
        out_string(c, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        break;
    default:
        if (ret == ENGINE_EBADTYPE)       out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY)  out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_EBKEYOOR)  out_string(c, "OUT_OF_RANGE");
        else if (ret == ENGINE_EOVERFLOW) out_string(c, "OVERFLOWED");
        else if (ret == ENGINE_ENOMEM)    out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, ret);
    }
}

static inline int get_bkey_from_str(const char *str, unsigned char *bkey)
{
    if (strncmp(str, "0x", 2) == 0) { /* hexadeciaml bkey */
        if (safe_strtohexa(str+2, bkey, MAX_BKEY_LENG)) {
            return (strlen(str+2)/2);
        }
    } else { /* 64 bit unsigned integer */
        if (safe_strtoull(str, (uint64_t*)bkey)) {
            return 0;
        }
    }
    return -1;
}

static inline int get_eflag_from_str(const char *str, unsigned char *eflag)
{
    if (strncmp(str, "0x", 2) == 0) {
        if (safe_strtohexa(str+2, eflag, MAX_EFLAG_LENG)) {
            return (strlen(str+2)/2);
        }
    }
    return -1;
}

static inline int get_bkey_range_from_str(const char *str, bkey_range *bkrange)
{
    char *delimiter = strstr(str, "..");
    if (delimiter != NULL) { /* range */
        char *nxt = delimiter + 2;
        *delimiter = '\0';
        if (strncmp(str, "0x", 2) == 0 && strncmp(nxt, "0x", 2) == 0) {
            if (! safe_strtohexa(str+2, bkrange->from_bkey, MAX_BKEY_LENG) ||
                ! safe_strtohexa(nxt+2, bkrange->to_bkey,   MAX_BKEY_LENG)) {
                *delimiter = '.'; return -1;
            }
            bkrange->from_nbkey = strlen(str+2)/2;
            bkrange->to_nbkey   = strlen(nxt+2)/2;
        } else {
            if (! safe_strtoull(str, (uint64_t*)bkrange->from_bkey) ||
                ! safe_strtoull(nxt, (uint64_t*)bkrange->to_bkey)) {
                *delimiter = '.'; return -1;
            }
            bkrange->from_nbkey = bkrange->to_nbkey = 0;
        }
        *delimiter = '.';
    } else { /* single index */
        if (strncmp(str, "0x", 2) == 0) { /* hexadeciaml bkey */
            if (! safe_strtohexa(str+2, bkrange->from_bkey, MAX_BKEY_LENG))
                return -1;
            bkrange->from_nbkey = strlen(str+2)/2;
        } else { /* 64 bit unsigned integer */
            if (! safe_strtoull(str, (uint64_t*)bkrange->from_bkey))
                return -1;
            bkrange->from_nbkey = 0;
        }
        bkrange->to_nbkey = BKEY_NULL;
    }
    return 0;
}

static inline int get_position_range_from_str(const char *str, uint32_t *from_posi, uint32_t *to_posi)
{
    char *delimiter = strstr(str, "..");
    if (delimiter != NULL) { /* range */
        char *nxt = delimiter + 2;
        *delimiter = '\0';
        if (! safe_strtoul(str, from_posi) ||
            ! safe_strtoul(nxt, to_posi)) {
            *delimiter = '.'; return -1;
        }
        *delimiter = '.';
    } else { /* single postion */
        if (! safe_strtoul(str, from_posi))
            return -1;
        *to_posi = *from_posi;
    }
    return 0;
}

static inline ENGINE_COMPARE_OP get_compare_op_from_str(const char *str)
{
    if (strlen(str) == 2) {
        if      (str[0] == 'E' && str[1] == 'Q') return COMPARE_OP_EQ;
        else if (str[0] == 'N' && str[1] == 'E') return COMPARE_OP_NE;
        else if (str[0] == 'L') {
                         if      (str[1] == 'T') return COMPARE_OP_LT;
                         else if (str[1] == 'E') return COMPARE_OP_LE;
        }
        else if (str[0] == 'G') {
                         if      (str[1] == 'T') return COMPARE_OP_GT;
                         else if (str[1] == 'E') return COMPARE_OP_GE;
        }
    }
    return COMPARE_OP_MAX;
}

static inline ENGINE_BITWISE_OP get_bitwise_op_from_str(const char *str)
{
    if (strlen(str) == 1) {
        if      (str[0] == '&') return BITWISE_OP_AND;
        else if (str[0] == '|') return BITWISE_OP_OR;
        else if (str[0] == '^') return BITWISE_OP_XOR;
    }
    return BITWISE_OP_MAX;
}

static inline int get_efilter_from_tokens(token_t *tokens, const int ntokens, eflag_filter *efilter)
{
    int token_count = 0;

    /* check and build element eflag filter */
    if (ntokens >= 3 && strncmp(tokens[2].value, "0x", 2) == 0) {
        uint32_t offset;
        int      length;

        if (! safe_strtoul(tokens[0].value, &offset) || offset >= MAX_EFLAG_LENG) {
            return -1;
        }
        efilter->fwhere = (uint8_t)offset;
        token_count = 1;

        if (ntokens >= 5 && strncmp(tokens[4].value, "0x", 2) == 0) {
            efilter->bitwop = get_bitwise_op_from_str(tokens[token_count].value);
            if (efilter->bitwop == BITWISE_OP_MAX) {
                return -1;
            }
            length = get_eflag_from_str(tokens[token_count+1].value, efilter->bitwval);
            if (length < 0) {
                return -1;
            }
            efilter->nbitwval = (uint8_t)length;
            token_count += 2;
        } else {
            efilter->nbitwval = 0;
        }

        efilter->compop = get_compare_op_from_str(tokens[token_count].value);
        if (efilter->compop == COMPARE_OP_MAX) {
            return -1;
        }

        if (efilter->compop == COMPARE_OP_EQ || efilter->compop == COMPARE_OP_NE) {
            /* single value or multiple valeus(IN, NOT IN filter) */
            char *ptr = NULL;
            char *saveptr = NULL;

            efilter->compvcnt = 0;
            ptr = strtok_r(tokens[token_count+1].value, ",", &saveptr);
            while (ptr != NULL) {
                length = get_eflag_from_str(ptr, &efilter->compval[efilter->compvcnt*efilter->ncompval]);
                if (++efilter->compvcnt > MAX_EFLAG_COMPARE_COUNT) {
                    return -1;
                }
                if (efilter->compvcnt == 1) {
                    if (length < 0 || (offset+length) > MAX_EFLAG_LENG)
                        return -1;
                    if (efilter->nbitwval > 0 && length != (int)efilter->nbitwval)
                        return -1;
                    efilter->ncompval = (uint8_t)length;
                } else {
                    if (length != efilter->ncompval)
                        return -1;
                }
                ptr = strtok_r(NULL, ",", &saveptr);
            }
        } else {
            length = get_eflag_from_str(tokens[token_count+1].value, efilter->compval);
            if (length < 0 || (offset+length) > MAX_EFLAG_LENG) {
                return -1;
            }
            if (efilter->nbitwval > 0 && length != (int)efilter->nbitwval) {
                /* the lengths of bitwise operand and compare operand must be same. */
                return -1;
            }
            efilter->ncompval = (uint8_t)length;
            efilter->compvcnt = 1;
        }
        token_count += 2; /* token_count will be 3 or 5 */
    } else {
        efilter->ncompval = 0;
    }
    return token_count;
}

static void process_bop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *key = tokens[BOP_KEY_TOKEN].value;
    size_t nkey = tokens[BOP_KEY_TOKEN].length;
    int subcommid;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if ((ntokens >= 6 && ntokens <= 14) &&
        ((strcmp(subcommand,"insert") == 0 && (subcommid = (int)OPERATION_BOP_INSERT)) ||
         (strcmp(subcommand,"upsert") == 0 && (subcommid = (int)OPERATION_BOP_UPSERT)) ))
    {
        unsigned char bkey[MAX_BKEY_LENG];
        unsigned char eflag[MAX_EFLAG_LENG];
        int      nbkey, neflag;
        int32_t  vlen;
        int      read_ntokens = BOP_KEY_TOKEN+1;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        /* [NOTE] c->coll_drop is also used as getrim flag in bop insert/upsert */
        c->coll_drop = false;
        if (c->noreply == false) {
            if (strcmp(tokens[ntokens-2].value, "getrim") == 0) {
                /* The getrim flag in bop insert/upsert command
                 * If an element is trimmed by maxcount overflow,
                 * the trimmed element must be gotten by clients.
                 */
                c->coll_drop = true;
            }
        }

        nbkey = get_bkey_from_str(tokens[read_ntokens].value, bkey);
        if (nbkey == -1) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        read_ntokens++;

        neflag = 0;
        if (ntokens > 6 && strncmp(tokens[read_ntokens].value, "0x", 2) == 0) {
            neflag = get_eflag_from_str(tokens[read_ntokens].value, eflag);
            if (neflag == -1) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens++;
        }

        if ((! safe_strtol(tokens[read_ntokens].value, &vlen)) || (vlen < 0)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;
        read_ntokens += 1;

        int post_ntokens = 1 + ((c->noreply || c->coll_drop) ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 2) {
            if (strcmp(tokens[read_ntokens].value, "create") != 0) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
            if (get_coll_create_attr_from_tokens(&tokens[read_ntokens+1], rest_ntokens-1,
                                                 ITEM_TYPE_BTREE, c->coll_attrp) != 0) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        } else {
            if (rest_ntokens != 0) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = NULL;
        }

        if (check_and_handle_pipe_state(c, vlen)) {
            process_bop_prepare_nread(c, subcommid, key, nkey, bkey, nbkey, eflag, neflag, vlen);
        }
    }
    else if ((ntokens >= 7 && ntokens <= 10) && (strcmp(subcommand, "create") == 0))
    {
        set_noreply_maybe(c, tokens, ntokens);

        int read_ntokens = BOP_KEY_TOKEN+1;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        c->coll_attrp = &c->coll_attr_space;
        if (get_coll_create_attr_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                             ITEM_TYPE_BTREE, c->coll_attrp) != 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_bop_create(c, key, nkey, c->coll_attrp);
    }
    else if ((ntokens >= 6 && ntokens <= 10) && (strcmp(subcommand, "update") == 0))
    {
        int32_t  vlen;
        int      read_ntokens = BOP_KEY_TOKEN+1;;
        int      post_ntokens;
        int      rest_ntokens;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        post_ntokens = 1 + (c->noreply ? 1 : 0);

        if (get_bkey_range_from_str(tokens[read_ntokens].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        /* Only single bkey supported */
        if (c->coll_bkrange.to_nbkey != BKEY_NULL) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        read_ntokens++;

        rest_ntokens = ntokens - read_ntokens - post_ntokens;
        if (rest_ntokens > 1) {
            uint32_t offset;
            int      length;
            if (rest_ntokens > 2) {
                if (! safe_strtoul(tokens[read_ntokens].value, &offset) || offset >= MAX_EFLAG_LENG) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
                c->coll_eupdate.fwhere = (uint8_t)offset;
                c->coll_eupdate.bitwop = get_bitwise_op_from_str(tokens[read_ntokens+1].value);
                if (c->coll_eupdate.bitwop == BITWISE_OP_MAX) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
                read_ntokens += 2;
                rest_ntokens -= 2;
            } else {
                c->coll_eupdate.bitwop = BITWISE_OP_MAX;
            }
            if (strncmp(tokens[read_ntokens].value, "0x", 2) == 0) {
                length = get_eflag_from_str(tokens[read_ntokens].value, c->coll_eupdate.eflag);
                if (length < 0) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            } else {
                if (! safe_strtol(tokens[read_ntokens].value, &length) || length != 0) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            }
            c->coll_eupdate.neflag = (uint8_t)length;
            read_ntokens += 1;
            rest_ntokens -= 1;
        } else {
            c->coll_eupdate.neflag = EFLAG_NULL;
        }

        if (rest_ntokens != 1) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (! safe_strtol(tokens[read_ntokens].value, &vlen) || (vlen < -1)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (vlen == -1) {
            if (check_and_handle_pipe_state(c, 0)) {
                if (c->coll_eupdate.neflag == EFLAG_NULL) {
                    /* Nothing to update */
                    //out_string(c, "CLIENT_ERROR nothing to update");
                    out_string(c, "NOTHING_TO_UPDATE");
                    return;
                }
                c->coll_key  = key;
                c->coll_nkey = nkey;
                c->coll_op   = OPERATION_BOP_UPDATE;
                process_bop_update_complete(c);
            }
        } else { /* vlen >= 0 */
            vlen += 2;
            if (check_and_handle_pipe_state(c, vlen)) {
                process_bop_update_prepare_nread(c, (int)OPERATION_BOP_UPDATE, key, nkey, vlen);
            }
        }
    }
    else if ((ntokens >= 5 && ntokens <= 13) && (strcmp(subcommand, "delete") == 0))
    {
        uint32_t count = 0;
        bool     drop_if_empty = false;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = 4;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 3) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens += used_ntokens;
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if (rest_ntokens > 0) {
            if (strcmp(tokens[read_ntokens+rest_ntokens-1].value, "drop")==0) {
                drop_if_empty = true;
                rest_ntokens -= 1;
            }
        }

        if (rest_ntokens > 0) {
            if (rest_ntokens == 1) {
                if (! safe_strtoul(tokens[read_ntokens].value, &count)) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            } else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        if (check_and_handle_pipe_state(c, 0)) {
            process_bop_delete(c, key, nkey, &c->coll_bkrange,
                               (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                               count, drop_if_empty);
        }
    }
    else if ((ntokens >= 6 && ntokens <= 9) && (strcmp(subcommand, "incr") == 0 || strcmp(subcommand, "decr") == 0))
    {
        uint64_t delta;
        uint64_t initial = 0;
        bool     incr = (strcmp(subcommand, "incr") == 0 ? true : false);
        bool     create = false;;
        eflag_t  eflagspc;
        eflag_t *eflagptr = NULL;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (c->coll_bkrange.to_nbkey != BKEY_NULL) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (! safe_strtoull(tokens[BOP_KEY_TOKEN+2].value, &delta) || delta < 1) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = 5;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens > 0) {
            if (! safe_strtoull(tokens[BOP_KEY_TOKEN+3].value, &initial)) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            if (rest_ntokens > 1) {
                int neflag = get_eflag_from_str(tokens[BOP_KEY_TOKEN+4].value, eflagspc.val);
                if (neflag == -1) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
                eflagspc.len = neflag;
                eflagptr = &eflagspc;
            }
            create = true;
        }

        if (check_and_handle_pipe_state(c, 0)) {
            process_bop_arithmetic(c, key, nkey, &c->coll_bkrange, incr,
                                   create, delta, initial, eflagptr);
        }
    }
    else if ((ntokens >= 5 && ntokens <= 13) && (strcmp(subcommand, "get") == 0))
    {
        uint32_t offset = 0;
        uint32_t count  = 0;
        bool delete = false;
        bool drop_if_empty = false;

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = 4;
        int post_ntokens = 1; /* "\r\n" */
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 3 && strncmp(tokens[read_ntokens+2].value, "0x", 2)==0) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens += used_ntokens;
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if (rest_ntokens > 0) {
            if (strcmp(tokens[read_ntokens+rest_ntokens-1].value, "delete")==0 ||
                strcmp(tokens[read_ntokens+rest_ntokens-1].value, "drop")==0) {
                delete = true;
                if (strlen(tokens[read_ntokens+rest_ntokens-1].value) == 4)
                    drop_if_empty = true;
                rest_ntokens -= 1;
            }
        }

        if (rest_ntokens > 0) {
            if (rest_ntokens == 1) {
                if (! safe_strtoul(tokens[read_ntokens].value, &count)) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            } else if (rest_ntokens == 2) {
                if ((! safe_strtoul(tokens[read_ntokens].value, &offset)) ||
                    (! safe_strtoul(tokens[read_ntokens+1].value, &count))) {
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            } else {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        process_bop_get(c, key, nkey, &c->coll_bkrange,
                        (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                        offset, count,
                        delete, drop_if_empty);
    }
    else if ((ntokens >= 5 && ntokens <= 10) && (strcmp(subcommand, "count") == 0))
    {
        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = 4;
        int post_ntokens = 1; /* "\r\n" */
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 3) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens += used_ntokens;
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if (rest_ntokens != 0) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_bop_count(c, key, nkey, &c->coll_bkrange,
                          (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter));
    }
#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
    else if ((ntokens >= 7 && ntokens <= 13) &&
             ((strcmp(subcommand, "mget") == 0  && (subcommid = (int)OPERATION_BOP_MGET)) ||
              (strcmp(subcommand, "smget") == 0 && (subcommid = (int)OPERATION_BOP_SMGET)) ))
    {
        uint32_t count, offset = 0;
        uint32_t lenkeys, numkeys;
#ifdef JHPARK_NEW_SMGET_INTERFACE
#if 1 // JHPARK_OLD_SMGET_INTERFACE
        int smgmode = set_smget_mode_maybe(c, tokens, ntokens);
#else
        bool unique = set_unique_maybe(c, tokens, ntokens);
#endif
#endif

        if ((! safe_strtoul(tokens[BOP_KEY_TOKEN].value, &lenkeys)) ||
            (! safe_strtoul(tokens[BOP_KEY_TOKEN+1].value, &numkeys))) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+2].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = 5;
#ifdef JHPARK_NEW_SMGET_INTERFACE
#if 1 // JHPARK_OLD_SMGET_INTERFACE
        int post_ntokens = (smgmode > 0 ? 3 : 2); /* "\r\n" */
#else
        int post_ntokens = (unique ? 3 : 2); /* "\r\n" */
#endif
#else
        int post_ntokens = 2; /* "\r\n" */
#endif
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 3) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens += used_ntokens;
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if (rest_ntokens > 1) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (rest_ntokens == 0) {
            if (! safe_strtoul(tokens[read_ntokens].value, &count)) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        } else { /* rest_ntokens == 1 */
            if ((! safe_strtoul(tokens[read_ntokens].value, &offset)) ||
                (! safe_strtoul(tokens[read_ntokens+1].value, &count))) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        /* validation checking on arguments */
        if (lenkeys < 1 || numkeys < 1 || count < 1) {
            /* ENGINE_EBADVALUE */
            out_string(c, "CLIENT_ERROR bad value"); return;
        }
        lenkeys += 2;
#ifdef SUPPORT_BOP_MGET
        if (subcommid == OPERATION_BOP_MGET) {
            if (numkeys > MAX_BMGET_KEY_COUNT || count > MAX_BMGET_ELM_COUNT) {
                /* ENGINE_EBADVALUE */
                out_string(c, "CLIENT_ERROR bad value"); return;
            }
        }
#endif
#ifdef SUPPORT_BOP_SMGET
        if (subcommid == OPERATION_BOP_SMGET) {
            if (numkeys > MAX_SMGET_KEY_COUNT ||
                (offset+count) > MAX_SMGET_REQ_COUNT) {
                /* ENGINE_EBADVALUE */
                out_string(c, "CLIENT_ERROR bad value"); return;
            }
        }
#endif

        c->coll_numkeys = numkeys;
        c->coll_lenkeys = lenkeys;
        c->coll_roffset = offset;
        c->coll_rcount  = count;
#ifdef JHPARK_NEW_SMGET_INTERFACE
#if 1 // JHPARK_OLD_SMGET_INTERFACE
        c->coll_smgmode = smgmode;
#else
        c->coll_unique  = unique;
#endif
#endif

        process_bop_prepare_nread_keys(c, subcommid, lenkeys, numkeys);
    }
#endif
    else if ((ntokens == 6) && (strcmp(subcommand, "position") == 0))
    {
        ENGINE_BTREE_ORDER order;

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (c->coll_bkrange.to_nbkey != BKEY_NULL) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "asc") == 0) {
            order = BTREE_ORDER_ASC;
        } else if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "desc") == 0) {
            order = BTREE_ORDER_DESC;
        } else {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_bop_position(c, key, nkey, &c->coll_bkrange, order);
    }
    else if ((ntokens == 6 || ntokens == 7) && (strcmp(subcommand, "pwg") == 0))
    {
        ENGINE_BTREE_ORDER order;
        uint32_t count = 0;

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (c->coll_bkrange.to_nbkey != BKEY_NULL) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "asc") == 0) {
            order = BTREE_ORDER_ASC;
        } else if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "desc") == 0) {
            order = BTREE_ORDER_DESC;
        } else {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (ntokens == 7) {
            if (! safe_strtoul(tokens[BOP_KEY_TOKEN+3].value, &count)) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            if (count > 100) { /* max limit on count: 100 */
                out_string(c, "CLIENT_ERROR too large count value");
                return;
            }
        }

        process_bop_pwg(c, key, nkey, &c->coll_bkrange, order, count);
    }
    else if ((ntokens == 6) && (strcmp(subcommand, "gbp") == 0))
    {
        uint32_t from_posi, to_posi;
        ENGINE_BTREE_ORDER order;

        if (strcmp(tokens[BOP_KEY_TOKEN+1].value, "asc") == 0) {
            order = BTREE_ORDER_ASC;
        } else if (strcmp(tokens[BOP_KEY_TOKEN+1].value, "desc") == 0) {
            order = BTREE_ORDER_DESC;
        } else {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (get_position_range_from_str(tokens[BOP_KEY_TOKEN+2].value, &from_posi, &to_posi)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_bop_gbp(c, key, nkey, order, from_posi, to_posi);
    }
    else
    {
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_getattr_command(conn *c, token_t *tokens, const size_t ntokens) {
    assert(c != NULL);
    char   *key = tokens[KEY_TOKEN].value;
    size_t nkey = tokens[KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    item_attr attr_data;
    ENGINE_ITEM_ATTR attr_ids[ATTR_END];
    uint32_t attr_count = 0;
    int i;

    if (ntokens > 3) {
        char *name;
        for (i = KEY_TOKEN+1; i < ntokens-1; i++) {
            name = tokens[i].value;
            if (strcmp(name, "flags")==0)               attr_ids[attr_count++] = ATTR_FLAGS;
            else if (strcmp(name, "expiretime")==0)     attr_ids[attr_count++] = ATTR_EXPIRETIME;
            else if (strcmp(name, "type")==0)           attr_ids[attr_count++] = ATTR_TYPE;
            else if (strcmp(name, "count")==0)          attr_ids[attr_count++] = ATTR_COUNT;
            else if (strcmp(name, "maxcount")==0)       attr_ids[attr_count++] = ATTR_MAXCOUNT;
            else if (strcmp(name, "overflowaction")==0) attr_ids[attr_count++] = ATTR_OVFLACTION;
            else if (strcmp(name, "readable")==0)       attr_ids[attr_count++] = ATTR_READABLE;
            else if (strcmp(name, "maxbkeyrange")==0)   attr_ids[attr_count++] = ATTR_MAXBKEYRANGE;
            else if (strcmp(name, "minbkey")==0)        attr_ids[attr_count++] = ATTR_MINBKEY;
            else if (strcmp(name, "maxbkey")==0)        attr_ids[attr_count++] = ATTR_MAXBKEY;
            else if (strcmp(name, "trimmed")==0)        attr_ids[attr_count++] = ATTR_TRIMMED;
            else break;
        }
        if (i < ntokens-1) {
            out_string(c, "ATTR_ERROR not found");
            return;
        }
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->getattr(mc_engine.v0, c, key, nkey,
                                attr_ids, attr_count, &attr_data, 0);

    if (settings.detail_enabled) {
        stats_prefix_record_getattr(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        char buffer[1024];
        char *str = &buffer[0];
        char *ptr = str; *ptr = '\0';

        STATS_HITS(c, getattr, key, nkey);

        if (attr_count > 0) {
            for (i = 0; i < attr_count; i++) {
                if (attr_ids[i] == ATTR_TYPE)
                    sprintf(ptr, "ATTR type=%s\r\n", get_item_type_str(attr_data.type));
                else if (attr_ids[i] == ATTR_FLAGS)
                    sprintf(ptr, "ATTR flags=%u\r\n", htonl(attr_data.flags));
                else if (attr_ids[i] == ATTR_EXPIRETIME)
                    sprintf(ptr, "ATTR expiretime=%d\r\n", (int32_t)attr_data.exptime);
                else if (attr_ids[i] == ATTR_COUNT)
                    sprintf(ptr, "ATTR count=%d\r\n", attr_data.count);
                else if (attr_ids[i] == ATTR_MAXCOUNT)
                    sprintf(ptr, "ATTR maxcount=%d\r\n", attr_data.maxcount);
                else if (attr_ids[i] == ATTR_OVFLACTION)
                    sprintf(ptr, "ATTR overflowaction=%s\r\n", get_ovflaction_str(attr_data.ovflaction));
                else if (attr_ids[i] == ATTR_READABLE)
                    sprintf(ptr, "ATTR readable=%s\r\n", (attr_data.readable ? "on" : "off"));
                else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
                    if (attr_data.maxbkeyrange.len == BKEY_NULL) {
                        sprintf(ptr, "ATTR maxbkeyrange=0\r\n");
                    } else {
                        if (attr_data.maxbkeyrange.len == 0) {
                            uint64_t bkey_temp;
                            memcpy((unsigned char*)&bkey_temp, attr_data.maxbkeyrange.val, sizeof(uint64_t));
                            sprintf(ptr, "ATTR maxbkeyrange=%"PRIu64"\r\n", bkey_temp);
                            //sprintf(ptr, "ATTR maxbkeyrange=%"PRIu64"\r\n", *(uint64_t*)attr_data.maxbkeyrange.val);
                        } else {
                            sprintf(ptr, "ATTR maxbkeyrange=0x");
                            ptr += strlen(ptr);
                            safe_hexatostr(attr_data.maxbkeyrange.val, attr_data.maxbkeyrange.len, ptr);
                            ptr += strlen(ptr);
                            sprintf(ptr, "\r\n");
                        }
                    }
                }
                else if (attr_ids[i] == ATTR_MINBKEY) {
                    if (attr_data.count > 0) {
                        if (attr_data.minbkey.len == 0) {
                            uint64_t bkey_temp;
                            memcpy((unsigned char*)&bkey_temp, attr_data.minbkey.val, sizeof(uint64_t));
                            sprintf(ptr, "ATTR minbkey=%"PRIu64"\r\n", bkey_temp);
                            //sprintf(ptr, "ATTR minbkey=%"PRIu64"\r\n", *(uint64_t*)attr_data.minbkey.val);
                        } else {
                            sprintf(ptr, "ATTR minbkey=0x");
                            ptr += strlen(ptr);
                            safe_hexatostr(attr_data.minbkey.val, attr_data.minbkey.len, ptr);
                            ptr += strlen(ptr);
                            sprintf(ptr, "\r\n");
                        }
                    } else {
                        sprintf(ptr, "ATTR minbkey=-1\r\n");
                    }
                }
                else if (attr_ids[i] == ATTR_MAXBKEY) {
                    if (attr_data.count > 0) {
                        if (attr_data.maxbkey.len == 0) {
                            uint64_t bkey_temp;
                            memcpy((unsigned char*)&bkey_temp, attr_data.maxbkey.val, sizeof(uint64_t));
                            sprintf(ptr, "ATTR maxbkey=%"PRIu64"\r\n", bkey_temp);
                            //sprintf(ptr, "ATTR maxbkey=%"PRIu64"\r\n", *(uint64_t*)attr_data.maxbkey.val);
                        } else {
                            sprintf(ptr, "ATTR maxbkey=0x");
                            ptr += strlen(ptr);
                            safe_hexatostr(attr_data.maxbkey.val, attr_data.maxbkey.len, ptr);
                            ptr += strlen(ptr);
                            sprintf(ptr, "\r\n");
                        }
                    } else {
                        sprintf(ptr, "ATTR maxbkey=-1\r\n");
                    }
                }
                else if (attr_ids[i] == ATTR_TRIMMED)
                    sprintf(ptr, "ATTR trimmed=%u\r\n", (attr_data.trimmed != 0 ? 1 : 0));
                ptr += strlen(ptr);
            }
        } else { /* attr_count == 0 */
            sprintf(ptr, "ATTR type=%s\r\n", get_item_type_str(attr_data.type));
            ptr += strlen(ptr);
            sprintf(ptr, "ATTR flags=%u\r\n", htonl(attr_data.flags));
            ptr += strlen(ptr);
            sprintf(ptr, "ATTR expiretime=%d\r\n", (int32_t)attr_data.exptime);
            ptr += strlen(ptr);
            if (attr_data.type == ITEM_TYPE_LIST || attr_data.type == ITEM_TYPE_SET ||
                attr_data.type == ITEM_TYPE_BTREE) {
                sprintf(ptr, "ATTR count=%d\r\n", attr_data.count);
                ptr += strlen(ptr);
                sprintf(ptr, "ATTR maxcount=%d\r\n", attr_data.maxcount);
                ptr += strlen(ptr);
                sprintf(ptr, "ATTR overflowaction=%s\r\n", get_ovflaction_str(attr_data.ovflaction));
                ptr += strlen(ptr);
                sprintf(ptr, "ATTR readable=%s\r\n", (attr_data.readable ? "on" : "off"));
                ptr += strlen(ptr);
            }
            if (attr_data.type == ITEM_TYPE_BTREE) {
                if (attr_data.maxbkeyrange.len == BKEY_NULL) {
                    sprintf(ptr, "ATTR maxbkeyrange=0\r\n");
                } else {
                    if (attr_data.maxbkeyrange.len == 0) {
                        uint64_t bkey_temp;
                        memcpy((unsigned char*)&bkey_temp, attr_data.maxbkeyrange.val, sizeof(uint64_t));
                        sprintf(ptr, "ATTR maxbkeyrange=%"PRIu64"\r\n", bkey_temp);
                        //sprintf(ptr, "ATTR maxbkeyrange=%"PRIu64"\r\n", *(uint64_t*)attr_data.maxbkeyrange.val);
                    } else {
                        sprintf(ptr, "ATTR maxbkeyrange=0x");
                        ptr += strlen(ptr);
                        safe_hexatostr(attr_data.maxbkeyrange.val, attr_data.maxbkeyrange.len, ptr);
                        ptr += strlen(ptr);
                        sprintf(ptr, "\r\n");
                    }
                }
                ptr += strlen(ptr);
                if (attr_data.count > 0) {
                    if (attr_data.minbkey.len == 0) {
                        uint64_t bkey_temp;
                        memcpy((unsigned char*)&bkey_temp, attr_data.minbkey.val, sizeof(uint64_t));
                        sprintf(ptr, "ATTR minbkey=%"PRIu64"\r\n", bkey_temp);
                        //sprintf(ptr, "ATTR minbkey=%"PRIu64"\r\n", *(uint64_t*)attr_data.minbkey.val);
                    } else {
                        sprintf(ptr, "ATTR minbkey=0x");
                        ptr += strlen(ptr);
                        safe_hexatostr(attr_data.minbkey.val, attr_data.minbkey.len, ptr);
                        ptr += strlen(ptr);
                        sprintf(ptr, "\r\n");
                    }
                } else {
                    sprintf(ptr, "ATTR minbkey=-1\r\n");
                }

                ptr += strlen(ptr);
                if (attr_data.count > 0) {
                    if (attr_data.maxbkey.len == 0) {
                        uint64_t bkey_temp;
                        memcpy((unsigned char*)&bkey_temp, attr_data.maxbkey.val, sizeof(uint64_t));
                        sprintf(ptr, "ATTR maxbkey=%"PRIu64"\r\n", bkey_temp);
                        //sprintf(ptr, "ATTR maxbkey=%"PRIu64"\r\n", *(uint64_t*)attr_data.maxbkey.val);
                    } else {
                        sprintf(ptr, "ATTR maxbkey=0x");
                        ptr += strlen(ptr);
                        safe_hexatostr(attr_data.maxbkey.val, attr_data.maxbkey.len, ptr);
                        ptr += strlen(ptr);
                        sprintf(ptr, "\r\n");
                    }
                } else {
                    sprintf(ptr, "ATTR maxbkey=-1\r\n");
                }
                ptr += strlen(ptr);
                sprintf(ptr, "ATTR trimmed=%u\r\n", (attr_data.trimmed != 0 ? 1 : 0));
                ptr += strlen(ptr);
            }
        }
        sprintf(ptr, "END");
        out_string(c, str);
        }
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, getattr, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_NOKEY(c, cmd_getattr);
        if (ret == ENGINE_EBADATTR) out_string(c, "ATTR_ERROR not found");
        else out_string(c, "SERVER_ERROR internal");
    }
}

static void process_setattr_command(conn *c, token_t *tokens, const size_t ntokens) {
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = tokens[KEY_TOKEN].value;
    size_t nkey = tokens[KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    item_attr attr_data;
    ENGINE_ITEM_ATTR attr_ids[ATTR_END];
    uint32_t attr_count = 0;
    int i;
    char *name, *value, *equal;

    for (i = KEY_TOKEN+1; i < ntokens-1; i++) {
        if ((equal = strchr(tokens[i].value, '=')) == NULL) break;
        *equal = '\0';
        name = tokens[i].value; value = equal + 1;

        if (strcmp(name, "expiretime")==0) {
            int32_t exptime_int;
            attr_ids[attr_count++] = ATTR_EXPIRETIME;
            if (! safe_strtol(value, &exptime_int)) break;
            attr_data.exptime = realtime(exptime_int);
        } else if (strcmp(name, "maxcount")==0) {
            attr_ids[attr_count++] = ATTR_MAXCOUNT;
            if (! safe_strtol(value, &attr_data.maxcount)) break;
        } else if (strcmp(name, "overflowaction")==0) {
            attr_ids[attr_count++] = ATTR_OVFLACTION;
            if (strcmp(value, "error")==0)
                attr_data.ovflaction = OVFL_ERROR;
            else if (strcmp(value, "head_trim")==0)
                attr_data.ovflaction = OVFL_HEAD_TRIM;
            else if (strcmp(value, "tail_trim")==0)
                attr_data.ovflaction = OVFL_TAIL_TRIM;
            else if (strcmp(value, "smallest_trim")==0)
                attr_data.ovflaction = OVFL_SMALLEST_TRIM;
            else if (strcmp(value, "largest_trim")==0)
                attr_data.ovflaction = OVFL_LARGEST_TRIM;
            else if (strcmp(value, "smallest_silent_trim")==0)
                attr_data.ovflaction = OVFL_SMALLEST_SILENT_TRIM;
            else if (strcmp(value, "largest_silent_trim")==0)
                attr_data.ovflaction = OVFL_LARGEST_SILENT_TRIM;
            else {
                out_string(c, "ATTR_ERROR bad value");
                return;
            }
        } else if (strcmp(name, "readable")==0) {
            attr_ids[attr_count++] = ATTR_READABLE;
            if (strcmp(value, "on")==0)
                attr_data.readable = 1;
            else {
                out_string(c, "ATTR_ERROR bad value");
                return;
            }
        } else if (strcmp(name, "maxbkeyrange")==0) {
            int length;
            length = get_bkey_from_str(value, attr_data.maxbkeyrange.val);
            if (length == -1) {
                out_string(c, "ATTR_ERROR bad value");
                return;
            }
            attr_data.maxbkeyrange.len = (uint8_t)length;
            if (attr_data.maxbkeyrange.len == 0) {
                uint64_t bkey_temp;
                memcpy((unsigned char*)&bkey_temp, attr_data.maxbkeyrange.val, sizeof(uint64_t));
                if (bkey_temp == 0) attr_data.maxbkeyrange.len = BKEY_NULL; /* reset maxbkeyrange */
            }
            //if (attr_data.maxbkeyrange.len == 0 && *(uint64_t*)attr_data.maxbkeyrange.val == 0)
            //    attr_data.maxbkeyrange.len = BKEY_NULL; /* reset maxbkeyrange */
            attr_ids[attr_count++] = ATTR_MAXBKEYRANGE;
        } else {
            break;
        }
    }
    if (i < ntokens-1) {
        out_string(c, "ATTR_ERROR not found");
        return;
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->setattr(mc_engine.v0, c, key, nkey,
                                attr_ids, attr_count, &attr_data, 0);
    if (ret == ENGINE_EWOULDBLOCK) {
        c->ewouldblock = true;
        ret = ENGINE_SUCCESS;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_setattr(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, setattr, key, nkey);
        out_string(c, "OK");
        break;
    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISS(c, setattr, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_NOKEY(c, cmd_setattr);
        if (ret == ENGINE_EBADATTR) out_string(c, "ATTR_ERROR not found");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "ATTR_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, ret);
    }
}

static void process_command(conn *c, char *command, int cmdlen)
{
    /* One more token is reserved in tokens strucure
     * for keeping the length of untokenized command.
     */
    token_t tokens[MAX_TOKENS+1];
    size_t ntokens;
    int comm;

    assert(c != NULL);
    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

    if (settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                       "<%d %s\n", c->sfd, command);
    }

    /*
     * for commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        out_string(c, "SERVER_ERROR out of memory preparing response");
        return;
    }

#ifdef COMMAND_LOGGING
    if (cmdlog_in_use) {
        if (cmdlog_write(c->client_ip, command) == false) {
            cmdlog_in_use = false;
        }
    }
#endif

    ntokens = tokenize_command(command, cmdlen, tokens, MAX_TOKENS);

    if ((ntokens >= 3) && ((strcmp(tokens[COMMAND_TOKEN].value, "get" ) == 0) ||
                           (strcmp(tokens[COMMAND_TOKEN].value, "bget") == 0)))
    {
        process_get_command(c, tokens, ntokens, false);
    }
    else if ((ntokens >= 3) && (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0))
    {
        process_get_command(c, tokens, ntokens, true);
    }
    else if ((ntokens == 6 || ntokens == 7) &&
        ((strcmp(tokens[COMMAND_TOKEN].value, "add"    ) == 0 && (comm = (int)OPERATION_ADD)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "set"    ) == 0 && (comm = (int)OPERATION_SET)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "replace") == 0 && (comm = (int)OPERATION_REPLACE)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "prepend") == 0 && (comm = (int)OPERATION_PREPEND)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "append" ) == 0 && (comm = (int)OPERATION_APPEND)) ))
    {
        process_update_command(c, tokens, ntokens, (ENGINE_STORE_OPERATION)comm, false);
    }
    else if ((ntokens == 7 || ntokens == 8) &&
         (strcmp(tokens[COMMAND_TOKEN].value, "cas"    ) == 0 && (comm = (int)OPERATION_CAS)))
    {
        process_update_command(c, tokens, ntokens, (ENGINE_STORE_OPERATION)comm, true);
    }
    else if ((ntokens == 4 || ntokens == 5 || ntokens == 7 || ntokens == 8) &&
        (strcmp(tokens[COMMAND_TOKEN].value, "incr") == 0))
    {
        process_arithmetic_command(c, tokens, ntokens, 1);
    }
    else if ((ntokens == 4 || ntokens == 5 || ntokens == 7 || ntokens == 8) &&
        (strcmp(tokens[COMMAND_TOKEN].value, "decr") == 0))
    {
        process_arithmetic_command(c, tokens, ntokens, 0);
    }
    else if ((ntokens >= 3 && ntokens <= 5) && (strcmp(tokens[COMMAND_TOKEN].value, "delete") == 0))
    {
        process_delete_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 5 && ntokens <= 13) && (strcmp(tokens[COMMAND_TOKEN].value, "lop") == 0))
    {
        process_lop_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 5 && ntokens <= 12) && (strcmp(tokens[COMMAND_TOKEN].value, "sop") == 0))
    {
        process_sop_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 5 && ntokens <= 14) && (strcmp(tokens[COMMAND_TOKEN].value, "bop") == 0))
    {
        process_bop_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 3 && ntokens <= 11) && (strcmp(tokens[COMMAND_TOKEN].value, "getattr") == 0))
    {
        process_getattr_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 4 && ntokens <=  8) && (strcmp(tokens[COMMAND_TOKEN].value, "setattr") == 0))
    {
        process_setattr_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "stats") == 0))
    {
        process_stat(c, tokens, ntokens);
    }
    else if ((ntokens >= 2 && ntokens <= 4) && (strcmp(tokens[COMMAND_TOKEN].value, "flush_all") == 0))
    {
        process_flush_command(c, tokens, ntokens, true);
    }
    else if ((ntokens >= 3 && ntokens <= 5) && (strcmp(tokens[COMMAND_TOKEN].value, "flush_prefix") == 0))
    {
        process_flush_command(c, tokens, ntokens, false);
    }
    else if ((ntokens >  2) && (strcmp(tokens[COMMAND_TOKEN].value, "config") == 0))
    {
        process_config_command(c, tokens, ntokens);
    }
#ifdef ENABLE_ZK_INTEGRATION
    else if ((ntokens == 3) && (strcmp(tokens[COMMAND_TOKEN].value, "set_zk_ensemble") == 0))
    {
        process_zk_ensemble_command(c, tokens, ntokens);
    }
    else if ((ntokens == 2) && (strcmp(tokens[COMMAND_TOKEN].value, "show_zk_ensemble") == 0))
    {
        process_zk_ensemble_command(c, tokens, ntokens);
    }
#endif
    else if ((ntokens == 2) && (strcmp(tokens[COMMAND_TOKEN].value, "version") == 0))
    {
        out_string(c, "VERSION " VERSION);
    }
#ifdef JHPARK_KEY_DUMP
    else if ((ntokens >= 3) && (strcmp(tokens[COMMAND_TOKEN].value, "dump") == 0))
    {
        process_dump_command(c, tokens, ntokens);
    }
#endif
    else if ((ntokens == 2) && (strcmp(tokens[COMMAND_TOKEN].value, "quit") == 0))
    {
        STATS_LOCK();
        mc_stats.quit_conns++;
        STATS_UNLOCK();
        conn_set_state(c, conn_closing);
    }
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "help") == 0))
    {
        process_help_command(c, tokens, ntokens);
    }
#ifdef COMMAND_LOGGING
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "cmdlog") == 0))
    {
        process_logging_command(c, tokens, ntokens);
    }
#endif
#ifdef DETECT_LONG_QUERY
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "lqdetect") == 0))
    {
        process_lqdetect_command(c, tokens, ntokens);
    }
#endif
    else /* no matching command */
    {
        if (settings.extensions.ascii != NULL) {
            process_extension_command(c, tokens, ntokens);
        } else {
            out_string(c, "ERROR unknown command");
        }
    }
    return;
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(conn *c) {
    assert(c != NULL);
    assert(c->rcurr <= (c->rbuf + c->rsize));
    assert(c->rbytes > 0);

    if (c->protocol == negotiating_prot || c->transport == udp_transport)  {
        if ((unsigned char)c->rbuf[0] == (unsigned char)PROTOCOL_BINARY_REQ) {
            c->protocol = binary_prot;
        } else {
            c->protocol = ascii_prot;
        }

        if (settings.verbose > 1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c,
                    "%d: Client using the %s protocol\n", c->sfd,
                    prot_text(c->protocol));
        }
    }

    if (c->protocol == binary_prot) {
        /* Do we have the complete packet header? */
        if (c->rbytes < sizeof(c->binary_header)) {
            /* need more data! */
            return 0;
        } else {
#ifdef NEED_ALIGN
            if (((long)(c->rcurr)) % 8 != 0) {
                /* must realign input buffer */
                memmove(c->rbuf, c->rcurr, c->rbytes);
                c->rcurr = c->rbuf;
                if (settings.verbose > 1) {
                    mc_logger->log(EXTENSION_LOG_DEBUG, c,
                            "%d: Realign input buffer\n", c->sfd);
                }
            }
#endif
            protocol_binary_request_header* req;
            req = (protocol_binary_request_header*)c->rcurr;

            if (settings.verbose > 1) {
                /* Dump the packet before we convert it to host order */
                char buffer[1024];
                ssize_t nw;
                nw = bytes_to_output_string(buffer, sizeof(buffer), c->sfd,
                                            true, "Read binary protocol data:",
                                            (const char*)req->bytes,
                                            sizeof(req->bytes));
                if (nw != -1) {
                    mc_logger->log(EXTENSION_LOG_DEBUG, c, "%s", buffer);
                }
            }

            c->binary_header = *req;
            c->binary_header.request.keylen = ntohs(req->request.keylen);
            c->binary_header.request.bodylen = ntohl(req->request.bodylen);
            c->binary_header.request.vbucket = ntohs(req->request.vbucket);
            c->binary_header.request.cas = ntohll(req->request.cas);


            if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ)
            {
                if (settings.verbose) {
                    if (c->binary_header.request.magic != PROTOCOL_BINARY_RES) {
                        mc_logger->log(EXTENSION_LOG_INFO, c,
                                "%d: Invalid magic:  %x\n", c->sfd,
                                c->binary_header.request.magic);
                    } else {
                        mc_logger->log(EXTENSION_LOG_INFO, c,
                                "%d: ERROR: Unsupported response packet received: %u\n",
                                c->sfd, (unsigned int)c->binary_header.request.opcode);
                    }
                }
                conn_set_state(c, conn_closing);
                return -1;
            }

            c->msgcurr = 0;
            c->msgused = 0;
            c->iovused = 0;
            if (add_msghdr(c) != 0) {
                out_string(c, "SERVER_ERROR out of memory");
                return 0;
            }

            c->cmd = c->binary_header.request.opcode;
            c->keylen = c->binary_header.request.keylen;
            c->opaque = c->binary_header.request.opaque;
            /* clear the returned cas value */
            c->cas = 0;

            dispatch_bin_command(c);

            c->rbytes -= sizeof(c->binary_header);
            c->rcurr += sizeof(c->binary_header);
        }
    } else {
        char *el, *cont;

        if (c->rbytes == 0)
            return 0;

        el = memchr(c->rcurr, '\n', c->rbytes);
        if (!el) {
            if (c->rbytes > 1024) {
                /*
                 * We didn't have a '\n' in the first k. This _has_ to be a
                 * large multiget, if not we should just nuke the connection.
                 */
                char *ptr = c->rcurr;
                while (*ptr == ' ') { /* ignore leading whitespaces */
                    ++ptr;
                }

                if (ptr - c->rcurr > 100) {
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                        "%d: Too many leading whitespaces(%d). Close the connection.\n",
                        c->sfd, (int)(ptr - c->rcurr));
                    conn_set_state(c, conn_closing);
                    return 1;
                }
                if (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5)) {
                    char buffer[16];
                    memcpy(buffer, ptr, 15); buffer[15] = '\0';
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                        "%d: Too long ascii command(%s). Close the connection.\n",
                        c->sfd, buffer);
                    conn_set_state(c, conn_closing);
                    return 1;
                }
            }

            return 0;
        }
        cont = el + 1;
        if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
            el--;
        }
        *el = '\0';

        assert(cont <= (c->rcurr + c->rbytes));

        process_command(c, c->rcurr, el - c->rcurr);

        c->rbytes -= (cont - c->rcurr);
        c->rcurr = cont;

        assert(c->rcurr <= (c->rbuf + c->rsize));
    }

    return 1;
}

/*
 * read a UDP request.
 */
static enum try_read_result try_read_udp(conn *c) {
    int res;

    assert(c != NULL);

    c->request_addr_size = sizeof(c->request_addr);
    res = recvfrom(c->sfd, c->rbuf, c->rsize,
                   0, &c->request_addr, &c->request_addr_size);
    if (res > 8) {
        unsigned char *buf = (unsigned char *)c->rbuf;
        STATS_ADD(c, bytes_read, res);

        /* Beginning of UDP packet is the request ID; save it. */
        c->request_id = buf[0] * 256 + buf[1];

        /* If this is a multi-packet request, drop it. */
        if (buf[4] != 0 || buf[5] != 1) {
            out_string(c, "SERVER_ERROR multi-packet request not supported");
            return READ_NO_DATA_RECEIVED;
        }

        /* Don't care about any of the rest of the header. */
        res -= 8;
        memmove(c->rbuf, c->rbuf + 8, res);

        c->rbytes += res;
        c->rcurr = c->rbuf;
        return READ_DATA_RECEIVED;
    }
    return READ_NO_DATA_RECEIVED;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 *
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start looking
 * at the data I've got after a number of reallocs...
 *
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c) {
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res;
    int num_allocs = 0;
    assert(c != NULL);

    if (c->rcurr != c->rbuf) {
        if (c->rbytes != 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes);
        c->rcurr = c->rbuf;
    }

    while (1) {
        if (c->rbytes >= c->rsize) {
            if (num_allocs == 4) {
                return gotdata;
            }
            ++num_allocs;
            char *new_rbuf = realloc(c->rbuf, c->rsize * 2);
            if (!new_rbuf) {
                if (settings.verbose > 0) {
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                            "Couldn't realloc input buffer\n");
                }
                c->rbytes = 0; /* ignore what we read */
                out_string(c, "SERVER_ERROR out of memory reading request");
                c->write_and_go = conn_closing;
                return READ_MEMORY_ERROR;
            }
            c->rcurr = c->rbuf = new_rbuf;
            c->rsize *= 2;
        }

        int avail = c->rsize - c->rbytes;
        res = read(c->sfd, c->rbuf + c->rbytes, avail);
        if (res > 0) {
            STATS_ADD(c, bytes_read, res);
            gotdata = READ_DATA_RECEIVED;
            c->rbytes += res;
            if (res == avail) {
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            /* The client called shutdown() to close the socket. */
            if (settings.verbose > 1) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c,
                        "Couldn't read in try_read_network: end of stream\n");
            }
            return READ_ERROR;
        }
        if (res == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            /* The client called close() to close the socket. */
            if (settings.verbose > 1) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c,
                        "Couldn't read in try_read_network: err=(%d:%s)\n",
                        errno, strerror(errno));
            }
            return READ_ERROR;
        }
    }
    return gotdata;
}

static bool update_event(conn *c, const int new_flags) {
    assert(c != NULL);

    struct event_base *base = c->event.ev_base;
    if (c->ev_flags == new_flags)
        return true;

    mc_logger->log(EXTENSION_LOG_DEBUG, NULL,
                   "Updated event for %d to read=%s, write=%s\n",
                   c->sfd, (new_flags & EV_READ ? "yes" : "no"),
                   (new_flags & EV_WRITE ? "yes" : "no"));

    if (event_del(&c->event) == -1) return false;
    event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = new_flags;
    if (event_add(&c->event, 0) == -1) return false;
    return true;
}

/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit(conn *c) {
    assert(c != NULL);

    if (c->msgcurr < c->msgused &&
            c->msglist[c->msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        c->msgcurr++;
    }
    if (c->msgcurr < c->msgused) {
        ssize_t res;
        struct msghdr *m = &c->msglist[c->msgcurr];

        res = sendmsg(c->sfd, m, 0);
        if (res > 0) {
            STATS_ADD(c, bytes_written, res);

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= m->msg_iov->iov_len) {
                res -= m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0) {
                m->msg_iov->iov_base = (caddr_t)m->msg_iov->iov_base + res;
                m->msg_iov->iov_len -= res;
            }
            return TRANSMIT_INCOMPLETE;
        }
        if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0) {
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                                   "Couldn't update event in transmit.\n");
                }
                conn_set_state(c, conn_closing);
                return TRANSMIT_HARD_ERROR;
            }
            return TRANSMIT_SOFT_ERROR;
        }
        /* if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
           we have a real error, on which we close the connection */
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                           "Failed to write, and not due to blocking: %s\n",
                           strerror(errno));
            //perror("Failed to write, and not due to blocking");
        }

        if (IS_UDP(c->transport))
            conn_set_state(c, conn_read);
        else
            conn_set_state(c, conn_closing);
        return TRANSMIT_HARD_ERROR;
    } else {
        return TRANSMIT_COMPLETE;
    }
}

bool conn_listening(conn *c)
{
    int sfd, flags = 1;
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);

    if ((sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen)) == -1) {
        if (errno == EMFILE) {
            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_INFO, c,
                               "Too many open connections\n");
            }
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            mc_logger->log(EXTENSION_LOG_WARNING, c,
                    "Failed to accept new client: %s\n", strerror(errno));

        }
        return false;
    }

    STATS_LOCK();
    int curr_conns = mc_stats.curr_conns++;
    STATS_UNLOCK();

    if (curr_conns >= settings.maxconns) {
        /* Allow admin connection even if # of connections is over maxconns */
        getpeername(sfd, (struct sockaddr*)&addr, &addrlen);
        struct sockaddr_in *sin = (struct sockaddr_in *)&addr;
        if (strcmp(inet_ntoa(sin->sin_addr), ADMIN_CLIENT_IP) != 0 ||
            curr_conns >= settings.maxconns + ADMIN_MAX_CONNECTIONS)
        {
            STATS_LOCK();
            ++mc_stats.rejected_conns;
            STATS_UNLOCK();

            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_INFO, c,
                               "Too many open connections (maxconns)\n");
            }
            safe_close(sfd);
            return false;
        }
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                "Failed to set nonblocking io: %s\n", strerror(errno));
        safe_close(sfd);
        return false;
    }

    dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST,
                      DATA_BUFFER_SIZE, tcp_transport);

    return false;
}

bool conn_waiting(conn *c) {
    if (!update_event(c, EV_READ | EV_PERSIST)) {
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, c,
                           "Couldn't update event in conn_waiting.\n");
        }
        conn_set_state(c, conn_closing);
        return true;
    }
    conn_set_state(c, conn_read);
    return false;
}

bool conn_read(conn *c) {
    int res = IS_UDP(c->transport) ? try_read_udp(c) : try_read_network(c);
    switch (res) {
    case READ_NO_DATA_RECEIVED:
        conn_set_state(c, conn_waiting);
        break;
    case READ_DATA_RECEIVED:
        conn_set_state(c, conn_parse_cmd);
        break;
    case READ_ERROR:
        conn_set_state(c, conn_closing);
        break;
    case READ_MEMORY_ERROR: /* Failed to allocate more memory */
        /* State already set by try_read_network */
        break;
    }

    return true;
}

bool conn_parse_cmd(conn *c) {
    if (try_read_command(c) == 0) {
        /* wee need more data! */
        conn_set_state(c, conn_waiting);
    }

    /* try_read_command eventually calls write functions
     * that may return EWOULDBLOCK and set ewouldblock true.
     * So, remove the current connection from the event loop
     * and wait for notify_io_complete event.
     * See also conn_nread.
     */
    if (c->ewouldblock) {
        LIBEVENT_THREAD *t = c->thread;
        bool block = false;

        LOCK_THREAD(t);
        if (c->premature_notify_io_complete) {
            /* notify_io_complete was called before we got here */
            c->premature_notify_io_complete = false;
        } else {
            event_del(&c->event);
            c->io_blocked = true;
            block = true;
        }
        UNLOCK_THREAD(t);
        c->ewouldblock = false;

        if (block)
            return false;
    }

    return true;
}

bool conn_new_cmd(conn *c) {
    /* Only process nreqs at a time to avoid starving other connections */
    --c->nevents;
    if (c->nevents >= 0) {
        reset_cmd_handler(c);
    } else {
        STATS_NOKEY(c, conn_yields);
        if (c->rbytes > 0) {
            /* We have already read in data into the input buffer,
               so libevent will most likely not signal read events
               on the socket (unless more data is available. As a
               hack we should just put in a request to write data,
               because that should be possible ;-)
            */
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0) {
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                                   "Couldn't update event in conn_new_cmd.\n");
                }
                conn_set_state(c, conn_closing);
                return true;
            }
        }
        return false;
    }

    return true;
}


bool conn_swallow(conn *c) {
    ssize_t res;
    /* we are reading sbytes and throwing them away */
    if (c->sbytes == 0) {
        conn_set_state(c, conn_new_cmd);
        return true;
    }

    /* first check if we have leftovers in the conn_read buffer */
    if (c->rbytes > 0) {
        int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
        c->sbytes -= tocopy;
        c->rcurr += tocopy;
        c->rbytes -= tocopy;
        return true;
    }

    /*  now try reading from the socket */
    res = read(c->sfd, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize);
    if (res > 0) {
        STATS_ADD(c, bytes_read, res);
        c->sbytes -= res;
        return true;
    }
    if (res == 0) { /* end of stream */
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                           "Couldn't read in conn_swallow: end of stream.\n");
        }
        conn_set_state(c, conn_closing);
        return true;
    }
    if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!update_event(c, EV_READ | EV_PERSIST)) {
            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, c,
                               "Couldn't update event in conn_swallow.\n");
            }
            conn_set_state(c, conn_closing);
            return true;
        }
        return false;
    }

    if (errno != ENOTCONN && errno != ECONNRESET) {
        /* otherwise we have a real error, on which we close the connection */
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                "Failed to read in conn_swallow, and not due to blocking: err=(%d:%s)\n",
                errno, strerror(errno));
    } else {
        mc_logger->log(EXTENSION_LOG_INFO, c,
                "Failed to read in conn_swallow, and not due to blocking: err=(%d:%s)\n",
                errno, strerror(errno));
    }

    conn_set_state(c, conn_closing);

    return true;

}

bool conn_nread(conn *c) {
    ssize_t res;

    if (c->rlbytes == 0) {
        complete_nread(c);

        bool block = false;
        if (c->ewouldblock) {
            LIBEVENT_THREAD *t = c->thread;

            LOCK_THREAD(t);
            if (c->premature_notify_io_complete) {
                /* notify_io_complete was called before we got here */
                c->premature_notify_io_complete = false;
            } else {
                event_del(&c->event);
                c->io_blocked = true;
                block = true;
            }
            UNLOCK_THREAD(t);
            c->ewouldblock = false;
        }
        return !block;
    }

    /* first check if we have leftovers in the conn_read buffer */
    if (c->rbytes > 0) {
        int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
        if (c->ritem != c->rcurr) {
            memmove(c->ritem, c->rcurr, tocopy);
        }
        c->ritem += tocopy;
        c->rlbytes -= tocopy;
        c->rcurr += tocopy;
        c->rbytes -= tocopy;
        if (c->rlbytes == 0) {
            return true;
        }
    }

    /*  now try reading from the socket */
    res = read(c->sfd, c->ritem, c->rlbytes);
    if (res > 0) {
        STATS_ADD(c, bytes_read, res);
        if (c->rcurr == c->ritem) {
            c->rcurr += res;
        }
        c->ritem += res;
        c->rlbytes -= res;
        return true;
    }
    if (res == 0) { /* end of stream */
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                           "Couldn't read in conn_nread: end of stream.\n");
        }
        conn_set_state(c, conn_closing);
        return true;
    }
    if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (!update_event(c, EV_READ | EV_PERSIST)) {
            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, c,
                               "Couldn't update event in conn_nread.\n");
            }
            conn_set_state(c, conn_closing);
            return true;
        }
        return false;
    }

    if (errno != ENOTCONN && errno != ECONNRESET) {
        /* otherwise we have a real error, on which we close the connection */
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                       "Failed to read in conn_nread, and not due to blocking: err=(%d:%s)\n"
                       "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                       errno, strerror(errno),
                       (long)c->rcurr, (long)c->ritem, (long)c->rbuf,
                       (int)c->rlbytes, (int)c->rsize);
    } else {
        mc_logger->log(EXTENSION_LOG_INFO, c,
                       "Failed to read in conn_nread, and not due to blocking: err=(%d:%s)\n"
                       "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                       errno, strerror(errno),
                       (long)c->rcurr, (long)c->ritem, (long)c->rbuf,
                       (int)c->rlbytes, (int)c->rsize);
    }
    conn_set_state(c, conn_closing);
    return true;
}

bool conn_write(conn *c) {
    /*
     * We want to write out a simple response. If we haven't already,
     * assemble it into a msgbuf list (this will be a single-entry
     * list for TCP or a two-entry list for UDP).
     */
    if (c->iovused == 0 || (IS_UDP(c->transport) && c->iovused == 1)) {
        if (add_iov(c, c->wcurr, c->wbytes) != 0) {
            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, c,
                               "Couldn't build response in conn_write.\n");
            }
            conn_set_state(c, conn_closing);
            return true;
        }
    }

    return conn_mwrite(c);
}

bool conn_mwrite(conn *c) {
    /* c->aiostat was set by notify_io_complete function.  */
    if (c->aiostat != ENGINE_SUCCESS) {
        /* The response must be reset according to c->aiostat. */
    }

    if (IS_UDP(c->transport) && c->msgcurr == 0 && build_udp_headers(c) != 0) {
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, c,
                           "Failed to build UDP headers in conn_mwrite.\n");
        }
        conn_set_state(c, conn_closing);
        return true;
    }

    /* Clear the ewouldblock so that the next read command from
     * the same connection does not falsely block and time out.
     */
    if (c->ewouldblock)
        c->ewouldblock = false;

    switch (transmit(c)) {
    case TRANSMIT_COMPLETE:
        if (c->state == conn_mwrite) {
            while (c->ileft > 0) {
                item *it = *(c->icurr);
                mc_engine.v1->release(mc_engine.v0, c, it);
                c->icurr++;
                c->ileft--;
            }
            while (c->suffixleft > 0) {
                char *suffix = *(c->suffixcurr);
                cache_free(c->thread->suffix_cache, suffix);
                c->suffixcurr++;
                c->suffixleft--;
            }
#ifdef DETECT_LONG_QUERY
            if (c->lq_bufcnt != 0) {
                lqdetect_buffer_release(c->lq_bufcnt);
                c->lq_bufcnt = 0;
            }
#endif
            if (c->coll_eitem != NULL) {
                conn_coll_eitem_free(c);
            }
            /* XXX:  I don't know why this wasn't the general case */
            if(c->protocol == binary_prot) {
                conn_set_state(c, c->write_and_go);
            } else {
                conn_set_state(c, conn_new_cmd);
            }
        } else if (c->state == conn_write) {
            if (c->write_and_free) {
                free(c->write_and_free);
                c->write_and_free = 0;
            }
            conn_set_state(c, c->write_and_go);
        } else {
            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, c,
                        "Unexpected state %s\n", state_text(c->state));
            }
            conn_set_state(c, conn_closing);
        }
        break;

    case TRANSMIT_INCOMPLETE:
    case TRANSMIT_HARD_ERROR:
        break;                   /* Continue in state machine. */

    case TRANSMIT_SOFT_ERROR:
        return false;
    }

    return true;
}

bool conn_closing(conn *c) {
    if (IS_UDP(c->transport)) {
        conn_cleanup(c);
    } else {
        conn_close(c);
    }
    return false;
}

void event_handler(const int fd, const short which, void *arg) {
    conn *c;

    c = (conn *)arg;
    assert(c != NULL);

    if (memcached_shutdown) {
        if (c->thread == NULL) {
            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_INFO, c,
                        "Main thread is now terminating from event handler.\n");
            }
            event_base_loopbreak(c->event.ev_base);
            return ;
        }
        if (memcached_shutdown > 1) {
            if (settings.verbose > 0) {
                mc_logger->log(EXTENSION_LOG_INFO, c,
                        "Worker thread[%d] is now terminating from event handler.\n",
                        c->thread->index);
            }
            event_base_loopbreak(c->event.ev_base);
            return ;
        }
    }

    c->which = which;

    /* sanity */
    if (fd != c->sfd) {
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                    "Catastrophic: event fd doesn't match conn fd!\n");
        }
        conn_close(c);
        return;
    }

    perform_callbacks(ON_SWITCH_CONN, c, c);

    c->nevents = settings.reqs_per_event;

    while (c->state(c)) {
        /* do task */
    }
}

static int new_socket(struct addrinfo *ai) {
    int sfd;
    int flags;

    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        safe_close(sfd);
        return -1;
    }
    return sfd;
}


/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&old_size, &intsize) != 0) {
        if (settings.verbose > 0)
            perror("getsockopt(SO_SNDBUF)");
        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, NULL,
                "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
    }
}



/**
 * Create a socket and bind it to a specific port number
 * @param port the port number to bind to
 * @param transport the transport protocol (TCP / UDP)
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.
 */
static int server_socket(int port, enum network_transport transport,
                         FILE *portnumber_file) {
    int sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints = { .ai_flags = AI_PASSIVE,
                              .ai_family = AF_UNSPEC };
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags =1;

    hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM;

    if (port == -1) {
        port = 0;
    }
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    error= getaddrinfo(settings.inter, port_buf, &hints, &ai);
    if (error != 0) {
        if (error != EAI_SYSTEM) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "getaddrinfo(): %s\n", gai_strerror(error));
        } else {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "getaddrinfo(): %s\n", strerror(error));
        }
        return 1;
    }

    for (next= ai; next; next= next->ai_next) {
        conn *listen_conn_add;
        if ((sfd = new_socket(next)) == -1) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
                safe_close(sfd);
                continue;
            }
        }
#endif

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        if (IS_UDP(transport)) {
            maximize_sndbuf(sfd);
        } else {
            error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");
        }

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
            if (errno != EADDRINUSE) {
                perror("bind()");
                safe_close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            safe_close(sfd);
            continue;
        } else {
            success++;
            if (!IS_UDP(transport) && listen(sfd, settings.backlog) == -1) {
                perror("listen()");
                safe_close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            if (portnumber_file != NULL &&
                (next->ai_addr->sa_family == AF_INET ||
                 next->ai_addr->sa_family == AF_INET6)) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                    if (next->ai_addr->sa_family == AF_INET) {
                        fprintf(portnumber_file, "%s INET: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in.sin_port));
                    } else {
                        fprintf(portnumber_file, "%s INET6: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
        }

        if (IS_UDP(transport)) {
            int c;

            for (c = 0; c < settings.num_threads; c++) {
                /* this is guaranteed to hit all threads because we round-robin */
                dispatch_conn_new(sfd, conn_read, EV_READ | EV_PERSIST,
                                  UDP_READ_BUFFER_SIZE, transport);
                STATS_LOCK();
                ++mc_stats.daemon_conns;
                STATS_UNLOCK();
            }
        } else {
            if (!(listen_conn_add = conn_new(sfd, conn_listening,
                                             EV_READ | EV_PERSIST, 1,
                                             transport, main_base, NULL))) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "failed to create listening connection\n");
                exit(EXIT_FAILURE);
            }
            listen_conn_add->next = listen_conn;
            listen_conn = listen_conn_add;
            STATS_LOCK();
            ++mc_stats.daemon_conns;
            STATS_UNLOCK();
        }
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

static int new_socket_unix(void) {
    int sfd;
    int flags;

    if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket()");
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        safe_close(sfd);
        return -1;
    }
    return sfd;
}

/* this will probably not work on windows */
static int server_socket_unix(const char *path, int access_mask) {
    int sfd;
    struct linger ling = {0, 0};
    struct sockaddr_un addr;
    struct stat tstat;
    int flags =1;
    int old_umask;

    if (!path) {
        return 1;
    }

    if ((sfd = new_socket_unix()) == -1) {
        return 1;
    }

    /*
     * Clean up a previous socket file if we left it around
     */
    if (lstat(path, &tstat) == 0) {
        if (S_ISSOCK(tstat.st_mode))
            unlink(path);
    }

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

    /*
     * the memset call clears nonstandard fields in some impementations
     * that otherwise mess things up.
     */
    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    assert(strcmp(addr.sun_path, path) == 0);
    old_umask = umask( ~(access_mask&0777));
    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind()");
        safe_close(sfd);
        umask(old_umask);
        return 1;
    }
    umask(old_umask);
    if (listen(sfd, settings.backlog) == -1) {
        perror("listen()");
        safe_close(sfd);
        return 1;
    }
    if (!(listen_conn = conn_new(sfd, conn_listening,
                                 EV_READ | EV_PERSIST, 1,
                                 local_transport, main_base, NULL))) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to create listening connection\n");
        exit(EXIT_FAILURE);
    }

    STATS_LOCK();
    ++mc_stats.daemon_conns;
    STATS_UNLOCK();

    return 0;
}

static struct event clockevent;

static void clock_handler(const int fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    static bool initialized = false;

    if (memcached_shutdown) {
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, NULL,
                    "Main thread is now terminating from clock handler.\n");
        }
        event_base_loopbreak(main_base);
        return ;
    }

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&clockevent);
    } else {
        initialized = true;
    }

    evtimer_set(&clockevent, clock_handler, 0);
    event_base_set(main_base, &clockevent);
    evtimer_add(&clockevent, &t);

    set_current_time();
}

static void usage(void) {
    printf(PACKAGE " " VERSION "\n");
    printf("-p <num>      TCP port number to listen on (default: 11211)\n"
           "-U <num>      UDP port number to listen on (default: 11211, 0 is off)\n"
           "-s <file>     UNIX socket path to listen on (disables network support)\n"
           "-a <mask>     access mask for UNIX socket, in octal (default: 0700)\n"
           "-l <ip_addr>  interface to listen on (default: INADDR_ANY, all addresses)\n"
           "-d            run as a daemon\n"
           "-r            maximize core file limit\n"
           "-u <username> assume identity of <username> (only when run as root)\n"
           "-m <num>      max memory to use for items in megabytes (default: 64 MB)\n"
           "-M            return error on memory exhausted (rather than removing items)\n"
#ifdef ENABLE_STICKY_ITEM
           "-g            sticky(gummed) item ratio of 0 ~ 100 (default: 0)\n"
#endif
           "-c <num>      max simultaneous connections (default: 1024)\n"
           "-k            lock down all paged memory.  Note that there is a\n"
           "              limit on how much memory you may lock.  Trying to\n"
           "              allocate more than that would fail, so be sure you\n"
           "              set the limit correctly for the user you started\n"
           "              the daemon with (not for -u <username> user;\n"
           "              under sh this is done with 'ulimit -S -l NUM_KB').\n"
           "-v            verbose (print errors/warnings while in event loop)\n"
           "-vv           very verbose (also print client commands/reponses)\n"
           "-vvv          extremely verbose (also print internal state transitions)\n"
           "-h            print this help and exit\n"
           "-i            print memcached and libevent license\n"
           "-P <file>     save PID in <file>, only used with -d option\n"
           "-f <factor>   chunk size growth factor (default: 1.25)\n"
           "-n <bytes>    minimum space allocated for key+value+flags (default: 48)\n");
    printf("-L            Try to use large memory pages (if available). Increasing\n"
           "              the memory page size could reduce the number of TLB misses\n"
           "              and improve the performance. In order to get large pages\n"
           "              from the OS, memcached will allocate the total item-cache\n"
           "              in one large chunk.\n");
    printf("-D <char>     Use <char> as the delimiter between key prefixes and IDs.\n"
           "              This is used for per-prefix stats reporting. The default is\n"
           "              \":\" (colon). If this option is specified, stats collection\n"
           "              is turned on automatically; if not, then it may be turned on\n"
           "              by sending the \"stats detail on\" command to the server.\n");
    printf("-t <num>      number of threads to use (default: 4)\n");
    printf("-R            Maximum number of requests per event, limits the number of\n"
           "              requests process for a given connection to prevent \n"
           "              starvation (default: 20)\n");
    printf("-C            Disable use of CAS\n");
    printf("-b            Set the backlog queue limit (default: 1024)\n");
    printf("-B            Binding protocol - one of ascii, binary, or auto (default)\n");
    printf("-I            Override the size of each slab page. Adjusts max item size\n"
           "              (default: 1mb, min: 1k, max: 128m)\n");
    printf("-E <engine>   Engine to load, must be given (for example, -E .libs/default_engine.so)\n");
    printf("-q            Disable detailed stats commands\n");
#ifdef SASL_ENABLED
    printf("-S            Require SASL authentication\n");
#endif
    printf("-X module,cfg Load the module and initialize it with the config\n");
    printf("-O ip:port    Tap ip:port\n");
#ifdef ENABLE_ZK_INTEGRATION
    printf("-z ip:port list Zookeeper ensemble cluster servers\n");
    printf("-o <secs>     Zookeeper session timeout in seconds\n");
#endif
    printf("\nEnvironment variables:\n"
           "MEMCACHED_PORT_FILENAME   File to write port information to\n"
           "MEMCACHED_TOP_KEYS        Number of top keys to keep track of\n");
}

static void usage_license(void) {
    printf(PACKAGE " " VERSION "\n\n");
    printf(
    "Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions are\n"
    "met:\n"
    "\n"
    "    * Redistributions of source code must retain the above copyright\n"
    "notice, this list of conditions and the following disclaimer.\n"
    "\n"
    "    * Redistributions in binary form must reproduce the above\n"
    "copyright notice, this list of conditions and the following disclaimer\n"
    "in the documentation and/or other materials provided with the\n"
    "distribution.\n"
    "\n"
    "    * Neither the name of the Danga Interactive nor the names of its\n"
    "contributors may be used to endorse or promote products derived from\n"
    "this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
    "\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
    "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
    "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
    "OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
    "SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n"
    "LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
    "OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    "\n"
    "\n"
    "This product includes software developed by Niels Provos.\n"
    "\n"
    "[ libevent ]\n"
    "\n"
    "Copyright 2000-2003 Niels Provos <provos@citi.umich.edu>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions\n"
    "are met:\n"
    "1. Redistributions of source code must retain the above copyright\n"
    "   notice, this list of conditions and the following disclaimer.\n"
    "2. Redistributions in binary form must reproduce the above copyright\n"
    "   notice, this list of conditions and the following disclaimer in the\n"
    "   documentation and/or other materials provided with the distribution.\n"
    "3. All advertising materials mentioning features or use of this software\n"
    "   must display the following acknowledgement:\n"
    "      This product includes software developed by Niels Provos.\n"
    "4. The name of the author may not be used to endorse or promote products\n"
    "   derived from this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n"
    "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
    "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n"
    "IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
    "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n"
    "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n"
    "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    );

    return;
}

static void save_pid(const pid_t pid, const char *pid_file) {
    FILE *fp;
    if (pid_file == NULL) {
        return;
    }

    if ((fp = fopen(pid_file, "w")) == NULL) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Could not open the pid file %s for writing: %s\n",
                pid_file, strerror(errno));
        return;
    }

    fprintf(fp,"%ld\n", (long)pid);
    if (fclose(fp) == -1) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Could not close the pid file %s: %s\n",
                pid_file, strerror(errno));
    }
}

static void remove_pidfile(const char *pid_file) {
    if (pid_file != NULL) {
        if (unlink(pid_file) != 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Could not remove the pid file %s: %s\n",
                    pid_file, strerror(errno));
        }
    }
}

#ifndef HAVE_SIGIGNORE
static int sigignore(int sig) {
    struct sigaction sa = { .sa_handler = SIG_IGN, .sa_flags = 0 };

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
        return -1;
    }
    return 0;
}
#endif /* !HAVE_SIGIGNORE */

static void sigterm_handler(int sig)
{
    assert(sig == SIGTERM || sig == SIGINT);

    if (settings.verbose) {
        mc_logger->log(EXTENSION_LOG_INFO, NULL,
                       "memcached shutdown by signal(%s)\n",
                       (sig == SIGINT ? "SIGINT" : "SIGTERM"));
    }
    memcached_shutdown = 1;

#ifdef ENABLE_ZK_INTEGRATION
    if (arcus_zk_cfg) {
        arcus_zk_shutdown = 1;
    }
#endif
}

static int install_sigterm_handler(void) {
    struct sigaction sa = {.sa_handler = sigterm_handler, .sa_flags = 0};

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(SIGTERM, &sa, 0) == -1 ||
        sigaction(SIGINT, &sa, 0) == -1) {
        return -1;
    }

    return 0;
}

/*
 * On systems that supports multiple page sizes we may reduce the
 * number of TLB-misses by using the biggest available page size
 */
static int enable_large_pages(void) {
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
    int ret = -1;
    size_t sizes[32];
    int avail = getpagesizes(sizes, 32);
    if (avail != -1) {
        size_t max = sizes[0];
        struct memcntl_mha arg = {0};
        int ii;

        for (ii = 1; ii < avail; ++ii) {
            if (max < sizes[ii]) {
                max = sizes[ii];
            }
        }

        arg.mha_flags   = 0;
        arg.mha_pagesize = max;
        arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

        if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to set large pages: %s\nWill use default page size\n",
                    strerror(errno));
        } else {
            ret = 0;
        }
    } else {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to get supported pagesizes: %s\nWill use default page size\n",
            strerror(errno));
    }

    return ret;
#else
    return 0;
#endif
}

static const char* get_server_version(void) {
    return VERSION;
}

static void store_engine_specific(const void *cookie,
                                  void *engine_data) {
    conn *c = (conn*)cookie;
    c->engine_storage = engine_data;
}

static void *get_engine_specific(const void *cookie) {
    conn *c = (conn*)cookie;
    return c->engine_storage;
}

static int get_socket_fd(const void *cookie) {
    conn *c = (conn *)cookie;
    return c->sfd;
}

static const char* get_client_ip(const void *cookie) {
    conn *c = (conn *)cookie;
    return (c != NULL ? c->client_ip : "null");
}

static int get_thread_index(const void *cookie) {
    conn *c = (conn *)cookie;
    return c->thread->index;
}

static int num_independent_stats(void) {
    return settings.num_threads + 1;
}

static void *new_independent_stats(void) {
    int ii;
    int nrecords = num_independent_stats();
    struct independent_stats *independent_stats = calloc(sizeof(independent_stats) + sizeof(struct thread_stats) * nrecords, 1);
    if (settings.topkeys > 0)
        independent_stats->topkeys = topkeys_init(settings.topkeys);
    for (ii = 0; ii < nrecords; ii++)
        pthread_mutex_init(&independent_stats->thread_stats[ii].mutex, NULL);
    return independent_stats;
}

static void release_independent_stats(void *stats) {
    int ii;
    int nrecords = num_independent_stats();
    struct independent_stats *independent_stats = stats;
    if (independent_stats->topkeys)
        topkeys_free(independent_stats->topkeys);
    for (ii = 0; ii < nrecords; ii++)
        pthread_mutex_destroy(&independent_stats->thread_stats[ii].mutex);
    free(independent_stats);
}

static inline struct independent_stats *get_independent_stats(conn *c) {
    struct independent_stats *independent_stats;
    if (mc_engine.v1->get_stats_struct != NULL) {
        independent_stats = mc_engine.v1->get_stats_struct(mc_engine.v0, (const void *)c);
        if (independent_stats == NULL)
            independent_stats = default_independent_stats;
    } else {
        independent_stats = default_independent_stats;
    }
    return independent_stats;
}

static inline struct thread_stats *get_thread_stats(conn *c) {
    struct independent_stats *independent_stats = get_independent_stats(c);
    assert(c->thread->index < num_independent_stats());
    return &independent_stats->thread_stats[c->thread->index];
}

static void count_eviction(const void *cookie, const void *key, const int nkey) {
    topkeys_t *tk = get_independent_stats((conn*)cookie)->topkeys;
    TK(tk, evictions, key, nkey, get_current_time());
}

#if 0 // not used code
/**
 * To make it easy for engine implementors that doesn't want to care about
 * writing their own incr/decr code, they can just set the arithmetic function
 * to NULL and use this implementation. It is not efficient, due to the fact
 * that it does multiple calls through the interface (get and then cas store).
 * If you don't care, feel free to use it..
 */
static ENGINE_ERROR_CODE internal_arithmetic(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const int nkey,
                                             const bool increment,
                                             const bool create,
                                             const uint64_t delta,
                                             const uint64_t initial,
                                             const rel_time_t exptime,
                                             uint64_t *cas,
                                             uint64_t *result,
                                             uint16_t vbucket)
{
    ENGINE_HANDLE_V1 *e = (ENGINE_HANDLE_V1*)handle;

    item *it = NULL;

    ENGINE_ERROR_CODE ret;
    ret = e->get(handle, cookie, &it, key, nkey, vbucket);

    if (ret == ENGINE_SUCCESS) {
        item_info info = { .nvalue = 1 };

        if (!e->get_item_info(handle, cookie, it, &info)) {
            e->release(handle, cookie, it);
            return ENGINE_FAILED;
        }

        /* Ensure that we don't run away into random memory */
        char *endptr = info.value[0].iov_base;
        int ii;
        for (ii = 0; ii < info.value[0].iov_len; ++ii) {
            if (isdigit(endptr[ii]) == 0) {
                break;
            }
        }

        uint64_t val;
        if (ii == info.value[0].iov_len || !safe_strtoull((const char*)info.value[0].iov_base, &val)) {
            e->release(handle, cookie, it);
            return ENGINE_EINVAL;
        }

        if (increment) {
            val += delta;
        } else {
            if (delta > val) {
                val = 0;
            } else {
                val -= delta;
            }
        }

        char value[80];
        size_t nb = snprintf(value, sizeof(value), "%"PRIu64"\r\n", val);
        *result = val;
        item *nit = NULL;
        if (e->allocate(handle, cookie, &nit, key,
                        nkey, nb, info.flags, info.exptime) != ENGINE_SUCCESS) {
            e->release(handle, cookie, it);
            return ENGINE_ENOMEM;
        }

        item_info i2 = { .nvalue = 1 };
        if (!e->get_item_info(handle, cookie, nit, &i2)) {
            e->release(handle, cookie, it);
            e->release(handle, cookie, nit);
            return ENGINE_FAILED;
        }

        memcpy(i2.value[0].iov_base, value, nb);
        e->item_set_cas(handle, cookie, nit, info.cas);
        ret = e->store(handle, cookie, nit, cas, OPERATION_CAS, vbucket);
        e->release(handle, cookie, it);
        e->release(handle, cookie, nit);
    } else if (ret == ENGINE_KEY_ENOENT && create) {
        char value[80];
        size_t nb = snprintf(value, sizeof(value), "%"PRIu64"\r\n", initial);
        *result = initial;
        if (e->allocate(handle, cookie, &it, key, nkey, nb, 0, exptime) != ENGINE_SUCCESS) {
            e->release(handle, cookie, it);
            return ENGINE_ENOMEM;
        }

        item_info info = { .nvalue = 1 };
        if (!e->get_item_info(handle, cookie, it, &info)) {
            e->release(handle, cookie, it);
            return ENGINE_FAILED;
        }

        memcpy(info.value[0].iov_base, value, nb);
        ret = e->store(handle, cookie, it, cas, OPERATION_CAS, vbucket);
        e->release(handle, cookie, it);
    }

    /* We had a race condition.. just call ourself recursively to retry */
    if (ret == ENGINE_KEY_EEXISTS) {
        return internal_arithmetic(handle, cookie, key, nkey, increment, create, delta,
                                   initial, exptime, cas, result, vbucket);
    }

    return ret;
}
#endif

/**
 * Register an extension if it's not already registered
 *
 * @param type the type of the extension to register
 * @param extension the extension to register
 * @return true if success, false otherwise
 */
static bool register_extension(extension_type_t type, void *extension)
{
    if (extension == NULL) {
        return false;
    }

    switch (type) {
    case EXTENSION_DAEMON:
        for (EXTENSION_DAEMON_DESCRIPTOR *ptr = settings.extensions.daemons;
             ptr != NULL;
             ptr = ptr->next) {
            if (ptr == extension) {
                return false;
            }
        }
        ((EXTENSION_DAEMON_DESCRIPTOR *)(extension))->next = settings.extensions.daemons;
        settings.extensions.daemons = extension;
        return true;
    case EXTENSION_LOGGER:
        settings.extensions.logger = extension;
        mc_logger = settings.extensions.logger;
        return true;
    case EXTENSION_ASCII_PROTOCOL:
        if (settings.extensions.ascii != NULL) {
            EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *last;
            for (last = settings.extensions.ascii; last->next != NULL;
                 last = last->next) {
                if (last == extension) {
                    return false;
                }
            }
            if (last == extension) {
                return false;
            }
            last->next = extension;
            last->next->next = NULL;
        } else {
            settings.extensions.ascii = extension;
            settings.extensions.ascii->next = NULL;
        }
        return true;

    default:
        return false;
    }
}

/**
 * Unregister an extension
 *
 * @param type the type of the extension to remove
 * @param extension the extension to remove
 */
static void unregister_extension(extension_type_t type, void *extension)
{
    switch (type) {
    case EXTENSION_DAEMON:
        {
            EXTENSION_DAEMON_DESCRIPTOR *prev = NULL;
            EXTENSION_DAEMON_DESCRIPTOR *ptr = settings.extensions.daemons;

            while (ptr != NULL && ptr != extension) {
                prev = ptr;
                ptr = ptr->next;
            }

            if (ptr != NULL && prev != NULL) {
                prev->next = ptr->next;
            }

            if (settings.extensions.daemons == ptr) {
                settings.extensions.daemons = ptr->next;
            }
        }
        break;
    case EXTENSION_LOGGER:
        if (settings.extensions.logger == extension) {
            if (get_stderr_logger() == extension) {
                settings.extensions.logger = get_null_logger();
            } else {
                settings.extensions.logger = get_stderr_logger();
            }
            mc_logger = settings.extensions.logger;
        }
        break;
    case EXTENSION_ASCII_PROTOCOL:
        {
            EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *prev = NULL;
            EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ptr = settings.extensions.ascii;

            while (ptr != NULL && ptr != extension) {
                prev = ptr;
                ptr = ptr->next;
            }

            if (ptr != NULL && prev != NULL) {
                prev->next = ptr->next;
            }

            if (settings.extensions.ascii == ptr) {
                settings.extensions.ascii = ptr->next;
            }
        }
        break;

    default:
        ;
    }

}

/**
 * Get the named extension
 */
static void* get_extension(extension_type_t type)
{
    switch (type) {
    case EXTENSION_DAEMON:
        return settings.extensions.daemons;

    case EXTENSION_LOGGER:
        return settings.extensions.logger;

    case EXTENSION_ASCII_PROTOCOL:
        return settings.extensions.ascii;

    default:
        return NULL;
    }
}

#ifdef ENABLE_CLUSTER_AWARE
static bool is_zk_integrated(void)
{
#ifdef ENABLE_ZK_INTEGRATION
    if (arcus_zk_cfg)
        return true;
#endif
    return false;
}

static bool is_my_key(const char *key, size_t nkey)
{
#ifdef ENABLE_ZK_INTEGRATION
    if (arcus_zk_cfg) {
        bool mine;
        if (arcus_key_is_mine(key, nkey, &mine) == 0) {
            return mine;
        }
        /* The cluster is invalid: go downward and return true */
    }
#endif
    return true;
}
#endif

static void shutdown_server(void)
{
    if (settings.verbose) {
        mc_logger->log(EXTENSION_LOG_INFO, NULL, "memcached shutdown by api\n");
    }
    memcached_shutdown = 1;

#ifdef ENABLE_ZK_INTEGRATION
    if (arcus_zk_cfg) {
        arcus_zk_shutdown = 1;
    }
#endif
}

static EXTENSION_LOGGER_DESCRIPTOR* get_logger(void)
{
    return settings.extensions.logger;
}

static EXTENSION_LOG_LEVEL get_log_level(void)
{
    EXTENSION_LOG_LEVEL ret;
    switch (settings.verbose) {
    case 0: ret = EXTENSION_LOG_WARNING; break;
    case 1: ret = EXTENSION_LOG_INFO; break;
    case 2: ret = EXTENSION_LOG_DEBUG; break;
    default:
        ret = EXTENSION_LOG_DETAIL;
    }
    return ret;
}

static void set_log_level(EXTENSION_LOG_LEVEL severity)
{
    switch (severity) {
    case EXTENSION_LOG_WARNING: settings.verbose = 0; break;
    case EXTENSION_LOG_INFO: settings.verbose = 1; break;
    case EXTENSION_LOG_DEBUG: settings.verbose = 2; break;
    default:
        settings.verbose = 3;
    }
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
}

/**
 * Callback the engines may call to get the public server interface
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
static SERVER_HANDLE_V1 *get_server_api(void)
{
    static SERVER_CORE_API core_api = {
        .get_auth_data = get_auth_data,
        .store_engine_specific = store_engine_specific,
        .get_engine_specific = get_engine_specific,
        .get_socket_fd = get_socket_fd,
        .get_client_ip = get_client_ip,
        .get_thread_index = get_thread_index,
        .server_version = get_server_version,
        .hash = mc_hash,
        .realtime = realtime,
        .notify_io_complete = notify_io_complete,
        .get_current_time = get_current_time,
        .parse_config = parse_config,
#ifdef ENABLE_CLUSTER_AWARE
        .is_zk_integrated = is_zk_integrated,
        .is_my_key = is_my_key,
#endif
        .shutdown = shutdown_server
    };

    static SERVER_STAT_API server_stat_api = {
        .new_stats = new_independent_stats,
        .release_stats = release_independent_stats,
        .evicting = count_eviction
    };

    static SERVER_LOG_API server_log_api = {
        .get_logger = get_logger,
        .get_level = get_log_level,
        .set_level = set_log_level
    };

    static SERVER_EXTENSION_API extension_api = {
        .register_extension = register_extension,
        .unregister_extension = unregister_extension,
        .get_extension = get_extension
    };

    static SERVER_CALLBACK_API callback_api = {
        .register_callback = register_callback,
        .perform_callbacks = perform_callbacks,
    };

    static SERVER_HANDLE_V1 rv = {
        .interface = 1,
        .core = &core_api,
        .stat = &server_stat_api,
        .extension = &extension_api,
        .callback = &callback_api,
        .log = &server_log_api
    };

    if (rv.engine == NULL) {
        rv.engine = mc_engine.v0;
    }

    return &rv;
}

/**
 * Load a shared object and initialize all the extensions in there.
 *
 * @param soname the name of the shared object (may not be NULL)
 * @param config optional configuration parameters
 * @return true if success, false otherwise
 */
static bool load_extension(const char *soname, const char *config) {
    if (soname == NULL) {
        return false;
    }

    /* Hack to remove the warning from C99 */
    union my_hack {
        MEMCACHED_EXTENSIONS_INITIALIZE initialize;
        void* voidptr;
    } funky = {.initialize = NULL };

    void *handle = dlopen(soname, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        const char *msg = dlerror();
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to open library \"%s\": %s\n",
                soname, msg ? msg : "unknown error");
        return false;
    }

    void *symbol = dlsym(handle, "memcached_extensions_initialize");
    if (symbol == NULL) {
        const char *msg = dlerror();
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Could not find symbol \"memcached_extensions_initialize\" in %s: %s\n",
                soname, msg ? msg : "unknown error");
        return false;
    }
    funky.voidptr = symbol;

    EXTENSION_ERROR_CODE error = (*funky.initialize)(config, get_server_api);

    if (error != EXTENSION_SUCCESS) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to initalize extensions from %s. Error code: %d\n",
                soname, error);
        dlclose(handle);
        return false;
    }

    if (settings.verbose > 0) {
        mc_logger->log(EXTENSION_LOG_INFO, NULL,
                "Loaded extensions from: %s\n", soname);
    }

    return true;
}

/**
 * Do basic sanity check of the runtime environment
 * @return true if no errors found, false if we can't use this env
 */
static bool sanitycheck(void) {
    /* One of our biggest problems is old and bogus libevents */
    const char *ever = event_get_version();
    if (ever != NULL) {
        if (strncmp(ever, "1.", 2) == 0) {
            /* Require at least 1.3 (that's still a couple of years old) */
            if ((ever[2] == '1' || ever[2] == '2') && !isdigit(ever[3])) {
                fprintf(stderr, "You are using libevent %s.\nPlease upgrade to"
                        " a more recent version (1.3 or newer)\n",
                        event_get_version());
                return false;
            }
        }
    }

    return true;
}

static void close_listen_sockets(void)
{
    struct conn *conn;

    /* Just close listen sockets only.
     * We do not free listen connections.
     */
    conn = listen_conn;
    while (conn != NULL) {
        close(conn->sfd);
        conn = conn->next;
    }
}

int main (int argc, char **argv) {
    int c;
    bool lock_memory = false;
    bool do_daemonize = false;
    //bool preallocate = false;
    int maxcore = 0;
    char *username = NULL;
    char *pid_file = NULL;
    struct passwd *pw;
    struct rlimit rlim;
    char unit = '\0';
    int size_max = 0;

    bool protocol_specified = false;
    bool tcp_specified = false;
    bool udp_specified = false;

    const char *engine = NULL;
    const char *engine_config = NULL;
    char old_options[1024] = { [0] = '\0' };
    char *old_opts = old_options;

#ifdef ENABLE_ZK_INTEGRATION
    int  arcus_zk_to=0;
#endif

    if (!sanitycheck()) {
        return EX_OSERR;
    }

    /* init settings */
    settings_init();

    /* memcached logger */
    mc_logger = settings.extensions.logger;

    if (memcached_initialize_stderr_logger(get_server_api) != EXTENSION_SUCCESS) {
        fprintf(stderr, "Failed to initialize log system\n");
        return EX_OSERR;
    }

    /* process arguments */
    while (-1 != (c = getopt(argc, argv,
          "a:"  /* access mask for unix socket */
          "p:"  /* TCP port number to listen on */
          "s:"  /* unix socket path to listen on */
          "U:"  /* UDP port number to listen on */
          "m:"  /* max memory to use for items in megabytes */
          "M"   /* return error on memory exhausted */
#ifdef ENABLE_STICKY_ITEM
          "g:"  /* sticky(gummed) item ratio */
#endif
          "c:"  /* max simultaneous connections */
          "k"   /* lock down all paged memory */
          "hi"  /* help, licence info */
          "r"   /* maximize core file limit */
          "v"   /* verbose */
          "d"   /* daemon mode */
          "l:"  /* interface to listen on */
          "u:"  /* user identity to run as */
          "P:"  /* save PID in file */
          "f:"  /* factor? */
          "n:"  /* minimum space allocated for key+value+flags */
          "t:"  /* threads */
          "D:"  /* prefix delimiter? */
          "L"   /* Large memory pages */
          "R:"  /* max requests per event */
          "C"   /* Disable use of CAS */
          "b:"  /* backlog queue limit */
          "B:"  /* Binding protocol */
          "I:"  /* Max item size */
          "S"   /* Sasl ON */
          "E:"  /* Engine to load */
          "e:"  /* Engine options */
          "q"   /* Disallow detailed stats */
          "X:"  /* Load extension */
#ifdef ENABLE_ZK_INTEGRATION
          "z:"  /* Arcus Zookeeper */
          "o:"  /* Arcus Zookeeper session timeout option (sec) */
#endif
        ))) {
        switch (c) {
        case 'a':
            /* access for unix domain socket, as octal mask (like chmod)*/
            settings.access= strtol(optarg,NULL,8);
            break;

        case 'U':
            settings.udpport = atoi(optarg);
            udp_specified = true;
            break;
        case 'p':
            settings.port = atoi(optarg);
            tcp_specified = true;
            break;
        case 's':
            settings.socketpath = optarg;
            break;
        case 'm':
            settings.maxbytes = ((size_t)atoi(optarg)) * 1024 * 1024;
             old_opts += sprintf(old_opts, "cache_size=%lu;",
                                 (unsigned long)settings.maxbytes);
           break;
        case 'M':
            settings.evict_to_free = 0;
            old_opts += sprintf(old_opts, "eviction=false;");
            break;
#ifdef ENABLE_STICKY_ITEM
        case 'g':
            settings.sticky_ratio = atoi(optarg);
            if (settings.sticky_ratio < 0 || settings.sticky_ratio > 100) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "The value of sticky(gummed) item ratio must be between 0 and 100.\n");
                return 1;
            }
            break;
#endif
        case 'c':
            settings.maxconns = atoi(optarg);
            break;
        case 'h':
            usage();
            exit(EXIT_SUCCESS);
        case 'i':
            usage_license();
            exit(EXIT_SUCCESS);
        case 'k':
            lock_memory = true;
            break;
        case 'v':
            settings.verbose++;
            perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
            break;
        case 'l':
            settings.inter= strdup(optarg);
            break;
        case 'd':
            do_daemonize = true;
            break;
        case 'r':
            maxcore = 1;
            break;
        case 'R':
            settings.reqs_per_event = atoi(optarg);
            if (settings.reqs_per_event == 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Number of requests per event must be greater than 0\n");
                return 1;
            }
            break;
        case 'u':
            username = optarg;
            break;
        case 'P':
            pid_file = optarg;
            break;
        case 'f':
            settings.factor = atof(optarg);
            if (settings.factor <= 1.0) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Factor must be greater than 1\n");
                return 1;
            }
             old_opts += sprintf(old_opts, "factor=%f;", settings.factor);
           break;
        case 'n':
            settings.chunk_size = atoi(optarg);
            if (settings.chunk_size == 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Chunk size must be greater than 0\n");
                return 1;
            }
            old_opts += sprintf(old_opts, "chunk_size=%u;", settings.chunk_size);
            break;
        case 't':
            settings.num_threads = atoi(optarg);
            if (settings.num_threads <= 0) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Number of threads must be greater than 0\n");
                return 1;
            }
            /* There're other problems when you get above 64 threads.
             * In the future we should portably detect # of cores for the
             * default.
             */
            if (settings.num_threads > 64) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "WARNING: Setting a high number of worker"
                        "threads is not recommended.\n"
                        " Set this value to the number of cores in"
                        " your machine or less.\n");
            }
            break;
        case 'D':
            settings.prefix_delimiter = optarg[0];
            old_opts += sprintf(old_opts, "prefix_delimiter=%c;", settings.prefix_delimiter);
            settings.detail_enabled = 1;
            break;
        case 'L' :
            if (enable_large_pages() == 0) {
                //preallocate = true;
                old_opts += sprintf(old_opts, "preallocate=true;");
            }
            break;
        case 'C' :
            settings.use_cas = false;
            old_opts += sprintf(old_opts, "use_cas=false;");
            break;
        case 'b' :
            settings.backlog = atoi(optarg);
            break;
        case 'B':
            protocol_specified = true;
            if (strcmp(optarg, "auto") == 0) {
                settings.binding_protocol = negotiating_prot;
            } else if (strcmp(optarg, "binary") == 0) {
                settings.binding_protocol = binary_prot;
            } else if (strcmp(optarg, "ascii") == 0) {
                settings.binding_protocol = ascii_prot;
            } else {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Invalid value for binding protocol: %s\n"
                        " -- should be one of auto, binary, or ascii\n", optarg);
                exit(EX_USAGE);
            }
            break;
        case 'I':
            unit = optarg[strlen(optarg)-1];
            if (unit == 'k' || unit == 'm' ||
                unit == 'K' || unit == 'M') {
                optarg[strlen(optarg)-1] = '\0';
                size_max = atoi(optarg);
                if (unit == 'k' || unit == 'K')
                    size_max *= 1024;
                if (unit == 'm' || unit == 'M')
                    size_max *= 1024 * 1024;
                settings.item_size_max = size_max;
            } else {
                settings.item_size_max = atoi(optarg);
            }
            /* small memory allocator needs the maximum item size larger than 20 KB */
            //if (settings.item_size_max < 1024) {
            if (settings.item_size_max < 1024 * 20) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Item max size cannot be less than 20KB.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024 * 128) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Cannot set item size limit higher than 128 mb.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "WARNING: Setting item max size above 1MB is not"
                    " recommended!\n"
                    " Raising this limit increases the minimum memory requirements\n"
                    " and will decrease your memory efficiency.\n"
                );
            }
#ifndef __WIN32__
            old_opts += sprintf(old_opts, "item_size_max=%zu;",
                                settings.item_size_max);
#else
            old_opts += sprintf(old_opts, "item_size_max=%lu;", (long unsigned)
                                settings.item_size_max);
#endif
            break;
        case 'E':
            engine = optarg;
            break;
        case 'e':
            engine_config = optarg;
            break;
        case 'q':
            settings.allow_detailed = false;
            break;
        case 'S': /* set Sasl authentication to true. Default is false */
#ifndef SASL_ENABLED
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "This server is not built with SASL support.\n");
            exit(EX_USAGE);
#endif
            settings.require_sasl = true;
            break;
        case 'X' :
            {
                char *ptr = strchr(optarg, ',');
                if (ptr != NULL) {
                    *ptr = '\0';
                    ++ptr;
                }
                if (!load_extension(optarg, ptr)) {
                    exit(EXIT_FAILURE);
                }
                if (ptr != NULL) {
                    *(ptr - 1) = ',';
                }
            }
            break;

#ifdef ENABLE_ZK_INTEGRATION
        case 'z': /* configure for Arcus zookeeper cluster */
                  /* host_list in the form of
                     -z 10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181 */

            arcus_zk_cfg = strdup(optarg);
            break;

        case 'o': /* Arcus Zookeeper session timeout */

            arcus_zk_to = atoi(optarg); // this value is in seconds
            break;
#endif

        default:
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    old_opts += sprintf(old_opts, "num_threads=%lu;", (unsigned long)settings.num_threads);

#ifdef ENABLE_STICKY_ITEM
    if (1) {
        size_t sticky_limit = 0;
        if (settings.sticky_ratio > 0) {
            sticky_limit = (settings.maxbytes / 100) * settings.sticky_ratio;
        }
        old_opts += sprintf(old_opts, "sticky_limit=%lu;", (unsigned long)sticky_limit);
    }
#endif
    if (settings.verbose) {
        old_opts += sprintf(old_opts, "verbose=%lu;", (unsigned long)settings.verbose);
    }

    if (install_sigterm_handler() != 0) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                       "Failed to install SIGTERM handler\n");
        exit(EXIT_FAILURE);
    }

    char *topkeys_env = getenv("MEMCACHED_TOP_KEYS");
    if (topkeys_env != NULL) {
        settings.topkeys = atoi(topkeys_env);
        if (settings.topkeys < 0) {
            settings.topkeys = 0;
        }
    }

    if (settings.require_sasl) {
        if (!protocol_specified) {
            settings.binding_protocol = binary_prot;
        } else {
            if (settings.binding_protocol == negotiating_prot) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ERROR: You cannot use auto-negotiating protocol while requiring SASL.\n");
                exit(EX_USAGE);
            }
            if (settings.binding_protocol == ascii_prot) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ERROR: You cannot use only ASCII protocol while requiring SASL.\n");
                exit(EX_USAGE);
            }
        }
    }

    if (tcp_specified && !udp_specified) {
        settings.udpport = settings.port;
    } else if (udp_specified && !tcp_specified) {
        settings.port = settings.udpport;
    }

#ifdef CONFIG_MAX_COLLECTION_SIZE
    /* Following code of setting max collection size will be deprecated. */
#endif
    if (1) { /* check max collection size from environment variables */
        int value;

        char *arcus_max_list_size = getenv("ARCUS_MAX_LIST_SIZE");
        if (arcus_max_list_size != NULL) {
            value = atoi(arcus_max_list_size);
            if (value > MAX_LIST_SIZE && value <= ARCUS_COLL_SIZE_LIMIT)
                MAX_LIST_SIZE = value;
            else {
                mc_logger->log(EXTENSION_LOG_INFO, NULL,
                        "ARCUS_MAX_LIST_SIZE incorrect value: %d, (Allowable values: %d ~ %d)\n",
                         value, MAX_LIST_SIZE, ARCUS_COLL_SIZE_LIMIT);
            }
        }
        char *arcus_max_set_size = getenv("ARCUS_MAX_SET_SIZE");
        if (arcus_max_set_size != NULL) {
            value = atoi(arcus_max_set_size);
            if (value > MAX_SET_SIZE && value <= ARCUS_COLL_SIZE_LIMIT)
                MAX_SET_SIZE = value;
            else {
                mc_logger->log(EXTENSION_LOG_INFO, NULL,
                        "ARCUS_MAX_SET_SIZE incorrect value: %d, (Allowable values: %d ~ %d)\n",
                         value, MAX_SET_SIZE, ARCUS_COLL_SIZE_LIMIT);
            }
        }
        char *arcus_max_btree_size = getenv("ARCUS_MAX_BTREE_SIZE");
        if (arcus_max_btree_size != NULL) {
            value = atoi(arcus_max_btree_size);
            if (value > MAX_BTREE_SIZE && value <= ARCUS_COLL_SIZE_LIMIT)
                MAX_BTREE_SIZE = value;
            else {
                mc_logger->log(EXTENSION_LOG_INFO, NULL,
                        "ARCUS_MAX_BTREE_SIZE incorrect value: %d, (Allowable values: %d ~ %d)\n",
                         value, MAX_BTREE_SIZE, ARCUS_COLL_SIZE_LIMIT);
            }
        }
        /* reset maximum elements of each collection */
        settings.max_list_size = MAX_LIST_SIZE;
        settings.max_set_size = MAX_SET_SIZE;
        settings.max_btree_size = MAX_BTREE_SIZE;

        old_opts += sprintf(old_opts, "max_list_size=%d;",  MAX_LIST_SIZE);
        old_opts += sprintf(old_opts, "max_set_size=%d;",   MAX_SET_SIZE);
        old_opts += sprintf(old_opts, "max_btree_size=%d;", MAX_BTREE_SIZE);
    }

    if (engine_config != NULL && strlen(old_options) > 0) {
        /* If there is -e, just append it to the "old" options that we have
         * accumulated so far.
         */
        old_opts += sprintf(old_opts, "%s", engine_config);
        engine_config = NULL; /* So we set it to old_options below... */
        /*
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "ERROR: You can't mix -e with the old options\n");
        return EX_USAGE;
        */
    }
    if (engine_config == NULL && strlen(old_options) > 0) {
        engine_config = old_options;
    }
    mc_logger->log(EXTENSION_LOG_INFO, NULL, "engine config: %s\n", engine_config);

    if (maxcore != 0) {
        struct rlimit rlim_new;
        /*
         * First try raising to infinity; if that fails, try bringing
         * the soft limit to the hard.
         */
        if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
            if (setrlimit(RLIMIT_CORE, &rlim_new)!= 0) {
                /* failed. try raising just to the old max */
                rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
                (void)setrlimit(RLIMIT_CORE, &rlim_new);
            }
        }
        /*
         * getrlimit again to see what we ended up with. Only fail if
         * the soft limit ends up 0, because then no core files will be
         * created at all.
         */

        if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "failed to ensure corefile creation\n");
            exit(EX_OSERR);
        }
    }

    /*
     * If needed, increase rlimits to allow as many connections
     * as needed.
     */

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        /* Make room for arcus_zk and others (+ 20) */
        int maxfiles = settings.maxconns + 20;
        if (rlim.rlim_cur < maxfiles)
            rlim.rlim_cur = maxfiles;
        if (rlim.rlim_max < rlim.rlim_cur)
            rlim.rlim_max = rlim.rlim_cur;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "failed to set rlimit for open files. Try running as"
                    " root or requesting smaller maxconns value.\n");
            exit(EX_OSERR);
        }
    }

    /* Sanity check for the connection structures */
    int nfiles = 0;
    if (settings.port != 0) {
        nfiles += 2;
    }
    if (settings.udpport != 0) {
        nfiles += settings.num_threads * 2;
    }

    if (settings.maxconns <= nfiles) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Configuratioin error. \n"
                "You specified %d connections, but the system will use at "
                "least %d\nconnection structures to start.\n",
                settings.maxconns, nfiles);
        exit(EX_USAGE);
    }

    /* lose root privileges if we have them */
    if (getuid() == 0 || geteuid() == 0) {
        if (username == 0 || *username == '\0') {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "can't run as root without the -u switch\n");
            exit(EX_USAGE);
        }
        if ((pw = getpwnam(username)) == 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "can't find the user %s to switch to\n", username);
            exit(EX_NOUSER);
        }
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "failed to assume identity of user %s: %s\n", username,
                    strerror(errno));
            exit(EX_OSERR);
        }
    }

#ifdef SASL_ENABLED
    init_sasl();
#endif /* SASL */

    /* daemonize if requested */
    /* if we want to ensure our ability to dump core, don't chdir to / */
    if (do_daemonize) {
        if (sigignore(SIGHUP) == -1) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to ignore SIGHUP: %s", strerror(errno));
        }
        if (daemonize(maxcore, settings.verbose) == -1) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "failed to daemon() in order to daemonize\n");
            exit(EXIT_FAILURE);
        }
    }

    /* lock paged memory if needed */
    if (lock_memory) {
#ifdef HAVE_MLOCKALL
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "warning: -k invalid, mlockall() failed: %s\n",
                    strerror(errno));
        }
#else
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "warning: -k invalid, mlockall() not supported on this platform.  proceeding without.\n");
#endif
    }

    /* initialize main thread libevent instance */
    main_base = event_init();

    /* Load the storage engine */
    ENGINE_HANDLE *engine_handle = NULL;
    if (!load_engine(engine, get_server_api, mc_logger, &engine_handle)) {
        /* Error already reported */
        exit(EXIT_FAILURE);
    }

    if (!init_engine(engine_handle, engine_config, mc_logger)) {
        return false;
    }

    if (settings.verbose > 0) {
        log_engine_details(engine_handle, mc_logger);
    }
    mc_engine.v1 = (ENGINE_HANDLE_V1 *) engine_handle;
#if 0 // not used code
    if (mc_engine.v1->arithmetic == NULL) {
        mc_engine.v1->arithmetic = internal_arithmetic;
    }
#endif
    /* initialize other stuff */
    stats_init();

    if (!(conn_cache = cache_create("conn", sizeof(conn), sizeof(void*),
                                    conn_constructor, conn_destructor))) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to create connection cache\n");
        exit(EXIT_FAILURE);
    }

    default_independent_stats = new_independent_stats();

#ifndef __WIN32__
    /*
     * ignore SIGPIPE signals; we can use errno == EPIPE if we
     * need that information
     */
    if (sigignore(SIGPIPE) == -1) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }
#endif

#ifdef COMMAND_LOGGING
    /* initialise command logging */
    cmdlog_init(settings.port, mc_logger);
#endif

#ifdef DETECT_LONG_QUERY
    if (lqdetect_init() == -1) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "Can't allocate long query detection buffer\n");
        exit(EXIT_FAILURE);
    }
#endif

    /* start up worker threads if MT mode */
    thread_init(settings.num_threads, main_base);

    /* initialise clock event */
    clock_handler(0, 0, 0);

    /* create unix mode sockets after dropping privileges */
    if (settings.socketpath != NULL) {
        if (server_socket_unix(settings.socketpath,settings.access)) {
            vperror("failed to listen on UNIX socket: %s", settings.socketpath);
            exit(EX_OSERR);
        }
    }

    /* create the listening socket, bind it, and init */
    if (settings.socketpath == NULL) {
        //int udp_port;

        const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
        char temp_portnumber_filename[PATH_MAX];
        FILE *portnumber_file = NULL;

        if (portnumber_filename != NULL) {
            snprintf(temp_portnumber_filename,
                     sizeof(temp_portnumber_filename),
                     "%s.lck", portnumber_filename);

            portnumber_file = fopen(temp_portnumber_filename, "a");
            if (portnumber_file == NULL) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open \"%s\": %s\n",
                        temp_portnumber_filename, strerror(errno));
            }
        }

        if (settings.port && server_socket(settings.port, tcp_transport,
                                           portnumber_file)) {
            vperror("failed to listen on TCP port %d", settings.port);
            exit(EX_OSERR);
        }

        /*
         * initialization order: first create the listening sockets
         * (may need root on low ports), then drop root if needed,
         * then daemonise if needed, then init libevent (in some cases
         * descriptors created by libevent wouldn't survive forking).
         */
        //udp_port = settings.udpport ? settings.udpport : settings.port;

        /* create the UDP listening socket and bind it */
        if (settings.udpport && server_socket(settings.udpport, udp_transport,
                                              portnumber_file)) {
            vperror("failed to listen on UDP port %d", settings.udpport);
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename, portnumber_filename);
        }
    }

    /* Drop privileges no longer needed */
    drop_privileges();

#ifdef ENABLE_ZK_INTEGRATION
    // initialize Arcus ZK cluster connection
    if (arcus_zk_cfg) {
        arcus_zk_init(arcus_zk_cfg, arcus_zk_to, mc_logger,
                      settings.verbose, settings.maxbytes, settings.port,
                      mc_engine.v1);
    }
#endif

    /* Save the PID in the pid file if we're a daemon.
     * Do this after the successful startup of memcached.
     */
    if (do_daemonize)
        save_pid(getpid(), pid_file);

    /* enter the event loop */
    event_base_loop(main_base, 0);

    mc_logger->log(EXTENSION_LOG_INFO, NULL, "Initiating arcus memcached shutdown...\n");

    /* remove the PID file if we're a daemon */
    if (do_daemonize)
        remove_pidfile(pid_file);

#ifdef ENABLE_ZK_INTEGRATION
    /* shutdown arcus ZK connection */
    if (arcus_zk_cfg) {
        arcus_zk_final("graceful shutdown");
        free(arcus_zk_cfg);
    }
#endif

    close_listen_sockets();
    mc_logger->log(EXTENSION_LOG_INFO, NULL, "Listen sockets closed.\n");

    memcached_shutdown = 2;
    threads_shutdown();
    mc_logger->log(EXTENSION_LOG_INFO, NULL, "Worker threads terminated.\n");

#ifdef COMMAND_LOGGING
    /* destroy command logging */
    cmdlog_final();
#endif

#ifdef DETECT_LONG_QUERY
    lqdetect_final();
#endif

    mc_engine.v1->destroy(mc_engine.v0);
    mc_logger->log(EXTENSION_LOG_INFO, NULL, "Memcached engine destroyed.\n");

    /* Clean up strdup() call for bind() address */
    if (settings.inter)
      free(settings.inter);

    mc_logger->log(EXTENSION_LOG_INFO, NULL, "Arcus memcached terminated.\n");
    return EXIT_SUCCESS;
}
