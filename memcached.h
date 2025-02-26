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
#ifndef MEMCACHED_H
#define MEMCACHED_H

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */
#include <event.h>
#include <pthread.h>
#include <memcached/protocol_binary.h>
#include <memcached/engine.h>
#include <memcached/extension.h>
#include "cache.h"
#include "topkeys.h"
#include "mc_util.h"
#include "engine_loader.h"

/* This is the address we use for admin purposes.  For example, doing stats
 * and heart beats from arcus_zk.
 * We count these connections separately from regular client connections.
 * Otherwise, there may be so many client connections that admins or heart
 * beats cannot connect to memcached.  So we set aside some connections.
 */
#define ADMIN_CLIENT_IP "127.0.0.1"
#define ADMIN_MAX_CONNECTIONS 10

/** Maximum length of a key. */
//#define KEY_MAX_LENGTH 250
#define KEY_MAX_LENGTH 16000 /* long key support */

/** Maximum length of a prefix */
#define PREFIX_MAX_LENGTH 250

/** Size of an incr buf. */
#define INCR_MAX_STORAGE_LEN 24

#define DATA_BUFFER_SIZE 2048
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
/* I'm told the max length of a 64-bit num converted to string is 20 bytes.
 * Plus a few for spaces, \r\n, \0 */
#define SUFFIX_SIZE 24

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL 200

/** Initial size of list of CAS suffixes appended to "gets" lines. */
#define SUFFIX_LIST_INITIAL 20

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 10

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

/* Binary protocol stuff */
#define MIN_BIN_PKT_LENGTH 16
#define BIN_PKT_HDR_WORDS (MIN_BIN_PKT_LENGTH/sizeof(uint32_t))

#define MAX_MGET_KEY_COUNT 10000
#define MAX_SOP_GET_COUNT 1000

#ifdef SUPPORT_BOP_MGET
/* In bop mget, max limit on the number of given keys */
#define MAX_BMGET_KEY_COUNT     200
/* In bop mget, max limit on the given count (requested count) */
#define MAX_BMGET_ELM_COUNT     50
#endif

#ifdef SUPPORT_BOP_SMGET
/* In bop smget, max limit on the number of given keys */
#define MAX_SMGET_KEY_COUNT     10000
/* In bop smget, max limit on (offset+count) */
#define MAX_SMGET_REQ_COUNT     2000
#endif

/* command pipelining limits */
#define PIPE_CMD_MAX_COUNT  500
#define PIPE_RES_DATA_SIZE  40 /* data string size */
#define PIPE_RES_HEAD_SIZE  20 /* head string size */
#define PIPE_RES_TAIL_SIZE  40 /* tail string size */
#define PIPE_RES_MAX_SIZE   ((PIPE_CMD_MAX_COUNT*PIPE_RES_DATA_SIZE)+PIPE_RES_HEAD_SIZE+PIPE_RES_TAIL_SIZE)

/* command pipelining states */
#define PIPE_STATE_OFF       0
#define PIPE_STATE_ON        1
#define PIPE_STATE_ERR_CFULL 2
#define PIPE_STATE_ERR_MFULL 3
#define PIPE_STATE_ERR_BAD   4

#define STAT_KEY_LEN 128
#define STAT_VAL_LEN 128

#define DEFAULT_REQS_PER_EVENT     20

/** Append a simple stat with a stat name, value format and value */
#define APPEND_STAT(name, fmt, val) \
    append_stat(name, add_stats, c, fmt, val);

/** Append an indexed stat with a stat name (with format), value format
    and value */
#define APPEND_NUM_FMT_STAT(name_fmt, num, name, fmt, val)          \
    klen = snprintf(key_str, STAT_KEY_LEN, name_fmt, num, name);    \
    vlen = snprintf(val_str, STAT_VAL_LEN, fmt, val);               \
    add_stats(key_str, klen, val_str, vlen, c);

/** Common APPEND_NUM_FMT_STAT format. */
#define APPEND_NUM_STAT(num, name, fmt, val) \
    APPEND_NUM_FMT_STAT("%d:%s", num, name, fmt, val)

enum bin_substates {
    bin_no_state,
    bin_reading_set_header,
    bin_reading_cas_header,
    bin_read_set_value,
    bin_reading_get_key,
    bin_reading_stat,
    bin_reading_del_header,
    bin_reading_incr_header,
    bin_read_flush_exptime,
    bin_read_flush_prefix_exptime,
    bin_reading_sasl_auth,
    bin_reading_sasl_auth_data,
    bin_reading_getattr,
    bin_reading_setattr,
    bin_reading_lop_create,
    bin_reading_lop_prepare_nread,
    bin_reading_lop_nread_complete,
    bin_reading_lop_delete,
    bin_reading_lop_get,
    bin_reading_sop_create,
    bin_reading_sop_prepare_nread,
    bin_reading_sop_nread_complete,
    bin_reading_sop_get,
    bin_reading_bop_create,
    bin_reading_bop_prepare_nread,
    bin_reading_bop_nread_complete,
    bin_reading_bop_update_prepare_nread,
    bin_reading_bop_update_nread_complete,
    bin_reading_bop_delete,
    bin_reading_bop_get,
    bin_reading_bop_count,
    bin_reading_bop_position,
    bin_reading_bop_pwg,
    bin_reading_bop_gbp,
#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
    bin_reading_bop_prepare_nread_keys,
    bin_reading_bop_nread_keys_complete,
#endif
    bin_reading_packet
};

enum protocol {
    ascii_prot = 3, /* arbitrary value. */
    binary_prot,
    negotiating_prot /* Discovering the protocol */
};

enum network_transport {
    local_transport, /* Unix sockets*/
    tcp_transport,
    udp_transport
};

#define IS_UDP(x) (x == udp_transport)

/**
 * Global stats.
 */
struct mc_stats {
    pthread_mutex_t mutex;
    unsigned int  daemon_conns; /* conns used by the server */
    unsigned int  curr_conns;
    unsigned int  quit_conns;
    unsigned int  rejected_conns; /* number of times I reject a client */
    unsigned int  total_conns;
    unsigned int  conn_structs;
};

union mc_engine {
    ENGINE_HANDLE *v0;
    ENGINE_HANDLE_V1 *v1;
};

#define MAX_VERBOSITY_LEVEL 2

/* When adding a setting, be sure to update process_stat_settings */
/**
 * Globally accessible settings as derived from the commandline.
 */
struct settings {
    size_t maxbytes;
    int maxconns;
    int port;
    int udpport;
    size_t sticky_limit;
    char *inter;
    int verbose;
    rel_time_t oldest_live; /* ignore existing items older than this */
    int evict_to_free;
    char *socketpath;   /* path to unix socket if using local socket */
    int access;  /* access mask (a la chmod) for unix domain socket */
    double factor;          /* chunk size growth factor */
    int chunk_size;
    int num_threads;        /* number of worker (without dispatcher) libevent threads to run */
    char prefix_delimiter;  /* character that marks a key prefix (for stats) */
    int detail_enabled;     /* nonzero if we're collecting detailed stats */
    bool allow_detailed;    /* detailed stats commands are allowed */
    int reqs_per_event;     /* Maximum number of io to process on each io-event. */
    bool use_cas;
    enum protocol binding_protocol;
    int backlog;
    size_t item_size_max;   /* Maximum item size, and upper end for slabs */
    bool sasl;              /* SASL on/off */
    bool require_sasl;      /* require SASL auth */
    uint32_t max_list_size;      /* Maximum elements in list collection */
    uint32_t max_set_size;       /* Maximum elements in set collection */
    uint32_t max_map_size;       /* Maximum elements in map collection */
    uint32_t max_btree_size;     /* Maximum elements in b+tree collection */
    uint32_t max_element_bytes;  /* Maximum element bytes of collections */
    uint32_t scrub_count;        /* count of scrubbing items at each try */
    uint32_t max_stats_prefixes; /* Maximum prefixes to view stats */
    int topkeys;            /* Number of top keys to track */
    struct {
        EXTENSION_DAEMON_DESCRIPTOR *daemons;
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ascii;
    } extensions;
};

extern struct settings settings;
extern EXTENSION_LOGGER_DESCRIPTOR *mc_logger;
extern union mc_engine mc_engine;

typedef struct conn conn;
typedef bool (*STATE_FUNC)(conn *);

#include "thread.h"

/**
 * The structure representing a connection into memcached.
 */
/* rtype in connection */
#define CONN_RTYPE_NONE  0
#define CONN_RTYPE_MBLCK 1
#define CONN_RTYPE_HINFO 2
#define CONN_RTYPE_EINFO 3

struct conn {
    int    sfd;
    short  nevents;
    void *sasl_conn;
    bool sasl_started;
    bool authenticated;
    STATE_FUNC   state;
    enum bin_substates substate;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */

    char   *rbuf;   /** buffer to read commands into */
    char   *rcurr;  /** but if we parsed some already, this is where we stopped */
    int    rsize;   /** total allocated size of rbuf */
    int    rbytes;  /** how much data, starting from rcur, do we have unparsed */

    char   *wbuf;
    char   *wcurr;
    int    wsize;
    int    wbytes;
    /** which state to go into after finishing current write */
    STATE_FUNC   write_and_go;
    void        *write_and_free; /** free this memory after finishing writing */

    int         rtype;  /* CONN_RTYPE_XXXXX */
    int         rindex; /* used when rtype is HINFO or EINFO */
    char       *ritem;  /** when we read in an item's value, it goes here */
    uint32_t    rlbytes;
    /* use memory blocks */
    uint32_t    rltotal;    /* Used when read data with memory block */
    mblck_node_t *membk;    /* current memory block pointer */
    mblck_list_t  memblist; /* (key or field) string memory block list */

    /* hash item and elem item info */
    item_info   hinfo; /* hash item info */
    eitem_info  einfo; /* elem item info */

    /* collection processing fields */
    void        *coll_eitem;
    char        *coll_resps;
    int          coll_ecount;
    int          coll_op;      /* (collection) operation type */
    char        *coll_key;
    int          coll_nkey;
    int          coll_index;   /* the list index of lop insert */
    item_attr    coll_attr_space;
    item_attr   *coll_attrp;
    bool         coll_getrim;  /* getrim flag. See process_bop_command() */
    bool         coll_delete;  /* delete flag. See process_mop_get_complete() */
    bool         coll_drop;    /* drop flag */
#ifdef JHPARK_OLD_SMGET_INTERFACE
    int          coll_smgmode; /* smget exec mode : 0(oldexec), 1(duplicate), 2(unique) */
#else
    bool         coll_unique;  /* unique flag (used in smget) */
#endif
    bkey_range   coll_bkrange; /* bkey range */
    eflag_filter coll_efilter; /* eflag filter */
    eflag_update coll_eupdate; /* eflag update */
    uint32_t     coll_roffset; /* request offset */
    uint32_t     coll_rcount;  /* request count */
    uint32_t     coll_numkeys; /* number of keys */
    uint32_t     coll_lenkeys; /* length of keys */
    void        *coll_strkeys; /* (comma separated) multiple keys */
    /* map collection */
    field_t      coll_field;   /* field in map collection */

    /* data for the nread state */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data. The data is read into ITEM_data(item) to avoid extra copying.
     */

    void   *item;     /* for commands set/add/replace  */
    ENGINE_STORE_OPERATION    store_op; /* which one is it: set/add/replace */


    /* data for the swallow state */
    int    sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    item   **ilist;   /* list of items to write out */
    int    isize;     /* size of ilist(item list) */
    item   **icurr;   /* item current position in ilist */
    int    ileft;     /* item count left for writing out */
#ifdef SCAN_COMMAND
    item   **pcurr;   /* prefix current position in ilist */
    int    pleft;     /* prefix count left for writing out */
#endif

    char   **suffixlist;
    int    suffixsize;
    char   **suffixcurr;
    int    suffixleft;

    enum protocol protocol;   /* which protocol this connection speaks */
    enum network_transport transport; /* what transport is used by this connection */

    /* data for UDP clients */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char hdrbuf[UDP_HEADER_SIZE]; /* udp packet headers */

    /* command pipelining processing fields */
    int               pipe_state;
    int               pipe_count;
    int               pipe_errlen; /* error response length */
    int               pipe_reslen; /* total response length */
    char              pipe_resbuf[PIPE_RES_MAX_SIZE];
    /*******
    int               pipe_cmd[PIPE_CMD_MAX_COUNT];
    ENGINE_ERROR_CODE pipe_res[PIPE_CMD_MAX_COUNT];
    bool              pipe_cod[PIPE_CMD_MAX_COUNT]; // create or drop
    *******/

    char   client_ip[16];

    bool   noreply;   /* True if the reply should not be sent. */
    /* current stats command */

    struct {
        char *buffer;
        size_t size;
        size_t offset;
    } dynamic_buffer;

    void *engine_storage;

    /** Current ascii protocol */
    EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ascii_cmd;


    /* Binary protocol stuff */
    /* This is where the binary header goes */
    protocol_binary_request_header binary_header;
    uint64_t cas; /* the cas to return */
    short cmd; /* current command being processed */
    int opaque;
    int keylen;
    conn   *next;     /* Used for generating a list of conn structures */
    LIBEVENT_THREAD *thread; /* Pointer to the thread object serving this connection */
    conn *conn_prev;  /* used in the conn_list of a thread in charge */
    conn *conn_next;  /* used in the conn_list of a thread in charge */

    ENGINE_ERROR_CODE aiostat;
    bool ewouldblock;
#ifdef MULTI_NOTIFY_IO_COMPLETE
    /* ewouldblock=true is set when the command returns EWOULDBLOCK.
     * The worker thread is going to remove the connection from the
     * event loop and set ewouldblock=false.  But these two events
     * (set and remove from the event loop) do not happen atomically.
     * Rarely, notify_io_complete runs before the worker thread removes
     * the connection from the event loop.  Below, three more variables
     * deal with these cases...
     *
     * io_blocked=true is set when the worker thread actually removes
     * the connection from the event loop.  The thread locks itself
     * and then performs these two operations (set and event loop).
     *
     * current_io_wait is the number of waiting for IO completion.
     * waitfor_io_complete() increments the current_io_wait and it must be
     * called in storage engine before notify_io_complete() is called.
     *
     * premature_io_complete is the number of notifying IO completion.
     * As the waitfor_io_complete() is called before the IO completion,
     * the notify_io_complete() just decrements the current_io_wait.
     *
     * The work thread checks the current_io_wait value at the end of
     * conn_parse_cmd and conn_nread state. If it is positive value,
     * the thread blocks the current processing while setting io_blocked=true.
     * If it is 0(zero), the thread continues the current processing
     * without any blocking.
     *
     * See conn_parse_cmd, conn_nread, waitfor_io_complete, and notify_io_complete.
     */
    bool io_blocked;
    unsigned int current_io_wait;       /* num of current io wait */
    unsigned int premature_io_complete; /* num of premature io complete */
#else
    /* ewouldblock=true is set when the command returns EWOULDBLOCK.
     * The worker thread is going to remove the connection from the
     * event loop and set ewouldblock=false.  But these two events
     * (set and remove from the event loop) do not happen atomically.
     * Rarely, notify_io_complete runs before the worker thread removes
     * the connection from the event loop.  Below, two more variables
     * deal with these cases...
     *
     * io_blocked=true is set when the worker thread actually removes
     * the connection from the event loop.  The thread locks itself
     * and then performs these two operations (set and event loop).
     *
     * notify_io_complete locks the thread and checks io_blocked.
     * If io_blocked=false, then we know for sure that the worker thread
     * has not removed the connection yet.  So, it sets
     * premature_io_complete=true.
     *
     * See conn_parse_cmd, conn_nread, and notify_io_complete.
     */
    bool io_blocked;
    bool premature_io_complete;
#endif
};

/* set connection's ewouldblock according to the given return value */
#define CONN_CHECK_AND_SET_EWOULDBLOCK(ret, conn) { \
    if ((ret) == ENGINE_EWOULDBLOCK) { \
        (conn)->ewouldblock = true; \
        (ret) = ENGINE_SUCCESS; \
    } \
}

/*
 * Macros for incrementing thread_stats
 */
/* The external variables used in below macros */
extern struct thread_stats *default_thread_stats;
extern topkeys_t *default_topkeys;

#define MY_THREAD_STATS(c) (&default_thread_stats[(c)->thread->index])

#define STATS_CMD(c, op, key, nkey) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_ONE(my_thread_stats, cmd_##op); \
    TK(default_topkeys, cmd_##op, key, nkey, get_current_time()); \
}

#define STATS_OKS(c, op, key, nkey) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_oks, cmd_##op); \
    TK(default_topkeys, op##_oks, key, nkey, get_current_time()); \
}

#define STATS_HITS(c, op, key, nkey) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_hits, cmd_##op); \
    TK(default_topkeys, op##_hits, key, nkey, get_current_time()); \
}

#define STATS_ELEM_HITS(c, op, key, nkey) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_elem_hits, cmd_##op); \
    TK(default_topkeys, op##_elem_hits, key, nkey, get_current_time()); \
}

#define STATS_NONE_HITS(c, op, key, nkey) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_none_hits, cmd_##op); \
    TK(default_topkeys, op##_none_hits, key, nkey, get_current_time()); \
}

#define STATS_MISSES(c, op, key, nkey) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_misses, cmd_##op); \
    TK(default_topkeys, op##_misses, key, nkey, get_current_time()); \
}

#define STATS_BADVAL(c, op, key, nkey) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_badval, cmd_##op); \
    TK(default_topkeys, op##_badval, key, nkey, get_current_time()); \
}

#define STATS_CMD_NOKEY(c, op) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_ONE(my_thread_stats, cmd_##op); \
}

#define STATS_OKS_NOKEY(c, op) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_oks, cmd_##op); \
}

#define STATS_ERRORS_NOKEY(c, op) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_TWO(my_thread_stats, op##_errors, cmd_##op); \
}

#define STATS_ADD(c, op, amt) { \
    struct thread_stats *my_thread_stats = MY_THREAD_STATS(c); \
    THREAD_STATS_INCR_AMT(my_thread_stats, op, amt); \
}

/*
 * Functions
 */
conn *conn_new(const int sfd, STATE_FUNC init_state, const int event_flags,
               const int read_buffer_size, enum network_transport transport,
               struct event_base *base, struct timeval *timeout);
#ifndef WIN32
extern int daemonize(int nochdir, int noclose);
#endif

#include "stats_prefix.h"
#include "trace.h"
#include "hash.h"
#include <memcached/util.h>

void LOCK_STATS(void);
void UNLOCK_STATS(void);

void LOCK_SETTING(void);
void UNLOCK_SETTING(void);

/* Lock wrappers for cache functions that are called from main loop. */
void accept_new_conns(const bool do_accept);
conn *conn_from_freelist(void);
bool  conn_add_to_freelist(conn *c);

/* Stat processing functions */
void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...);

void conn_set_state(conn *c, STATE_FUNC state);
const char *state_text(STATE_FUNC state);
void safe_close(int sfd);

void init_check_stdin(struct event_base *base);

void conn_close(conn *c);

#if HAVE_DROP_PRIVILEGES
extern void drop_privileges(void);
#else
#define drop_privileges()
#endif

/* connection state machine */
bool conn_listening(conn *c);
bool conn_new_cmd(conn *c);
bool conn_waiting(conn *c);
bool conn_read(conn *c);
bool conn_parse_cmd(conn *c);
bool conn_write(conn *c);
bool conn_nread(conn *c);
bool conn_swallow(conn *c);
bool conn_closing(conn *c);
bool conn_mwrite(conn *c);

/* If supported, give compiler hints for branch prediction. */
#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)
#endif
