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
#include "cmdlog.h"
#include "lqdetect.h"
#include "engine_loader.h"
#include "sasl_defs.h"

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
#define KEY_MAX_LENGTH 32000 /* long key support */

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

/* Max element value size */
#define MAX_ELEMENT_BYTES  (4*1024)

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
#define PIPE_MAX_CMD_COUNT  500
#define PIPE_MAX_RES_SIZE   ((PIPE_MAX_CMD_COUNT*40)+60) // 60: for head and tail response

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

/** Stats stored per slab (and per thread). */
struct slab_stats {
    uint64_t  cmd_set;
    uint64_t  get_hits;
    uint64_t  delete_hits;
    uint64_t  cas_hits;
    uint64_t  cas_badval;
};

/**
 * Stats stored per-thread.
 */
struct thread_stats {
    pthread_mutex_t   mutex;
    uint64_t          cmd_get;
    uint64_t          cmd_incr;
    uint64_t          cmd_decr;
    uint64_t          cmd_delete;
    uint64_t          get_misses;
    uint64_t          delete_misses;
    uint64_t          incr_misses;
    uint64_t          decr_misses;
    uint64_t          incr_hits;
    uint64_t          decr_hits;
    uint64_t          cmd_cas;
    uint64_t          cas_misses;
    uint64_t          bytes_read;
    uint64_t          bytes_written;
    uint64_t          cmd_flush;
    uint64_t          cmd_flush_prefix;
    uint64_t          conn_yields; /* # of yields for connections (-R option)*/
    uint64_t          auth_cmds;
    uint64_t          auth_errors;
    /* list command stats */
    uint64_t          cmd_lop_create;
    uint64_t          cmd_lop_insert;
    uint64_t          cmd_lop_delete;
    uint64_t          cmd_lop_get;
    /* set command stats */
    uint64_t          cmd_sop_create;
    uint64_t          cmd_sop_insert;
    uint64_t          cmd_sop_delete;
    uint64_t          cmd_sop_get;
    uint64_t          cmd_sop_exist;
    /* map command stats */
    uint64_t          cmd_mop_create;
    uint64_t          cmd_mop_insert;
    uint64_t          cmd_mop_update;
    uint64_t          cmd_mop_delete;
    uint64_t          cmd_mop_get;
    /* btree command stats */
    uint64_t          cmd_bop_create;
    uint64_t          cmd_bop_insert;
    uint64_t          cmd_bop_update;
    uint64_t          cmd_bop_delete;
    uint64_t          cmd_bop_get;
    uint64_t          cmd_bop_count;
    uint64_t          cmd_bop_position;
    uint64_t          cmd_bop_pwg;
    uint64_t          cmd_bop_gbp;
#ifdef SUPPORT_BOP_MGET
    uint64_t          cmd_bop_mget;
#endif
#ifdef SUPPORT_BOP_SMGET
    uint64_t          cmd_bop_smget;
#endif
    uint64_t          cmd_bop_incr;
    uint64_t          cmd_bop_decr;
    /* attr command stats */
    uint64_t          cmd_getattr;
    uint64_t          cmd_setattr;
    /* list hit & miss stats */
    uint64_t          lop_create_oks;
    uint64_t          lop_insert_hits;
    uint64_t          lop_insert_misses;
    uint64_t          lop_delete_elem_hits;
    uint64_t          lop_delete_none_hits;
    uint64_t          lop_delete_misses;
    uint64_t          lop_get_elem_hits;
    uint64_t          lop_get_none_hits;
    uint64_t          lop_get_misses;
    /* set hit & miss stats */
    uint64_t          sop_create_oks;
    uint64_t          sop_insert_hits;
    uint64_t          sop_insert_misses;
    uint64_t          sop_delete_elem_hits;
    uint64_t          sop_delete_none_hits;
    uint64_t          sop_delete_misses;
    uint64_t          sop_get_elem_hits;
    uint64_t          sop_get_none_hits;
    uint64_t          sop_get_misses;
    uint64_t          sop_exist_hits;
    uint64_t          sop_exist_misses;
    /* map hit & miss stats */
    uint64_t          mop_create_oks;
    uint64_t          mop_insert_hits;
    uint64_t          mop_insert_misses;
    uint64_t          mop_update_elem_hits;
    uint64_t          mop_update_none_hits;
    uint64_t          mop_update_misses;
    uint64_t          mop_delete_elem_hits;
    uint64_t          mop_delete_none_hits;
    uint64_t          mop_delete_misses;
    uint64_t          mop_get_elem_hits;
    uint64_t          mop_get_none_hits;
    uint64_t          mop_get_misses;
    /* btree hit & miss stats */
    uint64_t          bop_create_oks;
    uint64_t          bop_insert_hits;
    uint64_t          bop_insert_misses;
    uint64_t          bop_update_elem_hits;
    uint64_t          bop_update_none_hits;
    uint64_t          bop_update_misses;
    uint64_t          bop_delete_elem_hits;
    uint64_t          bop_delete_none_hits;
    uint64_t          bop_delete_misses;
    uint64_t          bop_get_elem_hits;
    uint64_t          bop_get_none_hits;
    uint64_t          bop_get_misses;
    uint64_t          bop_count_hits;
    uint64_t          bop_count_misses;
    uint64_t          bop_position_elem_hits;
    uint64_t          bop_position_none_hits;
    uint64_t          bop_position_misses;
    uint64_t          bop_pwg_elem_hits;
    uint64_t          bop_pwg_none_hits;
    uint64_t          bop_pwg_misses;
    uint64_t          bop_gbp_elem_hits;
    uint64_t          bop_gbp_none_hits;
    uint64_t          bop_gbp_misses;
#ifdef SUPPORT_BOP_MGET
    uint64_t          bop_mget_oks;
#endif
#ifdef SUPPORT_BOP_SMGET
    uint64_t          bop_smget_oks;
#endif
    uint64_t          bop_incr_elem_hits;
    uint64_t          bop_incr_none_hits;
    uint64_t          bop_incr_misses;
    uint64_t          bop_decr_elem_hits;
    uint64_t          bop_decr_none_hits;
    uint64_t          bop_decr_misses;
    /* attr hit & miss stats */
    uint64_t          getattr_hits;
    uint64_t          getattr_misses;
    uint64_t          setattr_hits;
    uint64_t          setattr_misses;
    struct slab_stats slab_stats[MAX_SLAB_CLASSES];
};


/**
 * The stats structure the engine keeps track of
 */
struct independent_stats {
    topkeys_t *topkeys;
    struct thread_stats thread_stats[];
};

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
    time_t        started;          /* when the process was started */
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
    int scrub_count;
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
#ifdef ENABLE_PERSISTENCE
    bool use_persistence;
#endif
    enum protocol binding_protocol;
    int backlog;
    size_t item_size_max;   /* Maximum item size, and upper end for slabs */
    bool sasl;              /* SASL on/off */
    bool require_sasl;      /* require SASL auth */
    int max_list_size;      /* Maximum elements in list collection */
    int max_set_size;       /* Maximum elements in set collection */
    int max_map_size;       /* Maximum elements in map collection */
    int max_btree_size;     /* Maximum elements in b+tree collection */
    int topkeys;            /* Number of top keys to track */
    struct {
        EXTENSION_DAEMON_DESCRIPTOR *daemons;
        EXTENSION_LOGGER_DESCRIPTOR *logger;
        EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ascii;
    } extensions;
};

struct engine_event_handler {
    EVENT_CALLBACK cb;
    const void *cb_data;
    struct engine_event_handler *next;
};

extern struct stats stats;
extern struct settings settings;
extern EXTENSION_LOGGER_DESCRIPTOR *mc_logger;

enum thread_type {
    GENERAL = 11
};

typedef struct {
    pthread_t thread_id;        /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
    struct event notify_event;  /* listen event for notify pipe */
    int notify_receive_fd;      /* receiving end of notify pipe */
    int notify_send_fd;         /* sending end of notify pipe */
    struct conn_queue *new_conn_queue; /* queue of new connections to handle */
    cache_t *suffix_cache;      /* suffix cache */
    pthread_mutex_t mutex;      /* Mutex to lock protect access to the pending_io */
    bool is_locked;
    struct conn *pending_io;    /* List of connection with pending async io ops */
    struct conn *conn_list;     /* connection list managed by this thread */
    int index;                  /* index of this thread in the threads array */
    enum thread_type type;      /* Type of IO this thread processes */
    token_buff_t token_buff;    /* token buffer */
    mblck_pool_t mblck_pool;    /* memory block pool */
} LIBEVENT_THREAD;

#define LOCK_THREAD(t)                          \
    if (pthread_mutex_lock(&t->mutex) != 0) {   \
        abort();                                \
    }                                           \
    assert(t->is_locked == false);              \
    t->is_locked = true;

#define UNLOCK_THREAD(t)                         \
    assert(t->is_locked == true);                \
    t->is_locked = false;                        \
    if (pthread_mutex_unlock(&t->mutex) != 0) {  \
        abort();                                 \
    }

typedef struct {
    pthread_t thread_id;        /* unique ID of this thread */
    struct event_base *base;    /* libevent handle this thread uses */
} LIBEVENT_DISPATCHER_THREAD;


typedef struct conn conn;
typedef bool (*STATE_FUNC)(conn *);

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
    sasl_conn_t *sasl_conn;
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
    int    isize;
    item   **icurr;
    int    ileft;

    char   **suffixlist;
    int    suffixsize;
    char   **suffixcurr;
    int    suffixleft;

#ifdef DETECT_LONG_QUERY
    int    lq_bufcnt;
#endif

    enum protocol protocol;   /* which protocol this connection speaks */
    enum network_transport transport; /* what transport is used by this connection */

    /* data for UDP clients */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char *hdrbuf; /* udp packet headers */
    int    hdrsize;   /* number of headers' worth of space is allocated */

    /* command pipelining processing fields */
    int               pipe_state;
    int               pipe_count;
    int               pipe_reslen;
    char             *pipe_resptr;
    char              pipe_response[PIPE_MAX_RES_SIZE];
    /*******
    int               pipe_cmd[PIPE_MAX_CMD_COUNT];
    ENGINE_ERROR_CODE pipe_res[PIPE_MAX_CMD_COUNT];
    bool              pipe_cod[PIPE_MAX_CMD_COUNT]; // create or drop
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
     * premature_notify_io_complete=true.
     *
     * See conn_parse_cmd, conn_nread, and notify_io_complete.
     */
    bool io_blocked;
    bool premature_notify_io_complete;
};

/*
 * Functions
 */
conn *conn_new(const int sfd, STATE_FUNC init_state, const int event_flags,
               const int read_buffer_size, enum network_transport transport,
               struct event_base *base, struct timeval *timeout);
#ifndef WIN32
extern int daemonize(int nochdir, int noclose);
#endif

#include "stats.h"
#include "trace.h"
#include "hash.h"
#include <memcached/util.h>

/*
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */

void thread_init(int nthreads, struct event_base *main_base);
void threads_shutdown(void);

int  dispatch_event_add(int thread, conn *c);
void dispatch_conn_new(int sfd, STATE_FUNC init_state, int event_flags,
                       int read_buffer_size, enum network_transport transport);

/* Lock wrappers for cache functions that are called from main loop. */
void accept_new_conns(const bool do_accept);
conn *conn_from_freelist(void);
bool  conn_add_to_freelist(conn *c);
int   is_listen_thread(void);

void STATS_LOCK(void);
void STATS_UNLOCK(void);
void threadlocal_stats_clear(struct thread_stats *stats);
void threadlocal_stats_reset(struct thread_stats *thread_stats);
void threadlocal_stats_aggregate(struct thread_stats *thread_stats, struct thread_stats *stats);
void slab_stats_aggregate(struct thread_stats *stats, struct slab_stats *out);

/* Stat processing functions */
void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...);

void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status);
void conn_set_state(conn *c, STATE_FUNC state);
const char *state_text(STATE_FUNC state);
void safe_close(int sfd);

void SETTING_LOCK(void);
void SETTING_UNLOCK(void);

// Number of times this connection is in the given pending list
int number_of_pending(conn *c, conn *pending);
bool has_cycle(conn *c);
bool list_contains(conn *h, conn *n);
conn *list_remove(conn *h, conn *n);
size_t list_to_array(conn **dest, size_t max_items, conn **l);

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
