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
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctype.h>
#include <pthread.h>
#include <assert.h>

#include "default_engine.h"
#include "replication.h"
#include "util_common.h"
#include "queue.h"
#include "msg_chan.h"

#ifndef __GNUC__
#error "Non-gcc is not supported"
/* __builtin_offsetof */
#endif

#ifdef offsetof
#error "offsetof is already defined"
#endif
#define offsetof(type, member) __builtin_offsetof(type, member)

/* Unsupported cases.  Catch them at compile time. */
#ifdef USE_SYSTEM_MALLOC
#error "System malloc is enabled"
#endif

#define ROUNDUP_4(x) (((x) + 3) & ~0x3)

static inline uint8_t
real_nbkey(uint8_t nbkey)
{
  return BTREE_REAL_NBKEY(nbkey);
}

static inline int
btree_elem_ntotal(btree_elem_item *elem)
{
    return BTREE_ELEM_SIZE(elem);
}

static inline int
set_elem_ntotal(set_elem_item *elem)
{
    return sizeof(set_elem_item) + elem->nbytes;
}

static inline int
list_elem_ntotal(list_elem_item *elem)
{
    return sizeof(list_elem_item) + elem->nbytes;
}

/* Enable code that we use for testing purposes only.  For example,
 * test_force_snapshot_queue_overflow forces alloc_cset_snapshot_queue to
 * fail.
 */
#define USE_TEST_CODE 1

/* Minimum size of the cset buffer.  2MB can hold about 128K 16B cset items.
 * At 30K requests/s, 2MB can buffer changes for about 4 seconds.
 * At 100K requests/s, only about 1 second.
 */
#define MIN_CSET_BUFFER_SIZE (2*1024*1024)

/* The maximum number of elements per collection item is 50,000.
 * We just need one pointer per element.  50,000 * 8 = 400,000B for the
 * snapshot.  The rest (~368K) is used to buffer updates that follow
 * the snapshot.  Each update to the list item currently requires
 * 12B.  So, in the worst case, we can buffer some 30,000 updates
 * in this snapshot queue.
 * We should unify the snapshot queue and the cset queue.  FIXME
 */
#define MIN_CSET_SNAPSHOT_QUEUE_SIZE ((512+256)*1024)

/* Thread local storage */
static struct {
  pthread_key_t printable_key_buf;
  pthread_key_t printable_bkey_buf;
} rp_tls;

struct rp_config {
  int loglevel;
  int msgchan_loglevel;
  int server_loglevel;
  int engine_loglevel;
  int max_unack; /* At most this many unack'd commands are in the network */
  int max_background_items;
  int max_background_bytes;
  /* Need something to limit bandwidth we use to copy in the background.
   * Perhaps a byte limit.  What we really want is to avoid choking user
   * requests while we copy items to the slave.  Once we are in sync,
   * everything is synchronous, and we shouldn't have to artificially limit
   * replication rates.
   * FIXME
   */
  int max_sync_wait_msec;
  int replication_hiwat_msec;
  int replication_lowat_msec;
  /* ASYNC->SYNC conditions */
  int async_to_sync_consecutive_acks;
  int async_to_sync_min_duration;
  int async_to_sync_max_cset_items;
  int max_msg_queue_length;
  int max_recv_queue_length;
  int message_body_buffer_size;
  int ignore_eviction_unlink;
  int ignore_replace_unlink;
  int max_retransmission_burst;
  int unlink_copy_key;
  int no_sync_mode;
  int notify_io_complete_thread;
  int cset_buffer_size;
  int cset_snapshot_queue_size;
  int cset_max_slab_memory; /* percent of memcached maxbytes */
  int copy_missing_key;
  int lru_update_in_async;
  int slave_flush_after_handshake;
  /* Garbage collection settings */
  int gc_enable;
  int gc_thresh_memory; /* percentage of cset slab memory */
  int gc_thresh_idle_time; /* idle (no progress) time in milliseconds */
  int gc_thresh_recov_memory; /* see gc_recovery */
  int gc_thresh_recov_time;
  int gc_max_reclaim; /* free at most this many garbage items per iteration */
  int gc_min_saved_bytes; /* GC item only if we can save this much */
  /* These are used only in standalone mode */
  int mode; /* RP_MODE_{DISABLED,MASTER,SLAVE} */
  char *master_addr_str; /* strdup */
  char *slave_addr_str; /* strdup */
} config;

int rp_loglevel; /* = config.loglevel so functions in other files can see it */

struct {
  uint64_t queued_messages;
  uint64_t msg_queue_alloc_cset;
  uint64_t msg_queue_alloc_cset_ok;
  uint64_t msg_queue_alloc_cset_no_message;
  uint64_t msg_queue_alloc_cset_no_buffer;
  uint64_t pushed_cset_item;
  uint64_t sent_message;
  uint64_t retransmission;
  uint64_t ackd_message;
  uint64_t btree_elem_insert;
  uint64_t btree_elem_replace;
  uint64_t btree_elem_unlink;
  uint64_t set_elem_insert;
  uint64_t set_elem_unlink;
  uint64_t list_elem_insert;
  uint64_t list_elem_unlink;
  uint64_t item_link;
  uint64_t item_unlink;
  uint64_t item_unlink_evict;
  uint64_t item_unlink_evict_delete_now;
  uint64_t item_unlink_evict_ignored;
  uint64_t item_unlink_immediate_recycle;
  uint64_t item_unlink_replace;
  uint64_t item_unlink_replace_ignored;
  uint64_t snapshot_abort;
  uint64_t lru_update;
  uint64_t lru_update_in_async_ignored;
  uint64_t flush_prefix;
  uint64_t flush_all;
  uint64_t setattr;
  uint64_t refcount_up;
  uint64_t refcount_down;
  uint64_t recv_allocate_no_buffer;
  uint64_t missing_keys_table_full;
  uint64_t missing_keys_buffer_full;
  uint64_t missing_keys_insert;
  uint64_t missing_keys_nonexistent;
  uint64_t clients_waited_too_long;
  uint64_t block_client;
  uint64_t wakeup_client;
  uint64_t thread_sleep;
  uint64_t snapshot_cset_item;
  uint64_t snapshot_queue_cset_item;
  uint64_t coll_snapshot;
  uint32_t snapshot_duration_max;
  uint32_t snapshot_duration_min;
  uint32_t snapshot_duration_avg;
  uint32_t snapshot_ccnt_max;
  uint32_t snapshot_ccnt_avg;
  uint32_t start_nack;
  uint32_t slave_nack;
  uint32_t cset_overflow;
  uint32_t snapshot_queue_overflow;
  uint64_t ping;
  uint32_t ack_latency_usec;
  uint32_t gc_count;
} master_stats;

/* Macros to sanity check reference counts.  We can nullify these later on.
 * The first 16 bits are refcount for both hash_item and element.
 */
#define MASTER_HOLD_REFERENCE(it_or_elem)          \
  do {                                             \
    uint16_t *_refcount = ((uint16_t*)it_or_elem); \
    *_refcount = *_refcount + 1;                   \
    master_stats.refcount_up++;                    \
    assert(*_refcount != 0); /* overflow */        \
  } while(0)

#define MASTER_RELEASE_REFERENCE()          \
  do {                                      \
    master_stats.refcount_down++;           \
  } while(0)

struct {
  uint64_t received_commands;
  uint64_t simple_item_link;
  uint64_t simple_item_link_error;
  uint64_t btree_item_link;
  uint64_t btree_item_link_replace;
  uint64_t btree_item_link_error;
  uint64_t set_item_link;
  uint64_t set_item_link_replace;
  uint64_t set_item_link_error;
  uint64_t list_item_link;
  uint64_t list_item_link_replace;
  uint64_t list_item_link_error;
  uint64_t item_unlink;
  uint64_t item_unlink_evict;
  uint64_t item_unlink_error;
  uint64_t coll_key;
  uint64_t coll_key_error;
  uint64_t btree_elem_insert;
  uint64_t btree_elem_insert_error;
  uint64_t btree_elem_unlink;
  uint64_t btree_elem_unlink_error;
  uint64_t btree_elem_no_coll_key;
  uint64_t set_elem_insert;
  uint64_t set_elem_insert_error;
  uint64_t set_elem_unlink;
  uint64_t set_elem_unlink_error;
  uint64_t set_elem_no_coll_key;
  uint64_t list_elem_insert;
  uint64_t list_elem_insert_error;
  uint64_t list_elem_unlink;
  uint64_t list_elem_unlink_error;
  uint64_t list_elem_no_coll_key;
  uint64_t lru_update;
  uint64_t lru_update_error;
  uint64_t flush;
  uint64_t flush_error;
  uint64_t flush_prefix;
  uint64_t flush_prefix_error;
  uint64_t attr_exptime;
  uint64_t attr_exptime_error;
  uint64_t attr_exptime_info;
  uint64_t attr_exptime_info_error;
  uint64_t attr_exptime_info_no_coll_key;
  uint64_t attr_exptime_info_bkey;
  uint64_t attr_exptime_info_bkey_error;
  uint64_t attr_exptime_info_bkey_no_coll_key;
  uint64_t junktime;
  uint64_t junktime_error;
  uint64_t ping;
} slave_stats;

#define RP_MODE_DISABLED 0
#define RP_MODE_MASTER   1
#define RP_MODE_SLAVE    2
static int mode;
static const char *mode_string[] = {
  "DISABLED", "MASTER", "SLAVE",
};

/* We get these from the default engine. */
static struct default_engine *engine;
static pthread_mutex_t *cache_lock;

static inline void
lock_cache(void)
{
  pthread_mutex_lock(cache_lock);
}

static inline void
unlock_cache(void)
{
  pthread_mutex_unlock(cache_lock);
}

struct key_buffer;
struct key_hash_table;

/* Pre-allocated fixed size buffers */
struct fixed_buffer {
  struct fixed_buffer *next;
  /* user buffer */
};

struct fixed_buffer_pool {
  int limit, free;
  int size; /* buffer size */
  struct fixed_buffer *list; /* free list */
  pthread_mutex_t lock;
  struct fixed_buffer *mem; /* malloc'd array of buffers */
};

struct wait_entry {
  TAILQ_ENTRY(wait_entry) list;
  uint64_t seq;
  uint64_t start_msec;
  void *cookie;
  int ret;
};
TAILQ_HEAD(wait_list, wait_entry);

#define MASTER_MSG_FROM_SENDREQ(req)                                    \
  (struct master_msg*)((char*)req - ((char*)(&(((struct master_msg*)0)->req))))

/* Common header for both master and slave messages */
struct netmsg_header {
  uint32_t master_sid;
  uint32_t seq;
  uint8_t type;
  uint8_t unused[3];
};

#define MASTER_NETMSG_TYPE_MIN       1 /* not valid type */
#define MASTER_NETMSG_TYPE_HANDSHAKE 1
#define MASTER_NETMSG_TYPE_CSET      2
#define MASTER_NETMSG_TYPE_PING      3
#define MASTER_NETMSG_TYPE_GRACEFUL_FAILOVER  4
#define MASTER_NETMSG_TYPE_MAX       4 /* not valid type */

#define SLAVE_NETMSG_TYPE_MIN          10
#define SLAVE_NETMSG_TYPE_ACK          10
#define SLAVE_NETMSG_TYPE_NACK         11
#define SLAVE_NETMSG_TYPE_HANDSHAKE    12
#define SLAVE_NETMSG_TYPE_MISSING_KEY  13
#define SLAVE_NETMSG_TYPE_GRACEFUL_FAILOVER_DONE  14
#define SLAVE_NETMSG_TYPE_MAX          14

struct master_netmsg_handshake {
  struct netmsg_header common;
  /* The master's session id.  The slave can determine if the master has
   * restarted.
   */
  uint32_t master_sid;
  /* Synchronize time */
  rel_time_t time; /* master's current relative time (get_current_time) */
  int junk_item_time;
};

struct master_netmsg_cset {
  struct netmsg_header common;
  /* The master and the slave each has one contiguous buffer of the exactly
   * same size (config.message_body_buffer_size). 
   * The cset message contain the offset within this memory buffer.  The slave
   * copies the message body to the area specified by the offset.  This avoids
   * memory allocations on the slave.
   */
  uint32_t remote_buffer_offset;
};

struct master_netmsg_ping {
  struct netmsg_header common;
  uint64_t now; /* master time in msec */
};

struct master_netmsg_graceful_failover {
  struct netmsg_header common;
};

struct slave_netmsg_handshake {
  struct netmsg_header common;
  uint32_t master_sid; /* return the sid from the master */
  uint32_t slave_sid; /* slave's own sid */
};

/* This buffer can store some 80 100B keys.
 * Buffer contains "<uint16_t len><key bytes><uint16_t len><key bytes>..."
 */
#define SLAVE_NETMSG_MISSING_KEY_BUFSIZE (8*1024)
struct slave_netmsg_missing_key {
  struct netmsg_header common;
  uint32_t keybuf_len;
  /* char keybuf[SLAVE_NETMSG_MISSING_KEY_BUFSIZE]; */
};

static const char *netmsg_type_string[] = {
  "INVALID", /* 0 */
  "HANDSHAKE", "CSET", "PING",
  "GRACEFUL_FAILOVER", /* the end of master messages */
  "INVALID", "INVALID", "INVALID", "INVALID", "INVALID", /* 9 */
  "ACK", "NACK", "HANDSHAKE", "MISSING_KEY", "GRACEFUL_FAILOVER_DONE",
};

static const int netmsg_type_size[] = {
  0, /* 0 */
  sizeof(struct master_netmsg_handshake),
  sizeof(struct master_netmsg_cset),
  sizeof(struct master_netmsg_ping),
  sizeof(struct master_netmsg_graceful_failover),
  0, 0, 0, 0, 0, /* 9 */
  sizeof(struct netmsg_header),
  sizeof(struct netmsg_header),
  sizeof(struct slave_netmsg_handshake),
  sizeof(struct slave_netmsg_missing_key),
  sizeof(struct netmsg_header),
};

/* This structure represents one message from the master to the slave.
 * The actual on-the-wire message formats are defined above (master_netmsg).
 * This structure includes bits we use for in-order transmission and
 * also retransmission.
 */
struct master_msg {
  TAILQ_ENTRY(master_msg) list;
  uint32_t seq; /* message sequence */
  bool in_net;
  uint64_t tx_msec; /* when we called msg_chan_send */
  uint64_t tx_usec;
  uint64_t cset_seq;
  /* Background cset items in this message */
  int bg_items;
  int bg_bytes;

  struct msg_chan_sendreq_user req; /* includes the header (netmsg) */
  int chunk_idx;
  int num_chunks;
};
TAILQ_HEAD(master_msg_list, master_msg);

/* The master thread creates messages and pushes them to this queue.
 * The queue takes care of sending and retransmitting.
 */
struct master_msg_queue {
  uint64_t last_send_msec;
  uint32_t seq;
  int count;
  struct master_msg_list send_list;
  struct master_msg_list ackd_list;
  struct master_msg *last_sent;
  /* Background items and the number of bytes used by them.
   * Use these to limit the amount of background copy traffic in the network.
   */
  int bg_items_in_net;
  int bg_bytes_in_net;
  
  /* Pre-allocated messages and the body buffer (chunks) */
  struct master_msg_list free_list;
  int max_chunks;
  char *contig_buffer;
  char *free_chunk_map; /* FIXME.  Use circular buffer instead. */

  /* Used to aggregate/build cset messages */
  hash_item *cset_last_it;
  struct master_msg *cset_msg;
  char *cset_free_ptr;
  int cset_free_bytes;
};
/* FIXME.  Turn these into configuration variables. */
#define MASTER_MSG_BUFFER_SMALL (8*1024)
#define MASTER_MSG_BUFFER_BIG   (1*1024*1024+1024)

/* In-memory change set (cset).
 * Do not call this "log" as we may remove arbitrary elements from the set,
 * or coalesce them.
 */
struct cset_item {
  uint8_t type;
  uint8_t flags;
  uint8_t unused[2];
};

struct cset_item_hash_item {
  uint8_t type;
  uint8_t flags;
  uint8_t unlink_cause;
  uint8_t unused[1];
  /* On 64-bit machine, there is 4B pad here.  Use packed?  FIXME. */
  hash_item *it;
};

struct cset_item_coll_elem {
  uint8_t type;
  uint8_t flags;
  uint8_t unused[2];
  hash_item *it;
  void *elem;
};

struct cset_item_list_elem {
  uint8_t type;
  uint8_t flags;
  int16_t idx;
  hash_item *it;
  list_elem_item *elem;
};

struct cset_item_flush {
  uint8_t type;
  uint8_t flags;
  uint8_t nprefix;
  uint8_t unused[1];
  char prefix[256];
};

struct cset_item_unlink_noref {
  uint8_t type;
  uint8_t flags;
  uint8_t unlink_cause;
  uint8_t nkey;
  /* char key[KEY_MAX_LENGTH]; */
  /* actual size of this struct is nkey + 4, rounded up to the nearest
   * multiple of 4
   */
};

struct cset_item_pointer {
  uint8_t type;
  uint8_t flags;
  /* The size of the item that this pointer replaces.  We use this to skip
   * to the next item.
   */
  uint16_t size;
  struct cset_item *csi;
};

struct cset_item_btree_unlink_noref {
  uint8_t type;
  uint8_t flags;
  uint8_t unused[2];
  hash_item *it; /* btree item */
  uint8_t nbkey; /* 0 = 8B... */
  /* char bkey[MAX_BKEY_LENG];
   * MAX_BKEY_LENG = 31, defined in types.h.
   * The actual size of this item is sizeof(struct ...), rounded up to the
   * nearest multiple of 4.
   */
};

#define CSI_LINK           1
#define CSI_UNLINK         2
#define CSI_LIST_INSERT    3
#define CSI_LIST_UNLINK    4
#define CSI_SET_INSERT     5
#define CSI_SET_UNLINK     6
#define CSI_ATTR_EXPTIME_INFO      7
#define CSI_ATTR_EXPTIME_INFO_BKEY 8
#define CSI_BTREE_INSERT   9
#define CSI_BTREE_UNLINK   10
#define CSI_LRU_UPDATE     11
#define CSI_FLUSH          12
#define CSI_ATTR_EXPTIME   13
#define CSI_JUNKTIME       14
/* Below are items with variable sizes */
#define CSI_UNLINK_NOREF   15
#define CSI_POINTER        16
#define CSI_BTREE_UNLINK_NOREF     17

static int cset_item_size_fixed[] = {
  0, /* invalid */
  sizeof(struct cset_item_hash_item), /* LINK */
  sizeof(struct cset_item_hash_item), /* UNLINK */
  sizeof(struct cset_item_coll_elem), /* LIST_INSERT */
  sizeof(struct cset_item_coll_elem), /* LIST_UNLINK */
  sizeof(struct cset_item_coll_elem), /* SET_INSERT */
  sizeof(struct cset_item_coll_elem), /* SET_UNLINK */
  sizeof(struct cset_item_hash_item), /* ATTR_EXPTIME_INFO */
  sizeof(struct cset_item_hash_item), /* ATTR_EXPTIME_INFO_BKEY */
  sizeof(struct cset_item_coll_elem), /* BTREE_INSERT */
  sizeof(struct cset_item_coll_elem), /* BTREE_UNLINK */
  sizeof(struct cset_item_hash_item), /* LRU_UPDATE */
  sizeof(struct cset_item_flush), /* FLUSH */
  sizeof(struct cset_item_hash_item), /* ATTR_EXPTIME */
  sizeof(struct cset_item), /* JUNKTIME */
  -1, /* CSI_UNLINK_NOREF */
  -1, /* POINTER */
  -1, /* CSI_BTREE_UNLINK_NOREF */
};

/* Background copy (copy_hash_table) or foreground copy (user requests) */
#define CSET_ITEM_FLAGS_BACKGROUND    (1<<0)

/* FIXME.  Only one bit of 8-bit flags is used right now.  Get rid of
 * the flags field and use the upper bits of type to store flags.
 */

/* 8B */
struct cset_snapshot_elem {
  void *elem;
};

/* 12B (16B) */
struct cset_snapshot_elem_btree_set {
  uint8_t type;
  uint8_t unused[3];
  void *elem;
};

/* 12B (16B) */
struct cset_snapshot_elem_list_insert {
  uint8_t type;
  uint8_t unused;
  int16_t idx;
  list_elem_item *elem;
};

/* 4B */
struct cset_snapshot_elem_list_unlink {
  uint8_t type;
  uint8_t unused;
  int16_t idx;
};

struct cset_snapshot_attr {
  uint8_t type;
  uint8_t unused[3];
};

/* Change set representations that appear in the body of master_netmsg_cset.
 * These get sent over the network to the slave.
 */
#define CSNM_LINK_SIMPLE_KEY   0
#define CSNM_LINK_BTREE_KEY    1
#define CSNM_LINK_SET_KEY      2
#define CSNM_LINK_LIST_KEY     3
#define CSNM_UNLINK_KEY        4
#define CSNM_COLL_KEY          5
#define CSNM_LINK_BTREE_ELEM   6
#define CSNM_UNLINK_BTREE_ELEM 7
#define CSNM_LINK_SET_ELEM     8
#define CSNM_UNLINK_SET_ELEM   9
#define CSNM_LINK_LIST_ELEM    10
#define CSNM_UNLINK_LIST_ELEM  11
#define CSNM_LRU_UPDATE_KEY    12
#define CSNM_UNLINK_EVICT_KEY             13
#define CSNM_UNLINK_EVICT_DELETE_NOW_KEY  14
#define CSNM_FLUSH                        15
#define CSNM_ATTR_EXPTIME                 16
#define CSNM_ATTR_EXPTIME_INFO            17
#define CSNM_ATTR_EXPTIME_INFO_BKEY       18
#define CSNM_JUNKTIME                     19

struct cset_netmsg_key {
  uint8_t type;
  uint8_t len; /* key length */
};

struct cset_netmsg_item_meta {
  uint32_t flags;
  rel_time_t exptime;
};

/* Simple key's value */
struct cset_netmsg_val {
  uint64_t cas; /* 0 means no cas.  See item_get_cas. */
  uint32_t len; /* value might be up to 1MB */
  /* value */
};

struct cset_netmsg_btree_elem {
  uint8_t type;
  uint8_t nbkey;
  uint16_t len;
  /* LINK: btree_elem_item_fixed
   * UNLINK: bkey
   */
};

struct cset_netmsg_set_elem {
  uint8_t type;
  uint8_t unused;
  uint16_t val_len; /* elem value */
};

struct cset_netmsg_list_elem {
  uint8_t type;
  uint8_t unused[3];
  uint16_t idx; /* list element index */
  uint16_t val_len; /* elem value */
};

struct cset_netmsg_flush {
  uint8_t type;
  uint8_t nprefix;
  uint8_t unused[2];
  char prefix[256];
};

struct cset_netmsg_exptime {
  rel_time_t exptime;
};

struct cset_netmsg_exptime_info {
  uint8_t type;
  uint8_t ovflact;
  uint8_t mflags;
  uint8_t unused;
  rel_time_t exptime;
  int32_t mcnt;
};

struct cset_netmsg_exptime_info_bkey {
  uint8_t type;
  uint8_t ovflact;
  uint8_t mflags;
  uint8_t unused;
  rel_time_t exptime;
  int32_t mcnt;
  bkey_t maxbkeyrange;
};

struct cset_netmsg_junktime {
  uint8_t type;
  uint8_t unused[3];
  int junk_item_time;
};

struct cset {
  bool overflow; /* overflowed? */
  bool background; /* doing background copy (copy_hash_table)? */
  int count; /* total count */
  int bg_items; /* cset items from background copy */
  int bg_bytes;
  uint64_t slab_bytes; /* slab bytes currently held by cset */
  uint64_t max_slab_bytes; /* engine.maxbytes * config.cset_max_memory/100 */
  struct cset_item *next_item; /* moved when pulling items from cset */

  uint64_t latest_seq; /* used when pushing items to cset */
  uint64_t oldest_seq; /* used when pulling items from cset */
  uint64_t completed_seq;

  /* Circular buffer of 4B words */
  struct {
    char *start, *end, *prod;
    int free;
    int size;
  } mem;
  /* FIXME.  Should be able to turn this queue into a lock free structure. */
  pthread_mutex_t lock;

  /* -- Background copy --
   * copy_hash_table adds items to the cset above.  Simple items or
   * small collection items go directly into the cset.  But, a large
   * collection item (e.g. 10+K elements) is staged here.  We add
   * some number of elements at a time to the cset to avoid head-of-line
   * blocking.
   *
   * Copying a large collection item is a three-step process.
   * 1. Add "cset_link" for the collection item to the cset.
   * 2. Add every single element to the snapshot queue below.
   * 3. Incrementally move elements from the snapshot queue to the cset.
   *    copy_hash_table performs this task.
   *
   * While the snapshot queue is not empty, updates to the collection item
   * are appended to the snapshot queue.
   */
  struct {
    bool on;
    bool abort;
    hash_item *it;
    int list_idx; /* increments each time we pull a list element */
    int size;
    char *head, *tail, *last; /* last = last snapshot element + 1 */
    /* This buffer is a simple array.  We allocate at the tail and pull
     * elements from the head.  When the tail reaches the end of the buffer,
     * we are out of memory, and snapshotting fails.
     */
    char *queue; /* separate buffer for the snapshot queue */
    uint8_t insert_type, unlink_type;
    uint64_t start_usec, end_usec;
  } snapshot;
#if USE_TEST_CODE
  bool test_force_snapshot_queue_overflow;
  bool test_force_snapshot_move_overflow;
  bool test_force_snapshot_move_overflow_tail;
#endif
};

/* A cache of wait entries.  Never free these. */
struct wait_entry_cache {
  int count;
  struct wait_list free_entries;
  /* The caller must take care of locking if necessary */
  /* pthread_mutex_t lock; */
};

struct master_recv_msg {
  TAILQ_ENTRY(master_recv_msg) list;
  union {
    struct netmsg_header common;
    struct slave_netmsg_handshake hs;
  } nm;
};
TAILQ_HEAD(master_recv_list, master_recv_msg);

/* FIXME.  Make a receive message pool API
 * - X number of type A
 * - Y number of type B
 * ...
 * The caller says "give me 1 message of type A".
 * Internally, we can still use fixed_buffer_pool.
 */

struct gc_state {
  /* When we enter ASYNC mode, reset and start checking for idleness.
   * We check oldest_seq once per second.
   * If idle > thresh_idle and bytes > thresh_bytes, start GC.
   *
   * Remain in GC until we scan the whole cset buffer.
   *
   * We do gc only once.  When GC completes, make a note and never do
   * GC again as long as we are in ASYNC mode.
   */
  int state;
#define GC_STATE_INVALID      0
#define GC_STATE_DETECT_IDLE  1 /* When we enter ASYNC */
#define GC_STATE_RUNNING_GC   2 /* When we start GC'ing */
#define GC_STATE_RECOVERY     3 /* When we finish GC.  Naming.  FIXME */
    
  /* thresh_bytes = cset.max_slab_bytes * config.gc_thresh_memory/100 */
  uint64_t thresh_bytes;
  uint64_t thresh_recov_bytes;
  
  /* Detect an idle period. */
  uint64_t idle_last_check_msec;
  uint64_t idle_start_msec;
  uint64_t idle_oldest_seq;
  
  /* Running GC */
  uint64_t start_msec;
  uint64_t start_oldest_seq;
  uint64_t start_slab_bytes;
  int start_count;
  struct cset_item *cset_next_item; /* next item to examine */
  char *cset_mem_prod; /* scan till next_item reaches this point */
  
  /* Stats */
  struct {
    uint64_t scan;
    uint64_t candidate_item;
    uint64_t candidate_item_bytes;
    uint64_t item_reclaimed;
    uint64_t cset_bytes;
    uint64_t item_released_bytes;
    uint64_t candidate_btree_elem;
    uint64_t candidate_btree_elem_bytes;
    uint64_t btree_elem_reclaimed;
    uint64_t btree_elem_released_bytes;
  } stats;

  /* Detect a normal period.  For some duration, slab bytes stay below
   * thresh_low_bytes.
   */
  uint64_t recov_last_check_msec;
  uint64_t recov_start_msec;
  uint64_t recov_last_slab_bytes;
};

static char *gc_state_str[] = {
  "INVALID", "DETECT_IDLE", "RUNNING_GC", "RECOVERY"
};

static struct {
#if USE_TEST_CODE
  bool test_force_slave_msg_chan_id;
#endif
  struct msg_chan *msg_chan;
  int slave_ch_id;
  int running;
  int state;
  /* Session id.  The master sets it to the current time during init.
   * It sends the id to the slave during HANDSHAKE.  The slave responds
   * with its session id.  If the slave id has changed, then we assume
   * the slave state is empty and start from scratch.  Otherwise, we resume
   * where we left off.
   */
  uint32_t master_sid;
  uint32_t slave_sid;
  
  /* Pre-allocated receive messages */
  struct fixed_buffer_pool recv_msg_pool;

  /* The list of reeceived messages to be processed by master thread */
  struct master_recv_list recv_list; /* tail=most recent */
  int recv_list_length;

  /* Sending messages to the slave, including retransmission.  It also
   * takes care of turining change set items into replication messages.
   */
  struct master_msg_queue msg_queue;
  
  /* The change set.  All updates to items are pushed into this, and then
   * master_thread pulls them out and sends replication messages to the slave.
   */
  struct cset cs;
  
  /* A single list of waiters.  The list is ordered by the sequence number.
   * They wait on cset's sequence numbers.
   */
  struct wait_list waiters;
  int waiter_count;
  struct wait_entry_cache w_cache;
  
  uint64_t now_msec; /* Current time in msec, updated by master_thread */
  uint64_t now_usec;
  
  bool sleeping;
  pthread_mutex_t lock;
  pthread_cond_t cond; /* master thread waits for events */
  struct cset_item_and_netsize {
    struct cset_item *csi;
    int net_size; /* Extremely ugly.  FIXME */
  } *csi_array; /* temporary array used to build messages */
  int csi_array_max;
  /* The hash table cursor.  Use it to walk the table and copy items in the
   * background (copy_hash_table).
   */
  bool cursor_valid;
  struct assoc_cursor cursor;
  /* */
  struct sockaddr_in listen_addr;
  struct sockaddr_in slave_addr;
  /* master_thread */
  pthread_t tid;
  pthread_attr_t attr;

  /* notify_io_complete_thread */
  struct {
    bool running;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_t tid;
    pthread_attr_t attr;
    struct wait_list waiters;
  } notify_io_complete;

  /* Garbage collection */
  struct gc_state gc;

  struct {
    int state;
    uint64_t check_next_msec;
#define GRACEFUL_FAILOVER_STATE_NONE     0
#define GRACEFUL_FAILOVER_STATE_START    1
#define GRACEFUL_FAILOVER_STATE_PREPARE  2
#define GRACEFUL_FAILOVER_STATE_DRAIN    3
#define GRACEFUL_FAILOVER_STATE_ACK      4
#define GRACEFUL_FAILOVER_STATE_SLAVE_DONE  5
#define GRACEFUL_FAILOVER_STATE_SLEEP    6
    bool slave_done; /* true when we receive GRACEFUL_FAILOVER_DONE */
  } graceful_failover;
  
  /* Missing keys from the slave */
  struct key_hash_table *missing_ktab;
  struct key_buffer *missing_kbuf;

  /* One pre-allocated missing key message.  This is out-of-band message.
   * We do not append it to recv_list.  This is so ugly.  FIXME
   */
  bool missing_kmsg_ready;
  struct slave_netmsg_missing_key missing_kmsg;
  char missing_kmsg_keybuf[SLAVE_NETMSG_MISSING_KEY_BUFSIZE];
} master;
#define MASTER_STATE_INVALID         0
#define MASTER_STATE_HANDSHAKE_START 1
#define MASTER_STATE_HANDSHAKE_WAIT  2
#define MASTER_STATE_START_CURSOR    3
#define MASTER_STATE_RUN             4
#define MASTER_STATE_NACK            5
#define MASTER_STATE_ASYNC_RUN       6
#define MASTER_STATE_GRACEFUL_FAILOVER  7

static const char *master_state_str[] = {
  "INVALID", "HANDSHAKE_START", "HANDSHAKE_WAIT", "START_CURSOR",
  "RUN", "NACK", "ASYNC_RUN", "GRACEFUL_FAILOVER",
};

static const char *graceful_failover_state_str[] = {
  "NONE", "START", "PREPARE", "DRAIN", "ACK", "SLEEP",
};

static inline void
lock_master(void)
{
  pthread_mutex_lock(&master.lock);
}

static inline void
unlock_master(void)
{
  pthread_mutex_unlock(&master.lock);
}

struct slave_msg {
  struct msg_chan_sendreq_user req;
};
#define SLAVE_MSG_FROM_SENDREQ(req) (struct slave_msg*)(req)

struct slave_recv_msg {
  TAILQ_ENTRY(slave_recv_msg) list;
  char *body; /* points to an offset in message_body_buffer */
  int body_size;
  union {
    struct netmsg_header common;
    struct master_netmsg_handshake hs;
    struct master_netmsg_cset cset;
    struct master_netmsg_ping ping;
  } header;
};
TAILQ_HEAD(slave_recv_list, slave_recv_msg);

struct slave_recv_queue {
  struct slave_recv_list list; /* tail=most recent */
  int length;
  pthread_mutex_t lock;
  pthread_cond_t cond;
};

static struct {
  struct msg_chan *msg_chan;
  int master_ch_id;
  int running;
  uint32_t master_sid;
  uint32_t slave_sid;
  uint32_t seq; /* Next master sequence number we expect */
  bool graceful_failover;
  /* Used to convert the master clock to the local clock */
  rel_time_t master_base_time;
  rel_time_t slave_base_time;
  /* The last collection item the slave processed.  The master and the slave
   * remember this to avoid sending the same key again and again, especially
   * when copying a large collection item in the background.
   */
  hash_item *last_coll_it;  
  /* Pre-allocated send messages */
  struct fixed_buffer_pool send_msg_pool;
  /* Pre-allocated receive messages */
  struct fixed_buffer_pool recv_msg_pool;
  /* Received messages from the master */
  struct slave_recv_queue recv;
  int message_body_buffer_size;
  char *message_body_buffer;
  /* testing */
  struct {
    int delay_response; /* msec */
  } test;
  struct sockaddr_in listen_addr;
  struct sockaddr_in master_addr;
  /* slave_thread */
  pthread_t tid;
  pthread_attr_t attr;
  /* Missing keys */
  struct key_hash_table *missing_ktab;
  struct key_buffer *missing_kbuf;
  uint64_t missing_kmsg_last_msec;
  struct msg_chan_sendreq_user missing_kmsg;
  char missing_kmsg_keybuf[SLAVE_NETMSG_MISSING_KEY_BUFSIZE];
  /* Used to add the master znode asynchronously.  See slave_thread and
   * zk_add_master.
   */
  int async_add_master_complete;
  uint64_t next_graceful_failover_done_msec;
} slave;

static inline void
lock_slave_recv(void)
{
  pthread_mutex_lock(&slave.recv.lock);
}

static inline void
unlock_slave_recv(void)
{
  pthread_mutex_unlock(&slave.recv.lock);
}

static void master_start_nack_processing(void);
static struct wait_entry *alloc_wait_entry(struct wait_entry_cache *cache);
static void free_wait_entry(struct wait_entry_cache *cache,
  struct wait_entry *e);
static int wake_waiters(uint64_t seq, uint64_t now, int elapsed,
  bool master_locked, struct wait_list *removed_waiters);
static int master_init(struct sockaddr_in *listen);
static int slave_init(struct sockaddr_in *listen, struct sockaddr_in *master);
static struct msg_chan *start_msg_chan(struct sockaddr_in *listen,
  const char *id);
static void destroy_slave(void);
static uint64_t getmsec(void);
#define COPY_BACKGROUND_SOURCE_HASH_TABLE    0
#define COPY_BACKGROUND_SOURCE_MISSING_KEYS  1
static void copy_background(int source);
static int calc_cset_netmsg_size(struct cset_item *csi);
static void cset_add_background(struct cset *cs, struct cset_item *csi);
static int config_read(struct rp_config *o, const char *path);

static int
init_fixed_buffer_pool(struct fixed_buffer_pool *p, int size, int limit)
{
  struct fixed_buffer *b;
  int total, i;
  
  total = (size+sizeof(*b)) * limit;
  b = malloc(total);
  if (b == NULL)
    return -1;
  memset(b, 0, total);
  p->mem = b;
  p->limit = limit;
  p->free = limit;
  p->size = size;
  p->list = NULL;
  for (i = 0; i < limit; i++) {
    b->next = p->list; /* push at the head */
    p->list = b;
    b = (void*)(((char*)(b+1)) + size); /* |hdr|user|hdr|user| ... | */
  }
  pthread_mutex_init(&p->lock, NULL);
  return 0;
}

static void
free_fixed_buffer_pool(struct fixed_buffer_pool *p)
{
  free(p->mem);
}

static void *
alloc_fixed_buffer(struct fixed_buffer_pool *p)
{
  void *user = NULL;
  struct fixed_buffer *b;
  pthread_mutex_lock(&p->lock);
  if ((b = p->list)) {
    p->free--;
    p->list = b->next;
    user = (void*)(b+1);
  }
  pthread_mutex_unlock(&p->lock);
  return user;
}

static void
free_fixed_buffer(struct fixed_buffer_pool *p, void *user)
{
  struct fixed_buffer *b = (void*)(((char*)user) - sizeof(struct fixed_buffer));
  pthread_mutex_lock(&p->lock);
  p->free++;
  /* push to the head */
  b->next = p->list;
  p->list = b;
  pthread_mutex_unlock(&p->lock);
}

#define INVALID_SID 0

static uint32_t
gen_sid(void)
{
  uint32_t id = getmsec();
  if (id == INVALID_SID)
    id++;
  return id;
}

/* The master thread periodically calls this function to handle overflows. */
static void
cset_check_overflow(struct cset *cs)
{
  if (cs->overflow) {
    master_start_nack_processing();
  }
}

static inline void
cset_lock(struct cset *cs)
{
  /* Memcached worker threads always lock cset.  The master thread
   * locks only if it has not done so.  copy_background locks cset and
   * calls various cset functions.  Worse, it may even end up calling functions
   * in items.c, which then calls rp functions, which then calls cset
   * functions...
   *
   * background=true means the master thread has locked cset and is running
   * in copy_background.  Check the thread id against to the master thread.
   * If it is not the master thread, lock.  This is so ugly.  FIXME
   * pthread_equals returns non-zero if two ids are equal.
   */
  if (!cs->background || pthread_equal(pthread_self(), master.tid) == 0)
    pthread_mutex_lock(&cs->lock);
}

static inline void
cset_unlock(struct cset *cs)
{
  if (!cs->background || pthread_equal(pthread_self(), master.tid) == 0)
    pthread_mutex_unlock(&cs->lock);
}

static void
cset_reset_snapshot_queue(struct cset *cs)
{
  cs->snapshot.on = false;
  cs->snapshot.abort = false;
  cs->snapshot.it = NULL;
  cs->snapshot.head = cs->snapshot.queue;
  cs->snapshot.tail = cs->snapshot.queue;
  cs->snapshot.last = cs->snapshot.queue;
  cs->snapshot.list_idx = 0;
}

static int
cset_item_size_from_type(uint8_t type, void *it_or_elem)
{
  int size;
  if (type < CSI_UNLINK_NOREF)
    size = cset_item_size_fixed[type];
  else if (type == CSI_UNLINK_NOREF) {
    hash_item *it = it_or_elem;
    assert(it_or_elem != NULL);
    /* nkey + 4, rounded up to the nearest multiple of 4 */
    size = ROUNDUP_4(sizeof(struct cset_item_unlink_noref) + it->nkey);
  }
  else if (type == CSI_BTREE_UNLINK_NOREF) {
    btree_elem_item *elem = it_or_elem;
    uint8_t nbkey;
    assert(elem != NULL);
    nbkey = real_nbkey(elem->nbkey);
    size = ROUNDUP_4(sizeof(struct cset_item_btree_unlink_noref) + nbkey);
  }
  else {
    /* CSI_POINTER is special cased.  The caller cannot use this function. */
    assert(0);
  }
  return size;
}

inline static int
cset_item_size_from_csi(struct cset_item *csi)
{
  int size;
  if (csi->type < CSI_UNLINK_NOREF)
    size = cset_item_size_fixed[csi->type];
  else if (csi->type == CSI_UNLINK_NOREF) {
    size = sizeof(struct cset_item_unlink_noref) +
      ((struct cset_item_unlink_noref*)csi)->nkey;
    size = ROUNDUP_4(size);
  }
  else if (csi->type == CSI_BTREE_UNLINK_NOREF) {
    uint8_t nbkey = ((struct cset_item_btree_unlink_noref*)csi)->nbkey;
    nbkey = real_nbkey(nbkey);
    size = ROUNDUP_4(sizeof(struct cset_item_btree_unlink_noref) + nbkey);
  }
  else /* CSI_POINTER */ {
    size = ((struct cset_item_pointer*)csi)->size;
  }
  return size;
}

static int
cset_init(void)
{
  struct cset *cs = &master.cs;
  int size;

  cs->count = 0;
  cs->latest_seq = 1; /* 0 is reserved */
  cs->oldest_seq = 1;
  cs->completed_seq = cs->latest_seq;

  /* cset circular buffer */
  size = config.cset_buffer_size;
  cs->mem.start = malloc(size);
  cs->mem.end = (char*)cs->mem.start + size;
  cs->mem.size = size;
  cs->mem.free = size;
  cs->mem.prod = cs->mem.start;

  cs->next_item = (void*)cs->mem.start;
  pthread_mutex_init(&cs->lock, NULL);

  size = config.cset_snapshot_queue_size;
  cs->snapshot.it = NULL;
  cs->snapshot.size = size;
  cs->snapshot.queue = malloc(size);
  cset_reset_snapshot_queue(cs);
  cs->overflow = false;

  cs->slab_bytes = 0;
  if (config.cset_max_slab_memory == 0)
    cs->max_slab_bytes = 0; /* No limit */
  else {
    cs->max_slab_bytes = engine->config.maxbytes;
    cs->max_slab_bytes *= config.cset_max_slab_memory;
    cs->max_slab_bytes /= 100;
    /* Roud up to make it a multiple of MB */
    if ((cs->max_slab_bytes % (1<<20)) != 0)
      cs->max_slab_bytes += ((1<<20) - (cs->max_slab_bytes % (1<<20)));
    /* Minimum 10MB */
    if (cs->max_slab_bytes < 10*(1<<20))
      cs->max_slab_bytes = 10 * (1<<20);
  }
  return 0;
}

static void
cset_start_background(struct cset *cs)
{
  cset_lock(cs);
  master.cs.background = true;
}

static void
cset_end_background(struct cset *cs)
{
  master.cs.background = false;
  cset_unlock(cs);
}

static void
cset_start_snapshot(struct cset *cs, hash_item *it)
{
  uint8_t type;
  if (cs->snapshot.it) {
    ERROR_DIE("Snapshot item already exists. snapshot.it=%p it=%p",
      (void*)cs->snapshot.it, (void*)it);
  }
  cset_reset_snapshot_queue(cs);
  cs->snapshot.it = it;
  /* Till we empty the snapshot queue (cset_move_snapshot_elem) */
  MASTER_HOLD_REFERENCE(it);
  type = (it->iflag & ITEM_IFLAG_COLL);
  cs->snapshot.insert_type = type + 1;
  cs->snapshot.unlink_type = type + 2;

  cs->snapshot.start_usec = getusec();
}

static void
cset_end_snapshot(struct cset *cs, hash_item *it)
{
  /* Statistics */
  uint32_t elapsed;
  coll_meta_info *meta;
  master_stats.coll_snapshot++;
  cs->snapshot.end_usec = getusec();
  elapsed = cs->snapshot.end_usec - cs->snapshot.start_usec;

  meta = (void*)item_get_meta(it);
  if (elapsed > master_stats.snapshot_duration_max) {
    master_stats.snapshot_duration_max = elapsed;
    master_stats.snapshot_ccnt_max = meta->ccnt;
  }
  if (elapsed < master_stats.snapshot_duration_min ||
    master_stats.snapshot_duration_min == 0) {
    /* 0 means never set
     * We +1 to the actual value so it is never 0.
     */
    master_stats.snapshot_duration_min = elapsed + 1;
  }
  /* Running average.  0.9, 0.1.  FIXME */
  master_stats.snapshot_duration_avg =
    master_stats.snapshot_duration_avg * 0.9 + elapsed * 0.1;
  master_stats.snapshot_ccnt_avg = 
    master_stats.snapshot_ccnt_avg * 0.9 + meta->ccnt * 0.1;
  
  /* If there are elements in the queue, move them incrementally to the cset. */
  if (cs->snapshot.tail > cs->snapshot.head) {
    cs->snapshot.on = true;
    cs->snapshot.last = cs->snapshot.tail;
  }
  else {
    /* Otherwise, this collection item is empty.  Release the collection item
     * Should not have started snapshotting in the first place, though.
     */
    /* The cache is already locked */
    rp_release_item(engine, cs->snapshot.it);
    MASTER_RELEASE_REFERENCE();
  }
}

/* The cache is already locked */
static void *
alloc_cset_snapshot_queue(struct cset *cs, int size)
{
  void *b = NULL;

#if USE_TEST_CODE
  /* TEST CODE */
  if (cs->snapshot.on && cs->test_force_snapshot_queue_overflow) {
    size = 1<<24; /* force overflow */
    cs->test_force_snapshot_queue_overflow = false; /* Turn off */
  }
#endif
  /* The caller has locked cset */
  if (cs->snapshot.tail + size < cs->snapshot.queue + cs->snapshot.size) {
    b = cs->snapshot.tail;
    cs->snapshot.tail += size;
  }
  else if (!cs->overflow) {
    print_log("No free space in the snapshot queue. Reset replication.");
    cs->overflow = true;
    master_stats.snapshot_queue_overflow++;
  }
  return b;
}

/* 8MB of circular buffer.  4B units.
 * We only need to keep two sequence numbers.  The most recent, and the oldest.
 * When master_thread pull cset items, we give individual sequence numbers.
 */

static struct cset_item *
alloc_cset_item_raw(struct cset *cs, int size)
{
  struct cset_item *csi = NULL;
  int free;

#if USE_TEST_CODE
  /* TEST CODE.  Force failure in cset_move_snapshot_elem. */
  if (cs->snapshot.on && cs->background &&
    (cs->test_force_snapshot_move_overflow ||
      cs->test_force_snapshot_move_overflow_tail)) {
    if (cs->test_force_snapshot_move_overflow) {
      size = 1<<24; /* force overflow */
      cs->test_force_snapshot_move_overflow = false; /* Turn off */
    }
    else if (cs->snapshot.head > cs->snapshot.last) {
      size = 1<<24; /* force overflow */
      cs->test_force_snapshot_move_overflow_tail = false; /* Turn off */
    }
  }
#endif
  
  if (cs->max_slab_bytes > 0 && cs->slab_bytes > cs->max_slab_bytes &&
    !cs->overflow) {
    print_log("The change set is holding too much slab memory. Reset"
      " replication. slab_memory_bytes=%llu",
      (long long unsigned)cs->slab_bytes);
    cs->overflow = true;
    master_stats.cset_overflow++;
  }
  
  /* +---------------------------------+
   * |   <B>    |    <A>    |   <C>    |
   * +---------------------------------+
   *            ^           ^
   *            |           |
   *           cons        prod
   *
   * <A>: [cons, prod)
   * Allocated cset items appear here.
   *
   * <B>+<C>: [prod, end) + [begin, cons)
   * Free space.
   *
   * We track the amount of free space, instead of consumer pointer.
   */
  if ((free = cs->mem.free) >= size) {
    /* Enough free words */
    if (cs->mem.prod + size > cs->mem.end) {
      /* But the buffer (prod --- prod+size) wraps.  Skip to the start of
       * the buffer, and see if we still have enough free space.
       */
      free -= (cs->mem.end - cs->mem.prod);
      if (free >= size) {
        uint32_t *p;
        cs->mem.free = free;
        /* Mark skipped words as unused */
        p = (uint32_t*)cs->mem.prod;
        while ((char*)p < cs->mem.end)
          *p++ = 0;
        cs->mem.prod = cs->mem.start;
        csi = (void*)cs->mem.prod;
      }
    }
    else {
      csi = (void*)cs->mem.prod;
    }
  }
  if (csi) {
    cs->mem.free -= size;
    cs->mem.prod += size;
    if (cs->mem.prod == cs->mem.end)
      cs->mem.prod = cs->mem.start;
  }
  else if (!cs->overflow) {
    print_log("No free space in the change set memory. Reset replication.");
    cs->overflow = true;
    master_stats.cset_overflow++;
  }
  
  return csi;
}

static inline struct cset_item *
alloc_cset_item_with_size(struct cset *cs, int type, int size)
{
  struct cset_item *csi;
  if ((csi = alloc_cset_item_raw(cs, size))) {
    cs->latest_seq++;
    cs->count++;
    csi->type = type;
    csi->flags = 0;
  }
  return csi;
}

static struct cset_item *
alloc_cset_item(struct cset *cs, int type)
{
  return alloc_cset_item_with_size(cs, type,
    cset_item_size_from_type(type, NULL));
}

static void
release_cset_item(struct cset *cs, struct cset_item *csi)
{
  hash_item *it = NULL;
  
  /* Release item and/or element.  See cset functions below that increment
   * reference counts.
   */
  switch (csi->type) {
    case CSI_BTREE_INSERT:
    case CSI_BTREE_UNLINK:
    {
      struct cset_item_coll_elem *e = (void*)csi;
      it = e->it;
      cs->slab_bytes -= btree_elem_ntotal(e->elem);
      rp_release_btree_elem(engine, e->elem);
      MASTER_RELEASE_REFERENCE();
    }
    break;
    case CSI_SET_INSERT:
    case CSI_SET_UNLINK:
    {
      struct cset_item_coll_elem *e = (void*)csi;
      it = e->it;
      cs->slab_bytes -= set_elem_ntotal(e->elem);
      rp_release_set_elem(engine, e->elem);
      MASTER_RELEASE_REFERENCE();
    }
    break;
    case CSI_LIST_INSERT:
    {
      struct cset_item_list_elem *e = (void*)csi;
      it = e->it;
      cs->slab_bytes -= list_elem_ntotal(e->elem);
      rp_release_list_elem(engine, e->elem);
      MASTER_RELEASE_REFERENCE();
    }
    break;
    case CSI_FLUSH:
    case CSI_JUNKTIME:
    case CSI_UNLINK_NOREF:
    {
      /* No references */
    }
    break;
    default:
    {
      it = ((struct cset_item_hash_item *)csi)->it;
    }
    break;
  }
  if (it) {
    /* We only count simple keys */
    if ((it->iflag & ITEM_IFLAG_COLL) == 0)
      cs->slab_bytes -= rp_item_ntotal(engine, it);
    rp_release_item(engine, it);
    MASTER_RELEASE_REFERENCE();
  }
}

static inline void
zero_cset_item(struct cset *cs, struct cset_item *csi, int size)
{
  uint32_t *p;

  /* An item is always contiguous, and its length is a multiple of 4 bytes.
   * Just write 0s.
   */
  p = (uint32_t*)csi;
  while (size > 0) {
    *p++ = 0;
    size -= 4;
  }      
}

/* This function may remove/zero any cset item in the buffer.  It does not
 * assume anything about the order in which items are freed.
 */
static void
free_cset_item(struct cset *cs, struct cset_item *csi, int net_size)
{
  int size;
  
  size = cset_item_size_from_csi(csi);
  cset_lock(cs);
  /* Decrement background items if necessary */
  if ((csi->flags & CSET_ITEM_FLAGS_BACKGROUND)) {
    cs->bg_items--;
    cs->bg_bytes -= net_size;
    assert(cs->bg_items >= 0 && cs->bg_bytes >= 0);
  }
  cs->mem.free += size;
  zero_cset_item(cs, csi, size);  
  cset_unlock(cs);
}

static struct cset_item *
cset_pull_next_item(struct cset *cs, bool locked)
{
  struct cset_item *csi = NULL;
  uint32_t *p;
  int size;
  if (!locked)
    cset_lock(cs);
  if (cs->count > 0) {
    p = (void*)cs->next_item;
    /* Skip 0-filled words till we hit the next item */
    while (*p == 0) {
      p++;
      if ((void*)p == cs->mem.end)
        p = (void*)cs->mem.start;
    }
    csi = (void*)p;
    /* For background items, we decrement counters in free_cset_item...
     * FIXME
     */
    /*
    if ((csi->flags & CSET_ITEM_FLAGS_BACKGROUND))
      cs->bg_count--;
    */    
    size = cset_item_size_from_csi(csi);
    p = (void*)(((char*)p) + size); /* next */
    if ((void*)p == cs->mem.end)
      p = (void*)cs->mem.start;
    cs->next_item = (void*)p;
    cs->count--;
    cs->oldest_seq++;

    /* Special case CSI_POINTER.  Return the pointed/referenced item.
     * And remove/zero the current CSI_POINTER item.
     */
    if (csi->type == CSI_POINTER) {
      struct cset_item *next_csi = ((struct cset_item_pointer*)csi)->csi;
      zero_cset_item(cs, csi, size);
      csi = next_csi; /* For now, it is CSI_UNLINK_NOREF. */
    }
  }
  if (!locked)
    cset_unlock(cs);
  return csi;
}

/* cset.background must be true.  cset is already locked.
 * The cache is also locked.
 */
void
rp_snapshot_add_elem(hash_item *it, void *elem)
{
  struct cset *cs = &master.cs;
  struct cset_snapshot_elem *se;
  
  if (cs->snapshot.it != it) {
    ERROR_DIE("Unexpected snapshot item. snapshot.it=%p it=%p",
      (void*)cs->snapshot.it, (void*)it);
  }

  se = alloc_cset_snapshot_queue(cs, sizeof(*se));
  if (se) {
    se->elem = elem;
    /* hold onto this till send the element */
    MASTER_HOLD_REFERENCE(elem);
    if ((it->iflag & ITEM_IFLAG_BTREE))
      cs->slab_bytes += btree_elem_ntotal(elem);
    else if ((it->iflag & ITEM_IFLAG_SET))
      cs->slab_bytes += set_elem_ntotal(elem);
    else
      cs->slab_bytes += list_elem_ntotal(elem);
    
    /* So we don't have to touch refcount in cset_move_snapshot_elem. */
    MASTER_HOLD_REFERENCE(it);
  }
  else {
    /* This is fatal error.  The snapshot queue is not big enough to
     * hold all elements of this collection item.  We cannot continue
     * replication.  FIXME.  Stop altogether?  Skip this item?
     */
    ERROR_DIE("No free space in the snapshot queue while building a snapshot.");
  }
}

/* The cache is NOT locked.  So, so not touch refcount as it is not atomic.
 * cset is already locked.  Also, make sure we do NOT try to lock the cache.
 * Otherwise, we may end up with a deadlock.
 * Master thread: lock cset -> lock cache
 * Worker thread: lock cache -> lock cset
 */
static int
cset_move_snapshot_elem(struct cset *cs)
{
  struct cset_item_coll_elem *csi;
  
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("cset_move_snapshot_elem. head=%p[%d] last=%p[%d] tail=%p",
      cs->snapshot.head, (int)(cs->snapshot.tail - cs->snapshot.head),
      cs->snapshot.last, (int)(cs->snapshot.tail - cs->snapshot.last),
      cs->snapshot.tail);
  if (!cs->snapshot.on)
    return -1;

  /* Move one element from the snapshot queue to the cset.
   *
   * rp_snapshot_add_elem incremented both it's and elem's refcount.
   * Don't do it again.
   */
  if (cs->snapshot.head < cs->snapshot.last) {
    struct cset_snapshot_elem *se = (void*)cs->snapshot.head;
    if ((csi = (void*)alloc_cset_item(cs, cs->snapshot.insert_type))) {
      csi->type = cs->snapshot.insert_type;
      csi->it = cs->snapshot.it;
      csi->elem = se->elem;
      /* csi->it->refcount++; */
      if (cs->snapshot.insert_type == CSI_LIST_INSERT)
        ((struct cset_item_list_elem*)csi)->idx = cs->snapshot.list_idx++;
      cs->snapshot.head += sizeof(*se);
      /* Update background counts... */
      if (cs->background)
        cset_add_background(cs, (void*)csi);
      master_stats.snapshot_cset_item++;
    }
    else
      return -1;
  }
  else if (cs->snapshot.head < cs->snapshot.tail) {
    uint8_t type = *((uint8_t*)cs->snapshot.head);
    if ((csi = (void*)alloc_cset_item(cs, type))) {
      csi->type = type;
      csi->it = cs->snapshot.it;
      /* csi->it->refcount++; */

      if (type == CSI_LIST_UNLINK) {
        struct cset_snapshot_elem_list_unlink *se = (void*)cs->snapshot.head;
        struct cset_item_list_elem *le = (void*)csi;
        le->idx = se->idx;
        le->elem = NULL;
        cs->snapshot.head += sizeof(*se);
      }
      else if (type == CSI_LIST_INSERT) {
        struct cset_snapshot_elem_list_insert *se = (void*)cs->snapshot.head;
        struct cset_item_list_elem *le = (void*)csi;
        le->idx = se->idx;
        le->elem = se->elem;
        cs->snapshot.head += sizeof(*se);
      }
      else if (type == CSI_ATTR_EXPTIME || type == CSI_ATTR_EXPTIME_INFO ||
        type == CSI_ATTR_EXPTIME_INFO_BKEY) {
        struct cset_snapshot_attr *sa = (void*)cs->snapshot.head;
        /* These only need cset_item_hash_item.
         * cset_item_coll_elem is a superset of cset_item_hash_item.
         * Type and it are already set above.  So, do nothing.
         */
        cs->snapshot.head += sizeof(*sa);
      }
      else {
        struct cset_snapshot_elem_btree_set *se = (void*)cs->snapshot.head;
        csi->elem = se->elem;
        cs->snapshot.head += sizeof(*se);
      }
      /* cset_{btree,set,list}_elem_{insert,unlink} already incremented elem
       * refcount.  Don't do it again.
       */
      if (cs->background)
        cset_add_background(cs, (void*)csi);
      master_stats.snapshot_queue_cset_item++;
    }
    else
      return -1;
  }
  else
    return -1; /* Something to indicate that we did not enqueue anything */
  return 0;
}

/* Release every element in the snapshot queue.  The cset is already locked.
 * The cache is not be locked.
 */
static void
cset_release_snapshot_queue(struct cset *cs)
{
  hash_item *it = cs->snapshot.it;
  char *head = cs->snapshot.head;
  while (head != cs->snapshot.tail) {
    void *elem;
    if (head < cs->snapshot.last) {
      struct cset_snapshot_elem *se = (void*)head;
      elem = se->elem;
      head += sizeof(*se);
    }
    else {
      uint8_t type = *((uint8_t*)head);
      if (type == CSI_LIST_UNLINK) {
        /* no element */
        head += sizeof(struct cset_snapshot_elem_list_unlink);
        elem = NULL;
      }
      else if (type == CSI_LIST_INSERT) {
        struct cset_snapshot_elem_list_insert *se = (void*)head;
        elem = se->elem;
        head += sizeof(*se);
      }
      else if (type == CSI_ATTR_EXPTIME || type == CSI_ATTR_EXPTIME_INFO ||
        type == CSI_ATTR_EXPTIME_INFO_BKEY) {
        /* no element */
        elem = NULL;
        head += sizeof(struct cset_snapshot_attr);
      }
      else {
        struct cset_snapshot_elem_btree_set *se = (void*)head;
        /* se is the same size as list_insert */
        elem = se->elem;
        head += sizeof(*se);
      }
    }

    /* See cset_move_snapshot_elem.  For every snapshot item, we need to
     * decrement snapshot item.
     */
    lock_cache();
    rp_release_item(engine, it);
    MASTER_RELEASE_REFERENCE();
    if (elem) {
      if ((it->iflag & ITEM_IFLAG_BTREE)) {
        cs->slab_bytes -= btree_elem_ntotal(elem);
        rp_release_btree_elem(engine, elem);
      }
      else if ((it->iflag & ITEM_IFLAG_SET)) {
        cs->slab_bytes -= set_elem_ntotal(elem);
        rp_release_set_elem(engine, elem);
      }
      else if ((it->iflag & ITEM_IFLAG_LIST)) {
        cs->slab_bytes -= list_elem_ntotal(elem);
        rp_release_list_elem(engine, elem);
      }
      MASTER_RELEASE_REFERENCE();
    }
    unlock_cache();
  }
  cs->snapshot.head = head;

  /* finally, release collection item one last time... */
  lock_cache();
  rp_release_item(engine, it);
  unlock_cache();
  MASTER_RELEASE_REFERENCE();
}

static void
cset_empty(void)
{
  struct cset *cs = &master.cs;
  struct cset_item *csi;

  /* The cache is unlocked.  cset is unlocked. */

  /* Free everything from the cset */
  while ((csi = cset_pull_next_item(cs, false))) {
    int net_size = calc_cset_netmsg_size(csi);
    /* FIXME */
    lock_cache();
    release_cset_item(cs, csi);
    free_cset_item(cs, csi, net_size);
    unlock_cache();
  }
  cs->mem.prod = cs->mem.start;
  cs->mem.free = cs->mem.size;
  cs->next_item = (void*)cs->mem.start;
  
  /* Then release every element in the snapshot queue */
  if (cs->snapshot.on)
    cset_release_snapshot_queue(cs);
  cset_reset_snapshot_queue(cs);
  cs->overflow = false;

  cs->slab_bytes = 0;
}

static void
cset_add_background(struct cset *cs, struct cset_item *csi)
{
  csi->flags |= CSET_ITEM_FLAGS_BACKGROUND;
  cs->bg_items++;
  cs->bg_bytes += calc_cset_netmsg_size(csi);
}

/* The cache is already locked */
static void
cset_link(hash_item *it)
{
  struct cset *cs = &master.cs;
  struct cset_item_hash_item *csi;

  cset_lock(cs);
  if ((csi = (void*)alloc_cset_item(cs, CSI_LINK))) {
    csi->it = it;
    /* We only count simple keys */
    if ((it->iflag & ITEM_IFLAG_COLL) == 0)
      cs->slab_bytes += rp_item_ntotal(engine, it);
    MASTER_HOLD_REFERENCE(it); /* till we send <key,value> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_unlink(hash_item *it, int cause)
{
  struct cset *cs = &master.cs;
  struct cset_item_hash_item *csi;
  
  cset_lock(cs);
  /* Remove the snapshot first and then insert the unlink item to the cset */
  if (cs->snapshot.on && cs->snapshot.it == it) {
    cs->snapshot.abort = true; /* copy_hash_table actually cleans up */
  }
  if ((csi = (void*)alloc_cset_item(cs, CSI_UNLINK))) {
    csi->unlink_cause = cause;
    csi->it = it;
    /* We only count simple keys */
    if ((it->iflag & ITEM_IFLAG_COLL) == 0)
      cs->slab_bytes += rp_item_ntotal(engine, it);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

static void
cset_unlink_noref(hash_item *it, int cause)
{
  struct cset *cs = &master.cs;
  struct cset_item_unlink_noref *csi;
  
  cset_lock(cs);
  /* Remove the snapshot first and then insert the unlink item to the cset */
  if (cs->snapshot.on && cs->snapshot.it == it) {
    cs->snapshot.abort = true; /* copy_hash_table actually cleans up */
  }
  if ((csi = (void*)alloc_cset_item_with_size(cs, CSI_UNLINK_NOREF,
        cset_item_size_from_type(CSI_UNLINK_NOREF, it)))) {
    csi->unlink_cause = cause;
    csi->nkey = it->nkey;
    memcpy(csi+1, item_get_key(it), it->nkey);
    if (cs->background)
      cset_add_background(cs, (void*)csi);
    /* No refcount, so no need to increase slab_bytes */
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_btree_elem_insert(hash_item *it, btree_elem_item *elem)
{
  struct cset *cs = &master.cs;
  struct cset_item_coll_elem *csi;
  struct cset_snapshot_elem_btree_set *se;
  
  cset_lock(cs);
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((se = alloc_cset_snapshot_queue(cs, sizeof(*se)))) {
      se->type = CSI_BTREE_INSERT;
      se->elem = elem;
      cs->slab_bytes += btree_elem_ntotal(elem);
      MASTER_HOLD_REFERENCE(elem);
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, CSI_BTREE_INSERT))) {
    csi->it = it;
    csi->elem = elem;
    /* We do not count collection hash_item.  Only count elements. */
    cs->slab_bytes += btree_elem_ntotal(elem);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    MASTER_HOLD_REFERENCE(elem); /* till we send <elem key, value> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_btree_elem_replace(hash_item *it, btree_elem_item *old_elem,
  btree_elem_item *new_elem)
{
  struct cset *cs = &master.cs;
  struct cset_item_coll_elem *csi;
  struct cset_snapshot_elem_btree_set *se;
  
  cset_lock(cs);
  /* slave always uses "replace" so this is same as insertion */
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((se = alloc_cset_snapshot_queue(cs, sizeof(*se)))) {
      se->type = CSI_BTREE_INSERT;
      se->elem = new_elem;
      cs->slab_bytes += btree_elem_ntotal(new_elem);
      MASTER_HOLD_REFERENCE(new_elem);
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, CSI_BTREE_INSERT))) {
    csi->it = it;
    csi->elem = new_elem;
    /* We do not count collection hash_item.  Only count elements. */
    cs->slab_bytes += btree_elem_ntotal(new_elem);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    MASTER_HOLD_REFERENCE(new_elem); /* till we send <elem key, value> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_btree_elem_unlink(hash_item *it, btree_elem_item *elem)
{
  struct cset *cs = &master.cs;
  struct cset_item_coll_elem *csi;
  struct cset_snapshot_elem_btree_set *se;
  
  cset_lock(cs);
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((se = alloc_cset_snapshot_queue(cs, sizeof(*se)))) {
      se->type = CSI_BTREE_UNLINK;
      se->elem = elem;
      cs->slab_bytes += btree_elem_ntotal(elem);
      MASTER_HOLD_REFERENCE(elem);
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, CSI_BTREE_UNLINK))) {
    csi->it = it;
    csi->elem = elem;
    cs->slab_bytes += btree_elem_ntotal(elem);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    MASTER_HOLD_REFERENCE(elem); /* till we send <elem key> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_set_elem_insert(hash_item *it, set_elem_item *elem)
{
  struct cset *cs = &master.cs;
  struct cset_item_coll_elem *csi;
  struct cset_snapshot_elem_btree_set *se;
  
  cset_lock(cs);
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((se = alloc_cset_snapshot_queue(cs, sizeof(*se)))) {
      se->type = CSI_SET_INSERT;
      se->elem = elem;
      cs->slab_bytes += set_elem_ntotal(elem);
      MASTER_HOLD_REFERENCE(elem);
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, CSI_SET_INSERT))) {
    csi->it = it;
    csi->elem = elem;
    cs->slab_bytes += set_elem_ntotal(elem);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    MASTER_HOLD_REFERENCE(elem); /* till we send <elem key, value> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_set_elem_unlink(hash_item *it, set_elem_item *elem)
{
  struct cset *cs = &master.cs;
  struct cset_item_coll_elem *csi;
  struct cset_snapshot_elem_btree_set *se;
  
  cset_lock(cs);
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((se = alloc_cset_snapshot_queue(cs, sizeof(*se)))) {
      se->type = CSI_SET_UNLINK;
      se->elem = elem;
      cs->slab_bytes += set_elem_ntotal(elem);
      MASTER_HOLD_REFERENCE(elem);
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, CSI_SET_UNLINK))) {
    csi->it = it;
    csi->elem = elem;
    cs->slab_bytes += set_elem_ntotal(elem);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    MASTER_HOLD_REFERENCE(elem); /* till we send <elem key> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_list_elem_insert(hash_item *it, list_elem_item *elem, int idx)
{
  struct cset *cs = &master.cs;
  struct cset_item_list_elem *csi;
  struct cset_snapshot_elem_list_insert *se;

  cset_lock(cs);
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((se = alloc_cset_snapshot_queue(cs, sizeof(*se)))) {
      se->type = CSI_LIST_INSERT;
      se->idx = idx;
      se->elem = elem;
      cs->slab_bytes += list_elem_ntotal(elem);
      MASTER_HOLD_REFERENCE(elem);
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, CSI_LIST_INSERT))) {
    csi->it = it;
    csi->elem = elem;
    csi->idx = idx;
    cs->slab_bytes += list_elem_ntotal(elem);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    MASTER_HOLD_REFERENCE(elem); /* till we send <elem key, value> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_list_elem_unlink(hash_item *it, int idx)
{
  struct cset *cs = &master.cs;
  struct cset_item_list_elem *csi;
  struct cset_snapshot_elem_list_unlink *se;

  cset_lock(cs);
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((se = alloc_cset_snapshot_queue(cs, sizeof(*se)))) {
      se->type = CSI_LIST_UNLINK;
      se->idx = idx;
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, CSI_LIST_UNLINK))) {
    csi->it = it;
    csi->idx = idx;
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_lru_update(hash_item *it)
{
  struct cset *cs = &master.cs;
  struct cset_item_hash_item *csi;
  
  cset_lock(cs);
  if ((csi = (void*)alloc_cset_item(cs, CSI_LRU_UPDATE))) {
    csi->it = it;
    if ((it->iflag & ITEM_IFLAG_COLL) == 0)
      cs->slab_bytes += rp_item_ntotal(engine, it);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_flush_cmd(const char *prefix, int nprefix)
{
  struct cset *cs = &master.cs;
  struct cset_item_flush *csi;
  
  cset_lock(cs);
  /* Remove the snapshot first if it is now flushed */
  if (cs->snapshot.on) {
    if (!rp_item_invalid(engine, cs->snapshot.it))
      cs->snapshot.abort = true; /* copy_hash_table actually cleans up */
  }
  if ((csi = (void*)alloc_cset_item(cs, CSI_FLUSH))) {
    memset(csi->prefix, 0, sizeof(csi->prefix));
    /* rp_flush checks nprefix < 255 */
    if (nprefix < 0)
      csi->nprefix = 255;
    else {
      csi->nprefix = (uint8_t)nprefix;
      memcpy(csi->prefix, prefix, nprefix);
    }
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_setattr(hash_item *it, uint8_t type)
{
  struct cset *cs = &master.cs;
  struct cset_item_hash_item *csi;
  struct cset_snapshot_attr *sa;
  
  cset_lock(cs);
  if (cs->snapshot.on && cs->snapshot.it == it) {
    if ((sa = alloc_cset_snapshot_queue(cs, sizeof(*sa)))) {
      sa->type = type;
      /* to avoid incrementing it in cset_move_snapshot_elem */
      MASTER_HOLD_REFERENCE(it);
    }
  }
  else if ((csi = (void*)alloc_cset_item(cs, type))) {
    csi->it = it;
    /* We only count simple keys */
    if ((it->iflag & ITEM_IFLAG_COLL) == 0)
      cs->slab_bytes += rp_item_ntotal(engine, it);
    MASTER_HOLD_REFERENCE(it); /* till we send <key> */
    if (cs->background)
      cset_add_background(cs, (void*)csi);
  }
  cset_unlock(cs);
}

/* The cache is already locked */
static void
cset_set_junktime(void)
{
  /* Just make a note, and do nothing else. */
  struct cset *cs = &master.cs;
  
  cset_lock(cs);
  alloc_cset_item(cs, CSI_JUNKTIME);
  cset_unlock(cs);
}

/* A simple hash table used to look up a small number of keys.
 * A few hundreds at most.
 */
struct key_hash_bucket;
struct key_hash_item {
  TAILQ_ENTRY(key_hash_item) hash_list; /* from hash bucket */
  TAILQ_ENTRY(key_hash_item) all_list; /* free list or use list */
  uint16_t nkey;
  /* key bytes reside in the user provided buffer */
  char *key;
};
TAILQ_HEAD(key_hash_list, key_hash_item);

struct key_hash_bucket {
  uint64_t gencount; /* use gen count to invalidate */
  struct key_hash_list list;
};

#define KEY_HASH_TABLE_BUCKET_COUNT 691
struct key_hash_table {
  uint64_t gencount;
  struct key_hash_bucket buckets[KEY_HASH_TABLE_BUCKET_COUNT];
  int max_keys;
  struct key_hash_list free_list;
  struct key_hash_list used_list;
};

static struct key_hash_table *
key_hash_table_init(int max_keys)
{
  struct key_hash_table *tab;
  struct key_hash_item *k;
  int i, size;

  print_log("key_hash_table_init. max_keys=%d", max_keys);

  size = sizeof(struct key_hash_table) +
    max_keys * sizeof(struct key_hash_item);
  tab = malloc(size);
  if (tab == NULL)
    return NULL;
  memset(tab, 0, size);
  
  for (i = 0; i < KEY_HASH_TABLE_BUCKET_COUNT; i++) {
    struct key_hash_bucket *b = &tab->buckets[i];
    TAILQ_INIT(&b->list);
  }
  tab->max_keys = max_keys;
  TAILQ_INIT(&tab->free_list);
  TAILQ_INIT(&tab->used_list);
  
  k = (struct key_hash_item*)(tab+1);
  for (i = 0; i < tab->max_keys; i++) {
    TAILQ_INSERT_TAIL(&tab->free_list, k, all_list);
    k++;
  }
  return tab;
}

static void
key_hash_table_destroy(struct key_hash_table *tab)
{
  if (tab) {
    /* Wipe the meta data portion to catch segfault early on */
    memset(tab, 0, sizeof(*tab));
    free(tab);
  }
}

static void
key_hash_table_reset(struct key_hash_table *tab)
{
  /* Bump gencount to invalidate all hash buckets and chains.
   * A bucket resets its item list when it see the new gencount.
   */
  tab->gencount++;

   /* Free all items */
  TAILQ_CONCAT(&tab->free_list, &tab->used_list, all_list);
}

static uint32_t
key_hash_table_compute_hash(uint16_t nkey, const char *key)
{
  uint32_t hash = 0;
  /* FIXME.  Use memcached's default? */
  while (nkey > 0) {
    hash += *key;
    key++;
    nkey--;
  }
  return hash;
}

static struct key_hash_item *
key_hash_table_lookup(struct key_hash_table *tab, uint16_t nkey,
  const char *key, uint32_t hash)
{
  struct key_hash_item *k;
  struct key_hash_bucket *b;
  int idx;

  idx = hash % KEY_HASH_TABLE_BUCKET_COUNT;
  b = &tab->buckets[idx];
  if (b->gencount != tab->gencount) {
    b->gencount = tab->gencount;
    TAILQ_INIT(&b->list);
  }
  k = TAILQ_FIRST(&b->list);
  while (k != NULL) {
    if (k->nkey == nkey) {
      if (k->key == key || memcmp(k->key, key, nkey) == 0)
        break;
    }
    k = TAILQ_NEXT(k, hash_list);
  }
  return k;
}

static void
key_hash_table_remove(struct key_hash_table *tab, struct key_hash_item *k,
  uint32_t hash)
{
  struct key_hash_bucket *b;
  int idx;

  idx = hash % KEY_HASH_TABLE_BUCKET_COUNT;
  b = &tab->buckets[idx];
  if (b->gencount != tab->gencount) {
    b->gencount = tab->gencount;
    TAILQ_INIT(&b->list);
  }
  else {
    TAILQ_REMOVE(&b->list, k, hash_list);
    TAILQ_REMOVE(&tab->used_list, k, all_list);
    TAILQ_INSERT_TAIL(&tab->free_list, k, all_list);
  }
}

static bool
key_hash_table_is_full(struct key_hash_table *tab)
{
  return (TAILQ_FIRST(&tab->free_list) == NULL) ? true : false;
}

static int
key_hash_table_insert(struct key_hash_table *tab, uint16_t nkey, char *key,
  uint32_t hash)
{
  struct key_hash_item *k;
  struct key_hash_bucket *b;
  int idx;
  
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("key_hash_table_insert. nkey=%u key=%s", nkey,
      rp_printable_key(key, nkey));
  
  k = TAILQ_FIRST(&tab->free_list);
  if (k == NULL)
    return -1;
  
  idx = hash % KEY_HASH_TABLE_BUCKET_COUNT;
  b = &tab->buckets[idx];
  if (b->gencount != tab->gencount) {
    b->gencount = tab->gencount;
    TAILQ_INIT(&b->list);
  }
  k->nkey = nkey;
  k->key = key;
  TAILQ_INSERT_HEAD(&b->list, k, hash_list);
  TAILQ_REMOVE(&tab->free_list, k, all_list);
  TAILQ_INSERT_TAIL(&tab->used_list, k, all_list);
  return 0;
}

/* Missing keys buffer.  It is one contiguous buffer with pop-head, push-tail
 * operations.  No random deletion or insertion.  The slave ships the whole
 * buffer to the master.
 *
 * FIXME.  Make something better and use it for both storing missing keys
 * and cset?
 */
struct key_buffer {
  int count;
  int head, tail; /* byte offset */
  int free; /* free bytes */
  int bufsize;
  /* char buf[bufsize]; */
};

static void
key_buffer_reset(struct key_buffer *kb)
{
  char *buf = (void*)(kb+1);
  memset(buf, 0, kb->bufsize);
  kb->count = 0;
  kb->head = kb->tail = 0;
  kb->free = kb->bufsize;
}

static struct key_buffer *
key_buffer_init(int bufsize)
{
  struct key_buffer *kb;
  int size;

  /* A multiple of 4 */
  bufsize = ROUNDUP_4(bufsize);
  size = sizeof(struct key_buffer) + bufsize;
  kb = malloc(size);
  if (kb == NULL)
    return NULL;
  kb->bufsize = bufsize;
  key_buffer_reset(kb);
  print_log("key_buffer_init. bufsize=%d", kb->bufsize);
  return kb;
}

static void
key_buffer_destroy(struct key_buffer *kb)
{
  if (kb) {
    memset(kb, 0, sizeof(*kb)); /* Zero everything */
    free(kb);
  }
}

static char *
key_buffer_push_tail(struct key_buffer *kb, uint16_t nkey, const char *key)
{
  char *buf = (void*)(kb+1);
  int size;
  
  /* Use 4B lines */
  size = sizeof(uint16_t) + nkey;
  size = ROUNDUP_4(size);
  if (kb->free >= size) {
    char *k = NULL;
    
    if ((kb->tail + size) > kb->bufsize) {
      /* Not enough space till the end of the buffer.  Skip to the start. */
      if ((kb->free - (kb->bufsize - kb->tail)) >= size) {
        /* Zero everything between tail and end */
        uint32_t *w = (void*)&buf[kb->tail];
        uint32_t *end = (void*)&buf[kb->bufsize];
        while (w < end) /* works even if tail > end */
          *w++ = 0;
        /* New key at the start of the buffer */
        kb->free -= (kb->bufsize - kb->tail) - size;
        k = buf;
        kb->tail = size;
      }
    }
    else {
      k = &buf[kb->tail];
      kb->tail += size;
      kb->free -= size;
    }
    if (k != NULL) {
      *(uint16_t*)k = nkey;
      k += sizeof(uint16_t);
      memcpy(k, key, nkey);
      print_log("key_buffer_push_tail ok. free=%d nkey=%u key=%s",
        kb->free, nkey, rp_printable_key(key, nkey));
      kb->count++;
      return k;
    }
  }
  print_log("key_buffer_push_tail failed. free=%d nkey=%u key=%s",
    kb->free, nkey, rp_printable_key(key, nkey));
  return NULL;
}

static int
key_buffer_peek_head(struct key_buffer *kb, uint16_t *nkey, char **key)
{
  char *buf = (void*)(kb+1);
  char *k = &buf[kb->head];
  uint16_t n = *((uint16_t*)k);
  if (n > 0) {
    *nkey = n;
    *key = k + sizeof(uint16_t);
    return 0;
  }
  return -1;
}

static void
key_buffer_pop_head(struct key_buffer *kb)
{
  /* Advance head.  Wrap if necessary. */
  char *buf = (void*)(kb+1);
  char *k = &buf[kb->head];
  uint16_t n = *((uint16_t*)k);
  if (n > 0) {
    int size = ROUNDUP_4(sizeof(uint16_t) + n);
    uint32_t *w = (void*)k;
    uint32_t *end = (void*)(k + size);
    while (w < end)
      *w++ = 0;
    kb->head += size;
    kb->free += size;
    if (kb->head >= kb->bufsize)
      kb->head = 0;
    kb->count--;
  }
}

static void
dtor_free(void *val)
{
  if (val != NULL)
    free(val);
}

static int
rp_init_tls(void)
{
  int s;
  s = pthread_key_create(&rp_tls.printable_key_buf, dtor_free);
  s |= pthread_key_create(&rp_tls.printable_bkey_buf, dtor_free);
  return (s == 0 ? 0 : -1);
}

int
rp_init(void *engine_ptr)
{
  /* For testing, get basic configuration from environment variables... */
  char *s;

  engine = engine_ptr;
  cache_lock = &engine->cache_lock;

  print_log_init(engine->server.log->get_logger()); /* Use memcached logger */  
  print_log("rp_init");
  memset(&config, 0, sizeof(config));
  mode = RP_MODE_DISABLED;

  if (0 != rp_init_tls()) {
    print_log("Failed to initialize thread local storage");
    return -1;
  }
  
  config.mode = RP_MODE_DISABLED;
  config.loglevel = RP_LOGLEVEL_INFO;
  config.msgchan_loglevel = MSG_CHAN_LOGLEVEL_INFO;
  config.server_loglevel = -1; /* default */
  config.engine_loglevel = -1; /* default */
  config.max_unack = 128; /* at 1msec/command, a total of 60msec */
  config.max_background_items = 200; /* */
  config.max_background_bytes = 64*1024; /* */
  /* 450msec.  It's enough to tolerate one TCP retransmission per direction
   * (200msec to slave, 200msec from slave).  The client's default timeout is
   * 500msec.
   */
  config.max_sync_wait_msec = 450;
  config.replication_hiwat_msec = 450;
  config.replication_lowat_msec = 300;
  config.max_msg_queue_length = 128;
  config.max_recv_queue_length = 200;
  /* Allocate at least BIG (1MB) + SMALL so we can send the biggest item
   * (currently 1MB).
   */
  config.message_body_buffer_size =
    MASTER_MSG_BUFFER_BIG + MASTER_MSG_BUFFER_SMALL;
  config.ignore_eviction_unlink = 1; /* do not copy evictions to the slave */
  config.max_retransmission_burst = 2; /* Magic number. */
  /* copy key names to cset instead of holding reference to hash_item */
  config.unlink_copy_key = 1;
  config.no_sync_mode = 0;
  /* do not copy replace-unlink event to the slave */
  config.ignore_replace_unlink = 1;
  config.notify_io_complete_thread = 1;
  config.cset_buffer_size = MIN_CSET_BUFFER_SIZE;
  config.cset_snapshot_queue_size = MIN_CSET_SNAPSHOT_QUEUE_SIZE;
  /* By default, use up to 20 percent of memory for change set */
  config.cset_max_slab_memory = 20;
  /* Do not report missing keys to the master */
  config.copy_missing_key = 0;
  /* Do not replicate LRU updates in ASYNC mode */
  config.lru_update_in_async = 0;
  /* Slave flushes the cache after handshake */
  config.slave_flush_after_handshake = 1;
  /* ASYNC->SYNC conditions.  These numbers are arbitrary... */
  /* 40 ACKs return in time */
  config.async_to_sync_consecutive_acks = 40;
  /* Timely ACKs for 1000msec */
  config.async_to_sync_min_duration = 1000;
  /* At most 100 cset items pending */
  config.async_to_sync_max_cset_items = 100;
  /* No garbage collection by default */
  config.gc_enable = 0;
  /* gc only if cset slab mem > 80% of max */
  config.gc_thresh_memory = 80;
  /* gc only if we have not received any acks for at least 5 seconds */
  config.gc_thresh_idle_time = 5000;
  /* Free at most 1000 items in one iteration.  After an iteration, the thread
   * is likely to sleep for 1 millisecond.
   */
  config.gc_max_reclaim = 1000;
  /* After finishing gc, slab bytes should be below thresh_recov_memory for 5
   * seconds, before we start considering gc again.
   */
  config.gc_thresh_recov_time = 5000;
  config.gc_thresh_recov_memory = 10; /* 10% of max slab bytes */
  /* GC an item or an element only if we can save at least  */
  config.gc_min_saved_bytes = 2048;
  
  s = engine->config.replication_config_file;
  if (s != NULL) {
    if (0 != config_read(&config, s)) {
      print_log("Failed to read configuration correctly. Aborting.");
      return -1;
    }
  }

  /* Sanity check cset_buffer_size.  Make it a multiple of 4KB.
   * Arbitrary numbers.
   */
  if (config.cset_buffer_size <= MIN_CSET_BUFFER_SIZE)
    config.cset_buffer_size = MIN_CSET_BUFFER_SIZE;
  else if ((config.cset_buffer_size % (4*1024)) != 0) {
    config.cset_buffer_size +=
      ((4*1024) - (config.cset_buffer_size % (4*1024)));
  }

  /* Sanity check cset_snapshot_queue_size.  Make it a multiple of 4KB.
   * Arbitrary numbers...
   */
  if (config.cset_snapshot_queue_size <= MIN_CSET_SNAPSHOT_QUEUE_SIZE)
    config.cset_snapshot_queue_size = MIN_CSET_SNAPSHOT_QUEUE_SIZE;
  else if ((config.cset_snapshot_queue_size % (4*1024)) != 0) {
    config.cset_snapshot_queue_size +=
      ((4*1024) - (config.cset_snapshot_queue_size % (4*1024)));
  }
  
  switch (config.server_loglevel) {
    case RP_LOGLEVEL_DEBUG:
      engine->server.log->set_level(EXTENSION_LOG_DETAIL);
      break;
    case RP_LOGLEVEL_INFO:
      engine->server.log->set_level(EXTENSION_LOG_INFO);
      break;
    case RP_LOGLEVEL_WARN:
    case RP_LOGLEVEL_FATAL:
      engine->server.log->set_level(EXTENSION_LOG_WARNING);
      break;
  }
  switch (config.engine_loglevel) {
    case RP_LOGLEVEL_DEBUG:
      /* Should not sett the field directly like this.. */
      engine->config.verbose = 2;
      break;
    case RP_LOGLEVEL_INFO:
      engine->config.verbose = 1;
      break;
    case RP_LOGLEVEL_WARN:
    case RP_LOGLEVEL_FATAL:
      engine->config.verbose = 0;
      break;
  }
  rp_loglevel = config.loglevel;
  print_log("config.logleve=%d", config.loglevel);
  print_log("config.msgchan_loglevel=%d", config.msgchan_loglevel);
  print_log("config.server_loglevel=%d", config.server_loglevel);
  print_log("config.engine_loglevel=%d", config.engine_loglevel);
  print_log("config.max_unack=%d", config.max_unack);
  print_log("config.max_background_items=%d", config.max_background_items);
  print_log("config.max_background_bytes=%d", config.max_background_bytes);
  print_log("config.max_sync_wait_msec=%d", config.max_sync_wait_msec);
  print_log("config.replication_hiwat_msec=%d", config.replication_hiwat_msec);
  print_log("config.replication_lowat_msec=%d", config.replication_lowat_msec);
  print_log("config.max_msg_queue_length=%d", config.max_msg_queue_length);
  print_log("config.max_recv_queue_length=%d", config.max_recv_queue_length);
  print_log("config.message_body_buffer_size=%d",
    config.message_body_buffer_size);
  print_log("config.ignore_eviction_unlink=%d", config.ignore_eviction_unlink);
  print_log("config.ignore_replace_unlink=%d", config.ignore_replace_unlink);
  print_log("config.max_retransmission_burst=%d",
    config.max_retransmission_burst);
  print_log("config.unlink_copy_key=%d", config.unlink_copy_key);
  print_log("config.no_sync_mode=%d", config.no_sync_mode);
  print_log("config.notify_io_complete_thread=%d",
    config.notify_io_complete_thread);
  print_log("config.cset_buffer_size=%d", config.cset_buffer_size);
  print_log("config.cset_snapshot_queue_size=%d",
    config.cset_snapshot_queue_size);
  print_log("config.cset_max_slab_memory=%d", config.cset_max_slab_memory);
  print_log("config.copy_missing_key=%d", config.copy_missing_key);
  print_log("config.lru_update_in_async=%d", config.lru_update_in_async);
  print_log("config.slave_flush_after_handshake=%d",
    config.slave_flush_after_handshake);
  print_log("config.async_to_sync_consecutive_acks=%d",
    config.async_to_sync_consecutive_acks);
  print_log("config.async_to_sync_min_duration=%d",
    config.async_to_sync_min_duration);
  print_log("config.async_to_sync_max_cset_items=%d",
    config.async_to_sync_max_cset_items);
  print_log("config.gc_enable=%d", config.gc_enable);
  print_log("config.gc_thresh_memory=%d", config.gc_thresh_memory);
  print_log("config.gc_thresh_idle_time=%d", config.gc_thresh_idle_time);
  print_log("config.gc_max_reclaim=%d", config.gc_max_reclaim);
  print_log("config.gc_thresh_recov_memory=%d", config.gc_thresh_recov_memory);
  print_log("config.gc_thresh_recov_time=%d", config.gc_thresh_recov_time);
  print_log("config.gc_min_saved_bytes=%d", config.gc_min_saved_bytes);
  return 0;
}

static int
set_mode(int m, struct sockaddr_in *laddr, struct sockaddr_in *master_addr)
{
  /* The only valid transitions are
   * disabled => master: startup
   * disabled => slave: startup
   * slave => master: failover
   * m => m: no changes
   *
   * We do not change the listen address on the fly.
   */
  print_log("set_mode. current_mode=%s new_mode=%s", mode_string[mode],
    mode_string[m]);
  if (mode == m) {
    print_log("The old mode and the new mode are the same."
      " No actions taken. mode=%s", mode_string[m]);
    return 0;
  }
  if (m == RP_MODE_DISABLED) {
    print_log("The new mode is DISABLED. This is an invalid transition.");
    return -1;
  }
  if (mode == RP_MODE_DISABLED) {
    if (m == RP_MODE_MASTER) {
      print_log("Transition from DISABLED to MASTER.");
      if (0 != master_init(laddr)) {
        print_log("master_init failed.");
        return -1;
      }
      mode = RP_MODE_MASTER; /* now in MASTER mode */
    }
    else if (m == RP_MODE_SLAVE) {
      char buf[INET_ADDRSTRLEN*2];
      const char *ip;
      
      print_log("Transition from DISABLED to SLAVE.");
      if (master_addr == NULL) {
        print_log("The master's address is null.  The slave mode requires"
          " the master's address");
        return -1;
      }
      ip = inet_ntop(AF_INET, (const void*)&master_addr->sin_addr,
        buf, sizeof(buf));
      print_log("master_address=%s:%d", (ip ? ip : "null"),
        ntohs(master_addr->sin_port));
      if (0 != slave_init(laddr, master_addr)) {
        print_log("slave_init failed.");
        return -1;
      }
      mode = RP_MODE_SLAVE; /* now in SLAVE mode */
    }
    else {
      ERROR_DIE("Program error.");
    }
  }
  else if (mode == RP_MODE_SLAVE) {
    if (m == RP_MODE_MASTER) {
      print_log("Failover. Transition from SLAVE to MASTER.");
      /* Stop the slave thread and clean up buffers */
      destroy_slave();

      /* Initialize the master data structures */
      if (0 != master_init(laddr)) {
        print_log("master_init failed.");
        return -1;
      }
      /* Done.  set_slave starts the master thread later on */
      mode = RP_MODE_MASTER;
    }
    else {
      print_log("Invalid mode transition");
      return -1;
    }
  }
  else {
    print_log("Invalid mode transition");
    return -1;
  }
  return 0;
}

uint64_t
rp_seq(void)
{
  uint64_t latest = RP_SEQ_NULL;
  if (mode == RP_MODE_MASTER) {
    /* The cache is locked.  The caller just wants to see if it (the thread)
     * has caused changes.  So do not bother locking.
     * The worker thread eventually calls rp_wait below, which locks
     * the master state and checks if we are really in SYNC mode.
     */
    latest = master.cs.latest_seq;
  }
  return latest;
}

/* Multiple worker threads call this function.  It must be thread safe. */
int
rp_wait(const void *cookie, int orig_ret)
{
  int s;
  
  if (mode != RP_MODE_MASTER)
    return RP_WAIT_COMPLETED;
  
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_wait. seq=%llu", (long long unsigned)master.cs.completed_seq);
  
  s = RP_WAIT_COMPLETED; /* client can proceed right away */
  lock_master();
  
  if (master.state == MASTER_STATE_RUN) {
    struct wait_entry *e;
    uint64_t latest = master.cs.latest_seq;
    
    if (master.cs.completed_seq < latest) {
      e = alloc_wait_entry(&master.w_cache);
      if (e) {
        e->cookie = (void*)cookie;
        e->ret = orig_ret;
        e->seq = latest;
        e->start_msec = master.now_msec;
        TAILQ_INSERT_TAIL(&master.waiters, e, list);
        master.waiter_count++;
        master_stats.block_client++;
        s = RP_WAIT_WOULDBLOCK;
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("rp_wait. enqueue one. entry=%p seq=%llu",
            (void*)e, (long long unsigned)e->seq);
      }
      else {
        print_log("failed to allocated wait queue entry");
        s = RP_WAIT_ERROR;
      }
    }
  }
  
  /* Most likely we have inserted cset items, so wake up the master
   * thread.  In the past, we did "lock->signal->unlock" in alloc_cset_item.
   * But signalling in this function, without the cache locked, improved
   * max throughput quite a bit (100Kpps->120Kpps).
   * As a general rule, do not do anything expensive with the cache locked.
   */
  if (master.state == MASTER_STATE_RUN || MASTER_STATE_ASYNC_RUN) {
    if (master.sleeping) {
      master.sleeping = false;
      pthread_cond_signal(&master.cond);
    }
  }
  
  unlock_master();
  return s;
}

static uint64_t
getmsec(void)
{
  struct timeval tv;
  uint64_t msec;
  
  if (0 != gettimeofday(&tv, NULL)) {
    PERROR_DIE("gettimeofday");
  }
  msec = ((uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec/1000);
  return msec;
}

static void
get_msec_usec(uint64_t *msec, uint64_t *usec)
{
  struct timeval tv;
  
  if (0 != gettimeofday(&tv, NULL)) {
    PERROR_DIE("gettimeofday");
  }
  *msec = ((uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec/1000);
  *usec = ((uint64_t)tv.tv_sec * 1000000 + (uint64_t)tv.tv_usec);
}

static struct wait_entry *
alloc_wait_entry(struct wait_entry_cache *cache)
{
  struct wait_entry *e;
  
  /* pthread_mutex_lock(&cache->lock); */
  e = TAILQ_FIRST(&cache->free_entries);
  if (e != NULL) {
    TAILQ_REMOVE(&cache->free_entries, e, list);
    cache->count--;
  }
  /* pthread_mutex_unlock(&cache->lock); */
  if (e == NULL) {
    /* Potentially, we will need one entry for each concurrent client */
    e = malloc(sizeof(*e));
  }
  return e;
}

static void
free_wait_entry(struct wait_entry_cache *cache, struct wait_entry *e)
{
  /* pthread_mutex_lock(&cache->lock); */
  TAILQ_INSERT_HEAD(&cache->free_entries, e, list);
  cache->count++;
  /* pthread_mutex_unlock(&cache->lock); */
}

static void
master_msg_queue_reset(struct master_msg_queue *mq)
{
  struct master_msg *msg;
  int i;
  
  /* Return all messages to the free list */
  while ((msg = TAILQ_FIRST(&mq->send_list))) {
    TAILQ_REMOVE(&mq->send_list, msg, list);    
    TAILQ_INSERT_TAIL(&mq->free_list, msg, list);
  }
  while ((msg = TAILQ_FIRST(&mq->ackd_list))) {
    TAILQ_REMOVE(&mq->ackd_list, msg, list);
    TAILQ_INSERT_HEAD(&mq->free_list, msg, list);
  }
  
  mq->seq = getmsec();
  mq->count = 0;
  mq->last_sent = NULL;
  mq->last_send_msec = 0;
  mq->bg_items_in_net = 0;
  mq->bg_bytes_in_net = 0;
  
  for (i = 0; i < mq->max_chunks; i++)
    mq->free_chunk_map[i] = 1; /* free */
  
  mq->cset_last_it = NULL;
  mq->cset_msg = NULL;
  mq->cset_free_ptr = NULL;
  mq->cset_free_bytes = 0;
}

static int
master_msg_queue_init(struct master_msg_queue *mq)
{
  struct master_msg *msg;
  char *buf;
  int i, num;

  /* Send queue */
  TAILQ_INIT(&mq->send_list);
  TAILQ_INIT(&mq->ackd_list);

  /* Messages and buffers */
  msg = malloc(config.max_msg_queue_length * sizeof(*msg));
  TAILQ_INIT(&mq->free_list);
  for (i = 0; i < config.max_msg_queue_length; i++) {
    TAILQ_INSERT_TAIL(&mq->free_list, msg, list);
    msg++;
  }
  
  num = config.message_body_buffer_size / MASTER_MSG_BUFFER_SMALL;  
  buf = malloc(num * MASTER_MSG_BUFFER_SMALL + (sizeof(char*) * num));
  mq->contig_buffer = buf;
  mq->max_chunks = num;
  mq->free_chunk_map = buf + (num * MASTER_MSG_BUFFER_SMALL);
  for (i = 0; i < mq->max_chunks; i++)
    mq->free_chunk_map[i] = 1; /* free */

  /* FIXME.  Use a circular buffer, instead of this ridiculous map.
   * Messages are sent and removed in order, so circular buffer is sufficient.
   */
  master_msg_queue_reset(mq);
  return 0;
}

/* For internal use.  The user never calls this directly. */
static void
master_msg_queue_append(struct master_msg_queue *mq, struct master_msg *msg)
{
  struct netmsg_header *nm;
  TAILQ_INSERT_TAIL(&mq->send_list, msg, list);
  mq->count++;
  msg->seq = mq->seq++;
  nm = (struct netmsg_header *)msg->req.hdr.s.user_hdr;
  nm->seq = msg->seq;
  master_stats.queued_messages++;
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
    print_log("master_msg_queue_append. seq=%u count=%d cset_seq=%llu",
      msg->seq, mq->count, (long long unsigned)msg->cset_seq);
  }
  /* Increment background item and byte count, if this message contains some */
  if (msg->bg_items > 0) {
    mq->bg_items_in_net += msg->bg_items;
    mq->bg_bytes_in_net += msg->bg_bytes;
  }
}

/* Allocate one small message.  Small messages only have headers
 * and no bodies.
 */
static void *
master_msg_queue_alloc_small(struct master_msg_queue *mq, uint8_t type)
{
  void *user = NULL;
  struct master_msg *msg = TAILQ_FIRST(&mq->free_list);
  if (msg) {
    struct msg_chan_sendreq_user *req;
    struct netmsg_header *m;
    
    msg->seq = 0;
    msg->in_net = false;
    msg->tx_msec = 0;
    msg->cset_seq = 0;
    msg->num_chunks = 0;
    msg->bg_items = 0;
    msg->bg_bytes = 0;

    req = &msg->req;
    req->remote = master.slave_ch_id;
    req->body = NULL;
    req->body_len = 0;
    req->hdr_len = netmsg_type_size[type];
    
    m = (struct netmsg_header *)msg->req.hdr.s.user_hdr;
    m->master_sid = master.master_sid;
    m->seq = 0;
    m->type = type;
    TAILQ_REMOVE(&mq->free_list, msg, list);

    master_msg_queue_append(mq, msg);
    user = m;
  }
  return user;
}

static struct master_msg *
master_msg_queue_alloc_cset(struct master_msg_queue *mq, int size)
{
  struct master_msg *msg = TAILQ_FIRST(&mq->free_list);
  int num, s;

  master_stats.msg_queue_alloc_cset++;
  if (msg == NULL) {
    if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
      print_log("master_msg_queue_alloc_cset. failed, no messages");
    master_stats.msg_queue_alloc_cset_no_message++;
    return NULL;
  }
  /* How many contiguous chunks do we need? */
  num = (size + MASTER_MSG_BUFFER_SMALL - 1) / MASTER_MSG_BUFFER_SMALL;
  /* FIXME */
  s = 0;
  while (s < mq->max_chunks && mq->free_chunk_map[s] == 0)
    s++;
  if (s < mq->max_chunks) {
    msg->chunk_idx = s;
    msg->num_chunks = num;
    while (num > 0 && s < mq->max_chunks && mq->free_chunk_map[s] == 1) {
      num--;
      s++;
    }
    if (num == 0) { /* have enough contiguous chunks */
      struct msg_chan_sendreq_user *req;
      struct master_netmsg_cset *m;
      uint32_t offset;
      
      s = msg->chunk_idx;
      num = msg->num_chunks;
      offset = s * MASTER_MSG_BUFFER_SMALL;
      while (num > 0) {
        mq->free_chunk_map[s++] = 0;
        num--;
      }

      mq->cset_free_ptr = mq->contig_buffer + offset;
      mq->cset_free_bytes = msg->num_chunks * MASTER_MSG_BUFFER_SMALL;
      mq->cset_msg = msg;
      mq->cset_last_it = NULL;

      msg->seq = 0;
      msg->in_net = false;
      msg->tx_msec = 0;
      msg->cset_seq = 0; /* filled in later */
      msg->bg_items = 0;
      msg->bg_bytes = 0;
      
      req = &msg->req;
      req->remote = master.slave_ch_id;
      req->body = mq->cset_free_ptr;
      req->body_len = 0; /* filled in later */
      req->hdr_len = sizeof(*m);

      m = (struct master_netmsg_cset *)msg->req.hdr.s.user_hdr;
      m->common.master_sid = master.master_sid;
      m->common.seq = 0;
      m->common.type = MASTER_NETMSG_TYPE_CSET;
      m->remote_buffer_offset = offset;
      TAILQ_REMOVE(&mq->free_list, msg, list);
      
      /* Don't append to the send list till we finalize the message.
       * See push_cset, flush_cset.
       */
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("master_msg_queue_alloc_cset. ok. size=%d msg=%p ptr=%p"
          " chunks=%d chunk_idx=%d", size, (void*)msg, mq->cset_free_ptr,
          msg->num_chunks, msg->chunk_idx);
      master_stats.msg_queue_alloc_cset_ok++;
      return msg;
    }
  }
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
    int i, count = 0;
    for (i = 0; i < mq->max_chunks; i++) {
      if (mq->free_chunk_map[s] != 0)
        count++;
    }
    print_log("master_msg_queue_alloc_cset. failed, not enough buffers."
      " size=%d free_chunks=%d", size, count);
  }
  master_stats.msg_queue_alloc_cset_no_buffer++;
  return NULL;
}

static int
calc_cset_netmsg_size(struct cset_item *csi)
{
  hash_item *it;
  int size;
  
  switch (csi->type) {
    case CSI_LINK:
    {
      struct cset_item_hash_item *i = (void*)csi;
      it = i->it;
      size = sizeof(struct cset_netmsg_key) + it->nkey
        + sizeof(struct cset_netmsg_item_meta);
      if ((it->iflag & ITEM_IFLAG_BTREE)) {
        size += sizeof(btree_meta_info); /* inferred from type */
      }
      else if ((it->iflag & ITEM_IFLAG_SET)) {
        size += sizeof(set_meta_info); /* inferred from type */
      }
      else if ((it->iflag & ITEM_IFLAG_LIST)) {
        size += sizeof(list_meta_info); /* inferred from type */
      }
      else {
        /* Simple key-value */
        size += sizeof(struct cset_netmsg_val) + it->nbytes;
      }
    }
    break;
    case CSI_UNLINK:
    case CSI_LRU_UPDATE:
    {
      /* Just need the item's key */
      struct cset_item_hash_item *i = (void*)csi;
      size = sizeof(struct cset_netmsg_key) + i->it->nkey;
    }
    break;
    case CSI_BTREE_INSERT:
    {
      struct cset_item_coll_elem *i = (void*)csi;
      btree_elem_item *elem = i->elem;
      it = i->it;
      size = sizeof(struct cset_netmsg_key) + it->nkey
        + sizeof(struct cset_netmsg_btree_elem) /* length */
        + sizeof(btree_elem_item_fixed) /* entire btree elem */
        + elem->neflag + elem->nbytes
        + real_nbkey(elem->nbkey);
    }
    break;
    case CSI_BTREE_UNLINK:
    {
      struct cset_item_coll_elem *i = (void*)csi;
      btree_elem_item *elem = i->elem;
      it = i->it;
      size = sizeof(struct cset_netmsg_key) + it->nkey
        + sizeof(struct cset_netmsg_btree_elem) + real_nbkey(elem->nbkey);
    }
    break;
    case CSI_SET_INSERT:
    case CSI_SET_UNLINK:
    {
      struct cset_item_coll_elem *i = (void*)csi;
      set_elem_item *elem = i->elem;
      it = i->it;
      size = sizeof(struct cset_netmsg_key) + it->nkey
        + sizeof(struct cset_netmsg_set_elem) + elem->nbytes;
    }
    break;
    case CSI_LIST_INSERT:
    {
      struct cset_item_list_elem *i = (void*)csi;
      list_elem_item *elem = i->elem;
      it = i->it;
      size = sizeof(struct cset_netmsg_key) + it->nkey
        + sizeof(struct cset_netmsg_list_elem) + elem->nbytes;
    }
    break;
    case CSI_LIST_UNLINK:
    {
      struct cset_item_list_elem *i = (void*)csi;
      size = sizeof(struct cset_netmsg_key) + i->it->nkey
        + sizeof(struct cset_netmsg_list_elem);
    }
    break;
    case CSI_FLUSH:
    {
      size = sizeof(struct cset_netmsg_flush);
    }
    break;
    case CSI_ATTR_EXPTIME:
    case CSI_ATTR_EXPTIME_INFO:
    case CSI_ATTR_EXPTIME_INFO_BKEY:
    {
      struct cset_item_hash_item *i = (void*)csi;
      it = i->it;
      size = sizeof(struct cset_netmsg_key) + it->nkey;
      if (csi->type == CSI_ATTR_EXPTIME)
        size += sizeof(struct cset_netmsg_exptime);
      else if (csi->type == CSI_ATTR_EXPTIME_INFO)
        size += sizeof(struct cset_netmsg_exptime_info);
      else
        size += sizeof(struct cset_netmsg_exptime_info_bkey);
    }
    break;
    case CSI_JUNKTIME:
    {
      size = sizeof(struct cset_netmsg_junktime);
    }
    break;
    case CSI_UNLINK_NOREF:
    {
      /* CSNM_UNLINK_KEY, the same message that CSI_UNLINK uses. */
      size = sizeof(struct cset_netmsg_key) +
        ((struct cset_item_unlink_noref*)csi)->nkey;
    }
    break;
    case CSI_BTREE_UNLINK_NOREF:
    {
      /* The same netmsg as CSI_BTREE_UNLINK.  CSNM_UNLINK_BTREE_ELEM. */
      struct cset_item_btree_unlink_noref *i = (void*)csi;
      it = i->it;
      size = sizeof(struct cset_netmsg_key) + it->nkey
        + sizeof(struct cset_netmsg_btree_elem) + real_nbkey(i->nbkey);
    }
    break;
    default:
    {
      printf("type=%d\n", csi->type);
      assert(0);
    }
    break;
  }
  return size;
}

static int
fill_cset_netmsg(struct master_msg_queue *mq, char *start,
  struct cset_item *csi)
{
  hash_item *it = ((struct cset_item_hash_item*)csi)->it;
  struct cset_netmsg_key *key;
  char *b = start;
  int i;
  struct {
    const char *src;
    char *dst;
    int len;
  } copy[2], *c;
  c = copy;
  copy[0].len = 0;
  copy[1].len = 0;

  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("fill_cset_netmsg. start=%p csi=%p", start, (void*)csi);
  
  if ((csi->type >= CSI_LIST_INSERT && csi->type <= CSI_BTREE_UNLINK)
      || csi->type == CSI_BTREE_UNLINK_NOREF) {
    /* Copy the key if it's not the last key */
    if (mq->cset_last_it != it) {
      key = (struct cset_netmsg_key*)b;
      b = (char*)(key+1);
      key->type = CSNM_COLL_KEY;
      key->len = it->nkey;
      c->dst = b;
      c->src = item_get_key(it);
      c->len = it->nkey;
      b += it->nkey;
      c++;
      
      mq->cset_last_it = it;
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("COLL_KEY. key=%s",
          rp_printable_key(item_get_key(it), it->nkey));
    }
  }
  
  switch (csi->type) {
    case CSI_LINK:
    {
      struct cset_netmsg_item_meta *meta;
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("CSI_LINK");
      key = (struct cset_netmsg_key*)b;
      meta = (struct cset_netmsg_item_meta*)(key+1);
      b = (char*)(meta+1);
      key->len = it->nkey;
      meta->flags = it->flags;
      meta->exptime = it->exptime;
      /* copy key */
      c->dst = b;
      c->src = item_get_key(it);
      c->len = it->nkey;
      c++;
      b += it->nkey;
      c->dst = b;
      if ((it->iflag & ITEM_IFLAG_COLL)) {
        c->src = item_get_meta(it);
        if ((it->iflag & ITEM_IFLAG_BTREE)) {
          key->type = CSNM_LINK_BTREE_KEY;
          c->len = sizeof(btree_meta_info);
          b += sizeof(btree_meta_info);
        }
        else if ((it->iflag & ITEM_IFLAG_SET)) {
          key->type = CSNM_LINK_SET_KEY;
          c->len = sizeof(set_meta_info);
          b += sizeof(set_meta_info);
        }
        else if ((it->iflag & ITEM_IFLAG_LIST)) {
          key->type = CSNM_LINK_LIST_KEY;
          c->len = sizeof(list_meta_info);
          b += sizeof(list_meta_info);
        }
      }
      else {
        struct cset_netmsg_val *val = (void*)b;
        key->type = CSNM_LINK_SIMPLE_KEY;
        val->cas = item_get_cas(it);
        val->len = it->nbytes;
        c->len = it->nbytes;
        c->dst = (char*)(val+1);
        c->src = item_get_data(it);
        b += sizeof(*val) + it->nbytes;
        //print_log("LINK_SIMPLE_KEY. key=%s nbytes=%d",
        //  rp_printable_key(item_get_key(it), it->nkey), it->nbytes);
      }
    }
    break;
    case CSI_UNLINK:
    case CSI_LRU_UPDATE:
    case CSI_UNLINK_NOREF:
    {
      key = (struct cset_netmsg_key*)b;
      if (csi->type == CSI_UNLINK || csi->type == CSI_UNLINK_NOREF) {
        int cause = ((struct cset_item_hash_item*)csi)->unlink_cause;
        if (cause == RP_ITEM_UNLINKED_CAUSE_NORMAL ||
          cause == RP_ITEM_UNLINKED_CAUSE_REPLACE)
          key->type = CSNM_UNLINK_KEY;
        else if (cause == RP_ITEM_UNLINKED_CAUSE_EVICT)
          key->type = CSNM_UNLINK_EVICT_KEY;
        else if (cause == RP_ITEM_UNLINKED_CAUSE_EVICT_DELETE_NOW)
          key->type = CSNM_UNLINK_EVICT_DELETE_NOW_KEY;
        else {
          ERROR_DIE("Unexpected CSI_UNLINK cause. cause=%d", cause);
        }
      }
      else
        key->type = CSNM_LRU_UPDATE_KEY;
      
      b = (char*)(key+1);
      /* copy key */
      c->dst = b;
      if (csi->type == CSI_UNLINK_NOREF) {
        struct cset_item_unlink_noref *noref = (void*)csi;
        key->len = noref->nkey;
        c->src = (void*)(noref+1);
      }
      else {
        key->len = it->nkey;
        c->src = item_get_key(it);
      }
      c->len = key->len;
      b += key->len;
    }
    break;
    case CSI_BTREE_INSERT:
    {
      btree_elem_item *elem = ((struct cset_item_coll_elem*)csi)->elem;
      struct cset_netmsg_btree_elem *e = (void*)b;
      e->type = CSNM_LINK_BTREE_ELEM;
      e->nbkey = elem->nbkey;
      b = (char*)(e+1);
      c->dst = b;
      c->src = (const char*)elem;
      c->len = sizeof(btree_elem_item_fixed)
        + real_nbkey(elem->nbkey) + elem->neflag + elem->nbytes;
      e->len = c->len;
      b += c->len;
    }
    break;
    case CSI_BTREE_UNLINK:
    {
      btree_elem_item *elem = ((struct cset_item_coll_elem*)csi)->elem;
      struct cset_netmsg_btree_elem *e = (void*)b;
      e->type = CSNM_UNLINK_BTREE_ELEM;
      e->nbkey = elem->nbkey;
      b = (char*)(e+1);
      c->dst = b;
      c->src = (const char*)(elem->data);
      c->len = real_nbkey(elem->nbkey);
      e->len = c->len;
      b += c->len;
    }
    break;    
    case CSI_BTREE_UNLINK_NOREF:
    {
      struct cset_item_btree_unlink_noref *noref = (void*)csi;
      struct cset_netmsg_btree_elem *e = (void*)b;
      e->type = CSNM_UNLINK_BTREE_ELEM;
      e->nbkey = noref->nbkey;
      b = (char*)(e+1);
      c->dst = b;
      c->src = (const char*)(noref+1);
      c->len = real_nbkey(noref->nbkey);
      e->len = c->len;
      b += c->len;
    }
    break;    
    case CSI_SET_INSERT:
    case CSI_SET_UNLINK:
    {
      set_elem_item *elem = ((struct cset_item_coll_elem*)csi)->elem;
      struct cset_netmsg_set_elem *e = (void*)b;
      e->type = (csi->type == CSI_SET_INSERT ?
        CSNM_LINK_SET_ELEM : CSNM_UNLINK_SET_ELEM);
      e->val_len = elem->nbytes;      
      b = (char*)(e+1);
      c->dst = b;
      c->src = elem->value;
      c->len = elem->nbytes;
      b += c->len;
    }
    break;
    case CSI_LIST_INSERT:
    case CSI_LIST_UNLINK:
    {
      list_elem_item *elem = ((struct cset_item_list_elem*)csi)->elem;
      struct cset_netmsg_list_elem *e = (void*)b;
      e->idx = ((struct cset_item_list_elem*)csi)->idx;
      b = (char*)(e+1);
      if (csi->type == CSI_LIST_INSERT) {
        e->type = CSNM_LINK_LIST_ELEM;
        e->val_len = elem->nbytes;
        c->dst = b;
        c->src = elem->value;
        c->len = elem->nbytes;
        b += c->len;
      }
      else {
        e->type = CSNM_UNLINK_LIST_ELEM;
        e->val_len = 0;
      }
    }
    break;
    case CSI_FLUSH:
    {
      struct cset_item_flush *csi_fl = (void*)csi;
      struct cset_netmsg_flush *fl = (void*)b;
      fl->type = CSNM_FLUSH;
      fl->nprefix = csi_fl->nprefix;
      c->dst = fl->prefix;
      c->src = csi_fl->prefix;
      c->len = sizeof(fl->prefix);
      b += sizeof(*fl);
    }
    break;
    case CSI_ATTR_EXPTIME:
    {
      struct cset_netmsg_exptime *e;
      
      key = (struct cset_netmsg_key*)b;
      key->type = CSNM_ATTR_EXPTIME;
      key->len = it->nkey;
      e = (void*)(key+1);
      e->exptime = it->exptime;
      b = (char*)(e+1);
      /* copy key */
      c->dst = b;
      c->src = item_get_key(it);
      c->len = it->nkey;
      c++;
      b += it->nkey;
    }
    break;
    case CSI_ATTR_EXPTIME_INFO:
    case CSI_ATTR_EXPTIME_INFO_BKEY:
    {
      struct cset_netmsg_exptime_info *e = (void*)b;
      coll_meta_info *info = (void*)item_get_meta(it);
      e->exptime = it->exptime;
      e->ovflact = info->ovflact;
      e->mflags = info->mflags;
      e->mcnt = info->mcnt;
      if (csi->type == CSI_ATTR_EXPTIME_INFO) {
        e->type =  CSNM_ATTR_EXPTIME_INFO;
        b += sizeof(*e);
      }
      else {
        struct cset_netmsg_exptime_info_bkey *bk = (void*)b;
        bk->type = CSNM_ATTR_EXPTIME_INFO_BKEY;
        /* struct copy */
        bk->maxbkeyrange = ((btree_meta_info*)info)->maxbkeyrange;
        b += sizeof(*bk);
      }
    }
    break;
    case CSI_JUNKTIME:
    {
      struct cset_netmsg_junktime *j = (void*)b;
      j->type = CSNM_JUNKTIME;
      j->junk_item_time = engine->config.junk_item_time;
      b += sizeof(*j);
    }
    break;
    default:
    {
      ERROR_DIE("Unexpected CSI type. type=%d", csi->type);
    }
    break;
  }

  for (i = 0; i < 2; i++) {
    if (copy[i].len > 0)
      memcpy(copy[i].dst, copy[i].src, copy[i].len);
  }
  
  /* We only use this for collection items, to avoid sending the same key
   * again and again.  It is useful when we copy snapshots.
   * We only consider a streak of element insert/unlink operations...
   */
  if (!((csi->type >= CSI_LIST_INSERT && csi->type <= CSI_BTREE_UNLINK)
      || csi->type == CSI_BTREE_UNLINK_NOREF)
    || csi->type == CSI_LIST_UNLINK) {
    /* Why LIST_UNLINK?  FIXME */
    mq->cset_last_it = NULL;
  }
  return (b - start);
}

static bool
master_msg_queue_push_cset(struct master_msg_queue *mq, struct cset_item *csi,
  uint64_t seq)
{
  struct master_msg *msg;
  int size = calc_cset_netmsg_size(csi);

  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("master_msg_queue_push_cset. csi=%p seq=%llu", (void*)csi,
      (long long unsigned)seq);
  msg = mq->cset_msg;
  if (msg && size > mq->cset_free_bytes) {
    struct msg_chan_sendreq_user *req = &msg->req;
    req->body_len = mq->cset_free_ptr - req->body; /* finalize length */
    master_msg_queue_append(mq, msg);
    mq->cset_msg = NULL;
    mq->cset_last_it = NULL;
    msg = NULL;
  }
  if (msg == NULL) {
    msg = master_msg_queue_alloc_cset(mq, size); /* sets mq->cset_msg */
    if (msg == NULL)
      return false;    
  }
  master_stats.pushed_cset_item++;
  size = fill_cset_netmsg(mq, mq->cset_free_ptr, csi);
  mq->cset_free_ptr += size;
  mq->cset_free_bytes -= size;
  /* Remember the latest sequence number so we can wake up waiters when
   * this message is ack'd.
   */
  if (seq != 0) {
    if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
      print_log("master_msg_queue_push_cset. update cset_seq. new=%llu"
        " old=%llu", (long long unsigned)seq,
        (long long unsigned)mq->cset_msg->cset_seq);
    mq->cset_msg->cset_seq = seq;
  }
  if ((csi->flags & CSET_ITEM_FLAGS_BACKGROUND)) {
    mq->cset_msg->bg_items++;
    mq->cset_msg->bg_bytes += size;
  }
  return true;
}

static void
master_msg_queue_flush_cset(struct master_msg_queue *mq)
{
  if (mq->cset_msg) {
    struct msg_chan_sendreq_user *req = &mq->cset_msg->req;
    req->body_len = mq->cset_free_ptr - req->body; /* finalize length */
    master_msg_queue_append(mq, mq->cset_msg);
    mq->cset_msg = NULL;
    mq->cset_last_it = NULL;
  }
}

static void
master_msg_queue_send(struct master_msg_queue *mq, uint64_t now)
{
  struct master_msg *msg, *first;
  int retrans_count;

  /* Next message to send */
  first = TAILQ_FIRST(&mq->send_list);
  if (mq->last_sent == NULL)
    msg = first;
  else
    msg = TAILQ_NEXT(mq->last_sent, list);

  /* Check if the very first message needs retransmission, and rewind
   * the cursor.
   */
  if (first != NULL && (now >= first->tx_msec + 1000 /* FIXME */))
    msg = first;
  
  /* Send messages from next till the end */
  retrans_count = 0;
  while (msg != NULL && !msg->in_net) {
    if (msg->tx_msec != 0) {
      /* Previously sent.  Retransmit if it's not been ack'd for too long. */
      if (!(now >= msg->tx_msec + 1000 /* FIXME */))
        break; /* No */
      retrans_count++;
      if (retrans_count > config.max_retransmission_burst)
        break;
      master_stats.retransmission++;
      print_log("master_msg_queue: retransmit seq=%llu elapsed=%u"
        " body_size=%u",
        (long long unsigned)msg->seq, (unsigned)(now - msg->tx_msec),
        msg->req.body_len);
    }
    msg->in_net = true;
    msg->tx_msec = now;
    msg->tx_usec = master.now_usec;
    if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
      print_log("master_msg_queue: send. seq=%u hdr_len=%d",
        msg->seq, msg->req.hdr_len);
    if (0 != msg_chan_send(master.msg_chan, &msg->req)) {
      ERROR_DIE("msg_chan_send failed"); /* FIXME */
    }
    mq->last_send_msec = now;
    mq->last_sent = msg;
    msg = TAILQ_NEXT(msg, list);
    master_stats.sent_message++;
  }
  
  /* Rewind if we retransmitted.  We will advance when the oldest message
   * ackd.
   */
  if (retrans_count > 0) {
    mq->last_sent = NULL;
  }
}

static void
master_msg_queue_free_ackd(struct master_msg_queue *mq)
{
  struct master_msg *msg;
  while ((msg = TAILQ_FIRST(&mq->ackd_list)) != NULL && !msg->in_net) {
    TAILQ_REMOVE(&mq->ackd_list, msg, list);
    mq->count--;
    TAILQ_INSERT_HEAD(&mq->free_list, msg, list);
    /* Return the chunks to the free pool */
    while (msg->num_chunks > 0) {
      if (mq->free_chunk_map[msg->chunk_idx] != 0) {
        ERROR_DIE("Corrupt free_chunk_map");
      }
      mq->free_chunk_map[msg->chunk_idx++] = 1;
      msg->num_chunks--;
    }
  }
}

static struct master_msg *
master_msg_queue_receive_ack(struct master_msg_queue *mq, uint32_t seq)
{
  struct master_msg *msg;
  uint32_t elapsed;
  
  msg = TAILQ_FIRST(&mq->send_list);
  if (msg != NULL && seq == msg->seq + 1) {
    if (msg == mq->last_sent)
      mq->last_sent = NULL;
    TAILQ_REMOVE(&mq->send_list, msg, list);    
    TAILQ_INSERT_TAIL(&mq->ackd_list, msg, list);
    master_stats.ackd_message++;
    if (rp_loglevel > RP_LOGLEVEL_DEBUG)
      print_log("master_msg_queue_receive_ack. ackd_msg_seq=%u", msg->seq);

    /* Running average of ACK latencies.  Weight: 0.9 old, 0.1 new.  FIXME. */
    elapsed = master.now_usec - msg->tx_usec;
    master_stats.ack_latency_usec =
      master_stats.ack_latency_usec * 0.9 + elapsed * 0.1;
    
    return msg;
  }
  return NULL;
}

static void
master_msg_queue_sent(struct master_msg *msg)
{
  /* Mark and do nothing else */
  msg->in_net = false;
}

static void
master_enqueue_handshake(void)
{
  struct master_netmsg_handshake *hs;

  hs = master_msg_queue_alloc_small(&master.msg_queue,
    MASTER_NETMSG_TYPE_HANDSHAKE);
  if (hs == NULL) {
    ERROR_DIE("The message queue should be empty, but failed to allocate"
      " a message.");
  }
  hs->master_sid = master.master_sid;
  hs->time = engine->server.core->get_current_time();
  hs->junk_item_time = engine->config.junk_item_time;
  print_log("enqueue HANDSHAKE. master_sid=%u time=%u junk_item_time=%d",
    hs->master_sid, hs->time, hs->junk_item_time);
}

static void
master_enqueue_ping(uint64_t now)
{
  struct master_netmsg_ping *ping;

  ping = master_msg_queue_alloc_small(&master.msg_queue,
    MASTER_NETMSG_TYPE_PING);
  if (ping == NULL) {
    ERROR_DIE("The message queue should be empty, but failed to allocate"
      " a message.");
  }
  ping->now = now;
  master_stats.ping++;
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("enqueue PING. now=%llu", (long long unsigned)ping->now);
}

static void
master_enqueue_graceful_failover(void)
{
  struct master_netmsg_graceful_failover *msg;

  msg = master_msg_queue_alloc_small(&master.msg_queue,
    MASTER_NETMSG_TYPE_GRACEFUL_FAILOVER);
  if (msg == NULL) {
    ERROR_DIE("The message queue should be empty, but failed to allocate"
      " a message.");
  }
}

/* Wake up clients either based on ack'd sequence numbers or the amount of
 * time they have been waiting.
 */
static int
wake_waiters(uint64_t seq, uint64_t now, int elapsed, bool master_locked,
  struct wait_list *removed_waiters)
{
  struct wait_entry *e;
  int count = 0;
  if (!master_locked)
    lock_master();
  while ((e = TAILQ_FIRST(&master.waiters)) != NULL) {
    if (elapsed > 0 && ((int)(now - e->start_msec) > elapsed)) {
      /* This client has waited too long, so wake it up */
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("wake_waiters. threshold=%d elapsed=%d", elapsed,
          (int)(now - e->start_msec));
      master_stats.clients_waited_too_long++;
    }
    else if (elapsed <= 0 && e->seq <= seq) {
      /* This client's sequence has been ack'd, so wake it up */
    }
    else
      break; /* No clients to wake up yet */
    
    TAILQ_REMOVE(&master.waiters, e, list);
    master.waiter_count--;
    TAILQ_INSERT_TAIL(removed_waiters, e, list);
    count++;
  }
  if (!master_locked)
    unlock_master();
  return count;
}

static void
callback_and_free_waiters(struct wait_list *waiters)
{
  struct wait_entry *e, *next;

  e = TAILQ_FIRST(waiters);
  if (e != NULL) {
    /* Do callbacks first */
    while (e != NULL) {
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("wake_waiters. wake one. entry=%p start_msec=%llu seq=%llu",
          (void*)e, (long long unsigned)e->start_msec,
          (long long unsigned)e->seq);
      
      engine->server.core->notify_io_complete(e->cookie, e->ret);      
      e = TAILQ_NEXT(e, list);
      master_stats.wakeup_client++;
    }
    
    /* Then free all entries */
    e = TAILQ_FIRST(waiters);
    lock_master();
    while (e != NULL) {
      next = TAILQ_NEXT(e, list);
      free_wait_entry(&master.w_cache, e);
      e = next;
    }
    unlock_master();
  }
}

static void
pass_waiters_to_notify_thread(struct wait_list *waiters)
{
  if (!TAILQ_EMPTY(waiters)) {
    pthread_mutex_lock(&master.notify_io_complete.lock);
    TAILQ_CONCAT(&master.notify_io_complete.waiters, waiters, list);
    pthread_cond_signal(&master.notify_io_complete.cond);
    pthread_mutex_unlock(&master.notify_io_complete.lock);
  }
}

/* The caller must make sure that neither cache nor master is locked. */
static void
master_start_nack_processing(void)
{
  /* Not clear what we should do in this case.  The admin must deal with this
   * case manually.
   */
  if (master.state == MASTER_STATE_GRACEFUL_FAILOVER) {
    print_log("Cannot process NACK while in GRACEFUL_FAILOVER state. Ignored.");
    return;
  }
  
  /* Stop accepting new commands from client requests.
   * User requests call replication code with the cache lock
   * acquired.  And then they unlock the cache and lock the master to
   * check the state agin.  So grab both the cache lock and the master lock.
   */
  lock_cache();
  lock_master();
  if (master.state != MASTER_STATE_NACK) {
    master.state = MASTER_STATE_NACK;
    master_stats.start_nack++;
  }
  unlock_master();
  unlock_cache();
  /* At this point, we are sure that no further requests enter cset or
   * the waiter list.
   */
}

static void
master_save_missing_keys(void)
{
  struct key_hash_table *ktab;
  struct key_buffer *kbuf;
  uint32_t len;
  char *cur, *end;
  
  len = master.missing_kmsg.keybuf_len;
  print_log("Missing keys from the slave. keybuf_len=%u", len);

  ktab = master.missing_ktab;
  kbuf = master.missing_kbuf;
  cur = master.missing_kmsg_keybuf;
  end = &master.missing_kmsg_keybuf[len];  
  while (cur + sizeof(uint16_t) <= end) {
    /* Probably misaligned access.  It is okay on x86.  It is just
     * slower than aligned access and is not atomic.  So do not worry
     * about alignment.
     */
    uint16_t nkey = *(uint16_t*)cur;
    char *key = cur + sizeof(uint16_t);
    if (cur + sizeof(uint16_t) + nkey <= end) {
      uint32_t hash;
      char *k;
      
      hash = key_hash_table_compute_hash(nkey, key);
      if (NULL == key_hash_table_lookup(ktab, nkey, key, hash)) {
        if (key_hash_table_is_full(ktab)) {
          /* Cannot push any more.  Ignore the keys */
          master_stats.missing_keys_table_full++;
          return;
        }
        k = key_buffer_push_tail(kbuf, nkey, key);
        if (k == NULL) {
          /* Cannot push any more.  Ignore the keys */
          master_stats.missing_keys_buffer_full++;
          return;
        }
        print_log("Save missing key=%s", rp_printable_key(key, nkey));
        key_hash_table_insert(ktab, nkey, k, hash);
        master_stats.missing_keys_insert++;
      }
      cur += sizeof(uint16_t) + nkey;
    }
    else
      break;
  }
}

/* We can push another background cset item into the cset only if we
 * have enough room in terms of the number of items and bytes in the
 * pipeline.  Very ugly right now.  FIXME
 */
static int
can_copy_background(void)
{
  return (config.max_background_items >
    (master.cs.bg_items + master.msg_queue.bg_items_in_net) &&
    config.max_background_bytes >
    (master.cs.bg_bytes + master.msg_queue.bg_bytes_in_net));
}

/* Call notify_io_complete for ACK'd requests.  notify_io_complete
 * locks the worker thread and then adds an event.  But, the worker thread
 * also locks itself when calling complete_nread (complete_update_ascii).
 * At high rates of update requests, this locking forces the master thread to
 * block too often, and too long.  So, call notify_io_complete in a separate
 * thread...
 *
 * FIXME.  This does not help at all.  Client's response time still increases.
 * The overall effect is similar to head-of-line blocking.  When
 * notify_io_complete blocks to wake up thread X, all other ACK'd requests
 * from other threads block as well.
 *
 * The real fix is to avoid LOCK_THREAD when calling complete_nread
 * (in memcached.c).
 */
static void *
notify_io_complete_thread(void *arg)
{
  struct wait_list local_waiters, *master_waiters;
  pthread_mutex_t *lock = &master.notify_io_complete.lock;
  pthread_cond_t *cond = &master.notify_io_complete.cond;
  
  master_waiters = &master.notify_io_complete.waiters;
  
  print_log("notify io complete thread is running");
  
  TAILQ_INIT(&local_waiters);
  while (master.notify_io_complete.running) {
    pthread_mutex_lock(lock);
    if (TAILQ_EMPTY(master_waiters)) {
      pthread_cond_wait(cond, lock);
    }
    /* Take the whole list */
    TAILQ_SWAP(&local_waiters, master_waiters, wait_entry, list);
    pthread_mutex_unlock(lock);

    /* Call notify_io_complete... */
    callback_and_free_waiters(&local_waiters);
    TAILQ_INIT(&local_waiters);
  }
  
  print_log("notify io complete thread has terminated");
  return NULL;
}

static void
gc_start_detect_idle(void)
{
  struct gc_state *gc = &master.gc;
  if (config.gc_enable) {
    gc->state = GC_STATE_DETECT_IDLE;
    gc->idle_last_check_msec = master.now_msec;
    gc->idle_start_msec = master.now_msec;
    gc->idle_oldest_seq = master.cs.oldest_seq;
  }
  else
    gc->state = GC_STATE_INVALID;
}

static void
gc_start(void)
{
  struct gc_state *gc = &master.gc;

  gc->start_count = master.cs.count;
  gc->start_msec = master.now_msec;
  gc->start_oldest_seq = master.cs.oldest_seq;
  gc->start_slab_bytes = master.cs.slab_bytes;
  /* Currently last item in cset.  gc_run scans till this item. */
  cset_lock(&master.cs);
  gc->cset_mem_prod = master.cs.mem.prod;
  gc->cset_next_item = master.cs.next_item;
  cset_unlock(&master.cs);

  /* This stops the master thread from pulling more item from the cset. */
  gc->state = GC_STATE_RUNNING_GC;
  master_stats.gc_count++;
  memset(&gc->stats, 0, sizeof(gc->stats));
  
  print_log("Start GC. idle_msec=%u now_msec=%llu slab_bytes=%llu"
    " oldest_seq=%llu cset_count=%d",
    (unsigned)(gc->start_msec - master.gc.idle_start_msec),
    (long long unsigned)gc->start_msec,
    (long long unsigned)gc->start_slab_bytes,
    (long long unsigned)gc->start_oldest_seq, gc->start_count);
}

static void
gc_detect_idle(void)
{
  uint64_t now = master.now_msec;
  struct gc_state *gc = &master.gc;
  
  /* Check once a second */
  if (now >= gc->idle_last_check_msec + 1000) {
    gc->idle_last_check_msec = now;
    if (gc->idle_oldest_seq == master.cs.oldest_seq) {
      /* Idle.  See if we have crossed the thresholds and should start
       * GC'ing.
       */
      print_log("gc_detect_idle. slab_bytes=%llu gc_thresh_bytes=%llu",
        (long long unsigned)master.cs.slab_bytes,
        (long long unsigned)master.gc.thresh_bytes);
      if ((now - gc->idle_start_msec) >= config.gc_thresh_idle_time
        && master.cs.slab_bytes >= master.gc.thresh_bytes) {
        gc_start();
      }
    }
    else {
      /* Made forward progress.  Not idle. */
      gc->idle_start_msec = now;
      gc->idle_oldest_seq = master.cs.oldest_seq;
    }
  }
}

static void
gc_recovery(void)
{
  uint64_t now = master.now_msec;
  struct gc_state *gc = &master.gc;

  /* Check once a second */
  if (now >= gc->recov_last_check_msec + 1000) {
    print_log("gc_recovery. slab_bytes=%llu gc_thresh_bytes=%llu",
      (long long unsigned)master.cs.slab_bytes,
      (long long unsigned)master.gc.thresh_bytes);
    if (gc->recov_last_slab_bytes < gc->thresh_recov_bytes &&
      master.cs.slab_bytes < gc->thresh_recov_bytes) {
      if ((now - gc->recov_start_msec) >= config.gc_thresh_recov_time) {
        /* Declare we have reached a normal period.  Go back to detection. */
        gc_start_detect_idle();        
      }
    }
    else {
      /* Slab bytes > low threshold.  Reset the start time. */
      gc->recov_start_msec = now;
    }
    gc->recov_last_check_msec = now;
    gc->recov_last_slab_bytes = master.cs.slab_bytes;
  }
}

/* Add CSI_UNLINK_NOREF and replace CSI_LINK with CSI_POINTER.
 * See cset_unlink_noref.  Here we use alloc_cset_item_raw directly because
 * CSI_POINTER + CSI_UNLINK_NOREF are logically one cset item consisting of
 * two discontiguous buffers.
 */
static bool
gc_replace_link_simple_item(struct cset_item_hash_item *link_csi)
{
  struct gc_state *gc = &master.gc;
  struct cset *cs = &master.cs;
  hash_item *it = link_csi->it;
  struct cset_item_unlink_noref *unlink_csi;
  struct cset_item_pointer *ptr_csi;
  int size, link_csi_size;
  
  size = cset_item_size_from_type(CSI_UNLINK_NOREF, it);
  cset_lock(cs);
  unlink_csi = (struct cset_item_unlink_noref*)alloc_cset_item_raw(cs, size);
  if (unlink_csi) {
    /* Do NOT increment latest_seq and count. */

    /* Set up UNLINK_NOREF */
    unlink_csi->type = CSI_UNLINK_NOREF;
    unlink_csi->flags = 0;
    unlink_csi->unlink_cause = RP_ITEM_UNLINKED_CAUSE_NORMAL;
    unlink_csi->nkey = it->nkey;
    memcpy(unlink_csi+1, item_get_key(it), it->nkey);

    /* Adjust background counts */
    if ((link_csi->flags & CSET_ITEM_FLAGS_BACKGROUND)) {
      unlink_csi->flags |= CSET_ITEM_FLAGS_BACKGROUND;
      cs->bg_bytes -= calc_cset_netmsg_size((struct cset_item*)link_csi);
      cs->bg_bytes += calc_cset_netmsg_size((struct cset_item*)unlink_csi);
    }

    /* Adjust slab_bytes while holding the cset lock */
    cs->slab_bytes -= rp_item_ntotal(engine, it);
    
    /* Replace CSI_LINK with CSI_POINTER */
    link_csi_size = cset_item_size_from_csi((struct cset_item*)link_csi);
    zero_cset_item(cs, (struct cset_item*)link_csi, link_csi_size);
    ptr_csi = (struct cset_item_pointer*)link_csi;
    ptr_csi->type = CSI_POINTER;
    ptr_csi->flags = 0;
    ptr_csi->size = link_csi_size;
    ptr_csi->csi = (struct cset_item*)unlink_csi;

    /* Update GC stats */
    gc->stats.item_reclaimed++;
    gc->stats.item_released_bytes += it->nbytes;
    gc->stats.cset_bytes += size;
  }
  cset_unlock(cs);

  if (unlink_csi) {
    /* Lock and release the item */
    lock_cache();
    rp_release_item(engine, it);
    unlock_cache();
    MASTER_RELEASE_REFERENCE();
    return true;
  }
  return false;
}

static bool
gc_replace_link_btree_elem(struct cset_item_coll_elem *link_csi)
{
  struct gc_state *gc = &master.gc;
  struct cset *cs = &master.cs;
  hash_item *it = link_csi->it;
  btree_elem_item *elem = link_csi->elem;
  struct cset_item_btree_unlink_noref *unlink_csi;
  struct cset_item_pointer *ptr_csi;
  int size, link_csi_size;
  uint8_t nbkey;
  
  size = cset_item_size_from_type(CSI_BTREE_UNLINK_NOREF, elem);
  cset_lock(cs);
  unlink_csi = (struct cset_item_btree_unlink_noref*)
    alloc_cset_item_raw(cs, size);
  if (unlink_csi) {
    /* Do NOT increment latest_seq and count. */

    /* Set up BTREE_UNLINK_NOREF */
    nbkey = real_nbkey(elem->nbkey);
    unlink_csi->type = CSI_BTREE_UNLINK_NOREF;
    unlink_csi->flags = 0;
    unlink_csi->unused[0] = 0;
    unlink_csi->unused[1] = 0;
    unlink_csi->it = it;
    unlink_csi->nbkey = elem->nbkey; /* length before translation... */
    memcpy(unlink_csi+1, elem->data, nbkey);

    /* Adjust background counts */
    if ((link_csi->flags & CSET_ITEM_FLAGS_BACKGROUND)) {
      unlink_csi->flags |= CSET_ITEM_FLAGS_BACKGROUND;
      cs->bg_bytes -= calc_cset_netmsg_size((struct cset_item*)link_csi);
      cs->bg_bytes += calc_cset_netmsg_size((struct cset_item*)unlink_csi);
    }

    /* Adjust slab_bytes while holding the cset lock */
    cs->slab_bytes -= btree_elem_ntotal(elem);

    /* Replace CSI_BTREE_INSERT with CSI_POINTER */
    link_csi_size = cset_item_size_from_csi((struct cset_item*)link_csi);
    zero_cset_item(cs, (struct cset_item*)link_csi,
      cset_item_size_from_csi((struct cset_item*)link_csi));
    ptr_csi = (struct cset_item_pointer*)link_csi;
    ptr_csi->type = CSI_POINTER;
    ptr_csi->flags = 0;
    ptr_csi->size = link_csi_size;
    ptr_csi->csi = (struct cset_item*)unlink_csi;

    /* Update GC stats */
    gc->stats.btree_elem_reclaimed++;
    gc->stats.btree_elem_released_bytes += elem->nbytes;
    gc->stats.cset_bytes += size;
  }
  cset_unlock(cs);

  if (unlink_csi) {
    /* Lock and release the element.  We did not copy the btree item's
     * key.  So its reference count is not affected.
     */
    lock_cache();
    rp_release_btree_elem(engine, elem);
    unlock_cache();
    MASTER_RELEASE_REFERENCE();
    return true;
  }
  return false;
}

static void
gc_run(void)
{
  struct gc_state *gc = &master.gc;
  struct cset *cs = &master.cs;
  uint32_t *p;
  void *prod;
  bool fin = false;
  int reclaimed = 0;
  
  /* Scan and do GC */
  prod = gc->cset_mem_prod;
  
  while ((p = (void*)gc->cset_next_item) != prod) {
    struct cset_item *csi = NULL;
    int size;
    
    /* Find the next item. Code duplication with cset_pull_next_item... */
    
    /* Skip 0-filled words till we hit the next item or the limit (prod). */
    while (*p == 0 && (void*)p != prod) {
      p++;
      if ((void*)p == cs->mem.end)
        p = (void*)cs->mem.start;
    }
    if ((void*)p == prod) {
      /* Reached the limit.  Stop GC. */
      fin = true;
      break;
    }
    csi = (void*)p;
    size = cset_item_size_from_csi(csi);
    p = (void*)(((char*)p) + size); /* next */
    if ((void*)p == cs->mem.end)
      p = (void*)cs->mem.start;
    gc->cset_next_item = (void*)p;

    gc->stats.scan++;
    
    /* Only care about LINK items and btree element insertions.
     *
     * Why not check for set and list elements?  To delete a set element,
     * we need to copy the element's value.  So there is no memory saving.
     * To delete a list element, we would need an index number.  But we
     * have no way of figuring it out here quickly.  Besides, btree is
     * the most commonly used anyway.
     */
    if (csi->type == CSI_LINK) {
      hash_item *it = ((struct cset_item_hash_item*)csi)->it;
      if (it->refcount == 1 /* cset is the only reference holder*/  &&
        /* Simple item, unlinked from the hash table */
        ((it->iflag & (ITEM_LINKED | ITEM_IFLAG_COLL)) == 0)) {

        /* No need to consider collection items because they only hold
         * the key string and some meta data.  Besides we do not count
         * these bytes towards slab_bytes.
         */
        gc->stats.candidate_item++;
        gc->stats.candidate_item_bytes += it->nbytes;

        /* Do not replace unless we actually save memory.  We copy the key
         * string to cset and release the value.  So, compare the value size
         * and the key length.
         */
        if (it->nbytes >= it->nkey + config.gc_min_saved_bytes) {
          if (!gc_replace_link_simple_item((struct cset_item_hash_item*)csi)) {
            /* Probably out of cset space.  Give up. */
            print_log("Failed to garbage collect CSI_LINK item. Probably"
              " out of cset buffer space.");
            fin = true;
            break;
          }
          reclaimed++;
        }
      }
    }
    else if (csi->type == CSI_BTREE_INSERT) {
      btree_elem_item *elem = ((struct cset_item_coll_elem*)csi)->elem;
      if (elem->refcount == 1 /* cset is the only reference holder */ &&
        elem->status == BTREE_ITEM_STATUS_UNLINK /* unlinked from item */) {
        
        gc->stats.candidate_btree_elem++;
        gc->stats.candidate_btree_elem_bytes += elem->nbytes;
        
        /* If the collection item itself is unlinked, can we just nullify
         * this cset item?  Actually, no because we do not know for sure
         * that we have an unlink cset item for that btree item.
         */
        
        /* Do not replace unless we actually save memory. */
        if (elem->nbytes >= MAX_BKEY_LENG + config.gc_min_saved_bytes) {
          if (!gc_replace_link_btree_elem((struct cset_item_coll_elem*)csi)) {
            print_log("Failed to garbage collect CSI_BTREE_INSERT item."
              " Probably out of cset buffer space.");
            fin = true;
            break;
          }
          reclaimed++;
        }
      }
    }

    /* Do not lock the cache too frequently for too long.
     * config.gc_max_reclaim
     */
    if (reclaimed >= config.gc_max_reclaim)
      break;

    /* CSI_POINTER and cset sequence numbers */
  }

  /* If not explicitly requested, see if we have reached the end of cset. */
  fin = fin || ((void*)gc->cset_next_item == gc->cset_mem_prod);
  if (!fin)
    return; /* Not finished, run again later */
  
  /* Finished */
  print_log("GC finished. elapsed_msec=%llu slab_bytes=%llu->%llu"
    " count=%d->%d oldest_seq=%llu->%llu"
    " scan=%llu candidate_simple_item=%llu candidate_simple_item_bytes=%llu"
    " simple_item_reclaimed=%llu simple_item_released_bytes=%llu"
    " added_cset_bytes=%llu"
    " candidate_btree_elem=%llu candidate_btree_elem_bytes=%llu"
    " btree_elem_reclaimed=%llu btree_elem_released_bytes=%llu",
    (long long unsigned)(master.now_msec - gc->start_msec),
    (long long unsigned)gc->start_slab_bytes,
    (long long unsigned)master.cs.slab_bytes,
    gc->start_count, master.cs.count,
    (long long unsigned)gc->start_oldest_seq,
    (long long unsigned)master.cs.oldest_seq,
    (long long unsigned)gc->stats.scan,
    (long long unsigned)gc->stats.candidate_item,
    (long long unsigned)gc->stats.candidate_item_bytes,
    (long long unsigned)gc->stats.item_reclaimed,
    (long long unsigned)gc->stats.item_released_bytes,
    (long long unsigned)gc->stats.cset_bytes,
    (long long unsigned)gc->stats.candidate_btree_elem,
    (long long unsigned)gc->stats.candidate_btree_elem_bytes,
    (long long unsigned)gc->stats.btree_elem_reclaimed,
    (long long unsigned)gc->stats.btree_elem_released_bytes);
  
  gc->state = GC_STATE_RECOVERY;
  gc->recov_last_check_msec = master.now_msec;
  gc->recov_last_slab_bytes = master.cs.slab_bytes;
  gc->recov_start_msec = master.now_msec;
}

/* now + msec in timespec format */
static inline void
get_timespec(struct timespec *ts, int msec)
{
  uint64_t now = getmsec();
  now += msec;
  ts->tv_sec = now / 1000;
  ts->tv_nsec = (now % 1000) * 1000000;
}

static void
master_handle_slave_handshake(struct slave_netmsg_handshake *slave_hs,
  struct master_netmsg_handshake *master_hs)
{
  print_log("Receive HANDSHAKE from the slave. master_sid=%u"
    " slave_sid=%u", slave_hs->master_sid, slave_hs->slave_sid);
  if (master.master_sid != slave_hs->master_sid) {
    /* The slave is broken?  FIXME */
    print_log("Unexpected master_sid in HANDSHAKE. received=%u"
      " expected=%u", slave_hs->master_sid, master.master_sid);
    master.state = MASTER_STATE_HANDSHAKE_START; /* try again... */
  }
  else {
    rel_time_t master_time = engine->server.core->get_current_time();
    
    if (master.slave_sid == 0) {
      /* The first time we are handshaking */
      print_log("Completed the first-time HANDSHAKE. slave_sid=%u",
        slave_hs->slave_sid);
    }
    else if (master.slave_sid != slave_hs->slave_sid) {
      /* The slave has restarted */
      print_log("The slave sid has changed. Assume the slave has"
        " restarted. previous_sid=%u current=%u",
        master.slave_sid, slave_hs->slave_sid);
      /* We must have gone through NACK. */
    }
            
    /* Handshaking took too long?  Do it again because the timestamp
     * in the message is stale.
     */
    if (master_time > master_hs->time + 1) {
      /* more than 1 seconds have elapsed */
      print_log("Handshaking took too long. Try again. elapsed=%u",
        master_time - (master_hs->time + 1));
      master.state = MASTER_STATE_HANDSHAKE_START;
    }
    else {
      /* Get the hash table cursor first. */
      master.slave_sid = slave_hs->slave_sid;
      master.state = MASTER_STATE_START_CURSOR;
      print_log("Transition to START_CURSOR");
    }
  }
}

/* master_thread calls this function. */
static void
master_graceful_failover(void)
{
  if (master.graceful_failover.state == GRACEFUL_FAILOVER_STATE_START) {
    master.graceful_failover.check_next_msec = master.now_msec;
    master.graceful_failover.state = GRACEFUL_FAILOVER_STATE_PREPARE;
  }

  /* See if we can commit to failover */
  if (master.graceful_failover.state == GRACEFUL_FAILOVER_STATE_PREPARE) {
    bool running;

    /* Do not check too often.  Once per second now. */
    if (master.graceful_failover.check_next_msec > master.now_msec)
      return;

    /* If we use SYNC mode, we should be in SYNC mode, not ASYNC.  If SYNC
     * is disabled, then ASYNC is okay.  Other than this check, the admin
     * should first check that replication is progressing in timely fashion.
     */
    if (!((master.state == MASTER_STATE_RUN) ||
        (config.no_sync_mode && master.state == MASTER_STATE_ASYNC_RUN))) {
      print_log("Graceful failover: The master is not in SYNC/ASYNC state."
        " Will check again in 1 second.");
      goto check_again;
    }
    if (master.cursor_valid || master.cs.snapshot.on) {
      print_log("Graceful failover: Background copy is still in progress."
        " Will check again in 1 second.");
      goto check_again;
    }
    
    /* See if cset is reasonably empty.  We want to drain the cset in 100msec.
     * Assumption: max replication rate of 100,000 requests/s and 100MB/s.
     * 10,000 equals roughly 100msec.
     * 10MB equals roughly 100msec.
     * FIXME magic numbers.
     */
    if (master.cs.count > 10*1000 || master.cs.slab_bytes > 10*1000000) {
      print_log("Graceful failover: Too many items or bytes buffered in the"
        " change set. Will check again in 1 second.");
      goto check_again;
    }
    
    /* Stop collection_delete_thread. */
    engine->coll_del_thread_disable = true;
    
    /* Wait till the item scrubber thread stops.  Set a flag so it never
     * runs again.  The admin runs this command manually, so this check is
     * probably an overkill, though.
     */
    pthread_mutex_lock(&engine->scrubber.lock);
    engine->scrubber.disable = true;
    running = engine->scrubber.running;
    pthread_mutex_unlock(&engine->scrubber.lock);
    if (running) {
      print_log("Graceful failover: The item scrubber thread is running."
        " Will check again in 1 second.");
      goto check_again;
    }

    if (!engine->coll_del_thread_stopped) {
      print_log("Graceful failover: collection_delete_thread is running."
        " Will check again in 1 second.");
      goto check_again;
    }

    /* All set now. */
    /* Atomically check the state again and change it. */
    lock_cache();
    lock_master();
    if ((master.state == MASTER_STATE_RUN) ||
      (config.no_sync_mode && master.state == MASTER_STATE_ASYNC_RUN)) {
      master.state = MASTER_STATE_GRACEFUL_FAILOVER;
      master.graceful_failover.state = GRACEFUL_FAILOVER_STATE_DRAIN;
    }
    unlock_master();
    unlock_cache();
  }

  if (master.graceful_failover.state == GRACEFUL_FAILOVER_STATE_DRAIN) {
    /* See if the cset is empty.  And send the final command to the slave. */
    if (master.cs.count == 0) {
      master_enqueue_graceful_failover();
      master.graceful_failover.state = GRACEFUL_FAILOVER_STATE_ACK;
      print_log("Graceful failover: Sent GRACEFUL_FAILOVER to the slave.");
    }
  }
  else if (master.graceful_failover.state == GRACEFUL_FAILOVER_STATE_ACK) {
    /* Wait for the final ACK from the slave. */
    if (master.msg_queue.count == 0) {
      print_log("Graceful failover: Received the final ACK from the slave."
        " Unblock user requests.");
      /* Unblock user requests.  Unimpl FIXME */
      master.graceful_failover.state = GRACEFUL_FAILOVER_STATE_SLAVE_DONE;
    }
  }
  else if (master.graceful_failover.state ==
    GRACEFUL_FAILOVER_STATE_SLAVE_DONE) {
    if (master.graceful_failover.slave_done) {
      print_log("Graceful failover: Received GRACEFUL_FAILOVER_DONE"
        " from the slave.");
      
      /* Remove the master from the server list.  This is blocking, and may
       * take a while.  It is okay.  The master is proxying/redirecting
       * user requests, and there is no replication going on.
       */
      engine->server.core->zk_remove_master();
      print_log("Graceful failover: Sleeping forever now.");      
      master.graceful_failover.state = GRACEFUL_FAILOVER_STATE_SLEEP;
    }
  }
  else if (master.graceful_failover.state == GRACEFUL_FAILOVER_STATE_SLEEP) {
    /* Do nothing.  The master thread keeps spinning, doing nothing.
     * The admin will kill the process when it becomes completely idle.
     */
  }

  return;
  
check_again:
  master.graceful_failover.check_next_msec = master.now_msec + 1000;
}

static void *
master_thread(void *arg)
{
  struct master_recv_list rx_list;
  struct master_recv_msg *rx_msg;
  uint64_t now;
  /* Used to track command delays in ASYNC mode */
  int async_ackd_count;
  uint64_t async_start_msec;
  struct master_msg_queue *mq = &master.msg_queue;
  struct master_msg *msg;
  int csi_count;
  int csi_array_max = master.csi_array_max;
  struct cset_item_and_netsize *csi_array = master.csi_array;
  struct cset_item_and_netsize *csi_del, *csi_head, *csi_tail, *csi_array_end;
  struct cset_item *csi;
  uint64_t cset_seq;
  bool no_buffer;
  uint64_t sync_mode_last_msec = 0;
  struct wait_list removed_waiters;
  
  print_log("master thread is running");
  
  TAILQ_INIT(&rx_list);
  csi_count = 0;
  csi_head = csi_tail = csi_array;
  csi_array_end = csi_head + csi_array_max;
  cset_seq = master.cs.oldest_seq;
  async_ackd_count = 0;
  async_start_msec = 0;
  no_buffer = false;

  while (master.running) {
    lock_master();
    /* Cannot send anything, and there are no received messages either.
     * Wait for a new event.  Wake up in a bit.
     */
    if ((master.cs.count == 0 || mq->count >= config.max_msg_queue_length ||
         no_buffer) &&
      TAILQ_FIRST(&master.recv_list) == NULL) {
      struct timespec ts;
      get_timespec(&ts, 1 /* wake up in 1msec */);
      master_stats.thread_sleep++;
      master.sleeping = true;
      pthread_cond_timedwait(&master.cond, &master.lock, &ts);
      master.sleeping = false;
    }

    /* Claim all received messages */
    TAILQ_SWAP(&rx_list, &master.recv_list, master_recv_msg, list);
    master.recv_list_length = 0;
    unlock_master();

    no_buffer = false;
    
    /* Pull a few change set items.  Do not move any item if we are doing
     * GC.  This check is ugly...
     */
    if (master.cs.count > 0 && !(master.state == MASTER_STATE_ASYNC_RUN
        && master.gc.state == GC_STATE_RUNNING_GC)) {
      cset_lock(&master.cs);
      /* do not completely fill the ring */
      while (csi_count < csi_array_max-1 &&
        (csi = cset_pull_next_item(&master.cs, true))) {
        csi_tail->csi = csi;
        csi_tail++;
        if (csi_tail == csi_array_end)
          csi_tail = csi_array;
        csi_count++;
      }
      cset_unlock(&master.cs);
    }
    
    /* Turn change set items into master messages */
    csi_del = csi_head;
    while (csi_head != csi_tail) {
      if (master_msg_queue_push_cset(mq, csi_head->csi, cset_seq)) {
        csi_head++;
        cset_seq++;
        if (csi_head == csi_array_end)
          csi_head = csi_array;
      }
      else {
        /* Out of messages or buffers.  Either way, we should wait a little
         * to receive acks and free up messages/buffers.
         * We sleep at most 1 msec.  See above.  Should try to avoid this
         * kind of polling.  FIXME.
         */
        no_buffer = true;
        break;
      }
    }
    master_msg_queue_flush_cset(mq);

    /* Free cset items that have been pushed to master messages */
    if (csi_del != csi_head) {
      struct cset_item_and_netsize *del;

      /* Compute net_size first as we need reference to it and elem, before
       * releaing it...  FIXME
       */
      del = csi_del;
      while (del != csi_head) {
        csi = del->csi;
        del->net_size = calc_cset_netmsg_size(csi);
        del++;
        if (del == csi_array_end)
          del = csi_array;
      }

      del = csi_del;
      lock_cache();
      while (del != csi_head) {
        csi = del->csi;
        del++;
        if (del == csi_array_end)
          del = csi_array;
        /* Releases item, element references.  This also decrements slab_bytes.
         * slab_bytes is technically protected by cset lock.  But we do not
         * lock cset here because memcached worker threads always lock
         * the cache before adding cset items.  If this assumption breaks, then
         * make sure to lock cset here.
         */
        release_cset_item(&master.cs, csi);
      }
      unlock_cache();
      
      while (csi_del != csi_head) {
        int net_size = csi_del->net_size;
        csi = csi_del->csi;
        csi_del->csi = NULL; /* clean up code expects free entries to be NULL */
        csi_del++;
        if (csi_del == csi_array_end)
          csi_del = csi_array;
        csi_count--;
        free_cset_item(&master.cs, csi, net_size); /* free cset bytes */
      }
    }
    
    get_msec_usec(&master.now_msec, &master.now_usec);
    /* now = getmsec(); */
    now = master.now_msec;

    /* Process messages from the slave */
    while ((rx_msg = TAILQ_FIRST(&rx_list)) != NULL) {
      struct netmsg_header *sm;
      uint32_t master_sid, seq;

      sm = &rx_msg->nm.common;
      master_sid = sm->master_sid;
      seq = sm->seq;
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG ||
        /* master.state == MASTER_STATE_ASYNC_RUN || */
        master.state == MASTER_STATE_NACK)
        print_log("master: message from the slave. sid=%u seq=%u",
          master_sid, seq);
      TAILQ_REMOVE(&rx_list, rx_msg, list);
      
      /* Drop if the message has an unexpected sid */
      if (master.master_sid != master_sid) {
        print_log("The received message has an unexpected cookie. Drop."
          " sid=%u expected=%u", master_sid, master.master_sid);
        goto next_rx_msg;
      }
      
      /* NACK is special.  Check it unconditionally.
       * If we receive a NACK, then invalidate all slave information
       * and re-handshake.
       */
      if (sm->type == SLAVE_NETMSG_TYPE_NACK) {
        print_log("NACK from the slave");
        master_stats.slave_nack++;
        if (master.state != MASTER_STATE_NACK) {
          print_log("Transition to STATE_NACK");
          master_start_nack_processing();
        }
        /* The rest of NACK processing continues at the bottom.
         * Look for MASTER_STATE_NACK.
         */
      }
      /* MISSING_KEY is out-of-band.  It never appears in rx_list. */

      /* Process the next oldest message we sent, if the received message
       * ack's it.  If not, drop it.
       */
      msg = master_msg_queue_receive_ack(mq, seq);
      if (msg == NULL)
        goto next_rx_msg;
      
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("master: ack'd message. seq=%u", seq);
      async_ackd_count++;
      /*
      if (master.state == MASTER_STATE_ASYNC_RUN && async_start_msec != 0)
        print_log("ackd_count=%d elapsed=%d state=%d cset=%d latest_seq=%llu"
          " oldest_seq=%llu csi_count=%d", async_ackd_count,
          (int)(now - async_start_msec), master.state, master.cs.count,
          (long long unsigned)master.cs.latest_seq,
          (long long unsigned)master.cs.oldest_seq, csi_count);
      */

      if (master.state == MASTER_STATE_HANDSHAKE_WAIT) {
        if (sm->type == SLAVE_NETMSG_TYPE_HANDSHAKE) {
          master_handle_slave_handshake((struct slave_netmsg_handshake *)sm,
            (struct master_netmsg_handshake *)msg->req.hdr.s.user_hdr);
        }
        else {
          /* The slave has used the wrong sequence number? */
          print_log("Expecting HANDSHAKE but received a different type."
            " type=%u seq=%u", sm->type, sm->seq);
          master.state = MASTER_STATE_HANDSHAKE_START; /* try again... */
        }
      }

      if (msg->cset_seq != 0) {
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("Updated completed_seq. seq=%llu",
            (long long unsigned)msg->cset_seq);
        TAILQ_INIT(&removed_waiters);
        lock_master();
        master.cs.completed_seq = msg->cset_seq+1;
        /* Wake up clients waiting on ack'd sequence numbers */
        wake_waiters(msg->cset_seq+1, 0, 0, true, &removed_waiters);
        unlock_master();
        if (config.notify_io_complete_thread)
          pass_waiters_to_notify_thread(&removed_waiters);
        else
          callback_and_free_waiters(&removed_waiters);
      }
      
      /* This ackd message contains background items.  They are no longer
       * in the network.  So, decrement the counts.
       */
      if (msg->bg_items > 0) {
        assert(mq->bg_items_in_net >= msg->bg_items);
        assert(mq->bg_bytes_in_net >= msg->bg_bytes);
        mq->bg_items_in_net -= msg->bg_items;
        mq->bg_bytes_in_net -= msg->bg_bytes;
      }
      
    next_rx_msg:
      /* Do not need this received message any longer */
      free_fixed_buffer(&master.recv_msg_pool, rx_msg);
    }

    /* Free the messages that have been ack'd */
    master_msg_queue_free_ackd(mq);
    
    /* Send/retransmit messages */
    master_msg_queue_send(mq, now);

    /* Start NACK if cset had overflowed */
    cset_check_overflow(&master.cs);
    
    /* State machine */
    if (master.state == MASTER_STATE_START_CURSOR) {
      bool valid;
      lock_cache();
      valid = assoc_cursor_begin(engine, &master.cursor);
      unlock_cache();
      if (valid) {
        /* Start syncing items now... */
        master.cursor_valid = true;
        lock_cache();
        lock_master();
        if (config.no_sync_mode) {
          master.state = MASTER_STATE_ASYNC_RUN;
          gc_start_detect_idle();
        }
        else
          master.state = MASTER_STATE_RUN;
        unlock_master();
        unlock_cache();
        if (master.state == MASTER_STATE_ASYNC_RUN)
          print_log("Transition to ASYNC_RUN (no SYNC mode)");
        else
          print_log("Transition to RUN");
      }
      else {
        /* Could not get the cursor because the hash table is expanding.
         * Try again later.
         */
      }
    }
    else if (master.state == MASTER_STATE_RUN ||
      master.state == MASTER_STATE_ASYNC_RUN) {
      /* Limit background outstanding items.  This is an indirect way of
       * dividing bandwith between foreground (client requests) and
       * background traffic.  FIXME
       */
      /* Copy items from the hash table */
      if (master.cursor_valid && can_copy_background())
        copy_background(COPY_BACKGROUND_SOURCE_HASH_TABLE);

      /* If we have not sent anything for a while, send a ping. */
      now = getmsec();
      if (mq->count == 0 /* no messages */ &&
        now >= mq->last_send_msec + 1000 /* 1sec.  FIXME */) {
        master_enqueue_ping(now);
      }

      /* Check missing keys reported by the slave, if we have finished copying
       * the hash table.
       */
      if (master.cursor_valid == false && master.missing_kmsg_ready) {
        lock_master();
        if (master.missing_kmsg_ready) {
          master_save_missing_keys();
          master.missing_kmsg_ready = false;
        }
        unlock_master();
      }

      if (master.cursor_valid == false && master.missing_kbuf->count > 0 &&
        can_copy_background()) {
        copy_background(COPY_BACKGROUND_SOURCE_MISSING_KEYS);
      }
    }
    
    if (master.state == MASTER_STATE_HANDSHAKE_START) {
      /* Wait till we completely drain the queue and then enqueue the command */
      if (mq->count == 0) {
        /* FIXME.  Prevent churn.  Wait some time before sending
         * another HANDSHAKE.
         */

        /* For each new handshake, we bump the sid */
        master.master_sid = gen_sid();
        print_log("Transition to HANDSHAKE_WAIT");
        master_enqueue_handshake();
        master.state = MASTER_STATE_HANDSHAKE_WAIT;
      }
    }
    else if (master.state == MASTER_STATE_HANDSHAKE_WAIT) {
      /* All processing is done above. */
    }
    else if (master.state == MASTER_STATE_RUN) {
      /* Transition to ASYNC mode if necessary.
       * - There are waiters that have waited too long.  OR
       * - Commands are getting delayed too much.
       */
      int count = 0, delay = 0;
      
      /* Complete user requests that have waited too long.
       * Then see how long the oldest command has been waiting for an ack.
       */
      if (sync_mode_last_msec + 1 < master.now_msec) {
        TAILQ_INIT(&removed_waiters);
        sync_mode_last_msec = master.now_msec;
        count = wake_waiters(0, now, config.max_sync_wait_msec, false,
          &removed_waiters);
        if (count == 0 &&
          (msg = TAILQ_FIRST(&mq->send_list)) != NULL && msg->tx_msec != 0) {
          delay = (int)(now - msg->tx_msec);
          /* tx_msec gets updated when we retransmit the command.
           * It's okay, though.  Retransmission timeout is normally larger than
           * the delay threshold.  Even if it is not, the wait threshold above
           * would force us to transition to ASYNC.
           */
        }
        if (config.notify_io_complete_thread)
          pass_waiters_to_notify_thread(&removed_waiters);
        else
          callback_and_free_waiters(&removed_waiters);
      }      
      if (count > 0 || delay > config.replication_hiwat_msec) {
        lock_cache();
        lock_master();
        master.state = MASTER_STATE_ASYNC_RUN;
        unlock_master();
        unlock_cache();
        /* Make sure every thread sees this change at this point */
        print_log("Transition to ASYNC_RUN. now=%llu count=%d delay=%d",
          (long long unsigned)now, count, delay);
        async_start_msec = 0;
        async_ackd_count = 0;
        gc_start_detect_idle();

        /* Wake up every one else waiting for completions */
        TAILQ_INIT(&removed_waiters);
        lock_master();
        master.cs.completed_seq = master.cs.latest_seq;
        wake_waiters(master.cs.completed_seq, 0, 0, true, &removed_waiters);
        unlock_master();
        if (config.notify_io_complete_thread)
          pass_waiters_to_notify_thread(&removed_waiters);
        else
          callback_and_free_waiters(&removed_waiters);
      }

      /* FIXME.  SYNC<->ASYNC.  We should measure replication latencies
       * once say 10msec.  We can simply recycle the waiter code already
       * in place.  For example, once every 10msec, if we are not already
       * measuring the latency, place a waiter on the latest sequence number.
       * When that waiter completes, we know measure the elapsed time.
       * To transition to SYNC, we would also check that we have finished
       * copying the hash table.
       */
    }
    else if (master.state == MASTER_STATE_ASYNC_RUN &&
      config.no_sync_mode == 0) {
      /* We are in ASYNC mode.  SYNC mode is enabled too.  So, see if we
       * should transition to SYNC mode.  We do so if the slave is ACK'ing
       * master messages in timely manner.
       */
      
      int delay = 0;
      if ((msg = TAILQ_FIRST(&mq->send_list)) != NULL && msg->tx_msec != 0)
        delay = (int)(now - msg->tx_msec);
      
      if (async_start_msec == 0) {
        /* Wait for the first command that completes under lowat threshold.
         * The command may have been sent before we transitioned to the async
         * mode.  But that is okay...
         */
        if (async_ackd_count > 0 && delay < config.replication_lowat_msec) {
          async_start_msec = now;
          print_log("ASYNC_RUN. Start counting messages. msec=%llu"
            " ackd_count=%d delay=%d", (long long unsigned)async_start_msec,
            async_ackd_count, delay);
        }
      }
      else {
        /* Count consecutive commands that complete under lowat threshold */
        if (delay >= config.replication_lowat_msec) {
          print_log("ASYNC_RUN. Messages still taking too long. now=%llu"
            " ackd_count=%d delay=%d", (long long unsigned)now,
            async_ackd_count, delay);
          async_start_msec = 0; // start over
          async_ackd_count = 0;
        }
        /* Transition to SYNC mode if all recent commands have finished
         * under lowat threshold.
         */
        else if (async_ackd_count >config.async_to_sync_consecutive_acks &&
          (now - async_start_msec) > config.async_to_sync_min_duration &&
          master.cs.count < config.async_to_sync_max_cset_items) {
          print_log("Transition to RUN. now=%llu ackd_count=%d cset_count=%d"
            " waiters=%d queued_messages=%d",
            (long long unsigned)now, async_ackd_count, master.cs.count,
            master.waiter_count, mq->count);
          lock_cache();
          lock_master();
          master.state = MASTER_STATE_RUN;
          unlock_master();
          unlock_cache();
        }
      }

      /* SYNC vs. ASYNC mode
       *
       * SYNC:
       * The slave is responding quickly.  The command queue is draining
       * quickly.
       *
       * If the item is copied, then the client waits till the change is copied
       * to the slave.  There is a maximum wait time, though.
       * If the item is not copied, then it is locally updated.  The client
       * does not wait.
       *
       * ASYNC:
       * The slave is responding slowly.  The command queue is filling up.
       * The client does not wait for replication to complete.  The items are
       * copied to the slave in the background.
       *
       * While the command queue has free space, the master keeps inserting
       * commands to the queue.  When the queue overflows, the master resets
       * replication state and starts over from handshake.
       *
       * SYNC->ASYNC conditions:
       * - A waiter has waited too long, above the threshold.
       * - Replication delay (send -> ack delay) is greater than the threshold.
       *
       * ASYNC->SYNC conditions:
       * - Replication delay is under the threshold for some time.
       *
       * When the command queue overflow, regardless of state, the master
       * resets replication state and starts over from handshake.
       *
       * -------
       * SYNC does NOT mean that the master and the slave have the exact same
       * items.
       */
    }
    else if (master.state == MASTER_STATE_NACK) {
      int i;
      
      /* Release the cursor so the hash table can expand if necessary */
      if (master.cursor_valid) {
        lock_cache();
        assoc_cursor_end(&master.cursor);
        master.cursor_valid = false;
        unlock_cache();
      }

      /* Free cset items that we have already pulled out of the cset */
      for (i = 0; i < master.csi_array_max; i++) {
        csi = master.csi_array[i].csi;
        master.csi_array[i].csi = NULL;
        if (csi != NULL) {
          int net_size = calc_cset_netmsg_size(csi);
          lock_cache();
          release_cset_item(&master.cs, csi);
          free_cset_item(&master.cs, csi, net_size);
          unlock_cache();
        }
      }
      csi_head = csi_tail = csi_array; /* Reset the pointers... */
      csi_count = 0;
      
      /* Clear the change set */
      cset_empty();
      /* These vars are all over the place.  FIXME... */
      cset_seq = master.cs.oldest_seq;
      
      /* Wake up all the waiters */
      TAILQ_INIT(&removed_waiters);
      lock_master();
      master.cs.completed_seq = master.cs.latest_seq;
      wake_waiters(master.cs.completed_seq, 0, 0, true, &removed_waiters);
      unlock_master();
      if (config.notify_io_complete_thread)
        pass_waiters_to_notify_thread(&removed_waiters);
      else
        callback_and_free_waiters(&removed_waiters);
      
      /* Wait for all sent commands to be ACK'd or NACK'd.
       * And then handshake again.
       */
      if (mq->count == 0) {
        /* Clear the missing keys too */
        key_hash_table_reset(master.missing_ktab);
        key_buffer_reset(master.missing_kbuf);
        
        /* Don't touch missing_kmsg, kmsg_ready.  msg_chan callbacks
         * control them.  It is okay to receive stale missing keys.
         */
        
        /* FIXME.  Reset stats... */
        print_log("Transition to HANDSHAKE_START");
        master.state = MASTER_STATE_HANDSHAKE_START;
        
        /* Note.  We can easily test NACK by running the master and the slave in
         * standalone mode, without ZK.  With ZK, when we kill the slave,
         * the master always (?) sees that the slave is gone.  The master
         * then simply stops the thread, instead of going through this NACK
         * processing.
         */
      }
    }
    else if (master.state == MASTER_STATE_START_CURSOR) {
      /* We are waiting for the hash expansion to finish. */
    }

    /* See if we need to perform gc. */
    if (master.state == MASTER_STATE_ASYNC_RUN && config.gc_enable) {
      if (master.gc.state == GC_STATE_DETECT_IDLE)
        gc_detect_idle();
      else if (master.gc.state == GC_STATE_RUNNING_GC)
        gc_run();
      else if (master.gc.state == GC_STATE_RECOVERY)
        gc_recovery();
    }

    if (master.graceful_failover.state != GRACEFUL_FAILOVER_STATE_NONE) {
      master_graceful_failover();
    }
    
    /* Explain communication patterns.  FIXME */
  }
  print_log("master thread has terminated");
  return NULL;
}

static int
master_init(struct sockaddr_in *laddr)
{
  int size;
  
  memset(&master_stats, 0, sizeof(master_stats));
  memset(&master, 0, sizeof(master));

  master.listen_addr = *laddr;
  
  /* Pre-allocate receive messages */
  if (0 != init_fixed_buffer_pool(&master.recv_msg_pool,
      sizeof(struct master_recv_msg), config.max_recv_queue_length)) {
    print_log("Failed to allocate the receive message pool");
    return -1;
  }

  /* A single missing_key message */
  master.missing_kmsg_ready = false;

  /* A lookup table to find duplicate missing keys */
  master.missing_ktab = key_hash_table_init(256 /* FIXME */);
  if (master.missing_ktab == NULL) {
    print_log("Failed to create the lookup table for missing keys");
    return -1;
  }
  
  /* We store missing keys in this buffer and pop one at a time.  And copy
   * it in the background, just like we walk the hash table and copy items.
   */
  master.missing_kbuf = key_buffer_init(SLAVE_NETMSG_MISSING_KEY_BUFSIZE);
  if (master.missing_kbuf == NULL) {
    print_log("Failed to create the memory buffer for missing keys");
    return -1;
  }

  TAILQ_INIT(&master.recv_list);
  pthread_mutex_init(&master.lock, NULL);
  pthread_cond_init(&master.cond, NULL);
  
  TAILQ_INIT(&master.waiters);
  master.waiter_count = 0;
  /* pthread_mutex_init(&master.w_cache.lock, NULL); */
  TAILQ_INIT(&master.w_cache.free_entries);
  master.w_cache.count = 0;
  
  /* Change set */
  if (0 != cset_init())
    return -1;
  
  /* Message queue */
  if (0 != master_msg_queue_init(&master.msg_queue))
    return -1;

  TAILQ_INIT(&master.notify_io_complete.waiters);
  master.notify_io_complete.running = false;
  
  master.state = MASTER_STATE_INVALID;
  master.csi_array_max = 40; /* FIXME */
  size = sizeof(struct cset_item_and_netsize) * master.csi_array_max;
  master.csi_array = malloc(size);
  memset(master.csi_array, 0, size);

  /* gc */
  if (config.gc_enable) {
    if (master.cs.max_slab_bytes == 0) {
      /* No limit, so do not bother with gc */
      config.gc_enable = 0;
      print_log("config.cset_max_slab_memory is 0. Turning off garbage"
        " collection.");
    }
    else {
      uint64_t thresh;
      /* Thresh memory (bytes) = max slab bytes * gc_thresh_memory (%) */
      thresh = master.cs.max_slab_bytes;
      thresh *= config.gc_thresh_memory;
      thresh /= 100;
      /* Minimum 10MB... */
      if (thresh < 10*(1<<20))
        thresh = 10*(1<<20);
      master.gc.thresh_bytes = thresh;

      thresh = master.cs.max_slab_bytes;
      thresh *= config.gc_thresh_recov_memory;
      thresh /= 100;
      master.gc.thresh_recov_bytes = thresh;
      /* Do not care about min or max for this threshold. */
    }
  }
  
  /* start_master actually creates the master thread and so on */
  return 0;
}

static void
stop_master(void)
{
  struct master_recv_msg *msg;
  int i;
  
  if (!master.running)
    return;

  /* FIXME.  Should combine this with nack? */
  print_log("stop_master");

  /* Stop the notify thread */
  if (master.notify_io_complete.running) {
    print_log("Waiting for the notify io complete thread to stop.");
    pthread_mutex_lock(&master.notify_io_complete.lock);
    master.notify_io_complete.running = false;
    pthread_cond_signal(&master.notify_io_complete.cond); /* wake it up */
    pthread_mutex_unlock(&master.notify_io_complete.lock);
    if (0 != pthread_join(master.notify_io_complete.tid, NULL)) {
      PERROR_DIE("pthread_join failed");
    }
    print_log("The notify io complete thread has stopped.");
  }
  
  /* Stop the master thread */
  print_log("Waiting for the master thread to stop.");
  master.running = 0;
  if (0 != pthread_join(master.tid, NULL)) {
    PERROR_DIE("pthread_join failed");
  }
  print_log("The master thread has stopped.");

  /* Stop inserting items into the change set */
  lock_cache();
  lock_master();
  master.state = MASTER_STATE_INVALID;
  unlock_master();
  unlock_cache();
  print_log("Stop further item insertions into the change set.");
  
  /* Stop the message channel */
  if (master.msg_chan) {
    print_log("Stopping the message channel.");
    msg_chan_destroy(master.msg_chan);
    master.msg_chan = NULL;
    master.slave_ch_id = 0;
    print_log("The message channel has stopped.");
  }
  
  /* Release the cursor */
  if (master.cursor_valid) {
    lock_cache();
    assoc_cursor_end(&master.cursor);
    master.cursor_valid = false;
    unlock_cache();
    print_log("Released the hash table cursor.");
  }
  
  /* Free cset items that the master thread has pulled out of the cset */
  for (i = 0; i < master.csi_array_max; i++) {
    struct cset_item *csi = master.csi_array[i].csi;
    master.csi_array[i].csi = NULL;
    if (csi != NULL) {
      int net_size = calc_cset_netmsg_size(csi);
      lock_cache();
      release_cset_item(&master.cs, csi);
      free_cset_item(&master.cs, csi, net_size);
      unlock_cache();
    }
  }
  
  /* Clear the change set */
  cset_empty();
  print_log("Cleared the change set.");
  
  /* Wake up all the waiters */
  print_log("Waking up all waiting clients.");
  {
    struct wait_list removed_waiters;
    TAILQ_INIT(&removed_waiters);
    master.cs.completed_seq = master.cs.latest_seq;
    wake_waiters(master.cs.completed_seq, 0, 0, false, &removed_waiters);
    callback_and_free_waiters(&removed_waiters);

    /* There might be waiters sitting in the notify io complete's list. */
    callback_and_free_waiters(&master.notify_io_complete.waiters);
  }

  /* Clear the receive queue */
  while ((msg = TAILQ_FIRST(&master.recv_list))) {
    TAILQ_REMOVE(&master.recv_list, msg, list);
    free_fixed_buffer(&master.recv_msg_pool, msg);
  }
  
  /* Clear the message queue */
  master_msg_queue_reset(&master.msg_queue);
  print_log("Reset the message queue.");
  
  /* Clear the missing keys */
  key_hash_table_reset(master.missing_ktab);
  key_buffer_reset(master.missing_kbuf);
  master.missing_kmsg_ready = false;
  
  print_log("The master is in INVALID state now");
}

static int
start_master(struct sockaddr_in *slave_addr)
{
  const char *local_msg_chan_id = "master";
  const char *remote_msg_chan_id = "slave";
  
  print_log("start_master");
#if USE_TEST_CODE
  if (master.test_force_slave_msg_chan_id) {
    local_msg_chan_id = "slave";
    remote_msg_chan_id = "master";
  }
#endif
  master.slave_addr = *slave_addr;
  master.msg_chan = start_msg_chan(&master.listen_addr, local_msg_chan_id);
  if (master.msg_chan == NULL) {
    print_log("Failed to create the message channel");
    return -1;
  }
  master.slave_ch_id = msg_chan_create_remote(master.msg_chan,
    remote_msg_chan_id, &master.slave_addr);
  if (master.slave_ch_id < 0)
    return -1;

  /* Starting state */
  master.state = MASTER_STATE_HANDSHAKE_START;
  master.master_sid = gen_sid();
  master.slave_sid = INVALID_SID;
  master.cursor_valid = false;
  memset(master.csi_array, 0,
    sizeof(struct cset_item_and_netsize) * master.csi_array_max);

  if (config.notify_io_complete_thread) {
    /* Start the notify io complete thread */
    TAILQ_INIT(&master.notify_io_complete.waiters);
    pthread_mutex_init(&master.notify_io_complete.lock, NULL);
    pthread_cond_init(&master.notify_io_complete.cond, NULL);
    pthread_attr_init(&master.notify_io_complete.attr);
    pthread_attr_setscope(&master.notify_io_complete.attr,
      PTHREAD_SCOPE_SYSTEM);
    master.notify_io_complete.running = true;
    pthread_create(&master.notify_io_complete.tid,
      &master.notify_io_complete.attr, notify_io_complete_thread, NULL);
  }

  /* A single master thread sends replication commands to the slave */
  pthread_attr_init(&master.attr);
  pthread_attr_setscope(&master.attr, PTHREAD_SCOPE_SYSTEM);
  master.running = 1;
  pthread_create(&master.tid, &master.attr, master_thread, NULL);

  return 0;
}

static int
set_slave(struct sockaddr_in *addr)
{
  /* We are the master.  We now have the slave. */
  print_log("set_slave");
  if (mode != RP_MODE_MASTER) {
    print_log("The server is not in MASTER mode.");
    return -1;
  }
  if (master.state == MASTER_STATE_GRACEFUL_FAILOVER) {
    print_log("Graceful failover in progress. The slave address is ignored.");
    return 0;
  }
  if (master.running) {
    /* Ignore if the slave address has not changed */
    if (addr != NULL &&
      master.slave_addr.sin_addr.s_addr == addr->sin_addr.s_addr &&
      master.slave_addr.sin_port == addr->sin_port) {
      print_log("The slave address has not changed. Do not restart"
        " the master thread.");
      return 0;
    }
    stop_master();
  }
  if (addr) {
    char buf[INET_ADDRSTRLEN*2];
    const char *ip;
    ip = inet_ntop(AF_INET, (const void*)&addr->sin_addr,
      buf, sizeof(buf));
    print_log("slave_addr=%s:%d", (ip ? ip : "null"), ntohs(addr->sin_port));
    if (0 == start_master(addr)) {
      print_log("Successfully started the master thread.");
      return 0;
    }
    else {
      print_log("Failed to start the master thread.");
      return -1;
    }
  }
  else {
    print_log("There are no slaves. Do not start the master thread.");
    return 0;
  }
}

static void
slave_send_ack(uint32_t master_sid, uint32_t seq, bool nack)
{
  struct slave_msg *msg;
  struct msg_chan_sendreq_user *req;
  struct netmsg_header *ack;

  if ((msg = alloc_fixed_buffer(&slave.send_msg_pool))) {
    req = &msg->req;
    req->remote = slave.master_ch_id;
    req->body = NULL;
    req->body_len = 0;
    req->hdr_len = sizeof(*ack);
    ack = (struct netmsg_header *)req->hdr.s.user_hdr;
    ack->master_sid = master_sid;
    ack->seq = seq + 1;
    ack->type = nack ? SLAVE_NETMSG_TYPE_NACK : SLAVE_NETMSG_TYPE_ACK;
    
    if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
      print_log("slave_send_ack. %s seq=%u", nack ? "NACK" : "ACK", ack->seq);
    }
    if (0 != msg_chan_send(slave.msg_chan, req)) {
      ERROR_DIE("msg_chan_send failed"); /* FIXME */
    }
  }
  else {
    print_log("slave_send_ack: failed to allocate message");
  }
}

static void
slave_send_handshake(uint64_t seq)
{
  struct slave_msg *msg;
  struct msg_chan_sendreq_user *req;
  struct slave_netmsg_handshake *hs;

  if ((msg = alloc_fixed_buffer(&slave.send_msg_pool))) {
    req = &msg->req;
    req->remote = slave.master_ch_id;
    req->body = NULL;
    req->body_len = 0;
    req->hdr_len = sizeof(*hs);
    hs = (struct slave_netmsg_handshake *)req->hdr.s.user_hdr;
    hs->common.master_sid = slave.master_sid;
    hs->common.seq = seq + 1;
    hs->common.type = SLAVE_NETMSG_TYPE_HANDSHAKE;
    hs->master_sid = slave.master_sid;
    hs->slave_sid = slave.slave_sid;
    
    print_log("send HANDSHAKE. seq=%llu master_sid=%llu slave_sid=%llu",
      (long long unsigned)hs->common.seq, (long long unsigned)hs->master_sid,
      (long long unsigned)hs->slave_sid);
    if (0 != msg_chan_send(slave.msg_chan, req)) {
      ERROR_DIE("msg_chan_send failed"); /* FIXME */
    }
  }
  else {
    print_log("slave_send_handshake: failed to allocate message");
  }
}

static int
slave_send_missing_keys(void)
{
  struct msg_chan_sendreq_user *req;
  struct slave_netmsg_missing_key *mk;
  uint16_t nkey;
  char *key;
  char *c;
  int body_len;
  
  /* Still being transmitted, or there are no keys */
  if (0 != slave.missing_kmsg.hdr_len ||
    0 != key_buffer_peek_head(slave.missing_kbuf, &nkey, &key))
    return -1;

  /* Pack all keys into kmsg_keybuf */
  c = slave.missing_kmsg_keybuf;
  while (0 == key_buffer_peek_head(slave.missing_kbuf, &nkey, &key)) {
    *((uint16_t*)c) = nkey;
    c += sizeof(uint16_t);
    memcpy(c, key, nkey);
    c += nkey;
    key_buffer_pop_head(slave.missing_kbuf);
  }
  key_hash_table_reset(slave.missing_ktab);
  /* Should just send the key buffer intact?  FIXME */
  
  body_len = c - slave.missing_kmsg_keybuf;
  
  req = &slave.missing_kmsg;
  req->remote = slave.master_ch_id;
  req->body = slave.missing_kmsg_keybuf;
  req->body_len = body_len;
  req->hdr_len = sizeof(*mk);
  
  mk = (struct slave_netmsg_missing_key *)req->hdr.s.user_hdr;
  mk->keybuf_len = body_len;
  mk->common.master_sid = slave.master_sid;
  mk->common.seq = 0;
  mk->common.type = SLAVE_NETMSG_TYPE_MISSING_KEY;
  
  print_log("Send MISSING_KEY. keybuf_len=%u", body_len);
  if (0 != msg_chan_send(slave.msg_chan, req)) {
    ERROR_DIE("msg_chan_send failed"); /* FIXME */
  }
  return 0;
}

static void
slave_send_graceful_failover_done(void)
{
  struct slave_msg *msg;
  struct msg_chan_sendreq_user *req;
  struct netmsg_header *done;

  if ((msg = alloc_fixed_buffer(&slave.send_msg_pool))) {
    req = &msg->req;
    req->remote = slave.master_ch_id;
    req->body = NULL;
    req->body_len = 0;
    req->hdr_len = sizeof(*done);
    done = (struct netmsg_header *)req->hdr.s.user_hdr;
    done->master_sid = slave.master_sid;
    done->seq = 0; /* no sequence number */
    done->type = SLAVE_NETMSG_TYPE_GRACEFUL_FAILOVER_DONE;
    
    print_log("Send GRACEFUL_FAILOVER_DONE");
    if (0 != msg_chan_send(slave.msg_chan, req)) {
      ERROR_DIE("msg_chan_send failed"); /* FIXME */
    }
  }
  else {
    print_log("slave_send_graceful_failover_done: failed to allocate message");
  }
}

/* We want three properties.
 * 1. convert(M) = S no matter when or how many times we convert.
 *    For example, we cannot have convert(M) = S at time T, and
 *    convert(M) = S' at time T'.
 * 2. If M1 != M2, convert(M1) != convert(M2)
 * 3. If M1 > M2, convert(M1) > convert(M2)
 */
static rel_time_t
slave_convert_master_time(rel_time_t time)
{
  rel_time_t local;
  if (slave.master_base_time >= time)
    local = slave.slave_base_time - (slave.master_base_time - time);
  else
    local = slave.slave_base_time + (time - slave.master_base_time);
  return local;
}

static rel_time_t
slave_convert_master_exptime(rel_time_t time)
{
  /* rel_time_t is uint32_t.  See include/memcached/types.h
   * So be careful not to do "rel_time_t < 0".
   */
  if (time == 0 || time == (rel_time_t)(-1))
    return time; /* 0 (never), -1 (sticky) */
  return slave_convert_master_time(time);
}

static void
slave_save_missing_key(uint16_t nkey, const char *key)
{
  uint32_t hash;
  char *k;

  if (!config.copy_missing_key)
    return;
  hash = key_hash_table_compute_hash(nkey, key);
  if (key_hash_table_lookup(slave.missing_ktab, nkey, key, hash)) {
    /* This key already is in the missing key buffer.  Do not add. */
  }
  else if (!key_hash_table_is_full(slave.missing_ktab) &&
    (k = key_buffer_push_tail(slave.missing_kbuf, nkey, key)) != NULL) {
    /* k points to the missing keys buffer.
     * This insertion must succeed.  We already checked if the table is full.
     */
    key_hash_table_insert(slave.missing_ktab, nkey, k, hash);
  }
}

static bool
slave_apply_cset_netmsg(const char *b, int size)
{
  struct cset_netmsg_key *key;
  const char *end = b + size;
  uint8_t type;
  ENGINE_ERROR_CODE err;
  rel_time_t engine_time = engine->server.core->get_current_time();

  while (b < end) {
    type = *b;
    err = ENGINE_FAILED;
    switch (type) {
      case CSNM_LINK_SIMPLE_KEY:
      case CSNM_LINK_BTREE_KEY:
      case CSNM_LINK_SET_KEY:
      case CSNM_LINK_LIST_KEY:
      {
        struct cset_netmsg_item_meta *meta;
        rel_time_t exptime;
        const char *keystr;
        
        key = (void*)b;
        meta = (void*)(key+1);
        keystr = (char*)(meta+1);
        exptime = slave_convert_master_exptime(meta->exptime);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("LINKED. key=%s engine_time=%u exptime=%u=>%u",
            rp_printable_key(keystr, key->len), engine_time, meta->exptime,
          exptime);
        b = keystr + key->len;
        
        switch (type) {
          case CSNM_LINK_SIMPLE_KEY:
          {
            struct cset_netmsg_val *val = (void*)b;
            b = (char*)(val+1);
            err = rp_apply_simple_item_link(engine, keystr, key->len,
              b, val->len, val->cas, meta->flags, engine_time, exptime);
            b += val->len;
            slave_stats.simple_item_link++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.simple_item_link_error++;
              err = ENGINE_SUCCESS; /* okay */
            }
          }
          break;
          case CSNM_LINK_BTREE_KEY:
          {
            err = rp_apply_btree_item_link(engine, keystr, key->len,
              (void*)b, meta->flags, engine_time, exptime);
            b += sizeof(btree_meta_info);
            slave_stats.btree_item_link++;
            if (err != ENGINE_SUCCESS) {
              if (err == ENGINE_KEY_EEXISTS)
                slave_stats.btree_item_link_replace++;
              else
                slave_stats.btree_item_link_error++;
              err = ENGINE_SUCCESS; /* okay */
            }
          }
          break;
          case CSNM_LINK_SET_KEY:
          {
            err = rp_apply_set_item_link(engine, keystr, key->len,
              (void*)b, meta->flags, engine_time, exptime);
            b += sizeof(set_meta_info);
            slave_stats.set_item_link++;
            if (err != ENGINE_SUCCESS) {
              if (err == ENGINE_KEY_EEXISTS)
                slave_stats.set_item_link_replace++;
              else
                slave_stats.set_item_link_error++;
              err = ENGINE_SUCCESS; /* okay */
            }
          }
          break;
          case CSNM_LINK_LIST_KEY:
          {
            err = rp_apply_list_item_link(engine, keystr, key->len,
              (void*)b, meta->flags, engine_time, exptime);
            b += sizeof(list_meta_info);
            slave_stats.list_item_link++;
            if (err != ENGINE_SUCCESS) {
              if (err == ENGINE_KEY_EEXISTS)
                slave_stats.list_item_link_replace++;
              else
                slave_stats.list_item_link_error++;
              err = ENGINE_SUCCESS; /* okay */
            }
          }
          break;
        }
      }
      break;
      case CSNM_UNLINK_KEY:
      case CSNM_UNLINK_EVICT_KEY:
      case CSNM_UNLINK_EVICT_DELETE_NOW_KEY:
      {
        key = (void*)b;
        b = (char*)(key+1);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("UNLINKED. key=%s", rp_printable_key(b, key->len));
        err = rp_apply_item_unlink(engine, b, key->len,
          (type == CSNM_UNLINK_EVICT_DELETE_NOW_KEY));
        if (type == CSNM_UNLINK_KEY)
          slave_stats.item_unlink++;
        else
          slave_stats.item_unlink_evict++;
        if (err != ENGINE_SUCCESS) {
          slave_stats.item_unlink_error++;
          err = ENGINE_SUCCESS; /* okay */
        }
        b += key->len;
      }
      break;
      case CSNM_COLL_KEY:
      {
        hash_item *it;
        char *keystr;
        key = (void*)b;
        keystr = (char*)(key+1);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("COLL_KEY. key=%s", rp_printable_key(keystr, key->len));
        it = item_get(engine, keystr, key->len); /* increments refcount */
        if (it != NULL) {
          err = ENGINE_SUCCESS;
        }
        else {          
          if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
            print_log("COLL_KEY. Non-existent key. key=%s",
              rp_printable_key(keystr, key->len));
          }
          /* This may happen.  The key might have expired or evicted. */
          err = ENGINE_SUCCESS; /* okay */
          slave_stats.coll_key_error++;
          
          /* Remember this missing key */
          slave_save_missing_key(key->len, keystr);
        }
        if (slave.last_coll_it)
            item_release(engine, slave.last_coll_it);
        slave.last_coll_it = it;
        slave_stats.coll_key++;
        b = keystr + key->len;
      }
      break;
      case CSNM_LINK_BTREE_ELEM:
      case CSNM_UNLINK_BTREE_ELEM:
      {
        struct cset_netmsg_btree_elem *e = (void*)b;
        hash_item *it;
        b = (char*)(e+1);
        if ((it = slave.last_coll_it)) {
          if (type == CSNM_LINK_BTREE_ELEM) {
            err = rp_apply_btree_elem_insert(engine, it, (void*)b);
            slave_stats.btree_elem_insert++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.btree_elem_insert_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
          else {
            err = rp_apply_btree_elem_unlink(engine, it, b, e->nbkey);
            slave_stats.btree_elem_unlink++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.btree_elem_unlink_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
        }
        else {
          if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
            print_log("%s failed. No collection key.",
              type == CSNM_LINK_BTREE_ELEM ? "CSNM_LINK_BTREE_ELEM" :
              "CSNM_UNLINK_BTREE_ELEM");
          }
          slave_stats.btree_elem_no_coll_key++;
          err = ENGINE_SUCCESS; /* okay */
        }
        b += e->len;
      }
      break;
      case CSNM_LINK_SET_ELEM:
      case CSNM_UNLINK_SET_ELEM:
      {
        struct cset_netmsg_set_elem *e = (void*)b;
        hash_item *it;
        b = (char*)(e+1);
        if ((it = slave.last_coll_it)) {
          if (type == CSNM_LINK_SET_ELEM) {
            err = rp_apply_set_elem_insert(engine, it, b, e->val_len);
            slave_stats.set_elem_insert++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.set_elem_insert_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
          else {
            err = rp_apply_set_elem_unlink(engine, it, b, e->val_len);
            slave_stats.set_elem_unlink++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.set_elem_unlink_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
        }
        else {
          if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
            print_log("%s failed. No collection key.",
              type == CSNM_LINK_SET_ELEM ? "CSNM_LINK_SET_ELEM" :
              "CSNM_UNLINK_SET_ELEM");
          }
          slave_stats.set_elem_no_coll_key++;
          err = ENGINE_SUCCESS; /* okay */
        }
        b += e->val_len;
      }
      break;
      case CSNM_LINK_LIST_ELEM:
      case CSNM_UNLINK_LIST_ELEM:
      {
        struct cset_netmsg_list_elem *e = (void*)b;
        hash_item *it;
        b = (char*)(e+1);
        if ((it = slave.last_coll_it)) {
          if (type == CSNM_LINK_LIST_ELEM) {
            err = rp_apply_list_elem_insert(engine, it,
              (int16_t)e->idx, b, e->val_len);
            slave_stats.list_elem_insert++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.list_elem_insert_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
          else {
            err = rp_apply_list_elem_unlink(engine, it,
              (int16_t)e->idx, 1 /* fcnt */);
            slave_stats.list_elem_unlink++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.list_elem_unlink_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
        }
        else {
          if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
            print_log("%s failed. No collection key.",
              type == CSNM_LINK_LIST_ELEM ? "CSNM_LINK_LIST_ELEM" :
              "CSNM_UNLINK_LIST_ELEM");
          }
          slave_stats.list_elem_no_coll_key++;
          err = ENGINE_SUCCESS; /* okay */
        }
        b += e->val_len; /* val_len = 0 when type==unlink */
      }
      break;
      case CSNM_LRU_UPDATE_KEY:
      {
        char *keystr;
        key = (void*)b;
        keystr = (char*)(key+1);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("LRU_UPDATE. key=%s", rp_printable_key(keystr, key->len));
        err = rp_apply_item_lru_update(engine, keystr, key->len);
        b = keystr + key->len;
        slave_stats.lru_update++;
        if (err != ENGINE_SUCCESS) {
          if (err == ENGINE_KEY_ENOENT) {
            /* Remember this missing key */
            slave_save_missing_key(key->len, keystr);
          }
          slave_stats.lru_update_error++;
          err = ENGINE_SUCCESS; /* okay */
        }
      }
      break;
      case CSNM_FLUSH:
      {
        struct cset_netmsg_flush *fl = (void*)b;        
        if (rp_loglevel >= RP_LOGLEVEL_INFO)
          print_log("FLUSH");
        err = rp_apply_flush(engine, fl->nprefix == 255 ? NULL : fl->prefix,
          fl->nprefix == 255 ? -1 : fl->nprefix);
        b += sizeof(*fl);
        if (fl->nprefix == 255) {
          slave_stats.flush++;
          if (err != ENGINE_SUCCESS)
            slave_stats.flush_error++;
        }
        else {
          slave_stats.flush_prefix++;
          if (err != ENGINE_SUCCESS)
            slave_stats.flush_prefix_error++;
        }
        err = ENGINE_SUCCESS; /* okay */
      }
      break;
      case CSNM_ATTR_EXPTIME:
      {
        struct cset_netmsg_exptime *e;
        rel_time_t exptime;
        const char *keystr;
        
        key = (void*)b;
        e = (void*)(key+1);
        exptime = slave_convert_master_exptime(e->exptime);
        keystr = (char*)(e+1);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
          print_log("ATTR_EXPTIME. key=%s exptime=%u=>%u",
            rp_printable_key(keystr, key->len), e->exptime, exptime);
        }
        err = rp_apply_setattr_exptime(engine, keystr, key->len, exptime);
        b = (keystr + key->len);
        slave_stats.attr_exptime++;
        if (err != ENGINE_SUCCESS) {
          if (err == ENGINE_KEY_ENOENT) {
            /* Remember this missing key */
            slave_save_missing_key(key->len, keystr);
          }
          slave_stats.attr_exptime_error++;
          err = ENGINE_SUCCESS; /* okay */
        }
      }
      break;
      case CSNM_ATTR_EXPTIME_INFO:
      case CSNM_ATTR_EXPTIME_INFO_BKEY:
      {
        hash_item *it;
        rel_time_t exptime;
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
          print_log("ATTR_EXPTIME_INFO. last_coll_it=%p",
            (void*)slave.last_coll_it);
        }
        if ((it = slave.last_coll_it)) {
          struct cset_netmsg_exptime_info_bkey *e = (void*)b;
          exptime = slave_convert_master_exptime(e->exptime);
          err = rp_apply_setattr_meta_info(engine, slave.last_coll_it,
            e->ovflact, e->mflags, exptime, e->mcnt,
            (type == CSNM_ATTR_EXPTIME_INFO ? NULL : &(e->maxbkeyrange)));
          if (type == CSNM_ATTR_EXPTIME_INFO) {
            slave_stats.attr_exptime_info++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.attr_exptime_info_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
          else {
            slave_stats.attr_exptime_info_bkey++;
            if (err != ENGINE_SUCCESS) {
              slave_stats.attr_exptime_info_bkey_error++;
              err = ENGINE_SUCCESS; /* FIXME.  Delete the item? */
            }
          }
        }
        else {
          if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
            print_log("%s failed. No collection key.",
              type == CSNM_ATTR_EXPTIME_INFO ? "CSNM_ATTR_EXPTIME_INFO" :
              "CSNM_ATTR_EXPTIME_INFO_BKEY");
          }
          if (type == CSNM_ATTR_EXPTIME_INFO)
            slave_stats.attr_exptime_info_no_coll_key++;
          else
            slave_stats.attr_exptime_info_bkey_no_coll_key++;
          err = ENGINE_SUCCESS; /* okay */
        }
        b += (type == CSNM_ATTR_EXPTIME_INFO ?
          sizeof(struct cset_netmsg_exptime_info) :
          sizeof(struct cset_netmsg_exptime_info_bkey));
      }
      break;
      case CSNM_JUNKTIME:
      {
        struct cset_netmsg_junktime *j = (void*)b;
        if (rp_loglevel >= RP_LOGLEVEL_INFO)
          print_log("JUNKTIME");
        err = rp_apply_junktime(engine, j->junk_item_time);
        b += sizeof(*j);
        slave_stats.junktime++;
        if (err != ENGINE_SUCCESS) {
          slave_stats.junktime_error++;
          err = ENGINE_SUCCESS; /* okay */
        }
      }
      break;
      default:
      {
        /* FIXME.  This is a fatal error. */
        print_log("Unexpected CSNM type. type=%d", type);
      }
      break;
    }
    if (err != ENGINE_SUCCESS) {
      print_log("slave_apply_cset_netmsg failed."
        " cset_netmsg_type=%d err=0x%x", type, err);
      return false; /* Failed.  What should we do?  FIXME */
    }
  }
  return true;
}

static void
slave_flush_all(bool flush_cache)
{
  if (slave.last_coll_it) {
    item_release(engine, slave.last_coll_it);
    slave.last_coll_it = NULL;
  }
  if (flush_cache)
    item_flush_expired(engine, 0, NULL); /* empty the cache */

  key_hash_table_reset(slave.missing_ktab);
  key_buffer_reset(slave.missing_kbuf);
}

static void
slave_graceful_failover(uint64_t now)
{
  uint64_t next = slave.next_graceful_failover_done_msec;
  if (slave.async_add_master_complete && next == 0) {
    print_log("Added the server as the new master in the cache list.");
    slave.next_graceful_failover_done_msec = now;
  }
  
  /* Send the message to the master.  This is out of band.  We keep
   * sending every second it till we become the master...
   */
  if (next != 0 && now >= next) {
    slave_send_graceful_failover_done();
    slave.next_graceful_failover_done_msec = now + 1000;
  }
}    

static void *
slave_thread(void *arg)
{
  bool handshake_done = false;
  
  print_log("slave thread is running");
  while (slave.running) {
    struct slave_recv_msg *msg;
    struct netmsg_header *mm;
    bool do_ack = false;
    uint64_t now;
    
    /* Process one received message at a time.  Do not make things complicated.
     * Receive commands from the master and either ACK or NACK.
     */
    
    lock_slave_recv();
    if ((msg = TAILQ_FIRST(&slave.recv.list)) == NULL) {
      struct timespec ts;
      get_timespec(&ts, 10 /* wake up in 10msec */);
      pthread_cond_timedwait(&slave.recv.cond, &slave.recv.lock, &ts);
    }
    if ((msg = TAILQ_FIRST(&slave.recv.list))) {
      TAILQ_REMOVE(&slave.recv.list, msg, list);
      slave.recv.length--;
    }
    unlock_slave_recv();

    /* Perform periodic tasks if any */
    now = getmsec();
    /* Send the missing key message at most once per second.  FIXME */
    if (config.copy_missing_key &&
      now >= slave.missing_kmsg_last_msec + 1000) {
      slave_send_missing_keys();
      slave.missing_kmsg_last_msec = now;
    }

    /* Everything below deals with the received message.  If there are
     * none, skip.
     */
    if (msg == NULL) {
      if (slave.graceful_failover)
        slave_graceful_failover(now);
      continue;
    }

    mm = &msg->header.common;
    if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
      print_log("received one message from the master. sid=%u seq=%u",
        mm->master_sid, mm->seq);
    slave_stats.received_commands++;

#if USE_TEST_CODE
    /* Testing */
    if (slave.test.delay_response > 0)
      usleep(1000 * slave.test.delay_response);
#endif
    
    /* Sanity check sid and sequence numbers for all messages
     * except HANDSHAKE.
     */
    if (mm->type >= MASTER_NETMSG_TYPE_CSET &&
      mm->type <= MASTER_NETMSG_TYPE_MAX) {
      if (!handshake_done) {
        print_log("Has not completed handshake. NACK the message.");
        slave_send_ack(mm->master_sid, mm->seq, true /* NACK */);
        /* NACK should cause the master to re-handshake. */
        goto next;
      }
      else if (slave.master_sid != mm->master_sid) {
        print_log("Unexpected master sid. NACK the message.");
        slave_send_ack(mm->master_sid, mm->seq, true /* NACK */);
        goto next;
      }
      else if (slave.seq != mm->seq) {
        print_log("Out-of-sequence message. type=%s seq=%u"
          " expected=%u body_size=%d", netmsg_type_string[mm->type],
          mm->seq, slave.seq, msg->body_size);
        if (slave.seq > mm->seq) {
          /* Out of sequence.  We've seen this message in the past.
           * ACK the message as the master apparently has not received the ACK.
           */
          slave_send_ack(slave.master_sid, mm->seq, false /* ACK */);
          goto next;
        }
        else {
          /* slave.seq < mm->seq */
          /* Out of sequence.  We have not seen all messages that come before
           * this message.  Re-send the last ACK.
           */
          slave_send_ack(slave.master_sid, slave.seq - 1, false /* ACK */);
        }
        goto next;
      }
      else {
        /* In-sequence, expected message */
      }
    }

    if (mm->type == MASTER_NETMSG_TYPE_CSET) {
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("CSET from the master. size=%d", msg->body_size);
      if (!slave_apply_cset_netmsg(msg->body, msg->body_size)) {
        ERROR_DIE("Failed to apply change set from the master.");
        /* FIXME */
      }
      /* This pointer cache is valid only within one cset message */
      if (slave.last_coll_it) {
        item_release(engine, slave.last_coll_it);
        slave.last_coll_it = NULL;
      }
      do_ack = true;
    }
    else if (mm->type == MASTER_NETMSG_TYPE_HANDSHAKE) {
      struct master_netmsg_handshake *hs = &msg->header.hs;

      print_log("HANDSHAKE from the master");
      /* The slave handshakes with the master one or more times
       * at the beginning, but never afterwards during the slave's
       * lifetime.
       *
       * The master may handshake multiple times over time as the slave
       * restarts.
       */

      /* The very first handshake.  Remember the starting sequence. */
      if (slave.master_sid == INVALID_SID)
        slave.seq = mm->seq;
      
      if (slave.master_sid != INVALID_SID &&
        slave.master_sid != hs->master_sid) {
        /* We've seen the handshake before.  And the master
         * apparently has changed its sid, we should start over from scratch.
         * This can legitimately happen when the master queue overflows.
         */
        print_log("The master sid has changed. %s the cache and reset"
          " replication state. old_sid=%u new_sid=%u",
          (config.slave_flush_after_handshake != 0 ? "Flush" : "Do not flush"),
          slave.master_sid, hs->master_sid);
        slave_flush_all(config.slave_flush_after_handshake != 0 ? true : false);
        slave.seq = mm->seq;
        /* FIXME.  We may not want to flush the whole cache.  Keep what
         * we have and gradually fill the cache with master items.
         */
      }
      
      if (slave.seq != mm->seq) {
        /* Out of sequence.  Re-send the last response. */
        print_log("Out-of-sequence HANDSHAKE. seq=%u"
          " expected=%u", mm->seq, slave.seq);
        slave_send_handshake(slave.seq - 1);
      }
      else {
        slave.master_sid = hs->master_sid;
        slave.master_base_time = hs->time;
        slave.slave_base_time = engine->server.core->get_current_time();
        engine->config.junk_item_time = hs->junk_item_time;
        print_log("master_base_time=%u slave_base_time=%u junk_item_time=%d",
          slave.master_base_time, slave.slave_base_time,
          /* Why is junk_item_time size_t?  FIXME */
          (int)engine->config.junk_item_time);
        handshake_done = true;
        
        slave_send_handshake(mm->seq);
        slave.seq++;
      }
    }
    else if (mm->type == MASTER_NETMSG_TYPE_PING) {
      struct master_netmsg_ping *ping = &msg->header.ping;
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG) {
        print_log("PING from the master. seq=%u now=%llu", mm->seq,
          (long long unsigned)ping->now);
      }
      do_ack = true;
      slave_stats.ping++;
    }
    else if (mm->type == MASTER_NETMSG_TYPE_GRACEFUL_FAILOVER) {
      print_log("GRACEFUL_FAILOVER from the master.");

      slave.graceful_failover = true;
      
      /* Add this server as the master to the server list.
       * This task may take a while (100s of msec) because of ZK's synchronous
       * replication.  So it runs asynchronously.  When it completes, we send
       * another message to the master.
       */
      slave.async_add_master_complete = 0;
      slave.next_graceful_failover_done_msec = 0;
      engine->server.core->zk_add_master(&slave.async_add_master_complete);
      
      print_log("Enabled all user requests. Asynchronously adding the server"
        " as the new master in the cache list.");
      do_ack = true;
      /* The slave thread keeps running until arcus_zk tells us to become
       * the master.
       */
    }
    else {
      ERROR_DIE("Unknown message from the master");
    }

    if (do_ack) {
      slave_send_ack(slave.master_sid, slave.seq, false /* ACK */);
      slave.seq++;
    }

next:
    free_fixed_buffer(&slave.recv_msg_pool, msg);
  }
  print_log("slave thread has terminated");
  return NULL;
}

static int
slave_init(struct sockaddr_in *laddr, struct sockaddr_in *master_addr)
{
  print_log("slave_init");
  memset(&slave_stats, 0, sizeof(slave_stats));
  memset(&slave, 0, sizeof(slave));

  slave.listen_addr = *laddr;
  slave.master_addr = *master_addr;
  slave.seq = 0;
  slave.master_sid = INVALID_SID;
  slave.slave_sid = gen_sid();
  TAILQ_INIT(&slave.recv.list);
  pthread_mutex_init(&slave.recv.lock, NULL);
  pthread_cond_init(&slave.recv.cond, NULL);
  slave.last_coll_it = NULL;

  /* Pre-allocate send messages.  One ack per received message.  So, allocate
   * max_recv_queue_length messages.
   */
  if (0 != init_fixed_buffer_pool(&slave.send_msg_pool,
      sizeof(struct slave_msg), config.max_recv_queue_length)) {
    print_log("Failed to allocate the send message pool");
    return -1;
  }

  /* Pre-allocate receive messages */
  if (0 != init_fixed_buffer_pool(&slave.recv_msg_pool,
      sizeof(struct slave_recv_msg), config.max_recv_queue_length)) {
    print_log("Failed to allocate the receive message pool");
    return -1;
  }

  /* Message body buffer */
  slave.message_body_buffer_size = config.message_body_buffer_size;
  slave.message_body_buffer = malloc(slave.message_body_buffer_size);
  if (slave.message_body_buffer == NULL) {
    print_log("Failed to allocate the message body buffer. size=%d",
      slave.message_body_buffer_size);
    return -1;
  }

  /* A lookup table to find duplicate missing keys */
  slave.missing_ktab = key_hash_table_init(256 /* FIXME */);
  if (slave.missing_ktab == NULL) {
    print_log("Failed to create the lookup table for missing keys");
    return -1;
  }
  /* Actual storage for missing keys */
  slave.missing_kbuf = key_buffer_init(SLAVE_NETMSG_MISSING_KEY_BUFSIZE);
  if (slave.missing_kbuf == NULL) {
    print_log("Failed to create the memory buffer for missing keys");
    return -1;
  }
  slave.missing_kmsg.hdr_len = 0; /* free to use */
  slave.missing_kmsg_last_msec = 0;
  
  slave.msg_chan = start_msg_chan(&slave.listen_addr, "slave");
  if (slave.msg_chan == NULL) {
    print_log("Failed to create the message channel");
    return -1;
  }
  slave.master_ch_id = msg_chan_create_remote(slave.msg_chan, "master",
    &slave.master_addr);
  if (slave.master_ch_id < 0)
    return -1;
  
  /* A single slave thread does everything */
  pthread_attr_init(&slave.attr);
  pthread_attr_setscope(&slave.attr, PTHREAD_SCOPE_SYSTEM);
  slave.running = 1;
  pthread_create(&slave.tid, &slave.attr, slave_thread, NULL);
    
  print_log("slave_init ok");
  return 0;
}

static void
destroy_slave(void)
{
  print_log("destroy_slave");
  if (!slave.running) {
    print_log("The slave is not running.");
    return;
  }
  
  print_log("Waiting for the slave thread to stop.");
  slave.running = 0;
  pthread_cond_signal(&slave.recv.cond);
  if (0 != pthread_join(slave.tid, NULL)) {
    PERROR_DIE("pthread_join failed");
  }
  print_log("The slave thread has stopped.");
  
  print_log("Stopping the message channel.");
  msg_chan_destroy(slave.msg_chan);
  slave.msg_chan = NULL;
  slave.master_ch_id = 0;
  print_log("The message channel has stopped.");
  
  free(slave.message_body_buffer);
  slave.message_body_buffer = NULL;

  free_fixed_buffer_pool(&slave.send_msg_pool);
  free_fixed_buffer_pool(&slave.recv_msg_pool);

  /* FIXME.  Use consistent names (free, destroy, etc. are confusing). */
  key_hash_table_destroy(slave.missing_ktab);
  key_buffer_destroy(slave.missing_kbuf);
  
  print_log("The slave is now stopped.");
}

/* Raw msg_chan */
static int
msg_cb_recv_allocate(void *arg, int remote, int body_size, int user_hdr_size,
  char *user_hdr, void **cookie, char **body_buf)
{
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("msg_cb_recv_allocate. body_size=%d user_hdr_size=%d",
      body_size, user_hdr_size);

  if (mode == RP_MODE_MASTER) {
    struct master_recv_msg *msg;
    struct netmsg_header *nm;
    
    if (user_hdr_size < sizeof(*nm)) {
      print_log("Unexpected user header size. received=%d expected=%d",
        user_hdr_size, (int)sizeof(*nm));
      return -1;
    }
    /* We expect zero-sized body from the slave.  The slave sends very small
     * messages.  The only exception is missing_key.
     */
    nm = (struct netmsg_header *)user_hdr;
    if (body_size != 0 && nm->type != SLAVE_NETMSG_TYPE_MISSING_KEY) {
      print_log("Unexpected body size. received=%d expected=%d type=%d",
        body_size, 0, nm->type);
      return -1;
    }
    if (nm->type < SLAVE_NETMSG_TYPE_MIN ||
      nm->type > SLAVE_NETMSG_TYPE_MAX) {
      const char *str = "INVALID";
      if (nm->type >= MASTER_NETMSG_TYPE_MIN &&
        nm->type <= MASTER_NETMSG_TYPE_MAX)
        str = netmsg_type_string[nm->type];
      print_log("Unexpected message type from the slave. type=%u[%s]",
        nm->type, str);
      return -1;
    }
    if (user_hdr_size != netmsg_type_size[nm->type]) {
      print_log("Unexpected user header size. type=%s "
        "received=%d expected=%d", netmsg_type_string[nm->type],
        user_hdr_size, netmsg_type_size[nm->type]);
      return -1;
    }
    /* Special case MISSING_KEY.  FIXME */
    if (nm->type == SLAVE_NETMSG_TYPE_MISSING_KEY) {
      /* More sanity checks on the body size */
      if (body_size <= 0 || body_size > sizeof(master.missing_kmsg_keybuf) ||
        body_size != ((struct slave_netmsg_missing_key*)nm)->keybuf_len) {
        print_log("Unexpected body size. type=%s "
          "body_size=%d keybuf_len=%d", netmsg_type_string[nm->type],
          body_size, ((struct slave_netmsg_missing_key*)nm)->keybuf_len);
        return -1;
      }
      /* This message is out of band.  It serves as a hint.  It is okay
       * for the master to ignore it, if it has no buffers.
       */

      /* ready=false so that the master thread does not use this buffer */
      lock_master(); /* FIXME.  Use a different lock */
      master.missing_kmsg_ready = false;
      unlock_master();
      
      memcpy(&master.missing_kmsg, user_hdr, user_hdr_size);
      *body_buf = master.missing_kmsg_keybuf;
      *cookie = NULL; /* No cookies */
    }
    /* Special case GRACEFUL_FAILOVER_DONE */
    else if (nm->type == SLAVE_NETMSG_TYPE_GRACEFUL_FAILOVER_DONE) {
      *body_buf = NULL;
      *cookie = NULL;
    }
    /* Allocate a received message structure and copy the header */
    else if ((msg = alloc_fixed_buffer(&master.recv_msg_pool))) {
      memcpy(&msg->nm, user_hdr, user_hdr_size);
      *body_buf = NULL;
      *cookie = msg;
    }
    else {
      print_log("The master receive queue is full.  Drop the received message."
        " seq=%u", nm->seq);
      master_stats.recv_allocate_no_buffer++;
      /* This will reset msg_chan, so this event better be rare... */
      return -1;
    }
  }
  else if (mode == RP_MODE_SLAVE) {
    struct slave_recv_msg *msg;
    struct netmsg_header *nm;
    
    if (user_hdr_size < sizeof(*nm)) {
      print_log("Unexpected user header size. received=%d expected=%d+",
        user_hdr_size, (int)sizeof(*nm));
      return -1;
    }
    nm = (struct netmsg_header *)user_hdr;
    /* Sanity checks */
    if (nm->type < MASTER_NETMSG_TYPE_MIN ||
      nm->type > MASTER_NETMSG_TYPE_MAX) {
      const char *str = "INVALID";
      if (nm->type >= SLAVE_NETMSG_TYPE_MIN &&
        nm->type <= SLAVE_NETMSG_TYPE_MAX)
        str = netmsg_type_string[nm->type];
      print_log("Unexpected message type from the master. type=%u[%s]",
        nm->type, str);
      return -1;
    }
    if (user_hdr_size != netmsg_type_size[nm->type]) {
      print_log("Unexpected user header size. type=%s "
        "received=%d expected=%d", netmsg_type_string[nm->type],
        user_hdr_size, netmsg_type_size[nm->type]);
      return -1;
    }
    /* Only cset messages have bodies */
    if (nm->type != MASTER_NETMSG_TYPE_CSET && body_size != 0) {
      print_log("Unexpected body size. type=%s received=%d expected=%d",
        netmsg_type_string[nm->type], body_size, 0);
      return -1;
    }
    if ((msg = alloc_fixed_buffer(&slave.recv_msg_pool))) {
      memcpy(&msg->header, user_hdr, user_hdr_size);
      if (nm->type == MASTER_NETMSG_TYPE_CSET) {
        struct master_netmsg_cset *cs = (void*)nm;
        uint32_t start = cs->remote_buffer_offset;
        uint32_t end = start + body_size;

        /* Sanity check the offset */
        if (start > slave.message_body_buffer_size ||
          end > slave.message_body_buffer_size ||
          end < start) {
          print_log("Invalid remote_buffer_offset in the master CSET message."
            " remote_buffer_offset=%u body_size=%d", start, body_size);
          return -1;
        }
        /* No sanity checks whether the area [start, end) is still in use... */
        
        msg->body_size = body_size;
        msg->body = slave.message_body_buffer + start;
        *body_buf = msg->body;
      }
      else {
        msg->body_size = 0;
        msg->body = NULL;
        *body_buf = NULL;
      }
      *cookie = msg;
    }
    else {
      print_log("The slave receive queue is full.  Drop the received message."
        " seq=%u", nm->seq);
      return -1;
    }
  }
  else {
    ERROR_DIE("Program error");
  }
  return 0;
}

static int
msg_cb_recv_cancel(void *arg, void *cookie)
{
  print_log("msg_cb_recv_cancel. cookie=%p", cookie);
  if (mode == RP_MODE_MASTER) {
    if (cookie)
      free_fixed_buffer(&master.recv_msg_pool, cookie);
  }
  else if (mode == RP_MODE_SLAVE) {
    if (cookie)
      free_fixed_buffer(&slave.recv_msg_pool, cookie);
  }
  else {
    ERROR_DIE("Program error");
  }
  return 0;
}

static int
msg_cb_recv(void *arg, int remote, void *cookie, char *user_hdr,
  int user_hdr_size)
{
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("msg_cb_recv. cookie=%p", cookie);
  if (mode == RP_MODE_MASTER) {
    struct master_recv_msg *msg;
    if (cookie == NULL) {
      uint8_t type = ((struct netmsg_header *)user_hdr)->type;
      bool err = true;
      
      if (type == SLAVE_NETMSG_TYPE_MISSING_KEY
        && user_hdr_size == sizeof(struct slave_netmsg_missing_key)) {
        /* The master thread may copy the keys out of this buffer now */
        lock_master();
        master.missing_kmsg_ready = true;
        unlock_master();
        err = false;
      }
      else if (type == SLAVE_NETMSG_TYPE_GRACEFUL_FAILOVER_DONE) {
        /* Just make a note.  The master thread is polling this flag.
         * Nothing else needed.
         */
        master.graceful_failover.slave_done = true;
        err = false;
      }
      
      if (err) {
        print_log("Unexpected receive message with null cookie. type=%d"
          " user_hdr_size=%d", type, user_hdr_size);
        return -1; /* Reset.  This is fatal. */
      }
      return 0;
    }
    msg = cookie;
    lock_master();
    TAILQ_INSERT_TAIL(&master.recv_list, msg, list);
    master.recv_list_length++;
    pthread_cond_signal(&master.cond);
    unlock_master();
  }
  else if (mode == RP_MODE_SLAVE) {
    struct slave_recv_msg *msg;
    if (cookie == NULL) {
      ERROR_DIE("null cookie");
    }
    msg = cookie;
    lock_slave_recv();
    TAILQ_INSERT_TAIL(&slave.recv.list, msg, list);
    slave.recv.length++;
    pthread_cond_signal(&slave.recv.cond);
    unlock_slave_recv();
  }
  else {
    ERROR_DIE("Program error");
  }
  return 0;
}

static void
msg_cb_send_done(void *arg, struct msg_chan_sendreq_user *req)
{
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("msg_cb_send_done. req=%p", (void*)req);
  if (mode == RP_MODE_MASTER) {
    struct master_msg *msg = MASTER_MSG_FROM_SENDREQ(req);
    master_msg_queue_sent(msg);
  }
  else if (mode == RP_MODE_SLAVE) {
    struct netmsg_header *nm = (void*)req->hdr.s.user_hdr;
    if (nm->type == SLAVE_NETMSG_TYPE_MISSING_KEY) {
      /* Just mark that this message is free to use.  Don't need lock.
       * The slave thread polls.
       */
      req->hdr_len = 0;
    }
    else {
      /* Everything else (ACK, NACK, HANDSHAKE) uses slave_msg and fixed
       * buffers.
       */
      free_fixed_buffer(&slave.send_msg_pool, SLAVE_MSG_FROM_SENDREQ(req));
    }
  }
  else {
    ERROR_DIE("Program error");
  }
}

static struct msg_chan *
start_msg_chan(struct sockaddr_in *laddr, const char *id)
{
  struct msg_chan_config conf;

  conf.listen_addr = *laddr;
  conf.ping_interval = 2000; /* msec */
  conf.ping_timeout = 2000; /* msec */
  conf.listen_backlog = 16;
  conf.sndbuf = 0; /* OS default */
  conf.rcvbuf = 0;
  conf.reconnect_interval = 1000; /* msec */
  conf.connect_timeout = 500; /* msec */
  conf.handshake_timeout = 500; /* msec */
  conf.idle_timeout = 30000; /* msec */
  conf.cb_arg = NULL;
  conf.cb_recv_allocate = msg_cb_recv_allocate;
  conf.cb_recv_cancel = msg_cb_recv_cancel;
  conf.cb_recv = msg_cb_recv;
  conf.cb_send_done = msg_cb_send_done;
  
  return msg_chan_init(&conf, (char*)id, config.msgchan_loglevel);
}

char *
rp_printable_key(const char *key, int nkey)
{
  char *buf;
  const char *c;
  int i;

  buf = pthread_getspecific(rp_tls.printable_key_buf);
  if (buf == NULL) {
    buf = malloc(1024);
    if (buf == NULL)
      return "out of memory";
    pthread_setspecific(rp_tls.printable_key_buf, buf);
  }
  
  c = key;
  for (i = 0; i < nkey && i < 1024-4; i++) {
    if (isgraph(c[i]))
      buf[i] = c[i];
    else
      buf[i] = '.';
  }
  if (i == 1024-4 && nkey != 1024-4) {
    /* Indicate the key is too long and is truncated */
    buf[i++] = '#';
    buf[i++] = '#';
    buf[i++] = '#';
  }
  buf[i] = '\0';
  return buf;
}

char *
rp_printable_bkey(const unsigned char *bkey, int nbkey)
{
  char *buf;
  const unsigned char *c;
  unsigned char swapped_bkey[8];
  int i, j;

  buf = pthread_getspecific(rp_tls.printable_bkey_buf);
  if (buf == NULL) {
    buf = malloc(1024);
    if (buf == NULL)
      return "out of memory";
    pthread_setspecific(rp_tls.printable_bkey_buf, buf);
  }
  
  if (nbkey == 0) { /* 0 implies 64-bit key... */
    uint64_t w;
    nbkey = 8;
    /* swap so it's a bit easier to read */
    w = *(uint64_t*)bkey;
    for (i = 0; i < 8; i++)
      swapped_bkey[i] = (w >> 8*(7-i)) & 0xff;
    bkey = swapped_bkey;
  }
  c = bkey;
  for (i = 0, j = 0; i < nbkey && j < 1024-4; i++, j += 2) {
    /* Each byte requires two ascii chars */
    uint8_t d = c[i];
    d >>= 4;
    if (d < 10)
      buf[j] = '0' + d;
    else
      buf[j] = 'a' + d - 10;
    d = c[i];
    d = d & 0x0f;
    if (d < 10)
      buf[j+1] = '0' + d;
    else
      buf[j+1] = 'a' + d - 10;
  }
  if (j == 1024-4 && i != nbkey) {
    /* Indicate the key is too long and is truncated */
    buf[j++] = '#';
    buf[j++] = '#';
    buf[j++] = '#';
  }
  buf[j] = '\0';
  return buf;
}

static bool
must_inform_slave(hash_item *it)
{
  /* The master needs to be in SYNC or ASYNC mode.  Otherwise, the change set
   * is empty.  We never copied anything to the slave.
   * In SYNC or ASYNC mode, check if we've scanned the whole hash table or
   * if the cursor has passed the given item.  If so, we have to tell
   * the slave about this item.
   */
  return ((master.state == MASTER_STATE_RUN ||
           master.state == MASTER_STATE_ASYNC_RUN) &&
          (master.cursor_valid == false ||
           assoc_cursor_in_visited_area(&master.cursor, it)));
  /* assoc_cursor_in_visited_area computes the hash value for the item key.
   * FIXME.  Can we avoid it?  The functions in items.c have already computed
   * the hash a moment ago...
   */
}

void
rp_item_linked(hash_item *it)
{
  if (mode != RP_MODE_MASTER)
    return;
  
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_item_linked. key=%s",
      rp_printable_key(item_get_key(it), it->nkey));

  master_stats.item_link++;
  if (must_inform_slave(it))
    cset_link(it);
  else {
      /* master_thread will copy this item later on */
  }
}

void
rp_item_unlinked(hash_item *it, int cause)
{
  if (mode != RP_MODE_MASTER)
    return;
  
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_item_unlinked. key=%s iflag=0x%x cause=%d",
      rp_printable_key(item_get_key(it), it->nkey), it->iflag, cause);

  master_stats.item_unlink++;
  if (cause == RP_ITEM_UNLINKED_CAUSE_EVICT)
    master_stats.item_unlink_evict++;
  else if (cause == RP_ITEM_UNLINKED_CAUSE_EVICT_DELETE_NOW)
    master_stats.item_unlink_evict_delete_now++;
  else if (cause == RP_ITEM_UNLINKED_CAUSE_IMMEDIATE_RECYCLE) {
    master_stats.item_unlink_immediate_recycle++;
    return;
  }
  else if (cause == RP_ITEM_UNLINKED_CAUSE_REPLACE) {
    master_stats.item_unlink_replace++;
    if (config.ignore_replace_unlink) {
      master_stats.item_unlink_replace_ignored++;
      return;
    }
  }
  
  if (cause != RP_ITEM_UNLINKED_CAUSE_NORMAL &&
    cause != RP_ITEM_UNLINKED_CAUSE_REPLACE &&
    config.ignore_eviction_unlink) {
    master_stats.item_unlink_evict_ignored++;
  }
  else if (must_inform_slave(it)) {
    if (config.unlink_copy_key)
      cset_unlink_noref(it, cause);
    else
      cset_unlink(it, cause);
  }
  else {
    /* The item has not been copied to the slave.  Do nothing. */
  }
}

void
rp_btree_elem_inserted(hash_item *it, btree_elem_item *elem)
{
  if (mode != RP_MODE_MASTER)
    return;

  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_btree_elem_inserted. key=%s bkey=%s",
      rp_printable_key(item_get_key(it), it->nkey),
      rp_printable_bkey(elem->data, elem->nbkey));
  
  if (must_inform_slave(it))
    cset_btree_elem_insert(it, elem);
  master_stats.btree_elem_insert++;
}

void
rp_btree_elem_replaced(hash_item *it, btree_elem_item *old_elem,
  btree_elem_item *new_elem)
{
  if (mode != RP_MODE_MASTER)
    return;

  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_btree_elem_replaced. key=%s bkey=%s",
      rp_printable_key(item_get_key(it), it->nkey),
      rp_printable_bkey(new_elem->data, new_elem->nbkey));

  if (must_inform_slave(it))
    cset_btree_elem_replace(it, old_elem, new_elem);
  master_stats.btree_elem_replace++;
}

void
rp_btree_elem_unlinked(hash_item *it, btree_elem_item *elem)
{
  if (mode != RP_MODE_MASTER)
    return;
  /* The item is being free'd incrementally.  See collection_delete_thread. */
  if ((it->iflag & ITEM_LINKED) == 0)
    return;
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_btree_elem_unlinked. key=%s bkey=%s",
      rp_printable_key(item_get_key(it), it->nkey),
      rp_printable_bkey(elem->data, elem->nbkey));

  if (must_inform_slave(it))
    cset_btree_elem_unlink(it, elem);
  master_stats.btree_elem_unlink++;
}

void
rp_set_elem_inserted(hash_item *it, set_elem_item *elem)
{
  if (mode != RP_MODE_MASTER)
    return;

  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_set_elem_inserted. key=%s",
      rp_printable_key(item_get_key(it), it->nkey));
  
  if (must_inform_slave(it))
    cset_set_elem_insert(it, elem);
  master_stats.set_elem_insert++;
}

void
rp_set_elem_unlinked(hash_item *it, set_elem_item *elem)
{
  if (mode != RP_MODE_MASTER)
    return;
  /* The item is being free'd incrementally.  See collection_delete_thread. */
  if ((it->iflag & ITEM_LINKED) == 0)
    return;
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_set_elem_unlinked. key=%s",
      rp_printable_key(item_get_key(it), it->nkey));

  if (must_inform_slave(it))
    cset_set_elem_unlink(it, elem);
  master_stats.set_elem_unlink++;
}

void
rp_list_elem_inserted(hash_item *it, list_elem_item *elem, int idx)
{
  if (mode != RP_MODE_MASTER)
    return;

  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_list_elem_inserted. key=%s index=%d",
      rp_printable_key(item_get_key(it), it->nkey), idx);
  
  /* items.c calls this function after it links the element into the list.
   * So prev and next pointers are valid.
   */
  
  if (must_inform_slave(it))
    cset_list_elem_insert(it, elem, idx);
  master_stats.list_elem_insert++;
}

void
rp_list_elem_unlinked(hash_item *it, list_elem_item *elem, int idx)
{
  if (mode != RP_MODE_MASTER)
    return;
  /* The item is being free'd incrementally.  See collection_delete_thread. */
  if ((it->iflag & ITEM_LINKED) == 0)
    return;
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_list_elem_unlinked. key=%s index=%d",
      rp_printable_key(item_get_key(it), it->nkey), idx);

  if (must_inform_slave(it))
    cset_list_elem_unlink(it, idx);
  master_stats.list_elem_unlink++;
}

void
rp_lru_update(hash_item *it)
{
  if (mode != RP_MODE_MASTER)
    return;
  
  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("rp_lru_update. key=%s",
      rp_printable_key(item_get_key(it), it->nkey));

  if (master.state == MASTER_STATE_ASYNC_RUN && !config.lru_update_in_async)
    master_stats.lru_update_in_async_ignored++;
  else if (must_inform_slave(it))
    cset_lru_update(it);
  master_stats.lru_update++;
}

void
rp_flush(const char *prefix, int nprefix, time_t when)
{
  if (mode != RP_MODE_MASTER)
    return;
  print_log("rp_flush. prefix=%s nprefix=%d when=%d",
    prefix ? prefix : "null", nprefix, (int)when);
  /* We only support when = "right away" */
  if (when > 0)
    return;
  if (nprefix >= 0) {
    if (nprefix >= 255) {
      print_log("rp_flush. prefix is too long (255 characters).");
      return;
    }
    master_stats.flush_prefix++;
  }
  else
    master_stats.flush_all++;
  if (master.state == MASTER_STATE_RUN ||
    master.state == MASTER_STATE_ASYNC_RUN) {
    cset_flush_cmd(prefix, nprefix);
  }
}

/* Setattr.  Save the attributes before/after the command.
 * Only the master uses this.  The cache is locked, so static globals are
 * okay.  We could do this comparison in item_setattr in items.c.  Do it here
 * instead to reduce diff footprint in items.c.
 */
static bool setattr_valid = false;
static rel_time_t setattr_exptime;
static coll_meta_info setattr_info;
static bkey_t setattr_maxbkeyrange;

void
rp_setattr_start(hash_item *it, coll_meta_info *info)
{
  if (mode != RP_MODE_MASTER)
    return;
  setattr_valid = false;
  if (must_inform_slave(it)) {
    setattr_valid = true;
    setattr_exptime = it->exptime;
    if ((it->iflag & ITEM_IFLAG_COLL)) {
      setattr_info = *info; /* struct copy */
      if ((it->iflag & ITEM_IFLAG_BTREE))
        setattr_maxbkeyrange = ((btree_meta_info *)info)->maxbkeyrange;
    }
  }
}

void
rp_setattr_end(hash_item *it, coll_meta_info *info)
{
  uint8_t type = 0;
  bkey_t *r;
  if (setattr_valid) {
    if (setattr_exptime != it->exptime)
      type = CSI_ATTR_EXPTIME;
    if ((it->iflag & ITEM_IFLAG_COLL)) {
      if (setattr_info.mcnt != info->mcnt ||
        setattr_info.ovflact != info->ovflact ||
        setattr_info.mflags != info->mflags)
        type = CSI_ATTR_EXPTIME_INFO;
      if ((it->iflag & ITEM_IFLAG_BTREE)) {
        r = &(((btree_meta_info *)info)->maxbkeyrange);
        if (setattr_maxbkeyrange.len != r->len ||
          memcmp(setattr_maxbkeyrange.val, r->val, r->len) != 0)
          type = CSI_ATTR_EXPTIME_INFO_BKEY;
      }
    }
    if (type != 0) {
      master_stats.setattr++;
      cset_setattr(it, type);
    }
    setattr_valid = false;
  }
}

void
rp_set_junktime(void)
{
  if (mode != RP_MODE_MASTER)
    return;
  cset_set_junktime();
}

/* master_thread calls this to copy items from the hash table to
 * the slave.
 */
static void
copy_background(int source)
{
  hash_item *it;
  struct cset *cs = &master.cs;
  int move_now, loop;

  if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
    print_log("copy_hash_table. bg_items=%d,%d bg_bytes=%d,%d",
      master.cs.bg_items, master.msg_queue.bg_items_in_net,
      master.cs.bg_bytes, master.msg_queue.bg_bytes_in_net);
  
move_snapshot:
  /* While snapshot is in progress, pause and move items from the snapshot
   * queue to the cset.  Do NOT acquire the cache lock.
   */
  if (cs->snapshot.on) { /* this thread toggles the flag */
    cset_start_background(cs);
    if (cs->snapshot.abort) {
      master_stats.snapshot_abort++;
      /* Stop worker threads from inserting into the snapshot queue */
      cs->snapshot.on = false;
      cset_end_background(cs);
      
      cset_release_snapshot_queue(cs);
      cset_reset_snapshot_queue(cs);
      return;
    }
    while (can_copy_background() && !cs->overflow) {
      /* Move one element from the snapshot queue to the cset */
      if (0 != cset_move_snapshot_elem(cs))
        break;
    }
    if (cs->snapshot.head == cs->snapshot.tail) {
      /* Emptied the snapshot queue. */
      cs->snapshot.on = false;
    }
    cset_end_background(cs);
    
    /* Release the collection item if the snapshot is empty. */
    if (cs->snapshot.on == false) {
      lock_cache();
      rp_release_item(engine, cs->snapshot.it);
      unlock_cache();
      MASTER_RELEASE_REFERENCE();
      cset_reset_snapshot_queue(cs);
    }
    return;
  }
  
  loop = 0;
  lock_cache();
  cset_start_background(cs); /* sets a flag and also locks cset */
  while (loop < 400 /* FIXME */ &&
    can_copy_background() && !cs->snapshot.on && !cs->overflow) {
    loop++;
    
    /* Get the next item to copy. */
    it = NULL;
    if (source == COPY_BACKGROUND_SOURCE_HASH_TABLE) {    
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("cursor bucket=%d", master.cursor.bucket);
      it = assoc_cursor_next(&master.cursor);
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("cursor_next=%p bucket=%d", (void*)it, master.cursor.bucket);
      if (it == NULL) {
        print_log("Finished visiting/copying all items on the hash table.");
        master.cursor_valid = false;
        assoc_cursor_end(&master.cursor);
        break;
      }
    }
    else if (source == COPY_BACKGROUND_SOURCE_MISSING_KEYS) {
      uint16_t nkey;
      char *key;
      
      /* Pop keys from the key buffer till we find a non-null item */
      while (it == NULL &&
        0 == key_buffer_peek_head(master.missing_kbuf, &nkey, &key)) {
        struct key_hash_item *khi;
        uint32_t hash;
        
        print_log("Copy missing key. nkey=%u key=%s",
          nkey, rp_printable_key(key, nkey));
        
        it = rp_item_get_locked_noref(engine, key, nkey);
        
        hash = key_hash_table_compute_hash(nkey, key);
        khi = key_hash_table_lookup(master.missing_ktab, nkey, key, hash);
        assert(khi != NULL);
        key_hash_table_remove(master.missing_ktab, khi, hash);
        key_buffer_pop_head(master.missing_kbuf);
        
        if (it == NULL)
          master_stats.missing_keys_nonexistent++;
        else {
          /* Got a non-null item.  Try to copy this item now. */
        }
      }
      if (master.missing_kbuf->count <= 0)
        print_log("Finished visiting/copying all missing keys");
      if (it == NULL)
        break;
    }
    
    /* Do not copy invalid (expired, deleted) items */
    if (rp_item_invalid(engine, it))
      continue;
    
    cset_link(it);
    if ((it->iflag & ITEM_IFLAG_COLL) == 0) {
      /* Simple key value */
      if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
        print_log("copy_hash_table: simple");
    }
    else {
      /* Collection item */
      
      /* Scan the whole btree/set/list with the cache lock acquired */
      if ((it->iflag & ITEM_IFLAG_BTREE)) {
        btree_meta_info *info = (btree_meta_info*)item_get_meta(it);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("copy_hash_table: btree ccnt=%u", info->ccnt);
        if (info->ccnt > 0) {
          cset_start_snapshot(cs, it);
          rp_btree_snapshot(engine, it);
          cset_end_snapshot(cs, it);
        }
      }
      else if ((it->iflag & ITEM_IFLAG_SET)) {
        set_meta_info *info = (set_meta_info*)item_get_meta(it);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("copy_hash_table: set ccnt=%u", info->ccnt);
        if (info->ccnt > 0) {
          cset_start_snapshot(cs, it);
          rp_set_snapshot(engine, it);
          cset_end_snapshot(cs, it);
        }
      }
      else if ((it->iflag & ITEM_IFLAG_LIST)) {
        list_meta_info *info = (list_meta_info*)item_get_meta(it);
        if (rp_loglevel >= RP_LOGLEVEL_DEBUG)
          print_log("copy_hash_table: list ccnt=%u", info->ccnt);
        if (info->ccnt > 0) {
          cset_start_snapshot(cs, it);
          rp_list_snapshot(engine, it);
          cset_end_snapshot(cs, it);
        }
      }
      else {
        ERROR_DIE("UNEXPECTED COLLECTION TYPE");
      }
    }
  }
  move_now = (cs->snapshot.on && can_copy_background());
  cset_end_background(cs);
  unlock_cache();
  if (move_now)
    goto move_snapshot;
}

static int
config_key_val(struct rp_config *o, int line_num, char *key, char *val)
{
  int err = 0;

  if (strcmp(key, "master_addr") == 0) {
    struct sockaddr_in addr;
    if (0 != parse_hostport(val, &addr, NULL)) {
      print_log("Invalid master_addr. line=%d value=%s", line_num, val);
      err = -1;
    }
    else {
      if (o->master_addr_str)
        free(o->master_addr_str);
      o->master_addr_str = strdup(val);
    }
  }
  else if (strcmp(key, "slave_addr") == 0) {
    struct sockaddr_in addr;
    if (0 != parse_hostport(val, &addr, NULL)) {
      print_log("Invalid slave_addr. line=%d value=%s", line_num, val);
      err = -1;
    }
    else {
      if (o->slave_addr_str)
        free(o->slave_addr_str);
      o->slave_addr_str = strdup(val);
    }
  }
  else if (strcmp(key, "mode") == 0) {
    if (strcmp(val, "master") == 0)
      o->mode = RP_MODE_MASTER;
    else if (strcmp(val, "slave") == 0)
      o->mode = RP_MODE_SLAVE;
    else if (strcmp(val, "disabled") == 0)
      o->mode = RP_MODE_DISABLED;
    else {
      print_log("Invalid mode. It must be master, slave, or disabled. "
        "line=%d value=%s", line_num, val);
      err = -1;
    }
  }
  else if (strcmp(key, "loglevel") == 0) {
    int n = atoi(val);
    if (n < 0 || n > RP_LOGLEVEL_DEBUG) {
      print_log("Invalid loglevel. It must be [0...3]. line=%d value=%d",
        line_num, n);
      err = -1;
    }
    else {
      o->loglevel = n;
    }
  }
  else if (strcmp(key, "msgchan_loglevel") == 0) {
    int n = atoi(val);
    if (n < 0 || n > MSG_CHAN_LOGLEVEL_DEBUG) {
      print_log("Invalid msgchan_loglevel. It must be [0...3]."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->msgchan_loglevel = n;
    }
  }
  else if (strcmp(key, "server_loglevel") == 0) {
    int n = atoi(val);
    if (n < 0 || n > RP_LOGLEVEL_DEBUG) {
      print_log("Invalid server_loglevel. It must be [0...3]. line=%d value=%d",
        line_num, n);
      err = -1;
    }
    else {
      o->server_loglevel = n;
    }
  }
  else if (strcmp(key, "engine_loglevel") == 0) {
    int n = atoi(val);
    if (n < 0 || n > RP_LOGLEVEL_DEBUG) {
      print_log("Invalid engine_loglevel. It must be [0...3]. line=%d value=%d",
        line_num, n);
      err = -1;
    }
    else {
      o->engine_loglevel = n;
    }
  }
  else if (strcmp(key, "max_unack") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid max_unack. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->max_unack = n;
    }
  }
  else if (strcmp(key, "max_background_items") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid max_background_items. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->max_background_items = n;
    }
  }
  else if (strcmp(key, "max_background_bytes") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid max_background_bytes. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->max_background_bytes = n;
    }
  }
  else if (strcmp(key, "max_sync_wait_msec") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid max_sync_wait_msec. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->max_sync_wait_msec = n;
    }
  }
  else if (strcmp(key, "replication_hiwat_msec") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid replication_hiwat_msec. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->replication_hiwat_msec = n;
    }
  }
  else if (strcmp(key, "replication_lowat_msec") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid replication_lowat_msec. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->replication_lowat_msec = n;
    }
  }
  else if (strcmp(key, "max_msg_queue_length") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid max_msg_queue_length. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->max_msg_queue_length = n;
    }
  }
  else if (strcmp(key, "max_recv_queue_length") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid max_recv_queue_length. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->max_recv_queue_length = n;
    }
  }
  else if (strcmp(key, "message_body_buffer_size") == 0) {
    int n = atoi(val);
    if (n <= o->message_body_buffer_size /* min */) {
      print_log("Invalid message_body_buffer_size. It must be greater than %d."
        " line=%d value=%d", o->message_body_buffer_size, line_num, n);
      err = -1;
    }
    else {
      o->message_body_buffer_size = n;
    }
  }
  else if (strcmp(key, "ignore_eviction_unlink") == 0) {
    int n = atoi(val);
    o->ignore_eviction_unlink = (n != 0);
  }
  else if (strcmp(key, "ignore_replace_unlink") == 0) {
    int n = atoi(val);
    o->ignore_replace_unlink = (n != 0);
  }
  else if (strcmp(key, "unlink_copy_key") == 0) {
    int n = atoi(val);
    o->unlink_copy_key = (n != 0);
  }
  else if (strcmp(key, "no_sync_mode") == 0) {
    int n = atoi(val);
    o->no_sync_mode = (n != 0);
  }
  else if (strcmp(key, "notify_io_complete_thread") == 0) {
    int n = atoi(val);
    o->notify_io_complete_thread = (n != 0);
  }
  else if (strcmp(key, "cset_buffer_size") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid cset_buffer_size. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->cset_buffer_size = n;
    }
  }
  else if (strcmp(key, "cset_snapshot_queue_size") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid cset_snapshot_queue_size. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->cset_snapshot_queue_size = n;
    }
  }
  else if (strcmp(key, "cset_max_slab_memory") == 0) {
    int n = atoi(val);
    if (n < 0 || n > 100) {
      print_log("Invalid cset_max_slab_memory (percent). It must be at least"
        " 0 and at most 100. 0 means no limit. line=%d value=%d",
        line_num, n);
      err = -1;
    }
    else {
      o->cset_max_slab_memory = n;
    }
  }
  else if (strcmp(key, "copy_missing_key") == 0) {
    int n = atoi(val);
    o->copy_missing_key = (n != 0);
  }
  else if (strcmp(key, "lru_update_in_async") == 0) {
    int n = atoi(val);
    o->lru_update_in_async = (n != 0);
  }
  else if (strcmp(key, "slave_flush_after_handshake") == 0) {
    int n = atoi(val);
    o->slave_flush_after_handshake = (n != 0);
  }
  else if (strcmp(key, "async_to_sync_consecutive_acks") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid async_to_sync_consecutive_acks. It must be greater"
        " than 0. line=%d value=%d",
        line_num, n);
      err = -1;
    }
    else {
      o->async_to_sync_consecutive_acks = n;
    }
  }
  else if (strcmp(key, "async_to_sync_min_duration") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid async_to_sync_min_duration. It must be greater"
        " than 0. line=%d value=%d",
        line_num, n);
      err = -1;
    }
    else {
      o->async_to_sync_min_duration = n;
    }
  }
  else if (strcmp(key, "async_to_sync_max_cset_items") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid async_to_sync_max_cset_items. It must be greater"
        " than 0. line=%d value=%d",
        line_num, n);
      err = -1;
    }
    else {
      o->async_to_sync_max_cset_items = n;
    }
  }
  else if (strcmp(key, "gc_enable") == 0) {
    int n = atoi(val);
    o->gc_enable = (n != 0);
  }
  else if (strcmp(key, "gc_max_reclaim") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid gc_max_reclaim. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->gc_max_reclaim = n;
    }
  }
  else if (strcmp(key, "gc_thresh_idle_time") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid gc_thresh_idle_time. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->gc_thresh_idle_time = n;
    }
  }
  else if (strcmp(key, "gc_thresh_memory") == 0) {
    int n = atoi(val);
    if (n <= 0 || n >= 100) {
      print_log("Invalid gc_thresh_memory (percent). It must be greater than"
        " 0 and smaller than 100. line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->gc_thresh_memory = n;
    }
  }
  else if (strcmp(key, "gc_thresh_recov_time") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid gc_thresh_recov_time. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->gc_thresh_recov_time = n;
    }
  }
  else if (strcmp(key, "gc_thresh_recov_memory") == 0) {
    int n = atoi(val);
    if (n < 0 || n > 100) {
      print_log("Invalid gc_thresh_recov_memory (percent). It must be between"
        " 0 and 100. line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->gc_thresh_recov_memory = n;
    }
  }
  else if (strcmp(key, "gc_min_saved_bytes") == 0) {
    int n = atoi(val);
    if (n <= 0) {
      print_log("Invalid gc_min_saved_bytes. It must be greater than 0."
        " line=%d value=%d", line_num, n);
      err = -1;
    }
    else {
      o->gc_min_saved_bytes = n;
    }
  }
  else {
    print_log("Unknown key. line=%d key=%s", line_num, key);
    err = -1;
  }
  
  return err;
}

static int
config_line(struct rp_config *o, int line_num, char line[])
{
  char *c;
  int found_key = 0, empty = 1;
  
  //printf("config line %d: %s\n", line_num, line);
  
  // key=value
  // no spaces in key names, no spaces between key and =
  // everything that follows = is the value, including spaces
  
  c = line;
  if (*c == '#') {
    // comment line
    return 0;
  }
  while (*c != '\0') {
    if (*c != ' ' && *c != '\t')
      empty = 0;
    if (*c == '=') {
      char *key = line;
      char *val = c+1;
      *c = '\0';
      found_key = 1;
      if (0 != config_key_val(o, line_num, key, val)) {
        return -1;
      }
      break;
    }
    c++;
  }
  
  if (!empty && !found_key) {
    print_log("Invalid line in the configuration file. line=%d", line_num);
    return -1;
  }
  
  return 0;
}

static int
config_read(struct rp_config *o, const char *path)
{
  int fd, s, i, line_e, line_num;
  char buf[1024], line[1024];
  
  fd = open(path, O_RDONLY);
  if (fd < 0) {
    ERRLOG(errno, "Cannot open configuration. path=%s", path);
    return -1;
  }

  line_num = 0;
  line_e = 0;
  while ((s = read(fd, buf, sizeof(buf))) > 0) {
    for (i = 0; i < s; i++) {
      if (line_e >= sizeof(line)) {
        print_log("A line in the configuration file is too long. line=%d",
          line_num);
        return -1;
      }
      line[line_e++] = buf[i];
      if (buf[i] == '\r') {
        line_e--;
      }
      else if (buf[i] == '\n') {
        line_e--;
        line[line_e] = '\0';
        
        // got a line
        line_num++;
        line_e = 0;
        if (0 != config_line(o, line_num, line)) {
          return -1;
        }
      }
    }
  }

  if (line_e > 0) {
    // last line without newline char
    line_num++;
    if (0 != config_line(o, line_num, line)) {
      return -1;
    }
  }
  return 0;
}

void
rp_stats(struct default_engine *engine, ADD_STAT add_stat, const void *cookie)
{
  char val[128];
  int len;

#if defined(STAT_32_N) || defined(STAT_64_N) || defined(STAT_32) || defined(STAT_64) || defined(STAT_32_SIGNED) || defined(STAT_32_N_SIGNED)
#error "already defined"
#endif
#define STAT_32_N_SIGNED(_name, _var) do {                             \
    len = sprintf(val, "%d", (unsigned)_var);                          \
    add_stat(_name, strlen(_name), val, len, cookie);                  \
  } while(0)
#define STAT_32_N(_name, _var) do {                                    \
    len = sprintf(val, "%u", (unsigned)_var);                          \
    add_stat(_name, strlen(_name), val, len, cookie);                  \
  } while(0)
#define STAT_64_N(_name, _var) do {                                   \
    len = sprintf(val, "%llu", (long long unsigned)_var);             \
    add_stat(_name, strlen(_name), val, len, cookie);                 \
  } while(0)
#define STAT_32_SIGNED(_var) STAT_32_N_SIGNED(#_var, _var)
#define STAT_32(_var) STAT_32_N(#_var, _var)
#define STAT_64(_var) STAT_64_N(#_var, _var)
    
  STAT_32(config.loglevel);
  STAT_32_SIGNED(config.msgchan_loglevel);
  STAT_32_SIGNED(config.server_loglevel);
  STAT_32_SIGNED(config.engine_loglevel);
  STAT_32(config.max_unack);
  STAT_32(config.max_background_items);
  STAT_32(config.max_background_bytes);
  STAT_32(config.max_sync_wait_msec);
  STAT_32(config.replication_hiwat_msec);
  STAT_32(config.replication_lowat_msec);
  STAT_32(config.max_msg_queue_length);
  STAT_32(config.max_recv_queue_length);
  STAT_32(config.message_body_buffer_size);
  STAT_32(config.ignore_eviction_unlink);
  STAT_32(config.ignore_replace_unlink);
  STAT_32(config.max_retransmission_burst);
  STAT_32(config.unlink_copy_key);
  STAT_32(config.no_sync_mode);
  STAT_32(config.notify_io_complete_thread);
  STAT_32(config.cset_buffer_size);
  STAT_32(config.cset_snapshot_queue_size);
  STAT_32(config.cset_max_slab_memory);
  STAT_64_N("config.cset_max_slab_memory_bytes", master.cs.max_slab_bytes);
  STAT_32(config.copy_missing_key);
  STAT_32(config.lru_update_in_async);
  STAT_32(config.slave_flush_after_handshake);
  STAT_32(config.async_to_sync_consecutive_acks);
  STAT_32(config.async_to_sync_min_duration);
  STAT_32(config.async_to_sync_max_cset_items);
  STAT_32(config.gc_enable);
  STAT_32(config.gc_thresh_memory);
  STAT_32(config.gc_thresh_idle_time);
  STAT_32(config.gc_max_reclaim);
  STAT_32(config.gc_thresh_recov_memory);
  STAT_32(config.gc_thresh_recov_time);
  STAT_32(config.gc_min_saved_bytes);
  
  if (mode == RP_MODE_DISABLED)
    add_stat("mode", 4, "disabled", 8, cookie);
  else if (mode == RP_MODE_MASTER) {
    const char *yesno;
    const char *gc_state = gc_state_str[master.gc.state];
    
    add_stat("mode", 4, "master", 6, cookie);
    add_stat("gc_state", 8, gc_state, strlen(gc_state), cookie);
    STAT_32_N("gc_count", master_stats.gc_count);
    yesno = engine->assoc.cursor_blocking_expansion ? "yes" : "no";
    add_stat("cursor_blocking_hash_table_expansion", 36,
      yesno, strlen(yesno), cookie);
    STAT_32_N("cursors_in_use", engine->assoc.cursors_in_use);
    add_stat("state", 5, master_state_str[master.state],
      strlen(master_state_str[master.state]), cookie);
    add_stat("graceful_failover.state", 23,
      graceful_failover_state_str[master.graceful_failover.state],
      strlen(graceful_failover_state_str[master.graceful_failover.state]),
      cookie);

    STAT_64_N("cset_slab_memory_bytes", master.cs.slab_bytes);
    STAT_32_N("cset_count", master.cs.count);
    STAT_32_N("cset_bg_items", master.cs.bg_items);
    STAT_32_N("cset_bg_bytes", master.cs.bg_bytes);
    STAT_32_N("message_queue_bg_items", master.msg_queue.bg_items_in_net);
    STAT_32_N("message_queue_bg_bytes", master.msg_queue.bg_bytes_in_net);
    STAT_32_N("message_queue_length", master.msg_queue.count);
    STAT_32_N("blocked_requests", master.waiter_count);

#ifdef MASTER_STAT_64
#error "already defined"
#endif
#define MASTER_STAT_64(_name) STAT_64_N(#_name, master_stats. _name)
    
    MASTER_STAT_64(queued_messages);
    MASTER_STAT_64(msg_queue_alloc_cset);
    MASTER_STAT_64(msg_queue_alloc_cset_ok);
    MASTER_STAT_64(msg_queue_alloc_cset_no_message);
    MASTER_STAT_64(msg_queue_alloc_cset_no_buffer);
    MASTER_STAT_64(pushed_cset_item);
    MASTER_STAT_64(sent_message);
    MASTER_STAT_64(retransmission);
    MASTER_STAT_64(ackd_message);
    MASTER_STAT_64(start_nack);
    MASTER_STAT_64(slave_nack);
    MASTER_STAT_64(cset_overflow);
    MASTER_STAT_64(snapshot_queue_overflow);
    MASTER_STAT_64(item_link);
    MASTER_STAT_64(item_unlink);
    MASTER_STAT_64(item_unlink_evict);
    MASTER_STAT_64(item_unlink_evict_delete_now);
    MASTER_STAT_64(item_unlink_evict_ignored);
    MASTER_STAT_64(item_unlink_immediate_recycle);
    MASTER_STAT_64(item_unlink_replace);
    MASTER_STAT_64(item_unlink_replace_ignored);
    MASTER_STAT_64(btree_elem_insert);
    MASTER_STAT_64(btree_elem_replace);
    MASTER_STAT_64(btree_elem_unlink);
    MASTER_STAT_64(set_elem_insert);
    MASTER_STAT_64(set_elem_unlink);
    MASTER_STAT_64(list_elem_insert);
    MASTER_STAT_64(list_elem_unlink);
    MASTER_STAT_64(snapshot_abort);
    MASTER_STAT_64(lru_update);
    MASTER_STAT_64(lru_update_in_async_ignored);
    MASTER_STAT_64(flush_prefix);
    MASTER_STAT_64(flush_all);
    MASTER_STAT_64(setattr);
    MASTER_STAT_64(refcount_up);
    MASTER_STAT_64(refcount_down);
    MASTER_STAT_64(recv_allocate_no_buffer);
    MASTER_STAT_64(missing_keys_table_full);
    MASTER_STAT_64(missing_keys_buffer_full);
    MASTER_STAT_64(missing_keys_insert);
    MASTER_STAT_64(missing_keys_nonexistent);
    STAT_32_N("missing_keys_current_count", master.missing_kbuf->count);   
    MASTER_STAT_64(coll_snapshot);
    MASTER_STAT_64(snapshot_duration_max);
    if (master_stats.snapshot_duration_min == 0) {
      add_stat("snapshot_duration_min", 21, "not set", 7, cookie);
    }
    else {
      STAT_32_N("snapshot_duration_min", master_stats.snapshot_duration_min-1);
    }
    MASTER_STAT_64(snapshot_duration_avg);
    MASTER_STAT_64(snapshot_ccnt_max);
    MASTER_STAT_64(snapshot_ccnt_avg);
    MASTER_STAT_64(clients_waited_too_long);
    MASTER_STAT_64(block_client);
    MASTER_STAT_64(wakeup_client);
    MASTER_STAT_64(thread_sleep);
    MASTER_STAT_64(snapshot_cset_item);
    MASTER_STAT_64(snapshot_queue_cset_item);
    MASTER_STAT_64(ping);
    MASTER_STAT_64(ack_latency_usec);
#undef MASTER_STAT_64
  }
  else if (mode == RP_MODE_SLAVE) {
    add_stat("mode", 4, "slave", 5, cookie);
    len = sprintf(val, "%llu",
      (long long unsigned)slave_stats.received_commands);
    add_stat("received_commmands", 18, val, len, cookie);

#ifdef SLAVE_STAT_64
#error "already defined"
#endif
#define SLAVE_STAT_64(_name) STAT_64_N(#_name, slave_stats. _name)

    SLAVE_STAT_64(simple_item_link);
    SLAVE_STAT_64(simple_item_link_error);
    SLAVE_STAT_64(btree_item_link);
    SLAVE_STAT_64(btree_item_link_replace);
    SLAVE_STAT_64(btree_item_link_error);
    SLAVE_STAT_64(set_item_link);
    SLAVE_STAT_64(set_item_link_replace);
    SLAVE_STAT_64(set_item_link_error);
    SLAVE_STAT_64(list_item_link);
    SLAVE_STAT_64(list_item_link_replace);
    SLAVE_STAT_64(list_item_link_error);
    SLAVE_STAT_64(item_unlink);
    SLAVE_STAT_64(item_unlink_evict);
    SLAVE_STAT_64(item_unlink_error);
    SLAVE_STAT_64(coll_key);
    SLAVE_STAT_64(coll_key_error);
    SLAVE_STAT_64(btree_elem_insert);
    SLAVE_STAT_64(btree_elem_insert_error);
    SLAVE_STAT_64(btree_elem_unlink);
    SLAVE_STAT_64(btree_elem_unlink_error);
    SLAVE_STAT_64(btree_elem_no_coll_key);
    SLAVE_STAT_64(set_elem_insert);
    SLAVE_STAT_64(set_elem_insert_error);
    SLAVE_STAT_64(set_elem_unlink);
    SLAVE_STAT_64(set_elem_unlink_error);
    SLAVE_STAT_64(set_elem_no_coll_key);
    SLAVE_STAT_64(list_elem_insert);
    SLAVE_STAT_64(list_elem_insert_error);
    SLAVE_STAT_64(list_elem_unlink);
    SLAVE_STAT_64(list_elem_unlink_error);
    SLAVE_STAT_64(list_elem_no_coll_key);
    SLAVE_STAT_64(lru_update);
    SLAVE_STAT_64(lru_update_error);
    SLAVE_STAT_64(flush);
    SLAVE_STAT_64(flush_error);
    SLAVE_STAT_64(flush_prefix);
    SLAVE_STAT_64(flush_prefix_error);
    SLAVE_STAT_64(attr_exptime);
    SLAVE_STAT_64(attr_exptime_error);
    SLAVE_STAT_64(attr_exptime_info);
    SLAVE_STAT_64(attr_exptime_info_error);
    SLAVE_STAT_64(attr_exptime_info_no_coll_key);
    SLAVE_STAT_64(attr_exptime_info_bkey);
    SLAVE_STAT_64(attr_exptime_info_bkey_error);
    SLAVE_STAT_64(attr_exptime_info_bkey_no_coll_key);
    SLAVE_STAT_64(junktime);
    SLAVE_STAT_64(junktime_error);
    SLAVE_STAT_64(ping);
#undef SLAVE_STAT_64
    
#undef STAT_32
#undef STAT_64
#undef STAT_32_N
#undef STAT_64_N
#undef STAT_32_SIGNED
#undef STAT_32_N_SIGNED
  }
}

ENGINE_ERROR_CODE
rp_user_cmd(const char *cmd)
{
  print_log("rp_user_cmd. cmd=%s", cmd == NULL ? "null" : cmd);
  if (cmd == NULL) {
  }
  else if (0 == strcmp(cmd, "graceful-failover")) {
    /* This command is intentionally long to force the user to be extra
     * cautious.
     */
    if (mode == RP_MODE_MASTER) {
      /* Set a flag and return.  The master thread picks up the flag and
       * starts the failover procedure.
       */
      master.graceful_failover.state = GRACEFUL_FAILOVER_STATE_START;
      print_log("Graceful failover requested.");
    }
    else {
      print_log("Cannot perform graceful-failover on the slave.");
    }
  }
#if USE_TEST_CODE
  else if (0 == strcmp(cmd, "delay600")) {
    /* Delay slave's reponse by 600msec.  This is for testing. */
    slave.test.delay_response = 600;
  }
  else if (0 == strcmp(cmd, "delay0")) {
    slave.test.delay_response = 0;
  }
  else if (0 == strcmp(cmd, "master_mode")) {
    /* Manually test failover.
     * Assume this server is currently the slave.
     * It becomes the master.
     *
     * slave.listen_addr = this server's listen address, read from ZK
     * The new slave's address = config.slave_addr.
     * See rp_standalone.
     */
    print_log("Test. Switch to the master mode.");
    if (mode != RP_MODE_SLAVE) {
      print_log("This server is not the slave.  Abort the attempt.");
    }
    else {
      struct sockaddr_in addr = slave.listen_addr;
      print_log("Calling set_mode");
      if (0 != set_mode(RP_MODE_MASTER, &addr, NULL)) {
        print_log("set_mode failed. mode=%s", mode_string[mode]);
        return ENGINE_FAILED;
      }
      print_log("set_mode succeeded");
      
      if (0 != parse_hostport(config.slave_addr_str, &addr, NULL))  {
        print_log("Failed to parse the slave address.");
        return ENGINE_FAILED;
      }
      print_log("Calling set_slave. slave_address=%s", config.slave_addr_str);
      master.test_force_slave_msg_chan_id = true;
      if (0 != set_slave(&addr)) {
        print_log("set_slave failed. addr=%s", config.slave_addr_str);
        return ENGINE_FAILED;
      }
      print_log("set_slave succeeded");
    }
  }
  else if (0 == strcmp(cmd, "snapshot_abort")) {
    /* Manually test snapshot abort... */
    if (mode == RP_MODE_MASTER) {
      cset_lock(&master.cs);
      if (master.cs.snapshot.on) {
        master.cs.snapshot.abort = true;
      }
      cset_unlock(&master.cs);
    }
  }
  else if (0 == strcmp(cmd, "snapshot_queue_overflow")) {
    /* Manually test snapshot abort... */
    if (mode == RP_MODE_MASTER) {
      cset_lock(&master.cs);
      master.cs.test_force_snapshot_queue_overflow = true;
      cset_unlock(&master.cs);
    }
  }
  else if (0 == strcmp(cmd, "snapshot_move_overflow")) {
    /* Fail cset_move_snapshot_elem... */
    if (mode == RP_MODE_MASTER) {
      cset_lock(&master.cs);
      master.cs.test_force_snapshot_move_overflow = true;
      cset_unlock(&master.cs);
    }
  }
  else if (0 == strcmp(cmd, "snapshot_move_overflow_tail")) {
    /* Fail cset_move_snapshot_elem... */
    if (mode == RP_MODE_MASTER) {
      cset_lock(&master.cs);
      master.cs.test_force_snapshot_move_overflow_tail = true;
      cset_unlock(&master.cs);
    }
  }
#endif
  else if (0 == strcmp(cmd, "dumpkey")) {
    /* Dpump all keys to a file */
    struct assoc *assoc = &engine->assoc;
    int fd, n;
    uint32_t i, hashsize;
    static char buf[1024*1024];
    char *c;
    sprintf(buf, "keydump.%d", getpid());
    fd = open(buf, O_WRONLY | O_CREAT | O_TRUNC,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd < 0) {
      print_log("Failed to open the dump file. path=%s err=%s",
        buf, strerror(errno));
      return ENGINE_SUCCESS;
    }
    n = 0;
    c = buf;
    lock_cache();
    if (assoc->expanding) {
      unlock_cache();
      close(fd);
      print_log("dumpkey. The hash table is expanding. Retry later.");
      return ENGINE_SUCCESS;
    }
    hashsize = 1 << assoc->hashpower; /* FIXME */
    for (i = 0; i <= hashsize; i++) {
      hash_item *it = assoc->primary_hashtable[i];
      while (it) {
        if (rp_item_invalid(engine, it)) {
          /* Skip expired items */
        }
        else {
          const char *k = item_get_key(it);
          int nkey = it->nkey;
          if (n + nkey + sizeof(nkey) > sizeof(buf)) {
            write(fd, buf, n);
            n = 0;
            c = buf;
          }
          if (n + nkey + sizeof(nkey) > sizeof(buf)) {
            print_log("dumpkey. Key is too long. nkey=%d", nkey);
          }
          else {
            /* 32-bit nkey, 8-bit iflag, the key */
            *(int*)c = nkey;
            c += sizeof(nkey);
            *(char*)c = (it->iflag & ITEM_IFLAG_COLL);
            c += 1;
            memcpy(c, k, nkey);
            c += nkey;
            n += sizeof(nkey) + 1 + nkey;
          }
        }
        it = it->h_next;
      }
    }
    unlock_cache();
    if (n > 0)
      write(fd, buf, n);    
    /* Write 32-bit 0 at the end */
    n = 0;
    write(fd, &n, sizeof(n));
    close(fd);
  }
  return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
rp_standalone(void)
{
  print_log("rp_standalone");
  if (config.mode != RP_MODE_DISABLED) {
    if (config.mode == RP_MODE_MASTER) {
      struct sockaddr_in addr;
      
      /* Set the mode and the listen address */
      print_log("master_listen_address=%s", config.master_addr_str);
      if (0 != parse_hostport(config.master_addr_str, &addr, NULL))  {
        print_log("Failed to parse the master listen address.");
        return ENGINE_FAILED;
      }
      if (0 != set_mode(config.mode, &addr, NULL)) {
        print_log("set_mode failed. mode=%s", mode_string[config.mode]);
        return ENGINE_FAILED;
      }

      /* Set the slave address */
      print_log("slave_address=%s", config.slave_addr_str);
      if (0 != parse_hostport(config.slave_addr_str, &addr, NULL))  {
        print_log("Failed to parse the slave address.");
        return ENGINE_FAILED;
      }
      if (0 != set_slave(&addr)) {
        print_log("set_slave failed. addr=%s", config.slave_addr_str);
        return ENGINE_FAILED;
      }
    }
    else {
      struct sockaddr_in laddr, master_addr;
      
      /* Set the mode and the listen address */
      print_log("slave_address=%s", config.slave_addr_str);
      if (0 != parse_hostport(config.slave_addr_str, &laddr, NULL))  {
        print_log("Failed to parse the slave address.");
        return ENGINE_FAILED;
      }
      print_log("master_address=%s", config.master_addr_str);
      if (0 != parse_hostport(config.master_addr_str, &master_addr, NULL))  {
        print_log("Failed to parse the master address.");
        return ENGINE_FAILED;
      }
      if (0 != set_mode(config.mode, &laddr, &master_addr)) {
        print_log("set_mode failed. mode=%s", mode_string[config.mode]);
        return ENGINE_FAILED;
      }
    }
    
    print_log("Set the replication mode. mode=%s", mode_string[mode]);
  }
  else {
    print_log("mode=RP_MODE_DISABLED. No replication.");
  }
  return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
rp_master_mode(const char *laddr)
{
  struct sockaddr_in addr;
  
  print_log("rp_master_mode. laddr=%s", laddr ? laddr : "null");
  if (mode == RP_MODE_MASTER) {
    print_log("The server is already in the master mode.");
    return ENGINE_SUCCESS;
  }
  if (laddr == NULL || 0 != parse_hostport(laddr, &addr, NULL)) {
    print_log("Invalid listen address for the master.");
    return ENGINE_FAILED;
  }
  if (0 != set_mode(RP_MODE_MASTER, &addr, NULL)) {
    print_log("Cannot start the master mode.");
    return ENGINE_FAILED;
    /* Should die?  FIXME */
  }
  print_log("The server has started the master mode.");
  return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
rp_slave_mode(const char *saddr, const char *maddr)
{
  struct sockaddr_in slave, master;
  
  print_log("rp_slave_mode. saddr=%s maddr=%s", saddr ? saddr : "null",
    maddr ? maddr : "null");
  if (mode == RP_MODE_SLAVE) {
    print_log("The server is already in the slave mode.");
    return ENGINE_SUCCESS;
  }
  if (saddr == NULL || 0 != parse_hostport(saddr, &slave, NULL)) {
    print_log("Invalid listen address for the slave.");
    return ENGINE_FAILED;
  }
  if (maddr == NULL || 0 != parse_hostport(maddr, &master, NULL)) {
    print_log("Invalid listen address for the master.");
    return ENGINE_FAILED;
  }
  if (0 != set_mode(RP_MODE_SLAVE, &slave, &master)) {
    print_log("Cannot start the slave mode.");
    return ENGINE_FAILED;
    /* Should die?  FIXME */
  }
  print_log("The server has started the slave mode.");
  return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
rp_slave_addr(const char *addr)
{
  struct sockaddr_in *slave, saddr;
  
  print_log("rp_slave_addr. addr=%s", addr ? addr : "null");

  if (addr != NULL && addr[0] != '\0') {
    if (0 != parse_hostport(addr, &saddr, NULL)) {
      print_log("Invalid listen address for the slave.");
      return ENGINE_FAILED;
    }
    slave = &saddr;
  }
  else {
    /* Okay, there are just no slaves. */
    slave = NULL;
  }
  if (0 != set_slave(slave)) {
    print_log("Failed to set the slave address.");
    return ENGINE_FAILED;
    /* Should die?  FIXME */
  }
  return ENGINE_SUCCESS;
}
