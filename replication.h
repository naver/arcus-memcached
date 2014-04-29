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
#ifndef _REPLICATION_H_
#define _REPLICATION_H_

#define RP_LOGLEVEL_DEBUG  3
#define RP_LOGLEVEL_INFO   2
#define RP_LOGLEVEL_WARN   1
#define RP_LOGLEVEL_FATAL  0
extern int rp_loglevel;

#define RP_SEQ_NULL 0

/* The engine calls this function during initialization.  It initializes
 * master/slave and starts necessary threads.
 */
int rp_init(void *engine);

/* A memcached worker thread calls this function to get the most recent
 * change set item's sequence number.  The caller must acquire the cache
 * lock first.  This function's use is very restricted.  See btree_elem_get.
 */
uint64_t rp_seq(void);

/* A memcached worker thread calls this function to wait for replication
 * to complete, with the cache lock released.  It never blocks.  If it needs
 * to block, it returns WOULDBLOCK.  Later a different thread calls
 * notify_io_complete.
 */
int rp_wait(const void *cookie, int orig_ret);
#define RP_WAIT_ERROR      -1
#define RP_WAIT_COMPLETED   0
#define RP_WAIT_WOULDBLOCK  1

/* The engine (engine internal threads or memcached worker threads) notifies
 * the replication module that items have changed.  The caller acquires
 * the cache lock.
 */
void rp_item_linked(hash_item *it); /* do_item_link */

#define RP_ITEM_UNLINKED_CAUSE_NORMAL           0
#define RP_ITEM_UNLINKED_CAUSE_EVICT            1
#define RP_ITEM_UNLINKED_CAUSE_EVICT_DELETE_NOW 2
/* item_alloc_internal unlinks an item and immediately uses it for the new
 * item.  rp_item_unlinked normally increments refcount and expects the
 * caller to honor that.  But, this particular case is an exception.
 * The caller does not check refcount again.  For now, we do not replicate
 * this unlink event to the slave.
 */
#define RP_ITEM_UNLINKED_CAUSE_IMMEDIATE_RECYCLE  3
/* The unlink event is immediately followed by the link event for the same
 * item.
 */
#define RP_ITEM_UNLINKED_CAUSE_REPLACE            4
void rp_item_unlinked(hash_item *it, int cause);

void rp_btree_elem_inserted(hash_item *it, btree_elem_item *elem);
void rp_btree_elem_replaced(hash_item *it, btree_elem_item *old_elem,
  btree_elem_item *new_elem);
void rp_btree_elem_unlinked(hash_item *it, btree_elem_item *elem);
void rp_set_elem_inserted(hash_item *it, set_elem_item *elem);
void rp_set_elem_unlinked(hash_item *it, set_elem_item *elem);
void rp_list_elem_inserted(hash_item *it, list_elem_item *elem, int idx);
void rp_list_elem_unlinked(hash_item *it, list_elem_item *elem, int idx);
void rp_snapshot_add_elem(hash_item *it, void *elem);
void rp_lru_update(hash_item *it); /* do_item_update */
void rp_flush(const char *prefix, int nprefix, time_t when);
void rp_setattr_start(hash_item *it, coll_meta_info *info);
void rp_setattr_end(hash_item *it, coll_meta_info *info);
void rp_set_junktime(void);

/* Util. */
char *rp_printable_key(const char *key, int nkey);
char *rp_printable_bkey(const unsigned char *bkey, int nbkey);

/* These are in items.c as they need functions that are static in items.c.
 * The slave calls these to affect changes to its hash table.
 */
ENGINE_ERROR_CODE rp_apply_simple_item_link(struct default_engine *engine,
  const char *key, uint32_t nkey, const char *value, uint32_t nbytes,
  uint64_t cas, uint32_t flags, rel_time_t time, rel_time_t exptime);
ENGINE_ERROR_CODE rp_apply_btree_item_link(struct default_engine *engine,
  const char *key, uint32_t nkey, btree_meta_info *meta, uint32_t flags,
  rel_time_t time, rel_time_t exptime);
ENGINE_ERROR_CODE rp_apply_set_item_link(struct default_engine *engine,
  const char *key, uint32_t nkey, set_meta_info *meta, uint32_t flags,
  rel_time_t time, rel_time_t exptime);
ENGINE_ERROR_CODE rp_apply_list_item_link(struct default_engine *engine,
  const char *key, uint32_t nkey, list_meta_info *meta, uint32_t flags,
  rel_time_t time, rel_time_t exptime);
ENGINE_ERROR_CODE rp_apply_item_unlink(struct default_engine *engine,
  const char *key, uint32_t nkey, bool delete_now);
ENGINE_ERROR_CODE rp_apply_btree_elem_insert(struct default_engine *engine,
  hash_item *it, btree_elem_item *master_elem);
ENGINE_ERROR_CODE rp_apply_btree_elem_unlink(struct default_engine *engine,
  hash_item *it, const char *bkey, uint32_t nbkey);
ENGINE_ERROR_CODE rp_apply_set_elem_insert(struct default_engine *engine,
  hash_item *it, const char *value, uint32_t nbytes);
ENGINE_ERROR_CODE rp_apply_set_elem_unlink(struct default_engine *engine,
  hash_item *it, const char *value, uint32_t nbytes);
ENGINE_ERROR_CODE rp_apply_list_elem_insert(struct default_engine *engine,
  hash_item *it, int index, const char *value, uint32_t nbytes);
ENGINE_ERROR_CODE rp_apply_list_elem_unlink(struct default_engine *engine,
  hash_item *it, int index, int fcnt);
ENGINE_ERROR_CODE rp_apply_item_lru_update(struct default_engine *engine,
  const char *key, uint32_t nkey);
ENGINE_ERROR_CODE rp_apply_flush(struct default_engine *engine,
  const char *prefix, int nprefix);
ENGINE_ERROR_CODE rp_apply_setattr_exptime(struct default_engine *engine,
  const char *key, uint32_t nkey, rel_time_t exptime);
ENGINE_ERROR_CODE rp_apply_setattr_meta_info(struct default_engine *engine,
  hash_item *it, uint8_t ovflact, uint8_t mflags, rel_time_t exptime,
  int32_t mcnt, bkey_t *maxbkeyrange);
ENGINE_ERROR_CODE rp_apply_junktime(struct default_engine *engine,
  int junk_item_time);

/* The master calls these to copy collection items */
int rp_btree_snapshot(struct default_engine *engine, hash_item *it);
int rp_set_snapshot(struct default_engine *engine, hash_item *it);
int rp_list_snapshot(struct default_engine *engine, hash_item *it);

/* Util to decrement refcount */
void rp_release_item(struct default_engine *engine, hash_item *it);
void rp_release_btree_elem(struct default_engine *engine,
  btree_elem_item *elem);
void rp_release_set_elem(struct default_engine *engine, set_elem_item *elem);
void rp_release_list_elem(struct default_engine *engine, list_elem_item *elem);

/* Util to compute item size.  All necessary macros and functions
 * are in items.c.  So put it in item.c as well.
 */
int rp_item_ntotal(struct default_engine *engine, hash_item *it);

/* Util to check if the item has expired, deleted */
bool rp_item_invalid(struct default_engine *engine, hash_item *it);

/* Util to look up the item with the cache already locked */
hash_item *rp_item_get_locked_noref(struct default_engine *engine,
  const char *key, int nkey);

/* The engine calls this to return statistics counters to the user */
void rp_stats(struct default_engine *engine, ADD_STAT add_stat,
  const void *cookie);

/* The engine forwards "replication ..." command to this function. */
ENGINE_ERROR_CODE rp_user_cmd(const char *cmd);

/* The engine calls this function to notify that there are no cluster managers.
 * The replication code should read its own configuration and start running
 * as either master or slave.
 */
ENGINE_ERROR_CODE rp_standalone(void);

/* The engine calls this function to tell it to start acting as the master.
 * addr=master's listen address
 */
ENGINE_ERROR_CODE rp_master_mode(const char *addr);

/* The engine calls this function to tell it to start acting as the slave.
 * saddr=slave's listen address
 * maddr=master's listen address
 */
ENGINE_ERROR_CODE rp_slave_mode(const char *saddr, const char *maddr);

/* The engine calls this function to tell the master about the slave's
 * listen address.  It may be null or empty.  In this case, there are no
 * slaves.
 * addr=slave's listen address
 */
ENGINE_ERROR_CODE rp_slave_addr(const char *addr);

#endif /* !defined(_REPLICATION_H_) */
