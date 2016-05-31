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
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "default_engine.h"

#define hashsize(n) ((uint32_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

#define GET_HASH_BUCKET(hash, mask)        ((hash) & (mask))
#define GET_HASH_TABIDX(hash, shift, mask) (((hash) >> (shift)) & (mask))

#define DEFAULT_PREFIX_HASHPOWER 10
#define DEFAULT_PREFIX_MAX_DEPTH 1

typedef struct {
    prefix_t   *pt;
    uint8_t     nprefix;
    uint32_t    hash;
} prefix_t_list_elem;

static EXTENSION_LOGGER_DESCRIPTOR *logger;
static prefix_t *root_pt = NULL; /* root prefix info */


static inline void *_get_prefix(prefix_t *prefix)
{
    return (void*)(prefix + 1);
}

ENGINE_ERROR_CODE assoc_init(struct default_engine *engine)
{
    struct assoc *assoc = &engine->assoc;

    logger = engine->server.log->get_logger();

    assoc->hashsize = hashsize(assoc->hashpower);
    assoc->hashmask = hashmask(assoc->hashpower);
    assoc->rootpower = 0;

    assoc->roottable = calloc(assoc->hashsize * 2, sizeof(void *));
    if (assoc->roottable == NULL) {
        return ENGINE_ENOMEM;
    }
    assoc->roottable[0].hashtable = (hash_item**)&assoc->roottable[assoc->hashsize];

    assoc->infotable = calloc(assoc->hashsize, sizeof(struct bucket_info));
    if (assoc->infotable == NULL) {
        free(assoc->roottable);
        return ENGINE_ENOMEM;
    }

    assoc->prefix_hashtable = calloc(hashsize(DEFAULT_PREFIX_HASHPOWER), sizeof(void *));
    if (assoc->prefix_hashtable == NULL) {
        free(assoc->roottable);
        free(assoc->infotable);
        return ENGINE_ENOMEM;
    }

    // initialize noprefix stats info
    memset(&assoc->noprefix_stats, 0, sizeof(prefix_t));
    root_pt = &assoc->noprefix_stats;

    logger->log(EXTENSION_LOG_INFO, NULL, "ASSOC module initialized.\n");
    return ENGINE_SUCCESS;
}

void assoc_final(struct default_engine *engine)
{
    struct assoc *assoc = &engine->assoc;
    int ii, table_count;

    for (ii=0; ii < assoc->rootpower; ++ii) {
         table_count = hashsize(ii); //2 ^ n
         free(assoc->roottable[table_count].hashtable);
    }
    free(assoc->roottable);
    free(assoc->infotable);
    free(assoc->prefix_hashtable);
    logger->log(EXTENSION_LOG_INFO, NULL, "ASSOC module destroyed.\n");
}

static void redistribute(struct default_engine *engine, unsigned int bucket)
{
    struct assoc *assoc = &engine->assoc;
    hash_item *it, **prev;
    uint32_t tabidx;
    uint32_t ii, table_count = hashsize(assoc->infotable[bucket].curpower);

    for (ii=0; ii < table_count; ++ii) {
         prev = &assoc->roottable[ii].hashtable[bucket];
         while (*prev != NULL) {
             it = *prev;
#ifdef LONG_KEY_SUPPORT
             tabidx = GET_HASH_TABIDX(it->hval, assoc->hashpower, hashmask(assoc->rootpower));
#else
             tabidx = GET_HASH_TABIDX(engine->server.core->hash(item_get_key(it), it->nkey, 0),
                                      assoc->hashpower, hashmask(assoc->rootpower));
#endif
             if (tabidx == ii) {
                 prev = &it->h_next;
             } else {
                 *prev = it->h_next;
                 it->h_next = assoc->roottable[tabidx].hashtable[bucket];
                 assoc->roottable[tabidx].hashtable[bucket] = it;
             }
         }
    }
    assoc->infotable[bucket].curpower = assoc->rootpower;
}

hash_item *assoc_find(struct default_engine *engine, uint32_t hash,
                      const char *key, const size_t nkey)
{
    struct assoc *assoc = &engine->assoc;
    hash_item *ret = NULL;
    hash_item *it;
    int depth = 0;
    uint32_t bucket = GET_HASH_BUCKET(hash, assoc->hashmask);
    uint32_t tabidx = GET_HASH_TABIDX(hash, assoc->hashpower,
                                      hashmask(assoc->infotable[bucket].curpower));

    it = assoc->roottable[tabidx].hashtable[bucket];
    while (it) {
#ifdef LONG_KEY_SUPPORT
        if ((nkey == it->nkey) && (hash == it->hval) && (memcmp(key, item_get_key(it), nkey) == 0)) {
#else
        if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
#endif
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */
static hash_item** _hashitem_before(struct default_engine *engine, uint32_t hash,
                                    const char *key, const size_t nkey)
{
    struct assoc *assoc = &engine->assoc;
    hash_item **pos;
    uint32_t bucket = GET_HASH_BUCKET(hash, assoc->hashmask);
    uint32_t tabidx = GET_HASH_TABIDX(hash, assoc->hashpower,
                                      hashmask(assoc->infotable[bucket].curpower));

    pos = &assoc->roottable[tabidx].hashtable[bucket];
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, item_get_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
static void assoc_expand(struct default_engine *engine)
{
    struct assoc *assoc = &engine->assoc;
    hash_item** new_hashtable;
    uint32_t ii, table_count = hashsize(assoc->rootpower); // 2 ^ n

    new_hashtable = calloc(assoc->hashsize * table_count, sizeof(void *));
    if (new_hashtable) {
        for (ii=0; ii < table_count; ++ii) {
            assoc->roottable[table_count+ii].hashtable = &new_hashtable[assoc->hashsize*ii];
        }
        assoc->rootpower++;
    }
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(struct default_engine *engine, uint32_t hash, hash_item *it)
{
    struct assoc *assoc = &engine->assoc;
    uint32_t bucket = GET_HASH_BUCKET(hash, assoc->hashmask);
    uint32_t tabidx;

    assert(assoc_find(engine, hash, item_get_key(it), it->nkey) == 0); /* shouldn't have duplicately named things defined */

    if (assoc->infotable[bucket].curpower != assoc->rootpower &&
        assoc->infotable[bucket].refcount == 0) {
        redistribute(engine, bucket);
    }
    tabidx = GET_HASH_TABIDX(hash, assoc->hashpower,
                             hashmask(assoc->infotable[bucket].curpower));

    // inserting actual hash_item to appropriate assoc_t
    it->h_next = assoc->roottable[tabidx].hashtable[bucket];
    assoc->roottable[tabidx].hashtable[bucket] = it;

    assoc->hash_items++;
    if (assoc->hash_items > (hashsize(assoc->hashpower + assoc->rootpower) * 3) / 2) {
        assoc_expand(engine);
    }
    MEMCACHED_ASSOC_INSERT(item_get_key(it), it->nkey, assoc->hash_items);
    return 1;
}

void assoc_delete(struct default_engine *engine, uint32_t hash,
                  const char *key, const size_t nkey)
{
    hash_item **before = _hashitem_before(engine, hash, key, nkey);

    if (*before) {
        hash_item *nxt;
        engine->assoc.hash_items--;

       /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, engine->assoc.hash_items);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;

        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}

#ifdef JHPARK_KEY_DUMP
/*
 * Assoc scan
 */
void assoc_scan_init(struct default_engine *engine, struct assoc_scan *scan)
{
    scan->guard_data = 23456;
    scan->cur_bucket = 0;
    scan->cur_tabidx = 0;
    scan->max_bucket = engine->assoc.hashsize;
    scan->array_size = MAX_SCAN_ITEMS;
    scan->item_count = 0;
}

void assoc_scan_next(struct default_engine *engine, struct assoc_scan *scan)
{
    assert(scan->guard_data == 23456);
    struct assoc *assoc = &engine->assoc;
    hash_item *it;
    uint32_t ii, ntables;
    uint32_t found_count;
    uint32_t access_count = 0;

    scan->item_count = 0;
    while (scan->cur_bucket < scan->max_bucket) {
        if (scan->cur_tabidx == 0) {
            /* increment bucket's reference count */
            assoc->infotable[scan->cur_bucket].refcount += 1;
        }
        ntables = hashsize(assoc->infotable[scan->cur_bucket].curpower);
        for (ii=scan->cur_tabidx; ii < ntables; ii++) {
            if (access_count > (MAX_SCAN_ITEMS/2)) {
                break; /* long time elapsed after holding cache lock */
            }
            found_count = 0;
            it = assoc->roottable[ii].hashtable[scan->cur_bucket];
            while (it != NULL) {
                access_count++;
                if (item_is_valid(engine, it)) {
                    if ((scan->item_count + found_count) >= scan->array_size) {
                        break; /* overflow */
                    }
                    scan->item_array[scan->item_count + found_count]= it;
                    found_count += 1;
                }
                it = it->h_next;
            }
            if (it != NULL) { /* overflow */
                scan->cur_tabidx = ii;
                break;
            }
            scan->item_count += found_count;
        }
        if (ii < ntables) break;

        /* decrement bucket's reference count */
        assoc->infotable[scan->cur_bucket].refcount -= 1;
        scan->cur_tabidx = 0;
        scan->cur_bucket += 1;
    }
}

void assoc_scan_final(struct default_engine *engine, struct assoc_scan *scan)
{
    assert(scan->guard_data == 23456);
    if (scan->cur_bucket < scan->max_bucket) {
        /* decrement bucket's reference count */
        engine->assoc.infotable[scan->cur_bucket].refcount -= 1;
    }
    scan->guard_data = 0;
}
#endif

/*
 * Prefix Management
 */
prefix_t *assoc_prefix_find(struct default_engine *engine, uint32_t hash,
                            const char *prefix, const size_t nprefix)
{
    prefix_t *pt;

    pt = engine->assoc.prefix_hashtable[hash & hashmask(DEFAULT_PREFIX_HASHPOWER)];
    while (pt) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0)) {
            return pt;
        }
        pt = pt->h_next;
    }
    return NULL;
}

static prefix_t** _prefixitem_before(struct default_engine *engine, uint32_t hash,
                                     const char *prefix, const size_t nprefix)
{
    prefix_t **pos;

    pos = &engine->assoc.prefix_hashtable[hash & hashmask(DEFAULT_PREFIX_HASHPOWER)];
    while (*pos && ((nprefix != (*pos)->nprefix) || memcmp(prefix, _get_prefix(*pos), nprefix))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

static int _prefix_insert(struct default_engine *engine, uint32_t hash, prefix_t *pt)
{
    assert(assoc_prefix_find(engine, hash, _get_prefix(pt), pt->nprefix) == NULL);

    pt->h_next = engine->assoc.prefix_hashtable[hash & hashmask(DEFAULT_PREFIX_HASHPOWER)];
    engine->assoc.prefix_hashtable[hash & hashmask(DEFAULT_PREFIX_HASHPOWER)] = pt;

    assert(pt->parent_prefix != NULL);
    pt->parent_prefix->prefix_items++;
    engine->assoc.tot_prefix_items++;
    return 1;
}

static void _prefix_delete(struct default_engine *engine, uint32_t hash,
                           const char *prefix, const uint8_t nprefix)
{
    prefix_t **prefix_before = _prefixitem_before(engine, hash, prefix, nprefix);
    prefix_t *pt = *prefix_before;
    prefix_t *prefix_nxt = NULL;

    assert(pt != NULL && pt->parent_prefix != NULL);

    pt->parent_prefix->prefix_items--;
    engine->assoc.tot_prefix_items--;

    prefix_nxt = pt->h_next;
    pt->h_next = 0;
    *prefix_before = prefix_nxt;

    free(pt); // release
}

bool assoc_prefix_isvalid(struct default_engine *engine, hash_item *it)
{
    rel_time_t current_time = engine->server.core->get_current_time();
    prefix_t *pt;

    if (it->nprefix == it->nkey) {
        /* the prefix of key: null */
        assert(root_pt != NULL);
        pt = root_pt;
        if (pt->oldest_live != 0 && pt->oldest_live <= current_time && it->time <= pt->oldest_live) {
            return false;
        }
    } else {
        /* the prifix of key: given */
        pt = assoc_prefix_find(engine, engine->server.core->hash(item_get_key(it), it->nprefix, 0),
                               item_get_key(it), it->nprefix);
        while (pt != NULL && pt != root_pt) {
            // validation check between prefix and hash_item
            if (pt->oldest_live != 0 && pt->oldest_live <= current_time && it->time <= pt->oldest_live) {
                return false;
            }
            // traversal parent prefixes to validate
            pt = pt->parent_prefix;
        }
    }
    return true;
}

void assoc_prefix_update_size(prefix_t *pt, ENGINE_ITEM_TYPE item_type,
                              const size_t item_size, const bool increment)
{
    assert(pt != NULL);

    // update prefix information
    if (increment == true) {
        if (item_type == ITEM_TYPE_KV)         pt->hash_items_bytes += item_size;
        else if (item_type == ITEM_TYPE_LIST)  pt->list_hash_items_bytes += item_size;
        else if (item_type == ITEM_TYPE_SET)   pt->set_hash_items_bytes += item_size;
        else if (item_type == ITEM_TYPE_BTREE) pt->btree_hash_items_bytes += item_size;
#if 0 // might be used later
        if (1) {
            prefix_t *curr_pt = pt->parent_prefix;
            while (curr_pt != NULL) {
                curr_pt->tot_hash_items_bytes += item_size;
                curr_pt = curr_pt->parent_prefix;
            }
        }
#endif
    } else {
        if (item_type == ITEM_TYPE_KV)         pt->hash_items_bytes -= item_size;
        else if (item_type == ITEM_TYPE_LIST)  pt->list_hash_items_bytes -= item_size;
        else if (item_type == ITEM_TYPE_SET)   pt->set_hash_items_bytes -= item_size;
        else if (item_type == ITEM_TYPE_BTREE) pt->btree_hash_items_bytes -= item_size;
#if 0 // might be used later
        if (1) {
            prefix_t *curr_pt = pt->parent_prefix;
            while (curr_pt != NULL) {
                curr_pt->tot_hash_items_bytes -= item_size;
                curr_pt = curr_pt->parent_prefix;
            }
        }
#endif
    }
}

ENGINE_ERROR_CODE assoc_prefix_link(struct default_engine *engine, hash_item *it,
                                    const size_t item_size, prefix_t **pfx_item)
{
    assert(it->nprefix == 0);
    const char *key = item_get_key(it);
    size_t     nkey = it->nkey;
    int prefix_depth = 0;
    int i = 0;
    char *token;
    prefix_t *pt = NULL;
    prefix_t_list_elem prefix_list[DEFAULT_PREFIX_MAX_DEPTH];

    // prefix discovering: we don't even know prefix existence at this time
    while ((token = memchr(key+i+1, engine->config.prefix_delimiter, nkey-i-1)) != NULL) {
        i = token - key;
        prefix_list[prefix_depth].nprefix = i;

        prefix_depth++;
        if (prefix_depth >= DEFAULT_PREFIX_MAX_DEPTH) {
            break;
        }
    }

    if (prefix_depth == 0) {
        pt = root_pt;
        time(&pt->create_time);
        it->nprefix = nkey;
    } else {
        for (i = prefix_depth - 1; i >= 0; i--) {
            prefix_list[i].hash = engine->server.core->hash(key, prefix_list[i].nprefix, 0);
            pt = assoc_prefix_find(engine, prefix_list[i].hash, key, prefix_list[i].nprefix);
            if (pt != NULL) break;
        }

        if (i < (prefix_depth - 1)) {
            if (prefix_depth == 1) {
                if (!mc_isvalidname(key, prefix_list[0].nprefix)) {
                    return ENGINE_PREFIX_ENAME; /* Invalid prefix name */
                }
            }

            // need building prefixes
            if (pt != NULL && i >= 0) {
                prefix_list[i].pt = pt; // i >= 0
            }

            for (int j = i + 1; j < prefix_depth; j++) {
                pt = (prefix_t*)malloc(sizeof(prefix_t) + prefix_list[j].nprefix + 1);
                if (pt == NULL) {
                    for (j = j - 1; j >= i + 1; j--) {
                        assert(prefix_list[j].pt != NULL);
                        _prefix_delete(engine, prefix_list[j].hash, key, prefix_list[j].nprefix);
                    }
                    return ENGINE_ENOMEM;
                }

                // building a prefix_t
                memset(pt, 0, sizeof(prefix_t));
                memcpy(pt + 1, key, prefix_list[j].nprefix);
                memcpy((char*)pt+sizeof(prefix_t)+prefix_list[j].nprefix, "\0", 1);
                pt->nprefix = prefix_list[j].nprefix;
                pt->parent_prefix = (j == 0 ? root_pt : prefix_list[j-1].pt);
                time(&pt->create_time);

                // registering allocated prefixes to prefix hastable
                _prefix_insert(engine, prefix_list[j].hash, pt);
                prefix_list[j].pt = pt;
            }
        }
        // update item information about prefix length
        it->nprefix = pt->nprefix;
    }

    assert(pt != NULL);

    // update prefix information
    if ((it->iflag & ITEM_IFLAG_LIST) != 0) {
        pt->list_hash_items++;
        pt->list_hash_items_bytes += item_size;
    } else if ((it->iflag & ITEM_IFLAG_SET) != 0) {
        pt->set_hash_items++;
        pt->set_hash_items_bytes += item_size;
    } else if ((it->iflag & ITEM_IFLAG_BTREE) != 0) {
        pt->btree_hash_items++;
        pt->btree_hash_items_bytes += item_size;
    } else {
        pt->hash_items++;
        pt->hash_items_bytes += item_size;
    }
#if 0 // might be used later
    if (1) {
        curr_pt = pt->parent_prefix;
        while (curr_pt != NULL) {
            curr_pt->tot_hash_items++;
            curr_pt->tot_hash_items_bytes += item_size;
            curr_pt = curr_pt->parent_prefix;
        }
    }
#endif

    *pfx_item = pt;
    return ENGINE_SUCCESS;
}

void assoc_prefix_unlink(struct default_engine *engine, hash_item *it,
                         const size_t item_size, bool drop_if_empty)
{
    prefix_t *pt;
    assert(it->nprefix != 0);

    if (it->nprefix == it->nkey) {
        pt = root_pt;
    } else {
        pt = assoc_prefix_find(engine, engine->server.core->hash(item_get_key(it), it->nprefix, 0),
                               item_get_key(it), it->nprefix);
    }
    assert(pt != NULL);

    // update prefix information
    if ((it->iflag & ITEM_IFLAG_LIST) != 0) {
        pt->list_hash_items--;
        pt->list_hash_items_bytes -= item_size;
    } else if ((it->iflag & ITEM_IFLAG_SET) != 0) {
        pt->set_hash_items--;
        pt->set_hash_items_bytes -= item_size;
    } else if ((it->iflag & ITEM_IFLAG_BTREE) != 0) {
        pt->btree_hash_items--;
        pt->btree_hash_items_bytes -= item_size;
    } else {
        pt->hash_items--;
        pt->hash_items_bytes -= item_size;
    }
#if 0 // might be used later
    if (1) {
        prefix_t *curr_pt = pt->parent_prefix;
        while (curr_pt != NULL) {
            curr_pt->tot_hash_items--;
            curr_pt->tot_hash_items_bytes -= item_size;
            curr_pt = curr_pt->parent_prefix;
        }
    }
#endif
    if (drop_if_empty) {
        while (pt != NULL) {
            prefix_t *parent_pt = pt->parent_prefix;

            if (pt != root_pt && pt->prefix_items == 0 && pt->hash_items == 0 &&
                pt->list_hash_items == 0 && pt->set_hash_items == 0 && pt->btree_hash_items == 0) {
                assert(pt->hash_items_bytes == 0 && pt->list_hash_items_bytes == 0 &&
                       pt->set_hash_items_bytes == 0 && pt->btree_hash_items_bytes == 0);
                _prefix_delete(engine, engine->server.core->hash(_get_prefix(pt), pt->nprefix, 0),
                               _get_prefix(pt), pt->nprefix);
            } else {
                break;
            }
            pt = parent_pt;
        }
    }
}

#if 0 // might be used later
static uint32_t do_assoc_count_invalid_prefix(struct default_engine *engine)
{
    prefix_t *pt;
    uint32_t i, size = hashsize(DEFAULT_PREFIX_HASHPOWER);
    uint32_t invalid_prefix = 0;

    for (i = 0; i < size; i++) {
        pt = engine->assoc.prefix_hashtable[i];
        while (pt) {
            if (pt->prefix_items == 0 && pt->hash_items == 0 &&
                pt->list_hash_items == 0 && pt->set_hash_items == 0 && pt->btree_hash_items == 0) {
                invalid_prefix++;
            }
            pt = pt->h_next;
        }
    }
    return invalid_prefix;
}
#endif

static ENGINE_ERROR_CODE
do_assoc_get_prefix_stats(struct default_engine *engine,
                          const char *prefix, const int  nprefix,
                          void *prefix_data)
{
    struct assoc *assoc = &engine->assoc;
    prefix_t *pt;

    if (nprefix < 0) /* all prefix stats */
    {
        const char *format = "PREFIX %s "
                             "itm %llu kitm %llu litm %llu sitm %llu bitm %llu " /* total item count */
                             "tsz %llu ktsz %llu ltsz %llu stsz %llu btsz %llu " /* total item bytes */
                             "time %04d%02d%02d%02d%02d%02d\r\n"; /* create time */
        char *buffer;
        struct tm *t;
        uint32_t prefix_hsize = hashsize(DEFAULT_PREFIX_HASHPOWER);
        uint32_t num_prefixes = assoc->tot_prefix_items;
        uint32_t sum_nameleng = 0; /* sum of prefix name length */
        uint32_t i, buflen, pos;
        uint64_t tot_item_count;
        uint64_t tot_item_bytes;

        /* get # of prefixes and num of prefix names */
        assert(root_pt != NULL);
        if (root_pt->hash_items > 0 || root_pt->list_hash_items > 0 ||
            root_pt->set_hash_items > 0 || root_pt->btree_hash_items > 0) {
            /* including valid null prefix (that is root prefix) */
            num_prefixes += 1;
            sum_nameleng += strlen("<null>");
        }
        for (i = 0; i < prefix_hsize; i++) {
            pt = assoc->prefix_hashtable[i];
            while (pt) {
                sum_nameleng += pt->nprefix;
                pt = pt->h_next;
            }
        }

        /* Allocate stats buffer: <length, prefix stats list, tail>.
         * Check the count of "%llu" and "%02d" in the above format string.
         *   - 10 : the count of "%llu" strings.
         *   -  5 : the count of "%02d" strings.
         */
        buflen = sizeof(uint32_t) /* length */
               + sum_nameleng
               + num_prefixes * (strlen(format) - 2 /* %s replaced by prefix name */
                                 + (10 * (20 - 4))  /* %llu replaced by 20-digit num */
                                 - ( 5 * ( 4 - 2))) /* %02d replaced by 2-digit num */
               + sizeof("END\r\n"); /* tail string */

        if ((buffer = malloc(buflen)) == NULL) {
            return ENGINE_ENOMEM;
        }

        /* write prefix stats in the buffer */
        pos = sizeof(uint32_t);
        if (num_prefixes > assoc->tot_prefix_items) { /* include root prefix */
            pt = root_pt;
            tot_item_count = pt->hash_items + pt->list_hash_items
                           + pt->set_hash_items + pt->btree_hash_items;
            tot_item_bytes = pt->hash_items_bytes + pt->list_hash_items_bytes
                           + pt->set_hash_items_bytes + pt->btree_hash_items_bytes;
            t = localtime(&pt->create_time);

            pos += snprintf(buffer+pos, buflen-pos, format, "<null>",
                            tot_item_count,
                            pt->hash_items, pt->list_hash_items,
                            pt->set_hash_items, pt->btree_hash_items,
                            tot_item_bytes,
                            pt->hash_items_bytes, pt->list_hash_items_bytes,
                            pt->set_hash_items_bytes, pt->btree_hash_items_bytes,
                            t->tm_year+1900, t->tm_mon+1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
            assert(pos < buflen);
        }
        for (i = 0; i < prefix_hsize; i++) {
            pt = assoc->prefix_hashtable[i];
            while (pt) {
                tot_item_count = pt->hash_items + pt->list_hash_items
                               + pt->set_hash_items + pt->btree_hash_items;
                tot_item_bytes = pt->hash_items_bytes + pt->list_hash_items_bytes
                               + pt->set_hash_items_bytes + pt->btree_hash_items_bytes;
                t = localtime(&pt->create_time);

                pos += snprintf(buffer+pos, buflen-pos, format, _get_prefix(pt),
                                tot_item_count,
                                pt->hash_items, pt->list_hash_items,
                                pt->set_hash_items, pt->btree_hash_items,
                                tot_item_bytes,
                                pt->hash_items_bytes, pt->list_hash_items_bytes,
                                pt->set_hash_items_bytes, pt->btree_hash_items_bytes,
                                t->tm_year+1900, t->tm_mon+1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
                assert(pos < buflen);
                pt = pt->h_next;
            }
        }
        memcpy(buffer+pos, "END\r\n", 6);
        *(uint32_t*)buffer = pos + 5 - sizeof(uint32_t);

        *(char**)prefix_data = buffer;
    }
    else /* prefix stats on the given prefix */
    {
        prefix_engine_stats *prefix_stats = (prefix_engine_stats*)prefix_data;

        if (prefix != NULL) {
            pt = assoc_prefix_find(engine, engine->server.core->hash(prefix,nprefix,0), prefix, nprefix);
        } else {
            pt = root_pt;
        }
        if (pt == NULL) {
            return ENGINE_PREFIX_ENOENT;
        }

        prefix_stats->hash_items = pt->hash_items;
        prefix_stats->hash_items_bytes = pt->hash_items_bytes;
        prefix_stats->prefix_items = pt->prefix_items;
        if (prefix != NULL)
            prefix_stats->tot_prefix_items = pt->prefix_items;
        else
            prefix_stats->tot_prefix_items = assoc->tot_prefix_items;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE assoc_get_prefix_stats(struct default_engine *engine,
                                         const char *prefix, const int nprefix, void *prefix_data)
{
    ENGINE_ERROR_CODE ret;
    pthread_mutex_lock(&engine->cache_lock);
    ret = do_assoc_get_prefix_stats(engine, prefix, nprefix, prefix_data);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}
