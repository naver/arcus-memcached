/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015-2016 JaM2in Co., Ltd.
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
             tabidx = GET_HASH_TABIDX(it->khash, assoc->hashpower, hashmask(assoc->rootpower));
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
    hash_item *it;
    int depth = 0;
    uint32_t bucket = GET_HASH_BUCKET(hash, assoc->hashmask);
    uint32_t tabidx = GET_HASH_TABIDX(hash, assoc->hashpower,
                                      hashmask(assoc->infotable[bucket].curpower));

    it = assoc->roottable[tabidx].hashtable[bucket];
    while (it) {
        if ((hash == it->khash) && (nkey == it->nkey) &&
            (memcmp(key, item_get_key(it), nkey) == 0)) {
            break; /* found */
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return it;
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
    logger->log(EXTENSION_LOG_INFO, NULL, "expand start");
    if (new_hashtable) {
        for (ii=0; ii < table_count; ++ii) {
            assoc->roottable[table_count+ii].hashtable = &new_hashtable[assoc->hashsize*ii];
        }
        assoc->rootpower++;
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "expand end.\n");
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

/*
 * Assoc scan functions
 */
void assoc_scan_init(struct default_engine *engine, struct assoc_scan *scan)
{
    /* initialize assoc_scan structure */
    scan->engine = engine;
    scan->hashsz = engine->assoc.hashsize;
    scan->bucket = 0;
    scan->tabcnt = 0; /* 0 means the scan on the current
                       * bucket chain has not yet started.
                       */

    /* initialize the placeholder item */
    scan->ph_item.refcount = 1;
    scan->ph_item.refchunk = 0;
    scan->ph_item.nkey = 0;
    scan->ph_item.nbytes = 0;
    scan->ph_item.iflag = ITEM_INTERNAL;
    scan->ph_item.h_next = NULL;
    scan->ph_linked = false;

    scan->initialized = true;
}

static void
unlink_scan_placeholder(struct assoc *assoc, struct assoc_scan *scan)
{
    hash_item **p = &assoc->roottable[scan->tabidx].hashtable[scan->bucket];
    assert(*p != NULL);
    while (*p != &scan->ph_item)
        p = &((*p)->h_next);
    *p = (*p)->h_next;
    scan->ph_linked = false;
}

int assoc_scan_next(struct assoc_scan *scan, hash_item **item_array, int array_size)
{
    assert(scan->initialized && array_size > 0);
    struct assoc *assoc = &scan->engine->assoc;
    hash_item *next;
    int item_count = 0;
    int scan_cost = 0;
    int scan_done = false;

    while (scan->bucket < scan->hashsz)
    {
        if (scan->tabcnt == 0) {
            /* start the scan on the current bucket */
            scan->tabcnt = hashsize(assoc->infotable[scan->bucket].curpower);
            scan->tabidx = 0;
            assert(scan->tabcnt > 0);
            /* increment bucket's reference count */
            assoc->infotable[scan->bucket].refcount += 1;
        }

        while (scan->tabidx < scan->tabcnt) {
            if (scan_cost > (2*array_size) && item_count > 0) {
                /* too large scan cost, stop the scan */
                scan_done = true;  break;
            }
            if (scan->ph_linked) {
                next = scan->ph_item.h_next;
                unlink_scan_placeholder(assoc, scan);
            } else {
                next = assoc->roottable[scan->tabidx].hashtable[scan->bucket];
            }
            scan_cost++;
            while (next != NULL) {
                if (next->nkey > 0) { /* Not placeholder item */
                    item_array[item_count] = next; /* user cache item */
                    if (++item_count >= array_size)
                        break;
                }
                next = next->h_next;
                scan_cost++;
            }
            if (next != NULL) {
                if (next->h_next != NULL) {
                    /* add a placeholder item for the next scan */
                    scan->ph_item.h_next = next->h_next;
                    next->h_next = &scan->ph_item;
                    scan->ph_linked = true;
                } else {
                    scan->tabidx += 1;
                }
                /* the array is full of items. stop the scan. */
                scan_done = true;  break;
            }
            scan->tabidx += 1;
        }
        if (scan_done) break;

        /* finish the scan on the current bucket */
        /* decrement bucket's reference count */
        assoc->infotable[scan->bucket].refcount -= 1;
        /* goto the next bucket */
        scan->bucket += 1;
        scan->tabcnt = 0;
    }
    return item_count;
}

void assoc_scan_final(struct assoc_scan *scan)
{
    assert(scan->initialized);

    if (scan->ph_linked) {
        unlink_scan_placeholder(&scan->engine->assoc, scan);
    }
    if (scan->bucket < scan->hashsz) {
        /* decrement bucket's reference count */
        scan->engine->assoc.infotable[scan->bucket].refcount -= 1;
    }
    scan->initialized = false;
}

/*
 * Prefix Management
 */
static inline void *_get_prefix(prefix_t *prefix)
{
    return (void*)(prefix + 1);
}

prefix_t *assoc_prefix_find(struct default_engine *engine, uint32_t hash,
                            const char *prefix, const int nprefix)
{
    prefix_t *pt = engine->assoc.prefix_hashtable[hash & hashmask(DEFAULT_PREFIX_HASHPOWER)];
    while (pt) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0)) {
            return pt;
        }
        pt = pt->h_next;
    }
    return NULL;
}

bool assoc_prefix_isvalid(struct default_engine *engine, hash_item *it, rel_time_t current_time)
{
    prefix_t *pt = it->pfxptr;
    do {
        if (pt->oldest_live != 0 &&
            pt->oldest_live <= current_time &&
            it->time <= pt->oldest_live)
            return false;
        /* traverse parent prefixes to validate them */
        pt = pt->parent_prefix;
    } while(pt != NULL && pt != root_pt);

    return true;
}

void assoc_prefix_update_size(prefix_t *pt, ENGINE_ITEM_TYPE item_type,
                              const size_t item_size, const bool increment)
{
    assert(item_type >= ITEM_TYPE_KV && item_type < ITEM_TYPE_MAX);

    // update prefix information
    if (increment) {
        pt->items_bytes[item_type] += item_size;
        pt->total_bytes_exclusive += item_size;
#if 0 // might be used later
        if (1) {
            prefix_t *curr_pt = pt->parent_prefix;
            while (curr_pt != NULL) {
                curr_pt->total_bytes_inclusive += item_size;
                curr_pt = curr_pt->parent_prefix;
            }
        }
#endif
    } else { /* decrement */
        pt->items_bytes[item_type] -= item_size;
        pt->total_bytes_exclusive -= item_size;
#if 0 // might be used later
        if (1) {
            prefix_t *curr_pt = pt->parent_prefix;
            while (curr_pt != NULL) {
                curr_pt->total_bytes_inclusive -= item_size;
                curr_pt = curr_pt->parent_prefix;
            }
        }
#endif
    }
}

static int _prefix_insert(struct default_engine *engine, uint32_t hash, prefix_t *pt)
{
    assert(assoc_prefix_find(engine, hash, _get_prefix(pt), pt->nprefix) == NULL);

#ifdef NEW_PREFIX_STATS_MANAGEMENT
    (void)engine->server.core->prefix_stats_insert(_get_prefix(pt), pt->nprefix);
#endif

    int bucket = hash & hashmask(DEFAULT_PREFIX_HASHPOWER);
    pt->h_next = engine->assoc.prefix_hashtable[bucket];
    engine->assoc.prefix_hashtable[bucket] = pt;

    assert(pt->parent_prefix != NULL);
    pt->parent_prefix->prefix_items++;
    engine->assoc.tot_prefix_items++;
    return 1;
}

static void _prefix_delete(struct default_engine *engine, uint32_t hash,
                           const char *prefix, const int nprefix)
{
    int bucket = hash & hashmask(DEFAULT_PREFIX_HASHPOWER);
    prefix_t *prev_pt = NULL;
    prefix_t *pt = engine->assoc.prefix_hashtable[bucket];
    while (pt) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0))
            break; /* found */
        prev_pt = pt; pt = pt->h_next;
    }
    if (pt) {
        assert(pt->parent_prefix != NULL);
        pt->parent_prefix->prefix_items--;
        engine->assoc.tot_prefix_items--;

        /* unlink and free the prefix structure */
        if (prev_pt) prev_pt->h_next = pt->h_next;
        else         engine->assoc.prefix_hashtable[bucket] = pt->h_next;
        free(pt);

#ifdef NEW_PREFIX_STATS_MANAGEMENT
        (void)engine->server.core->prefix_stats_delete(prefix, nprefix);
#endif
    }
}

ENGINE_ERROR_CODE assoc_prefix_link(struct default_engine *engine, hash_item *it,
                                    const size_t item_size)
{
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
        /* save prefix pointer in hash_item */
        it->pfxptr = pt;
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
                if (PREFIX_IS_RSVD(key, pt->nprefix)) {
                    pt->internal = 1; /* internal prefix */
                }
                pt->parent_prefix = (j == 0 ? root_pt : prefix_list[j-1].pt);
                time(&pt->create_time);

                // registering allocated prefixes to prefix hastable
                _prefix_insert(engine, prefix_list[j].hash, pt);
                prefix_list[j].pt = pt;
            }
        }
        /* save prefix pointer in hash_item */
        it->pfxptr = pt;
    }
    assert(pt != NULL);

    /* update prefix information */
    int item_type = GET_ITEM_TYPE(it);
    pt->items_count[item_type] += 1;
    pt->items_bytes[item_type] += item_size;
    pt->total_count_exclusive += 1;
    pt->total_bytes_exclusive += item_size;
#if 0 // might be used later
    if (1) {
        prefix_t *curr_pt = pt->parent_prefix;
        while (curr_pt != NULL) {
            curr_pt->total_count_inclusive += 1;
            curr_pt->total_bytes_inclusive += item_size;
            curr_pt = curr_pt->parent_prefix;
        }
    }
#endif

    return ENGINE_SUCCESS;
}

void assoc_prefix_unlink(struct default_engine *engine, hash_item *it,
                         const size_t item_size, bool drop_if_empty)
{
    prefix_t *pt = it->pfxptr;
    it->pfxptr = NULL;
    assert(pt != NULL);

    /* update prefix information */
    int item_type = GET_ITEM_TYPE(it);
    pt->items_count[item_type] -= 1;
    pt->items_bytes[item_type] -= item_size;
    pt->total_count_exclusive -= 1;
    pt->total_bytes_exclusive -= item_size;
#if 0 // might be used later
    if (1) {
        prefix_t *curr_pt = pt->parent_prefix;
        while (curr_pt != NULL) {
            curr_pt->total_count_inclusive -= 1;
            curr_pt->total_bytes_inclusive -= item_size;
            curr_pt = curr_pt->parent_prefix;
        }
    }
#endif

    if (drop_if_empty) {
        while (pt != NULL && pt != root_pt) {
            prefix_t *parent_pt = pt->parent_prefix;

            if (pt->prefix_items > 0 || pt->total_count_exclusive > 0)
                break; /* NOT empty */
            assert(pt->total_bytes_exclusive == 0);
            _prefix_delete(engine, engine->server.core->hash(_get_prefix(pt), pt->nprefix, 0),
                           _get_prefix(pt), pt->nprefix);

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
            if (pt->prefix_items == 0 && pt->total_count_exclusive == 0)
                invalid_prefix++;
            pt = pt->h_next;
        }
    }
    return invalid_prefix;
}
#endif

static ENGINE_ERROR_CODE
do_assoc_get_prefix_stats(struct default_engine *engine,
                          const char *prefix, const int nprefix,
                          void *prefix_data)
{
    struct assoc *assoc = &engine->assoc;
    prefix_t *pt;

    if (nprefix < 0) /* all prefix stats */
    {
        const char *format = "PREFIX %s "
                             "itm %llu kitm %llu litm %llu sitm %llu mitm %llu bitm %llu " /* total item count */
                             "tsz %llu ktsz %llu ltsz %llu stsz %llu mtsz %llu btsz %llu " /* total item bytes */
                             "time %04d%02d%02d%02d%02d%02d\r\n"; /* create time */
        char *buffer;
        struct tm *t;
        uint32_t prefix_hsize = hashsize(DEFAULT_PREFIX_HASHPOWER);
        uint32_t num_prefixes = assoc->tot_prefix_items;
        uint32_t sum_nameleng = 0; /* sum of prefix name length */
        uint32_t i, buflen, pos;

        /* get # of prefixes and num of prefix names */
        assert(root_pt != NULL);
        if (root_pt->total_count_exclusive > 0) {
            /* Include the valid null prefix (that is root prefix) */
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
                                 + (12 * (20 - 4))  /* %llu replaced by 20-digit num */
                                 - ( 5 * ( 4 - 2))) /* %02d replaced by 2-digit num */
               + sizeof("END\r\n"); /* tail string */

        if ((buffer = malloc(buflen)) == NULL) {
            return ENGINE_ENOMEM;
        }

        /* write prefix stats in the buffer */
        pos = sizeof(uint32_t);
        if (num_prefixes > assoc->tot_prefix_items) { /* include root prefix */
            pt = root_pt;
            t = localtime(&pt->create_time);
            pos += snprintf(buffer+pos, buflen-pos, format, "<null>",
                            pt->total_count_exclusive,
                            pt->items_count[ITEM_TYPE_KV],
                            pt->items_count[ITEM_TYPE_LIST],
                            pt->items_count[ITEM_TYPE_SET],
                            pt->items_count[ITEM_TYPE_MAP],
                            pt->items_count[ITEM_TYPE_BTREE],
                            pt->total_bytes_exclusive,
                            pt->items_bytes[ITEM_TYPE_KV],
                            pt->items_bytes[ITEM_TYPE_LIST],
                            pt->items_bytes[ITEM_TYPE_SET],
                            pt->items_bytes[ITEM_TYPE_MAP],
                            pt->items_bytes[ITEM_TYPE_BTREE],
                            t->tm_year+1900, t->tm_mon+1, t->tm_mday,
                            t->tm_hour, t->tm_min, t->tm_sec);
            assert(pos < buflen);
        }
        for (i = 0; i < prefix_hsize; i++) {
            pt = assoc->prefix_hashtable[i];
            while (pt) {
                t = localtime(&pt->create_time);
                pos += snprintf(buffer+pos, buflen-pos, format, _get_prefix(pt),
                                pt->total_count_exclusive,
                                pt->items_count[ITEM_TYPE_KV],
                                pt->items_count[ITEM_TYPE_LIST],
                                pt->items_count[ITEM_TYPE_SET],
                                pt->items_count[ITEM_TYPE_MAP],
                                pt->items_count[ITEM_TYPE_BTREE],
                                pt->total_bytes_exclusive,
                                pt->items_bytes[ITEM_TYPE_KV],
                                pt->items_bytes[ITEM_TYPE_LIST],
                                pt->items_bytes[ITEM_TYPE_SET],
                                pt->items_bytes[ITEM_TYPE_MAP],
                                pt->items_bytes[ITEM_TYPE_BTREE],
                                t->tm_year+1900, t->tm_mon+1, t->tm_mday,
                                t->tm_hour, t->tm_min, t->tm_sec);
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
            pt = assoc_prefix_find(engine, engine->server.core->hash(prefix,nprefix,0),
                                   prefix, nprefix);
        } else {
            pt = root_pt;
        }
        if (pt == NULL) {
            return ENGINE_PREFIX_ENOENT;
        }

        prefix_stats->hash_items = pt->items_count[ITEM_TYPE_KV];
        prefix_stats->hash_items_bytes = pt->items_bytes[ITEM_TYPE_KV];
        prefix_stats->prefix_items = pt->prefix_items;
        if (prefix != NULL)
            prefix_stats->tot_prefix_items = pt->prefix_items;
        else
            prefix_stats->tot_prefix_items = assoc->tot_prefix_items;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE assoc_get_prefix_stats(struct default_engine *engine,
                                         const char *prefix, const int nprefix,
                                         void *prefix_data)
{
    ENGINE_ERROR_CODE ret;
    pthread_mutex_lock(&engine->cache_lock);
    ret = do_assoc_get_prefix_stats(engine, prefix, nprefix, prefix_data);
    pthread_mutex_unlock(&engine->cache_lock);
    return ret;
}
