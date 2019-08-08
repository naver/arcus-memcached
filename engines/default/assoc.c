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

#define DEFAULT_ROOTSIZE 512

typedef struct {
    prefix_t   *pt;
    uint8_t     nprefix;
    uint32_t    hash;
} prefix_t_list_elem;

static struct engine_config *config=NULL; // engine config
static struct assoc         *assocp=NULL; // engine assoc
static SERVER_CORE_API      *svcore=NULL; // server core api
static EXTENSION_LOGGER_DESCRIPTOR *logger;
static prefix_t *root_pt = NULL; /* root prefix info */


ENGINE_ERROR_CODE assoc_init(struct default_engine *engine)
{
    /* initialize global variables */
    config = &engine->config;
    assocp = &engine->assoc;
    svcore = engine->server.core;
    logger = engine->server.log->get_logger();

    assocp->hashsize = hashsize(assocp->hashpower);
    assocp->hashmask = hashmask(assocp->hashpower);
    assocp->rootpower = 0;
    assocp->rootsize = DEFAULT_ROOTSIZE;

    assocp->roottable = calloc(assocp->rootsize, sizeof(void *));
    if (assocp->roottable == NULL) {
        return ENGINE_ENOMEM;
    }

    assocp->roottable[0].hashtable = calloc(assocp->hashsize, sizeof(void*));
    if (assocp->roottable[0].hashtable == NULL) {
        free(assocp->roottable);
        return ENGINE_ENOMEM;
    }

    assocp->infotable = calloc(assocp->hashsize, sizeof(struct bucket_info));
    if (assocp->infotable == NULL) {
        free(assocp->roottable[0].hashtable);
        free(assocp->roottable);
        return ENGINE_ENOMEM;
    }

    assocp->prefix_hashtable = calloc(hashsize(DEFAULT_PREFIX_HASHPOWER), sizeof(void *));
    if (assocp->prefix_hashtable == NULL) {
        free(assocp->roottable[0].hashtable);
        free(assocp->roottable);
        free(assocp->infotable);
        return ENGINE_ENOMEM;
    }

    // initialize noprefix stats info
    memset(&assocp->noprefix_stats, 0, sizeof(prefix_t));
    root_pt = &assocp->noprefix_stats;

    logger->log(EXTENSION_LOG_INFO, NULL, "ASSOC module initialized.\n");
    return ENGINE_SUCCESS;
}

void assoc_final(struct default_engine *engine)
{
    int ii, table_count;

    free(assocp->roottable[0].hashtable);
    for (ii=0; ii < assocp->rootpower; ++ii) {
         table_count = hashsize(ii); //2 ^ n
         free(assocp->roottable[table_count].hashtable);
    }
    free(assocp->roottable);
    free(assocp->infotable);
    free(assocp->prefix_hashtable);
    logger->log(EXTENSION_LOG_INFO, NULL, "ASSOC module destroyed.\n");
}

static void redistribute(unsigned int bucket)
{
    hash_item *it, **prev;
    uint32_t tabidx;
    uint32_t ii, table_count = hashsize(assocp->infotable[bucket].curpower);

    for (ii=0; ii < table_count; ++ii) {
         prev = &assocp->roottable[ii].hashtable[bucket];
         while (*prev != NULL) {
             it = *prev;
             tabidx = GET_HASH_TABIDX(it->khash, assocp->hashpower, hashmask(assocp->rootpower));
             if (tabidx == ii) {
                 prev = &it->h_next;
             } else {
                 *prev = it->h_next;
                 it->h_next = assocp->roottable[tabidx].hashtable[bucket];
                 assocp->roottable[tabidx].hashtable[bucket] = it;
             }
         }
    }
    assocp->infotable[bucket].curpower = assocp->rootpower;
}

hash_item *assoc_find(const char *key, const uint32_t nkey, uint32_t hash)
{
    hash_item *it;
    int depth = 0;
    uint32_t bucket = GET_HASH_BUCKET(hash, assocp->hashmask);
    uint32_t tabidx = GET_HASH_TABIDX(hash, assocp->hashpower,
                                      hashmask(assocp->infotable[bucket].curpower));

    it = assocp->roottable[tabidx].hashtable[bucket];
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
static hash_item** _hashitem_before(const char *key, const uint32_t nkey, uint32_t hash)
{
    hash_item **pos;
    uint32_t bucket = GET_HASH_BUCKET(hash, assocp->hashmask);
    uint32_t tabidx = GET_HASH_TABIDX(hash, assocp->hashpower,
                                      hashmask(assocp->infotable[bucket].curpower));

    pos = &assocp->roottable[tabidx].hashtable[bucket];
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, item_get_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
static void assoc_expand(void)
{
    hash_item** new_hashtable;
    uint32_t ii, table_count = hashsize(assocp->rootpower); // 2 ^ n

    if (table_count * 2 > assocp->rootsize) {
        struct table *reallocated_roottable = realloc(assocp->roottable, sizeof(void*) * assocp->rootsize * 2);
        if (reallocated_roottable == NULL) {
            return;
        }
        assocp->roottable = reallocated_roottable;
        assocp->rootsize *= 2;
    }
    new_hashtable = calloc(assocp->hashsize * table_count, sizeof(void *));
    if (new_hashtable) {
        for (ii=0; ii < table_count; ++ii) {
            assocp->roottable[table_count+ii].hashtable = &new_hashtable[assocp->hashsize*ii];
        }
        assocp->rootpower++;
    }
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(hash_item *it, uint32_t hash)
{
    uint32_t bucket = GET_HASH_BUCKET(hash, assocp->hashmask);
    uint32_t tabidx;

    assert(assoc_find(item_get_key(it), it->nkey, hash) == 0); /* shouldn't have duplicately named things defined */

    if (assocp->infotable[bucket].curpower != assocp->rootpower &&
        assocp->infotable[bucket].refcount == 0) {
        redistribute(bucket);
    }
    tabidx = GET_HASH_TABIDX(hash, assocp->hashpower,
                             hashmask(assocp->infotable[bucket].curpower));

    // inserting actual hash_item to appropriate assoc_t
    it->h_next = assocp->roottable[tabidx].hashtable[bucket];
    assocp->roottable[tabidx].hashtable[bucket] = it;

    assocp->hash_items++;
    if (assocp->hash_items > (hashsize(assocp->hashpower + assocp->rootpower) * 3) / 2) {
        assoc_expand();
    }
    MEMCACHED_ASSOC_INSERT(item_get_key(it), it->nkey, assocp->hash_items);
    return 1;
}

void assoc_delete(const char *key, const uint32_t nkey, uint32_t hash)
{
    hash_item **before = _hashitem_before(key, nkey, hash);

    if (*before) {
        hash_item *nxt;
        assocp->hash_items--;

       /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, assocp->hash_items);
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
static void _init_scan_placeholder(struct assoc_scan *scan)
{
    /* initialize the placeholder item */
    scan->ph_item.refcount = 1;
    scan->ph_item.refchunk = 0;
    scan->ph_item.nkey = 0;
    scan->ph_item.nbytes = 0;
    scan->ph_item.iflag = ITEM_INTERNAL;
    scan->ph_item.h_next = NULL;
    scan->ph_linked = false;
}

static void _link_scan_placeholder(struct assoc_scan *scan, hash_item *item)
{
    /* link the placeholder item behind the given item */
    scan->ph_item.h_next = item->h_next;
    item->h_next = &scan->ph_item;
    scan->ph_linked = true;
}

static hash_item *_unlink_scan_placeholder(struct assoc_scan *scan)
{
    /* unlink the placeholder item and return the next item */
    hash_item **p = &assocp->roottable[scan->tabidx].hashtable[scan->bucket];
    assert(*p != NULL);
    while (*p != &scan->ph_item)
        p = &((*p)->h_next);
    *p = (*p)->h_next;
    scan->ph_linked = false;
    return *p;
}

void assoc_scan_init(struct assoc_scan *scan)
{
    /* initialize assoc_scan structure */
    scan->hashsz = assocp->hashsize;
    scan->bucket = 0;
    scan->tabcnt = 0; /* 0 means the scan on the current
                       * bucket chain has not yet started.
                       */
    _init_scan_placeholder(scan);
    scan->initialized = true;
}

int assoc_scan_next(struct assoc_scan *scan, hash_item **item_array, int array_size)
{
    assert(scan->initialized && array_size > 0);
    hash_item *next;
    int item_count = 0;
    int scan_cost = 0;
    int scan_done = false;

    while (scan->bucket < scan->hashsz)
    {
        if (scan->tabcnt == 0) {
            /* start the scan on the current bucket */
            scan->tabcnt = hashsize(assocp->infotable[scan->bucket].curpower);
            scan->tabidx = 0;
            assert(scan->tabcnt > 0);
            /* increment bucket's reference count */
            assocp->infotable[scan->bucket].refcount += 1;
        }

        while (scan->tabidx < scan->tabcnt) {
            if (scan_cost > (2*array_size) && item_count > 0) {
                /* too large scan cost, stop the scan */
                scan_done = true;  break;
            }
            if (scan->ph_linked) {
                next = _unlink_scan_placeholder(scan);
            } else {
                next = assocp->roottable[scan->tabidx].hashtable[scan->bucket];
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
                    _link_scan_placeholder(scan, next);
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
        assocp->infotable[scan->bucket].refcount -= 1;
        /* goto the next bucket */
        scan->bucket += 1;
        scan->tabcnt = 0;
    }
    if (item_count == 0) { /* NOT found */
        if (scan->bucket >= scan->hashsz)
            item_count = -1; /* the end */
    }
    return item_count;
}

void assoc_scan_final(struct assoc_scan *scan)
{
    assert(scan->initialized);

    if (scan->ph_linked) {
        (void)_unlink_scan_placeholder(scan);
    }
    if (scan->bucket < scan->hashsz) {
        /* decrement bucket's reference count */
        assocp->infotable[scan->bucket].refcount -= 1;
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

static prefix_t *_prefix_find(const char *prefix, const int nprefix, uint32_t hash)
{
    prefix_t *pt = assocp->prefix_hashtable[hash & hashmask(DEFAULT_PREFIX_HASHPOWER)];
    while (pt) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0)) {
            return pt;
        }
        pt = pt->h_next;
    }
    return NULL;
}

static int _prefix_insert(prefix_t *pt, uint32_t hash)
{
    assert(_prefix_find(_get_prefix(pt), pt->nprefix, hash) == NULL);

#ifdef NEW_PREFIX_STATS_MANAGEMENT
    (void)svcore->prefix_stats_insert(_get_prefix(pt), pt->nprefix);
#endif

    int bucket = hash & hashmask(DEFAULT_PREFIX_HASHPOWER);
    pt->h_next = assocp->prefix_hashtable[bucket];
    assocp->prefix_hashtable[bucket] = pt;

    assert(pt->parent_prefix != NULL);
    pt->parent_prefix->prefix_items++;
    assocp->tot_prefix_items++;
    return 1;
}

static void _prefix_delete(const char *prefix, const int nprefix, uint32_t hash)
{
    int bucket = hash & hashmask(DEFAULT_PREFIX_HASHPOWER);
    prefix_t *prev_pt = NULL;
    prefix_t *pt = assocp->prefix_hashtable[bucket];
    while (pt) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0))
            break; /* found */
        prev_pt = pt; pt = pt->h_next;
    }
    if (pt) {
        assert(pt->parent_prefix != NULL);
        pt->parent_prefix->prefix_items--;
        assocp->tot_prefix_items--;

        /* unlink and free the prefix structure */
        if (prev_pt) prev_pt->h_next = pt->h_next;
        else         assocp->prefix_hashtable[bucket] = pt->h_next;
        free(pt);

#ifdef NEW_PREFIX_STATS_MANAGEMENT
        (void)svcore->prefix_stats_delete(prefix, nprefix);
#endif
    }
}

prefix_t *assoc_prefix_find(const char *prefix, const int nprefix)
{
    if (nprefix == 0) { /* null prefix */
        return &assocp->noprefix_stats;
    }
    if (nprefix > 0) {
        return _prefix_find(prefix, nprefix, svcore->hash(prefix, nprefix, 0));
    } else {
        return NULL;
    }
}

ENGINE_ERROR_CODE assoc_prefix_link(hash_item *it, const uint32_t item_size,
                                    bool *internal)
{
    const char *key = item_get_key(it);
    uint32_t   nkey = it->nkey;
    int prefix_depth = 0;
    int i = 0;
    char *token;
    prefix_t *pt = NULL;
    prefix_t_list_elem prefix_list[DEFAULT_PREFIX_MAX_DEPTH];

    // prefix discovering: we don't even know prefix existence at this time
    while ((token = memchr(key+i+1, config->prefix_delimiter, nkey-i-1)) != NULL) {
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
            prefix_list[i].hash = svcore->hash(key, prefix_list[i].nprefix, 0);
            pt = _prefix_find(key, prefix_list[i].nprefix, prefix_list[i].hash);
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
                        _prefix_delete(key, prefix_list[j].nprefix, prefix_list[j].hash);
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
                _prefix_insert(pt, prefix_list[j].hash);
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

    *internal = (pt->internal ? true : false);
    return ENGINE_SUCCESS;
}

void assoc_prefix_unlink(hash_item *it, const uint32_t item_size, bool drop_if_empty)
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
            _prefix_delete(_get_prefix(pt), pt->nprefix,
                           svcore->hash(_get_prefix(pt), pt->nprefix, 0));

            pt = parent_pt;
        }
    }
}

bool assoc_prefix_issame(prefix_t *pt, const char *prefix, const int nprefix)
{
    assert(nprefix >= 0);

    if (nprefix == 0) { /* null prefix */
        if (pt->nprefix == 0)
            return true;
        else
            return false;
    } else {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0))
            return true;
        else
            return false;
    }
}

void assoc_prefix_bytes_incr(prefix_t *pt, ENGINE_ITEM_TYPE item_type, const uint32_t bytes)
{
    /* It's called when a collection element is inserted */
    assert(item_type > ITEM_TYPE_KV && item_type < ITEM_TYPE_MAX);

    pt->items_bytes[item_type] += bytes;
    pt->total_bytes_exclusive += bytes;
#if 0 // might be used later
    if (1) {
        prefix_t *curr_pt = pt->parent_prefix;
        while (curr_pt != NULL) {
            curr_pt->total_bytes_inclusive += bytes;
            curr_pt = curr_pt->parent_prefix;
        }
    }
#endif
}

void assoc_prefix_bytes_decr(prefix_t *pt, ENGINE_ITEM_TYPE item_type, const uint32_t bytes)
{
    /* It's called when a collection element is removed */
    assert(item_type > ITEM_TYPE_KV && item_type < ITEM_TYPE_MAX);

    pt->items_bytes[item_type] -= bytes;
    pt->total_bytes_exclusive -= bytes;
#if 0 // might be used later
    if (1) {
        prefix_t *curr_pt = pt->parent_prefix;
        while (curr_pt != NULL) {
            curr_pt->total_bytes_inclusive -= bytes;
            curr_pt = curr_pt->parent_prefix;
        }
    }
#endif
}

bool assoc_prefix_isvalid(hash_item *it, rel_time_t current_time)
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

#if 0 // might be used later
static uint32_t do_assoc_count_invalid_prefix(void)
{
    prefix_t *pt;
    uint32_t i, size = hashsize(DEFAULT_PREFIX_HASHPOWER);
    uint32_t invalid_prefix = 0;

    for (i = 0; i < size; i++) {
        pt = assocp->prefix_hashtable[i];
        while (pt) {
            if (pt->prefix_items == 0 && pt->total_count_exclusive == 0)
                invalid_prefix++;
            pt = pt->h_next;
        }
    }
    return invalid_prefix;
}
#endif

ENGINE_ERROR_CODE assoc_prefix_get_stats(const char *prefix, const int nprefix, void *prefix_data)
{
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
        uint32_t num_prefixes = assocp->tot_prefix_items;
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
            pt = assocp->prefix_hashtable[i];
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
        if (num_prefixes > assocp->tot_prefix_items) { /* include root prefix */
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
            pt = assocp->prefix_hashtable[i];
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
            pt = _prefix_find(prefix, nprefix, svcore->hash(prefix,nprefix,0));
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
            prefix_stats->tot_prefix_items = assocp->tot_prefix_items;
    }
    return ENGINE_SUCCESS;
}
