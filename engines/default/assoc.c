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

static struct assoc         *assocp=NULL; // engine assoc
static EXTENSION_LOGGER_DESCRIPTOR *logger;

static inline uint32_t CUR_HASH_TABIDX(uint32_t hash, uint32_t bucket)
{
    if (assocp->expanding) {
        /* Note that hash table buckets are expanded in backward */
        if (bucket < assocp->exp_bucket) { /* NOT yet expanded */
            return (hash >> assocp->hashpower) & assocp->prevmask;
        }
        if (bucket > assocp->exp_bucket) { /* Already expanded */
            return (hash >> assocp->hashpower) & assocp->rootmask;
        }
        if (true) { /* (bucket == assocp->exp_bucket) */
            uint32_t tabidx;
            tabidx = (hash >> assocp->hashpower) & assocp->prevmask;
            if (tabidx < assocp->exp_tabidx) { /* Already expanded */
                return (hash >> assocp->hashpower) & assocp->rootmask;
            }
            return tabidx;
        }
    } else {
        return (hash >> assocp->hashpower) & assocp->rootmask;
    }
}

ENGINE_ERROR_CODE assoc_init(struct default_engine *engine)
{
    /* initialize global variables */
    assocp = &engine->assoc;
    logger = engine->server.log->get_logger();

    assocp->hashpower = 17; /* (1<<17) => 128K hash size */
    assocp->hashsize = hashsize(assocp->hashpower);
    assocp->hashmask = hashmask(assocp->hashpower);
    assocp->rootpower = 0; /* (1<<0) => 1 hash table */
    assocp->rootsize = hashsize(assocp->rootpower);
    assocp->rootmask = hashmask(assocp->rootpower);
    assocp->roottabsz = 512;
    /* expansion status */
    assocp->expanding = false;

    assocp->roottable = NULL;
    assocp->infotable = NULL;

    /* hash items and expansion limit */
    assocp->hash_items = 0;
    assocp->hash_expansion_limit = (assocp->hashsize * assocp->rootsize * 3) / 2;

    assocp->infotable = calloc(assocp->hashsize, sizeof(struct bucket_info));
    if (assocp->infotable == NULL) {
        return ENGINE_ENOMEM;
    }
    assocp->roottable = calloc(assocp->roottabsz, sizeof(void *));
    if (assocp->roottable == NULL) {
        free(assocp->infotable);
        return ENGINE_ENOMEM;
    }
    assocp->roottable[0].hashtable = calloc(assocp->hashsize, sizeof(void*));
    if (assocp->roottable[0].hashtable == NULL) {
        free(assocp->infotable);
        free(assocp->roottable);
        return ENGINE_ENOMEM;
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "ASSOC module initialized.\n");
    return ENGINE_SUCCESS;
}

void assoc_final(struct default_engine *engine)
{
    if (assocp == NULL) {
        return; /* nothing to do */
    }
    if (assocp->roottable) {
        if (assocp->roottable[0].hashtable) {
            free(assocp->roottable[0].hashtable);
        }
        for (int ii=0; ii < assocp->rootpower; ++ii) {
            int table_count = hashsize(ii); //2 ^ n
            if (assocp->roottable[table_count].hashtable)
                free(assocp->roottable[table_count].hashtable);
        }
        free(assocp->roottable);
    }
    if (assocp->infotable) {
        free(assocp->infotable);
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "ASSOC module destroyed.\n");
}

static void redistribute(void)
{
    hash_item **prev;
    hash_item *it;
    uint32_t tabidx;

    /* How many buckets should redistribute at a time?
     * If it is too few, it will longer hashtable expansion and decrease average throughput.
     * If it is too many, it will shorten hashtable expansion but it can result a throughput drop.
     */
    for (uint32_t rdbcnt = 0; rdbcnt < 4; rdbcnt++) {
        prev = &assocp->roottable[assocp->exp_tabidx].hashtable[assocp->exp_bucket];
        while ((it = *prev) != NULL) {
            tabidx = (it->khash >> assocp->hashpower) & assocp->rootmask;
            //tabidx = GET_HASH_TABIDX(it->khash, assocp->hashpower, assocp->rootmask);
            if (tabidx == assocp->exp_tabidx) {
                prev = &it->h_next;
            } else {
                *prev = it->h_next;
                it->h_next = assocp->roottable[tabidx].hashtable[assocp->exp_bucket];
                assocp->roottable[tabidx].hashtable[assocp->exp_bucket] = it;
            }
        }
        assocp->exp_tabidx += 1;
        if (assocp->exp_tabidx >= assocp->prevsize) break;
    }

    if (assocp->exp_tabidx >= assocp->prevsize) {
        assocp->exp_tabidx = 0;
        /* set the next bucket in backward */
        if (assocp->exp_bucket > 0) {
            assocp->exp_bucket -= 1;
        } else {
            /* No bucket to expand. Stop expansion */
            assocp->expanding = false;
            logger->log(EXTENSION_LOG_INFO, NULL, "hash table expansion completed.\n");
        }
    }
}

hash_item *assoc_find(const char *key, const uint32_t nkey, uint32_t hash)
{
    hash_item *it;
    int depth = 0;
    uint32_t bucket = GET_HASH_BUCKET(hash, assocp->hashmask);
    uint32_t tabidx = CUR_HASH_TABIDX(hash, bucket);

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
    uint32_t tabidx = CUR_HASH_TABIDX(hash, bucket);

    pos = &assocp->roottable[tabidx].hashtable[bucket];
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, item_get_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

static int assoc_expand_roottable(uint32_t new_roottabsz)
{
    struct table *new_roottable;

    new_roottable = realloc(assocp->roottable, sizeof(void*) * new_roottabsz);
    if (new_roottable == NULL) {
        return -1;
    }
    assocp->roottable = new_roottable;
    assocp->roottabsz = new_roottabsz;
    return 0;
}

/* grows the hashtable to the next power of 2. */
static void assoc_expand(void)
{
    hash_item** new_hashtable;

    if (assocp->roottabsz < (assocp->rootsize * 2)) {
        if (assoc_expand_roottable(assocp->roottabsz * 2) < 0) {
            return;
        }
    }

    new_hashtable = calloc(assocp->hashsize * assocp->rootsize, sizeof(void *));
    if (new_hashtable == NULL) {
        return;
    }
    for (int ii=0; ii < assocp->rootsize; ++ii) {
        assocp->roottable[assocp->rootsize+ii].hashtable = &new_hashtable[assocp->hashsize*ii];
    }
    assocp->rootpower += 1;
    assocp->prevsize = assocp->rootsize;
    assocp->prevmask = assocp->rootmask;
    assocp->rootsize = hashsize(assocp->rootpower);
    assocp->rootmask = hashmask(assocp->rootpower);

    /* set hash_expansion_limit */
    assocp->hash_expansion_limit = (assocp->hashsize * assocp->rootsize * 3) / 2;

    /* set hash table expansion */
    assocp->expanding = true;
    assocp->exp_bucket = assocp->hashsize - 1;
    assocp->exp_tabidx = 0;

    logger->log(EXTENSION_LOG_INFO, NULL, "hash table expansion started(size: %u -> %u).\n",
            assocp->hashsize * assocp->rootsize / 2, assocp->hashsize * assocp->rootsize);
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(hash_item *it, uint32_t hash)
{
    uint32_t bucket = GET_HASH_BUCKET(hash, assocp->hashmask);
    uint32_t tabidx = CUR_HASH_TABIDX(hash, bucket);

    /* shouldn't have duplicately named things defined */
    assert(assoc_find(item_get_key(it), it->nkey, hash) == 0);

    /* inserting actual hash_item to appropriate assoc_t */
    it->h_next = assocp->roottable[tabidx].hashtable[bucket];
    assocp->roottable[tabidx].hashtable[bucket] = it;

    assocp->hash_items++;

    if (assocp->expanding) {
        if (assocp->infotable[assocp->exp_bucket].refcount == 0) {
            redistribute();
        }
    } else {
        if (assocp->hash_items > assocp->hash_expansion_limit) {
            assoc_expand();
        }
    }

    MEMCACHED_ASSOC_INSERT(item_get_key(it), it->nkey, assocp->hash_items);
    return 1;
}

void assoc_replace(hash_item *old_it, hash_item *new_it)
{
    hash_item **before = _hashitem_before(item_get_key(old_it), old_it->nkey, old_it->khash);

    /* The DTrace probe cannot be triggered as the last instruction
     * due to possible tail-optimization by the compiler
     */
    MEMCACHED_ASSOC_DELETE(key, old_it->nkey, assocp->hash_items);
    new_it->h_next = old_it->h_next;
    *before = new_it;
    (old_it)->h_next = NULL;

    MEMCACHED_ASSOC_INSERT(item_get_key(new_it), new_it->nkey, assocp->hash_items);
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

int assoc_scan_next(struct assoc_scan *scan, hash_item **item_array,
                    int array_size, int elem_limit)
{
    assert(scan->initialized && array_size > 0);
    hash_item *next;
    coll_meta_info *info;
    int item_count = 0;
    int elem_count = 0;
    int scan_cost = 0;
    int scan_done = false;

    while (scan->bucket < scan->hashsz)
    {
        if (scan->tabcnt == 0) {
            /* start the scan on the current bucket */
            scan->tabcnt = assocp->rootsize;
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
                    item_array[item_count++] = next; /* user cache item */
                    if (item_count >= array_size) {
                        break;
                    }
                    if (elem_limit > 0 && IS_COLL_ITEM(next)) {
                        info = (coll_meta_info *)item_get_meta(next);
                        elem_count += info->ccnt;
                        if (elem_count > elem_limit)
                            break;
                    }
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

bool assoc_scan_in_visited_area(struct assoc_scan *scan, hash_item *it)
{
    assert(scan->initialized);
    uint32_t bucket = GET_HASH_BUCKET(it->khash, assocp->hashmask);
    uint32_t tabidx;

    /* The given item is in the visited area if
     * (1) it's bucket < scan's bucket
     * (2) it's bucker == scan's bucket, but it's tabidx < scan's tabidx
     * (3) or, it comes before the scan
     */
    if (bucket < scan->bucket) {
        return true;
    }
    if (bucket == scan->bucket) {
        tabidx = CUR_HASH_TABIDX(it->khash, bucket);
        if (tabidx < scan->tabidx) {
            return true;
        }
        if (tabidx == scan->tabidx) {
            hash_item *p = assocp->roottable[tabidx].hashtable[scan->bucket];
            if (scan->ph_linked) {
                while (p != &scan->ph_item) {
                    if (p == it) { /* We hit it before scan */
                        return true;
                    }
                    p = p->h_next;
                }
            }
            /* No, we hit the scan first */
        }
    }
    return false;
}

void assoc_scan_final(struct assoc_scan *scan)
{
    assert(scan->initialized);

    if (scan->ph_linked) {
        (void)_unlink_scan_placeholder(scan);
    }
    if (scan->bucket < scan->hashsz && scan->tabcnt > 0) {
        /* decrement bucket's reference count */
        assocp->infotable[scan->bucket].refcount -= 1;
    }
    scan->initialized = false;
}
