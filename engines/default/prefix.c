/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015-current JaM2in Co., Ltd.
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

#define DEFAULT_PREFIX_HASHPOWER 10
#define DEFAULT_PREFIX_MAX_DEPTH 1

typedef struct {
    prefix_t   *pt;
    uint8_t     nprefix;
    uint32_t    hash;
} prefix_t_list_elem;

static struct engine_config *config=NULL; // engine config
static struct prefix        *prefxp=NULL; // engine prefix
static SERVER_CORE_API      *svcore=NULL; // server core api
static EXTENSION_LOGGER_DESCRIPTOR *logger;
static prefix_t *null_pt = NULL; /* null prefix info */

ENGINE_ERROR_CODE prefix_init(struct default_engine *engine)
{
    /* initialize global variables */
    config = &engine->config;
    prefxp = &engine->prefix;
    svcore = engine->server.core;
    logger = engine->server.log->get_logger();

    prefxp->tot_prefix_items = 0;

    prefxp->hashtable = calloc(hashsize(DEFAULT_PREFIX_HASHPOWER), sizeof(void *));
    if (prefxp->hashtable == NULL) {
        return ENGINE_ENOMEM;
    }
    // initialize null prefix stats info
    memset(&prefxp->null_prefix_data, 0, sizeof(prefix_t));
    null_pt = &prefxp->null_prefix_data;


    logger->log(EXTENSION_LOG_INFO, NULL, "PREFIX module initialized.\n");
    return ENGINE_SUCCESS;
}

void prefix_final(struct default_engine *engine)
{
    if (prefxp == NULL) {
        return; /* nothing to do */
    }
    if (prefxp->hashtable) {
        free(prefxp->hashtable);
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "PREFIX module destroyed.\n");
}

static inline void *_get_prefix(prefix_t *prefix)
{
    return (void*)(prefix + 1);
}

static prefix_t *_prefix_find(const char *prefix, const int nprefix, uint32_t hash)
{
    prefix_t *pt = prefxp->hashtable[hash & hashmask(DEFAULT_PREFIX_HASHPOWER)];
    while (pt) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0))
            break;
        pt = pt->h_next;
    }
    return pt;
}

#ifdef NESTED_PREFIX
static void _prefix_inclusive_stats_init(prefix_t* pt, bool isleaf)
{
    if (isleaf) {
        pt->items_count_inclusive[ITEM_TYPE_KV] = 0;
        pt->items_bytes_inclusive[ITEM_TYPE_KV] = 0;
        pt->items_count_inclusive[ITEM_TYPE_LIST] = 0;
        pt->items_bytes_inclusive[ITEM_TYPE_LIST] = 0;
        pt->items_count_inclusive[ITEM_TYPE_SET] = 0;
        pt->items_bytes_inclusive[ITEM_TYPE_SET] = 0;
        pt->items_count_inclusive[ITEM_TYPE_MAP] = 0;
        pt->items_bytes_inclusive[ITEM_TYPE_MAP] = 0;
        pt->items_count_inclusive[ITEM_TYPE_BTREE] = 0;
        pt->items_bytes_inclusive[ITEM_TYPE_BTREE] = 0;
        pt->total_count_inclusive = 0;
        pt->total_bytes_inclusive = 0;
    } else {
        pt->items_count_inclusive[ITEM_TYPE_KV] = pt->items_count_exclusive[ITEM_TYPE_KV];
        pt->items_bytes_inclusive[ITEM_TYPE_KV] = pt->items_bytes_exclusive[ITEM_TYPE_KV];
        pt->items_count_inclusive[ITEM_TYPE_LIST] = pt->items_count_exclusive[ITEM_TYPE_LIST];
        pt->items_bytes_inclusive[ITEM_TYPE_LIST] = pt->items_bytes_exclusive[ITEM_TYPE_LIST];
        pt->items_count_inclusive[ITEM_TYPE_SET] = pt->items_count_exclusive[ITEM_TYPE_SET];
        pt->items_bytes_inclusive[ITEM_TYPE_SET] = pt->items_bytes_exclusive[ITEM_TYPE_SET];
        pt->items_count_inclusive[ITEM_TYPE_MAP] = pt->items_count_exclusive[ITEM_TYPE_MAP];
        pt->items_bytes_inclusive[ITEM_TYPE_MAP] = pt->items_bytes_exclusive[ITEM_TYPE_MAP];
        pt->items_count_inclusive[ITEM_TYPE_BTREE] = pt->items_count_exclusive[ITEM_TYPE_BTREE];
        pt->items_bytes_inclusive[ITEM_TYPE_BTREE] = pt->items_bytes_exclusive[ITEM_TYPE_BTREE];
        pt->total_count_inclusive = pt->total_count_exclusive;
        pt->total_bytes_inclusive = pt->total_count_exclusive;
    }
}
#endif

static int _prefix_insert(prefix_t *pt, uint32_t hash)
{
    assert(_prefix_find(_get_prefix(pt), pt->nprefix, hash) == NULL);

#ifdef NEW_PREFIX_STATS_MANAGEMENT
    (void)svcore->prefix_stats_insert(_get_prefix(pt), pt->nprefix);
#endif

    int bucket = hash & hashmask(DEFAULT_PREFIX_HASHPOWER);
    pt->h_next = prefxp->hashtable[bucket];
    prefxp->hashtable[bucket] = pt;

#ifdef NESTED_PREFIX
    if (pt->parent_prefix != NULL) {
        pt->parent_prefix->prefix_items++;
        if (pt->parent_prefix->prefix_items == 1)
            _prefix_inclusive_stats_init(pt->parent_prefix, false);
    } else {
        prefxp->tot_prefix_items++;
    }
#else
    assert(pt->parent_prefix != NULL);
    pt->parent_prefix->prefix_items++;
    prefxp->tot_prefix_items++;
#endif
    return 1;
}

static void _prefix_delete(const char *prefix, const int nprefix, uint32_t hash)
{
    int bucket = hash & hashmask(DEFAULT_PREFIX_HASHPOWER);
    prefix_t *prev_pt = NULL;
    prefix_t *pt = prefxp->hashtable[bucket];
    while (pt) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0))
            break; /* found */
        prev_pt = pt;
        pt = pt->h_next;
    }
    if (pt) {
#ifdef NESTED_PREFIX
        if (pt->parent_prefix != NULL) {
            pt->parent_prefix->prefix_items--;
            if (pt->parent_prefix->prefix_items == 0)
                _prefix_inclusive_stats_init(pt->parent_prefix, true);
        } else {
            prefxp->tot_prefix_items--;
        }
#else
        assert(pt->parent_prefix != NULL);
        pt->parent_prefix->prefix_items--;
        prefxp->tot_prefix_items--;
#endif
        /* unlink and free the prefix structure */
        if (prev_pt) prev_pt->h_next = pt->h_next;
        else         prefxp->hashtable[bucket] = pt->h_next;
        free(pt);

#ifdef NEW_PREFIX_STATS_MANAGEMENT
        (void)svcore->prefix_stats_delete(prefix, nprefix);
#endif
    }
}

prefix_t *prefix_find(const char *prefix, const int nprefix)
{
    if (nprefix < 0) {
        return NULL;
    }
    if (nprefix > 0) {
        return _prefix_find(prefix, nprefix, svcore->hash(prefix, nprefix, 0));
    } else {
        return &prefxp->null_prefix_data; /* null prefix */
    }
}

ENGINE_ERROR_CODE prefix_link(hash_item *it, const uint32_t item_size, bool *internal)
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
        pt = null_pt;
        time(&pt->create_time);
        /* save prefix pointer in hash_item */
        it->pfxptr = pt;
    } else {
        for (i = prefix_depth-1; i >= 0; i--) {
            prefix_list[i].hash = svcore->hash(key, prefix_list[i].nprefix, 0);
            pt = _prefix_find(key, prefix_list[i].nprefix, prefix_list[i].hash);
            if (pt != NULL) break;
#ifdef NESTED_PREFIX
            if (i == 0) {
                if (!mc_isvalidname(key, prefix_list[0].nprefix)) {
                    return ENGINE_PREFIX_ENAME; /* Invalid prefix name */
                }
            } else {
                uint32_t prefix_offset = prefix_list[i - 1].nprefix + 1;
                if (!mc_isvalidname(key + prefix_offset,
                                    prefix_list[i].nprefix - prefix_offset)) {
                    return ENGINE_PREFIX_ENAME; /* Invalid prefix name */
                }
            }
#endif
        }
        if (i < (prefix_depth-1)) {
#ifdef NESTED_PREFIX
#else
            if (prefix_depth == 1) {
                if (!mc_isvalidname(key, prefix_list[0].nprefix)) {
                    return ENGINE_PREFIX_ENAME; /* Invalid prefix name */
                }
            }
#endif
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
#ifdef NESTED_PREFIX
                if (PREFIX_IS_RSVD(key, prefix_list[0].nprefix)) {
                    pt->internal = 1; /* internal prefix */
                }
                pt->parent_prefix = (j == 0 ? NULL : prefix_list[j-1].pt);
#else
                if (PREFIX_IS_RSVD(key, pt->nprefix)) {
                    pt->internal = 1; /* internal prefix */
                }
                pt->parent_prefix = (j == 0 ? null_pt : prefix_list[j-1].pt);
#endif
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
#ifdef NESTED_PREFIX
    if (pt->prefix_items > 0) {
        pt->items_count_inclusive[item_type] += 1;
        pt->items_bytes_inclusive[item_type] += item_size;
        pt->total_count_inclusive += 1;
        pt->total_bytes_inclusive += item_size;
    }
    pt->items_count_exclusive[item_type] += 1;
    pt->items_bytes_exclusive[item_type] += item_size;
    pt->total_count_exclusive += 1;
    pt->total_bytes_exclusive += item_size;

    prefix_t *parent_pt = pt->parent_prefix;
    while (parent_pt != NULL) {
        parent_pt->items_count_inclusive[item_type] += 1;
        parent_pt->items_bytes_inclusive[item_type] += item_size;
        parent_pt->total_count_inclusive += 1;
        parent_pt->total_bytes_inclusive += item_size;
        parent_pt = parent_pt->parent_prefix;
    }
#else
    pt->items_count[item_type] += 1;
    pt->items_bytes[item_type] += item_size;
    pt->total_count_exclusive += 1;
    pt->total_bytes_exclusive += item_size;
#endif

    *internal = (pt->internal ? true : false);
    return ENGINE_SUCCESS;
}

void prefix_unlink(hash_item *it, const uint32_t item_size, bool drop_if_empty)
{
    prefix_t *pt = it->pfxptr;
    it->pfxptr = NULL;
    assert(pt != NULL);

    /* update prefix information */
    int item_type = GET_ITEM_TYPE(it);
#ifdef NESTED_PREFIX
    if (pt->prefix_items > 0) {
        pt->items_count_inclusive[item_type] -= 1;
        pt->items_bytes_inclusive[item_type] -= item_size;
        pt->total_count_inclusive -= 1;
        pt->total_bytes_inclusive -= item_size;
    }
    pt->items_count_exclusive[item_type] -= 1;
    pt->items_bytes_exclusive[item_type] -= item_size;
    pt->total_count_exclusive -= 1;
    pt->total_bytes_exclusive -= item_size;

    prefix_t *parent_pt = pt->parent_prefix;
    while (parent_pt != NULL) {
        parent_pt->items_count_inclusive[item_type] -= 1;
        parent_pt->items_bytes_inclusive[item_type] -= item_size;
        parent_pt->total_count_inclusive -= 1;
        parent_pt->total_bytes_inclusive -= item_size;
        parent_pt = parent_pt->parent_prefix;
    }
#else
    pt->items_count[item_type] -= 1;
    pt->items_bytes[item_type] -= item_size;
    pt->total_count_exclusive -= 1;
    pt->total_bytes_exclusive -= item_size;
#endif

    if (drop_if_empty) {
        while (pt != NULL && pt != null_pt) {
#ifdef NESTED_PREFIX
            parent_pt = pt->parent_prefix;
#else
            prefix_t *parent_pt = pt->parent_prefix;
#endif
            if (pt->prefix_items > 0 || pt->total_count_exclusive > 0)
                break; /* NOT empty */
            assert(pt->total_bytes_exclusive == 0);
            _prefix_delete(_get_prefix(pt), pt->nprefix,
                           svcore->hash(_get_prefix(pt), pt->nprefix, 0));
            pt = parent_pt;
        }
    }
}

/* if prefix has child prefixes */
bool prefix_isincluded(prefix_t *pt, const char *prefix, const int nprefix)
{
    assert(nprefix >= 0);

    if (nprefix > 0) {
        char* pfx = _get_prefix(pt);
        if (pt->nprefix == nprefix && memcmp(pfx, prefix, nprefix) == 0)
            return true;
        if (pt->nprefix > nprefix && *(pfx+nprefix) == config->prefix_delimiter
                                  && memcmp(pfx, prefix, nprefix) == 0)
            return true;
    } else { /* null prefix */
        if (pt->nprefix == 0)
            return true;
    }
    return false;
}

bool prefix_issame(prefix_t *pt, const char *prefix, const int nprefix)
{
    assert(nprefix >= 0);

    if (nprefix > 0) {
        if ((nprefix == pt->nprefix) && (memcmp(prefix, _get_prefix(pt), nprefix) == 0))
            return true;
    } else { /* null prefix */
        if (pt->nprefix == 0)
            return true;
    }
    return false;
}

void prefix_bytes_incr(prefix_t *pt, ENGINE_ITEM_TYPE item_type, const uint32_t bytes)
{
    /* It's called when a collection element is inserted */
    assert(item_type < ITEM_TYPE_MAX);

#ifdef NESTED_PREFIX
    pt->items_bytes_exclusive[item_type] += bytes;
    pt->total_bytes_exclusive += bytes;
    if (pt->prefix_items > 0) {
        pt->items_bytes_inclusive[item_type] += bytes;
        pt->total_bytes_inclusive += bytes;
    }

    prefix_t *parent_pt = pt->parent_prefix;
    while (parent_pt != NULL) {
        parent_pt->items_bytes_inclusive[item_type] += bytes;
        parent_pt->total_bytes_inclusive += bytes;
        parent_pt = parent_pt->parent_prefix;
    }
#else
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
#endif
}

void prefix_bytes_decr(prefix_t *pt, ENGINE_ITEM_TYPE item_type, const uint32_t bytes)
{
    /* It's called when a collection element is removed */
    assert(item_type < ITEM_TYPE_MAX);

#ifdef NESTED_PREFIX
    pt->items_bytes_exclusive[item_type] -= bytes;
    pt->total_bytes_exclusive -= bytes;
    if (pt->prefix_items > 0) {
        pt->items_bytes_inclusive[item_type] -= bytes;
        pt->total_bytes_inclusive -= bytes;
    }

    prefix_t *parent_pt = pt->parent_prefix;
    while (parent_pt != NULL) {
        parent_pt->items_bytes_inclusive[item_type] -= bytes;
        parent_pt->total_bytes_inclusive -= bytes;
        parent_pt = parent_pt->parent_prefix;
    }
#else
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
#endif
}

bool prefix_isvalid(hash_item *it, rel_time_t current_time)
{
    prefix_t *pt = it->pfxptr;
    do {
        if (pt->oldest_live != 0 &&
            pt->oldest_live <= current_time &&
            it->time <= pt->oldest_live)
            return false;
        /* traverse parent prefixes to validate them */
        pt = pt->parent_prefix;
    } while(pt != NULL && pt != null_pt);

    return true;
}

#if 0 // might be used later
static uint32_t do_count_invalid_prefix(void)
{
    prefix_t *pt;
    uint32_t i, size = hashsize(DEFAULT_PREFIX_HASHPOWER);
    uint32_t invalid_prefix = 0;

    for (i = 0; i < size; i++) {
        pt = prefxp->hashtable[i];
        while (pt) {
            if (pt->prefix_items == 0 && pt->total_count_exclusive == 0)
                invalid_prefix++;
            pt = pt->h_next;
        }
    }
    return invalid_prefix;
}
#endif

uint32_t prefix_count(void)
{
    return prefxp->tot_prefix_items;
}

ENGINE_ERROR_CODE prefix_get_stats(const char *prefix, const int nprefix, void *prefix_data)
{
    prefix_t *pt;

    if (nprefix < 0) /* all prefix stats */
    {
#ifdef NESTED_PREFIX
        const char *format = "PREFIX %s "
                             "itm %llu kitm %llu litm %llu sitm %llu mitm %llu bitm %llu " /* total item count */
                             "tsz %llu ktsz %llu ltsz %llu stsz %llu mtsz %llu btsz %llu " /* total item bytes */
                             "chd %llu citm %llu ctsz %llu "
                             "time %04d%02d%02d%02d%02d%02d\r\n"; /* create time */
#else
        const char *format = "PREFIX %s "
                             "itm %llu kitm %llu litm %llu sitm %llu mitm %llu bitm %llu " /* total item count */
                             "tsz %llu ktsz %llu ltsz %llu stsz %llu mtsz %llu btsz %llu " /* total item bytes */
                             "time %04d%02d%02d%02d%02d%02d\r\n"; /* create time */
#endif
        char *buffer;
        struct tm *t;
        uint32_t prefix_hsize = hashsize(DEFAULT_PREFIX_HASHPOWER);
        uint32_t num_prefixes = prefxp->tot_prefix_items;
        uint32_t sum_nameleng = 0; /* sum of prefix name length */
        uint32_t i, buflen, pos;

        /* get # of prefixes and length of prefix names */
        if (null_pt->total_count_exclusive > 0) {
            /* Include the null prefix if it is valid */
            num_prefixes += 1;
            sum_nameleng += strlen("<null>");
        }
        for (i = 0; i < prefix_hsize; i++) {
            pt = prefxp->hashtable[i];
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
#ifdef NESTED_PREFIX
        buflen = sizeof(uint32_t) /* length */
               + sum_nameleng
               + num_prefixes * (strlen(format) - 2 /* %s replaced by prefix name */
                                 + (15 * (20 - 4))  /* %llu replaced by 20-digit num */
                                 - ( 5 * ( 4 - 2))) /* %02d replaced by 2-digit num */
               + sizeof("END\r\n"); /* tail string */
#else
        buflen = sizeof(uint32_t) /* length */
               + sum_nameleng
               + num_prefixes * (strlen(format) - 2 /* %s replaced by prefix name */
                                 + (12 * (20 - 4))  /* %llu replaced by 20-digit num */
                                 - ( 5 * ( 4 - 2))) /* %02d replaced by 2-digit num */
               + sizeof("END\r\n"); /* tail string */
#endif

        if ((buffer = malloc(buflen)) == NULL) {
            return ENGINE_ENOMEM;
        }

        /* write prefix stats in the buffer */
        pos = sizeof(uint32_t);
        if (num_prefixes > prefxp->tot_prefix_items) { /* include null prefix */
            pt = null_pt;
            t = localtime(&pt->create_time);
#ifdef NESTED_PREFIX
            assert(pt->prefix_items == 0);
            pos += snprintf(buffer+pos, buflen-pos, format, "<null>",
                            pt->total_count_exclusive,
                            pt->items_count_exclusive[ITEM_TYPE_KV],
                            pt->items_count_exclusive[ITEM_TYPE_LIST],
                            pt->items_count_exclusive[ITEM_TYPE_SET],
                            pt->items_count_exclusive[ITEM_TYPE_MAP],
                            pt->items_count_exclusive[ITEM_TYPE_BTREE],
                            pt->total_bytes_exclusive,
                            pt->items_bytes_exclusive[ITEM_TYPE_KV],
                            pt->items_bytes_exclusive[ITEM_TYPE_LIST],
                            pt->items_bytes_exclusive[ITEM_TYPE_SET],
                            pt->items_bytes_exclusive[ITEM_TYPE_MAP],
                            pt->items_bytes_exclusive[ITEM_TYPE_BTREE],
                            pt->prefix_items,
                            0,
                            0,
                            t->tm_year+1900, t->tm_mon+1, t->tm_mday,
                            t->tm_hour, t->tm_min, t->tm_sec);
#else
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
#endif
            assert(pos < buflen);
        }
        for (i = 0; i < prefix_hsize; i++) {
            pt = prefxp->hashtable[i];
            while (pt) {
#ifdef NESTED_PREFIX
                if (pt->parent_prefix != NULL) {
                    pt = pt->h_next;
                    continue;
                }
                t = localtime(&pt->create_time);
                if (pt->prefix_items > 0) {
                    pos += snprintf(buffer+pos, buflen-pos, format, _get_prefix(pt),
                                    pt->total_count_inclusive,
                                    pt->items_count_inclusive[ITEM_TYPE_KV],
                                    pt->items_count_inclusive[ITEM_TYPE_LIST],
                                    pt->items_count_inclusive[ITEM_TYPE_SET],
                                    pt->items_count_inclusive[ITEM_TYPE_MAP],
                                    pt->items_count_inclusive[ITEM_TYPE_BTREE],
                                    pt->total_bytes_inclusive,
                                    pt->items_bytes_inclusive[ITEM_TYPE_KV],
                                    pt->items_bytes_inclusive[ITEM_TYPE_LIST],
                                    pt->items_bytes_inclusive[ITEM_TYPE_SET],
                                    pt->items_bytes_inclusive[ITEM_TYPE_MAP],
                                    pt->items_bytes_inclusive[ITEM_TYPE_BTREE],
                                    pt->prefix_items,
                                    pt->total_count_inclusive - pt->total_count_exclusive,
                                    pt->total_bytes_inclusive - pt->total_bytes_exclusive,
                                    t->tm_year+1900, t->tm_mon+1, t->tm_mday,
                                    t->tm_hour, t->tm_min, t->tm_sec);
                } else {
                    pos += snprintf(buffer+pos, buflen-pos, format, _get_prefix(pt),
                                    pt->total_count_exclusive,
                                    pt->items_count_exclusive[ITEM_TYPE_KV],
                                    pt->items_count_exclusive[ITEM_TYPE_LIST],
                                    pt->items_count_exclusive[ITEM_TYPE_SET],
                                    pt->items_count_exclusive[ITEM_TYPE_MAP],
                                    pt->items_count_exclusive[ITEM_TYPE_BTREE],
                                    pt->total_bytes_exclusive,
                                    pt->items_bytes_exclusive[ITEM_TYPE_KV],
                                    pt->items_bytes_exclusive[ITEM_TYPE_LIST],
                                    pt->items_bytes_exclusive[ITEM_TYPE_SET],
                                    pt->items_bytes_exclusive[ITEM_TYPE_MAP],
                                    pt->items_bytes_exclusive[ITEM_TYPE_BTREE],
                                    pt->prefix_items,
                                    0,
                                    0,
                                    t->tm_year+1900, t->tm_mon+1, t->tm_mday,
                                    t->tm_hour, t->tm_min, t->tm_sec);
                }
#else
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
#endif
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
            pt = null_pt;
        }
        if (pt == NULL) {
            return ENGINE_PREFIX_ENOENT;
        }

#ifdef NESTED_PREFIX
        prefix_stats->hash_items = pt->items_count_inclusive[ITEM_TYPE_KV];
        prefix_stats->hash_items_bytes = pt->items_bytes_inclusive[ITEM_TYPE_KV];
#else
        prefix_stats->hash_items = pt->items_count[ITEM_TYPE_KV];
        prefix_stats->hash_items_bytes = pt->items_bytes[ITEM_TYPE_KV];
#endif
        prefix_stats->prefix_items = pt->prefix_items;
        if (prefix != NULL)
            prefix_stats->tot_prefix_items = pt->prefix_items;
        else
            prefix_stats->tot_prefix_items = prefxp->tot_prefix_items;
    }
    return ENGINE_SUCCESS;
}
