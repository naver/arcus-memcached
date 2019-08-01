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
/*
 * Detailed statistics management. For simple stats like total number of
 * "get" requests, we use inline code in memcached.c and friends, but when
 * stats detail mode is activated, the code here records more information.
 *
 * Author:
 *   Steven Grimm <sgrimm@facebook.com>
 */
#include "config.h"
#include "cmd_in_second.h"
#include "memcached.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
 * Stats are tracked on the basis of key prefixes. This is a simple
 * fixed-size hash of prefixes; we run the prefixes through the same
 * CRC function used by the cache hashtable.
 */
typedef struct _prefix_stats PREFIX_STATS;
struct _prefix_stats {
    char         *prefix;
    size_t        prefix_len;
    uint64_t      num_gets;
    uint64_t      num_sets;
    uint64_t      num_deletes;
    uint64_t      num_hits;
    uint64_t      num_incrs;
    uint64_t      num_decrs;
    uint64_t      num_lop_creates;
    uint64_t      num_lop_inserts;
    uint64_t      num_lop_deletes;
    uint64_t      num_lop_gets;
    uint64_t      num_sop_creates;
    uint64_t      num_sop_inserts;
    uint64_t      num_sop_deletes;
    uint64_t      num_sop_gets;
    uint64_t      num_sop_exists;
    uint64_t      num_mop_creates;
    uint64_t      num_mop_inserts;
    uint64_t      num_mop_updates;
    uint64_t      num_mop_deletes;
    uint64_t      num_mop_gets;
    uint64_t      num_bop_creates;
    uint64_t      num_bop_inserts;
    uint64_t      num_bop_updates;
    uint64_t      num_bop_deletes;
    uint64_t      num_bop_incrs;
    uint64_t      num_bop_decrs;
    uint64_t      num_bop_gets;
    uint64_t      num_bop_counts;
    uint64_t      num_bop_positions; /* find position */
    uint64_t      num_bop_pwgs;      /* find position with get */
    uint64_t      num_bop_gbps;      /* get by position */
    uint64_t      num_getattrs;
    uint64_t      num_setattrs;
    uint64_t      num_lop_insert_hits;
    uint64_t      num_lop_delete_hits;
    uint64_t      num_lop_get_hits;
    uint64_t      num_sop_insert_hits;
    uint64_t      num_sop_delete_hits;
    uint64_t      num_sop_get_hits;
    uint64_t      num_sop_exist_hits;
    uint64_t      num_mop_insert_hits;
    uint64_t      num_mop_update_hits;
    uint64_t      num_mop_delete_hits;
    uint64_t      num_mop_get_hits;
    uint64_t      num_bop_insert_hits;
    uint64_t      num_bop_update_hits;
    uint64_t      num_bop_delete_hits;
    uint64_t      num_bop_incr_hits;
    uint64_t      num_bop_decr_hits;
    uint64_t      num_bop_get_hits;
    uint64_t      num_bop_count_hits;
    uint64_t      num_bop_position_hits;
    uint64_t      num_bop_pwg_hits;
    uint64_t      num_bop_gbp_hits;
    PREFIX_STATS *next;
};

//#define PREFIX_HASH_SIZE 256
#define PREFIX_HASH_SIZE 1024
#define PREFIX_MAX_DEPTH 1
#define PREFIX_MAX_COUNT 10000

static PREFIX_STATS *prefix_stats[PREFIX_HASH_SIZE];
static void (*func_when_prefix_overflow)(void);
static int max_prefixes = PREFIX_MAX_COUNT;
static int num_prefixes = 0;
static int total_prefix_size = 0;
static char *null_prefix_str = "<null>";

void stats_prefix_init(void (*cb_when_prefix_overflow)(void)) {
    memset(prefix_stats, 0, sizeof(prefix_stats));
    /* callback function when prefix overflow */
    func_when_prefix_overflow = cb_when_prefix_overflow;
}

/*
 * Cleans up all our previously collected stats. NOTE: the stats lock is
 * assumed to be held when this is called.
 */
void stats_prefix_clear() {
    PREFIX_STATS *curr, *next;
    int hidx;

    for (hidx = 0; hidx < PREFIX_HASH_SIZE; hidx++) {
        for (curr = prefix_stats[hidx]; curr != NULL; curr = next) {
            next = curr->next;
            free(curr->prefix);
            free(curr);
        }
        prefix_stats[hidx] = NULL;
    }
    num_prefixes = 0;
    total_prefix_size = 0;
}

int stats_prefix_count() {
    return num_prefixes;
}

#ifdef NEW_PREFIX_STATS_MANAGEMENT
int stats_prefix_insert(const char *prefix, const size_t nprefix) {
    PREFIX_STATS *pfs = NULL;
    uint32_t hashval = mc_hash(prefix, nprefix, 0) % PREFIX_HASH_SIZE;

    STATS_LOCK();
    do {
        if (num_prefixes >= max_prefixes) {
            /* prefix overflow */
            func_when_prefix_overflow();
            break;
        }

        pfs = calloc(sizeof(PREFIX_STATS), 1);
        if (NULL == pfs) {
            perror("Can't allocate space for stats structure: calloc");
            break;
        }

        pfs->prefix = malloc(nprefix + 1);
        if (NULL == pfs->prefix) {
            perror("Can't allocate space for copy of prefix: malloc");
            free(pfs); pfs = NULL;
            break;
        }

        if (nprefix > 0)
            strncpy(pfs->prefix, prefix, nprefix);
        pfs->prefix[nprefix] = '\0';      /* because strncpy() sucks */
        pfs->prefix_len = nprefix;

        pfs->next = prefix_stats[hashval];
        prefix_stats[hashval] = pfs;
        num_prefixes++;

        if (nprefix > 0)
            total_prefix_size += nprefix;
        else /* nprefix == 0 */
            total_prefix_size += strlen(null_prefix_str);
    } while(0);
    STATS_UNLOCK();

    return (pfs != NULL) ? 0 : -1;
}
#endif

int stats_prefix_delete(const char *prefix, const size_t nprefix) {
    PREFIX_STATS *curr, *prev;
    int hidx;
    int ret = -1;

    STATS_LOCK();
    if (nprefix == 0) {
        hidx = mc_hash(prefix, nprefix, 0) % PREFIX_HASH_SIZE;
        prev = NULL;
        for (curr = prefix_stats[hidx]; curr != NULL; prev = curr, curr = curr->next) {
            if (curr->prefix_len == 0) break;
        }
        if (curr != NULL) { /* found */
            if (prev == NULL) prefix_stats[hidx] = curr->next;
            else              prev->next = curr->next;
            num_prefixes--;
            total_prefix_size -= strlen(null_prefix_str);

            free(curr->prefix);
            free(curr);
            ret = 0;
        }
    } else { /* nprefix > 0 */
        hidx = mc_hash(prefix, nprefix, 0) % PREFIX_HASH_SIZE;
        prev = NULL;
        for (curr = prefix_stats[hidx]; curr != NULL; prev = curr, curr = curr->next) {
            if (curr->prefix_len == nprefix && strncmp(curr->prefix, prefix, nprefix) == 0)
                break;
        }
        if (curr != NULL) { /* found */
            if (prev == NULL) prefix_stats[hidx] = curr->next;
            else              prev->next = curr->next;
            num_prefixes--;
            total_prefix_size -= curr->prefix_len;

            free(curr->prefix);
            free(curr);
            ret = 0;
        }
#if 0 // OLD_CODE for deleting multi-level prefixes
        // Full scan for sub-prefixies (we would fix it in future)
        for (hidx = 0; hidx < PREFIX_HASH_SIZE; hidx++) {
            prev = NULL;
            for (curr = prefix_stats[hidx]; curr != NULL; curr = next) {
                next = curr->next;
                if ((curr->prefix_len >= nprefix && strncmp(curr->prefix, prefix, nprefix) == 0) &&
                    (curr->prefix_len == nprefix || *(curr->prefix+nprefix)==settings.prefix_delimiter)) {
                    if (prev == NULL) prefix_stats[hidx] = curr->next;
                    else              prev->next = curr->next;
                    num_prefixes--;
                    total_prefix_size -= curr->prefix_len;

                    free(curr->prefix);
                    free(curr);
                    ret = 0;
                } else {
                    prev = curr;
                }
            }
        }
#endif
    }
    STATS_UNLOCK();
    return ret;
}

/*
 * Returns the stats structure for a prefix, creating it if it's not already
 * in the list.
 */
/*@null@*/
static PREFIX_STATS *stats_prefix_find(const char *key, const size_t nkey) {
    PREFIX_STATS *pfs;
    uint32_t hashval;
    size_t length;
    char *token = NULL;
    int i = 0;
    int prefix_depth = 0;

    assert(key != NULL);

    while ((token = memchr(key + i + 1, settings.prefix_delimiter, nkey - i - 1)) != NULL) {
        i = token - key;
        prefix_depth++;

        if (prefix_depth >= PREFIX_MAX_DEPTH) {
            break;
        }
    }

    if (prefix_depth <= 0) {
        length = 0;
    } else {
        length = i;
    }

    hashval = mc_hash(key, length, 0) % PREFIX_HASH_SIZE;

    for (pfs = prefix_stats[hashval]; NULL != pfs; pfs = pfs->next) {
        if ((pfs->prefix_len==length) && (length==0 || strncmp(pfs->prefix, key, length)==0))
            return pfs;
    }

#ifdef NEW_PREFIX_STATS_MANAGEMENT
    return NULL;
#else
    if (length > 0) {
        if (!mc_isvalidname(key, length)) {
            /* Invalid prefix name */
            return NULL;
        }
    }

    if (num_prefixes >= max_prefixes) {
        /* prefix overflow */
        func_when_prefix_overflow();
        return NULL;
    }

    pfs = calloc(sizeof(PREFIX_STATS), 1);
    if (NULL == pfs) {
        perror("Can't allocate space for stats structure: calloc");
        return NULL;
    }

    pfs->prefix = malloc(length + 1);
    if (NULL == pfs->prefix) {
        perror("Can't allocate space for copy of prefix: malloc");
        free(pfs);
        return NULL;
    }

    if (length > 0)
        strncpy(pfs->prefix, key, length);
    pfs->prefix[length] = '\0';      /* because strncpy() sucks */
    pfs->prefix_len = length;

    pfs->next = prefix_stats[hashval];
    prefix_stats[hashval] = pfs;
    num_prefixes++;

    if (length > 0)
        total_prefix_size += length;
    else /* length == 0 */
        total_prefix_size += strlen(null_prefix_str);

    return pfs;
#endif
}

/*
 * Records a "get" of a key.
 */
void stats_prefix_record_get(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_gets++;
        if (is_hit) {
            pfs->num_hits++;
        }
    }
    STATS_UNLOCK();
}

/*
 * Records a "delete" of a key.
 */
void stats_prefix_record_delete(const char *key, const size_t nkey, const char* client_ip) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_deletes++;
    }
    STATS_UNLOCK();
}

/*
 * Records a "set" of a key.
 */
void stats_prefix_record_set(const char *key, const size_t nkey, const char* client_ip) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_sets++;
    }
    cmd_in_second_write("", "set", key, client_ip);
    STATS_UNLOCK();
}

/*
 * Records a "incr" of a key.
 */
void stats_prefix_record_incr(const char *key, const size_t nkey, const char* client_ip) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_incrs++;
    }
    cmd_in_second_write("", "incr", key, client_ip);
    STATS_UNLOCK();
}

/*
 * Records a "decr" of a key.
 */
void stats_prefix_record_decr(const char *key, const size_t nkey, const char* client_ip) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_decrs++;
    }
    cmd_in_second_write("", "decr", key, client_ip);
    STATS_UNLOCK();
}

/*
 * LIST stats
 */
void stats_prefix_record_lop_create(const char *key, const size_t nkey, const char* client_ip) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_lop_creates++;
    }
    cmd_in_second_write("lop", "create", key, client_ip);
    STATS_UNLOCK();
}

void stats_prefix_record_lop_insert(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_lop_inserts++;
        if (is_hit) {
            pfs->num_lop_insert_hits++;
        }
    }
    cmd_in_second_write("lop", "insert", key, client_ip);
    STATS_UNLOCK();
}

void stats_prefix_record_lop_delete(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_lop_deletes++;
        if (is_hit) {
            pfs->num_lop_delete_hits++;
        }
    }
    cmd_in_second_write("lop", "delete", key, client_ip);
    STATS_UNLOCK();
}

void stats_prefix_record_lop_get(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_lop_gets++;
        if (is_hit) {
            pfs->num_lop_get_hits++;
        }
    }
    cmd_in_second_write("lop", "get", key, client_ip);
    STATS_UNLOCK();
}

/*
 * SET stats
 */
void stats_prefix_record_sop_create(const char *key, const size_t nkey, const char* client_ip) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_sop_creates++;
    }
    cmd_in_second_write("sop", "create", key, client_ip);
    STATS_UNLOCK();
}

void stats_prefix_record_sop_insert(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_sop_inserts++;
        if (is_hit) {
            pfs->num_sop_insert_hits++;
        }
    }
    cmd_in_second_write("sop", "insert", key, client_ip);
    STATS_UNLOCK();
}

void stats_prefix_record_sop_delete(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_sop_deletes++;
        if (is_hit) {
            pfs->num_sop_delete_hits++;
        }
    }
    cmd_in_second_write("sop", "delete", key, client_ip);
    STATS_UNLOCK();
}

void stats_prefix_record_sop_get(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_sop_gets++;
        if (is_hit) {
            pfs->num_sop_get_hits++;
        }
    }
    cmd_in_second_write("sop", "get", key, client_ip);
    STATS_UNLOCK();
}

void stats_prefix_record_sop_exist(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_sop_exists++;
        if (is_hit) {
            pfs->num_sop_exist_hits++;
        }
    }
    cmd_in_second_write("sop", "exist", key, client_ip);
    STATS_UNLOCK();
}

/*
 * MAP stats
 */
#ifdef CMD_IN_SECOND
void stats_prefix_record_mop_create(const char *key, const size_t nkey, const char* client_ip) {
#else
void stats_prefix_record_mop_create(const char *key, const size_t nkey) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_mop_creates++;
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("mop", "create", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_mop_insert(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_mop_insert(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_mop_inserts++;
        if (is_hit) {
            pfs->num_mop_insert_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("mop", "insert", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_mop_update(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_mop_update(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_mop_updates++;
        if (is_hit) {
            pfs->num_mop_update_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("mop", "update", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_mop_delete(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_mop_delete(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_mop_deletes++;
        if (is_hit) {
            pfs->num_mop_delete_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("mop", "delete", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_mop_get(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_mop_get(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_mop_gets++;
        if (is_hit) {
            pfs->num_mop_get_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("mop", "get", key, client_ip);
#endif
    STATS_UNLOCK();
}

/*
 * B+TREE stats
 */
#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_create(const char *key, const size_t nkey, const char* client_ip) {
#else
void stats_prefix_record_bop_create(const char *key, const size_t nkey) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_creates++;
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "create", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_insert(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_insert(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_inserts++;
        if (is_hit) {
            pfs->num_bop_insert_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "insert", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_update(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_update(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_updates++;
        if (is_hit) {
            pfs->num_bop_update_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "update", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_delete(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_delete(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_deletes++;
        if (is_hit) {
            pfs->num_bop_delete_hits++;
        }
    }
    cmd_in_second_write("bop", "delete", key, client_ip);
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_incr(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_incr(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_incrs++;
        if (is_hit) {
            pfs->num_bop_incr_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "incr", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_decr(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_decr(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_decrs++;
        if (is_hit) {
            pfs->num_bop_decr_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "decr", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_get(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_get(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_gets++;
        if (is_hit) {
            pfs->num_bop_get_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "get", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_count(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_count(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_counts++;
        if (is_hit) {
            pfs->num_bop_count_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "count", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_position(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_position(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_positions++;
        if (is_hit) {
            pfs->num_bop_position_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "position", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_pwg(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_pwg(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_pwgs++;
        if (is_hit) {
            pfs->num_bop_pwg_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "pwg", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_bop_gbp(const char *key, const size_t nkey, const char* client_ip, const bool is_hit) {
#else
void stats_prefix_record_bop_gbp(const char *key, const size_t nkey, const bool is_hit) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_bop_gbps++;
        if (is_hit) {
            pfs->num_bop_gbp_hits++;
        }
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("bop", "gbp", key, client_ip);
#endif
    STATS_UNLOCK();
}

/*
 * ATTR stats
 */
#ifdef CMD_IN_SECOND
void stats_prefix_record_getattr(const char *key, const size_t nkey, const char* client_ip) {
#else
void stats_prefix_record_getattr(const char *key, const size_t nkey) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_getattrs++;
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("", "getattr", key, client_ip);
#endif
    STATS_UNLOCK();
}

#ifdef CMD_IN_SECOND
void stats_prefix_record_setattr(const char *key, const size_t nkey, const char* client_ip) {
#else
void stats_prefix_record_setattr(const char *key, const size_t nkey) {
#endif
    PREFIX_STATS *pfs;

    STATS_LOCK();
    pfs = stats_prefix_find(key, nkey);
    if (NULL != pfs) {
        pfs->num_setattrs++;
    }
#ifdef CMD_IN_SECOND
    cmd_in_second_write("", "setattr", key, client_ip);
#endif
    STATS_UNLOCK();
}

/*
 * Returns stats in textual form suitable for writing to a client.
 */
/*@null@*/
char *stats_prefix_dump(int *length) {
    const char *format = "PREFIX %s "
                         "get %llu hit %llu set %llu del %llu inc %llu dec %llu lcs %llu lis %llu lih %llu lds %llu "
                         "ldh %llu lgs %llu lgh %llu scs %llu sis %llu sih %llu sds %llu sdh %llu sgs %llu sgh %llu "
                         "ses %llu seh %llu mcs %llu mis %llu mih %llu mus %llu muh %llu mds %llu mdh %llu mgs %llu "
                         "mgh %llu bcs %llu bis %llu bih %llu bus %llu buh %llu bds %llu bdh %llu bps %llu bph %llu "
                         "bms %llu bmh %llu bgs %llu bgh %llu bns %llu bnh %llu pfs %llu pfh %llu pgs %llu pgh %llu "
                         "gps %llu gph %llu gas %llu sas %llu\r\n";
    PREFIX_STATS *pfs;
    char *buf;
    int i, pos;
    size_t size = 0, written = 0, total_written = 0;

    /*
     * Figure out how big the buffer needs to be. This is the sum of the
     * lengths of the prefixes themselves, plus the size of one copy of
     * the per-prefix output with 20-digit values for all the counts,
     * plus space for the "END" at the end.
     */
    STATS_LOCK();
    size = strlen(format) + total_prefix_size +
           num_prefixes * (strlen(format) - 2 /* %s */
                           + 54 * (20 - 4)) /* %llu replaced by 20-digit num */
                           + sizeof("END\r\n");
    buf = malloc(size);
    if (NULL == buf) {
        perror("Can't allocate stats response: malloc");
        STATS_UNLOCK();
        return NULL;
    }

    pos = 0;
    for (i = 0; i < PREFIX_HASH_SIZE; i++) {
        for (pfs = prefix_stats[i]; NULL != pfs; pfs = pfs->next) {
            written = snprintf(buf + pos, size-pos, format,
                           (pfs->prefix_len == 0 ? null_prefix_str : pfs->prefix),
                           pfs->num_gets, pfs->num_hits, pfs->num_sets, pfs->num_deletes,
                           pfs->num_incrs, pfs->num_decrs,
                           pfs->num_lop_creates,
                           pfs->num_lop_inserts, pfs->num_lop_insert_hits,
                           pfs->num_lop_deletes, pfs->num_lop_delete_hits,
                           pfs->num_lop_gets, pfs->num_lop_get_hits,
                           pfs->num_sop_creates,
                           pfs->num_sop_inserts, pfs->num_sop_insert_hits,
                           pfs->num_sop_deletes, pfs->num_sop_delete_hits,
                           pfs->num_sop_gets, pfs->num_sop_get_hits,
                           pfs->num_sop_exists, pfs->num_sop_exist_hits,
                           pfs->num_mop_creates,
                           pfs->num_mop_inserts, pfs->num_mop_insert_hits,
                           pfs->num_mop_updates, pfs->num_mop_update_hits,
                           pfs->num_mop_deletes, pfs->num_mop_delete_hits,
                           pfs->num_mop_gets, pfs->num_mop_get_hits,
                           pfs->num_bop_creates,
                           pfs->num_bop_inserts, pfs->num_bop_insert_hits,
                           pfs->num_bop_updates, pfs->num_bop_update_hits,
                           pfs->num_bop_deletes, pfs->num_bop_delete_hits,
                           pfs->num_bop_incrs, pfs->num_bop_incr_hits,
                           pfs->num_bop_decrs, pfs->num_bop_decr_hits,
                           pfs->num_bop_gets, pfs->num_bop_get_hits,
                           pfs->num_bop_counts, pfs->num_bop_count_hits,
                           pfs->num_bop_positions, pfs->num_bop_position_hits,
                           pfs->num_bop_pwgs, pfs->num_bop_pwg_hits,
                           pfs->num_bop_gbps, pfs->num_bop_gbp_hits,
                           pfs->num_getattrs, pfs->num_setattrs);
            pos += written;
            total_written += written;
            assert(total_written < size);
        }
    }

    STATS_UNLOCK();
    memcpy(buf + pos, "END\r\n", 6);

    *length = pos + 5;
    return buf;
}


#ifdef UNIT_TEST

/****************************************************************************
      To run unit tests, compile with $(CC) -DUNIT_TEST stats.c assoc.o
      (need assoc.o to get the hash() function).
****************************************************************************/

struct settings settings;

static char *current_test = "";
static int test_count = 0;
static int fail_count = 0;

static void fail(char *what) { printf("\tFAIL: %s\n", what); fflush(stdout); fail_count++; }
static void test_equals_int(char *what, int a, int b) { test_count++; if (a != b) fail(what); }
static void test_equals_ptr(char *what, void *a, void *b) { test_count++; if (a != b) fail(what); }
static void test_equals_str(char *what, const char *a, const char *b) { test_count++; if (strcmp(a, b)) fail(what); }
static void test_equals_ull(char *what, uint64_t a, uint64_t b) { test_count++; if (a != b) fail(what); }
static void test_notequals_ptr(char *what, void *a, void *b) { test_count++; if (a == b) fail(what); }
static void test_notnull_ptr(char *what, void *a) { test_count++; if (NULL == a) fail(what); }

static void test_prefix_find() {
    PREFIX_STATS *pfs1, *pfs2;

    pfs1 = stats_prefix_find("abc");
    test_notnull_ptr("initial prefix find", pfs1);
    test_equals_ull("request counts", 0ULL,
        pfs1->num_gets + pfs1->num_sets + pfs1->num_deletes + pfs1->num_hits);
    pfs2 = stats_prefix_find("abc");
    test_equals_ptr("find of same prefix", pfs1, pfs2);
    pfs2 = stats_prefix_find("abc:");
    test_equals_ptr("find of same prefix, ignoring delimiter", pfs1, pfs2);
    pfs2 = stats_prefix_find("abc:d");
    test_equals_ptr("find of same prefix, ignoring extra chars", pfs1, pfs2);
    pfs2 = stats_prefix_find("xyz123");
    test_notequals_ptr("find of different prefix", pfs1, pfs2);
    pfs2 = stats_prefix_find("ab:");
    test_notequals_ptr("find of shorter prefix", pfs1, pfs2);
}

static void test_prefix_record_get() {
    PREFIX_STATS *pfs;

    stats_prefix_record_get("abc:123", 0);
    pfs = stats_prefix_find("abc:123");
    test_equals_ull("get count after get #1", 1, pfs->num_gets);
    test_equals_ull("hit count after get #1", 0, pfs->num_hits);
    stats_prefix_record_get("abc:456", 0);
    test_equals_ull("get count after get #2", 2, pfs->num_gets);
    test_equals_ull("hit count after get #2", 0, pfs->num_hits);
    stats_prefix_record_get("abc:456", 1);
    test_equals_ull("get count after get #3", 3, pfs->num_gets);
    test_equals_ull("hit count after get #3", 1, pfs->num_hits);
    stats_prefix_record_get("def:", 1);
    test_equals_ull("get count after get #4", 3, pfs->num_gets);
    test_equals_ull("hit count after get #4", 1, pfs->num_hits);
}

static void test_prefix_record_delete() {
    PREFIX_STATS *pfs;

    stats_prefix_record_delete("abc:123");
    pfs = stats_prefix_find("abc:123");
    test_equals_ull("get count after delete #1", 0, pfs->num_gets);
    test_equals_ull("hit count after delete #1", 0, pfs->num_hits);
    test_equals_ull("delete count after delete #1", 1, pfs->num_deletes);
    test_equals_ull("set count after delete #1", 0, pfs->num_sets);
    stats_prefix_record_delete("def:");
    test_equals_ull("delete count after delete #2", 1, pfs->num_deletes);
}

static void test_prefix_record_set() {
    PREFIX_STATS *pfs;

    stats_prefix_record_set("abc:123");
    pfs = stats_prefix_find("abc:123");
    test_equals_ull("get count after set #1", 0, pfs->num_gets);
    test_equals_ull("hit count after set #1", 0, pfs->num_hits);
    test_equals_ull("delete count after set #1", 0, pfs->num_deletes);
    test_equals_ull("set count after set #1", 1, pfs->num_sets);
    stats_prefix_record_delete("def:");
    test_equals_ull("set count after set #2", 1, pfs->num_sets);
}

static void test_prefix_dump() {
    int hashval = mc_hash("abc", 3, 0) % PREFIX_HASH_SIZE;
    char tmp[500];
    char *expected;
    int keynum;
    int length;

    test_equals_str("empty stats", "END\r\n", stats_prefix_dump(&length));
    test_equals_int("empty stats length", 5, length);
    stats_prefix_record_set("abc:123");
    expected = "PREFIX abc get 0 hit 0 set 1 del 0\r\nEND\r\n";
    test_equals_str("stats after set", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after set", strlen(expected), length);
    stats_prefix_record_get("abc:123", 0);
    expected = "PREFIX abc get 1 hit 0 set 1 del 0\r\nEND\r\n";
    test_equals_str("stats after get #1", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after get #1", strlen(expected), length);
    stats_prefix_record_get("abc:123", 1);
    expected = "PREFIX abc get 2 hit 1 set 1 del 0\r\nEND\r\n";
    test_equals_str("stats after get #2", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after get #2", strlen(expected), length);
    stats_prefix_record_delete("abc:123");
    expected = "PREFIX abc get 2 hit 1 set 1 del 1\r\nEND\r\n";
    test_equals_str("stats after del #1", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after del #1", strlen(expected), length);

    /* The order of results might change if we switch hash functions. */
    stats_prefix_record_delete("def:123");
    expected = "PREFIX abc get 2 hit 1 set 1 del 1\r\n"
               "PREFIX def get 0 hit 0 set 0 del 1\r\n"
               "END\r\n";
    test_equals_str("stats after del #2", expected, stats_prefix_dump(&length));
    test_equals_int("stats length after del #2", strlen(expected), length);

    /* Find a key that hashes to the same bucket as "abc" */
    for (keynum = 0; keynum < PREFIX_HASH_SIZE * 100; keynum++) {
        snprintf(tmp, sizeof(tmp), "%d", keynum);
        if (hashval == mc_hash(tmp, strlen(tmp), 0) % PREFIX_HASH_SIZE) {
            break;
        }
    }
    stats_prefix_record_set(tmp);
    snprintf(tmp, sizeof(tmp),
             "PREFIX %d get 0 hit 0 set 1 del 0\r\n"
             "PREFIX abc get 2 hit 1 set 1 del 1\r\n"
             "PREFIX def get 0 hit 0 set 0 del 1\r\n"
             "END\r\n", keynum);
    test_equals_str("stats with two stats in one bucket",
                    tmp, stats_prefix_dump(&length));
    test_equals_int("stats length with two stats in one bucket",
                    strlen(tmp), length);
}

static void run_test(char *what, void (*func)(void)) {
    current_test = what;
    test_count = fail_count = 0;
    puts(what);
    fflush(stdout);

    stats_prefix_clear();
    (func)();
    printf("\t%d / %d pass\n", (test_count - fail_count), test_count);
}

/* In case we're compiled in thread mode */
void mt_stats_lock() { }
void mt_stats_unlock() { }

main(int argc, char **argv) {
    stats_prefix_init();
    settings.prefix_delimiter = ':';
    run_test("stats_prefix_find", test_prefix_find);
    run_test("stats_prefix_record_get", test_prefix_record_get);
    run_test("stats_prefix_record_delete", test_prefix_record_delete);
    run_test("stats_prefix_record_set", test_prefix_record_set);
    run_test("stats_prefix_dump", test_prefix_dump);
}

#endif
