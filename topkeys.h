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
#ifndef TOPKEYS_H
#define TOPKEYS_H 1

#include <memcached/engine.h>
#include <memcached/genhash.h>

/* A list of operations for which we have int stats */
#define TK_OPS(C)  C(get_hits) C(get_misses) C(cmd_set) \
                   C(incr_hits) C(incr_misses) C(decr_hits) C(decr_misses) \
                   C(delete_hits) C(delete_misses) C(evictions) \
                   C(cas_hits) C(cas_misses) C(cas_badval)

/* A list of list operations */
#define TK_LOPS(C)  C(lop_create_oks) C(lop_insert_hits) C(lop_insert_misses) \
                    C(lop_delete_elem_hits) C(lop_delete_none_hits) C(lop_delete_misses) \
                    C(lop_get_elem_hits) C(lop_get_none_hits) C(lop_get_misses)

/* A list of set operations */
#define TK_SOPS(C)  C(sop_create_oks) C(sop_insert_hits) C(sop_insert_misses) \
                    C(sop_delete_elem_hits) C(sop_delete_none_hits) C(sop_delete_misses) \
                    C(sop_get_elem_hits) C(sop_get_none_hits) C(sop_get_misses) \
                    C(sop_exist_hits) C(sop_exist_misses)

/* A list of map operations */
#define TK_MOPS(C)  C(mop_create_oks) C(mop_insert_hits) C(mop_insert_misses) \
                    C(mop_update_elem_hits) C(mop_update_none_hits) C(mop_update_misses) \
                    C(mop_delete_elem_hits) C(mop_delete_none_hits) C(mop_delete_misses) \
                    C(mop_get_elem_hits) C(mop_get_none_hits) C(mop_get_misses)

/* A list of btree operations */
/* The multiple key operations, bop_mget_oks and bop_smget_oks, are excluded. */
#define TK_BOPS(C)  C(bop_create_oks) C(bop_insert_hits) C(bop_insert_misses) \
                    C(bop_update_elem_hits) C(bop_update_none_hits) C(bop_update_misses) \
                    C(bop_delete_elem_hits) C(bop_delete_none_hits) C(bop_delete_misses) \
                    C(bop_get_elem_hits) C(bop_get_none_hits) C(bop_get_misses) \
                    C(bop_count_hits) C(bop_count_misses) \
                    C(bop_position_elem_hits) C(bop_position_none_hits) C(bop_position_misses) \
                    C(bop_pwg_elem_hits) C(bop_pwg_none_hits) C(bop_pwg_misses) \
                    C(bop_gbp_elem_hits) C(bop_gbp_none_hits) C(bop_gbp_misses) \
                    C(bop_incr_elem_hits) C(bop_incr_none_hits) C(bop_incr_misses) \
                    C(bop_decr_elem_hits) C(bop_decr_none_hits) C(bop_decr_misses)

/* A list of attribute operations */
#define TK_AOPS(C)  C(getattr_hits) C(getattr_misses) C(setattr_hits) C(setattr_misses)

#define TK_MAX_VAL_LEN 250

/* Update the correct stat for a given operation */
#define TK(tk, op, key, nkey, ctime) { \
    if (tk) { \
        assert(key); \
        assert(nkey > 0); \
        pthread_mutex_lock(&tk->mutex); \
        topkey_item_t *tmp = topkeys_item_get_or_create( \
            (tk), (key), (nkey), (ctime)); \
        tmp->op++; \
        pthread_mutex_unlock(&tk->mutex); \
    } \
}

typedef struct dlist {
    struct dlist *next;
    struct dlist *prev;
} dlist_t;

typedef struct topkey_item {
    dlist_t list; /* Must be at the beginning because we downcast! */
    int nkey;
    rel_time_t ctime, atime; /* Time this item was created/last accessed */
#define TK_CUR(name) int name;
    TK_OPS(TK_CUR)
    TK_LOPS(TK_CUR)
    TK_SOPS(TK_CUR)
    TK_MOPS(TK_CUR)
    TK_BOPS(TK_CUR)
    TK_AOPS(TK_CUR)
#undef TK_CUR
    char key[]; /* A variable length array in the struct itself */
} topkey_item_t;

typedef struct topkeys {
    dlist_t list;
    pthread_mutex_t mutex;
    genhash_t *hash;
    int nkeys;
    int max_keys;
} topkeys_t;

topkeys_t *topkeys_init(int max_keys);
void topkeys_free(topkeys_t *topkeys);
topkey_item_t *topkeys_item_get_or_create(topkeys_t *tk,
                                          const void *key, size_t nkey,
                                          const rel_time_t ctime);
ENGINE_ERROR_CODE topkeys_stats(topkeys_t *tk,
                                const void *cookie,
                                const rel_time_t current_time,
                                ADD_STAT add_stat);

#endif
