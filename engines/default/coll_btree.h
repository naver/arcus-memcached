/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2020 JaM2in Co., Ltd.
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
#ifndef ITEM_COLL_BTREE_H
#define ITEM_COLL_BTREE_H

#include "item_base.h"

/*
 * B+Tree Collection
 */
ENGINE_ERROR_CODE btree_struct_create(const char *key, const uint32_t nkey,
                                      item_attr *attrp, const void *cookie);

btree_elem_item *btree_elem_alloc(const uint32_t nbkey, const uint32_t neflag, const uint32_t nbytes,
                                  const void *cookie);

void btree_elem_free(btree_elem_item *elem);

void btree_elem_release(btree_elem_item **elem_array, const int elem_count);

ENGINE_ERROR_CODE btree_elem_insert(const char *key, const uint32_t nkey,
                                    btree_elem_item *elem, const bool replace_if_exist, item_attr *attrp,
                                    bool *replaced, bool *created, btree_elem_item **trimmed_elems,
                                    uint32_t *trimmed_count, uint32_t *trimmed_flags, const void *cookie);

ENGINE_ERROR_CODE btree_elem_update(const char *key, const uint32_t nkey, const bkey_range *bkrange,
                                    const eflag_update *eupdate, const char *value, const uint32_t nbytes,
                                    const void *cookie);

ENGINE_ERROR_CODE btree_elem_delete(const char *key, const uint32_t nkey,
                                    const bkey_range *bkrange, const eflag_filter *efilter,
                                    const uint32_t req_count, const bool drop_if_empty,
                                    uint32_t *del_count, uint32_t *opcost, bool *dropped,
                                    const void *cookie);

ENGINE_ERROR_CODE btree_elem_arithmetic(const char *key, const uint32_t nkey,
                                        const bkey_range *bkrange,
                                        const bool increment, const bool create,
                                        const uint64_t delta, const uint64_t initial,
                                        const eflag_t *eflagp,
                                        uint64_t *result, const void *cookie);

ENGINE_ERROR_CODE btree_elem_get(const char *key, const uint32_t nkey,
                                 const bkey_range *bkrange, const eflag_filter *efilter,
                                 const uint32_t offset, const uint32_t req_count,
                                 const bool delete, const bool drop_if_empty,
                                 struct elems_result *eresult,
                                 const void *cookie);

ENGINE_ERROR_CODE btree_elem_count(const char *key, const uint32_t nkey,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   uint32_t *elem_count, uint32_t *opcost);

ENGINE_ERROR_CODE btree_posi_find(const char *key, const uint32_t nkey, const bkey_range *bkrange,
                                  ENGINE_BTREE_ORDER order, int *position);

ENGINE_ERROR_CODE btree_posi_find_with_get(const char *key, const uint32_t nkey,
                                           const bkey_range *bkrange, ENGINE_BTREE_ORDER order,
                                           const int count, int *position,
                                           struct elems_result *eresult);

ENGINE_ERROR_CODE btree_elem_get_by_posi(const char *key, const uint32_t nkey,
                                  ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                  struct elems_result *eresult);

#ifdef SUPPORT_BOP_SMGET
#ifdef JHPARK_OLD_SMGET_INTERFACE
/* smget old interface */
ENGINE_ERROR_CODE btree_elem_smget_old(token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   btree_elem_item **elem_array, uint32_t *kfnd_array,
                                   uint32_t *flag_array, uint32_t *elem_count,
                                   uint32_t *missed_key_array, uint32_t *missed_key_count,
                                   bool *trimmed, bool *duplicated);
#endif

/* smget new interface */
ENGINE_ERROR_CODE btree_elem_smget(token_t *key_array, const int key_count,
                                   const bkey_range *bkrange, const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   const bool unique,
                                   smget_result_t *result);
#endif

uint32_t btree_elem_delete_with_count(btree_meta_info *info, const uint32_t count);

void btree_elem_get_all(btree_meta_info *info, elems_result_t *eresult);

uint32_t btree_elem_ntotal(btree_elem_item *elem);
uint8_t  btree_real_nbkey(uint8_t nbkey);

ENGINE_ERROR_CODE btree_coll_getattr(hash_item *it, item_attr *attrp,
                                     ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt);
ENGINE_ERROR_CODE btree_coll_setattr(hash_item *it, item_attr *attrp,
                                     ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt);

ENGINE_ERROR_CODE btree_apply_item_link(void *engine, const char *key, const uint32_t nkey,
                                        item_attr *attrp);
ENGINE_ERROR_CODE btree_apply_elem_insert(void *engine, hash_item *it,
                                          const char *bkey, const uint32_t nbkey,
                                          const uint32_t neflag, const uint32_t nbytes);
ENGINE_ERROR_CODE btree_apply_elem_delete(void *engine, hash_item *it,
                                          const char *bkey, const uint32_t nbkey,
                                          const bool drop_if_empty);
ENGINE_ERROR_CODE btree_apply_elem_delete_logical(void *engine, hash_item *it,
                                                  const bkey_range *bkrange,
                                                  const eflag_filter *efilter,
                                                  const uint32_t offset, const uint32_t count,
                                                  const bool drop_if_empty);

ENGINE_ERROR_CODE item_btree_coll_init(void *engine_ptr);
void item_btree_coll_final(void *engine_ptr);

#endif
