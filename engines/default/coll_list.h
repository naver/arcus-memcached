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
#ifndef ITEM_COLL_LIST_H
#define ITEM_COLL_LIST_H

#include "item_base.h"

#ifdef REORGANIZE_ITEM_COLL // LIST
/*
 * List Collection
 */
ENGINE_ERROR_CODE list_struct_create(const char *key, const uint32_t nkey,
                                     item_attr *attrp, const void *cookie);

list_elem_item *list_elem_alloc(const uint32_t nbytes, const void *cookie);

void list_elem_free(list_elem_item *elem);

void list_elem_release(list_elem_item **elem_array, const int elem_count);

ENGINE_ERROR_CODE list_elem_insert(const char *key, const uint32_t nkey,
                                   int index, list_elem_item *elem,
                                   item_attr *attrp,
                                   bool *created, const void *cookie);

ENGINE_ERROR_CODE list_elem_delete(const char *key, const uint32_t nkey,
                                   int from_index, int to_index,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped,
                                   const void *cookie);

ENGINE_ERROR_CODE list_elem_get(const char *key, const uint32_t nkey,
                                int from_index, int to_index,
                                const bool delete, const bool drop_if_empty,
                                struct elems_result *eresult,
                                const void *cookie);

uint32_t list_elem_delete_with_count(list_meta_info *info, const uint32_t count);

void list_elem_get_all(list_meta_info *info, elems_result_t *eresult);

uint32_t list_elem_ntotal(list_elem_item *elem);

ENGINE_ERROR_CODE list_coll_getattr(hash_item *it, item_attr *attrp,
                                    ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt);
ENGINE_ERROR_CODE list_coll_setattr(hash_item *it, item_attr *attrp,
                                    ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt);
#endif

#ifdef REORGANIZE_ITEM_COLL // APPLY LIST
ENGINE_ERROR_CODE list_apply_item_link(void *engine, const char *key, const uint32_t nkey,
                                       item_attr *attrp);
ENGINE_ERROR_CODE list_apply_elem_insert(void *engine, hash_item *it,
                                         const int nelems, const int index,
                                         const char *value, const uint32_t nbytes);
ENGINE_ERROR_CODE list_apply_elem_delete(void *engine, hash_item *it,
                                         const int nelems, const int index,
                                         const int count, const bool drop_if_empty);
#endif

#ifdef REORGANIZE_ITEM_COLL // LIST
ENGINE_ERROR_CODE item_list_coll_init(void *engine_ptr);
void item_list_coll_final(void *engine_ptr);
#endif

#endif
