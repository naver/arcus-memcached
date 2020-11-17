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
#ifndef ITEM_COLL_MAP_H
#define ITEM_COLL_MAP_H

#include "item_base.h"

#ifdef REORGANIZE_ITEM_COLL // MAP
/*
 * Map Collection
 */
ENGINE_ERROR_CODE map_struct_create(const char *key, const uint32_t nkey,
                                    item_attr *attrp, const void *cookie);

map_elem_item *map_elem_alloc(const int nfield,
                              const uint32_t nbytes, const void *cookie);

void map_elem_free(map_elem_item *elem);

void map_elem_release(map_elem_item **elem_array, const int elem_count);

ENGINE_ERROR_CODE map_elem_insert(const char *key, const uint32_t nkey,
                                  map_elem_item *elem,
                                  item_attr *attrp,
                                  bool *created, const void *cookie);

ENGINE_ERROR_CODE map_elem_update(const char *key, const uint32_t nkey,
                                  const field_t *field,
                                  const char *value, const uint32_t nbytes,
                                  const void *cookie);

ENGINE_ERROR_CODE map_elem_delete(const char *key, const uint32_t nkey,
                                  const int numfields, const field_t *flist,
                                  const bool drop_if_empty, uint32_t *del_count,
                                  bool *dropped, const void *cookie);

ENGINE_ERROR_CODE map_elem_get(const char *key, const uint32_t nkey,
                               const int numfields, const field_t *flist,
                               const bool delete, const bool drop_if_empty,
                               struct elems_result *eresult,
                               const void *cookie);

uint32_t map_elem_delete_with_count(map_meta_info *info, const uint32_t count);

void map_elem_get_all(map_meta_info *info, elems_result_t *eresult);

uint32_t map_elem_ntotal(map_elem_item *elem);

ENGINE_ERROR_CODE map_coll_getattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt);
ENGINE_ERROR_CODE map_coll_setattr(hash_item *it, item_attr *attrp,
                                   ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_cnt);
#endif
#ifdef REORGANIZE_ITEM_COLL // MAP
ENGINE_ERROR_CODE item_map_coll_init(void *engine_ptr);
void item_map_coll_final(void *engine_ptr);
#endif

#endif
