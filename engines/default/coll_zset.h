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
#ifndef ITEM_COLL_ZSET_H
#define ITEM_COLL_ZSET_H

#include "item_base.h"

/*
 * ZSet Collection
 */
ENGINE_ERROR_CODE zset_struct_create(const char *key, const uint32_t nkey,
                                     const void *value, const uint32_t nbytes,
                                     item_attr *attrp, const void *cookie);

zset_elem_item *zset_elem_alloc(const uint32_t nbkey, const uint32_t neflag, const uint32_t nbytes);

void zset_elem_free(zset_elem_item *elem);

ENGINE_ERROR_CODE zset_elem_insert(const char *key, const uint32_t nkey,
                                   zset_elem_item *elem, const bool replace_if_exist,
                                   ENGINE_UPDATE_COND cond, item_attr *attrp,
                                   bool *replaced, bool *created, const void *cookie);

ENGINE_ERROR_CODE zset_elem_update(const char *key, const uint32_t nkey,
                                   const char *member, const uint32_t nbytes,
                                   const void *score, const uint32_t nscore,
                                   ENGINE_UPDATE_COND cond, const eflag_update *eupdate,
                                   const void *cookie);

ENGINE_ERROR_CODE zset_elem_delete(const char *key, const uint32_t nkey,
                                   const int nummembers, const field_t *mlist,
                                   const bool drop_if_empty,
                                   uint32_t *del_count, bool *dropped,
                                   const void *cookie);

ENGINE_ERROR_CODE zset_elem_delete_by_score(const char *key, const uint32_t nkey,
                                            const bkey_range *srange, const eflag_filter *efilter,
                                            const uint32_t req_count, const bool drop_if_empty,
                                            uint32_t *del_count, uint32_t *opcost, bool *dropped,
                                            const void *cookie);

uint32_t zset_elem_delete_with_count(zset_meta_info *info, const uint32_t count);

ENGINE_ERROR_CODE item_zset_coll_init(void *engine_ptr);
void item_zset_coll_final(void *engine_ptr);

#endif
