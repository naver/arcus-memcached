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

ENGINE_ERROR_CODE item_zset_coll_init(void *engine_ptr);
void item_zset_coll_final(void *engine_ptr);

#endif
