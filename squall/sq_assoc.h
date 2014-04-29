/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
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
#ifndef SQ_ASSOC_H
#define SQ_ASSOC_H

#include "sq_items.h"

typedef enum {
    ASSOC_TYPE_HASH = 1,
    ASSOC_TYPE_BTREE
} ASSOC_TYPE;

struct assoc_conf {
    ASSOC_TYPE assoc_type;
    uint32_t   page_size;
    uint32_t (*hash_func)(const void *data, size_t size, uint32_t seed);
};

/* associative array */
int  assoc_mgr_init(void *conf, void *srvr);
void assoc_mgr_final(void);

ENGINE_ERROR_CODE assoc_find(int pfxid, const char *key, const size_t nkey, ItemDesc *itdesc);
ENGINE_ERROR_CODE assoc_exist(int pfxid, const char *key, const size_t nkey, bool *exist);
ENGINE_ERROR_CODE assoc_insert(int pfxid, ItemDesc *itdesc);
ENGINE_ERROR_CODE assoc_delete(int pfxid, ItemDesc *itdesc);
ENGINE_ERROR_CODE assoc_toggle(int pfxid, ItemDesc *old_itdesc, ItemDesc *new_itdesc);
ENGINE_ERROR_CODE assoc_dbcheck(int pfxid, uint64_t *item_count);
#endif
