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
#ifndef MEM_POOL_H
#define MEM_POOL_H

#include <memcached/types.h>

#ifdef ENABLE_LARGE_ITEM

#define LARGE_POINTER_ARRAY_LENGTH      1024 // 16MB
#define MAX_LARGE_POINTER_ARRAY_POOL    1000

typedef struct {
    uint32_t     pool_id;
    value_item **addnl;
} large_pa;

typedef struct {
    uint32_t         used_cnt;
    uint32_t         pool_size;
    large_pa        *large_pa_pool;
    pthread_mutex_t  large_pa_pool_lock;
} large_pa_pool_t;

/* mem pool functions */
void pointer_array_pool_init(void);
void pointer_array_pool_final(void);
void pointer_array_pool_alloc(large_pa *pa, uint32_t elem_count);
void pointer_array_free(large_pa *pa);

#endif

#endif
