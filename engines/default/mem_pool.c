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

#ifdef ENABLE_LARGE_ITEM

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "mem_pool.h"

static large_pa_pool_t *pool;

/* Large Pointer Array Pool Lock */
static inline void LOCK_POINTER_ARRAY_POOL(void)
{
    pthread_mutex_lock(&pool->large_pa_pool_lock);
}

static inline void UNLOCK_POINTER_ARRAY_POOL(void)
{
    pthread_mutex_unlock(&pool->large_pa_pool_lock);
}

void pointer_array_pool_init(void)
{
    pool = (large_pa_pool_t *)malloc(sizeof(large_pa_pool_t));
    pool->large_pa_pool = (large_pa *)malloc(sizeof(large_pa) * MAX_LARGE_POINTER_ARRAY_POOL);
    pool->used_cnt = 0;
    pool->pool_size = MAX_LARGE_POINTER_ARRAY_POOL;

    for (int i = 0; i < pool->pool_size; i++) {
        // -1 is unallocated state
        pool->large_pa_pool[i].pool_id = -1;
        pool->large_pa_pool[i].addnl = (value_item **)malloc(sizeof(value_item *) * LARGE_POINTER_ARRAY_LENGTH);
    }
    pthread_mutex_init(&pool->large_pa_pool_lock, NULL);
}

void pointer_array_pool_final(void)
{
    pthread_mutex_destroy(&pool->large_pa_pool_lock);
    for (int i = 0; i < pool->pool_size; i++) {
        free(pool->large_pa_pool[i].addnl);
    }
    free(pool->large_pa_pool);
    free(pool);
}

/*
 It's -1 that pool id is not allocated state. When it's 0, new memory allocated state.
 otherwise if it's positive number are allocated memory from the pool.
 */
static void do_pointer_array_pool_alloc(large_pa *pa, uint32_t elem_count)
{
    if (elem_count <= LARGE_POINTER_ARRAY_LENGTH &&
        pool->used_cnt < MAX_LARGE_POINTER_ARRAY_POOL) {
        for (int i = 0; i < pool->pool_size; i++) {
            if (pool->large_pa_pool[i].pool_id == -1) {
                pool->large_pa_pool[i].pool_id = i + 1;
                pa->pool_id = i + 1;
                pa->addnl = pool->large_pa_pool[i].addnl;
                pool->used_cnt++;
                break;
            }
        }
    } else {
        pa->pool_id = 0;
        pa->addnl = (value_item **)malloc(sizeof(value_item *) * elem_count);
    }
}

void pointer_array_pool_alloc(large_pa *pa, uint32_t elem_count)
{
    assert(pa->addnl == NULL);
    assert(elem_count > 0);
    LOCK_POINTER_ARRAY_POOL();
    do_pointer_array_pool_alloc(pa, elem_count);
    UNLOCK_POINTER_ARRAY_POOL();
}

static void do_pointer_array_free(large_pa *pa)
{
    assert(pa->pool_id > 0 && pa->pool_id <= LARGE_POINTER_ARRAY_LENGTH);
    pool->large_pa_pool[pa->pool_id - 1].pool_id = -1;
    pool->used_cnt--;
    pa->pool_id = -1;
    pa->addnl = NULL;
}

void pointer_array_free(large_pa *pa)
{
    assert(pa->addnl != NULL);
    if (pa->pool_id == 0) {
        free(pa->addnl);
        pa->pool_id = -1;
        pa->addnl = NULL;
    } else {
        LOCK_POINTER_ARRAY_POOL();
        do_pointer_array_free(pa);
        UNLOCK_POINTER_ARRAY_POOL();
    }
}

#endif
