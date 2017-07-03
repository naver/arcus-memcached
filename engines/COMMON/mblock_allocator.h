/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2017 JaM2in Co., Ltd.
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
#ifndef MEMCACHED_ENGINE_BLOCK_ALLOCATOR_H
#define MEMCACHED_ENGINE_BLOCK_ALLOCATOR_H

#include <memcached/types.h>

#define BLOCK_ALLOCATOR_DEFAULT_SIZE 500
#define BLOCK_ALLOCATOR_MAXINIT_SIZE 1000

/* block allocator API */
int  mblock_allocator_init(size_t);
void mblock_allocator_destroy(void);
void mblock_allocator_stats(mblock_stats *blk_stat);

bool mblock_list_alloc(uint32_t blck_cnt, mem_block_t **head_blk, mem_block_t **tail_blk);
void mblock_list_free(uint32_t blck_cnt, mem_block_t *head_blk, mem_block_t *tail_blk);

bool eblk_prepare(eblock_result_t *result, uint32_t elem_count);
void eblk_truncate(eblock_result_t *result);
void eblk_add_elem(eblock_result_t *result, eitem *elem);

#endif
