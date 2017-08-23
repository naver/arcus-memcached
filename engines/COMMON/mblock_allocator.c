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

#include <stdlib.h>
//#include <pthread.h>
#include <assert.h>
#include "mblock_allocator.h"

static mem_block_t *pool_head = NULL;
static mem_block_t *pool_tail = NULL;
static uint32_t initial_mblocks;
static uint32_t total_mblocks;
static uint32_t free_mblocks;

//static pthread_mutex_t pool_mutex;

static void do_mblock_allocator_free_all() {
    mem_block_t *helper;
    while (pool_head != NULL) {
        helper = pool_head;
        pool_head = pool_head->next;
        free(helper);
    }
    pool_tail = NULL;
}

static void prepare_eblk_add_elem(eblock_result_t *result) {
    if (result->tail_blk == NULL) {
        result->tail_blk = result->head_blk;
        result->elem_cnt = 0;
    } else {
        assert(result->elem_cnt > 0);
        if (result->elem_cnt % EITEMS_PER_BLOCK == 0)
            result->tail_blk = result->tail_blk->next;
    }
}

int mblock_allocator_init(size_t nblocks) {
    mem_block_t *helper = NULL;
    int i;
    size_t nblk;

    if (nblocks == 0)                                nblk = BLOCK_ALLOCATOR_DEFAULT_SIZE;
    else if (nblocks > BLOCK_ALLOCATOR_MAXINIT_SIZE) nblk = BLOCK_ALLOCATOR_MAXINIT_SIZE;
    else                                             nblk = nblocks;

    for (i = 0; i < nblk; i++) {
        helper = (mem_block_t *)malloc(sizeof(mem_block_t));
        if (helper == NULL) break;

        helper->next = NULL;
        if (pool_tail) pool_tail->next = helper;
        else           pool_head = helper;
        pool_tail = helper;
    }
    if (i < nblk) { /* incompleted state */
        do_mblock_allocator_free_all();
        return -1;
    }

    //pthread_mutex_init(&pool_mutex, NULL);
    initial_mblocks = nblk;
    total_mblocks= nblk;
    free_mblocks = nblk;

    return 0;
}

void mblock_allocator_destroy() {
    //pthread_mutex_lock(&pool_mutex);
    do_mblock_allocator_free_all();

    initial_mblocks = 0;
    total_mblocks = 0;
    free_mblocks = 0;
    //pthread_mutex_unlock(&pool_mutex);
    //pthread_mutex_destroy(&pool_mutex);
}

void mblock_allocator_stats(mblock_stats *blk_stat) {
    //pthread_mutex_lock(&pool_mutex);
    blk_stat->total_mblocks = total_mblocks;
    blk_stat->free_mblocks = free_mblocks;
    //pthread_mutex_unlock(&pool_mutex);
}

/* As a function that returns single block, it is not currently used
void *allocate_single_block() {
    mem_block_t *ret;

    //pthread_mutex_lock(&pool_mutex);

    if ((ret = pool_head) != NULL) {
        pool_head = pool_head->next;
        if (pool_head == NULL)
            pool_tail = NULL;
        ret->next = NULL;
        free_mblocks--;
    } else {
        // TODO :
        // This malloc() inside mutex may raise some performance issue,
        // Is there any way to execute malloc and counter adjustment
        // outside the mutex lock?
        ret = (mem_block_t *)malloc(sizeof(mem_block_t));
        if (ret != NULL) {
            total_blocks++;
            ret->next = NULL;
        }
    }

    //pthread_mutex_unlock(&pool_mutex);

    return (void *)ret;
}
*/

bool mblock_list_alloc(uint32_t blck_cnt, mem_block_t **head_blk, mem_block_t **tail_blk) {
    assert(blck_cnt > 0);
    uint32_t alloc_cnt = 0;
    *head_blk = *tail_blk = NULL;

    //pthread_mutex_lock(&pool_mutex);
    if (free_mblocks > 0) {
        if (blck_cnt >= free_mblocks) {
            *head_blk = pool_head;
            *tail_blk = pool_tail;
            alloc_cnt = free_mblocks;

            pool_head = pool_tail = NULL;
            free_mblocks = 0;
        } else { /* free_mblocks > blck_cnt */
            *head_blk = pool_head;
            alloc_cnt = 1;
            while (alloc_cnt < blck_cnt) {
                pool_head = pool_head->next;
                alloc_cnt++;
            }
            *tail_blk = pool_head;

            pool_head = pool_head->next;
            free_mblocks -= alloc_cnt;

            (*tail_blk)->next = NULL;
        }
    }
    //pthread_mutex_unlock(&pool_mutex);

    if (alloc_cnt < blck_cnt) {
        // TODO :
        // This malloc() inside mutex may raise some performance issue,
        // Is there any way to execute malloc and counter adjustment
        // outside the mutex lock?
        mem_block_t *new_blk = NULL;
        uint32_t     new_cnt = 0;
        while (alloc_cnt < blck_cnt) {
            if ((new_blk = (mem_block_t *)malloc(sizeof(mem_block_t))) == NULL) break;
            new_blk->next = NULL;

            if (*head_blk) (*tail_blk)->next = new_blk;
            else           *head_blk = new_blk;
            (*tail_blk) = new_blk;

            new_cnt++;
            alloc_cnt++;
        }
        //pthread_mutex_lock(&pool_mutex);
        total_mblocks += new_cnt;
        //pthread_mutex_unlock(&pool_mutex);
        if (alloc_cnt < blck_cnt) {
            mblock_list_free(alloc_cnt, head_blk, tail_blk);
            return false;
        }
    }

    return true;
}

void mblock_list_free(uint32_t blck_cnt, mem_block_t **head_blk, mem_block_t **tail_blk) {
    //mem_block_t *bye = NULL;
    //mem_block_t *bye_helper = NULL;

    //pthread_mutex_lock(&pool_mutex);
    if (*head_blk == NULL || blck_cnt == 0)
        return;

    assert(pool_tail == NULL || pool_tail->next == NULL);
    assert((*tail_blk)->next == NULL);

    if (pool_head == NULL) {
        pool_head = *head_blk;
    } else {
        pool_tail->next = *head_blk;
    }
    pool_tail = *tail_blk;

    *head_blk = *tail_blk = NULL;
    free_mblocks += blck_cnt;
    assert(free_mblocks <= total_mblocks);

    // TODO : implement intelligent resize logic
    /*
    if (total_mblocks > initial_mblocks
            && free_mblocks > initial_mblocks / 2) {
        bye = pool_head;
        while(total_mblocks > initial_mblocks) {
            bye_helper = pool_head;
            pool_head = pool_head->next;
            free_mblocks--;
            total_mblocks--;
        }
        bye_helper->next = NULL;
    }*/

    //pthread_mutex_unlock(&pool_mutex);

    // rest of resize logic
    /*while (bye != NULL) {
        bye_helper = bye;
        bye = bye->next;
        free(bye_helper);
    }*/
}

bool eblk_prepare(eblock_result_t *result, uint32_t elem_count) {
    assert(elem_count > 0);
    uint32_t blkcnt;
    if (result->head_blk == NULL) { // empty block
        blkcnt = ((elem_count - 1) / EITEMS_PER_BLOCK) + 1;
        if (!mblock_list_alloc(blkcnt, &result->head_blk, &result->last_blk)) {
            result->elem_cnt = 0;
            return false;
        }
        result->tail_blk = NULL;
    } else {
        mem_block_t *head;
        mem_block_t *last;
        uint32_t curr_blkcnt = result->blck_cnt;
        int alloc_blkcnt;
        blkcnt = ((result->elem_cnt + elem_count - 1) / EITEMS_PER_BLOCK) + 1;
        alloc_blkcnt = blkcnt - curr_blkcnt;
        if (alloc_blkcnt > 0) { // need append block
            if (!mblock_list_alloc((alloc_blkcnt), &head, &last))
                return false;
            result->last_blk->next = head;
            result->last_blk = last;
        }
    }
    result->blck_cnt = blkcnt;
    return true;
}

void eblk_truncate(eblock_result_t *result) {
    assert(result->last_blk->next == NULL);
    /* returns empty blocklist */
    if (result->tail_blk != NULL) {
        if (result->tail_blk != result->last_blk) {
            mem_block_t *free_head = result->tail_blk->next;
            mem_block_t *free_tail = result->last_blk;
            uint32_t used_nblks = ((result->elem_cnt - 1) / EITEMS_PER_BLOCK) + 1;
            uint32_t free_nblks = result->blck_cnt - used_nblks;

            mblock_list_free(free_nblks, &free_head, &free_tail);
            result->tail_blk->next = NULL;
            result->last_blk = result->tail_blk;
            result->blck_cnt -= free_nblks;
        }
    } else { /* ENGINE_ELEM_ENOENT case */
        mblock_list_free(result->blck_cnt, &result->head_blk, &result->last_blk);
        result->head_blk = result->tail_blk = result->last_blk = NULL;
        result->elem_cnt = result->blck_cnt = 0;
    }
}

void eblk_add_elem(eblock_result_t *result, eitem *elem) {
    prepare_eblk_add_elem(result);
    result->tail_blk->items[result->elem_cnt++ % EITEMS_PER_BLOCK] = (eitem *)elem;
}

void eblk_add_elem_with_posi(eblock_result_t *result, eitem *elem, int posi) {
    mem_block_t *curr_blk = result->head_blk;
    int move_block_count = (posi / EITEMS_PER_BLOCK);

    while (move_block_count > 0) {
        curr_blk = curr_blk->next;
        move_block_count--;
    }

    prepare_eblk_add_elem(result);
    curr_blk->items[posi % EITEMS_PER_BLOCK] = (eitem *)elem;
    result->elem_cnt++;
}
