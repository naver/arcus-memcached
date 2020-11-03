/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2016 JaM2in Co., Ltd.
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
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sched.h>
#include <inttypes.h>
#include <sys/time.h> /* gettimeofday() */

#include "default_engine.h"

#if 0 // ENABLE_PERSISTENCE_01_ITEM_SCAN
/**
 * item scan
 */
/* item scan macros */
#define MAX_ITSCAN_COUNT 10
#define MAX_ITSCAN_ITEMS 128

/* item scan structure */
typedef struct _item_scan_t {
    struct assoc_scan asscan; /* assoc scan */
    const char *prefix;
    int        nprefix;
    bool       is_used;
    struct _item_scan_t *next;
} item_scan_t;

/* static global variables */
static item_scan_t     g_itscan_array[MAX_ITSCAN_COUNT];
static item_scan_t    *g_itscan_free_list;
static uint32_t        g_itscan_free_count;
static bool            g_itscan_init = false;
static pthread_mutex_t g_itscan_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Internal item scan functions
 */
static void do_itscan_prepare(void)
{
    item_scan_t *sp; /* scan pointer */

    for (int i = 0; i < MAX_ITSCAN_COUNT; i++) {
        sp = &g_itscan_array[i];
        sp->is_used = false;
        if (i < (MAX_ITSCAN_COUNT-1)) sp->next = &g_itscan_array[i+1];
        else                          sp->next = NULL;
    }
    g_itscan_free_list = &g_itscan_array[0];
    g_itscan_free_count = MAX_ITSCAN_COUNT;
}

static item_scan_t *do_itscan_alloc(void)
{
    item_scan_t *sp; /* scan pointer */

    pthread_mutex_lock(&g_itscan_lock);
    if (g_itscan_init != true) {
        do_itscan_prepare();
        g_itscan_init = true;
    }
    if ((sp = g_itscan_free_list) != NULL) {
        g_itscan_free_list = sp->next;
        g_itscan_free_count -= 1;
    }
    pthread_mutex_unlock(&g_itscan_lock);
    return sp;
}

static void do_itscan_free(item_scan_t *sp)
{
    pthread_mutex_lock(&g_itscan_lock);
    sp->next = g_itscan_free_list;
    g_itscan_free_list = sp;
    g_itscan_free_count += 1;
    pthread_mutex_unlock(&g_itscan_lock);
}

/*
 * External item scan functions
 */
void *itscan_open(struct default_engine *engine, const char *prefix, const int nprefix,
                  CB_SCAN_OPEN cb_scan_open)
{
    item_scan_t *sp = do_itscan_alloc();
    if (sp != NULL) {
        LOCK_CACHE();
        assoc_scan_init(&sp->asscan);
        if (cb_scan_open != NULL) {
            cb_scan_open(&sp->asscan);
        }
        UNLOCK_CACHE();
        sp->prefix = prefix;
        sp->nprefix = nprefix;
        sp->is_used = true;
    }
    return (void*)sp;
}

int itscan_getnext(void *scan, void **item_array, elems_result_t *erst_array, int item_arrsz)
{
    item_scan_t *sp = (item_scan_t *)scan;
    hash_item *it;
    elems_result_t *eresult;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    rel_time_t curtime = svcore->get_current_time();
    int scan_count = item_arrsz < MAX_ITSCAN_ITEMS
                   ? item_arrsz : MAX_ITSCAN_ITEMS;
    int elem_limit = (erst_array ? 1000 : 0);
    int item_count;
    int real_count;

    LOCK_CACHE();
    item_count = assoc_scan_next(&sp->asscan, (hash_item**)item_array, scan_count, elem_limit);
    if (item_count < 0) {
        real_count = -1; /* The end of assoc scan */
    } else {
        /* Currently, item_count > 0.
         * See the internals of assoc_scan_next(). It does not return 0.
         */
        real_count = 0;
        for (int idx = 0; idx < item_count; idx++) {
            it = (hash_item *)item_array[idx];
            /* Is it an internal or invalid item ? */
            if ((it->iflag & ITEM_INTERNAL) != 0 ||
                do_item_isvalid(it, curtime) != true) {
                item_array[idx] = NULL; continue;
            }
            /* Is it not the prefix item ? */
            if (sp->nprefix >= 0) {
                if (!assoc_prefix_issame(it->pfxptr, sp->prefix, sp->nprefix)) {
                    item_array[idx] = NULL; continue;
                }
            }
            /* Found the valid items */
            if (real_count < idx) {
                item_array[real_count] = item_array[idx];
            }
            if (erst_array != NULL) {
                eresult = &erst_array[real_count];
                eresult->elem_count = 0;
                if (IS_COLL_ITEM(it)) {
                    ret = coll_elem_get_all(it, eresult, false);
                    if (ret == ENGINE_ENOMEM) break;
                }
            }
            ITEM_REFCOUNT_INCR(it);
            real_count += 1;
        }
    }
    UNLOCK_CACHE();

    if (ret == ENGINE_ENOMEM) {
        if (real_count > 0) {
            itscan_release(scan, item_array, erst_array, real_count);
        }
        real_count = -2; /* OUT OF MEMORY */
    }
    return real_count;
}

void itscan_release(void *scan, void **item_array, elems_result_t *erst_array, int item_count)
{
    if (erst_array != NULL) {
        for (int idx = 0; idx < item_count; idx++) {
            if (erst_array[idx].elem_count > 0) {
                hash_item *it = (hash_item *)item_array[idx];
                assert(IS_COLL_ITEM(it));
                coll_elem_release(&erst_array[idx], GET_ITEM_TYPE(it));
            }
        }
    }

    LOCK_CACHE();
    for (int idx = 0; idx < item_count; idx++) {
        do_item_release(item_array[idx]);
    }
    UNLOCK_CACHE();
}

void itscan_close(void *scan, CB_SCAN_CLOSE cb_scan_close, bool success)
{
    item_scan_t *sp = (item_scan_t *)scan;

    LOCK_CACHE();
    assoc_scan_final(&sp->asscan);
    if (cb_scan_close != NULL) {
        cb_scan_close(success);
    }
    UNLOCK_CACHE();

    sp->is_used = false;
    do_itscan_free(sp);
}
#endif
