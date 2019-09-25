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
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
#include "config.h"

#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <inttypes.h>
#include <stdarg.h>

#include "default_engine.h"

#define CHUNK_ALIGN_BYTES 8
#define DONT_PREALLOC_SLABS

#define MAX_SPACE_SHORTAGE_LEVEL 100
#define SSL_FOR_BACKGROUND_EVICT 10  /* space shortage level for background evict */
#define SM_MAX_CLASS_INFO 10

static int RSVD_SLAB_COUNT = 4; /* # of reserved slabs */
static int RSVD_SLAB_RATIO = 5; /* reserved slab ratio */

/* sm slot head */
typedef struct _sm_slot {
    uint32_t    status; /* 0: free slot */
    uint16_t    offset;
    uint16_t    length;
    struct _sm_slot *prev;
    struct _sm_slot *next;
} sm_slot_t;

/* sm slot tail */
typedef struct _sm_tail {
    uint16_t    offset;
    uint16_t    length; /* 0: free slot */
} sm_tail_t;

/* sm block */
typedef struct _sm_blck {
    struct _sm_blck *prev;
    struct _sm_blck *next;
    uint32_t    frspc; /* (NOT USED) free slot space in each block */
    uint32_t    frcnt; /* (NOT USED) free slot count in each block */
    /* The size of sm block strcture must be a multiple of
     * the size of the smallest slot unit. (ex, 16 or 32).
     * For that purpose, following dummy field was added.
     */
    uint64_t    dummy64;
} sm_blck_t;

/* sm slot list */
typedef struct _sm_slist {
    sm_slot_t   *head;
    sm_slot_t   *tail;
    uint64_t    space;
    uint64_t    count;
} sm_slist_t;

/* sm block list */
typedef struct _sm_blist {
    sm_blck_t   *head;
    sm_blck_t   *tail;
    uint64_t    count;
} sm_blist_t;

/* sm slot class meta info */
typedef struct _sm_class {
    uint32_t sulen; /* current slot unit length */
    uint32_t sucnt; /* current slot unit count */
    uint32_t tolen; /* total slot unit length */
    uint32_t tocnt; /* total slot unit count */
} sm_class_t;

typedef struct _sm_anchor {
    int         space_shortage_level; /* 0, 1 ~ 100 */
    int         used_num_classes;   /* # of used slot classes */
    int         free_num_classes;   /* # of free slot classes */
    int         used_minid;         /* min sm classid of used slots */
    int         used_maxid;         /* max sm classid of used slots */
    int         used_01pct_clsid;   /* last 1% classid of total used space */
    int         free_minid;         /* max sm classid of free slots */
    int         free_maxid;         /* max sm classid of free slots (excluding the largest free slot) */
    sm_blist_t  used_blist;         /* used block list */
    sm_slist_t *used_slist;         /* used slot info */
    sm_slist_t *free_slist;         /* free slot list */
    uint64_t    used_total_space;   /* the amount of used space */
    uint64_t    used_01pct_space;   /* the amount of last 1% total used space */
    uint64_t    free_small_space;   /* the amount of free space that can't be used */
    uint64_t    free_avail_space;   /* the amount of free space that can be used */
    uint64_t    free_chunk_space;   /* the amount of free chunk space */
    uint64_t    free_limit_space;   /* the amount of minimum free space that must be maintained */
    sm_class_t  class_info[SM_MAX_CLASS_INFO]; /* class meta info */
    uint32_t    class_info_count;   /* class meta info count */
} sm_anchor_t;

/* sm slab class id */
#define SM_SLAB_CLSID   0

/* sm block size (Note the maximum value is 512K) */
#define SM_BLOCK_SIZE   262144 // 256K

/* The minimum and maximum size of sm slot */
#define SM_MIN_SLOT_SIZE 32
//#define SM_MAX_SLOT_SIZE 8192 // 8K
#define SM_MAX_SLOT_SIZE 49152 // 48K

/* slot unit info used to calculate sm slot classes */
//#define SM_SLOT_UNIT_LEN 8
//#define SM_SLOT_UNIT_CNT 1024
#define SM_SLOT_UNIT_LEN 8
#define SM_SLOT_UNIT_CNT 128

/* macros for converting offset and length values of slots */
#define SM_SLOT_OFFSET(o)  ((o)/8)
#define SM_SLOT_LENGTH(l)  ((l)/8)
#define SM_REAL_OFFSET(o)  ((o)*8)
#define SM_REAL_LENGTH(l)  ((l)*8)
/****
#define SM_SLOT_OFFSET(o)  (o)
#define SM_SLOT_LENGTH(l)  (l)
#define SM_REAL_OFFSET(o)  (o)
#define SM_REAL_LENGTH(l)  (l)
****/

/* macros for checking the validity of offset and length values */
#define SM_VALID_OFFSET(o) (((o)%8)==0)
#define SM_VALID_LENGTH(l) (((l)%8)==0)

/* macros for checking the used/free state of the given slot */
/* 1) check the used/free state using status value of slot head. */
#define SM_USED_SLOT(slot) ((slot)->status != 0)
#define SM_FREE_SLOT(slot) ((slot)->status == 0)
/* 2) check the used/free state using length value of slot tail.
 *    All free slots have 0 as the length value of slot tail.
 *    Exceptionally, only the smallest free slot of 8 bytes,
 *    has the real size as the length value.
 */
#define SM_USED_TAIL(tail) ((tail)->length >  1)
#define SM_FREE_TAIL(tail) ((tail)->length <= 1)
/****
#define SM_USED_TAIL(tail) ((tail)->length >  8)
#define SM_FREE_TAIL(tail) ((tail)->length <= 8)
****/

/* Number of sm slot classes */
static int SM_NUM_CLASSES = 0; /* computed in do_smmgr_init */

/* The head and body size of sm block */
static int SM_BHEAD_SIZE = sizeof(sm_blck_t);
static int SM_BBODY_SIZE = SM_BLOCK_SIZE - sizeof(sm_blck_t);

static sm_anchor_t sm_anchor;

static struct engine_config *config=NULL; // engine config
static struct slabs         *slabsp=NULL; // engine slabs;
static EXTENSION_LOGGER_DESCRIPTOR *logger;


/* global variable */
int MAX_SM_VALUE_LEN = SM_MAX_SLOT_SIZE - sizeof(sm_tail_t);

/*
 * Forward Declarations
 */
static void *do_slabs_alloc(const size_t size, unsigned int id);
static void  do_slabs_free(void *ptr, const size_t size, unsigned int id);

#ifndef DONT_PREALLOC_SLABS
/* Preallocate as many slab pages as possible (called from slabs_init)
   on start-up, so users don't get confused out-of-memory errors when
   they do have free (in-slab) space, but no space to make new slabs.
   if maxslabs is 18 (POWER_LARGEST - POWER_SMALLEST + 1), then all
   slab types can be made.  if max memory is less than 18 MB, only the
   smaller ones will be made.  */
static void slabs_preallocate (const unsigned int maxslabs);
#endif

static void do_slabs_check_space_shortage_level(void)
{
    uint64_t curr_avail_space = sm_anchor.free_chunk_space
                              + sm_anchor.free_avail_space;
    if (curr_avail_space < sm_anchor.free_limit_space) {
        /* How do we compute the space_shortage_level ?
         * Use the following formula.
         * => (free_limit_space / (curr_avail_space / N)) - (N-1)
         */
        int ssl;    /* space shortage level */
        int num=10; /* N */
        if ((curr_avail_space / num) > 0) {
            ssl = (sm_anchor.free_limit_space / (curr_avail_space / num)) - (num-1);
            if (ssl > MAX_SPACE_SHORTAGE_LEVEL) ssl = MAX_SPACE_SHORTAGE_LEVEL;
        } else {
            ssl = MAX_SPACE_SHORTAGE_LEVEL;
        }
        /*** Disable printing the increment of space shortage level ***
        if (ssl > sm_anchor.space_shortage_level) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "Space shortage level increases: %d => %d "
                        "free_space(small=%llu, avail=%llu, chunk=%llu)\n",
                        sm_anchor.space_shortage_level, ssl,
                        (unsigned long long)sm_anchor.free_small_space,
                        (unsigned long long)sm_anchor.free_avail_space,
                        (unsigned long long)sm_anchor.free_chunk_space);
        }
        ***************************************************************/
        sm_anchor.space_shortage_level = ssl;
    } else {
        sm_anchor.space_shortage_level = 0;
    }
}

static void do_smmgr_set_class_info(int idx, int sulen, int sucnt, int tolen, int tocnt)
{
    sm_anchor.class_info[idx].sulen = sulen;
    sm_anchor.class_info[idx].sucnt = sucnt;
    sm_anchor.class_info[idx].tolen = tolen;
    sm_anchor.class_info[idx].tocnt = tocnt;
}

static int do_smmgr_build_class_info(void)
{
    int count = 0;
    int sunit_len = SM_SLOT_UNIT_LEN;
    int sunit_cnt = SM_SLOT_UNIT_CNT;
    int total_len = 0;
    int total_cnt = 0;

    while ((total_len + (sunit_len*sunit_cnt)) < SM_MAX_SLOT_SIZE) {
        do_smmgr_set_class_info(count, sunit_len, sunit_cnt, total_len, total_cnt);
        count += 1;
        /* adjust total_cnt, total_len, sunit_len */
        total_cnt += sunit_cnt;
        total_len += (sunit_len*sunit_cnt);
        sunit_len *= 2;
    }

    assert(((SM_MAX_SLOT_SIZE - total_len) % sunit_len) == 0);
    sunit_cnt = (SM_MAX_SLOT_SIZE - total_len) / sunit_len;
    do_smmgr_set_class_info(count, sunit_len, sunit_cnt, total_len, total_cnt);
    count += 1;

    total_cnt += sunit_cnt;
    total_len += (sunit_len*sunit_cnt);
    assert(total_len == SM_MAX_SLOT_SIZE);
    do_smmgr_set_class_info(count, 0, 0, total_len, total_cnt);
    count += 1;
    assert(count <= SM_MAX_CLASS_INFO);
    sm_anchor.class_info_count = count;

    if (config->verbose > 2) {
        sm_class_t *cls;
        fprintf(stderr, "sm class info\n");
        fprintf(stderr, "index  sunit_len  sunit_cnt  total_len  total_cnt\n");
        fprintf(stderr, "-------------------------------------------------\n");
        for (int i=0; i < sm_anchor.class_info_count; i++) {
            cls = &sm_anchor.class_info[i];
            fprintf(stderr, "%5d %10d %10d %10d %10d\n",
                    i, cls->sulen, cls->sucnt, cls->tolen, cls->tocnt);
        }
        fprintf(stderr, "-------------------------------------------------\n");
        /******
        fprintf(stderr, "sm classes\n");
        fprintf(stderr, "-------------------------------------------------\n");
        int s, t, index = 0;
        for (s = 0; s < sm_anchor.class_info_count; s++) {
            cls = &sm_anchor.class_info[s];
            for (t = 1; t <= cls->sucnt; t++) {
                fprintf(stderr, "sm class [%4d] : %7d\n",
                        index, cls->tolen + (t*cls->sulen));
                index += 1;
            }
        }
        fprintf(stderr, "-------------------------------------------------\n");
        ******/
    }

    return total_cnt;
}

static int do_smmgr_init(void)
{
    /* small memory allocator */
    memset(&sm_anchor, 0, sizeof(sm_anchor_t));

    /* set the number of sm slot classes */
    SM_NUM_CLASSES = do_smmgr_build_class_info();
    SM_NUM_CLASSES += 1; /* 1: big free slot list */

    sm_anchor.used_minid = SM_NUM_CLASSES;
    sm_anchor.used_maxid = -1;
    sm_anchor.free_minid = SM_NUM_CLASSES;
    sm_anchor.free_maxid = -1;
    sm_anchor.used_01pct_clsid = -1;

    /* allocate used/free slot list structure */
    int need_size = SM_NUM_CLASSES * sizeof(sm_slist_t) * 2;
    sm_anchor.used_slist = (sm_slist_t*)malloc(need_size);
    if (sm_anchor.used_slist == NULL) return -1;
    sm_anchor.free_slist = sm_anchor.used_slist + SM_NUM_CLASSES;
    memset(sm_anchor.used_slist, 0, need_size);

    /* slab allocator */
    /* slab class 0 is used for collection items ans small-sized kv items */
    slabclass_t *p = &slabsp->slabclass[0];
    p->size = SM_BLOCK_SIZE;
    p->perslab = config->item_size_max / p->size;
    p->rsvd_slabs = 0; // undefined

    return 0;
}

static void do_smmgr_final(void)
{
    if (sm_anchor.used_slist != NULL) {
        free(sm_anchor.used_slist);
        sm_anchor.used_slist = NULL;
        sm_anchor.free_slist = NULL;
    }
}

static inline int do_smmgr_slen(int size)
{
    int slen = (int)size + sizeof(sm_tail_t);
    slen = (((slen-1) / 8) + 1) * 8;
    if (slen < SM_MIN_SLOT_SIZE)
        slen = SM_MIN_SLOT_SIZE;
    return slen;
}

static inline int do_smmgr_memid(int slen, bool above)
{
    assert((slen%8) == 0);
    if (slen >= SM_MAX_SLOT_SIZE) return SM_NUM_CLASSES-1;

    sm_class_t *cls = &sm_anchor.class_info[0];
    while (slen >= (cls+1)->tolen) {
        cls++;
    }
    int smid = cls->tocnt + ((slen-cls->tolen) / cls->sulen);
    /* The above code logic likes followings.
    if (slen < ( 2*1024)) return (  0 + ((slen          ) /  16));
    if (slen < ( 6*1024)) return (128 + ((slen-( 2*1024)) /  32));
    if (slen < (14*1024)) return (256 + ((slen-( 6*1024)) /  64));
    if (slen < (30*1024)) return (384 + ((slen-(14*1024)) / 128));
    else                  return (512 + ((slen-(30*1024)) / 256));
    *****************************************/
    if (above && ((slen-cls->tolen) % cls->sulen) != 0) {
        smid++;
    }
    return smid;
#if 0 // old code
    return (slen/8);
#endif
}

static void do_smmgr_used_slot_list_add(int targ)
{
    if (targ < sm_anchor.used_minid) {
        sm_anchor.used_minid = targ;
    }
    if (targ > sm_anchor.used_maxid) {
        sm_anchor.used_maxid = targ;
    }
    sm_anchor.used_num_classes += 1;
}

static void do_smmgr_used_slot_list_del(int targ)
{
    int smid;
    if (targ == sm_anchor.used_minid) {
        if (targ == sm_anchor.used_maxid) {
            sm_anchor.used_minid = SM_NUM_CLASSES;
            sm_anchor.used_maxid = -1;
        } else {
            for (smid = targ+1; smid <= sm_anchor.used_maxid; smid++) {
                if (sm_anchor.used_slist[smid].count > 0) break;
            }
            assert(smid <= sm_anchor.used_maxid);
            sm_anchor.used_minid = smid;
        }
    } else {
        if (targ == sm_anchor.used_maxid) {
            for (smid = targ-1; smid >= sm_anchor.used_minid; smid--) {
                if (sm_anchor.used_slist[smid].count > 0) break;
            }
            assert(smid >= sm_anchor.used_minid);
            sm_anchor.used_maxid = smid;
        }
    }
    sm_anchor.used_num_classes -= 1;
}

static void do_smmgr_free_slot_list_add(int targ)
{
    if (targ >= (SM_NUM_CLASSES-1)) {
        /* big free slot */
        return;
    }

    if (targ < sm_anchor.free_minid) {
        sm_anchor.free_minid = targ;
    }
    if (targ > sm_anchor.free_maxid) {
        sm_anchor.free_maxid = targ;
    }
    sm_anchor.free_num_classes += 1;
}

static void do_smmgr_free_slot_list_del(int targ)
{
    if (targ >= (SM_NUM_CLASSES-1)) {
        /* big free slot */
        return;
    }

    int smid;
    if (targ == sm_anchor.free_minid) {
        if (targ == sm_anchor.free_maxid) {
            sm_anchor.free_minid = SM_NUM_CLASSES;
            sm_anchor.free_maxid = -1;
        } else {
            for (smid = targ+1; smid <= sm_anchor.free_maxid; smid++) {
                if (sm_anchor.free_slist[smid].head != NULL) break;
            }
            assert(smid <= sm_anchor.free_maxid);
            sm_anchor.free_minid = smid;
        }
    } else {
        if (targ == sm_anchor.free_maxid) {
            for (smid = targ-1; smid >= sm_anchor.free_minid; smid--) {
                if (sm_anchor.free_slist[smid].head != NULL) break;
            }
            assert(smid >= sm_anchor.free_minid);
            sm_anchor.free_maxid = smid;
        }
    }
    sm_anchor.free_num_classes -= 1;
}

static void do_smmgr_used_slot_init(sm_slot_t *slot, int offset, int length)
{
    assert(SM_VALID_OFFSET(offset) && SM_VALID_LENGTH(length));
    sm_tail_t *tail = (sm_tail_t*)((char*)slot + length - sizeof(sm_tail_t));
    tail->offset = (uint16_t)SM_SLOT_OFFSET(offset);
    tail->length = (uint16_t)SM_SLOT_LENGTH(length); /* used slot */
    /* Set it as used slot.
     * In eager invalidation, incomplete slot can be inspected.
     * At that time, the slot must be regared as an used slot.
     */
    slot->status = (uint32_t)-1; /* used slot */
}

static void do_smmgr_free_slot_init(sm_slot_t *slot, int offset, int length)
{
    assert(SM_VALID_OFFSET(offset) && SM_VALID_LENGTH(length));
    sm_tail_t *tail = (sm_tail_t*)((char*)slot + length - sizeof(sm_tail_t));
    tail->offset = (uint16_t)SM_SLOT_OFFSET(offset);
    tail->length = 0; /* free slot */
    slot->status = 0; /* free slot */
    slot->offset = tail->offset;
    slot->length = (uint16_t)SM_SLOT_LENGTH(length);
}

static void do_smmgr_free_slot_link(sm_slot_t *slot, int offset, int length)
{
    sm_slist_t *list;
    int slen = length;
    int smid;

    /* initialize the free slot */
    do_smmgr_free_slot_init(slot, offset, slen);

    if (slen < SM_MIN_SLOT_SIZE) {
        sm_anchor.free_small_space += slen;
        return;
    }

    smid = do_smmgr_memid(slen, false);
    list = &sm_anchor.free_slist[smid];

    /* link the slot to the tail of the list */
    slot->next = NULL;
    if (list->tail == NULL) {
        assert(list->head == NULL && list->count == 0 && list->space == 0);
        slot->prev = NULL;
        list->head = slot;
    } else {
        assert(list->head != NULL && list->count > 0 && list->space > 0);
        slot->prev = list->tail;
        slot->prev->next = slot;
    }
    list->tail = slot;
    list->space += slen;
    list->count += 1;
    if (list->count == 1) {
        do_smmgr_free_slot_list_add(smid);
    }

    if (sm_anchor.used_01pct_clsid != -1) {
        if (smid < sm_anchor.used_01pct_clsid) {
            sm_anchor.free_small_space += slen;
        } else {
            sm_anchor.free_avail_space += slen;
        }
    } else {
        if (smid == (SM_NUM_CLASSES-1)) { /* big free slot */
            sm_anchor.free_avail_space += slen;
        } else {
            /* The free small or avail space will be set
             * in do_smmgr_01pct_first_set().
             */
        }
    }
}

static void do_smmgr_free_slot_unlink(sm_slot_t *slot)
{
    sm_slist_t *list;
    int slen = SM_REAL_LENGTH(slot->length);
    int smid;

    if (slen < SM_MIN_SLOT_SIZE) {
        sm_anchor.free_small_space -= slen;
        return;
    }

    smid = do_smmgr_memid(slen, false);
    assert(sm_anchor.used_01pct_clsid != -1);
    if (smid < sm_anchor.used_01pct_clsid) {
        sm_anchor.free_small_space -= slen;
    } else {
        sm_anchor.free_avail_space -= slen;
    }

    list = &sm_anchor.free_slist[smid];
    if (list->count == 1) {
        assert(list->space == slen && list->head == slot && list->tail == slot);
        list->head = NULL;
        list->tail = NULL;
        list->space = 0;
        list->count = 0;
        do_smmgr_free_slot_list_del(smid);
    } else {
        assert(list->count > 1 && list->space > slen &&
               list->head != NULL && list->tail != NULL);
        if (slot->prev != NULL) slot->prev->next = slot->next;
        if (slot->next != NULL) slot->next->prev = slot->prev;
        if (list->head == slot) list->head = slot->next;
        if (list->tail == slot) list->tail = slot->prev;
        list->space -= slen;
        list->count -= 1;
    }
}

#if 0 // can be used later
static void do_smmgr_used_blck_check(void)
{
    sm_blck_t *blck;
    sm_slot_t *slot;
    sm_tail_t *tail;
    uint64_t blck_count = 0;
    uint64_t used_count = 0;
    uint64_t free_count = 0;
    uint32_t comp_length;

    blck = sm_anchor.used_blist.head;
    while (blck != NULL) {
        blck_count += 1;
        tail = (sm_tail_t*)((char*)blck + SM_BLOCK_SIZE - sizeof(sm_tail_t));
        while (((char*)tail - (char*)blck) > SM_BHEAD_SIZE) {
            slot = (sm_slot_t*)((char*)blck + SM_REAL_OFFSET(tail->offset));
            if (SM_USED_TAIL(tail)) { /* used slot */
                used_count += 1;
                assert(SM_USED_SLOT(slot));
            } else { /* free slot */
                free_count += 1;
                comp_length = (char*)tail - (char*)slot + sizeof(sm_tail_t);
                assert(SM_FREE_SLOT(slot));
                assert(slot->offset == tail->offset);
                assert(SM_REAL_LENGTH(slot->length) == comp_length);
            }
            tail = (sm_tail_t*)((char*)slot - sizeof(sm_tail_t));
        }
        blck = blck->next;
    }
    assert(blck_count == sm_anchor.used_blist.count);
}
#endif

static void do_smmgr_used_blck_link(sm_blck_t *blck)
{
    blck->frspc = 0;
    blck->frcnt = 0;

    blck->prev = sm_anchor.used_blist.tail;
    blck->next = NULL;
    if (sm_anchor.used_blist.head == NULL) {
        sm_anchor.used_blist.head = blck;
        sm_anchor.used_blist.tail = blck;
    } else {
        blck->prev->next = blck;
        sm_anchor.used_blist.tail = blck;
    }
    sm_anchor.used_blist.count += 1;
}

static void do_smmgr_used_blck_unlink(sm_blck_t *blck)
{
    if (blck->prev != NULL) blck->prev->next = blck->next;
    if (blck->next != NULL) blck->next->prev = blck->prev;
    if (sm_anchor.used_blist.head == blck) sm_anchor.used_blist.head = blck->next;
    if (sm_anchor.used_blist.tail == blck) sm_anchor.used_blist.tail = blck->prev;
    sm_anchor.used_blist.count -= 1;
}

static sm_blck_t *do_smmgr_blck_alloc(void)
{
    sm_blck_t *blck = (sm_blck_t *)do_slabs_alloc(SM_BLOCK_SIZE, SM_SLAB_CLSID);
    if (blck != NULL) {
        if (sm_anchor.free_limit_space > 0) {
            sm_anchor.free_chunk_space -= SM_BLOCK_SIZE;
        }
        do_smmgr_used_blck_link(blck);
    }
    return blck;
}

static void do_smmgr_blck_free(sm_blck_t *blck)
{
    do_smmgr_used_blck_unlink(blck);
    do_slabs_free(blck, SM_BLOCK_SIZE, SM_SLAB_CLSID);
    if (sm_anchor.free_limit_space > 0) {
        sm_anchor.free_chunk_space += SM_BLOCK_SIZE;
    }
}

static void do_smmgr_01pct_first_set(int slen, int targ)
{
    /* the first SM slot allocation */
    assert(sm_anchor.used_01pct_clsid == -1);

    if (sm_anchor.free_maxid != -1) {
        assert(sm_anchor.free_maxid == sm_anchor.free_minid);
        assert(sm_anchor.free_small_space == 0);
        assert(sm_anchor.free_avail_space == 0);

        int smid = sm_anchor.free_maxid;
        if (smid < targ) {
            sm_anchor.free_small_space = sm_anchor.free_slist[smid].space;
        } else {
            sm_anchor.free_avail_space = sm_anchor.free_slist[smid].space;
        }
    }
    sm_anchor.used_01pct_clsid = targ;
    sm_anchor.used_01pct_space = slen;
}

static void do_smmgr_01pct_last_clear(void)
{
    /* the last SM slot release */
    assert(sm_anchor.free_small_space == 0);
    assert(sm_anchor.free_avail_space == 0);

    sm_anchor.used_01pct_clsid = -1;
    sm_anchor.used_01pct_space = 0;
}

static void do_smmgr_01pct_check_and_move_right(void)
{
    uint64_t space_standard;
    uint64_t space_adjusted;
    int smid, i;

    /* get 1% of total_used_space, that is positive value. */
    space_standard = sm_anchor.used_total_space/100 + 1;

    /* find the new used_01pct_clsid */
    for (smid = sm_anchor.used_01pct_clsid; smid < sm_anchor.used_maxid; smid++) {
        if (sm_anchor.used_slist[smid].space > 0) {
            if ((sm_anchor.used_01pct_space - sm_anchor.used_slist[smid].space) < space_standard) {
                break;
            }
            sm_anchor.used_01pct_space -= sm_anchor.used_slist[smid].space;
        }
    }
    if (smid != sm_anchor.used_01pct_clsid) {
        /* adjust free small & avail space */
        space_adjusted = 0;
        for (i = sm_anchor.used_01pct_clsid; i < smid; i++) {
            if (sm_anchor.free_slist[i].space > 0)
                space_adjusted += sm_anchor.free_slist[i].space;
        }
        sm_anchor.free_small_space += space_adjusted;
        sm_anchor.free_avail_space -= space_adjusted;

        if (config->verbose > 1) {
            if (sm_anchor.free_limit_space > 0 && space_adjusted >= (sm_anchor.free_limit_space/10)) {
                logger->log(EXTENSION_LOG_INFO, NULL,
                            "Large free_avail_space reduction(%llu): small=%llu, avail=%llu, chunk=%llu "
                            "That was caused by the change of the last 1%% clsid of used space(%d => %d).\n",
                            (unsigned long long)space_adjusted,
                            (unsigned long long)sm_anchor.free_small_space,
                            (unsigned long long)sm_anchor.free_avail_space,
                            (unsigned long long)sm_anchor.free_chunk_space,
                            sm_anchor.used_01pct_clsid, smid);
            }
        }

        /* set the new used_01pct_clsid */
        sm_anchor.used_01pct_clsid = smid;
    }
}

static void do_smmgr_01pct_check_and_move_left(void)
{
    uint64_t space_standard;
    uint64_t space_adjusted;
    int smid, i;

    /* get 1% of total_used_space, that is positive value. */
    space_standard = sm_anchor.used_total_space/100 + 1;

    if (sm_anchor.used_01pct_space < space_standard) {
        /* find the new used_01pct_clsid */
        smid = sm_anchor.used_01pct_clsid - 1;
        if (smid > sm_anchor.used_maxid) {
            smid = sm_anchor.used_maxid;
        }
        for ( ; smid >= sm_anchor.used_minid; smid--) {
            if (sm_anchor.used_slist[smid].space > 0) {
                sm_anchor.used_01pct_space += sm_anchor.used_slist[smid].space;
                if (sm_anchor.used_01pct_space >= space_standard) {
                    break;
                }
            }
        }
        assert(smid >= sm_anchor.used_minid);

        /* adjust free small & avail space */
        space_adjusted = 0;
        for (i = smid; i < sm_anchor.used_01pct_clsid; i++) {
            if (sm_anchor.free_slist[i].space > 0)
                space_adjusted += sm_anchor.free_slist[i].space;
        }
        sm_anchor.free_small_space -= space_adjusted;
        sm_anchor.free_avail_space += space_adjusted;

        /* set the new used_01pct_clsid */
        sm_anchor.used_01pct_clsid = smid;
    } else {
         if (sm_anchor.used_slist[sm_anchor.used_01pct_clsid].space == 0) {
            /* An uncommon case, caused by using nonprecise 01pct space.
             * In the following state, freeing the 01pct slot is the example.
             * - used total space: 100000, (where, 1% is 1000.)
             * - used classes
             *   - maxid: count=1, space=999
             *   - 01pct: count=1, space=800
             * After freeing the 01pct slot,
             * the used total space becomes 99200, (where, 1% is 992.)
             * the used 01pct space becomes 999.
             * That is, the used 01pct space(999) > the 1% space(992).
             * In this case, we move right the used 01pct clsid.
             */
            /* find the new used_01pct_clsid */
            smid = sm_anchor.used_01pct_clsid + 1;
            for ( ; smid <= sm_anchor.used_maxid; smid++) {
                if (sm_anchor.used_slist[smid].space > 0) break;
            }
            assert(smid <= sm_anchor.used_maxid);

            /* adjust free small & avail space */
            space_adjusted = 0;
            for (i = sm_anchor.used_01pct_clsid; i < smid; i++) {
                if (sm_anchor.free_slist[i].space > 0)
                    space_adjusted += sm_anchor.free_slist[i].space;
            }
            sm_anchor.free_small_space += space_adjusted;
            sm_anchor.free_avail_space -= space_adjusted;

            /* set the new used_01pct_clsid */
            sm_anchor.used_01pct_clsid = smid;
        }
    }
}

static void *do_smmgr_alloc(const size_t size)
{
    sm_slot_t *cur_slot = NULL;
    sm_slot_t *nxt_slot;
    int smid, targ;
    int slen;

    if (sm_anchor.free_limit_space > 0) {
        do_slabs_check_space_shortage_level();
        if (sm_anchor.space_shortage_level >= SSL_FOR_BACKGROUND_EVICT)
            coll_del_thread_wakeup();
    }

    //do_smmgr_used_blck_check();

    slen = do_smmgr_slen(size);
    targ = do_smmgr_memid(slen, true);

    /* find the free slot list from which we allocate a slot. */
    do {
        if (targ > sm_anchor.free_maxid) {
            /* the clsid of big free slot */
            smid = SM_NUM_CLASSES-1; break;
        }
        if (sm_anchor.free_slist[targ].head != NULL) {
            smid = targ; break;
        }
        /* look for a 2 times larger free slot */
        smid = do_smmgr_memid(slen*2, false);
        if (sm_anchor.space_shortage_level > 0) {
            /* use free small memory if possible. */
            int twice = smid;
            /* Since targ <= sm_anchor.free_maxid,
             * sm_anchor.used_01pct_clsid != -1.
             */
            if (smid >= sm_anchor.used_01pct_clsid) {
                smid = sm_anchor.used_01pct_clsid-1;
            }
            for ( ; smid > targ; smid--) {
                if (sm_anchor.free_slist[smid].head != NULL) break;
            }
            if (smid > targ) break;
            smid = twice;
        }
        /* This is a heuristic method */
        if (smid > sm_anchor.free_maxid) {
            smid = sm_anchor.free_maxid;
        } else {
            for ( ; smid <= sm_anchor.free_maxid; smid++) {
                if (sm_anchor.free_slist[smid].head != NULL) break;
            }
            assert(smid <= sm_anchor.free_maxid);
        }
    } while(0);

    /* allocate a slot from the free slot list */
    cur_slot = sm_anchor.free_slist[smid].head;
    if (cur_slot == NULL) {
        sm_blck_t *blck = do_smmgr_blck_alloc();
        if (blck == NULL) return NULL;

        cur_slot = (sm_slot_t*)((char*)blck + SM_BHEAD_SIZE);
        do_smmgr_used_slot_init(cur_slot, SM_BHEAD_SIZE, slen);

        nxt_slot = (sm_slot_t*)((char*)cur_slot + slen);
        do_smmgr_free_slot_link(nxt_slot, SM_BHEAD_SIZE+slen, SM_BBODY_SIZE-slen);
    } else {
        int cur_offset = SM_REAL_OFFSET(cur_slot->offset);
        int cur_length = SM_REAL_LENGTH(cur_slot->length);
        assert(cur_length >= slen);

        do_smmgr_free_slot_unlink(cur_slot);
        do_smmgr_used_slot_init(cur_slot, cur_offset, slen);
        if (cur_length > slen) {
            nxt_slot = (sm_slot_t*)((char*)cur_slot + slen);
            do_smmgr_free_slot_link(nxt_slot, cur_offset+slen, cur_length-slen);
        }
    }

    /* used slot stats */
    sm_anchor.used_total_space += slen;
    sm_anchor.used_slist[targ].space += slen;
    sm_anchor.used_slist[targ].count += 1;
    if (sm_anchor.used_slist[targ].count == 1) {
        do_smmgr_used_slot_list_add(targ);
    }

    /* adjust used 01pct class */
    if (sm_anchor.used_total_space == slen) {
        assert(sm_anchor.used_01pct_space == 0);
        do_smmgr_01pct_first_set(slen, targ);
    } else {
        if (targ >= sm_anchor.used_01pct_clsid) {
            sm_anchor.used_01pct_space += slen;
            do_smmgr_01pct_check_and_move_right();
        } else {
            do_smmgr_01pct_check_and_move_left();
        }
    }
    return (void*)cur_slot;
}

static sm_slot_t *sm_get_prev_free_slot(sm_slot_t *cur_slot, sm_blck_t *cur_blck)
{
    sm_slot_t *prv_slot;
    sm_tail_t *prv_tail = (sm_tail_t*)((char*)cur_slot - sizeof(sm_tail_t));

    if (SM_FREE_TAIL(prv_tail)) { /* free slot */
        prv_slot = (sm_slot_t*)((char*)cur_blck + SM_REAL_OFFSET(prv_tail->offset));
        assert(prv_slot->offset == prv_tail->offset && SM_FREE_SLOT(prv_slot));
    } else {
        prv_slot = NULL;
    }
    return prv_slot;
}

static sm_slot_t *sm_get_next_free_slot(sm_tail_t *cur_tail)
{
    sm_slot_t *nxt_slot = (sm_slot_t*)((char*)cur_tail + sizeof(sm_tail_t));
    sm_tail_t *nxt_tail;

    if (SM_FREE_SLOT(nxt_slot)) { /* free slot */
        nxt_tail = (sm_tail_t*)((char*)nxt_slot + SM_REAL_LENGTH(nxt_slot->length)
                                                - sizeof(sm_tail_t));
        assert(nxt_tail->offset == nxt_slot->offset && SM_FREE_TAIL(nxt_tail));
    } else {
        nxt_slot = NULL;
    }
    return nxt_slot;
}

static void do_smmgr_free(void *ptr, const size_t size)
{
    sm_blck_t *cur_blck;
    sm_slot_t *cur_slot;
    sm_tail_t *cur_tail;
    int targ;
    int slen;

    if (sm_anchor.free_limit_space > 0) {
        do_slabs_check_space_shortage_level();
        if (sm_anchor.space_shortage_level >= SSL_FOR_BACKGROUND_EVICT)
            coll_del_thread_wakeup();
    }

    slen = do_smmgr_slen(size);
    targ = do_smmgr_memid(slen, true);

    cur_slot = (sm_slot_t*)ptr;
    cur_tail = (sm_tail_t*)((char*)cur_slot + slen - sizeof(sm_tail_t));

    int cur_offset = SM_REAL_OFFSET(cur_tail->offset);
    int cur_length = SM_REAL_LENGTH(cur_tail->length);
    assert(cur_length == slen);

    cur_blck = (sm_blck_t*)((char*)cur_slot - cur_offset);

    /* check and merge the prev slot if it exists as freed state. */
    if (cur_offset > SM_BHEAD_SIZE) {
        sm_slot_t *prv_slot = sm_get_prev_free_slot(cur_slot, cur_blck);
        if (prv_slot != NULL) {
            do_smmgr_free_slot_unlink(prv_slot);
            cur_offset  = SM_REAL_OFFSET(prv_slot->offset);
            cur_length += SM_REAL_LENGTH(prv_slot->length);
            cur_slot = prv_slot;
        }
    }
    /* check and merge the next slot if it exists as freed state. */
    if ((cur_offset + cur_length) < SM_BLOCK_SIZE) {
        sm_slot_t *nxt_slot = sm_get_next_free_slot(cur_tail);
        if (nxt_slot != NULL) {
            do_smmgr_free_slot_unlink(nxt_slot);
            cur_length += SM_REAL_LENGTH(nxt_slot->length);
        }
    }
    /* free the slot */
    if (cur_offset > SM_BHEAD_SIZE || cur_length < SM_BBODY_SIZE) {
        do_smmgr_free_slot_link(cur_slot, cur_offset, cur_length);
    } else {
        do_smmgr_blck_free(cur_blck);
    }

    /* used slot stats */
    assert(sm_anchor.used_slist[targ].count >= 1);
    sm_anchor.used_total_space -= slen;
    sm_anchor.used_slist[targ].space -= slen;
    sm_anchor.used_slist[targ].count -= 1;
    if (sm_anchor.used_slist[targ].count == 0) {
        do_smmgr_used_slot_list_del(targ);
    }

    /* adjust used 01pct class */
    if (sm_anchor.used_total_space == 0) {
        assert(sm_anchor.used_01pct_space == slen);
        do_smmgr_01pct_last_clear();
    } else {
        if (targ >= sm_anchor.used_01pct_clsid) {
            sm_anchor.used_01pct_space -= slen;
            do_smmgr_01pct_check_and_move_left();
        } else {
            do_smmgr_01pct_check_and_move_right();
        }
    }
    //do_smmgr_used_blck_check();
}

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
ENGINE_ERROR_CODE slabs_init(struct default_engine *engine,
                             const size_t limit, const double factor, const bool prealloc)
{
    /* initialize global variables */
    config = &engine->config;
    slabsp = &engine->slabs;
    logger = engine->server.log->get_logger();

    slabsp->mem_limit = limit;
    slabsp->mem_reserved = (limit / 100) * RSVD_SLAB_RATIO;
    if (slabsp->mem_reserved < (RSVD_SLAB_COUNT*config->item_size_max))
        slabsp->mem_reserved = (RSVD_SLAB_COUNT*config->item_size_max);

    if (prealloc) {
        /* Allocate everything in a big chunk with malloc */
        slabsp->mem_base = malloc(slabsp->mem_limit);
        if (slabsp->mem_base != NULL) {
            slabsp->mem_current = slabsp->mem_base;
            slabsp->mem_avail = slabsp->mem_limit;
        } else {
            return ENGINE_ENOMEM;
        }
    } else {
        slabsp->mem_base = NULL;
        slabsp->mem_current = NULL;
        slabsp->mem_avail = 0;
    }

    /* initialize slab classes */
    slabclass_t *p;
    int i;
    unsigned int size;

    memset(slabsp->slabclass, 0, sizeof(slabsp->slabclass));

    size = sizeof(hash_item) + config->chunk_size;
    i = POWER_SMALLEST;
    while (i < POWER_LARGEST && size <= config->item_size_max / factor) {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }
        p = &slabsp->slabclass[i];
        p->size = size;
        p->perslab = config->item_size_max / p->size;
        p->rsvd_slabs = RSVD_SLAB_COUNT;
        if (config->verbose > 1) {
            fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                    i, p->size, p->perslab);
        }
        size *= factor;
        i += 1;
    }
    slabsp->power_largest = i;
    p = &slabsp->slabclass[i];
    p->size = config->item_size_max;
    p->perslab = 1;
    p->rsvd_slabs = RSVD_SLAB_COUNT;
    if (config->verbose > 1) {
        fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",
                i, p->size, p->perslab);
    }

    /* for the test suite:  faking of how much we've already malloc'd */
    {
        char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
        if (t_initial_malloc) {
            slabsp->mem_malloced = (size_t)atol(t_initial_malloc);
        }
    }
#ifndef DONT_PREALLOC_SLABS
    {
        char *pre_alloc = getenv("T_MEMD_SLABS_ALLOC");

        if (pre_alloc == NULL || atoi(pre_alloc) != 0) {
            slabs_preallocate(power_largest);
        }
    }
#endif

    if (do_smmgr_init() != 0) {
        if (slabsp->mem_base != NULL) {
            free(slabsp->mem_base);
            slabsp->mem_base = NULL;
        }
        return ENGINE_ENOMEM;
    }

    logger->log(EXTENSION_LOG_INFO, NULL, "SLABS module initialized.\n");
    return ENGINE_SUCCESS;
}

void slabs_final(struct default_engine *engine)
{
    if (slabsp == NULL) {
        return; /* nothing to do */
    }

    /* Free memory allocated. */
    if (slabsp->mem_base) {
        free(slabsp->mem_base);
    }
    do_smmgr_final();
    logger->log(EXTENSION_LOG_INFO, NULL, "SLABS module destroyed.\n");
}

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(const size_t size)
{
    int res = POWER_SMALLEST;

    if (size == 0)
        return 0;
    while (size > slabsp->slabclass[res].size)
        if (res++ == slabsp->power_largest)     /* won't fit in the biggest slab */
            return 0;
    return res;
}

unsigned int slabs_space_size(const size_t size)
{
    if (size <= MAX_SM_VALUE_LEN) {
        return do_smmgr_slen(size);
    }
    int clsid = slabs_clsid(size);
    if (clsid == 0)
        return 0;
    else
        return slabsp->slabclass[clsid].size;
}

int slabs_space_shortage_level(void)
{
    return sm_anchor.space_shortage_level;
}

static int grow_slab_list(const unsigned int id)
{
    slabclass_t *p = &slabsp->slabclass[id];
    if (p->slabs == p->list_size) {
        size_t new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));
        if (new_list == 0) return 0;
        p->list_size = new_size;
        p->slab_list = new_list;
    }
    return 1;
}

static void *memory_allocate(size_t size)
{
    void *ret;

    if (slabsp->mem_base == NULL) {
        /* We are not using a preallocated large memory chunk */
        ret = malloc(size);
    } else {
        ret = slabsp->mem_current;

        if (size > slabsp->mem_avail) {
            return NULL;
        }

        /* mem_current pointer _must_ be aligned!!! */
        if (size % CHUNK_ALIGN_BYTES) {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }

        slabsp->mem_current = ((char*)slabsp->mem_current) + size;
        if (size < slabsp->mem_avail) {
            slabsp->mem_avail -= size;
        } else {
            slabsp->mem_avail = 0;
        }
    }
    return ret;
}

static int do_slabs_newslab(const unsigned int id)
{
    slabclass_t *p = &slabsp->slabclass[id];
    int len = p->size * p->perslab;
    char *ptr;

    if ((slabsp->mem_limit && slabsp->mem_malloced + len > slabsp->mem_limit && p->slabs >= p->rsvd_slabs) ||
        (grow_slab_list(id) == 0) ||
        ((ptr = memory_allocate((size_t)len)) == 0)) {

        MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(id);
        return 0;
    }

    memset(ptr, 0, (size_t)len);
    p->end_page_ptr = ptr;
    p->end_page_free = p->perslab;

    p->slab_list[p->slabs++] = ptr;
    slabsp->mem_malloced += len;

    if (id == SM_SLAB_CLSID && p->rsvd_slabs > 0 && p->slabs > p->rsvd_slabs) {
        sm_anchor.free_limit_space += (p->perslab * p->size);
        sm_anchor.free_chunk_space += (p->perslab * p->size);
    }

    if ((slabsp->mem_limit <= slabsp->mem_malloced) ||
        ((slabsp->mem_limit - slabsp->mem_malloced) < slabsp->mem_reserved))
    {
        if (slabsp->slabclass[SM_SLAB_CLSID].rsvd_slabs == 0) { /* undefined */
            /* define the reserved slab count of slab class 0 */
            slabclass_t *z = &slabsp->slabclass[SM_SLAB_CLSID];
            unsigned int additional_slabs = (z->slabs/100) * RSVD_SLAB_RATIO;
            if (additional_slabs < RSVD_SLAB_COUNT)
                additional_slabs = RSVD_SLAB_COUNT;
            z->rsvd_slabs = z->slabs + additional_slabs;
            sm_anchor.free_limit_space = (additional_slabs * z->perslab) * SM_BLOCK_SIZE;
            sm_anchor.free_chunk_space = sm_anchor.free_limit_space
                                       + (z->sl_curr + z->end_page_free) * SM_BLOCK_SIZE;
        }
    }
    MEMCACHED_SLABS_SLABCLASS_ALLOCATE(id);

    return 1;
}

#ifndef DONT_PREALLOC_SLABS
static void slabs_preallocate(const unsigned int maxslabs)
{
    int i;
    unsigned int prealloc = 0;

    /* pre-allocate a 1MB slab in every size class so people don't get
       confused by non-intuitive "SERVER_ERROR out of memory"
       messages.  this is the most common question on the mailing
       list.  if you really don't want this, you can rebuild without
       these three lines.  */

    for (i = POWER_SMALLEST; i <= POWER_LARGEST; i++) {
        if (++prealloc > maxslabs)
            return;
        do_slabs_newslab(i);
    }

    /* slab class 0 is used for collection items and small-size kv items */
    do_slabs_newslab(0);
}
#endif

/*@null@*/
static void *do_slabs_alloc(const size_t size, unsigned int id)
{
    slabclass_t *p;
    void *ret = NULL;

    if (size <= MAX_SM_VALUE_LEN) {
        return do_smmgr_alloc(size);
    }

    p = &slabsp->slabclass[id];

#ifdef USE_SYSTEM_MALLOC
    if (slabsp->mem_limit && slabsp->mem_malloced + size > slabsp->mem_limit) {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
        return 0;
    }
    slabsp->mem_malloced += size;
    ret = malloc(size);
    MEMCACHED_SLABS_ALLOCATE(size, id, 0, ret);
    return ret;
#endif

    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    if (! (p->end_page_ptr != 0 || p->sl_curr != 0 ||
           do_slabs_newslab(id) != 0)) {
        /* We don't have more memory available */
        ret = NULL;
    } else if (p->sl_curr != 0) {
        /* return off our freelist */
        ret = p->slots[--p->sl_curr];
    } else {
        /* if we recently allocated a whole page, return from that */
        assert(p->end_page_ptr != NULL);
        ret = p->end_page_ptr;
        if (--p->end_page_free != 0) {
            p->end_page_ptr = ((caddr_t)p->end_page_ptr) + p->size;
        } else {
            p->end_page_ptr = 0;
        }
    }

    if (ret) {
        p->requested += size;
        MEMCACHED_SLABS_ALLOCATE(size, id, p->size, ret);
    } else {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
    }
    return ret;
}

static void do_slabs_free(void *ptr, const size_t size, unsigned int id)
{
    slabclass_t *p;

    if (size <= MAX_SM_VALUE_LEN) {
        do_smmgr_free(ptr, size);
        return;
    }

    MEMCACHED_SLABS_FREE(size, id, ptr);
    p = &slabsp->slabclass[id];

#ifdef USE_SYSTEM_MALLOC
    slabsp->mem_malloced -= size;
    free(ptr);
    return;
#endif

    if (p->sl_curr == p->sl_total) { /* need more space on the free list */
        int new_size = (p->sl_total != 0) ? p->sl_total * 2 : 16;  /* 16 is arbitrary */
        void **new_slots = realloc(p->slots, new_size * sizeof(void *));
        if (new_slots == 0)
            return;
        p->slots = new_slots;
        p->sl_total = new_size;
    }
    p->slots[p->sl_curr++] = ptr;
    p->requested -= size;
    return;
}

void add_statistics(const void *cookie, ADD_STAT add_stats,
                    const char* prefix, int num, const char *key,
                    const char *fmt, ...)
{
    char name[80], val[80];
    int klen = 0, vlen;
    va_list ap;

    assert(cookie);
    assert(add_stats);
    assert(key);

    va_start(ap, fmt);
    vlen = vsnprintf(val, sizeof(val) - 1, fmt, ap);
    va_end(ap);

    if (prefix != NULL) {
        klen = snprintf(name, sizeof(name), "%s:", prefix);
    }

    if (num != -1) {
        klen += snprintf(name + klen, sizeof(name) - klen, "%d:", num);
    }

    klen += snprintf(name + klen, sizeof(name) - klen, "%s", key);

    add_stats(name, klen, val, vlen, cookie);
}

/*@null@*/
static void do_slabs_stats(ADD_STAT add_stats, const void *cookie)
{
    int i, total;
    /* Get the per-thread stats which contain some interesting aggregates */
#ifdef FUTURE
    struct conn *conn = (struct conn*)cookie;
    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(c, &thread_stats);
#endif

    /* small memory classes */
    add_statistics(cookie, add_stats, "SM", -1, "used_num_classes", "%d", sm_anchor.used_num_classes);
    add_statistics(cookie, add_stats, "SM", -1, "free_num_classes", "%d", sm_anchor.free_num_classes);
    add_statistics(cookie, add_stats, "SM", -1, "used_min_classid", "%d", sm_anchor.used_minid);
    add_statistics(cookie, add_stats, "SM", -1, "used_max_classid", "%d", sm_anchor.used_maxid);
    add_statistics(cookie, add_stats, "SM", -1, "used_01pct_classid", "%d", sm_anchor.used_01pct_clsid);
    add_statistics(cookie, add_stats, "SM", -1, "free_min_classid", "%d", sm_anchor.free_minid);
    add_statistics(cookie, add_stats, "SM", -1, "free_max_classid", "%d", sm_anchor.free_maxid);
    add_statistics(cookie, add_stats, "SM", -1, "free_big_slot_space", "%"PRIu64, sm_anchor.free_slist[SM_NUM_CLASSES-1].space);
    add_statistics(cookie, add_stats, "SM", -1, "used_total_space", "%"PRIu64, sm_anchor.used_total_space);
    add_statistics(cookie, add_stats, "SM", -1, "used_01pct_space", "%"PRIu64, sm_anchor.used_01pct_space);
    add_statistics(cookie, add_stats, "SM", -1, "free_small_space", "%"PRIu64, sm_anchor.free_small_space);
    add_statistics(cookie, add_stats, "SM", -1, "free_avail_space", "%"PRIu64, sm_anchor.free_avail_space);
    add_statistics(cookie, add_stats, "SM", -1, "free_chunk_space", "%"PRIu64, sm_anchor.free_chunk_space);
    add_statistics(cookie, add_stats, "SM", -1, "free_limit_space", "%"PRIu64, sm_anchor.free_limit_space);
    add_statistics(cookie, add_stats, "SM", -1, "space_shortage_level", "%d", sm_anchor.space_shortage_level);

    total = 0;
    int min_slab_id = POWER_SMALLEST;
    min_slab_id = SM_SLAB_CLSID;
    for (i = min_slab_id; i <= slabsp->power_largest; i++) {
        slabclass_t *p = &slabsp->slabclass[i];
        if (p->slabs != 0) {
            uint32_t perslab, slabs;
            slabs = p->slabs;
            perslab = p->perslab;

            add_statistics(cookie, add_stats, NULL, i, "chunk_size", "%u", p->size);
            add_statistics(cookie, add_stats, NULL, i, "chunks_per_page", "%u", perslab);
            add_statistics(cookie, add_stats, NULL, i, "reserved_pages", "%u", p->rsvd_slabs);
            add_statistics(cookie, add_stats, NULL, i, "total_pages", "%u", slabs);
            add_statistics(cookie, add_stats, NULL, i, "total_chunks", "%u", slabs*perslab);
            add_statistics(cookie, add_stats, NULL, i, "used_chunks", "%u", (slabs*perslab)-p->sl_curr-p->end_page_free);
            add_statistics(cookie, add_stats, NULL, i, "free_chunks", "%u", p->sl_curr);
            add_statistics(cookie, add_stats, NULL, i, "free_chunks_end", "%u", p->end_page_free);
            add_statistics(cookie, add_stats, NULL, i, "mem_requested", "%llu", (unsigned long long)p->requested);
#ifdef FUTURE
            add_statistics(cookie, add_stats, NULL, i, "get_hits", "%"PRIu64, thread_stats.slab_stats[i].get_hits);
            add_statistics(cookie, add_stats, NULL, i, "cmd_set", "%"PRIu64, thread_stats.slab_stats[i].set_cmds);
            add_statistics(cookie, add_stats, NULL, i, "delete_hits", "%"PRIu64, thread_stats.slab_stats[i].delete_hits);
            add_statistics(cookie, add_stats, NULL, i, "cas_hits", "%"PRIu64, thread_stats.slab_stats[i].cas_hits);
            add_statistics(cookie, add_stats, NULL, i, "cas_badval", "%"PRIu64, thread_stats.slab_stats[i].cas_badval);
#endif
            total++;
        }
    }

    /* add overall slab stats and append terminator */
    add_statistics(cookie, add_stats, NULL, -1, "active_slabs", "%d", total);
    add_statistics(cookie, add_stats, NULL, -1, "memory_limit", "%llu", (unsigned long long)slabsp->mem_limit);
    add_statistics(cookie, add_stats, NULL, -1, "total_malloced", "%llu", (unsigned long long)slabsp->mem_malloced);
}

static ENGINE_ERROR_CODE do_slabs_set_memlimit(size_t memlimit)
{
    if (slabsp->mem_base != NULL) {
        /* We are using a preallocated large memory chunk */
        return ENGINE_EBADVALUE;
    }
    if (memlimit < (slabsp->mem_malloced + (slabsp->mem_malloced/10))) {
        /* We cannot set mem_limit smaller than (mem_malloced * 1.1) */
        return ENGINE_EBADVALUE;
    }

    size_t new_mem_reserved = (memlimit / 100) * RSVD_SLAB_RATIO;
    if (new_mem_reserved < (RSVD_SLAB_COUNT*config->item_size_max))
        new_mem_reserved = (RSVD_SLAB_COUNT*config->item_size_max);

    if (slabsp->slabclass[SM_SLAB_CLSID].rsvd_slabs != 0) {
        /* memlimit > slabsp->mem_malloced */
        if ((memlimit - slabsp->mem_malloced) < new_mem_reserved) {
            return ENGINE_EBADVALUE;
        }
    }
    slabsp->mem_limit = memlimit;
    slabsp->mem_reserved = new_mem_reserved;
    slabsp->slabclass[SM_SLAB_CLSID].rsvd_slabs = 0; /* undefined */
    sm_anchor.free_limit_space = 0;
    sm_anchor.free_chunk_space = 0;
    sm_anchor.space_shortage_level = 0;
    return ENGINE_SUCCESS;
}

void *slabs_alloc(size_t size, unsigned int id)
{
    void *ret;

    if (id < POWER_SMALLEST || id > slabsp->power_largest)
        return NULL;
    pthread_mutex_lock(&slabsp->lock);
    ret = do_slabs_alloc(size, id);
    pthread_mutex_unlock(&slabsp->lock);
    return ret;
}

void slabs_free(void *ptr, size_t size, unsigned int id)
{
    if (id < POWER_SMALLEST || id > slabsp->power_largest)
        return;
    pthread_mutex_lock(&slabsp->lock);
    do_slabs_free(ptr, size, id);
    pthread_mutex_unlock(&slabsp->lock);
}

void slabs_stats(ADD_STAT add_stats, const void *c)
{
    pthread_mutex_lock(&slabsp->lock);
    do_slabs_stats(add_stats, c);
    pthread_mutex_unlock(&slabsp->lock);
}

void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal)
{
    slabclass_t *p;

    if (id < POWER_SMALLEST || id > slabsp->power_largest)
        return;
    pthread_mutex_lock(&slabsp->lock);
    p = &slabsp->slabclass[id];
    p->requested = p->requested - old + ntotal;
    pthread_mutex_unlock(&slabsp->lock);
}

ENGINE_ERROR_CODE slabs_set_memlimit(size_t memlimit)
{
    ENGINE_ERROR_CODE ret;
    pthread_mutex_lock(&slabsp->lock);
    ret = do_slabs_set_memlimit(memlimit);
    pthread_mutex_unlock(&slabsp->lock);
    return ret;
}

