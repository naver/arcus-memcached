/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
/*
 * Page Buffer
 *
 */
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/mman.h>

#include "squall_engine.h"
#include "sq_buffer.h"
#include "sq_disk.h"
#include "sq_prefix.h"
#include "sq_log.h"

#define BUF_USE_PAGE_LOCK       1
#define FLUSH_ALL_MULTI_THREADS 1

#define BUF_MIN_PAGE_COUNT      1024
#define BUF_MAX_REF_4_WARM      5
#define BUF_FCB_QUEUE_COUNT     1     /* # of FCB QUEUEs */
#define BUF_FCB_QUEUE_SIZE      256   /* FCB QUEUE SIZE */
#define BUF_FLUSH_PAGE_COUNT    16

#ifdef FLUSH_ALL_MULTI_THREADS
#define BUF_MAX_IO_THREAD       32
#endif


//#define ENABLE_ORDERED_DIRTY_PAGE_LIST

/*
 * Internal Data Structures
 */

/* 1 byte reserve table */
static const unsigned char BitReverseTable256[] =
{
  0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0,
  0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8,
  0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
  0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC,
  0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2,
  0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
  0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6,
  0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
  0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
  0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9,
  0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
  0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
  0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3,
  0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
  0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
  0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF
};

/* buffer control block(BCB) zone */
enum bcb_zone {
    BCB_ZONE_FREE = 0,     /* free zone */
    BCB_ZONE_VOID,         /* don't exist in any list (free, lru, perm) */
    BCB_ZONE_COLD_CLEAN,   /* cold clean lru zone */
    BCB_ZONE_COLD_DIRTY,   /* cold dirty lru zone */
    BCB_ZONE_MIDDLE,       /* middle lru zone (not warm and not cold) */
    BCB_ZONE_WARM,         /* warm lru zone */
    BCB_ZONE_PERM          /* permanent zone */
};

/* BCB pin & latch type */
enum buf_pin_latch_type {
    BUF_PIN_LATCH = 1,
    BUF_LATCH_ONLY
};

struct buf_hash;

/* buffer latch wait entry */
typedef struct buf_lawt {
    pthread_cond_t      cond;
    uint8_t             r_mode;     /* request latch mode */
    uint8_t             r_type;     /* request type */
    struct buf_lawt    *w_next;     /* the next wait thread */
} buf_LAWT;

/* buffer page lock entry */
typedef struct buf_pglk {
    pthread_cond_t      cond;
    PageID              pgid;
    uint16_t            wcnt;       /* lock wait thread count */
    uint8_t             lked;       /* true(locked) or false(unlocked) */
    struct buf_pglk    *l_next;     /* the next page lock */
} buf_PGLK;

/* buffer control block(BCB): memory structure */
typedef struct buf_bcb {
    struct buf_bcb     *b_prev;     /* prev BCB */
    struct buf_bcb     *b_next;     /* next BCB */
#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
    struct buf_bcb     *d_prev;     /* prev BCB at dirty page list */
    struct buf_bcb     *d_next;     /* next BCB at dirty page list */
#endif
    struct buf_hash    *h_root;     /* hash root (hash entry itself) */
    struct buf_bcb     *h_next;     /* hash next */
    PageID              pgid;       /* page id */
    LogSN               oldest_lsn; /* the oldest page lsn */
    uint16_t            fcnt;       /* fix count */
    uint16_t            pcnt;       /* pin count (pin only count among fix count) */
    uint8_t             mode;       /* buffer latch mode */
    uint8_t             zone;       /* bcb zone: warm/cold(clean or dirty)/void/free */
    uint8_t             rcnt;       /* reference count: to check if bcb must be moved to warm lru zone */
    uint8_t             dirty;      /* is dirty now ? true or false */
    uint8_t             flush;      /* is flushing now ? true or false */
    uint8_t             victim;     /* is victim bcb ? true or false */
    uint16_t            wcnt;       /* size of wait thread list */
    buf_LAWT           *w_head;     /* head of wait thread list */
    buf_LAWT           *w_tail;     /* tail of wait thread list */
    buf_LAWT           *w_flsh;     /* flush wait list */
    Page               *pgptr;      /* page address */
} buf_BCB;

/* buffer hash table entry */
typedef struct buf_hash {
    /* pthread_spinlock_t lock; */
    pthread_mutex_t     lock;
    buf_BCB            *h_next;     /* hash next */
    buf_PGLK           *l_next;     /* lock next */
    uint32_t            h_leng;     /* hash chain length */
    uint32_t            l_leng;     /* lock chain length */
} buf_HASH;

/* buffer LRU list */
typedef struct buf_lru_list {
    pthread_mutex_t     lock;
    buf_BCB            *head;       /* head of LRU list (head of warm LRU zone) */
    buf_BCB            *middle;     /* head of middle LRU zone */
    buf_BCB            *cold_dirty; /* head of cold dirty LRU zone */
    buf_BCB            *cold_clean; /* head of cold clean LRU zone */
    buf_BCB            *tail;       /* tail of LRU list */
    uint32_t            nwarm;      /* # of bcbs in warm lru zone */
    uint32_t            nmiddle;    /* # of bcbs in middle lru zone */
    uint32_t            ndirty;     /* # of bcbs in cold dirty lru zone */
    uint32_t            nclean;     /* # of bcbs in cold clean lru zone */
    uint32_t            wlimit;     /* warm limit: max # of bcbs in warm zone */
    uint32_t            mlimit;     /* middle limit: max # of bcbs in middle zone */
} buf_LRU_LIST;

/* buffer free list */
typedef struct buf_free_list {
    pthread_mutex_t     lock;
    buf_BCB            *head;       /* free BCB list */
    uint32_t            count;      /* # of free BCBs */
} buf_FREE_LIST;

/* buffer oldest dirty list : NOT USED */
typedef struct buf_drty_list {
    pthread_mutex_t     lock;
    buf_BCB            *head;       /* head of dirty BCB list */
    buf_BCB            *tail;       /* tail of dirty BCC list */
    uint32_t            dirty_cnt;  /* # of dirty BCBs */
    uint32_t            histo_cnt;
    uint32_t            histo_res[8];
} buf_DRTY_LIST;

typedef struct buf_perm_list {
    pthread_mutex_t     lock;
    buf_BCB            *head;       /* head of permanent BCB list */
    buf_BCB            *tail;       /* tail of permanent BCB list */
    uint32_t            ccnt;       /* current BCB count */
    uint32_t            mcnt;       /* maximum BCB count */
} buf_PERM_LIST;

/* buf latch wait entry list */
typedef struct buf_lawt_list {
    pthread_mutex_t     lock;
    buf_LAWT           *head;       /* free lawt entry list */
    uint32_t            count;      /* # of available lawt entries */
} buf_LAWT_LIST;

/* buf page lock entry list */
typedef struct buf_pglk_list {
    pthread_mutex_t     lock;
    buf_PGLK           *head;       /* free pglk entry list */
    uint32_t            count;      /* # of available pglk entries */
} buf_PGLK_LIST;

/* flush control block(FCB) */
typedef struct buf_fcb {
    buf_HASH           *hash;
    buf_BCB            *bcb;
    Page               *pgptr;
    PageID              pgid;
    struct aiocb        iocb;       /* structure for POSIX AIO */
} buf_FCB;

/* buffer (circular) FCB queue */
typedef struct buf_fcb_queue {
    buf_FCB             list[BUF_FCB_QUEUE_SIZE]; /* FCB queue */
    uint32_t            fcnt;       /* # of free FCBs */
    uint32_t            head;       /* head pointer */
    uint32_t            tail;       /* tail pointer */
    uint32_t            prep;       /* prepared pointer */
    struct buf_fcb_queue *next;
} buf_FCB_QUEUE;

/* page flush buffer list */
typedef struct buf_fcbq_list {
    pthread_mutex_t     lock;
    buf_FCB_QUEUE      *head;       /* free page buffer entry list */
    uint32_t            count;      /* # of available page buffer entries */
} buf_FCBQ_LIST;

/* page flush buffer entry */
typedef struct buf_pbuf {
    Page              *buff;
    struct buf_pbuf   *next;
} buf_PBUF;

/* page flush buffer list */
typedef struct buf_pbuf_list {
    pthread_mutex_t     lock;
    buf_PBUF           *head;       /* free page buffer entry list */
    uint32_t            count;      /* # of available page buffer entries */
} buf_PBUF_LIST;

/* buffer flusher */
typedef struct buf_flusher {
    pthread_mutex_t     lock;
    pthread_cond_t      cond;
    bool                sleep;
    bool                start;
    bool                init;       /* start or stop request */
} buf_FLUSHER;

/* buffer stats */
typedef struct buf_stat_wrap {
    pthread_mutex_t     lock;
    struct buffer_stat  stat;
    bool                enable;         /* true or false */
} buf_STAT_WRAP;

/* page buffer global structure */
struct buffer_global {
    buf_HASH           *hash_tab;       /* buffer hash table */
    Page               *page_tab;       /* page table: each page entry is mapped to each bcb entry */
    buf_BCB            *bcb_tab;        /* BCB table */
    buf_LAWT           *lawt_tab;       /* buffer lawt(latch wait) entry array */
    buf_PGLK           *pglk_tab;       /* buffer lglk(page lock) entry array */
    Page               *page_buf;       /* page buffer: used to flush page images */
    buf_FCB_QUEUE      *fcbq_tab;       /* FCB queue table */
    buf_PBUF           *pbuf_tab;       /* page flush buffer entry array */
    buf_LRU_LIST        lru_list;       /* lru list */
    buf_FREE_LIST       free_list;      /* free BCB list */
    buf_DRTY_LIST       drty_list;      /* NOT USED */
    buf_PERM_LIST       perm_list;      /* permanent BCB list */
    buf_LAWT_LIST       lawt_list;      /* free lawt(latch wait) entry list */
    buf_PGLK_LIST       pglk_list;      /* free pglk(page lock) entry list */
    buf_FCBQ_LIST       fcbq_list;      /* FCB queue list */
    buf_PBUF_LIST       pbuf_list;      /* page flush buffer entry list */
    buf_FLUSHER         pgflusher;      /* page flusher */
    buf_STAT_WRAP       buf_stat;       /* buffer statistics */
    void               *zero_filled_page; /* for checking zero filled page */
    uint32_t            ini_nbuffer;    /* initial # of BCBs */
    uint32_t            max_nbuffer;    /* maximum # of BCBs */
    uint32_t            hash_size;      /* hash table size */
    uint32_t            max_pbufs;      /* # of page flush buffers */
    uint32_t            pbuf_size;      /* page flush buffer size */
    uint32_t            lawt_size;      /* lawt table size */
    uint32_t            pglk_size;      /* pglk table size */
    uint32_t            page_size;      /* data page size */
    uint32_t            HWM_clean;      /* HWM on the # of (cold clean + free) BCBs */
    uint32_t            LWM_clean;      /* LWM on the # of (cold clean + free) BCBs */
    uint32_t            pgnum_nbits;    /* # of bits for presenting pgnum */
    uint32_t            flushall_hval;
    pthread_mutex_t     flushall_lock;
    LogSN               log_amount_lsn;
    volatile bool       chkpt_flush;    /* is chkpt flush doing now ? */
    volatile bool       initialized;    /* is buffer initialized ? */
};

static struct config    *config = NULL;
static SERVER_HANDLE_V1 *server = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct buffer_global buf_gl;

/*
 * Internal Functions
 */

/* Page Buffer Stats Functions */

static void do_buf_stat_clear()
{
    buf_gl.buf_stat.stat.total_fix_count = 0;
    buf_gl.buf_stat.stat.total_hit_count = 0;
    buf_gl.buf_stat.stat.fixed_buffer_count = 0;
    buf_gl.buf_stat.stat.buffer_replace_count = 0;
    buf_gl.buf_stat.stat.disk_read_count = 0;
    buf_gl.buf_stat.stat.disk_write_count = 0;
}

static void do_buf_stat_init()
{
    pthread_mutex_init(&buf_gl.buf_stat.lock, NULL);
    do_buf_stat_clear();
    buf_gl.buf_stat.enable = true;
}

static void do_buf_stat_fix(bool is_buf_hit, bool is_first_fix)
{
    pthread_mutex_lock(&buf_gl.buf_stat.lock);
    buf_gl.buf_stat.stat.total_fix_count++;
    if (is_buf_hit) buf_gl.buf_stat.stat.total_hit_count++;
    if (is_first_fix) buf_gl.buf_stat.stat.fixed_buffer_count++;
    pthread_mutex_unlock(&buf_gl.buf_stat.lock);
}

static void do_buf_stat_unfix(bool is_last_unfix)
{
    if (is_last_unfix) {
        pthread_mutex_lock(&buf_gl.buf_stat.lock);
        buf_gl.buf_stat.stat.fixed_buffer_count--;
        pthread_mutex_unlock(&buf_gl.buf_stat.lock);
    }
}

static void do_buf_stat_replacement(void)
{
    pthread_mutex_lock(&buf_gl.buf_stat.lock);
    buf_gl.buf_stat.stat.buffer_replace_count++;
    pthread_mutex_unlock(&buf_gl.buf_stat.lock);
}

static void do_buf_stat_disk_read(void)
{
    pthread_mutex_lock(&buf_gl.buf_stat.lock);
    buf_gl.buf_stat.stat.disk_read_count++;
    pthread_mutex_unlock(&buf_gl.buf_stat.lock);
}

static void do_buf_stat_disk_write(void)
{
    pthread_mutex_lock(&buf_gl.buf_stat.lock);
    buf_gl.buf_stat.stat.disk_write_count++;
    pthread_mutex_unlock(&buf_gl.buf_stat.lock);
}

static void do_buf_page_flusher_sleep(void)
{
    buf_FLUSHER *pgflusher = &buf_gl.pgflusher;
    struct timeval  tv;
    struct timespec to;

    /* 50 ms sleep */
    gettimeofday(&tv, NULL);
    if ((tv.tv_usec + 50000) < 1000000) {
        tv.tv_usec += 50000;
        to.tv_sec  = tv.tv_sec;
        to.tv_nsec = tv.tv_usec * 1000;
    } else {
        to.tv_sec  = tv.tv_sec + 1;
        to.tv_nsec = 0;
    }

    pthread_mutex_lock(&pgflusher->lock);
    pgflusher->sleep = true;
    pthread_cond_timedwait(&pgflusher->cond, &pgflusher->lock, &to);
    pgflusher->sleep = false;
    pthread_mutex_unlock(&pgflusher->lock);
}

static void do_buf_page_flusher_wakeup(void)
{
    buf_FLUSHER *pgflusher = &buf_gl.pgflusher;

    /* wakeup the page flusher */
    pthread_mutex_lock(&pgflusher->lock);
    if (pgflusher->sleep == true)
        pthread_cond_signal(&pgflusher->cond);
    pthread_mutex_unlock(&pgflusher->lock);
}

static inline uint32_t do_buf_pgid_hash(const PageID pgid)
{
    /* hash value : <reversed pfxid, flnum, pgnum, ftype> */
    return ((uint32_t)BitReverseTable256[pgid.pfxid & 0x00FF] << 24 |
            (uint32_t)pgid.pfxid >> 14 |
            (uint32_t)pgid.flnum << (buf_gl.pgnum_nbits+2) |
            pgid.pgnum << 2) % buf_gl.hash_size;
    //return (((uint32_t)pgid.pfxid << 16 | (uint32_t)pgid.flnum << 24 | pgid.pgnum) % buf_gl.hash_size);
    //return (((uint32_t)pgid.pfxid << 16 | (uint32_t)pgid.flnum | pgid.pgnum) % buf_gl.hash_size);
    //return (((uint32_t)pgid.pfxid | (uint32_t)pgid.flnum << 16 | pgid.pgnum) % buf_gl.hash_size);
}

static inline buf_BCB *do_buf_find_bcb(buf_HASH *hash, PageID pgid)
{
    buf_BCB *bcb = hash->h_next;
    while (bcb != NULL) {
        if (PAGEID_IS_EQ(&bcb->pgid, &pgid)) break;
        bcb = bcb->h_next;
    }
    return bcb;
}

static inline buf_BCB *do_buf_get_bcb(Page *pgptr)
{
    return &buf_gl.bcb_tab[((char*)pgptr - (char*)buf_gl.page_tab) / buf_gl.page_size];
}

/* FCB queue alloc & free */
static buf_FCB_QUEUE *do_buf_alloc_fcb_queue()
{
    buf_FCB_QUEUE *fcbq = NULL;
    pthread_mutex_lock(&buf_gl.fcbq_list.lock);
    if (buf_gl.fcbq_list.head != NULL) {
        fcbq = buf_gl.fcbq_list.head;
        buf_gl.fcbq_list.head = fcbq->next;
        buf_gl.fcbq_list.count -= 1;
    }
    pthread_mutex_unlock(&buf_gl.fcbq_list.lock);
    /* FCB queues must be prepared in advance */
    assert(fcbq != NULL);
    return fcbq;
}

static void do_buf_free_fcb_queue(buf_FCB_QUEUE *fcbq)
{
    pthread_mutex_lock(&buf_gl.fcbq_list.lock);
    fcbq->next = buf_gl.fcbq_list.head;
    buf_gl.fcbq_list.head = fcbq;
    buf_gl.fcbq_list.count += 1;
    pthread_mutex_unlock(&buf_gl.fcbq_list.lock);
}

/* page flush buffer alloc & free */
static buf_PBUF *do_buf_alloc_page_buffer()
{
    buf_PBUF *pbuf = NULL;
    pthread_mutex_lock(&buf_gl.pbuf_list.lock);
    if (buf_gl.pbuf_list.head != NULL) {
        pbuf = buf_gl.pbuf_list.head;
        buf_gl.pbuf_list.head = pbuf->next;
        buf_gl.pbuf_list.count -= 1;
    }
    pthread_mutex_unlock(&buf_gl.pbuf_list.lock);
    /**** This case must not be occurred ****
    if (pbuf == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] pbuf alloc fail\n");
    }
    *****************************************/
    /* [NOTE] page buffer allocattion for page flush ****
     * Enough page buffers must be prepared in advance while considering # of work threads.
     * So, a page buffer can always be allocated. If not, it's a severe bug in somewhere.
     ****************************************************/
    assert(pbuf != NULL);
    return pbuf;
}

static void do_buf_free_page_buffer(buf_PBUF *pbuf)
{
    pthread_mutex_lock(&buf_gl.pbuf_list.lock);
    pbuf->next = buf_gl.pbuf_list.head;
    buf_gl.pbuf_list.head = pbuf;
    buf_gl.pbuf_list.count += 1;
    pthread_mutex_unlock(&buf_gl.pbuf_list.lock);
}

/* latch wait entry alloc & free */

static buf_LAWT *do_buf_alloc_lawt_entry()
{
    buf_LAWT *lawt = NULL;
    pthread_mutex_lock(&buf_gl.lawt_list.lock);
    if (buf_gl.lawt_list.head != NULL) {
        lawt = buf_gl.lawt_list.head;
        buf_gl.lawt_list.head = lawt->w_next;
        buf_gl.lawt_list.count -= 1;
    }
    pthread_mutex_unlock(&buf_gl.lawt_list.lock);
    /**** This case must not be occurred ****
    if (lawt == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] lawt alloc fail\n");
        lawt = (buf_LAWT*)malloc(sizeof(buf_LAWT));
    }
    *****************************************/
    /* [NOTE] latch wait entry allocattion ***
     * Enough latch wait entries must be prepared in advance while considering # of work threads.
     * So, a latch wait entry can always be allocated. If not, it's a severe bug in somewhere.
     *****************************************/
    assert(lawt != NULL);
    return lawt;
}

static void do_buf_free_lawt_entry(buf_LAWT *lawt)
{
    pthread_mutex_lock(&buf_gl.lawt_list.lock);
    lawt->w_next = buf_gl.lawt_list.head;
    buf_gl.lawt_list.head = lawt;
    buf_gl.lawt_list.count += 1;
    pthread_mutex_unlock(&buf_gl.lawt_list.lock);
}

/* page lock entry alloc & free */

static buf_PGLK *do_buf_alloc_pglk_entry()
{
    buf_PGLK *pglk = NULL;
    pthread_mutex_lock(&buf_gl.pglk_list.lock);
    if (buf_gl.pglk_list.head != NULL) {
        pglk = buf_gl.pglk_list.head;
        buf_gl.pglk_list.head = pglk->l_next;
        buf_gl.pglk_list.count -= 1;
    }
    pthread_mutex_unlock(&buf_gl.pglk_list.lock);
    /**** This case must not be occurred ****
    if (pglk == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] pglk alloc fail\n");
        pglk = (buf_PGLK*)malloc(sizeof(buf_PGLK));
    }
    *****************************************/
    /* [NOTE] page lock entry allocattion ***
     * Enough page lock entries must be prepared in advance while considering # of work threads.
     * So, a page lock entry can always be allocated. If not, it's a severe bug in somewhere.
     *****************************************/
    assert(pglk != NULL);
    return pglk;
}

static void do_buf_free_pglk_entry(buf_PGLK *pglk)
{
    pthread_mutex_lock(&buf_gl.pglk_list.lock);
    pglk->l_next = buf_gl.pglk_list.head;
    buf_gl.pglk_list.head = pglk;
    buf_gl.pglk_list.count += 1;
    pthread_mutex_unlock(&buf_gl.pglk_list.lock);
}

/* Buffer Page lock & unlock */

static void do_buf_lock_page(buf_HASH *hash, const PageID pgid, bool *holder)
{
    buf_PGLK *pglk = hash->l_next;
    while (pglk != NULL) {
        if (PAGEID_IS_EQ(&pglk->pgid, &pgid)) break;
        pglk = pglk->l_next;
    }
    if (pglk == NULL) {
        pglk = do_buf_alloc_pglk_entry();
        pglk->pgid = pgid;
        pglk->lked = true; /* locked */
        pglk->wcnt = 0;
        pglk->l_next = hash->l_next;
        hash->l_next = pglk;
        hash->l_leng += 1;
        *holder = true; /* hold the page lock */
    } else {
        pglk->wcnt += 1;
        while (pglk->lked == true)
            pthread_cond_wait(&pglk->cond, &hash->lock);
        pglk->wcnt -= 1;
        if (pglk->wcnt == 0)
            do_buf_free_pglk_entry(pglk);
        *holder = false; /* doesn't hold the page lock */
    }
}

static void do_buf_unlock_page(buf_HASH *hash, const PageID pgid)
{
    buf_PGLK *pglk, *prev=NULL;

    pglk = hash->l_next;
    while (pglk != NULL) {
        if (PAGEID_IS_EQ(&pglk->pgid, &pgid)) break;
        prev = pglk; pglk = pglk->l_next;
    }
    if (pglk != NULL) {
        if (prev == NULL) hash->l_next = pglk->l_next;
        else              prev->l_next = pglk->l_next;
        hash->l_leng -= 1;

        pglk->lked = false; /* unlocked */
        if (pglk->wcnt > 0) {
            pthread_cond_broadcast(&pglk->cond);
        } else {
            do_buf_free_pglk_entry(pglk);
        }
    } else {
        fprintf(stderr, "SEVERE ERROR\n");
    }
}

/* Permanent BCB list management */

static bool do_buf_perm_list_isfull(void)
{
    /* return just hint info without holding a lock */
    if (buf_gl.perm_list.ccnt >= buf_gl.perm_list.mcnt)
        return true;
    else
        return false;
}

static void do_buf_link_to_perm_list(buf_BCB *bcb)
{
    buf_PERM_LIST *perm = &buf_gl.perm_list;

    bcb->zone = BCB_ZONE_PERM;
    pthread_mutex_lock(&perm->lock);
    if (perm->head == NULL) {
        assert(perm->tail == NULL);
        bcb->b_prev = bcb->b_next = NULL;
        perm->head = perm->tail = bcb;
    } else {
        bcb->b_prev = NULL;
        bcb->b_next = perm->head;
        perm->head->b_prev = bcb;
        perm->head = bcb;
    }
    perm->ccnt += 1;
    pthread_mutex_unlock(&perm->lock);
}

static void do_buf_unlink_from_perm_list(buf_BCB *bcb)
{
    buf_PERM_LIST *perm = &buf_gl.perm_list;

    pthread_mutex_lock(&perm->lock);
    if (bcb->b_prev == NULL) {
        assert(perm->head == bcb);
        perm->head = bcb->b_next;
    } else {
        bcb->b_prev->b_next = bcb->b_next;
    }
    if (bcb->b_next == NULL) {
        assert(perm->tail == bcb);
        perm->tail = bcb->b_prev;
    } else {
        bcb->b_next->b_prev = bcb->b_prev;
    }
    perm->ccnt -= 1;
    pthread_mutex_unlock(&perm->lock);
    bcb->zone = BCB_ZONE_VOID;
}

/* HASH link, unlink operation */
static inline void do_buf_link_to_hash(buf_HASH *hash, buf_BCB *bcb)
{
    /* link bcb to the hash chain */
    bcb->h_root = (void*)hash;
    bcb->h_next = hash->h_next;
    hash->h_next = bcb;
    hash->h_leng += 1;
}

static inline void do_buf_unlink_from_hash(buf_HASH *hash, buf_BCB *prev, buf_BCB *bcb)
{
    /* unlink bcb from the hash chain */
    if (prev == NULL) hash->h_next = bcb->h_next;
    else              prev->h_next = bcb->h_next;
    hash->h_leng -= 1;
    bcb->h_root = NULL;
    bcb->h_next = NULL;
}

/* LRU link, unlink, and relocate operation */

static inline void do_buf_link_to_lru(buf_BCB *bcb)
{
    buf_LRU_LIST *lru = &buf_gl.lru_list;

    /* The caller is holding both hash lock and lru lock */
    if (bcb->victim == true) {
        /* do not link the victim bcb to the lru list */
        return;
    }
    do {
        if (lru->nwarm < lru->wlimit || bcb->rcnt >= BUF_MAX_REF_4_WARM) {
            /* link to LRU warm zone */
            bcb->zone = BCB_ZONE_WARM;
            bcb->rcnt = 0;
            bcb->b_prev = NULL;
            bcb->b_next = lru->head;
            lru->head = bcb;
            if (lru->tail == NULL) lru->tail = bcb;
            if (lru->nwarm < lru->wlimit) {
                lru->nwarm += 1; break;
            }
            lru->middle = (lru->middle != NULL ? lru->middle->b_prev : lru->tail);
            lru->middle->zone = BCB_ZONE_MIDDLE;
        } else {
            /* link to LRU middle zone */
            if (lru->middle != NULL) {
                bcb->b_prev = lru->middle->b_prev;
                bcb->b_next = lru->middle;
            } else {
                bcb->b_prev = lru->tail;
                bcb->b_next = NULL;
                lru->tail = bcb;
            }
            lru->middle = bcb;
            bcb->zone = BCB_ZONE_MIDDLE;
        }
        if (lru->nmiddle < lru->mlimit) {
            lru->nmiddle += 1; break;
        }
        if (lru->cold_dirty != NULL) {
            /*** LINK_BCB_TO_COLD_DIRTY_OR_CLEAN_ZONE ****
             * Is it better to have below logic ?
             *********************************************
            buf_BCB *mov_bcb = lru->cold_dirty->b_prev;
            if (lru->cold_clean == NULL || mov_bcb->dirty) {
                mov_bcb->zone = BCB_ZONE_COLD_DIRTY;
                lru->cold_dirty = mov_bcb; lru->ndirty += 1;
            } else {
                mov_bcb->b_prev->b_next = mov_bcb->b_next;
                mov_bcb->b_next->b_prev = mov_bcb->b_prev;
                mov_bcb->b_prev = lru->cold_clean->b_prev;
                mov_bcb->b_next = lru->cold_clean;
                mov_bcb->b_prev->b_next = mov_bcb;
                mov_bcb->b_next->b_prev = mov_bcb;
                mov_bcb->zone = BCB_ZONE_COLD_CLEAN;
                lru->cold_clean = mov_bcb; lru->nclean += 1;
            }
            ************************************************/
            lru->cold_dirty = lru->cold_dirty->b_prev;
            lru->cold_dirty->zone = BCB_ZONE_COLD_DIRTY;
            lru->ndirty += 1;
        } else {
            buf_BCB *mov_bcb = (lru->cold_clean != NULL ?
                                lru->cold_clean->b_prev : lru->tail);
            if (mov_bcb->dirty) {
                mov_bcb->zone = BCB_ZONE_COLD_DIRTY;
                lru->cold_dirty = mov_bcb; lru->ndirty += 1;
            } else {
                mov_bcb->zone = BCB_ZONE_COLD_CLEAN;
                lru->cold_clean = mov_bcb; lru->nclean += 1;
            }
        }
    } while(0);
    if (bcb->b_prev != NULL) bcb->b_prev->b_next = bcb;
    if (bcb->b_next != NULL) bcb->b_next->b_prev = bcb;
}

static inline void do_buf_unlink_from_lru(buf_BCB *bcb)
{
    buf_LRU_LIST *lru = &buf_gl.lru_list;

    if (bcb->b_prev == NULL) {
        assert(lru->head == bcb);
        lru->head = bcb->b_next;
    } else {
        bcb->b_prev->b_next = bcb->b_next;
    }
    if (bcb->b_next == NULL) {
        assert(lru->tail == bcb);
        lru->tail = bcb->b_prev;
    } else {
        bcb->b_next->b_prev = bcb->b_prev;
    }
    do {
        if (bcb->zone == BCB_ZONE_COLD_CLEAN) {
            if (lru->cold_clean == bcb)
                lru->cold_clean = bcb->b_next;
            lru->nclean -= 1; break;
        }
        if (bcb->zone == BCB_ZONE_COLD_DIRTY) {
            if (lru->cold_dirty == bcb) {
                lru->cold_dirty = bcb->b_next;
                if (lru->cold_dirty == lru->cold_clean)
                    lru->cold_dirty = NULL;
            }
            lru->ndirty -= 1; break;
        }
        if (bcb->zone == BCB_ZONE_MIDDLE) {
            if (lru->middle == bcb) {
                lru->middle = lru->middle->b_next;
            }
        } else {
            assert(bcb->zone == BCB_ZONE_WARM);
            if (lru->nwarm > (lru->wlimit/2) || lru->middle == NULL) {
                lru->nwarm -= 1; break;
            }
            lru->middle->zone = BCB_ZONE_WARM;
            lru->middle->rcnt = 0;
            lru->middle = lru->middle->b_next;
        }
        if (lru->nmiddle > (lru->mlimit/2) || (lru->cold_dirty == NULL && lru->cold_clean == NULL)) {
            lru->nmiddle -= 1; break;
        }
        if (lru->cold_dirty != NULL) {
            lru->cold_dirty->zone = BCB_ZONE_MIDDLE;
            lru->cold_dirty = lru->cold_dirty->b_next;
            if (lru->cold_dirty == lru->cold_clean)
                lru->cold_dirty = NULL;
            lru->ndirty -= 1;
        } else { /* lru->cold_clean != NULL */
            lru->cold_clean->zone = BCB_ZONE_MIDDLE;
            lru->cold_clean = lru->cold_clean->b_next;
            lru->nclean -= 1;
        }
    } while(0);
    //bcb->zone = BCB_ZONE_VOID;
}

static void do_buf_relocate_at_lru(buf_BCB *bcb)
{
    if (bcb->zone == BCB_ZONE_VOID) {
        pthread_mutex_lock(&buf_gl.lru_list.lock);
        assert(bcb->zone == BCB_ZONE_VOID);
        do_buf_link_to_lru(bcb);
        pthread_mutex_unlock(&buf_gl.lru_list.lock);
    } else { /* BCB_ZONE_MIDDLE | BCB_ZONE_COLD_DIRTY | BCB_ZONE_COLD_CLEAN */
        if (bcb->rcnt >= BUF_MAX_REF_4_WARM ||
            bcb->zone == BCB_ZONE_COLD_DIRTY || bcb->zone == BCB_ZONE_COLD_CLEAN) {
            pthread_mutex_lock(&buf_gl.lru_list.lock);
            if (bcb->zone != BCB_ZONE_VOID)
                do_buf_unlink_from_lru(bcb);
            do_buf_link_to_lru(bcb);
            pthread_mutex_unlock(&buf_gl.lru_list.lock);
        }
    }
}

#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
static inline void do_buf_link_to_dpl(buf_BCB *bcb)
{
    buf_DRTY_LIST *dpl = &buf_gl.drty_list;
    int search_depth = 0;

    /* The caller is holding hash lock */
    pthread_mutex_lock(&dpl->lock);
    if (dpl->head == NULL) {
        bcb->d_prev = NULL;
        bcb->d_next = NULL;
        dpl->head = bcb;
        dpl->tail = bcb;
    } else {
        if (LOGSN_IS_LT(&bcb->oldest_lsn, &dpl->tail->oldest_lsn)) {
            buf_BCB *prv = dpl->tail->d_prev;
            search_depth += 1;
            while (prv != NULL && LOGSN_IS_LT(&bcb->oldest_lsn, &prv->oldest_lsn)) {
                prv = prv->d_prev;
                search_depth += 1;
            }
            if (prv == NULL) {
                /* link to the head */
                bcb->d_prev = NULL;
                bcb->d_next = dpl->head;
                dpl->head->d_prev = bcb;
                dpl->head = bcb;
            } else {
                /* link to the middle */
                assert(prv->d_next != NULL);
                bcb->d_prev = prv;
                bcb->d_next = prv->d_next;
                prv->d_next->d_prev = bcb;
                prv->d_next = bcb;
            }
        } else {
            /* link to the tail */
            bcb->d_prev = dpl->tail;
            bcb->d_next = NULL;
            dpl->tail->d_next = bcb;
            dpl->tail = bcb;
        }
    }
    dpl->dirty_cnt += 1;
    /** Use below code later if needed ****
    if (search_depth > 7) search_depth = 7;
    dpl->histo_res[search_depth] += 1;
    if ((++dpl->histo_cnt) == 100000) {
        logger->log(EXTENSION_LOG_INFO, NULL, "DPL search depth histogram: count=%u\n", dpl->dirty_cnt);
        for (int i = 0; i < 8; i++) {
            if (dpl->histo_res[i] > 0) {
                logger->log(EXTENSION_LOG_INFO, NULL, "\t %02d : %06u\n", i, dpl->histo_res[i]);
                dpl->histo_res[i] = 0;
            }
        }
        dpl->histo_cnt = 0;
    }
    **************************************/
    pthread_mutex_unlock(&dpl->lock);
}

static inline void do_buf_unlink_from_dpl(buf_BCB *bcb)
{
    buf_DRTY_LIST *dpl = &buf_gl.drty_list;

    pthread_mutex_lock(&dpl->lock);
    if (bcb->d_prev == NULL) dpl->head = bcb->d_next;
    else bcb->d_prev->d_next = bcb->d_next;
    if (bcb->d_next == NULL) dpl->tail = bcb->d_prev;
    else bcb->d_next->d_prev = bcb->d_prev;
    dpl->dirty_cnt -= 1;
    pthread_mutex_unlock(&dpl->lock);
    bcb->d_prev = bcb->d_next = NULL;
}

static void do_buf_min_oldest_lsn(LogSN *min_oldest_lsn)
{
    buf_DRTY_LIST *dpl = &buf_gl.drty_list;

    LOGSN_SET_NULL(min_oldest_lsn);
    pthread_mutex_lock(&dpl->lock);
    if (dpl->head != NULL) {
        *min_oldest_lsn = dpl->head->oldest_lsn;
    }
    pthread_mutex_unlock(&dpl->lock);
}
#endif

static void do_buf_block_request(buf_HASH *hash, buf_BCB *bcb, enum buf_latch_mode mode,
                                 enum buf_pin_latch_type type, enum buf_priority prio)
{
    buf_LAWT *lawt = do_buf_alloc_lawt_entry();

    /* set latch mode and type */
    lawt->r_mode = mode;
    lawt->r_type = type;

    if (prio == BUF_PRIO_HIGH) {
        /* high priority: add to the head */
        lawt->w_next = bcb->w_head;
        bcb->w_head = lawt;
        if (bcb->w_tail == NULL) bcb->w_tail = lawt;
    } else {
        /* low priority: add to the tail */
        lawt->w_next = NULL;
        if (bcb->w_tail == NULL) {
            bcb->w_head = lawt;
            bcb->w_tail = lawt;
        } else {
            bcb->w_tail->w_next = lawt;
            bcb->w_tail = lawt;
        }
    }
    bcb->wcnt += 1;

    /* do sleep */
    while (lawt->r_mode == mode) {
        pthread_cond_wait(&lawt->cond, &hash->lock);
    }
    do_buf_free_lawt_entry(lawt);
}

static void do_buf_wakeup_request(buf_BCB *bcb)
{
    buf_LAWT *lawt;

    while ((lawt = bcb->w_head) != NULL) {
        if (bcb->mode == BUF_MODE_WRITE) break;
        if (bcb->mode == BUF_MODE_READ) {
            if (lawt->r_mode == BUF_MODE_WRITE) break;
        }

        bcb->w_head = lawt->w_next;
        if (bcb->w_head == NULL) bcb->w_tail = NULL;
        bcb->wcnt--;

        if (bcb->mode == BUF_MODE_NONE)
            bcb->mode = lawt->r_mode;

        if (lawt->r_type == BUF_PIN_LATCH)
            bcb->fcnt++;
        else /* BUF_LATCH_ONLY */
            bcb->pcnt--;

        lawt->r_mode = BUF_MODE_NONE;
        pthread_cond_signal(&lawt->cond);
    }
}

/* fix : pin + latch */
static int do_buf_fix_bcb(buf_HASH *hash, buf_BCB *bcb, enum buf_latch_mode mode, bool dotry, bool *isfirst)
{
    *isfirst = (bcb->fcnt == 0 ? true : false);
    if (bcb->fcnt == 0) {
        assert(bcb->pcnt == 0);
        bcb->mode = mode;
        bcb->fcnt = 1;
    } else if (bcb->fcnt == bcb->pcnt) {
        assert(bcb->mode == BUF_MODE_NONE);
        bcb->mode = mode;
        bcb->fcnt += 1;
    } else { /* bcb->fcnt > bcb->pcnt */
        if ((mode == BUF_MODE_READ) && (bcb->mode == BUF_MODE_READ && bcb->wcnt == 0)) {
            bcb->fcnt += 1;
        } else {
            /* mode == BUF_MODE_WRITE: latch conflict, wait until the latch is released */
            /* bcb->mode == BUF_MODE_WRITE: wait until the writer unfixes the bcb */
            /* bcb->wcnt >= 1: some writer is blocked, so wait behind the writer */

            if (dotry) return -1; /* cannot fix the bcb immediatly */

            /* block the requester */
            do_buf_block_request(hash, bcb, mode, BUF_PIN_LATCH, BUF_PRIO_LOW);
        }
    }
    if (bcb->zone == BCB_ZONE_COLD_DIRTY || bcb->zone == BCB_ZONE_COLD_CLEAN) {
        /* Unlink the BCB early from LRU list
         * in order to prevent the BCB from being chosen as victim.
         */
        pthread_mutex_lock(&buf_gl.lru_list.lock);
        if (bcb->zone == BCB_ZONE_COLD_DIRTY || bcb->zone == BCB_ZONE_COLD_CLEAN) {
            do_buf_unlink_from_lru(bcb);
            bcb->zone = BCB_ZONE_VOID;
        }
        pthread_mutex_unlock(&buf_gl.lru_list.lock);
    }
    return 0;
}

/* unfix: unlatch & unpin */
static void do_buf_unfix_bcb(buf_BCB *bcb, bool lru_reposition, bool *islast)
{
    *islast = (bcb->fcnt == 1 && bcb->wcnt == 0 ? true : false);
    if (bcb->fcnt > 0) {
        bcb->fcnt -= 1;
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] latch & unlatch mismatch\n");
    }
    if ((bcb->zone == BCB_ZONE_VOID) ||
        (bcb->zone != BCB_ZONE_WARM && lru_reposition == true)) {
        if (bcb->rcnt < BUF_MAX_REF_4_WARM) {
            bcb->rcnt += 1;
        }
        do_buf_relocate_at_lru(bcb);
    }
    if (bcb->fcnt == 0 || bcb->fcnt == bcb->pcnt) {
        bcb->mode = BUF_MODE_NONE;
        if (bcb->wcnt > 0) { /* wakeup blocked request */
            do_buf_wakeup_request(bcb);
        }
    }
}

static void do_buf_unlatch_bcb(buf_BCB *bcb)
{
    assert(bcb->fcnt > 0);
    bcb->pcnt += 1;
    if (bcb->fcnt == bcb->pcnt) {
        bcb->mode = BUF_MODE_NONE;
        if (bcb->wcnt > 0) { /* wakeup blocked request */
            do_buf_wakeup_request(bcb);
        } else {
            assert(bcb->w_head == NULL);
        }
    } else {
        assert(bcb->mode != BUF_MODE_WRITE);
    }
}

static void do_buf_pin_bcb(buf_BCB *bcb, bool *isfirst)
{
    *isfirst = (bcb->fcnt == 0 ? true : false);
    bcb->fcnt += 1;
    bcb->pcnt += 1;
    if (bcb->zone == BCB_ZONE_COLD_DIRTY || bcb->zone == BCB_ZONE_COLD_CLEAN) {
        /* Unlink the BCB early from LRU list
         * in order to prevent the BCB from being chosen as victim.
         */
        pthread_mutex_lock(&buf_gl.lru_list.lock);
        if (bcb->zone == BCB_ZONE_COLD_DIRTY || bcb->zone == BCB_ZONE_COLD_CLEAN) {
            do_buf_unlink_from_lru(bcb);
            bcb->zone = BCB_ZONE_VOID;
        }
        pthread_mutex_unlock(&buf_gl.lru_list.lock);
    }
}

static void do_buf_unpin_bcb(buf_BCB *bcb, bool *islast)
{
    *islast = (bcb->fcnt == 1 ? true : false);
    if (bcb->pcnt > 0 && bcb->fcnt > 0) {
        bcb->pcnt -= 1;
        bcb->fcnt -= 1;
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] pin & unpin mismatch\n");
    }
    if (bcb->zone != BCB_ZONE_WARM) {
        if (bcb->rcnt < BUF_MAX_REF_4_WARM) {
            bcb->rcnt += 1;
        }
        do_buf_relocate_at_lru(bcb);
    }
}

static void do_buf_relatch_bcb(buf_HASH *hash, buf_BCB *bcb, enum buf_latch_mode mode)
{
    assert(bcb->pcnt > 0);
    if (bcb->mode == BUF_MODE_NONE) {
        assert(bcb->fcnt == bcb->pcnt);
        bcb->pcnt -= 1;
        bcb->mode = mode;
    } else { /* bcb->mode != BUF_MODE_NONE */
        assert(bcb->fcnt > bcb->pcnt);
        if ((mode == BUF_MODE_READ) && (bcb->mode == BUF_MODE_READ && bcb->wcnt == 0)) {
            /* latch mode is already set. so, do nothing */
            bcb->pcnt -= 1;
        } else {
            /* mode == BUF_MODE_WRITE: latch conflict, wait until the latch is released */
            /* bcb->mode == BUF_MODE_WRITE: wait until the writer unfixes the bcb */
            /* bcb->wcnt >= 1: some writer is blocked, so wait behind the writer */
            /* block the requester */
            do_buf_block_request(hash, bcb, mode, BUF_LATCH_ONLY, BUF_PRIO_LOW);
        }
    }
}

static void do_buf_uplatch_bcb(buf_HASH *hash, buf_BCB *bcb, enum buf_latch_mode mode)
{
    assert(bcb->fcnt > bcb->pcnt);
    assert(bcb->mode == BUF_MODE_READ && mode == BUF_MODE_WRITE);

    if (bcb->fcnt == (bcb->pcnt+1) && bcb->wcnt == 0) {
        bcb->mode = mode;
    } else {
        do_buf_unlatch_bcb(bcb);
        /* mode == BUF_MODE_WRITE: latch conflict, wait until the latch is released */
        /* bcb->mode == BUF_MODE_WRITE: wait until the writer unfixes the bcb */
        /* bcb->wcnt >= 1: some writer is blocked, so wait behind the writer */
        /* block the requester */
        do_buf_block_request(hash, bcb, mode, BUF_LATCH_ONLY, BUF_PRIO_LOW);
    }
}

static void do_buf_dwlatch_bcb(buf_HASH *hash, buf_BCB *bcb, enum buf_latch_mode mode)
{
    assert(bcb->fcnt == (bcb->pcnt+1));
    assert(bcb->mode == BUF_MODE_WRITE && mode == BUF_MODE_READ);

    bcb->mode = mode;
    if (bcb->wcnt > 0) {
        do_buf_wakeup_request(bcb);
    }
}

/* BCB flush functions */

static void do_buf_block_flush(buf_HASH *hash, buf_BCB *bcb)
{
    /* bcb->flush == true */
    buf_LAWT *lawt = do_buf_alloc_lawt_entry();

    /* add to the flush wait list */
    lawt->r_mode = BUF_MODE_READ;
    lawt->w_next = bcb->w_flsh;
    bcb->w_flsh = lawt;

    /* do sleep */
    while (lawt->r_mode == BUF_MODE_READ) {
        pthread_cond_wait(&lawt->cond, &hash->lock);
    }
    do_buf_free_lawt_entry(lawt);
}

static void do_buf_wakeup_flush(buf_BCB *bcb)
{
    buf_LAWT *lawt;

    while (bcb->w_flsh != NULL) {
        lawt = bcb->w_flsh;
        bcb->w_flsh = lawt->w_next;

        lawt->r_mode = BUF_MODE_NONE;
        pthread_cond_signal(&lawt->cond);
    }
}

static void do_buf_write_page(PageID pgid, Page *pgptr, bool check)
{
    ssize_t nwrite;

    /* When a data page is written to disk,
     * some referenced items, whose refcount is larger than 0, might exist on the pages.
     */
    if (check) { /* bcb->victim == true */
        /* Page Validation Check Before Write */
        /* If the data page was chosed as victim, the page must not have referenced items */
        assert(pfx_page_check_before_write(pgptr) == true);
    }

    nwrite = disk_page_pwrite(pfx_get_fd_from_pgid(pgid), pgptr, pgid.pgnum, buf_gl.page_size);
    if (nwrite != buf_gl.page_size) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] bufffer page(%d|%d|%d) write(%ld!=%ld) error=(%d|%s)\n",
                    pgid.pfxid, pgid.flnum, pgid.pgnum, nwrite, (ssize_t)buf_gl.page_size, errno, strerror(errno));
        /* [FATAL] untreatable error => abnormal shutdown by assertion */
    }
    assert(nwrite == buf_gl.page_size);

    if (buf_gl.buf_stat.enable) {
        do_buf_stat_disk_write();
    }
}

static void do_buf_flush_bcb(buf_HASH *hash, buf_BCB *bcb, Page *pgbuf)
{
    bool hold_latch;
    bool is_last_unfix;

    if (bcb->flush == true) { /* Not the first flusher */
        /* wait until the first flusher flushes the bcb */
        do_buf_block_flush(hash, bcb);
        assert(bcb->flush == false);
        return;
    }

    /* bcb->flush == false : the first flusher */
    bcb->flush = true;

    if (bcb->mode == BUF_MODE_WRITE) {
        do_buf_block_request(hash, bcb, BUF_MODE_READ, BUF_PIN_LATCH, BUF_PRIO_HIGH);
        hold_latch = true;
    } else {
        hold_latch = false;
    }

    /* save page image and then clear dirty information */
    memcpy((void*)pgbuf, (void*)bcb->pgptr, buf_gl.page_size);
#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
    if (config->sq.use_recovery)
        do_buf_unlink_from_dpl(bcb);
#endif
    LOGSN_SET_NULL(&bcb->oldest_lsn); bcb->dirty = false;

    if (hold_latch == true) {
        do_buf_unfix_bcb(bcb, false, &is_last_unfix);
        if (buf_gl.buf_stat.enable) {
            do_buf_stat_unfix(is_last_unfix);
        }
    }
    pthread_mutex_unlock(&hash->lock);

    /* 1. flush logs in order to obey WAL protocol */
    if (config->sq.use_recovery) {
        log_buffer_flush(&pgbuf->header.pglsn, true);
    }
    /* 2. disk write the copied page image */
    do_buf_write_page(bcb->pgid, pgbuf, bcb->victim);

    pthread_mutex_lock(&hash->lock);

    bcb->flush = false;
    if (bcb->w_flsh != NULL) { /* wakeup the other flushers if exist */
        do_buf_wakeup_flush(bcb);
    }

#if 0 // version 1 - simple implementation while holding read latch
    (void)do_buf_fix_bcb(hash, bcb, BUF_MODE_READ, false);

    /* recheck needed: other thread might have flushed it */
    if (bcb->dirty) {
        bcb->flush = true;
        pthread_mutex_unlock(&hash->lock);

        /* 1. flush logs in order to obey WAL protocol */
        if (config->sq.use_recovery) {
            log_buffer_flush(&pgbuf->header.pglsn, true);
        }
        /* 2. disk write the copied page image */
        do_buf_write_page(bcb->pgid, pgbuf, bcb->victim);

        pthread_mutex_lock(&hash->lock);
        LOGSN_SET_NULL(&bcb->oldest_lsn);
        bcb->dirty = false;
        bcb->flush = false;
    }

    do_buf_unfix_bcb(bcb, false);
#endif
}

/* BCB alloc & free functions */
static buf_BCB *do_buf_get_victim_bcb(bool force_flush)
{
    buf_LRU_LIST *lru = &buf_gl.lru_list;
    buf_BCB      *bcb;
    buf_PBUF     *pbuf = NULL;
    PageID   vtm_pgid;
    int      srhcount;

again:
    /* find the victim bcb */
    srhcount = 20; /* must be not too large value */
    pthread_mutex_lock(&lru->lock);
    bcb = lru->tail;
    while (bcb != NULL) {
        if (bcb->zone == BCB_ZONE_MIDDLE) {
            bcb = NULL; break;
        }
        if ((bcb->fcnt == 0 && bcb->wcnt == 0) &&
            (force_flush || (bcb->dirty == false && bcb->flush == false))) {
            /* unlink the bcb to prevent it from being victimized by others */
            assert(bcb->zone != BCB_ZONE_VOID);
            do_buf_unlink_from_lru(bcb);
            bcb->zone = BCB_ZONE_VOID;
            vtm_pgid = bcb->pgid; break;
        }
        bcb = bcb->b_prev;
        if ((--srhcount) == 0) bcb = NULL;
    }
    pthread_mutex_unlock(&lru->lock);

    if (bcb != NULL) {
        buf_HASH *hash = &buf_gl.hash_tab[do_buf_pgid_hash(vtm_pgid)];
        buf_BCB  *save = bcb;
        buf_BCB  *prev = NULL;

        pthread_mutex_lock(&hash->lock);
        bcb = hash->h_next;
        while (bcb != NULL) {
            if (PAGEID_IS_EQ(&bcb->pgid, &vtm_pgid) && bcb == save)
                break;
            prev = bcb; bcb = bcb->h_next;
        }
        if (bcb != NULL) {
            do {
                if (bcb->fcnt > 0 || bcb->wcnt > 0 || bcb->victim != false || bcb->zone != BCB_ZONE_VOID) {
                    bcb = NULL; break;
                }
                if (bcb->dirty == true || bcb->flush == true) {
                    if (force_flush == false) {
                        bcb = NULL; break;
                    }
                    /* force_flush == true */
                    if (pbuf == NULL) {
                        pbuf = do_buf_alloc_page_buffer();
                    }
                    bcb->victim = true;
                    do_buf_flush_bcb(hash, bcb, pbuf->buff);
                    bcb->victim = false;
                    if (bcb->fcnt > 0 || bcb->wcnt > 0 || bcb->zone != BCB_ZONE_VOID) {
                        bcb = NULL; break;
                    }
                    if (bcb->dirty == true || bcb->flush == true || PAGEID_IS_NE(&bcb->pgid, &vtm_pgid)) {
                        bcb = NULL; break;
                    }
                    /* OK... validation check is completed. */
                    /* Now, recheck if the prev bcb is valid */
                    if ((prev == NULL && hash->h_next != bcb) || (prev != NULL && prev->h_next != bcb)) {
                        /* refind the previous bcb in order to unlink the bcb */
                        save = bcb;
                        prev = NULL;
                        bcb = hash->h_next;
                        while (bcb != NULL) {
                            if (PAGEID_IS_EQ(&bcb->pgid, &vtm_pgid) && bcb == save)
                                break;
                            prev = bcb; bcb = bcb->h_next;
                        }
                        if (bcb == NULL) {
                            fprintf(stderr, "{NOTE} victim bcb is absent.\n");
                            break;
                        }
                    }
                }
                do_buf_unlink_from_hash(hash, prev, bcb);
            } while(0);
        }
        pthread_mutex_unlock(&hash->lock);

        if (bcb == NULL) {
            goto again;
        }
        if (buf_gl.buf_stat.enable) {
            do_buf_stat_replacement();
        }
    }

    if (pbuf != NULL) {
        do_buf_free_page_buffer(pbuf);
    }
    return bcb;
}

static buf_BCB *do_buf_alloc_bcb(void)
{
    buf_BCB *bcb = NULL;
    struct timespec sleep_time = {0, 100000}; // 100 us
    int  retry_count = 0;
    bool force_flush = false;
    //bool force_flush = true;

    while (bcb == NULL) {
        /* step 1: allocate bcb from free list */
        if (buf_gl.free_list.head != NULL) {
            pthread_mutex_lock(&buf_gl.free_list.lock);
            if (buf_gl.free_list.head != NULL) {
                bcb = buf_gl.free_list.head;
                buf_gl.free_list.head = bcb->b_next;
                buf_gl.free_list.count -= 1;
            }
            pthread_mutex_unlock(&buf_gl.free_list.lock);
            if (bcb != NULL) {
                bcb->zone = (uint8_t)BCB_ZONE_VOID;
                break;
            }
        }
        /* step 2: allocate bcb from lru list */
        bcb = do_buf_get_victim_bcb(force_flush);
        if (bcb != NULL) break;

        nanosleep(&sleep_time, NULL); // sleep 100 us.
        retry_count++;
    }
    if ((buf_gl.free_list.count + buf_gl.lru_list.nclean) < buf_gl.LWM_clean) {
        if (buf_gl.pgflusher.sleep == true)
            do_buf_page_flusher_wakeup();
    }
    if (retry_count > 100 || (force_flush == true && retry_count > 0)) {
        logger->log(EXTENSION_LOG_INFO, NULL, "[WARNING] bcb alloc retry count = %d\n", retry_count);
    }
    return bcb;
}

static void do_buf_free_bcb(buf_BCB *bcb)
{
    /* initialize bcb as free state */
    //PAGEID_SET_NULL(&bcb->pgid);
    LOGSN_SET_NULL(&bcb->oldest_lsn);
    bcb->fcnt   = 0;
    bcb->pcnt   = 0;
    bcb->mode   = (uint8_t)BUF_MODE_NONE;
    bcb->zone   = (uint8_t)BCB_ZONE_FREE;
    bcb->rcnt   = 0;
    bcb->dirty  = false;
    bcb->flush  = false;
    bcb->victim = false;
    bcb->wcnt   = 0;
    bcb->w_head = NULL;
    bcb->w_tail = NULL;
    bcb->w_flsh = NULL;

    /* connect bcb to the free list */
    pthread_mutex_lock(&buf_gl.free_list.lock);
    bcb->b_next = buf_gl.free_list.head;
    buf_gl.free_list.head = bcb;
    buf_gl.free_list.count += 1;
    pthread_mutex_unlock(&buf_gl.free_list.lock);
}

/* BCB fetch functions */

static int do_buf_read_page(PageID pgid, Page *pgptr)
{
    ssize_t nread;

    nread = disk_page_pread(pfx_get_fd_from_pgid(pgid), pgptr, pgid.pgnum, buf_gl.page_size);
    if (nread != buf_gl.page_size) {
        /* CHECK if the page was absent in disk */
        if (nread == 0 && errno == 0) {
            /* initialize page image : void page */
            PAGEID_SET_NULL(&pgptr->header.self_pgid);
            return 0; /* non-existent page */
        }
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "[FATAL] buffer page(%u|%u|%u) read(%ld!=%ld) error=(%d|%s)\n",
                    pgid.pfxid, pgid.flnum, pgid.pgnum,
                    nread, (ssize_t)buf_gl.page_size, errno, strerror(errno));
        return -1;
    }
    if (PAGEID_IS_NULL(&pgptr->header.self_pgid)) {
        /* CHECK if the page was hole in sparse file.
         * The sparse file can be existent
         * because each initailized page isn't immediately flushed to disk.
         */
        if (memcmp((void*)pgptr, buf_gl.zero_filled_page, buf_gl.page_size) == 0) {
            return 0; /* non-existent page */
        }
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "[FATAL] buffer page(%u|%u|%u) read from disk has NULL PAGEID.\n",
                    pgid.pfxid, pgid.flnum, pgid.pgnum);
        return -1;
    }
    /* Page Validation Check After Read */
    if (pfx_page_check_after_read(pgptr) == false) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "[FATAL] buffer page(%u|%u|%u) read from disk is invalid.\n",
                    pgid.pfxid, pgid.flnum, pgid.pgnum);
        return -1;
    }
    if (buf_gl.buf_stat.enable) {
        do_buf_stat_disk_read();
    }
    return 0;
}

static int do_buf_fetch_bcb(buf_BCB *bcb, PageID pgid, bool newpg)
{
    if (newpg) {
        /* initialize page image : void page */
        PAGEID_SET_NULL(&bcb->pgptr->header.self_pgid);
    } else {
        if (do_buf_read_page(pgid, bcb->pgptr) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] buf fetch bcb - read page fail\n");
            return -1; /* ERROR */
        }
        if (PAGEID_IS_NULL(&bcb->pgptr->header.self_pgid)) {
            /* A non-existent page */
            if (config->sq.use_recovery) {
                if (config->sq.recovery_phase == SQUALL_RECOVERY_NONE) {
                    return -2; /* The page doesn't exist in disk */
                }
                if (config->sq.recovery_phase == SQUALL_RECOVERY_REDO) {
                    return 0; /* SUCCESS - valid page */
                }
            }
            logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] buf fetch bcb - non-existent page.\n");
            return -1; /* ERROR */
        }
    }
    return 0; /* SUCCESS */
}

static buf_BCB *do_buf_build_bcb(buf_HASH *hash, PageID pgid, bool newpg)
{
    buf_BCB *bcb = NULL;
#ifdef BUF_USE_PAGE_LOCK
    bool lock_holder;

again:
    do_buf_lock_page(hash, pgid, &lock_holder);
    if (lock_holder == false) { /* lock waiter */
        bcb = do_buf_find_bcb(hash, pgid);
        if (bcb == NULL) {
            /* [IMPORTANT] this case must not be occurred. */
            logger->log(EXTENSION_LOG_WARNING, NULL, "[NOTE] buffer already disappeared\n");
            goto again;
        }
    } else { /* lock holder */
        pthread_mutex_unlock(&hash->lock);

        /* Allocating a bcb and fetching page image are time-consuming tasks */
        bcb = do_buf_alloc_bcb();
        assert(bcb != NULL && bcb->dirty == false);

        if (do_buf_fetch_bcb(bcb, pgid, newpg) < 0) {
            /* return value : -1 or -2 */
            do_buf_free_bcb(bcb); bcb = NULL;
        } else {
            /* initialize bcb */
            bcb->pgid = pgid;
            bcb->mode = BUF_MODE_NONE;
            bcb->fcnt = bcb->pcnt = 0;
        }

        pthread_mutex_lock(&hash->lock);
        if (bcb != NULL) {
            do_buf_link_to_hash(hash, bcb);
        }
        do_buf_unlock_page(hash, pgid);
    }
#else // BUF_USE_PAGE_LOCK
    bcb = do_buf_alloc_bcb();
    assert(bcb != NULL && bcb->dirty == false);

    if (do_buf_fetch_bcb(bcb, pgid, newpg) != 0) {
        /* SEVERE error */
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] buffer - fetch bcb fail\n");
        do_buf_free_bcb(bcb); bcb = NULL;
    } else {
        /* initialize bcb and link to hash chain */
        bcb->pgid = pgid;
        bcb->mode = BUF_MODE_NONE;
        bcb->fcnt = bcb->pcnt = 0;
        do_buf_link_to_hash(hash, bcb);
    }
#endif

    return bcb;
}



/* Initialization */

static void do_buf_init_bcb_entry(buf_BCB *bcb)
{
    /* initialize bcb as free state */
    /* bcb->b_prev */
    /* bcb->b_next */
    bcb->h_root = NULL;
    bcb->h_next = NULL;
    PAGEID_SET_NULL(&bcb->pgid);
    LOGSN_SET_NULL(&bcb->oldest_lsn);
    bcb->fcnt   = 0;
    bcb->pcnt   = 0;
    bcb->mode   = (uint8_t)BUF_MODE_NONE;
    bcb->zone   = (uint8_t)BCB_ZONE_FREE;
    bcb->rcnt   = 0;
    bcb->dirty  = false;
    bcb->flush  = false;
    bcb->victim = false;
    bcb->wcnt   = 0;
    bcb->w_head = NULL;
    bcb->w_tail = NULL;
    bcb->w_flsh = NULL;
}

static int do_buf_init_bcb_list(void)
{
    buf_BCB *bcb;
    int i;

#ifdef SQUALL_USE_MMAP
    /* allocate page table : page size alignement needed */
    buf_gl.page_tab = mmap(NULL, (size_t)buf_gl.max_nbuffer*buf_gl.page_size,
                          PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_SHARED, -1, 0);
    if (buf_gl.page_tab == MAP_FAILED) {
        fprintf(stderr, "buffer manager - page table alloc error\n");
        return -1; /* no more memory */
    }
    /* allocate BCB table */
    buf_gl.bcb_tab  = mmap(NULL, (size_t)buf_gl.max_nbuffer*sizeof(buf_BCB),
                          PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_SHARED, -1, 0);
    if (buf_gl.bcb_tab == MAP_FAILED) {
        fprintf(stderr, "buffer manager - bcb table alloc error\n");
        return -1; /* no more memory */
    }
#else
    /* allocate page table : page size alignement needed */
    void *page_tab_space;
    if (posix_memalign(&page_tab_space, (size_t)buf_gl.page_size,
                       (size_t)buf_gl.max_nbuffer*buf_gl.page_size) != 0) {
        fprintf(stderr, "buffer manager - page table alloc error\n");
        return -1; /* no more memory */
    }
    buf_gl.page_tab = (Page*)page_tab_space;
    /* allocate BCB table */
    void *bcb_tab_space;
    if (posix_memalign(&bcb_tab_space, (size_t)buf_gl.page_size,
                       (size_t)buf_gl.max_nbuffer*sizeof(buf_BCB)) != 0) {
        fprintf(stderr, "buffer manager - bcb table alloc error\n");
        return -1; /* no more memory */
    }
    buf_gl.bcb_tab = (buf_BCB*)bcb_tab_space;
#endif

    /* initialize each BCB and Page */
    for (i = 0; i < buf_gl.max_nbuffer; i++) {
        bcb = &buf_gl.bcb_tab[i];
        do_buf_init_bcb_entry(bcb);
        bcb->b_prev = NULL;
        bcb->b_next = (i==(buf_gl.max_nbuffer-1) ? NULL :  &buf_gl.bcb_tab[i+1]);
        bcb->pgptr  = (Page*)((char*)buf_gl.page_tab + ((size_t)buf_gl.page_size * i));
    }

    /* initialize free list */
    buf_gl.free_list.head  = &buf_gl.bcb_tab[0];
    buf_gl.free_list.count = buf_gl.max_nbuffer;
    pthread_mutex_init(&buf_gl.free_list.lock, NULL);

    /* initialize lru list */
    buf_gl.lru_list.head   = NULL;
    buf_gl.lru_list.middle = NULL;
    buf_gl.lru_list.cold_dirty = NULL;
    buf_gl.lru_list.cold_clean = NULL;
    buf_gl.lru_list.tail   = NULL;
    buf_gl.lru_list.nwarm  = 0;
    buf_gl.lru_list.nmiddle = 0;
    buf_gl.lru_list.ndirty = 0;
    buf_gl.lru_list.nclean = 0;
    buf_gl.lru_list.wlimit = (buf_gl.max_nbuffer/10) * 3; /* 30% */
    buf_gl.lru_list.mlimit = (buf_gl.max_nbuffer/10) * 3; /* 30% */
    pthread_mutex_init(&buf_gl.lru_list.lock, NULL);

    /* initailize dirty list */
    buf_gl.drty_list.head = NULL;
    buf_gl.drty_list.tail = NULL;
    buf_gl.drty_list.dirty_cnt = 0;
    buf_gl.drty_list.histo_cnt = 0;
    for (i = 0; i < 8; i++)
        buf_gl.drty_list.histo_res[i] = 0;
    pthread_mutex_init(&buf_gl.drty_list.lock, NULL);

    buf_gl.perm_list.head = NULL;
    buf_gl.perm_list.tail = NULL;
    buf_gl.perm_list.ccnt = 0;
    buf_gl.perm_list.mcnt = buf_gl.max_nbuffer/5; /* max 20% */
    pthread_mutex_init(&buf_gl.perm_list.lock, NULL);
    return 0;
}

static int do_buf_init_pbuf_list(const int nthread)
{
    char          *bufptr;
    buf_FCB_QUEUE *fcbque;
    buf_PBUF      *pbuf;
    int            i, j;

#ifdef FLUSH_ALL_MULTI_THREADS
    buf_gl.pbuf_size = nthread + BUF_MAX_IO_THREAD; /* # of page flush buffers */
#else
    buf_gl.pbuf_size = nthread + 5; /* # of page flush buffers */
#endif
    buf_gl.max_pbufs = (BUF_FCB_QUEUE_COUNT * BUF_FCB_QUEUE_SIZE) + buf_gl.pbuf_size;

    /* allocate page bufer for flushing : page size alignement needed */
#ifdef SQUALL_USE_MMAP
    buf_gl.page_buf = mmap(NULL, (size_t)buf_gl.max_pbufs*buf_gl.page_size,
                          PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_SHARED, -1, 0);
    if (buf_gl.page_buf == MAP_FAILED) {
        fprintf(stderr, "buffer manager - page buffers alloc error\n");
        return -1; /* no more memory */
    }
#else
    void *page_buf_space;
    if (posix_memalign(&page_buf_space, (size_t)buf_gl.page_size,
                       (size_t)buf_gl.max_pbufs*buf_gl.page_size) != 0) {
        fprintf(stderr, "buffer manager - page buffers alloc error\n");
        return -1; /* no more memory */
    }
    buf_gl.page_buf = (Page*)page_buf_space;
#endif

    /* initialize FCB queues */
    buf_gl.fcbq_tab = (buf_FCB_QUEUE*)malloc(BUF_FCB_QUEUE_COUNT * sizeof(buf_FCB_QUEUE));
    if (buf_gl.fcbq_tab == NULL) {
        fprintf(stderr, "buffer manager - FCB queue table alloc error\n");
        return -1; /* no more memory */
    }
    for (i = 0; i < BUF_FCB_QUEUE_COUNT; i++) {
        bufptr = (char*)buf_gl.page_buf + (buf_gl.page_size * i * BUF_FCB_QUEUE_SIZE);
        fcbque = &buf_gl.fcbq_tab[i];
        for (j = 0; j < BUF_FCB_QUEUE_SIZE; j++) {
            fcbque->list[j].pgptr = (Page*)(bufptr + (buf_gl.page_size * j));
        }
        fcbque->fcnt = BUF_FCB_QUEUE_SIZE; /* # of free FCBs */
        fcbque->head = 0;
        fcbque->tail = 0;
        fcbque->prep = 0;
        fcbque->next = (i==(BUF_FCB_QUEUE_COUNT-1) ? NULL : &buf_gl.fcbq_tab[i+1]);
    }
    buf_gl.fcbq_list.head = &buf_gl.fcbq_tab[0];
    buf_gl.fcbq_list.count = BUF_FCB_QUEUE_COUNT;
    pthread_mutex_init(&buf_gl.fcbq_list.lock, NULL);

    /* initialize page flush buffer list */
    buf_gl.pbuf_tab = (buf_PBUF*)malloc(buf_gl.pbuf_size * sizeof(buf_PBUF));
    if (buf_gl.pbuf_tab == NULL) {
        fprintf(stderr, "buffer manager - page buffer table alloc error\n");
        return -1; /* no more memory */
    }
    bufptr = (char*)buf_gl.page_buf + (buf_gl.page_size * BUF_FCB_QUEUE_COUNT * BUF_FCB_QUEUE_SIZE);
    for (i = 0; i < buf_gl.pbuf_size; i++) {
         pbuf = &buf_gl.pbuf_tab[i];
         pbuf->buff = (Page *)(bufptr + (buf_gl.page_size * i));
         pbuf->next = (i==(buf_gl.pbuf_size-1) ? NULL :  &buf_gl.pbuf_tab[i+1]);
    }
    buf_gl.pbuf_list.head  = &buf_gl.pbuf_tab[0];
    buf_gl.pbuf_list.count = buf_gl.pbuf_size;
    pthread_mutex_init(&buf_gl.pbuf_list.lock, NULL);
    return 0;
}

static int do_buf_init_wait_list(const int nthread)
{
    buf_LAWT *lawt;
    buf_PGLK *pglk;
    int i;

    buf_gl.lawt_size = nthread * 2; // guarantee enough entries
    buf_gl.lawt_tab = (buf_LAWT*)malloc(buf_gl.lawt_size*sizeof(buf_LAWT));
    if (buf_gl.lawt_tab == NULL) {
        fprintf(stderr, "buffer manager - latch wait table alloc error\n");
        return -1; /* no more memory */
    }
    buf_gl.pglk_size = nthread * 2; // guarantee enough entries
    buf_gl.pglk_tab = (buf_PGLK*)malloc(buf_gl.pglk_size*sizeof(buf_PGLK));
    if (buf_gl.pglk_tab == NULL) {
        fprintf(stderr, "buffer manager - page lock table alloc error\n");
        return -1; /* no more memory */
    }

    /* initialize each lawt entry */
    for (i = 0; i < buf_gl.lawt_size; i++) {
         lawt = &buf_gl.lawt_tab[i];
         pthread_cond_init(&lawt->cond, NULL);
         lawt->r_mode = BUF_MODE_NONE;
         lawt->w_next = (i==(buf_gl.lawt_size-1) ? NULL :  &buf_gl.lawt_tab[i+1]);
    }
    /* initialize free lawt list */
    buf_gl.lawt_list.head  = &buf_gl.lawt_tab[0];
    buf_gl.lawt_list.count = buf_gl.lawt_size;
    pthread_mutex_init(&buf_gl.lawt_list.lock, NULL);

    /* initialize each pglk entry */
    for (i = 0; i < buf_gl.pglk_size; i++) {
         pglk = &buf_gl.pglk_tab[i];
         pthread_cond_init(&pglk->cond, NULL);
         PAGEID_SET_NULL(&pglk->pgid);
         pglk->wcnt   = 0;
         pglk->lked   = false;
         pglk->l_next = (i==(buf_gl.pglk_size-1) ? NULL :  &buf_gl.pglk_tab[i+1]);
    }
    /* initialize free pglk list */
    buf_gl.pglk_list.head  = &buf_gl.pglk_tab[0];
    buf_gl.pglk_list.count = buf_gl.pglk_size;
    pthread_mutex_init(&buf_gl.pglk_list.lock, NULL);
    return 0;
}

static int do_buf_init_hash_table()
{
    buf_HASH *hash;

    /* set hash table size */
    buf_gl.hash_size = (buf_gl.max_nbuffer * 2) + 1;

    /* allocate buffer hash table */
#ifdef SQUALL_USE_MMAP
    buf_gl.hash_tab = mmap(NULL, (size_t)buf_gl.hash_size*sizeof(buf_HASH),
                          PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_SHARED, -1, 0);
    if (buf_gl.hash_tab == MAP_FAILED) {
        fprintf(stderr, "buffer manager - hash table alloc error\n");
        return -1; /* no more memory */
    }
#else
    void *hash_tab_space;
    if (posix_memalign(&hash_tab_space, (size_t)buf_gl.page_size,
                       (size_t)buf_gl.hash_size*sizeof(buf_HASH)) != 0) {
        fprintf(stderr, "buffer manager - hash table alloc error\n");
        return -1; /* no more memory */
    }
    buf_gl.hash_tab = (buf_HASH*)hash_tab_space;
#endif
    /* initialize each hash entry */
    for (int i = 0; i < buf_gl.hash_size; i++) {
         hash = &buf_gl.hash_tab[i];
         pthread_mutex_init(&hash->lock, NULL);
         hash->h_next = NULL; hash->h_leng = 0;
         hash->l_next = NULL; hash->l_leng = 0;
    }
    return 0;
}

static int do_buf_init_zero_filled_page()
{
     buf_gl.zero_filled_page = malloc(buf_gl.page_size);
     if (buf_gl.zero_filled_page == NULL) {
         fprintf(stderr, "buffer manager - zero filled page alloc error\n");
         return -1; /* no more memory */
     }
     memset(buf_gl.zero_filled_page, 0, buf_gl.page_size);
     return 0;
}

/* Page Flusher functions */
static void do_buf_pgflush_prepare(buf_FCB_QUEUE *queue, buf_HASH *hash, buf_BCB *bcb)
{
    bool hold_latch = false;
    bool is_last_unfix;

    bcb->flush = true;

    if (bcb->mode == BUF_MODE_WRITE) {
        do_buf_block_request(hash, bcb, BUF_MODE_READ, BUF_PIN_LATCH, BUF_PRIO_HIGH);
        hold_latch = true;
    }

    /* allocate FCB entry */
    buf_FCB *fcb = &queue->list[queue->prep];
    if ((++queue->prep) == BUF_FCB_QUEUE_SIZE) queue->prep = 0;
    assert(queue->prep != queue->head);
    queue->fcnt -= 1;

    /* save page image and oldest_lsn */
    fcb->hash = hash;
    fcb->bcb  = bcb;
    fcb->pgid = bcb->pgid;
    memcpy((void*)fcb->pgptr, (void*)bcb->pgptr, buf_gl.page_size);

    /* clear dirty information */
#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
    if (config->sq.use_recovery)
        do_buf_unlink_from_dpl(bcb);
#endif
    LOGSN_SET_NULL(&bcb->oldest_lsn); bcb->dirty = false;

    if (hold_latch == true) {
        do_buf_unfix_bcb(bcb, false, &is_last_unfix);
        if (buf_gl.buf_stat.enable) {
            do_buf_stat_unfix(is_last_unfix);
        }
    }
}

static void do_buf_pgflush_log_wal(buf_FCB_QUEUE *queue)
{
    LogSN          max_pglsn;
    uint32_t       curr;

    assert(queue->tail != queue->prep);

    LOGSN_SET_NULL(&max_pglsn);
    curr = queue->tail;
    while (curr != queue->prep) {
        if (LOGSN_IS_GT(&queue->list[curr].pgptr->header.pglsn, &max_pglsn))
            max_pglsn = queue->list[curr].pgptr->header.pglsn;
        if ((++curr) == BUF_FCB_QUEUE_SIZE) curr = 0;
    }
    /* flush logs in order to obey WAL protocol */
    log_buffer_flush(&max_pglsn, true);
}

static void do_buf_pgflush_commit(buf_FCB_QUEUE *queue)
{
    assert(queue->tail == 0 && queue->prep > 0);
    buf_FCB *fcb;

    while (queue->tail != queue->prep)
    {
        fcb = &queue->list[queue->tail];
        assert(PAGEID_IS_EQ(&fcb->pgid, &fcb->bcb->pgid));

        /* disk write the copied page image */
        do_buf_write_page(fcb->pgid, fcb->pgptr, fcb->bcb->victim);

        pthread_mutex_lock(&fcb->hash->lock);
        fcb->bcb->flush = false;
        if (fcb->bcb->w_flsh != NULL) { /* wakeup the other flushers if exist */
            do_buf_wakeup_flush(fcb->bcb);
        }
        pthread_mutex_unlock(&fcb->hash->lock);

        /* free FCB entry */
        queue->fcnt += 1;
        queue->tail += 1; /* increment tail pointer */
    }
    assert(queue->fcnt == BUF_FCB_QUEUE_SIZE);

    /* reset tail and prep pointer */
    queue->tail = queue->prep = 0;
}

static void do_buf_pgflush_aio_write(buf_FCB_QUEUE *queue)
{
    buf_FCB *fcb;
    int      ret;

    while (queue->tail != queue->prep)
    {
        fcb = &queue->list[queue->tail];
        assert(PAGEID_IS_EQ(&fcb->pgid, &fcb->bcb->pgid));

        /* When a data page is written to disk,
         * some referenced items, whose refcount is larger than 0, might exist on the pages.
         */
        if (fcb->bcb->victim == true) {
            /* Page Validation Check Before Write */
            /* If the data page was chosed as victim, the page must not have referenced items */
            assert(pfx_page_check_before_write(fcb->pgptr) == true);
        }
        ret = disk_page_aio_write(pfx_get_fd_from_pgid(fcb->pgid), fcb->pgptr,
                                  fcb->pgid.pgnum, buf_gl.page_size, &fcb->iocb);
        if (ret != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[FATAL] buf flush bcb - page(%d|%d|%d) aio write error=(%d|%s)\n",
                        fcb->pgid.pfxid, fcb->pgid.flnum, fcb->pgid.pgnum, errno, strerror(errno));
            /* [FATAL] untreatable error => abnormal shutdown by assertion */
        }
        assert(ret == 0);

        /* increment tail pointer */
        if ((++queue->tail) == BUF_FCB_QUEUE_SIZE) {
            queue->tail = 0;
        }
    }
}

static void do_buf_pgflush_aio_wait(buf_FCB_QUEUE *queue, int count)
{
    buf_FCB *fcb;
    int      ret;

    if (queue->fcnt > count) return;

    while (queue->head != queue->tail && queue->fcnt <= count)
    {
        fcb = &queue->list[queue->head];
        assert(PAGEID_IS_EQ(&fcb->pgid, &fcb->bcb->pgid));

        ret = disk_aio_wait(&fcb->iocb);
        if (ret != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[FATAL] buf flush bcb - page(%d|%d|%d) aio wait error=(%d|%s)\n",
                        fcb->pgid.pfxid, fcb->pgid.flnum, fcb->pgid.pgnum, errno, strerror(errno));
            /* [FATAL] untreatable error => abnormal shutdown by assertion */
        }
        assert(ret == 0);

        if (buf_gl.buf_stat.enable) {
            do_buf_stat_disk_write();
        }

        pthread_mutex_lock(&fcb->hash->lock);
        fcb->bcb->flush = false;
        if (fcb->bcb->w_flsh != NULL) { /* wakeup the other flushers if exist */
            do_buf_wakeup_flush(fcb->bcb);
        }
        pthread_mutex_unlock(&fcb->hash->lock);

        /* free FCB entry */
        queue->fcnt += 1;
        /* increment tail pointer */
        if ((++queue->head) == BUF_FCB_QUEUE_SIZE) {
            queue->head = 0;
        }
    }
}

#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
static int do_buf_flush_dpl_bcbs(buf_FCB_QUEUE *fcbque, PageID *pgid_list, int list_size)
{
    buf_DRTY_LIST *dpl = &buf_gl.drty_list;
    buf_HASH      *hash;
    buf_BCB       *bcb;
    LogSN          min_age_lsn;
    int            dirty_count = 0;
    int            i,nprepared = 0;

    /* min_age_lsn : log_fsync_lsn - 5GB */
    log_get_flush_lsn(&min_age_lsn);
    if (LOGSN_IS_LE(&min_age_lsn, &buf_gl.log_amount_lsn))
        return dirty_count;

    min_age_lsn.filenum -= buf_gl.log_amount_lsn.filenum;
    if (buf_gl.log_amount_lsn.roffset > 0) {
        if (min_age_lsn.roffset < buf_gl.log_amount_lsn.roffset) {
            min_age_lsn.filenum -= 1;
            min_age_lsn.roffset += (config->sq.log_file_size*1024*1024);
        }
        min_age_lsn.roffset -= buf_gl.log_amount_lsn.roffset;
    }

    pthread_mutex_lock(&dpl->lock);
    bcb = dpl->head;
    while (bcb != NULL && LOGSN_IS_LT(&bcb->oldest_lsn, &min_age_lsn)) {
        pgid_list[dirty_count] = bcb->pgid;
        if ((++dirty_count) >= list_size) break;
        bcb = bcb->d_next;
    }
    pthread_mutex_unlock(&dpl->lock);

    if (dirty_count > 0) {
        if (config->sq.use_directio) {
            do_buf_pgflush_aio_wait(fcbque, dirty_count);
        }
        for (i = 0; i < dirty_count; i++) {
            hash = &buf_gl.hash_tab[do_buf_pgid_hash(pgid_list[i])];
            pthread_mutex_lock(&hash->lock);
            bcb = do_buf_find_bcb(hash, pgid_list[i]);
            if (bcb != NULL && bcb->dirty == true && bcb->flush == false) {
                do_buf_pgflush_prepare(fcbque, hash, bcb);
                nprepared += 1;
            }
            pthread_mutex_unlock(&hash->lock);
        }
        if (nprepared > 0) {
            if (config->sq.use_recovery) {
                do_buf_pgflush_log_wal(fcbque);
            }
            if (config->sq.use_directio) {
                do_buf_pgflush_aio_write(fcbque);
            } else {
                do_buf_pgflush_commit(fcbque);
            }
        }
    }
    return dirty_count;
}
#endif

static int do_buf_flush_lru_bcbs(buf_FCB_QUEUE *fcbque, PageID *pgid_list, int list_size)
{
    buf_LRU_LIST *lru = &buf_gl.lru_list;
    buf_HASH     *hash;
    buf_BCB      *bcb;
    int           check_count = list_size;
    int           dirty_count = 0;

    pthread_mutex_lock(&lru->lock);
    bcb = (lru->cold_clean != NULL ? lru->cold_clean->b_prev : lru->tail);
    while (bcb != NULL && bcb->zone == BCB_ZONE_COLD_DIRTY) {
        //if (bcb == lru->cold_dirty) break;
        if (bcb->dirty == true) {
            pgid_list[dirty_count++] = bcb->pgid;
        } else {
            if (dirty_count == 0) {
                bcb->zone = BCB_ZONE_COLD_CLEAN;
                lru->cold_clean = bcb;
                lru->nclean += 1;
                lru->ndirty -= 1;
            }
        }
        if ((--check_count) == 0) break;
        bcb = bcb->b_prev;
    }
    pthread_mutex_unlock(&lru->lock);

    if (dirty_count > 0) {
        int i, nprepared = 0;
        if (config->sq.use_directio) {
            do_buf_pgflush_aio_wait(fcbque, dirty_count);
        }
        for (i = 0; i < dirty_count; i++) {
            hash = &buf_gl.hash_tab[do_buf_pgid_hash(pgid_list[i])];
            pthread_mutex_lock(&hash->lock);
            bcb = do_buf_find_bcb(hash, pgid_list[i]);
            if (bcb != NULL && bcb->dirty == true && bcb->flush == false) {
                do_buf_pgflush_prepare(fcbque, hash, bcb);
                nprepared += 1;
            }
            pthread_mutex_unlock(&hash->lock);
        }
        if (nprepared > 0) {
            if (config->sq.use_recovery) {
                do_buf_pgflush_log_wal(fcbque);
            }
            if (config->sq.use_directio) {
                do_buf_pgflush_aio_write(fcbque);
            } else {
                do_buf_pgflush_commit(fcbque);
            }
        }
    }
    return dirty_count;
}

static void *page_flush_thread(void *arg)
{
    PageID pgid_list[BUF_FLUSH_PAGE_COUNT];
    buf_FCB_QUEUE *fcbque;
    uint32_t flush_execs = 0;
    uint32_t dirty_count;
    uint32_t cur_ndirty_from_lru = 0;
    uint32_t cur_ndirty_from_dpl = 0;
    uint32_t tot_ndirty_from_lru = 0;
    uint32_t tot_ndirty_from_dpl = 0;

    assert(buf_gl.pgflusher.init == true);

    buf_gl.pgflusher.start = true;
    //fprintf(stderr, "page flush thread started\n");
    fcbque = do_buf_alloc_fcb_queue();
    while (buf_gl.pgflusher.init)
    {
        if ((buf_gl.free_list.count + buf_gl.lru_list.nclean) < buf_gl.HWM_clean) {
            while (buf_gl.pgflusher.init && (buf_gl.free_list.count + buf_gl.lru_list.nclean) < buf_gl.HWM_clean)
            {
                dirty_count = do_buf_flush_lru_bcbs(fcbque, pgid_list, BUF_FLUSH_PAGE_COUNT);
                cur_ndirty_from_lru += dirty_count;
            }
            tot_ndirty_from_lru += cur_ndirty_from_lru;
        }
        if (buf_gl.pgflusher.init == false) break;

#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
        if (config->sq.use_recovery) {
            int max_dpl_flush_count = 128;
            do {
                dirty_count = do_buf_flush_dpl_bcbs(fcbque, pgid_list, BUF_FLUSH_PAGE_COUNT);
                cur_ndirty_from_dpl += dirty_count;
            }
            while (buf_gl.pgflusher.init &&
                   dirty_count == BUF_FLUSH_PAGE_COUNT && cur_ndirty_from_dpl < max_dpl_flush_count);

            tot_ndirty_from_dpl += cur_ndirty_from_dpl;

            if (dirty_count == BUF_FLUSH_PAGE_COUNT && cur_ndirty_from_dpl >= max_dpl_flush_count) {
                continue; /* because, there are still many other old dirty pages */
            }
            if (buf_gl.pgflusher.init == false) break;
        }
#endif

        if ((cur_ndirty_from_lru + cur_ndirty_from_dpl) > 0) {
            if (config->sq.use_directio) {
                /* wait until all write requests are completed */
                do_buf_pgflush_aio_wait(fcbque, BUF_FCB_QUEUE_SIZE);
            }
        }
        cur_ndirty_from_lru = 0;
        cur_ndirty_from_dpl = 0;

        /* sleep */
        do_buf_page_flusher_sleep();

        flush_execs += 1;
        if ((tot_ndirty_from_lru + tot_ndirty_from_dpl) >= 100000) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "page flush thread: ndirty_from_lru=%u, ndirty_from_dpl=%u, exec_count=%u\n",
                        tot_ndirty_from_lru, tot_ndirty_from_dpl, flush_execs);
            tot_ndirty_from_lru = 0;
            tot_ndirty_from_dpl = 0;
            flush_execs = 0;
        }
    }
    if ((cur_ndirty_from_lru + cur_ndirty_from_dpl) > 0) {
        if (config->sq.use_directio) {
            /* wait until all write requests are completed */
            do_buf_pgflush_aio_wait(fcbque, BUF_FCB_QUEUE_SIZE);
        }
    }
    do_buf_free_fcb_queue(fcbque);
    //fprintf(stderr, "page flush thread terminated\n");
    buf_gl.pgflusher.start = false;
    return NULL;
}

static void do_buf_page_flusher_init(void)
{
    pthread_mutex_init(&buf_gl.pgflusher.lock, NULL);
    pthread_cond_init(&buf_gl.pgflusher.cond, NULL);
    buf_gl.pgflusher.sleep = false;
    buf_gl.pgflusher.start = false;
    buf_gl.pgflusher.init = false;
}

static int do_buf_page_flusher_start(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.
    pthread_t tid;
    int       ret;

    /* start request */
    buf_gl.pgflusher.init = true;

    /* create page flush thread */
    ret = pthread_create(&tid, NULL, page_flush_thread, NULL);
    if (ret != 0) {
        buf_gl.pgflusher.init = false;
        fprintf(stderr, "Can't create page flush thread: %s\n", strerror(ret));
        return -1;
    }
    /* wait until the page flush thread starts */
    while (buf_gl.pgflusher.start == false) {
        nanosleep(&sleep_time, NULL);
    }
    return 0;
}

static void do_buf_page_flusher_stop(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    if (buf_gl.pgflusher.init == true) {
        /* stop request */
        pthread_mutex_lock(&buf_gl.pgflusher.lock);
        buf_gl.pgflusher.init = false;
        pthread_mutex_unlock(&buf_gl.pgflusher.lock);

        /* wait until page flush thread stops */
        while (buf_gl.pgflusher.start == true) {
            if (buf_gl.pgflusher.sleep == true) {
                do_buf_page_flusher_wakeup();
            }
            nanosleep(&sleep_time, NULL);
        }
    }
}

/* Page Buffer Manager Init and Final */
static void do_buf_global_init(void)
{
    buf_gl.ini_nbuffer = config->sq.data_buffer_count;
    if (buf_gl.ini_nbuffer < BUF_MIN_PAGE_COUNT)
        buf_gl.ini_nbuffer = BUF_MIN_PAGE_COUNT;
    buf_gl.max_nbuffer = config->sq.data_buffer_count;
    if (buf_gl.max_nbuffer < buf_gl.ini_nbuffer)
        buf_gl.max_nbuffer = buf_gl.ini_nbuffer;
    buf_gl.page_size = config->sq.data_page_size;

    /***************************/
    /* set the flush threshold */
    /***************************/
    /* flush threshold range must be between 0 and 10 */
    //uint32_t flush_threshold_range = 0;
    //uint32_t flush_threshold_range = 1; /* 10.5% ~ 9.5% */
    uint32_t flush_threshold_range = 4; /* 12% ~ 8% */
    assert(flush_threshold_range <= 10);
    if (flush_threshold_range == 0) {
        /* single threshold */
        buf_gl.HWM_clean = buf_gl.max_nbuffer / 10;  // (1/10) = 10%
        buf_gl.LWM_clean = buf_gl.max_nbuffer / 10;  // (1/10) = 10%
    } else {
        /* range threshold : HWM ~ LWM */
        uint32_t bufcnt_10_percent = buf_gl.max_nbuffer / 10; // 10%
        uint32_t bufcnt_half_range = (bufcnt_10_percent*flush_threshold_range) / (10*2);
        buf_gl.HWM_clean = bufcnt_10_percent + bufcnt_half_range;
        buf_gl.LWM_clean = bufcnt_10_percent - bufcnt_half_range;
    }

    /* # of bits for representing page number */
    uint32_t npage_per_file = config->sq.data_file_size * ((1024*1024) / config->sq.data_page_size);
    uint32_t npage_per_bits = 2;
    buf_gl.pgnum_nbits = 1;
    while (npage_per_bits < npage_per_file) {
        npage_per_bits *= 2;
        buf_gl.pgnum_nbits += 1;
    }

    pthread_mutex_init(&buf_gl.flushall_lock, NULL);

    /* initialize log_amount_lsn : 2 * buffer_size */
    uint32_t log_amount_mb = (buf_gl.page_size/1024) * (2*buf_gl.max_nbuffer/1024);
    //uint32_t log_amount_mb = 5 * 1024; // 5 GB
    buf_gl.log_amount_lsn.filenum = (log_amount_mb / config->sq.log_file_size);
    buf_gl.log_amount_lsn.roffset = (log_amount_mb % config->sq.log_file_size) * 1024 * 1024;

    /* flush by checkpoint task */
    buf_gl.chkpt_flush = false;
}

static void do_buf_struct_final()
{
#ifdef SQUALL_USE_MMAP
    if (buf_gl.page_tab != MAP_FAILED) {
        munmap((void*)buf_gl.page_tab, buf_gl.max_nbuffer*buf_gl.page_size);
        buf_gl.page_tab = MAP_FAILED;
    }
    if (buf_gl.bcb_tab != MAP_FAILED) {
        munmap((void*)buf_gl.bcb_tab, buf_gl.max_nbuffer*sizeof(buf_BCB));
        buf_gl.bcb_tab = MAP_FAILED;
    }
    if (buf_gl.page_buf != MAP_FAILED) {
        munmap((void*)buf_gl.page_buf, buf_gl.max_pbufs*buf_gl.page_size);
        buf_gl.page_buf = MAP_FAILED;
    }
    if (buf_gl.hash_tab != MAP_FAILED) {
        munmap((void*)buf_gl.hash_tab, buf_gl.hash_size*sizeof(buf_HASH));
        buf_gl.hash_tab = MAP_FAILED;
    }
#else
    if (buf_gl.page_tab != NULL) {
        free((void*)buf_gl.page_tab);
        buf_gl.page_tab = NULL;
    }
    if (buf_gl.bcb_tab != NULL) {
        free((void*)buf_gl.bcb_tab);
        buf_gl.bcb_tab = NULL;
    }
    if (buf_gl.page_buf != NULL) {
        free((void*)buf_gl.page_buf);
        buf_gl.page_buf = NULL;
    }
    if (buf_gl.hash_tab != NULL) {
        free((void*)buf_gl.hash_tab);
        buf_gl.hash_tab = NULL;
    }
#endif
    if (buf_gl.lawt_tab != NULL) {
        free((void*)buf_gl.lawt_tab);
        buf_gl.lawt_tab = NULL;
    }
    if (buf_gl.pglk_tab != NULL) {
        free((void*)buf_gl.pglk_tab);
        buf_gl.pglk_tab = NULL;
    }
    if (buf_gl.fcbq_tab != NULL) {
        free((void*)buf_gl.fcbq_tab);
        buf_gl.fcbq_tab = NULL;
    }
    if (buf_gl.pbuf_tab != NULL) {
        free((void*)buf_gl.pbuf_tab);
        buf_gl.pbuf_tab = NULL;
    }
    if (buf_gl.zero_filled_page != NULL) {
        free(buf_gl.zero_filled_page);
        buf_gl.zero_filled_page = NULL;
    }
}

static int do_buf_struct_init(void)
{
    int ret = 0;

#ifdef SQUALL_USE_MMAP
    buf_gl.page_tab = MAP_FAILED;
    buf_gl.bcb_tab  = MAP_FAILED;
    buf_gl.page_buf = MAP_FAILED;
    buf_gl.hash_tab = MAP_FAILED;
#else
    buf_gl.page_tab = NULL;
    buf_gl.bcb_tab  = NULL;
    buf_gl.page_buf = NULL;
    buf_gl.hash_tab = NULL;
#endif
    buf_gl.lawt_tab = NULL;
    buf_gl.pglk_tab = NULL;
    buf_gl.fcbq_tab = NULL;
    buf_gl.pbuf_tab = NULL;
    buf_gl.zero_filled_page = NULL;

    do {
        if ((ret = do_buf_init_bcb_list()) != 0) {
            break;
        }
        if ((ret = do_buf_init_pbuf_list(config->num_threads)) != 0) {
            break;
        }
        if ((ret = do_buf_init_wait_list(config->num_threads)) != 0) {
            break;
        }
        if ((ret = do_buf_init_hash_table()) != 0) {
            break;
        }
        if ((ret = do_buf_init_zero_filled_page()) != 0) {
            break;
        }
    } while(0);

    if (ret != 0) {
        do_buf_struct_final();
    }
    return ret;
}

/*
 * External Functions
 */
int buf_mgr_init(void *conf, void *srvr)
{
    config = (struct config *)conf;
    server = (SERVER_HANDLE_V1 *)srvr;
    logger = server->log->get_logger();

    buf_gl.initialized = false;

    /* validation check */
    if ((long)config->sq.data_page_size % sysconf(_SC_PAGESIZE)) {
        fprintf(stderr, "buffer manager - page size mismatch \n");
        return -1; /* page size problem */
    }

    /* initialize buffer manager */
    do_buf_global_init();
    if (do_buf_struct_init() != 0) {
        return -1;
    }
    do_buf_stat_init();
    do_buf_page_flusher_init();

    buf_gl.initialized = true;
    return 0;
}

void buf_mgr_final(void)
{
    do_buf_struct_final();
}

int buf_mgr_page_flusher_start(void)
{
    return do_buf_page_flusher_start();
}

void buf_mgr_page_flusher_stop(void)
{
    do_buf_page_flusher_stop();
}

Page *buf_fix(const PageID pgid, enum buf_latch_mode mode, bool dotry, bool newpg)
{
    buf_HASH *hash;
    buf_BCB  *bcb;
    bool      is_buf_hit;
    bool      is_first_fix;

    hash = &buf_gl.hash_tab[do_buf_pgid_hash(pgid)];
    pthread_mutex_lock(&hash->lock);
    do {
        bcb = do_buf_find_bcb(hash, pgid);
        if (bcb != NULL) { /* found */
            is_buf_hit = true;
        } else { /* not found */
            is_buf_hit = false;
            if (dotry) break;
            bcb = do_buf_build_bcb(hash, pgid, newpg);
            if (bcb == NULL) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] buf_fix - build bcb fail\n");
                break;
            }
        }
        if (do_buf_fix_bcb(hash, bcb, mode, dotry, &is_first_fix) != 0) {
            assert(dotry == true);
            bcb = NULL;
        }
    } while(0);
    pthread_mutex_unlock(&hash->lock);

    if (bcb != NULL) {
        if (buf_gl.buf_stat.enable) {
            do_buf_stat_fix(is_buf_hit, is_first_fix);
        }
        return bcb->pgptr;
    } else {
        return NULL;
    }
}

Page *buf_fix_if_exist(const PageID pgid, enum buf_latch_mode mode)
{
    buf_HASH *hash;
    buf_BCB  *bcb;
    bool      is_buf_hit;
    bool      is_first_fix;

    hash = &buf_gl.hash_tab[do_buf_pgid_hash(pgid)];
    pthread_mutex_lock(&hash->lock);
    bcb = do_buf_find_bcb(hash, pgid);
    if (bcb != NULL) { /* found */
        is_buf_hit = true;
        (void)do_buf_fix_bcb(hash, bcb, mode, false, &is_first_fix);
    } else { /* not found */
        is_buf_hit = false;
    }
    pthread_mutex_unlock(&hash->lock);

    if (bcb != NULL) {
        if (buf_gl.buf_stat.enable) {
            do_buf_stat_fix(is_buf_hit, is_first_fix);
        }
        return bcb->pgptr;
    } else {
        return NULL;
    }
}

void buf_unfix(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    bool is_last_unfix;

    pthread_mutex_lock(&bcb->h_root->lock);
    do_buf_unfix_bcb(bcb, true, &is_last_unfix); /* Need to repostion bcb in lru */
    pthread_mutex_unlock(&bcb->h_root->lock);
    if (buf_gl.buf_stat.enable) {
        do_buf_stat_unfix(is_last_unfix);
    }
}

void buf_unfix_invalidate(Page *pgptr)
{
    buf_BCB  *bcb = do_buf_get_bcb(pgptr);
    buf_HASH *hash = bcb->h_root;
    bool is_last_unfix;

    pthread_mutex_lock(&hash->lock);
    assert(bcb->fcnt == 1 && bcb->wcnt == 0 && bcb->dirty == false);
    do_buf_unfix_bcb(bcb, false, &is_last_unfix); /* No need to reposition bcb in lru */
    if (bcb->fcnt == 0 && bcb->mode == BUF_MODE_NONE) {
        buf_BCB *prev = NULL;
        buf_BCB *curr = hash->h_next;
        while (curr != NULL) {
            if (curr == bcb) break;
            prev = curr; curr = curr->h_next;
        }
        assert(curr == bcb);
        if (bcb->zone != BCB_ZONE_VOID) {
            pthread_mutex_lock(&buf_gl.lru_list.lock);
            if (bcb->zone != BCB_ZONE_VOID) {
                do_buf_unlink_from_lru(bcb);
                bcb->zone = BCB_ZONE_VOID;
            }
            pthread_mutex_unlock(&buf_gl.lru_list.lock);
        }
        do_buf_unlink_from_hash(hash, prev, bcb);
        do_buf_free_bcb(bcb);
    }
    pthread_mutex_unlock(&hash->lock);
    if (buf_gl.buf_stat.enable) {
        do_buf_stat_unfix(is_last_unfix);
    }
}

void buf_unlatch_only(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    pthread_mutex_lock(&bcb->h_root->lock);
    do_buf_unlatch_bcb(bcb);
    pthread_mutex_unlock(&bcb->h_root->lock);
}

void buf_relatch_only(Page *pgptr, enum buf_latch_mode mode)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    pthread_mutex_lock(&bcb->h_root->lock);
    do_buf_relatch_bcb(bcb->h_root, bcb, mode);
    pthread_mutex_unlock(&bcb->h_root->lock);
}

void buf_uplatch_only(Page *pgptr, enum buf_latch_mode mode)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    pthread_mutex_lock(&bcb->h_root->lock);
    do_buf_uplatch_bcb(bcb->h_root, bcb, mode);
    pthread_mutex_unlock(&bcb->h_root->lock);
}

void buf_dwlatch_only(Page *pgptr, enum buf_latch_mode mode)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    pthread_mutex_lock(&bcb->h_root->lock);
    do_buf_dwlatch_bcb(bcb->h_root, bcb, mode);
    pthread_mutex_unlock(&bcb->h_root->lock);
}

Page *buf_pin_only(const PageID pgid, bool dotry, bool newpg)
{
    buf_HASH *hash;
    buf_BCB  *bcb;
    bool      is_buf_hit;
    bool      is_first_fix;

    hash = &buf_gl.hash_tab[do_buf_pgid_hash(pgid)];
    pthread_mutex_lock(&hash->lock);
    do {
        bcb = do_buf_find_bcb(hash, pgid);
        if (bcb != NULL) { /* found */
            is_buf_hit = true;
        } else { /* bcb == NULL: not found */
            is_buf_hit = false;
            if (dotry) break;
            bcb = do_buf_build_bcb(hash, pgid, newpg);
            if (bcb == NULL) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] buf_pin_only - build bcb fail\n");
                break;
            }
        }
        do_buf_pin_bcb(bcb, &is_first_fix);
    } while(0);
    pthread_mutex_unlock(&hash->lock);

    if (bcb != NULL) {
        if (buf_gl.buf_stat.enable) {
            do_buf_stat_fix(is_buf_hit, is_first_fix);
        }
        return bcb->pgptr;
    } else {
        return NULL;
    }
}

void buf_unpin_only(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    bool is_last_unfix;

    pthread_mutex_lock(&bcb->h_root->lock);
    do_buf_unpin_bcb(bcb, &is_last_unfix);
    pthread_mutex_unlock(&bcb->h_root->lock);
    if (buf_gl.buf_stat.enable) {
        do_buf_stat_unfix(is_last_unfix);
    }
}

Page *buf_permanent_pin(const PageID pgid, bool newpg)
{
    buf_HASH *hash;
    buf_BCB  *bcb;

    hash = &buf_gl.hash_tab[do_buf_pgid_hash(pgid)];
    pthread_mutex_lock(&hash->lock);
    bcb = do_buf_find_bcb(hash, pgid);
    if (bcb == NULL) {
        if (newpg) {
            /* when new page is pinned after server started */
            if (do_buf_perm_list_isfull() == false) {
                bcb = do_buf_build_bcb(hash, pgid, newpg);
                assert(bcb != NULL);
            }
        } else {
            /* when old page is pinned at server restart time */
            assert(do_buf_perm_list_isfull() == false);
            bcb = do_buf_build_bcb(hash, pgid, newpg);
            /* The bcb can be NULL
             * when disk page image is absent.
             */
        }
    }
    if (bcb != NULL) {
        assert(bcb->pcnt == bcb->fcnt);
        bcb->fcnt += 1;
        bcb->pcnt += 1;
        if (bcb->pcnt == 1) {
            do_buf_link_to_perm_list(bcb);
        }
    }
    pthread_mutex_unlock(&hash->lock);

    return (bcb != NULL ? bcb->pgptr : NULL);
}

void buf_permanent_unpin(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);

    assert(bcb->dirty == false);
    pthread_mutex_lock(&bcb->h_root->lock);
    assert(bcb->pcnt > 0 && bcb->pcnt == bcb->fcnt);
    bcb->pcnt -= 1;
    bcb->fcnt -= 1;
    if (bcb->pcnt == 0) {
        do_buf_unlink_from_perm_list(bcb);
        do_buf_free_bcb(bcb);
    }
    pthread_mutex_unlock(&bcb->h_root->lock);
}

void buf_permanent_latch(Page *pgptr, enum buf_latch_mode mode)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);

    /* The fcnt and pcnt hadnling is different from that of buf_relatch_only */
    pthread_mutex_lock(&bcb->h_root->lock);
    if (bcb->mode == BUF_MODE_NONE) {
        bcb->mode = mode;
        bcb->fcnt += 1;
    } else {
        assert(bcb->fcnt > bcb->pcnt);
        if ((mode == BUF_MODE_READ) && (bcb->mode == BUF_MODE_READ && bcb->wcnt == 0)) {
            /* latch mode is already set. so, do nothing */
            bcb->fcnt += 1;
        } else {
            /* mode == BUF_MODE_WRITE: latch conflict, wait until the latch is released */
            /* bcb->mode == BUF_MODE_WRITE: wait until the writer unfixes the bcb */
            /* bcb->wcnt >= 1: some writer is blocked, so wait behind the writer */
            /* block the requester */
            do_buf_block_request(bcb->h_root, bcb, mode, BUF_PIN_LATCH, BUF_PRIO_LOW);
        }
    }
    pthread_mutex_unlock(&bcb->h_root->lock);
}

void buf_permanent_unlatch(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);

    pthread_mutex_lock(&bcb->h_root->lock);
    bcb->fcnt -= 1;
    if (bcb->fcnt == bcb->pcnt) {
        bcb->mode = BUF_MODE_NONE;
        if (bcb->wcnt > 0) { /* wakeup blocked request */
            do_buf_wakeup_request(bcb);
        } else {
            assert(bcb->w_head == NULL);
        }
    } else {
        assert(bcb->mode != BUF_MODE_WRITE);
    }
    pthread_mutex_unlock(&bcb->h_root->lock);
}

void buf_invalidate_all(int pfxid)
{
    //buf_FCB_QUEUE *fcbque = do_buf_alloc_fcb_queue();
    //buf_PBUF *pbuf = do_buf_alloc_page_buffer();
    buf_HASH *hash;
    buf_BCB  *bcb, *prev, *next;

    for (int i = 0; i < buf_gl.hash_size; i++) {
        hash = &buf_gl.hash_tab[i];
        if (hash->h_next == NULL) continue;

        pthread_mutex_lock(&hash->lock);
        prev = NULL;
        bcb = hash->h_next;
        while (bcb != NULL) {
            next = bcb->h_next;
            if (pfxid == -1 || pfxid == PREFIXID_FROM_PAGEID(&bcb->pgid)) {
                if (bcb->fcnt > 0 || bcb->wcnt > 0) {
                    bool is_first_fix, is_last_unfix;
                    /* wait until the bcb is not used by others */
                    (void)do_buf_fix_bcb(hash, bcb, BUF_MODE_WRITE, false, &is_first_fix);
                    assert(is_first_fix == false);
                    do_buf_unfix_bcb(bcb, false, &is_last_unfix);
                    if (buf_gl.buf_stat.enable) {
                        do_buf_stat_unfix(is_last_unfix);
                    }
                    assert(bcb->fcnt == 0 && bcb->wcnt == 0);
                }
                if (bcb->zone != BCB_ZONE_VOID) {
                    pthread_mutex_lock(&buf_gl.lru_list.lock);
                    if (bcb->zone != BCB_ZONE_VOID) {
                        do_buf_unlink_from_lru(bcb);
                        bcb->zone = BCB_ZONE_VOID;
                    }
                    pthread_mutex_unlock(&buf_gl.lru_list.lock);
                }
                /*******
                if (bcb->dirty == true && bcb->flush == false) {
                    do_buf_flush_bcb(hash, bcb, pbuf->buff);
                }
                ********/
                do_buf_unlink_from_hash(hash, prev, bcb);
                do_buf_free_bcb(bcb);
            } else {
                prev = bcb;
            }
            bcb = next;
        }
        pthread_mutex_unlock(&hash->lock);
    }

    //do_buf_free_fcb_queue(fcbque);
    //do_buf_free_page_buffer(pbuf);
}

static void do_buf_flush_all_slow(int pfxid)
{
    buf_HASH *hash;
    buf_BCB  *bcb;
    buf_PBUF *pbuf = do_buf_alloc_page_buffer();

    for (int i = 0; i < buf_gl.hash_size; i++) {
        hash = &buf_gl.hash_tab[i];
        if (hash->h_next == NULL) continue;

        pthread_mutex_lock(&hash->lock);
        bcb = hash->h_next;
        while (bcb != NULL) {
            if ((bcb->dirty == true) && (pfxid == -1 || pfxid == PREFIXID_FROM_PAGEID(&bcb->pgid))) {
                do_buf_flush_bcb(hash, bcb, pbuf->buff);
            }
            bcb = bcb->h_next;
        }
        pthread_mutex_unlock(&hash->lock);
    }

    do_buf_free_page_buffer(pbuf);
}

#ifdef FLUSH_ALL_MULTI_THREADS
static void *buf_flush_all_thread(void *arg)
{
    int thrnum = *(int*)arg;
#if 0 // static partioning
    uint32_t part_size = (buf_gl.hash_size-1) / BUF_MAX_IO_THREAD;
#endif
    uint32_t part_size = 256;
    uint32_t min, max, i;
    uint32_t total_count = 0;
    uint32_t flush_count = 0;
    buf_HASH *hash;
    buf_BCB  *bcb;
    buf_PBUF *pbuf;

    assert(thrnum < BUF_MAX_IO_THREAD);

#if 0 // static partioning
    min = part_size * thrnum;
    if (thrnum < (BUF_MAX_IO_THREAD-1)) {
        max = part_size * (thrnum+1);
    } else {
        max = buf_gl.hash_size;
    }
#endif

    if (config->verbose >= 2)
        fprintf(stderr, "flush_all[%02d] begin\n", thrnum);
    pbuf = do_buf_alloc_page_buffer();
    while (1) {
        min = buf_gl.hash_size;
        pthread_mutex_lock(&buf_gl.flushall_lock);
        if (buf_gl.flushall_hval < buf_gl.hash_size) {
            min = buf_gl.flushall_hval;
            buf_gl.flushall_hval += part_size;
        }
        pthread_mutex_unlock(&buf_gl.flushall_lock);
        if (min >= buf_gl.hash_size) break;

        max = min + part_size;
        if (max > buf_gl.hash_size)
            max = buf_gl.hash_size;

        for (i = min; i < max; i++) {
            hash = &buf_gl.hash_tab[i];
            if (hash->h_next == NULL) continue;

            pthread_mutex_lock(&hash->lock);
            bcb = hash->h_next;
            while (bcb != NULL) {
                total_count += 1;
                if (bcb->dirty == true) {
                    flush_count += 1;
                    do_buf_flush_bcb(hash, bcb, pbuf->buff);
                }
                bcb = bcb->h_next;
            }
            pthread_mutex_unlock(&hash->lock);
        }
    }
    do_buf_free_page_buffer(pbuf);
    if (config->verbose >= 2)
        fprintf(stderr, "flush_all[%02d] end - flush(%u/%u)\n", thrnum, flush_count, total_count);
    return NULL;
}

static void do_buf_flush_all_fast(void)
{
    pthread_t tid[BUF_MAX_IO_THREAD];
    int thrnum[BUF_MAX_IO_THREAD];
    int thrcnt, i, ret;

    /* initialize flushall_hval to 0. */
    buf_gl.flushall_hval = 0;

    /* create flush threads */
    for (thrcnt = 0; thrcnt < BUF_MAX_IO_THREAD; thrcnt++) {
        thrnum[thrcnt] = thrcnt;
        ret = pthread_create(&tid[thrcnt], NULL, buf_flush_all_thread, &thrnum[thrcnt]);
        if (ret != 0) {
            fprintf(stderr, "Can't create flush_all_thread[%d] : %s\n", thrcnt, strerror(ret));
            break;
        }
    }
    if (thrcnt > 0) {
        for (i = 0; i < thrcnt; i++) {
            pthread_join(tid[i], NULL);
        }
    } else {
        do_buf_flush_all_slow(-1);
    }
}
#else // USE_POSIX_AIO
static void do_buf_flush_all_aio(void)
{
    buf_FCB_QUEUE *fcbque = do_buf_alloc_fcb_queue();
    buf_HASH *hash;
    buf_BCB  *bcb;
    int i, nprepared=0;

    /* This functions is only called when system is shutting down */
    /* fast flush method => high throughput */

#if 1 // hash table scanning
    for (i = 0; i < buf_gl.hash_size; i++) {
        hash = &buf_gl.hash_tab[i];
        if (hash->h_next == NULL) continue;

        if (config->sq.use_directio) {
            do_buf_pgflush_aio_wait(fcbque, hash->h_leng);
        }
        pthread_mutex_lock(&hash->lock);
        bcb = hash->h_next;
        while (bcb != NULL) {
            if (bcb->dirty == true) {
                if (bcb->flush == true) {
                    /* wait until the first flusher flushes the bcb */
                    do_buf_block_flush(hash, bcb);
                } else {
                    do_buf_pgflush_prepare(fcbque, hash, bcb);
                    nprepared += 1;
                }
            }
            bcb = bcb->h_next;
        }
        pthread_mutex_unlock(&hash->lock);

        if ((nprepared >= BUF_FLUSH_PAGE_COUNT) || (i == (buf_gl.hash_size-1) && nprepared > 0)) {
            if (config->sq.use_recovery) {
                do_buf_pgflush_log_wal(fcbque);
            }
            if (config->sq.use_directio) {
                do_buf_pgflush_aio_write(fcbque);
            } else {
                do_buf_pgflush_commit(fcbque);
            }
            nprepared = 0;
        }
    }
#else
    for (i = 0; i < buf_gl.max_nbuffer; i++) {
        bcb = &buf_gl.bcb_tab[i];
        if (bcb->dirty == false) continue;

        if (config->sq.use_directio) {
            do_buf_pgflush_aio_wait(fcbque, 1);
        }
        hash = bcb->h_root;
        pthread_mutex_lock(&hash->lock);
        if (bcb->dirty == true) {
            if (bcb->flush == true) {
                /* wait until the first flusher flushes the bcb */
                do_buf_block_flush(hash, bcb);
            } else {
                do_buf_pgflush_prepare(fcbque, hash, bcb);
                nprepared += 1;
            }
        }
        pthread_mutex_unlock(&hash->lock);

        if ((nprepared >= BUF_FLUSH_PAGE_COUNT) || (i == (buf_gl.max_nbuffer-1) && nprepared > 0)) {
            if (config->sq.use_recovery) {
                do_buf_pgflush_log_wal(fcbque);
            }
            if (config->sq.use_directio) {
                do_buf_pgflush_aio_write(fcbque);
            } else {
                do_buf_pgflush_commit(fcbque);
            }
            nprepared = 0;
        }
    }
#endif
    if (config->sq.use_directio) {
        do_buf_pgflush_aio_wait(fcbque, BUF_FCB_QUEUE_SIZE);
    }
    do_buf_free_fcb_queue(fcbque);

}
#endif

void buf_flush_all(int pfxid)
{
    time_t bgn_time, end_time;

    bgn_time = time(NULL);
    if (pfxid == -1) {
#ifdef FLUSH_ALL_MULTI_THREADS
        do_buf_flush_all_fast();
#else
        do_buf_flush_all_aio();
#endif
    } else {
        do_buf_flush_all_slow(pfxid);
    }
    end_time = time(NULL);
    logger->log(EXTENSION_LOG_INFO, NULL, "flush_all(%d): exec_time(%"PRIu64" secs).\n",
                pfxid, (uint64_t)(end_time-bgn_time));
#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
    assert(buf_gl.drty_list.dirty_cnt == 0);
#endif
}

void buf_flush_checkpoint(LogSN *last_chkpt_lsn, LogSN *min_oldest_lsn)
{
#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
    do_buf_min_oldest_lsn(min_oldest_lsn);
    logger->log(EXTENSION_LOG_INFO, NULL, "buffer flush by checkpoint: min_oldest_lsn=(%u-%u)\n",
                min_oldest_lsn->filenum, min_oldest_lsn->roffset);
#else
    buf_HASH *hash;
    buf_BCB  *bcb;
    buf_PBUF *pbuf = do_buf_alloc_page_buffer();
    uint32_t total_count = 0;
    uint32_t flush_count = 0;

    /* flush by checkpoint thread */
    /* Pages must be slowly flushed in order to reduce effects on normal processing */

    LOGSN_SET_NULL(min_oldest_lsn);

    buf_gl.chkpt_flush = true;
#if 1 // hash table scanning
    for (int i = 0; i < buf_gl.hash_size; i++) {
        hash = &buf_gl.hash_tab[i];
        if (hash->h_next == NULL) continue;

        pthread_mutex_lock(&hash->lock);
        bcb = hash->h_next;
        while (bcb != NULL) {
            total_count += 1;
            if (bcb->dirty == true) {
                assert(!LOGSN_IS_NULL(&bcb->oldest_lsn));
                if (LOGSN_IS_LE(&bcb->oldest_lsn, last_chkpt_lsn)) {
                    flush_count += 1;
                    do_buf_flush_bcb(hash, bcb, pbuf->buff);
                } else {
                    if (LOGSN_IS_NULL(min_oldest_lsn) || LOGSN_IS_LT(&bcb->oldest_lsn, min_oldest_lsn)) {
                        *min_oldest_lsn = bcb->oldest_lsn;
                    }
                }
            }
            bcb = bcb->h_next;
        }
        pthread_mutex_unlock(&hash->lock);
    }
#else // bcb table scanning
    for (int i = 0; i < buf_gl.max_nbuffer; i++) {
        bcb = &buf_gl.bcb_tab[i];
        if (bcb->dirty == false) continue;

        total_count += 1;
        hash = bcb->h_root;
        pthread_mutex_lock(&hash->lock);
        if (bcb->dirty == true) {
            if (LOGSN_IS_LE(&bcb->oldest_lsn, last_chkpt_lsn)) {
                flush_count += 1;
                do_buf_flush_bcb(hash, bcb, pbuf->buff);
            } else {
                if (LOGSN_IS_NULL(min_oldest_lsn) || LOGSN_IS_LT(&bcb->oldest_lsn, min_oldest_lsn)) {
                    *min_oldest_lsn = bcb->oldest_lsn;
                }
            }
        }
        pthread_mutex_unlock(&hash->lock);
    }
#endif
    buf_gl.chkpt_flush = false;

    do_buf_free_page_buffer(pbuf);

    logger->log(EXTENSION_LOG_INFO, NULL,
                "buffer flush by checkpoint: count=(%u/%u), min_oldest_lsn=(%u-%u)\n",
                flush_count, total_count, min_oldest_lsn->filenum, min_oldest_lsn->roffset);
#endif
}

bool buf_sanity_check(void)
{
    buf_BCB *bcb;
    bool     sane = true;

    for (int i = 0; i < buf_gl.max_nbuffer; i++) {
        bcb = &buf_gl.bcb_tab[i];
        if (bcb->zone == BCB_ZONE_FREE) continue;

        if (bcb->zone == BCB_ZONE_PERM) {
            assert(bcb->fcnt == bcb->pcnt && bcb->pcnt == 1);
        } else {
            assert(bcb->fcnt == 0 && bcb->pcnt == 0);
        }
        assert(bcb->mode == BUF_MODE_NONE);
        assert(bcb->wcnt == 0);
    }
    return sane;
}

int buf_get_latch_mode(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    return (int)bcb->mode;
}

void buf_set_lsn(Page *pgptr, LogSN lsn)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    assert(bcb->fcnt > 0);
    page_set_lsn(pgptr, lsn);
    if (LOGSN_IS_NULL(&bcb->oldest_lsn)) {
        pthread_mutex_lock(&bcb->h_root->lock);
        bcb->oldest_lsn = lsn;
        pthread_mutex_unlock(&bcb->h_root->lock);
    }
}

void buf_set_dirty(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
#if 1 // BUF_SET_DIRTY_REWRITE
    assert(bcb->fcnt > 0);
    if (bcb->dirty == false) {
        if (bcb->zone == BCB_ZONE_COLD_CLEAN) {
            pthread_mutex_lock(&buf_gl.lru_list.lock);
            if (bcb->zone == BCB_ZONE_COLD_CLEAN) {
                do_buf_unlink_from_lru(bcb);
                bcb->zone = BCB_ZONE_VOID;
            }
            pthread_mutex_unlock(&buf_gl.lru_list.lock);
        }
        bcb->dirty = true;
#ifdef ENABLE_ORDERED_DIRTY_PAGE_LIST
        if (config->sq.use_recovery)
            do_buf_link_to_dpl(bcb);
#endif
    }
#else
    if (bcb->dirty == false)
        bcb->dirty = true;
#endif
}

bool buf_is_the_only_writer(Page *pgptr)
{
    buf_BCB *bcb = do_buf_get_bcb(pgptr);
    assert(bcb->mode == BUF_MODE_WRITE);
    return ((bcb->fcnt == 1 && bcb->pcnt == 0) ? true : false);
}

void buf_stat_start(void)
{
    pthread_mutex_lock(&buf_gl.buf_stat.lock);
    do_buf_stat_clear();
    buf_gl.buf_stat.enable = true;
    pthread_mutex_unlock(&buf_gl.buf_stat.lock);
}

void buf_stat_stop(void)
{
    pthread_mutex_lock(&buf_gl.buf_stat.lock);
    buf_gl.buf_stat.enable = false;
    pthread_mutex_unlock(&buf_gl.buf_stat.lock);
}

void buf_stat_get(struct buffer_stat *stats)
{
    uint32_t max_hshchain_size = 0;
    uint32_t sum_hshchain_size = 0;
    uint32_t cur_hshchain_size;
    int hash_bucket = -1;

    /* no buf_stat lock will be acquired */

    for (int i = 0; i < buf_gl.hash_size; i++) {
        cur_hshchain_size = buf_gl.hash_tab[i].h_leng;
        sum_hshchain_size += cur_hshchain_size;
        if (cur_hshchain_size > max_hshchain_size) {
            max_hshchain_size = cur_hshchain_size;
            hash_bucket = i;
        }
    }
#if 0 /* print the longest hash chain */
    if (hash_bucket != -1) {
        /* print the pageids in the longest hash chain */
        buf_HASH *hash = &buf_gl.hash_tab[hash_bucket];
        buf_BCB  *bcb;
        fprintf(stderr, "print pageids in the longest hash chain\n");
        pthread_mutex_lock(&hash->lock);
        bcb = hash->h_next;
        while (bcb != NULL) {
            fprintf(stderr, "pageid (%d|%d|%d)\n", bcb->pgid.pfxid, bcb->pgid.flnum, bcb->pgid.pgnum);
            bcb = bcb->h_next;
        }
        pthread_mutex_unlock(&hash->lock);
    }
#endif
    stats->total_fix_count = buf_gl.buf_stat.stat.total_fix_count;
    stats->total_hit_count = buf_gl.buf_stat.stat.total_hit_count;
    stats->fixed_buffer_count = buf_gl.buf_stat.stat.fixed_buffer_count;
    stats->buffer_free_count = buf_gl.free_list.count;
    stats->buffer_used_count = buf_gl.max_nbuffer - stats->buffer_free_count;
    stats->hash_table_size = buf_gl.hash_size;
    stats->hash_chain_max_size = max_hshchain_size;
    stats->lru_perm_count = buf_gl.perm_list.ccnt;
    stats->lru_warm_count = buf_gl.lru_list.nwarm;
    stats->lru_middle_count = buf_gl.lru_list.nmiddle;
    stats->lru_cold_dirty_count = buf_gl.lru_list.ndirty;
    stats->lru_cold_clean_count = buf_gl.lru_list.nclean;
    stats->buffer_replace_count = buf_gl.buf_stat.stat.buffer_replace_count;
    stats->disk_read_count = buf_gl.buf_stat.stat.disk_read_count;
    stats->disk_write_count = buf_gl.buf_stat.stat.disk_write_count;
}

void buf_stat_reset(void)
{
    pthread_mutex_lock(&buf_gl.buf_stat.lock);
    do_buf_stat_clear();
    pthread_mutex_unlock(&buf_gl.buf_stat.lock);
}
