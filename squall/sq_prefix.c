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
 * Prefix Management
 */
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/time.h>

#include "squall_config.h"
#include "sq_disk.h"
#include "sq_buffer.h"
#include "sq_prefix.h"
#include "sq_log.h"

/*
 * Prefix Internal Defines and Data Structures
 */

/* data page usage type */
#define PAGE_USAGE_FREE  0
#define PAGE_USAGE_HALF  1
#define PAGE_USAGE_FULL  2

/* free type : total free vs. partial free */
#define FREE_TYPE_TOTAL   0
#define FREE_TYPE_PARTIAL 1
#define FREE_TYPE_COUNT   2

#ifdef USE_WARM_FREE_PAGE
#define WARM_PAGE_MIN_SLOT_SIZE 128
#endif

/* system info : 512 bytes */
typedef struct _system_info {
    char         magic_code[SQUALL_MAGIC_CODELEN]; /* squall engine magic code */
    time_t       db_create_time; /* db_create time */
    time_t       sys_start_time; /* system start time */
    time_t       sys_stop_time;  /* system stop time */
    time_t       sys_flush_time; /* [NOT USED] system flush time */
    uint64_t     sys_epoch;      /* system epoch */
    uint32_t     spc_count;      /* # of prefix spaces */
    uint32_t     pfx_count;      /* # of prefixes */
    uint8_t      rsvd8[448];     /* reserved space */
    //uint8_t      pfx_bitmap[2048];
    //prefix_spac shared_pfxspace;   /* shared prefix space */
} system_info;

/* prefix hash table entry */
typedef struct _prefix_hash {
    pthread_mutex_t     lock;
    prefix_desc        *next; /* hash next */
} prefix_hash;

/* prefix delete queue */
typedef struct _prefix_del_queue {
    prefix_desc        *head;
    prefix_desc        *tail;
    unsigned int        size;
} pfx_DEL_QUEUE;

typedef struct _prefix_gc_thread {
    pthread_mutex_t     lock;
    pthread_cond_t      cond;
    bool                start;
    bool                sleep;
    bool                init;    /* start or stop request */
} pfx_GC_THREAD;

/* prefix global structure */
struct prefix_global {
    pthread_mutex_t spc_lock;    /* only for alloc/free prefix space */
    pthread_mutex_t pfx_lock;    /* only for alloc/free prefix desciptor */

    char            magic_code[SQUALL_MAGIC_CODELEN];
    prefix_space   *space_tab;
    prefix_desc    *desc_tab;
    prefix_hash    *hash_tab;
    void           *page_buf;

    system_info     sysinfo;      /* squall system info */
    uint16_t        num_spces;      /* current # of prefix spaces */
    uint16_t        max_spces;      /* maximum # of prefix spaces */
    uint16_t        num_pfxes;      /* current # of prefixes */
    uint16_t        max_pfxes;      /* maximum # of prefixes */
    int16_t         spc_freeLWM;  /* the first free prefix_space entry */
    int16_t         spc_freeNXT;  /* the first contiguous free prefix_space entry */
    int16_t         pfx_freeLWM;  /* the first free prefix_desc entry */
    int16_t         pfx_freeNXT;  /* the frrst contiguous free prefix_desc entry */
    uint32_t        hash_size;

#ifdef USE_WARM_FREE_PAGE
    uint32_t        page_dv02_size;     /* page size /  2 */
    uint32_t        page_dv04_size;     /* page size /  4 */
    uint32_t        page_dv08_size;     /* page size /  8 */
    uint32_t        page_dv16_size;     /* page size / 16 */
#endif

    /* file space information */
    uint32_t        npage_per_file;     /* # of pages per file */
    uint32_t        npage_per_grow;     /* # of pages per grow */
    uint32_t        page_body_size;     /* normal page body size (unit: Bytes) */
    uint32_t        pmap_body_size;     /* pmap page body size (unit: Bytes) */

    /* page used and free masks */
    uint8_t         bit1_used_mask[8];
    uint8_t         bit1_free_mask[8];

    /* prefix delete queue */
    pfx_DEL_QUEUE   del_queue;
    pfx_GC_THREAD   gc_thread;

    bool            implicit_empty_prefix_delete;
    volatile bool   initialized;
};

static struct config    *config = NULL;
static SERVER_HANDLE_V1 *server = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct prefix_global pfx_gl; /* prefix global data */

//static char *ANCH_FILENAME_FORMAT = "%s/squall_data_anchor";
static char *SQUALL_FILEPATH_FORMAT[SQUALL_FILE_TYPE_COUNT] = {
            "%s/squall_data_anchor_%01d",
            "%s/squall_%05d_data_%05d",
            "%s/squall_%05d_ovfl_%05d",
            "%s/squall_%05d_indx_%05d" };


/*
 * Prefix Private Functions
 */
static inline char *get_file_type_str(uint16_t ftype)
{
    if (ftype == SQUALL_FILE_TYPE_DATA)      return "data";
    else if (ftype == SQUALL_FILE_TYPE_OVFL) return "overflow";
    else if (ftype == SQUALL_FILE_TYPE_INDX) return "index";
    else                                     return "unknown";
}

/*********************************/
/* Page Map(PMAP) Page Functions */
/*********************************/

static void mpage_init(pmap_page *pmapg, PageID mpgid, int partial_bitcnt)
{
    assert(partial_bitcnt == 1 || partial_bitcnt == 0);
    pmap_hdr *pmhdr = &pmapg->hdr;

    LOGSN_SET_NULL(&pmhdr->map_lsn);
    pmhdr->map_pgid = mpgid;
    PAGEID_SET_NULL(&pmhdr->map_prev);
    PAGEID_SET_NULL(&pmhdr->map_next);

    pmhdr->pgtype = SQUALL_PAGE_TYPE_PMAP;
    pmhdr->pbitcnt = partial_bitcnt; /* additional # of bits */
    if (pmhdr->pbitcnt > 0) {
        pmhdr->poffset = pfx_gl.pmap_body_size / 2;
    } else {
        pmhdr->poffset = 0;
    }

    pmhdr->tot_pgs = 0;
    pmhdr->tfr_pgs = pmhdr->pfr_pgs = 0;
    pmhdr->tfr_min = pmhdr->pfr_min = -1;
    pmhdr->tfr_max = pmhdr->pfr_max = -1;

    memset(pmapg->body, 0, pfx_gl.pmap_body_size);
}

static int mpage_grow_space(pmap_page *pmapg, int grow_pgcnt)
{
    assert((grow_pgcnt % 8) == 0);
    int free_pgcnt;

    if (pmapg->hdr.tot_pgs == 0) {
        free_pgcnt = grow_pgcnt - 1;
        /* allocate pmap page */
        pmapg->body[0] |= pfx_gl.bit1_used_mask[0];
    } else {
        free_pgcnt = grow_pgcnt;
    }
    if (pmapg->hdr.tfr_min == -1)
        pmapg->hdr.tfr_min = pmapg->hdr.tot_pgs / 8;
    pmapg->hdr.tfr_max = ((pmapg->hdr.tot_pgs + grow_pgcnt) / 8) - 1;
    pmapg->hdr.tfr_pgs += free_pgcnt;
    pmapg->hdr.tot_pgs += grow_pgcnt;
    assert(pmapg->hdr.tot_pgs <= ((pfx_gl.pmap_body_size*8) / (1+pmapg->hdr.pbitcnt)));

    return free_pgcnt;
}

static inline void do_mpage_increment_free_page_count(pmap_page *pmapg, int mcidx)
{
    if (pmapg->hdr.tfr_pgs == 0) {
        pmapg->hdr.tfr_min = pmapg->hdr.tfr_max = mcidx;
    } else {
        if (mcidx < pmapg->hdr.tfr_min) pmapg->hdr.tfr_min = mcidx;
        if (mcidx > pmapg->hdr.tfr_max) pmapg->hdr.tfr_max = mcidx;
    }
    pmapg->hdr.tfr_pgs += 1;
}

static inline void do_mpage_decrement_free_page_count(pmap_page *pmapg, int mcidx)
{
    pmapg->hdr.tfr_pgs -= 1;
    if (pmapg->hdr.tfr_pgs == 0) {
        pmapg->hdr.tfr_min = pmapg->hdr.tfr_max = -1;
    } else {
        if (pmapg->body[mcidx] == 0xFF) {
            for (mcidx = mcidx+1; mcidx <= pmapg->hdr.tfr_max; mcidx++) {
                if (pmapg->body[mcidx] != 0xFF) break;
            }
            assert(mcidx <= pmapg->hdr.tfr_max);
            pmapg->hdr.tfr_min = mcidx;
        }
    }
}

static inline void do_mpage_increment_partial_page_count(pmap_page *pmapg, int mcidx)
{
    if (pmapg->hdr.pfr_pgs == 0) {
        pmapg->hdr.pfr_min = pmapg->hdr.pfr_max = mcidx;
    } else {
        if (mcidx < pmapg->hdr.pfr_min) pmapg->hdr.pfr_min = mcidx;
        if (mcidx > pmapg->hdr.pfr_max) pmapg->hdr.pfr_max = mcidx;
    }
    pmapg->hdr.pfr_pgs += 1;
}

static inline void do_mpage_decrement_partial_page_count(pmap_page *pmapg, int mcidx)
{
    pmapg->hdr.pfr_pgs -= 1;
    if (pmapg->hdr.pfr_pgs == 0) {
        pmapg->hdr.pfr_min = pmapg->hdr.pfr_max = -1;
    } else {
        if (pmapg->body[mcidx] == 0x00) {
            for (mcidx = mcidx+1; mcidx <= pmapg->hdr.pfr_max; mcidx++) {
                if (pmapg->body[mcidx] != 0x00) break;
            }
            assert(mcidx <= pmapg->hdr.pfr_max);
            pmapg->hdr.pfr_min = mcidx;
        }
    }
}

static void do_mpage_set_used_page(pmap_page *pmapg, int pbidx)
{
    int mcidx = (pbidx / 8);
    int mbidx = (pbidx % 8);

    assert((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00);
    pmapg->body[mcidx] |= pfx_gl.bit1_used_mask[mbidx]; /* set used */

    do_mpage_decrement_free_page_count(pmapg, mcidx);
}

static void do_mpage_set_free_page(pmap_page *pmapg, int pbidx)
{
    int mcidx = (pbidx / 8);
    int mbidx = (pbidx % 8);

    assert((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00);
    pmapg->body[mcidx] &= pfx_gl.bit1_free_mask[mbidx]; /* set free */

    do_mpage_increment_free_page_count(pmapg, mcidx);
}

static int do_mpage_get_partial_page(pmap_page *pmapg)
{
    int mcidx, mbidx;
    int pbidx;

    /* find a partial page and clear the bit of the page */
    mcidx = pmapg->hdr.pfr_min;
    assert(mcidx != -1);
    for (mbidx = 0; mbidx < 8; mbidx++) {
        if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00) {
            pmapg->body[mcidx] &= pfx_gl.bit1_free_mask[mbidx]; /* clear partial */
            break;
        }
    }
    assert(mbidx < 8);

    /* get page bit index in the pmap */
    pbidx = ((mcidx - pmapg->hdr.poffset) * 8) + mbidx;

    do_mpage_decrement_partial_page_count(pmapg, mcidx);

    return pbidx;
}

static bool do_mpage_is_partial_page(pmap_page *pmapg, int pbidx)
{
    int mcidx = (pbidx / 8) + pmapg->hdr.poffset;
    int mbidx = (pbidx % 8);

    return (((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00) ? true : false);
}

static void do_mpage_set_partial_page(pmap_page *pmapg, int pbidx)
{
    int mcidx = (pbidx / 8) + pmapg->hdr.poffset;
    int mbidx = (pbidx % 8);

    assert((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00);
    pmapg->body[mcidx] |= pfx_gl.bit1_used_mask[mbidx]; /* set partial */

    do_mpage_increment_partial_page_count(pmapg, mcidx);
}

static void do_mpage_clear_partial_page(pmap_page *pmapg, int pbidx)
{
    int mcidx = (pbidx / 8) + pmapg->hdr.poffset;
    int mbidx = (pbidx % 8);

    /* clear the bit of given partial page */
    assert((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00);
    pmapg->body[mcidx] &= pfx_gl.bit1_free_mask[mbidx];

    /* decrement # of partial free pages */
    pmapg->hdr.pfr_pgs -= 1;
    do {
        if (pmapg->body[mcidx] != 0x00) {
            /* partial free pages still exist */
            break;
        }
        if (pmapg->hdr.pfr_pgs == 0) {
            pmapg->hdr.pfr_min = pmapg->hdr.pfr_max = -1;
            break;
        }
        if (mcidx == pmapg->hdr.pfr_min) {
            for (mcidx = mcidx+1; mcidx <= pmapg->hdr.pfr_max; mcidx++) {
                if (pmapg->body[mcidx] != 0x00) break;
            }
            assert(mcidx <= pmapg->hdr.pfr_max);
            pmapg->hdr.pfr_min = mcidx;
            break;
        }
        if (mcidx == pmapg->hdr.pfr_max) {
            for (mcidx = mcidx-1; mcidx >= pmapg->hdr.pfr_min; mcidx--) {
                if (pmapg->body[mcidx] != 0x00) break;
            }
            assert(mcidx >= pmapg->hdr.pfr_min);
            pmapg->hdr.pfr_max = mcidx;
            break;
        }
    } while(0);
}

static int mpage_alloc_page(pmap_page *pmapg)
{
    assert(pmapg->hdr.tfr_pgs >= 1);
    int mcidx, mbidx;
    int pbidx; /* page bit index in this pmap */

    /* find an free page and set the bit of the page */
    mcidx = pmapg->hdr.tfr_min;
    assert(mcidx != -1);
    for (mbidx = 0; mbidx < 8; mbidx++) {
        if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00) {
            pmapg->body[mcidx] |= pfx_gl.bit1_used_mask[mbidx]; /* set used */
            break;
        }
    }
    assert(mbidx < 8);

    /* get page bit index in the pmap */
    pbidx = (mcidx * 8) + mbidx;

    do_mpage_decrement_free_page_count(pmapg, mcidx);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        LogSN curr_lsn;
        log_get_write_lsn(&curr_lsn);
        /* NOTE) PageLSN of page map page ********
         * The PageLSN of page map page must be smaller than log write lsn.
         * The log record of log write lsn might be non-existent.
         * So, if we flush those page map pages,
         * it makes assertion fauilure since no log record to flush remains.
         * Refer to the code of log_buffer_flush() function.
         ******************************************/
        if (curr_lsn.roffset > 0) {
            curr_lsn.roffset -= 8;
        } else {
            curr_lsn.filenum -= 1;
            curr_lsn.roffset = config->sq.log_file_size * 1024 * 1024;
        }
        buf_set_lsn((Page*)pmapg, curr_lsn); /* update PageLSN */
#if 0 // OLD_REOCVERY_CODE
        /* page allocation (redo-only) : <pgid> */
        PGAllocLog pgalo_log;
        lrec_pg_alloc_init((LogRec*)&pgalo_log);
        log_record_write((LogRec*)&pgalo_log, (Trans*)itdesc->trx);
        buf_set_lsn((Page*)pmapg, pgalo_log.header.self_lsn); /* update PageLSN */
#endif
    }
#endif

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */

    return pbidx;
}

static void mpage_free_page(pmap_page *pmapg, int pbidx)
{
    int mcidx = pbidx / 8;
    int mbidx = pbidx % 8;

    assert((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00);
    pmapg->body[mcidx] &= pfx_gl.bit1_free_mask[mbidx]; /* set free */

    do_mpage_increment_free_page_count(pmapg, mcidx);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        LogSN curr_lsn;
        log_get_write_lsn(&curr_lsn);
        /* NOTE) PageLSN of page map page ********
         * The PageLSN of page map page must be smaller than log write lsn.
         * The log record of log write lsn might be non-existent.
         * So, if we flush those page map pages,
         * it makes assertion fauilure since no log record to flush remains.
         * Refer to the code of log_buffer_flush() function.
         ******************************************/
        if (curr_lsn.roffset > 0) {
            curr_lsn.roffset -= 8;
        } else {
            curr_lsn.filenum -= 1;
            curr_lsn.roffset = config->sq.log_file_size * 1024 * 1024;
        }
        buf_set_lsn((Page*)pmapg, curr_lsn); /* update PageLSN */
#if 0 // OLD_REOCVERY_CODE
        /* page free (redo-only) : <pgid> */
        PGFreeLog pgfre_log;
        lrec_pg_free_init((LogRec*)&pgfre_log);
        log_record_write((LogRec*)&pgfre_log, (Trans*)itdesc->trx);
        buf_set_lsn((Page*)pmapg, pgfre_log.header.self_lsn); /* update PageLSN */
#endif
    }
#endif

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */
}

static int mpage_alloc_page_bulk(pmap_page *pmapg, int count, int *pbidx)
{
    assert(pmapg->hdr.pbitcnt == 0 && pmapg->hdr.tfr_pgs >= count);
    int mcidx, mbidx;
    int pbcnt = 0;

    /* find an free page and set the bit of the page */
    for (mcidx = pmapg->hdr.tfr_min; mcidx <= pmapg->hdr.tfr_max; mcidx++) {
        if (pmapg->body[mcidx] == 0xFF) continue;
        for (mbidx = 0; mbidx < 8; mbidx++) {
            if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00) {
                pmapg->body[mcidx] |= pfx_gl.bit1_used_mask[mbidx];
                pbidx[pbcnt++] = (mcidx * 8) + mbidx;
                if (pbcnt >= count) break;
            }
        }
        if (pbcnt >= count) break;
    }
    assert(mcidx <= pmapg->hdr.tfr_max);

    /* decrement # of free pages */
    pmapg->hdr.tfr_pgs -= pbcnt;
    if (pmapg->hdr.tfr_pgs == 0) {
        pmapg->hdr.tfr_min = pmapg->hdr.tfr_max = -1;
    } else {
        if (pmapg->body[mcidx] == 0xFF) {
            for (mcidx = mcidx+1; mcidx <= pmapg->hdr.tfr_max; mcidx++) {
                if (pmapg->body[mcidx] != 0xFF) break;
            }
            assert(mcidx <= pmapg->hdr.tfr_max);
            pmapg->hdr.tfr_min = mcidx;
        } else {
            if (pmapg->hdr.tfr_min < mcidx)
                pmapg->hdr.tfr_min = mcidx;
        }
    }

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        LogSN curr_lsn;
        log_get_write_lsn(&curr_lsn);
        /* NOTE) PageLSN of page map page ********
         * The PageLSN of page map page must be smaller than log write lsn.
         * The log record of log write lsn might be non-existent.
         * So, if we flush those page map pages,
         * it makes assertion fauilure since no log record to flush remains.
         * Refer to the code of log_buffer_flush() function.
         ******************************************/
        if (curr_lsn.roffset > 0) {
            curr_lsn.roffset -= 8;
        } else {
            curr_lsn.filenum -= 1;
            curr_lsn.roffset = config->sq.log_file_size * 1024 * 1024;
        }
        buf_set_lsn((Page*)pmapg, curr_lsn); /* update PageLSN */
#if 0 // OLD_REOCVERY_CODE
        /* page allocation (redo-only) : <pgid> */
        PGAllocLog pgalo_log;
        lrec_pg_alloc_init((LogRec*)&pgalo_log);
        log_record_write((LogRec*)&pgalo_log, (Trans*)itdesc->trx);
        buf_set_lsn((Page*)pmapg, pgalo_log.header.self_lsn); /* update PageLSN */
#endif
    }
#endif

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */

    return pbcnt;
}

static void mpage_free_page_bulk(pmap_page *pmapg, int count, int *pbidx)
{
    assert(pmapg->hdr.pbitcnt == 0);
    int mcidx, mbidx;
    int min_mcidx;
    int max_mcidx;

    /* clear the bits of given pages */
    mcidx = pbidx[0] / 8;
    mbidx = pbidx[0] % 8;
    assert((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00);
    pmapg->body[mcidx] &= pfx_gl.bit1_free_mask[mbidx];
    min_mcidx = mcidx;
    max_mcidx = mcidx;

    for (int i = 1; i < count; i++) {
        mcidx = pbidx[i] / 8;
        mbidx = pbidx[i] % 8;
        assert((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00);
        pmapg->body[mcidx] &= pfx_gl.bit1_free_mask[mbidx];
        if (mcidx < min_mcidx) min_mcidx = mcidx;
        if (mcidx > max_mcidx) max_mcidx = mcidx;
    }

    /* increment # of free pages */
    if (pmapg->hdr.tfr_pgs == 0) {
        pmapg->hdr.tfr_min = min_mcidx;
        pmapg->hdr.tfr_max = max_mcidx;
    } else {
        if (min_mcidx < pmapg->hdr.tfr_min) pmapg->hdr.tfr_min = min_mcidx;
        if (max_mcidx > pmapg->hdr.tfr_max) pmapg->hdr.tfr_max = max_mcidx;
    }
    pmapg->hdr.tfr_pgs += count;

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        LogSN curr_lsn;
        log_get_write_lsn(&curr_lsn);
        /* NOTE) PageLSN of page map page ********
         * The PageLSN of page map page must be smaller than log write lsn.
         * The log record of log write lsn might be non-existent.
         * So, if we flush those page map pages,
         * it makes assertion fauilure since no log record to flush remains.
         * Refer to the code of log_buffer_flush() function.
         ******************************************/
        if (curr_lsn.roffset > 0) {
            curr_lsn.roffset -= 8;
        } else {
            curr_lsn.filenum -= 1;
            curr_lsn.roffset = config->sq.log_file_size * 1024 * 1024;
        }
        buf_set_lsn((Page*)pmapg, curr_lsn); /* update PageLSN */
#if 0 // OLD_REOCVERY_CODE
        /* page free (redo-only) : <pgid> */
        PGFreeLog pgfre_log;
        lrec_pg_free_init((LogRec*)&pgfre_log);
        log_record_write((LogRec*)&pgfre_log, (Trans*)itdesc->trx);
        buf_set_lsn((Page*)pmapg, pgfre_log.header.self_lsn); /* update PageLSN */
#endif
    }
#endif

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */
}

static int mpage_get_partial_page(pmap_page *pmapg)
{
    assert(pmapg->hdr.pbitcnt > 0 && pmapg->hdr.pfr_pgs > 0);
    int pbidx = do_mpage_get_partial_page(pmapg);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        LogSN curr_lsn;
        log_get_write_lsn(&curr_lsn);
        /* NOTE) PageLSN of page map page ********
         * The PageLSN of page map page must be smaller than log write lsn.
         * The log record of log write lsn might be non-existent.
         * So, if we flush those page map pages,
         * it makes assertion fauilure since no log record to flush remains.
         * Refer to the code of log_buffer_flush() function.
         ******************************************/
        if (curr_lsn.roffset > 0) {
            curr_lsn.roffset -= 8;
        } else {
            curr_lsn.filenum -= 1;
            curr_lsn.roffset = config->sq.log_file_size * 1024 * 1024;
        }
        buf_set_lsn((Page*)pmapg, curr_lsn); /* update PageLSN */
#if 0 // OLD_REOCVERY_CODE
        /* page allocation (redo-only) : <pgid> */
        PGAllocLog pgalo_log;
        lrec_pg_alloc_init((LogRec*)&pgalo_log);
        log_record_write((LogRec*)&pgalo_log, (Trans*)itdesc->trx);
        buf_set_lsn((Page*)pmapg, pgalo_log.header.self_lsn); /* update PageLSN */
#endif
    }
#endif

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */

    return pbidx;
}

static void mpage_set_partial_page(pmap_page *pmapg, int pbidx)
{
    assert(pmapg->hdr.pbitcnt > 0);
    do_mpage_set_partial_page(pmapg, pbidx);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        LogSN curr_lsn;
        log_get_write_lsn(&curr_lsn);
        /* NOTE) PageLSN of page map page ********
         * The PageLSN of page map page must be smaller than log write lsn.
         * The log record of log write lsn might be non-existent.
         * So, if we flush those page map pages,
         * it makes assertion fauilure since no log record to flush remains.
         * Refer to the code of log_buffer_flush() function.
         ******************************************/
        if (curr_lsn.roffset > 0) {
            curr_lsn.roffset -= 8;
        } else {
            curr_lsn.filenum -= 1;
            curr_lsn.roffset = config->sq.log_file_size * 1024 * 1024;
        }
        buf_set_lsn((Page*)pmapg, curr_lsn); /* update PageLSN */
#if 0 // OLD_REOCVERY_CODE
        /* page free (redo-only) : <pgid> */
        PGFreeLog pgfre_log;
        lrec_pg_free_init((LogRec*)&pgfre_log);
        log_record_write((LogRec*)&pgfre_log, (Trans*)itdesc->trx);
        buf_set_lsn((Page*)pmapg, pgfre_log.header.self_lsn); /* update PageLSN */
#endif
    }
#endif

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */
}

#ifdef ENABLE_RECOVERY
static int mpage_check_and_set_used(pmap_page *pmapg, int pbidx, int new_pgspace)
{
    int mcidx = pbidx / 8;
    int mbidx = pbidx % 8;
    int old_pgspace;

    if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00) {
        /* The given page was in state of free page */
        old_pgspace = PAGE_SPACE_FREE;
        pmapg->body[mcidx] |= pfx_gl.bit1_used_mask[mbidx];

        /* decrement # of free pages */
        pmapg->hdr.tfr_pgs -= 1;
        do {
            if (pmapg->body[mcidx] != 0xFF) {
                /* free pages still exist in the character */
                break;
            }
            if (pmapg->hdr.tfr_pgs == 0) {
                pmapg->hdr.tfr_min = pmapg->hdr.tfr_max = -1;
                break;
            }
            if (mcidx == pmapg->hdr.tfr_min) {
                for (mcidx = mcidx+1; mcidx <= pmapg->hdr.tfr_max; mcidx++) {
                    if (pmapg->body[mcidx] != 0xFF) break;
                }
                assert(mcidx <= pmapg->hdr.tfr_max);
                pmapg->hdr.tfr_min = mcidx;
                break;
            }
            if (mcidx == pmapg->hdr.tfr_max) {
                for (mcidx = mcidx-1; mcidx >= pmapg->hdr.tfr_min; mcidx--) {
                    if (pmapg->body[mcidx] != 0xFF) break;
                }
                assert(mcidx >= pmapg->hdr.tfr_min);
                pmapg->hdr.tfr_max = mcidx;
                break;
            }
        } while(0);

        if (new_pgspace == PAGE_SPACE_HALF) {
            /* must set as partial free page */
            assert(pmapg->hdr.pbitcnt > 0);
            do_mpage_set_partial_page(pmapg, pbidx);
        }
    } else {
        /* The given page was in state of used page */
        if (pmapg->hdr.poffset > 0) { /* data page */
            if (do_mpage_is_partial_page(pmapg, pbidx)) {
                old_pgspace = PAGE_SPACE_HALF;
                if (new_pgspace == PAGE_SPACE_FULL) {
                    do_mpage_clear_partial_page(pmapg, pbidx);
                }
            } else {
                old_pgspace = PAGE_SPACE_FULL;
                if (new_pgspace == PAGE_SPACE_HALF) {
                    do_mpage_set_partial_page(pmapg, pbidx);
                }
            }
        } else { /* other pages */
            assert(new_pgspace == PAGE_SPACE_FULL);
            old_pgspace = PAGE_SPACE_FULL;
        }
    }
    return old_pgspace;
}

static int mpage_check_and_set_free(pmap_page *pmapg, int pbidx)
{
    int mcidx = pbidx / 8;
    int mbidx = pbidx % 8;
    int old_pgspace;

    if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00) {
        /* The given page was in state of free page */
        old_pgspace = PAGE_SPACE_FREE;
    } else {
        /* free the page */
        pmapg->body[mcidx] &= pfx_gl.bit1_free_mask[mbidx];

        /* increment # of free pages */
        if (pmapg->hdr.tfr_pgs == 0) {
            pmapg->hdr.tfr_min = pmapg->hdr.tfr_max = mcidx;
        } else {
            if (mcidx < pmapg->hdr.tfr_min) pmapg->hdr.tfr_min = mcidx;
            if (mcidx > pmapg->hdr.tfr_max) pmapg->hdr.tfr_max = mcidx;
        }
        pmapg->hdr.tfr_pgs += 1;

        if (pmapg->hdr.pbitcnt > 0 && do_mpage_is_partial_page(pmapg, pbidx) == true) {
            do_mpage_clear_partial_page(pmapg, pbidx);
            old_pgspace = PAGE_SPACE_HALF;
        } else {
            old_pgspace = PAGE_SPACE_FULL;
        }
    }
    return old_pgspace;
}
#endif

static bool mpage_check_consistency(pmap_page *pmapg)
{
    int mcidx, mbidx;
    int min_mcidx;
    int max_mcidx;
    int used_pgcnt;
    int free_pgcnt;

    if ((pmapg->body[0] & pfx_gl.bit1_used_mask[0]) == 0x00) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: pmap page is not used page\n");
        return false;
    }
    if ((pmapg->hdr.tot_pgs % 8) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: total pages is not multiple of 8 pages\n");
        return false;
    }

    min_mcidx = max_mcidx = -1;
    free_pgcnt = 0;
    for (mcidx = 0; mcidx < (pmapg->hdr.tot_pgs/8); mcidx++) {
        used_pgcnt = 0;
        for (mbidx = 0; mbidx < 8; mbidx++) {
            if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00)
                free_pgcnt += 1;
            else
                used_pgcnt += 1;
        }
        if (used_pgcnt < 8) {
            if (min_mcidx == -1) min_mcidx = mcidx;
            max_mcidx = mcidx;
        }
    }
    if (free_pgcnt != pmapg->hdr.tfr_pgs) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: free page count mismatch\n");
        return false;
    }
    if (min_mcidx != pmapg->hdr.tfr_min) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: min free character mismatch\n");
        return false;
    }
    if (max_mcidx != pmapg->hdr.tfr_max) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: max free character mismatch\n");
        return false;
    }

    min_mcidx = max_mcidx = -1;
    used_pgcnt = 0;
    if (pmapg->hdr.pbitcnt > 0) {
        for (mcidx = pmapg->hdr.poffset; mcidx < (pmapg->hdr.poffset + (pmapg->hdr.tot_pgs/8)); mcidx++) {
            free_pgcnt = 0;
            for (mbidx = 0; mbidx < 8; mbidx++) {
                if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) == 0x00)
                    free_pgcnt += 1;
                else
                    used_pgcnt += 1;
            }
            if (free_pgcnt < 8) {
                if (min_mcidx == -1) min_mcidx = mcidx;
                max_mcidx = mcidx;
            }
        }
    }
    if (used_pgcnt != pmapg->hdr.pfr_pgs) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: partial page count mismatch\n");
        return false;
    }
    if (min_mcidx != pmapg->hdr.pfr_min) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: min partial character mismatch\n");
        return false;
    }
    if (max_mcidx != pmapg->hdr.pfr_max) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "mpage consistency check: max partial character mismatch\n");
        return false;
    }
    return true;
}

static int mpage_get_first_used_page(pmap_page *pmapg)
{
    int mcidx = 0;
    int mbidx = 1; /* skip pmap page */

    for ( ; mbidx < 8; mbidx++) {
        if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00) {
            return ((mcidx * 8) + mbidx); /* found */
        }
    }
    for (mcidx = mcidx+1; mcidx < (pmapg->hdr.tot_pgs/8); mcidx++) {
        if (pmapg->body[mcidx] == 0x00) continue;
        for (mbidx = 0; mbidx < 8; mbidx++) {
            if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00) {
                return ((mcidx * 8) + mbidx); /* found */
            }
        }
    }
    return 0; /* not found */
}

static int mpage_get_next_used_page(pmap_page *pmapg, int pbidx)
{
    int mcidx = pbidx / 8;
    int mbidx = pbidx % 8;

    if (mcidx == 0 && mbidx == 0) {
        mbidx = 1; /* skip pmap page */
    }
    for ( ; mbidx < 8; mbidx++) {
        if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00) {
            return ((mcidx * 8) + mbidx); /* found */
        }
    }
    for (mcidx = mcidx+1; mcidx < (pmapg->hdr.tot_pgs/8); mcidx++) {
        if (pmapg->body[mcidx] == 0x00) continue;
        for (mbidx = 0; mbidx < 8; mbidx++) {
            if ((pmapg->body[mcidx] & pfx_gl.bit1_used_mask[mbidx]) != 0x00) {
                return ((mcidx * 8) + mbidx); /* found */
            }
        }
    }
    return 0; /* not found */
}

#ifdef USE_WARM_FREE_PAGE
static void do_pfx_data_wfrpg_init(prefix_desc *pfdesc)
{
    warm_frpg_info *wminfo = &pfdesc->wfrpg_info;
    int clsid, entid;

    /* initialize that as empty warm free pages */
    wminfo->rcnt = 0;
    wminfo->ccnt = 0;
    wminfo->mcnt = WPPS;

    /* used list according to warm classes */
    for (clsid = 0; clsid < WCLS; clsid++) {
        wminfo->used[clsid] = -1;
    }

    /* free list of warm page entries */
    wminfo->free = 0;
    for (entid = 0; entid < wminfo->mcnt; entid++) {
        PAGEID_SET_NULL(&wminfo->went[entid].pgid);
        wminfo->went[entid].frsz = 0;
        wminfo->went[entid].wcls = -1; /* NOT defined */
        wminfo->went[entid].next = (entid < (wminfo->mcnt-1) ?  entid+1 : -1);
    }
}

static inline int do_pfx_data_wfrpg_clsid(uint32_t size)
{
    /* Assume WCLS = 8 */
    /* class :  4KB page
       ----------------------
           0 :  128 ~  255
           1 :  256 ~  511
           2 :  512 ~  767
           3 :  768 ~ 1023
           4 : 1024 ~ 1535
           5 : 1536 ~ 2047
           6 : 2048 ~ 3071
           7 : 3072 ~ 4096
    */
    if (size < pfx_gl.page_dv04_size) return (size/pfx_gl.page_dv16_size);
    if (size < pfx_gl.page_dv02_size) return (size/pfx_gl.page_dv08_size)+2;
    else                              return (size/pfx_gl.page_dv04_size)+4;
}

static int do_pfx_data_wfrpg_get(prefix_desc *pfdesc, uint32_t size, PageID *pgid)
{
    warm_frpg_info *wminfo = &pfdesc->wfrpg_info;
    int clsid, entid, i;

    if (wminfo->ccnt == 0) return -1; /* no warm page */
    if (size <= WARM_PAGE_MIN_SLOT_SIZE) {
        clsid = 0;
    } else {
        clsid = do_pfx_data_wfrpg_clsid(size); /* 0 <= clsid < WCLS */
        if ((++clsid) >= WCLS) return -1; /* no warm page */
    }

    entid = -1; /* NOT found */
    pthread_mutex_lock(&pfdesc->warm_lock);
    if (wminfo->ccnt > 0) {
        for (i = clsid; i < WCLS; i++) {
            if (wminfo->used[i] != -1) { /* found */
                entid = wminfo->used[i];
                wminfo->used[i] = wminfo->went[entid].next;
                wminfo->went[entid].next = -2; /* referenced */
                wminfo->rcnt += 1; /* # of referenced warm pages */
                break;
            }
        }
    }
    pthread_mutex_unlock(&pfdesc->warm_lock);
    if (entid != -1) {
        *pgid = wminfo->went[entid].pgid;
    }
    return entid;
}

static void do_pfx_data_wfrpg_release(prefix_desc *pfdesc, int entid, uint32_t size)
{
    warm_frpg_info *wminfo = &pfdesc->wfrpg_info;
    int clsid, prvid, curid;

    assert(wminfo->went[entid].next == -2);

    if (size < WARM_PAGE_MIN_SLOT_SIZE) {
        clsid = -1;
        PAGEID_SET_NULL(&wminfo->went[entid].pgid);
        wminfo->went[entid].frsz = 0;
        wminfo->went[entid].wcls = -1;
    } else {
        clsid = do_pfx_data_wfrpg_clsid(size); /* 0 <= clsid < WCLS */
        wminfo->went[entid].frsz = size;
        wminfo->went[entid].wcls = clsid;
    }

    pthread_mutex_lock(&pfdesc->warm_lock);
    wminfo->rcnt -= 1;
    if (clsid == -1) { /* link it to free list */
        wminfo->went[entid].next = wminfo->free;
        wminfo->free = entid;
        wminfo->ccnt -= 1;
    } else { /* link it to used list */
        /* let's keep the ascending order */
        prvid = -1;
        curid = wminfo->used[clsid];
        while (curid != -1) {
            if (size <= wminfo->went[curid].frsz) break;
            prvid = curid;
            curid = wminfo->went[curid].next;
        }
        if (prvid == -1) {
            wminfo->went[entid].next = wminfo->used[clsid];
            wminfo->used[clsid] = entid;
        } else {
            wminfo->went[entid].next = wminfo->went[prvid].next;
            wminfo->went[prvid].next = entid;
        }
    }
    pthread_mutex_unlock(&pfdesc->warm_lock);
}

static bool do_pfx_data_wfrpg_exist(prefix_desc *pfdesc, PageID pgid)
{
    warm_frpg_info *wminfo = &pfdesc->wfrpg_info;
    int entid, i;

    if (wminfo->ccnt == 0) return false;

    entid = -1;
    pthread_mutex_lock(&pfdesc->warm_lock);
    if (wminfo->ccnt > 0) {
        for (i = 0; i < wminfo->mcnt; i++) {
            if (PAGEID_IS_EQ(&pgid, &wminfo->went[i].pgid)) {
                entid = i; break;
            }
        }
    }
    pthread_mutex_unlock(&pfdesc->warm_lock);
    return (entid != -1 ? true : false);
}

static int do_pfx_data_wfrpg_insert(prefix_desc *pfdesc, PageID pgid, uint32_t size)
{
    warm_frpg_info *wminfo = &pfdesc->wfrpg_info;
    int clsid, entid, i, prvid, curid;

    clsid = do_pfx_data_wfrpg_clsid(size); /* 0 <= clsid < WCLS */

    entid = -1;
    pthread_mutex_lock(&pfdesc->warm_lock);
    if (wminfo->ccnt < wminfo->mcnt) {
        /* allocate free entry */
        entid = wminfo->free;
        wminfo->free = wminfo->went[entid].next;
        wminfo->ccnt += 1;
    } else { /* wminfo->ccnt == wminfo->mcnt */
        /* find a warm page having the smallest free space
         * whose size is smaller than the free size of given page.
         */
        if (wminfo->rcnt < wminfo->ccnt) {
            for (i = 0; i < clsid; i++) {
                if (wminfo->used[i] != -1) {
                    entid = wminfo->used[i];
                    wminfo->used[i] = wminfo->went[entid].next;
                    break;
                }
            }
            if (i == clsid && wminfo->used[i] != -1) {
                entid = wminfo->used[i];
                if (size > wminfo->went[entid].frsz) {
                    wminfo->used[i] = wminfo->went[entid].next;
                } else {
                    entid = -1; /* NOT found */
                }
            }
        }
    }
    if (entid != -1) {
        wminfo->went[entid].pgid = pgid;
        wminfo->went[entid].frsz = size;
        wminfo->went[entid].wcls = clsid;

        /* let's keep the ascending order */
        prvid = -1;
        curid = wminfo->used[clsid];
        while (curid != -1) {
            if (size <= wminfo->went[curid].frsz) break;
            prvid = curid;
            curid = wminfo->went[curid].next;
        }
        if (prvid == -1) {
            wminfo->went[entid].next = wminfo->used[clsid];
            wminfo->used[clsid] = entid;
        } else {
            wminfo->went[entid].next = wminfo->went[prvid].next;
            wminfo->went[prvid].next = entid;
        }
    }
    pthread_mutex_unlock(&pfdesc->warm_lock);
    return (entid != -1 ? 1 : 0); /* # of inserted pages */
}

static int do_pfx_data_wfrpg_delete(prefix_desc *pfdesc, PageID pgid)
{
    warm_frpg_info *wminfo = &pfdesc->wfrpg_info;
    int clsid, entid, i, prvid, curid;

    if (wminfo->ccnt == 0) return 0; /* # of deleted pages */

    entid = -1;
    pthread_mutex_lock(&pfdesc->warm_lock);
    if (wminfo->ccnt > 0) {
        for (i = 0; i < wminfo->mcnt; i++) {
            if (PAGEID_IS_EQ(&pgid, &wminfo->went[i].pgid)) {
                entid = i; break;
            }
        }
        if (entid != -1) {
            if (wminfo->went[entid].next == -2) { /* referenced */
                entid = -2; /* cannot delete it */
            } else {
                clsid = wminfo->went[entid].wcls;
                assert(wminfo->used[clsid] != -1);

                if (wminfo->used[clsid] == entid) {
                    wminfo->used[clsid] = wminfo->went[entid].next;
                } else {
                    prvid = wminfo->used[clsid];
                    curid = wminfo->went[prvid].next;
                    while (curid != -1 && curid != entid) {
                        prvid = curid;
                        curid = wminfo->went[curid].next;
                    }
                    assert(curid == entid);
                    wminfo->went[prvid].next = wminfo->went[entid].next;
                }
                PAGEID_SET_NULL(&wminfo->went[entid].pgid);
                wminfo->went[entid].frsz = 0;
                wminfo->went[entid].wcls = -1;
                wminfo->went[entid].next = wminfo->free;
                wminfo->free = entid;
                wminfo->ccnt -= 1;
            }
        }
    }
    pthread_mutex_unlock(&pfdesc->warm_lock);
    if (entid == -2) return -1; /* connot delete because someone try to access the page */
    else return (entid != -1 ? 1 : 0); /* # of deleted pages */
}
#endif

/* file space management */

static file_desc *do_pfx_fldsc_node_alloc(int flnum, int pmcnt)
{
    file_desc *fldsc;

    /* file descriptor + pmap page array + tfrpg count array + pfrpg count array */
    fldsc = (file_desc*)malloc(sizeof(file_desc) + ((sizeof(void*)+sizeof(uint32_t)+sizeof(uint32_t)) * pmcnt));
    if (fldsc != NULL) {
        fldsc->fd = -1;
        fldsc->flnum = (uint16_t)flnum;
        fldsc->npage = 0;
        fldsc->pmaps = (pmap_page**)((char*)fldsc + sizeof(file_desc));
        fldsc->pmtfs = (uint32_t*)((char*)fldsc->pmaps + (sizeof(void*) * pmcnt));
        fldsc->pmpfs = (uint32_t*)((char*)fldsc->pmtfs + (sizeof(uint32_t) * pmcnt));
        for (int i = 0; i < pmcnt; i++) {
            fldsc->pmaps[i] = NULL;
            fldsc->pmtfs[i] = 0;
            fldsc->pmpfs[i] = 0;
        }
        fldsc->tfree.fnext = fldsc->pfree.fnext = 0;
        fldsc->tfree.pgcnt = fldsc->pfree.pgcnt = 0;
    }
    return fldsc;
}

static void do_pfx_fldsc_node_free(file_desc *fldsc)
{
    free(fldsc);
}

static int do_pfx_fldsc_table_grow(file_mgmt *flmgt)
{
    file_desc **old_fldsc_tab;
    file_desc **new_fldsc_tab;

    new_fldsc_tab = (file_desc**)malloc(flmgt->fldsc_siz * 2 * sizeof(file_desc*));
    if (new_fldsc_tab == NULL) {
        return -1; /* out of memory */
    }
    memcpy(&new_fldsc_tab[0], &flmgt->fldsc_tab[0], flmgt->fldsc_siz*sizeof(file_desc*));
    memset(&new_fldsc_tab[flmgt->fldsc_siz], 0, flmgt->fldsc_siz*sizeof(file_desc*));

    old_fldsc_tab = flmgt->fldsc_tab;
    flmgt->fldsc_tab = new_fldsc_tab;
    flmgt->fldsc_siz *= 2;
    free(old_fldsc_tab);
    return 0;
}

static int do_pfx_file_create(file_mgmt *flmgt, int flnum)
{
    char       flnam[MAX_FILE_NAME_LENG];
    file_desc *fldsc;

    sprintf(flnam, SQUALL_FILEPATH_FORMAT[flmgt->ftype],
            config->sq.data_path, flmgt->spcid, flnum);

    if (flmgt->fldsc_siz < flnum) {
        if (do_pfx_fldsc_table_grow(flmgt) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix file(%s) create - fldsc table grow fail\n", flnam);
            return -1; /* out of memory */
        }
        assert(flmgt->fldsc_siz >= flnum);
    }

    fldsc = do_pfx_fldsc_node_alloc(flnum, flmgt->npmap_pfile);
    if (fldsc == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix file(%s) create - fldsc node alloc fail\n", flnam);
        return -1; /* out of memory */
    }

    /* open the file */
    int oflag = (config->sq.use_directio ? (O_RDWR | O_CREAT | O_DIRECT) : (O_RDWR | O_CREAT));
    fldsc->fd = disk_open(flnam, oflag, 0660);
    if (fldsc->fd == -1) {
        do_pfx_fldsc_node_free(fldsc);
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix file(%s) create - disk open error=(%d|%s)\n",
                    flnam, errno, strerror(errno));
        return -1; /* SEVERE erorr: file open fail */
    }
#if 0 // disable POSIX_FADVISE
    if (posix_fadvise(fldsc->fd, 0, 0, POSIX_FADV_RANDOM) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[WARNING] prefix file(%s) create - fadvise random error=(%d|%s)\n",
                    flnam, errno, strerror(errno));
    }
#endif

    flmgt->fldsc_tab[flnum-1] = fldsc;
    flmgt->fldsc_cnt += 1;
    return 0;
}

static int do_pfx_file_open(file_mgmt *flmgt, int flnum, int pgcnt)
{
    char       flnam[MAX_FILE_NAME_LENG];
    file_desc *fldsc;

    sprintf(flnam, SQUALL_FILEPATH_FORMAT[flmgt->ftype],
            config->sq.data_path, flmgt->spcid, flnum);

    if (flmgt->fldsc_siz < flnum) {
        if (do_pfx_fldsc_table_grow(flmgt) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix file(%s) open - fldsc table grow fail\n", flnam);
            return -1; /* out of memory */
        }
        assert(flmgt->fldsc_siz >= flnum);
    }

    fldsc = do_pfx_fldsc_node_alloc(flnum, flmgt->npmap_pfile);
    if (fldsc == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix file(%s) open - fldsc node alloc fail\n", flnam);
        return -1; /* out of memory */
    }
    fldsc->npage = pgcnt; /* current # of pages in the file */

    /* open the file */
    int oflag = (config->sq.use_directio ? (O_RDWR | O_DIRECT) : (O_RDWR));
    fldsc->fd = disk_open(flnam, oflag, 0660);
    if (fldsc->fd == -1) {
        do_pfx_fldsc_node_free(fldsc);
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix file(%s) open - disk open error=(%d|%s)\n",
                    flnam, errno, strerror(errno));
        return -1; /* SEVERE erorr: file open fail */
    }
#if 0 // disable POSIX_FADVISE
    if (posix_fadvise(fldsc->fd, 0, 0, POSIX_FADV_RANDOM) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[WARNING] prefix file(%s) open - fadvise random error=(%d|%s)\n",
                    flnam, errno, strerror(errno));
    }
#endif

    flmgt->fldsc_tab[flnum-1] = fldsc;
    flmgt->fldsc_cnt += 1;
#if 0
    if (pgcnt != -1) {
        if (fldsc->tfree.pgcnt > 0) {
            flmgt->tfree.pgcnt += fldsc->tfree.pgcnt;
            if (flmgt->tfree.fhead == 0) {
                flmgt->tfree.fhead = flnum;
            } else {
                flmgt->fldsc_tab[flmgt->tfree.ftail-1]->tfree.fnext = flnum;
            }
            flmgt->tfree.ftail = flnum;
        }
        if (fldsc->pfree.pgcnt > 0) {
            flmgt->pfree.pgcnt += fldsc->pfree.pgcnt;
            if (flmgt->pfree.fhead == 0) {
                flmgt->pfree.fhead = flnum;
            } else {
                flmgt->fldsc_tab[flmgt->pfree.ftail-1]->pfree.fnext = flnum;
            }
            flmgt->pfree.ftail = flnum;
        }
    }
#endif
    return 0;
}

static int do_pfx_file_load(file_mgmt *flmgt, int flnum)
{
    file_desc *fldsc = flmgt->fldsc_tab[flnum-1];
    pmap_page *pmapg;
    PageID     mpgid;
    uint32_t   real_pgcnt;
    int        pmcnt, i;

    if (fldsc->npage > 0) {
        /* prefix to prefix space is one-to-one mapping
         * so, spcid can be used instead of pfxid.
         */
        mpgid.pfxid = MAKE_PAGEID_PFXID(flmgt->ftype, flmgt->spcid);
        mpgid.flnum = fldsc->flnum;

        real_pgcnt = 0;
        pmcnt = ((fldsc->npage-1) / flmgt->npage_ppmap) + 1;
        for (i = 0; i < pmcnt; i++) {
            mpgid.pgnum = (i * flmgt->npage_ppmap);
            pmapg = (pmap_page*)buf_permanent_pin(mpgid, false);
            if (pmapg == NULL) break; /* non-existent pmap page */

            fldsc->pmaps[i] = pmapg;
            if (pmapg->hdr.tfr_pgs > 0) {
                fldsc->pmtfs[i] += pmapg->hdr.tfr_pgs;
                fldsc->tfree.pgcnt += pmapg->hdr.tfr_pgs;
            }
            if (pmapg->hdr.pfr_pgs > 0) {
                fldsc->pmpfs[i] += pmapg->hdr.pfr_pgs;
                fldsc->pfree.pgcnt += pmapg->hdr.pfr_pgs;
            }
            real_pgcnt += pmapg->hdr.tot_pgs;
        }
        if (real_pgcnt != fldsc->npage) {
            assert(real_pgcnt < fldsc->npage);
            fldsc->npage = real_pgcnt;
            /* The total page count of prefix space becomes inconsistent
             * with the sum of page counts of file descriptors.
             * But, recovery redo logic makes that consistent later.
             * So, do not adjust the total page count of pfspace, now.
             */
        }
    }
    return 0;
}

static void do_pfx_file_unload(file_mgmt *flmgt, int flnum)
{
    file_desc *fldsc = flmgt->fldsc_tab[flnum-1];

    for (int i = 0; i < flmgt->npmap_pfile; i++) {
        if (fldsc->pmaps[i] == NULL) break;
        buf_permanent_unpin((Page*)fldsc->pmaps[i]);
    }
}

static void do_pfx_file_close(file_mgmt *flmgt, int flnum)
{
    file_desc *fldsc;

    fldsc = flmgt->fldsc_tab[flnum-1];
    flmgt->fldsc_tab[flnum-1] = NULL;
    flmgt->fldsc_cnt -= 1;

    if (config->sq.use_directio != true) {
        disk_fsync(fldsc->fd);
    }
    disk_close(fldsc->fd); fldsc->fd = -1;
    do_pfx_fldsc_node_free(fldsc);
}

/* prefix space : file_mgmt function */
static int do_pfx_flmgt_init(file_mgmt *flmgt, uint16_t spcid, uint16_t ftype, uint32_t flcnt, uint64_t pgcnt)
{
    /* initialize basic file mgmt info */
    flmgt->spcid = spcid;
    flmgt->ftype = ftype;
    if (ftype == SQUALL_FILE_TYPE_DATA) {
        flmgt->npage_pbyte = 4; /* A page is represented with 2 bits */
    } else {
        assert(ftype == SQUALL_FILE_TYPE_OVFL || ftype == SQUALL_FILE_TYPE_INDX);
        flmgt->npage_pbyte = 8; /* A page is represented with 1 bits */
    }
    flmgt->npage_ppmap = flmgt->npage_pbyte * pfx_gl.pmap_body_size;
    flmgt->npmap_pfile = ((pfx_gl.npage_per_file-1) / flmgt->npage_ppmap) + 1;

    /* initialize file desc table */
    flmgt->fldsc_cnt = 0;
    flmgt->fldsc_siz = 16; /* minimum fldsc table size */
    while (flmgt->fldsc_siz < flcnt) {
        flmgt->fldsc_siz *= 2;
    }
    flmgt->fldsc_tab = (file_desc**)malloc(flmgt->fldsc_siz*sizeof(file_desc*));
    if (flmgt->fldsc_tab == NULL) {
        fprintf(stderr, "prefix space(id=%d): file desc table memory alloc error\n", spcid);
        return -1; /* no more memory */
    }
    memset(flmgt->fldsc_tab, 0, flmgt->fldsc_siz*sizeof(file_desc*));

    /* initialize free file chain */
    flmgt->tfree.fhead = flmgt->pfree.fhead = 0;
    flmgt->tfree.ftail = flmgt->pfree.ftail = 0;
    flmgt->tfree.pgcnt = flmgt->pfree.pgcnt = 0;

    /* open all the files */
    int flnum, npage_at_file;
    for (flnum = 1; flnum <= flcnt; flnum++) {
        npage_at_file = (flnum < flcnt ? pfx_gl.npage_per_file
                                       : (int)(pgcnt % pfx_gl.npage_per_file));
        if (do_pfx_file_open(flmgt, flnum, npage_at_file) != 0) {
            fprintf(stderr, "prefix space(%d) %s file(%d) open error\n",
                    spcid, get_file_type_str(flmgt->ftype), flnum);
            return -1; /* prefix file open fail */
        }
    }
    return 0;
}

static int do_pfx_flmgt_load(file_mgmt *flmgt)
{
    assert(flmgt->fldsc_tab != NULL);

    for (int flnum = 1; flnum <= flmgt->fldsc_cnt; flnum++) {
        if (do_pfx_file_load(flmgt, flnum) != 0) {
            return -1;
        }
        /* file list having total free pages */
        if (flmgt->fldsc_tab[flnum-1]->tfree.pgcnt > 0) {
            flmgt->tfree.pgcnt += flmgt->fldsc_tab[flnum-1]->tfree.pgcnt;
            if (flmgt->tfree.fhead == 0) {
                flmgt->tfree.fhead = flnum;
            } else {
                flmgt->fldsc_tab[flmgt->tfree.ftail-1]->tfree.fnext = flnum;
            }
            flmgt->tfree.ftail = flnum;
        }
        /* file list having partial free pages */
        if (flmgt->fldsc_tab[flnum-1]->pfree.pgcnt > 0) {
            flmgt->pfree.pgcnt += flmgt->fldsc_tab[flnum-1]->pfree.pgcnt;
            if (flmgt->pfree.fhead == 0) {
                flmgt->pfree.fhead = flnum;
            } else {
                flmgt->fldsc_tab[flmgt->pfree.ftail-1]->pfree.fnext = flnum;
            }
            flmgt->pfree.ftail = flnum;
        }
    }
    return 0;
}

static void do_pfx_flmgt_final(file_mgmt *flmgt)
{
    if (flmgt->fldsc_tab != NULL) {
        for (int flnum = flmgt->fldsc_cnt; flnum >= 1; flnum--) {
            do_pfx_file_unload(flmgt, flnum);
            do_pfx_file_close(flmgt, flnum);
        }
        free(flmgt->fldsc_tab);
        flmgt->fldsc_tab = NULL;
    }
}

static int do_pfx_flmgt_check(file_mgmt *flmgt, uint32_t flcnt, uint64_t pgcnt)
{
    file_desc *fldsc;
    pmap_page *pmapg;
    int        flnum;
    int        pmidx, ret=0;
    uint64_t   sum_pgs_total = 0;
    uint64_t   map_pgs_total = 0;
    uint64_t   use_pgs_total = 0;
    uint64_t   fre_pgs_total = 0;
    uint32_t   dsk_pgs_pfile;
    uint32_t   sum_pgs_pfile;
    uint32_t   map_pgs_pfile;
    uint32_t   use_pgs_pfile;
    uint32_t   fre_pgs_pfile;

    assert(flcnt == flmgt->fldsc_cnt);

    for (flnum = 1; flnum <= flcnt; flnum++) {
        fldsc = flmgt->fldsc_tab[flnum-1];
        assert(fldsc != NULL);

        sum_pgs_pfile = 0;
        map_pgs_pfile = 0; /* # of pmap pages */
        use_pgs_pfile = 0; /* # of used pages */
        fre_pgs_pfile = 0; /* # of free pages */

        for (pmidx = 0; pmidx < flmgt->npmap_pfile; pmidx++) {
            pmapg = fldsc->pmaps[pmidx];
            if (pmapg == NULL || pmapg->hdr.tot_pgs == 0) {
                assert(flnum == flcnt);
                break;
            }
            if (mpage_check_consistency(pmapg) == false) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "PREIFX SPACE [%d] %s file(%d) check - inconsitent pmap(%d) page\n",
                            flmgt->spcid, get_file_type_str(flmgt->ftype), flnum, pmidx);
                ret = -1; break;
            }
            map_pgs_pfile += 1;
            use_pgs_pfile += (pmapg->hdr.tot_pgs - pmapg->hdr.tfr_pgs - 1);
            fre_pgs_pfile += pmapg->hdr.tfr_pgs;
            sum_pgs_pfile += pmapg->hdr.tot_pgs;
        }
        if (ret != 0) break;

        dsk_pgs_pfile = (flnum < flcnt ? pfx_gl.npage_per_file : (uint32_t)(pgcnt % pfx_gl.npage_per_file));
        if (sum_pgs_pfile != dsk_pgs_pfile) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "PREIFX SPACE [%d] %s file(%d) check - "
                                                     "page count mismatch: total(%u) != space(%u)\n",
                        flmgt->spcid, get_file_type_str(flmgt->ftype), flnum, sum_pgs_pfile, dsk_pgs_pfile);
            ret = -1; break;
        }

        sum_pgs_total += sum_pgs_pfile;
        use_pgs_total += use_pgs_pfile;
        fre_pgs_total += fre_pgs_pfile;
        map_pgs_total += map_pgs_pfile;
    }

    if (ret == 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "PREFIX SPACE [%d] %s : file_count=%u page_count=%lu (pmap=%lu used=%lu free=%lu)\n",
                    flmgt->spcid, get_file_type_str(flmgt->ftype), flcnt,
                    sum_pgs_total, map_pgs_total, use_pgs_total, fre_pgs_total);
    }
    return ret;
}

/* Prefix Space Management */

static prefix_space *do_pfx_space_alloc(int spcid)
{
    prefix_space *pfspace;

    if (pfx_gl.num_spces >= pfx_gl.max_spces)
        return NULL;

    pthread_mutex_lock(&pfx_gl.spc_lock);
    if (pfx_gl.num_spces >= pfx_gl.max_spces) {
        pfspace = NULL; /* no free space */
    } else {
        if (spcid == -1) {
            spcid = pfx_gl.spc_freeLWM;
            assert(spcid >= 0 && spcid < pfx_gl.max_spces);
            pfspace = &pfx_gl.space_tab[spcid];
        } else {
            assert(spcid >= 0 && spcid < pfx_gl.max_spces);
            pfspace = &pfx_gl.space_tab[spcid];
            if (pfspace->used != 0) pfspace = NULL; /* alreay used */
        }
        if (pfspace != NULL) {
            /* adjust spc_freeLWM and spc_freeNXT */
            assert(spcid >= pfx_gl.spc_freeLWM);
            if (spcid == pfx_gl.spc_freeLWM) {
                int16_t i;
                for (i = pfx_gl.spc_freeLWM+1; i < pfx_gl.max_spces; i++) {
                    if (pfx_gl.space_tab[i].used == 0) break; /* found */
                }
                pfx_gl.spc_freeLWM = i;
                if (pfx_gl.spc_freeNXT < pfx_gl.spc_freeLWM)
                    pfx_gl.spc_freeNXT = pfx_gl.spc_freeLWM;
            } else {
                if (pfx_gl.spc_freeNXT == spcid)
                    pfx_gl.spc_freeNXT = spcid+1;
            }
            pfspace->used = 1; /* set used */
            pfx_gl.num_spces += 1;
        }
    }
    pthread_mutex_unlock(&pfx_gl.spc_lock);
    return pfspace;
}

static void do_pfx_space_free(prefix_space *pfspace)
{
    assert(pfspace->pfxcount == 0);
    pthread_mutex_lock(&pfx_gl.spc_lock);
    if (pfspace->spcid < pfx_gl.spc_freeLWM) {
        pfx_gl.spc_freeLWM = pfspace->spcid;
    }
    if ((pfspace->spcid+1) == pfx_gl.spc_freeNXT) {
        int16_t i;
        for (i = pfspace->spcid-1; i >= 0; i--) {
            if (pfx_gl.space_tab[i].used != 0) break;
        }
        pfx_gl.spc_freeNXT = i+1;
    }
    pfspace->used = 0; /* set free */
    pfx_gl.num_spces -= 1;
    pthread_mutex_unlock(&pfx_gl.spc_lock);
}

static int do_pfx_space_load(prefix_space *pfspace)
{
    if (do_pfx_flmgt_load(&pfspace->data_flmgt) != 0) {
        return -1;
    }
    if (do_pfx_flmgt_load(&pfspace->ovfl_flmgt) != 0) {
        return -1;
    }
    if (do_pfx_flmgt_load(&pfspace->indx_flmgt) != 0) {
        return -1;
    }
    return 0;
}

static void do_pfx_space_final(prefix_space *pfspace)
{
    do_pfx_flmgt_final(&pfspace->indx_flmgt);
    do_pfx_flmgt_final(&pfspace->ovfl_flmgt);
    do_pfx_flmgt_final(&pfspace->data_flmgt);
}

static int do_pfx_space_init(prefix_space *pfspace, prefix_disk_space *dskinfo)
{
    /* initialize prefix space disk info */
    if (dskinfo != NULL) {
        memcpy(&pfspace->d, dskinfo, sizeof(prefix_disk_space));
    }

    /* initialize file management */
    pfspace->data_flmgt.fldsc_tab = NULL;
    pfspace->ovfl_flmgt.fldsc_tab = NULL;
    pfspace->indx_flmgt.fldsc_tab = NULL;

    int ret;
    do {
        ret = do_pfx_flmgt_init(&pfspace->data_flmgt, pfspace->spcid, SQUALL_FILE_TYPE_DATA,
                                pfspace->d.data_total_flcnt, pfspace->d.data_total_pgcnt);
        if (ret != 0) {
            fprintf(stderr, "prefix space(id=%d) data file mgmt init error\n", pfspace->spcid);
            break; /* "no more memory" or "file open error" */
        }
        ret = do_pfx_flmgt_init(&pfspace->ovfl_flmgt, pfspace->spcid, SQUALL_FILE_TYPE_OVFL,
                                pfspace->d.ovfl_total_flcnt, pfspace->d.ovfl_total_pgcnt);
        if (ret != 0) {
            fprintf(stderr, "prefix space(id=%d) ovfl file mgmt init error\n", pfspace->spcid);
            break; /* "no more memory" or "file open error" */
        }
        ret = do_pfx_flmgt_init(&pfspace->indx_flmgt, pfspace->spcid, SQUALL_FILE_TYPE_INDX,
                                pfspace->d.indx_total_flcnt, pfspace->d.indx_total_pgcnt);
        if (ret != 0) {
            fprintf(stderr, "prefix space(id=%d) indx file mgmt init error\n", pfspace->spcid);
            break; /* "no more memory" or "file open error" */
        }
    } while(0);

    if (ret != 0) {
        do_pfx_flmgt_final(&pfspace->indx_flmgt);
        do_pfx_flmgt_final(&pfspace->ovfl_flmgt);
        do_pfx_flmgt_final(&pfspace->data_flmgt);
    }
    return ret;
}

static void do_pfx_space_dsk_init(prefix_space *pfspace)
{
    prefix_disk_space *dskspc = &pfspace->d;

    memset((void*)dskspc, 0, sizeof(prefix_disk_space));
    LOGSN_SET_NULL(&dskspc->crtlsn);
    LOGSN_SET_NULL(&dskspc->updlsn);
    dskspc->disk_file_size = config->sq.data_file_size; /* unit: MB */
    dskspc->disk_page_size = config->sq.data_page_size; /* unit: Bytes */
    dskspc->space_id = pfspace->spcid;
    dskspc->data_total_flcnt = 0;
    dskspc->ovfl_total_flcnt = 0;
    dskspc->indx_total_flcnt = 0;
    dskspc->data_total_pgcnt = 0;
    dskspc->ovfl_total_pgcnt = 0;
    dskspc->indx_total_pgcnt = 0;
    dskspc->disk_quota_pgcnt = 0; /* unlimited */
}

static prefix_space *do_pfx_space_create(int spcid)
{
    prefix_space *pfspace;

    pfspace = do_pfx_space_alloc(spcid);
    if (pfspace != NULL) {
        /* initialize disk info of prefix space */
        do_pfx_space_dsk_init(pfspace);

        /* initialize prefix space */
        if (do_pfx_space_init(pfspace, NULL) != 0) {
            do_pfx_space_free(pfspace);
            pfspace = NULL;
        }
    }
    return pfspace;
}

static void do_pfx_space_remove(prefix_space *pfspace)
{
    char flnam[MAX_FILE_NAME_LENG];
    int  flnum;

    /* remove prefix database files: how to ?? */

    for (flnum = 1; flnum <= pfspace->d.data_total_flcnt; flnum++) {
        sprintf(flnam, SQUALL_FILEPATH_FORMAT[SQUALL_FILE_TYPE_DATA],
                config->sq.data_path, pfspace->spcid, flnum);
        (void)disk_unlink(flnam);
    }
    for (flnum = 1; flnum <= pfspace->d.ovfl_total_flcnt; flnum++) {
        sprintf(flnam, SQUALL_FILEPATH_FORMAT[SQUALL_FILE_TYPE_OVFL],
                config->sq.data_path, pfspace->spcid, flnum);
        (void)disk_unlink(flnam);
    }
    for (flnum = 1; flnum <= pfspace->d.indx_total_flcnt; flnum++) {
        sprintf(flnam, SQUALL_FILEPATH_FORMAT[SQUALL_FILE_TYPE_INDX],
                config->sq.data_path, pfspace->spcid, flnum);
        (void)disk_unlink(flnam);
    }
}

static prefix_space *do_pfx_space_open(int spcid)
{
    assert(spcid >= 0 && spcid < pfx_gl.max_spces);
    prefix_space *pfspace = NULL;

    pthread_mutex_lock(&pfx_gl.spc_lock);
    if (pfx_gl.space_tab[spcid].used != 0) {
        pfspace = &pfx_gl.space_tab[spcid];
        pfspace->pfxcount += 1;
    }
    pthread_mutex_unlock(&pfx_gl.spc_lock);
    return pfspace;
}

static void do_pfx_space_close(int spcid)
{
    assert(spcid >= 0 && spcid < pfx_gl.max_spces);
    prefix_space *pfspace = &pfx_gl.space_tab[spcid];

    pthread_mutex_lock(&pfx_gl.spc_lock);
    assert(pfspace->used != 0 && pfspace->pfxcount > 0);
    pfspace->pfxcount -= 1;
    pthread_mutex_unlock(&pfx_gl.spc_lock);
}

#if 1 /* will be used later */
static prefix_space *do_pfx_space_find(int spcid)
{
    assert(spcid >= 0 && spcid < pfx_gl.max_spces);
    prefix_space *pfspace = NULL;

    pthread_mutex_lock(&pfx_gl.spc_lock);
    if (pfx_gl.space_tab[spcid].used != 0) {
        pfspace = &pfx_gl.space_tab[spcid];
    }
    pthread_mutex_unlock(&pfx_gl.spc_lock);
    return pfspace;
}
#endif

static prefix_space *do_pfx_space_get_first(void)
{
    prefix_space *pfspace = NULL;
    pthread_mutex_lock(&pfx_gl.spc_lock);
    if (pfx_gl.num_spces > 0) {
        for (int i = 0; i < pfx_gl.spc_freeNXT; i++) {
            if (pfx_gl.space_tab[i].used != 0) {
                pfspace = &pfx_gl.space_tab[i]; break;
            }
        }
    }
    pthread_mutex_unlock(&pfx_gl.spc_lock);
    return pfspace;
}

static prefix_space *do_pfx_space_get_next(int spcid)
{
    prefix_space *pfspace = NULL;
    pthread_mutex_lock(&pfx_gl.spc_lock);
    for (int i = spcid+1; i < pfx_gl.spc_freeNXT; i++) {
        if (pfx_gl.space_tab[i].used != 0) {
            pfspace = &pfx_gl.space_tab[i]; break;
        }
    }
    pthread_mutex_unlock(&pfx_gl.spc_lock);
    return pfspace;
}

/* Prefix Descriptor Management */

static prefix_desc *do_pfx_desc_alloc(int pfxid)
{
    prefix_desc *pfdesc;

    if (pfx_gl.num_pfxes >= pfx_gl.max_pfxes) {
        return NULL;
    }

    pthread_mutex_lock(&pfx_gl.pfx_lock);
    if (pfx_gl.num_pfxes >= pfx_gl.max_pfxes) {
        pfdesc = NULL; /* no free desc */
    } else {
        if (pfxid == -1) {
            pfxid = pfx_gl.pfx_freeLWM;
            assert(pfxid >= 0 && pfxid < pfx_gl.max_pfxes);
            pfdesc = &pfx_gl.desc_tab[pfxid];
        } else {
            assert(pfxid >= 0 && pfxid < pfx_gl.max_pfxes);
            pfdesc = &pfx_gl.desc_tab[pfxid];
            if (pfdesc->used != 0) pfdesc = NULL; /* alreay used */
        }
        if (pfdesc != NULL) {
            /* adjust pfx_freeLWM and pfx_freeNXT */
            assert(pfxid >= pfx_gl.pfx_freeLWM);
            if (pfxid == pfx_gl.pfx_freeLWM) {
                int16_t i;
                for (i = pfx_gl.pfx_freeLWM+1; i < pfx_gl.max_pfxes; i++) {
                    if (pfx_gl.desc_tab[i].used == 0) break; /* found */
                }
                pfx_gl.pfx_freeLWM = i;
                if (pfx_gl.pfx_freeNXT < pfx_gl.pfx_freeLWM)
                    pfx_gl.pfx_freeNXT = pfx_gl.pfx_freeLWM;
            } else {
                if (pfx_gl.pfx_freeNXT == pfxid)
                    pfx_gl.pfx_freeNXT = pfxid+1;
            }
            pfdesc->used = 1; /* set used */
            pfx_gl.num_pfxes += 1;
        }
    }
    pthread_mutex_unlock(&pfx_gl.pfx_lock);
    return pfdesc;
}

static void do_pfx_desc_free(prefix_desc *pfdesc)
{
    pthread_mutex_lock(&pfx_gl.pfx_lock);
    if (pfdesc->pfxid < pfx_gl.pfx_freeLWM) {
        pfx_gl.pfx_freeLWM = pfdesc->pfxid;
    }
    if ((pfdesc->pfxid+1) == pfx_gl.pfx_freeNXT) {
        int16_t i;
        for (i = pfdesc->pfxid-1; i >= 0; i--) {
            if (pfx_gl.desc_tab[i].used != 0) break;
        }
        pfx_gl.pfx_freeNXT = i+1;
    }
    pfdesc->used = 0; /* set free */
    pfx_gl.num_pfxes -= 1;
    pthread_mutex_unlock(&pfx_gl.pfx_lock);
}

static void do_pfx_desc_final(prefix_desc *pfdesc)
{
    if (pfdesc->space != NULL) {
        do_pfx_space_close(pfdesc->space->spcid);
    }
}

static int do_pfx_desc_init(prefix_desc *pfdesc, prefix_disk_desc *dskinfo)
{
    if (dskinfo != NULL) {
        memcpy(&pfdesc->d, dskinfo, sizeof(prefix_disk_desc));
    }

    /* basic memory information */
    // pfdesc->next: will be set when it is linked */
    pfdesc->name = pfdesc->d.stats.name;
    pfdesc->nlen = strlen(pfdesc->d.stats.name);
    pfdesc->tlen = (pfdesc->nlen > 0 ? pfdesc->nlen+1 : 0);
    pfdesc->hval = pfdesc->d.hash_val;
    pfdesc->refcount = 0;
    // pfdesc->pfxid: has already been set */
    // pfdesc->used : hab already been set to true */

    /* prefix space information */
    pfdesc->space = do_pfx_space_open(pfdesc->d.space_id);
    if (pfdesc->space == NULL) {
        return -1; /* no space found */
    }

#ifdef USE_WARM_FREE_PAGE
    /* warm free page information (hint) */
    do_pfx_data_wfrpg_init(pfdesc);
#endif

    return 0;
}

static void do_pfx_desc_remove(prefix_desc *pfdesc)
{
    assert(pfdesc->d.private_space != 0);

    if (pfdesc->d.private_space) {
        /* The prefix is using private prefix space */
        do_pfx_space_final(pfdesc->space);
        do_pfx_space_remove(pfdesc->space);
        do_pfx_space_free(pfdesc->space);
    } else {
        /* The prefix is using shared prefix space */
        /* IMPLEMENT LATER */
    }
}

static prefix_desc *do_pfx_desc_get_first(void)
{
    prefix_desc *pfdesc = NULL;
    pthread_mutex_lock(&pfx_gl.pfx_lock);
    if (pfx_gl.num_pfxes > 0) {
        for (int i = 0; i < pfx_gl.pfx_freeNXT; i++) {
            if (pfx_gl.desc_tab[i].used != 0) {
                pfdesc = &pfx_gl.desc_tab[i]; break;
            }
        }
    }
    pthread_mutex_unlock(&pfx_gl.pfx_lock);
    return pfdesc;
}

static prefix_desc *do_pfx_desc_get_next(int pfxid)
{
    prefix_desc *pfdesc = NULL;
    pthread_mutex_lock(&pfx_gl.pfx_lock);
    for (int i = pfxid+1; i < pfx_gl.pfx_freeNXT; i++) {
        if (pfx_gl.desc_tab[i].used != 0) {
            pfdesc = &pfx_gl.desc_tab[i]; break;
        }
    }
    pthread_mutex_unlock(&pfx_gl.pfx_lock);
    return pfdesc;
}

/* Prefix Hash Management */

static inline prefix_desc *do_pfx_hash_find(prefix_hash *pfhash, const char *pfx, const size_t npfx)
{
    prefix_desc *pfdesc = pfhash->next;
    while (pfdesc != NULL) {
        if (pfdesc->nlen == npfx && memcmp(pfdesc->name, pfx, npfx)==0)
            break;
        pfdesc = pfdesc->next;
    }
    return pfdesc;
}

static void do_pfx_hash_link(prefix_hash *pfhash, prefix_desc *pfdesc)
{
    /* link the prefix desc to prefix hash chain */
    pfdesc->next = pfhash->next;
    pfhash->next = pfdesc;
    pfdesc->lnked = 1;
}

static void do_pfx_hash_unlink(prefix_hash *pfhash, prefix_desc *pvdesc, prefix_desc *pfdesc)
{
    /* unlink the prefix desc from prefix hash chain */
    if (pvdesc != NULL) pvdesc->next = pfdesc->next;
    else                pfhash->next = pfdesc->next;
    pfdesc->lnked = 0;
}

/* Prefix Create & Remove */

static void do_pfx_desc_dsk_init(prefix_desc *pfdesc, prefix_space *pfspace,
                                 const char *pfx, const size_t npfx, const uint32_t hval,
                                 const bool explicit)
{
    prefix_disk_desc *dskdsc = &pfdesc->d;

    memset((void*)dskdsc, 0, sizeof(prefix_disk_desc));

    /* step 1: disk common information */
    LOGSN_SET_NULL(&dskdsc->crtlsn);
    LOGSN_SET_NULL(&dskdsc->updlsn);
    dskdsc->prefix_id = pfdesc->pfxid;
    dskdsc->space_id = pfspace->spcid;
    dskdsc->hash_val = hval;
    dskdsc->use_cas = config->use_cas;
    dskdsc->explicit_create = (explicit ? 1 : 0);
    dskdsc->delete_request = 0;
    dskdsc->private_space = 1; /* private prefix space */

    /* step 2: btree and hash index information */
    dskdsc->bndx_global_depth = -1;
    PAGEID_SET_NULL(&dskdsc->bndx_root_pgid);
    dskdsc->hndx_global_depth = -1;
    for (int i = 0; i < HASH_INDEX_MAX_DEPTH; i++) {
        dskdsc->hndx_npage_per_depth[i] = 0;
    }

    /* step 3: prefix stats information */
    memcpy(dskdsc->stats.name, pfx, npfx);
    dskdsc->stats.name[npfx] = '\0';
    time(&dskdsc->stats.create_time);
}

static prefix_desc *do_pfx_create(const uint32_t hval, const char *prefix, const size_t nprefix, const bool explicit)
{
    prefix_hash *pfhash;
    prefix_desc *pfdesc;
    prefix_space *pfspace;

    /* The caller is holding prefix hash lock */
    assert(nprefix > 0);

    pfdesc = do_pfx_desc_alloc(-1);
    if (pfdesc == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix(%s) desc alloc error\n", prefix);
        return NULL;
    }
    assert(pfdesc->pfxid > 0); /* not default prefix => newly created */

    if (explicit) {
        /* use private prefix space */
        pfspace = do_pfx_space_create(pfdesc->pfxid);
    } else {
        /* use shared prefix space */
        //pfspace = do_pfx_space_find(0); /* spcid = 0 */
        pfspace = do_pfx_space_create(pfdesc->pfxid);
    }
    if (pfspace == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix(%s) space create error\n", prefix);
        do_pfx_desc_free(pfdesc);
        return NULL;
    }
    assert(pfspace->spcid == pfdesc->pfxid);

    /* initialize disk info of prefix descriptor */
    do_pfx_desc_dsk_init(pfdesc, pfspace, prefix, nprefix, hval, explicit);

    /* initialize prefix descriptor */
    if (do_pfx_desc_init(pfdesc, NULL) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix(%s) desc init error\n", prefix);
        do_pfx_space_remove(pfspace);
        do_pfx_space_free(pfspace);
        do_pfx_desc_free(pfdesc);
        return NULL;
    }

    pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
    do_pfx_hash_link(pfhash, pfdesc);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* prefix (and prefix space) create */
        /* NTS(nested top action) : redo-only */
        PFCreateLog pfcrt_log;
        lrec_pf_create_init((LogRec*)&pfcrt_log);
        pfcrt_log.body.pfxid = pfdesc->pfxid;
        pfcrt_log.body.spcid = pfspace->spcid;
        pfcrt_log.body.use_cas = config->use_cas;
        pfcrt_log.body.exp_crt = (explicit ? 1 : 0);
        pfcrt_log.body.nprefix = (uint16_t)nprefix;
        pfcrt_log.prefix = (void*)prefix;
        pfcrt_log.header.length = GET_8_ALIGN_SIZE(offsetof(PFCreateData,data) + nprefix);

        log_record_write((LogRec*)&pfcrt_log, NULL);
        /* update create LSN on prefix and prefix space */
        pfspace->d.crtlsn = pfcrt_log.header.self_lsn;
        pfdesc->d.crtlsn = pfcrt_log.header.self_lsn;
    }
#endif

    return pfdesc;
}

static void do_pfx_delete(prefix_desc *pfdesc)
{
    if (pfdesc != NULL) { /* case 1: delete the given prefix only */
        /* step 1: invalidate buffer pages */
        buf_invalidate_all(pfdesc->pfxid);

        do_pfx_desc_final(pfdesc);
        do_pfx_desc_remove(pfdesc);
        do_pfx_desc_free(pfdesc);
    } else { /* case 2: delete all prefixes */
        /* step 1: invalidate all buffer pages */
        buf_invalidate_all(-1);
    }
}

/* Prefix Disk ... */

static void do_pfx_system_info_init(void)
{
    system_info *syspage = &pfx_gl.sysinfo;

    memset((void*)syspage, 0, sizeof(system_info));
    memcpy(syspage->magic_code, pfx_gl.magic_code, SQUALL_MAGIC_CODELEN);
    time(&syspage->db_create_time);
    syspage->sys_start_time = syspage->db_create_time;
    syspage->sys_stop_time = 0;
    syspage->sys_epoch = 0;
    syspage->spc_count = 0;
    syspage->pfx_count = 0;
}

static int do_pfx_disk_flush(const char *anch_path)
{
    prefix_space *pfspace;
    prefix_desc  *pfdesc;
    int     spccnt; /* # of spaces */
    int     pfxcnt; /* # of prefixes */
    int     fd, ret=0;
    off_t   offset;
    ssize_t nwrite;

    fd = disk_open(anch_path, O_RDWR | O_CREAT, 0660);
    if (fd == -1) {
        fprintf(stderr, "data anchor(%s) create error=(%d:%s)\n", anch_path, errno, strerror(errno));
        return -1;
    }

    spccnt = pfxcnt = 0;
    offset = sizeof(system_info);
    do {
        /* write prefix spaces info */
        pfspace = do_pfx_space_get_first();
        while (pfspace != NULL) {
            nwrite = disk_byte_pwrite(fd, &pfspace->d, sizeof(prefix_disk_space), offset);
            if (nwrite != sizeof(prefix_disk_space)) {
                fprintf(stderr, "data anchor(%s) pfxspc(id=%d) write(%ld!=%ld) error=(%d:%s)\n",
                        anch_path, pfspace->spcid, nwrite, sizeof(prefix_disk_space), errno, strerror(errno));
                ret = -1; break;
            }
            offset += sizeof(prefix_disk_space);
            spccnt += 1;
            pfspace = do_pfx_space_get_next(pfspace->spcid);
        }
        if (ret == -1) break;

        /* write prefixes info */
        pfdesc = do_pfx_desc_get_first();
        while (pfdesc != NULL) {
            nwrite = disk_byte_pwrite(fd, &pfdesc->d, sizeof(prefix_disk_desc), offset);
            if (nwrite != sizeof(prefix_disk_desc)) {
                fprintf(stderr, "data anchor(%s) prefix(id=%d) write(%ld!=%ld) error=(%d:%s)\n",
                        anch_path, pfdesc->pfxid, nwrite, sizeof(prefix_disk_desc), errno, strerror(errno));
                ret = -1; break;
            }
            offset += sizeof(prefix_disk_desc);
            pfxcnt += 1;
            pfdesc = do_pfx_desc_get_next(pfdesc->pfxid);
        }
        if (ret == -1) break;

        /* reset system info */
        pfx_gl.sysinfo.spc_count = spccnt;
        pfx_gl.sysinfo.pfx_count = pfxcnt;

        /* write system info */
        nwrite = disk_byte_pwrite(fd, &pfx_gl.sysinfo, sizeof(system_info), 0);
        if (nwrite !=  sizeof(system_info)) {
            fprintf(stderr, "data anchor(%s) system info write(%ld!=%ld) error=(%d:%s)\n",
                    anch_path, nwrite, sizeof(system_info), errno, strerror(errno));
            ret = -1; break;
        }

        /* sync the data anchor file */
        if (disk_fsync(fd) != 0) {
            fprintf(stderr, "data anchor(%s) sync error=(%d:%s)\n",
                    anch_path, errno, strerror(errno));
            ret = -1; break;
        }
    } while(0);

    disk_close(fd);
    return ret;
}

static int do_pfx_disk_create(const char *anch_name)
{
    prefix_space *pfspace;
    prefix_desc  *pfdesc;
    prefix_hash  *pfhash;
    uint32_t      hashval;
    int           ret;

    /* initialize system info */
    do_pfx_system_info_init();

    /* create the first(default) prefix space */
    pfspace = do_pfx_space_create(0);
    assert(pfspace != NULL && pfspace->spcid == 0);

    /* create null prefix */
    pfdesc = do_pfx_desc_alloc(0);
    assert(pfdesc != NULL && pfdesc->pfxid == 0);

    /* init the null prefix */
    hashval = server->core->hash(NULL, 0, 0);
    do_pfx_desc_dsk_init(pfdesc, pfspace, NULL, 0, hashval, false);
    do_pfx_desc_init(pfdesc, NULL);

    /* link the null prefix */
    pfhash = &pfx_gl.hash_tab[hashval % pfx_gl.hash_size];
    do_pfx_hash_link(pfhash, pfdesc);

    /* create data anchor file for the first time */
    ret = do_pfx_disk_flush(anch_name);
    if (ret != 0) {
        disk_unlink(anch_name);
    }
    return ret;
}

static void do_pfx_disk_unload(bool normal_shutdown)
{
    prefix_hash *pfhash;
    prefix_desc *pfdesc;
    prefix_space *pfspace;
    int   i;

    /* flush all dirty page maps and anchor file */
    if (normal_shutdown) {
        char anch_name[MAX_FILE_NAME_LENG];
        sprintf(anch_name, "%s/squall_data_anchor_%01d", config->sq.data_path, 0);

        /* set system stop time */
        time(&pfx_gl.sysinfo.sys_stop_time);
        do_pfx_disk_flush(anch_name);
    }

    /* terminate prefix descriptors */
    for (i = 0; i < pfx_gl.hash_size; i++) {
        pfhash = &pfx_gl.hash_tab[i];
        while (pfhash->next != NULL) {
            pfdesc = pfhash->next;
            pfhash->next = pfdesc->next;
            do_pfx_desc_final(pfdesc);
            do_pfx_desc_free(pfdesc);
        }
    }

    /* terminate prefix spaces */
    for (i = 0; i < pfx_gl.spc_freeNXT; i++) {
        pfspace = &pfx_gl.space_tab[i];
        if (pfspace->used) {
            do_pfx_space_final(pfspace);
            do_pfx_space_free(pfspace);
        }
    }
}

static int do_pfx_disk_load(int dbidx)
{
    assert(dbidx == 0 || dbidx == 1);
    prefix_disk_space *dskspc;
    prefix_disk_desc  *dskdsc;
    prefix_space      *pfspace;
    prefix_desc       *pfdesc;
    prefix_hash       *pfhash;
    char    anch_name[MAX_FILE_NAME_LENG];
    int     spccnt; /* # of spaces */
    int     pfxcnt; /* # of prefixes */
    int     fd, i, ret=0;
    off_t   offset;
    ssize_t nread;
    ssize_t nwrite;

    sprintf(anch_name, "%s/squall_data_anchor_%01d", config->sq.data_path, dbidx);

    fd = disk_open(anch_name, O_RDWR, 0);
    if (fd == -1) {
        if (errno != ENOENT) {
            fprintf(stderr, "data anchor(%s) open error (%d:%s)\n", anch_name, errno, strerror(errno));
            return -1;
        }
        /* The data anchor file does not exist - create it */
        fprintf(stderr, "create data anchor file.\n");
        if ((ret = do_pfx_disk_create(anch_name)) != 0) {
            fprintf(stderr, "create data anchor file is failed.\n");
        }
        return ret;
    }

    offset = 0;
    do {
        /* read system info */
        nread = disk_byte_pread(fd, &pfx_gl.sysinfo, sizeof(system_info), offset);
        if (nread != sizeof(system_info)) {
            fprintf(stderr, "system info read(%ld!=%ld) error=(%d:%s)\n",
                    nread, sizeof(system_info), errno, strerror(errno));
            ret = -1; break;
        }
        if (memcmp(pfx_gl.sysinfo.magic_code, pfx_gl.magic_code, SQUALL_MAGIC_CODELEN) != 0) {
            fprintf(stderr, "system magic code mismatch\n");
            ret = -1; break;
        }
        if (pfx_gl.sysinfo.spc_count > config->sq.max_prefix_count ||
            pfx_gl.sysinfo.pfx_count > config->sq.max_prefix_count) {
            fprintf(stderr, "system max prefix count mismatch\n");
            ret = -1; break;
        }
        offset += sizeof(system_info);

        /* get space count and prefix count and reset them */
        spccnt = pfx_gl.sysinfo.spc_count;
        pfxcnt = pfx_gl.sysinfo.pfx_count;

        /* read prefix spaces info */
        dskspc = (prefix_disk_space*)pfx_gl.page_buf;
        for (i = 0; i < spccnt; i++) {
            nread = disk_byte_pread(fd, dskspc, sizeof(prefix_disk_space), offset);
            if (nread != sizeof(prefix_disk_space)) {
                fprintf(stderr, "prefix space(id=%d) read(%ld!=%ld) error=(%d:%s)\n",
                        dskspc->space_id, nread, sizeof(prefix_disk_space), errno, strerror(errno));
                ret = -1; break;
            }
            if (dskspc->disk_file_size != config->sq.data_file_size ||
                dskspc->disk_page_size != config->sq.data_page_size) {
                fprintf(stderr, "prefix space(id=%d)  file or page size mismatch\n", dskspc->space_id);
                ret = -1; break;
            }
            offset += sizeof(prefix_disk_space);

            pfspace = do_pfx_space_alloc(dskspc->space_id);
            assert(pfspace != NULL);

            if (do_pfx_space_init(pfspace, dskspc) != 0) {
                do_pfx_space_free(pfspace);
                fprintf(stderr, "prefix space(id=%d) init error\n", dskspc->space_id);
                ret = -1; break;
            }
        }
        if (ret != 0) break;

        /* read prefix desciptor */
        dskdsc = (prefix_disk_desc*)pfx_gl.page_buf;
        for (i = 0; i < pfxcnt; i++) {
            nread = disk_byte_pread(fd, dskdsc, sizeof(prefix_disk_desc), offset);
            if (nread != sizeof(prefix_disk_desc)) {
                fprintf(stderr, "prefix desc(name=%s, id=%d) read(%ld!=%ld) error=(%d:%s)\n",
                        dskdsc->stats.name, dskdsc->prefix_id,
                        nread, sizeof(prefix_disk_desc), errno, strerror(errno));
                ret = -1; break;
            }
            if (dskdsc->use_cas != config->use_cas) {
                fprintf(stderr, "prefix desc(name=%s, id=%d) use_cas mismatch\n",
                        dskdsc->stats.name, dskdsc->prefix_id);
                ret = -1; break;
            }
            offset += sizeof(prefix_disk_desc);

            pfdesc = do_pfx_desc_alloc(dskdsc->prefix_id);
            assert(pfdesc != NULL);

            if (do_pfx_desc_init(pfdesc, dskdsc) != 0) {
                do_pfx_desc_free(pfdesc);
                fprintf(stderr, "prefix desc(name=%s, id=%d) init error\n",
                        dskdsc->stats.name, dskdsc->prefix_id);
                ret = -1; break;
            }

            /* link the prefix descriptor */
            pfhash = &pfx_gl.hash_tab[pfdesc->hval % pfx_gl.hash_size];
            do_pfx_hash_link(pfhash, pfdesc);
        }

        /* load all the map pages of prefix spaces */
        pfspace = do_pfx_space_get_first();
        while (pfspace != NULL) {
            if (do_pfx_space_load(pfspace) != 0) {
                ret = -1; break;
            }
            pfspace = do_pfx_space_get_next(pfspace->spcid);
        }

        /* set system start time */
        time(&pfx_gl.sysinfo.sys_start_time);
        /* increase system epoch */
        pfx_gl.sysinfo.sys_epoch += 1;

        /* write and sync system info */
        nwrite = disk_byte_pwrite(fd, &pfx_gl.sysinfo, sizeof(system_info), 0);
        if (nwrite != sizeof(system_info)) {
            fprintf(stderr, "system info write(%ld!=%ld) error=(%d:%s)\n",
                    nwrite, sizeof(system_info), errno, strerror(errno));
            ret = -1; break;
        }
        if (disk_fsync(fd) != 0) {
            fprintf(stderr, "anchor file sync error=(%d:%s)\n", errno, strerror(errno));
            ret = -1; break;
        }
    } while(0);

    disk_close(fd);
    return ret;
}

/* prefix delete queue: push and pop */
static void do_pfx_push_del_queue(prefix_desc *pfdesc)
{
    pfdesc->next = NULL;
    pthread_mutex_lock(&pfx_gl.gc_thread.lock);
    if (pfx_gl.del_queue.head == NULL) {
        pfx_gl.del_queue.head = pfdesc;
    } else {
        pfx_gl.del_queue.tail->next = pfdesc;
    }
    pfx_gl.del_queue.tail = pfdesc;
    pfx_gl.del_queue.size += 1;
    if (pfx_gl.gc_thread.sleep == true) {
        /* wake up prefix delete thead */
        pthread_cond_signal(&pfx_gl.gc_thread.cond);
    }
    pthread_mutex_unlock(&pfx_gl.gc_thread.lock);
}

static prefix_desc *do_pfx_pop_del_queue(void)
{
    /* pop an prefix descriptor from the head of delete queue */
    prefix_desc *pfdesc;

    pthread_mutex_lock(&pfx_gl.gc_thread.lock);
    pfdesc = pfx_gl.del_queue.head;
    if (pfdesc != NULL) {
        if (pfdesc->refcount == 0) {
            /* we can pop it */
            pfx_gl.del_queue.head = pfdesc->next;
            if (pfx_gl.del_queue.head == NULL) {
                pfx_gl.del_queue.tail = NULL;
            }
            pfx_gl.del_queue.size -= 1;
        } else {
            /* we cannot pop it */
            pfdesc = NULL;
        }
    }
    pthread_mutex_unlock(&pfx_gl.gc_thread.lock);
    return pfdesc;
}

/* prefix gc thread: sleep and wakeup */
static void do_pfx_gc_thread_sleep(void)
{
    struct timeval  tv;
    struct timespec to;

    /* 1 second sleep */
    gettimeofday(&tv, NULL);
    to.tv_sec = tv.tv_sec + 1;
    to.tv_nsec = tv.tv_usec * 1000;

    pthread_mutex_lock(&pfx_gl.gc_thread.lock);
    pfx_gl.gc_thread.sleep = true;
    pthread_cond_timedwait(&pfx_gl.gc_thread.cond,
                           &pfx_gl.gc_thread.lock, &to);
    pfx_gl.gc_thread.sleep = false;
    pthread_mutex_unlock(&pfx_gl.gc_thread.lock);
}

static void do_pfx_gc_thread_wakeup(void)
{
    pthread_mutex_lock(&pfx_gl.gc_thread.lock);
    if (pfx_gl.gc_thread.sleep == true) {
        /* wake up prefix gc thread */
        pthread_cond_signal(&pfx_gl.gc_thread.cond);
    }
    pthread_mutex_unlock(&pfx_gl.gc_thread.lock);
}

/* prefix gc thread: main function */
static void *pfx_gc_thread(void *arg)
{
    prefix_desc *pfdesc;

    assert(pfx_gl.gc_thread.init);

    pfx_gl.gc_thread.start = true;
    //fprintf(stderr, "prefix gc thread started\n");
    while (1)
    {
        pfdesc = do_pfx_pop_del_queue();
        if (pfdesc != NULL) {
            assert(pfdesc->refcount == 0);
            /* No one is accessing the prefix
             * so, delete the prefix
             */
            do_pfx_delete(pfdesc);
            continue;
        }

        if (pfx_gl.gc_thread.init == false) {
            assert(pfx_gl.del_queue.head == NULL);
            break;
        }
        do_pfx_gc_thread_sleep();
    }
    pfx_gl.gc_thread.start = false;
    //fprintf(stderr, "prefix gc thread terminated\n");
    return NULL;
}

/* prefix global data initialization */
static void do_pfx_global_init(void)
{
    int i;

    pthread_mutex_init(&pfx_gl.spc_lock, NULL);
    pthread_mutex_init(&pfx_gl.pfx_lock, NULL);

    /* make squall engine magic code */
    memcpy(pfx_gl.magic_code, "-SQUALL--ENGINE-", SQUALL_MAGIC_CODELEN);
    for (i = 0; i < SQUALL_MAGIC_CODELEN; i += 2) {
        pfx_gl.magic_code[i] = (char)i;
    }

    /* # of spaces and prefixes */
    pfx_gl.num_spces = 0;
    pfx_gl.max_spces = config->sq.max_prefix_count;
    pfx_gl.num_pfxes = 0;
    pfx_gl.max_pfxes = config->sq.max_prefix_count;

    /* free positions */
    pfx_gl.spc_freeLWM = 0;
    pfx_gl.spc_freeNXT = 0;
    pfx_gl.pfx_freeLWM = 0;
    pfx_gl.pfx_freeNXT = 0;

    /* hash table size */
    pfx_gl.hash_size = config->sq.max_prefix_count * 2;

#ifdef USE_WARM_FREE_PAGE
    /* several page sizes */
    pfx_gl.page_dv02_size = config->sq.data_page_size / 2;
    pfx_gl.page_dv04_size = config->sq.data_page_size / 4;
    pfx_gl.page_dv08_size = config->sq.data_page_size / 8;
    pfx_gl.page_dv16_size = config->sq.data_page_size / 16;
#endif

    /* initialize file space information */
    pfx_gl.npage_per_file = config->sq.data_file_size * ((1024*1024) / config->sq.data_page_size);
    //pfx_gl.npage_per_grow = (2*1024*1024) / config->sq.data_page_size; /* 2 MB */
    pfx_gl.npage_per_grow = (1024*1024) / config->sq.data_page_size; /* 1 MB */
    pfx_gl.page_body_size = config->sq.data_page_size - sizeof(PGHDR);
    pfx_gl.pmap_body_size = config->sq.data_page_size - sizeof(pmap_hdr);

    /* initialize page used and free bit masks */
    for (i = 0; i < 8; i++) {
        pfx_gl.bit1_used_mask[i] = (0x80 >> i);
        pfx_gl.bit1_free_mask[i] = ~pfx_gl.bit1_used_mask[i];
    }

    /* implicit empty prefix delete flag */
    pfx_gl.implicit_empty_prefix_delete = false;
}

static void do_pfx_del_queue_init(void)
{
    /* initialize prefix delete queue */
    pfx_gl.del_queue.head = NULL;
    pfx_gl.del_queue.tail = NULL;
    pfx_gl.del_queue.size = 0;
}

static void do_pfx_struct_final(void)
{
    if (pfx_gl.space_tab != NULL) {
        free(pfx_gl.space_tab); pfx_gl.space_tab = NULL;
    }
    if (pfx_gl.desc_tab != NULL) {
        free(pfx_gl.desc_tab); pfx_gl.desc_tab = NULL;
    }
    if (pfx_gl.hash_tab != NULL) {
        free(pfx_gl.hash_tab); pfx_gl.hash_tab = NULL;
    }
    if (pfx_gl.page_buf != NULL) {
        free(pfx_gl.page_buf); pfx_gl.page_buf = NULL;
    }
}

static int do_pfx_struct_init(void)
{
    int i, ret=0;

    pfx_gl.space_tab = NULL;
    pfx_gl.desc_tab = NULL;
    pfx_gl.hash_tab = NULL;
    pfx_gl.page_buf = NULL;

    do {
        /* allocate and initialize prefix space table */
        pfx_gl.space_tab = (prefix_space*)malloc(config->sq.max_prefix_count * sizeof(prefix_space));
        if (pfx_gl.space_tab == NULL) {
            fprintf(stderr, "prefix manager - prefix space table alloc error\n");
            ret = -1; break; /* no more memory */
        }
        for (i = 0; i < config->sq.max_prefix_count; i++) {
            memset(&pfx_gl.space_tab[i].d, 0, sizeof(prefix_disk_space));
            pfx_gl.space_tab[i].spcid = (uint16_t)i;
            pfx_gl.space_tab[i].pfxcount = 0;
            pfx_gl.space_tab[i].used = 0; /* set not-used */
            pthread_mutex_init(&pfx_gl.space_tab[i].data_lock, NULL);
            pthread_mutex_init(&pfx_gl.space_tab[i].ovfl_lock, NULL);
            pthread_mutex_init(&pfx_gl.space_tab[i].indx_lock, NULL);
        }

        /* allocate and initialize prefix desc table */
        pfx_gl.desc_tab = (prefix_desc*)malloc(config->sq.max_prefix_count * sizeof(prefix_desc));
        if (pfx_gl.desc_tab == NULL) {
            fprintf(stderr, "prefix manager - prefix descriptor table alloc error\n");
            ret = -1; break; /* no more memory */
        }
        for (i = 0; i < config->sq.max_prefix_count; i++) {
            memset(&pfx_gl.desc_tab[i].d, 0, sizeof(prefix_disk_desc));
            pfx_gl.desc_tab[i].pfxid = (uint16_t)i;
            pfx_gl.desc_tab[i].refcount = 0;
            pfx_gl.desc_tab[i].used = 0; /* set not-used */
            pfx_gl.desc_tab[i].lnked = 0; /* set unlinked */
            //pthread_mutex_init(&pfx_gl.desc_tab[i].lock, NULL);
            pthread_mutex_init(&pfx_gl.desc_tab[i].root_lock, NULL);
#ifdef USE_WARM_FREE_PAGE
            pthread_mutex_init(&pfx_gl.desc_tab[i].warm_lock, NULL);
#endif
            pthread_mutex_init(&pfx_gl.desc_tab[i].stat_lock, NULL);
        }

        /* allocate and initialize prefix hash table */
        pfx_gl.hash_tab = (prefix_hash*)malloc(pfx_gl.hash_size * sizeof(prefix_hash));
        if (pfx_gl.hash_tab == NULL) {
            fprintf(stderr, "prefix manager - prefix hash table alloc error\n");
            ret = -1; break; /* no more memory */
        }
        for (i = 0; i < pfx_gl.hash_size; i++) {
            pthread_mutex_init(&pfx_gl.hash_tab[i].lock, NULL);
            pfx_gl.hash_tab[i].next = NULL;
        }

        /* allocate page buffer for flushing : page size alignement needed */
        if (posix_memalign((void**)&pfx_gl.page_buf, (size_t)config->sq.data_page_size,
                           (size_t)config->sq.data_page_size) != 0) {
            fprintf(stderr, "prefix manager - page buffer alloc error\n");
            ret = -1; break; /* no more memory */
        }
    } while(0);

    if (ret != 0) {
        do_pfx_struct_final();
    }
    return ret;
}

static void do_pfx_gc_thread_init(void)
{
    pthread_mutex_init(&pfx_gl.gc_thread.lock, NULL);
    pthread_cond_init(&pfx_gl.gc_thread.cond, NULL);
    pfx_gl.gc_thread.sleep = false;
    pfx_gl.gc_thread.start = false;
    pfx_gl.gc_thread.init = false;
}

static int do_pfx_gc_thread_start(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.
    pthread_t tid;
    int       ret;

    /* start request */
    pfx_gl.gc_thread.init = true;

    /* create prefix gc thread */
    ret = pthread_create(&tid, NULL, pfx_gc_thread, NULL);
    if (ret != 0) {
        pfx_gl.gc_thread.init = false;
        fprintf(stderr, "Can't create prefix deelete thread: %s\n", strerror(ret));
        return -1;
    }
    /* wait until the prefix gc thread starts */
    while (pfx_gl.gc_thread.start == false) {
        nanosleep(&sleep_time, NULL);
    }
    return 0;
}

static void do_pfx_gc_thread_stop(void)
{
    struct timespec sleep_time = {0, 10000000}; // 10 msec.

    if (pfx_gl.gc_thread.init == true) {
        /* stop request */
        pthread_mutex_lock(&pfx_gl.gc_thread.lock);
        pfx_gl.gc_thread.init = false;
        pthread_mutex_unlock(&pfx_gl.gc_thread.lock);

        /* wait until the prefix gc thread stops */
        while (pfx_gl.gc_thread.start == true) {
            if (pfx_gl.gc_thread.sleep == true) {
                do_pfx_gc_thread_wakeup();
            }
            nanosleep(&sleep_time, NULL);
        }
    }
}

/*
 * Prefix Public Functions
 */
int pfx_mgr_init(void *conf, void *srvr)
{
    config = (struct config *)conf;
    server = (SERVER_HANDLE_V1 *)srvr;
    logger = server->log->get_logger();

    pfx_gl.initialized = false;

    do_pfx_global_init();
    if (do_pfx_struct_init() != 0) {
        return -1;
    }
    if (do_pfx_disk_load(config->sq.dbidx) != 0) {
        do_pfx_disk_unload(false);
        do_pfx_struct_final();
        return -1;
    }

    do_pfx_del_queue_init();
    do_pfx_gc_thread_init();

    pfx_gl.initialized = true;
    return 0;
}

void pfx_mgr_final(void)
{
    /* do prefix final tasks */
    do_pfx_disk_unload(true);
    do_pfx_struct_final();
}

int pfx_mgr_gc_thread_start(void)
{
    return do_pfx_gc_thread_start();
}

void pfx_mgr_gc_thread_stop(void)
{
    do_pfx_gc_thread_stop();
}

prefix_desc *pfx_find(const char *prefix, const size_t nprefix)
{
    uint32_t     hval   = server->core->hash(prefix, nprefix, 0);
    prefix_hash *pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
    prefix_desc *pfdesc;

    pthread_mutex_lock(&pfhash->lock);
    pfdesc = do_pfx_hash_find(pfhash, prefix, nprefix);
    pthread_mutex_unlock(&pfhash->lock);
    return pfdesc;
}

int pfx_open(const char *prefix, const size_t nprefix, bool create)
{
    uint32_t     hval   = server->core->hash(prefix, nprefix, 0);
    prefix_hash *pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
    prefix_desc *pfdesc;

    pthread_mutex_lock(&pfhash->lock);
    pfdesc = do_pfx_hash_find(pfhash, prefix, nprefix);
    if (pfdesc != NULL) {
        pfdesc->refcount += 1;
    } else {
        if (create) {
            pfdesc = do_pfx_create(hval, prefix, nprefix, false);
            if (pfdesc != NULL) {
                pfdesc->refcount += 1;
            }
        }
    }
    pthread_mutex_unlock(&pfhash->lock);
    return (pfdesc != NULL ? (int)pfdesc->pfxid : -1);
}

void pfx_open_with_id(int pfxid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    prefix_hash *pfhash = &pfx_gl.hash_tab[pfdesc->hval % pfx_gl.hash_size];

    pthread_mutex_lock(&pfhash->lock);
    pfdesc->refcount += 1;
    pthread_mutex_unlock(&pfhash->lock);
}

int pfx_open_first(void)
{
    prefix_hash *pfhash;
    prefix_desc *pfdesc;
    uint32_t     svhash; /* saved hash value */
    bool         opened = false;

    pfdesc = do_pfx_desc_get_first();
    while (pfdesc != NULL) {
        svhash = pfdesc->hval;
        pfhash = &pfx_gl.hash_tab[svhash % pfx_gl.hash_size];
        pthread_mutex_lock(&pfhash->lock);
        if (pfdesc->used == 1 && svhash == pfdesc->hval) {
            pfdesc->refcount += 1;
            opened = true;
        }
        pthread_mutex_unlock(&pfhash->lock);
        if (opened == true) break;
        pfdesc = do_pfx_desc_get_next(pfdesc->pfxid);
    }
    return (pfdesc == NULL ? -1 : pfdesc->pfxid);
}

int pfx_open_next(int pfxid)
{
    prefix_hash *pfhash;
    prefix_desc *pfdesc;
    uint32_t     svhash; /* saved hash value */
    bool         opened = false;

    pfdesc = do_pfx_desc_get_next(pfxid);
    while (pfdesc != NULL) {
        svhash = pfdesc->hval;
        pfhash = &pfx_gl.hash_tab[svhash % pfx_gl.hash_size];
        pthread_mutex_lock(&pfhash->lock);
        if (pfdesc->used == 1 && svhash == pfdesc->hval) {
            pfdesc->refcount += 1;
            opened = true;
        }
        pthread_mutex_unlock(&pfhash->lock);
        if (opened == true) break;
        pfdesc = do_pfx_desc_get_next(pfdesc->pfxid);
    }
    return (pfdesc == NULL ? -1 : pfdesc->pfxid);
}

void pfx_close(int pfxid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    prefix_hash *pfhash = &pfx_gl.hash_tab[pfdesc->hval % pfx_gl.hash_size];

    pthread_mutex_lock(&pfhash->lock);
    pfdesc->refcount -= 1;
    if (pfdesc->refcount == 0) {
        /* what to do ?? */
        /* check if the prefix is empty */
        if (pfx_gl.implicit_empty_prefix_delete) {
            int i;
            for (i = 0; i < ITEM_TYPE_MAX; i++) {
                 if (pfdesc->d.stats.num_items[i] > 0) break;
            }
            if (i == ITEM_TYPE_MAX) { /* empty prefix */
                /* push the request to delete the prefix */
                do_pfx_push_del_queue(pfdesc);
            }
        }
    }
    pthread_mutex_unlock(&pfhash->lock);
}

ENGINE_ERROR_CODE pfx_create(const char *prefix, const size_t nprefix, int *pfxid)
{
    ENGINE_ERROR_CODE ret;
    uint32_t     hval   = server->core->hash(prefix, nprefix, 0);
    prefix_hash *pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
    prefix_desc *pfdesc;

    /* prefix create/delete operation is serialized by prefix hash lock */
    pthread_mutex_lock(&pfhash->lock);
    /* check the prefix uniqueness */
    if (do_pfx_hash_find(pfhash, prefix, nprefix) != NULL) {
        ret = ENGINE_PREFIX_EEXIST;
    } else {
        pfdesc = do_pfx_create(hval, prefix, nprefix, true);
        if (pfdesc != NULL) {
            *pfxid = (int)pfdesc->pfxid;
            ret = ENGINE_SUCCESS;
        } else {
            ret = ENGINE_PREFIX_EOVERFLOW;
        }
    }
    pthread_mutex_unlock(&pfhash->lock);
    return ret;
}

static int do_pfx_delete_one(const char *prefix, const size_t nprefix)
{
    prefix_hash *pfhash;
    prefix_desc *pvdesc;
    prefix_desc *pfdesc;
    uint32_t     hval = server->core->hash(prefix, nprefix, 0);
    int          ret;

    pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
    pvdesc = NULL;

    pthread_mutex_lock(&pfhash->lock);
    pfdesc = pfhash->next;
    while (pfdesc != NULL) {
        if (pfdesc->nlen == nprefix && memcmp(pfdesc->name, prefix, nprefix)==0)
            break;
        pvdesc = pfdesc; pfdesc = pfdesc->next;
    }
    if (pfdesc != NULL) {
        /* unlink the prefix descriptor from prefix hash chain */
        do_pfx_hash_unlink(pfhash, pvdesc, pfdesc);

#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery) { /* TRANSACTION */
            /* prefix (and prefix space) drop */
            /* NTS(nested top action) : redo-only */
            PFDropLog pfdrp_log;
            lrec_pf_drop_init((LogRec*)&pfdrp_log);
            pfdrp_log.body.pfxid = pfdesc->pfxid;
            pfdrp_log.body.spcid = pfdesc->space->spcid;
            pfdrp_log.body.nprefix = (uint16_t)nprefix;
            pfdrp_log.prefix = (void*)prefix;
            pfdrp_log.header.length = GET_8_ALIGN_SIZE(offsetof(PFDropData,data) + nprefix);

            log_record_write((LogRec*)&pfdrp_log, NULL);
            /* update PageLSN */
            pfdesc->space->d.updlsn = pfdrp_log.header.self_lsn;
            pfdesc->d.updlsn = pfdrp_log.header.self_lsn;
        }
#endif
        /* Just push the prefix descriptor into the prefix delete queue.
         * Then, prefix gc thread will delete the prefix in background.
         */
        do_pfx_push_del_queue(pfdesc);
        ret = 0;
    } else {
        ret = -1;
    }
    pthread_mutex_unlock(&pfhash->lock);

    return ret;
}

static void do_pfx_delete_all(void)
{
    prefix_hash *pfhash;
    prefix_desc *pfdesc;

    for (int i = 0; i < pfx_gl.hash_size; i++) {
        pfhash = &pfx_gl.hash_tab[i];
        if (pfhash->next == NULL) continue;

        pthread_mutex_lock(&pfhash->lock);
        while ((pfdesc = pfhash->next) != NULL) {
            /* unlink the prefix descriptor from prefix hash chain */
            do_pfx_hash_unlink(pfhash, NULL, pfdesc);

#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                /* prefix (and prefix space) drop */
                /* NTS(nested top action) : redo-only */
                PFDropLog pfdrp_log;
                lrec_pf_drop_init((LogRec*)&pfdrp_log);
                pfdrp_log.body.pfxid = pfdesc->pfxid;
                pfdrp_log.body.spcid = pfdesc->space->spcid;
                pfdrp_log.body.nprefix = pfdesc->nlen;
                pfdrp_log.prefix = pfdesc->name;
                pfdrp_log.header.length = GET_8_ALIGN_SIZE(offsetof(PFDropData,data) + pfdesc->nlen);

                log_record_write((LogRec*)&pfdrp_log, NULL);
                /* update PageLSN */
                pfdesc->space->d.updlsn = pfdrp_log.header.self_lsn;
                pfdesc->d.updlsn = pfdrp_log.header.self_lsn;
            }
#endif
            /* Just push the prefix descriptor into the prefix delete queue.
             * Then, prefix gc thread will delete the prefix in background.
             */
            do_pfx_push_del_queue(pfdesc);
        }
        pthread_mutex_unlock(&pfhash->lock);
    }
}

ENGINE_ERROR_CODE pfx_delete(const char *prefix, const size_t nprefix)
{
    /* prefix create/delete operation must be serialized by prefix hash lock */
    if (nprefix >= 0) { /* delete given prefix only */
        if (do_pfx_delete_one(prefix, nprefix) != 0) {
            return ENGINE_PREFIX_ENOENT;
        }
    } else { /* nprefix == -1: delete all prefixes */
        do_pfx_delete_all();
    }
    return ENGINE_SUCCESS;
}

void pfx_data_anchor_flush(void)
{
    char anch_name[MAX_FILE_NAME_LENG];
    sprintf(anch_name, "%s/squall_data_anchor_%01d", config->sq.data_path, 0);

    do_pfx_disk_flush(anch_name);
}

static ENGINE_ERROR_CODE do_pfx_space_check(prefix_space *pfspace)
{
    if (do_pfx_flmgt_check(&pfspace->data_flmgt, pfspace->d.data_total_flcnt, pfspace->d.data_total_pgcnt) != 0) {
        return ENGINE_EINCONSISTENT;
    }
    if (do_pfx_flmgt_check(&pfspace->ovfl_flmgt, pfspace->d.ovfl_total_flcnt, pfspace->d.ovfl_total_pgcnt) != 0) {
        return ENGINE_EINCONSISTENT;
    }
    if (do_pfx_flmgt_check(&pfspace->indx_flmgt, pfspace->d.indx_total_flcnt, pfspace->d.indx_total_pgcnt) != 0) {
        return ENGINE_EINCONSISTENT;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE pfx_space_dbcheck(void)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    prefix_space *pfspace = do_pfx_space_get_first();
    while (pfspace != NULL) {
        ret = do_pfx_space_check(pfspace);
        if (ret != ENGINE_SUCCESS) {
            break;
        }
        pfspace = do_pfx_space_get_next(pfspace->spcid);
    }
    return ret;
}

prefix_desc *pfx_desc_get(int pfxid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    return &pfx_gl.desc_tab[pfxid];
}

prefix_desc *pfx_desc_get_first(void)
{
    return do_pfx_desc_get_first();
}

prefix_desc *pfx_desc_get_next(int pfxid)
{
    return do_pfx_desc_get_next(pfxid);
}

size_t pfx_get_length(const char *key, const size_t nkey)
{
    char *token = memchr(key, config->prefix_delimiter, nkey);
    return (token != NULL ? token - key : 0);
}

int pfx_get_stats(const char *prefix, const int nprefix, struct prefix_stat *stats)
{
    int count = 0;

    /* do not need to hold prefix lock */
    if (nprefix < 0) { /* all prefix stats */
        for (uint16_t i = 0; i < pfx_gl.pfx_freeNXT; i++) {
            if (pfx_gl.desc_tab[i].used && pfx_gl.desc_tab[i].lnked) {
                stats[count] = pfx_gl.desc_tab[i].d.stats;
                count += 1;
            }
        }
    } else { /* the given prefix stats */
        uint32_t     hval   = server->core->hash(prefix, nprefix, 0);
        prefix_hash *pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
        prefix_desc *pfdesc;

        pthread_mutex_lock(&pfhash->lock);
        pfdesc = do_pfx_hash_find(pfhash, prefix, nprefix);
        pthread_mutex_unlock(&pfhash->lock);
        if (pfdesc != NULL) {
            stats[count] = pfdesc->d.stats;
            count += 1;
        }
    }
    return count;
}

bool pfx_use_cas(int pfxid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    assert(pfdesc->used == 1 && pfdesc->lnked == 1);
    return (pfdesc->d.use_cas != 0 ? true : false);
}

void pfx_item_stats_arith(int pfxid, int type, int count, int size, uint64_t *tot_count, uint64_t *tot_size)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    assert(pfdesc->used == 1 && pfdesc->lnked == 1);

    pthread_mutex_lock(&pfdesc->stat_lock);
    if (count != 0) pfdesc->d.stats.num_items[type] += count;
    pfdesc->d.stats.siz_items[type] += size;
    if (tot_count != NULL) *tot_count = pfdesc->d.stats.num_items[type];
    if (tot_size  != NULL) *tot_size  = pfdesc->d.stats.siz_items[type];
    pthread_mutex_unlock(&pfdesc->stat_lock);
}

/************************/
/*** space management ***/
/************************/

static inline bool do_pfx_disk_quota_check(prefix_space *pfspace)
{
    prefix_disk_space *dskspc = &pfspace->d;
    if (dskspc->disk_quota_pgcnt == 0) return true;

    uint64_t sum_pgcnt = dskspc->data_total_pgcnt + dskspc->ovfl_total_pgcnt
                       + dskspc->indx_total_pgcnt + pfx_gl.npage_per_grow;
    return (sum_pgcnt <= dskspc->disk_quota_pgcnt ? true : false);
}

static void do_pfx_fldsc_link(file_mgmt *flmgt, file_desc *fldsc, int flnum, int frtype)
{
    if (frtype == FREE_TYPE_TOTAL) {
        if (flmgt->tfree.fhead == 0) {
            fldsc->tfree.fnext = 0;
            flmgt->tfree.fhead = flnum;
            flmgt->tfree.ftail = flnum;
        }
        else if (flnum < flmgt->tfree.fhead) {
            fldsc->tfree.fnext = flmgt->tfree.fhead;
            flmgt->tfree.fhead = flnum;
        }
        else if (flnum > flmgt->tfree.ftail) {
            fldsc->tfree.fnext = 0;
            flmgt->fldsc_tab[flmgt->tfree.ftail-1]->tfree.fnext = flnum;
            flmgt->tfree.ftail = flnum;
        }
        else { /* flmgt->tfree.fhead < flnum < flmgt->tfree.ftail */
            /* find the previous file desc */
            int prvfn; /* prev file number */
            for (prvfn = flnum-1; prvfn >= 1; prvfn--) {
                if (flmgt->fldsc_tab[prvfn-1]->tfree.pgcnt > 0) break;
            }
            assert(prvfn >= 1);
            fldsc->tfree.fnext = flmgt->fldsc_tab[prvfn-1]->tfree.fnext;
            flmgt->fldsc_tab[prvfn-1]->tfree.fnext = flnum;
        }
    } else { /* FREE_TYPE_PARTIAL */
        if (flmgt->pfree.fhead == 0) {
            fldsc->pfree.fnext = 0;
            flmgt->pfree.fhead = flnum;
            flmgt->pfree.ftail = flnum;
        }
        else if (flnum < flmgt->pfree.fhead) {
            fldsc->pfree.fnext = flmgt->pfree.fhead;
            flmgt->pfree.fhead = flnum;
        }
        else if (flnum > flmgt->pfree.ftail) {
            fldsc->pfree.fnext = 0;
            flmgt->fldsc_tab[flmgt->pfree.ftail-1]->pfree.fnext = flnum;
            flmgt->pfree.ftail = flnum;
        }
        else { /* flmgt->pfree.fhead < flnum < flmgt->pfree.ftail */
            /* find the previous file desc */
            int prvfn; /* prev file number */
            for (prvfn = flnum-1; prvfn >= 1; prvfn--) {
                if (flmgt->fldsc_tab[prvfn-1]->pfree.pgcnt > 0) break;
            }
            assert(prvfn >= 1);
            fldsc->pfree.fnext = flmgt->fldsc_tab[prvfn-1]->pfree.fnext;
            flmgt->fldsc_tab[prvfn-1]->pfree.fnext = flnum;
        }
    }
}

static void do_pfx_fldsc_unlink(file_mgmt *flmgt, file_desc *fldsc, int flnum, int frtype)
{
    if (frtype == FREE_TYPE_TOTAL) {
        if (flnum == flmgt->tfree.fhead) {
            flmgt->tfree.fhead = fldsc->tfree.fnext;
            if (flmgt->tfree.fhead == 0)
                flmgt->tfree.ftail = 0;
        }
        else { /* flmgt->tfree.fhead < flnum <= flmgt->tfree.ftail */
            /* find the previous file desc */
            int prvfn; /* prev file number */
            for (prvfn = flnum-1; prvfn >= 1; prvfn--) {
                if (flmgt->fldsc_tab[prvfn-1]->tfree.pgcnt > 0) break;
            }
            assert(prvfn >= 1);
            flmgt->fldsc_tab[prvfn-1]->tfree.fnext = fldsc->tfree.fnext;
            if (flmgt->tfree.ftail == flnum)
                flmgt->tfree.ftail = prvfn;
        }
        fldsc->tfree.fnext = 0;
    } else { /* FREE_TYPE_PARTIAL */
        if (flnum == flmgt->pfree.fhead) {
            flmgt->pfree.fhead = fldsc->pfree.fnext;
            if (flmgt->pfree.fhead == 0)
                flmgt->pfree.ftail = 0;
        }
        else { /* flmgt->pfree.fhead < flnum <= flmgt->pfree.ftail */
            /* find the previous file desc */
            int prvfn; /* prev file number */
            for (prvfn = flnum-1; prvfn >= 1; prvfn--) {
                if (flmgt->fldsc_tab[prvfn-1]->pfree.pgcnt > 0) break;
            }
            assert(prvfn >= 1);
            flmgt->fldsc_tab[prvfn-1]->pfree.fnext = fldsc->pfree.fnext;
            if (flmgt->pfree.ftail == flnum)
                flmgt->pfree.ftail = prvfn;
        }
        fldsc->pfree.fnext = 0;
    }
}

static int do_pfx_file_space_grow(file_mgmt *flmgt, const uint32_t flnum, const uint32_t pgnum,
                                  const uint32_t grow_pgcnt)
{
    file_desc *fldsc;
    pmap_page *pmapg;
    int        pmidx;
    int        free_pgcnt;

    fldsc = flmgt->fldsc_tab[flnum-1];
    assert(fldsc != NULL);

    /* The newly growed pages will be represented by single pmap page */
    pmidx = (pgnum / flmgt->npage_ppmap);
    assert(pmidx == ((pgnum + grow_pgcnt - 1) / flmgt->npage_ppmap));
    assert((pgnum % grow_pgcnt) == 0 && (flmgt->npage_ppmap % grow_pgcnt) == 0);

    /* adjust page maps to make it to include new free pages */
    if (fldsc->pmaps[pmidx] == NULL) {
        PageID mpgid;
        mpgid.pfxid = MAKE_PAGEID_PFXID(flmgt->ftype, flmgt->spcid);
        mpgid.flnum = fldsc->flnum;
        mpgid.pgnum = (pmidx * flmgt->npage_ppmap);

        fldsc->pmaps[pmidx] = (pmap_page*)buf_permanent_pin(mpgid, true); /* NEW PAGE */
        if (fldsc->pmaps[pmidx] == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[FATAL] prefix space(%d) %s file(%d) grow fail - permanent pin buffer area is full.\n",
                        flmgt->spcid, get_file_type_str(flmgt->ftype), fldsc->flnum);
            return -2; /* Out of (buffer) memory */
        }
        int partial_nbitcnt = (flmgt->ftype == SQUALL_FILE_TYPE_DATA ? 1 : 0);
        mpage_init(fldsc->pmaps[pmidx], mpgid, partial_nbitcnt);
    }
    pmapg = fldsc->pmaps[pmidx];

    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    assert(pmapg->hdr.tot_pgs == (pgnum % flmgt->npage_ppmap));
    free_pgcnt = mpage_grow_space(pmapg, grow_pgcnt);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* file space grow */
        /* NTS(nested top action) : redo-only */
        /* redo info : <spcid, ftype, flnum, pgnum, pgcnt> */
        FLGrowLog flgrw_log;
        lrec_fl_grow_init((LogRec*)&flgrw_log);
        flgrw_log.body.spcid = flmgt->spcid;
        flgrw_log.body.ftype = flmgt->ftype;
        flgrw_log.body.flnum = flnum;
        flgrw_log.body.pgnum = pgnum;
        flgrw_log.body.pgcnt = grow_pgcnt;
        flgrw_log.header.length = sizeof(FLGrowData);

        log_record_write((LogRec*)&flgrw_log, NULL);
        /* update PageLSN */
        buf_set_lsn((Page*)pmapg, flgrw_log.header.self_lsn);
    }
#endif

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */
    buf_permanent_unlatch((Page*)pmapg);

    /* increment # of pages in the file */
    fldsc->npage += grow_pgcnt;

    /* increment # of total free pages */
    fldsc->pmtfs[pmidx] += free_pgcnt;
    fldsc->tfree.pgcnt += free_pgcnt;
    if (fldsc->tfree.pgcnt == free_pgcnt) { /* link into total free file list */
        do_pfx_fldsc_link(flmgt, fldsc, flnum, FREE_TYPE_TOTAL);
    }
    flmgt->tfree.pgcnt += free_pgcnt;
    return 0;
}

static int do_pfx_data_space_grow(prefix_space *pfspace)
{
    file_mgmt *flmgt = &pfspace->data_flmgt;

    /* check prefix disk quota */
    if (do_pfx_disk_quota_check(pfspace) == false) {
        return 0; /* no more disk space allowed */
    }

    /* Assume that (pfx_gl.npage_per_file % pfx_gl.npage_per_grow) == 0 */
    /* create a new file if needed */
    if ((pfspace->d.data_total_pgcnt % pfx_gl.npage_per_file) == 0 &&
        (pfspace->d.data_total_pgcnt / pfx_gl.npage_per_file) == pfspace->d.data_total_flcnt)
    {
        if (do_pfx_file_create(flmgt, pfspace->d.data_total_flcnt+1) < 0) {
            return -1; /* file create fail */
        }
        pfspace->d.data_total_flcnt += 1;
    }

    /* initialize page map for the new free pages and adjust free file list */
    if (do_pfx_file_space_grow(flmgt, pfspace->d.data_total_flcnt,
                               pfspace->d.data_total_pgcnt % pfx_gl.npage_per_file,
                               pfx_gl.npage_per_grow) < 0){
        return -1; /* buffer permanent pin fail */
    }
    pfspace->d.data_total_pgcnt += pfx_gl.npage_per_grow;

    return pfx_gl.npage_per_grow;
}

static int do_pfx_data_page_alloc(prefix_desc *pfdesc, PageID *pgid)
{
    file_mgmt *flmgt = &pfdesc->space->data_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    uint16_t   flnum;
    int        pmidx;
    int        pbidx;

#if 0 // Consider this case later
    if (pfdesc->d.private_space == 0) {
        /* shared prefix space */
    }
#endif

    /* Assume that private prefix space is used */
    pthread_mutex_lock(&pfdesc->space->data_lock);

    /* grow the space if it's lack of free pages */
    if (flmgt->tfree.pgcnt < 1) {
        int ret = do_pfx_data_space_grow(pfdesc->space);
        if (ret <= 0) {
            pthread_mutex_unlock(&pfdesc->space->data_lock);
            return ret;
        }
    }
    assert(flmgt->tfree.pgcnt > 0);

    /* get data file having free page */
    flnum = flmgt->tfree.fhead;
    assert(flnum > 0);
    fldsc = flmgt->fldsc_tab[flnum-1];

    /* find pgmap of the file having free page */
    for (pmidx = 0; pmidx < flmgt->npmap_pfile; pmidx++) {
        if (fldsc->pmtfs[pmidx] > 0) break;
    }
    assert(pmidx < flmgt->npmap_pfile);

    /* decrement # of total free pages */
    fldsc->pmtfs[pmidx] -= 1;
    fldsc->tfree.pgcnt -= 1;
    if (fldsc->tfree.pgcnt == 0) { /* unlink from total free file list */
        do_pfx_fldsc_unlink(flmgt, fldsc, flnum, FREE_TYPE_TOTAL);
    }
    flmgt->tfree.pgcnt -= 1;

    pthread_mutex_unlock(&pfdesc->space->data_lock);

    pmapg = fldsc->pmaps[pmidx];
    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    pbidx = mpage_alloc_page(pmapg);
    buf_permanent_unlatch((Page*)pmapg);
    assert(pbidx > 0);

    /* get pageid of the found free page */
    pgid->pfxid = SQUALL_FILEBITS_DATA | pfdesc->pfxid;
    pgid->flnum = flnum;
    pgid->pgnum = (pmidx * flmgt->npage_ppmap) + pbidx;
    return 1;
}

static void do_pfx_data_page_free(prefix_desc *pfdesc, PageID pgid)
{
    file_mgmt *flmgt = &pfdesc->space->data_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    int        pmidx;
    int        pbidx;

    pmidx = pgid.pgnum / flmgt->npage_ppmap;
    pbidx = pgid.pgnum % flmgt->npage_ppmap;
    assert(pbidx > 0);

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    pmapg = fldsc->pmaps[pmidx];

    /* It is guranteed that the page is not a parital page */
    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    mpage_free_page(pmapg, pbidx);
    buf_permanent_unlatch((Page*)pmapg);

    pthread_mutex_lock(&pfdesc->space->data_lock);
    /* increment # of total free pages */
    fldsc->pmtfs[pmidx] += 1;
    fldsc->tfree.pgcnt += 1;
    if (fldsc->tfree.pgcnt == 1) { /* link into total free file list */
        do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_TOTAL);
    }
    flmgt->tfree.pgcnt += 1;
    pthread_mutex_unlock(&pfdesc->space->data_lock);
}

static int do_pfx_data_pfrpg_get(prefix_desc *pfdesc, PageID *pgid)
{
    file_mgmt *flmgt = &pfdesc->space->data_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    uint16_t   flnum;
    int        pmidx = -1;
    int        pbidx;

    if (flmgt->pfree.pgcnt == 0) {
        return 0; /* no partial free page */
    }

    pthread_mutex_lock(&pfdesc->space->data_lock);
    if (flmgt->pfree.pgcnt > 0) {
        /* get data file having partial free page */
        flnum = flmgt->pfree.fhead; assert(flnum > 0);
        fldsc = flmgt->fldsc_tab[flnum-1];

        /* find pmap having partial free page */
        for (pmidx = 0; pmidx < flmgt->npmap_pfile; pmidx++) {
            if (fldsc->pmpfs[pmidx] > 0) break;
        }
        assert(pmidx < flmgt->npmap_pfile);

        /* decrement # of partial free pages */
        fldsc->pmpfs[pmidx] -= 1;
        fldsc->pfree.pgcnt -= 1;
        if (fldsc->pfree.pgcnt == 0) { /* unlink from partial free file list */
            do_pfx_fldsc_unlink(flmgt, fldsc, flnum, FREE_TYPE_PARTIAL);
        }
        flmgt->pfree.pgcnt -= 1;
    }
    pthread_mutex_unlock(&pfdesc->space->data_lock);
    if (pmidx == -1) {
        return 0; /* no partial free page */
    }

    /* allocate a partial free page */
    pmapg = fldsc->pmaps[pmidx];
    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    pbidx = mpage_get_partial_page(pmapg);
    buf_permanent_unlatch((Page*)pmapg);
    assert(pbidx > 0);

    /* get pageid of the found partial free page */
    pgid->pfxid = SQUALL_FILEBITS_DATA | pfdesc->pfxid;
    pgid->flnum = flnum;
    pgid->pgnum = (pmidx * flmgt->npage_ppmap) + pbidx;
    return 1;
}

static void do_pfx_data_pfrpg_set(prefix_desc *pfdesc, PageID pgid)
{
    file_mgmt *flmgt = &pfdesc->space->data_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    int        pmidx;
    int        pbidx;

    pmidx = pgid.pgnum / flmgt->npage_ppmap;
    pbidx = pgid.pgnum % flmgt->npage_ppmap;
    assert(pbidx > 0);

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    pmapg = fldsc->pmaps[pmidx];

    pthread_mutex_lock(&pfdesc->space->data_lock);
    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    mpage_set_partial_page(pmapg, pbidx);
    buf_permanent_unlatch((Page*)pmapg);

    /* increment # of partial free pages */
    fldsc->pmpfs[pmidx] += 1;
    fldsc->pfree.pgcnt += 1;
    if (fldsc->pfree.pgcnt == 1) { /* link into partial free file list */
        do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
    }
    flmgt->pfree.pgcnt += 1;
    pthread_mutex_unlock(&pfdesc->space->data_lock);
}

static bool do_pfx_data_pfrpg_clear(prefix_desc *pfdesc, PageID pgid)
{
    file_mgmt *flmgt = &pfdesc->space->data_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    int        pmidx;
    int        pbidx;
    bool       cleared = true;

    pmidx = pgid.pgnum / flmgt->npage_ppmap;
    pbidx = pgid.pgnum % flmgt->npage_ppmap;
    assert(pbidx > 0);

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    pmapg = fldsc->pmaps[pmidx];

    /* [NOTE] ****
     * When the to-be-freed page was a partial free page before,
     * there might be some threads that try to allocate slots from the page.
     * So. the bit mark for partial free page must be cleared ahead.
     * For this bit clear task, data lock must be held before doing that.
     *
     * The to-be-free page has value of NULL_PAGEID in the page header.
     * So, even if other thread exists accessing the paeg to allocate slots
     * before the partial page bit mark of the page is cleared,
     * the thread will give up using the page
     * after knowing that the pageid of the page is NULL_PAGEID.
     */
    pthread_mutex_lock(&pfdesc->space->data_lock);
    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    if (do_mpage_is_partial_page(pmapg, pbidx) == true)
    {
        if (fldsc->pmpfs[pmidx] == pmapg->hdr.pfr_pgs) {
            /* decrement # of partial free pages */
            fldsc->pmpfs[pmidx] -= 1;
            fldsc->pfree.pgcnt -= 1;
            if (fldsc->pfree.pgcnt == 0) { /* unlink from partial free file list */
                do_pfx_fldsc_unlink(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
            }
            flmgt->pfree.pgcnt -= 1;
            /* clear partial free page */
            do_mpage_clear_partial_page(pmapg, pbidx);
        } else {
            assert(fldsc->pmpfs[pmidx] < pmapg->hdr.pfr_pgs);
            /* There are some threads that try to allocate partial pages.
             * Following concurrency control guarantees the above statement.
             * - set partial page
             *   => do that while holding both data_lock and pmap page latch
             * - get partial page
             *   => reserve the page by decrement pmpfs while holding data_lock only.
             *   => allocate the page from the pmap page while holding pmap page latch.
             */
            cleared = false;
        }
    }
    buf_permanent_unlatch((Page*)pmapg);
    pthread_mutex_unlock(&pfdesc->space->data_lock);
    return cleared;
}


#ifdef USE_WARM_FREE_PAGE
/* allocate data slot from warm free page */
static int do_pfx_data_slot_from_wfrpg(prefix_desc *pfdesc, uint32_t size, ItemDesc *itdesc)
{
    Page       *pgptr;
    PageID      pgid;
    uint32_t    ntotal = size+sizeof(PGSLOT);
    int         trycount = 3;
    int         wmid;

    /* [NOTE] we must check the validity of warm free page.
     * After getting a warm free page, we fix the page.
     * During the time, the page might be freed. So, the page can be empty.
     * In a more severe case, the page might be freed and re-registered as warm free page.
     * At that moment, the free size might be different than expected.
     */
    while (1)
    {
        if (--trycount < 0) {
            fprintf(stderr, "[INFO] Fail to get warm free page (trycount=3)\n");
            return 0; /* not found */
        }
        wmid = do_pfx_data_wfrpg_get(pfdesc, ntotal, &pgid);
        if (wmid == -1) {
            return 0; /* no warm free page */
        }
        pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
        if (pgptr == NULL) {
            /* Do not need to restore warm free page info */
            /* Because, it's SEVERE error enough to stop the process */
            fprintf(stderr, "The warm page(%d|%d|%d) cannot be fixed in page buffer.\n",
                    pgid.pfxid, pgid.flnum, pgid.pgnum);
            do_pfx_data_wfrpg_release(pfdesc, wmid, 0); /* delete */
            return -1;
        }
        assert(PAGEID_IS_EQ(&pgid, &pgptr->header.self_pgid));
        if (page_get_cntig_free_size(pgptr) < ntotal) {
            fprintf(stderr, "The warm page(%d|%d|%d) is lack of space: space(%d) < request(%d)\n",
                   pgid.pfxid, pgid.flnum, pgid.pgnum, page_get_cntig_free_size(pgptr), (int)ntotal);
            do_pfx_data_wfrpg_release(pfdesc, wmid, 0); /* delete */
            buf_unfix(pgptr); continue;
        }
        break; /* found */
    }

    itdesc->ptr = dpage_alloc_slot(pgptr, size, &itdesc->iid);
    assert(itdesc->ptr != NULL);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* data slot allocation : redo-undo type */
        ITAllocLog italo_log;
        lrec_it_alloc_init((LogRec*)&italo_log);
        italo_log.body.itmid = itdesc->iid;
        italo_log.body.size = size;
        italo_log.body.method = 1; /* alloc from warm page */
        italo_log.body.pgspace = PAGE_SPACE_FULL;
        italo_log.header.length = sizeof(ITAllocData);

        log_record_write((LogRec*)&italo_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, italo_log.header.self_lsn);
    }
#endif

    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    do_pfx_data_wfrpg_release(pfdesc, wmid, page_get_cntig_free_size(pgptr));

    buf_unlatch_only(pgptr);
    return 1;
}
#endif

/* allocate data slot from partial free page */
static int do_pfx_data_slot_from_pfrpg(prefix_desc *pfdesc, uint32_t size, ItemDesc *itdesc)
{
    Page       *pgptr;
    PageID      pgid;
    uint32_t    ntotal = size+sizeof(PGSLOT);
    uint32_t    frsz;
    int         spctype = PAGE_SPACE_FULL;
    int         trycount = 3;

    while (1)
    {
        if (--trycount < 0) {
            fprintf(stderr, "[INFO] Fail to get partial free page (trycount=3)\n");
            return 0; /* not found */
        }
        if (do_pfx_data_pfrpg_get(pfdesc, &pgid) == 0) {
            return 0; /* no partial free page */
        }
        pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
        if (pgptr == NULL) {
            /* How to handle ?? restore as partial free page */
            do_pfx_data_pfrpg_set(pfdesc, pgid);
            return -1;
        }
        /**** Page Validation Check For Data Slot Allocation ****
         * The page might be freed and then reallocated by other thread.
         * So, following two conditions must be checked
         * 1. The pageid of the page must be the same with the old one.
         *    => The page was not freed before fixing a page in buffer.
         *    => Even if the page was freed,
         *       the page has still the same prefix and the dame page type.
         * 2. The page must have enough free space to allocate a free slot of wanted size
         ********************************************************/
        if (PAGEID_IS_NE(&pgid, &pgptr->header.self_pgid)) {
            if (!PAGEID_IS_NULL(&pgptr->header.self_pgid)) {
                fprintf(stderr, "The partial page(%d|%d|%d) is different from the gotten page(%d|%d|%d)\n",
                        pgptr->header.self_pgid.pfxid, pgptr->header.self_pgid.flnum,
                        pgptr->header.self_pgid.pgnum, pgid.pfxid, pgid.flnum, pgid.pgnum);
            }
            buf_unfix(pgptr); continue;
        }
        if (page_get_cntig_free_size(pgptr) < ntotal) {
            fprintf(stderr, "The partial page(%d|%d|%d) is lack of space: space(%d) < request(%d)\n",
                   pgid.pfxid, pgid.flnum, pgid.pgnum, (int)page_get_cntig_free_size(pgptr), (int)ntotal);
            buf_unfix(pgptr); continue;
        }
        break; /* found */
    }

    itdesc->ptr = dpage_alloc_slot(pgptr, size, &itdesc->iid);
    assert(itdesc->ptr != NULL);

    /* get the amount of contiguous free space */
    frsz = page_get_cntig_free_size(pgptr);
    do {
#ifdef USE_WARM_FREE_PAGE
        /* register the free page as warm free page */
        if (frsz >= WARM_PAGE_MIN_SLOT_SIZE) {
            if (do_pfx_data_wfrpg_insert(pfdesc, pgid, frsz) > 0) {
                break; /* inserted as warm page */
            }
        }
#endif
        /* set partial free page if needed */
        if (frsz >= (pfx_gl.page_body_size/2)) {
            do_pfx_data_pfrpg_set(pfdesc, pgid);
            spctype = PAGE_SPACE_HALF;
        }
    } while(0);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* data slot allocation : redo-undo type */
        ITAllocLog italo_log;
        lrec_it_alloc_init((LogRec*)&italo_log);
        italo_log.body.itmid = itdesc->iid;
        italo_log.body.size = size;
        italo_log.body.method = 2; /* alloc from partial free page */
        italo_log.body.pgspace = spctype;
        italo_log.header.length = sizeof(ITAllocData);

        log_record_write((LogRec*)&italo_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, italo_log.header.self_lsn);
    }
#endif

    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    buf_unlatch_only(pgptr);
    return 1; /* # of allocated slot */
}

/* allocate data slot from total free page */
static int do_pfx_data_slot_from_tfrpg(prefix_desc *pfdesc, uint32_t size, ItemDesc *itdesc)
{
    Page    *pgptr;
    PageID   pgid;
    uint32_t frsz;
    int      spctype = PAGE_SPACE_FULL;

    int ret = do_pfx_data_page_alloc(pfdesc, &pgid);
    if (ret <= 0) { /* SEVERE error or NO Disk Space */
        return ret;
    }

    /* fix buffer with (newpg == true) */
    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
    assert(pgptr != NULL);

    dpage_init(pgptr, pgid);
    itdesc->ptr = dpage_alloc_slot(pgptr, size, &itdesc->iid);
    assert(itdesc->ptr != NULL);

    /* get the amount of contiguous free space */
    frsz = page_get_cntig_free_size(pgptr);
    do {
#ifdef USE_WARM_FREE_PAGE
        /* register the free page as warm free page */
        if (frsz >= WARM_PAGE_MIN_SLOT_SIZE) {
            if (do_pfx_data_wfrpg_insert(pfdesc, pgid, frsz) > 0) {
                break; /* inserted as warm page */
            }
        }
#endif
        /* set partial free page if needed */
        if (frsz >= (pfx_gl.page_body_size/2)) {
            do_pfx_data_pfrpg_set(pfdesc, pgid);
            spctype = PAGE_SPACE_HALF;
        }
    } while(0);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* data slot allocation : redo-undo type */
        ITAllocLog italo_log;
        lrec_it_alloc_init((LogRec*)&italo_log);
        italo_log.body.itmid = itdesc->iid;
        italo_log.body.size = size;
        italo_log.body.method = 3; /* alloc from total free page */
        italo_log.body.pgspace = spctype;
        italo_log.header.length = sizeof(ITAllocData);

        log_record_write((LogRec*)&italo_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, italo_log.header.self_lsn);
    }
#endif

    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    buf_unlatch_only(pgptr);
    return ret;
}

ENGINE_ERROR_CODE pfx_data_slot_alloc(int pfxid, uint32_t size, ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    int allocs;

    do {
#ifdef USE_WARM_FREE_PAGE
        /* step 1: allocate data slot from warm free page */
        allocs = do_pfx_data_slot_from_wfrpg(pfdesc, size, itdesc);
        if (allocs < 0) { /* SEVERE error */
            ret = ENGINE_FAILED; break;
        }
        if (allocs > 0) { /* allocated the data slot */
            break;
        }
#endif
        /* step 2: allocate data slot from partial free page */
        if ((size+sizeof(PGSLOT)) <= (pfx_gl.page_body_size/2)) {
            allocs = do_pfx_data_slot_from_pfrpg(pfdesc, size, itdesc);
            if (allocs < 0) { /* SEVERE error */
                ret = ENGINE_FAILED; break;
            }
            if (allocs > 0) { /* allocated the data slot */
                break;
            }
        }
        /* step 3: allocate data slot from total free page */
        allocs = do_pfx_data_slot_from_tfrpg(pfdesc, size, itdesc);
        if (allocs < 0) { /* SEVERE error */
            ret = ENGINE_FAILED; break;
        }
        if (allocs > 0) { /* allocated the data slot */
            break;
        }
        /* allocs == 0 : No more disk space */
        ret = ENGINE_ENODISK;
    } while(0);

    return ret;
}

void pfx_data_slot_free(int pfxid, ItemDesc *itdesc)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    Page        *pgptr;
    PageID       pgid;
    uint16_t     slotid;
    bool         flag_page_empty;
    bool         flag_page_free = false;
    bool         flag_page_compact = false;
    bool         flag_page_partial = false;

    pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(itdesc->ptr, config->sq.data_page_size);
    buf_relatch_only(pgptr, BUF_MODE_WRITE);

    pgid = pgptr->header.self_pgid;
    slotid = SLOTID_FROM_ITEMID(&itdesc->iid);

    int old_frsz, new_frsz;
    old_frsz = page_get_cntig_free_size(pgptr);
    (void)dpage_free_slot(pgptr, slotid);
    new_frsz = page_get_cntig_free_size(pgptr);

    if (dpage_is_empty(pgptr)) {
        flag_page_empty = true;
        flag_page_free = true;

        dpage_clear_space(pgptr);

        /**** [Empty Page Case] Some Note On Concurrency Control Issue ****
         * There might be other thread that try to access the page to allocate slots.
         * The thread found the page through warm page pool or partial page map.
         * So, we must delete the page from warm page pool and partal page map
         * before free the page actually.
         ******************************************************************/
        do {
            /* delete the page from warm page pool */
#ifdef USE_WARM_FREE_PAGE
            if (do_pfx_data_wfrpg_delete(pfdesc, pgid) == -1) {
                /* The page is an warm page that we cannot delete.
                 * Because, other thread try to access this page to allocate slots.
                 */
                flag_page_free = false; break;
            }
#endif
            /* delete the page from partial page map */
            if (do_pfx_data_pfrpg_clear(pfdesc, pgid) == false) {
                /* There is a hint that other thread try to access this page.
                 * so, we cannot free this page.
                 */
                flag_page_free = false; break;
            }
        } while(0);

        if (flag_page_free == true) {
            /* Now, free the page */
            /* To notify other thread that this page was successfully freed,
             * clear the pageid of the page.
             */
            PAGEID_SET_NULL(&pgptr->header.self_pgid); /* mark free page */
            do_pfx_data_page_free(pfdesc, pgid);
        }
    } else {
        flag_page_empty = false;
        if (page_get_total_free_size(pgptr) >= (pfx_gl.page_body_size/2) &&
            new_frsz < (pfx_gl.page_body_size/4) && buf_is_the_only_writer(pgptr)) {
            if (dpage_compact_space(pgptr) != 0) {
                flag_page_compact = true;
                new_frsz = page_get_cntig_free_size(pgptr);
            }
        }
        if (old_frsz < (pfx_gl.page_body_size/2) && new_frsz >= (pfx_gl.page_body_size/2)) {
            do {
#ifdef USE_WARM_FREE_PAGE
                if (do_pfx_data_wfrpg_exist(pfdesc, pgid)) {
                    break; /* do nothing */
                }
#endif
                do_pfx_data_pfrpg_set(pfdesc, pgid);
                flag_page_partial = true;
            } while(0);
        }
    }

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* data slot allocation : redo-undo type */
        ITFreeLog itfre_log;
        lrec_it_free_init((LogRec*)&itfre_log);
        itfre_log.body.itmid = itdesc->iid;
        itfre_log.body.empty = flag_page_empty;
        itfre_log.body.free = flag_page_free;
        itfre_log.body.compact = flag_page_compact;
        itfre_log.body.partial = flag_page_partial;
        itfre_log.header.length = sizeof(ITFreeData);

        log_record_write((LogRec*)&itfre_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, itfre_log.header.self_lsn);
    }
#endif

    /* set buffer page dirty */
    buf_set_dirty(pgptr);
    buf_unfix(pgptr);
}

/***********************************/
/* overflow page & file management */
/***********************************/

static int do_pfx_ovfl_space_grow(prefix_space *pfspace)
{
    file_mgmt *flmgt = &pfspace->ovfl_flmgt;

    /* check prefix disk quota */
    if (do_pfx_disk_quota_check(pfspace) == false) {
        return 0; /* no more disk space allowed */
    }

    /* Assume that (pfx_gl.npage_per_file % pfx_gl.npage_per_grow) == 0 */
    /* create a new file if needed */
    if ((pfspace->d.ovfl_total_pgcnt % pfx_gl.npage_per_file) == 0 &&
        (pfspace->d.ovfl_total_pgcnt / pfx_gl.npage_per_file) == pfspace->d.ovfl_total_flcnt)
    {
        if (do_pfx_file_create(flmgt, pfspace->d.ovfl_total_flcnt+1) < 0) {
            return -1; /* file create fail */
        }
        pfspace->d.ovfl_total_flcnt += 1;
    }

    /* initialize page map for the new free pages and adjust free file list */
    if (do_pfx_file_space_grow(flmgt, pfspace->d.ovfl_total_flcnt,
                               pfspace->d.ovfl_total_pgcnt % pfx_gl.npage_per_file,
                               pfx_gl.npage_per_grow) < 0) {
        return -1; /* buffer permanent pin fail */
    }
    pfspace->d.ovfl_total_pgcnt += pfx_gl.npage_per_grow;

    return pfx_gl.npage_per_grow;
}

static void do_pfx_ovfl_page_free(prefix_desc *pfdesc, int count, PageID *pgids)
{
    file_mgmt *flmgt = &pfdesc->space->ovfl_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    int        flnum;
    int        pmidx;
    int        pblen = 256;
    int        pbcnt;      /* # of page bits to free */
    int        pbidx[pblen]; /* page bit indexes */
    int        next;

    pbcnt = 0;
    flnum = pgids[0].flnum;
    pmidx = pgids[0].pgnum / flmgt->npage_ppmap;
    pbidx[pbcnt++] = pgids[0].pgnum % flmgt->npage_ppmap;
    assert(pbidx[pbcnt-1] > 0);

    for (next = 1; next < count; next++) {
        if (pbcnt < pblen && flnum == pgids[next].flnum &&
            pmidx == (pgids[next].pgnum / flmgt->npage_ppmap))
        {
            pbidx[pbcnt++] = pgids[next].pgnum % flmgt->npage_ppmap;
            assert(pbidx[pbcnt-1] > 0);
            continue;
        }
        fldsc = flmgt->fldsc_tab[flnum-1];
        pmapg = fldsc->pmaps[pmidx];

        buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
        mpage_free_page_bulk(pmapg, pbcnt, &pbidx[0]);
        buf_permanent_unlatch((Page*)pmapg);

        pthread_mutex_lock(&pfdesc->space->ovfl_lock);
        /* increment # of total free pages */
        fldsc->pmtfs[pmidx] += pbcnt;
        fldsc->tfree.pgcnt += pbcnt;
        if (fldsc->tfree.pgcnt == pbcnt) { /* link into total free file list */
            do_pfx_fldsc_link(flmgt, fldsc, flnum, FREE_TYPE_TOTAL);
        }
        flmgt->tfree.pgcnt += pbcnt;
        pthread_mutex_unlock(&pfdesc->space->ovfl_lock);

        pbcnt = 0;
        flnum = pgids[next].flnum;
        pmidx = pgids[next].pgnum / flmgt->npage_ppmap;
        pbidx[pbcnt++] = pgids[next].pgnum % flmgt->npage_ppmap;
        assert(pbidx[pbcnt-1] > 0);
    }
    if (pbcnt > 0) {
        fldsc = flmgt->fldsc_tab[flnum-1];
        pmapg = fldsc->pmaps[pmidx];

        buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
        mpage_free_page_bulk(pmapg, pbcnt, &pbidx[0]);
        buf_permanent_unlatch((Page*)pmapg);

        pthread_mutex_lock(&pfdesc->space->ovfl_lock);
        /* increment # of total free pages */
        fldsc->pmtfs[pmidx] += pbcnt;
        fldsc->tfree.pgcnt += pbcnt;
        if (fldsc->tfree.pgcnt == pbcnt) { /* link into total free file list */
            do_pfx_fldsc_link(flmgt, fldsc, flnum, FREE_TYPE_TOTAL);
        }
        flmgt->tfree.pgcnt += pbcnt;
        pthread_mutex_unlock(&pfdesc->space->ovfl_lock);
    }
#if 0 /* simple implementation */
    for (i = 0; i < count; i++) {
        pmidx = pgids[i].pgnum / flmgt->npage_ppmap;
        pbidx = pgids[i].pgnum % flmgt->npage_ppmap;

        /* find file desc and pmap page */
        fldsc = flmgt->fldsc_tab[pgids[i].flnum-1];
        pmapg = fldsc->pmaps[pmidx];

        buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
        mpage_free_page_bulk(pmapg, 1, &pbidx);
        buf_permanent_unlatch((Page*)pmapg);

        pthread_mutex_lock(&pfdesc->space->ovfl_lock);
        /* increment # of total free pages */
        fldsc->pmtfs[pmidx] += 1;
        fldsc->tfree.pgcnt += 1;
        if (fldsc->tfree.pgcnt == 1) { /* link into total free file list */
            do_pfx_fldsc_link(flmgt, fldsc, pgids[i].flnum, FREE_TYPE_TOTAL);
        }
        flmgt->tfree.pgcnt += 1;
        pthread_mutex_unlock(&pfdesc->space->ovfl_lock);
    }
#endif
}

static int do_pfx_ovfl_page_alloc(prefix_desc *pfdesc, int count, PageID *pgids)
{
    file_mgmt *flmgt = &pfdesc->space->ovfl_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    uint16_t   flnum;
    int        pmidx, i;
    int        pblen = 256;
    int        pbcnt;
    int        pbidx[pblen];
    int        allocable;
    int        allocated = 0;

#if 0 // Consider this case later
    if (pfdesc->d.private_space == 0) {
        /* shared prefix space */
    }
#endif

    /* Assume that private prefix space is used */
    while (allocated < count)
    {
        pthread_mutex_lock(&pfdesc->space->ovfl_lock);

        /* grow the space if it's lack of free pages */
        while (flmgt->tfree.pgcnt < (count-allocated)) {
            int ret = do_pfx_ovfl_space_grow(pfdesc->space);
            if (ret <= 0) {
                pthread_mutex_unlock(&pfdesc->space->ovfl_lock);
                if (allocated > 0) {
                    do_pfx_ovfl_page_free(pfdesc, allocated, pgids);
                }
                return ret;
            }
        }
        assert(flmgt->tfree.pgcnt >= (count-allocated));

        /* find the file containing enough free pages */
        flnum = flmgt->tfree.fhead;
        while (flnum != 0) {
            fldsc = flmgt->fldsc_tab[flnum-1];
            if (fldsc->tfree.pgcnt >= (count-allocated)) break;
            flnum = fldsc->tfree.fnext;
        }
        if (flnum == 0) { /* allocate from the first file having free pages */
            flnum = flmgt->tfree.fhead;
            fldsc = flmgt->fldsc_tab[flnum-1];
        }

        pmidx = 0;
        while (pmidx < flmgt->npmap_pfile) {
            if (fldsc->pmtfs[pmidx] == 0) {
                pmidx += 1; continue;
            }

            /* get allocable page count */
            allocable = (fldsc->pmtfs[pmidx] < (count-allocated) ?
                         fldsc->pmtfs[pmidx] : (count-allocated));
            if (allocable > pblen) allocable = pblen;

            /* decrement # of total free pages */
            fldsc->pmtfs[pmidx] -= allocable;
            fldsc->tfree.pgcnt -= allocable;
            if (fldsc->tfree.pgcnt == 0) { /* unlink from total free file list */
                do_pfx_fldsc_unlink(flmgt, fldsc, flnum, FREE_TYPE_TOTAL);
            }
            flmgt->tfree.pgcnt -= allocable;
            pthread_mutex_unlock(&pfdesc->space->ovfl_lock);

            pmapg = fldsc->pmaps[pmidx];
            buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
            pbcnt = mpage_alloc_page_bulk(pmapg, allocable, &pbidx[0]);
            buf_permanent_unlatch((Page*)pmapg);
            assert(pbcnt == allocable);

            for (i = 0; i < pbcnt; i++) {
                assert(pbidx[i] > 0);
                pgids[allocated].pfxid = (SQUALL_FILEBITS_OVFL | pfdesc->pfxid);
                pgids[allocated].flnum = flnum;
                pgids[allocated].pgnum = (pmidx * flmgt->npage_ppmap) + pbidx[i];
                allocated += 1;
            }
            if (allocated >= count) break;

            pthread_mutex_lock(&pfdesc->space->ovfl_lock);
        }
        if (allocated >= count) break;

        pthread_mutex_unlock(&pfdesc->space->ovfl_lock);
    }

    return allocated;
}

ENGINE_ERROR_CODE pfx_ovfl_slot_alloc(int pfxid, uint32_t count, ItemID *osids, void **osptr)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    Page        *pgptr;
    PageID       pgid;
    int          allocs, i;

    allocs = do_pfx_ovfl_page_alloc(pfdesc, count, (PageID*)osids);
    if (allocs < 0) { /* SEVERE error */
        return ENGINE_FAILED;
    }
    if (allocs == 0) { /* no more disk space */
        return ENGINE_ENODISK;
    }
    for (i = 0; i < count; i++) {
        /* fix the new page with newpg == true */
        pgid = *((PageID*)osids + i);
        pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
        assert(pgptr != NULL);

        /* ovfl page init - 3rd argument means the size of fixed length slot */
        opage_init(pgptr, pgid, pfx_gl.page_body_size);
        osptr[i] = opage_alloc_slot(pgptr, pfx_gl.page_body_size, &osids[i]);
        assert(osptr[i] != NULL);

        /* set buffer page dirty */
        //buf_set_dirty(pgptr); /* Is this necessary ? */
        buf_unlatch_only(pgptr);
    }
    return ENGINE_SUCCESS;
}

void pfx_ovfl_slot_free(int pfxid, uint32_t count, ItemID *osids, void **osptr)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    Page        *pgptr;
    PageID       pageid;
    uint32_t     slotid, i;

    if (osptr == NULL) {
        /* The ovfl pages wern't pinned in the page buffer */
        for (i = 0; i < count; i++) {
            GET_PAGEID_SLOTID_FROM_ITEMID(&osids[i], &pageid, &slotid);
#if 0 // MAYBE_NOT_NEEDED
            pgptr = buf_fix(pageid, BUF_MODE_WRITE, false, false);
            assert(pgptr != NULL);

            (void)opage_free_slot(pgptr, slotid);
            PAGEID_SET_NULL(&pgptr->header.self_pgid); /* mark free page */

            /* set buffer page dirty */
            buf_set_dirty(pgptr);
            buf_unfix(pgptr);
#endif

            /* No one refer to the ovfl page. So, we can free it immediately */
            do_pfx_ovfl_page_free(pfdesc, 1, &pageid);
        }
    } else {
        /* The ovfl pages were pinned in the page buffer */
        for (i = 0; i < count; i++) {
            pgptr = (Page*)PAGEPTR_FROM_ITEMPTR(osptr[i], config->sq.data_page_size);
            pageid = pgptr->header.self_pgid;
#if 0 // MAYBE_NOT_NEEDED
            slotid = 0;
            /* Do not need to relatch the page buffer
             * Because, the ovfl page is beging only accessed by current thread.
             */
            (void)opage_free_slot(pgptr, slotid);
            PAGEID_SET_NULL(&pgptr->header.self_pgid); /* mark free page */

            /* set buffer page dirty */
            buf_set_dirty(pgptr);
#endif
            buf_unpin_only(pgptr);

            /* No one refer to the ovfl page. So, we can free it immediately */
            do_pfx_ovfl_page_free(pfdesc, 1, &pageid);
        }
    }
    /* If pageid array memory were used with memory allocation,
     * the pages can be freed collectively with one function call like the following.
     *   do_pfx_ovfl_page_free(pfdesc, count, pgids);
     */
}

/* btree index support */

static int do_pfx_indx_space_grow(prefix_space *pfspace)
{
    file_mgmt *flmgt = &pfspace->indx_flmgt;

    /* check prefix disk quota */
    if (do_pfx_disk_quota_check(pfspace) == false) {
        return 0; /* no more disk space allowed */
    }

    /* Assume that (pfx_gl.npage_per_file % pfx_gl.npage_per_grow) == 0 */
    /* create a new file if needed */
    if ((pfspace->d.indx_total_pgcnt % pfx_gl.npage_per_file) == 0 &&
        (pfspace->d.indx_total_pgcnt / pfx_gl.npage_per_file) == pfspace->d.indx_total_flcnt)
    {
        if (do_pfx_file_create(flmgt, pfspace->d.indx_total_flcnt+1) < 0) {
            return -1; /* file create fail */
        }
        pfspace->d.indx_total_flcnt += 1;
    }

    /* initialize page map for the new free pages and adjust free file list */
    if (do_pfx_file_space_grow(flmgt, pfspace->d.indx_total_flcnt,
                               pfspace->d.indx_total_pgcnt % pfx_gl.npage_per_file,
                               pfx_gl.npage_per_grow) < 0) {
        return -1; /* buffer permanent pin fail */
    }
    pfspace->d.indx_total_pgcnt += pfx_gl.npage_per_grow;

    return pfx_gl.npage_per_grow;
}

static int do_pfx_bndx_page_alloc(prefix_desc *pfdesc, PageID *pgid)
{
    file_mgmt *flmgt = &pfdesc->space->indx_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    uint16_t   flnum;
    int        pmidx;
    int        pbidx;

#if 0 // Consider this case later
    if (pfdesc->d.private_space == 0) {
        /* shared prefix space */
    }
#endif

    /* Assume that private prefix space is used */

    pthread_mutex_lock(&pfdesc->space->indx_lock);

    /* grow the space if it's lack of free pages */
    if (flmgt->tfree.pgcnt < 1) {
        int ret = do_pfx_indx_space_grow(pfdesc->space);
        if (ret <= 0) {
            pthread_mutex_unlock(&pfdesc->space->indx_lock);
            return ret;
        }
    }
    assert(flmgt->tfree.pgcnt > 0);

    /* get indx file having free page */
    flnum = flmgt->tfree.fhead;
    assert(flnum > 0);
    fldsc = flmgt->fldsc_tab[flnum-1];

    /* find pgmap of the file having free page */
    for (pmidx = 0; pmidx < flmgt->npmap_pfile; pmidx++) {
        if (fldsc->pmtfs[pmidx] > 0) break;
    }
    assert(pmidx < flmgt->npmap_pfile);

    /* decrement # of total free pages */
    fldsc->pmtfs[pmidx] -= 1;
    fldsc->tfree.pgcnt -= 1;
    if (fldsc->tfree.pgcnt == 0) { /* unlink from total free file list */
        do_pfx_fldsc_unlink(flmgt, fldsc, flnum, FREE_TYPE_TOTAL);
    }
    flmgt->tfree.pgcnt -= 1;
    pthread_mutex_unlock(&pfdesc->space->indx_lock);

    pmapg = fldsc->pmaps[pmidx];
    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    pbidx = mpage_alloc_page(pmapg);
    buf_permanent_unlatch((Page*)pmapg);
    assert(pbidx > 0);

    /* get pageid of the found free page */
    pgid->pfxid = SQUALL_FILEBITS_INDX | pfdesc->pfxid;
    pgid->flnum = flnum;
    pgid->pgnum = (pmidx * flmgt->npage_ppmap) + pbidx;
    return 1; /* # of allocated pages */
}

static void do_pfx_bndx_page_free(prefix_desc *pfdesc, PageID pgid)
{
    file_mgmt *flmgt = &pfdesc->space->indx_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    int        pmidx;
    int        pbidx;

    pbidx = pgid.pgnum % flmgt->npage_ppmap;
    pmidx = pgid.pgnum / flmgt->npage_ppmap;
    assert(pbidx > 0);

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    pmapg = fldsc->pmaps[pmidx];

    buf_permanent_latch((Page*)pmapg, BUF_MODE_WRITE);
    mpage_free_page(pmapg, pbidx);
    buf_permanent_unlatch((Page*)pmapg);

    pthread_mutex_lock(&pfdesc->space->indx_lock);
    /* increment # of total free pages */
    fldsc->pmtfs[pmidx] += 1;
    fldsc->tfree.pgcnt += 1;
    if (fldsc->tfree.pgcnt == 1) { /* unlink into total free file list */
        do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_TOTAL);
    }
    flmgt->tfree.pgcnt += 1;
    pthread_mutex_unlock(&pfdesc->space->indx_lock);
}

ENGINE_ERROR_CODE pfx_bndx_page_alloc(int pfxid, PageID *pgid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];

    int allocs = do_pfx_bndx_page_alloc(pfdesc, pgid);
    if (allocs < 0) { /* SEVERE error */
        return ENGINE_FAILED;
    }
    if (allocs == 0) {
        return ENGINE_ENODISK;
    }
    return ENGINE_SUCCESS;
}

void pfx_bndx_page_free(int pfxid, PageID pgid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    do_pfx_bndx_page_free(pfdesc, pgid);
}

/* hash index support */

ENGINE_ERROR_CODE pfx_hndx_page_set_used(prefix_desc *pfdesc, PageID pgid)
{
    prefix_space *pfspace = pfdesc->space;
    file_mgmt *flmgt = &pfspace->indx_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    int        pmidx, pbidx;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    pmidx = pgid.pgnum / flmgt->npage_ppmap;
    pbidx = pgid.pgnum % flmgt->npage_ppmap;

    pthread_mutex_lock(&pfspace->indx_lock);
    do {
        if (pgid.flnum > pfspace->d.indx_total_flcnt) {
            if (do_pfx_file_create(flmgt, pgid.flnum) < 0) {
                ret = ENGINE_FAILED; /* index file create fail */
                break;
            }
            pfspace->d.indx_total_flcnt = pgid.flnum;
        } else {
            if (flmgt->fldsc_tab[pgid.flnum-1] == NULL) {
                if (do_pfx_file_create(flmgt, pgid.flnum) < 0) {
                    ret = ENGINE_FAILED; /* index file create fail */
                    break;
                }
            }
        }
        /*******
        if (pgid.flnum > pfspace->d.indx_total_flcnt) {
            for (int i = pfspace->d.indx_total_flcnt+1; i <= pgid.flnum; i++) {
                if (do_pfx_file_create(flmgt, i) < 0) {
                    ret = -1; break;
                }
                pfspace->d.indx_total_flcnt += 1;
            }
        }
        *******/

        /* set the bit of the new page */
        fldsc = pfspace->indx_flmgt.fldsc_tab[pgid.flnum-1];
        pmapg = fldsc->pmaps[pmidx];
        pmapg->body[pbidx/8] |= pfx_gl.bit1_used_mask[pbidx%8];

        /* set dirty flag */
        buf_set_dirty((Page*)pmapg); /* set pmap dirty */

        /* increment indx page count */
        fldsc->npage += 1;
        pfspace->d.indx_total_pgcnt += 1;
    } while(0);
    pthread_mutex_unlock(&pfspace->indx_lock);

    return ret;
}

void pfx_hndx_page_set_free(prefix_desc *pfdesc, PageID pgid)
{
    prefix_space *pfspace = pfdesc->space;
    file_mgmt *flmgt = &pfspace->indx_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    int        pmidx, pbidx;

    pmidx = pgid.pgnum / flmgt->npage_ppmap;
    pbidx = pgid.pgnum % flmgt->npage_ppmap;

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    pmapg = fldsc->pmaps[pmidx];

    pthread_mutex_lock(&pfspace->indx_lock);

    /* clear the bit of the new page */
    pmapg->body[pbidx/8] &= pfx_gl.bit1_free_mask[pbidx%8];

    /* set dirty flag */
    buf_set_dirty((Page*)pmapg); /* set pmap dirty */

    /* decrement indx page count */
    fldsc->npage -= 1;
    pfspace->d.indx_total_pgcnt -= 1;

    pthread_mutex_unlock(&pfspace->indx_lock);
}

bool pfx_hndx_page_is_used(prefix_desc *pfdesc, PageID pgid)
{
    prefix_space *pfspace = pfdesc->space;
    file_mgmt *flmgt = &pfspace->indx_flmgt;
    file_desc *fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    pmap_page *pmapg;
    int        pmidx, pbidx;

    if (fldsc == NULL) return false;

    pmidx = pgid.pgnum / flmgt->npage_ppmap;
    pbidx = pgid.pgnum % flmgt->npage_ppmap;
    pmapg = fldsc->pmaps[pmidx];

    if ((pmapg->body[pbidx/8] & pfx_gl.bit1_used_mask[pbidx%8]) == pfx_gl.bit1_used_mask[pbidx%8]) {
        return true;
    } else {
        return false;
    }
}

/* Others */

static PageID do_pfx_data_page_get_first(prefix_desc *pfdesc)
{
    file_mgmt *flmgt = &pfdesc->space->data_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    PageID     pgid;
    int        flnum;
    int        pmidx;
    int        pbidx = 0;
    bool       found = false;

    if (pfdesc->d.private_space == 0) {
        PAGEID_SET_NULL(&pgid); /* Not supported until now */
        return pgid;
    }

    for (flnum = 1; flnum <= flmgt->fldsc_cnt; flnum++) {
        fldsc = flmgt->fldsc_tab[flnum-1];
        for (pmidx = 0; pmidx < flmgt->npmap_pfile; pmidx++) {
            pmapg = fldsc->pmaps[pmidx];
            buf_permanent_latch((Page*)pmapg, BUF_MODE_READ);
            pbidx = mpage_get_first_used_page(pmapg);
            buf_permanent_unlatch((Page*)pmapg);
            if (pbidx > 0) { /* found */
                found = true; break;
            }
        }
        if (pmidx < flmgt->npmap_pfile) break;
    }

    if (found == true) {
        pgid.pfxid = SQUALL_FILEBITS_DATA | pfdesc->pfxid;
        pgid.flnum = flnum;
        pgid.pgnum = (pmidx * flmgt->npage_ppmap) + pbidx;
    } else {
        PAGEID_SET_NULL(&pgid);
    }
    return pgid;
}

static PageID do_pfx_data_page_get_next(prefix_desc *pfdesc, PageID prev_pgid)
{
    file_mgmt *flmgt = &pfdesc->space->data_flmgt;
    file_desc *fldsc;
    pmap_page *pmapg;
    PageID     pgid;
    int        flnum, pgnum;
    int        pmidx, pbidx;
    int        frst_pmidx;
    int        frst_pbidx;
    bool       found = false;

    assert(pfdesc->d.private_space != 0);
    assert(!PAGEID_IS_NULL(&prev_pgid));

    /* find the next page */
    if (prev_pgid.pgnum < (pfx_gl.npage_per_file-1)) {
        flnum = prev_pgid.flnum;
        pgnum = prev_pgid.pgnum + 1;
    } else {
        flnum = prev_pgid.flnum + 1;
        pgnum = 0;
    }
    frst_pmidx = pgnum / flmgt->npage_ppmap;
    frst_pbidx = pgnum % flmgt->npage_ppmap;
    if (frst_pbidx == 0) frst_pbidx = 1; /* skip pmap page */

    while (flnum <= flmgt->fldsc_cnt)
    {
        fldsc = flmgt->fldsc_tab[flnum-1];

        for (pmidx = frst_pmidx; pmidx < flmgt->npmap_pfile; pmidx++) {
            pmapg = fldsc->pmaps[pmidx];
            if (pmapg == NULL || pmapg->hdr.tot_pgs == 0) {
                assert(flnum == flmgt->fldsc_cnt);
                break;
            }
            pbidx = (pmidx == frst_pmidx ? frst_pbidx : 1);
            buf_permanent_latch((Page*)pmapg, BUF_MODE_READ);
            pbidx = mpage_get_next_used_page(pmapg, pbidx);
            buf_permanent_unlatch((Page*)pmapg);
            if (pbidx > 0) {
                found = true; break;
            }
        }
        if (found == true) break;

        flnum += 1;
        frst_pmidx = 0;
        frst_pbidx = 1;
    }

    if (found == true) {
        pgid.pfxid = SQUALL_FILEBITS_DATA | pfdesc->pfxid;
        pgid.flnum = flnum;
        pgid.pgnum = (pmidx * flmgt->npage_ppmap) + pbidx;
    } else {
        PAGEID_SET_NULL(&pgid);
    }
    return pgid;
}

#if 0 /* will be used later */
static Page *do_pfx_data_page_fix(prefix_desc *pfdesc, PageID pgid, bool readonly)
{
    enum buf_latch_mode latch_mode = (readonly ? BUF_MODE_READ : BUF_MODE_WRITE);
    Page *pgptr;

    pgptr = buf_fix(pgid, latch_mode, false, false);
    if (pgptr == NULL) { /* SEVERE error */
        logger->log(EXTENSION_LOG_WARNING, NULL, "data page(%d|%d|%d) fix fail\n",
                                                 pgid.pfxid, pgid.flnum, pgid.pgnum);
        return NULL;
    }
    /**** Page Validation Check For Data Slot Allocation ****
     * The page might be freed and then reallocated by other thread.
     * So, following conditions must be checked
     * 1. The pageid of the page must be the same with the old one.
     *    => The page was not freed before fixing a page in buffer.
     *    => Even if the page was freed,
     *       the page has still the same prefix and the dame page type.
     ********************************************************/
    if (PAGEID_IS_NE(&pgid, &pgptr->header.self_pgid)) {
        if (!PAGEID_IS_NULL(&pgptr->header.self_pgid)) {
            logger->log(EXTENSION_LOG_INFO, NULL,
                        "The data page(%d|%d|%d) is different from the gotten page(%d|%d|%d)\n",
                        pgptr->header.self_pgid.pfxid, pgptr->header.self_pgid.flnum,
                        pgptr->header.self_pgid.pgnum, pgid.pfxid, pgid.flnum, pgid.pgnum);
        }
        buf_unfix(pgptr); return NULL;
    }
    /* It is guaranteed that
     * the page is pertained to the prefix and is in allocated state.
     * However, the page might be in an empty state.
     */
    return pgptr;
}

static void do_pfx_data_page_unfix(prefix_desc *pfdesc, Page *pgptr)
{
    if (buf_get_latch_mode(pgptr) != (int)BUF_MODE_WRITE) {
        buf_unfix(pgptr);
        return;
    }

    PageID pgid = pgptr->header.self_pgid;

    if (dpage_is_empty(pgptr)) {
        /**** [Empty Page Case] Some Note On Concurrency Control Issue ****
         * Some other threads might try to access this page.
         * Those threads want to allocate data slots from warm free page or partial free page.
         ******************************************************************/
        bool do_real_free = true;

        dpage_clear_space(pgptr);

#ifdef USE_WARM_FREE_PAGE
        if (do_pfx_data_wfrpg_delete(pfdesc, pgid) == -1) {
            /* warm page that cannot delete, because someone try to access the page to allocate space  */
            flag_page_free = false;
        }
#endif
        if (do_real_free) {
            /* The current thread is the only writer or the page is still empty */
            PAGEID_SET_NULL(&pgptr->header.self_pgid); /* mark free page */
        }
        /* set buffer page dirty */
        buf_set_dirty(pgptr);
        buf_unfix(pgptr);

        if (do_real_free) {
            /* free the data page */
            do_pfx_data_page_free(pfdesc, pgid);
        }
    } else {
        if (page_get_total_free_size(pgptr) >= (pfx_gl.page_body_size/2) &&
            page_get_cntig_free_size(pgptr) <= (pfx_gl.page_body_size/2)) {
            if (buf_is_the_only_writer(pgptr)) {
                dpage_compact_space(pgptr); /* data page compaction */
            }
        }
#ifdef USE_WARM_FREE_PAGE
        if (do_pfx_data_wfrpg_exist(pfdesc, pgid)) {
            /* do nothing */
        } else
#endif
        {
           do_pfx_data_pfrpg_set(pfdesc, pgid); /* try mode */
        }
        /* set buffer page dirty */
        buf_set_dirty(pgptr);
        buf_unfix(pgptr);
    }
}
#endif

PageID pfx_data_page_get_first(int pfxid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    return do_pfx_data_page_get_first(pfdesc);
}

PageID pfx_data_page_get_next(int pfxid, PageID prev_pgid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];
    return do_pfx_data_page_get_next(pfdesc, prev_pgid);
}

bool pfx_page_check_before_write(Page *pgptr)
{
    bool result = true;
    int pgtype = page_get_type(pgptr);

    if (pgtype == SQUALL_PAGE_TYPE_DATA) {
        /* check if each data item's refcount is 0(zero). */
        data_item *itptr;
        int i, total_slot_count;

        total_slot_count = page_get_total_slot_count(pgptr);
        for (i = 0; i < total_slot_count; i++) {
            itptr = dpage_find_slot(pgptr, i);
            if (itptr != NULL) { /* used slot */
                if (itptr->refcount != 0) {
                    result = false; break;
                }
            }
        }
    } else {
        /* nothing to check before write */
        assert(pgtype == SQUALL_PAGE_TYPE_OVFL || pgtype == SQUALL_PAGE_TYPE_INDX ||
               pgtype == SQUALL_PAGE_TYPE_PMAP);
    }
    return result;
}

bool pfx_page_check_after_read(Page *pgptr)
{
    PageID pageid = page_get_pageid(pgptr);
    int    pgtype = page_get_type(pgptr);

    assert(!PAGEID_IS_NULL(&pageid));

    if (pgtype == SQUALL_PAGE_TYPE_DATA) {
        /* check if each item's refcount is 0(zero). */
        data_item *itptr;
        int i, total_slot_count;

        /* [NOTE] Page Validation Check After Read */
        /* When data pages are written to disk,
         * some referenced items, whose refcount is larger than 0, might exist on the pages.
         * If the items were referenced by read-purpose threads,
         * the pages will not re-written to disk after the references are releasesd.
         * Therefore, disk image of data pages might have items of positive refcount.
         * So, when read data pages, we must clear the positive refcount to 0.
         */
        total_slot_count = page_get_total_slot_count(pgptr);
        for (i = 0; i < total_slot_count; i++) {
            itptr = dpage_find_slot(pgptr, i);
            if (itptr != NULL) { /* used slot */
                if (itptr->refcount != 0) {
                    itptr->refcount = 0;
                }
            }
        }
    } else {
        /* nothing to check after read */
        assert(pgtype == SQUALL_PAGE_TYPE_OVFL || pgtype == SQUALL_PAGE_TYPE_INDX ||
               pgtype == SQUALL_PAGE_TYPE_PMAP);
    }
    return true;
}

int pfx_get_fd_from_pgid(PageID pgid)
{
    prefix_space *pfspace;
    int prefixid = PREFIXID_FROM_PAGEID(&pgid);
    int filebits = FILEBITS_FROM_PAGEID(&pgid);
    int fd;

    assert(prefixid >= 0 && prefixid < config->sq.max_prefix_count);
    pfspace = pfx_gl.desc_tab[prefixid].space;

    switch (filebits) {
      case SQUALL_FILEBITS_DATA:
        assert(pgid.flnum >= 1 && pgid.flnum <= pfspace->d.data_total_flcnt);
        fd = pfspace->data_flmgt.fldsc_tab[pgid.flnum-1]->fd;
        break;
      case SQUALL_FILEBITS_OVFL:
        assert(pgid.flnum >= 1 && pgid.flnum <= pfspace->d.ovfl_total_flcnt);
        fd = pfspace->ovfl_flmgt.fldsc_tab[pgid.flnum-1]->fd;
        break;
      case SQUALL_FILEBITS_INDX:
        assert(pgid.flnum >= 1 && pgid.flnum <= pfspace->d.indx_total_flcnt);
        fd = pfspace->indx_flmgt.fldsc_tab[pgid.flnum-1]->fd;
        break;
      default:
        fd = -1;
    }
    assert(fd != -1);
    return fd;
}

#ifdef ENABLE_RECOVERY
int pfx_redo_create(const char *prefix, const size_t nprefix, int pfxid, int spcid, bool exp_crt, LogSN lsn)
{
    prefix_hash *pfhash;
    prefix_desc *pfdesc;
    prefix_space *pfspace;
    uint32_t     hval = server->core->hash(prefix, nprefix, 0);

    assert(nprefix > 0 && pfxid > 0 && pfxid == spcid);

    /* prefix create/delete operation is serialized by prefix hash lock */
    pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
    pthread_mutex_lock(&pfhash->lock);

    assert(do_pfx_hash_find(pfhash, prefix, nprefix) == NULL);
    do {
        pfdesc = do_pfx_desc_alloc(pfxid);
        if (pfdesc == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix(%s) desc alloc error\n", prefix);
            break;
        }
        pfspace = do_pfx_space_create(spcid);
        if (pfspace == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix(%s) space create error\n", prefix);
            do_pfx_desc_free(pfdesc); pfdesc = NULL;
            break;
        }

        /* initialize disk info of prefix descriptor */
        do_pfx_desc_dsk_init(pfdesc, pfspace, prefix, nprefix, hval, exp_crt);

        /* initialize prefix descriptor */
        if (do_pfx_desc_init(pfdesc, NULL) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "[FATAL] prefix(%s) desc init error\n", prefix);
            do_pfx_space_remove(pfspace);
            do_pfx_space_free(pfspace);
            do_pfx_desc_free(pfdesc); pfdesc = NULL;
            break;
        }

        /* link it to the hash chain */
        pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
        do_pfx_hash_link(pfhash, pfdesc);
    } while(0);

    if (pfdesc != NULL) {
        /* set create lsn */
        pfdesc->space->d.crtlsn = lsn;
        pfdesc->d.crtlsn = lsn;
    }
    pthread_mutex_unlock(&pfhash->lock);

    return (pfdesc != NULL ? 0 : -1);
}

int pfx_redo_delete(const char *prefix, const size_t nprefix, LogSN lsn)
{
    prefix_hash *pfhash;
    prefix_desc *pvdesc;
    prefix_desc *pfdesc;
    uint32_t     hval = server->core->hash(prefix, nprefix, 0);

    pfhash = &pfx_gl.hash_tab[hval % pfx_gl.hash_size];
    pvdesc = NULL;
    pthread_mutex_lock(&pfhash->lock);
    pfdesc = pfhash->next;
    while (pfdesc != NULL) {
        if (pfdesc->nlen == nprefix && memcmp(pfdesc->name, prefix, nprefix)==0)
            break;
        pvdesc = pfdesc; pfdesc = pfdesc->next;
    }
    if (pfdesc != NULL) {
        /* unlink the prefix descriptor from prefix hash chain */
        do_pfx_hash_unlink(pfhash, pvdesc, pfdesc);
        /* set update lsn */
        pfdesc->space->d.updlsn = lsn;
        pfdesc->d.updlsn = lsn;
        /* Just push the prefix descriptor into the prefix delete queue.
         * Then, prefix gc thread will delete the prefix in background.
         */
        do_pfx_push_del_queue(pfdesc);
    }
    pthread_mutex_unlock(&pfhash->lock);
    return (pfdesc != NULL ? 0 : -1);
}

int pfx_redo_flgrow(int spcid, int ftype, int flnum, int pgnum, int grow_pgcnt, LogSN lsn)
{
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    pmap_page    *pmapg;
    int           pmidx;

    pfspace = do_pfx_space_find(spcid);
    if (pfspace == NULL) {
        return -1; /* redo fail */
    }

    /* No locking is needed */

    if (ftype == SQUALL_FILE_TYPE_DATA) {
        flmgt = &pfspace->data_flmgt;
        if (pfspace->d.data_total_flcnt < flnum) {
            assert(pfspace->d.data_total_flcnt == (flnum-1) && pgnum == 0);
            assert((pfspace->d.data_total_pgcnt % pfx_gl.npage_per_file) == 0);
            assert((pfspace->d.data_total_pgcnt / pfx_gl.npage_per_file) == pfspace->d.data_total_flcnt);
            if (do_pfx_file_create(flmgt, pfspace->d.data_total_flcnt+1) < 0) {
                return -1; /* file create fail */
            }
            pfspace->d.data_total_flcnt += 1;
        }
    }
    else if (ftype == SQUALL_FILE_TYPE_OVFL) {
        flmgt = &pfspace->ovfl_flmgt;
        if (pfspace->d.ovfl_total_flcnt < flnum) {
            assert(pfspace->d.ovfl_total_flcnt == (flnum-1) && pgnum == 0);
            assert((pfspace->d.ovfl_total_pgcnt % pfx_gl.npage_per_file) == 0);
            assert((pfspace->d.ovfl_total_pgcnt / pfx_gl.npage_per_file) == pfspace->d.ovfl_total_flcnt);
            if (do_pfx_file_create(flmgt, pfspace->d.ovfl_total_flcnt+1) < 0) {
                return -1; /* file create fail */
            }
            pfspace->d.ovfl_total_flcnt += 1;
        }
    }
    else if (ftype == SQUALL_FILE_TYPE_INDX) {
        flmgt = &pfspace->indx_flmgt;
        if (pfspace->d.indx_total_flcnt < flnum) {
            assert(pfspace->d.indx_total_flcnt == (flnum-1) && pgnum == 0);
            assert((pfspace->d.indx_total_pgcnt % pfx_gl.npage_per_file) == 0);
            assert((pfspace->d.indx_total_pgcnt / pfx_gl.npage_per_file) == pfspace->d.indx_total_flcnt);
            if (do_pfx_file_create(flmgt, pfspace->d.indx_total_flcnt+1) < 0) {
                return -1; /* file create fail */
            }
            pfspace->d.indx_total_flcnt += 1;
        }
    }
    else {
        return -1; /* redo fail */
    }

    fldsc = flmgt->fldsc_tab[flnum-1];
    assert(fldsc != NULL);

    pmidx = (pgnum / flmgt->npage_ppmap);
    if (fldsc->pmaps[pmidx] == NULL) {
        PageID mpgid;
        mpgid.pfxid = MAKE_PAGEID_PFXID(flmgt->ftype, flmgt->spcid);
        mpgid.flnum = fldsc->flnum;
        mpgid.pgnum = (pmidx * flmgt->npage_ppmap);

        fldsc->pmaps[pmidx] = (pmap_page*)buf_permanent_pin(mpgid, true); /* NEW PAGE */
        if (fldsc->pmaps[pmidx] == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[FATAL] prefix space(%d) %s file(%d) grow fail - permanent pin buffer area is full.\n",
                        flmgt->spcid, get_file_type_str(flmgt->ftype), fldsc->flnum);
            return -2; /* Out of (buffer) memory */
        }
        int partial_nbitcnt = (flmgt->ftype == SQUALL_FILE_TYPE_DATA ? 1 : 0);
        mpage_init(fldsc->pmaps[pmidx], mpgid, partial_nbitcnt);
    }
    pmapg = fldsc->pmaps[pmidx];

    if (LOGSN_IS_LT(&pmapg->hdr.map_lsn, &lsn)) {
        /* redo the action */
        assert(pmapg->hdr.tot_pgs == (pgnum % flmgt->npage_ppmap));
        int free_pgcnt = mpage_grow_space(pmapg, grow_pgcnt);

        buf_set_lsn((Page*)pmapg, lsn);
        buf_set_dirty((Page*)pmapg);

        /* increment # of pages in the file */
        fldsc->npage += grow_pgcnt;

        /* increment # of total free pages */
        fldsc->pmtfs[pmidx] += free_pgcnt;
        fldsc->tfree.pgcnt += free_pgcnt;
        if (fldsc->tfree.pgcnt == free_pgcnt) { /* link into total free file list */
            do_pfx_fldsc_link(flmgt, fldsc, flnum, FREE_TYPE_TOTAL);
        }
        flmgt->tfree.pgcnt += free_pgcnt;
    }

    uint64_t total_page_count = ((flnum-1) * pfx_gl.npage_per_file) + pgnum + grow_pgcnt;
    if (ftype == SQUALL_FILE_TYPE_DATA) {
        if (pfspace->d.data_total_pgcnt < total_page_count)
            pfspace->d.data_total_pgcnt = total_page_count;
    }
    else if (ftype == SQUALL_FILE_TYPE_OVFL) {
        if (pfspace->d.ovfl_total_pgcnt < total_page_count)
            pfspace->d.ovfl_total_pgcnt = total_page_count;
    }
    else { /* ftype == SQUALL_FILE_TYPE_OVFL */
        if (pfspace->d.indx_total_pgcnt < total_page_count)
            pfspace->d.indx_total_pgcnt = total_page_count;
    }
    return 0;
}

int pfx_redo_pgalloc(PageID pgid, int new_pgspace, LogSN lsn)
{
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    pmap_page    *pmapg;
    int           pmidx;
    int           pfxid = (int)PREFIXID_FROM_PAGEID(&pgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&pgid);
    int           spcid = pfxid;

    pfspace = do_pfx_space_find(spcid);
    if (pfspace == NULL) {
        return -1; /* redo fail */
    }

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    /* No locking is needed */

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    assert(fldsc != NULL);

    pmidx = (pgid.pgnum / flmgt->npage_ppmap);
    pmapg = fldsc->pmaps[pmidx];
    assert(pmapg != NULL);

    if (LOGSN_IS_LT(&pmapg->hdr.map_lsn, &lsn)) {
        /* redo the action */
        int pbidx = pgid.pgnum % flmgt->npage_ppmap;
        int old_pgspace;

        assert(new_pgspace == PAGE_SPACE_HALF || new_pgspace == PAGE_SPACE_FULL);

        old_pgspace = mpage_check_and_set_used(pmapg, pbidx, new_pgspace);
        buf_set_lsn((Page*)pmapg, lsn);
        buf_set_dirty((Page*)pmapg);

        if (old_pgspace == PAGE_SPACE_FREE) { /* it was a free page before */
            /* decrement # of total free pages */
            fldsc->pmtfs[pmidx] -= 1;
            fldsc->tfree.pgcnt -= 1;
            if (fldsc->tfree.pgcnt == 0) { /* unlink from total free file list */
                do_pfx_fldsc_unlink(flmgt, fldsc, pgid.flnum, FREE_TYPE_TOTAL);
            }
            flmgt->tfree.pgcnt -= 1;
        }
        if (new_pgspace == PAGE_SPACE_HALF) {
            if (old_pgspace != new_pgspace) {
                /* increment # of partial free pages */
                fldsc->pmpfs[pmidx] += 1;
                fldsc->pfree.pgcnt += 1;
                if (fldsc->pfree.pgcnt == 1) { /* link into partial free file list */
                    do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
                }
                flmgt->pfree.pgcnt += 1;
            }
        } else { /* new_pgspace == PAGE_SPACE_FULL */
            if (old_pgspace == PAGE_SPACE_HALF) {
                /* decrement # of partial free pages */
                fldsc->pmpfs[pmidx] -= 1;
                fldsc->pfree.pgcnt -= 1;
                if (fldsc->pfree.pgcnt == 0) { /* unlink from partial free file list */
                    do_pfx_fldsc_unlink(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
                }
                flmgt->pfree.pgcnt -= 1;
            }
        }
    }
    return 0;
}

int pfx_redo_pgfree(PageID pgid, LogSN lsn)
{
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    pmap_page    *pmapg;
    int           pmidx;
    int           pfxid = (int)PREFIXID_FROM_PAGEID(&pgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&pgid);
    int           spcid = pfxid;

    pfspace = do_pfx_space_find(spcid);
    if (pfspace == NULL) {
        return -1; /* redo fail */
    }

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    /* No locking is needed */

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    assert(fldsc != NULL);

    pmidx = (pgid.pgnum / flmgt->npage_ppmap);
    pmapg = fldsc->pmaps[pmidx];
    assert(pmapg != NULL);

    if (LOGSN_IS_LT(&pmapg->hdr.map_lsn, &lsn)) {
        /* redo the action */
        int pbidx = pgid.pgnum % flmgt->npage_ppmap;
        int old_pgspace;

        old_pgspace = mpage_check_and_set_free(pmapg, pbidx);
        buf_set_lsn((Page*)pmapg, lsn);
        buf_set_dirty((Page*)pmapg);

        if (old_pgspace != PAGE_SPACE_FREE) { /* it was a free page before */
            /* increment # of total free pages */
            fldsc->pmtfs[pmidx] += 1;
            fldsc->tfree.pgcnt += 1;
            if (fldsc->tfree.pgcnt == 1) { /* link into total free file list */
                do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_TOTAL);
            }
            flmgt->tfree.pgcnt += 1;
       }
       if (old_pgspace == PAGE_SPACE_HALF) {
            /* decrement # of partial free pages */
            fldsc->pmpfs[pmidx] -= 1;
            fldsc->pfree.pgcnt -= 1;
            if (fldsc->pfree.pgcnt == 0) { /* unlink from partial free file list */
                do_pfx_fldsc_unlink(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
            }
            flmgt->pfree.pgcnt -= 1;
        }
    }
    return 0;
}

int pfx_redo_pgpartial(PageID pgid, LogSN lsn)
{
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    pmap_page    *pmapg;
    int           pmidx;
    int           pfxid = (int)PREFIXID_FROM_PAGEID(&pgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&pgid);
    int           spcid = pfxid;

    if (ftype != SQUALL_FILE_TYPE_DATA) {
        return -1; /* redo fail */
    }

    pfspace = do_pfx_space_find(spcid);
    if (pfspace == NULL) {
        return -1; /* redo fail */
    }

    /* No locking is needed */

    flmgt = &pfspace->data_flmgt;
    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    assert(fldsc != NULL);

    pmidx = (pgid.pgnum / flmgt->npage_ppmap);
    pmapg = fldsc->pmaps[pmidx];
    assert(pmapg != NULL);

    if (LOGSN_IS_LT(&pmapg->hdr.map_lsn, &lsn)) {
        /* redo the action */
        int pbidx = pgid.pgnum % flmgt->npage_ppmap;
        int old_pgspace;

        old_pgspace = mpage_check_and_set_used(pmapg, pbidx, PAGE_SPACE_HALF);
        buf_set_lsn((Page*)pmapg, lsn);
        buf_set_dirty((Page*)pmapg);

        if (old_pgspace == PAGE_SPACE_FREE) { /* it was a free page before */
            /* decrement # of total free pages */
            fldsc->pmtfs[pmidx] -= 1;
            fldsc->tfree.pgcnt -= 1;
            if (fldsc->tfree.pgcnt == 0) { /* unlink from total free file list */
                do_pfx_fldsc_unlink(flmgt, fldsc, pgid.flnum, FREE_TYPE_TOTAL);
            }
            flmgt->tfree.pgcnt -= 1;
        }
        if (old_pgspace != PAGE_SPACE_HALF) {
            /* increment # of partial free pages */
            fldsc->pmpfs[pmidx] += 1;
            fldsc->pfree.pgcnt += 1;
            if (fldsc->pfree.pgcnt == 1) { /* link into partial free file list */
                do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
            }
            flmgt->pfree.pgcnt += 1;
        }
    }
    return 0;
}

int pfx_redo_pgalloc_v2(PageID *pgids, int pgcnt, bool partial, LogSN lsn)
{
    assert(pgcnt >= 1);
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    pmap_page    *pmapg;
    PageID        pgid = pgids[0];
    int           pmidx, pbidx;
    int           pfxid = (int)PREFIXID_FROM_PAGEID(&pgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&pgid);
    int           spcid = pfxid;

    pfspace = do_pfx_space_find(spcid);
    assert(pfspace != NULL);

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    /* No locking is needed */

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    assert(fldsc != NULL);

    pmidx = (pgid.pgnum / flmgt->npage_ppmap);
    pmapg = fldsc->pmaps[pmidx];
    assert(pmapg != NULL);

    if (LOGSN_IS_LT(&pmapg->hdr.map_lsn, &lsn)) {
        /* redo the action */
        if (partial == true) {
            assert(ftype == SQUALL_FILE_TYPE_DATA && pgcnt == 1);
            pbidx = (pgid.pgnum % flmgt->npage_ppmap);
            do_mpage_clear_partial_page(pmapg, pbidx);

            /* decrement # of partial free pages */
            fldsc->pmpfs[pmidx] -= 1;
            fldsc->pfree.pgcnt -= 1;
            if (fldsc->pfree.pgcnt == 0) { /* unlink from partial free file list */
                do_pfx_fldsc_unlink(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
            }
            flmgt->pfree.pgcnt -= 1;
        } else {
            for (int i = 0; i < pgcnt; i++) {
                assert(pmidx == (pgids[i].pgnum / flmgt->npage_ppmap));
                pbidx = (pgids[i].pgnum % flmgt->npage_ppmap);
                do_mpage_set_used_page(pmapg, pbidx);

                /* decrement # of total free pages */
                fldsc->pmtfs[pmidx] -= 1;
                fldsc->tfree.pgcnt -= 1;
                if (fldsc->tfree.pgcnt == 0) { /* unlink from total free file list */
                    do_pfx_fldsc_unlink(flmgt, fldsc, pgids[i].flnum, FREE_TYPE_TOTAL);
                }
                flmgt->tfree.pgcnt -= 1;
            }
        }
        buf_set_lsn((Page*)pmapg, lsn);
        buf_set_dirty((Page*)pmapg);
    }
    return 0;
}

int pfx_redo_pgfree_v2(PageID *pgids, int pgcnt, bool partial, LogSN lsn)
{
    assert(pgcnt >= 1);
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    pmap_page    *pmapg;
    PageID        pgid = pgids[0];
    int           pmidx, pbidx;
    int           pfxid = (int)PREFIXID_FROM_PAGEID(&pgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&pgid);
    int           spcid = pfxid;

    pfspace = do_pfx_space_find(spcid);
    assert(pfspace != NULL);

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    /* No locking is needed */

    fldsc = flmgt->fldsc_tab[pgid.flnum-1];
    assert(fldsc != NULL);

    pmidx = (pgid.pgnum / flmgt->npage_ppmap);
    pmapg = fldsc->pmaps[pmidx];
    assert(pmapg != NULL);

    if (LOGSN_IS_LT(&pmapg->hdr.map_lsn, &lsn)) {
        /* redo the action */
        if (partial == true) {
            assert(ftype == SQUALL_FILE_TYPE_DATA && pgcnt == 1);
            pbidx = (pgid.pgnum % flmgt->npage_ppmap);
            do_mpage_set_partial_page(pmapg, pbidx);

            /* increment # of partial free pages */
            fldsc->pmpfs[pmidx] += 1;
            fldsc->pfree.pgcnt += 1;
            if (fldsc->pfree.pgcnt == 1) { /* link into partial free file list */
                do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_PARTIAL);
            }
            flmgt->pfree.pgcnt += 1;
        } else {
            for (int i = 0; i < pgcnt; i++) {
                assert(pmidx == (pgids[i].pgnum / flmgt->npage_ppmap));
                pbidx = (pgids[i].pgnum % flmgt->npage_ppmap);
                do_mpage_set_free_page(pmapg, pbidx);

                /* increment # of total free pages */
                fldsc->pmtfs[pmidx] += 1;
                fldsc->tfree.pgcnt += 1;
                if (fldsc->tfree.pgcnt == 1) { /* link into total free file list */
                    do_pfx_fldsc_link(flmgt, fldsc, pgid.flnum, FREE_TYPE_TOTAL);
                }
                flmgt->tfree.pgcnt += 1;
                flmgt->tfree.pgcnt -= 1;
            }
        }
        buf_set_lsn((Page*)pmapg, lsn);
        buf_set_dirty((Page*)pmapg);
    }
    return 0;
}

void pfx_redo_item_stats(int pfxid, int type, uint64_t tot_count, uint64_t tot_size)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc *pfdesc = &pfx_gl.desc_tab[pfxid];

    pthread_mutex_lock(&pfdesc->stat_lock);
    pfdesc->d.stats.num_items[type] = tot_count;
    pfdesc->d.stats.siz_items[type] = tot_size;
    pthread_mutex_unlock(&pfdesc->stat_lock);
}

int mpage_redo_alloc_page(pmap_page *pmapg, int *pbidx_list, int count)
{
    PageID        mpgid = pmapg->hdr.map_pgid;
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    int           spcid = (int)PREFIXID_FROM_PAGEID(&mpgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&mpgid);

    for (int i = 0; i < count; i++) {
        do_mpage_set_used_page(pmapg, pbidx_list[i]);
    }

    pfspace = do_pfx_space_find(spcid);
    assert(pfspace != NULL);

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    fldsc = flmgt->fldsc_tab[mpgid.flnum-1];
    assert(fldsc != NULL);

    /* decrement # of total free pages */
    fldsc->pmtfs[mpgid.pgnum / flmgt->npage_ppmap] -= count;
    fldsc->tfree.pgcnt -= count;
    if (fldsc->tfree.pgcnt == 0) { /* unlink from total free file list */
        do_pfx_fldsc_unlink(flmgt, fldsc, mpgid.flnum, FREE_TYPE_TOTAL);
    }
    flmgt->tfree.pgcnt -= count;
    return 0;
}

int mpage_redo_free_page(pmap_page *pmapg, int *pbidx_list, int count)
{
    PageID        mpgid = pmapg->hdr.map_pgid;
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    int           spcid = (int)PREFIXID_FROM_PAGEID(&mpgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&mpgid);

    for (int i = 0; i < count; i++) {
        do_mpage_set_free_page(pmapg, pbidx_list[i]);
    }

    pfspace = do_pfx_space_find(spcid);
    assert(pfspace != NULL);

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    fldsc = flmgt->fldsc_tab[mpgid.flnum-1];
    assert(fldsc != NULL);

    /* increment # of total free pages */
    fldsc->pmtfs[mpgid.pgnum / flmgt->npage_ppmap] += count;
    fldsc->tfree.pgcnt += count;
    if (fldsc->tfree.pgcnt == count) { /* link into total free file list */
        do_pfx_fldsc_link(flmgt, fldsc, mpgid.flnum, FREE_TYPE_TOTAL);
    }
    flmgt->tfree.pgcnt += count;
    return 0;
}

int mpage_redo_get_partial_page(pmap_page *pmapg, int pbidx)
{
    PageID        mpgid = pmapg->hdr.map_pgid;
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    int           spcid = (int)PREFIXID_FROM_PAGEID(&mpgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&mpgid);

    do_mpage_clear_partial_page(pmapg, pbidx);

    pfspace = do_pfx_space_find(spcid);
    assert(pfspace != NULL);

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    fldsc = flmgt->fldsc_tab[mpgid.flnum-1];
    assert(fldsc != NULL);

    /* decrement # of partial free pages */
    fldsc->pmpfs[mpgid.pgnum / flmgt->npage_ppmap] -= 1;
    fldsc->pfree.pgcnt -= 1;
    if (fldsc->pfree.pgcnt == 0) { /* unlink from partial free file list */
        do_pfx_fldsc_unlink(flmgt, fldsc, mpgid.flnum, FREE_TYPE_PARTIAL);
    }
    flmgt->pfree.pgcnt -= 1;
    return 0;
}

int mpage_redo_set_partial_page(pmap_page *pmapg, int pbidx)
{
    PageID        mpgid = pmapg->hdr.map_pgid;
    prefix_space *pfspace;
    file_mgmt    *flmgt;
    file_desc    *fldsc;
    int           spcid = (int)PREFIXID_FROM_PAGEID(&mpgid);
    int           ftype = (int)FILETYPE_FROM_PAGEID(&mpgid);

    do_mpage_set_partial_page(pmapg, pbidx);

    pfspace = do_pfx_space_find(spcid);
    assert(pfspace != NULL);

    if (ftype == SQUALL_FILE_TYPE_DATA)      flmgt = &pfspace->data_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_OVFL) flmgt = &pfspace->ovfl_flmgt;
    else if (ftype == SQUALL_FILE_TYPE_INDX) flmgt = &pfspace->indx_flmgt;
    else { return -1; /* redo fail */ }

    fldsc = flmgt->fldsc_tab[mpgid.flnum-1];
    assert(fldsc != NULL);

    /* increment # of partial free pages */
    fldsc->pmpfs[mpgid.pgnum / flmgt->npage_ppmap] += 1;
    fldsc->pfree.pgcnt += 1;
    if (fldsc->pfree.pgcnt == 1) { /* link into partial free file list */
        do_pfx_fldsc_link(flmgt, fldsc, mpgid.flnum, FREE_TYPE_PARTIAL);
    }
    flmgt->pfree.pgcnt += 1;
    return 0;
}
#endif

/******************** OLD CODE *************************************/

#if 0 /* free data item */
void pfx_free_data_item(int pfxid, ItemDesc *itdesc)
{
    ItemID  *ovfl_ids;
    Page    *pgptr;
    PageID   pgid;
    uint32_t slotit, i;

    /* free ovfl data slots */
    if (itdesc->ptr->novfl > 0) {
        ovfl_ids = item_get_data(itdesc->ptr);
        for (i = 0; i < it->novfl; i++) {
            GET_PAGEID_SLOTID_FROM_ITEMID(&ovfl_ids[i], &pgid, &slotid);
            pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
            if (pgptr != NULL) {
                opage_free_slot(pgptr, slotid);
                buf_unfix(pgptr);
            } else {
                /* SEVERE error */
            }
        }
    }

    /* free base data slot */
    GET_PAGEID_SLOTID_FROM_ITEMID(&itdesc->iid, &pgid, &slotid);
    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr != NULL) {
        dpage_free_slot(pgptr, slotid);
        buf_unfix(pgptr);
    } else {
        /* SEVERE error */
    }
}
#endif

#if 0 /* data slot classid management - check later */
/* The following data slot size to classid mapping
 * is needed when fixed length slot page is used.
 */
static inline int do_pfx_data_slot_clsid(const uint32_t size)
{
    assert(0 < size <= max_slot_size);
    uint32_t basis_size;
    int      clsid = 0;

    do {
        clsid++;
        basis_size = page__bosize / (1 << clsid);
    } while(size < basis_size);

    if (size > basis_size) {
        clsid--;
    }
    return clsid;
}

static inline uint32_t do_pfx_data_slot_size(const int clsid)
{
    assert(0 <= clsid < 16);
    return (clsid == 0 ?  max_slot_size : (page_size / (1 << clsid)));
}
#endif

#if 0 // data page alloc & free from data bitmap page */
static int do_pfx_data_bmap_alloc_page(PageID pgid, int *midx)
{
    Page *pgptr;

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        return -1; /* SEVERE error */
    }

    *midx = mpage_find_free(pgptr);
    if (*midx != -1) {
        mpage_set_used(pgptr, *midx);
    }

    /* LOGGING */

    buf_unfix(pgptr);
    return ((*midx != -1) ? 0 : -1);
}

static int do_pfx_data_bmap_free_page(PageID pgid, int midx)
{
    Page *pgptr;

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        return -1; /* SEVERE error */
    }

    mpage_set_free(pgptr, midx);

    /* LOGGING */

    buf_unfix(pgptr);
    return 0;
}
#endif

#if 0 /* data page link and unlink */
static int do_pfx_data_page_link(prefix_desc *pfdesc, PageID *allo_pgptr)
{
    /* link the new page into the used page list */
    PageUsedList *uslist = &pfdesc->d.data_base_used;
    Page  *tail_pgptr = NULL;
    PageID allo_pgid = allo_pgptr->header.self_pgid;
    int count = 1;

    pthread_mutex_lock(&pfdesc->data_lock);
    do {
        if (PAGEID_IS_NULL(&uslist->tail)) {
            uslist->head = allo_pgptr->header.self_pgid;
            uslist->tail = allo_pgptr->header.self_pgid;
        } else {
            tail_pgptr = buf_fix(uslist->tail, BUF_MODE_WRITE, false, false);
            if (tail_pgptr == NULL) {
                count = -1; /* SEVERE error */
                break;
            }
            uslist->tail = allo_pgptr->header.self_pgid;
        }
    } while(0);
    pthread_mutex_unlock(&pfdesc->data_lock);

    if (tail_pgptr == NULL) {
        allo_pgptr->header.prev_pgid = tail_pgptr->header.self_pgid;
        tail_pgptr->header.next_pgid = allo_pgptr->header.self_pgid;

        /* LOGGING */

        buf_unfix(tail_pgptr);
    }

    return count; /* -1 or 1 : link page count */
}

static int do_pfx_data_page_unlink(prefix_desc *pfdesc, Page *free_pgptr)
{
    PageUsedList *uslist = &pfdesc->d.data_base_used;
    PageID prev_pgid = free_pgptr->header.prev_pgid;
    PageID next_pgid = free_pgptr->header.next_pgid;
    Page *prev_pgptr;
    Page *next_pgptr;

    /* data page having base slots */

    if (PAGEID_IS_NULL(&prev_pgid)) {
        /* the first page */
        if (PAGEID_IS_NULL(&next_pgid)) {
            pthread_mutex_lock(&pfdesc->data_lock);
            PAGEID_SET_NULL(&uslist->head);
            PAGEID_SET_NULL(&uslist->tail);
            pthread_mutex_unlock(&pfdesc->data_lock);
        } else {
            next_pgptr = buf_fix(next_pgid, BUF_MODE_WRITE, false, false);
            if (next_pgptr == NULL) {
                return -1; /* SEVERE error */
            }

            next_pgptr->header.prev_pgid = prev_pgid;
            PAGEID_SET_NULL(&pghdr->next_pgid);

            /* LOGGING */

            buf_unfix(next_pgptr);

            pthread_mutex_lock(&pfdesc->data_lock);
            uslist->head = next_pgid;
            pthread_mutex_unlock(&pfdesc->data_lock);
        }
    } else {
        /* the other page */
        /* fix the prev page with trymode = true */
        prev_pgptr = buf_fix(prev_pgid, BUF_MODE_WRITE, true, false); // TRY_MODE
        if (prev_pgptr == NULL) {
            buf_unlatch_only(pgptr);
            prev_pgptr = buf_fix(prev_pgid, BUF_MODE_WRITE, false, false);
            buf_latch_only(pgptr, BUF_MODE_WRITE);
            if (prev_pgptr == NULL) {
                return -1; /* SEVERE error */
            }
            if (dpage_is_empty(pgptr) != true) {
                return 0;
            }
        }

        prev_pgptr->header.next_pgid = next_pgid;
        PAGEID_SET_NULL(&pghdr->prev_pgid);

        if (PAGEID_IS_NULL(&pghdr->next_pgid)) {
            /* LOGGING */

            pthread_mutex_lock(&pfdesc->data_lock);
            uslist->tail = prev_pgid;
            pthread_mutex_unlock(&pfdesc->data_lock);
        } else {
            next_pgptr = buf_fix(next_pgid, BUF_MODE_WRITE, false, false);
            if (next_pgptr == NULL) {
                return -1; /* SEVERE error */
            }

            next_pgptr->header.prev_pgid = prev_pgid;
            PAGEID_SET_NULL(&pghdr->next_pgid);

            /* LOGGING */
            buf_unfix(next_pgptr);
        }
    }
}
#endif

#if 0 /* data page find */
int pfx_data_page_find(int pfxid, enum squall_page_type type, uint32_t size, PageID *pgid)
{
    assert(pfxid >= 0 && pfxid < config->sq.max_prefix_count);
    prefix_desc  *pfdesc = &pfx_gl.desc_tab[pfxid];
    PageFreeList *frlist;
    int  clsid = do_pfx_data_slot_clsid(size);
    int  frcnt;

    frlist = (type == SQUALL_PAGE_TYPE_DATA ? &pfdesc->d.data_base_free[clsid]
                                            : &pfdesc->d.data_ovfl_free[clsid]);
    if (frlist->wcnt == 0) {
    }

    pthread_mutex_lock(&pfdesc->data_lock);
    do {
        if (frlist->wcnt == 0) {
            int pgcnt = do_pfx_data_page_alloc(pfdesc, *pgid);
            if (pgcnt <= 0) {
            }
        }
        assert(frlist->wcnt > 0);

        *pgid = frlist->warm[frlist->widx];
        frcnt = frlist->fcnt[frlist->widx];

        if ((--frlist->fcnt[frlist->widx]) == 0) {

        }
        if (frlist->wcnt > 1) {
            if ((++frlist->widx) == frlist->wcnt)
                frlist->widx = 0;
        }
    } while(0);
    pthread_mutex_unlock(&pfdesc->data_lock);

    pgptr = buf_fix(pgid, BUF_MODE_WRITE, false, false);
    if (pgptr == NULL) {
        break; /* SEVERE error */
    }
    nfree = dpage_num_free_slot(pgptr);
    if (nfree == 0) {
        pgid = pgptr->header.fnxt_pgid
        buf_unfix(pgptr);
        continue;
    }
    if (nfree == 1) {
        pthread_mutex_lock(&pfdesc->data_lock);
        pfdesc->d.data_base_free[clsid] = pgptr->header.fnxt_pgid;
        pthread_mutex_lock(&pfdesc->data_lock);
    }
    /* return value - frcnt(free slot count)
     *                 -1: SEVERE error
     *                  0: out of space
     *   positive integer: # of free slots in the returned page
     */
    return frcnt;
}
#endif

#if 0 /* the first old version */
int do_pfx_alloc_data_slot(prefix_desc *pfdesc, uint32_t size)
{
    uint32_t max_slot_size = page_max_slot_size();
    uint32_t cur_slot_size = (size < max_slot_size ? size : max_slot_size);
    bool alloc_fail = false;

    pthread_mutex_lock(&pfdesc->data_lock);
    do {
        if (size >= (max_slot_size - min_slot_size)) {
            /* allocate from the scattered free page */
            if (!PAGEID_IS_NULL(&pfdesc->data_sfree_head)) {
                data_pgptr = buf_fix(pfdesc->data_sfree_head, BUF_MODE_WRITE, false, false);
                if (data_pgptr == NULL) {
                    alloc_fail = true; break; /* SEVERE problem */
                }
                pfdesc->data_sfree_head = data_pgptr->header.next_pgid;
                PAGEID_SET_NULL(&data_pgptr->header.next_pgid);
                pfdesc->data_sfree_pgcnt -= 1;
                break;
            }
            /* allocate from the contiguous free page */
            if (!PAGEID_IS_NULL(&pfdesc->data_cfree_next)) {
                data_pgptr = buf_fix(pfdesc->data_cfree_next, BUF_MODE_WRITE, false, false);
                if (data_pgptr == NULL) {
                    alloc_fail = true; break; /* SEVERE problem */
                }
                pfdesc->data_cfree_pgcnt -= 1;
                if (pfdesc->data_cfree_pgcnt == 0) {
                    PAGEID_SET_NULL(&pfdesc->data_cfree_next);
                } else {
                    PAGEID_INCREMENT(&pfdesc->data_cfree_next, max_page_num);
                }
                break;
            }
            /* allocate from the new page: check prefix disk quota */
            if ((pfdesc->data_total_pgcnt + pfdesc->ovfl_total_pgcnt + pfdesc->indx_total_pgcnt) < pfdesc->disk_quota) {
                data_pgptr = buf_fix(data_alloc_next, BUF_MODE_WRITE, false, true); // NEW_PAGE
                if (data_pgptr == NULL) {
                    alloc_fail = true; break; /* SEVERE problem */
                }
                //assert(PAGEID_IS_NULL(&data_pgptr->header.self_pgid));

                pfdesc->data_total_pgcnt += 1;
                PAGEID_INCREMENT(&pfdesc->data_alloc_next, max_page_num);
                break;
            }
        } else {
            /* allocate from the contiguous free page */
            if (!PAGEID_IS_NULL(&pfdesc->data_pfree_head)) {
                data_pgptr = buf_fix(pfdesc->data_pfree_head, BUF_MODE_WRITE, false, false);
                if (data_pgptr == NULL) {
                    alloc_fail = true; break; /* SEVERE problem */
                }
                pfdesc->data_pfree_head = data_pgptr->header.next_pgid;
                PAGEID_SET_NULL(&data_pgptr->header.next_pgid);
                pfdesc->data_pfree_pgcnt -= 1;
                break;
            }
        }
    } while(0);
    pthread_mutex_unlock(&pfdesc->data_lock);

    if (alloc_fail == true) return NULL; /* SEVERE problem */
    if (data_pgptr == NULL) return NULL; /* NO space available */

    length = (data_item*)dpage_alloc_slot(data_pgptr, size, &data_itmid);
    if (length == 0) { /* SEVERE error */
        /* how to handle this error ?? */
        buf_unfix(data_pgptr);
        alloc_fail = true;
        return NULL;
    }

    data_it = dpage_find_slot(data_pgptr, data_itmid);
    data_it->nval = XX;
    buf_unlatch(data_pgptr);

    do {
        remain_size -= data_it->nval;

        while (remain_size > 0) {
        }
    } while(0);
}
#endif
