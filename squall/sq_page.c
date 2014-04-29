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
#include "config.h"
#include <stdio.h>
#include <assert.h>

#include "squall_engine.h"
#include "sq_page.h"
#include "sq_buffer.h"
#include "sq_log.h"

/* page manager global structure */
struct page_global {
    uint32_t    page_size;  /* page total size */
    uint32_t    pghead_size; /* page header size */
    uint32_t    pgbody_size; /* page body size */
    uint32_t    pgslot_size; /* page slot size */
    char        slot_CHAR_USED;
    char        slot_CHAR_FREE;
};

/* global variables */
static struct config    *config = NULL;
static SERVER_HANDLE_V1 *server = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct page_global page_gl;


/************************/
/* Index Page Functions */
/************************/
/* Variable Length Slot Page */

static int xpage_compact_space(Page *pgptr)
{
    PGHDR   *pghdr = &pgptr->header;
    PGSLOT  *slot;
    char     curr_buffer[page_gl.page_size];
    uint16_t curr_offset;
    uint16_t curr_length;
    uint16_t slot_length;
    uint16_t cont_length;
    int      snum, i;

    /* variable length slot page */
    assert(pghdr->pgsltsz == 0);

    slot_length = pghdr->used_nslots * sizeof(PGSLOT);
    cont_length = page_gl.page_size - pghdr->cont_offset - slot_length;
    if (cont_length == pghdr->totl_frsize) {
        assert(pghdr->scat_frslot.offset == 0);
        return 0; /* nothing to compact */
    }
    assert(cont_length < pghdr->totl_frsize);

    /* there are some scattered free slots */
    curr_offset = sizeof(PGHDR);
    curr_length = 0;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    for (snum = 0; snum < pghdr->used_nslots; snum++, slot--) {
        assert(slot->offset >= sizeof(PGHDR) && slot->offset < page_gl.page_size); /* USED slot */
        if (slot->offset == curr_offset)
            curr_offset += slot->length;
        else
            break;
    }
    for (i = snum; i < pghdr->used_nslots; i++, slot--) {
        assert(slot->offset >= sizeof(PGHDR) && slot->offset < page_gl.page_size); /* USED slot */
        /* save slot content in temp buffer and adjust the slot offset */
        memcpy(&curr_buffer[curr_length], (char*)pgptr + slot->offset, slot->length);
        slot->offset = curr_offset + curr_length;
        curr_length += slot->length;
    }
    if (curr_length > 0) {
        /* move the buffered content into the page */
        memcpy((char*)pgptr + curr_offset, curr_buffer, curr_length);
    }

    /* clear scatter free slot list */
    pghdr->scat_frslot.offset = 0;
    pghdr->scat_frslot.length = 0;

    /* adjust contiguous free slot and recheck with total free size */
    pghdr->cont_offset = curr_offset + curr_length;
    cont_length = page_gl.page_size - pghdr->cont_offset - slot_length;
    assert(cont_length == pghdr->totl_frsize);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* LOGGING - page compaction */
        /* NTS(nested top action) : redo-only */
        PGCompactLog pgcpt_log;
        lrec_pg_compact_init((LogRec*)&pgcpt_log);
        pgcpt_log.body.pgid = pghdr->self_pgid;
        pgcpt_log.body.doffset = curr_offset;
        pgcpt_log.body.dlength = curr_length;
        pgcpt_log.body.soffset = page_gl.page_size - slot_length;
        pgcpt_log.body.slength = (pghdr->used_nslots - snum) * sizeof(PGSLOT);
        pgcpt_log.pgptr = pgptr;
        pgcpt_log.header.length = offsetof(PGCompactData, data)
                                + pgcpt_log.body.dlength + GET_8_ALIGN_SIZE(pgcpt_log.body.slength);
        log_record_write((LogRec*)&pgcpt_log, NULL);
        /* update PageLSN */
        buf_set_lsn(pgptr, pgcpt_log.header.self_lsn);
    }
#endif

    /* set buffer page dirty */
    buf_set_dirty(pgptr);
    return 0;
}

void xpage_init(Page *pgptr, PageID pgid, int pgdepth)
{
    PGHDR *pghdr = &pgptr->header;

    LOGSN_SET_NULL(&pghdr->pglsn);
    pghdr->self_pgid = pgid;
    PAGEID_SET_NULL(&pghdr->prev_pgid);
    PAGEID_SET_NULL(&pghdr->next_pgid);

    pghdr->pgtype  = SQUALL_PAGE_TYPE_INDX;
    pghdr->pgdepth = pgdepth;
    pghdr->pgsltsz = 0; /* variable length slot */
    pghdr->pgdummy = 0; /* NOT USED */

    /* variable length slot page */
    pghdr->used_nslots = 0;
    pghdr->totl_nslots = 0; /* NOT USED */
    pghdr->totl_frsize = page_gl.pgbody_size;
    pghdr->cont_offset = page_gl.pghead_size;
    pghdr->scat_frslot.offset = 0;
    pghdr->scat_frslot.length = 0;

    /* init reserved space */
    pghdr->rsvd32 = 0;
    pghdr->rsvd64 = 0;
}

void xpage_clear_space(Page *pgptr)
{
    PGHDR *pghdr = &pgptr->header;

    pghdr->used_nslots = 0;
    pghdr->totl_frsize = page_gl.pgbody_size;
    pghdr->cont_offset = page_gl.pghead_size;
    pghdr->scat_frslot.offset = 0;
    pghdr->scat_frslot.length = 0;
}

void *xpage_find_slot(Page *pgptr, uint32_t slotid)
{
    assert(slotid < pgptr->header.used_nslots);

    PGSLOT *slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= slotid;
    assert(slot->offset >= sizeof(PGHDR)); /* USED slot */

    return (void*)((char*)pgptr + slot->offset);
}

void *xpage_alloc_slot(Page *pgptr, uint32_t slotid, uint32_t size)
{
    assert(slotid <= pgptr->header.used_nslots);
    assert((size % SLOT_ALIGN_BYTES) == 0 && size < page_gl.pgbody_size);
    PGHDR    *pghdr = &pgptr->header;
    PGSLOT   *slot;
    uint16_t ntotal = (uint16_t)size + sizeof(PGSLOT);
    uint16_t slot_length;
    uint16_t cont_length;

    assert(ntotal <= pghdr->totl_frsize);

    slot_length = pghdr->used_nslots * sizeof(PGSLOT);
    cont_length = page_gl.page_size - pghdr->cont_offset - slot_length;
    if ((cont_length < ntotal) &&
        (pghdr->scat_frslot.length != size || cont_length < sizeof(PGSLOT))) {
        /* do index page compaction */
        if (xpage_compact_space(pgptr) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage alloc slot: compact space fail\n");
            return NULL; /* SEVERE error */
        }
        cont_length = page_gl.page_size - pghdr->cont_offset - slot_length;
        assert(cont_length >= ntotal && pghdr->scat_frslot.offset == 0);
    }

    /* allocate slot */
    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= pghdr->used_nslots;
    for (int i = pghdr->used_nslots; i > slotid; i--, slot++) {
        *slot = *(slot+1);
    }

    /* allocate space */
    if (pghdr->scat_frslot.length == size && cont_length >= sizeof(PGSLOT)) {
        /* reuse the scattered free slot */
        slot->offset = pghdr->scat_frslot.offset;
        slot->length = size;
        pghdr->scat_frslot = *(PGSLOT*)((char*)pgptr + pghdr->scat_frslot.offset);
    } else {
        /* allocate from contiguous free slot */
        slot->offset = pghdr->cont_offset;
        slot->length = size;
        pghdr->cont_offset += size;
    }
    pghdr->totl_frsize -= ntotal;
    pghdr->used_nslots += 1;

    return (void*)((char*)pgptr + slot->offset);
}

void *xpage_realloc_slot(Page *pgptr, uint32_t slotid, uint32_t size)
{
    assert(slotid < pgptr->header.used_nslots);
    assert((size % SLOT_ALIGN_BYTES) == 0 && size < page_gl.pgbody_size);
    PGHDR    *pghdr = &pgptr->header;
    PGSLOT   *slot;
    PGSLOT    temp;
    uint16_t diff_length;
    uint16_t cont_length;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= slotid;
    assert(slot->offset >= sizeof(PGHDR)); /* USED slot */

    /* case 1: the same size */
    if (size == slot->length) {
        return (void*)((char*)pgptr + slot->offset);
    }

    /**********************************/
    /* case 2: decrease the slot size */
    /**********************************/
    if (size < slot->length) {
        diff_length = slot->length - (uint16_t)size;
        /* truncate the slot */
        if ((slot->offset + slot->length) == pghdr->cont_offset) {
            slot->length -= diff_length;
            pghdr->cont_offset -= diff_length;
        } else {
            temp.offset = slot->offset + (uint16_t)size;
            temp.length = diff_length;
            *(PGSLOT*)((char*)pgptr + temp.offset) = pghdr->scat_frslot;
            pghdr->scat_frslot = temp;
            slot->length -= diff_length;
        }
        pghdr->totl_frsize += diff_length;
        return (void*)((char*)pgptr + slot->offset);
    }

    /**********************************/
    /* case 3: increase the slot size */
    /**********************************/
    diff_length = (uint16_t)size - slot->length;

    /* 3-1) expand the space if it is at the last position */
    if ((slot->offset + slot->length) == pghdr->cont_offset) {
        cont_length = page_gl.page_size - pghdr->cont_offset - (pghdr->used_nslots * sizeof(PGSLOT));
        if (diff_length <= cont_length) {
            slot->length += diff_length;
            pghdr->cont_offset += diff_length;

            pghdr->totl_frsize -= diff_length;
            return (void*)((char*)pgptr + slot->offset);
        }
    }

    /* 3-2) reuse the scattered free slot if possible */
    if ((uint16_t)size <= pghdr->scat_frslot.length) {
        /* allocate free space from scattered free slot */
        temp.offset = pghdr->scat_frslot.offset;
        temp.length = (uint16_t)size;

        if ((uint16_t)size == pghdr->scat_frslot.length) {
            pghdr->scat_frslot = *(PGSLOT*)((char*)pgptr + pghdr->scat_frslot.offset);
        } else {
            pghdr->scat_frslot.offset += (uint16_t)size;
            pghdr->scat_frslot.length -= (uint16_t)size;
            *(PGSLOT*)((char*)pgptr + pghdr->scat_frslot.offset) = *(PGSLOT*)((char*)pgptr + temp.offset);
        }

        /* copy original data into the newly allocated slot */
        memcpy((char*)pgptr + temp.offset, (char*)pgptr + slot->offset, slot->length);

        /* free the original slot */
        if ((slot->offset + slot->length) == pghdr->cont_offset) {
            pghdr->cont_offset = slot->offset;
        } else {
            *(PGSLOT*)((char*)pgptr + slot->offset) = pghdr->scat_frslot;
            pghdr->scat_frslot = *slot;
        }

        /* change slot meta info */
        *slot = temp;

        pghdr->totl_frsize -= diff_length;
        return (void*)((char*)pgptr + slot->offset);
    }

    /* lack of contiguous free space : do compaction */
    assert(size <= pghdr->totl_frsize);

    if (xpage_compact_space(pgptr) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "xpage realloc slot: compact space fail\n");
        return NULL; /* SEVERE error */
    }

    /* allocate from contiguous free space */
    if ((slot->offset + slot->length) == pghdr->cont_offset) {
        slot->length += diff_length;
        pghdr->cont_offset += diff_length;
    } else {
        /* copy original data into the to-be-allocated slot */
        memcpy((char*)pgptr + pghdr->cont_offset, (char*)pgptr + slot->offset, slot->length);

        /* free the original slot */
        *(PGSLOT*)((char*)pgptr + slot->offset) = pghdr->scat_frslot;
        pghdr->scat_frslot = *slot;

        /* adjust slot information */
        slot->offset = pghdr->cont_offset;
        slot->length += diff_length;
        pghdr->cont_offset = (slot->offset + slot->length);
    }
    pghdr->totl_frsize -= diff_length;
    return (void*)((char*)pgptr + slot->offset);

#if 0 /* OLD_VERSION: problem => old data is not preserved  */
    assert(size <= pghdr->totl_frsize);

    if (size != slot->length) {
        /* check if enough space exist in the page */
        cont_length = page_gl.page_size - pghdr->cont_offset - (pghdr->used_nslots * sizeof(PGSLOT));
        if (cont_length < size) {
            /* do index page compaction */
            if (xpage_compact_space(pgptr) != 0) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "xpage realloc slot: compact space fail\n");
                return NULL; /* SEVERE error */
            }
        }

        /* free the old slot */
        if ((slot->offset + slot->length) == pghdr->cont_offset) {
            pghdr->cont_offset = slot->offset;
        } else {
            *(PGSLOT*)((char*)pgptr + slot->offset) = pghdr->scat_frslot;
            pghdr->scat_frslot = *slot;
        }
        pghdr->totl_frsize -= slot->length;

        /* allocate the new slot */
        if (pghdr->scat_frslot.length == size) {
            /* reuse the scattered free slot */
            slot->offset = pghdr->scat_frslot.offset;
            slot->length = size;
            pghdr->scat_frslot = *(PGSLOT*)((char*)pgptr + pghdr->scat_frslot.offset);
        } else {
            /* allocate from contiguous free slot */
            slot->offset = pghdr->cont_offset;
            slot->length = size;
            pghdr->cont_offset += size;
        }
        pghdr->totl_frsize += size;
    }
    return (void*)((char*)pgptr + slot->offset);
#endif
}

int xpage_free_slot(Page *pgptr, uint32_t slotid)
{
    assert(slotid < pgptr->header.used_nslots);
    PGHDR  *pghdr = &pgptr->header;
    PGSLOT *slot;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= slotid;
    assert(slot->offset >= sizeof(PGHDR)); /* USED slot */

    /* adjust free slot info */
    if ((slot->offset + slot->length) == pghdr->cont_offset) {
        pghdr->cont_offset = slot->offset;
    } else {
        *(PGSLOT*)((char*)pgptr + slot->offset) = pghdr->scat_frslot;
        pghdr->scat_frslot = *slot;
    }
    pghdr->totl_frsize += (slot->length + sizeof(PGSLOT));

    /* move slots */
    for (int i = slotid+1; i < pghdr->used_nslots; i++, slot--) {
        *slot = *(slot-1);
    }
    pghdr->used_nslots -= 1;
    return 1;
}

void xpage_move_slots(Page *dst_pgptr, Page *src_pgptr, uint32_t slotid, uint32_t count)
{
    PGHDR  *dst_pghdr = &dst_pgptr->header;
    PGHDR  *src_pghdr = &src_pgptr->header;
    PGSLOT *dst_slot;
    PGSLOT *src_slot;

    /* Append slots of source page to the destination page */
    assert(slotid == 0 || (slotid+count) == src_pghdr->used_nslots);

    dst_slot = (PGSLOT*)((char*)dst_pgptr + page_gl.page_size - sizeof(PGSLOT));
    dst_slot -= dst_pghdr->used_nslots;
    src_slot = (PGSLOT*)((char*)src_pgptr + page_gl.page_size - sizeof(PGSLOT));
    src_slot -= slotid;

    for (int i = 0; i < count; i++, src_slot--, dst_slot--) {
        /* copy the slot */
        dst_slot->offset = dst_pghdr->cont_offset;
        dst_slot->length = src_slot->length;
        memcpy((char*)dst_pgptr + dst_slot->offset,
               (char*)src_pgptr + src_slot->offset, dst_slot->length);
        dst_pghdr->cont_offset += dst_slot->length;
        dst_pghdr->totl_frsize -= (dst_slot->length + sizeof(PGSLOT));

        /* remove the source slot */
        if ((src_slot->offset + src_slot->length) == src_pghdr->cont_offset) {
            src_pghdr->cont_offset = src_slot->offset;
        } else {
            *(PGSLOT*)((char*)src_pgptr + src_slot->offset) = src_pghdr->scat_frslot;
            src_pghdr->scat_frslot = *src_slot;
        }
        src_pghdr->totl_frsize += (src_slot->length + sizeof(PGSLOT));
    }
    if (slotid == 0) {
        src_slot = (PGSLOT*)((char*)src_pgptr + page_gl.page_size - sizeof(PGSLOT));
        for (int j = count; j < src_pghdr->used_nslots; j++, src_slot--) {
             *src_slot = *(src_slot-count);
        }
    }

    dst_pghdr->used_nslots += count;
    src_pghdr->used_nslots -= count;
}

void xpage_copy_slots(Page *dst_pgptr, Page *src_pgptr, uint32_t slotid, uint32_t count)
{
    PGHDR  *dst_pghdr = &dst_pgptr->header;
    PGHDR  *src_pghdr = &src_pgptr->header;
    PGSLOT *dst_slot;
    PGSLOT *src_slot;

    /* Append slots of source page to the destination page */
    assert(slotid == 0 || (slotid+count) == src_pghdr->used_nslots);

    dst_slot = (PGSLOT*)((char*)dst_pgptr + page_gl.page_size - sizeof(PGSLOT));
    dst_slot -= dst_pghdr->used_nslots;
    src_slot = (PGSLOT*)((char*)src_pgptr + page_gl.page_size - sizeof(PGSLOT));
    src_slot -= slotid;

    for (int i = 0; i < count; i++, src_slot--, dst_slot--) {
        /* copy the slot */
        dst_slot->offset = dst_pghdr->cont_offset;
        dst_slot->length = src_slot->length;
        memcpy((char*)dst_pgptr + dst_slot->offset,
               (char*)src_pgptr + src_slot->offset, dst_slot->length);
        dst_pghdr->cont_offset += dst_slot->length;
        dst_pghdr->totl_frsize -= (dst_slot->length + sizeof(PGSLOT));
    }
    dst_pghdr->used_nslots += count;
}

void xpage_truncate_slots(Page *pgptr, uint32_t slotid, uint32_t count)
{
    PGHDR  *pghdr = &pgptr->header;
    PGSLOT *slot;

    assert(slotid == 0 || (slotid+count) == pghdr->used_nslots);

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= slotid;

    for (int i = 0; i < count; i++, slot--) {
        if ((slot->offset + slot->length) == pghdr->cont_offset) {
            pghdr->cont_offset = slot->offset;
        } else {
            *(PGSLOT*)((char*)pgptr + slot->offset) = pghdr->scat_frslot;
            pghdr->scat_frslot = *slot;
        }
        pghdr->totl_frsize += (slot->length + sizeof(PGSLOT));
    }
    if (slotid == 0) {
        slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
        for (int j = count; j < pghdr->used_nslots; j++, slot--) {
             *slot = *(slot-count);
        }
    }
    pghdr->used_nslots -= count;
}

bool xpage_check_consistency(Page *pgptr, int pgdepth)
{
    PGHDR  *pghdr = &pgptr->header;
    PGSLOT *slot;
    uint16_t slot_length = (pghdr->used_nslots * sizeof(PGSLOT));
    uint16_t used_length = 0;
    uint16_t free_length = 0;
    bool     consistent = true;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));

    for (int i = 0; i < pghdr->used_nslots; i++, slot--) {
        if (slot->offset < sizeof(PGHDR) || (slot->offset % SLOT_ALIGN_BYTES) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: used slot offset fail\n");
            consistent = false; break;
        }
        if ((slot->length % SLOT_ALIGN_BYTES) != 0 || (slot->offset+slot->length) > (page_gl.page_size-slot_length)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: used slot length fail\n");
            consistent = false; break;
        }
        used_length += slot->length;
    }
    while (consistent == true) {
        if (pghdr->totl_frsize != (page_gl.pgbody_size - used_length - slot_length)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: total free size fail\n");
            consistent = false; break;
        }
        if (pghdr->scat_frslot.offset != 0) {
            PGSLOT temp = pghdr->scat_frslot;
            do {
                if (temp.offset < sizeof(PGHDR) || (temp.offset % SLOT_ALIGN_BYTES) != 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: free slot offset fail\n");
                    consistent = false; break;
                }
                if ((temp.length % SLOT_ALIGN_BYTES) != 0 || (temp.offset+temp.length) > pghdr->cont_offset) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: free slot length fail\n");
                    consistent = false; break;
                }
                free_length += temp.length;
                /* get the next free slot */
                temp = *(PGSLOT*)((char*)pgptr + temp.offset);
            } while (temp.offset != 0);
            if (consistent != true) break;
        }
        if (pghdr->cont_offset != (sizeof(PGHDR) + used_length + free_length)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: contiguous space offset fail\n");
            consistent = false; break;
        }
        if (pghdr->pgtype != SQUALL_PAGE_TYPE_INDX) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: page type mismatch\n");
            consistent = false; break;
        }
        if (pghdr->pgdepth != pgdepth) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: page depth mismatch\n");
            consistent = false; break;
        }
        if (pghdr->pgsltsz != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "xpage consistency check: page slot size mismatch\n");
            consistent = false; break;
        }
        break;
    }
    return consistent;
}

int xpage_get_depth(Page *pgptr)
{
    return (int)pgptr->header.pgdepth;
}

void xpage_set_depth(Page *pgptr, int pgdepth)
{
    assert(pgdepth < 100);
    pgptr->header.pgdepth = (int8_t)pgdepth;
}

bool xpage_is_empty(Page *pgptr)
{
    return (pgptr->header.used_nslots == 0 ? true : false);
}

bool xpage_has_free_space(Page *pgptr, uint32_t size)
{
    return ((size + sizeof(PGSLOT)) <= pgptr->header.totl_frsize);
}

#if 1 // ENABLE_RECOVERY
void xpage_redo_build(Page *pgptr, char *logdata, uint32_t dataleng, uint32_t slotleng)
{
    memcpy((char*)pgptr + sizeof(PGHDR), logdata, dataleng);
    memcpy((char*)pgptr + page_gl.page_size - slotleng, logdata + dataleng, slotleng);

    pgptr->header.used_nslots = slotleng / sizeof(PGSLOT);
    pgptr->header.totl_frsize = page_gl.page_size - sizeof(PGHDR) - dataleng - slotleng;
    pgptr->header.cont_offset = sizeof(PGHDR) + dataleng;
    pgptr->header.scat_frslot.offset = 0;
    pgptr->header.scat_frslot.length = 0;
}

void xpage_alloc_slot_redo(Page *pgptr, uint32_t slotid, uint16_t offset, uint16_t length)
{
    assert(slotid <= pgptr->header.used_nslots);
    PGHDR  *pghdr = &pgptr->header;
    PGSLOT *slot;

    /* allocate slot */
    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= pghdr->used_nslots;
    for (int i = pghdr->used_nslots; i > slotid; i--, slot++) {
        *slot = *(slot+1);
    }
    slot->offset = offset;
    slot->length = length;

    if (offset == pghdr->scat_frslot.offset) {
        /* reuse the scattered free slot */
        pghdr->scat_frslot = *(PGSLOT*)((char*)pgptr + pghdr->scat_frslot.offset);
    } else {
        /* allocate from contiguous free slot */
        assert(offset == pghdr->cont_offset);
        pghdr->cont_offset += length;
    }
    pghdr->totl_frsize -= (length + sizeof(PGSLOT));
    pghdr->used_nslots += 1;
}

void xpage_free_slot_redo(Page *pgptr, uint32_t slotid)
{
    assert(slotid < pgptr->header.used_nslots);
    PGHDR  *pghdr = &pgptr->header;
    PGSLOT *slot;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= slotid;
    assert(slot->offset >= sizeof(PGHDR)); /* USED slot */

    /* adjust free slot info */
    if ((slot->offset + slot->length) == pghdr->cont_offset) {
        pghdr->cont_offset = slot->offset;
    } else {
        *(PGSLOT*)((char*)pgptr + slot->offset) = pghdr->scat_frslot;
        pghdr->scat_frslot = *slot;
    }
    pghdr->totl_frsize += (slot->length + sizeof(PGSLOT));

    /* move slots */
    for (int i = slotid+1; i < pghdr->used_nslots; i++, slot--) {
        *slot = *(slot-1);
    }
    pghdr->used_nslots -= 1;
}
#endif

/***********************/
/* Data Page Functions */
/***********************/
/* Data Base Page: Variable Length Slot Page */

void dpage_init(Page *pgptr, PageID pgid)
{
    PGHDR *pghdr = &pgptr->header;

    LOGSN_SET_NULL(&pghdr->pglsn);
    pghdr->self_pgid = pgid;
    PAGEID_SET_NULL(&pghdr->prev_pgid);
    PAGEID_SET_NULL(&pghdr->next_pgid);

    pghdr->pgtype  = SQUALL_PAGE_TYPE_DATA;
    pghdr->pgdepth = -1; /* NOT USED */
    pghdr->pgsltsz = 0;
    pghdr->pgdummy = 0;  /* NOT USED */

    pghdr->used_nslots = 0;
    pghdr->totl_nslots = 0;
    pghdr->totl_frsize = page_gl.pgbody_size;
    pghdr->cont_offset = page_gl.pghead_size;
    pghdr->scat_frslot.offset = 0;
    pghdr->scat_frslot.length = 0;

    /* init reserved space */
    pghdr->rsvd32 = 0;
    pghdr->rsvd64 = 0;
}

void dpage_clear_space(Page *pgptr)
{
    PGHDR *pghdr = &pgptr->header;
    assert(pghdr->used_nslots == 0);

    pghdr->totl_nslots = 0;
    pghdr->totl_frsize = page_gl.pgbody_size;
    pghdr->cont_offset = page_gl.pghead_size;
    pghdr->scat_frslot.offset = 0;
    pghdr->scat_frslot.length = 0;
}

void *dpage_find_slot(Page *pgptr, uint32_t slotid)
{
    assert(slotid >= 0 && slotid < pgptr->header.totl_nslots);
    PGSLOT *slot;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= slotid;

    if (slot->offset >= sizeof(PGHDR)) { /* USED slot */
        return (void*)((char*)pgptr + slot->offset);
    } else {
        return NULL;
    }
}

void *dpage_alloc_slot(Page *pgptr, uint32_t size, ItemID *itmid)
{
    assert((size % SLOT_ALIGN_BYTES) == 0 && size < page_gl.pgbody_size);
    PGHDR   *pghdr = &pgptr->header;
    PGSLOT  *slot;
    char    *item;
    uint32_t slotid;
    uint16_t ntotal;
    uint16_t cont_length;

    ntotal = (uint16_t)size;
    if (pghdr->used_nslots == pghdr->totl_nslots) {
        ntotal += sizeof(PGSLOT);
    }
    /* comapre with contiguous free space size */
    /* even if total free space size is larger than the requested size,
       the space cannot be used through the space compaction.
       because, other used slots are now being accessed by other threads.
       So, those slots cannot be moved within the page.
    */
    cont_length = page_gl.page_size - pghdr->cont_offset - (pghdr->totl_nslots*sizeof(PGSLOT));
    if (ntotal > cont_length) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "dpage alloc slot: no enough contiguous space\n");
        return NULL; /* no more contiguous free space: cannot allocate slot */
    }

    /* find the slot */
    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    if (pghdr->used_nslots < pghdr->totl_nslots) {
        for (slotid = 0; slotid < pghdr->totl_nslots; slotid++, slot--) {
            if (slot->offset == 0) break; /* FREE slot */
        }
        /* slotid and slot is found */
        assert(slotid < pghdr->totl_nslots);
    } else { /* pghdr->used_nslots == pghdr->totl_nslots */
        slotid = pghdr->totl_nslots;
        slot -= slotid;

        /* adjust totl_frsize and totl_nslots */
        pghdr->totl_frsize -= sizeof(PGSLOT);
        pghdr->totl_nslots += 1;
    }

    /* allocate the slot space */
    /* the data slot can only be freed when refcount is 0.
     * So, the scattered free space can also be used for the new slot.
     */
    if (size == pghdr->scat_frslot.length) {
        /* use the scattered free space */
        item = (char*)pgptr + pghdr->scat_frslot.offset;
        *slot = pghdr->scat_frslot;
        pghdr->scat_frslot = *(PGSLOT*)item;
    } else {
        /* use the contiguous free space */
        item = (char*)pgptr + pghdr->cont_offset;
        slot->offset = pghdr->cont_offset;
        slot->length = size;
        pghdr->cont_offset += size;
    }
    pghdr->totl_frsize -= size;
    pghdr->used_nslots += 1;

    GET_ITEMID_FROM_PAGEID_SLOTID(&pghdr->self_pgid, &slotid, itmid);
    return item;
}


int dpage_free_slot(Page *pgptr, uint32_t slotid)
{
    assert(slotid < pgptr->header.totl_nslots);
    PGHDR  *pghdr = &pgptr->header;
    PGSLOT *slot;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    slot -= slotid;
    assert(slot->offset >= sizeof(PGHDR)); /* must be an used slot */

    /* free space */
    if (slot->offset + slot->length == pghdr->cont_offset) {
        pghdr->cont_offset = slot->offset;
    } else {
        *(PGSLOT*)((char*)pgptr + slot->offset) = pghdr->scat_frslot;
        pghdr->scat_frslot = *slot;
    }
    pghdr->totl_frsize += slot->length;

    /* free slot */
    pghdr->used_nslots -= 1;
    if ((slotid+1) == pghdr->totl_nslots) {
        /* the last slot : remove it */
        pghdr->totl_frsize += sizeof(PGSLOT);
        pghdr->totl_nslots -= 1;
        /* remove the previous freed slots */
        if (pghdr->used_nslots < pghdr->totl_nslots) {
            slot++;
            for (int i = slotid-1; i >= 0; i--, slot++) {
                if (slot->offset != 0) break;
                pghdr->totl_frsize += sizeof(PGSLOT);
                pghdr->totl_nslots -= 1;
            }
            assert(pghdr->used_nslots <= pghdr->totl_nslots);
        }
    } else {
        /* not the last slot : set it as free state */
        slot->offset = 0;
    }
    return 1;
}

int dpage_delmark_slot(Page *pgptr, uint32_t slotid)
{
    /********************
    assert(slotid < pgptr->header.totl_nslots);
    PGHDR *pghdr = &pgptr->header;

    if (pghdr->pgsltsz == 0) {
        PGSLOT *slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT))
        slot -= slotid;

        assert(slot->status == SLOT_STATUS_USED);
        slot->status = SLOT_STATUS_DELMARK;
    } else {
    }
    *********************/
    return 0;
}

bool dpage_check_consistency(Page *pgptr)
{
    PGHDR   *pghdr = &pgptr->header;
    PGSLOT  *slot;
    uint16_t slot_count = 0;
    uint16_t slot_length = (pghdr->totl_nslots * sizeof(PGSLOT));
    uint16_t used_length = 0;
    uint16_t free_length = 0;
    bool     consistent = true;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));

    for (int i = 0; i < pghdr->totl_nslots; i++, slot--) {
        if (slot->offset == 0) continue;
        if (slot->offset < sizeof(PGHDR) || (slot->offset % SLOT_ALIGN_BYTES) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: used slot offset fail\n");
            consistent = false; break;
        }
        if ((slot->length % SLOT_ALIGN_BYTES) != 0 || (slot->offset+slot->length) > (page_gl.page_size-slot_length)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: used slot length fail\n");
            consistent = false; break;
        }
        used_length += slot->length;
        slot_count += 1;
    }
    while (consistent == true) {
        if (pghdr->used_nslots != slot_count) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: used slot count fail\n");
            consistent = false; break;
        }
        if (pghdr->totl_frsize != (page_gl.pgbody_size - used_length - slot_length)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: total free size fail\n");
            consistent = false; break;
        }
        if (pghdr->scat_frslot.offset != 0) {
            PGSLOT temp = pghdr->scat_frslot;
            do {
                if (temp.offset < sizeof(PGHDR) || (temp.offset % SLOT_ALIGN_BYTES) != 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: free slot offset fail\n");
                    consistent = false; break;
                }
                if ((temp.length % SLOT_ALIGN_BYTES) != 0 || (temp.offset+temp.length) > pghdr->cont_offset) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: free slot length fail\n");
                    consistent = false; break;
                }
                free_length += temp.length;
                /* get the next free slot */
                temp = *(PGSLOT*)((char*)pgptr + temp.offset);
            } while (temp.offset != 0);
            if (consistent != true) break;
        }
        if (pghdr->cont_offset != (sizeof(PGHDR) + used_length + free_length)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: contiguous space offset fail\n");
            consistent = false; break;
        }
        break;
    }
    return consistent;
}

bool dpage_is_empty(Page *pgptr)
{
    return (pgptr->header.used_nslots == 0 ? true : false);
}

int dpage_compact_space(Page *pgptr)
{
    PGHDR   *pghdr = &pgptr->header;
    PGSLOT  *slot;
    char     curr_buffer[page_gl.page_size];
    uint16_t curr_offset;
    uint16_t curr_length;
    uint16_t slot_length;
    uint16_t cont_length;
    int      snum, i;

    /* variable length slot page */
    assert(pghdr->pgsltsz == 0);

    /* Assumption)
     *    All items are not being referenced by others, now.
     *    Need to check it ??
     */
    slot_length = pghdr->totl_nslots * sizeof(PGSLOT);
    cont_length = page_gl.page_size - pghdr->cont_offset - slot_length;
    if (cont_length == pghdr->totl_frsize) {
        assert(pghdr->scat_frslot.offset == 0);
        return 0; /* nothing to compact */
    }
    assert(cont_length < pghdr->totl_frsize);

    /* there are some scattered free slots */
    curr_offset = sizeof(PGHDR);
    curr_length = 0;

    slot = (PGSLOT*)((char*)pgptr + page_gl.page_size - sizeof(PGSLOT));
    for (snum = 0; snum < pghdr->totl_nslots; snum++, slot--) {
        if (slot->offset >= sizeof(PGHDR)) { /* USED slot */
            assert(slot->offset < page_gl.page_size);
            if (slot->offset == curr_offset)
                curr_offset += slot->length;
            else
                break;
        } else {
            assert(slot->offset == 0);
        }
    }
    for (i = snum; i < pghdr->totl_nslots; i++, slot--) {
        if (slot->offset >= sizeof(PGHDR)) { /* USED slot */
            assert(slot->offset < page_gl.page_size);
            /* save slot content in temp buffer and adjust the slot offset */
            memcpy(&curr_buffer[curr_length], (char*)pgptr + slot->offset, slot->length);
            slot->offset = curr_offset + curr_length;
            curr_length += slot->length;
        } else {
            assert(slot->offset == 0);
        }
    }
    if (curr_length > 0) {
        /* move the buffered content into the page */
        memcpy((char*)pgptr + curr_offset, curr_buffer, curr_length);
    }

    /* clear scatter free slot list */
    pghdr->scat_frslot.offset = 0;
    pghdr->scat_frslot.length = 0;

    /* reset contiguous slot offset and recheck with total free size */
    pghdr->cont_offset = curr_offset + curr_length;
    cont_length = page_gl.page_size - pghdr->cont_offset - slot_length;
    assert(cont_length == pghdr->totl_frsize);

    /* Do not compact slot structures
     * Because, it makes ItemID to be changed.
     */
#if 0 // refer to item free log
#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* LOGGING - page compaction */
        /* NTS(nested top action) : redo-only */
        PGCompactLog pgcpt_log;
        lrec_pg_compact_init((LogRec*)&pgcpt_log);
        pgcpt_log.body.pgid = pghdr->self_pgid;
        pgcpt_log.body.doffset = curr_offset;
        pgcpt_log.body.dlength = curr_length;
        pgcpt_log.body.soffset = page_gl.page_size - slot_length;
        pgcpt_log.body.slength = (pghdr->totl_nslots - snum) * sizeof(PGSLOT);
        pgcpt_log.pgptr = pgptr;
        pgcpt_log.header.length = offsetof(PGCompactData, data)
                                + pgcpt_log.body.dlength + GET_8_ALIGN_SIZE(pgcpt_log.body.slength);
        log_record_write((LogRec*)&pgcpt_log, NULL);
        /* update PageLSN */
        buf_set_lsn(pgptr, pgcpt_log.header.self_lsn);
    }
#endif
#endif

    /* set buffer page dirty */
    buf_set_dirty(pgptr);
    return 1; /* compaction is performed */
}

/***************************/
/* Overflow Page Functions */
/***************************/
/* Data Ovfl Page: Fixed Length Slot Page - single slot case */

//#define USE_SLOT_STATUS_BYTE_ARRAY 1

void opage_init(Page *pgptr, PageID pgid, uint32_t slot_size)
{
    PGHDR *pghdr = &pgptr->header;

    /* currently, only single slot size of page body size is used */
    assert(slot_size == page_gl.pgbody_size);
    /* check the validation of the given slot size */
    //assert(slot_size >= SLOT_MIN_SIZE && slot_size <= page_gl.pgbody_size);
    //assert((slot_size & SLOT_ALIGN_BYTES) == 0);

    LOGSN_SET_NULL(&pghdr->pglsn);
    pghdr->self_pgid = pgid;
    PAGEID_SET_NULL(&pghdr->prev_pgid);
    PAGEID_SET_NULL(&pghdr->next_pgid);

    pghdr->pgtype  = SQUALL_PAGE_TYPE_OVFL;
    pghdr->pgdepth = -1; /* NOT USED */
    pghdr->pgsltsz = (uint16_t)slot_size;
    pghdr->pgdummy = 0; /* NOT USED */

    pghdr->used_nslots = 0;
#ifdef USE_SLOT_STATUS_BYTE_ARRAY
    pghdr->totl_nslots = page_gl.pgbody_size / (slot_size+1);
    if (1) { /* slot map (byte representation) */
        char *smap = pgptr->body + (pghdr->totl_nslots * pghdr->pgsltsz);
        for (int i = 0; i < pghdr->totl_nslots; i++) {
            smap[i] = page_gl.slot_CHAR_FREE;
        }
    }
    pghdr->totl_frsize = 0; /* NOT USED */
    pghdr->cont_offset = 0; /* NOT USED */
#else
    pghdr->totl_nslots = page_gl.pgbody_size / slot_size;
    if (1) {
        char *item;
        for (int i = 0; i < pghdr->totl_nslots; i++) {
            item = pgptr->body + (i * slot_size);
            if (i < (pghdr->totl_nslots-1)) {
                *(uint16_t*)item = sizeof(PGHDR) + ((i+1)*slot_size);
            } else {
                *(uint16_t*)item = 0;
            }
            *(item + pghdr->pgsltsz - 1) = page_gl.slot_CHAR_FREE;
        }
    }
    pghdr->totl_frsize = 0; /* NOT USED */
    pghdr->cont_offset = page_gl.pghead_size; /* the anchor of free slot list */
#endif
    pghdr->scat_frslot.offset = 0; /* NOT USED */
    pghdr->scat_frslot.length = 0; /* NOT USED */

    /* init reserved space */
    pghdr->rsvd32 = 0;
    pghdr->rsvd64 = 0;
}

void *opage_find_slot(Page *pgptr, uint32_t slotid)
{
    assert(slotid == 0); /* only single slot size is used */
    PGHDR *pghdr = &pgptr->header;

#ifdef USE_SLOT_STATUS_BYTE_ARRAY
    char *smap = pgptr->body + (pghdr->totl_nslots * pghdr->pgsltsz);
    if (smap[slotid] == page_gl.slot_CHAR_USED) { /* USED slot */
        return (void*)(pgptr->body + (slotid * pghdr->pgsltsz));
    } else {
        return NULL;
    }
#else
    /* The last character of each slot represents used or free state */
    char *item = pgptr->body + (slotid * pghdr->pgsltsz);
    if (*(item + pghdr->pgsltsz - 1) != page_gl.slot_CHAR_USED) { /* NOT USED slot */
        item = NULL;
    }
    return (void*)item;
#endif
}

void *opage_alloc_slot(Page *pgptr, uint32_t size, ItemID *itmid)
{
    PGHDR   *pghdr = &pgptr->header;
    char    *item;
    uint32_t slotid;

    assert(size == pghdr->pgsltsz);

    if (pghdr->used_nslots >= pghdr->totl_nslots) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "dpage alloc slot: no free slot\n");
        return NULL;
    }

#ifdef USE_SLOT_STATUS_BYTE_ARRAY
    char *smap = pgptr->body + (pghdr->totl_nslots * pghdr->pgsltsz);
    for (slotid = 0; slotid < pghdr->totl_nslots; slotid++) {
         if (smap[slotid] == page_gl.slot_CHAR_FREE) {
             smap[slotid] = page_gl.slot_CHAR_USED; /* set USED slot */
             break;
         }
    }
    assert(slotid < pghdr->totl_nslots);
    item = pgptr->body + (slotid * pghdr->pgsltsz);
#else
    /* cont_offset: the first free slot offset */
    assert(pghdr->cont_offset >= sizeof(PGHDR));
    slotid = (pghdr->cont_offset - sizeof(PGHDR)) / pghdr->pgsltsz;

    item = (char*)pgptr + pghdr->cont_offset;
    assert(*(item + pghdr->pgsltsz - 1) == page_gl.slot_CHAR_FREE);
    *(item + pghdr->pgsltsz - 1) = page_gl.slot_CHAR_USED; /* set USED slot */
    pghdr->cont_offset = *(uint16_t*)item; /* the next free slot */
#endif
    pghdr->used_nslots += 1;

    GET_ITEMID_FROM_PAGEID_SLOTID(&pghdr->self_pgid, &slotid, itmid);
    return item;
}


int opage_free_slot(Page *pgptr, uint32_t slotid)
{
    assert(slotid == 0); /* only single slot size is used */
    PGHDR *pghdr = &pgptr->header;

#ifdef USE_SLOT_STATUS_BYTE_ARRAY
    char *smap = pgptr->body + (pghdr->totl_nslots * pghdr->pgsltsz);
    if (smap[slotid] == page_gl.slot_CHAR_FREE) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "dpage alloc slot: already freed slot\n");
        return 0; /* already free slot */
    }
    /* set FREE slot */
    smap[slotid] = page_gl.slot_CHAR_FREE;
#else
    /* fixed length slot page */
    char *item = pgptr->body + (slotid * pghdr->pgsltsz);
    if (*(item + pghdr->pgsltsz - 1) == page_gl.slot_CHAR_FREE) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "dpage alloc slot: already freed slot\n");
        return 0; /* already freed slot */
    }
    *(item + pghdr->pgsltsz - 1) = page_gl.slot_CHAR_FREE;
    /* link it to the free slot list */
    *(uint16_t*)item = pghdr->cont_offset;
    pghdr->cont_offset = sizeof(PGHDR) + (slotid * pghdr->pgsltsz);
#endif
    pghdr->used_nslots -= 1;
    return 1;
}

bool opage_check_consistency(Page *pgptr)
{
    PGHDR *pghdr = &pgptr->header;
    uint16_t used_count, i;
    bool     consistent = true;

#ifdef USE_SLOT_STATUS_BYTE_ARRAY
    char *smap = pgptr->body + (pghdr->totl_nslots * pghdr->pgsltsz);
    used_count = 0;
    for (i = 0; i < pghdr->totl_nslots; i++) {
        if (smap[i] == page_gl.slot_CHAR_USED) {
            used_count += 1;
        }
    }
    if (pghdr->used_nslots != used_count) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: used slot count fail\n");
        consistent = false;
    }
#else
    used_count = 0;
    for (i = 0; i < pghdr->totl_nslots; i++) {
        if (*(pgptr->body + (i * pghdr->pgsltsz) + (pghdr->pgsltsz - 1)) == page_gl.slot_CHAR_USED) {
            used_count += 1;
        }
    }
    if (pghdr->used_nslots != used_count) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: used slot count fail\n");
        consistent = false;
    } else {
        uint16_t free_count = 0;
        uint16_t free_offset = pghdr->cont_offset;
        while (free_offset != 0) {
            if (free_offset < sizeof(PGHDR)) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: free slot offset fail\n");
                consistent = false; break;
            }
            if (*((char*)pgptr + free_offset + pghdr->pgsltsz - 1) != page_gl.slot_CHAR_FREE) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: free slot status fail\n");
                consistent = false; break;
            }
            free_count += 1;
            free_offset = *(uint16_t*)((char*)pgptr + free_offset);
        }
        if (consistent == true && free_count != (pghdr->totl_nslots - used_count)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "dpage consistency check: free slot count fail\n");
            consistent = false;
        }
    }
#endif
    return consistent;
}

/*************************/
/* Common Page Functions */
/*************************/

PageID page_get_pageid(Page *pgptr)
{
    return pgptr->header.self_pgid;
}

uint32_t page_get_total_used_size(Page *pgptr)
{
    /* variable length slot page only */
    return (page_gl.pgbody_size - pgptr->header.totl_frsize);
}

uint32_t page_get_total_free_size(Page *pgptr)
{
    /* variable length slot page only */
    return pgptr->header.totl_frsize;
}

uint32_t page_get_cntig_free_size(Page *pgptr)
{
    /* variable length slot page only */
    return (page_gl.page_size - pgptr->header.cont_offset
            - (pgptr->header.totl_nslots * sizeof(PGSLOT)));
}

uint32_t page_get_max_varslot_size(void)
{
    /* variable length slot page only */
    return (page_gl.pgbody_size - sizeof(PGSLOT));
}

uint32_t page_get_body_size(void)
{
    return page_gl.pgbody_size;
}

uint32_t page_get_total_slot_count(Page *pgptr)
{
    return pgptr->header.totl_nslots;
}

uint32_t page_get_used_slot_count(Page *pgptr)
{
    return pgptr->header.used_nslots;
}

int page_get_type(Page *pgptr)
{
    return pgptr->header.pgtype;
}

#if 1 // ENABLE_RECOVERY
void page_set_lsn(Page *pgptr, LogSN lsn)
{
    pgptr->header.pglsn = lsn;
}

LogSN page_get_lsn(Page *pgptr)
{
    return pgptr->header.pglsn;
}
#endif

PageID page_get_self_pageid(Page *pgptr)
{
    return pgptr->header.self_pgid;
}

void   page_set_self_pageid(Page *pgptr, PageID self_pgid)
{
    pgptr->header.self_pgid = self_pgid;
}

PageID page_get_next_pageid(Page *pgptr)
{
    return pgptr->header.next_pgid;
}

void   page_set_next_pageid(Page *pgptr, PageID next_pgid)
{
    pgptr->header.next_pgid = next_pgid;
}

PageID page_get_prev_pageid(Page *pgptr)
{
    return pgptr->header.prev_pgid;
}

void   page_set_prev_pageid(Page *pgptr, PageID prev_pgid)
{
    pgptr->header.prev_pgid = prev_pgid;
}

/*
 * Page Manager Init and Final
 */

void page_mgr_init(void *conf, void *srvr)
{
    config = (struct config *)conf;
    server = (SERVER_HANDLE_V1 *)srvr;
    logger = server->log->get_logger();

    page_gl.page_size  = config->sq.data_page_size;
    page_gl.pghead_size = sizeof(PGHDR);
    page_gl.pgbody_size = page_gl.page_size - page_gl.pghead_size;
    page_gl.pgslot_size = sizeof(PGSLOT);
    page_gl.slot_CHAR_USED = 'U';
    page_gl.slot_CHAR_FREE = 'F';
}

void page_mgr_final(void)
{
    /* do nothing */
    return;
}

