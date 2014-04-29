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

#include "squall_config.h"
#include "sq_buffer.h"
#include "sq_assoc.h"
#include "sq_prefix.h"
#include "sq_log.h"

/* Enable it in order to key & data consistency check */
//#define ENABLE_KEY_DATA_CONSISTENCY_CHECK 1

/* index scan structure */
typedef struct _indx_scan {
    prefix_desc *pfdesc;
    Page        *pgptr;
    int         slotid; /* current slotid */
    int         result; /* scan result : 0(success), -1(fail) */
} indx_scan;

/* index operation */
enum indx_op {
    INDX_SEARCH = 0,    /* search an index item */
    INDX_TOGGLE,        /* toggle the itemid of the found index item */
    INDX_INSERT,        /* insert an index item */
    INDX_DELETE         /* delete an index item */
};

typedef struct _hndx_item {
    union {
        Page   *pgptr;  /* The non-leaf page => lower non-leaf page address */
        PageID  pgid;   /* the lowest non-leaf page => leaf PageID */
        ItemID  itmid;  /* the leaf page => ItemID */
    } ptr;
    uint32_t    hval;   /* key hash value */
    uint8_t     nkey;   /* the length of sub-key string */
    char        key[1]; /* the sub-key string */
} hndx_item;

typedef struct _bndx_item {
    union {
        Page   *pgptr;  /* The non-leaf page => lower non-leaf page address */
        PageID  pgid;   /* the lowest non-leaf page => leaf PageID */
        ItemID  itmid;  /* the leaf page => ItemID */
    } ptr;
    uint8_t     nkey;   /* the length of sub-key string */
    char        key[1]; /* the sub-key string */
} bndx_item;

typedef struct _indx_item {
    union {
        Page   *pgptr;  /* The non-leaf page => lower non-leaf page address */
        PageID  pgid;   /* the lowest non-leaf page => leaf PageID */
        ItemID  itmid;  /* the leaf page => ItemID */
    } ptr;
    uint8_t     nkey;   /* the length of sub-key string */
    char        key[1]; /* the sub-key string */
} indx_item;

#define HNDX_ITEM_SIZE(nkey) GET_8_ALIGN_SIZE(sizeof(ItemID)+5+nkey)
#define BNDX_ITEM_SIZE(nkey) GET_8_ALIGN_SIZE(sizeof(ItemID)+1+nkey)
#define INDX_ITEM_SIZE(nkey) GET_8_ALIGN_SIZE(sizeof(ItemID)+1+nkey)

struct assoc_global {
    ASSOC_TYPE  assoc_type;
    uint32_t    page_size;
    uint32_t    page_body_size;
    uint32_t    npage_per_file;
    uint32_t    max_key_length;
    uint32_t    max_bndx_item_size;
    uint32_t    key_hbit_mask[32];
    uint32_t    key_hval_mask[32];
    uint8_t     set_used_mask[8];
    uint8_t     set_free_mask[8];
};

static struct config    *config = NULL;
static SERVER_HANDLE_V1 *server = NULL;
static EXTENSION_LOGGER_DESCRIPTOR *logger = NULL;
static struct assoc_global assoc_gl;

/*
 * Hash Index Management
 *
 *   use sparse file : some pages will be absent in the file.
 *   Each pgmap[pgsn] bit indicates the existence of the page.
 *
 *   hash value of depth 4
 *       0000  0001  0010  0011  0100  0101  0110  0111
 *       1000  1001  1010  1011  1100  1101  1110  1111
 */

static PageID
do_bhash_get_leaf_pgid(prefix_desc *pfdesc, const uint32_t khval)
{
    PageID  leaf_pgid;
    int     gbl_depth = pfdesc->d.hndx_global_depth;
    int     cur_depth;

    if (gbl_depth == 0) {
        leaf_pgid.pfxid = (SQUALL_FILEBITS_INDX | pfdesc->pfxid);
        leaf_pgid.flnum = 1;
        leaf_pgid.pgnum = 0;
    } else {
        PageSN leaf_pgsn;
        cur_depth = gbl_depth;
        while (cur_depth > 0) {
            leaf_pgsn = khval & assoc_gl.key_hval_mask[cur_depth];

            leaf_pgid.pfxid = (SQUALL_FILEBITS_INDX | pfdesc->pfxid);
            leaf_pgid.flnum = (leaf_pgsn / assoc_gl.npage_per_file) + 1;
            leaf_pgid.pgnum = (leaf_pgsn % assoc_gl.npage_per_file);

            if (pfx_hndx_page_is_used(pfdesc, leaf_pgid) == true) {
                break; /* found */
            }
            cur_depth--;
        }
        assert(cur_depth > 0);
    }
    return leaf_pgid;
}

static ENGINE_ERROR_CODE
do_bhash_fix_leaf(prefix_desc *pfdesc, const uint32_t khval,
                  enum indx_op xop, Page **returned_pgptr)
{
    Page       *leaf_pgptr;
    PageID      leaf_pgid;
    PageSN      leaf_pgsn;
    int         cur_nretry = 0;
    int         max_nretry = 3;
    enum buf_latch_mode latch_mode;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    latch_mode = (xop == INDX_SEARCH ? BUF_MODE_READ : BUF_MODE_WRITE);

    while (1) {
        /* find the hash leaf pageid */
        leaf_pgid = do_bhash_get_leaf_pgid(pfdesc, khval);

        leaf_pgptr = buf_fix(leaf_pgid, latch_mode, false, false);
        if (leaf_pgptr == NULL) { /* SEVERE problem */
            ret = ENGINE_BUFF_FAILED; break;
        }

        /* 1. check if the page is a real existent page */
        /* The absent page of sparse file will be filled with zeros */
        if (PAGEID_IS_NULL(&leaf_pgptr->header.self_pgid) ||
            leaf_pgptr->header.pgdepth == -1) {
            /* An absent page in sparse file */
            buf_unfix(leaf_pgptr); leaf_pgptr = NULL;
            if ((++cur_nretry) > max_nretry) {
                ret = ENGINE_FAILED; break;
            }
            continue;
        }

        /* 2. check if it is the correct page with page depth */
        leaf_pgsn = khval & assoc_gl.key_hval_mask[leaf_pgptr->header.pgdepth];
        if (leaf_pgid.flnum != (leaf_pgsn / assoc_gl.npage_per_file + 1) ||
            leaf_pgid.pgnum != (leaf_pgsn % assoc_gl.npage_per_file)) {
            /* Incorrect page */
            buf_unfix(leaf_pgptr); leaf_pgptr = NULL;
            if ((++cur_nretry) > max_nretry) {
                ret = ENGINE_FAILED; break;
            }
            continue;
        }
        break;
    }

#if 0 // OLD_CODE
    pthread_mutex_lock(&pfdesc->indx_lock);
    /* find the hash leaf pageid */
    leaf_pgid = do_bhash_get_leaf_pgid(pfdesc, khval);
    assert(!PAGEID_IS_NULL(&leaf_pgid));
    while (1) {
        /* fix the leaf page with trymode is on(true) */
        leaf_pgptr = buf_fix(leaf_pgid, latch_mode, true, false); // TRY_MODE
        if (leaf_pgptr == NULL) {
            pthread_mutex_unlock(&pfdesc->indx_lock);
            /* fix the leaf page with trymode=off(false)
             * after releasing index lock
             */
            leaf_pgptr = buf_fix(leaf_pgid, latch_mode, false, false);
            if (leaf_pgptr == NULL) { /* SEVERE problem */
                ret = ENGINE_BUFF_FAILED; break;
            }
            pthread_mutex_lock(&pfdesc->indx_lock);

            /* recheck the validity of the fixed leaf page */
            PageID save_pgid = leaf_pgid;
            leaf_pgid = do_bhash_get_leaf_pgid(pfdesc, khval);
            assert(!PAGEID_IS_NULL(&leaf_pgid));
            if (leaf_pgid != save_pgid) {
                buf_unfix(leaf_pgptr);
                if ((++cur_nretry) <= max_nretry) continue;

                /* fix the leaf page with trymode=off(false)
                 * while holding index lock
                 */
                leaf_pgptr = buf_fix(leaf_pgid, latch_mode, false, false);
                /* if leaf_pgptr == NULL, SEVERE problem */
            }
        }
        break;
    }
    pthread_mutex_unlock(&pfdesc->indx_lock);
#endif

    if (ret == ENGINE_SUCCESS) {
        *returned_pgptr = leaf_pgptr;
    }
    return ret;
}


static inline int
do_bhash_key_comp(const uint32_t hval1, const char *key1, const size_t nkey1,
                  const uint32_t hval2, const char *key2, const size_t nkey2)
{
    int comp;

    if (hval1 != hval2) {
        if (hval1 < hval2) return -1;
        else               return 1;
    }

    if (nkey1 == nkey2) {
        if (nkey1 == 0) comp = 0;
        else            comp = memcmp(key1, key2, nkey1);
    } else {
        if (nkey1 < nkey2) {
            if (nkey1 == 0) comp = -1;
            else {
                comp = memcmp(key1, key2, nkey1);
                if (comp == 0) comp = -1;
            }
        } else {
            if (nkey2 == 0) comp = 1;
            else {
                comp = memcmp(key1, key2, nkey2);
                if (comp == 0) comp = 1;
            }
        }
    }
    return comp;
}

static hndx_item *
do_bhash_search_in_leaf(Page *pgptr, const uint32_t hval,
                        const char *key, const size_t nkey, uint32_t *slotid)
{
    hndx_item *xit;  /* index item */
    int16_t    mid, left, right, comp;

    /*** Assume the placement of index items in leaf pages *****
      <key1, itm1>, <key2, itm2>, .... <keyn, itmn>
    *************************************************************/
    left  = 0;
    right = pgptr->header.used_nslots-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        xit  = xpage_find_slot(pgptr, mid);
        comp = do_bhash_key_comp(hval, key, nkey, xit->hval, xit->key, xit->nkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* found the index item */
        *slotid = (uint32_t)mid;
        return xit;
    } else { /* not found, left indicates the next larger key */
        *slotid = (uint32_t)left;
        return NULL;
    }
}

static ENGINE_ERROR_CODE
do_bhash_find_item(Page *pgptr, const uint32_t hval,
                   const char *key, const size_t nkey, ItemID *itmid)
{
    hndx_item  *indxit;  /* index item */
    uint32_t    slotid;

    indxit = do_bhash_search_in_leaf(pgptr, hval, key, nkey, &slotid);
    if (indxit == NULL) { /* not found */
        ITEMID_SET_NULL(itmid);
        return ENGINE_KEY_ENOENT;
    } else { /* found */
        *itmid = indxit->ptr.itmid;
        return ENGINE_SUCCESS;
    }
}

static ENGINE_ERROR_CODE
do_assoc_bhash_find(int pfxid, const uint32_t hval,
                    const char *key, const size_t nkey, ItemID *itmid)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    if (pfdesc->d.hndx_global_depth == -1) { /* empty hash index */
        return ENGINE_KEY_ENOENT;
    }

    ret = do_bhash_fix_leaf(pfdesc, hval, INDX_SEARCH, &pgptr);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "assoc bhash find - fix leaf fail\n");
        return ret; /* ENGINE_BUFF_FAILED | ENGINE_FAILED */
    }
    assert(pgptr != NULL); /* leaf page pointer */

    /* subkey string information */
    subkey  = (char*)key + pfdesc->tlen;
    nsubkey = nkey       - pfdesc->tlen;

    /* find the index item */
    ret = do_bhash_find_item(pgptr, hval, subkey, nsubkey, itmid);
    buf_unfix(pgptr);
    /* The access on the key were serialized through key lock before time.
     * So. the found itemid can be preserved even if we unfix the leaf page early.
     */

    return ret; /* ENGINE_SUCCESS | ENGINE_KEY_ENOENT */
}

static ENGINE_ERROR_CODE
do_bhash_root_build(prefix_desc *pfdesc, Page **root_pgptr)
{
    PageID new_pgid;
    Page  *new_pgptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    /* How do we serialize building a root page ? */

    new_pgid.pfxid = (SQUALL_FILEBITS_INDX | pfdesc->pfxid);
    new_pgid.flnum = 1;
    new_pgid.pgnum = 0;

    pthread_mutex_lock(&pfdesc->root_lock);
    do {
        if (pfdesc->d.hndx_global_depth >= 0) { /* not empty hash index */
            /* The root page was built by someone.
             * So, try to fix the root page in the caller function.
             */
            *root_pgptr = NULL;
            break;
        }

        /* set used in the bit of the page */
        ret = pfx_hndx_page_set_used(pfdesc, new_pgid);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "bhash root build - page set used fail\n");
            break;
        }

        /* fix the new page with newpg = true */
        new_pgptr = buf_fix(new_pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
        assert(new_pgptr != NULL);

        /* initialize the indx page */
        xpage_init(new_pgptr, new_pgid, 0);

        /* set buffer page dirty */
        buf_set_dirty(new_pgptr);

        /* adjust hash index depth info */
        pfdesc->d.hndx_global_depth = 0;
        pfdesc->d.hndx_npage_per_depth[0] = 1;

        /* return the fixed root page */
        *root_pgptr = new_pgptr;
    } while(0);
    pthread_mutex_unlock(&pfdesc->root_lock);

    return ret;
}

static void
do_bhash_slot_move_right(Page *spl_pgptr, Page *new_pgptr)
{
    PGHDR   *spl_pghdr = &spl_pgptr->header;
    PGHDR   *new_pghdr = &new_pgptr->header;
    PGSLOT  *src_slot;  /* source slot */
    PGSLOT  *dst_slot;  /* destination slot */
    hndx_item *indxit;

    /* move some slots into the new page from the split page */
    src_slot = (PGSLOT*)((char*)spl_pgptr + assoc_gl.page_size - sizeof(PGSLOT));
    dst_slot = (PGSLOT*)((char*)new_pgptr + assoc_gl.page_size - sizeof(PGSLOT));

    for (int i = 0; i < spl_pghdr->used_nslots; i++, src_slot--) {
        indxit = (hndx_item*)((char*)spl_pgptr + src_slot->offset);
        if ((indxit->hval & assoc_gl.key_hbit_mask[spl_pghdr->pgdepth]) != 0) {
            /* make new slot */
            dst_slot->offset = new_pghdr->cont_offset;
            dst_slot->length = src_slot->length;
            memcpy((char*)new_pgptr + dst_slot->offset, (void*)indxit, dst_slot->length);
            dst_slot--;

            new_pghdr->cont_offset += dst_slot->length;
            new_pghdr->used_nslots += 1;

            /* mark the source slot as moved slot */
            src_slot->offset = 0;
        }
    }

    if (new_pghdr->used_nslots > 0) { /* some slots are moved into new page */
        char buffer[assoc_gl.page_size];
        uint16_t buf_offset = sizeof(PGHDR);

        src_slot = (PGSLOT*)((char*)spl_pgptr + assoc_gl.page_size - sizeof(PGSLOT));
        dst_slot = src_slot;
        for (int i = 0; i < spl_pghdr->used_nslots; i++, src_slot--) {
            if (src_slot->offset == 0) continue;

            memcpy((char*)buffer + buf_offset, (char*)spl_pgptr + src_slot->offset,
                   src_slot->length);
            dst_slot->offset = buf_offset;
            if (dst_slot != src_slot) {
                dst_slot->length = src_slot->length;
                dst_slot--;
            }
            buf_offset += src_slot->length;
        }
        memcpy((char*)spl_pgptr+sizeof(PGHDR), buffer+sizeof(PGHDR),
               buf_offset-sizeof(PGHDR));

        spl_pghdr->used_nslots -= new_pghdr->used_nslots;
        spl_pghdr->cont_offset = buf_offset;
        spl_pghdr->totl_frsize = assoc_gl.page_size - spl_pghdr->cont_offset
                               - (spl_pghdr->used_nslots * sizeof(PGSLOT));
    }
}

static ENGINE_ERROR_CODE
do_bhash_page_split(prefix_desc *pfdesc, Page **pgptr, const uint32_t ins_khval)
{
    Page    *spl_pgptr = *pgptr;
    PGHDR   *spl_pghdr = &spl_pgptr->header;
    Page    *new_pgptr;
    PGHDR   *new_pghdr;
    PageID   spl_pgid, new_pgid; /* page ID */
    PageSN   spl_pgsn, new_pgsn; /* page SN(sequence number) */

    /* split page ID and SN */
    spl_pgid = spl_pghdr->self_pgid;
    spl_pgsn = ((spl_pgid.flnum-1) * assoc_gl.npage_per_file) + spl_pgid.pgnum;

    /* new page SN and ID */
    new_pgsn = spl_pgsn | assoc_gl.key_hbit_mask[spl_pghdr->pgdepth+1];
    new_pgid.pfxid = spl_pghdr->self_pgid.pfxid;
    new_pgid.flnum = (new_pgsn / assoc_gl.npage_per_file) + 1;
    new_pgid.pgnum = (new_pgsn % assoc_gl.npage_per_file);

    /* set used in the bit of the page */
    ENGINE_ERROR_CODE ret = pfx_hndx_page_set_used(pfdesc, new_pgid);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "bhash page split - page set used fail\n");
        return ret;
    }

    /* fix the new page with newpg = true */
    new_pgptr = buf_fix(new_pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
    assert(new_pgptr != NULL);
    new_pghdr = &new_pgptr->header;
    //assert(PAGEID_IS_NULL(&new_pghdr->self_pgid));

    /* move slots based on the hash values of keys */
    spl_pghdr->pgdepth += 1;
    xpage_init(new_pgptr, new_pgid, spl_pghdr->pgdepth);
    do_bhash_slot_move_right(spl_pgptr, new_pgptr);

    /* set buffer page dirty */
    buf_set_dirty(spl_pgptr);
    buf_set_dirty(new_pgptr);

    /* adjust hash index depth info */
    pthread_mutex_lock(&pfdesc->root_lock);
    if (spl_pghdr->pgdepth > pfdesc->d.hndx_global_depth) {
        assert(spl_pghdr->pgdepth == (pfdesc->d.hndx_global_depth+1));
        pfdesc->d.hndx_global_depth += 1;
    }
    assert(spl_pghdr->pgdepth > 0);
    pfdesc->d.hndx_npage_per_depth[spl_pghdr->pgdepth-1] -= 1;
    pfdesc->d.hndx_npage_per_depth[spl_pghdr->pgdepth  ] += 2;
    pthread_mutex_unlock(&pfdesc->root_lock);

    /* LOGGING */
    /* mini transaction : redo-only */
    //log_init_record(&logrec, type, ...);
    //log_record_write(&logrec);

    if ((ins_khval & assoc_gl.key_hbit_mask[spl_pghdr->pgdepth]) != 0) {
        /* insert into the new page */
        buf_unfix(spl_pgptr);
        *pgptr = new_pgptr;
    } else {
        /* insert into the old page */
        buf_unfix(new_pgptr);
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_bhash_insert_item(Page *pgptr, const uint32_t hval,
                     const char *key, const size_t nkey,
                     ItemID itmid, prefix_desc *pfdesc)
{
    hndx_item  *indxit;
    uint32_t    slotid;
    uint32_t    itemsz;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    indxit = do_bhash_search_in_leaf(pgptr, hval, key, nkey, &slotid);
    if (indxit != NULL) { /* found */
        return ENGINE_KEY_EEXISTS;
    }

    /* check the free space is enough */
    itemsz = HNDX_ITEM_SIZE(nkey);
    while (xpage_has_free_space(pgptr, itemsz) == false) {
        ret = do_bhash_page_split(pfdesc, &pgptr, hval);
        if (ret != ENGINE_SUCCESS) {
            break; /* SEVERE error */
        }
    }

    if (ret == ENGINE_SUCCESS) {
        indxit = (hndx_item*)xpage_alloc_slot(pgptr, slotid, itemsz);
        if (indxit == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "bhash insert - slot alloc fail\n");
            return ENGINE_PAGE_FAILED;
        }

        indxit->ptr.itmid = itmid;
        indxit->hval = hval;
        indxit->nkey = nkey;
        memcpy(indxit->key, key, nkey);

        /* set buffer page dirty */
        buf_set_dirty(pgptr);

        /* LOGGING */
        //log_record_write();
    }
    return ret;
}

static ENGINE_ERROR_CODE
do_assoc_bhash_insert(PrefixID pfxid, ItemDesc *itdesc)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr = NULL;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    if (pfdesc->d.hndx_global_depth == -1) { /* empty hash index */
        ret = do_bhash_root_build(pfdesc, &pgptr); /* build root page */
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "assoc bhash insert - root build fail\n");
            return ret; /* ?? */
        }
        /* pgptr might be NULL. */
    }
    if (pgptr == NULL) { /* root page exists */
        ret = do_bhash_fix_leaf(pfdesc, itdesc->hsh, INDX_INSERT, &pgptr);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "assoc bhash insert - fix leaf fail\n");
            return ret; /* ENGINE_BUFF_FAILED | ENGINE_FAILED */
        }
        assert(pgptr != NULL); /* leaf page pointer */
    }

    /* subkey string information */
    subkey  = item_get_key(itdesc->ptr) + pfdesc->tlen;
    nsubkey = itdesc->ptr->nkey         - pfdesc->tlen;

    /* insert the index item */
    ret = do_bhash_insert_item(pgptr, itdesc->hsh, subkey, nsubkey, itdesc->iid, pfdesc);
    buf_unfix(pgptr);

    if (ret != ENGINE_SUCCESS && ret != ENGINE_KEY_EEXISTS) {
        /* ENGINE_PAGE_FAILED */
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc bhash insert - insert item fail\n");
    }
    return ret;
}

static void
do_bhash_slot_move_left(Page *mrg_pgptr, Page *oth_pgptr)
{
    PGHDR   *mrg_pghdr = &mrg_pgptr->header;
    PGHDR   *oth_pghdr = &oth_pgptr->header;
    PGSLOT  *src_slot;  /* source slot */
    PGSLOT  *dst_slot;  /* destination slot */
    hndx_item *indxit;

    if (oth_pghdr->used_nslots > 0) {
        /* move all slots of the other page into the merge page */
        src_slot = (PGSLOT*)((char*)oth_pgptr + assoc_gl.page_size - sizeof(PGSLOT));
        dst_slot = (PGSLOT*)((char*)mrg_pgptr + assoc_gl.page_size - sizeof(PGSLOT));

        mrg_pghdr->cont_offset = sizeof(PGHDR);
        for (int i = 0; i < oth_pghdr->used_nslots; i++, src_slot--) {
            indxit = (hndx_item*)((char*)oth_pgptr + src_slot->offset);

            dst_slot->offset = mrg_pghdr->cont_offset;
            dst_slot->length = src_slot->length;
            memcpy((char*)mrg_pgptr + dst_slot->offset, (void*)indxit, dst_slot->length);
            dst_slot--;

            mrg_pghdr->cont_offset += dst_slot->length;
            mrg_pghdr->used_nslots += 1;
        }
        mrg_pghdr->totl_frsize = assoc_gl.page_size - mrg_pghdr->cont_offset
                               - (mrg_pghdr->used_nslots * sizeof(PGSLOT));

        /* clear the space */
        xpage_clear_space(oth_pgptr);
    }
}

static int
do_bhash_page_merge(prefix_desc *pfdesc, Page **pgptr)
{
    Page    *mrg_pgptr = *pgptr;
    PGHDR   *mrg_pghdr = &mrg_pgptr->header; /* the empty page */
    Page    *oth_pgptr;
    PGHDR   *oth_pghdr;
    PageID   mrg_pgid, oth_pgid; /* page ID */
    PageSN   mrg_pgsn, oth_pgsn; /* page SN(sequence number) */
    int      ret;

    /* merge page ID and SN */
    mrg_pgid = mrg_pghdr->self_pgid;
    mrg_pgsn = ((mrg_pgid.flnum-1) * assoc_gl.npage_per_file) + mrg_pgid.pgnum;

    if (mrg_pgsn == 0) { // root page
        return 0; /* cannot merge */
    }

    assert(mrg_pgsn <= assoc_gl.key_hval_mask[mrg_pghdr->pgdepth]);
    if ((mrg_pgsn & assoc_gl.key_hbit_mask[mrg_pghdr->pgdepth]) != 0) {
        /* merge page : right page, other page : left page */
        /* other(left) page SN and ID */
        oth_pgsn = mrg_pgsn & assoc_gl.key_hval_mask[mrg_pghdr->pgdepth-1];
        oth_pgid.pfxid = mrg_pghdr->self_pgid.pfxid;
        oth_pgid.flnum = (oth_pgsn / assoc_gl.npage_per_file) + 1;
        oth_pgid.pgnum = (oth_pgsn % assoc_gl.npage_per_file);

        /* In order to avoid latch deadlock, following fix logic is needed */
        /* fix the left page with trymode = true */
        oth_pgptr = buf_fix(oth_pgid, BUF_MODE_WRITE, true, false); // TRY_MODE
        if (oth_pgptr == NULL) {
            int save_pgdepth = mrg_pghdr->pgdepth;

            /* refix the left page with trymode = false while unlatching merge page */
            buf_unlatch_only(mrg_pgptr);
            oth_pgptr = buf_fix(oth_pgid, BUF_MODE_WRITE, false, false);
            buf_relatch_only(mrg_pgptr, BUF_MODE_WRITE); /* relatch the empty page */
            if (oth_pgptr == NULL) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "bhash page merge - fix other page fail\n");
                return -1; /* SEVERE error */
            }
            /* check if the merge page is still the empty page */
            if (mrg_pghdr->used_nslots > 0 || mrg_pghdr->pgdepth != save_pgdepth) {
                /* someone insered keys on the page. so, do not merge pages */
                buf_unfix(oth_pgptr);
                return 0; /* cannot merge */
            }
        }
        oth_pghdr = &oth_pgptr->header;

        assert(oth_pghdr->pgdepth >= mrg_pghdr->pgdepth);
        if (oth_pghdr->pgdepth == mrg_pghdr->pgdepth) {
            /* set the merge page as free */
            mrg_pghdr->pgdepth = -1; /* free page */
            oth_pghdr->pgdepth -= 1;

            /* set buffer page dirty */
            buf_set_dirty(mrg_pgptr);
            buf_set_dirty(oth_pgptr);

            /* adjust hash index depth info */
            pthread_mutex_lock(&pfdesc->root_lock);
            assert(oth_pghdr->pgdepth < pfdesc->d.hndx_global_depth);
            pfdesc->d.hndx_npage_per_depth[oth_pghdr->pgdepth]   += 1;
            pfdesc->d.hndx_npage_per_depth[oth_pghdr->pgdepth+1] -= 2;
            if (pfdesc->d.hndx_npage_per_depth[pfdesc->d.hndx_global_depth] == 0) {
                pfdesc->d.hndx_global_depth -= 1;
            }
            pthread_mutex_unlock(&pfdesc->root_lock);

            /* set free in the bit of the merge page */
            pfx_hndx_page_set_free(pfdesc, mrg_pghdr->self_pgid);

            buf_unfix(mrg_pgptr);
            *pgptr = oth_pgptr;
            ret = 1; /* merged */
        } else {
            /* Cannot merge with right page */
            /* So, the page is still to be empty page */
            buf_unfix(oth_pgptr);
            ret = 0; /* cannot merge */
        }
    } else {
        /* merge page : left page, other page : right page */
        /* other(right) page SN and ID */
        oth_pgsn = mrg_pgsn | assoc_gl.key_hbit_mask[mrg_pghdr->pgdepth];
        oth_pgid.pfxid = mrg_pghdr->self_pgid.pfxid;
        oth_pgid.flnum = (oth_pgsn / assoc_gl.npage_per_file) + 1;
        oth_pgid.pgnum = (oth_pgsn % assoc_gl.npage_per_file);

        /* fix the other page */
        oth_pgptr = buf_fix(oth_pgid, BUF_MODE_WRITE, false, false);
        if (oth_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "bhash page merge - fix other page fail\n");
            return -1; /* SEVERE error */
        }
        oth_pghdr = &oth_pgptr->header;

        /* merge pages if possible */
        assert(mrg_pghdr->pgdepth <= oth_pghdr->pgdepth);
        if (mrg_pghdr->pgdepth == oth_pghdr->pgdepth) {
            /* get all slots from other(right) page */
            do_bhash_slot_move_left(mrg_pgptr, oth_pgptr);

            /* set the other page as free */
            mrg_pghdr->pgdepth -= 1;
            oth_pghdr->pgdepth = -1; /* free page */

            /* set buffer page dirty */
            buf_set_dirty(mrg_pgptr);
            buf_set_dirty(oth_pgptr);

            /* adjust hash index depth info */
            pthread_mutex_lock(&pfdesc->root_lock);
            assert(mrg_pghdr->pgdepth < pfdesc->d.hndx_global_depth);
            pfdesc->d.hndx_npage_per_depth[mrg_pghdr->pgdepth]   += 1;
            pfdesc->d.hndx_npage_per_depth[mrg_pghdr->pgdepth+1] -= 2;
            if (pfdesc->d.hndx_npage_per_depth[pfdesc->d.hndx_global_depth] == 0) {
                pfdesc->d.hndx_global_depth -= 1;
            }
            pthread_mutex_unlock(&pfdesc->root_lock);

            /* set free in the bit of the other page */
            pfx_hndx_page_set_free(pfdesc, oth_pghdr->self_pgid);

            ret = 1; /* merged */
        } else {
            /* Cannot merge with right page */
            /* So, the page is still to be empty page */
            ret = 0; /* cannot merge */
        }
        buf_unfix(oth_pgptr);
    }
    return ret;
}

static ENGINE_ERROR_CODE
do_bhash_delete_item(Page *pgptr, const uint32_t hval,
                     const char *key, const size_t nkey,
                     ItemID itmid, prefix_desc *pfdesc)
{
    hndx_item  *indxit;
    uint32_t    slotid;
    int         mrgcnt;

    indxit = do_bhash_search_in_leaf(pgptr, hval, key, nkey, &slotid);
    if (indxit == NULL) { /* not found */
        return ENGINE_KEY_ENOENT;
    }

    /* check if the itemid is the same itemid */
    if (!ITEMID_IS_EQ(&indxit->ptr.itmid, &itmid)) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "bhash delete - itemid mismatch\n");
        return ENGINE_INDX_FAILED;
    }
    /* delete the index item */
    if (xpage_free_slot(pgptr, slotid) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "bhash delete - free slot fail\n");
        return ENGINE_PAGE_FAILED;
    }

    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    /* LOGGIND */
    //log_record_write();

    /* remove the page if it is empty */
    mrgcnt = 0;
    while (xpage_is_empty(pgptr)) {
        mrgcnt = do_bhash_page_merge(pfdesc, &pgptr);
        if (mrgcnt <= 0) {
            break; /* cannot merge or error */
        }
    }
    if (mrgcnt < 0) { /* error case */
        logger->log(EXTENSION_LOG_WARNING, NULL, "bhash delete - page merge fail\n");
        return ENGINE_BUFF_FAILED;
    }

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_assoc_bhash_delete(PrefixID pfxid, ItemDesc *itdesc)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    if (pfdesc->d.hndx_global_depth == -1) { /* empty hash index */
        return ENGINE_KEY_ENOENT;
    }

    ret = do_bhash_fix_leaf(pfdesc, itdesc->hsh, INDX_DELETE, &pgptr);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc bhash delete - fix leaf fail\n");
        return ret; /* ENGINE_BUFF_FAILED | ENGINE_FAILED */
    }
    assert(pgptr != NULL); /* leaf page pointer */

    /* subkey string information */
    subkey  = item_get_key(itdesc->ptr) + pfdesc->tlen;
    nsubkey = itdesc->ptr->nkey         - pfdesc->tlen;

    /* delete the index item */
    ret = do_bhash_delete_item(pgptr, itdesc->hsh, subkey, nsubkey, itdesc->iid, pfdesc);
    buf_unfix(pgptr);

    if (ret != ENGINE_SUCCESS && ret != ENGINE_KEY_ENOENT) {
        /* ENGINE_INDX_FAILED | ENGINE_PAGE_FIALED */
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc bhash delete - delete item fail\n");
    }
    return ret;
}

static ENGINE_ERROR_CODE
do_bhash_toggle_item(Page *pgptr, const uint32_t hval,
                     const char *key, const size_t nkey,
                     ItemID old_itmid, ItemID new_itmid)
{
    hndx_item  *indxit;
    uint32_t    slotid;

    indxit = do_bhash_search_in_leaf(pgptr, hval, key, nkey, &slotid);
    if (indxit == NULL) { /* not found */
        return ENGINE_KEY_ENOENT;
    }

    if (! ITEMID_IS_EQ(&indxit->ptr.itmid, &old_itmid)) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "bhash toggle - itemid mismatch\n");
        return ENGINE_INDX_FAILED;
    }

    indxit->ptr.itmid = new_itmid;

    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    /* LOGGING */
    //log_record_write();

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_assoc_bhash_toggle(PrefixID pfxid, ItemDesc *old_itdesc, ItemDesc *new_itdesc)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    if (pfdesc->d.hndx_global_depth == -1) { /* empty hash index */
        return ENGINE_KEY_ENOENT;
    }

    ret = do_bhash_fix_leaf(pfdesc, old_itdesc->hsh, INDX_TOGGLE, &pgptr);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc bhash toggle - fix leaf fail\n");
        return ret; /* ENGINE_BUFF_FAILED | ENGINE_FAILED */
    }
    assert(pgptr != NULL); /* leaf page pointer */

    /* subkey string information */
    subkey  = item_get_key(old_itdesc->ptr) + pfdesc->tlen;
    nsubkey = old_itdesc->ptr->nkey         - pfdesc->tlen;

    /* toggle the index item */
    ret = do_bhash_toggle_item(pgptr, old_itdesc->hsh, subkey, nsubkey,
                               old_itdesc->iid, new_itdesc->iid);
    buf_unfix(pgptr);

    if (ret != ENGINE_SUCCESS && ret != ENGINE_KEY_ENOENT) {
        /* ENGINE_INDX_FAILED */
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc bhash toggle - toggle item fail\n");
    }
    return ret;
}

/*
 * btree index implementation
 */

static inline int
do_btree_key_comp(const char *key1, const size_t nkey1,
                  const char *key2, const size_t nkey2)
{
    //assert(nkey1 > 0 && nkey2 > 0);
    int comp;

    if (nkey1 == nkey2) {
        return memcmp(key1, key2, nkey1);
    }
    if (nkey1 < nkey2) {
        comp = memcmp(key1, key2, nkey1);
        if (comp == 0) comp = -1;
    } else {
        comp = memcmp(key1, key2, nkey2);
        if (comp == 0) comp = 1;
    }
    return comp;
}

#if 0
static int
do_btree_page_alloc(prefix_desc *pfdesc, Page **alloc_pgptr)
{
    int   ret = 0;
    Page *pgptr;

    pthread_mutex_lock(&pfdesc->alloc_lock);
    do {
        /* allocate from the scattered free page */
        if (pfdesc->d.indx_sfree_pgcnt > 0) {
            pgptr = buf_fix(pfdesc->d.indx_sfree_head, BUF_MODE_WRITE, false, false);
            if (pgptr == NULL) {
                ret = -1; break; /* SEVERE problem */
            }
            pfdesc->d.indx_sfree_head = pgptr->header.next_pgid;
            PAGEID_SET_NULL(&pgptr->header.next_pgid);
            pfdesc->d.indx_sfree_pgcnt -= 1;
            break;
        }
        /* allocate from the contiguous free page */
        if (pfdesc->d.indx_cfree_pgcnt > 0) {
            pgptr = buf_fix(pfdesc->d.indx_cfree_next, BUF_MODE_WRITE, false, false);
            if (pgptr == NULL) {
                ret = -1; break; /* SEVERE problem */
            }
            pfdesc->d.indx_cfree_pgcnt -= 1;
            /* Even if indx_cfree_pgcnt is 0, increment indx_cfree_next. */
            PAGEID_INCREMENT(&pfdesc->d.indx_cfree_next, assoc_gl.npage_per_file);
            break;
        }
        /* allocate from the new page: check prefix disk quota */
        if ((pfdesc->d.indx_total_pgcnt + pfdesc->d.data_total_pgcnt) < pfdesc->d.disk_quota_pgcnt) {
            pgptr = buf_fix(pfdesc->d.indx_cfree_next, BUF_MODE_WRITE, false, true); // NEW_PAGE
            assert(pgptr != NULL);
            //assert(PAGEID_IS_NULL(&pgptr->header.self_pgid));

            if (xpage_init(pgptr, pfdesc->d.indx_cfree_next, depth) != 0) {
                buf_unfix(pgptr); pgptr = NULL;
                ret = -1; break; /* SEVERE problem */
            }
            pfdesc->d.indx_total_pgcnt += 1;
            PAGEID_INCREMENT(&pfdesc->d.indx_cfree_next, assoc_gl.npage_per_file);
            break;
        } else {
            pgptr = NULL;
        }
    } while(0);
    pthread_mutex_unlock(&pfdesc->alloc_lock);

    /* LOGGING */

    *alloc_pgptr = pgptr;
    return ret;
}

static int
do_btree_page_free(prefix_desc *pfdesc, Page *pgptr)
{
    int    ret = 0;
    PageID next_pgid = pgptr->header.self_pgid;
    PAGEID_INCREMENT(&next_pgid, assoc_gl.npage_per_file);

    pthread_mutex_lock(&pfdesc->alloc_lock);
    if (PAGEID_IS_EQ(&next_pgid, pfdesc->d.indx_cfree_next)) {
        pfdesc->d.indx_cfree_next = pgptr->header.self_pgid;
        pfdesc->d.indx_cfree_pgcnt += 1;
        if (pfdesc->d.indx_cfree_pgcnt > 1000) {
            pfdesc->d.indx_cfree_pgcnt -= 500;
            pfdesc->d.indx_total_pgcnt -= 500;
            /* truncate the index file */
        }
    } else {
        pgptr->header.next_pgid = pfdesc->d.indx_sfree_head;
        pfdesc->d.indx_sfree_head = pgptr->header.self_pgid;
        pfdesc->d.indx_sfree_pgcnt += 1;
    }
    pthread_mutex_unlock(&pfdesc->alloc_lock);

    /* LOGGING */

    buf_unfix(pgptr);

    return ret;
}
#endif

static ENGINE_ERROR_CODE
do_btree_root_split(prefix_desc *pfdesc, Page *spl_pgptr)
{
    PageID      lft_pgid;   /* left  pgid */
    PageID      rht_pgid;   /* right pgid */
    Page       *lft_pgptr;  /* left  pgptr */
    Page       *rht_pgptr;  /* right pgptr */
    bndx_item  *src_xit;
    bndx_item  *dst_xit;
    uint32_t    itemsz;
    int         page_depth;
    uint32_t    slot_count;
    uint32_t    half_count;
    ENGINE_ERROR_CODE ret;

    page_depth = spl_pgptr->header.pgdepth;
    slot_count = spl_pgptr->header.used_nslots;
    half_count = slot_count / 2;
    assert(half_count > 0);

    ret = pfx_bndx_page_alloc(pfdesc->pfxid, &lft_pgid);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree root split - alloc left page fail\n");
        return ret;
    }
    ret = pfx_bndx_page_alloc(pfdesc->pfxid, &rht_pgid);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree root split - alloc right page fail\n");
        pfx_bndx_page_free(pfdesc->pfxid, lft_pgid);
        return ret;
    }

    lft_pgptr = buf_fix(lft_pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
    assert(lft_pgptr != NULL);
    rht_pgptr = buf_fix(rht_pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
    assert(rht_pgptr != NULL);

    /* build the left child and right child pages */
    xpage_init(lft_pgptr, lft_pgid, page_depth);
    xpage_init(rht_pgptr, rht_pgid, page_depth);
    xpage_copy_slots(lft_pgptr, spl_pgptr, 0, half_count);
    xpage_copy_slots(rht_pgptr, spl_pgptr, half_count, (slot_count-half_count));
    if (page_depth == 0) {
        lft_pgptr->header.next_pgid = rht_pgid;
        rht_pgptr->header.prev_pgid = lft_pgid;
    }

    /* rebuild root page */
    /* 1) clear the root page space */
    xpage_clear_space(spl_pgptr);
    /* 2) make the first slot(separator) */
    /* The first key of the root page must be the most minimum key */
    itemsz = BNDX_ITEM_SIZE(0);
    dst_xit = (bndx_item*)xpage_alloc_slot(spl_pgptr, 0, itemsz);
    dst_xit->ptr.pgid = lft_pgid;
    dst_xit->nkey = 0;
    /* 3) make the second slot(separator) */
    src_xit = xpage_find_slot(rht_pgptr, 0);
    itemsz = BNDX_ITEM_SIZE(src_xit->nkey);
    dst_xit = (bndx_item*)xpage_alloc_slot(spl_pgptr, 1, itemsz);
    dst_xit->ptr.pgid = rht_pgid;
    dst_xit->nkey = src_xit->nkey;
    memcpy(dst_xit->key, src_xit->key, dst_xit->nkey);
    /* 4) increment page depth (including bndx_global_depth) */
    spl_pgptr->header.pgdepth = page_depth+1;
    pthread_mutex_lock(&pfdesc->root_lock);
    pfdesc->d.bndx_global_depth = page_depth+1;
    pthread_mutex_unlock(&pfdesc->root_lock);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* NTS(nested top action) : redo-only */
        BTRtSplLog rtspl_log;
        lrec_bt_rtsplit_init((LogRec*)&rtspl_log);
        rtspl_log.body.spl_pgid = spl_pgptr->header.self_pgid;
        rtspl_log.body.lft_pgid = lft_pgid;
        rtspl_log.body.rht_pgid = rht_pgid;
        rtspl_log.body.spl_dleng = spl_pgptr->header.cont_offset - sizeof(PGHDR);
        rtspl_log.body.spl_sleng = spl_pgptr->header.used_nslots * sizeof(PGSLOT);
        rtspl_log.body.lft_dleng = lft_pgptr->header.cont_offset - sizeof(PGHDR);
        rtspl_log.body.lft_sleng = lft_pgptr->header.used_nslots * sizeof(PGSLOT);
        rtspl_log.body.rht_dleng = rht_pgptr->header.cont_offset - sizeof(PGHDR);
        rtspl_log.body.rht_sleng = rht_pgptr->header.used_nslots * sizeof(PGSLOT);
        rtspl_log.body.spl_depth = spl_pgptr->header.pgdepth;
        rtspl_log.spl_pgptr = spl_pgptr;
        rtspl_log.lft_pgptr = lft_pgptr;
        rtspl_log.rht_pgptr = rht_pgptr;
        rtspl_log.header.length = offsetof(BTRtSplData, data)
                + rtspl_log.body.spl_dleng + GET_8_ALIGN_SIZE(rtspl_log.body.spl_sleng)
                + rtspl_log.body.lft_dleng + GET_8_ALIGN_SIZE(rtspl_log.body.lft_sleng)
                + rtspl_log.body.rht_dleng + GET_8_ALIGN_SIZE(rtspl_log.body.rht_sleng);

        log_record_write((LogRec*)&rtspl_log, NULL);
        /* update PageLSN */
        buf_set_lsn(spl_pgptr, rtspl_log.header.self_lsn);
        buf_set_lsn(lft_pgptr, rtspl_log.header.self_lsn);
        buf_set_lsn(rht_pgptr, rtspl_log.header.self_lsn);
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(lft_pgptr);
    buf_set_dirty(rht_pgptr);
    buf_set_dirty(spl_pgptr);

    /* unfix the left child and right child page */
    buf_unfix(lft_pgptr);
    buf_unfix(rht_pgptr);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_root_merge(prefix_desc *pfdesc, Page *mrg_pgptr)
{
    PageID     low_pgid;
    Page      *low_pgptr;  /* lower(child) pgptr */
    bndx_item *indxit;
    int        page_depth;
    uint32_t   slot_count;

    assert(mrg_pgptr->header.used_nslots == 1);

    indxit = xpage_find_slot(mrg_pgptr, 0);
    low_pgid = indxit->ptr.pgid;

    low_pgptr = buf_fix(low_pgid, BUF_MODE_WRITE, false, false);
    if (low_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree root merge - fix lower page fail\n");
        return ENGINE_BUFF_FAILED;
    }

    page_depth = mrg_pgptr->header.pgdepth;
    slot_count = low_pgptr->header.used_nslots;

    /* rebuild the root page with the lower page image */
    xpage_clear_space(mrg_pgptr);
    xpage_copy_slots(mrg_pgptr, low_pgptr, 0, slot_count);

    /* clear the space of lower page */
    xpage_clear_space(low_pgptr);

    /* free the lower page */
    //PAGEID_SET_NULL(&low_pgptr->header.self_pgid); /* mark free page */
    pfx_bndx_page_free(pfdesc->pfxid, low_pgid);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* NTS(nested top action) : redo-only */
        BTRtMrgLog rtmrg_log;
        lrec_bt_rtmerge_init((LogRec*)&rtmrg_log);
        rtmrg_log.body.mrg_pgid = mrg_pgptr->header.self_pgid;
        rtmrg_log.body.chd_pgid = low_pgid;
        rtmrg_log.body.mrg_dleng = mrg_pgptr->header.cont_offset - sizeof(PGHDR);
        rtmrg_log.body.mrg_sleng = mrg_pgptr->header.used_nslots * sizeof(PGSLOT);
        rtmrg_log.body.mrg_depth = mrg_pgptr->header.pgdepth;
        rtmrg_log.mrg_pgptr = (void*)mrg_pgptr;
        rtmrg_log.header.length = offsetof(BTRtMrgData, data)
                + rtmrg_log.body.mrg_dleng + GET_8_ALIGN_SIZE(rtmrg_log.body.mrg_sleng);

        log_record_write((LogRec*)&rtmrg_log, NULL);
        /* update PageLSN */
        buf_set_lsn(low_pgptr, rtmrg_log.header.self_lsn);
        buf_set_lsn(mrg_pgptr, rtmrg_log.header.self_lsn);
    }
#endif

    /* set buffer page dirty */
    buf_set_dirty(low_pgptr);
    buf_set_dirty(mrg_pgptr);

    buf_unfix(low_pgptr);

    /* decrement page depth of root page (including bndx_global_depth) */
    mrg_pgptr->header.pgdepth = page_depth-1;
    pthread_mutex_lock(&pfdesc->root_lock);
    pfdesc->d.bndx_global_depth = page_depth-1;
    pthread_mutex_unlock(&pfdesc->root_lock);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_page_split(prefix_desc *pfdesc, const char *key, const size_t nkey,
                    Page *upp_pgptr, uint32_t slotid, Page **low_pgptr)
{
    PageID      rht_pgid;   /* right pgid */
    Page       *rht_pgptr;  /* right pgptr */
    Page       *nxt_pgptr;  /* next  pgptr */
    Page       *spl_pgptr = *low_pgptr;
    bndx_item  *rht_xit;
    bndx_item  *upp_xit;
    uint32_t    itemsz;
    int         page_depth;
    uint32_t    slot_count;
    uint32_t    half_count;
    ENGINE_ERROR_CODE ret;

    if (spl_pgptr->header.pgdepth == 0 && !PAGEID_IS_NULL(&spl_pgptr->header.next_pgid)) {
        /* Need to fix the next page to make leaf page link */
        nxt_pgptr = buf_fix(spl_pgptr->header.next_pgid, BUF_MODE_WRITE, false, false);
        if (nxt_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree page split - fix next page fail\n");
            return ENGINE_BUFF_FAILED;
        }
    } else {
        /* No leaf page link */
        nxt_pgptr = NULL;
    }

    ret = pfx_bndx_page_alloc(pfdesc->pfxid, &rht_pgid);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree page split - alloc right page fail\n");
        if (nxt_pgptr != NULL) buf_unfix(nxt_pgptr);
        return ret;
    }

    rht_pgptr = buf_fix(rht_pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
    assert(rht_pgptr != NULL);

    page_depth = spl_pgptr->header.pgdepth;
    slot_count = spl_pgptr->header.used_nslots;
    half_count = slot_count / 2;
    if ((slot_count % 2) != 0) half_count += 1;
    assert(half_count < slot_count);

    /* build the right page */
    xpage_init(rht_pgptr, rht_pgid, page_depth);
    xpage_move_slots(rht_pgptr, spl_pgptr, half_count, (slot_count-half_count));

    /* update leaf page link if those pages are leafs */
    if (spl_pgptr->header.pgdepth == 0) { /* leaf page */
        rht_pgptr->header.prev_pgid = spl_pgptr->header.self_pgid;
        rht_pgptr->header.next_pgid = spl_pgptr->header.next_pgid;
        if (nxt_pgptr != NULL) {
            nxt_pgptr->header.prev_pgid = rht_pgid;
        }
        spl_pgptr->header.next_pgid = rht_pgid;
    }

    /* add a new separator in the upper page */
    rht_xit = xpage_find_slot(rht_pgptr, 0);
    itemsz = BNDX_ITEM_SIZE(rht_xit->nkey);

    upp_xit = xpage_alloc_slot(upp_pgptr, slotid+1, itemsz);
    memcpy((void*)upp_xit, (void*)rht_xit, itemsz);
    upp_xit->ptr.pgid = rht_pgid;

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        /* NTS(nested top action) : redo-only */
        BTPgSplLog pgspl_log;
        lrec_bt_pgsplit_init((LogRec*)&pgspl_log);
        pgspl_log.body.upp_pgid = upp_pgptr->header.self_pgid;
        pgspl_log.body.spl_pgid = spl_pgptr->header.self_pgid;
        pgspl_log.body.rht_pgid = rht_pgptr->header.self_pgid;
        if (nxt_pgptr == NULL) PAGEID_SET_NULL(&pgspl_log.body.nxt_pgid);
        else pgspl_log.body.nxt_pgid = nxt_pgptr->header.self_pgid;
        pgspl_log.body.rht_dleng = rht_pgptr->header.cont_offset - sizeof(PGHDR);
        pgspl_log.body.rht_sleng = rht_pgptr->header.used_nslots * sizeof(PGSLOT);
        pgspl_log.body.upp_slotid = slotid+1;
        pgspl_log.body.upp_itemsz = itemsz;
        pgspl_log.body.spl_depth = spl_pgptr->header.pgdepth;
        pgspl_log.rht_pgptr = (void*)rht_pgptr;
        pgspl_log.upp_itptr = (void*)upp_xit;
        pgspl_log.header.length = offsetof(BTPgSplData, data) + pgspl_log.body.upp_itemsz
                + pgspl_log.body.rht_dleng + GET_8_ALIGN_SIZE(pgspl_log.body.rht_sleng);

        log_record_write((LogRec*)&pgspl_log, NULL);
        /* update PageLSN */
        if (nxt_pgptr != NULL) buf_set_lsn(nxt_pgptr, pgspl_log.header.self_lsn);
        buf_set_lsn(rht_pgptr, pgspl_log.header.self_lsn);
        buf_set_lsn(spl_pgptr, pgspl_log.header.self_lsn);
        buf_set_lsn(upp_pgptr, pgspl_log.header.self_lsn);
    }
#endif

    if (nxt_pgptr != NULL) {
        /* set buffer page dirty */
        buf_set_dirty(nxt_pgptr);
        buf_unfix(nxt_pgptr);
    }

    /* set buffer page dirty */
    buf_set_dirty(rht_pgptr);
    buf_set_dirty(spl_pgptr);
    buf_set_dirty(upp_pgptr);

    if (do_btree_key_comp(key, nkey, rht_xit->key, rht_xit->nkey) < 0) {
        buf_unfix(rht_pgptr);
    } else {
        buf_unfix(spl_pgptr);
        *low_pgptr = rht_pgptr;
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_page_merge(prefix_desc *pfdesc, Page *upp_pgptr, uint32_t slotid, Page **low_pgptr)
{
    Page       *mrg_pgptr = *low_pgptr;
    Page       *oth_pgptr;
    PageID      mrg_pgid = mrg_pgptr->header.self_pgid;
    bndx_item  *upp_xit;
    bndx_item  *src_xit;
    bndx_item  *dst_xit;
    uint32_t    itemsz;
    uint32_t    slot_count;
    uint32_t    half_count;

    assert(mrg_pgptr->header.used_nslots == 1);
    assert(upp_pgptr->header.used_nslots >= 2);

    if (slotid < (upp_pgptr->header.used_nslots-1)) {
        /* merge with right(next) page */
        /* find the right page and fix it */
        upp_xit = xpage_find_slot(upp_pgptr, slotid+1);
        oth_pgptr = buf_fix(upp_xit->ptr.pgid, BUF_MODE_WRITE, false, false);
        if (oth_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree page merge - fix right page fail\n");
            return ENGINE_BUFF_FAILED;
        }

        /* [IMPORTANT] Non-leaf page must have enough free space to save one more separator */
        int size_if_merge = page_get_total_used_size(mrg_pgptr)
                          + page_get_total_used_size(oth_pgptr)
                          + (assoc_gl.max_bndx_item_size + sizeof(PGSLOT));
        if (size_if_merge > assoc_gl.page_body_size) {
            /* cannot merge pages */
            /* so, move front-positioned slots of right page into the merge page */
            /* save the last slot of mrg_pgptr */
            char itembuf[assoc_gl.max_bndx_item_size];
            src_xit = xpage_find_slot(mrg_pgptr, 0);
            itemsz = BNDX_ITEM_SIZE(src_xit->nkey);
            memcpy((void*)itembuf, (void*)src_xit, itemsz);

            slot_count = (int)oth_pgptr->header.used_nslots;
            half_count = slot_count / 2;
            assert(half_count > 0);

            /* build new merge page */
            xpage_clear_space(mrg_pgptr);
            dst_xit = xpage_alloc_slot(mrg_pgptr, 0, itemsz);
            memcpy((void*)dst_xit, (void*)itembuf, itemsz);
            xpage_move_slots(mrg_pgptr, oth_pgptr, 0, half_count);

            /* change the separator of the other page in the upper page */
            src_xit = xpage_find_slot(oth_pgptr, 0);
            itemsz = BNDX_ITEM_SIZE(src_xit->nkey);

            dst_xit = xpage_realloc_slot(upp_pgptr, slotid+1, itemsz);
            assert(dst_xit != NULL);
            memcpy((void*)dst_xit->key, (void*)src_xit->key, src_xit->nkey);
            dst_xit->nkey = src_xit->nkey;

#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                /* NTS(nested top action) : redo-only */
                BTPgMrgLog pgmrg_log;
                lrec_bt_pgmerge_init((LogRec*)&pgmrg_log);
                pgmrg_log.body.upp_pgid = upp_pgptr->header.self_pgid;
                pgmrg_log.body.mrg_pgid = mrg_pgptr->header.self_pgid;
                pgmrg_log.body.oth_pgid = oth_pgptr->header.self_pgid;
                PAGEID_SET_NULL(&pgmrg_log.body.opp_pgid);
                pgmrg_log.body.mrg_type = BT_PGMRG_RHT_REBAL;
                pgmrg_log.body.mrg_depth = mrg_pgptr->header.pgdepth;
                pgmrg_log.body.mrg_dleng = mrg_pgptr->header.cont_offset - sizeof(PGHDR);
                pgmrg_log.body.mrg_sleng = mrg_pgptr->header.used_nslots * sizeof(PGSLOT);
                pgmrg_log.body.oth_slotid = 0;
                pgmrg_log.body.upp_slotid = slotid+1;
                pgmrg_log.body.idx_itemsz = itemsz; /* upper page item */
                pgmrg_log.mrg_pgptr = (void*)mrg_pgptr;
                pgmrg_log.idx_itptr = (void*)dst_xit;
                pgmrg_log.header.length = offsetof(BTPgMrgData, data) + pgmrg_log.body.idx_itemsz
                                        + pgmrg_log.body.mrg_dleng + GET_8_ALIGN_SIZE(pgmrg_log.body.mrg_sleng);

                log_record_write((LogRec*)&pgmrg_log, NULL);
                /* update PageLSN */
                buf_set_lsn(mrg_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(oth_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(upp_pgptr, pgmrg_log.header.self_lsn);
            }
#endif
        } else {
            /* can merge pages */
            Page *lft_pgptr = NULL;
            if (mrg_pgptr->header.pgdepth == 0 && !PAGEID_IS_NULL(&mrg_pgptr->header.prev_pgid)) {
                /* fix the previous page to update leaf page chain */
                lft_pgptr = buf_fix(mrg_pgptr->header.prev_pgid, BUF_MODE_WRITE, true, false); // TRY_MODE
                if (lft_pgptr == NULL) {
                    buf_unlatch_only(mrg_pgptr);
                    buf_unlatch_only(oth_pgptr);
                    lft_pgptr = buf_fix(mrg_pgptr->header.prev_pgid, BUF_MODE_WRITE, false, false);
                    /* the relatch order is important in order to avoid latch deadlock */
                    buf_relatch_only(mrg_pgptr, BUF_MODE_WRITE);
                    buf_relatch_only(oth_pgptr, BUF_MODE_WRITE);
                    if (lft_pgptr == NULL) {
                        buf_unfix(oth_pgptr);
                        logger->log(EXTENSION_LOG_WARNING, NULL, "btree page merge - fix left leaf fail\n");
                        return ENGINE_BUFF_FAILED;
                    }
                }
            }

            /* move the slot into the right page */
            src_xit = xpage_find_slot(mrg_pgptr, 0);
            itemsz = BNDX_ITEM_SIZE(src_xit->nkey);
            dst_xit = xpage_alloc_slot(oth_pgptr, 0, itemsz);
            memcpy((void*)dst_xit, (void*)src_xit, itemsz);

            /* update leaf page chain if those pages are leafs */
            if (mrg_pgptr->header.pgdepth == 0) {
                oth_pgptr->header.prev_pgid = mrg_pgptr->header.prev_pgid;
                if (lft_pgptr != NULL) {
                    lft_pgptr->header.next_pgid = mrg_pgptr->header.next_pgid;
                }
            }

            /* remove the separator of empty page from the upper page */
            /* The two slots in upper page will be merged into
             * one slot of <min separator, pageid of other page>.
             */
            dst_xit = xpage_find_slot(upp_pgptr, slotid);
            assert(PAGEID_IS_EQ(&upp_xit->ptr.pgid, &oth_pgptr->header.self_pgid));
            dst_xit->ptr.pgid = oth_pgptr->header.self_pgid;
            xpage_free_slot(upp_pgptr, slotid+1);

            /* clear the space of merge page */
            xpage_clear_space(mrg_pgptr);

            /* free the merge page */
            //PAGEID_SET_NULL(&mrg_pgptr->header.self_pgid); /* mark free page */
            pfx_bndx_page_free(pfdesc->pfxid, mrg_pgid);

#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                /* NTS(nested top action) : redo-only */
                BTPgMrgLog pgmrg_log;
                lrec_bt_pgmerge_init((LogRec*)&pgmrg_log);
                pgmrg_log.body.upp_pgid = upp_pgptr->header.self_pgid;
                pgmrg_log.body.mrg_pgid = mrg_pgptr->header.self_pgid;
                pgmrg_log.body.oth_pgid = oth_pgptr->header.self_pgid;
                if (lft_pgptr == NULL) PAGEID_SET_NULL(&pgmrg_log.body.opp_pgid);
                else pgmrg_log.body.opp_pgid = lft_pgptr->header.self_pgid;
                pgmrg_log.body.mrg_type = BT_PGMRG_RHT_MERGE;
                pgmrg_log.body.mrg_depth = mrg_pgptr->header.pgdepth;
                pgmrg_log.body.mrg_dleng = 0;
                pgmrg_log.body.mrg_sleng = 0;
                pgmrg_log.body.oth_slotid = 0;
                pgmrg_log.body.upp_slotid = slotid;
                pgmrg_log.body.idx_itemsz = itemsz; /* right page item */
                pgmrg_log.mrg_pgptr = (void*)NULL;
                pgmrg_log.idx_itptr = (void*)dst_xit;
                pgmrg_log.header.length = offsetof(BTPgMrgData, data) + pgmrg_log.body.idx_itemsz;

                log_record_write((LogRec*)&pgmrg_log, NULL);
                /* update PageLSN */
                if (lft_pgptr != NULL) buf_set_lsn(lft_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(mrg_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(oth_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(upp_pgptr, pgmrg_log.header.self_lsn);
            }
#endif

            if (lft_pgptr != NULL) {
                /* set buffer page dirty */
                buf_set_dirty(lft_pgptr);
                buf_unfix(lft_pgptr);
            }
        }
    } else { /* the last slot */
        /* merge with left(previous) page */
        /* find the left page and fix it */
        /* Can do we fix the left page unconditionally ??
         * If the lower page is leaf page, then leaf page traversal is an occurrable task.
         * So, to avoid deadlock by page latch, fix the page conditionally(try mode).
         */
        upp_xit = xpage_find_slot(upp_pgptr, slotid-1);
        oth_pgptr = buf_fix(upp_xit->ptr.pgid, BUF_MODE_WRITE, true, false); // TRY_MODE
        if (oth_pgptr == NULL) {
            buf_unlatch_only(mrg_pgptr);
            oth_pgptr = buf_fix(upp_xit->ptr.pgid, BUF_MODE_WRITE, false, false);
            buf_relatch_only(mrg_pgptr, BUF_MODE_WRITE);
            if (oth_pgptr == NULL) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "btree page merge - fix left page fail\n");
                return ENGINE_BUFF_FAILED;
            }
        }

        /* [IMPORTANT] Non-leaf page must have enough free space to save one more separator */
        int size_if_merge = page_get_total_used_size(mrg_pgptr)
                          + page_get_total_used_size(oth_pgptr)
                          + (assoc_gl.max_bndx_item_size + sizeof(PGSLOT));
        if (size_if_merge > assoc_gl.page_body_size) {
            /* cannot merge pages */
            /* move back-positioned slots of other page into the merge page */
            /* save the last slot of mrg_pgptr */
            char itembuf[assoc_gl.max_bndx_item_size];
            src_xit = xpage_find_slot(mrg_pgptr, 0);
            itemsz = BNDX_ITEM_SIZE(src_xit->nkey);
            memcpy((void*)itembuf, (void*)src_xit, itemsz);

            slot_count = (int)oth_pgptr->header.used_nslots;
            half_count = slot_count / 2;
            if ((slot_count % 2) != 0) half_count += 1;
            assert(half_count < slot_count);

            /* build new merge page */
            xpage_clear_space(mrg_pgptr);
            xpage_move_slots(mrg_pgptr, oth_pgptr, half_count, (slot_count-half_count));
            dst_xit = xpage_alloc_slot(mrg_pgptr, (slot_count-half_count), itemsz);
            memcpy((void*)dst_xit, (void*)itembuf, itemsz);

            /* change the separator of merge page in the upper page */
            src_xit = xpage_find_slot(mrg_pgptr, 0);
            itemsz = BNDX_ITEM_SIZE(src_xit->nkey);

            dst_xit = xpage_realloc_slot(upp_pgptr, slotid, itemsz);
            assert(dst_xit != NULL);
            memcpy((void*)dst_xit->key, (void*)src_xit->key, src_xit->nkey);
            dst_xit->nkey = src_xit->nkey;

#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                /* NTS(nested top action) : redo-only */
                BTPgMrgLog pgmrg_log;
                lrec_bt_pgmerge_init((LogRec*)&pgmrg_log);
                pgmrg_log.body.upp_pgid = upp_pgptr->header.self_pgid;
                pgmrg_log.body.mrg_pgid = mrg_pgptr->header.self_pgid;
                pgmrg_log.body.oth_pgid = oth_pgptr->header.self_pgid;
                PAGEID_SET_NULL(&pgmrg_log.body.opp_pgid);
                pgmrg_log.body.mrg_type = BT_PGMRG_LFT_REBAL;
                pgmrg_log.body.mrg_depth = mrg_pgptr->header.pgdepth;
                pgmrg_log.body.mrg_dleng = mrg_pgptr->header.cont_offset - sizeof(PGHDR);
                pgmrg_log.body.mrg_sleng = mrg_pgptr->header.used_nslots * sizeof(PGSLOT);
                pgmrg_log.body.oth_slotid = oth_pgptr->header.used_nslots-1;
                pgmrg_log.body.upp_slotid = slotid;
                pgmrg_log.body.idx_itemsz = itemsz; /* upper page item */
                pgmrg_log.mrg_pgptr = (void*)mrg_pgptr;
                pgmrg_log.idx_itptr = (void*)dst_xit;
                pgmrg_log.header.length = offsetof(BTPgMrgData, data) + pgmrg_log.body.idx_itemsz
                                        + pgmrg_log.body.mrg_dleng + GET_8_ALIGN_SIZE(pgmrg_log.body.mrg_sleng);

                log_record_write((LogRec*)&pgmrg_log, NULL);
                /* update PageLSN */
                buf_set_lsn(mrg_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(oth_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(upp_pgptr, pgmrg_log.header.self_lsn);
            }
#endif
        } else {
            /* can merge pages */
            Page *rht_pgptr = NULL;
            if (mrg_pgptr->header.pgdepth == 0 && !PAGEID_IS_NULL(&mrg_pgptr->header.next_pgid)) {
                /* fix the right page to update leaf page chain */
                rht_pgptr = buf_fix(mrg_pgptr->header.next_pgid, BUF_MODE_WRITE, false, false);
                if (rht_pgptr == NULL) {
                    buf_unfix(oth_pgptr);
                    logger->log(EXTENSION_LOG_WARNING, NULL, "btree page merge - fix right leaf fail\n");
                    return ENGINE_BUFF_FAILED;
                }
            }

            /* move the slot info the left page */
            src_xit = xpage_find_slot(mrg_pgptr, 0);
            itemsz = BNDX_ITEM_SIZE(src_xit->nkey);
            dst_xit = xpage_alloc_slot(oth_pgptr, oth_pgptr->header.used_nslots, itemsz);
            memcpy((void*)dst_xit, (void*)src_xit, itemsz);

            /* update leaf page chain if those pages are leafs */
            if (mrg_pgptr->header.pgdepth == 0) { /* leaf page */
                oth_pgptr->header.next_pgid = mrg_pgptr->header.next_pgid;
                if (rht_pgptr != NULL) {
                    rht_pgptr->header.prev_pgid = mrg_pgptr->header.prev_pgid;
                }
            }

            /* remove separator of merged page from the upper page */
            xpage_free_slot(upp_pgptr, slotid);

            /* clear the space of merge page */
            xpage_clear_space(mrg_pgptr);

            /* free the merge page */
            //PAGEID_SET_NULL(&mrg_pgptr->header.self_pgid); /* mark free page */
            pfx_bndx_page_free(pfdesc->pfxid, mrg_pgid);

#ifdef ENABLE_RECOVERY
            if (config->sq.use_recovery) { /* TRANSACTION */
                /* NTS(nested top action) : redo-only */
                BTPgMrgLog pgmrg_log;
                lrec_bt_pgmerge_init((LogRec*)&pgmrg_log);
                pgmrg_log.body.upp_pgid = upp_pgptr->header.self_pgid;
                pgmrg_log.body.mrg_pgid = mrg_pgptr->header.self_pgid;
                pgmrg_log.body.oth_pgid = oth_pgptr->header.self_pgid;
                if (rht_pgptr == NULL) PAGEID_SET_NULL(&pgmrg_log.body.opp_pgid);
                else pgmrg_log.body.opp_pgid = rht_pgptr->header.self_pgid;
                pgmrg_log.body.mrg_type = BT_PGMRG_LFT_MERGE;
                pgmrg_log.body.mrg_depth = mrg_pgptr->header.pgdepth;
                pgmrg_log.body.mrg_dleng = 0;
                pgmrg_log.body.mrg_sleng = 0;
                pgmrg_log.body.oth_slotid = oth_pgptr->header.used_nslots-1;
                pgmrg_log.body.idx_itemsz = itemsz; /* left page item */
                pgmrg_log.body.upp_slotid = slotid;
                pgmrg_log.mrg_pgptr = (void*)NULL;
                pgmrg_log.idx_itptr = (void*)dst_xit;
                pgmrg_log.header.length = offsetof(BTPgMrgData, data) + pgmrg_log.body.idx_itemsz;

                log_record_write((LogRec*)&pgmrg_log, NULL);
                /* update PageLSN */
                if (rht_pgptr != NULL) buf_set_lsn(rht_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(mrg_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(oth_pgptr, pgmrg_log.header.self_lsn);
                buf_set_lsn(upp_pgptr, pgmrg_log.header.self_lsn);
            }
#endif

            if (rht_pgptr != NULL) {
                /* set buffer page dirty */
                buf_set_dirty(rht_pgptr);
                buf_unfix(rht_pgptr);
            }
        }
    }

    /* set buffer page dirty */
    buf_set_dirty(mrg_pgptr);
    buf_set_dirty(oth_pgptr);
    buf_set_dirty(upp_pgptr);

    if (xpage_is_empty(mrg_pgptr)) {
        buf_unfix(mrg_pgptr);
        *low_pgptr = oth_pgptr;
    } else {
        buf_unfix(oth_pgptr);
    }
    return ENGINE_SUCCESS;
}

static bndx_item *
do_btree_search_in_nonleaf(Page *pgptr, const char *key, const size_t nkey, uint32_t *slotid)
{
    assert(pgptr->header.used_nslots > 0); /* no empty nonleaf page */
    bndx_item *xit; /* index item */
    int16_t    mid, left, right, comp;

    if (pgptr->header.used_nslots == 1) {
        *slotid = 0;
        return xpage_find_slot(pgptr, 0);
    }

    /* pgptr->header.used_nslots > 1 */
    /*** Assume the placement of index items in non-leaf pages *****
      <NULL, pid1>, <sep2, pid2>, .... <sepn, pidn>
      so, the largest key of pid1 < sep2 <= the smallest key of pid2
    ****************************************************************/
    left  = 1;
    right = pgptr->header.used_nslots-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        xit  = xpage_find_slot(pgptr, mid);
        comp = do_btree_key_comp(key, nkey, xit->key, xit->nkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* found the index item: xit */
        *slotid = (uint32_t)mid;
        return xit;
    } else {
        assert(right >= 0);
        *slotid = (uint32_t)right;
        return xpage_find_slot(pgptr, right);
    }
}

static ENGINE_ERROR_CODE
do_btree_fix_leaf_4_search(prefix_desc *pfdesc, const char *key, const size_t nkey,
                           Page **returned_pgptr)
{
    Page       *curr_pgptr;
    Page       *next_pgptr;
    bndx_item  *indxit;
    uint32_t    slotid;

    assert(!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid));

    /* fix the root page */
    curr_pgptr = buf_fix(pfdesc->d.bndx_root_pgid, BUF_MODE_READ, false, false);
    if (curr_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for search - fix root fail\n");
        return ENGINE_BUFF_FAILED;
    }

    /* latch crabbing */
    while (curr_pgptr->header.pgdepth > 0) /* non-leaf page */
    {
        indxit = do_btree_search_in_nonleaf(curr_pgptr, key, nkey, &slotid);

        next_pgptr = buf_fix(indxit->ptr.pgid, BUF_MODE_READ, false, false);
        if (next_pgptr == NULL) {
            buf_unfix(curr_pgptr);
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for search - fix child fail\n");
            return ENGINE_BUFF_FAILED;
        }

        buf_unfix(curr_pgptr);
        curr_pgptr = next_pgptr;
    }

    *returned_pgptr = curr_pgptr;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_fix_leaf_4_toggle(prefix_desc *pfdesc, const char *key, const size_t nkey,
                           Page **returned_pgptr)
{
    Page       *curr_pgptr;
    Page       *next_pgptr;
    bndx_item  *indxit;
    uint32_t    slotid;
    enum buf_latch_mode latch_mode;

    assert(!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid));

    /* fix the root page */
    latch_mode = (pfdesc->d.bndx_global_depth > 0 ? BUF_MODE_READ : BUF_MODE_WRITE);
    curr_pgptr = buf_fix(pfdesc->d.bndx_root_pgid, latch_mode, false, false);
    if (curr_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for toggle - fix root fail\n");
        return ENGINE_BUFF_FAILED;
    }
    if (curr_pgptr->header.pgdepth == 0 && latch_mode == BUF_MODE_READ) {
        /* latch mismatch, since bndx_global_depth is referenced without lock */
        latch_mode = BUF_MODE_WRITE;
        buf_uplatch_only(curr_pgptr, latch_mode);
    }

    /* latch crabbing */
    while (curr_pgptr->header.pgdepth > 0) /* non-leaf page */
    {
        indxit = do_btree_search_in_nonleaf(curr_pgptr, key, nkey, &slotid);

        latch_mode = (curr_pgptr->header.pgdepth > 1 ? BUF_MODE_READ : BUF_MODE_WRITE);
        next_pgptr = buf_fix(indxit->ptr.pgid, latch_mode, false, false);
        if (next_pgptr == NULL) {
            buf_unfix(curr_pgptr);
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for toggle - fix child fail\n");
            return ENGINE_BUFF_FAILED;
        }

        buf_unfix(curr_pgptr);
        curr_pgptr = next_pgptr;
    }

    *returned_pgptr = curr_pgptr;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_fix_leaf_4_insert(prefix_desc *pfdesc, const char *key, const size_t nkey,
                           Page **returned_pgptr)
{
    Page       *prev_pgptr = NULL;
    Page       *curr_pgptr;
    Page       *next_pgptr;
    bndx_item  *indxit;
    uint32_t    itemsz;
    uint32_t    slotid;
    enum buf_latch_mode curr_latch_mode;
    enum buf_latch_mode next_latch_mode;
    ENGINE_ERROR_CODE ret;

    assert(!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid));

    /* [Assumption] root page is maintained until the end of prefix lifetime */

    /* fix the root page */
    curr_latch_mode = (pfdesc->d.bndx_global_depth > 0 ? BUF_MODE_READ : BUF_MODE_WRITE);
    curr_pgptr = buf_fix(pfdesc->d.bndx_root_pgid, curr_latch_mode, false, false);
    if (curr_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for insert - fix root fail\n");
        return ENGINE_BUFF_FAILED;
    }
    if (curr_pgptr->header.pgdepth == 0 && curr_latch_mode == BUF_MODE_READ) {
        /* latch mismatch, since bndx_global_depth is referenced without lock */
        curr_latch_mode = BUF_MODE_WRITE;
        buf_uplatch_only(curr_pgptr, curr_latch_mode);
    }
    /* [IMPORTANT] Save enough free space for page merge of data rebalance.
     * When a page merge is performed as data rebalance,
     * the new separator of the page might become bigger than existing one.
     * So, the upper page must have enough free space to have the new separator.
     * That is, nonleaf pages must have free space of (max item size * 2).
     */
    while (1) {
        itemsz = (curr_pgptr->header.pgdepth == 0 ? BNDX_ITEM_SIZE(nkey)
                                                  : assoc_gl.max_bndx_item_size*2);
        if (xpage_has_free_space(curr_pgptr, itemsz) == false) {
            if (curr_latch_mode == BUF_MODE_READ) {
                curr_latch_mode = BUF_MODE_WRITE;
                buf_uplatch_only(curr_pgptr, curr_latch_mode);
                continue; /* must recheck if root split is needed */
            }
            ret = do_btree_root_split(pfdesc, curr_pgptr);
            if (ret != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for insert - root split fail\n");
                buf_unfix(curr_pgptr);
                return ret;
            }
        }
        break;
    }

    /* latch crabbing */
    while (curr_pgptr->header.pgdepth > 0) /* non-leaf page */
    {
        indxit = do_btree_search_in_nonleaf(curr_pgptr, key, nkey, &slotid);

        next_latch_mode = (curr_pgptr->header.pgdepth > 1 ? BUF_MODE_READ : BUF_MODE_WRITE);
        next_pgptr = buf_fix(indxit->ptr.pgid, next_latch_mode, false, false);
        if (next_pgptr == NULL) {
            if (prev_pgptr != NULL) buf_unfix(prev_pgptr);
            buf_unfix(curr_pgptr);
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for insert - fix child fail\n");
            return ENGINE_BUFF_FAILED;
        }
        itemsz = (next_pgptr->header.pgdepth == 0 ? BNDX_ITEM_SIZE(nkey)
                                                  : assoc_gl.max_bndx_item_size*2);
        if (xpage_has_free_space(next_pgptr, itemsz) == false) {
            if (curr_latch_mode == BUF_MODE_READ) {
                buf_unfix(next_pgptr);
                curr_latch_mode = BUF_MODE_WRITE;
                buf_uplatch_only(curr_pgptr, curr_latch_mode);
                /* The current page image might be changed.
                 * so, btree search is needed again..
                 */
                continue;
            }
            bool need_split = true;
            if (next_latch_mode == BUF_MODE_READ) {
                next_latch_mode = BUF_MODE_WRITE;
                buf_uplatch_only(next_pgptr, next_latch_mode);
                /* recheck if split is needed */
                itemsz = (next_pgptr->header.pgdepth == 0 ? BNDX_ITEM_SIZE(nkey)
                                                          : assoc_gl.max_bndx_item_size*2);
                if (xpage_has_free_space(next_pgptr, itemsz) != false) {
                    need_split = false;
                }
            }
            if (need_split) {
                ret = do_btree_page_split(pfdesc, key, nkey, curr_pgptr, slotid, &next_pgptr);
                if (ret != ENGINE_SUCCESS) {
                    if (prev_pgptr != NULL) buf_unfix(prev_pgptr);
                    buf_unfix(curr_pgptr);
                    buf_unfix(next_pgptr);
                    logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for insert - page split fail\n");
                    return ret;
                }
            }
        }
        if (prev_pgptr != NULL) {
            buf_unfix(prev_pgptr);
        }
        if (curr_latch_mode == BUF_MODE_WRITE) {
            buf_dwlatch_only(curr_pgptr, BUF_MODE_READ); /* BUF_MODE_WRITE => BUF_MODE_READ */
        }
        prev_pgptr = curr_pgptr;
        curr_pgptr = next_pgptr;
        curr_latch_mode = next_latch_mode;
    }

    if (prev_pgptr != NULL) {
        buf_unfix(prev_pgptr);
    }
    *returned_pgptr = curr_pgptr;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_fix_leaf_4_delete(prefix_desc *pfdesc, const char *key, const size_t nkey,
                           Page **returned_pgptr)
{
    Page       *prev_pgptr = NULL;
    Page       *curr_pgptr;
    Page       *next_pgptr;
    bndx_item  *indxit;
    uint32_t    slotid;
    enum buf_latch_mode curr_latch_mode;
    enum buf_latch_mode next_latch_mode;
    ENGINE_ERROR_CODE ret;

    assert(!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid));

    /* [Assumption] root page is maintained until the end of prefix lifetime */

    /* fix the root page */
    curr_latch_mode = (pfdesc->d.bndx_global_depth > 0 ? BUF_MODE_READ : BUF_MODE_WRITE);
    curr_pgptr = buf_fix(pfdesc->d.bndx_root_pgid, curr_latch_mode, false, false);
    if (curr_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for delete - fix root fail\n");
        return ENGINE_BUFF_FAILED;
    }
    if (curr_pgptr->header.pgdepth == 0 && curr_latch_mode == BUF_MODE_READ) {
        /* latch mismatch, since bndx_global_depth is referenced without lock */
        curr_latch_mode = BUF_MODE_WRITE;
        buf_uplatch_only(curr_pgptr, curr_latch_mode);
    }
    while (1) {
        if (curr_pgptr->header.used_nslots == 1 && curr_pgptr->header.pgdepth > 0) {
            if (curr_latch_mode == BUF_MODE_READ) {
                curr_latch_mode = BUF_MODE_WRITE;
                buf_uplatch_only(curr_pgptr, curr_latch_mode);
                continue; /* must recheck if root merge should be performed */
            }
            ret = do_btree_root_merge(pfdesc, curr_pgptr);
            if (ret != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for delete - root merge fail\n");
                buf_unfix(curr_pgptr);
                return ret;
            }
        }
        break;
    }

    /* latch crabbing */
    while (curr_pgptr->header.pgdepth > 0) /* non-leaf page */
    {
        indxit = do_btree_search_in_nonleaf(curr_pgptr, key, nkey, &slotid);

        next_latch_mode = (curr_pgptr->header.pgdepth > 1 ? BUF_MODE_READ : BUF_MODE_WRITE);
        next_pgptr = buf_fix(indxit->ptr.pgid, next_latch_mode, false, false);
        if (next_pgptr == NULL) {
            if (prev_pgptr != NULL) buf_unfix(prev_pgptr);
            buf_unfix(curr_pgptr);
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for delete - fix child fail\n");
            return ENGINE_BUFF_FAILED;
        }
        if (next_pgptr->header.used_nslots == 1) {
            if (curr_latch_mode == BUF_MODE_READ) {
                buf_unfix(next_pgptr);
                curr_latch_mode = BUF_MODE_WRITE;
                buf_uplatch_only(curr_pgptr, curr_latch_mode);
                /* The current page image might be changed.
                 * so, btree search is needed again..
                 */
                continue;
            }
            bool need_merge = true;
            if (next_latch_mode == BUF_MODE_READ) {
                next_latch_mode = BUF_MODE_WRITE;
                buf_uplatch_only(next_pgptr, next_latch_mode);
                if (next_pgptr->header.used_nslots > 1) {
                    need_merge = false;
                }
            }
            if (need_merge) {
                ret = do_btree_page_merge(pfdesc, curr_pgptr, slotid, &next_pgptr);
                if (ret != ENGINE_SUCCESS) {
                    if (prev_pgptr != NULL) buf_unfix(prev_pgptr);
                    buf_unfix(curr_pgptr);
                    buf_unfix(next_pgptr);
                    logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf for delete - page merge fail\n");
                    return ret;
                }
            }
        }
        if (prev_pgptr != NULL) {
            buf_unfix(prev_pgptr);
        }
        if (curr_latch_mode == BUF_MODE_WRITE) {
            buf_dwlatch_only(curr_pgptr, BUF_MODE_READ); /* BUF_MODE_WRITE => BUF_MODE_READ */
        }
        prev_pgptr = curr_pgptr;
        curr_pgptr = next_pgptr;
        curr_latch_mode = next_latch_mode;
    }

    if (prev_pgptr != NULL) {
        buf_unfix(prev_pgptr);
    }
    *returned_pgptr = curr_pgptr;
    return ENGINE_SUCCESS;
}

#if 0 // OLD_CODE
static ENGINE_ERROR_CODE
do_btree_fix_leaf(prefix_desc *pfdesc, const char *key, const size_t nkey,
                  enum indx_op xop, Page **returned_pgptr)
{
    Page       *curr_pgptr;
    Page       *next_pgptr;
    bndx_item  *indxit;
    uint32_t    itemsz;
    uint32_t    slotid;
    enum buf_latch_mode latch_mode;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    assert(!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid));

    /* decide the latch mode to hold on the root page */
    if (xop == INDX_SEARCH)
        latch_mode = BUF_MODE_READ;
    else if (xop == INDX_INSERT || xop == INDX_DELETE)
        latch_mode = BUF_MODE_WRITE;
    else /* xop == INDX_TOGGLE */
        latch_mode = (pfdesc->d.bndx_global_depth > 0 ? BUF_MODE_READ : BUF_MODE_WRITE);

    /* fix the root page */
again:
    curr_pgptr = buf_fix(pfdesc->d.bndx_root_pgid, latch_mode, false, false);
    if (curr_pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf - fix root fail\n");
        return ENGINE_BUFF_FAILED;
    }
    if (xop == INDX_TOGGLE && latch_mode == BUF_MODE_READ && curr_pgptr->header.pgdepth == 0) {
        /* latch mismatch is possible, since bndx_global_depth is referenced without lock */
        buf_unfix(curr_pgptr);
        latch_mode = BUF_MODE_WRITE;
        goto again;
    }

    if (xop == INDX_INSERT) { /* check root split */
        if (curr_pgptr->header.pgdepth == 0)
            itemsz = BNDX_ITEM_SIZE(nkey);
        else {
            itemsz = assoc_gl.max_bndx_item_size * 2;
            /* [IMPORTANT] Save enough free space for page merge of data rebalance.
             * In case of page merge that is performed as a form of data rebalance,
             * the separator of the page is to be bigger than existing one.
             * So, the upper page must have enough free space to contain the new separator.
             */
        }
        if (xpage_has_free_space(curr_pgptr, itemsz) == false) {
            ret = do_btree_root_split(pfdesc, curr_pgptr);
            if (ret != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf - root split fail\n");
                buf_unfix(curr_pgptr);
                return ret;
            }
        }
    } else if (xop == INDX_DELETE) { /* check root merge */
        if (curr_pgptr->header.used_nslots == 1 && curr_pgptr->header.pgdepth > 0) {
            ret = do_btree_root_merge(pfdesc, curr_pgptr);
            if (ret != ENGINE_SUCCESS) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf - root merge fail\n");
                buf_unfix(curr_pgptr);
                return ret;
            }
        }
    }

    /* latch crabbing */
    while (curr_pgptr->header.pgdepth > 0) /* non-leaf page */
    {
        indxit = do_btree_search_in_nonleaf(curr_pgptr, key, nkey, &slotid);

        if (curr_pgptr->header.pgdepth == 1 && xop == INDX_TOGGLE) {
            latch_mode = BUF_MODE_WRITE; /* leaf latch mode of toggle operation */
        }
        next_pgptr = buf_fix(indxit->ptr.pgid, latch_mode, false, false);
        if (next_pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf - fix child fail\n");
            ret = ENGINE_BUFF_FAILED; break;
        }

        if (xop == INDX_INSERT) { /* check page split */
            if (next_pgptr->header.pgdepth == 0)
                itemsz = BNDX_ITEM_SIZE(nkey);
            else {
                itemsz = assoc_gl.max_bndx_item_size * 2;
                /* [IMPORTANT] Save enough free space for page merge of data rebalance.
                 * In case of page merge that is performed as a form of data rebalance,
                 * the separator of the page is to be bigger than existing one.
                 * So, the upper page must have enough free space to contain the new separator.
                 */
            }
            if (xpage_has_free_space(next_pgptr, itemsz) == false) {
                ret = do_btree_page_split(pfdesc, key, nkey, curr_pgptr, slotid, &next_pgptr);
                if (ret != ENGINE_SUCCESS) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf - page split fail\n");
                    buf_unfix(next_pgptr); break;
                }
            }
        } else if (xop == INDX_DELETE) { /* check page merge */
            if (next_pgptr->header.used_nslots == 1) {
                ret = do_btree_page_merge(pfdesc, curr_pgptr, slotid, &next_pgptr);
                if (ret != ENGINE_SUCCESS) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "btree fix leaf - page merge fail\n");
                    buf_unfix(next_pgptr); break;
                }
            }
        }

        buf_unfix(curr_pgptr);
        curr_pgptr = next_pgptr;
    }

    if (ret == ENGINE_SUCCESS) {
        *returned_pgptr = curr_pgptr;
    } else {
        buf_unfix(curr_pgptr);
    }
    return ret;
}
#endif

#if 0 // OLD_CODE
static Page *
do_btree_fix_leaf(prefix_desc *pfdesc, const char *key, const size_t nkey, bool readonly)
{
    PageID leaf_pgid;
    PageID save_pgid;
    Page  *leaf_pgptr = NULL;
    int    retry_count = 0;
    enum buf_latch_mode latch_mode = (readonly ? BUF_MODE_READ : BUF_MODE_WRITE);

    pthread_mutex_lock(&pfdesc->lock);
    while (1) {
        /* find the index leaf pageid */
        if (pfdesc->bt_root != NULL) {
            leaf_pgid = do_btree_get_leaf_pgid(pfdesc->bt_root, hash, key, nkey);
        } else {
            assert(pfdesc->d.bndx_leaf_head == pfdesc->d.bndx_leaf_tail);
            leaf_pgid = pfdesc->d.bndx_leaf_head;
        }
        if (leaf_pgptr != NULL) { /* the leaf page is already fixed */
            /* check the pageid of the leaf page fixed before */
            if (PAGEID_IS_EQ(save_pgid, leaf_pgid)) {
                /* if the same, the fixed leaf page is the correct page */
                break; /* found */
            } else {
                /* if different, unfix the leaf page and do fixing again */
                buf_unfix(leaf_pgptr); leaf_pgptr = NULL;
                retry_count += 1;
            }
        }
        if (PAGEID_IS_NULL(leaf_pgid)) {
            break; /* currently, no key exist */
        }
        if (retry_count < 2/* max_retry_count */) {
            bool trymode = true;
            while (1) {
                leaf_pgptr = buf_fix(leaf_pgid, latch_mode, trymode, false); // maybe TRY_MODE
                if (leaf_pgptr == NULL) {
                    if (trymode == true) {
                        pthread_mutex_unlock(&pfdesc->lock);
                        trymode = false; continue;
                    }
                }
                break;
            }

            if (leaf_pgptr != NULL) {
                if (trymode == false) {
                    /* check if the leaf page is still the wanted page after hold pfdesc->lock */
                    save_pgid = leaf_pgid;
                    pthread_mutex_lock(&pfdesc->lock);
                    continue;
                }
            }
        } else { /* retry_count >= 2 */
            /* trymode == false */
            leaf_pgptr = buf_fix(leaf_pgid, latch_mode, false, false);
        }
        break;
    }
    pthread_mutex_unlock(&pfdesc->lock);
    return leaf_pgptr;
}
#endif

static bndx_item *
do_btree_search_in_leaf(Page *pgptr, const char *key, const size_t nkey, uint32_t *slotid)
{
    bndx_item *xit; /* index item */
    int16_t    mid, left, right, comp;

    /*** Assume the placement of index items in leaf pages *****
      <key1, itm1>, <key2, itm2>, .... <keyn, itmn>
    *************************************************************/
    if (xpage_is_empty(pgptr)) {
        /* The empty leaf page can be existed.
         * In this case, the leaf page is the only page of the index.
         * That is, the leaf page is a root page.
         */
        *slotid = 0;
        return NULL;
    }

    left  = 0;
    right = pgptr->header.used_nslots-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        xit  = xpage_find_slot(pgptr, mid);
        comp = do_btree_key_comp(key, nkey, xit->key, xit->nkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* found the index item */
        *slotid = (uint32_t)mid;
        return xit;
    } else { /* not found, left indicates the next larger key */
        *slotid = (uint32_t)left;
        return NULL;
    }
}

static ENGINE_ERROR_CODE
do_btree_find_item(Page *pgptr, const char *key, const size_t nkey, ItemID *itmid)
{
    bndx_item  *indxit;
    uint32_t    slotid;

    indxit = do_btree_search_in_leaf(pgptr, key, nkey, &slotid);
    if (indxit == NULL) { /* not found */
        ITEMID_SET_NULL(itmid);
        return ENGINE_KEY_ENOENT;
    } else { /* found */
        *itmid = indxit->ptr.itmid;
        return ENGINE_SUCCESS;
    }
}

static ENGINE_ERROR_CODE
do_btree_insert_item(Page *pgptr, const char *key, const size_t nkey, ItemDesc *itdesc)
{
    bndx_item  *indxit;
    uint32_t    slotid;
    uint32_t    itemsz;

    indxit = do_btree_search_in_leaf(pgptr, key, nkey, &slotid);
    if (indxit != NULL) { /* found */
        return ENGINE_KEY_EEXISTS;
    }

    itemsz = BNDX_ITEM_SIZE(nkey);
    indxit = (bndx_item*)xpage_alloc_slot(pgptr, slotid, itemsz);
    if (indxit == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree insert - slot alloc fail\n");
        return ENGINE_PAGE_FAILED;
    }

    indxit->ptr.itmid = itdesc->iid;
    indxit->nkey = nkey;
    memcpy(indxit->key, key, nkey);

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        BTInsertLog btins_log;
        lrec_bt_insert_init((LogRec*)&btins_log);
        GET_ITEMID_FROM_PAGEID_SLOTID(&pgptr->header.self_pgid, &slotid, &btins_log.body.xitmid);
        btins_log.body.length = itemsz;
        btins_log.itptr = (void*)indxit;
        btins_log.header.length = GET_8_ALIGN_SIZE(offsetof(BTInsertData, data) + itemsz);

        log_record_write((LogRec*)&btins_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, btins_log.header.self_lsn);
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_delete_item(Page *pgptr, const char *key, const size_t nkey, ItemDesc *itdesc)
{
    bndx_item  *indxit;
    uint32_t    slotid;

    indxit = do_btree_search_in_leaf(pgptr, key, nkey, &slotid);
    if (indxit == NULL) { /* not found */
        return ENGINE_KEY_ENOENT;
    }

    /* The found index item must have the the given itemid */
    assert(ITEMID_IS_EQ(&indxit->ptr.itmid, &itdesc->iid));
    if (xpage_free_slot(pgptr, slotid) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree delete - free slot fail\n");
        return ENGINE_PAGE_FAILED;
    }

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        BTDeleteLog btdel_log;
        lrec_bt_delete_init((LogRec*)&btdel_log);
        GET_ITEMID_FROM_PAGEID_SLOTID(&pgptr->header.self_pgid, &slotid, &btdel_log.body.xitmid);
        btdel_log.body.ditmid = itdesc->iid;
        btdel_log.body.length = nkey;
        btdel_log.key = (char*)key;
        btdel_log.header.length = GET_8_ALIGN_SIZE(offsetof(BTDeleteData,data) + nkey);

        log_record_write((LogRec*)&btdel_log, (Trans*)itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, btdel_log.header.self_lsn);
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_toggle_item(Page *pgptr, const char *key, const size_t nkey,
                     ItemDesc *old_itdesc, ItemDesc *new_itdesc)
{
    bndx_item  *indxit;
    uint32_t    slotid;

    indxit = do_btree_search_in_leaf(pgptr, key, nkey, &slotid);
    if (indxit == NULL) { /* not found */
        return ENGINE_KEY_ENOENT;
    }

    /* The found index item must have the the given old itemid */
    assert(ITEMID_IS_EQ(&indxit->ptr.itmid, &old_itdesc->iid));
    indxit->ptr.itmid = new_itdesc->iid;

#ifdef ENABLE_RECOVERY
    if (config->sq.use_recovery) { /* TRANSACTION */
        BTToggleLog bttog_log;
        lrec_bt_toggle_init((LogRec*)&bttog_log);
        GET_ITEMID_FROM_PAGEID_SLOTID(&pgptr->header.self_pgid, &slotid, &bttog_log.body.xitmid);
        bttog_log.body.old_ditmid = old_itdesc->iid;
        bttog_log.body.new_ditmid = new_itdesc->iid;
        bttog_log.body.length = nkey;
        bttog_log.key = (char*)key;
        bttog_log.header.length = GET_8_ALIGN_SIZE(offsetof(BTToggleData,data) + nkey);

        log_record_write((LogRec*)&bttog_log, (Trans*)new_itdesc->trx);
        /* update PageLSN */
        buf_set_lsn(pgptr, bttog_log.header.self_lsn);
    }
#endif
    /* set buffer page dirty */
    buf_set_dirty(pgptr);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_assoc_btree_find(int pfxid, const char *key, const size_t nkey, ItemID *itmid)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    if (PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) { /* empty btree index */
        return ENGINE_KEY_ENOENT;
    }

    /* subkey string information */
    subkey  = (char*)key  + pfdesc->tlen;
    nsubkey = nkey        - pfdesc->tlen;

    //ret = do_btree_fix_leaf(pfdesc, subkey, nsubkey, INDX_SEARCH, &pgptr);
    ret = do_btree_fix_leaf_4_search(pfdesc, subkey, nsubkey, &pgptr);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree find - fix leaf fail\n");
        return ret; /* ENGINE_BUFF_FAILED */
    }
    assert(pgptr != NULL); /* leaf page pointer */

    /* find the index item */
    ret = do_btree_find_item(pgptr, subkey, nsubkey, itmid);
    buf_unfix(pgptr);
    /* The access on the key were serialized through key lock before time.
     * So. the found itemid can be preserved even if we unfix the leaf page early.
     */

    return ret; /* ENGINE_SUCCESS | ENGINE_KEY_ENOENT */
}

static ENGINE_ERROR_CODE
do_assoc_btree_exist(int pfxid, const char *key, const size_t nkey, bool *exist)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ItemID      itmid;
    ENGINE_ERROR_CODE ret;

    if (PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) { /* empty btree index */
        *exist = false;
        return ENGINE_SUCCESS;
    }

    /* subkey string information */
    subkey  = (char*)key  + pfdesc->tlen;
    nsubkey = nkey        - pfdesc->tlen;

    //ret = do_btree_fix_leaf(pfdesc, subkey, nsubkey, INDX_SEARCH, &pgptr);
    ret = do_btree_fix_leaf_4_search(pfdesc, subkey, nsubkey, &pgptr);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree find - fix leaf fail\n");
        return ret; /* ENGINE_BUFF_FAILED */
    }
    assert(pgptr != NULL); /* leaf page pointer */

    /* find the index item */
    ret = do_btree_find_item(pgptr, subkey, nsubkey, &itmid);
    buf_unfix(pgptr);
    /* The access on the key were serialized through key lock before time.
     * So. the found itemid can be preserved even if we unfix the leaf page early.
     */
    if (ret == ENGINE_SUCCESS) {
        *exist = true;
    } else {
        *exist = false;
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE
do_btree_root_build(prefix_desc *pfdesc, Page **root_pgptr)
{
    PageID new_pgid;
    Page  *new_pgptr;
    ENGINE_ERROR_CODE ret;

    /* How do we serialize building a root page ? */

    pthread_mutex_lock(&pfdesc->root_lock);
    do {
        if (!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) { /* not empty btree index */
            /* The root page was built by someone.
             * So, try to fix the root page in the caller function.
             */
            *root_pgptr = NULL;
            ret = ENGINE_SUCCESS; break;
        }

        /* allocate btree index page */
        ret = pfx_bndx_page_alloc(pfdesc->pfxid, &new_pgid);
        if (ret != ENGINE_SUCCESS) {
            /* ENGINE_ENODISK | ENGINE_FAILED */
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree root build - page alloc fail\n");
            break;
        }

        /* fix the new index page with newpg = true */
        new_pgptr = buf_fix(new_pgid, BUF_MODE_WRITE, false, true); // NEW_PAGE
        assert(new_pgptr != NULL);

        /* initialize the indx page */
        xpage_init(new_pgptr, new_pgid, 0);

#ifdef ENABLE_RECOVERY
        if (config->sq.use_recovery) { /* TRANSACTION */
            /* NTS(nested top action) : redo-only */
            BTRtBldLog rtbld_log;
            lrec_bt_rtbuild_init((LogRec*)&rtbld_log);
            rtbld_log.body.pgid = new_pgid;
            rtbld_log.header.length = sizeof(BTRtBldData);

            log_record_write((LogRec*)&rtbld_log, NULL);
            /* update PageLSN */
            buf_set_lsn(new_pgptr, rtbld_log.header.self_lsn);
        }
#endif
        /* set buffer page dirty */
        buf_set_dirty(new_pgptr);

        /* register the root page */
        pfdesc->d.bndx_root_pgid = new_pgid;
        pfdesc->d.bndx_global_depth = 0;

        /* return the fixed root page */
        *root_pgptr = new_pgptr;
        ret = ENGINE_SUCCESS;
    } while(0);
    pthread_mutex_unlock(&pfdesc->root_lock);

    return ret;
}

/* Note: this isn't an assoc_update.
 * The key must not already exist to call this
 */
static ENGINE_ERROR_CODE
do_assoc_btree_insert(PrefixID pfxid, ItemDesc *itdesc)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr = NULL;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    /* subkey string information */
    subkey  = item_get_key(itdesc->ptr) + pfdesc->tlen;
    nsubkey = itdesc->ptr->nkey         - pfdesc->tlen;

    if (PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) { /* empty btree index */
        ret = do_btree_root_build(pfdesc, &pgptr); /* build root page */
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree insert - root build fail\n");
            return ret; /* ENGINE_ENODISK | ENGINE_BUFF_FAILED | ENGINE_FAILED */
        }
        /* pgptr might be NULL: the root page was built by someone. */
    }
    if (pgptr != NULL) {
        /* The root page was built by current thread.
         * So, the root page is the only index page of leaf type.
         * And, the leaf page has already been fixed.
         */
    } else { /* pgptr == NULL */
        /* The root page was built by other thread.
         * so, we have to travese the index to fix corresponding leaf page.
         */
        //ret = do_btree_fix_leaf(pfdesc, subkey, nsubkey, INDX_INSERT, &pgptr);
        ret = do_btree_fix_leaf_4_insert(pfdesc, subkey, nsubkey, &pgptr);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree insert - fix leaf fail\n");
            return ret; /* ?? */
        }
        assert(pgptr != NULL); /* leaf page pointer */
    }

    /* insert the index item */
    ret = do_btree_insert_item(pgptr, subkey, nsubkey, itdesc);
    buf_unfix(pgptr);

    if (ret != ENGINE_SUCCESS && ret != ENGINE_KEY_EEXISTS) {
        /* ENGINE_PAGE_FAILED */
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree insert - insert item fail\n");
    }
    return ret;
}

static ENGINE_ERROR_CODE
do_assoc_btree_delete(PrefixID pfxid, ItemDesc *itdesc)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    if (PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) { /* empty btree index */
        return ENGINE_KEY_ENOENT;
    }

    /* subkey string information */
    subkey  = item_get_key(itdesc->ptr) + pfdesc->tlen;
    nsubkey = itdesc->ptr->nkey         - pfdesc->tlen;

    //ret = do_btree_fix_leaf(pfdesc, subkey, nsubkey, INDX_DELETE, &pgptr);
    ret = do_btree_fix_leaf_4_delete(pfdesc, subkey, nsubkey, &pgptr);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree delete - fix leaf fail\n");
        return ret;
    }
    assert(pgptr != NULL); /* leaf page pointer */

    /* delete the index item */
    ret = do_btree_delete_item(pgptr, subkey, nsubkey, itdesc);
    assert(xpage_is_empty(pgptr) == false || pgptr->header.pgdepth == 0);
    buf_unfix(pgptr);

    if (ret != ENGINE_SUCCESS && ret != ENGINE_KEY_ENOENT) {
         /* ENGINE_INDX_FAILED | ENGINE_PAGE_FAILED */
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree delete - delete item fail\n");
    }
    return ret;
}

static ENGINE_ERROR_CODE
do_assoc_btree_toggle(PrefixID pfxid, ItemDesc *old_itdesc, ItemDesc *new_itdesc)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    Page        *pgptr;
    char        *subkey;    /* subkey string */
    size_t      nsubkey;    /* the length of subkey */
    ENGINE_ERROR_CODE ret;

    if (PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) { /* empty btree index */
        return ENGINE_KEY_ENOENT;
    }

    /* subkey string information */
    subkey  = item_get_key(old_itdesc->ptr) + pfdesc->tlen;
    nsubkey = old_itdesc->ptr->nkey         - pfdesc->tlen;

    //ret = do_btree_fix_leaf(pfdesc, subkey, nsubkey, INDX_TOGGLE, &pgptr);
    ret = do_btree_fix_leaf_4_toggle(pfdesc, subkey, nsubkey, &pgptr);
    if (ret != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree toggle - fix leaf fail\n");
        return ENGINE_BUFF_FAILED; /* SEVERE error */
    }
    assert(pgptr != NULL); /* leaf page pointer */

    /* toggle the index item */
    ret = do_btree_toggle_item(pgptr, subkey, nsubkey, old_itdesc, new_itdesc);
    buf_unfix(pgptr);

    if (ret != ENGINE_SUCCESS && ret != ENGINE_KEY_ENOENT) {
        /* ENGINE_INDX_FAILED */
        logger->log(EXTENSION_LOG_WARNING, NULL, "assoc btree toggle - toggle item fail\n");
    }
    return ret;
}

#if 0 // OLD_CONSISTENCY_CHECK_CODE
static bndx_item *
do_btree_get_first_item(indx_scan *xscanp)
{
    bndx_item *indxit;
    Page      *pgptr;
    char       minkey = 0x00;

    //if (do_btree_fix_leaf(xscanp->pfdesc, &minkey, 1, INDX_SEARCH, &pgptr) != ENGINE_SUCCESS) {
    if (do_btree_fix_leaf_4_search(xscanp->pfdesc, &minkey, 1, &pgptr) != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "index page check - first page fix fail\n");
        xscanp->result = -1; /* fail */
        return NULL;
    }
    if (xpage_check_consistency(pgptr, 0) == false) {
        buf_unfix(pgptr);
        logger->log(EXTENSION_LOG_WARNING, NULL, "index page check - first page consistency fail\n");
        xscanp->result = -1; /* fail */
        return NULL;
    }
    if (xpage_is_empty(pgptr)) {
        buf_unfix(pgptr);
        indxit = NULL; /* no items */
    } else {
        xscanp->pgptr = pgptr;
        xscanp->slotid = 0;
        indxit = xpage_find_slot(pgptr, xscanp->slotid);
        assert(indxit != NULL);
    }
    return indxit;
}

static bndx_item *
do_btree_get_next_item(indx_scan *xscanp)
{
    bndx_item *indxit;
    Page      *pgptr;

    xscanp->slotid += 1;

    if (xscanp->slotid >= page_get_used_slot_count(xscanp->pgptr)) {
        if (PAGEID_IS_NULL(&xscanp->pgptr->header.next_pgid)) {
            buf_unfix(xscanp->pgptr); xscanp->pgptr = NULL;
            xscanp->slotid = -1; /* the end */
            return NULL;
        }
        pgptr = buf_fix(xscanp->pgptr->header.next_pgid, BUF_MODE_READ, false, false);
        if (pgptr == NULL) {
            buf_unfix(xscanp->pgptr); xscanp->pgptr = NULL;
            logger->log(EXTENSION_LOG_WARNING, NULL, "index page check - next page fix fail\n");
            xscanp->slotid = -1;
            xscanp->result = -1; /* fail */
            return NULL;
        }
        assert(xpage_is_empty(pgptr) == false);
        if (xpage_check_consistency(pgptr, 0) == false) {
            buf_unfix(pgptr);
            buf_unfix(xscanp->pgptr); xscanp->pgptr = NULL;
            logger->log(EXTENSION_LOG_WARNING, NULL, "index page check - next page consistency fail\n");
            xscanp->slotid = -1;
            xscanp->result = -1; /* fail */
            return NULL;
        }
        buf_unfix(xscanp->pgptr);
        xscanp->pgptr = pgptr;
        xscanp->slotid = 0;
    }

    indxit = xpage_find_slot(xscanp->pgptr, xscanp->slotid);
    assert(indxit != NULL);
    return indxit;
}
#endif

#ifdef ENABLE_KEY_DATA_CONSISTENCY_CHECK
/* it is also reimplemented in do_item_data_check function. */
static int
do_assoc_data_check(ItemID itmid, char *key, int nkey)
{
    Page        *pgptr;
    data_item   *itptr;
    PageID       pgid;
    uint32_t     slotid;
    int          result = 0;

    GET_PAGEID_SLOTID_FROM_ITEMID(&itmid, &pgid, &slotid);
    pgptr = buf_fix(pgid, BUF_MODE_READ, false, false);
    if (pgptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - fix data page fail\n");
        return -1;
    }

    itptr = dpage_find_slot(pgptr, slotid);
    if (itptr == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - find data item fail\n");
        result = -1;
    } else {
        if (do_btree_key_comp(key, nkey, item_get_key(itptr), itptr->nkey) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - data item mismatch\n");
            result = -1;
        }
    }
    buf_unfix(pgptr);
    return result;
}
#endif

#if 0 // OLD_CONSISTENCY_CHECK_CODE
static ENGINE_ERROR_CODE
do_btree_item_check(prefix_desc *pfdesc, uint64_t *item_count, uint64_t *leaf_count)
{
    Page        *prev_pgptr;
    bndx_item   *prev_xit;
    bndx_item   *curr_xit;
    uint64_t     tot_key_cnt = 0;
    uint64_t     tot_lpg_cnt = 0;
    indx_scan    idxscan;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    /* initialize index scan structure */
    idxscan.pfdesc = pfdesc;
    idxscan.pgptr = NULL;
    idxscan.slotid = -1;
    idxscan.result = 0;

    /* do index scan */
    prev_pgptr = NULL;
    prev_xit = NULL;
    curr_xit = do_btree_get_first_item(&idxscan);
    while (curr_xit != NULL) {
        if (prev_xit != NULL &&
            do_btree_key_comp(prev_xit->key, prev_xit->nkey, curr_xit->key, curr_xit->nkey) > 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - prev key is larger than curr key\n");
            buf_unfix(idxscan.pgptr);
            ret = ENGINE_INDX_FAILED; break;
        }
#ifdef ENABLE_KEY_DATA_CONSISTENCY_CHECK // it is also reimplemented in do_item_data_check function.
        if (do_assoc_data_check(curr_xit->ptr.itmid, curr_xit->key, curr_xit->nkey) != 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - key & data mismatch\n");
            buf_unfix(idxscan.pgptr);
            ret = ENGINE_INDX_FAILED; break;
        }
#endif
        tot_key_cnt += 1;
        if (prev_pgptr != idxscan.pgptr) {
            prev_pgptr = idxscan.pgptr;
            tot_lpg_cnt += 1;
        }
        prev_xit = curr_xit;
        curr_xit = do_btree_get_next_item(&idxscan);
    }
    if (ret == ENGINE_SUCCESS) {
        if (idxscan.result == -1) {
            ret = ENGINE_INDX_FAILED;
        } else {
            *item_count = tot_key_cnt;
            *leaf_count = tot_lpg_cnt;
        }
    }
    return ret;
}
#endif

static int
do_btree_page_check(PageID cur_pgid, Page *cur_pgptr, Page *prv_pgptr, Page *nxt_pgptr,
                    int pgdepth, uint64_t *itcount, uint64_t *pgcount, uint64_t *lfcount)
{
    indx_item *prv_itptr, *nxt_itptr;
    indx_item *cur_itptr, *chd_itptr;
    Page      *chd_pgptr;
    Page      *chd_prvpg, *chd_nxtpg;
    uint64_t   chd_itcnt, tot_itcnt=0;
    uint64_t   chd_pgcnt, tot_pgcnt=0;
    uint64_t   chd_lfcnt, tot_lfcnt=0;
    int        i, comp, ret=0;

    do {
        if (!PAGEID_IS_EQ(&cur_pgptr->header.self_pgid, &cur_pgid)) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - curr pgid mismatch\n");
            ret = -1; break;
        }
        if (xpage_check_consistency(cur_pgptr, pgdepth) == false) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - curr page (%u|%u|%u) is inconsistent\n",
                        cur_pgid.pfxid, cur_pgid.flnum, cur_pgid.pgnum);
            ret = -1; break;
        }
        if (pgdepth > 0) { /* nonleaf page */
            if (!PAGEID_IS_NULL(&cur_pgptr->header.prev_pgid) ||
                !PAGEID_IS_NULL(&cur_pgptr->header.next_pgid)) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - nonleaf prev and next pgid mismatch\n");
                ret = -1; break;
            }
        } else { /* leaf page */
            if (prv_pgptr == NULL) {
                if (!PAGEID_IS_NULL(&cur_pgptr->header.prev_pgid)) ret = -1;
            } else {
                if (!PAGEID_IS_EQ(&cur_pgptr->header.prev_pgid, &prv_pgptr->header.self_pgid)) ret = -1;
            }
            if (nxt_pgptr == NULL) {
                if (!PAGEID_IS_NULL(&cur_pgptr->header.next_pgid)) ret = -1;
            } else {
                if (!PAGEID_IS_EQ(&cur_pgptr->header.next_pgid, &nxt_pgptr->header.self_pgid)) ret = -1;
            }
            if (ret == -1) {
                logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - leaf prev and next pgid mismatch \n");
                break;
            }
        }

        /* get prv_itptr */
        if (prv_pgptr == NULL) {
            prv_itptr = NULL;
        } else {
            prv_itptr = xpage_find_slot(prv_pgptr, prv_pgptr->header.used_nslots-1);
            assert(prv_itptr != NULL);
        }

        /* get cur_itptr */
        cur_itptr = xpage_find_slot(cur_pgptr, 0);
        assert(cur_itptr != NULL);

        for (i = 0; i < cur_pgptr->header.used_nslots; i++)
        {
            /* get nxt_itptr */
            if (i < (cur_pgptr->header.used_nslots-1)) {
                nxt_itptr = xpage_find_slot(cur_pgptr, i+1);
                assert(nxt_itptr != NULL);
            } else {
                if (nxt_pgptr == NULL) nxt_itptr = NULL;
                else {
                    nxt_itptr = xpage_find_slot(nxt_pgptr, 0);
                    assert(nxt_itptr != NULL);
                }
            }

            if (prv_itptr != NULL) {
                comp = do_btree_key_comp(cur_itptr->key, cur_itptr->nkey, prv_itptr->key, prv_itptr->nkey);
                assert(comp > 0);
                if (comp <= 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - curr key is smaller than prev key\n");
                    ret = -1; break;
                }
            }

            if (pgdepth > 0) { /* nonleaf pages */
                chd_pgptr = buf_fix(cur_itptr->ptr.pgid, BUF_MODE_READ, false, false);
                assert(chd_pgptr != NULL);

                if (i > 0) {
                    chd_itptr = xpage_find_slot(chd_pgptr, 0); /* min key */
                    assert(chd_itptr != NULL);

                    comp = do_btree_key_comp(cur_itptr->key, cur_itptr->nkey, chd_itptr->key, chd_itptr->nkey);
                    assert(comp <= 0);
                    if (comp > 0) {
                        logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - curr key isn't same to min key of child page\n");
                        ret = -1; break;
                    }
                }

                /* get child prev pgptr */
                if (prv_itptr == NULL) {
                    chd_prvpg = NULL;
                } else {
                    chd_prvpg = buf_fix(prv_itptr->ptr.pgid, BUF_MODE_READ, false, false);
                    assert(chd_prvpg != NULL);
                }
                /* get child next pgptr */
                if (nxt_itptr == NULL) {
                    chd_nxtpg = NULL;
                } else {
                    chd_nxtpg = buf_fix(nxt_itptr->ptr.pgid, BUF_MODE_READ, false, false);
                    assert(chd_nxtpg != NULL);
                }

                ret = do_btree_page_check(cur_itptr->ptr.pgid, chd_pgptr, chd_prvpg, chd_nxtpg,
                                          pgdepth-1, &chd_itcnt, &chd_pgcnt, &chd_lfcnt);

                if (chd_prvpg != NULL) buf_unfix(chd_prvpg);
                if (chd_nxtpg != NULL) buf_unfix(chd_nxtpg);
                buf_unfix(chd_pgptr);

                if (ret != 0) break;

                tot_itcnt += chd_itcnt;
                tot_pgcnt += chd_pgcnt;
                tot_lfcnt += chd_lfcnt;
            } else { /* leaf pages */
                /* what do we check in leaf pages ?? */
#ifdef ENABLE_KEY_DATA_CONSISTENCY_CHECK // it is also reimplemented in do_item_data_check function.
                if (do_assoc_data_check(cur_itptr->ptr.itmid, cur_itptr->key, cur_itptr->nkey) != 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL, "btree check - key & data mismatch\n");
                    ret = -1; break;
                }
#endif
            }
            prv_itptr = cur_itptr;
            cur_itptr = nxt_itptr;
        }
    } while(0);

    if (ret == 0) {
        if (pgdepth > 0) { /* nonleaf page */
            *itcount = tot_itcnt;
            *pgcount = tot_pgcnt + 1;
            *lfcount = tot_lfcnt;
        } else {            /* leaf page */
            *itcount = cur_pgptr->header.used_nslots;
            *pgcount = 1;
            *lfcount = 1;
        }
    }
    return ret;
}

static ENGINE_ERROR_CODE
do_assoc_btree_check(PrefixID pfxid, uint64_t *item_count)
{
    prefix_desc *pfdesc = pfx_desc_get(pfxid);
    uint64_t total_itcnt = 0;
    uint64_t total_pgcnt = 0;
    uint64_t leafs_pgcnt = 0;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) {
        PageID root_pgid = pfdesc->d.bndx_root_pgid;
        Page  *root_pgptr;

        root_pgptr = buf_fix(root_pgid, BUF_MODE_READ, false, false);
        assert(root_pgptr != NULL);

        if (do_btree_page_check(root_pgid, root_pgptr, NULL, NULL, pfdesc->d.bndx_global_depth,
                                &total_itcnt, &total_pgcnt, &leafs_pgcnt) < 0) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "PREFIX [%s] btree index inconsistent\n",
                        (pfdesc->nlen == 0 ? "NULL" : pfdesc->name));
            ret = ENGINE_EINCONSISTENT;
        }

        buf_unfix(root_pgptr);
    }
    if (ret == ENGINE_SUCCESS) {
        *item_count = total_itcnt;
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "PREFIX [%s] index_key_count=%"PRIu64" index_page_count=%"PRIu64" leaf_page_count=%"PRIu64"\n",
                    (pfdesc->nlen == 0 ? "NULL" : pfdesc->name), total_itcnt, total_pgcnt, leafs_pgcnt);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "PREFIX [%s] index inconsistent\n",
                    (pfdesc->nlen == 0 ? "NULL" : pfdesc->name));
    }
#if 0 // OLD_CONSISTENCY_CHECK_CODE
    uint64_t total_itcnt = 0;
    uint64_t leafs_pgcnt = 0;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (!PAGEID_IS_NULL(&pfdesc->d.bndx_root_pgid)) {
        ret = do_btree_item_check(pfdesc, &total_itcnt, &leafs_pgcnt);
        if (ret != ENGINE_SUCCESS) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "PREFIX [%s] index item inconsistent\n",
                        (pfdesc->nlen == 0 ? "NULL" : pfdesc->name));
        }
    }
    if (ret == ENGINE_SUCCESS) {
        *item_count = total_itcnt;
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "PREFIX [%s] index_key_count=%"PRIu64" index_page_count=%"PRIu64" leaf_page_count=%"PRIu64"\n",
                    (pfdesc->nlen == 0 ? "NULL" : pfdesc->name), total_itcnt, total_pgcnt, leafs_pgcnt);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL, "PREFIX [%s] index inconsistent\n",
                    (pfdesc->nlen == 0 ? "NULL" : pfdesc->name));
    }
#endif
    return ret;
}

/*
 * External Functions
 */

int assoc_mgr_init(void *conf, void *srvr)
{
    int i, ret = 0;

    config = (struct config *)conf;
    server = (SERVER_HANDLE_V1 *)srvr;
    logger = server->log->get_logger();

    //assoc_gl.assoc_type = ASSOC_TYPE_HASH;
    assoc_gl.assoc_type = ASSOC_TYPE_BTREE;
    assoc_gl.page_size  = config->sq.data_page_size;
    assoc_gl.page_body_size = config->sq.data_page_size - sizeof(PGHDR);
    assoc_gl.npage_per_file = config->sq.data_file_size * ((1024*1024) / config->sq.data_page_size);
    assoc_gl.max_key_length = 250; /* KEY_MAX_LENGTH */
    assoc_gl.max_bndx_item_size = BNDX_ITEM_SIZE(assoc_gl.max_key_length);

    if (assoc_gl.assoc_type == ASSOC_TYPE_HASH) {
        assoc_gl.key_hbit_mask[0] = 0x00000000;
        assoc_gl.key_hval_mask[0] = 0x00000000;

        for (i = 1; i < 32; i++) {
            assoc_gl.key_hbit_mask[i] = (0x00000001 << (i-1));
            assoc_gl.key_hval_mask[i] = (assoc_gl.key_hbit_mask[i] | assoc_gl.key_hval_mask[i-1]);
        }
        for (i = 0; i < 8; i++) {
            assoc_gl.set_used_mask[i] = (0x80 >> i);
            assoc_gl.set_free_mask[i] = (assoc_gl.set_used_mask[i] ^ 0xFF);
        }
    }

    /* how to rebuild index ?? */
    return ret;
}

void assoc_mgr_final(void)
{
    /* do nothing */
    return;
}

ENGINE_ERROR_CODE assoc_find(int pfxid, const char *key, const size_t nkey, ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;

    if (assoc_gl.assoc_type == ASSOC_TYPE_HASH) {
        ret = do_assoc_bhash_find(pfxid, itdesc->hsh, key, nkey, &itdesc->iid);
    } else { /* ASSOC_TYPE_BTREE */
        ret = do_assoc_btree_find(pfxid, key, nkey, &itdesc->iid);
    }
    if (ret == ENGINE_SUCCESS) {
        Page    *pgptr;
        PageID   pgid;
        uint32_t slotid;

        GET_PAGEID_SLOTID_FROM_ITEMID(&itdesc->iid, &pgid, &slotid);
        pgptr = buf_fix(pgid, BUF_MODE_READ, false, false);
        if (pgptr == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL, "assoc find - fix data page fail\n");
            itdesc->ptr = NULL;
            return ENGINE_BUFF_FAILED;
        }

        itdesc->ptr = dpage_find_slot(pgptr, slotid);
        assert(itdesc->ptr != NULL);

        buf_unlatch_only(pgptr);
        //MEMCACHED_ASSOC_FIND(key, nkey, 0);
    } else {
        /* ENGINE_KEY_ENOENT | ENGINE_BUFF_FAILED */
    }
    return ret;
}

ENGINE_ERROR_CODE assoc_exist(int pfxid, const char *key, const size_t nkey, bool *exist)
{
    ENGINE_ERROR_CODE ret;

    if (assoc_gl.assoc_type == ASSOC_TYPE_HASH) {
        ret = ENGINE_ENOTSUP;
    } else { /* ASSOC_TYPE_BTREE */
        ret = do_assoc_btree_exist(pfxid, key, nkey, exist);
    }
    return ret;
}

ENGINE_ERROR_CODE assoc_insert(int pfxid, ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;

    if (assoc_gl.assoc_type == ASSOC_TYPE_HASH) {
        ret = do_assoc_bhash_insert(pfxid, itdesc);
    } else { /* ASSOC_TYPE_BTREE */
        ret = do_assoc_btree_insert(pfxid, itdesc);
    }
    if (ret == ENGINE_SUCCESS) {
        //MEMCACHED_ASSOC_INSERT(item_get_key(itdesc->ptr), itdesc->ptr->nkey, 0);
    } else {
        /* ENGINE_KEY_EEXISTS | ENGINE_ENODISK | ENGINE_BUFF_FAILED | ENGINE_FAILED */
    }
    return ret;
}

ENGINE_ERROR_CODE assoc_delete(int pfxid, ItemDesc *itdesc)
{
    ENGINE_ERROR_CODE ret;

    if (assoc_gl.assoc_type == ASSOC_TYPE_HASH) {
        ret = do_assoc_bhash_delete(pfxid, itdesc);
    } else { /* ASSOC_TYPE_BTREE */
        ret = do_assoc_btree_delete(pfxid, itdesc);
    }
    if (ret == ENGINE_SUCCESS) {
        //MEMCACHED_ASSOC_DELETE(item_get_key(itdesc->ptr), itdesc->ptr->nkey, 0);
    } else {
        /* ENGINE_KEY_ENOENT | ENGINE_FAILED */
    }
    return ret;
}

ENGINE_ERROR_CODE assoc_toggle(int pfxid, ItemDesc *old_itdesc, ItemDesc *new_itdesc)
{
    ENGINE_ERROR_CODE ret;

    if (assoc_gl.assoc_type == ASSOC_TYPE_HASH) {
        ret = do_assoc_bhash_toggle(pfxid, old_itdesc, new_itdesc);
    } else { /* ASSOC_TYPE_BTREE */
        ret = do_assoc_btree_toggle(pfxid, old_itdesc, new_itdesc);
    }
    if (ret == ENGINE_SUCCESS) {
        //MEMCACHED_ASSOC_TOGGLE(item_get_key(itdesc->ptr), itdesc->ptr->nkey, 0);
    } else {
        /* ENGINE_KEY_ENOENT | ENGINE_BUFF_FAILED | ENGINE_INDX_FAILED | ENGINE_FAILED */
    }
    return ret;
}

ENGINE_ERROR_CODE assoc_dbcheck(int pfxid, uint64_t *item_count)
{
    ENGINE_ERROR_CODE ret;

    if (assoc_gl.assoc_type == ASSOC_TYPE_HASH) {
        ret = ENGINE_SUCCESS;
    } else { /* ASSOC_TYPE_BTREE */
        ret = do_assoc_btree_check(pfxid, item_count);
    }
    return ret;
}
