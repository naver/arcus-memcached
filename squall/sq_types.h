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
#ifndef SQ_TYPES_H
#define SQ_TYPES_H

#if 1 // NEW ENGINE ERROR CODE */
/***** NOTE *****
 This define must be moved to include/memcached/types.h
 Refer to ENGINE_ITEM_TYPE.
*****************/
#define ENGINE_PREFIX_EEXIST    0x53
#define ENGINE_PREFIX_EOVERFLOW 0x54
//#define ENGINE_PREFIX_ELOAD     0x55
//#define ENGINE_PREFIX_EBUILD    0x56
#define ENGINE_ETRANS           0x5F
#define ENGINE_ENODISK          0x60
#define ENGINE_ENOCONFFILE      0x61
#define ENGINE_EINCONSISTENT    0x62

#define ENGINE_PAGE_FAILED      0x64
#define ENGINE_INDX_FAILED      0x65
#define ENGINE_BUFF_FAILED      0x66
#endif

/* Use mmap function instead of posix_memalign function */
//#define SQUALL_USE_MMAP         1

/* squall file size (unit: MB) : 1024 ~ 8192 MB */
#define SQUALL_MIN_FILE_SIZE    (1*1024)
#define SQUALL_MAX_FILE_SIZE    (8*1024)
#define SQUALL_DFT_FILE_SIZE    (2*1024)

/* squall page size (unit: bytes) : 4/8/16/32/64 KB */
#define SQUALL_MIN_PAGE_SIZE    ( 4*1024)
#define SQUALL_MAX_PAGE_SIZE    (64*1024)
#define SQUALL_DFT_PAGE_SIZE    SQUALL_MIN_PAGE_SIZE

/* file types used by squall engine */
enum squall_file_type {
    SQUALL_FILE_TYPE_ANCH = 0,
    SQUALL_FILE_TYPE_DATA,
    SQUALL_FILE_TYPE_OVFL,
    SQUALL_FILE_TYPE_INDX,
    SQUALL_FILE_TYPE_COUNT
};

/* page types used by squall engine */
enum squall_page_type {
    SQUALL_PAGE_TYPE_PMAP = 0,
    SQUALL_PAGE_TYPE_DATA,
    SQUALL_PAGE_TYPE_OVFL,
    SQUALL_PAGE_TYPE_INDX,
    SQUALL_PAGE_TYPE_COUNT
};

/* file bits of each file type */
#define SQUALL_FILEBITS_DATA    0x4000
#define SQUALL_FILEBITS_OVFL    0x8000
#define SQUALL_FILEBITS_INDX    0xC000

/* file type and prefix id bits and masks */
#define SQUALL_FILETYPE_NBIT    2
#define SQUALL_PREFIXID_NBIT    14
#define SQUALL_FILETYPE_MASK    0xC000
#define SQUALL_PREFIXID_MASK    0x3FFF

/* page number and slot number bits and masks */
#define SQUALL_PAGENUMB_NBIT    20
#define SQUALL_SLOTNUMB_NBIT    12
#define SQUALL_PAGENUMB_MASK    0xFFFFF000
#define SQUALL_SLOTNUMB_MASK    0x00000FFF

#define SLOT_MIN_SIZE           40
#define SLOT_ALIGN_BYTES        8

#define MAX_FILE_NAME_LENG      256

/* transaction commit mode */
enum tran_commit_mode {
    MEMORY_COMMIT             = 1, /* reach to squall log buffer only (no write & no fsync) */
    SYSTEM_COMMIT             = 2, /* reach to OS cache by write call (no fsync) */
    DISK_COMMIT_EVERY_SECOND  = 3, /* system commit + fsync every 1 second */
    DISK_COMMIT_EVERY_COMMAND = 4  /* system commit + fsync every 1 command */
};


/*
 * Database Types
 */
/* page id structure */
typedef struct pageid {
    uint16_t    pfxid;   /* prefix id : file type (upper 2bits) + prefix number(lower 14bits) */
    uint16_t    flnum;   /* file number : 1, 2, ... */
    uint32_t    pgnum;   /* page number : 0, 1, ... */
} PageID;

/* page sn(sequence number) */
typedef uint32_t PageSN;

/* item id structure */
typedef struct itemid {
    uint16_t    pfxid;   /* prefix id : */
    uint16_t    flnum;   /* file number : 1, 2, ... */
    uint32_t    itnum;   /* item number : page number(upper 20bits) + slot number(lower 12bits) */
} ItemID;

typedef struct logsn {
    uint32_t    filenum; /* log file number : 1, 2, ... */
    uint32_t    roffset; /* log record offset */
} LogSN;

/* PageID, ItemID, LogSN : SET_NULL and IS_NULL */
#define PAGEID_SET_NULL(pid) \
        do { (pid)->pfxid = 0; (pid)->flnum = 0; (pid)->pgnum = 0; } while(0)
#define ITEMID_SET_NULL(iid) \
        do { (iid)->pfxid = 0; (iid)->flnum = 0; (iid)->itnum = 0; } while(0)
#define LOGSN_SET_NULL(lsn)  \
        do { (lsn)->filenum = 0; (lsn)->roffset = 0; } while(0)
#define PAGEID_IS_NULL(pid)  ((pid)->pfxid == 0)
#define ITEMID_IS_NULL(iid)  ((iid)->pfxid == 0)
#define LOGSN_IS_NULL(lsn)   ((lsn)->filenum == 0)

/* PageID comparison */
#define PAGEID_IS_EQ(p1, p2) \
        ((p1)->pfxid == (p2)->pfxid && (p1)->flnum == (p2)->flnum && (p1)->pgnum == (p2)->pgnum)
#define PAGEID_IS_NE(p1, p2) \
        ((p1)->pfxid != (p2)->pfxid || (p1)->flnum != (p2)->flnum || (p1)->pgnum != (p2)->pgnum)
#define PAGEID_IS_LT(p1, p2) \
        (((p1)->pfxid <  (p2)->pfxid) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum <  (p2)->flnum) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum == (p2)->flnum && (p1)->pgnum <  (p2)->pgnum))
#define PAGEID_IS_LE(p1, p2) \
        (((p1)->pfxid <  (p2)->pfxid) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum <  (p2)->flnum) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum == (p2)->flnum && (p1)->pgnum <= (p2)->pgnum))
#define PAGEID_IS_GT(p1, p2) \
        (((p1)->pfxid >  (p2)->pfxid) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum >  (p2)->flnum) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum == (p2)->flnum && (p1)->pgnum >  (p2)->pgnum))
#define PAGEID_IS_GE(p1, p2) \
        (((p1)->pfxid >  (p2)->pfxid) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum >  (p2)->flnum) || \
         ((p1)->pfxid == (p2)->pfxid && (p1)->flnum == (p2)->flnum && (p1)->pgnum >= (p2)->pgnum))

/* ItemID comparison */
#define ITEMID_IS_EQ(i1, i2) \
        ((i1)->pfxid == (i2)->pfxid && (i1)->flnum == (i2)->flnum && (i1)->itnum == (i2)->itnum)
#define ITEMID_IS_NE(i1, i2) \
        ((i1)->pfxid != (i2)->pfxid || (i1)->flnum != (i2)->flnum || (i1)->itnum != (i2)->itnum)
#define ITEMID_IS_LT(i1, i2) \
        (((i1)->pfxid <  (i2)->pfxid) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum <  (i2)->flnum) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum == (i2)->flnum && (i1)->itnum <  (i2)->itnum))
#define ITEMID_IS_LE(i1, i2) \
        (((i1)->pfxid <  (i2)->pfxid) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum <  (i2)->flnum) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum == (i2)->flnum && (i1)->itnum <= (i2)->itnum))
#define ITEMID_IS_GT(i1, i2) \
        (((i1)->pfxid >  (i2)->pfxid) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum >  (i2)->flnum) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum == (i2)->flnum && (i1)->itnum >  (i2)->itnum))
#define ITEMID_IS_GE(i1, i2) \
        (((i1)->pfxid >  (i2)->pfxid) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum >  (i2)->flnum) || \
         ((i1)->pfxid == (i2)->pfxid && (i1)->flnum == (i2)->flnum && (i1)->itnum >= (i2)->itnum))

/* LogSN comparison */
#define LOGSN_IS_EQ(lsn1, lsn2) ((lsn1)->filenum == (lsn2)->filenum && (lsn1)->roffset == (lsn2)->roffset)
#define LOGSN_IS_NE(lsn1, lsn2) ((lsn1)->filenum != (lsn2)->filenum || (lsn1)->roffset != (lsn2)->roffset)
#define LOGSN_IS_LT(lsn1, lsn2) (((lsn1)->filenum <  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && (lsn1)->roffset <  (lsn2)->roffset))
#define LOGSN_IS_LE(lsn1, lsn2) (((lsn1)->filenum <  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && (lsn1)->roffset <= (lsn2)->roffset))
#define LOGSN_IS_GT(lsn1, lsn2) (((lsn1)->filenum >  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && (lsn1)->roffset >  (lsn2)->roffset))
#define LOGSN_IS_GE(lsn1, lsn2) (((lsn1)->filenum >  (lsn2)->filenum) || \
                                 ((lsn1)->filenum == (lsn2)->filenum && (lsn1)->roffset >= (lsn2)->roffset))

/* PageID increment and decrement */
#define PAGEID_INCREMENT(pid, npage_per_file) \
        do { (pid)->pgnum += 1; \
             if ((pid)->pgnum >= (npage_per_file)) { \
                 (pid)->flnum += 1; (pid)->pgnum = 0; \
             } \
        } while(0)
#define PAGEID_DECREMENT(pid, npage_per_file) \
        do { if ((pid)->pgnum == 0) { \
                 (pid)->flnum -= 1; (pid)->pgnum = (npage_per_file); \
             } \
             (pid)->pgnum -= 1; \
        } while(0)

/* Get PageID (or slotid) from ItemID */
#define GET_PAGEID_FROM_ITEMID(iid, pid) \
        do { (pid)->pfxid = (iid)->pfxid; (pid)->flnum = (iid)->flnum; \
             (pid)->pgnum = ((iid)->itnum >> SQUALL_SLOTNUMB_NBIT); } while(0)
#define GET_PAGEID_SLOTID_FROM_ITEMID(iid, pid, sid) \
        do { (pid)->pfxid = (iid)->pfxid; (pid)->flnum = (iid)->flnum; \
             (pid)->pgnum = ((iid)->itnum >> SQUALL_SLOTNUMB_NBIT); \
             *(sid) = ((iid)->itnum & SQUALL_SLOTNUMB_MASK); } while(0)

/* Get ItemID from PageID and slotid */
#define GET_ITEMID_FROM_PAGEID_SLOTID(pid, sid, iid) \
        do { (iid)->pfxid = (pid)->pfxid; (iid)->flnum = (pid)->flnum; \
             (iid)->itnum = (((pid)->pgnum << SQUALL_SLOTNUMB_NBIT) | (uint32_t)*(sid)); } while(0)

/* ??? */
#define PREFIXID_FROM_PAGEID(pid) ((pid)->pfxid & SQUALL_PREFIXID_MASK)
#define FILEBITS_FROM_PAGEID(pid) ((pid)->pfxid & SQUALL_FILETYPE_MASK)
#define FILETYPE_FROM_PAGEID(pid) (((pid)->pfxid & SQUALL_FILETYPE_MASK) >> SQUALL_PREFIXID_NBIT)

#define PREFIXID_FROM_ITEMID(iid) ((iid)->pfxid & SQUALL_PREFIXID_MASK)
#define FILEBITS_FROM_ITEMID(iid) ((iid)->pfxid & SQUALL_FILETYPE_MASK)
#define FILETYPE_FROM_ITEMID(iid) (((iid)->pfxid & SQUALL_FILETYPE_MASK) >> SQUALL_PREFIXID_NBIT)
#define SLOTID_FROM_ITEMID(iid)   ((iid)->itnum & SQUALL_SLOTNUMB_MASK)
#define PGNUMB_FROM_ITEMID(iid)   (((iid)->itnum & SQUALL_PAGENUMB_MASK) >> SQUALL_SLOTNUMB_NBIT)

#define MAKE_PAGEID_PFXID(ftype, pfxid) ((ftype) << SQUALL_PREFIXID_NBIT | (pfxid))
#define MAKE_ITEMID_PFXID(ftype, pfxid) ((ftype) << SQUALL_PREFIXID_NBIT | (pfxid))

/* Get pageptr from itemptr */
#define PAGEPTR_FROM_ITEMPTR(iptr, pgsz) ((uint64_t)(iptr) & ~((uint64_t)((pgsz)-1)))

/* Get aligned size */
#define GET_ANY_ALIGN_SIZE(size, align) \
        (((size) % (align)) == 0 ? (size) : ((size) + ((align) - ((size) % (align)))))
#define GET_8_ALIGN_SIZE(size) \
        (((size) % 8) == 0 ? (size) : ((size) + (8 - ((size) % 8))))

#endif
