/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015 JaM2in Co., Ltd.
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
#ifndef MEMCACHED_TYPES_H
#define MEMCACHED_TYPES_H 1

#include <sys/types.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __WIN32__
struct iovec {
    size_t iov_len;
    void* iov_base;
};
#else
#include <sys/uio.h>
#endif

#define PERSISTENCE_CONFIG 1
#define NESTED_PREFIX
#define PROXY_SUPPORT
//#define NEW_PREFIX_STATS_MANAGEMENT
#define SUPPORT_BOP_MGET
#define SUPPORT_BOP_SMGET
#define JHPARK_OLD_SMGET_INTERFACE
#define MULTI_NOTIFY_IO_COMPLETE
#define MAX_EFLAG_COMPARE_COUNT 100

#ifdef __cplusplus
extern "C" {
#endif
    /**
     * Time relative to server start. Smaller than time_t on 64-bit systems.
     */
    typedef uint32_t rel_time_t;

    /**
     * Response codes for engine operations.
     */
    typedef enum {
        ENGINE_SUCCESS     = 0x00, /**< The command executed successfully */
        ENGINE_KEY_ENOENT  = 0x01, /**< The key does not exists */
        ENGINE_KEY_EEXISTS = 0x02, /**< The key already exists */
        ENGINE_ENOMEM      = 0x03, /**< Could not allocate memory */
        ENGINE_NOT_STORED  = 0x04, /**< The item was not stored */
        ENGINE_EINVAL      = 0x05, /**< Invalid arguments */
        ENGINE_ENOTSUP     = 0x06, /**< The engine does not support this */
        ENGINE_EWOULDBLOCK = 0x07, /**< This would cause the engine to block */
        ENGINE_E2BIG       = 0x08, /**< The data is too big for the engine */
        ENGINE_WANT_MORE   = 0x09, /**< The engine want more data if the frontend
                                    * have more data available. */
        ENGINE_DISCONNECT  = 0x0a, /**< Tell the server to disconnect this client */
        ENGINE_EACCESS     = 0x0b, /**< Access control violations */
        ENGINE_NOT_MY_VBUCKET = 0x0c, /** < This vbucket doesn't belong to me */
        ENGINE_EDUPLICATE  = 0x0d, /** < Duplicate value(ex, bkey) */

        ENGINE_EBADTYPE     = 0x32, /**< Not supported operation, bad type */
        ENGINE_EOVERFLOW    = 0x33, /**< The collection is full of elements */
        ENGINE_EBADVALUE    = 0x34, /**< Bad(not allowed) value */
        ENGINE_EINDEXOOR    = 0x35, /**< Index out of range in list item */
        ENGINE_EBKEYOOR     = 0x36, /**< B+tree key out of range */
        ENGINE_ELEM_ENOENT  = 0x37, /**< The element does not exist */
        ENGINE_ELEM_EEXISTS = 0x38, /**< The element already exists */
        ENGINE_EBADATTR     = 0x39, /**< Attribute not found */
        ENGINE_EBADBKEY     = 0x3a, /**< BKEY Mismatch */
        ENGINE_EBADEFLAG    = 0x3b, /**< EFlag Mismatch */
        ENGINE_UNREADABLE   = 0x3c, /**< unreadable collection item */

        ENGINE_PREFIX_ENAME  = 0x51, /**< Invalid prefix name */
        ENGINE_PREFIX_ENOENT = 0x52, /**< Attribute not found */
        ENGINE_FAILED      = 0xff  /**< Generic failue. */
    } ENGINE_ERROR_CODE;

    /**
     * Engine storage operations.
     */
    typedef enum {
        OPERATION_ADD = 1, /**< Store with add semantics */
        OPERATION_SET,     /**< Store with set semantics */
        OPERATION_REPLACE, /**< Store with replace semantics */
        OPERATION_APPEND,  /**< Store with append semantics */
        OPERATION_PREPEND, /**< Store with prepend semantics */
        OPERATION_CAS      /**< Store with set semantics. */
    } ENGINE_STORE_OPERATION;

    /**
     * Engine retrieval operations.
     */
    typedef enum {
        OPERATION_GET = 11, /**< Retrieve with get semantics */
        OPERATION_GETS,    /**< Retrieve with gets semantics */
        OPERATION_MGET,     /**< Retrieve with mget semantics */
        OPERATION_MGETS     /**< Retrieve with mgets semantics */
    } ENGINE_RETRIEVE_OPERATION;

    /* collection operation */
    typedef enum {
        /* list operation */
        OPERATION_LOP_CREATE = 0x50, /**< List operation with create structure semantics */
        OPERATION_LOP_INSERT,        /**< List operation with insert element semantics */
        OPERATION_LOP_DELETE,        /**< List operation with delete element semantics */
        OPERATION_LOP_GET,           /**< List operation with get element semantics */

        /* set operation */
        OPERATION_SOP_CREATE = 0x60, /**< Set operation with create structure semantics */
        OPERATION_SOP_INSERT,        /**< Set operation with insert element semantics */
        OPERATION_SOP_DELETE,        /**< Set operation with delete element semantics */
        OPERATION_SOP_EXIST,         /**< Set operation with check existence of element semantics */
        OPERATION_SOP_GET,           /**< Set operation with get element semantics */

        /* map operation */
        OPERATION_MOP_CREATE = 0x70, /**< Map operation with create structure semantics */
        OPERATION_MOP_INSERT,        /**< Map operation with insert element semantics */
        OPERATION_MOP_UPDATE,        /**< Map operation with update element semantics */
        OPERATION_MOP_DELETE,        /**< Map operation with delete element semantics */
        OPERATION_MOP_GET,            /**< Map operation with get element semantics */

        /* b+tree operation */
        OPERATION_BOP_CREATE = 0x80, /**< B+tree operation with create structure semantics */
        OPERATION_BOP_INSERT,        /**< B+tree operation with insert element semantics */
        OPERATION_BOP_UPSERT,        /**< B+tree operation with upsert element semantics */
        OPERATION_BOP_UPDATE,        /**< B+tree operation with update element semantics */
        OPERATION_BOP_DELETE,        /**< B+tree operation with delete element semantics */
        OPERATION_BOP_GET,           /**< B+tree operation with get element semantics */
        OPERATION_BOP_COUNT,         /**< B+tree operation with count element semantics */
        OPERATION_BOP_POSITION,      /**< B+tree operation with find position */
        OPERATION_BOP_PWG,           /**< B+tree operation with find position with get */
        OPERATION_BOP_GBP,           /**< B+tree operation with get element by position */
        // SUPPORT_BOP_MGET
        OPERATION_BOP_MGET,          /**< B+tree operation with mget(multiple get) element semantics */
        // SUPPORT_BOP_SMGET
        OPERATION_BOP_SMGET          /**< B+tree operation with smget(sort-merge get) element semantics */
    } ENGINE_COLL_OPERATION;

    /* item type */
    typedef enum {
        ITEM_TYPE_KV = 0,
        ITEM_TYPE_LIST,
        ITEM_TYPE_SET,
        ITEM_TYPE_MAP,
        ITEM_TYPE_BTREE,
        ITEM_TYPE_MAX
    } ENGINE_ITEM_TYPE;

    /* overflow action */
    typedef enum {
        OVFL_ERROR = 1,
        OVFL_HEAD_TRIM,
        OVFL_TAIL_TRIM,
        OVFL_SMALLEST_TRIM,
        OVFL_LARGEST_TRIM,
        OVFL_SMALLEST_SILENT_TRIM,
        OVFL_LARGEST_SILENT_TRIM
    } ENGINE_OVFL_ACTION;

    /* item attributes */
    typedef enum {
        ATTR_TYPE = 0,    /**< item type : kv, list, set, b+tree */
        ATTR_FLAGS,       /**< application flags */
        ATTR_EXPIRETIME,  /**< item expire time */
        ATTR_COUNT,       /**< current element count */
        ATTR_MAXCOUNT,    /**< maximum element count */
        ATTR_OVFLACTION,  /**< list overflow actions */
        ATTR_READABLE,
        ATTR_MAXBKEYRANGE,
        ATTR_MINBKEY,
        ATTR_MAXBKEY,
        ATTR_TRIMMED,
        ATTR_END
    } ENGINE_ITEM_ATTR;

    /* btree order for sorting/scanning */
    typedef enum {
        BTREE_ORDER_ASC = 1,
        BTREE_ORDER_DESC
    } ENGINE_BTREE_ORDER;

    /* eflag filter : compare operation */
    typedef enum {
        COMPARE_OP_EQ = 0,
        COMPARE_OP_NE,
        COMPARE_OP_LT,
        COMPARE_OP_LE,
        COMPARE_OP_GT,
        COMPARE_OP_GE,
        COMPARE_OP_MAX
    } ENGINE_COMPARE_OP;

    /* eflag filter : bitwise operation */
    typedef enum {
        BITWISE_OP_AND = 0,
        BITWISE_OP_OR,
        BITWISE_OP_XOR,
        BITWISE_OP_MAX
    } ENGINE_BITWISE_OP;

    /* vlaue item */
    typedef struct {
        uint32_t len;       /* value length */
        char     ptr[1];    /* value itself */
    } value_item;

    /**
     * Data common to any item stored in memcached.
     */
    typedef void item;
    typedef void eitem; /* element item */

    typedef struct {
        uint64_t cas;       /* cas value */
        uint32_t flags;     /**< Flags associated with the item (in network byte order)*/
        rel_time_t exptime; /**< When the item will expire (relative to process startup) */
        uint8_t clsid;      /** class id for the object */
        uint16_t nkey;      /**< The total length of the key (in bytes) */
        uint32_t nbytes;    /**< The total length of the data (in bytes) */
        uint32_t nvalue;    /* size of value data */
        uint32_t naddnl;    /* additional value item count */
        const void *key;    /* key string */
        const void *value;  /* value data */
        value_item **addnl; /* additional value items */
    } item_info;

    /* collection element info */
    typedef struct {
        uint32_t   nbytes;  /* total size of the data (in bytes) */
        uint32_t   nvalue;  /* size of value data */
        uint32_t   naddnl;  /* additional value item count */
        uint16_t   nscore;  /* size of score data */
        uint16_t   neflag;  /* size of eflag data */
        const char *value;  /* value data itself */
        value_item **addnl; /* additional value items */
        const unsigned char *score; /* score data */
        const unsigned char *eflag; /* eflag data */
    } eitem_info;

    /* element info that is trimmed by maxcount overflow */
    typedef struct {
        eitem   *elems;
        uint32_t count;
        uint32_t flags;
    } eitem_result;

    /* used to get elements */
    typedef struct {
        void     **elem_array;
        uint32_t elem_arrsz;
        uint32_t elem_count;
    } elems_result_t;

    /* result fields in common for each collection on get operation */
    struct elems_result {
        eitem** elem_array; //output variable that will receive the located item
        uint32_t elem_count; //number of output elements
        uint32_t opcost_or_eindex; //for b+tree
        uint32_t flags;
        bool dropped; //dropped if empty
        bool trimmed; //trimmed on btree
    };

    /*
     * bkey and eflag
     */
#define MIN_BKEY_LENG  1
#define MAX_BKEY_LENG  31
#define MAX_EFLAG_LENG 31
#define BKEY_NULL  255
#define EFLAG_NULL 255
#define MAX_FIELD_LENG 250

    /* field list structure */
    typedef struct {
        char *value;
        size_t length;
    } field_t;

    /* bkey type */
    typedef struct {
        unsigned char val[MAX_BKEY_LENG];
        uint8_t       len;
    } bkey_t;
    /*******
    typedef struct {
        union {
           uint64_t      ui;
           unsigned char uc[MAX_BKEY_LENG];
        } val;
        uint8_t len;
    } bkey_t;
    *******/

    /* eflag type */
    typedef struct {
        unsigned char val[MAX_EFLAG_LENG];
        uint8_t       len;
    } eflag_t;

    /* bkey_range structure */
    typedef struct {
        /******
        bkey_t   from_bkey;
        bkey_t   to_bkey;
        ******/
        unsigned char from_bkey[MAX_BKEY_LENG];
        unsigned char to_bkey[MAX_BKEY_LENG];
        uint8_t from_nbkey;
        uint8_t to_nbkey;
    } bkey_range;

    /* eflag_filter structure */
    typedef struct {
        unsigned char bitwval[MAX_EFLAG_LENG];
        unsigned char compval[MAX_EFLAG_LENG * MAX_EFLAG_COMPARE_COUNT];
        uint8_t nbitwval; /* bitwise value length */
        uint8_t ncompval; /* compare value length */
        uint8_t compvcnt; /* # of compare values */
        uint8_t fwhere; /* filter offset */
        uint8_t bitwop; /* bitwise operation */
        uint8_t compop; /* compare operation */
    } eflag_filter;

    /* eflag_update structure */
    typedef struct {
        unsigned char eflag[MAX_EFLAG_LENG];
        uint8_t  neflag;
        uint8_t  fwhere;
        uint8_t  bitwop;
        uint8_t reserved[6];
    } eflag_update;

    /* Key info of the found elements in smget */
    typedef struct {
        uint16_t kidx;  /* key index in keys array */
        uint32_t flag;  /* item flags */
    } smget_ehit_t;

    /* Key info of the missed/trimmed keys in smget */
    typedef struct {
        uint16_t kidx;  /* key index in keys array */
        uint16_t code;  /* error code if the key is missed */
    } smget_emis_t;

    /* smget result structure */
    typedef struct {
        eitem       **elem_array; /* found elements in smget */
        smget_ehit_t *elem_kinfo; /* key info of found elements */
        smget_emis_t *miss_kinfo; /* key info of missed keys */
        smget_emis_t *trim_kinfo; /* key info of trimmed keys */
        eitem       **trim_elems; /* the element before trim in each trimmed key */
        uint32_t      elem_count; /* # of found elements */
        uint32_t      miss_count; /* # of missed keys */
        uint32_t      trim_count; /* # of trimmed keys */
        uint32_t      elem_arrsz; /* elem array size */
        uint32_t      keys_arrsz; /* miss & trim array size */
        bool          duplicated; /* bkey is duplicated ? */
        bool          ascending;  /* bkey order of found elements: ascending ? */
    } smget_result_t;

    /* item attribute structure */
    typedef struct {
        uint32_t flags; /**< Flags associated with the item (in network byte order)*/
        rel_time_t exptime; /**< When the item will expire (relative to process
                             * startup) */
        int32_t  count;
        int32_t  maxcount;
        bkey_t   maxbkeyrange;
        bkey_t   minbkey;
        bkey_t   maxbkey;
        uint8_t  type;
        uint8_t  ovflaction;
        uint8_t  readable;
        uint8_t  trimmed;
    } item_attr;

    /* prefix stats of engine */
    typedef struct {
        uint64_t hash_items_bytes;
        uint64_t hash_items;
        uint32_t prefix_items;
        uint32_t tot_prefix_items;
    } prefix_engine_stats;

    typedef struct {
        const char *username;
        const char *config;
    } auth_data_t;

    /* Forward declaration of the server handle -- to be filled in later */
    typedef struct server_handle_v1_t SERVER_HANDLE_V1;

#ifdef __cplusplus
}
#endif

#endif /* MEMCACHED_TYPES_H */
