/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2020 JaM2in Co., Ltd.
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
#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include "memcached/types.h"
#include "typed_ops.h"
#include <assert.h>
#include <string.h>

/* bkey type */
#define BKEY_TYPE_UNKNOWN 0
#define BKEY_TYPE_UINT64  1
#define BKEY_TYPE_BINARY  2

/* get bkey real size */
#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))

/* bkey range type */
#define BKEY_RANGE_TYPE_SIN 1 /* single bkey */
#define BKEY_RANGE_TYPE_ASC 2 /* ascending bkey range */
#define BKEY_RANGE_TYPE_DSC 3 /* descending bkey range */

/* btree scan direction */
#define BTREE_DIRECTION_PREV 2
#define BTREE_DIRECTION_NEXT 1
#define BTREE_DIRECTION_NONE 0

/* btree element item or btree node item */
#define BTREE_GET_ELEM_ITEM(node, indx) ((btree_elem_item *)((node)->item[indx]))
#define BTREE_GET_NODE_ITEM(node, indx) ((btree_indx_node *)((node)->item[indx]))

#define BKEY_COMP(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_COMP((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_COMP((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISEQ(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISEQ((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISEQ((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISNE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISNE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISNE((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISLT(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISLT((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISLT((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISLE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISLE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISLE((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISGT(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISGT((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISGT((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_ISGE(bk1, nbk1, bk2, nbk2) \
        (((nbk1)==0 && (nbk2)==0) ? UINT64_ISGE((const uint64_t*)(bk1),(const uint64_t*)(bk2)) \
                                  : BINARY_ISGE((bk1),(nbk1),(bk2),(nbk2)))

#define BKEY_COPY(bk, nbk, res) \
        ((nbk)==0 ? UINT64_COPY((const uint64_t*)(bk), (uint64_t*)(res)) \
                  : BINARY_COPY((bk), (nbk), (res)))

#define BKEY_DIFF(bk1, nbk1, bk2, nbk2, len, res) \
        ((len)==0 ? UINT64_DIFF((const uint64_t*)(bk1), (const uint64_t*)(bk2), (uint64_t*)(res)) \
                  : BINARY_DIFF((bk1), (nbk1), (bk2), (nbk2), (len), (res)))

#define BKEY_INCR(bk, nbk) \
        ((nbk)==0 ? UINT64_INCR((uint64_t*)(bk)) : BINARY_INCR((bk), (nbk)))

#define BKEY_DECR(bk, nbk) \
        ((nbk)==0 ? UINT64_DECR((uint64_t*)(bk)) : BINARY_DECR((bk), (nbk)))

#endif
