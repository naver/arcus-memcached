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

#ifndef TYPED_OPS_H
#define TYPED_OPS_H

#include "memcached/types.h"
#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <string.h>

static inline int UINT64_COMP(const uint64_t *v1, const uint64_t *v2)
{
    if (*v1 == *v2) return  0;
    if (*v1 <  *v2) return -1;
    else            return  1;
}

static inline bool UINT64_ISEQ(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 == *v2) ? true : false);
}

static inline bool UINT64_ISNE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 != *v2) ? true : false);
}

static inline bool UINT64_ISLT(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 <  *v2) ? true : false);
}

static inline bool UINT64_ISLE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 <= *v2) ? true : false);
}

static inline bool UINT64_ISGT(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 >  *v2) ? true : false);
}

static inline bool UINT64_ISGE(const uint64_t *v1, const uint64_t *v2)
{
    return ((*v1 >= *v2) ? true : false);
}

static inline void UINT64_COPY(const uint64_t *v, uint64_t *result)
{
    *result = *v;
}

static inline void UINT64_DIFF(const uint64_t *v1, const uint64_t *v2, uint64_t *result)
{
    assert(*v1 >= *v2);
    *result = *v1 - *v2;
}

static inline void UINT64_INCR(uint64_t *v)
{
    assert(*v < UINT64_MAX);
    *v += 1;
}

static inline void UINT64_DECR(uint64_t *v)
{
    assert(*v > 0);
    *v -= 1;
}

static inline int BINARY_COMP(const unsigned char *v1, const int nv1,
                              const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return -1;
        else                return  1;
    }
    if (nv1 == nv2) return  0;
    if (nv1 <  nv2) return -1;
    else            return  1;
}

static inline bool BINARY_ISEQ(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    if (nv1 != nv2) return false;
    for (int i=0; i < nv1; i++) {
        if (v1[i] != v2[i]) return false;
    }
    return true;
}

static inline bool BINARY_ISNE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    if (nv1 != nv2) return true;
    for (int i=0; i < nv1; i++) {
        if (v1[i] != v2[i]) return true;
    }
    return false;
}

static inline bool BINARY_ISLT(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return true;
        else                return false;
    }
    if (nv1 < nv2) return true;
    else           return false;
}

static inline bool BINARY_ISLE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] <  v2[i]) return true;
        else                return false;
    }
    if (nv1 <= nv2) return true;
    else            return false;
}

static inline bool BINARY_ISGT(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] >  v2[i]) return true;
        else                return false;
    }
    if (nv1 > nv2) return true;
    else           return false;
}

static inline bool BINARY_ISGE(const unsigned char *v1, const int nv1,
                               const unsigned char *v2, const int nv2)
{
    assert(nv1 > 0 && nv2 > 0);
    int min_nv = (nv1 < nv2 ? nv1 : nv2);
    for (int i=0; i < min_nv; i++) {
        if (v1[i] == v2[i]) continue;
        if (v1[i] >  v2[i]) return true;
        else                return false;
    }
    if (nv1 >= nv2) return true;
    else            return false;
}

static inline void BINARY_COPY(const unsigned char *v, const int length,
                               unsigned char *result)
{
    if (length > 0)
        memcpy(result, v, length);
}

static inline void BINARY_DIFF(const unsigned char *v1, const uint8_t nv1,
                               const unsigned char *v2, const uint8_t nv2,
                               const int precision, unsigned char *result)
{
    assert(precision > 0);
    unsigned char bkey1_space[MAX_BKEY_LENG];
    unsigned char bkey2_space[MAX_BKEY_LENG];
    int i, subtraction;

    if (nv1 < precision) {
        memcpy(bkey1_space, v1, nv1);
        for (i=nv1; i<precision; i++)
            bkey1_space[i] = 0x00;
        v1 = bkey1_space;
    }
    if (nv2 < precision) {
        memcpy(bkey2_space, v2, nv2);
        for (i=nv2; i<precision; i++)
            bkey2_space[i] = 0x00;
        v2 = bkey2_space;
    }

    /* assume that the value of v1 >= the value of v2 */
    subtraction = 0;
    for (i = (precision-1); i >= 0; i--) {
        if (subtraction == 0) {
            if (v1[i] >= v2[i]) {
                result[i] = v1[i] - v2[i];
            } else {
                result[i] = 0xFF - v2[i] + v1[i] + 1;
                subtraction = 1;
            }
        } else {
            if (v1[i] > v2[i]) {
                result[i] = v1[i] - v2[i] - 1;
                subtraction = 0;
            } else {
                result[i] = 0xFF - v2[i] + v1[i];
            }
        }
    }
}

static inline void BINARY_INCR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; i--) {
        if (v[i] < 0xFF) {
            v[i] += 1;
            break;
        }
        v[i] = 0x00;
    }
    assert(i >= 0);
}

static inline void BINARY_DECR(unsigned char *v, const int length)
{
    assert(length > 0);
    int i;
    for (i = (length-1); i >= 0; i--) {
        if (v[i] > 0x00) {
            v[i] -= 1;
            break;
        }
        v[i] = 0xFF;
    }
    assert(i >= 0);
}

static inline void BINARY_AND(const unsigned char *v1, const unsigned char *v2,
                              const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] & v2[i];
    }
}

static inline void BINARY_OR(const unsigned char *v1, const unsigned char *v2,
                             const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] | v2[i];
    }
}

static inline void BINARY_XOR(const unsigned char *v1, const unsigned char *v2,
                              const int length, unsigned char *result)
{
    for (int i=0; i < length; i++) {
        result[i] = v1[i] ^ v2[i];
    }
}

extern bool (*UINT64_COMPARE_OP[COMPARE_OP_MAX]) (const uint64_t *v1, const uint64_t *v2);

extern bool (*BINARY_COMPARE_OP[COMPARE_OP_MAX]) (const unsigned char *v1, const int nv1,
                                                  const unsigned char *v2, const int nv2);

extern void (*BINARY_BITWISE_OP[BITWISE_OP_MAX]) (const unsigned char *v1, const unsigned char *v2,
                                                  const int length, unsigned char *result);

#endif
