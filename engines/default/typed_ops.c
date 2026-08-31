/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "typed_ops.h"

bool (*UINT64_COMPARE_OP[COMPARE_OP_MAX]) (const uint64_t *v1, const uint64_t *v2)
    = { UINT64_ISEQ, UINT64_ISNE, UINT64_ISLT, UINT64_ISLE, UINT64_ISGT, UINT64_ISGE };

bool (*BINARY_COMPARE_OP[COMPARE_OP_MAX]) (const unsigned char *v1, const int nv1,
                                           const unsigned char *v2, const int nv2)
    = { BINARY_ISEQ, BINARY_ISNE, BINARY_ISLT, BINARY_ISLE, BINARY_ISGT, BINARY_ISGE };

void (*BINARY_BITWISE_OP[BITWISE_OP_MAX]) (const unsigned char *v1, const unsigned char *v2,
                                           const int length, unsigned char *result)
    = { BINARY_AND, BINARY_OR, BINARY_XOR };
