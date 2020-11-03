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
#ifndef ITEM_SCAN_H
#define ITEM_SCAN_H

#if 0 // ENABLE_PERSISTENCE_01_ITEM_SCAN
/**
 * item scan
 */
/* callback functions */
typedef void (*CB_SCAN_OPEN)(void *scanp);
typedef void (*CB_SCAN_CLOSE)(bool success);

void *itscan_open(struct default_engine *engine, const char *prefix, const int nprefix,
                  CB_SCAN_OPEN cb_scan_open);
int   itscan_getnext(void *scan, void **item_array, elems_result_t *erst_array, int item_arrsz);
void  itscan_release(void *scan, void **item_array, elems_result_t *erst_array, int item_count);
void  itscan_close(void *scan, CB_SCAN_CLOSE cb_scan_close, bool success);
#endif

#endif
