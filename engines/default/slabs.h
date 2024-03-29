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
/* slabs memory allocation */
#ifndef SLABS_H
#define SLABS_H

#include "default_engine.h"

/* powers-of-N allocation structures */

typedef struct {
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    void       **slots;     /* list of item ptrs */
    unsigned int sl_total;  /* size of previous array */
    unsigned int sl_curr;   /* first free slot */

    void        *end_page_ptr;  /* pointer to next free item at end of page, or 0 */
    unsigned int end_page_free; /* number of items remaining at end of last alloced page */

    unsigned int slabs;     /* how many slabs were allocated for this class */
    unsigned int rsvd_slabs;/* how many slabs were reserved for allocation */ // Arcus added it

    void       **slab_list; /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    unsigned int killing;   /* index+1 of dying slab, or zero if none */
    size_t       requested; /* The number of requested bytes */
} slabclass_t;

struct slabs {
   slabclass_t slabclass[MAX_SLAB_CLASSES];
   size_t mem_limit;
   size_t mem_malloced;
   size_t mem_reserved; // Arcus Added it
   int    power_largest;

   void  *mem_base;
   void  *mem_current;
   size_t mem_avail;

   /**
    * Access to the slab allocator is protected by this lock
    */
   pthread_mutex_t lock;
};

/* extern variables */
/* Maximum value length for using the small memory allocator */
extern int MAX_SM_VALUE_LEN;

/** Init the subsystem. 1st argument is the limit on no. of bytes to allocate,
    0 if no limit. 2nd argument is the growth factor; each slab will use a chunk
    size equal to the previous slab's chunk size times this factor.
    3rd argument specifies if the slab allocator should allocate all memory
    up front (if true), or allocate memory in chunks as it is needed (if false)
*/
ENGINE_ERROR_CODE slabs_init(struct default_engine *engine,
                             const size_t limit, const double factor,
                             const bool prealloc);
void              slabs_final(struct default_engine *engine);


/**
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */
unsigned int slabs_clsid(const size_t size);

unsigned int slabs_space_size(const size_t size);

int   slabs_space_shortage_level(void);

/* temporary SM dump function */
void  slabs_dump_SM_info(void);

/** Allocate object of given length. 0 on error */ /*@null@*/
void *slabs_alloc(const size_t size, unsigned int id);

/** Free previously allocated object */
void  slabs_free(void *ptr, size_t size, unsigned int id);

/** Fill buffer with stats */ /*@null@*/
void  slabs_stats(ADD_STAT add_stats, const void *c);

/** Fill buffer with stats */ /*@null@*/
void  slabs_stats_basic(ADD_STAT add_stats, const void *c);

/** Adjust the stats for memory requested */
void  slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal);

void  add_statistics(const void *cookie, ADD_STAT add_stats,
                     const char *prefix, int num, const char *key,
                     const char *fmt, ...);

ENGINE_ERROR_CODE slabs_set_memlimit(size_t memlimit);
#endif
