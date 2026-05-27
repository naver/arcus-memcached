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
#include "config.h"
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "default_engine.h"
#include "hash_tree.h"

#define HTREE_HASHIDX_MASK       0x0000000F
#define HTREE_MAX_HASHCHAIN_SIZE 64

#define HTREE_GET_HASHIDX(hval, hdepth) \
    (((hval) & (HTREE_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

extern int genhash_string_hash(const void *p, size_t nkey);

typedef struct {
    htree_elem_item **pos;  /* array mode: current write position */
    htree_elem_item  *tail; /* chain mode: current tail */
} htree_collect_ctx;

static inline uint32_t htree_hash(htree_ops *ops, const htree_elem_item *elem)
{
    uint16_t nkey;
    const void *key = ops->get_key(elem, &nkey);
    return (uint32_t)genhash_string_hash(key, nkey);
}

static inline bool htree_key_eq(htree_ops *ops,
                                const htree_elem_item *a, const htree_elem_item *b)
{
    uint16_t na, nb;
    const void *ka = ops->get_key(a, &na);
    const void *kb = ops->get_key(b, &nb);
    return na == nb && memcmp(ka, kb, na) == 0;
}

static inline bool htree_key_eq_raw(htree_ops *ops, const htree_elem_item *elem,
                                    const void *key, uint16_t nkey)
{
    uint16_t lnkey;
    const void *lkey = ops->get_key(elem, &lnkey);
    return lnkey == nkey && memcmp(lkey, key, nkey) == 0;
}

static void collect_to_array(htree_elem_item *elem, htree_collect_ctx *ctx)
{
    *ctx->pos++ = elem;
}

static void collect_to_chain(htree_elem_item *elem, htree_collect_ctx *ctx)
{
    ctx->tail->next = elem;
    ctx->tail = elem;
}

static htree_node *do_htree_node_alloc(const uint8_t depth, const void *cookie)
{
    size_t ntotal = sizeof(htree_node);
    htree_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL, cookie);
    if (node != NULL) {
        node->slabs_clsid  = slabs_clsid(ntotal);
        node->refcount     = 0;
        node->hdepth       = depth;
        node->tot_elem_cnt = 0;
        memset(node->hcnt, 0, HTREE_HASHTAB_SIZE * sizeof(int16_t));
        memset(node->htab, 0, HTREE_HASHTAB_SIZE * sizeof(void *));
    }
    return node;
}

static void do_htree_node_free(htree_node *node)
{
    do_item_mem_free(node, sizeof(htree_node));
}

static inline void space_delta_add(ssize_t *delta, ssize_t size)
{
    if (delta) *delta += size;
}

static inline bool is_leaf_node(const htree_node *node)
{
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1)
            return false;
    }
    return true;
}

/* Split par_node's chain at par_hidx into a new child node:
 * allocate child, transfer existing chain into it, and link it as a child. */
static bool do_htree_node_split(htree_node *par_node, const int par_hidx,
                                const void *cookie)
{
    /* allocate a new child node one level deeper */
    htree_node *n_node = do_htree_node_alloc(par_node->hdepth + 1, cookie);
    if (n_node == NULL)
        return false;

    htree_elem_item *elem;
    while (par_node->htab[par_hidx] != NULL) {
        /* pop from par_node's chain */
        elem = (htree_elem_item *)par_node->htab[par_hidx];
        par_node->htab[par_hidx] = elem->next;

        /* re-compute hidx at child depth */
        int hidx = HTREE_GET_HASHIDX(elem->hval, n_node->hdepth);

        /* insert into child node's slot */
        elem->next = n_node->htab[hidx];
        n_node->htab[hidx] = elem;
        n_node->hcnt[hidx] += 1;
        n_node->tot_elem_cnt += 1;
    }
    assert(n_node->tot_elem_cnt == par_node->hcnt[par_hidx]);

    /* replace the chain slot with the child node */
    par_node->htab[par_hidx] = n_node;
    par_node->hcnt[par_hidx] = -1;
    return true;
}

/* Merge node back into par_node at par_hidx:
 * transfer node's hash chain back into par_node's slot and free node. */
static void do_htree_node_merge(htree_node **root_pptr,
                                htree_node *par_node, const int par_hidx)
{
    htree_node *node;

    if (par_node == NULL) { /* removing the root: must already be empty */
        node = *root_pptr;
        *root_pptr = NULL;
        assert(node->tot_elem_cnt == 0);
        do_htree_node_free(node);
        return;
    }
    /* merge child node's elements back into par_node's hash chain */
    assert(par_node->hcnt[par_hidx] == -1); /* par_hidx must point to a child node */
    node = (htree_node *)par_node->htab[par_hidx];

    htree_elem_item *head = NULL;
    int fcnt = 0;
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        assert(node->hcnt[hidx] >= 0); /* child must be a leaf (no grandchildren) */
        if (node->hcnt[hidx] == 0)
            continue;

        /* prepend this slot's chain onto head */
        fcnt += node->hcnt[hidx];
        while (node->htab[hidx] != NULL) {
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            node->htab[hidx] = elem->next;
            node->hcnt[hidx] -= 1;
            elem->next = head;
            head = elem;
        }
    }
    assert(fcnt == (int)node->tot_elem_cnt);

    /* replace the child slot with the collected flat chain */
    par_node->htab[par_hidx] = head;
    par_node->hcnt[par_hidx] = fcnt;
    do_htree_node_free(node);
}

/* Split the hash chain at *hidx_ptr into a new child node if it is full,
 * updating *node_pptr and *hidx_ptr to point to the insertion slot in the child. */
static bool do_htree_node_try_split(htree_node **node_pptr,
                                    int *hidx_ptr, uint32_t hval,
                                    ssize_t *delta, const void *cookie)
{
    htree_node *par_node = *node_pptr;
    int hidx = *hidx_ptr;
    /* chain not full: no split needed */
    if (par_node->hcnt[hidx] < HTREE_MAX_HASHCHAIN_SIZE)
        return true;

    /* split: allocate child, transfer chain, and link as child of par_node */
    if (!do_htree_node_split(par_node, hidx, cookie))
        return false;
    space_delta_add(delta, (ssize_t)slabs_space_size(sizeof(htree_node)));

    /* update *node_pptr and *hidx_ptr so caller inserts into the child node */
    *node_pptr = (htree_node *)par_node->htab[hidx];
    *hidx_ptr = HTREE_GET_HASHIDX(hval, (*node_pptr)->hdepth);
    return true;
}

/* Merge child back into par_node if child is empty or too sparse. */
static void do_htree_node_try_merge(htree_node **root_pptr,
                                    htree_node *par_node, int par_hidx,
                                    htree_node *child, ssize_t *delta)
{
    if (*root_pptr == NULL) return;
    /* merge if empty, or if leaf has too few elems (< MAX/2) to keep as a separate node */
    if (child->tot_elem_cnt == 0 ||
        (par_node != NULL &&
         is_leaf_node(child) &&
         child->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE / 2))) {

        do_htree_node_merge(root_pptr, par_node, par_hidx);
        space_delta_add(delta, -(ssize_t)slabs_space_size(sizeof(htree_node)));
    }
}

static void do_htree_elem_link(htree_node *root,
                               htree_node *node, const int hidx,
                               htree_elem_item *elem)
{
    /* prepend elem to the hash chain */
    elem->next       = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;

    /* increment tot_elem_cnt on every node from root down to the target node */
    htree_node *cur = root;
    while (cur != node) {
        cur->tot_elem_cnt += 1;
        int cidx = HTREE_GET_HASHIDX(elem->hval, cur->hdepth);
        assert(cur->hcnt[cidx] == -1);
        cur = (htree_node *)cur->htab[cidx];
    }
    node->tot_elem_cnt += 1;
}

static void do_htree_elem_unlink(htree_node *root,
                                 htree_node *node, int hidx,
                                 htree_elem_item *prev, htree_elem_item *elem)
{
    /* remove link from the hash chain */
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->next = NULL;
    node->hcnt[hidx] -= 1;

    /* decrement tot_elem_cnt on every node from root down to the target node */
    htree_node *cur = root;
    while (cur != node) {
        cur->tot_elem_cnt -= 1;
        int cidx = HTREE_GET_HASHIDX(elem->hval, cur->hdepth);
        assert(cur->hcnt[cidx] == -1);
        cur = (htree_node *)cur->htab[cidx];
    }
    node->tot_elem_cnt -= 1;
}

/* Skip *skip elems then collect up to *take from a single hash chain.
 * prev is tracked from the skip phase so unlink can splice at the right point. */
static uint32_t do_htree_chain_range(htree_node *node, int hidx,
                                     uint32_t *skip, uint32_t *take,
                                     bool unlink,
                                     void (*collect)(htree_elem_item *, htree_collect_ctx *),
                                     htree_collect_ctx *ctx)
{
    assert(*skip < (uint32_t)node->hcnt[hidx]);
    uint32_t fcnt = 0;
    htree_elem_item *prev = NULL;
    htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];

    while (*skip > 0) {
        prev = elem;
        elem = elem->next;
        (*skip)--;
    }

    while (elem != NULL && *take > 0) {
        htree_elem_item *next = elem->next;
        collect(elem, ctx);
        (*take)--;
        fcnt++;
        if (unlink) {
            if (prev != NULL) prev->next = next;
            else              node->htab[hidx] = next;
            node->hcnt[hidx]--;
            node->tot_elem_cnt--;
            elem->next = NULL;
        }
        elem = next;
    }
    return fcnt;
}

/* DFS over the hash tree: skip *skip elems then collect up to *take
 * via collect_to_array (array output) or collect_to_chain (linked-list output).
 * collect_to_chain relinks elems via next pointers; must pair with unlink=true. */
static uint32_t do_htree_range(htree_node **root_pptr,
                               htree_node *node,
                               uint32_t *skip, uint32_t *take,
                               bool unlink,
                               void (*collect)(htree_elem_item *, htree_collect_ctx *),
                               htree_collect_ctx *ctx,
                               ssize_t *htree_space_delta)
{
    assert(collect != collect_to_chain || unlink);
    uint32_t fcnt = 0;

    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE && *take > 0; hidx++) {
        bool is_child = (node->hcnt[hidx] == -1);
        if (node->hcnt[hidx] == 0) continue;

        /* skip the entire slot if it falls within the skip range */
        uint32_t elem_cnt = is_child ? ((htree_node *)node->htab[hidx])->tot_elem_cnt
                                     : (uint32_t)node->hcnt[hidx];
        if (*skip >= elem_cnt) {
            *skip -= elem_cnt;
            continue;
        }

        if (is_child) {
            /* child node: recurse deeper to find elems within the skip/take range */
            htree_node *child = (htree_node *)node->htab[hidx];
            uint32_t got = do_htree_range(root_pptr, child, skip, take,
                                          unlink, collect, ctx, htree_space_delta);
            if (unlink && got > 0) {
                do_htree_node_try_merge(root_pptr, node, hidx, child, htree_space_delta);
                node->tot_elem_cnt -= got;
            }
            fcnt += got;
        } else {
            /* hash chain: skip remaining offset then collect up to *take elems */
            fcnt += do_htree_chain_range(node, hidx, skip, take, unlink, collect, ctx);
        }
    }
    return fcnt;
}

/* DFS over the hash tree: collect count elems by deciding each one randomly.
 * Each elem is accepted with probability (needed / remain). */
static int do_htree_sampling(htree_node *node,
                             uint32_t remain, uint32_t count,
                             htree_elem_item **elem_array)
{
    int fcnt = 0;
    for (int hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            fcnt += do_htree_sampling(child_node, remain,
                                      count - fcnt, &elem_array[fcnt]);
            remain -= child_node->tot_elem_cnt; /* mark child's elems as seen */
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
            while (elem != NULL) {
                if ((rand() % remain) < (count - (uint32_t)fcnt)) {
                    elem_array[fcnt++] = elem;
                    if ((uint32_t)fcnt >= count) break;
                }
                remain -= 1;
                elem = elem->next;
            }
        }
        if ((uint32_t)fcnt >= count) break;
    }
    return fcnt;
}

static htree_elem_item *do_htree_elem_get_at_offset(htree_node *node, uint32_t offset)
{
    /* acts as a 1-elem array for collect_to_array */
    htree_elem_item *elem = NULL;
    uint32_t skip = offset, take = 1;
    htree_collect_ctx ctx = { .pos = &elem };

    /* skip `offset`, read 1 */
    do_htree_range(&node, node, &skip, &take, false, collect_to_array, &ctx, NULL);
    return elem;
}

htree_elem_item *htree_elem_find(htree_node *root,
                                 const void *key, uint16_t nkey,
                                 htree_ops *ops,
                                 htree_elem_pos *pos)
{
    if (root == NULL)
        return NULL;

    htree_node *node = root;
    int hidx;
    uint32_t hval = (uint32_t)genhash_string_hash(key, nkey);

    while (true) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_node *)node->htab[hidx];
    }

    htree_elem_item *prev = NULL;
    htree_elem_item *find;
    for (find = (htree_elem_item *)node->htab[hidx];
         find != NULL;
         find = find->next) {
        if (find->hval == hval && htree_key_eq_raw(ops, find, key, nkey))
            break;
        prev = find;
    }

    if (find != NULL && pos != NULL) {
        pos->node = node;
        pos->hidx = hidx;
        pos->prev = prev;
    }
    return find;
}

void htree_elem_replace_at(htree_elem_pos *pos, htree_elem_item *new_elem)
{
    htree_elem_item *old_elem = (pos->prev != NULL) ? pos->prev->next
                              : (htree_elem_item *)pos->node->htab[pos->hidx];
    new_elem->hval = old_elem->hval;
    new_elem->next = old_elem->next;
    if (pos->prev != NULL)
        pos->prev->next = new_elem;
    else
        pos->node->htab[pos->hidx] = new_elem;
}

ENGINE_ERROR_CODE htree_elem_link(htree_node **root_pptr,
                                  htree_elem_item *elem,
                                  htree_ops *ops,
                                  ssize_t *htree_space_delta,
                                  const void *cookie)
{
    assert(htree_space_delta != NULL);
    *htree_space_delta = 0;
    elem->hval = htree_hash(ops, elem);

    /* allocate root node if the tree is empty */
    if (*root_pptr == NULL) {
        *root_pptr = do_htree_node_alloc(0, cookie);
        if (*root_pptr == NULL)
            return ENGINE_ENOMEM;
        space_delta_add(htree_space_delta, (ssize_t)slabs_space_size(sizeof(htree_node)));
        int hidx = HTREE_GET_HASHIDX(elem->hval, 0);
        do_htree_elem_link(*root_pptr, *root_pptr, hidx, elem);
        return ENGINE_SUCCESS;
    }

    /* traverse to the leaf node that should contain this element */
    htree_node *node = *root_pptr;
    int hidx;
    while (true) {
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = (htree_node *)node->htab[hidx];
    }

    /* check for duplicate key in the hash chain */
    for (htree_elem_item *find = (htree_elem_item *)node->htab[hidx];
         find != NULL; find = find->next) {
        if (find->hval == elem->hval && htree_key_eq(ops, find, elem))
            return ENGINE_ELEM_EEXISTS;
    }

    /* split the hash chain into a new child node if it is full */
    if (!do_htree_node_try_split(&node, &hidx, elem->hval,
                                 htree_space_delta, cookie))
        return ENGINE_ENOMEM;

    do_htree_elem_link(*root_pptr, node, hidx, elem);
    return ENGINE_SUCCESS;
}

htree_elem_item *htree_elem_unlink(htree_node **root_pptr,
                                   const void *key, uint16_t nkey,
                                   htree_ops *ops,
                                   ssize_t *htree_space_delta)
{
    if (*root_pptr == NULL) return NULL;
    assert(htree_space_delta != NULL);
    *htree_space_delta = 0;

    uint32_t hval = (uint32_t)genhash_string_hash(key, nkey);

    /* traverse to the leaf node, tracking parent for potential merge */
    htree_node *par_node = NULL;
    int par_hidx = 0;
    htree_node *node = *root_pptr;
    int hidx;
    while (true) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0) break;
        par_node = node;
        par_hidx = hidx;
        node = (htree_node *)node->htab[hidx];
    }

    /* find the element in the hash chain */
    htree_elem_item *prev = NULL;
    htree_elem_item *elem = (htree_elem_item *)node->htab[hidx];
    while (elem != NULL) {
        if (elem->hval == hval && htree_key_eq_raw(ops, elem, key, nkey))
            break;
        prev = elem;
        elem = elem->next;
    }
    if (elem == NULL) return NULL;

    do_htree_elem_unlink(*root_pptr, node, hidx, prev, elem);
    do_htree_node_try_merge(root_pptr, par_node, par_hidx, node, htree_space_delta);

    /* returns the unlinked elem for caller post-processing (e.g. CLOG, free). */
    return elem;
}

htree_elem_item *htree_elem_unlink_at_offset(htree_node **root_pptr,
                                             uint32_t offset,
                                             ssize_t *htree_space_delta)
{
    if (*root_pptr == NULL) return NULL;
    assert(htree_space_delta != NULL);
    *htree_space_delta = 0;

    /* sentinel head: collect_to_chain links the result onto dummy.next */
    htree_elem_item dummy = {0};
    uint32_t skip = offset, take = 1;
    htree_collect_ctx ctx = { .tail = &dummy };

    /* skip `offset`, unlink 1 */
    do_htree_range(root_pptr, *root_pptr, &skip, &take, true, collect_to_chain, &ctx,
                   htree_space_delta);
    if (dummy.next == NULL) return NULL;

    do_htree_node_try_merge(root_pptr, NULL, 0, *root_pptr, htree_space_delta);
    return dummy.next;
}

htree_elem_item *htree_elem_unlink_by_cnt(htree_node **root_pptr,
                                          uint32_t count,
                                          ssize_t *htree_space_delta)
{
    if (*root_pptr == NULL) return NULL;
    if (htree_space_delta) *htree_space_delta = 0;

    /* count == 0 means "all"; take == 0 when tree is empty */
    uint32_t actual = (count > 0) ? count : (*root_pptr)->tot_elem_cnt;
    if (actual == 0) return NULL;

    /* sentinel head: collect_to_chain links the result onto dummy.next */
    htree_elem_item dummy = {0};
    uint32_t skip = 0, take = actual;
    htree_collect_ctx ctx = { .tail = &dummy };

    /* skip 0, unlink `take` elems */
    do_htree_range(root_pptr, *root_pptr, &skip, &take, true, collect_to_chain, &ctx,
                   htree_space_delta);

    do_htree_node_try_merge(root_pptr, NULL, 0, *root_pptr, htree_space_delta);
    return dummy.next;
}

uint32_t htree_elem_get_by_cnt(htree_node **root_pptr,
                               uint32_t count,
                               htree_elem_item **elem_array,
                               bool unlink,
                               ssize_t *htree_space_delta)
{
    assert(elem_array != NULL);
    assert(!unlink || htree_space_delta != NULL);

    if (*root_pptr == NULL) return 0;
    if (htree_space_delta) *htree_space_delta = 0;

    /* count == 0 means "all"; take == 0 when tree is empty */
    uint32_t actual = (count > 0) ? count : (*root_pptr)->tot_elem_cnt;
    if (actual == 0) return 0;

    uint32_t skip = 0, take = actual;
    htree_collect_ctx ctx = { .pos = elem_array };

    /* skip 0, collect `take` elems */
    uint32_t fcnt = do_htree_range(root_pptr, *root_pptr, &skip, &take, unlink,
                                   collect_to_array, &ctx,
                                   unlink ? htree_space_delta : NULL);

    if (unlink)
        do_htree_node_try_merge(root_pptr, NULL, 0, *root_pptr, htree_space_delta);
    return fcnt;
}

int htree_elem_get_rand(htree_node *node,
                        uint32_t total_count, uint32_t count,
                        htree_elem_item **elem_array)
{
    int fcnt = 0;

    if (count <= total_count / 10) {
        /* sparse: offset dedup hash table */
        int capacity = (int)(1.3 * (double)count);
        typedef struct { int cnt; int keys[3]; } _bucket;
        _bucket *buckets = calloc(capacity, sizeof(_bucket));
        if (buckets == NULL) return 0;
        while ((uint32_t)fcnt < count) {
            int rand_offset = rand() % (int)total_count;
            int idx = rand_offset % capacity;
            _bucket *b = &buckets[idx];
            bool dup = false;
            for (int i = 0; i < b->cnt; i++) {
                if (b->keys[i] == rand_offset) { dup = true; break; }
            }
            if (!dup && b->cnt < 3) {
                b->keys[b->cnt++] = rand_offset;
                htree_elem_item *found = do_htree_elem_get_at_offset(node, rand_offset);
                assert(found != NULL);
                elem_array[fcnt++] = found;
            }
        }
        free(buckets);
    } else {
        /* dense: reservoir sampling + Fisher-Yates shuffle */
        fcnt = do_htree_sampling(node, total_count, count, elem_array); /* reservoir sampling */

        /* sampling collects elems in DFS order, so shuffle to randomize output order */
        for (int i = fcnt - 1; i > 0; i--) { /* Fisher-Yates shuffle */
            int rand_idx = rand() % (i + 1);
            if (rand_idx != i) {
                htree_elem_item *tmp = elem_array[i];
                elem_array[i] = elem_array[rand_idx];
                elem_array[rand_idx] = tmp;
            }
        }
    }
    return fcnt;
}
