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
#include "hash_tree.h"
#include "default_engine.h"
#include <stdlib.h>
#include <assert.h>

#define HTREE_MAX_DEPTH 8  /* 32-bit hash, 4bits per level */
#define HTREE_HASHIDX_MASK 0x0000000F
#define HTREE_MAX_HASHCHAIN_SIZE 64

#define HTREE_GET_HASHIDX(hval, hdepth) \
        (((hval) & (HTREE_HASHIDX_MASK << ((hdepth)*4))) >> ((hdepth)*4))

static inline int htree_hash_eq(const int h1, const void *k1, size_t nkey1,
                                const int h2, const void *k2, size_t nkey2)
{
    return (h1 == h2 && nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0);
}

typedef struct {
    htree_node *node;
    int         hidx;
} htree_path_entry;

typedef struct {
    htree_elem_item  *curr;
    htree_path_entry path[HTREE_MAX_DEPTH];
    int              depth;
} htree_elem_posi;

/*
 * Hash table management
 */
typedef struct hash_node {
    int cnt;
    int keys[3];
} hash_node;

typedef struct hash_table {
    hash_node *buckets;
    int capacity;
} hash_table;

static bool hash_init(hash_table *ht, int count)
{
    ht->capacity = (int)(1.3 * (double)count);
    ht->buckets = (hash_node*)calloc(ht->capacity, sizeof(hash_node));
    if (ht->buckets == NULL)
        return false;
    return true;
}

static void hash_free(hash_table *ht)
{
    if (ht->buckets) {
        free(ht->buckets);
        ht->buckets = NULL;
        ht->capacity = 0;
    }
}

static bool hash_insert(hash_table *ht, int key)
{
    size_t idx = key % ht->capacity;
    hash_node *bucket = &ht->buckets[idx];
    if (bucket->cnt == 3)
        return false;
    for (int i = 0; i < bucket->cnt; i++) {
        if (bucket->keys[i] == key)
            return false;
    }
    bucket->keys[bucket->cnt] = key;
    bucket->cnt++;
    return true;
}

/*
 * Hash tree manangement
 */
static htree_node *do_htree_node_alloc(uint8_t hash_depth)
{
    size_t ntotal = sizeof(htree_node);

    htree_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL);
    if (node != NULL) {
        node->slabs_clsid = slabs_clsid(ntotal);
        assert(node->slabs_clsid > 0);

        node->refcount    = 0;
        node->hdepth      = hash_depth;
        node->tot_elem_cnt = 0;
        memset(node->hcnt, 0, HTREE_HASHTAB_SIZE*sizeof(uint16_t));
        memset(node->htab, 0, HTREE_HASHTAB_SIZE*sizeof(void*));
    }
    return node;
}

static void do_htree_node_free(htree_node *node)
{
    do_item_mem_free(node, sizeof(htree_node));
}

static void do_htree_node_link(htree_meta *htree,
                             htree_node *par_node, const int par_hidx,
                             htree_node *node)
{
    if (par_node == NULL) {
        htree->root = node;
    } else {
        htree_elem_item *elem;
        while (par_node->htab[par_hidx] != NULL) {
            elem = par_node->htab[par_hidx];
            par_node->htab[par_hidx] = elem->next;

            int hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
            elem->next = node->htab[hidx];
            node->htab[hidx] = elem;
            node->hcnt[hidx] += 1;
            node->tot_elem_cnt += 1;
        }
        assert(node->tot_elem_cnt == par_node->hcnt[par_hidx]);
        par_node->htab[par_hidx] = node;
        par_node->hcnt[par_hidx] = -1; /* child hash node */
    }
}

static void do_htree_node_unlink(htree_meta *htree,
                                 htree_node *par_node, const int par_hidx)
{
    htree_node *node;

    if (par_node == NULL) {
        node = htree->root;
        htree->root = NULL;
        assert(node->tot_elem_cnt == 0);
    } else {
        assert(par_node->hcnt[par_hidx] == -1); /* child hash node */
        htree_elem_item *head = NULL;
        htree_elem_item *elem;
        int hidx, fcnt = 0;

        node = (htree_node *)par_node->htab[par_hidx];
        assert(node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2));

        for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
            assert(node->hcnt[hidx] >= 0);
            if (node->hcnt[hidx] > 0) {
                fcnt += node->hcnt[hidx];
                while (node->htab[hidx] != NULL) {
                    elem = node->htab[hidx];
                    node->htab[hidx] = elem->next;
                    node->hcnt[hidx] -= 1;

                    elem->next = head;
                    head = elem;
                }
                assert(node->hcnt[hidx] == 0);
            }
        }
        assert(fcnt == node->tot_elem_cnt);
        node->tot_elem_cnt = 0;

        par_node->htab[par_hidx] = head;
        par_node->hcnt[par_hidx] = fcnt;
    }

    /* free the node */
    do_htree_node_free(node);
}

static ENGINE_ERROR_CODE do_htree_elem_link(htree_meta *htree,
                                            htree_node *node, int hidx,
                                            htree_elem_item *elem, size_t *space_increased)
{
    if (node->hcnt[hidx] >= HTREE_MAX_HASHCHAIN_SIZE) {
        htree_node *n_node = do_htree_node_alloc(node->hdepth+1);
        if (n_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_htree_node_link(htree, node, hidx, n_node);
        *space_increased += slabs_space_size(sizeof(htree_node));

        node = n_node;
        hidx = HTREE_GET_HASHIDX(elem->hval, node->hdepth);
    }

    elem->linked++;
    elem->next = node->htab[hidx];
    node->htab[hidx] = elem;
    node->hcnt[hidx] += 1;
    node->tot_elem_cnt += 1;

    htree_node *par_node = htree->root;
    while (par_node != node) {
        par_node->tot_elem_cnt += 1;
        int par_hidx = HTREE_GET_HASHIDX(elem->hval, par_node->hdepth);
        assert(par_node->hcnt[par_hidx] == -1);
        par_node = par_node->htab[par_hidx];
    }
    return ENGINE_SUCCESS;
}

static void do_htree_elem_unlink(htree_node *node, const int hidx,
                                 htree_elem_item *prev, htree_elem_item *elem)
{
    if (prev != NULL) prev->next = elem->next;
    else              node->htab[hidx] = elem->next;
    elem->linked--;
    node->hcnt[hidx] -= 1;
    node->tot_elem_cnt -= 1;
}

static htree_elem_item *do_htree_elem_delete(htree_meta *htree, htree_node *node,
                                             const int hval, const char *key, const int nkey,
                                             size_t *space_decreased)
{
    htree_elem_item *deleted = NULL;
    int hidx = HTREE_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        htree_node *child_node = node->htab[hidx];
        deleted = do_htree_elem_delete(htree, child_node, hval, key, nkey, space_decreased);
        if (deleted) {
            if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2)) {
                do_htree_node_unlink(htree, node, hidx);
                *space_decreased += slabs_space_size(sizeof(htree_node));
            }
            node->tot_elem_cnt -= 1;
        }
    } else {
        if (node->hcnt[hidx] > 0) {
            htree_elem_item *prev = NULL;
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                uint16_t enkey;
                const void *ekey = htree->ops->get_key(elem, &enkey);
                if (htree_hash_eq(hval, key, nkey, elem->hval, ekey, enkey)) {
                    do_htree_elem_unlink(node, hidx, prev, elem);
                    deleted = elem;
                    break;
                }
                prev = elem;
                elem = elem->next;
            }
        }
    }
    return deleted;
}

static uint32_t do_htree_elem_delete_bulk(htree_meta *htree, htree_node *node, const uint32_t count,
                                          htree_elem_item **deleted_head,
                                          size_t *space_decreased)
{
    int hidx;
    uint32_t fcnt = 0;

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            uint32_t rcnt = (count > 0 ? (count - fcnt) : 0);
            uint32_t ecnt = do_htree_elem_delete_bulk(htree, child_node, rcnt, deleted_head, space_decreased);
            fcnt += ecnt;
            if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2)) {
                do_htree_node_unlink(htree, node, hidx);
                *space_decreased += slabs_space_size(sizeof(htree_node));
            }
            node->tot_elem_cnt -= ecnt;
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                fcnt++;
                do_htree_elem_unlink(node, hidx, NULL, elem);

                /* link deleted elem into list; returned to caller */
                elem->next = *deleted_head;
                *deleted_head = elem;

                if (count > 0 && fcnt >= count) break;
                elem = node->htab[hidx];
            }
        }
        if (count > 0 && fcnt >= count) break;
    }
    return fcnt;
}

static bool do_htree_elem_get(htree_meta *htree, htree_node *node,
                              const int hval, const void *key, const uint16_t nkey,
                              const bool delete, htree_elem_item **elem_array,
                              size_t *space_decreased)
{
    assert(elem_array != NULL);
    bool ret;
    int hidx = HTREE_GET_HASHIDX(hval, node->hdepth);

    if (node->hcnt[hidx] == -1) {
        htree_node *child_node = node->htab[hidx];
        ret = do_htree_elem_get(htree, child_node, hval, key, nkey, delete, elem_array, space_decreased);
        if (ret && delete) {
            if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2)) {
                do_htree_node_unlink(htree, node, hidx);
                *space_decreased += slabs_space_size(sizeof(htree_node));
            }
            node->tot_elem_cnt -= 1;
        }
    } else {
        ret = false;
        if (node->hcnt[hidx] > 0) {
            htree_elem_item *prev = NULL;
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                uint16_t enkey;
                const void *ekey = htree->ops->get_key(elem, &enkey);
                if (htree_hash_eq(hval, key, nkey, elem->hval, ekey, enkey)) {
                    elem->refcount++;
                    elem_array[0] = elem;
                    if (delete) {
                        do_htree_elem_unlink(node, hidx, prev, elem);
                    }
                    ret = true;
                    break;
                }
                prev = elem;
                elem = elem->next;
            }
        }
    }
    return ret;
}

static uint32_t do_htree_elem_get_bulk(htree_meta *htree, htree_node *node, const uint32_t count,
                                       const bool delete, htree_elem_item **elem_array,
                                       size_t *space_decreased)
{
    assert(elem_array != NULL);
    int hidx;
    size_t fcnt = 0; /* found count */

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            size_t rcnt = (count > 0 ? (count - fcnt) : 0);
            size_t ecnt = do_htree_elem_get_bulk(htree, child_node, rcnt, delete, &elem_array[fcnt], space_decreased);
            fcnt += ecnt;
            if (delete) {
                if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2)) {
                    do_htree_node_unlink(htree, node, hidx);
                    *space_decreased += slabs_space_size(sizeof(htree_node));
                }
                node->tot_elem_cnt -= ecnt;
            }
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                elem->refcount++;
                elem_array[fcnt] = elem;
                fcnt++;
                if (delete) do_htree_elem_unlink(node, hidx, NULL, elem);
                if (count > 0 && fcnt >= count) break;
                elem = (delete ? node->htab[hidx] : elem->next);
            }
        }
        if (count > 0 && fcnt >= count) break;
    }
    return fcnt;
}

static htree_elem_item *do_htree_elem_get_by_offset(htree_meta *htree, htree_node *node,
                                                    uint32_t offset, const bool delete,
                                                    size_t *space_decreased)
{
    int hidx;
    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            if (offset >= child_node->tot_elem_cnt) {
                offset -= child_node->tot_elem_cnt;
                continue;
            }
            htree_elem_item *found = do_htree_elem_get_by_offset(htree, child_node, offset, delete, space_decreased);
            if (delete) {
                if (child_node->tot_elem_cnt < (HTREE_MAX_HASHCHAIN_SIZE/2)) {
                    do_htree_node_unlink(htree, node, hidx);
                    *space_decreased += slabs_space_size(sizeof(htree_node));
                }
                node->tot_elem_cnt -= 1;
            }
            return found;
        } else if (node->hcnt[hidx] > 0) {
            if (offset >= node->hcnt[hidx]) {
                offset -= node->hcnt[hidx];
                continue;
            }
            htree_elem_item *prev = NULL;
            htree_elem_item *elem = node->htab[hidx];
            while (offset > 0) {
                prev = elem;
                elem = elem->next;
                offset -= 1;
            }
            elem->refcount++;
            if (delete) do_htree_elem_unlink(node, hidx, prev, elem);
            return elem;
        }
    }
    return NULL;
}

static int do_htree_elem_get_sampling(htree_meta *htree, htree_node *node,
                                      uint32_t remain, const uint32_t count,
                                      htree_elem_item **elem_array)
{
    int hidx;
    int fcnt = 0; /* found count */

    for (hidx = 0; hidx < HTREE_HASHTAB_SIZE; hidx++) {
        if (node->hcnt[hidx] == -1) {
            htree_node *child_node = (htree_node *)node->htab[hidx];
            fcnt += do_htree_elem_get_sampling(htree, child_node, remain,
                                               count - fcnt, &elem_array[fcnt]);
            remain -= child_node->tot_elem_cnt;
        } else if (node->hcnt[hidx] > 0) {
            htree_elem_item *elem = node->htab[hidx];
            while (elem != NULL) {
                if ((rand() % remain) < (count - fcnt)) {
                    elem->refcount++;
                    elem_array[fcnt] = elem;
                    fcnt++;
                    if (fcnt >= count) break;
                }
                remain -= 1;
                elem = elem->next;
            }
        }
        if (fcnt >= count) break;
    }
    return fcnt;
}

static uint32_t do_htree_elem_get_rand(htree_meta *htree, const uint32_t count,
                                       const bool delete, htree_elem_item **elem_array,
                                       size_t *space_decreased)
{
    assert(elem_array != NULL);
    uint32_t fcnt = 0;

    if (delete) { /* Deleting partial elements */
        while (fcnt < count) {
            int rand_offset = (rand() % htree->root->tot_elem_cnt);
            htree_elem_item *found = do_htree_elem_get_by_offset(htree, htree->root,
                                                                 rand_offset, delete, space_decreased);
            assert(found != NULL);
            elem_array[fcnt++] = found;
        }
    } else if (count <= htree->root->tot_elem_cnt / 10) { /* Use hash table */
        hash_table offset_ht;
        if (!hash_init(&offset_ht, count))
            return 0;
        while (fcnt < count) {
            int rand_offset = (rand() % htree->root->tot_elem_cnt);
            if (hash_insert(&offset_ht, rand_offset)) {
                htree_elem_item *found = do_htree_elem_get_by_offset(htree, htree->root,
                                                                     rand_offset, false, NULL);
                assert(found != NULL);
                elem_array[fcnt++] = found;
            }
        }
        hash_free(&offset_ht);
    } else { /* Use sampling */
        fcnt = do_htree_elem_get_sampling(htree, htree->root, htree->root->tot_elem_cnt, count,
                                          elem_array);
        for (int i = fcnt - 1; i > 0; i--) {
            int rand_idx = rand() % (i + 1);
            if (rand_idx != i) {
                htree_elem_item *temp = elem_array[i];
                elem_array[i] = elem_array[rand_idx];
                elem_array[rand_idx] = temp;
            }
        }
    }
    return fcnt;
}

static void do_htree_traverse_advance(htree_elem_posi *posi)
{
    while (posi->depth >= 0) {
        htree_node *node = posi->path[posi->depth].node;
        int hidx;

        for (hidx = posi->path[posi->depth].hidx; hidx < HTREE_HASHTAB_SIZE; hidx++) {
            if (node->hcnt[hidx] == -1) {
                posi->path[posi->depth].hidx = hidx + 1;
                posi->depth++;
                posi->path[posi->depth].node = (htree_node *)node->htab[hidx];
                posi->path[posi->depth].hidx = 0;
                break;
            } else if (node->hcnt[hidx] > 0) {
                posi->path[posi->depth].hidx = hidx + 1;
                posi->curr = (htree_elem_item *)node->htab[hidx];
                return;
            }
        }
        if (hidx >= HTREE_HASHTAB_SIZE) posi->depth--;
    }
    posi->curr = NULL;
}

/*
 * Hash tree Interface Functions
 */
void htree_init(htree_meta *htree, htree_ops *ops)
{
    htree->root = NULL;
    htree->ops  = ops;
}

htree_elem_item *htree_elem_find(htree_meta *htree,
                                 const int hval,
                                 const void *key, uint16_t nkey,
                                 htree_prev_info *pinfo)
{
    if (htree->root == NULL) {
        return NULL;
    }

    htree_node *node = htree->root;
    int hidx;

    while (node != NULL) {
        hidx = HTREE_GET_HASHIDX(hval, node->hdepth);
        if (node->hcnt[hidx] >= 0)
            break;
        node = node->htab[hidx];
    }

    htree_elem_item *prev = NULL;
    htree_elem_item *elem;
    for (elem = node->htab[hidx]; elem != NULL; elem = elem->next) {
        uint16_t enkey;
        const void *ekey = htree->ops->get_key(elem, &enkey);
        if (htree_hash_eq(hval, key, nkey, elem->hval, ekey, enkey))
            break;
        prev = elem;
    }

    if (pinfo != NULL) {
        pinfo->node = node;
        pinfo->prev = prev;
        pinfo->hidx = hidx;
    }

    return elem;
}

ENGINE_ERROR_CODE htree_elem_insert(htree_meta *htree, htree_elem_item *elem,
                                    size_t *space_increased)
{
    htree_node *node;
    int hidx;

    /* create the root hash node if it does not exist */
    if (htree->root == NULL) {
        node = do_htree_node_alloc(0);
        if (node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_htree_node_link(htree, NULL, 0, node);
        *space_increased += slabs_space_size(sizeof(htree_node));

        hidx = HTREE_GET_HASHIDX(elem->hval, 0);
        return do_htree_elem_link(htree, node, hidx, elem, space_increased);
    }

    htree_elem_item *find;
    htree_prev_info pinfo;

    uint16_t nkey;
    const void *key = htree->ops->get_key(elem, &nkey);
    find = htree_elem_find(htree, elem->hval, key, nkey, &pinfo);
    if (find != NULL) {
        return ENGINE_ELEM_EEXISTS;
    }

    node = pinfo.node;
    hidx = pinfo.hidx;
    return do_htree_elem_link(htree, node, hidx, elem, space_increased);
}

ENGINE_ERROR_CODE htree_elem_add(htree_meta *htree, htree_elem_item *elem,
                                 htree_prev_info *pinfo, size_t *space_increased)
{
    htree_node *node;
    int hidx;

    if (htree->root == NULL) {
        node = do_htree_node_alloc(0);
        if (node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_htree_node_link(htree, NULL, 0, node);
        *space_increased += slabs_space_size(sizeof(htree_node));

        hidx = HTREE_GET_HASHIDX(elem->hval, 0);
        return do_htree_elem_link(htree, node, hidx, elem, space_increased);
    }

    node = pinfo->node;
    hidx = pinfo->hidx;
    return do_htree_elem_link(htree, node, hidx, elem, space_increased);
}

htree_elem_item *htree_elem_replace(htree_prev_info *pinfo, htree_elem_item *new_elem)
{
    htree_elem_item *prev = pinfo->prev;
    htree_elem_item *old_elem;
    int hidx = pinfo->hidx;

    if (prev != NULL) {
        old_elem = prev->next;
    } else {
        old_elem = (htree_elem_item *)pinfo->node->htab[hidx];
    }
    assert(new_elem->hval == old_elem->hval);

    new_elem->next = old_elem->next;
    if (prev != NULL) {
        prev->next = new_elem;
    } else {
        pinfo->node->htab[hidx] = new_elem;
    }
    new_elem->linked++;

    old_elem->linked--;
    assert(old_elem->linked == 0);

    return old_elem;
}

htree_elem_item *htree_elem_delete(htree_meta *htree,
                                   const int hval, const char *key, const int nkey,
                                   size_t *space_decreased)
{
    htree_elem_item *deleted = do_htree_elem_delete(htree, htree->root, hval, key, nkey, space_decreased);

    if (deleted != NULL && htree->root->tot_elem_cnt == 0) {
        do_htree_node_unlink(htree, NULL, 0);
        *space_decreased += slabs_space_size(sizeof(htree_node));
    }
    return deleted;
}

uint32_t htree_elem_delete_bulk(htree_meta *htree, const uint32_t count,
                                htree_elem_item **deleted_head,
                                size_t *space_decreased)
{
    int fcnt = do_htree_elem_delete_bulk(htree, htree->root, count, deleted_head, space_decreased);

    if (fcnt > 0 && htree->root->tot_elem_cnt == 0) {
        do_htree_node_unlink(htree, NULL, 0);
        *space_decreased += slabs_space_size(sizeof(htree_node));
    }

    return fcnt;
}

bool htree_elem_get(htree_meta *htree,
                    const int hval, const void *key, const uint16_t nkey,
                    const bool delete, htree_elem_item **elem_array,
                    size_t *space_decreased)
{
    bool ret = do_htree_elem_get(htree, htree->root, hval, key, nkey, delete, elem_array, space_decreased);

    if (delete) {
        if (ret && htree->root->tot_elem_cnt == 0) {
            do_htree_node_unlink(htree, NULL, 0);
            *space_decreased += slabs_space_size(sizeof(htree_node));
        }
    }

    return ret;
}

uint32_t htree_elem_get_bulk(htree_meta *htree, const uint32_t count,
                             const bool delete, htree_elem_item **elem_array,
                             size_t *space_decreased)
{
    uint32_t fcnt = do_htree_elem_get_bulk(htree, htree->root, count, delete, elem_array, space_decreased);

    if (delete) {
        if (fcnt > 0 && htree->root->tot_elem_cnt == 0) {
            do_htree_node_unlink(htree, NULL, 0);
            *space_decreased += slabs_space_size(sizeof(htree_node));
        }
    }

    return fcnt;
}

uint32_t htree_elem_get_rand(htree_meta *htree, const uint32_t count,
                             const bool delete, htree_elem_item **elem_array,
                             size_t *space_decreased)
{
    uint32_t fcnt = do_htree_elem_get_rand(htree, count, delete, elem_array, space_decreased);

    if (delete) {
        if (fcnt > 0 && htree->root->tot_elem_cnt == 0) {
            do_htree_node_unlink(htree, NULL, 0);
            *space_decreased += slabs_space_size(sizeof(htree_node));
        }
    }

    return fcnt;
}

void htree_traverse_init(htree_meta *htree, void *posi)
{
    htree_elem_posi *ep = (htree_elem_posi *)posi;
    if (htree->root == NULL) {
        ep->curr = NULL;
        return;
    }
    ep->path[0].node = htree->root;
    ep->path[0].hidx = 0;
    ep->depth = 0;
    do_htree_traverse_advance(ep);
}

uint32_t htree_traverse_next(void *posi, void **elem_array, uint32_t count)
{
    htree_elem_posi *ep = (htree_elem_posi *)posi;
    uint32_t fcnt = 0;
    while (fcnt < count && ep->curr != NULL) {
        ep->curr->refcount++;
        elem_array[fcnt++] = ep->curr;
        if (ep->curr->next != NULL)
            ep->curr = ep->curr->next;
        else
            do_htree_traverse_advance(ep);
    }
    return fcnt;
}
