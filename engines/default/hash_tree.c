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
#include <assert.h>

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
                                 htree_elem_pos *pos)
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

    if (pos != NULL) {
        pos->node = node;
        pos->prev = prev;
        pos->hidx = hidx;
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
    htree_elem_pos pos;

    uint16_t nkey;
    const void *key = htree->ops->get_key(elem, &nkey);
    find = htree_elem_find(htree, elem->hval, key, nkey, &pos);
    if (find != NULL) {
        return ENGINE_ELEM_EEXISTS;
    }

    node = pos.node;
    hidx = pos.hidx;
    return do_htree_elem_link(htree, node, hidx, elem, space_increased);
}

ENGINE_ERROR_CODE htree_elem_add(htree_meta *htree, htree_elem_item *elem,
                                 htree_elem_pos *pos, size_t *space_increased)
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

    node = pos->node;
    hidx = pos->hidx;
    return do_htree_elem_link(htree, node, hidx, elem, space_increased);
}

htree_elem_item *htree_elem_replace(htree_elem_pos *pos, htree_elem_item *new_elem)
{
    htree_elem_item *prev = pos->prev;
    htree_elem_item *old_elem;
    int hidx = pos->hidx;

    if (prev != NULL) {
        old_elem = prev->next;
    } else {
        old_elem = (htree_elem_item *)pos->node->htab[hidx];
    }
    assert(new_elem->hval == old_elem->hval);

    new_elem->next = old_elem->next;
    if (prev != NULL) {
        prev->next = new_elem;
    } else {
        pos->node->htab[hidx] = new_elem;
    }
    new_elem->linked++;

    old_elem->linked--;
    assert(old_elem->linked == 0);

    return old_elem;
}
