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
#include "ds_btree.h"
#include "default_engine.h"
#include <assert.h>
#include <string.h>

/* btree scan direction */
#define DS_BTREE_DIRECTION_PREV 2
#define DS_BTREE_DIRECTION_NEXT 1
#define DS_BTREE_DIRECTION_NONE 0

/* btree position debugging */
static bool ds_btree_position_debug = false;

bool (*BINARY_COMPARE_OP[COMPARE_OP_MAX])(const unsigned char *v1, const int nv1,
                                          const unsigned char *v2, const int nv2)
    = { BINARY_ISEQ, BINARY_ISNE, BINARY_ISLT, BINARY_ISLE, BINARY_ISGT, BINARY_ISGE };

void (*BINARY_BITWISE_OP[BITWISE_OP_MAX])(const unsigned char *v1, const unsigned char *v2,
                                          const int length, unsigned char *result)
    = { BINARY_AND, BINARY_OR, BINARY_XOR };

/*
 * Btree management
 */
static ds_btree_indx_node *_ds_btree_node_alloc(const uint8_t node_depth)
{
    size_t ntotal = (node_depth > 0 ? sizeof(ds_btree_indx_node) : sizeof(ds_btree_leaf_node));

    ds_btree_indx_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL);
    if (node != NULL) {
        node->slabs_clsid = slabs_clsid(ntotal);
        assert(node->slabs_clsid > 0);

        node->refcount    = 0;
        node->ndepth      = node_depth;
        node->used_count  = 0;
        node->prev = node->next = NULL;
        memset(node->item, 0, DS_BTREE_ITEM_COUNT*sizeof(void*));
        if (node_depth > 0)
            memset(node->ecnt, 0, DS_BTREE_ITEM_COUNT*sizeof(uint16_t));
    }
    return node;
}

static inline void _ds_btree_incr_posi(ds_btree_elem_posi *posi)
{
    if (posi->indx < (posi->node->used_count-1)) {
        posi->indx += 1;
    } else {
        posi->node = posi->node->next;
        posi->indx = 0;
    }
}

static inline void _ds_btree_decr_posi(ds_btree_elem_posi *posi)
{
    if (posi->indx > 0) {
        posi->indx -= 1;
    } else {
        posi->node = posi->node->prev;
        if (posi->node != NULL)
            posi->indx = posi->node->used_count-1;
        else
            posi->indx = DS_BTREE_ITEM_COUNT;
    }
}

static void _ds_btree_incr_path(ds_btree_elem_posi *path, int depth)
{
    ds_btree_indx_node *saved_node;

    while (depth < DS_BTREE_MAX_DEPTH) {
        saved_node = path[depth].node;
        _ds_btree_incr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < DS_BTREE_MAX_DEPTH);
}

static void _ds_btree_decr_path(ds_btree_elem_posi *path, int depth)
{
    ds_btree_indx_node *saved_node;

    while (depth < DS_BTREE_MAX_DEPTH) {
        saved_node = path[depth].node;
        _ds_btree_decr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < DS_BTREE_MAX_DEPTH);
}

static ds_btree_indx_node *_ds_btree_find_leaf(ds_btree_meta *btree,
                                               const void *bkey, const uint32_t nbkey,
                                               ds_btree_elem_posi *path,
                                               ds_btree_elem_item **found_elem)
{
    ds_btree_indx_node *node = btree->root;
    ds_btree_ops *ops = btree->ops;
    ds_btree_elem_item *elem;
    int mid, left, right, comp;
    const void *sep_bkey;
    uint32_t sep_nbkey;

    *found_elem = NULL; /* the same bkey is not found */

    while (node->ndepth > 0) {
        left  = 1;
        right = node->used_count-1;

        while (left <= right) {
            mid  = (left + right) / 2;
            elem = ds_btree_get_first_elem(node->item[mid]); /* separator */
            sep_bkey = ops->get_bkey(elem, &sep_nbkey);
            comp = ops->bkey_cmp(bkey, nbkey, sep_bkey, sep_nbkey);
            if (comp == 0) { /* the same bkey is found */
                *found_elem = elem;
                if (path) {
                    path[node->ndepth].node = node;
                    path[node->ndepth].indx = mid;
                }
                node = ds_btree_get_first_leaf(node->item[mid], path);
                assert(node->ndepth == 0);
                break;
            }
            if (comp <  0) right = mid-1;
            else           left  = mid+1;
        }
        if (left <= right) { /* found the element */
            break;
        }

        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = right;
        }
        node = (ds_btree_indx_node *)(node->item[right]);
    }
    return node;
}

static void _ds_btree_consistency_check(ds_btree_meta *btree,
                                        ds_btree_indx_node *node, uint32_t ecount, bool detail)
{
    uint32_t i, tot_ecnt;

    if (node == NULL) { /* root node */
        assert(ecount == 0);
        return;
    }

    if (node->prev != NULL) {
        assert(node->prev->next == node);
    }
    if (node->next != NULL) {
        assert(node->next->prev == node);
    }
    if (node->ndepth > 0) { /* nonleaf page check */
        tot_ecnt = 0;
        for (i = 0; i < node->used_count; i++) {
            assert(node->item[i] != NULL);
            assert(node->ecnt[i] > 0);
            _ds_btree_consistency_check(btree, (ds_btree_indx_node*)node->item[i], node->ecnt[i], detail);
            tot_ecnt += node->ecnt[i];
        }
        assert(tot_ecnt == ecount);
    } else { /* node->ndepth == 0: leaf page check */
        for (i = 0; i < node->used_count; i++) {
            assert(node->item[i] != NULL);
        }
        assert(node->used_count == ecount);
        if (detail) {
            ds_btree_ops *ops = btree->ops;
            ds_btree_elem_item *p_elem;
            ds_btree_elem_item *c_elem;
            const void *p_bkey, *c_bkey;
            uint32_t    p_nbkey, c_nbkey;
            int comp;

            if (node->prev == NULL) {
                p_elem = NULL;
            } else {
                p_elem = DS_BTREE_GET_ELEM_ITEM(node->prev, node->prev->used_count-1);
            }
            for (i = 0; i < node->used_count; i++) {
                c_elem = DS_BTREE_GET_ELEM_ITEM(node, i);
                if (p_elem != NULL) {
                    p_bkey = ops->get_bkey(p_elem, &p_nbkey);
                    c_bkey = ops->get_bkey(c_elem, &c_nbkey);
                    comp = ops->bkey_cmp(p_bkey, p_nbkey, c_bkey, c_nbkey);
                    assert(comp < 0);
                }
                p_elem = c_elem;
            }
            if (node->next == NULL) {
                c_elem = NULL;
            } else {
                c_elem = DS_BTREE_GET_ELEM_ITEM(node->next, 0);
            }
            if (c_elem != NULL) {
                p_bkey = ops->get_bkey(p_elem, &p_nbkey);
                c_bkey = ops->get_bkey(c_elem, &c_nbkey);
                comp = ops->bkey_cmp(p_bkey, p_nbkey, c_bkey, c_nbkey);
                assert(comp < 0);
            }
        }
    }
}

static void _ds_btree_node_item_move(ds_btree_indx_node *c_node, /* current node */
                                     ds_btree_indx_node *n_node, /* neighbor node */
                                     int direction, int move_count)
{
    assert(move_count > 0);
    int i;

    if (direction == DS_BTREE_DIRECTION_NEXT) {
        if (c_node->ndepth == 0) { /* leaf node */
            for (i = (n_node->used_count-1); i >= 0; i--) {
                n_node->item[move_count+i] = n_node->item[i];
            }
            for (i = 0; i < move_count; i++) {
                n_node->item[i] = c_node->item[c_node->used_count-move_count+i];
                c_node->item[c_node->used_count-move_count+i] = NULL;
            }
        } else { /* c_node->ndepth > 0: nonleaf node */
            for (i = (n_node->used_count-1); i >= 0; i--) {
                n_node->item[move_count+i] = n_node->item[i];
                n_node->ecnt[move_count+i] = n_node->ecnt[i];
            }
            for (i = 0; i < move_count; i++) {
                n_node->item[i] = c_node->item[c_node->used_count-move_count+i];
                c_node->item[c_node->used_count-move_count+i] = NULL;
                n_node->ecnt[i] = c_node->ecnt[c_node->used_count-move_count+i];
                c_node->ecnt[c_node->used_count-move_count+i] = 0;
            }
        }
    } else { /* DS_BTREE_DIRECTION_PREV */
        if (c_node->ndepth == 0) { /* leaf node */
            for (i = 0; i < move_count; i++) {
                n_node->item[n_node->used_count+i] = c_node->item[i];
            }
            for (i = move_count; i < c_node->used_count; i++) {
                c_node->item[i-move_count] = c_node->item[i];
                c_node->item[i] = NULL;
            }
        } else { /* c_node->ndepth > 0: nonleaf node */
            for (i = 0; i < move_count; i++) {
                n_node->item[n_node->used_count+i] = c_node->item[i];
                n_node->ecnt[n_node->used_count+i] = c_node->ecnt[i];
            }
            for (i = move_count; i < c_node->used_count; i++) {
                c_node->item[i-move_count] = c_node->item[i];
                c_node->item[i] = NULL;
                c_node->ecnt[i-move_count] = c_node->ecnt[i];
                c_node->ecnt[i] = 0;
            }
        }
    }
    n_node->used_count += move_count;
    c_node->used_count -= move_count;
}

static void _ds_btree_ecnt_move_split(ds_btree_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    ds_btree_elem_posi  posi;
    ds_btree_indx_node *saved_node;

    while (depth < DS_BTREE_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == DS_BTREE_DIRECTION_NEXT) {
            _ds_btree_incr_posi(&posi);
        } else {
            _ds_btree_decr_posi(&posi);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < DS_BTREE_MAX_DEPTH);
}

static void _ds_btree_ecnt_move_merge(ds_btree_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    ds_btree_elem_posi  posi;
    ds_btree_indx_node *saved_node;

    while (depth < DS_BTREE_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == DS_BTREE_DIRECTION_NEXT) {
            do {
                _ds_btree_incr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        } else {
            do {
                _ds_btree_decr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < DS_BTREE_MAX_DEPTH);
}

static void _ds_btree_node_sbalance(ds_btree_indx_node *node, ds_btree_elem_posi *path, int depth)
{
    ds_btree_elem_posi *posi;
    int direction;
    int move_count;
    int elem_count; /* total count of elements moved */
    int i;

    /* balance the number of elements with neighber node */
    if (node->next != NULL && node->prev != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     DS_BTREE_DIRECTION_NEXT : DS_BTREE_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     DS_BTREE_DIRECTION_NEXT : DS_BTREE_DIRECTION_PREV);
    }
    if (direction == DS_BTREE_DIRECTION_NEXT) {
        if (node->next->used_count > 0) {
            move_count = (node->used_count - node->next->used_count) / 2;
        } else {
            move_count = (node->next->next == NULL ? (node->used_count / 10)
                                                   : (node->used_count / 2));
        }
        if (move_count == 0) move_count = 1;

        if (depth == 0) {
            elem_count = move_count;
        } else {
            elem_count = 0;
            for (i = 0; i < move_count; i++) {
                elem_count += node->ecnt[node->used_count-move_count+i];
            }
        }

        _ds_btree_node_item_move(node, node->next, direction, move_count);

        /* move element count in upper btree nodes */
        _ds_btree_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx >= node->used_count) {
            posi->node = node->next;
            posi->indx -= node->used_count;
            /* adjust upper path info */
            _ds_btree_incr_path(path, depth+1);
        }
    } else {
        if (node->prev->used_count > 0) {
            move_count = (node->used_count - node->prev->used_count) / 2;
        } else {
            move_count = (node->prev->prev == NULL ? (node->used_count / 10)
                                                   : (node->used_count / 2));
        }
        if (move_count == 0) move_count = 1;

        if (depth == 0) {
            elem_count = move_count;
        } else {
            elem_count = 0;
            for (i = 0; i < move_count; i++) {
                elem_count += node->ecnt[i];
            }
        }

        _ds_btree_node_item_move(node, node->prev, direction, move_count);

        /* move element count in upper btree nodes */
        _ds_btree_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx < move_count) {
            posi->node = node->prev;
            posi->indx += (node->prev->used_count-move_count);
            /* adjust upper path info */
            _ds_btree_decr_path(path, depth+1);
        } else {
            posi->indx -= move_count;
        }
    }
}

static void _ds_btree_node_link(ds_btree_meta *btree, ds_btree_indx_node *node,
                                ds_btree_elem_posi *p_posi, size_t *space_increased)
{
    /*
     * p_posi: the position of to-be-linked node in parent node.
     */
    if (p_posi == NULL) {
        /* No parent node : make a new root node */
        if (btree->root == NULL) {
            node->used_count = 0;
        } else {
            node->item[0] = btree->root;
            node->ecnt[0] = btree->tot_elem_cnt;
            node->used_count = 1;
        }
        btree->root = node;
    } else {
        /* Parent node exists */
        ds_btree_indx_node *p_node = p_posi->node;
        assert(p_node->used_count >= 1);
        assert(p_posi->indx <= p_node->used_count);

        if (p_posi->indx == 0) {
            node->prev = (p_node->prev == NULL ?
                          NULL : p_node->prev->item[p_node->prev->used_count-1]);
            node->next = p_node->item[p_posi->indx];
        } else if (p_posi->indx < p_node->used_count) {
            node->prev = p_node->item[p_posi->indx-1];
            node->next = p_node->item[p_posi->indx];
        } else { /* p_posi->index == p_node->used_count */
            node->prev = p_node->item[p_posi->indx-1];
            node->next = (p_node->next == NULL ?
                          NULL : p_node->next->item[0]);
        }
        if (node->prev != NULL) node->prev->next = node;
        if (node->next != NULL) node->next->prev = node;

        for (int i = (p_node->used_count-1); i >= p_posi->indx; i--) {
            p_node->item[i+1] = p_node->item[i];
            p_node->ecnt[i+1] = p_node->ecnt[i];
        }
        p_node->item[p_posi->indx] = node;
        p_node->ecnt[p_posi->indx] = 0;
        p_node->used_count++;
    }

    if (node->ndepth > 0) *space_increased += slabs_space_size(sizeof(ds_btree_indx_node));
    else                  *space_increased += slabs_space_size(sizeof(ds_btree_leaf_node));
}

static ENGINE_ERROR_CODE _ds_btree_node_split(ds_btree_meta *btree, ds_btree_elem_posi *path, size_t *space_increased)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    ds_btree_indx_node *s_node;
    ds_btree_indx_node *n_node[DS_BTREE_MAX_DEPTH]; /* neighber nodes */
    ds_btree_elem_posi  p_posi;
    int     i, direction;
    uint8_t btree_depth = 0;

    s_node = path[btree_depth].node;
    do {
        if ((s_node->next != NULL && s_node->next->used_count < (DS_BTREE_ITEM_COUNT/2)) ||
            (s_node->prev != NULL && s_node->prev->used_count < (DS_BTREE_ITEM_COUNT/2))) {
            _ds_btree_node_sbalance(s_node, path, btree_depth);
            break;
        }

        n_node[btree_depth] = _ds_btree_node_alloc(btree_depth);
        if (n_node[btree_depth] == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        btree_depth += 1;
        assert(btree_depth < DS_BTREE_MAX_DEPTH);
        if (btree_depth > btree->root->ndepth) {
            ds_btree_indx_node *r_node = _ds_btree_node_alloc(btree_depth);
            if (r_node == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            _ds_btree_node_link(btree, r_node, NULL, space_increased);

            path[btree_depth].node = r_node;
            path[btree_depth].indx = 0;
            break;
        }
        s_node = path[btree_depth].node;
    }
    while (s_node->used_count >= DS_BTREE_ITEM_COUNT);

    if (ret == ENGINE_SUCCESS) {
        for (i = btree_depth-1; i >= 0; i--) {
            s_node = path[i].node;
            if (s_node->prev == NULL && s_node->next == NULL) {
                direction = (path[i].indx < (DS_BTREE_ITEM_COUNT/2) ?
                             DS_BTREE_DIRECTION_PREV : DS_BTREE_DIRECTION_NEXT);
            } else {
                direction = (s_node->prev == NULL ?
                             DS_BTREE_DIRECTION_PREV : DS_BTREE_DIRECTION_NEXT);
            }
            p_posi = path[i+1];
            if (direction == DS_BTREE_DIRECTION_NEXT) p_posi.indx += 1;
            _ds_btree_node_link(btree, n_node[i], &p_posi, space_increased);

            if (direction == DS_BTREE_DIRECTION_PREV) {
                /* adjust upper path */
                path[i+1].indx += 1;
                //_ds_btree_incr_path(path, i+1);
            }
            _ds_btree_node_sbalance(s_node, path, i);
        }
    } else {
        for (i = 0; i < btree_depth; i++) {
            ds_btree_node_free(n_node[i]);
        }
    }
    if (ds_btree_position_debug) {
        _ds_btree_consistency_check(btree, btree->root, btree->tot_elem_cnt, true);
    }
    return ret;
}

/* merge check */
static void _ds_btree_node_mbalance(ds_btree_indx_node *node, ds_btree_elem_posi *path, int depth)
{
    int direction;

    if (node->prev != NULL && node->next != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     DS_BTREE_DIRECTION_NEXT : DS_BTREE_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     DS_BTREE_DIRECTION_NEXT : DS_BTREE_DIRECTION_PREV);
    }
    if (direction == DS_BTREE_DIRECTION_NEXT) {
        _ds_btree_node_item_move(node, node->next, direction, node->used_count);
    } else {
        _ds_btree_node_item_move(node, node->prev, direction, node->used_count);
    }

    int elem_count = path[depth+1].node->ecnt[path[depth+1].indx];
    _ds_btree_ecnt_move_merge(path, depth+1, direction, elem_count);
}

static void _ds_btree_node_unlink(ds_btree_meta *btree, ds_btree_indx_node *node,
                                  ds_btree_elem_posi *p_posi, size_t *space_decreased)
{
    if (p_posi == NULL) {
        /* No parent node : remove the root node */
        btree->root = NULL;
    } else {
        /* unlink the given node from b+tree */
        if (node->prev != NULL) node->prev->next = node->next;
        if (node->next != NULL) node->next->prev = node->prev;
        node->prev = node->next = NULL;

        /* Parent node exists */
        ds_btree_indx_node *p_node = p_posi->node;
        assert(p_node->ecnt[p_posi->indx] == 0);
        for (int i = p_posi->indx+1; i < p_node->used_count; i++) {
            p_node->item[i-1] = p_node->item[i];
            p_node->ecnt[i-1] = p_node->ecnt[i];
        }
        p_node->item[p_node->used_count-1] = NULL;
        p_node->ecnt[p_node->used_count-1] = 0;
        p_node->used_count--;
    }

    if (node->ndepth > 0) *space_decreased += slabs_space_size(sizeof(ds_btree_indx_node));
    else                  *space_decreased += slabs_space_size(sizeof(ds_btree_leaf_node));

    /* The amount of space to be decreased become different according to node depth.
     * So, the btree node must be freed after collection space is decreased.
     */
    ds_btree_node_free(node);
}

static void _ds_btree_node_detach(ds_btree_indx_node *node, size_t *space_decreased)
{
    /* unlink the given node from b+tree */
    if (node->prev != NULL) node->prev->next = node->next;
    if (node->next != NULL) node->next->prev = node->prev;
    node->prev = node->next = NULL;

    if (node->ndepth > 0) *space_decreased += slabs_space_size(sizeof(ds_btree_indx_node));
    else                  *space_decreased += slabs_space_size(sizeof(ds_btree_leaf_node));

    /* The amount of space to be decreased become different according to node depth.
     * So, the btree node must be freed after collection space is decreased.
     */
    ds_btree_node_free(node);
}

static inline void _ds_btree_node_remove_null_items(ds_btree_elem_posi *posi, const bool forward, const int null_count)
{
    ds_btree_indx_node *node = posi->node;
    assert(null_count <= node->used_count);

    if (null_count < node->used_count) {
        int f, i;
        int rem_count = 0;
        f = (forward ? posi->indx : 0);
        for ( ; f < node->used_count; f++) {
            if (node->item[f] == NULL) {
                rem_count++;
                break;
            }
        }
        for (i = f+1; i < node->used_count; i++) {
            if (node->item[i] != NULL) {
                node->item[f] = node->item[i];
                node->item[i] = NULL;
                if (node->ndepth > 0) {
                    node->ecnt[f] = node->ecnt[i];
                    node->ecnt[i] = 0;
                }
                f++;
            } else {
                rem_count++;
            }
        }
        assert(rem_count == null_count);
    }
    node->used_count -= null_count;
}

static void _ds_btree_node_merge(ds_btree_meta *btree, ds_btree_elem_posi *path,
                                 const bool forward, const int leaf_node_count,
                                 size_t *space_decreased)
{
    ds_btree_indx_node *node;
    int cur_node_count = leaf_node_count;
    int par_node_count;
    uint8_t btree_depth = 0;

    /*
     * leaf_node_count : # of leaf nodes to be merged.
     * cur_node_count  : # of current nodes to be merged in the current btree depth.
     * par_node_count  : # of parent nodes that might be merged after the current merge.
     */
    while (cur_node_count > 0)
    {
        par_node_count = 0;
        if (cur_node_count == 1) {
            node = path[btree_depth].node;
            if (node == btree->root) {
                if (node->used_count == 0) {
                    _ds_btree_node_unlink(btree, node, NULL, space_decreased);
                } else {
                    ds_btree_indx_node *new_root;
                    while (node->used_count == 1 && node->ndepth > 0) {
                        new_root = DS_BTREE_GET_NODE_ITEM(node, 0);
                        _ds_btree_node_unlink(btree, node, NULL, space_decreased);
                        btree->root = new_root;
                        node = new_root;
                    }
                }
            } else {
                if (node->used_count == 0) {
                    _ds_btree_node_unlink(btree, node, &path[btree_depth+1], space_decreased);
                    par_node_count = 1;
                }
                else if (node->used_count < (DS_BTREE_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (DS_BTREE_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (DS_BTREE_ITEM_COUNT/2))) {
                        _ds_btree_node_mbalance(node, path, btree_depth);
                        _ds_btree_node_unlink(btree, node, &path[btree_depth+1], space_decreased);
                        par_node_count = 1;
                    }
                }
            }
        } else { /* cur_node_count > 1 */
            ds_btree_elem_posi  upth[DS_BTREE_MAX_DEPTH] = {{ 0 }}; /* upper node path */
            ds_btree_elem_posi  s_posi;
            int cur_unlink_cnt = 0;
            int i, upp_depth = btree_depth+1;

            /* prepare upper node path */
            for (i = upp_depth; i <= btree->root->ndepth; i++) {
                upth[i] = path[i];
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = DS_BTREE_GET_NODE_ITEM(s_posi.node, s_posi.indx);
                assert(node != NULL);

                if (node->used_count == 0) {
                    _ds_btree_node_detach(node, space_decreased);
                    s_posi.node->item[s_posi.indx] = NULL;
                    assert(s_posi.node->ecnt[s_posi.indx] == 0);
                }

                if (i == cur_node_count) break;

                if (forward) _ds_btree_incr_posi(&s_posi);
                else         _ds_btree_decr_posi(&s_posi);
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = DS_BTREE_GET_NODE_ITEM(upth[upp_depth].node, upth[upp_depth].indx);
                if (node == NULL) {
                    cur_unlink_cnt++;
                }
                else if (node->used_count < (DS_BTREE_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (DS_BTREE_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (DS_BTREE_ITEM_COUNT/2))) {
                        _ds_btree_node_mbalance(node, upth, btree_depth);
                        _ds_btree_node_detach(node, space_decreased);
                        upth[upp_depth].node->item[upth[upp_depth].indx] = NULL;
                        assert(upth[upp_depth].node->ecnt[upth[upp_depth].indx] == 0);
                        cur_unlink_cnt++;
                    }
                }

                if (i == cur_node_count) break;

                if (forward) _ds_btree_incr_path(upth, upp_depth);
                else         _ds_btree_decr_path(upth, upp_depth);

                if (s_posi.node != upth[upp_depth].node) {
                    if (cur_unlink_cnt > 0) {
                        _ds_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                        cur_unlink_cnt = 0;
                    }
                    s_posi = upth[upp_depth];
                    par_node_count += 1;
                }
            }
            if (cur_unlink_cnt > 0) {
                _ds_btree_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                par_node_count += 1;
            }
        }
        btree_depth += 1;
        cur_node_count = par_node_count;
    }
    if (ds_btree_position_debug) {
        _ds_btree_consistency_check(btree, btree->root, btree->tot_elem_cnt, true);
    }
}

static ENGINE_ERROR_CODE _ds_btree_elem_link(ds_btree_meta *btree,
                                             ds_btree_elem_posi *path, ds_btree_elem_item *elem,
                                             size_t *space_increased)
{
    /* If the leaf node is full of elements, split it ahead. */
    if (path[0].node->used_count >= DS_BTREE_ITEM_COUNT) {
        ENGINE_ERROR_CODE ret = _ds_btree_node_split(btree, path, space_increased);
        if (ret != ENGINE_SUCCESS) {
            return ret;
        }
    }

    /* insert the element into the leaf page */
    elem->linked++;
    if (path[0].indx < path[0].node->used_count) {
        for (int i = (path[0].node->used_count-1); i >= path[0].indx; i--) {
            path[0].node->item[i+1] = path[0].node->item[i];
        }
    }
    path[0].node->item[path[0].indx] = elem;
    path[0].node->used_count++;
    /* increment element count in upper nodes */
    for (int i = 1; i <= btree->root->ndepth; i++) {
        path[i].node->ecnt[path[i].indx]++;
    }
    btree->tot_elem_cnt++;

    return ENGINE_SUCCESS;
}

static void _ds_btree_elem_unlink(ds_btree_meta *btree, ds_btree_elem_posi *path,
                                  size_t *space_decreased)
{
    ds_btree_elem_posi *posi = &path[0];
    ds_btree_elem_item *elem = DS_BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    int i;

    elem->linked--;

    /* remove the element from the leaf node */
    ds_btree_indx_node *node = posi->node;
    for (i = posi->indx+1; i < node->used_count; i++) {
        node->item[i-1] = node->item[i];
    }
    node->item[node->used_count-1] = NULL;
    node->used_count--;
    /* decrement element count in upper nodes */
    for (i = 1; i <= btree->root->ndepth; i++) {
        path[i].node->ecnt[path[i].indx]--;
    }
    btree->tot_elem_cnt--;

    if (node->used_count < (DS_BTREE_ITEM_COUNT/2)) {
        _ds_btree_node_merge(btree, path, true, 1, space_decreased);
    }
}

static int _ds_btree_posi_from_path(ds_btree_meta *btree,
                                    ds_btree_elem_posi *path, ENGINE_BTREE_ORDER order)
{
    int d, i, bpos;

    bpos = path[0].indx;
    for (d = 1; d <= btree->root->ndepth; d++) {
        for (i = 0; i < path[d].indx; i++) {
            bpos += path[d].node->ecnt[i];
        }
    }
    if (order == BTREE_ORDER_DESC) {
        bpos = btree->tot_elem_cnt - bpos - 1;
    }
    return bpos; /* btree position */
}

static int _ds_btree_elem_batch_get(ds_btree_elem_posi posi, const int count,
                                    const bool forward, const bool reverse,
                                    ds_btree_elem_item **elem_array)
{
    ds_btree_elem_item *elem;
    int nfound = 0;
    while (nfound < count) {
        if (forward) _ds_btree_incr_posi(&posi);
        else         _ds_btree_decr_posi(&posi);
        if (posi.node == NULL) break;

        elem = DS_BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
        elem->refcount++;
        if (reverse) elem_array[count-nfound-1] = elem;
        else         elem_array[nfound] = elem;
        nfound += 1;
    }
    return nfound;
}

/*
 * Btree Interface Functions
 */
void ds_btree_init(ds_btree_meta *btree, ds_btree_ops *ops)
{
    btree->root = NULL;
    btree->ops  = ops;
    btree->tot_elem_cnt = 0;
}

void ds_btree_node_free(ds_btree_indx_node *node)
{
    size_t ntotal = (node->ndepth > 0 ? sizeof(ds_btree_indx_node) : sizeof(ds_btree_leaf_node));
    do_item_mem_free(node, ntotal);
}

ds_btree_elem_item *ds_btree_get_first_elem(ds_btree_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (ds_btree_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return (ds_btree_elem_item *)(node->item[0]);
}

ds_btree_elem_item *ds_btree_get_last_elem(ds_btree_indx_node *node)
{
    while (node->ndepth > 0) {
        node = (ds_btree_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return (ds_btree_elem_item *)(node->item[node->used_count-1]);
}

ds_btree_indx_node *ds_btree_get_first_leaf(ds_btree_indx_node *node,
                                            ds_btree_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = 0;
        }
        node = (ds_btree_indx_node *)(node->item[0]);
    }
    assert(node->ndepth == 0);
    return node;
}

ds_btree_indx_node *ds_btree_get_last_leaf(ds_btree_indx_node *node,
                                           ds_btree_elem_posi *path)
{
    while (node->ndepth > 0) {
        if (path) {
            path[node->ndepth].node = node;
            path[node->ndepth].indx = node->used_count-1;
        }
        node = (ds_btree_indx_node *)(node->item[node->used_count-1]);
    }
    assert(node->ndepth == 0);
    return node;
}

ds_btree_elem_item *ds_btree_elem_find(ds_btree_meta *btree,
                                       const void *bkey, uint32_t nbkey,
                                       ds_btree_elem_posi *path)
{
    if (btree->root == NULL) {
        return NULL;
    }

    ds_btree_indx_node *node;
    ds_btree_elem_item *elem;
    ds_btree_ops *ops = btree->ops;
    int mid, left, right, comp;

    /* find leaf node */
    node = _ds_btree_find_leaf(btree, bkey, nbkey, path, &elem);
    if (elem != NULL) { /* the ins_elem is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to _ds_btree_find_leaf() function.
         */
        path[0].node = node;
        path[0].indx = 0;
        return elem;
    }

    /* do search the ins_elem in leaf node */
    left  = 0;
    right = node->used_count-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        elem = DS_BTREE_GET_ELEM_ITEM(node, mid);

        uint32_t enbkey;
        const void *ebkey = ops->get_bkey(elem, &enbkey);
        comp = ops->bkey_cmp(bkey, nbkey, ebkey, enbkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* the ins_elem is found */
        path[0].node = node;
        path[0].indx = mid;
        return elem;
    } else {             /* the ins_elem is not found */
        path[0].node = node;
        path[0].indx = left;
        return NULL;
    }
}

ds_btree_elem_item *ds_btree_find_first(ds_btree_meta *btree,
                                        const int bkrtype, const bkey_range *bkrange,
                                        ds_btree_elem_posi *path, const bool path_flag)
{
    ds_btree_indx_node *root = btree->root;
    ds_btree_ops       *ops = btree->ops;
    ds_btree_indx_node *node;
    ds_btree_elem_item *elem;
    int mid, left, right, comp;
    const void *bkey;
    uint32_t nbkey;

    if (bkrange == NULL) {
        assert(bkrtype != BKEY_RANGE_TYPE_SIN);
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            path[0].node = ds_btree_get_first_leaf(root, (path_flag ? path : NULL));
            path[0].indx = 0;
        } else {
            path[0].node = ds_btree_get_last_leaf(root, (path_flag ? path : NULL));
            path[0].indx = path[0].node->used_count - 1;
        }
        path[0].bkeq = false;

        elem = DS_BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
        assert(elem != NULL);
        return elem;
    }

    /* find leaf node */
    node = _ds_btree_find_leaf(btree, bkrange->from_bkey, bkrange->from_nbkey,
                              (path_flag ? path : NULL), &elem);
    if (elem != NULL) { /* the bkey(from_bkey) is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to _ds_btree_find_leaf() function.
         */
        path[0].bkeq = true;
        path[0].node = node;
        path[0].indx = 0;
        return elem;
    }

    /* do search the bkey(from_bkey) in leaf node */
    left  = 0;
    right = node->used_count-1;

    while (left <= right) {
        mid  = (left + right) / 2;
        elem = DS_BTREE_GET_ELEM_ITEM(node, mid);
        bkey = ops->get_bkey(elem, &nbkey);
        comp = ops->bkey_cmp(bkrange->from_bkey, bkrange->from_nbkey, bkey, nbkey);
        if (comp == 0) break;
        if (comp <  0) right = mid-1;
        else           left  = mid+1;
    }

    if (left <= right) { /* the bkey(from_bkey) is found. */
        path[0].bkeq = true;
        path[0].node = node;
        path[0].indx = mid;
        /* elem != NULL */
    } else {             /* the bkey(from_bkey) is not found */
        path[0].bkeq = false;
        switch (bkrtype) {
          case BKEY_RANGE_TYPE_SIN: /* single bkey */
            if (left > 0 && left < node->used_count) {
                /* In order to represent the bkey is NOT outside of btree,
                 * set any existent element position.
                 */
                path[0].node = node;
                path[0].indx = left;
            } else {
                if (left >= node->used_count) {
                    path[0].node = node->next;
                    path[0].indx = 0;
                    if (path[0].node != NULL) {
                        if (path_flag) _ds_btree_incr_path(path, 1);
                    }
                } else { /* left == 0 && right == -1 */
                    path[0].node = node->prev;
                    if (node->prev != NULL) {
                        path[0].indx = node->prev->used_count-1;
                        if (path_flag) _ds_btree_decr_path(path, 1);
                    } else {
                        path[0].indx = DS_BTREE_ITEM_COUNT;
                    }
                }
            }
            elem = NULL;
            break;
          case BKEY_RANGE_TYPE_ASC: /* ascending bkey range */
            /* find the next element */
            if (left < node->used_count) {
                path[0].node = node;
                path[0].indx = left;
            } else {
                path[0].node = node->next;
                path[0].indx = 0;
                if (path[0].node != NULL) {
                    if (path_flag) _ds_btree_incr_path(path, 1);
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = DS_BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                bkey = ops->get_bkey(elem, &nbkey);
                if (ops->bkey_cmp(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey) > 0)
                    elem = NULL;
            }
            break;
          case BKEY_RANGE_TYPE_DSC: /* descending bkey range */
            /* find the prev element */
            if (right >= 0) {
                path[0].node = node;
                path[0].indx = right;
            } else {
                path[0].node = node->prev;
                if (node->prev != NULL) {
                    path[0].indx = node->prev->used_count-1;
                    if (path_flag) _ds_btree_decr_path(path, 1);
                } else {
                    path[0].indx = DS_BTREE_ITEM_COUNT;
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = DS_BTREE_GET_ELEM_ITEM(path[0].node, path[0].indx);
                bkey = ops->get_bkey(elem, &nbkey);
                if (ops->bkey_cmp(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey) < 0)
                    elem = NULL;
            }
            break;
        }
    }
    return elem;
}

ds_btree_elem_item *ds_btree_find_prev(ds_btree_meta *btree,
                                       ds_btree_elem_posi *posi,
                                       const bkey_range *bkrange)
{
    ds_btree_ops *ops = btree->ops;
    ds_btree_elem_item *elem;

    _ds_btree_decr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = DS_BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        const void *bkey;
        uint32_t nbkey;
        int comp;
        bkey = ops->get_bkey(elem, &nbkey);
        comp = ops->bkey_cmp(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp < 0) elem = NULL;
        }
    } else {
        posi->bkeq = false;
    }
    return elem;
}

ds_btree_elem_item *ds_btree_find_next(ds_btree_meta *btree,
                                       ds_btree_elem_posi *posi,
                                       const bkey_range *bkrange)
{
    ds_btree_ops *ops = btree->ops;
    ds_btree_elem_item *elem;

    _ds_btree_incr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = DS_BTREE_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        const void *bkey;
        uint32_t nbkey;
        int comp;
        bkey = ops->get_bkey(elem, &nbkey);
        comp = ops->bkey_cmp(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey);
        if (comp == 0) {
            posi->bkeq = true;
        } else {
            posi->bkeq = false;
            if (comp > 0) elem = NULL;
        }
    } else {
        posi->bkeq = false;
    }
    return elem;
}

ds_btree_elem_item *ds_btree_elem_replace(ds_btree_elem_posi *posi, ds_btree_elem_item *new_elem)
{
    ds_btree_elem_item *old_elem = DS_BTREE_GET_ELEM_ITEM(posi->node, posi->indx);

    old_elem->linked--;
    new_elem->linked++;
    posi->node->item[posi->indx] = new_elem;

    return old_elem;
}

ds_btree_elem_item *ds_btree_elem_delete(ds_btree_meta *btree,
                                         const int bkrtype, const bkey_range *bkrange,
                                         const eflag_filter *efilter,
                                         void *delete_arg,
                                         uint32_t *opcost, size_t *space_decreased)
{
    ds_btree_indx_node *root = btree->root;
    ds_btree_elem_item *elem;
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];

    if (opcost) *opcost = 0;
    if (root == NULL) return NULL;

    assert(root->ndepth < DS_BTREE_MAX_DEPTH);
    elem = ds_btree_find_first(btree, bkrtype, bkrange, path, true);
    if (elem == NULL) return NULL;

    assert(path[0].bkeq == true);
    if (opcost) *opcost += 1;
    if (efilter == NULL || ds_btree_elem_filter(btree, elem, efilter)) {
        _ds_btree_elem_unlink(btree, path, space_decreased);
        btree->ops->delete_post(elem, delete_arg);
        return elem;
    }
    return NULL;
}

uint32_t ds_btree_elem_delete_bulk(ds_btree_meta *btree,
                                   const int bkrtype, const bkey_range *bkrange,
                                   const eflag_filter *efilter,
                                   const uint32_t offset, const uint32_t count,
                                   void *delete_arg,
                                   uint32_t *opcost, size_t *space_decreased)
{
    ds_btree_indx_node *root = btree->root;
    ds_btree_elem_item *elem;
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];
    ds_btree_elem_posi upth[DS_BTREE_MAX_DEPTH]; /* upper node path */
    uint32_t tot_found = 0;
    uint32_t cur_found = 0;
    uint32_t node_cnt = 1;
    uint32_t skip_cnt = 0;
    int i;
    bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);

    elem = ds_btree_find_first(btree, bkrtype, bkrange, path, true);
    if (elem == NULL) return 0;

    ds_btree_elem_posi c_posi = path[0];
    ds_btree_elem_posi s_posi = c_posi; /* save the current posi */

    /* prepare upper node path
     * used to incr/decr element counts in upper nodes.
     */
    for (i = 1; i <= root->ndepth; i++) {
        upth[i] = path[i];
    }
    /* clear the bkeq flag of current posi */
    c_posi.bkeq = false;

    do {
        if (opcost) *opcost += 1;
        if (efilter == NULL || ds_btree_elem_filter(btree, elem, efilter)) {
            if (skip_cnt < offset) {
                skip_cnt++;
            } else {
                elem->linked--;
                c_posi.node->item[c_posi.indx] = NULL;
                btree->ops->delete_post(elem, (void *)delete_arg);

                cur_found++;
                if (count > 0 && (tot_found+cur_found) >= count) break;
            }
        }

        /* get the next element */
        if (c_posi.bkeq == true) {
            elem = NULL; /* reached to the end of bkey range */
        } else {
            elem = (forward ? ds_btree_find_next(btree, &c_posi, bkrange)
                            : ds_btree_find_prev(btree, &c_posi, bkrange));
        }
        if (elem == NULL) break;

        if (s_posi.node != c_posi.node) {
            node_cnt += 1;
            if (cur_found > 0) {
                _ds_btree_node_remove_null_items(&s_posi, forward, cur_found);
                /* decrement element count in upper nodes */
                for (i = 1; i <= root->ndepth; i++) {
                    assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                    upth[i].node->ecnt[upth[i].indx] -= cur_found;
                }
                tot_found += cur_found;
                cur_found = 0;
            }
            if (root->ndepth > 0) {
                /* adjust upper node path */
                if (forward) _ds_btree_incr_path(upth, 1);
                else         _ds_btree_decr_path(upth, 1);
            }
            s_posi = c_posi;
        }
    } while (elem != NULL);

    if (cur_found > 0) {
        _ds_btree_node_remove_null_items(&s_posi, forward, cur_found);
        /* decrement element count in upper nodes */
        for (i = 1; i <= root->ndepth; i++) {
            assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
            upth[i].node->ecnt[upth[i].indx] -= cur_found;
        }
        tot_found += cur_found;
    }
    if (tot_found > 0) {
        btree->tot_elem_cnt -= tot_found;
        _ds_btree_node_merge(btree, path, forward, node_cnt, space_decreased);
    }
    return tot_found;
}

ds_btree_elem_item *ds_btree_delete_first_elem(ds_btree_meta *btree,
                                               size_t *space_decreased)
{
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];
    ds_btree_indx_node *leaf = ds_btree_get_first_leaf(btree->root, path);
    path[0].node = leaf;
    path[0].indx = 0;
    ds_btree_elem_item *elem = DS_BTREE_GET_ELEM_ITEM(leaf, 0);
    _ds_btree_elem_unlink(btree, path, space_decreased);
    return elem;
}

ds_btree_elem_item *ds_btree_delete_last_elem(ds_btree_meta *btree,
                                              size_t *space_decreased)
{
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];
    ds_btree_indx_node *leaf = ds_btree_get_last_leaf(btree->root, path);
    path[0].node = leaf;
    path[0].indx = leaf->used_count - 1;
    ds_btree_elem_item *elem = DS_BTREE_GET_ELEM_ITEM(leaf, leaf->used_count - 1);
    _ds_btree_elem_unlink(btree, path, space_decreased);
    return elem;
}

ENGINE_ERROR_CODE ds_btree_elem_add(ds_btree_meta *btree,
                                    ds_btree_elem_posi *path, ds_btree_elem_item *elem,
                                    size_t *space_increased)
{
    /* create the root node if it does not exist */
    if (btree->root == NULL) {
        ds_btree_indx_node *r_node = _ds_btree_node_alloc(0);
        if (r_node == NULL) {
            return ENGINE_ENOMEM;
        }
        _ds_btree_node_link(btree, r_node, NULL, space_increased);

        path[0].node = btree->root;
        path[0].indx = 0;
    }

    return _ds_btree_elem_link(btree, path, elem, space_increased);
}

uint32_t ds_btree_posi_outside(const ds_btree_elem_posi *posi, const int bkrtype)
{
    if (posi->node == NULL) {
        if (posi->indx == DS_BTREE_ITEM_COUNT) return DS_BTREE_OUTSIDE_LEFT;
        if (posi->indx == 0)                   return DS_BTREE_OUTSIDE_RIGHT;
        return 0;
    }
    /* the bkey of the found elem isn't same with the from_bkey of bkey range */
    assert(posi->node->ndepth == 0); /* leaf node */
    if (bkrtype == BKEY_RANGE_TYPE_ASC) {
        if (posi->node->prev == NULL && posi->indx == 0) /* the first element */
            return DS_BTREE_OUTSIDE_LEFT;
    } else if (bkrtype == BKEY_RANGE_TYPE_DSC) {
        if (posi->node->next == NULL && posi->indx == posi->node->used_count - 1) /* the last element */
            return DS_BTREE_OUTSIDE_RIGHT;
    }
    return 0;
}

bool ds_btree_elem_get(ds_btree_meta *btree,
                       const int bkrtype, const bkey_range *bkrange,
                       const eflag_filter *efilter,
                       const bool delete, void *delete_arg,
                       ds_btree_elem_item **elem_array,
                       uint32_t *opcost, uint32_t *outside, size_t *space_decreased)
{
    assert(btree->root);
    ds_btree_indx_node *root = btree->root;
    ds_btree_elem_item *elem;
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];

    assert(root->ndepth < DS_BTREE_MAX_DEPTH);
    elem = ds_btree_find_first(btree, bkrtype, bkrange, path, delete);
    if (elem == NULL) {
        if (outside) *outside = ds_btree_posi_outside(&path[0], BKEY_RANGE_TYPE_SIN);
        return false;
    }

    if (opcost) *opcost += 1;
    if (outside) *outside = 0;
    if (efilter == NULL || ds_btree_elem_filter(btree, elem, efilter)) {
        elem->refcount++;
        if (delete) {
            _ds_btree_elem_unlink(btree, path, space_decreased);
            btree->ops->delete_post(elem, delete_arg);
        }
        elem_array[0] = elem;
        return true;
    }
    return false;
}

uint32_t ds_btree_elem_get_bulk(ds_btree_meta *btree,
                                const int bkrtype, const bkey_range *bkrange,
                                const eflag_filter *efilter,
                                const uint32_t offset, const uint32_t count,
                                const bool delete, void *delete_arg,
                                ds_btree_elem_item **elem_array,
                                uint32_t *opcost, uint32_t *outside, size_t *space_decreased)
{
    assert(btree->root);
    ds_btree_indx_node *root = btree->root;
    ds_btree_elem_item *elem;
    ds_btree_elem_posi path[DS_BTREE_MAX_DEPTH];
    ds_btree_elem_posi upth[DS_BTREE_MAX_DEPTH]; /* upper node path */
    uint32_t tot_found = 0;
    uint32_t cur_found = 0;
    uint32_t node_cnt = 1;
    uint32_t skip_cnt = 0;
    int i;
    bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);

    assert(root->ndepth < DS_BTREE_MAX_DEPTH);
    elem = ds_btree_find_first(btree, bkrtype, bkrange, path, delete);
    if (elem == NULL) {
        if (outside) *outside = ds_btree_posi_outside(&path[0], bkrtype);
        return 0;
    }

    ds_btree_elem_posi c_posi = path[0];
    ds_btree_elem_posi s_posi = c_posi; /* save the current posi */
    if (outside) *outside = 0;

    if (c_posi.bkeq == false) {
        if (outside) *outside |= ds_btree_posi_outside(&c_posi, bkrtype);
    }

    if (delete) {
        /* prepare upper node path
         * used to incr/decr element counts  in upper nodes.
         */
        for (i = 1; i <= root->ndepth; i++) {
            upth[i] = path[i];
        }
    }
    /* clear the bkeq flag of current posi */
    c_posi.bkeq = false;

    do {
        if (opcost) *opcost += 1;
        if (efilter == NULL || ds_btree_elem_filter(btree, elem, efilter)) {
            if (skip_cnt < offset) {
                skip_cnt++;
            } else {
                elem->refcount++;
                elem_array[tot_found+cur_found] = elem;
                if (delete) {
                    elem->linked--;
                    c_posi.node->item[c_posi.indx] = NULL;
                    btree->ops->delete_post(elem, delete_arg);
                }
                cur_found++;
                if (count > 0 && (tot_found+cur_found) >= count) break;
            }
        }

        /* get the next element */
        if (c_posi.bkeq == true) {
            elem = NULL; /* reached to the end of bkey range */
        } else {
            elem = (forward ? ds_btree_find_next(btree, &c_posi, bkrange)
                            : ds_btree_find_prev(btree, &c_posi, bkrange));
        }
        if (elem == NULL) break;

        if (s_posi.node != c_posi.node) {
            node_cnt += 1;
            if (cur_found > 0) {
                if (delete) {
                    _ds_btree_node_remove_null_items(&s_posi, forward, cur_found);
                    /* decrement element count in upper nodes */
                    for (i = 1; i <= root->ndepth; i++) {
                        assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                        upth[i].node->ecnt[upth[i].indx] -= cur_found;
                    }
                }
                tot_found += cur_found;
                cur_found = 0;
            }
            if (delete && root->ndepth > 0) {
                /* adjust upper node path */
                if (forward) _ds_btree_incr_path(upth, 1);
                else         _ds_btree_decr_path(upth, 1);
            }
            s_posi = c_posi;
        }
    } while (elem != NULL);

    if (cur_found > 0) {
        if (delete) {
            _ds_btree_node_remove_null_items(&s_posi, forward, cur_found);
            /* decrement element count in upper nodes */
            for (i = 1; i <= root->ndepth; i++) {
                assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                upth[i].node->ecnt[upth[i].indx] -= cur_found;
            }
        }
        tot_found += cur_found;
    }

    if (delete && tot_found > 0) {
        btree->tot_elem_cnt -= tot_found;
        _ds_btree_node_merge(btree, path, forward, node_cnt, space_decreased);
    }

    if (c_posi.node == NULL) {
        if (outside) *outside |= ds_btree_posi_outside(&c_posi, bkrtype);
    }

    return tot_found;
}

uint32_t ds_btree_elem_count(ds_btree_meta *btree,
                             const int bkrtype, const bkey_range *bkrange,
                             const eflag_filter *efilter, uint32_t *opcost)
{
    ds_btree_elem_posi  posi;
    ds_btree_elem_item *elem;
    ds_btree_ops       *ops = btree->ops;
    uint32_t tot_found = 0; /* total found count */
    uint32_t tot_access = 0; /* total access count */

    if (opcost) {
        *opcost = 0;
    }

    if (btree->root == NULL) {
        return 0;
    }

#if 1 // BOP_COUNT_OPTIMIZE
    /* check if the bkey range is full range */
    if (bkrtype != BKEY_RANGE_TYPE_SIN && efilter == NULL) {
        ds_btree_elem_item *min_bkey_elem = ds_btree_get_first_elem(btree->root);
        ds_btree_elem_item *max_bkey_elem = ds_btree_get_last_elem(btree->root);

        uint32_t min_nbkey, max_nbkey;
        const void *min_bkey = ops->get_bkey(min_bkey_elem, &min_nbkey);
        const void *max_bkey = ops->get_bkey(max_bkey_elem, &max_nbkey);

        int min_comp, max_comp;
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            min_comp = ops->bkey_cmp(bkrange->from_bkey, bkrange->from_nbkey, min_bkey, min_nbkey);
            max_comp = ops->bkey_cmp(bkrange->to_bkey,   bkrange->to_nbkey,   max_bkey, max_nbkey);
        } else { /* BKEY_RANGE_TYPE_DSC */
            min_comp = ops->bkey_cmp(bkrange->to_bkey,   bkrange->to_nbkey,   min_bkey, min_nbkey);
            max_comp = ops->bkey_cmp(bkrange->from_bkey, bkrange->from_nbkey, max_bkey, max_nbkey);
        }
        if (min_comp <= 0 && max_comp >= 0) {
            return btree->tot_elem_cnt;
        }
    }
#endif

    elem = ds_btree_find_first(btree, bkrtype, bkrange, &posi, false);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) {
            assert(posi.bkeq == true);
            tot_access++;
            if (efilter == NULL || ds_btree_elem_filter(btree, elem, efilter))
                tot_found++;
        } else { /* BKEY_RANGE_TYPE_ASC || BKEY_RANGE_TYPE_DSC */
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            posi.bkeq = false;
            do {
                tot_access++;
                if (efilter == NULL || ds_btree_elem_filter(btree, elem, efilter))
                    tot_found++;

                if (posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? ds_btree_find_next(btree, &posi, bkrange)
                                : ds_btree_find_prev(btree, &posi, bkrange));
            } while (elem != NULL);
        }
    }
    if (opcost)
        *opcost = tot_access;
    return tot_found;
}

int ds_btree_posi_find(ds_btree_meta *btree,
                       const int bkrtype, const bkey_range *bkrange,
                       ENGINE_BTREE_ORDER order)
{
    ds_btree_elem_posi  path[DS_BTREE_MAX_DEPTH];
    ds_btree_elem_item *elem;
    int bpos; /* btree position */

    if (btree->root == NULL) return -1; /* not found */

    elem = ds_btree_find_first(btree, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        assert(path[0].bkeq == true);
        bpos = _ds_btree_posi_from_path(btree, path, order);
        assert(bpos >= 0);
    } else {
        bpos = -1; /* not found */
    }
    return bpos;
}

int ds_btree_posi_find_with_get(ds_btree_meta *btree,
                                const int bkrtype, const bkey_range *bkrange,
                                ENGINE_BTREE_ORDER order, const int count,
                                ds_btree_elem_item **elem_array,
                                uint32_t *elem_count, uint32_t *elem_index)
{
    ds_btree_elem_posi  path[DS_BTREE_MAX_DEPTH];
    ds_btree_elem_item *elem;
    int bpos = -1; /* NOT found */

    if (btree->root == NULL) return -1; /* not found */

    elem = ds_btree_find_first(btree, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        int ecnt, eidx;
        assert(path[0].bkeq == true);
        bpos = _ds_btree_posi_from_path(btree, path, order);
        assert(bpos >= 0);

        ecnt = 1;                             /* elem count */
        eidx = (bpos < count) ? bpos : count; /* elem index in elem array */
        elem->refcount++;
        elem_array[eidx] = elem;

        if (order == BTREE_ORDER_ASC) {
            ecnt += _ds_btree_elem_batch_get(path[0], eidx,  false, true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += _ds_btree_elem_batch_get(path[0], count, true,  false, &elem_array[eidx+1]);
        } else {
            ecnt += _ds_btree_elem_batch_get(path[0], eidx,  true,  true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += _ds_btree_elem_batch_get(path[0], count, false, false, &elem_array[eidx+1]);
        }
        *elem_count = (uint32_t)ecnt;
        *elem_index = (uint32_t)eidx;
    }
    return bpos; /* btree_position */
}

ENGINE_ERROR_CODE ds_btree_elem_get_by_posi(ds_btree_meta *btree,
                                            const int index, const uint32_t count, const bool forward,
                                            ds_btree_elem_item **elem_array, uint32_t *elem_count)
{
    ds_btree_elem_posi  posi;
    ds_btree_indx_node *node;
    ds_btree_elem_item *elem;
    int i, tot_ecnt;
    uint32_t nfound; /* found count */

    if (btree->root == NULL) return ENGINE_ELEM_ENOENT;

    node = btree->root;
    tot_ecnt = 0;
    while (node->ndepth > 0) {
        for (i = 0; i < node->used_count; i++) {
            assert(node->ecnt[i] > 0);
            if ((tot_ecnt + node->ecnt[i]) > index) break;
            tot_ecnt += node->ecnt[i];
        }
        assert(i < node->used_count);
        node = (ds_btree_indx_node *)node->item[i];
    }
    assert(node->ndepth == 0);
    posi.node = node;
    posi.indx = index-tot_ecnt;
    posi.bkeq = false;

    elem = DS_BTREE_GET_ELEM_ITEM(posi.node, posi.indx);
    elem->refcount++;
    elem_array[0] = elem;
    nfound = 1;
    nfound += _ds_btree_elem_batch_get(posi, count-1, forward, false, &elem_array[nfound]);

    *elem_count = nfound;
    if (*elem_count > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
}

void ds_btree_traverse_init(ds_btree_meta *btree, void *posi)
{
    ds_btree_elem_posi *bp = (ds_btree_elem_posi *)posi;
    ds_btree_indx_node *node = btree->root;
    if (node == NULL || node->used_count == 0) {
        bp->node = NULL;
        return;
    }
    bp->node = ds_btree_get_first_leaf(node, NULL);
    bp->indx = 0;
}

uint32_t ds_btree_traverse_next(void *posi, void **elem_array, uint32_t count)
{
    ds_btree_elem_posi *bp = (ds_btree_elem_posi *)posi;
    uint32_t fcnt = 0;
    while (fcnt < count && bp->node != NULL) {
        ds_btree_elem_item *elem = DS_BTREE_GET_ELEM_ITEM(bp->node, bp->indx);
        elem->refcount++;
        elem_array[fcnt++] = elem;
        if (++bp->indx >= bp->node->used_count) {
            bp->node = bp->node->next;
            bp->indx = 0;
        }
    }
    return fcnt;
}
