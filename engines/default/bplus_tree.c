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
#include "bplus_tree.h"
#include "default_engine.h"
#include <assert.h>
#include <string.h>

/*
 * Bplus management
 */
static bool bplus_position_debug = false;

static bplus_indx_node *do_bplus_node_alloc(const uint8_t node_depth)
{
    size_t ntotal = (node_depth > 0 ? sizeof(bplus_indx_node) : sizeof(bplus_leaf_node));

    bplus_indx_node *node = do_item_mem_alloc(ntotal, LRU_CLSID_FOR_SMALL);
    if (node != NULL) {
        node->slabs_clsid = slabs_clsid(ntotal);
        assert(node->slabs_clsid > 0);

        node->refcount    = 0;
        node->ndepth      = node_depth;
        node->used_count  = 0;
        node->prev = node->next = NULL;
        memset(node->item, 0, BPLUS_ITEM_COUNT*sizeof(void*));
        if (node_depth > 0)
            memset(node->ecnt, 0, BPLUS_ITEM_COUNT*sizeof(uint16_t));
    }
    return node;
}

static void do_bplus_node_free(bplus_indx_node *node)
{
    size_t ntotal = (node->ndepth > 0 ? sizeof(bplus_indx_node) : sizeof(bplus_leaf_node));
    do_item_mem_free(node, ntotal);
}

static inline void do_bplus_incr_posi(bplus_elem_posi *posi)
{
    if (posi->indx < (posi->node->used_count-1)) {
        posi->indx += 1;
    } else {
        posi->node = posi->node->next;
        posi->indx = 0;
    }
}

static inline void do_bplus_decr_posi(bplus_elem_posi *posi)
{
    if (posi->indx > 0) {
        posi->indx -= 1;
    } else {
        posi->node = posi->node->prev;
        if (posi->node != NULL)
            posi->indx = posi->node->used_count-1;
        else
            posi->indx = BPLUS_ITEM_COUNT;
    }
}

static void do_bplus_incr_path(bplus_elem_posi *path, int depth)
{
    bplus_indx_node *saved_node;

    while (depth < BPLUS_MAX_DEPTH) {
        saved_node = path[depth].node;
        do_bplus_incr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < BPLUS_MAX_DEPTH);
}

static void do_bplus_decr_path(bplus_elem_posi *path, int depth)
{
    bplus_indx_node *saved_node;

    while (depth < BPLUS_MAX_DEPTH) {
        saved_node = path[depth].node;
        do_bplus_decr_posi(&path[depth]);
        if (path[depth].node == saved_node) break;
        depth += 1;
    }
    assert(depth < BPLUS_MAX_DEPTH);
}

static bplus_indx_node *do_bplus_find_leaf(bplus_meta *bplus,
                                           const void *bkey, const uint32_t nbkey,
                                           bplus_elem_posi *path,
                                           bplus_elem_item **found_elem)
{
    bplus_indx_node *node = bplus->root;
    bplus_ops *ops = bplus->ops;
    bplus_elem_item *elem;
    int mid, left, right, comp;
    const void *sep_bkey;
    uint32_t sep_nbkey;

    *found_elem = NULL; /* the same bkey is not found */

    while (node->ndepth > 0) {
        left  = 1;
        right = node->used_count-1;

        while (left <= right) {
            mid  = (left + right) / 2;
            elem = bplus_get_first_elem(node->item[mid]); /* separator */
            sep_bkey = ops->get_bkey(elem, &sep_nbkey);
            comp = BKEY_COMP(bkey, nbkey, sep_bkey, sep_nbkey);
            if (comp == 0) { /* the same bkey is found */
                *found_elem = elem;
                if (path) {
                    path[node->ndepth].node = node;
                    path[node->ndepth].indx = mid;
                }
                node = bplus_get_first_leaf(node->item[mid], path);
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
        node = (bplus_indx_node *)(node->item[right]);
    }
    return node;
}

static void do_bplus_consistency_check(bplus_meta *bplus,
                                       bplus_indx_node *node, uint32_t ecount, bool detail)
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
            do_bplus_consistency_check(bplus, (bplus_indx_node*)node->item[i], node->ecnt[i], detail);
            tot_ecnt += node->ecnt[i];
        }
        assert(tot_ecnt == ecount);
    } else { /* node->ndepth == 0: leaf page check */
        for (i = 0; i < node->used_count; i++) {
            assert(node->item[i] != NULL);
        }
        assert(node->used_count == ecount);
        if (detail) {
            bplus_ops *ops = bplus->ops;
            bplus_elem_item *p_elem;
            bplus_elem_item *c_elem;
            const void *p_bkey, *c_bkey;
            uint32_t    p_nbkey, c_nbkey;
            int comp;

            if (node->prev == NULL) {
                p_elem = NULL;
            } else {
                p_elem = BPLUS_GET_ELEM_ITEM(node->prev, node->prev->used_count-1);
            }
            for (i = 0; i < node->used_count; i++) {
                c_elem = BPLUS_GET_ELEM_ITEM(node, i);
                if (p_elem != NULL) {
                    p_bkey = ops->get_bkey(p_elem, &p_nbkey);
                    c_bkey = ops->get_bkey(c_elem, &c_nbkey);
                    comp = BKEY_COMP(p_bkey, p_nbkey, c_bkey, c_nbkey);
                    assert(comp < 0);
                }
                p_elem = c_elem;
            }
            if (node->next == NULL) {
                c_elem = NULL;
            } else {
                c_elem = BPLUS_GET_ELEM_ITEM(node->next, 0);
            }
            if (c_elem != NULL) {
                p_bkey = ops->get_bkey(p_elem, &p_nbkey);
                c_bkey = ops->get_bkey(c_elem, &c_nbkey);
                comp = BKEY_COMP(p_bkey, p_nbkey, c_bkey, c_nbkey);
                assert(comp < 0);
            }
        }
    }
}

static void do_bplus_node_item_move(bplus_indx_node *c_node, /* current node */
                                    bplus_indx_node *n_node, /* neighbor node */
                                    int direction, int move_count)
{
    assert(move_count > 0);
    int i;

    if (direction == BPLUS_DIRECTION_NEXT) {
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
    } else { /* BPLUS_DIRECTION_PREV */
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

static void do_bplus_ecnt_move_split(bplus_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    bplus_elem_posi  posi;
    bplus_indx_node *saved_node;

    while (depth < BPLUS_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == BPLUS_DIRECTION_NEXT) {
            do_bplus_incr_posi(&posi);
        } else {
            do_bplus_decr_posi(&posi);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < BPLUS_MAX_DEPTH);
}

static void do_bplus_ecnt_move_merge(bplus_elem_posi *path, int depth, int direction, uint32_t elem_count)
{
    bplus_elem_posi  posi;
    bplus_indx_node *saved_node;

    while (depth < BPLUS_MAX_DEPTH) {
        posi = path[depth];
        posi.node->ecnt[posi.indx] -= elem_count;

        saved_node = posi.node;
        if (direction == BPLUS_DIRECTION_NEXT) {
            do {
                do_bplus_incr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        } else {
            do {
                do_bplus_decr_posi(&posi);
            } while (posi.node->used_count == 0 ||
                     posi.node->ecnt[posi.indx] == 0);
        }
        posi.node->ecnt[posi.indx] += elem_count;
        if (saved_node == posi.node) break;
        depth += 1;
    }
    assert(depth < BPLUS_MAX_DEPTH);
}

static void do_bplus_node_sbalance(bplus_indx_node *node, bplus_elem_posi *path, int depth)
{
    bplus_elem_posi *posi;
    int direction;
    int move_count;
    int elem_count; /* total count of elements moved */
    int i;

    /* balance the number of elements with neighber node */
    if (node->next != NULL && node->prev != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     BPLUS_DIRECTION_NEXT : BPLUS_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     BPLUS_DIRECTION_NEXT : BPLUS_DIRECTION_PREV);
    }
    if (direction == BPLUS_DIRECTION_NEXT) {
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

        do_bplus_node_item_move(node, node->next, direction, move_count);

        /* move element count in upper btree nodes */
        do_bplus_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx >= node->used_count) {
            posi->node = node->next;
            posi->indx -= node->used_count;
            /* adjust upper path info */
            do_bplus_incr_path(path, depth+1);
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

        do_bplus_node_item_move(node, node->prev, direction, move_count);

        /* move element count in upper btree nodes */
        do_bplus_ecnt_move_split(path, depth+1, direction, elem_count);

        /* adjust posi information */
        posi = &path[depth];
        if (posi->indx < move_count) {
            posi->node = node->prev;
            posi->indx += (node->prev->used_count-move_count);
            /* adjust upper path info */
            do_bplus_decr_path(path, depth+1);
        } else {
            posi->indx -= move_count;
        }
    }
}

static void do_bplus_node_link(bplus_meta *bplus, bplus_indx_node *node,
                               bplus_elem_posi *p_posi, size_t *space_increased)
{
    /*
     * p_posi: the position of to-be-linked node in parent node.
     */
    if (p_posi == NULL) {
        /* No parent node : make a new root node */
        if (bplus->root == NULL) {
            node->used_count = 0;
        } else {
            node->item[0] = bplus->root;
            node->ecnt[0] = bplus->tot_elem_cnt;
            node->used_count = 1;
        }
        bplus->root = node;
    } else {
        /* Parent node exists */
        bplus_indx_node *p_node = p_posi->node;
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

    if (node->ndepth > 0) *space_increased += slabs_space_size(sizeof(bplus_indx_node));
    else                  *space_increased += slabs_space_size(sizeof(bplus_leaf_node));
}

static ENGINE_ERROR_CODE do_bplus_node_split(bplus_meta *bplus, bplus_elem_posi *path, size_t *space_increased)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    bplus_indx_node *s_node;
    bplus_indx_node *n_node[BPLUS_MAX_DEPTH]; /* neighber nodes */
    bplus_elem_posi  p_posi;
    int     i, direction;
    uint8_t btree_depth = 0;

    s_node = path[btree_depth].node;
    do {
        if ((s_node->next != NULL && s_node->next->used_count < (BPLUS_ITEM_COUNT/2)) ||
            (s_node->prev != NULL && s_node->prev->used_count < (BPLUS_ITEM_COUNT/2))) {
            do_bplus_node_sbalance(s_node, path, btree_depth);
            break;
        }

        n_node[btree_depth] = do_bplus_node_alloc(btree_depth);
        if (n_node[btree_depth] == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        btree_depth += 1;
        assert(btree_depth < BPLUS_MAX_DEPTH);
        if (btree_depth > bplus->root->ndepth) {
            bplus_indx_node *r_node = do_bplus_node_alloc(btree_depth);
            if (r_node == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            do_bplus_node_link(bplus, r_node, NULL, space_increased);

            path[btree_depth].node = r_node;
            path[btree_depth].indx = 0;
            break;
        }
        s_node = path[btree_depth].node;
    }
    while (s_node->used_count >= BPLUS_ITEM_COUNT);

    if (ret == ENGINE_SUCCESS) {
        for (i = btree_depth-1; i >= 0; i--) {
            s_node = path[i].node;
            if (s_node->prev == NULL && s_node->next == NULL) {
                direction = (path[i].indx < (BPLUS_ITEM_COUNT/2) ?
                             BPLUS_DIRECTION_PREV : BPLUS_DIRECTION_NEXT);
            } else {
                direction = (s_node->prev == NULL ?
                             BPLUS_DIRECTION_PREV : BPLUS_DIRECTION_NEXT);
            }
            p_posi = path[i+1];
            if (direction == BPLUS_DIRECTION_NEXT) p_posi.indx += 1;
            do_bplus_node_link(bplus, n_node[i], &p_posi, space_increased);

            if (direction == BPLUS_DIRECTION_PREV) {
                /* adjust upper path */
                path[i+1].indx += 1;
                //do_bplus_incr_path(path, i+1);
            }
            do_bplus_node_sbalance(s_node, path, i);
        }
    } else {
        for (i = 0; i < btree_depth; i++) {
            do_bplus_node_free(n_node[i]);
        }
    }
    if (bplus_position_debug) {
        do_bplus_consistency_check(bplus, bplus->root, bplus->tot_elem_cnt, true);
    }
    return ret;
}

/* merge check */
static void do_bplus_node_mbalance(bplus_indx_node *node, bplus_elem_posi *path, int depth)
{
    int direction;

    if (node->prev != NULL && node->next != NULL) {
        direction = (node->next->used_count < node->prev->used_count ?
                     BPLUS_DIRECTION_NEXT : BPLUS_DIRECTION_PREV);
    } else {
        direction = (node->next != NULL ?
                     BPLUS_DIRECTION_NEXT : BPLUS_DIRECTION_PREV);
    }
    if (direction == BPLUS_DIRECTION_NEXT) {
        do_bplus_node_item_move(node, node->next, direction, node->used_count);
    } else {
        do_bplus_node_item_move(node, node->prev, direction, node->used_count);
    }

    int elem_count = path[depth+1].node->ecnt[path[depth+1].indx];
    do_bplus_ecnt_move_merge(path, depth+1, direction, elem_count);
}

static void do_bplus_node_unlink(bplus_meta *bplus, bplus_indx_node *node,
                                 bplus_elem_posi *p_posi, size_t *space_decreased)
{
    if (p_posi == NULL) {
        /* No parent node : remove the root node */
        bplus->root = NULL;
    } else {
        /* unlink the given node from b+tree */
        if (node->prev != NULL) node->prev->next = node->next;
        if (node->next != NULL) node->next->prev = node->prev;
        node->prev = node->next = NULL;

        /* Parent node exists */
        bplus_indx_node *p_node = p_posi->node;
        assert(p_node->ecnt[p_posi->indx] == 0);
        for (int i = p_posi->indx+1; i < p_node->used_count; i++) {
            p_node->item[i-1] = p_node->item[i];
            p_node->ecnt[i-1] = p_node->ecnt[i];
        }
        p_node->item[p_node->used_count-1] = NULL;
        p_node->ecnt[p_node->used_count-1] = 0;
        p_node->used_count--;
    }

    if (node->ndepth > 0) *space_decreased += slabs_space_size(sizeof(bplus_indx_node));
    else                  *space_decreased += slabs_space_size(sizeof(bplus_leaf_node));

    /* The amount of space to be decreased become different according to node depth.
     * So, the btree node must be freed after collection space is decreased.
     */
    do_bplus_node_free(node);
}

static void do_bplus_node_detach(bplus_indx_node *node, size_t *space_decreased)
{
    /* unlink the given node from b+tree */
    if (node->prev != NULL) node->prev->next = node->next;
    if (node->next != NULL) node->next->prev = node->prev;
    node->prev = node->next = NULL;

    if (node->ndepth > 0) *space_decreased += slabs_space_size(sizeof(bplus_indx_node));
    else                  *space_decreased += slabs_space_size(sizeof(bplus_leaf_node));

    /* The amount of space to be decreased become different according to node depth.
     * So, the btree node must be freed after collection space is decreased.
     */
    do_bplus_node_free(node);
}

static inline void do_bplus_node_remove_null_items(bplus_elem_posi *posi, const bool forward, const int null_count)
{
    bplus_indx_node *node = posi->node;
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

static void do_bplus_node_merge(bplus_meta *bplus, bplus_elem_posi *path,
                                const bool forward, const int leaf_node_count,
                                size_t *space_decreased)
{
    bplus_indx_node *node;
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
            if (node == bplus->root) {
                if (node->used_count == 0) {
                    do_bplus_node_unlink(bplus, node, NULL, space_decreased);
                } else {
                    bplus_indx_node *new_root;
                    while (node->used_count == 1 && node->ndepth > 0) {
                        new_root = BPLUS_GET_NODE_ITEM(node, 0);
                        do_bplus_node_unlink(bplus, node, NULL, space_decreased);
                        bplus->root = new_root;
                        node = new_root;
                    }
                }
            } else {
                if (node->used_count == 0) {
                    do_bplus_node_unlink(bplus, node, &path[btree_depth+1], space_decreased);
                    par_node_count = 1;
                }
                else if (node->used_count < (BPLUS_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (BPLUS_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (BPLUS_ITEM_COUNT/2))) {
                        do_bplus_node_mbalance(node, path, btree_depth);
                        do_bplus_node_unlink(bplus, node, &path[btree_depth+1], space_decreased);
                        par_node_count = 1;
                    }
                }
            }
        } else { /* cur_node_count > 1 */
            bplus_elem_posi  upth[BPLUS_MAX_DEPTH] = {{ 0 }}; /* upper node path */
            bplus_elem_posi  s_posi;
            int cur_unlink_cnt = 0;
            int i, upp_depth = btree_depth+1;

            /* prepare upper node path */
            for (i = upp_depth; i <= bplus->root->ndepth; i++) {
                upth[i] = path[i];
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = BPLUS_GET_NODE_ITEM(s_posi.node, s_posi.indx);
                assert(node != NULL);

                if (node->used_count == 0) {
                    do_bplus_node_detach(node, space_decreased);
                    s_posi.node->item[s_posi.indx] = NULL;
                    assert(s_posi.node->ecnt[s_posi.indx] == 0);
                }

                if (i == cur_node_count) break;

                if (forward) do_bplus_incr_posi(&s_posi);
                else         do_bplus_decr_posi(&s_posi);
            }

            s_posi = upth[upp_depth];
            for (i = 1; i <= cur_node_count; i++) {
                node = BPLUS_GET_NODE_ITEM(upth[upp_depth].node, upth[upp_depth].indx);
                if (node == NULL) {
                    cur_unlink_cnt++;
                }
                else if (node->used_count < (BPLUS_ITEM_COUNT/2)) {
                    if ((node->prev != NULL && node->prev->used_count < (BPLUS_ITEM_COUNT/2)) ||
                        (node->next != NULL && node->next->used_count < (BPLUS_ITEM_COUNT/2))) {
                        do_bplus_node_mbalance(node, upth, btree_depth);
                        do_bplus_node_detach(node, space_decreased);
                        upth[upp_depth].node->item[upth[upp_depth].indx] = NULL;
                        assert(upth[upp_depth].node->ecnt[upth[upp_depth].indx] == 0);
                        cur_unlink_cnt++;
                    }
                }

                if (i == cur_node_count) break;

                if (forward) do_bplus_incr_path(upth, upp_depth);
                else         do_bplus_decr_path(upth, upp_depth);

                if (s_posi.node != upth[upp_depth].node) {
                    if (cur_unlink_cnt > 0) {
                        do_bplus_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                        cur_unlink_cnt = 0;
                    }
                    s_posi = upth[upp_depth];
                    par_node_count += 1;
                }
            }
            if (cur_unlink_cnt > 0) {
                do_bplus_node_remove_null_items(&s_posi, forward, cur_unlink_cnt);
                par_node_count += 1;
            }
        }
        btree_depth += 1;
        cur_node_count = par_node_count;
    }
    if (bplus_position_debug) {
        do_bplus_consistency_check(bplus, bplus->root, bplus->tot_elem_cnt, true);
    }
}

static ENGINE_ERROR_CODE do_bplus_elem_link(bplus_meta *bplus,
                                            bplus_elem_posi *path, bplus_elem_item *elem,
                                            size_t *space_increased)
{
    /* If the leaf node is full of elements, split it ahead. */
    if (path[0].node->used_count >= BPLUS_ITEM_COUNT) {
        ENGINE_ERROR_CODE ret = do_bplus_node_split(bplus, path, space_increased);
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
    for (int i = 1; i <= bplus->root->ndepth; i++) {
        path[i].node->ecnt[path[i].indx]++;
    }
    bplus->tot_elem_cnt++;

    return ENGINE_SUCCESS;
}

static void do_bplus_elem_unlink(bplus_meta *bplus, bplus_elem_posi *path,
                                 size_t *space_decreased)
{
    bplus_elem_posi *posi = &path[0];
    bplus_elem_item *elem = BPLUS_GET_ELEM_ITEM(posi->node, posi->indx);
    int i;

    elem->linked--;

    /* remove the element from the leaf node */
    bplus_indx_node *node = posi->node;
    for (i = posi->indx+1; i < node->used_count; i++) {
        node->item[i-1] = node->item[i];
    }
    node->item[node->used_count-1] = NULL;
    node->used_count--;
    /* decrement element count in upper nodes */
    for (i = 1; i <= bplus->root->ndepth; i++) {
        path[i].node->ecnt[path[i].indx]--;
    }
    bplus->tot_elem_cnt--;

    if (node->used_count < (BPLUS_ITEM_COUNT/2)) {
        do_bplus_node_merge(bplus, path, true, 1, space_decreased);
    }
}

static int do_bplus_posi_from_path(bplus_meta *bplus,
                                   bplus_elem_posi *path, ENGINE_BTREE_ORDER order)
{
    int d, i, bpos;

    bpos = path[0].indx;
    for (d = 1; d <= bplus->root->ndepth; d++) {
        for (i = 0; i < path[d].indx; i++) {
            bpos += path[d].node->ecnt[i];
        }
    }
    if (order == BTREE_ORDER_DESC) {
        bpos = bplus->tot_elem_cnt - bpos - 1;
    }
    return bpos; /* btree position */
}

static int do_bplus_elem_batch_get(bplus_elem_posi posi, const int count,
                                   const bool forward, const bool reverse,
                                   bplus_elem_item **elem_array)
{
    bplus_elem_item *elem;
    int nfound = 0;
    while (nfound < count) {
        if (forward) do_bplus_incr_posi(&posi);
        else         do_bplus_decr_posi(&posi);
        if (posi.node == NULL) break;

        elem = BPLUS_GET_ELEM_ITEM(posi.node, posi.indx);
        elem->refcount++;
        if (reverse) elem_array[count-nfound-1] = elem;
        else         elem_array[nfound] = elem;
        nfound += 1;
    }
    return nfound;
}

/*
 * Bplus Interface Functions
 */
void bplus_init(bplus_meta *bplus, bplus_ops *ops)
{
    bplus->root = NULL;
    bplus->ops = ops;
    bplus->tot_elem_cnt = 0;
}

bplus_elem_item *bplus_elem_find(bplus_meta *bplus,
                                 const void *bkey, uint32_t nbkey,
                                 bplus_elem_posi *path)
{
    if (bplus->root == NULL) {
        return NULL;
    }

    bplus_indx_node *node;
    bplus_elem_item *elem;
    bplus_ops *ops = bplus->ops;
    int mid, left, right, comp;

    /* find leaf node */
    node = do_bplus_find_leaf(bplus, bkey, nbkey, path, &elem);
    if (elem != NULL) { /* the ins_elem is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to do_bplus_find_leaf() function.
         */
        path[0].node = node;
        path[0].indx = 0;
        return elem;
    }

    /* do search the ins_elem in leaf node */
    left  = 0;
    right = node->used_count-1;

    uint32_t enbkey;
    const void *ebkey;
    while (left <= right) {
        mid  = (left + right) / 2;
        elem = BPLUS_GET_ELEM_ITEM(node, mid);

        ebkey = ops->get_bkey(elem, &enbkey);
        comp = BKEY_COMP(bkey, nbkey, ebkey, enbkey);
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

bplus_elem_item *bplus_find_first(bplus_meta *bplus,
                                  const int bkrtype, const bkey_range *bkrange,
                                  bplus_elem_posi *path, const bool path_flag)
{
    bplus_indx_node *root = bplus->root;
    bplus_ops       *ops = bplus->ops;
    bplus_indx_node *node;
    bplus_elem_item *elem;
    int mid, left, right, comp;
    const void *bkey;
    uint32_t nbkey;

    if (bkrange == NULL) {
        assert(bkrtype != BKEY_RANGE_TYPE_SIN);
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            path[0].node = bplus_get_first_leaf(root, (path_flag ? path : NULL));
            path[0].indx = 0;
        } else {
            path[0].node = bplus_get_last_leaf(root, (path_flag ? path : NULL));
            path[0].indx = path[0].node->used_count - 1;
        }
        path[0].bkeq = false;

        elem = BPLUS_GET_ELEM_ITEM(path[0].node, path[0].indx);
        assert(elem != NULL);
        return elem;
    }

    /* find leaf node */
    node = do_bplus_find_leaf(bplus, bkrange->from_bkey, bkrange->from_nbkey,
                              (path_flag ? path : NULL), &elem);
    if (elem != NULL) { /* the bkey(from_bkey) is found */
        /* while traversing to leaf node, the bkey can be found.
         * refer to do_bplus_find_leaf() function.
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
        elem = BPLUS_GET_ELEM_ITEM(node, mid);
        bkey = ops->get_bkey(elem, &nbkey);
        comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey, bkey, nbkey);
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
                /* In order to represent the bkey is NOT outside of bplus,
                 * set any existent element position.
                 */
                path[0].node = node;
                path[0].indx = left;
            } else {
                if (left >= node->used_count) {
                    path[0].node = node->next;
                    path[0].indx = 0;
                    if (path[0].node != NULL) {
                        if (path_flag) do_bplus_incr_path(path, 1);
                    }
                } else { /* left == 0 && right == -1 */
                    path[0].node = node->prev;
                    if (node->prev != NULL) {
                        path[0].indx = node->prev->used_count-1;
                        if (path_flag) do_bplus_decr_path(path, 1);
                    } else {
                        path[0].indx = BPLUS_ITEM_COUNT;
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
                    if (path_flag) do_bplus_incr_path(path, 1);
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = BPLUS_GET_ELEM_ITEM(path[0].node, path[0].indx);
                bkey = ops->get_bkey(elem, &nbkey);
                if (BKEY_ISGT(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey))
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
                    if (path_flag) do_bplus_decr_path(path, 1);
                } else {
                    path[0].indx = BPLUS_ITEM_COUNT;
                }
            }
            if (path[0].node == NULL) {
                elem = NULL;
            } else {
                elem = BPLUS_GET_ELEM_ITEM(path[0].node, path[0].indx);
                bkey = ops->get_bkey(elem, &nbkey);
                if (BKEY_ISLT(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey))
                    elem = NULL;
            }
            break;
        }
    }
    return elem;
}

bplus_elem_item *bplus_find_next(bplus_meta *bplus,
                                 bplus_elem_posi *posi,
                                 const bkey_range *bkrange)
{
    bplus_ops *ops = bplus->ops;
    bplus_elem_item *elem;

    do_bplus_incr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = BPLUS_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        const void *bkey;
        uint32_t nbkey;
        int comp;
        bkey = ops->get_bkey(elem, &nbkey);
        comp = BKEY_COMP(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey);
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

bplus_elem_item *bplus_find_prev(bplus_meta *bplus,
                                 bplus_elem_posi *posi,
                                 const bkey_range *bkrange)
{
    bplus_ops *ops = bplus->ops;
    bplus_elem_item *elem;

    do_bplus_decr_posi(posi);
    if (posi->node == NULL) {
        posi->bkeq = false;
        return NULL;
    }

    elem = BPLUS_GET_ELEM_ITEM(posi->node, posi->indx);
    if (bkrange != NULL) {
        const void *bkey;
        uint32_t nbkey;
        int comp;
        bkey = ops->get_bkey(elem, &nbkey);
        comp = BKEY_COMP(bkey, nbkey, bkrange->to_bkey, bkrange->to_nbkey);
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

ENGINE_ERROR_CODE bplus_elem_add(bplus_meta *bplus,
                                 bplus_elem_posi *path, bplus_elem_item *elem,
                                 size_t *space_increased)
{
    /* create the root node if it does not exist */
    if (bplus->root == NULL) {
        bplus_indx_node *r_node = do_bplus_node_alloc(0);
        if (r_node == NULL) {
            return ENGINE_ENOMEM;
        }
        do_bplus_node_link(bplus, r_node, NULL, space_increased);

        path[0].node = bplus->root;
        path[0].indx = 0;
    }

    return do_bplus_elem_link(bplus, path, elem, space_increased);
}

bplus_elem_item *bplus_elem_replace(bplus_elem_posi *posi, bplus_elem_item *new_elem)
{
    bplus_elem_item *old_elem = BPLUS_GET_ELEM_ITEM(posi->node, posi->indx);

    old_elem->linked--;
    new_elem->linked++;
    posi->node->item[posi->indx] = new_elem;

    return old_elem;
}

bplus_elem_item *bplus_elem_delete(bplus_meta *bplus,
                                   const int bkrtype, const bkey_range *bkrange,
                                   const eflag_filter *efilter,
                                   void *delete_arg,
                                   uint32_t *opcost, size_t *space_decreased)
{
    bplus_indx_node *root = bplus->root;
    bplus_elem_item *elem;
    bplus_elem_posi path[BPLUS_MAX_DEPTH];

    if (opcost) *opcost = 0;
    if (root == NULL) return 0;

    assert(root->ndepth < BPLUS_MAX_DEPTH);
    elem = bplus_find_first(bplus, bkrtype, bkrange, path, true);
    if (elem == NULL) return NULL;

    assert(path[0].bkeq == true);
    if (opcost) *opcost += 1;
    if (efilter == NULL || bplus_elem_filter(bplus, elem, efilter)) {
        do_bplus_elem_unlink(bplus, path, space_decreased);
        bplus->ops->delete_post(elem, delete_arg);
        return elem;
    }
    return NULL;
}

uint32_t bplus_elem_delete_bulk(bplus_meta *bplus,
                                const int bkrtype, const bkey_range *bkrange,
                                const eflag_filter *efilter,
                                const uint32_t offset, const uint32_t count,
                                void *delete_arg,
                                uint32_t *opcost, size_t *space_decreased)
{
    bplus_indx_node *root = bplus->root;
    bplus_elem_item *elem;
    bplus_elem_posi path[BPLUS_MAX_DEPTH];
    bplus_elem_posi upth[BPLUS_MAX_DEPTH]; /* upper node path */
    uint32_t tot_found = 0;
    uint32_t cur_found = 0;
    uint32_t node_cnt = 1;
    uint32_t skip_cnt = 0;
    int i;
    bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);

    elem = bplus_find_first(bplus, bkrtype, bkrange, path, true);
    if (elem == NULL) return 0;

    bplus_elem_posi c_posi = path[0];
    bplus_elem_posi s_posi = c_posi; /* save the current posi */

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
        if (efilter == NULL || bplus_elem_filter(bplus, elem, efilter)) {
            if (skip_cnt < offset) {
                skip_cnt++;
            } else {
                elem->linked--;
                c_posi.node->item[c_posi.indx] = NULL;
                bplus->ops->delete_post(elem, delete_arg);

                cur_found++;
                if (count > 0 && (tot_found+cur_found) >= count) break;
            }
        }

        /* get the next element */
        if (c_posi.bkeq == true) {
            elem = NULL; /* reached to the end of bkey range */
        } else {
            elem = (forward ? bplus_find_next(bplus, &c_posi, bkrange)
                            : bplus_find_prev(bplus, &c_posi, bkrange));
        }
        if (elem == NULL) break;

        if (s_posi.node != c_posi.node) {
            node_cnt += 1;
            if (cur_found > 0) {
                do_bplus_node_remove_null_items(&s_posi, forward, cur_found);
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
                if (forward) do_bplus_incr_path(upth, 1);
                else         do_bplus_decr_path(upth, 1);
            }
            s_posi = c_posi;
        }
    } while (elem != NULL);

    if (cur_found > 0) {
        do_bplus_node_remove_null_items(&s_posi, forward, cur_found);
        /* decrement element count in upper nodes */
        for (i = 1; i <= root->ndepth; i++) {
            assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
            upth[i].node->ecnt[upth[i].indx] -= cur_found;
        }
        tot_found += cur_found;
    }
    if (tot_found > 0) {
        bplus->tot_elem_cnt -= tot_found;
        do_bplus_node_merge(bplus, path, forward, node_cnt, space_decreased);
    }
    return tot_found;
}

bplus_elem_item *bplus_delete_first_elem(bplus_meta *bplus, size_t *space_decreased)
{
    bplus_elem_posi path[BPLUS_MAX_DEPTH];
    bplus_indx_node *leaf = bplus_get_first_leaf(bplus->root, path);
    path[0].node = leaf;
    path[0].indx = 0;
    bplus_elem_item *elem = BPLUS_GET_ELEM_ITEM(leaf, 0);
    do_bplus_elem_unlink(bplus, path, space_decreased);
    return elem;
}

bplus_elem_item *bplus_delete_last_elem(bplus_meta *bplus, size_t *space_decreased)
{
    bplus_elem_posi path[BPLUS_MAX_DEPTH];
    bplus_indx_node *leaf = bplus_get_last_leaf(bplus->root, path);
    path[0].node = leaf;
    path[0].indx = leaf->used_count - 1;
    bplus_elem_item *elem = BPLUS_GET_ELEM_ITEM(leaf, leaf->used_count - 1);
    do_bplus_elem_unlink(bplus, path, space_decreased);
    return elem;
}

uint32_t bplus_posi_outside(const bplus_elem_posi *posi, const int bkrtype)
{
    if (posi->node == NULL) {
        if (posi->indx == BPLUS_ITEM_COUNT) return BPLUS_OUTSIDE_LEFT;
        if (posi->indx == 0)                return BPLUS_OUTSIDE_RIGHT;
        return 0;
    }
    /* the bkey of the found elem isn't same with the from_bkey of bkey range */
    assert(posi->node->ndepth == 0); /* leaf node */
    if (bkrtype == BKEY_RANGE_TYPE_ASC) {
        if (posi->node->prev == NULL && posi->indx == 0) /* the first element */
            return BPLUS_OUTSIDE_LEFT;
    } else if (bkrtype == BKEY_RANGE_TYPE_DSC) {
        if (posi->node->next == NULL && posi->indx == posi->node->used_count - 1) /* the last element */
            return BPLUS_OUTSIDE_RIGHT;
    }
    return 0;
}

bool bplus_elem_get(bplus_meta *bplus,
                    const int bkrtype, const bkey_range *bkrange,
                    const eflag_filter *efilter,
                    const bool delete, void *delete_arg,
                    bplus_elem_item **elem_array,
                    uint32_t *opcost, uint32_t *outside, size_t *space_decreased)
{
    assert(bplus->root);
    bplus_indx_node *root = bplus->root;
    bplus_elem_item *elem;
    bplus_elem_posi path[BPLUS_MAX_DEPTH];

    assert(root->ndepth < BPLUS_MAX_DEPTH);
    elem = bplus_find_first(bplus, bkrtype, bkrange, path, delete);
    if (elem == NULL) {
        if (outside) *outside = bplus_posi_outside(&path[0], BKEY_RANGE_TYPE_SIN);
        return false;
    }

    if (opcost) *opcost += 1;
    if (outside) *outside = 0;
    if (efilter == NULL || bplus_elem_filter(bplus, elem, efilter)) {
        elem->refcount++;
        if (delete) {
            do_bplus_elem_unlink(bplus, path, space_decreased);
            bplus->ops->delete_post(elem, delete_arg);
        }
        elem_array[0] = elem;
        return true;
    }
    return false;
}

uint32_t bplus_elem_get_bulk(bplus_meta *bplus,
                             const int bkrtype, const bkey_range *bkrange,
                             const eflag_filter *efilter,
                             const uint32_t offset, const uint32_t count,
                             const bool delete, void *delete_arg,
                             bplus_elem_item **elem_array,
                             uint32_t *opcost, uint32_t *outside, size_t *space_decreased)
{
    assert(bplus->root);
    bplus_indx_node *root = bplus->root;
    bplus_elem_item *elem;
    bplus_elem_posi path[BPLUS_MAX_DEPTH];
    bplus_elem_posi upth[BPLUS_MAX_DEPTH]; /* upper node path */
    uint32_t tot_found = 0;
    uint32_t cur_found = 0;
    uint32_t node_cnt = 1;
    uint32_t skip_cnt = 0;
    int i;
    bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);

    assert(root->ndepth < BPLUS_MAX_DEPTH);
    elem = bplus_find_first(bplus, bkrtype, bkrange, path, delete);
    if (elem == NULL) {
        if (outside) *outside = bplus_posi_outside(&path[0], bkrtype);
        return 0;
    }

    bplus_elem_posi c_posi = path[0];
    bplus_elem_posi s_posi = c_posi; /* save the current posi */
    if (outside) *outside = 0;

    if (c_posi.bkeq == false) {
        if (outside) *outside |= bplus_posi_outside(&c_posi, bkrtype);
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
        if (efilter == NULL || bplus_elem_filter(bplus, elem, efilter)) {
            if (skip_cnt < offset) {
                skip_cnt++;
            } else {
                elem->refcount++;
                elem_array[tot_found+cur_found] = elem;
                if (delete) {
                    elem->linked--;
                    c_posi.node->item[c_posi.indx] = NULL;
                    bplus->ops->delete_post(elem, delete_arg);
                }
                cur_found++;
                if (count > 0 && (tot_found+cur_found) >= count) break;
            }
        }

        /* get the next element */
        if (c_posi.bkeq == true) {
            elem = NULL; /* reached to the end of bkey range */
        } else {
            elem = (forward ? bplus_find_next(bplus, &c_posi, bkrange)
                            : bplus_find_prev(bplus, &c_posi, bkrange));
        }
        if (elem == NULL) break;

        if (s_posi.node != c_posi.node) {
            node_cnt += 1;
            if (cur_found > 0) {
                if (delete) {
                    do_bplus_node_remove_null_items(&s_posi, forward, cur_found);
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
                if (forward) do_bplus_incr_path(upth, 1);
                else         do_bplus_decr_path(upth, 1);
            }
            s_posi = c_posi;
        }
    } while (elem != NULL);

    if (cur_found > 0) {
        if (delete) {
            do_bplus_node_remove_null_items(&s_posi, forward, cur_found);
            /* decrement element count in upper nodes */
            for (i = 1; i <= root->ndepth; i++) {
                assert(upth[i].node->ecnt[upth[i].indx] >= cur_found);
                upth[i].node->ecnt[upth[i].indx] -= cur_found;
            }
        }
        tot_found += cur_found;
    }

    if (delete && tot_found > 0) {
        bplus->tot_elem_cnt -= tot_found;
        do_bplus_node_merge(bplus, path, forward, node_cnt, space_decreased);
    }

    if (c_posi.node == NULL) {
        if (outside) *outside |= bplus_posi_outside(&c_posi, bkrtype);
    }

    return tot_found;
}

int bplus_posi_find(bplus_meta *bplus,
                    const int bkrtype, const bkey_range *bkrange,
                    ENGINE_BTREE_ORDER order)
{
    bplus_elem_posi  path[BPLUS_MAX_DEPTH];
    bplus_elem_item *elem;
    int bpos; /* btree position */

    if (bplus->root == NULL) return -1; /* not found */

    elem = bplus_find_first(bplus, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        assert(path[0].bkeq == true);
        bpos = do_bplus_posi_from_path(bplus, path, order);
        assert(bpos >= 0);
    } else {
        bpos = -1; /* not found */
    }
    return bpos;
}

int bplus_posi_find_with_get(bplus_meta *bplus,
                             const int bkrtype, const bkey_range *bkrange,
                             ENGINE_BTREE_ORDER order, const int count,
                             bplus_elem_item **elem_array,
                             uint32_t *elem_count, uint32_t *elem_index)
{
    bplus_elem_posi  path[BPLUS_MAX_DEPTH];
    bplus_elem_item *elem;
    int bpos = -1; /* NOT found */

    if (bplus->root == NULL) return -1; /* not found */

    elem = bplus_find_first(bplus, bkrtype, bkrange, path, true);
    if (elem != NULL) {
        int ecnt, eidx;
        assert(path[0].bkeq == true);
        bpos = do_bplus_posi_from_path(bplus, path, order);
        assert(bpos >= 0);

        ecnt = 1;                             /* elem count */
        eidx = (bpos < count) ? bpos : count; /* elem index in elem array */
        elem->refcount++;
        elem_array[eidx] = elem;

        if (order == BTREE_ORDER_ASC) {
            ecnt += do_bplus_elem_batch_get(path[0], eidx,  false, true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += do_bplus_elem_batch_get(path[0], count, true,  false, &elem_array[eidx+1]);
        } else {
            ecnt += do_bplus_elem_batch_get(path[0], eidx,  true,  true,  &elem_array[0]);
            assert((ecnt-1) == eidx);
            ecnt += do_bplus_elem_batch_get(path[0], count, false, false, &elem_array[eidx+1]);
        }
        *elem_count = (uint32_t)ecnt;
        *elem_index = (uint32_t)eidx;
    }
    return bpos; /* btree_position */
}

ENGINE_ERROR_CODE bplus_elem_get_by_posi(bplus_meta *bplus,
                                         const int index, const uint32_t count, const bool forward,
                                         bplus_elem_item **elem_array, uint32_t *elem_count)
{
    bplus_elem_posi  posi;
    bplus_indx_node *node;
    bplus_elem_item *elem;
    int i, tot_ecnt;
    uint32_t nfound; /* found count */

    if (bplus->root == NULL) return ENGINE_ELEM_ENOENT;

    node = bplus->root;
    tot_ecnt = 0;
    while (node->ndepth > 0) {
        for (i = 0; i < node->used_count; i++) {
            assert(node->ecnt[i] > 0);
            if ((tot_ecnt + node->ecnt[i]) > index) break;
            tot_ecnt += node->ecnt[i];
        }
        assert(i < node->used_count);
        node = (bplus_indx_node *)node->item[i];
    }
    assert(node->ndepth == 0);
    posi.node = node;
    posi.indx = index-tot_ecnt;
    posi.bkeq = false;

    elem = BPLUS_GET_ELEM_ITEM(posi.node, posi.indx);
    elem->refcount++;
    elem_array[0] = elem;
    nfound = 1;
    nfound += do_bplus_elem_batch_get(posi, count-1, forward, false, &elem_array[nfound]);

    *elem_count = nfound;
    if (*elem_count > 0) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ELEM_ENOENT;
    }
}

uint32_t bplus_elem_count(bplus_meta *bplus,
                          const int bkrtype, const bkey_range *bkrange,
                          const eflag_filter *efilter, uint32_t *opcost)
{
    bplus_elem_posi  posi;
    bplus_elem_item *elem;
    bplus_ops       *ops = bplus->ops;
    uint32_t tot_found = 0; /* total found count */
    uint32_t tot_access = 0; /* total access count */

    if (opcost) {
        *opcost = 0;
    }

    if (bplus->root == NULL) {
        return 0;
    }

#if 1 // BOP_COUNT_OPTIMIZE
    /* check if the bkey range is full range */
    if (bkrtype != BKEY_RANGE_TYPE_SIN && efilter == NULL) {
        bplus_elem_item *min_bkey_elem = bplus_get_first_elem(bplus->root);
        bplus_elem_item *max_bkey_elem = bplus_get_last_elem(bplus->root);

        uint32_t min_nbkey, max_nbkey;
        const void *min_bkey = ops->get_bkey(min_bkey_elem, &min_nbkey);
        const void *max_bkey = ops->get_bkey(max_bkey_elem, &max_nbkey);

        int min_comp, max_comp;
        if (bkrtype == BKEY_RANGE_TYPE_ASC) {
            min_comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey, min_bkey, min_nbkey);
            max_comp = BKEY_COMP(bkrange->to_bkey,   bkrange->to_nbkey,   max_bkey, max_nbkey);
        } else { /* BKEY_RANGE_TYPE_DSC */
            min_comp = BKEY_COMP(bkrange->to_bkey,   bkrange->to_nbkey,   min_bkey, min_nbkey);
            max_comp = BKEY_COMP(bkrange->from_bkey, bkrange->from_nbkey, max_bkey, max_nbkey);
        }
        if (min_comp <= 0 && max_comp >= 0) {
            return bplus->tot_elem_cnt;
        }
    }
#endif

    elem = bplus_find_first(bplus, bkrtype, bkrange, &posi, false);
    if (elem != NULL) {
        if (bkrtype == BKEY_RANGE_TYPE_SIN) {
            assert(posi.bkeq == true);
            tot_access++;
            if (efilter == NULL || bplus_elem_filter(bplus, elem, efilter))
                tot_found++;
        } else { /* BKEY_RANGE_TYPE_ASC || BKEY_RANGE_TYPE_DSC */
            bool forward = (bkrtype == BKEY_RANGE_TYPE_ASC ? true : false);
            posi.bkeq = false;
            do {
                tot_access++;
                if (efilter == NULL || bplus_elem_filter(bplus, elem, efilter))
                    tot_found++;

                if (posi.bkeq == true) {
                    elem = NULL; break;
                }
                elem = (forward ? bplus_find_next(bplus, &posi, bkrange)
                                : bplus_find_prev(bplus, &posi, bkrange));
            } while (elem != NULL);
        }
    }
    if (opcost)
        *opcost = tot_access;
    return tot_found;
}
