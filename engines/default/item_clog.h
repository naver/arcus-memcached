/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2019 JaM2in Co., Ltd.
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
#ifndef ITEM_CLOG_H
#define ITEM_CLOG_H

extern bool item_clog_enabled;

/* functions for generate change logs */
void CLOG_GE_ITEM_LINK(hash_item *it);
void CLOG_GE_ITEM_UNLINK(hash_item *it, enum item_unlink_cause cause);
void CLOG_GE_ITEM_UPDATE(hash_item *it);
void CLOG_GE_ITEM_FLUSH(const char *prefix, const int nprefix, time_t when);
void CLOG_GE_LIST_ELEM_INSERT(list_meta_info *info,
                              const int index, list_elem_item *elem);
void CLOG_GE_LIST_ELEM_DELETE(list_meta_info *info,
                              int index, uint32_t count, const bool forward,
                              enum elem_delete_cause cause);
void CLOG_GE_MAP_ELEM_INSERT(map_meta_info *info,
                             map_elem_item *old_elem,
                             map_elem_item *new_elem);
void CLOG_GE_MAP_ELEM_DELETE(map_meta_info *info,
                             map_elem_item *elem,
                             enum elem_delete_cause cause);
void CLOG_GE_SET_ELEM_INSERT(set_meta_info *info,
                             set_elem_item *elem);
void CLOG_GE_SET_ELEM_DELETE(set_meta_info *info,
                             set_elem_item *elem,
                             enum elem_delete_cause cause);
void CLOG_GE_BTREE_ELEM_INSERT(btree_meta_info *info,
                               btree_elem_item *old_elem,
                               btree_elem_item *new_elem);
void CLOG_GE_BTREE_ELEM_DELETE(btree_meta_info *info,
                               btree_elem_item *elem,
                               enum elem_delete_cause cause);
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
void CLOG_GE_BTREE_ELEM_DELETE_LGCAL(btree_meta_info *info,
                                     const bkey_range *bkrange,
                                     const eflag_filter *efilter,
                                     uint32_t offset, uint32_t reqcount);
#endif
void CLOG_GE_ITEM_SETATTR(hash_item *it,
                          ENGINE_ITEM_ATTR *attr_ids, uint32_t attr_cnt);

/* macros for CLOG function substitution. */
#define CLOG_ITEM_LINK(a) \
    if (item_clog_enabled) { \
        CLOG_GE_ITEM_LINK(a); \
    }
#define CLOG_ITEM_UNLINK(a,b) \
    if (item_clog_enabled) { \
        CLOG_GE_ITEM_UNLINK(a,b); \
    }
#define CLOG_ITEM_UPDATE(a) \
    if (item_clog_enabled) { \
        CLOG_GE_ITEM_UPDATE(a); \
    }
#define CLOG_ITEM_FLUSH(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_ITEM_FLUSH(a,b,c); \
    }
#define CLOG_LIST_ELEM_INSERT(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_LIST_ELEM_INSERT(a,b,c); \
    }
#define CLOG_LIST_ELEM_DELETE(a,b,c,d,e) \
    if (item_clog_enabled) { \
        CLOG_GE_LIST_ELEM_DELETE(a,b,c,d,e); \
    }
#define CLOG_MAP_ELEM_INSERT(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_MAP_ELEM_INSERT(a,b,c); \
    }
#define CLOG_MAP_ELEM_DELETE(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_MAP_ELEM_DELETE(a,b,c); \
    }
#define CLOG_SET_ELEM_INSERT(a,b) \
    if (item_clog_enabled) { \
        CLOG_GE_SET_ELEM_INSERT(a,b); \
    }
#define CLOG_SET_ELEM_DELETE(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_SET_ELEM_DELETE(a,b,c); \
    }
#define CLOG_BTREE_ELEM_INSERT(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_BTREE_ELEM_INSERT(a,b,c); \
    }
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
#define CLOG_BTREE_ELEM_DELETE(gen_logical,a,b,c) \
    if (item_clog_enabled && !gen_logical) { \
        CLOG_GE_BTREE_ELEM_DELETE(a,b,c); \
    }
#define CLOG_BTREE_ELEM_DELETE_LGCAL(gen_logical,a,b,c,d,e) \
    if (item_clog_enabled && gen_logical) { \
        CLOG_GE_BTREE_ELEM_DELETE_LGCAL(a,b,c,d,e); \
    }
#else
#define CLOG_BTREE_ELEM_DELETE(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_BTREE_ELEM_DELETE(a,b,c); \
    }
#endif
#define CLOG_ITEM_SETATTR(a,b,c) \
    if (item_clog_enabled) { \
        CLOG_GE_ITEM_SETATTR(a,b,c); \
    }

/* functions for initialize change log module */
void item_clog_init(struct default_engine *engine);
void item_clog_final(struct default_engine *engine);
void item_clog_set_enable(bool enable);
#ifdef ENABLE_PERSISTENCE
void item_clog_set_scan(struct assoc_scan *cs);
void item_clog_reset_scan(bool success);
#endif

#endif
