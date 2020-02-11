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
#include <assert.h>

#include "default_engine.h"
#include "item_clog.h"
#ifdef ENABLE_PERSISTENCE
#include "cmdlogmgr.h"
#include "cmdlogrec.h"
#include "cmdlogbuf.h"
#endif

bool item_clog_enabled = false;

static struct engine_config *config=NULL; // engine config

static EXTENSION_LOGGER_DESCRIPTOR *logger;

#ifdef ENABLE_PERSISTENCE
static struct assoc_scan *scanp=NULL; // checkpoint scan pointer

#define NEED_DUAL_WRITE(it) ((scanp != NULL) && (assoc_scan_in_visited_area(scanp, it)))
#endif

/*
 * Generate change logs
 */
void CLOG_GE_ITEM_LINK(hash_item *it)
{
    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
        if (config->use_persistence) {
            log_waiter_t *waiter = cmdlog_get_cur_waiter();
            if (waiter->elem_clog_with_collection == false) {
                ITLinkLog log;
                (void)lrec_construct_link_item((LogRec*)&log, it);
                log_record_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
            }
        }
#else
        if (config->use_persistence) {
            ITLinkLog log;
            (void)lrec_construct_link_item((LogRec*)&log, it);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
        }
#endif
#endif
    }
}

void CLOG_GE_ITEM_UNLINK(hash_item *it, enum item_unlink_cause cause)
{
    if ((cause == ITEM_UNLINK_NORMAL ||
         cause == ITEM_UNLINK_EVICT || cause == ITEM_UNLINK_STALE) &&
        (it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
        if (config->use_persistence) {
            log_waiter_t *waiter = cmdlog_get_cur_waiter();
            /* TODO::remove waiter == NULL condition after waiter process in eviction logic */
            if (waiter == NULL || waiter->elem_clog_with_collection == false) {
                ITUnlinkLog log;
                (void)lrec_construct_unlink_item((LogRec*)&log, it);
                log_record_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
            }
        }
#else
        if (config->use_persistence) {
            ITUnlinkLog log;
            (void)lrec_construct_unlink_item((LogRec*)&log, it);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
        }
#endif
#endif
    }
}

void CLOG_GE_ITEM_UPDATE(hash_item *it)
{
    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
    }
}

void CLOG_GE_ITEM_FLUSH(const char *prefix, const int nprefix, time_t when)
{
    if (1)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            if (when <= 0) {
                ITFlushLog log;
                (void)lrec_construct_flush_item((LogRec*)&log, prefix, nprefix);
                bool need_dual_write = (scanp != NULL ? true : false);
                log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), need_dual_write);
            }
        }
#endif
    }
}

void CLOG_GE_LIST_ELEM_INSERT(list_meta_info *info,
                              const int index, list_elem_item *elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            ListElemInsLog log;
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
            lrec_attr_info attr;
            log_waiter_t *waiter = cmdlog_get_cur_waiter();
            bool create = waiter->elem_clog_with_collection;
            (void)lrec_construct_list_elem_insert((LogRec*)&log, it, info->ccnt, index, elem, create, &attr);
            log_record_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
#else
            (void)lrec_construct_list_elem_insert((LogRec*)&log, it, info->ccnt, index, elem);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
#endif
        }
#endif
    }
}

void CLOG_GE_LIST_ELEM_DELETE(list_meta_info *info,
                              int index, uint32_t count, const bool forward,
                              enum elem_delete_cause cause)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((cause == ELEM_DELETE_NORMAL) &&
        (it->iflag & ITEM_INTERNAL) == 0)
    {
        if (forward == false) {
            /* change it to the forward delete */
            if (index < 0) {
                index += info->ccnt;
                if (index < 0)
                    index = 0;
            }
            if (index < count) {
                count = index + 1;
                index = 0;
            } else {
                index -= (count-1);
            }
        }
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            ListElemDelLog log;
            (void)lrec_construct_list_elem_delete((LogRec*)&log, it, info->ccnt, index, count);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
        }
#endif
    }
}

void CLOG_GE_MAP_ELEM_INSERT(map_meta_info *info,
                             map_elem_item *old_elem,
                             map_elem_item *new_elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            MapElemInsLog log;
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
            lrec_attr_info attr;
            log_waiter_t *waiter = cmdlog_get_cur_waiter();
            bool create = waiter->elem_clog_with_collection;
            (void)lrec_construct_map_elem_insert((LogRec*)&log, it, new_elem, create, &attr);
            log_record_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
#else
            (void)lrec_construct_map_elem_insert((LogRec*)&log, it, new_elem);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
#endif
        }
#endif
    }
}

void CLOG_GE_MAP_ELEM_DELETE(map_meta_info *info,
                             map_elem_item *elem,
                             enum elem_delete_cause cause)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((cause == ELEM_DELETE_NORMAL) &&
        (it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            MapElemDelLog log;
            (void)lrec_construct_map_elem_delete((LogRec*)&log, it, elem);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
        }
#endif
    }
}

void CLOG_GE_SET_ELEM_INSERT(set_meta_info *info,
                             set_elem_item *elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            SetElemInsLog log;
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
            lrec_attr_info attr;
            log_waiter_t *waiter = cmdlog_get_cur_waiter();
            bool create = waiter->elem_clog_with_collection;
            (void)lrec_construct_set_elem_insert((LogRec*)&log, it, elem, create, &attr);
            log_record_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
#else
            (void)lrec_construct_set_elem_insert((LogRec*)&log, it, elem);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
#endif
        }
#endif
    }
}

void CLOG_GE_SET_ELEM_DELETE(set_meta_info *info,
                             set_elem_item *elem,
                             enum elem_delete_cause cause)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((cause == ELEM_DELETE_NORMAL) &&
        (it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            SetElemDelLog log;
            (void)lrec_construct_set_elem_delete((LogRec*)&log, it, elem);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
        }
#endif
    }
}

void CLOG_GE_BTREE_ELEM_INSERT(btree_meta_info *info,
                               btree_elem_item *old_elem,
                               btree_elem_item *new_elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            BtreeElemInsLog log;
#ifdef ENABLE_PERSISTENCE_03_OPTIMIZE
            lrec_attr_info attr;
            log_waiter_t *waiter = cmdlog_get_cur_waiter();
            bool create = waiter->elem_clog_with_collection;
            (void)lrec_construct_btree_elem_insert((LogRec*)&log, it, new_elem, create, &attr);
            log_record_write((LogRec*)&log, waiter, NEED_DUAL_WRITE(it));
#else
            (void)lrec_construct_btree_elem_insert((LogRec*)&log, it, new_elem);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
#endif
        }
#endif
    }
}

void CLOG_GE_BTREE_ELEM_DELETE(btree_meta_info *info,
                               btree_elem_item *elem,
                               enum elem_delete_cause cause)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((cause == ELEM_DELETE_NORMAL) &&
        (it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            BtreeElemDelLog log;
            (void)lrec_construct_btree_elem_delete((LogRec*)&log, it, elem);
            log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
        }
#endif
    }
}

void CLOG_GE_ITEM_SETATTR(hash_item *it,
                          ENGINE_ITEM_ATTR *attr_ids, uint32_t attr_cnt)
{
    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
#ifdef ENABLE_PERSISTENCE
        if (config->use_persistence) {
            uint8_t attr_type = 0;
            for (int i = 0; i < attr_cnt; i++) {
                if (attr_ids[i] == ATTR_EXPIRETIME) {
                    if (attr_type < UPD_SETATTR_EXPTIME)
                        attr_type = UPD_SETATTR_EXPTIME;
                }
                else if (attr_ids[i] == ATTR_MAXCOUNT ||
                         attr_ids[i] == ATTR_OVFLACTION ||
                         attr_ids[i] == ATTR_READABLE) {
                    if (attr_type < UPD_SETATTR_EXPTIME_INFO)
                        attr_type = UPD_SETATTR_EXPTIME_INFO;
                }
                else if (attr_ids[i] == ATTR_MAXBKEYRANGE) {
                    if (attr_type < UPD_SETATTR_EXPTIME_INFO_BKEY)
                        attr_type = UPD_SETATTR_EXPTIME_INFO_BKEY;
                }
            }
            if (attr_type > 0) {
                ITSetAttrLog log;
                (void)lrec_construct_setattr((LogRec*)&log, it, attr_type);
                log_record_write((LogRec*)&log, cmdlog_get_cur_waiter(), NEED_DUAL_WRITE(it));
            }
        }
#endif
    }
}

/*
 * Initialize change log module
 */
void item_clog_init(struct default_engine *engine)
{
    config = &engine->config;
    logger = engine->server.log->get_logger();

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM change log module initialized.\n");
}

void item_clog_final(struct default_engine *engine)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM change log module destroyed.\n");
}

void item_clog_set_enable(bool enable)
{
    item_clog_enabled = enable;
}

#ifdef ENABLE_PERSISTENCE
void item_clog_set_scan(struct assoc_scan *cs)
{
    /* Cache locked */
    if (config->use_persistence) {
        assert(scanp == NULL);
        scanp = cs;
    }
}

void item_clog_reset_scan(bool success)
{
    /* Cache locked */
    if (scanp != NULL) {
        scanp = NULL;
        cmdlog_complete_dual_write(success);
    }
}
#endif
