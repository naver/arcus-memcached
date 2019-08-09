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
#include "default_engine.h"
#include "item_clog.h"

bool item_clog_enabled = false;

static struct engine_config *config=NULL; // engine config

static EXTENSION_LOGGER_DESCRIPTOR *logger;

/*
 * Generate change logs
 */
void CLOG_GE_ITEM_LINK(hash_item *it)
{
    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
    }
}

void CLOG_GE_ITEM_UNLINK(hash_item *it, enum item_unlink_cause cause)
{
    if ((cause == ITEM_UNLINK_NORMAL ||
         cause == ITEM_UNLINK_EVICT || cause == ITEM_UNLINK_STALE) &&
        (it->iflag & ITEM_INTERNAL) == 0)
    {
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
    }
}

void CLOG_GE_LIST_ELEM_INSERT(list_meta_info *info,
                              const int index, list_elem_item *elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
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
    }
}

void CLOG_GE_MAP_ELEM_INSERT(map_meta_info *info,
                             map_elem_item *old_elem,
                             map_elem_item *new_elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
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
    }
}

void CLOG_GE_SET_ELEM_INSERT(set_meta_info *info,
                             set_elem_item *elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
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
    }
}

void CLOG_GE_BTREE_ELEM_INSERT(btree_meta_info *info,
                               btree_elem_item *old_elem,
                               btree_elem_item *new_elem)
{
    hash_item *it = (hash_item *)COLL_GET_HASH_ITEM(info);

    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
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
    }
}

void CLOG_GE_ITEM_SETATTR(hash_item *it,
                          ENGINE_ITEM_ATTR *attr_ids, uint32_t attr_cnt)
{
    if ((it->iflag & ITEM_INTERNAL) == 0)
    {
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
