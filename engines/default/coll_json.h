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
 *
 */
#ifndef ITEM_COLL_JSON_H
#define ITEM_COLL_JSON_H

#include "item_base.h"
#include "json/jsondata.h"

/*
 * JSON Collection
 */
ENGINE_ERROR_CODE json_struct_create(const char *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie);

json_elem_item *json_elem_alloc(struct default_engine *engine, json_node_type type,
                                json_value *value, const void *cookie);

ENGINE_ERROR_CODE json_elem_append(struct default_engine *engine, json_elem_item **dest,
                                   json_elem_item *e, json_elem_item *e_temp,
                                   json_node_type type, const void *cookie);

ENGINE_ERROR_CODE json_elem_delete(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   const char *path, const size_t npath,
                                   const bool drop_if_empty,
                                   bool *dropped, const void *cookie);

ENGINE_ERROR_CODE json_elem_get(struct default_engine *engine,
                                const char *key, const size_t nkey,
                                const char *path, const size_t npath,
                                json_elem_item **elem, void **it_ptr,
                                json_elem_item **node_array, int *parent_idx_array,
                                int *node_count);

ENGINE_ERROR_CODE json_elem_set(struct default_engine *engine,
                                const char *key, const size_t nkey,
                                const char *path, const size_t npath,
                                json_elem_item *elem_item, item_attr *attrp,
                                bool *created, const void *cookie);

ENGINE_ERROR_CODE json_elem_render(json_elem_item **nodes, int *parent_idxs,
                                int node_count, char **buffer, size_t *len);
ENGINE_ERROR_CODE json_elem_unlink(struct default_engine *handle, json_elem_item **e);
void json_elem_release(struct default_engine *engine, json_elem_item **nodes, int count);

void json_elem_scalar(struct default_engine *engine, json_elem_item **e, json_elem_item **dest);

ENGINE_ERROR_CODE item_json_coll_init(void *engine_ptr);
void item_json_coll_final(void *engine_ptr);

#endif
