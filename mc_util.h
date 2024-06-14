/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2018 JaM2in Co., Ltd.
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
#ifndef MC_UTIL_H
#define MC_UTIL_H

#include <memcached/callback.h>
#include <memcached/extension.h>

/* length of string representing 4 bytes integer is 10 */
#define UINT32_STR_LENG 10

/*
 * token buffer structure
 */
typedef struct _token_buff {
    token_t *array;
    uint32_t count;
    uint32_t nused;
} token_buff_t;

/*
 * memory block structure
 */
typedef struct _mblck_node {
    struct _mblck_node *next;
    char data[1];
} mblck_node_t;

typedef struct _mblck_list {
    void         *pool;
    mblck_node_t *head;
    mblck_node_t *tail;
    uint32_t  blck_cnt;
    uint32_t  body_len;
    uint32_t  item_cnt;
    uint32_t  item_len;
} mblck_list_t;

typedef struct _mblck_pool {
    mblck_node_t *head;
    mblck_node_t *tail;
    uint32_t  blck_len;
    uint32_t  body_len;
    uint32_t  used_cnt;
    uint32_t  free_cnt;
} mblck_pool_t;

/*
 * memory block macros
 */
#define MBLCK_HEAD(l) ((l)->head)
#define MBLCK_TAIL(l) ((l)->tail)
// #define MBLCK_ICNT(l) ((l)->item_cnt)
// #define MBLCK_ILEN(l) ((l)->item_len)
#define MBLCK_COUNT(l) ((l)->blck_cnt)
#define MBLCK_BDLEN(l) ((l)->body_len)
#define NEXT_MBLCK(b) ((b)->next)
#define DATA_MBLCK(b) ((b)->data)

/* memory block functions */
int  mblck_pool_create(mblck_pool_t *pool, uint32_t blck_len, uint32_t blck_cnt);
void mblck_pool_destroy(mblck_pool_t *pool);
int  mblck_list_alloc(mblck_pool_t *pool, uint32_t item_len, uint32_t item_cnt,
                      mblck_list_t *list);
void mblck_list_merge(mblck_list_t *pri_list, mblck_list_t *add_list);
void mblck_list_free(mblck_pool_t *pool, mblck_list_t *list);

/* token buffer functions */
int   token_buff_create(token_buff_t *buff, uint32_t count);
void  token_buff_destroy(token_buff_t *buff);
void *token_buff_get(token_buff_t *buff, uint32_t count);
void  token_buff_release(token_buff_t *buff, void *tokens);

/* tokenize functions */
size_t tokenize_command(char *command, int cmdlen, token_t *tokens, const size_t max_tokens);
int    detokenize(token_t *tokens, int ntokens, char *buffer, int length);
int    tokenize_keys(char *keystr, int keylen, int keycnt, char delimiter, token_t *tokens);
ENGINE_ERROR_CODE tokenize_mblocks(mblck_list_t *blist, int keylen, int keycnt,
                                   int maxklen, bool must_backward_compatible,
                                   token_t *tokens);
ENGINE_ERROR_CODE tokenize_sblocks(mblck_list_t *blist, int keylen, int keycnt,
                                   int maxklen, bool must_backward_compatible,
                                   token_t *tokens);

/* event callback functions */
void register_callback(ENGINE_HANDLE *eh,
                       ENGINE_EVENT_TYPE type,
                       EVENT_CALLBACK cb, const void *cb_data);
void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data, const void *c);

#endif
