/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "mc_util.h"

/*
 * memory block : internal functions
 */
static void do_mblck_pool_init(mblck_pool_t *pool, uint32_t blck_len)
{
    pool->tail = NULL;
    pool->head = NULL;
    pool->blck_len = blck_len;
    pool->body_len = blck_len - sizeof(void *);
    pool->used_cnt = 0;
    pool->free_cnt = 0;
}

static uint32_t do_mblck_pool_grow(mblck_pool_t *pool, uint32_t blck_cnt)
{
    mblck_node_t *blck_ptr;
    int count;

    for (count = 0; count < blck_cnt; count++) {
        blck_ptr = (mblck_node_t *)malloc(pool->blck_len);
        if (blck_ptr == NULL) {
            break;
        }

        blck_ptr->next = NULL;
        if (pool->tail) pool->tail->next = blck_ptr;
        else            pool->head = blck_ptr;
        pool->tail = blck_ptr;
    }
    pool->free_cnt += count;
    return count;
}

static void do_mblck_pool_final(mblck_pool_t *pool)
{
    mblck_node_t *blck_ptr;

    while (pool->head != NULL) {
        blck_ptr = pool->head;
        pool->head = pool->head->next;
        free(blck_ptr);
        pool->free_cnt -= 1;
    }
    assert(pool->free_cnt == 0);
    pool->tail = NULL;

    pool->blck_len = 0;
    pool->body_len = 0;
}

/*
 * memory block : external functions
 */
int mblck_pool_create(mblck_pool_t *pool, uint32_t blck_len, uint32_t blck_cnt)
{
    do_mblck_pool_init(pool, blck_len);
    if (do_mblck_pool_grow(pool, blck_cnt) < blck_cnt) {
        /* incomplete memory block pool */
        do_mblck_pool_final(pool);
        return -1;
    }
    return 0;
}

void mblck_pool_destroy(mblck_pool_t *pool)
{
    assert(pool->used_cnt == 0);
    do_mblck_pool_final(pool);
}

int mblck_list_alloc(mblck_pool_t *pool, uint32_t item_len, uint32_t item_cnt,
                     mblck_list_t *list)
{
    uint32_t nitems_per_blck = pool->body_len / item_len;
    uint32_t blck_cnt = ((item_cnt-1) / nitems_per_blck) + 1;

    if (pool->free_cnt < blck_cnt) {
        uint32_t need_cnt = blck_cnt - pool->free_cnt;
        if (do_mblck_pool_grow(pool, need_cnt) < need_cnt) {
            return -1; /* out of memory */
        }
    }
    assert(pool->free_cnt >= blck_cnt);

    list->pool = (void*)pool;
    list->head = pool->head;
    list->tail = list->head;
    list->blck_cnt = 1;
    while (list->blck_cnt < blck_cnt) {
        list->tail = list->tail->next;
        list->blck_cnt += 1;
    }
    pool->head = list->tail->next;
    list->tail->next = NULL;

    if (pool->head == NULL) {
        pool->tail = NULL;
    }
    pool->used_cnt += list->blck_cnt;
    pool->free_cnt -= list->blck_cnt;

    list->item_cnt = item_cnt;
    list->item_len = item_len;
    list->body_len = nitems_per_blck * item_len;
    return 0;
}

void mblck_list_merge(mblck_list_t *pri_list, mblck_list_t *add_list)
{
    assert(pri_list->pool == add_list->pool);
    //assert(pri_list->item_len == add_list->item_len);
    pri_list->tail->next = add_list->head;
    pri_list->tail = add_list->tail;
    pri_list->blck_cnt += add_list->blck_cnt;
    /* clear the add_list */
    add_list->head = NULL;
    add_list->tail = NULL;
    add_list->blck_cnt = 0;
    /* FIXME: item_cnt and item_len: how to merge them ? */
}

void mblck_list_free(mblck_pool_t *pool, mblck_list_t *list)
{
    assert(list->pool != NULL);

    if (list->blck_cnt > 0) {
        list->tail->next = pool->head;
        pool->head = list->head;
        if (pool->tail == NULL) {
            pool->tail = list->tail;
        }
        pool->used_cnt -= list->blck_cnt;
        pool->free_cnt += list->blck_cnt;
        /* clear the list */
        list->head = NULL;
        list->tail = NULL;
        list->blck_cnt = 0;
    }
    list->pool = NULL;
}

/*
 * token buffer functions
 */
int token_buff_create(token_buff_t *buff, uint32_t count)
{
    buff->array = malloc(sizeof(token_t) * count);
    if (buff->array == NULL) {
        return -1;
    }
    buff->count = count;
    buff->nused = 0;
    return 0;
}

void token_buff_destroy(token_buff_t *buff)
{
    assert(buff->nused == 0);
    free(buff->array);
    buff->array = NULL;
    buff->count = 0;
}

void *token_buff_get(token_buff_t *buff, uint32_t count)
{
    assert(buff->nused == 0);

    if (count > buff->count) {
        token_t *new_array;
        uint32_t new_count = buff->count;
        while (new_count < count) {
            new_count += 5000;
        }
        new_array = realloc(buff->array, sizeof(token_t) * new_count);
        if (new_array == NULL) {
            return NULL;
        }
        buff->count = new_count;
        buff->array = new_array;
    }
    buff->nused += 1;
    return (void*)buff->array;
}

void token_buff_release(token_buff_t *buff, void *tokens)
{
    assert(tokens == (void*)buff->array);
    assert(buff->nused == 1);
    buff->nused -= 1;
}

/*
 * tokernize functions
 */

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
size_t tokenize_command(char *command, int cmdlen, token_t *tokens, const size_t max_tokens)
{
    assert(command != NULL && tokens != NULL && max_tokens > 1);
    char *s, *e = NULL;
    size_t ntokens = 0;
    size_t checked = 0;

    s = command;
    while (ntokens < max_tokens - 1) {
        e = memchr(s, ' ', cmdlen - checked);
        if (e) {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
            }
            s = (++e);
            checked = s - command;
        } else {
            e = command + cmdlen;
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }
            break; /* string end */
        }
    }

    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    if (*e == '\0') {
        tokens[ntokens].value = NULL;
    } else {
        assert(ntokens == (max_tokens-1));
        tokens[ntokens].value = e;
        /* The next reserved token keeps the length of untokenized command. */
        tokens[ntokens+1].length = cmdlen - (e - command);
    }
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

int detokenize(token_t *tokens, int ntokens, char *buffer, int length)
{
    int i, nb;
    char *p;

    nb = ntokens; // account for spaces, which is ntokens-1, plus the null
    for (i = 0; i < ntokens; ++i) {
        nb += tokens[i].length;
    }
    if (nb > length) {
        return -1; /* buffer overflow */
    }

    p = buffer;
    for (i = 0; i < ntokens; ++i) {
        memcpy(p, tokens[i].value, tokens[i].length);
        p += tokens[i].length;
        *p = ' ';
        p++;
    }
    buffer[nb - 1] = '\0';
    return nb;
}

int tokenize_keys(char *keystr, int keylen, int keycnt, char delimiter, token_t *tokens)
{
    assert(keystr != NULL && keylen > 0 && keycnt > 0 && tokens != NULL);
    char *s, *e;
    int checked = 0;
    int ntokens = 0;
    bool finish = false;

    s = keystr;
    for (e = s; ntokens < keycnt; ++e, ++checked) {
        if (checked >= keylen) {
            if (s == e) break;
            tokens[ntokens].value = s;
            tokens[ntokens].length = e - s;
            ntokens++;
            if (ntokens == keycnt)
                finish = true;
            break; /* string end */
        }
        if (*e == delimiter) {
            if (s == e) break;
            tokens[ntokens].value = s;
            tokens[ntokens].length = e - s;
            ntokens++;
            s = e + 1;
        } else if (*e == ' ') {
            break; /* invalid character in key string */
        }
    }
    if (finish == true) {
        return ntokens;
    } else {
        return -1; /* some errors */
    }
}

/*
 * string memory block
 */
static int check_sblock_tail_string(mblck_list_t *sblcks, int length)
{
    mblck_node_t *blckptr;
    char         *dataptr;
    uint32_t      bodylen = MBLCK_GET_BODYLEN(sblcks);
    uint32_t      lastlen;
    uint32_t      numblks;

    /* check the last "\r\n" string */
    blckptr = MBLCK_GET_TAILBLK(sblcks);
    dataptr = MBLCK_GET_BODYPTR(blckptr);
    lastlen = (length % bodylen) > 0
            ? (length % bodylen) : bodylen;

    if (*(dataptr + lastlen - 1) != '\n') {
        return -1; /* invalid strings */
    }
    if ((--lastlen) == 0) {
        numblks = MBLCK_GET_NUMBLKS(sblcks);
        blckptr = MBLCK_GET_HEADBLK(sblcks);
        for (int i = 1; i < numblks-1; i++) {
             blckptr = MBLCK_GET_NEXTBLK(blckptr);
        }
        dataptr = MBLCK_GET_BODYPTR(blckptr);
        lastlen = bodylen;
    }
    if (*(dataptr + lastlen - 1) != '\r') {
        return -1; /* invalid strings */
    }
    return 0;
}

static int tokenize_mblck(char *keystr, int keylen, int keycnt, char delimiter, token_t *tokens)
{
    char *s, *e;
    int checked = 0;
    int ntokens = 0;
    bool finish = false;

    s = keystr;
    for (e = s; ntokens < keycnt; ++e, ++checked) {
        if (checked >= keylen) {
            if (s == e) break;
            tokens[ntokens].value = s;
            tokens[ntokens].length = e - s;
            ntokens++;
            finish = true;
            break; /* string end */
        }
        if (*e == delimiter) {
            if (s == e) break;
            tokens[ntokens].value = s;
            tokens[ntokens].length = e - s;
            ntokens++;
            s = e + 1;
        } else if (*e == ' ') {
            break; /* invalid character in key string */
        }
    }
    if (finish == true) {
        return ntokens;
    } else {
        return -1; /* some errors */
    }
}

/* segmented token structure */
typedef struct {
    char *value;
    uint32_t length;
    uint32_t tokidx;
} segtok_t;

static int build_complete_strings(mblck_list_t *blist, token_t *tokens,
                                  segtok_t *segtoks, int segcnt)
{
    assert(blist->pool != NULL);
    mblck_list_t add_blcks;
    mblck_node_t *blckptr;
    char         *dataptr;
    char         *saveptr;
    token_t      *tok_ptr;
    uint32_t      bodylen = MBLCK_GET_BODYLEN(blist);
    uint32_t      numblks = 1;
    uint32_t      datalen = 0;
    uint32_t      complen, i;

    /* calculate the # of blocks needed */
    for (i = 0; i < segcnt; i++) {
        tok_ptr = &tokens[segtoks[i].tokidx];
        complen = segtoks[i].length + tok_ptr->length;
        if ((datalen + complen) > bodylen) {
            numblks += 1;
            datalen = complen;
        } else {
            datalen += complen;
        }
    }

    /* allocate new mblock list */
    if (mblck_list_alloc((mblck_pool_t*)blist->pool, bodylen, numblks, &add_blcks) < 0) {
        return -1;
    }

    /* build the complete strings with new mblock */
    blckptr = MBLCK_GET_HEADBLK(&add_blcks);
    dataptr = MBLCK_GET_BODYPTR(blckptr);
    datalen = 0;

    for (i = 0; i < segcnt; i++) {
        tok_ptr = &tokens[segtoks[i].tokidx];
        complen = segtoks[i].length + tok_ptr->length;

        if ((datalen + complen) > bodylen) {
            blckptr = MBLCK_GET_NEXTBLK(blckptr);
            dataptr = MBLCK_GET_BODYPTR(blckptr);
            datalen = 0;
        }

        saveptr = &dataptr[datalen];

        memcpy(dataptr + datalen, segtoks[i].value, segtoks[i].length);
        datalen += segtoks[i].length;
        memcpy(dataptr + datalen, tok_ptr->value, tok_ptr->length);
        datalen += tok_ptr->length;

        tok_ptr->value = saveptr;
        tok_ptr->length = complen;
    }

    /* merge to main mblock list */
    mblck_list_merge(blist, &add_blcks);
    return 0;
}

#define SEGTOK_ARRAY_SIZE 32
/*
 * Assume key string blocks without the trailing "\r\n" characters.
 */
int tokenize_mblocks(mblck_list_t *blist, int keylen, int keycnt, char delimiter, token_t *tokens)
{
    assert(keylen > 0 && keycnt > 0 && tokens != NULL);
    mblck_node_t *blckptr;
    char         *dataptr;
    uint32_t    numblks;
    uint32_t    chkblks;
    uint32_t    bodylen = MBLCK_GET_BODYLEN(blist);
    uint32_t    datalen;
    uint32_t    lastlen;
    uint32_t    ntokens = 0;
    segtok_t    segtoks[SEGTOK_ARRAY_SIZE];
    uint32_t    nsegtok = 0;
    int         ret = 0;

    /* prepare block info */
    numblks = ((keylen - 1) / bodylen) + 1;
    lastlen = (keylen % bodylen) > 0
            ? (keylen % bodylen) : bodylen;

    /* get the first block */
    chkblks = 1;
    blckptr = MBLCK_GET_HEADBLK(blist);
    dataptr = MBLCK_GET_BODYPTR(blckptr);
    datalen = (chkblks < numblks) ? bodylen : lastlen;

    while (ntokens < keycnt) {
        /* check the last character */
        bool segmented_blck = false;
        if (chkblks < numblks) {
            if (dataptr[datalen-1] == delimiter) {
                datalen -= 1;
            } else {
                segmented_blck = true;
            }
        }

        /* tokenize string in the block */
        int tokcnt = tokenize_mblck(dataptr, datalen, keycnt-ntokens, delimiter,
                                    &tokens[ntokens]);
        if (tokcnt <= 0) {
            ret = -1; break;
        }
        ntokens += tokcnt;

        /* check the end of strings */
        if (chkblks >= numblks) {
            ret = (ntokens == keycnt) ? 0 : -1;
            break; /* string end */
        }

        /* get the next block */
        chkblks += 1;
        blckptr = MBLCK_GET_NEXTBLK(blckptr);
        dataptr = MBLCK_GET_BODYPTR(blckptr);
        datalen = (chkblks < numblks) ? bodylen : lastlen;
        if (segmented_blck == false)
            continue;

        /* The segmented block is found. */
        if (dataptr[0] == delimiter) {
            /* NOT segmented string */
            dataptr += 1;
            datalen -= 1;
        } else {
            /* real segmented string: save it */
            if (nsegtok >= SEGTOK_ARRAY_SIZE) {
                if (build_complete_strings(blist, tokens, segtoks, nsegtok) != 0) {
                    ret = -2; break; /* out of memory */
                }
                nsegtok = 0;
            }
            ntokens -= 1;
            segtoks[nsegtok].value = tokens[ntokens].value;
            segtoks[nsegtok].length = (uint32_t)tokens[ntokens].length;
            segtoks[nsegtok].tokidx = ntokens;
            nsegtok += 1;
        }
    }

    if (ret == 0 && nsegtok > 0) {
        if (build_complete_strings(blist, tokens, segtoks, nsegtok) != 0) {
            return -2; /* out of memory */
        }
    }
    return ret;
}

/*
 * Assume key string blocks in ascii protocol.
 */
int tokenize_sblocks(mblck_list_t *blist, int keylen, int keycnt, char delimiter, token_t *tokens)
{
    assert(keylen > 2);

    /* check the last "\r\n" string */
    if (check_sblock_tail_string(blist, keylen) != 0) {
        return -1; /* invalid tail string */
    }
    return tokenize_mblocks(blist, keylen-2, keycnt, delimiter, tokens);
}
