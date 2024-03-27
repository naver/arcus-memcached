#include "config.h"
#include "memcached.h"
#include "proto_ascii.h"
#ifdef ENABLE_ZK_INTEGRATION
#include "arcus_zk.h"
#include "arcus_hb.h"
#endif

#include <stdlib.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <string.h>

#ifndef PROTO_ASCII_H // Remove later.

#define ZK_CONNECTIONS 1

#define COMMAND_TOKEN 0
#define SUBCOMMAND_TOKEN 1
#define PREFIX_TOKEN 1
#define KEY_TOKEN 1
#define LOP_KEY_TOKEN 2
#define SOP_KEY_TOKEN 2
#define MOP_KEY_TOKEN 2
#define BOP_KEY_TOKEN 2

#define MAX_TOKENS 30

#ifdef ENABLE_ZK_INTEGRATION
extern char *arcus_zk_cfg;
#endif

#ifdef COMMAND_LOGGING
bool cmdlog_in_use;
#endif

#ifdef DETECT_LONG_QUERY
bool lqdetect_in_use;
#endif

extern struct settings settings;
extern struct mc_stats mc_stats;
extern EXTENSION_LOGGER_DESCRIPTOR *mc_logger;

extern union {
    ENGINE_HANDLE *v0;
    ENGINE_HANDLE_V1 *v1;
} mc_engine;

#if COMMON_PROCESS || 1
/** error handling */
static void handle_unexpected_errorcode_ascii(conn *c, const char *func_name, ENGINE_ERROR_CODE ret)
{
    if (ret == ENGINE_DISCONNECT) {
        conn_set_state(c, conn_closing);
    } else if (ret == ENGINE_ENOTSUP) {
        out_string(c, "NOT_SUPPORTED");
    } else {
        mc_logger->log(EXTENSION_LOG_WARNING, c, "[%s] Unexpected Error: %d\n",
                       func_name, (int)ret);
        out_string(c, "SERVER_ERROR internal");
    }
}

static void print_invalid_command(conn *c, token_t *tokens, const size_t ntokens)
{
    /* To understand this function's implementation,
     * You must know how the command is tokenized.
     * See tokenize_command().
     */
    if (ntokens >= 2) {
        int i;
        /* make single string */
        for (i = 0; i < (ntokens-2); i++) {
            tokens[i].value[tokens[i].length] = ' ';
        }
        mc_logger->log(EXTENSION_LOG_INFO, c, "[%s] INVALID_COMMAND: %s\n",
                       c->client_ip, tokens[0].value);
        /* restore the tokens */
        for (i = 0; i < (ntokens-2); i++) {
            tokens[i].value[tokens[i].length] = '\0';
        }
    }
}

/** collection type common/util */
static void pipe_state_clear(conn *c)
{
    c->pipe_state = PIPE_STATE_OFF;
    c->pipe_count = 0;
}

static inline void set_pipe_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    char *token_value = tokens[ntokens-2].value;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command_ascii()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "pipe" option is not reliable anyway,
      so it can't be helped.
    */
    if (token_value && strcmp(token_value, "pipe") == 0) {
        c->noreply = true;
        if (c->pipe_state == PIPE_STATE_OFF) /* first pipe */
            c->pipe_state = PIPE_STATE_ON;
    } else {
        c->noreply = false;
    }
}

static inline void set_noreply_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    char *token_value = tokens[ntokens-2].value;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command_ascii()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "noreply" option is not reliable anyway, so
      it can't be helped.
    */
    if (token_value && strcmp(token_value, "noreply") == 0) {
        c->noreply = true;
    } else {
        c->noreply = false;
    }
}

static inline void set_pipe_noreply_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    char *token_value = tokens[ntokens-2].value;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command_ascii()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "noreply" or "pipe" option is not reliable anyway,
      so it can't be helped.
    */
    if (token_value) {
        if (strcmp(token_value, "noreply") == 0) {
            c->noreply = true;
        } else if (strcmp(token_value, "pipe") == 0) {
            c->noreply = true;
            if (c->pipe_state == PIPE_STATE_OFF) /* first pipe */
                c->pipe_state = PIPE_STATE_ON;
        } else {
            c->noreply = false;
        }
    } else {
        c->noreply = false;
    }
}

static bool check_and_handle_pipe_state(conn *c)
{
    if (c->pipe_state == PIPE_STATE_OFF || c->pipe_state == PIPE_STATE_ON) {
        return true;
    } else {
        assert(c->pipe_state == PIPE_STATE_ERR_CFULL ||
               c->pipe_state == PIPE_STATE_ERR_MFULL ||
               c->pipe_state == PIPE_STATE_ERR_BAD);
        if (c->noreply) {
            c->noreply = false; /* reset noreply */
        } else  {
            /* The last command of pipelining has come. */
            pipe_state_clear(c);
        }
        return false;
    }
}

static inline int get_coll_create_attr_from_tokens(token_t *tokens, const int ntokens,
                                                   int coll_type, item_attr *attrp)
{
    assert(coll_type==ITEM_TYPE_LIST || coll_type==ITEM_TYPE_SET ||
           coll_type==ITEM_TYPE_MAP || coll_type==ITEM_TYPE_BTREE);
    int64_t exptime;

    /* create attributes: flags, exptime, maxcount, ovflaction, unreadable */
    /* support arcus 1.5 backward compatibility. */
    if (ntokens < 1 || ntokens > 5) return -1;
    //if (ntokens < 3 || ntokens > 5) return -1;

    /* flags */
    if (! safe_strtoul(tokens[0].value, &attrp->flags)) return -1;
    attrp->flags = htonl(attrp->flags);

    /* exptime */
    if (ntokens >= 2) {
        if (! safe_strtoll(tokens[1].value, &exptime)) return -1;
    } else {
        exptime = 0; /* default value */
    }
    attrp->exptime = realtime(exptime);

    /* maxcount */
    if (ntokens >= 3) {
        if (! safe_strtol(tokens[2].value, &attrp->maxcount)) return -1;
    } else {
        attrp->maxcount = 0; /* default value */
    }

    attrp->ovflaction = 0; /* undefined : will be set to default later */
    attrp->readable   = 1; /* readable = on */

    if (ntokens >= 4) {
        if (strcmp(tokens[3].value, "error") == 0) {
            attrp->ovflaction = OVFL_ERROR;
        } else {
            if (coll_type == ITEM_TYPE_LIST) {
                if (strcmp(tokens[3].value, "head_trim") == 0)
                    attrp->ovflaction = OVFL_HEAD_TRIM;
                else if (strcmp(tokens[3].value, "tail_trim") == 0)
                    attrp->ovflaction = OVFL_TAIL_TRIM;
            }
            else if (coll_type == ITEM_TYPE_BTREE) {
                if (strcmp(tokens[3].value, "smallest_trim") == 0)
                    attrp->ovflaction = OVFL_SMALLEST_TRIM;
                else if (strcmp(tokens[3].value, "largest_trim") == 0)
                    attrp->ovflaction = OVFL_LARGEST_TRIM;
                else if (strcmp(tokens[3].value, "smallest_silent_trim") == 0)
                    attrp->ovflaction = OVFL_SMALLEST_SILENT_TRIM;
                else if (strcmp(tokens[3].value, "largest_silent_trim") == 0)
                    attrp->ovflaction = OVFL_LARGEST_SILENT_TRIM;
            }
        }
        if (attrp->ovflaction != 0) { /* defined */
            if (ntokens == 5) {
                if (strcmp(tokens[4].value, "unreadable") != 0) return -1;
                attrp->readable = 0;
            }
        } else { /* undefined */
            if (ntokens == 5) return -1; /* ovflaction must be defined */
            else { /* ntokens == 4 */
                if (strcmp(tokens[3].value, "unreadable") != 0) return -1;
                attrp->readable = 0;
            }
        }
    }
    return 0;
}

static bool ascii_response_handler(const void *cookie, int nbytes, const char *dta)
{
    conn *c = (conn*)cookie;
    if (!grow_dynamic_buffer(c, nbytes)) {
        if (settings.verbose > 0) {
            mc_logger->log(EXTENSION_LOG_INFO, c,
                    "<%d ERROR: Failed to allocate memory for response\n", c->sfd);
        }
        return false;
    }

    char *buf = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    memcpy(buf, dta, nbytes);
    c->dynamic_buffer.offset += nbytes;
    return true;
}

#endif

#if KEY_VALUE_PROCESS || 1
/** set */
static void process_update_command(conn *c, token_t *tokens, const size_t ntokens,
                                   ENGINE_STORE_OPERATION store_op, bool handle_cas)
{
    assert(c != NULL);
    char *key;
    size_t nkey;
    unsigned int flags;
    int64_t exptime=0;
    int vlen;
    uint64_t req_cas_id=0;
    item *it;

    set_noreply_maybe(c, tokens, ntokens);

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;
    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if ((! safe_strtoul(tokens[2].value, (uint32_t *)&flags)) ||
        (! safe_strtoll(tokens[3].value, &exptime)) ||
        (! safe_strtol(tokens[4].value, (int32_t *)&vlen)) ||
        (vlen < 0 || vlen > (INT_MAX-2)))
    {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    vlen += 2;

    // does cas value exist?
    if (handle_cas) {
        if (!safe_strtoull(tokens[5].value, &req_cas_id)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->allocate(mc_engine.v0, c, &it, key, nkey, vlen,
                                 htonl(flags), realtime(exptime), req_cas_id);
    if (ret == ENGINE_SUCCESS) {
        if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &c->hinfo)) {
            mc_engine.v1->release(mc_engine.v0, c, it);
            out_string(c, "SERVER_ERROR error getting item data");
            ret = ENGINE_ENOMEM;
        } else {
            c->item = it;
            ritem_set_first(c, CONN_RTYPE_HINFO, vlen);
            c->store_op = store_op;
            conn_set_state(c, conn_nread);
        }
    } else {
        if (ret == ENGINE_E2BIG) {
            out_string(c, "CLIENT_ERROR object too large for cache");
        } else if (ret == ENGINE_ENOMEM) {
            out_string(c, "SERVER_ERROR out of memory storing object");
        } else {
            handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    if (ret != ENGINE_SUCCESS) {
        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too?
         */
        if (store_op == OPERATION_SET) {
            /* set temporarily noreply for the ASYNC interface */
            /* noreply flag is cleared in out_string() if it was set */
            assert(c->noreply == false);
            c->noreply = true;
            mc_engine.v1->remove(mc_engine.v0, c, key, nkey, 0, 0);
            c->noreply = false;
        }

        if (ret != ENGINE_DISCONNECT) {
            /* swallow the data line */
            c->sbytes = vlen;
            if (c->state == conn_write) {
                c->write_and_go = conn_swallow;
            } else { /* conn_new_cmd (by noreply) */
                conn_set_state(c, conn_swallow);
            }
        }
    }
}

/** get */
/**
 * Get a suffix buffer and insert it into the list of used suffix buffers
 * @param c the connection object
 * @return a pointer to a new suffix buffer or NULL if allocation failed
 */
static char *get_suffix_buffer(conn *c)
{
    if (c->suffixleft == c->suffixsize) {
        char **new_suffix_list;
        size_t sz = sizeof(char*) * c->suffixsize * 2;

        new_suffix_list = realloc(c->suffixlist, sz);
        if (new_suffix_list) {
            c->suffixsize *= 2;
            c->suffixlist = new_suffix_list;
        } else {
            if (settings.verbose > 1) {
                mc_logger->log(EXTENSION_LOG_DEBUG, c,
                        "=%d Failed to resize suffix buffer\n", c->sfd);
            }
            return NULL;
        }
    }

    char *suffix = cache_alloc(c->thread->suffix_cache);
    if (suffix != NULL) {
        *(c->suffixlist + c->suffixleft) = suffix;
        ++c->suffixleft;
    }
    return suffix;
}

static ENGINE_ERROR_CODE
process_get_single(conn *c, char *key, size_t nkey, bool return_cas)
{
    item *it;
    char *cas_val = NULL;
    int   cas_len = 0;
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->get(mc_engine.v0, c, &it, key, nkey, 0);
    if (ret != ENGINE_SUCCESS) {
        it = NULL;
    }
    if (settings.detail_enabled) {
        stats_prefix_record_get(key, nkey, (it != NULL));
    }

    if (it) {
        if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &c->hinfo)) {
            mc_engine.v1->release(mc_engine.v0, c, it);
            return ENGINE_ENOMEM;
        }
        assert(hinfo_check_ascii_tail_string(&c->hinfo) == 0); /* check "\r\n" */

        /* Rebuild the suffix */
        char *suffix = get_suffix_buffer(c);
        if (suffix == NULL) {
            mc_engine.v1->release(mc_engine.v0, c, it);
            return ENGINE_ENOMEM;
        }
        int suffix_len = snprintf(suffix, SUFFIX_SIZE, " %u %u\r\n",
                                  htonl(c->hinfo.flags), c->hinfo.nbytes - 2);
        /* suffix_len < SUFFIX_SIZE because the length of nbytes string is smaller than 10. */

        /* rebuild cas value */
        if (return_cas) {
            suffix_len -= 2; /* remove "\r\n" from suffix string */
            cas_val = get_suffix_buffer(c);
            if (cas_val == NULL) {
                mc_engine.v1->release(mc_engine.v0, c, it);
                return ENGINE_ENOMEM;
            }
            cas_len = snprintf(cas_val, SUFFIX_SIZE, " %"PRIu64"\r\n", c->hinfo.cas);
        }

        /* Construct the response. Each hit adds three elements to the
         * outgoing data list:
         *   VALUE <key> <falgs> <bytes>[ <cas>]\r\n" + "<data>\r\n"
         */
        if (add_iov(c, "VALUE ", 6) != 0 ||
            add_iov(c, c->hinfo.key, c->hinfo.nkey) != 0 ||
            add_iov(c, suffix, suffix_len) != 0 ||
            (return_cas && add_iov(c, cas_val, cas_len) != 0) ||
            add_iov_hinfo_value(c, &c->hinfo) != 0)
        {
            mc_engine.v1->release(mc_engine.v0, c, it);
            return ENGINE_ENOMEM;
        }

        /* save the item */
        if (c->ileft >= c->isize) {
            item **new_list = realloc(c->ilist, sizeof(item *) * c->isize * 2);
            if (new_list) {
                c->isize *= 2;
                c->ilist = new_list;
            } else {
                mc_engine.v1->release(mc_engine.v0, c, it);
                return ENGINE_ENOMEM;/* out of memory */
            }
        }
        *(c->ilist + c->ileft) = it;
        c->ileft++;

        if (settings.verbose > 1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c, ">%d sending key %s\n",
                           c->sfd, key);
        }
        /* item_get() has incremented it->refcount for us */
        STATS_HITS(c, get, key, nkey);
        MEMCACHED_COMMAND_GET(c->sfd, key, nkey, c->hinfo.nbytes, c->hinfo.cas);
    } else {
        STATS_MISSES(c, get, key, nkey);
        MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
    }
    /* Even if the key is not found, return ENGINE_SUCCESS.  */
    return ENGINE_SUCCESS;
}

/* ntokens is overwritten here... shrug.. */
static inline void process_get_command(conn *c, token_t *tokens, size_t ntokens, bool return_cas)
{
    assert(c != NULL);
    token_t *key_token = &tokens[KEY_TOKEN];
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        while (key_token->length != 0) {
            if (key_token->length > KEY_MAX_LENGTH) {
                ret = ENGINE_EINVAL; break;
            }
            /* do get operation for each key */
            ret = process_get_single(c, key_token->value, key_token->length,
                                     return_cas);
            if (ret != ENGINE_SUCCESS) {
                break; /* ret == ENGINE_ENOMEM */
            }
            key_token++;
        }
        if (ret != ENGINE_SUCCESS) break;

        /* If the command string hasn't been fully processed, get the next set of tokens. */
        if (key_token->value != NULL) {
            /* The next reserved token has the length of untokenized command. */
            ntokens = tokenize_command(key_token->value, (key_token+1)->length,
                                       tokens, MAX_TOKENS);
            key_token = tokens;
        }
    } while(key_token->value != NULL);

    /* Some items and suffixes might have saved in the above execution.
     * To release the items and free the suffixes, the below code is needed.
     */
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;

    if (ret != ENGINE_SUCCESS) {
        /* Releasing items on ilist and freeing suffixes will be
         * performed later by calling out_string() function.
         * See conn_write() and conn_mwrite() state.
         */
        if (ret == ENGINE_EINVAL)
            out_string(c, "CLIENT_ERROR bad command line format");
        else /* ret == ENGINE_ENOMEM */
            out_string(c, "SERVER_ERROR out of memory writing get response");
        return;
    }

    if (settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c, ">%d END\n", c->sfd);
    }

    /* If the loop was terminated because of out-of-memory, it is not
     * reliable to add END\r\n to the buffer, because it might not end
     * in \r\n. So we send SERVER_ERROR instead.
     */
    if ((add_iov(c, "END\r\n", 5) != 0) ||
        (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
        out_string(c, "SERVER_ERROR out of memory writing get response");
    } else {
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
    }
}

/** mget */
static void process_mget_complete(conn *c, bool return_cas)
{
    assert(return_cas ? (c->coll_op == OPERATION_MGETS) : (c->coll_op == OPERATION_MGET));
    assert(c->coll_strkeys != NULL);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    token_t *key_tokens;

    do {
        key_tokens = (token_t*)token_buff_get(&c->thread->token_buff, c->coll_numkeys);
        if (key_tokens != NULL) {
            bool must_backward_compatible = false;
            ret = tokenize_sblocks(&c->memblist, c->coll_lenkeys, c->coll_numkeys,
                                   KEY_MAX_LENGTH, must_backward_compatible, key_tokens);
            if (ret != ENGINE_SUCCESS) {
                break; /* ENGINE_EBADVALUE | ENGINE_ENOMEM */
            }
        } else {
            ret = ENGINE_ENOMEM; break;
        }

        /* do get operation for each key */
        for (int k = 0; k < c->coll_numkeys; k++) {
            ret = process_get_single(c, key_tokens[k].value, key_tokens[k].length,
                                     return_cas);
            if (ret != ENGINE_SUCCESS) {
                break; /* ret == ENGINE_ENOMEM*/
            }
        }

        /* Some items and suffixes might have saved in the above execution.
         * To release the items and free the suffixes, the below code is needed.
         */
        c->icurr = c->ilist;
        c->suffixcurr = c->suffixlist;

        if (ret != ENGINE_SUCCESS) {
            /* Releasing items on ilist and freeing suffixes will be
             * performed later by calling out_string() function.
             * See conn_write() and conn_mwrite() state.
             */
            break;
        }

        if (settings.verbose > 1) {
            mc_logger->log(EXTENSION_LOG_DEBUG, c, ">%d END\n", c->sfd);
        }

        /* If the loop was terminated because of out-of-memory, it is not
         * reliable to add END\r\n to the buffer, because it might not end
         * in \r\n. So we send SERVER_ERROR instead.
         */
        if ((add_iov(c, "END\r\n", 5) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            /* Releasing items on ilist and freeing suffixes will be
             * performed later by calling out_string() function.
             * See conn_write() and conn_mwrite() state.
             */
            ret = ENGINE_ENOMEM;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
    } else {
        if (ret == ENGINE_EBADVALUE)   out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory writing get response");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }

    /* free token buffer */
    if (key_tokens != NULL) {
        token_buff_release(&c->thread->token_buff, key_tokens);
    }
    if (ret != ENGINE_SUCCESS) {
        /* free key string memory blocks */
        assert(c->coll_strkeys == (void*)&c->memblist);
        mblck_list_free(&c->thread->mblck_pool, &c->memblist);
        c->coll_strkeys = NULL;
    }
}

static void process_prepare_nread_keys(conn *c, uint32_t vlen, uint32_t kcnt, bool return_cas)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    /* allocate memory blocks needed */
    if (mblck_list_alloc(&c->thread->mblck_pool, 1, vlen, &c->memblist) < 0) {
        ret = ENGINE_ENOMEM;
    }
    if (ret == ENGINE_SUCCESS) {
        c->coll_strkeys = (void*)&c->memblist;
        ritem_set_first(c, CONN_RTYPE_MBLCK, vlen);
        c->coll_op = (return_cas ? OPERATION_MGETS : OPERATION_MGET);
        conn_set_state(c, conn_nread);
    } else {
        out_string(c, "SERVER_ERROR out of memory");
        c->sbytes = vlen;
        c->write_and_go = conn_swallow;
    }
}

static inline void process_mget_command(conn *c, token_t *tokens, const size_t ntokens, bool return_cas)
{
    uint32_t lenkeys, numkeys;

    if ((! safe_strtoul(tokens[COMMAND_TOKEN+1].value, &lenkeys)) ||
        (! safe_strtoul(tokens[COMMAND_TOKEN+2].value, &numkeys)) ||
        (lenkeys > (UINT_MAX-2)) || (lenkeys == 0) || (numkeys == 0)) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (numkeys > MAX_MGET_KEY_COUNT ||
        numkeys > ((lenkeys/2) + 1) ||
        lenkeys > ((numkeys*KEY_MAX_LENGTH) + numkeys-1)) {
        /* ENGINE_EBADVALUE */
        out_string(c, "CLIENT_ERROR bad value");
        c->sbytes = lenkeys + 2;
        c->write_and_go = conn_swallow;
        return;
    }
    lenkeys += 2;

    c->coll_numkeys = numkeys;
    c->coll_lenkeys = lenkeys;

    process_prepare_nread_keys(c, lenkeys, numkeys, return_cas);
}

/** delete */
static void process_delete_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c->ewouldblock == false);
    char *key;
    size_t nkey;

    if (ntokens > 3) {
        /* See "delete <key> [<time>] [noreply]\r\n" */
        bool zero_time = strcmp(tokens[KEY_TOKEN+1].value, "0") == 0;
        set_noreply_maybe(c, tokens, ntokens);

        if ((ntokens >= 6) ||
            (ntokens == 5 && (!zero_time || !c->noreply)) ||
            (ntokens == 4 && (!zero_time && !c->noreply)))
        {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format.  "
                          "Usage: delete <key> [noreply]");
            return;
        }
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;
    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->remove(mc_engine.v0, c, key, nkey, 0, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    /* For some reason the SLAB_INCR tries to access this... */
    if (ret == ENGINE_SUCCESS) {
        STATS_HITS(c, delete, key, nkey);
        out_string(c, "DELETED");
    } else if (ret == ENGINE_KEY_ENOENT) {
        STATS_MISSES(c, delete, key, nkey);
        out_string(c, "NOT_FOUND");
    } else {
        STATS_CMD_NOKEY(c, delete);
        handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** incr/decr */
static void process_arithmetic_command(conn *c, token_t *tokens, const size_t ntokens,
                                       const bool incr)
{
    assert(c != NULL);
    assert(c->ewouldblock == false);
    uint64_t delta;
    char *key;
    size_t nkey;

    set_noreply_maybe(c, tokens, ntokens);

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;
    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    if (!safe_strtoull(tokens[2].value, &delta)) {
        out_string(c, "CLIENT_ERROR invalid numeric delta argument");
        return;
    }

    bool create = false;
    unsigned int flags  = 0;
    int64_t exptime = 0;
    uint64_t init_value = 0;

    if (ntokens >= 7) {
        if (! (safe_strtoul(tokens[3].value, (uint32_t *)&flags)
               && safe_strtoll(tokens[4].value, &exptime)
               && safe_strtoull(tokens[5].value, &init_value))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        create = true;
    }

    if (settings.detail_enabled) {
        if (incr) stats_prefix_record_incr(key, nkey);
        else      stats_prefix_record_decr(key, nkey);
    }

    ENGINE_ERROR_CODE ret;
    uint64_t cas;
    uint64_t result;

    ret = mc_engine.v1->arithmetic(mc_engine.v0, c, key, nkey,
                                   incr, create, delta, init_value,
                                   htonl(flags), realtime(exptime),
                                   &cas, &result, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);

    char temp[INCR_MAX_STORAGE_LEN];
    switch (ret) {
    case ENGINE_SUCCESS:
        if (incr) {
            STATS_HITS(c, incr, key, nkey);
        } else {
            STATS_HITS(c, decr, key, nkey);
        }
        snprintf(temp, sizeof(temp), "%"PRIu64, result);
        out_string(c, temp);
        break;
    case ENGINE_KEY_ENOENT:
        if (incr) {
            STATS_MISSES(c, incr, key, nkey);
        } else {
            STATS_MISSES(c, decr, key, nkey);
        }
        out_string(c, "NOT_FOUND");
        break;
    default:
        if (incr) {
            STATS_CMD_NOKEY(c, incr);
        } else {
            STATS_CMD_NOKEY(c, decr);
        }
        if (ret == ENGINE_EINVAL)
            out_string(c, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        else if (ret == ENGINE_PREFIX_ENAME)
            out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM)
            out_string(c, "SERVER_ERROR out of memory");
        else if (ret == ENGINE_NOT_STORED)
            out_string(c, "SERVER_ERROR failed to store item");
        else if (ret == ENGINE_EBADTYPE)
            out_string(c, "TYPE_MISMATCH");
        else
            handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

#endif

#if LIST_PROCESS || 1
/** lop create */
static void process_lop_create(conn *c, char *key, size_t nkey, item_attr *attrp)
{
    assert(c->ewouldblock == false);
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->list_struct_create(mc_engine.v0, c, key, nkey, attrp, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        stats_prefix_record_lop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, lop_create, key, nkey);
        out_string(c, "CREATED");
        break;
    default:
        STATS_CMD_NOKEY(c, lop_create);
        if (ret == ENGINE_KEY_EEXISTS)       out_string(c, "EXISTS");
        else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** lop insert */
static void process_lop_insert_complete(conn *c)
{
    assert(c->coll_op == OPERATION_LOP_INSERT);
    assert(c->coll_eitem != NULL);
    ENGINE_ERROR_CODE ret;

    mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_LIST, c->coll_eitem, &c->einfo);

    if (einfo_check_ascii_tail_string(&c->einfo) != 0) { /* check "\r\n" */
        ret = ENGINE_EINVAL;
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool created;
        ret = mc_engine.v1->list_elem_insert(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                             c->coll_index, c->coll_eitem,
                                             c->coll_attrp, &created, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            stats_prefix_record_lop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }
#ifdef DETECT_LONG_QUERY
        if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
            if (! lqdetect_lop_insert(c->client_ip, c->coll_key, c->coll_index)) {
                lqdetect_in_use = false;
            }
        }
#endif

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, lop_insert, c->coll_key, c->coll_nkey);
            if (created) out_string(c, "CREATED_STORED");
            else         out_string(c, "STORED");
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISSES(c, lop_insert, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_CMD_NOKEY(c, lop_insert);
            if (ret == ENGINE_EBADTYPE)          out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_EOVERFLOW)    out_string(c, "OVERFLOWED");
            else if (ret == ENGINE_EINDEXOOR)    out_string(c, "OUT_OF_RANGE");
            else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
            else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    if (ret != ENGINE_SUCCESS) {
        mc_engine.v1->list_elem_free(mc_engine.v0, c, c->coll_eitem);
    }
    c->coll_eitem = NULL;
}

/** lop delete */
static void process_lop_delete(conn *c, char *key, size_t nkey,
                               int32_t from_index, int32_t to_index, bool drop_if_empty)
{
    assert(c->ewouldblock == false);
    uint32_t del_count;
    bool     dropped;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->list_elem_delete(mc_engine.v0, c, key, nkey,
                                         from_index, to_index, drop_if_empty,
                                         &del_count, &dropped, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_lop_delete(key, nkey, is_hit);
    }
#ifdef DETECT_LONG_QUERY
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (! lqdetect_lop_delete(c->client_ip, key, del_count,
                                  from_index, to_index, drop_if_empty)) {
            lqdetect_in_use = false;
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, lop_delete, key, nkey);
        if (dropped) out_string(c, "DELETED_DROPPED");
        else         out_string(c, "DELETED");
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, lop_delete, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISSES(c, lop_delete, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_CMD_NOKEY(c, lop_delete);
        if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** lop get */
static ENGINE_ERROR_CODE
out_lop_get_response(conn *c, bool delete, struct elems_result *eresultp)
{
    eitem  **elem_array = eresultp->elem_array;
    uint32_t elem_count = eresultp->elem_count;
    int      bufsize;
    char    *respbuf; /* response string buffer */
    char    *respptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* allocate response string buffer */
        bufsize = ((2*UINT32_STR_LENG) + 30) /* response head and tail size */
                  + (elem_count * (UINT32_STR_LENG+2)); /* response body size */
        respbuf = (char*)malloc(bufsize);
        if (respbuf == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr = respbuf;

        /* make response head */
        sprintf(respptr, "VALUE %u %u\r\n", htonl(eresultp->flags), elem_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        /* make response body */
        for (int i = 0; i < elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_LIST,
                                        elem_array[i], &c->einfo);
            sprintf(respptr, "%u ", c->einfo.nbytes-2);
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);
        }
        if (ret == ENGINE_ENOMEM) break;

        /* make response tail */
        if (delete) {
            sprintf(respptr, "%s\r\n", (eresultp->dropped ? "DELETED_DROPPED" : "DELETED"));
        } else {
            sprintf(respptr, "END\r\n");
        }
        if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        c->coll_eitem  = (void *)elem_array;
        c->coll_ecount = elem_count;
        c->coll_resps  = respbuf;
        c->coll_op     = OPERATION_LOP_GET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else { /* ENGINE_ENOMEM */
        assert(elem_array != NULL);
        mc_engine.v1->list_elem_release(mc_engine.v0, c, elem_array, elem_count);
        if (elem_array)
            free(elem_array);
        if (respbuf)
            free(respbuf);
    }
    return ret;
}

static void process_lop_get(conn *c, char *key, size_t nkey,
                            int32_t from_index, int32_t to_index,
                            bool delete, bool drop_if_empty)
{
    assert(c->ewouldblock == false);
    struct elems_result eresult;
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->list_elem_get(mc_engine.v0, c, key, nkey,
                                      from_index, to_index, delete, drop_if_empty,
                                      &eresult, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_lop_get(key, nkey, is_hit);
    }
#ifdef DETECT_LONG_QUERY
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (! lqdetect_lop_get(c->client_ip, key, eresult.elem_count,
                               from_index, to_index, delete, drop_if_empty)) {
            lqdetect_in_use = false;
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        ret = out_lop_get_response(c, delete, &eresult);
        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, lop_get, key, nkey);
        } else {
            STATS_CMD_NOKEY(c, lop_get);
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, lop_get, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, lop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, lop_get);
        if (ret == ENGINE_EBADTYPE)    out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** list dispatcher */
static inline int get_list_range_from_str(char *str, int32_t *from_index, int32_t *to_index)
{
    char *delimiter = strstr(str, "..");
    if (delimiter != NULL) { /* range */
        *delimiter = '\0';
        if (! (safe_strtol(str, from_index) &&
               safe_strtol(delimiter + 2, to_index))) {
            *delimiter = '.';
            return -1;
        }
        *delimiter = '.';
    } else { /* single index */
        if (! (safe_strtol(str, from_index)))
            return -1;
        *to_index = *from_index;
    }
    return 0;
}

static void process_lop_prepare_nread(conn *c, int cmd, size_t vlen,
                                      char *key, size_t nkey, int32_t index)
{
    eitem *elem;
    ENGINE_ERROR_CODE ret;

    if (vlen > settings.max_element_bytes) {
        ret = ENGINE_E2BIG;
    } else {
        ret = mc_engine.v1->list_elem_alloc(mc_engine.v0, c, key, nkey, vlen, &elem);
    }
    if (ret == ENGINE_SUCCESS) {
        mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_LIST, elem, &c->einfo);
        ritem_set_first(c, CONN_RTYPE_EINFO, vlen);
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = OPERATION_LOP_INSERT;
        c->coll_index  = index;
        conn_set_state(c, conn_nread);
    } else {
        if (settings.detail_enabled) {
            stats_prefix_record_lop_insert(key, nkey, false);
        }
        STATS_CMD_NOKEY(c, lop_insert);
        if (ret == ENGINE_E2BIG)       out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);

        if (ret != ENGINE_DISCONNECT) {
            /* swallow the data line */
            c->sbytes = vlen;
            if (c->state == conn_write) {
                c->write_and_go = conn_swallow;
            } else { /* conn_new_cmd (by noreply) */
                conn_set_state(c, conn_swallow);
            }
        }
    }
}

static void process_lop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *key = tokens[LOP_KEY_TOKEN].value;
    size_t nkey = tokens[LOP_KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    c->coll_key = key;
    c->coll_nkey = nkey;

    if ((ntokens >= 6 && ntokens <= 13) && (strcmp(subcommand,"insert") == 0))
    {
        int32_t index, vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if ((! safe_strtol(tokens[LOP_KEY_TOKEN+1].value, &index)) ||
            (! safe_strtol(tokens[LOP_KEY_TOKEN+2].value, &vlen)) ||
            (vlen < 0 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        int read_ntokens = LOP_KEY_TOKEN + 3;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 2) {
            if (strcmp(tokens[read_ntokens].value, "create") != 0 ||
                get_coll_create_attr_from_tokens(&tokens[read_ntokens+1], rest_ntokens-1,
                                                 ITEM_TYPE_LIST, &c->coll_attr_space) != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
        } else {
            if (rest_ntokens != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = NULL;
        }

        if (check_and_handle_pipe_state(c)) {
            process_lop_prepare_nread(c, (int)OPERATION_LOP_INSERT, vlen, key, nkey, index);
        } else { /* pipe error */
            c->sbytes = vlen;
            conn_set_state(c, conn_swallow);
        }
    }
    else if ((ntokens >= 7 && ntokens <= 10) && (strcmp(subcommand, "create") == 0))
    {
        set_noreply_maybe(c, tokens, ntokens);

        int read_ntokens = LOP_KEY_TOKEN+1;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (get_coll_create_attr_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                             ITEM_TYPE_LIST, &c->coll_attr_space) != 0) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        c->coll_attrp = &c->coll_attr_space;
        process_lop_create(c, key, nkey, c->coll_attrp);
    }
    else if ((ntokens >= 5 && ntokens <= 7) && (strcmp(subcommand, "delete") == 0))
    {
        int32_t from_index, to_index;
        bool drop_if_empty = false;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if (get_list_range_from_str(tokens[LOP_KEY_TOKEN+1].value, &from_index, &to_index)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (ntokens >= 6) {
            if (ntokens == 7 || c->noreply == false) {
                drop_if_empty = (strcmp(tokens[LOP_KEY_TOKEN+2].value, "drop") == 0);
            }
            if ((ntokens == 6 && (c->noreply == false && drop_if_empty == false)) ||
                (ntokens == 7 && (c->noreply == false || drop_if_empty == false))) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        if (check_and_handle_pipe_state(c)) {
            process_lop_delete(c, key, nkey, from_index, to_index, drop_if_empty);
        } else { /* pipe error */
            conn_set_state(c, conn_new_cmd);
        }
    }
    else if ((ntokens==5 || ntokens==6) && (strcmp(subcommand, "get") == 0))
    {
        int32_t from_index, to_index;
        bool delete = false;
        bool drop_if_empty = false;

        if (get_list_range_from_str(tokens[LOP_KEY_TOKEN+1].value, &from_index, &to_index)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (ntokens == 6) {
            if (strcmp(tokens[LOP_KEY_TOKEN+2].value, "delete")==0) {
                delete = true;
            } else if (strcmp(tokens[LOP_KEY_TOKEN+2].value, "drop")==0) {
                delete = true;
                drop_if_empty = true;
            } else {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        process_lop_get(c, key, nkey, from_index, to_index, delete, drop_if_empty);
    }
    else
    {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#endif

#if SET_PROCESS || 1
/** sop create */
static void process_sop_create(conn *c, char *key, size_t nkey, item_attr *attrp)
{
    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->set_struct_create(mc_engine.v0, c, key, nkey, attrp, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        stats_prefix_record_sop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, sop_create, key, nkey);
        out_string(c, "CREATED");
        break;
    default:
        STATS_CMD_NOKEY(c, sop_create);
        if (ret == ENGINE_KEY_EEXISTS)       out_string(c, "EXISTS");
        else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** sop insert */
static void process_sop_insert_complete(conn *c)
{
    assert(c->coll_op == OPERATION_SOP_INSERT);
    assert(c->coll_eitem != NULL);
    ENGINE_ERROR_CODE ret;

    mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_SET, c->coll_eitem, &c->einfo);

    if (einfo_check_ascii_tail_string(&c->einfo) != 0) { /* check "\r\n" */
        ret = ENGINE_EINVAL;
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool created;
        ret = mc_engine.v1->set_elem_insert(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                            c->coll_eitem, c->coll_attrp, &created, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            stats_prefix_record_sop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, sop_insert, c->coll_key, c->coll_nkey);
            if (created) out_string(c, "CREATED_STORED");
            else         out_string(c, "STORED");
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISSES(c, sop_insert, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_CMD_NOKEY(c, sop_insert);
            if (ret == ENGINE_EBADTYPE)          out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_EOVERFLOW)    out_string(c, "OVERFLOWED");
            else if (ret == ENGINE_ELEM_EEXISTS) out_string(c, "ELEMENT_EXISTS");
            else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
            else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    if (ret != ENGINE_SUCCESS) {
        mc_engine.v1->set_elem_free(mc_engine.v0, c, c->coll_eitem);
    }
    c->coll_eitem = NULL;
}

/** sop delete */
static void process_sop_delete_complete(conn *c)
{
    assert(c->coll_op == OPERATION_SOP_DELETE);
    assert(c->coll_eitem != NULL);
    value_item *value = (value_item *)c->coll_eitem;

    if (strncmp(&value->ptr[value->len-2], "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool dropped;
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->set_elem_delete(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                            value->ptr, value->len, c->coll_drop,
                                            &dropped, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
            stats_prefix_record_sop_delete(c->coll_key, c->coll_nkey, is_hit);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_ELEM_HITS(c, sop_delete, c->coll_key, c->coll_nkey);
            if (dropped) out_string(c, "DELETED_DROPPED");
            else         out_string(c, "DELETED");
            break;
        case ENGINE_ELEM_ENOENT:
            STATS_NONE_HITS(c, sop_delete, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND_ELEMENT");
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISSES(c, sop_delete, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_CMD_NOKEY(c, sop_delete);
            if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    free(c->coll_eitem);
    c->coll_eitem = NULL;
}

/** sop get */
static ENGINE_ERROR_CODE
out_sop_get_response(conn *c, bool delete, struct elems_result *eresultp)
{
    eitem  **elem_array = eresultp->elem_array;
    uint32_t elem_count = eresultp->elem_count;
    int      bufsize;
    char    *respbuf; /* response string buffer */
    char    *respptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* allocate response string buffer */
        bufsize = ((2*UINT32_STR_LENG) + 30) /* response head and tail size */
                + (elem_count * (UINT32_STR_LENG+2)); /* response body size */
        respbuf = (char*)malloc(bufsize);
        if (respbuf == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr = respbuf;

        /* make response head */
        sprintf(respptr, "VALUE %u %u\r\n", htonl(eresultp->flags), elem_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        /* make response body */
        for (int i = 0; i < elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_SET,
                                        elem_array[i], &c->einfo);
            sprintf(respptr, "%u ", c->einfo.nbytes-2);
            if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += strlen(respptr);
        }
        if (ret == ENGINE_ENOMEM) break;

        /* make response tail */
        if (delete) {
            sprintf(respptr, "%s\r\n", (eresultp->dropped ? "DELETED_DROPPED" : "DELETED"));
        } else {
            sprintf(respptr, "END\r\n");
        }
        if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        c->coll_eitem  = (void *)elem_array;
        c->coll_ecount = elem_count;
        c->coll_resps  = respbuf;
        c->coll_op     = OPERATION_SOP_GET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else { /* ENGINE_ENOMEM */
        mc_engine.v1->set_elem_release(mc_engine.v0, c, elem_array, elem_count);
        if (elem_array)
            free(elem_array);
        if (respbuf)
            free(respbuf);
    }
    return ret;
}

static void process_sop_get(conn *c, char *key, size_t nkey, uint32_t count,
                            bool delete, bool drop_if_empty)
{
    assert(c->ewouldblock == false);
    struct elems_result eresult;
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->set_elem_get(mc_engine.v0, c, key, nkey,
                                     count, delete, drop_if_empty,
                                     &eresult, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_sop_get(key, nkey, is_hit);
    }
#ifdef DETECT_LONG_QUERY
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (! lqdetect_sop_get(c->client_ip, key, eresult.elem_count,
                               count, delete, drop_if_empty)) {
            lqdetect_in_use = false;
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        ret = out_sop_get_response(c, delete, &eresult);
        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, sop_get, key, nkey);
        } else {
            STATS_CMD_NOKEY(c, sop_get);
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, sop_get, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, sop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, sop_get);
        if (ret == ENGINE_EBADTYPE)    out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** sop exist */
static void process_sop_exist_complete(conn *c)
{
    assert(c->coll_op == OPERATION_SOP_EXIST);
    assert(c->coll_eitem != NULL);
    value_item *value = (value_item *)c->coll_eitem;

    if (strncmp(&value->ptr[value->len-2], "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool exist;
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->set_elem_exist(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                           value->ptr, value->len, &exist, 0);
        if (settings.detail_enabled) {
            stats_prefix_record_sop_exist(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, sop_exist, c->coll_key, c->coll_nkey);
            if (exist) out_string(c, "EXIST");
            else       out_string(c, "NOT_EXIST");
            break;
        case ENGINE_KEY_ENOENT:
        case ENGINE_UNREADABLE:
            STATS_MISSES(c, sop_exist, c->coll_key, c->coll_nkey);
            if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
            else                          out_string(c, "UNREADABLE");
            break;
        default:
            STATS_CMD_NOKEY(c, sop_exist);
            if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    free(c->coll_eitem);
    c->coll_eitem = NULL;
}

/** set dispatcher */
static void process_sop_prepare_nread(conn *c, int cmd, size_t vlen, char *key, size_t nkey)
{
    eitem *elem = NULL;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (vlen > settings.max_element_bytes) {
        ret = ENGINE_E2BIG;
    } else {
        if (cmd == (int)OPERATION_SOP_INSERT) {
            ret = mc_engine.v1->set_elem_alloc(mc_engine.v0, c, key, nkey, vlen, &elem);
        } else { /* OPERATION_SOP_DELETE or OPERATION_SOP_EXIST */
            if ((elem = (eitem *)malloc(sizeof(value_item) + vlen)) == NULL)
                ret = ENGINE_ENOMEM;
            else
                ((value_item*)elem)->len = vlen;
        }
    }
    if (ret == ENGINE_SUCCESS) {
        if (cmd == (int)OPERATION_SOP_INSERT) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_SET, elem, &c->einfo);
            ritem_set_first(c, CONN_RTYPE_EINFO, vlen);
        } else {
            c->ritem = ((value_item *)elem)->ptr;
            c->rlbytes = vlen;
            c->rltotal = 0;
        }
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = cmd;
        conn_set_state(c, conn_nread);
    } else {
        if (cmd == (int)OPERATION_SOP_INSERT) {
            if (settings.detail_enabled)
                stats_prefix_record_sop_insert(key, nkey, false);
            STATS_CMD_NOKEY(c, sop_insert);
        } else if (cmd == (int)OPERATION_SOP_DELETE) {
            if (settings.detail_enabled)
                stats_prefix_record_sop_delete(key, nkey, false);
            STATS_CMD_NOKEY(c, sop_delete);
        } else {
            if (settings.detail_enabled)
                stats_prefix_record_sop_exist(key, nkey, false);
            STATS_CMD_NOKEY(c, sop_exist);
        }
        if (ret == ENGINE_E2BIG)       out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);

        if (ret != ENGINE_DISCONNECT) {
            /* swallow the data line */
            c->sbytes = vlen;
            if (c->state == conn_write) {
                c->write_and_go = conn_swallow;
            } else { /* conn_new_cmd (by noreply) */
                conn_set_state(c, conn_swallow);
            }
        }
    }
}

static void process_sop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *key = tokens[SOP_KEY_TOKEN].value;
    size_t nkey = tokens[SOP_KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    c->coll_key = key;
    c->coll_nkey = nkey;

    if ((ntokens >= 5 && ntokens <= 12) && (strcmp(subcommand,"insert") == 0))
    {
        int32_t vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if ((! safe_strtol(tokens[SOP_KEY_TOKEN+1].value, &vlen)) ||
            (vlen < 0 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        int read_ntokens = SOP_KEY_TOKEN + 2;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 2) {
            if (strcmp(tokens[read_ntokens].value, "create") != 0 ||
                get_coll_create_attr_from_tokens(&tokens[read_ntokens+1], rest_ntokens-1,
                                                 ITEM_TYPE_SET, &c->coll_attr_space) != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
        } else {
            if (rest_ntokens != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = NULL;
        }

        if (check_and_handle_pipe_state(c)) {
            process_sop_prepare_nread(c, (int)OPERATION_SOP_INSERT, vlen, key, nkey);
        } else { /* pipe error */
            c->sbytes = vlen;
            conn_set_state(c, conn_swallow);
        }
    }
    else if ((ntokens >= 7 && ntokens <= 10) && (strcmp(subcommand, "create") == 0))
    {
        set_noreply_maybe(c, tokens, ntokens);

        int read_ntokens = SOP_KEY_TOKEN+1;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (get_coll_create_attr_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                             ITEM_TYPE_SET, &c->coll_attr_space) != 0) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        c->coll_attrp = &c->coll_attr_space;
        process_sop_create(c, key, nkey, c->coll_attrp);
    }
    else if ((ntokens >= 5 && ntokens <= 7) && (strcmp(subcommand, "delete") == 0))
    {
        int32_t vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if ((! safe_strtol(tokens[SOP_KEY_TOKEN+1].value, &vlen)) ||
            (vlen < 0 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        c->coll_drop = false;
        if (ntokens >= 6) {
            if (ntokens == 7 || c->noreply == false) {
                c->coll_drop = (strcmp(tokens[SOP_KEY_TOKEN+2].value, "drop")==0);
            }
            if ((ntokens == 6 && (c->noreply == false && c->coll_drop == false)) ||
                (ntokens == 7 && (c->noreply == false || c->coll_drop == false))) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        if (check_and_handle_pipe_state(c)) {
            process_sop_prepare_nread(c, (int)OPERATION_SOP_DELETE, vlen, key, nkey);
        } else { /* pipe error */
            c->sbytes = vlen;
            conn_set_state(c, conn_swallow);
        }
    }
    else if ((ntokens==5 || ntokens==6) && strcmp(subcommand, "exist") == 0)
    {
        int32_t vlen;

        set_pipe_maybe(c, tokens, ntokens);

        if ((! safe_strtol(tokens[SOP_KEY_TOKEN+1].value, &vlen)) ||
            (vlen < 0 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        if (check_and_handle_pipe_state(c)) {
            process_sop_prepare_nread(c, (int)OPERATION_SOP_EXIST, vlen, key, nkey);
        } else { /* pipe error */
            c->sbytes = vlen;
            conn_set_state(c, conn_swallow);
        }
    }
    else if ((ntokens==5 || ntokens==6) && (strcmp(subcommand, "get") == 0))
    {
        bool delete = false;
        bool drop_if_empty = false;
        uint32_t count = 0;

        if (! safe_strtoul(tokens[SOP_KEY_TOKEN+1].value, &count)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (ntokens == 6) {
            if (strcmp(tokens[SOP_KEY_TOKEN+2].value, "delete")==0) {
                delete = true;
            } else if (strcmp(tokens[SOP_KEY_TOKEN+2].value, "drop")==0) {
                delete = true;
                drop_if_empty = true;
            } else {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        process_sop_get(c, key, nkey, count, delete, drop_if_empty);
    }
    else
    {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#endif

#if MAP_PROCESS || 1
/** mop create */
static void process_mop_create(conn *c, char *key, size_t nkey, item_attr *attrp)
{
    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->map_struct_create(mc_engine.v0, c, key, nkey, attrp, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        stats_prefix_record_mop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, mop_create, key, nkey);
        out_string(c, "CREATED");
        break;
    default:
        STATS_CMD_NOKEY(c, mop_create);
        if (ret == ENGINE_KEY_EEXISTS)       out_string(c, "EXISTS");
        else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** mop insert */
static void process_mop_insert_complete(conn *c)
{
    assert(c->coll_op == OPERATION_MOP_INSERT);
    assert(c->coll_eitem != NULL);
    ENGINE_ERROR_CODE ret;

    mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_MAP, c->coll_eitem, &c->einfo);

    /* copy the field string into the element item. */
    memcpy((void*)c->einfo.score, c->coll_field.value, c->coll_field.length);

    if (einfo_check_ascii_tail_string(&c->einfo) != 0) { /* check "\r\n" */
        ret = ENGINE_EINVAL;
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        bool created;
        ret = mc_engine.v1->map_elem_insert(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                            c->coll_eitem, c->coll_attrp, &created, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            stats_prefix_record_mop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, mop_insert, c->coll_key, c->coll_nkey);
            if (created) out_string(c, "CREATED_STORED");
            else         out_string(c, "STORED");
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISSES(c, mop_insert, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_CMD_NOKEY(c, mop_insert);
            if (ret == ENGINE_EBADTYPE)          out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_EOVERFLOW)    out_string(c, "OVERFLOWED");
            else if (ret == ENGINE_ELEM_EEXISTS) out_string(c, "ELEMENT_EXISTS");
            else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
            else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    if (ret != ENGINE_SUCCESS) {
        mc_engine.v1->map_elem_free(mc_engine.v0, c, c->coll_eitem);
    }
    c->coll_eitem = NULL;
}

/** mop update */
static void process_mop_update_complete(conn *c)
{
    assert(c->coll_op == OPERATION_MOP_UPDATE);
    assert(c->coll_eitem != NULL);
    value_item *value = (value_item *)c->coll_eitem;

    if (strncmp(&value->ptr[value->len-2], "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        ENGINE_ERROR_CODE ret;
        ret = mc_engine.v1->map_elem_update(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                            &c->coll_field, value->ptr, value->len, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            stats_prefix_record_mop_update(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_ELEM_HITS(c, mop_update, c->coll_key, c->coll_nkey);
            out_string(c, "UPDATED");
            break;
        case ENGINE_ELEM_ENOENT:
            STATS_NONE_HITS(c, mop_update, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND_ELEMENT");
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISSES(c, mop_update, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_CMD_NOKEY(c, mop_update);
            if (ret == ENGINE_EBADTYPE)    out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    free((void*)c->coll_eitem);
    c->coll_eitem = NULL;
}

/** mop delete */
static void process_mop_delete_complete(conn *c)
{
    assert(c->coll_op == OPERATION_MOP_DELETE);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    field_t *fld_tokens = NULL;
    uint32_t del_count = 0;
    bool dropped;

    if (c->coll_strkeys != NULL) {
        fld_tokens = (field_t*)token_buff_get(&c->thread->token_buff, c->coll_numkeys);
        if (fld_tokens != NULL) {
            bool must_backward_compatible = true; /* Must be backward compatible */
            ret = tokenize_sblocks(&c->memblist, c->coll_lenkeys, c->coll_numkeys,
                                   MAX_FIELD_LENG, must_backward_compatible, (token_t*)fld_tokens);
            /* ret : ENGINE_SUCCESS | ENGINE_EBADVALUE | ENGINE_ENOMEM */
        } else {
            ret = ENGINE_ENOMEM;
        }
    }

    if (ret == ENGINE_SUCCESS) {
        ret = mc_engine.v1->map_elem_delete(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                            c->coll_numkeys, fld_tokens, c->coll_drop,
                                            &del_count, &dropped, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
            stats_prefix_record_mop_delete(c->coll_key, c->coll_nkey, is_hit);
        }
#ifdef DETECT_LONG_QUERY
        if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
            if (! lqdetect_mop_delete(c->client_ip, c->coll_key, del_count,
                                      c->coll_numkeys, c->coll_drop)) {
                lqdetect_in_use = false;
            }
        }
#endif
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, mop_delete, c->coll_key, c->coll_nkey);
        if (dropped) out_string(c, "DELETED_DROPPED");
        else         out_string(c, "DELETED");
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, mop_delete, c->coll_key, c->coll_nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISSES(c, mop_delete, c->coll_key, c->coll_nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_CMD_NOKEY(c, mop_delete);
        if (ret == ENGINE_EBADTYPE)       out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_ENOMEM)    out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }

    /* free key strings and tokens buffer */
    if (c->coll_strkeys != NULL) {
        /* free token buffer */
        if (fld_tokens != NULL) {
            token_buff_release(&c->thread->token_buff, fld_tokens);
        }
        /* free key string memory blocks */
        assert(c->coll_strkeys == (void*)&c->memblist);
        mblck_list_free(&c->thread->mblck_pool, &c->memblist);
        c->coll_strkeys = NULL;
    }
}

/** mop get */
static int make_mop_elem_response(char *bufptr, eitem_info *einfo)
{
    char *tmpptr = bufptr;

    /* field */
    assert(einfo->nscore > 0);
    memcpy(tmpptr, einfo->score, einfo->nscore);
    tmpptr += (int)einfo->nscore;

    /* nbytes */
    sprintf(tmpptr, " %u ", einfo->nbytes-2);
    tmpptr += strlen(tmpptr);

    return (int)(tmpptr - bufptr);
}

static ENGINE_ERROR_CODE
out_mop_get_response(conn *c, bool delete, struct elems_result *eresultp)
{
    eitem  **elem_array = eresultp->elem_array;
    uint32_t elem_count = eresultp->elem_count;
    int      bufsize;
    int      resplen;
    char    *respbuf; /* response string buffer */
    char    *respptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* allocate response string buffer */
        bufsize = ((2*UINT32_STR_LENG) + 30) /* response head and tail size */
                  + (elem_count * ((MAX_FIELD_LENG+2) + (UINT32_STR_LENG+2))); /* response body size */
        respbuf = (char*)malloc(bufsize);
        if (respbuf == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr = respbuf;

        /* make response head */
        sprintf(respptr, "VALUE %u %u\r\n", htonl(eresultp->flags), elem_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        /* make response body */
        for (int i = 0; i < elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_MAP,
                                        elem_array[i], &c->einfo);
            resplen = make_mop_elem_response(respptr, &c->einfo);
            if ((add_iov(c, respptr, resplen) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += resplen;
        }
        if (ret == ENGINE_ENOMEM) break;

        /* make response tail */
        if (delete) {
            sprintf(respptr, "%s\r\n", (eresultp->dropped ? "DELETED_DROPPED" : "DELETED"));
        } else {
            sprintf(respptr, "END\r\n");
        }
        if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        c->coll_eitem  = (void *)elem_array;
        c->coll_ecount = elem_count;
        c->coll_resps  = respbuf;
        c->coll_op     = OPERATION_MOP_GET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else { /* ENGINE_ENOMEM */
        mc_engine.v1->map_elem_release(mc_engine.v0, c, elem_array, elem_count);
        if (elem_array)
            free(elem_array);
        if (respbuf)
            free(respbuf);
    }
    return ret;
}

static void process_mop_get_complete(conn *c)
{
    assert(c->coll_op == OPERATION_MOP_GET);
    assert(c->ewouldblock == false);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct elems_result eresult;
    field_t *fld_tokens = NULL;
    bool delete = c->coll_delete;
    bool drop_if_empty = c->coll_drop;

    if (c->coll_strkeys != NULL) {
        fld_tokens = (field_t*)token_buff_get(&c->thread->token_buff, c->coll_numkeys);
        if (fld_tokens != NULL) {
            bool must_backward_compatible = true; /* Must be backward compatible */
            ret = tokenize_sblocks(&c->memblist, c->coll_lenkeys, c->coll_numkeys,
                                   MAX_FIELD_LENG, must_backward_compatible, (token_t*)fld_tokens);
            /* ret : ENGINE_SUCCESS | ENGINE_EBADVALUE | ENGINE_ENOMEM */
        } else {
            ret = ENGINE_ENOMEM;
        }
    }

    if (ret == ENGINE_SUCCESS) {
        ret = mc_engine.v1->map_elem_get(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                         c->coll_numkeys, fld_tokens, delete, drop_if_empty,
                                         &eresult, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
            stats_prefix_record_mop_get(c->coll_key, c->coll_nkey, is_hit);
        }
#ifdef DETECT_LONG_QUERY
        if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
            if (! lqdetect_mop_get(c->client_ip, c->coll_key, eresult.elem_count,
                                   c->coll_numkeys, delete, drop_if_empty)) {
                lqdetect_in_use = false;
            }
        }
#endif
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        ret = out_mop_get_response(c, delete, &eresult);
        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, mop_get, c->coll_key, c->coll_nkey);
        } else {
            STATS_CMD_NOKEY(c, mop_get);
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, mop_get, c->coll_key, c->coll_nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, mop_get, c->coll_key, c->coll_nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, mop_get);
        if (ret == ENGINE_EBADTYPE)       out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_ENOMEM)    out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }

    /* free key strings and tokens buffer */
    if (c->coll_strkeys != NULL) {
        /* free token buffer */
        if (fld_tokens != NULL) {
            token_buff_release(&c->thread->token_buff, fld_tokens);
        }
        /* free key string memory blocks */
        assert(c->coll_strkeys == (void*)&c->memblist);
        mblck_list_free(&c->thread->mblck_pool, &c->memblist);
        c->coll_strkeys = NULL;
    }
}

/** map dispatcher */
static void process_mop_prepare_nread(conn *c, int cmd, char *key, size_t nkey, field_t *field, size_t vlen)
{
    assert(cmd == (int)OPERATION_MOP_INSERT || (int)OPERATION_MOP_UPDATE);
    eitem *elem = NULL;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (vlen > settings.max_element_bytes) {
        ret = ENGINE_E2BIG;
    } else if (cmd == OPERATION_MOP_INSERT) {
        ret = mc_engine.v1->map_elem_alloc(mc_engine.v0, c, key, nkey, field->length, vlen, &elem);
    } else {
        if ((elem = (eitem *)malloc(sizeof(value_item) + vlen)) == NULL)
            ret = ENGINE_ENOMEM;
        else
            ((value_item*)elem)->len = vlen;
    }
    if (ret == ENGINE_SUCCESS) {
        if (cmd == OPERATION_MOP_INSERT) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_MAP, elem, &c->einfo);
            ritem_set_first(c, CONN_RTYPE_EINFO, vlen);
        } else {
            c->ritem   = ((value_item *)elem)->ptr;
            c->rlbytes = vlen;
            c->rltotal = 0;
        }
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = cmd;
        c->coll_field  = *field;
        conn_set_state(c, conn_nread);
    } else {
        if (settings.detail_enabled) {
            stats_prefix_record_mop_insert(key, nkey, false);
        }
        if (cmd == OPERATION_MOP_INSERT) {
            STATS_CMD_NOKEY(c, mop_insert);
        } else if (cmd == OPERATION_MOP_UPDATE) {
            STATS_CMD_NOKEY(c, mop_update);
        }
        if (ret == ENGINE_E2BIG)       out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);

        if (ret != ENGINE_DISCONNECT) {
            /* swallow the data line */
            c->sbytes = vlen;
            if (c->state == conn_write) {
                c->write_and_go = conn_swallow;
            } else { /* conn_new_cmd (by noreply) */
                conn_set_state(c, conn_swallow);
            }
        }
    }
}

static void process_mop_prepare_nread_fields(conn *c, int cmd, char *key, size_t nkey, uint32_t flen)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    /* allocate memory blocks needed */
    if (mblck_list_alloc(&c->thread->mblck_pool, 1, flen, &c->memblist) < 0) {
        ret = ENGINE_ENOMEM;
    }
    if (ret == ENGINE_SUCCESS) {
        c->coll_strkeys = (void*)&c->memblist;
        ritem_set_first(c, CONN_RTYPE_MBLCK, flen);
        c->coll_ecount = 1;
        c->coll_op = cmd;
        c->coll_lenkeys = flen;
        conn_set_state(c, conn_nread);
    } else {
        if (cmd == OPERATION_MOP_DELETE) {
            STATS_CMD_NOKEY(c, mop_delete);
        } else if (cmd == OPERATION_MOP_GET) {
            STATS_CMD_NOKEY(c, mop_get);
        }
        /* ret == ENGINE_ENOMEM */
        out_string(c, "SERVER_ERROR out of memory");

        /* swallow the data line */
        c->sbytes = flen;
        if (c->state == conn_write) {
            c->write_and_go = conn_swallow;
        } else { /* conn_new_cmd (by noreply) */
            conn_set_state(c, conn_swallow);
        }
    }
}

static void process_mop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *key = tokens[MOP_KEY_TOKEN].value;
    size_t nkey = tokens[MOP_KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    c->coll_key = key;
    c->coll_nkey = nkey;

    if ((ntokens >= 6 && ntokens <= 13) && (strcmp(subcommand,"insert") == 0))
    {
        field_t field;
        int32_t vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        field.value = tokens[MOP_KEY_TOKEN+1].value;
        field.length = tokens[MOP_KEY_TOKEN+1].length;
        if (field.length > MAX_FIELD_LENG) {
            out_string(c, "CLIENT_ERROR too long field name");
            return;
        }

        if ((! safe_strtol(tokens[MOP_KEY_TOKEN+2].value, &vlen)) ||
            (vlen < 0 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        int read_ntokens = MOP_KEY_TOKEN + 3;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 2) {
            if (strcmp(tokens[read_ntokens].value, "create") != 0 ||
                get_coll_create_attr_from_tokens(&tokens[read_ntokens+1], rest_ntokens-1,
                                                 ITEM_TYPE_MAP, &c->coll_attr_space) != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
        } else {
            if (rest_ntokens != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = NULL;
        }

        if (check_and_handle_pipe_state(c)) {
            process_mop_prepare_nread(c, (int)OPERATION_MOP_INSERT, key, nkey, &field, vlen);
        } else { /* pipe error */
            c->sbytes = vlen;
            conn_set_state(c, conn_swallow);
        }
    }
    else if ((ntokens >= 7 && ntokens <= 10) && (strcmp(subcommand, "create") == 0))
    {
        set_noreply_maybe(c, tokens, ntokens);

        int read_ntokens = MOP_KEY_TOKEN+1;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (get_coll_create_attr_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                             ITEM_TYPE_MAP, &c->coll_attr_space) != 0) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        c->coll_attrp = &c->coll_attr_space;
        process_mop_create(c, key, nkey, c->coll_attrp);
    }
    else if ((ntokens >= 6 && ntokens <= 7) && (strcmp(subcommand, "update") == 0))
    {
        field_t field;
        int32_t vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        field.value = tokens[MOP_KEY_TOKEN+1].value;
        field.length = tokens[MOP_KEY_TOKEN+1].length;
        if (field.length > MAX_FIELD_LENG) {
            out_string(c, "CLIENT_ERROR too long field name");
            return;
        }

        if ((! safe_strtol(tokens[MOP_KEY_TOKEN+2].value, &vlen)) ||
            (vlen < 0 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        if (check_and_handle_pipe_state(c)) {
            process_mop_prepare_nread(c, (int)OPERATION_MOP_UPDATE, key, nkey, &field, vlen);
        } else { /* pipe error */
            c->sbytes = vlen;
            conn_set_state(c, conn_swallow);
        }
    }
    else if ((ntokens >= 6 && ntokens <= 8) && (strcmp(subcommand, "delete") == 0))
    {
        uint32_t lenfields, numfields;
        bool drop_if_empty = false;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if ((! safe_strtoul(tokens[MOP_KEY_TOKEN+1].value, &lenfields)) ||
            (! safe_strtoul(tokens[MOP_KEY_TOKEN+2].value, &numfields))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (lenfields > 0) {
            if (numfields == 0 ||
                lenfields > (UINT_MAX-2) ||
                lenfields > ((numfields*MAX_FIELD_LENG) + numfields-1)) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad value");
                return;
            }
        }
        if (numfields > 0) {
            if (lenfields == 0 ||
                numfields > settings.max_map_size ||
                numfields > ((lenfields/2) + 1)) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad value");
                return;
            }
        }

        int read_ntokens = MOP_KEY_TOKEN + 3;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens > 0) {
            if (strcmp(tokens[read_ntokens].value, "drop")==0) {
                drop_if_empty = true;
            } else {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        c->coll_numkeys = numfields;
        c->coll_drop = drop_if_empty;

        if (check_and_handle_pipe_state(c)) {
            if (lenfields == 0 && numfields == 0) {
                c->coll_lenkeys = lenfields;
                c->coll_op      = OPERATION_MOP_DELETE;
                process_mop_delete_complete(c);
            } else {
                lenfields += 2;
                process_mop_prepare_nread_fields(c, (int)OPERATION_MOP_DELETE, key, nkey, lenfields);
            }
        } else { /* pipe error */
            if (lenfields > 0) {
                c->sbytes = lenfields;
                conn_set_state(c, conn_swallow);
            } else {
                conn_set_state(c, conn_new_cmd);
            }
        }
    }
    else if ((ntokens >= 6 && ntokens <= 7) && (strcmp(subcommand, "get") == 0))
    {
        uint32_t lenfields, numfields;
        bool delete = false;
        bool drop_if_empty = false;

        if ((! safe_strtoul(tokens[MOP_KEY_TOKEN+1].value, &lenfields)) ||
            (! safe_strtoul(tokens[MOP_KEY_TOKEN+2].value, &numfields))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (lenfields > 0) {
            if (numfields == 0 ||
                lenfields > (UINT_MAX-2) ||
                lenfields > ((numfields*MAX_FIELD_LENG) + numfields-1)) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad value");
                return;
            }
        }
        if (numfields > 0) {
            if (lenfields == 0 ||
                numfields > settings.max_map_size ||
                numfields > ((lenfields/2) + 1)) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad value");
                return;
            }
        }

        if (ntokens == 7) {
            if (strcmp(tokens[MOP_KEY_TOKEN+3].value, "delete")==0) {
                delete = true;
            } else if (strcmp(tokens[MOP_KEY_TOKEN+3].value, "drop")==0) {
                delete = true;
                drop_if_empty = true;
            } else {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        c->coll_numkeys = numfields;
        c->coll_delete = delete;
        c->coll_drop = drop_if_empty;

        if (lenfields == 0 && numfields == 0) {
            c->coll_lenkeys = lenfields;
            c->coll_op      = OPERATION_MOP_GET;
            process_mop_get_complete(c);
        } else {
            lenfields += 2;
            process_mop_prepare_nread_fields(c, (int)OPERATION_MOP_GET, key, nkey, lenfields);
        }
    }
    else
    {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#endif

#if BTREE_PROCESS || 1
/** btree common/util */
static int make_bop_elem_response(char *bufptr, eitem_info *einfo)
{
    char *tmpptr = bufptr;

    /* bkey */
    if (einfo->nscore > 0) {
        memcpy(tmpptr, "0x", 2); tmpptr += 2;
        safe_hexatostr(einfo->score, einfo->nscore, tmpptr);
        tmpptr += strlen(tmpptr);
    } else {
        sprintf(tmpptr, "%"PRIu64"", *(uint64_t*)einfo->score);
        tmpptr += strlen(tmpptr);
    }
    /* eflag */
    if (einfo->neflag > 0) {
        memcpy(tmpptr, " 0x", 3); tmpptr += 3;
        safe_hexatostr(einfo->eflag, einfo->neflag, tmpptr);
        tmpptr += strlen(tmpptr);
    }
    /* nbytes */
    sprintf(tmpptr, " %u ", einfo->nbytes-2);
    tmpptr += strlen(tmpptr);

    return (int)(tmpptr - bufptr);
}

/** bop create */
static void process_bop_create(conn *c, char *key, size_t nkey, item_attr *attrp)
{
    assert(c->ewouldblock == false);

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_struct_create(mc_engine.v0, c, key, nkey, attrp, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        stats_prefix_record_bop_create(key, nkey);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS(c, bop_create, key, nkey);
        out_string(c, "CREATED");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_create);
        if (ret == ENGINE_KEY_EEXISTS)       out_string(c, "EXISTS");
        else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
        else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** bop insert/upsert */
static ENGINE_ERROR_CODE
out_bop_trim_response(conn *c, void *elem, uint32_t flags)
{
    eitem **elem_array;
    int   bufsize;
    int   resplen;
    char *respbuf; /* response string buffer */
    char *respptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* allocate response string buffer */
        bufsize = sizeof(void*) /* an element pointer */
                + ((2*UINT32_STR_LENG) + 30) /* response head and tail size */
                + ((MAX_BKEY_LENG*2+2) + (MAX_EFLAG_LENG*2+2) + UINT32_STR_LENG+3); /* response body size */
        respbuf = (char*)malloc(bufsize);
        if (respbuf == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr = respbuf;

        /* save the trimmed element */
        elem_array = (eitem **)respptr;
        elem_array[0] = elem;
        respptr = (char*)&elem_array[1];

        /* make response */
        mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE, elem, &c->einfo);
        sprintf(respptr, "VALUE %u 1\r\n", htonl(flags));
        resplen = strlen(respptr);
        resplen += make_bop_elem_response(respptr + resplen, &c->einfo);

        if ((add_iov(c, respptr, resplen) != 0) ||
            (add_iov_einfo_value(c, &c->einfo) != 0) ||
            (add_iov(c, "TRIMMED\r\n", 9) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0))
        {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        c->coll_eitem  = elem_array;
        c->coll_ecount = 1;
        c->coll_resps  = respbuf;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else { /* ENGINE_ENOMEM */
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, &elem, 1);
        if (respbuf)
            free(respbuf);
        out_string(c, "SERVER_ERROR out of memory writing trim response");
    }
    return ret;
}

static void process_bop_insert_complete(conn *c)
{
    assert(c->coll_op == OPERATION_BOP_INSERT ||
           c->coll_op == OPERATION_BOP_UPSERT);
    assert(c->coll_eitem != NULL);

    mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE, c->coll_eitem, &c->einfo);

    if (einfo_check_ascii_tail_string(&c->einfo) != 0) { /* check "\r\n" */
        /* release the btree element */
        mc_engine.v1->btree_elem_free(mc_engine.v0, c, c->coll_eitem);
        c->coll_eitem = NULL;
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        eitem_result trim_result; // contain the info of an element trimmed by maxcount overflow
        bool created;
        bool replaced;
        bool replace_if_exist = (c->coll_op == OPERATION_BOP_UPSERT ? true : false);
        ENGINE_ERROR_CODE ret;

        ret = mc_engine.v1->btree_elem_insert(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                              c->coll_eitem, replace_if_exist,
                                              c->coll_attrp, &replaced, &created,
                                              (c->coll_getrim ? &trim_result : NULL), 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            stats_prefix_record_bop_insert(c->coll_key, c->coll_nkey, (ret==ENGINE_SUCCESS));
        }

        /* release the btree element in advance since coll_eitem field is to be used, soon. */
        if (ret != ENGINE_SUCCESS) {
            mc_engine.v1->btree_elem_free(mc_engine.v0, c, c->coll_eitem);
        }
        c->coll_eitem = NULL;

        switch (ret) {
        case ENGINE_SUCCESS:
            STATS_HITS(c, bop_insert, c->coll_key, c->coll_nkey);
            if (c->coll_getrim && trim_result.elems != NULL) { /* getrim flag */
                assert(trim_result.count == 1);
                (void)out_bop_trim_response(c, trim_result.elems, trim_result.flags);
            } else {
                /* no getrim flag or no trimmed element */
                if (replaced) {
                    out_string(c, "REPLACED");
                } else {
                    if (created) out_string(c, "CREATED_STORED");
                    else         out_string(c, "STORED");
                }
            }
            break;
        case ENGINE_KEY_ENOENT:
            STATS_MISSES(c, bop_insert, c->coll_key, c->coll_nkey);
            out_string(c, "NOT_FOUND");
            break;
        default:
            STATS_CMD_NOKEY(c, bop_insert);
            if (ret == ENGINE_EBADTYPE)          out_string(c, "TYPE_MISMATCH");
            else if (ret == ENGINE_EBADBKEY)     out_string(c, "BKEY_MISMATCH");
            else if (ret == ENGINE_EOVERFLOW)    out_string(c, "OVERFLOWED");
            else if (ret == ENGINE_EBKEYOOR)     out_string(c, "OUT_OF_RANGE");
            else if (ret == ENGINE_ELEM_EEXISTS) out_string(c, "ELEMENT_EXISTS");
            else if (ret == ENGINE_PREFIX_ENAME) out_string(c, "CLIENT_ERROR invalid prefix name");
            else if (ret == ENGINE_ENOMEM)       out_string(c, "SERVER_ERROR out of memory");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }
}

/** bop update */
static void process_bop_update_complete(conn *c)
{
    assert(c->coll_op == OPERATION_BOP_UPDATE);
    assert(c->ewouldblock == false);
    char *new_value = NULL;
    int  new_nbytes = 0;

    if (c->coll_eitem != NULL) {
        value_item *value = (value_item *)c->coll_eitem;
        if (strncmp(&value->ptr[value->len-2], "\r\n", 2) != 0) {
            out_string(c, "CLIENT_ERROR bad data chunk");
            free((void*)c->coll_eitem);
            c->coll_eitem = NULL;
            return;
        }
        new_value  = value->ptr;
        new_nbytes = value->len;
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_update(mc_engine.v0, c, c->coll_key, c->coll_nkey,
                                          &c->coll_bkrange,
                                          (c->coll_eupdate.neflag == EFLAG_NULL ? NULL : &c->coll_eupdate),
                                          new_value, new_nbytes, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_bop_update(c->coll_key, c->coll_nkey, is_hit);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, bop_update, c->coll_key, c->coll_nkey);
        out_string(c, "UPDATED");
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_update, c->coll_key, c->coll_nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISSES(c, bop_update, c->coll_key, c->coll_nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_update);
        if (ret == ENGINE_EBADTYPE)       out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY)  out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_EBADEFLAG) out_string(c, "EFLAG_MISMATCH");
        else if (ret == ENGINE_ENOMEM)    out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }

    if (c->coll_eitem != NULL) {
        free((void*)c->coll_eitem);
        c->coll_eitem = NULL;
    }
}

/** bop delete */
static void process_bop_delete(conn *c, char *key, size_t nkey,
                               bkey_range *bkrange, eflag_filter *efilter,
                               uint32_t count, bool drop_if_empty)
{
    assert(c->ewouldblock == false);
    uint32_t del_count;
    uint32_t acc_count; /* access count */
    bool     dropped;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_delete(mc_engine.v0, c, key, nkey,
                                          bkrange, efilter, count, drop_if_empty,
                                          &del_count, &acc_count, &dropped, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_bop_delete(key, nkey, is_hit);
    }
#ifdef DETECT_LONG_QUERY
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (! lqdetect_bop_delete(c->client_ip, key, acc_count,
                                  bkrange, efilter, count, drop_if_empty)) {
            lqdetect_in_use = false;
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, bop_delete, key, nkey);
        if (dropped) out_string(c, "DELETED_DROPPED");
        else         out_string(c, "DELETED");
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_delete, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISSES(c, bop_delete, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_delete);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** bop get */
static ENGINE_ERROR_CODE
out_bop_get_response(conn *c, bool delete, struct elems_result *eresultp)
{
    eitem  **elem_array = eresultp->elem_array;
    uint32_t elem_count = eresultp->elem_count;
    int      bufsize;
    int      resplen;
    char *respbuf; /* response string buffer */
    char *respptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* allocate response string buffer */
        bufsize = ((2*UINT32_STR_LENG) + 30) /* response head and tail size */
                + (elem_count * ((MAX_BKEY_LENG*2+2) + (MAX_EFLAG_LENG*2+2) + UINT32_STR_LENG+3)); /* response body size */
        respbuf = (char*)malloc(bufsize);
        if (respbuf == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr = respbuf;

        /* make response head */
        sprintf(respptr, "VALUE %u %u\r\n", htonl(eresultp->flags), elem_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        /* make response body */
        for (int i = 0; i < elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE,
                                        elem_array[i], &c->einfo);
            resplen = make_bop_elem_response(respptr, &c->einfo);
            if ((add_iov(c, respptr, resplen) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += resplen;
        }
        if (ret == ENGINE_ENOMEM) break;

        /* make response tail */
        if (delete) {
            sprintf(respptr, "%s\r\n", (eresultp->dropped ? "DELETED_DROPPED" : "DELETED"));
        } else {
            sprintf(respptr, "%s\r\n", (eresultp->trimmed ? "TRIMMED" : "END"));
        }
        if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        c->coll_eitem  = (void *)elem_array;
        c->coll_ecount = elem_count;
        c->coll_resps  = respbuf;
        c->coll_op     = OPERATION_BOP_GET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else { /* ENGINE_ENOMEM */
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
        if (elem_array)
            free(elem_array);
        if (respbuf)
            free(respbuf);
    }
    return ret;
}

static void process_bop_get(conn *c, char *key, size_t nkey,
                            const bkey_range *bkrange, const eflag_filter *efilter,
                            const uint32_t offset, const uint32_t count,
                            const bool delete, const bool drop_if_empty)
{
    assert(c->ewouldblock == false);
    struct elems_result eresult;
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->btree_elem_get(mc_engine.v0, c, key, nkey,
                                       bkrange, efilter, offset, count,
                                       delete, drop_if_empty, &eresult, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_bop_get(key, nkey, is_hit);
    }
#ifdef DETECT_LONG_QUERY
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (! lqdetect_bop_get(c->client_ip, key, eresult.opcost_or_eindex,
                               bkrange, efilter, offset, count, delete, drop_if_empty)) {
            lqdetect_in_use = false;
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        ret = out_bop_get_response(c, delete, &eresult);
        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_get, key, nkey);
        } else {
            STATS_CMD_NOKEY(c, bop_get);
            out_string(c, "SERVER_ERROR out of memory writing get response");
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_get, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_EBKEYOOR:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, bop_get, key, nkey);
        if (ret == ENGINE_KEY_ENOENT)    out_string(c, "NOT_FOUND");
        else if (ret == ENGINE_EBKEYOOR) out_string(c, "OUT_OF_RANGE");
        else                             out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_get);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_ENOMEM)   out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}
/** bop count */
static void process_bop_count(conn *c, char *key, size_t nkey,
                              const bkey_range *bkrange, const eflag_filter *efilter)
{
    char buffer[32];
    uint32_t elem_count;
    uint32_t opcost; /* operation cost */

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_count(mc_engine.v0, c, key, nkey,
                                         bkrange, efilter,
                                         &elem_count, &opcost, 0);
    if (settings.detail_enabled) {
        stats_prefix_record_bop_count(key, nkey, (ret==ENGINE_SUCCESS));
    }
#ifdef DETECT_LONG_QUERY
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (! lqdetect_bop_count(c->client_ip, key, opcost, bkrange, efilter)) {
            lqdetect_in_use = false;
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, bop_count, key, nkey);
        sprintf(buffer, "COUNT=%u", elem_count);
        out_string(c, buffer);
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, bop_count, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_count);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** bop incr/decr */
static void process_bop_arithmetic(conn *c, char *key, size_t nkey, bkey_range *bkrange,
                                   const bool incr, const bool create,
                                   const uint64_t delta, const uint64_t initial,
                                   const eflag_t *eflagp)
{
    assert(c->ewouldblock == false);
    uint64_t result;
    char temp[INCR_MAX_STORAGE_LEN];

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_elem_arithmetic(mc_engine.v0, c, key, nkey,
                                              bkrange, incr, create,
                                              delta, initial, eflagp, &result, 0);
    CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        if (incr) stats_prefix_record_bop_incr(key, nkey, is_hit);
        else      stats_prefix_record_bop_decr(key, nkey, is_hit);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (incr) {
            STATS_ELEM_HITS(c, bop_incr, key, nkey);
        } else {
            STATS_ELEM_HITS(c, bop_decr, key, nkey);
        }
        snprintf(temp, sizeof(temp), "%"PRIu64, result);
        out_string(c, temp);
        break;
    case ENGINE_ELEM_ENOENT:
        if (incr) {
            STATS_NONE_HITS(c, bop_incr, key, nkey);
        } else {
            STATS_NONE_HITS(c, bop_decr, key, nkey);
        }
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
        if (incr) {
            STATS_MISSES(c, bop_incr, key, nkey);
        } else {
            STATS_MISSES(c, bop_decr, key, nkey);
        }
        out_string(c, "NOT_FOUND");
        break;
    default:
        if (incr) {
            STATS_CMD_NOKEY(c, bop_incr);
        } else {
            STATS_CMD_NOKEY(c, bop_decr);
        }
        if (ret == ENGINE_EINVAL) out_string(c, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        else if (ret == ENGINE_EBADTYPE)  out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY)  out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_EBKEYOOR)  out_string(c, "OUT_OF_RANGE");
        else if (ret == ENGINE_EOVERFLOW) out_string(c, "OVERFLOWED");
        else if (ret == ENGINE_ENOMEM)    out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** bop mget */
#ifdef SUPPORT_BOP_MGET
static ENGINE_ERROR_CODE
process_bop_mget_single(conn *c, const char *key, size_t nkey,
                        struct elems_result *eresultp,
                        char *resultbuf, int *resultlen)
{
    char *respptr = resultbuf;
    int   resplen;
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->btree_elem_get(mc_engine.v0, c, key, nkey,
                                       &c->coll_bkrange,
                                       (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                                       c->coll_roffset, c->coll_rcount, false, false,
                                       eresultp, 0);
    /* The read-only operation do not return ENGINE_EWOULDBLOCK */
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_bop_get(key, nkey, is_hit);
    }

    if (ret == ENGINE_SUCCESS) {
        do {
            /* format : VALUE <key> <status> <flags> <ecount>\r\n */
            sprintf(respptr, " %s %u %u\r\n",
                             (eresultp->trimmed ? "TRIMMED" : "OK"),
                             htonl(eresultp->flags), eresultp->elem_count);
            resplen = strlen(respptr);

            if ((add_iov(c, "VALUE ", 6) != 0) ||
                (add_iov(c, key, nkey) != 0) ||
                (add_iov(c, respptr, resplen) != 0)) {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += resplen;

            for (int e = 0; e < eresultp->elem_count; e++) {
                mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE,
                                            eresultp->elem_array[e], &c->einfo);
                sprintf(respptr, "ELEMENT ");
                resplen = strlen(respptr);
                resplen += make_bop_elem_response(respptr + resplen, &c->einfo);

                if ((add_iov(c, respptr, resplen) != 0) ||
                    (add_iov_einfo_value(c, &c->einfo) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
        } while(0);

        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_get, key, nkey);
        } else { /* ret == ENGINE_ENOMEM */
            STATS_CMD_NOKEY(c, bop_get);
            mc_engine.v1->btree_elem_release(mc_engine.v0, c,
                                             eresultp->elem_array,
                                             eresultp->elem_count);
            free(eresultp->elem_array);
            eresultp->elem_array = NULL;
            eresultp->elem_count = 0;
        }
    } else {
        if (ret == ENGINE_ELEM_ENOENT) {
            STATS_NONE_HITS(c, bop_get, key, nkey);
            sprintf(respptr, " %s\r\n", "NOT_FOUND_ELEMENT");
            ret = ENGINE_SUCCESS;
        }
        else if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_EBKEYOOR || ret == ENGINE_UNREADABLE) {
            STATS_MISSES(c, bop_get, key, nkey);
            if (ret == ENGINE_KEY_ENOENT)    sprintf(respptr, " %s\r\n", "NOT_FOUND");
            else if (ret == ENGINE_EBKEYOOR) sprintf(respptr, " %s\r\n", "OUT_OF_RANGE");
            else                             sprintf(respptr, " %s\r\n", "UNREADABLE");
            ret = ENGINE_SUCCESS;
        }
        else if (ret == ENGINE_EBADTYPE || ret == ENGINE_EBADBKEY) {
            STATS_CMD_NOKEY(c, bop_get);
            if (ret == ENGINE_EBADTYPE) sprintf(respptr, " %s\r\n", "TYPE_MISMATCH");
            else                        sprintf(respptr, " %s\r\n", "BKEY_MISMATCH");
            ret = ENGINE_SUCCESS;
        }
        else {
            /* ENGINE_ENOMEM or ENGINE_DISCONNECT or SERVER error */
            STATS_CMD_NOKEY(c, bop_get);
        }
        if (ret == ENGINE_SUCCESS) {
            resplen = strlen(respptr);
            if ((add_iov(c, "VALUE ", 6) != 0) ||
                (add_iov(c, key, nkey) != 0) ||
                (add_iov(c, respptr, resplen) != 0)) {
                ret = ENGINE_ENOMEM;
            }
            respptr += resplen;
        }
    }
    if (ret == ENGINE_SUCCESS) {
        *resultlen = (respptr - resultbuf);
    }
    return ret;
}

static void process_bop_mget_complete(conn *c)
{
    assert(c->coll_op == OPERATION_BOP_MGET);
    assert(c->coll_eitem != NULL);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct elems_result *eresult = (struct elems_result *)c->coll_eitem;
    char *resultptr = (char*)eresult + (c->coll_numkeys * sizeof(struct elems_result));
    int k,resultlen;
    token_t *key_tokens;

    key_tokens = (token_t*)token_buff_get(&c->thread->token_buff, c->coll_numkeys);
    if (key_tokens != NULL) {
        bool must_backward_compatible = true; /* Must be backward compatible */
        ret = tokenize_sblocks(&c->memblist, c->coll_lenkeys, c->coll_numkeys,
                               KEY_MAX_LENGTH, must_backward_compatible, key_tokens);
        /* ret : ENGINE_SUCCESS | ENGINE_EBADVALUE | ENGINE_ENOMEM */
    } else {
        ret = ENGINE_ENOMEM;
    }
    if (ret == ENGINE_SUCCESS) {
        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }
        for (k = 0; k < c->coll_numkeys; k++) {
            ret = process_bop_mget_single(c, key_tokens[k].value, key_tokens[k].length,
                                          &eresult[k], resultptr, &resultlen);
            if (ret != ENGINE_SUCCESS) {
                break; /* ENGINE_ENOMEM or ENGINE_DISCONNECT or SEVERE error */
            }
            resultptr += resultlen;
        }
        if (k == c->coll_numkeys) {
            sprintf(resultptr, "END\r\n");
            if ((add_iov(c, resultptr, strlen(resultptr)) != 0) ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM;
            }
        }
        if (ret != ENGINE_SUCCESS) { /* release elements */
            int bop_get_count = k;
            for (k = 0; k < bop_get_count; k++) {
                /* Individual bop get might not get elements.
                 * So, we check the element array is not empty.
                 */
                if (eresult[k].elem_array != NULL) {
                    mc_engine.v1->btree_elem_release(mc_engine.v0, c,
                                                     eresult[k].elem_array,
                                                     eresult[k].elem_count);
                    free(eresult[k].elem_array);
                    eresult[k].elem_array = NULL;
                    eresult[k].elem_count = 0;
                }
            }
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_OKS_NOKEY(c, bop_mget);
        /* Remember this command so we can garbage collect it later */
        /* c->coll_eitem  = (void *)elem_array; */
        c->coll_ecount = 0;
        for (k = 0; k < c->coll_numkeys; k++) {
            if (eresult[k].elem_array != NULL) {
                c->coll_ecount += eresult[k].elem_count;
            }
        }
        c->coll_op = OPERATION_BOP_MGET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
        break;
    default:
        STATS_CMD_NOKEY(c, bop_mget);
        if (ret == ENGINE_EBADVALUE)   out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }

    /* free token buffer */
    if (key_tokens != NULL) {
        token_buff_release(&c->thread->token_buff, key_tokens);
    }
    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            /* free key string memory blocks */
            assert(c->coll_strkeys == (void*)&c->memblist);
            mblck_list_free(&c->thread->mblck_pool, &c->memblist);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

/** bop smget */
#ifdef SUPPORT_BOP_SMGET
static char *get_smget_miss_response(int res)
{
    if (res == ENGINE_KEY_ENOENT)      return " NOT_FOUND\r\n";
    else if (res == ENGINE_UNREADABLE) return " UNREADABLE\r\n";
    else if (res == ENGINE_EBKEYOOR)   return " OUT_OF_RANGE\r\n";
    else                               return " UNKNOWN\r\n";
}

static int make_smget_trim_response(char *bufptr, eitem_info *einfo)
{
    char *tmpptr = bufptr;

    /* bkey */
    if (einfo->nscore > 0) {
        memcpy(tmpptr, " 0x", 3); tmpptr += 3;
        safe_hexatostr(einfo->score, einfo->nscore, tmpptr);
        tmpptr += strlen(tmpptr);
    } else {
        sprintf(tmpptr, " %"PRIu64"", *(uint64_t*)einfo->score);
        tmpptr += strlen(tmpptr);
    }
    sprintf(tmpptr, "\r\n");
    tmpptr += 2;
    return (int)(tmpptr - bufptr);
}

#ifdef JHPARK_OLD_SMGET_INTERFACE
static ENGINE_ERROR_CODE
out_bop_smget_old_response(conn *c, token_t *key_tokens,
                           eitem **elem_array, uint32_t *kfnd_array,
                           uint32_t *flag_array, uint32_t *kmis_array,
                           uint32_t elem_count, uint32_t kmis_count,
                           bool trimmed, bool duplicated)
{
    char *respptr = ((char*)kmis_array + (c->coll_numkeys * sizeof(uint32_t)));
    int   resplen;
    int   i, kidx;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        sprintf(respptr, "VALUE %u\r\n", elem_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        for (i = 0; i < elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE,
                                        elem_array[i], &c->einfo);
            sprintf(respptr, " %u ", htonl(flag_array[i])); /* flags */
            resplen = strlen(respptr);
            resplen += make_bop_elem_response(respptr + resplen, &c->einfo);
            kidx = kfnd_array[i];
            if ((add_iov(c, key_tokens[kidx].value, key_tokens[kidx].length) != 0) ||
                (add_iov(c, respptr, resplen) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += resplen;
        }
        if (ret == ENGINE_ENOMEM) break;

        sprintf(respptr, "MISSED_KEYS %u\r\n", kmis_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        if (kmis_count > 0) {
            for (i = 0; i < kmis_count; i++) {
                /* the last key string does not have delimiter character */
                kidx = kmis_array[i];
                if ((add_iov(c, key_tokens[kidx].value, key_tokens[kidx].length) != 0) ||
                    (add_iov(c, "\r\n", 2) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
            if (ret == ENGINE_ENOMEM) break;
        }

        if (trimmed == true) {
            sprintf(respptr, (duplicated ? "DUPLICATED_TRIMMED\r\n" : "TRIMMED\r\n"));
        } else {
            sprintf(respptr, (duplicated ? "DUPLICATED\r\n" : "END\r\n"));
        }
        if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        /* Remember this command so we can garbage collect it later */
        /* c->coll_eitem  = (void *)elem_array; */
        c->coll_ecount = elem_count;
        c->coll_op     = OPERATION_BOP_SMGET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else {
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
        out_string(c, "SERVER_ERROR out of memory writing get response");
    }
    return ret;
}

static void process_bop_smget_complete_old(conn *c)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    int smget_count = c->coll_roffset + c->coll_rcount;
    token_t *key_tokens;

    eitem   **elem_array = (eitem  **)c->coll_eitem;
    uint32_t *kfnd_array = (uint32_t*)((char*)elem_array + (smget_count*sizeof(eitem*)));
    uint32_t *flag_array = (uint32_t*)((char*)kfnd_array + (smget_count*sizeof(uint32_t)));
    uint32_t *kmis_array = (uint32_t*)((char*)flag_array + (smget_count*sizeof(uint32_t)));
    uint32_t  elem_count = 0;
    uint32_t  kmis_count = 0;
    bool trimmed;
    bool duplicated;

    key_tokens = (token_t*)token_buff_get(&c->thread->token_buff, c->coll_numkeys);
    if (key_tokens != NULL) {
        bool must_backward_compatible = true; /* Must be backward compatible */
        ret = tokenize_sblocks(&c->memblist, c->coll_lenkeys, c->coll_numkeys,
                               KEY_MAX_LENGTH, must_backward_compatible, key_tokens);
        /* ret : ENGINE_SUCCESS | ENGINE_EBADVALUE | ENGINE_ENOMEM */
    } else {
        ret = ENGINE_ENOMEM;
    }
    if (ret == ENGINE_SUCCESS) {
        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }
        assert(c->coll_numkeys > 0);
        assert(c->coll_rcount > 0);
        assert((c->coll_roffset + c->coll_rcount) <= MAX_SMGET_REQ_COUNT);
        ret = mc_engine.v1->btree_elem_smget_old(mc_engine.v0, c, key_tokens, c->coll_numkeys,
                                             &c->coll_bkrange,
                                             (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                                             c->coll_roffset, c->coll_rcount,
                                             elem_array, kfnd_array, flag_array, &elem_count,
                                             kmis_array, &kmis_count, &trimmed, &duplicated, 0);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (out_bop_smget_old_response(c, key_tokens, elem_array,
                                       kfnd_array, flag_array, kmis_array,
                                       elem_count, kmis_count,
                                       trimmed, duplicated) == ENGINE_SUCCESS) {
            STATS_OKS_NOKEY(c, bop_smget);
        } else {
            STATS_CMD_NOKEY(c, bop_smget);
        }
        break;
    default:
        STATS_CMD_NOKEY(c, bop_smget);
        if (ret == ENGINE_EBADVALUE)     out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_EBKEYOOR) out_string(c, "OUT_OF_RANGE");
        else if (ret == ENGINE_ENOMEM)   out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }

    /* free token buffer */
    if (key_tokens != NULL) {
        token_buff_release(&c->thread->token_buff, key_tokens);
    }

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            /* free key string memory blocks */
            assert(c->coll_strkeys == (void*)&c->memblist);
            mblck_list_free(&c->thread->mblck_pool, &c->memblist);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}
#endif

static ENGINE_ERROR_CODE
out_bop_smget_response(conn *c, token_t *key_tokens, smget_result_t *smresp)
{
    char *respptr = (char *)&smresp->miss_kinfo[c->coll_numkeys];
    int   resplen;
    int   i, kidx;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* Change smget response head string: VALUE => ELEMENTS.
         * It makes incompatible with the clients of lower version.
         */
        sprintf(respptr, "ELEMENTS %u\r\n", smresp->elem_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        for (i = 0; i < smresp->elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE,
                                        smresp->elem_array[i], &c->einfo);
            sprintf(respptr, " %u ", htonl(smresp->elem_kinfo[i].flag));
            resplen = strlen(respptr);
            resplen += make_bop_elem_response(respptr + resplen, &c->einfo);
            kidx = smresp->elem_kinfo[i].kidx;
            if ((add_iov(c, key_tokens[kidx].value, key_tokens[kidx].length) != 0) ||
                (add_iov(c, respptr, resplen) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += resplen;
        }
        if (ret == ENGINE_ENOMEM) break;

        sprintf(respptr, "MISSED_KEYS %u\r\n", smresp->miss_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        if (smresp->miss_count > 0) {
            char *str = NULL;
            for (i = 0; i < smresp->miss_count; i++) {
                /* the last key string does not have delimiter character */
                kidx = smresp->miss_kinfo[i].kidx;
                str = get_smget_miss_response(smresp->miss_kinfo[i].code);
                if ((add_iov(c, key_tokens[kidx].value, key_tokens[kidx].length) != 0) ||
                    (add_iov(c, str, strlen(str)) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
            }
            if (ret == ENGINE_ENOMEM) break;
        }

        sprintf(respptr, "TRIMMED_KEYS %u\r\n", smresp->trim_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        if (smresp->trim_count > 0) {
            for (i = 0; i < smresp->trim_count; i++) {
                mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE,
                                            smresp->trim_elems[i], &c->einfo);
                resplen = make_smget_trim_response(respptr, &c->einfo);
                kidx = smresp->trim_kinfo[i].kidx;
                if ((add_iov(c, key_tokens[kidx].value, key_tokens[kidx].length) != 0) ||
                    (add_iov(c, respptr, resplen) != 0)) {
                    ret = ENGINE_ENOMEM; break;
                }
                respptr += resplen;
            }
            if (ret == ENGINE_ENOMEM) break;
        }

        sprintf(respptr, (smresp->duplicated ? "DUPLICATED\r\n" : "END\r\n"));
        if ((add_iov(c, respptr, strlen(respptr)) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        /* Remember this command so we can garbage collect it later */
        /* c->coll_eitem  = (void *)elem_array; */
        c->coll_ecount = smresp->elem_count+smresp->trim_count;
        c->coll_op     = OPERATION_BOP_SMGET;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else {
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, smresp->elem_array,
                                         smresp->elem_count+smresp->trim_count);
        out_string(c, "SERVER_ERROR out of memory writing get response");
    }
    return ret;
}

static void process_bop_smget_complete(conn *c)
{
    assert(c->coll_op == OPERATION_BOP_SMGET);
    assert(c->coll_eitem != NULL);
#ifdef JHPARK_OLD_SMGET_INTERFACE
    if (c->coll_smgmode == 0) {
        process_bop_smget_complete_old(c);
        return;
    }
#endif
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    token_t *key_tokens;
    smget_result_t smres;

    smres.elem_array = (eitem **)c->coll_eitem;
    smres.elem_kinfo = (smget_ehit_t *)&smres.elem_array[c->coll_rcount+c->coll_numkeys];
    smres.miss_kinfo = (smget_emis_t *)&smres.elem_kinfo[c->coll_rcount];

    key_tokens = (token_t*)token_buff_get(&c->thread->token_buff, c->coll_numkeys);
    if (key_tokens != NULL) {
        bool must_backward_compatible = true; /* Must be backward compatible */
        ret = tokenize_sblocks(&c->memblist, c->coll_lenkeys, c->coll_numkeys,
                               KEY_MAX_LENGTH, must_backward_compatible, key_tokens);
        /* ret : ENGINE_SUCCESS | ENGINE_EBADVALUE | ENGINE_ENOMEM */
    } else {
        ret = ENGINE_ENOMEM;
    }
    if (ret == ENGINE_SUCCESS) {
        if (c->coll_bkrange.to_nbkey == BKEY_NULL) {
            memcpy(c->coll_bkrange.to_bkey, c->coll_bkrange.from_bkey,
                   (c->coll_bkrange.from_nbkey==0 ? sizeof(uint64_t) : c->coll_bkrange.from_nbkey));
            c->coll_bkrange.to_nbkey = c->coll_bkrange.from_nbkey;
        }
        assert(c->coll_numkeys > 0);
        assert(c->coll_rcount > 0);
        assert((c->coll_roffset + c->coll_rcount) <= MAX_SMGET_REQ_COUNT);
        ret = mc_engine.v1->btree_elem_smget(mc_engine.v0, c, key_tokens, c->coll_numkeys,
                                             &c->coll_bkrange,
                                             (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                                             c->coll_roffset, c->coll_rcount,
#ifdef JHPARK_OLD_SMGET_INTERFACE
                                             (c->coll_smgmode == 2 ? true : false),
#else
                                             c->coll_unique,
#endif
                                             &smres, 0);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        if (out_bop_smget_response(c, key_tokens, &smres) == ENGINE_SUCCESS) {
            STATS_OKS_NOKEY(c, bop_smget);
        } else {
            STATS_CMD_NOKEY(c, bop_smget);
        }
        break;
    default:
        STATS_CMD_NOKEY(c, bop_smget);
        if (ret == ENGINE_EBADVALUE)     out_string(c, "CLIENT_ERROR bad data chunk");
        else if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else if (ret == ENGINE_ENOMEM)   out_string(c, "SERVER_ERROR out of memory");
#if 0 // JHPARK_SMGET_OFFSET_HANDLING
        else if (ret == ENGINE_EBKEYOOR) out_string(c, "OUT_OF_RANGE");
#endif
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }

    /* free token buffer */
    if (key_tokens != NULL) {
        token_buff_release(&c->thread->token_buff, key_tokens);
    }

    if (ret != ENGINE_SUCCESS) {
        if (c->coll_strkeys != NULL) {
            /* free key string memory blocks */
            assert(c->coll_strkeys == (void*)&c->memblist);
            mblck_list_free(&c->thread->mblck_pool, &c->memblist);
            c->coll_strkeys = NULL;
        }
        if (c->coll_eitem != NULL) {
            free((void *)c->coll_eitem);
            c->coll_eitem = NULL;
        }
    }
}

#endif

/** bop position */
static void process_bop_position(conn *c, char *key, size_t nkey,
                                 const bkey_range *bkrange, ENGINE_BTREE_ORDER order)
{
    char buffer[32];
    int position;

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->btree_posi_find(mc_engine.v0, c, key, nkey,
                                        bkrange, order, &position, 0);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_bop_position(key, nkey, is_hit);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_ELEM_HITS(c, bop_position, key, nkey);
        sprintf(buffer, "POSITION=%d", position);
        out_string(c, buffer);
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_position, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, bop_position, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_position);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** bop gbp */
static ENGINE_ERROR_CODE
out_bop_gbp_response(conn *c, struct elems_result *eresultp)
{
    eitem  **elem_array = eresultp->elem_array;
    uint32_t elem_count = eresultp->elem_count;
    int      bufsize;
    int      resplen;
    char    *respbuf; /* response string buffer */
    char    *respptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* allocate response string buffer */
        bufsize = ((2*UINT32_STR_LENG) + 30) /* response head and tail size */
                  + (elem_count * ((MAX_BKEY_LENG*2+2) + (MAX_EFLAG_LENG*2+2) + UINT32_STR_LENG+3)); /* result body size */
        respbuf = (char*)malloc(bufsize);
        if (respbuf == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr = respbuf;

        /* make response head */
        sprintf(respptr, "VALUE %u %u\r\n", htonl(eresultp->flags), elem_count);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        /* make response body */
        for (int i = 0; i < elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE,
                                        elem_array[i], &c->einfo);
            resplen = make_bop_elem_response(respptr, &c->einfo);
            if ((add_iov(c, respptr, resplen) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += resplen;
        }
        if (ret == ENGINE_ENOMEM) break;

        /* make response tail */
        if ((add_iov(c, "END\r\n", 5) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        c->coll_eitem  = (void *)elem_array;
        c->coll_ecount = elem_count;
        c->coll_resps  = respbuf;
        c->coll_op     = OPERATION_BOP_GBP;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else { /* ENGINE_ENOMEM */
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
        if (elem_array)
            free(elem_array);
        if (respbuf)
            free(respbuf);
        out_string(c, "SERVER_ERROR out of memory writing get response");
    }
    return ret;
}

static void process_bop_gbp(conn *c, char *key, size_t nkey,
                            ENGINE_BTREE_ORDER order,
                            uint32_t from_posi, uint32_t to_posi)
{
    struct elems_result eresult;
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->btree_elem_get_by_posi(mc_engine.v0, c, key, nkey,
                                               order, from_posi, to_posi, &eresult, 0);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_bop_gbp(key, nkey, is_hit);
    }
#ifdef DETECT_LONG_QUERY
    if (lqdetect_in_use && ret == ENGINE_SUCCESS) {
        if (! lqdetect_bop_gbp(c->client_ip, key, eresult.elem_count,
                               from_posi, to_posi, order)) {
            lqdetect_in_use = false;
        }
    }
#endif

    switch (ret) {
    case ENGINE_SUCCESS:
        ret = out_bop_gbp_response(c, &eresult);
        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_gbp, key, nkey);
        } else {
            STATS_CMD_NOKEY(c, bop_gbp);
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_gbp, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, bop_gbp, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_gbp);
        if (ret == ENGINE_EBADTYPE) out_string(c, "TYPE_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** bop pwg */
static ENGINE_ERROR_CODE
out_bop_pwg_response(conn *c, int position, struct elems_result *eresultp)
{
    eitem  **elem_array = eresultp->elem_array;
    uint32_t elem_count = eresultp->elem_count;
    int      bufsize;
    int      resplen;
    char    *respbuf; /* response string buffer */
    char    *respptr;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    do {
        /* allocate response string buffer */
        bufsize = ((4*UINT32_STR_LENG) + 30) /* response head and tail size */
                  + (elem_count * ((MAX_BKEY_LENG*2+2) + (MAX_EFLAG_LENG*2+2) + UINT32_STR_LENG+3)); /* result body size */
        respbuf = (char*)malloc(bufsize);
        if (respbuf == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr = respbuf;

        /* make response head */
        sprintf(respptr, "VALUE %d %u %u %u\r\n", position, htonl(eresultp->flags), elem_count,
                                                  eresultp->opcost_or_eindex);
        if (add_iov(c, respptr, strlen(respptr)) != 0) {
            ret = ENGINE_ENOMEM; break;
        }
        respptr += strlen(respptr);

        /* make response body */
        for (int i = 0; i < elem_count; i++) {
            mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE,
                                        elem_array[i], &c->einfo);
            resplen = make_bop_elem_response(respptr, &c->einfo);
            if ((add_iov(c, respptr, resplen) != 0) ||
                (add_iov_einfo_value(c, &c->einfo) != 0))
            {
                ret = ENGINE_ENOMEM; break;
            }
            respptr += resplen;
        }
        if (ret == ENGINE_ENOMEM) break;

        /* make response tail */
        if ((add_iov(c, "END\r\n", 5) != 0) ||
            (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
            ret = ENGINE_ENOMEM; break;
        }
    } while(0);

    if (ret == ENGINE_SUCCESS) {
        c->coll_eitem  = (void *)elem_array;
        c->coll_ecount = elem_count;
        c->coll_resps  = respbuf;
        c->coll_op     = OPERATION_BOP_PWG;
        conn_set_state(c, conn_mwrite);
        c->msgcurr     = 0;
    } else { /* ENGINE_ENOMEM */
        mc_engine.v1->btree_elem_release(mc_engine.v0, c, elem_array, elem_count);
        if (elem_array)
            free(elem_array);
        if (respbuf)
            free(respbuf);
        out_string(c, "SERVER_ERROR out of memory writing get response");
    }
    return ret;
}

static void process_bop_pwg(conn *c, char *key, size_t nkey, const bkey_range *bkrange,
                            ENGINE_BTREE_ORDER order, const uint32_t count)
{
    struct elems_result eresult;
    int position;
    ENGINE_ERROR_CODE ret;

    ret = mc_engine.v1->btree_posi_find_with_get(mc_engine.v0, c, key, nkey,
                                                 bkrange, order, count, &position,
                                                 &eresult, 0);
    if (settings.detail_enabled) {
        bool is_hit = (ret==ENGINE_SUCCESS || ret==ENGINE_ELEM_ENOENT);
        stats_prefix_record_bop_pwg(key, nkey, is_hit);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        ret = out_bop_pwg_response(c, position, &eresult);
        if (ret == ENGINE_SUCCESS) {
            STATS_ELEM_HITS(c, bop_pwg, key, nkey);
        } else {
            STATS_CMD_NOKEY(c, bop_pwg);
        }
        break;
    case ENGINE_ELEM_ENOENT:
        STATS_NONE_HITS(c, bop_pwg, key, nkey);
        out_string(c, "NOT_FOUND_ELEMENT");
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_UNREADABLE:
        STATS_MISSES(c, bop_pwg, key, nkey);
        if (ret == ENGINE_KEY_ENOENT) out_string(c, "NOT_FOUND");
        else                          out_string(c, "UNREADABLE");
        break;
    default:
        STATS_CMD_NOKEY(c, bop_pwg);
        if (ret == ENGINE_EBADTYPE)      out_string(c, "TYPE_MISMATCH");
        else if (ret == ENGINE_EBADBKEY) out_string(c, "BKEY_MISMATCH");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** btree dispatcher */
#ifdef JHPARK_OLD_SMGET_INTERFACE
static inline int set_smget_mode_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int mode_index = ntokens - 2;
    int mode_value = 0;

    if (tokens[mode_index].value) {
        if (strcmp(tokens[mode_index].value, "duplicate") == 0)
            mode_value = 1;
        else if (strcmp(tokens[mode_index].value, "unique") == 0)
            mode_value = 2;
    }
    return mode_value;
}
#else
static inline bool set_unique_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int unique_index = ntokens - 2;

    if (tokens[unique_index].value
        && strcmp(tokens[unique_index].value, "unique") == 0)
        return true;
    else
        return false;
}
#endif

static inline int get_bkey_from_str(const char *str, unsigned char *bkey)
{
    if (strncmp(str, "0x", 2) == 0) { /* hexadeciaml bkey */
        if (safe_strtohexa(str+2, bkey, MAX_BKEY_LENG)) {
            return (strlen(str+2)/2);
        }
    } else { /* 64 bit unsigned integer */
        if (safe_strtoull(str, (uint64_t*)bkey)) {
            return 0;
        }
    }
    return -1;
}

static inline int get_eflag_from_str(const char *str, unsigned char *eflag)
{
    if (strncmp(str, "0x", 2) == 0) {
        if (safe_strtohexa(str+2, eflag, MAX_EFLAG_LENG)) {
            return (strlen(str+2)/2);
        }
    }
    return -1;
}

static inline int get_bkey_range_from_str(const char *str, bkey_range *bkrange)
{
    char *delimiter = strstr(str, "..");
    if (delimiter != NULL) { /* range */
        char *nxt = delimiter + 2;
        *delimiter = '\0';
        if (strncmp(str, "0x", 2) == 0 && strncmp(nxt, "0x", 2) == 0) {
            if (! safe_strtohexa(str+2, bkrange->from_bkey, MAX_BKEY_LENG) ||
                ! safe_strtohexa(nxt+2, bkrange->to_bkey,   MAX_BKEY_LENG)) {
                *delimiter = '.'; return -1;
            }
            bkrange->from_nbkey = strlen(str+2)/2;
            bkrange->to_nbkey   = strlen(nxt+2)/2;
        } else {
            if (! safe_strtoull(str, (uint64_t*)bkrange->from_bkey) ||
                ! safe_strtoull(nxt, (uint64_t*)bkrange->to_bkey)) {
                *delimiter = '.'; return -1;
            }
            bkrange->from_nbkey = bkrange->to_nbkey = 0;
        }
        *delimiter = '.';
    } else { /* single index */
        if (strncmp(str, "0x", 2) == 0) { /* hexadeciaml bkey */
            if (! safe_strtohexa(str+2, bkrange->from_bkey, MAX_BKEY_LENG))
                return -1;
            bkrange->from_nbkey = strlen(str+2)/2;
        } else { /* 64 bit unsigned integer */
            if (! safe_strtoull(str, (uint64_t*)bkrange->from_bkey))
                return -1;
            bkrange->from_nbkey = 0;
        }
        bkrange->to_nbkey = BKEY_NULL;
    }
    return 0;
}

static inline int get_position_range_from_str(const char *str, uint32_t *from_posi, uint32_t *to_posi)
{
    char *delimiter = strstr(str, "..");
    if (delimiter != NULL) { /* range */
        char *nxt = delimiter + 2;
        *delimiter = '\0';
        if (! safe_strtoul(str, from_posi) ||
            ! safe_strtoul(nxt, to_posi)) {
            *delimiter = '.'; return -1;
        }
        *delimiter = '.';
    } else { /* single postion */
        if (! safe_strtoul(str, from_posi))
            return -1;
        *to_posi = *from_posi;
    }
    return 0;
}

static inline ENGINE_COMPARE_OP get_compare_op_from_str(const char *str)
{
    if (strlen(str) == 2) {
        if      (str[0] == 'E' && str[1] == 'Q') return COMPARE_OP_EQ;
        else if (str[0] == 'N' && str[1] == 'E') return COMPARE_OP_NE;
        else if (str[0] == 'L') {
                         if      (str[1] == 'T') return COMPARE_OP_LT;
                         else if (str[1] == 'E') return COMPARE_OP_LE;
        }
        else if (str[0] == 'G') {
                         if      (str[1] == 'T') return COMPARE_OP_GT;
                         else if (str[1] == 'E') return COMPARE_OP_GE;
        }
    }
    return COMPARE_OP_MAX;
}

static inline ENGINE_BITWISE_OP get_bitwise_op_from_str(const char *str)
{
    if (strlen(str) == 1) {
        if      (str[0] == '&') return BITWISE_OP_AND;
        else if (str[0] == '|') return BITWISE_OP_OR;
        else if (str[0] == '^') return BITWISE_OP_XOR;
    }
    return BITWISE_OP_MAX;
}

static inline int get_efilter_from_tokens(token_t *tokens, const int ntokens, eflag_filter *efilter)
{
    int token_count = 0;

    /* check and build element eflag filter */
    if (ntokens >= 3 && strncmp(tokens[2].value, "0x", 2) == 0) {
        uint32_t offset;
        int      length;

        if (! safe_strtoul(tokens[0].value, &offset) || offset >= MAX_EFLAG_LENG) {
            return -1;
        }
        efilter->fwhere = (uint8_t)offset;
        token_count = 1;

        if (ntokens >= 5 && strncmp(tokens[4].value, "0x", 2) == 0) {
            efilter->bitwop = get_bitwise_op_from_str(tokens[token_count].value);
            if (efilter->bitwop == BITWISE_OP_MAX) {
                return -1;
            }
            length = get_eflag_from_str(tokens[token_count+1].value, efilter->bitwval);
            if (length < 0) {
                return -1;
            }
            efilter->nbitwval = (uint8_t)length;
            token_count += 2;
        } else {
            efilter->nbitwval = 0;
        }

        efilter->compop = get_compare_op_from_str(tokens[token_count].value);
        if (efilter->compop == COMPARE_OP_MAX) {
            return -1;
        }

        if (efilter->compop == COMPARE_OP_EQ || efilter->compop == COMPARE_OP_NE) {
            /* single value or multiple valeus(IN, NOT IN filter) */
            char *ptr = NULL;
            char *saveptr = NULL;

            efilter->compvcnt = 0;
            ptr = strtok_r(tokens[token_count+1].value, ",", &saveptr);
            while (ptr != NULL) {
                length = get_eflag_from_str(ptr, &efilter->compval[efilter->compvcnt*efilter->ncompval]);
                if (++efilter->compvcnt > MAX_EFLAG_COMPARE_COUNT) {
                    return -1;
                }
                if (efilter->compvcnt == 1) {
                    if (length < 0 || (offset+length) > MAX_EFLAG_LENG)
                        return -1;
                    if (efilter->nbitwval > 0 && length != (int)efilter->nbitwval)
                        return -1;
                    efilter->ncompval = (uint8_t)length;
                } else {
                    if (length != efilter->ncompval)
                        return -1;
                }
                ptr = strtok_r(NULL, ",", &saveptr);
            }
        } else {
            length = get_eflag_from_str(tokens[token_count+1].value, efilter->compval);
            if (length < 0 || (offset+length) > MAX_EFLAG_LENG) {
                return -1;
            }
            if (efilter->nbitwval > 0 && length != (int)efilter->nbitwval) {
                /* the lengths of bitwise operand and compare operand must be same. */
                return -1;
            }
            efilter->ncompval = (uint8_t)length;
            efilter->compvcnt = 1;
        }
        token_count += 2; /* token_count will be 3 or 5 */
    } else {
        efilter->ncompval = 0;
    }
    return token_count;
}

static void process_bop_update_prepare_nread(conn *c, int cmd,
                                             char *key, size_t nkey, const int vlen)
{
    assert(cmd == (int)OPERATION_BOP_UPDATE);
    eitem *elem = NULL;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (vlen > settings.max_element_bytes) {
        ret = ENGINE_E2BIG;
    } else {
        if ((elem = (eitem *)malloc(sizeof(value_item) + vlen)) == NULL)
            ret = ENGINE_ENOMEM;
        else
            ((value_item*)elem)->len = vlen;
    }
    if (ret == ENGINE_SUCCESS) {
        c->ritem   = ((value_item *)elem)->ptr;
        c->rlbytes = vlen;
        c->rltotal = 0;
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = cmd;
        conn_set_state(c, conn_nread);
    } else {
        if (settings.detail_enabled) {
            stats_prefix_record_bop_update(key, nkey, false);
        }
        STATS_CMD_NOKEY(c, bop_update);
        if (ret == ENGINE_E2BIG) out_string(c, "CLIENT_ERROR too large value");
        else                     out_string(c, "SERVER_ERROR out of memory");

        /* swallow the data line */
        c->sbytes = vlen;
        if (c->state == conn_write) {
            c->write_and_go = conn_swallow;
        } else { /* conn_new_cmd (by noreply) */
            conn_set_state(c, conn_swallow);
        }
    }
}

static void process_bop_prepare_nread(conn *c, int cmd, char *key, size_t nkey,
                                      const unsigned char *bkey, const int nbkey,
                                      const unsigned char *eflag, const int neflag,
                                      const int vlen)
{
    eitem *elem;
    ENGINE_ERROR_CODE ret;

    if (vlen > settings.max_element_bytes) {
        ret = ENGINE_E2BIG;
    } else {
        ret = mc_engine.v1->btree_elem_alloc(mc_engine.v0, c, key, nkey,
                                             nbkey, neflag, vlen, &elem);
    }
    if (ret == ENGINE_SUCCESS) {
        mc_engine.v1->get_elem_info(mc_engine.v0, c, ITEM_TYPE_BTREE, elem, &c->einfo);
        memcpy((void*)c->einfo.score, bkey, (c->einfo.nscore==0 ? sizeof(uint64_t) : c->einfo.nscore));
        if (c->einfo.neflag > 0)
            memcpy((void*)c->einfo.eflag, eflag, c->einfo.neflag);
        ritem_set_first(c, CONN_RTYPE_EINFO, vlen);
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 1;
        c->coll_op     = cmd; /* OPERATION_BOP_INSERT | OPERATION_BOP_UPSERT */
        conn_set_state(c, conn_nread);
    } else {
        if (settings.detail_enabled) {
            stats_prefix_record_bop_insert(key, nkey, false);
        }
        STATS_CMD_NOKEY(c, bop_insert);
        if (ret == ENGINE_E2BIG)       out_string(c, "CLIENT_ERROR too large value");
        else if (ret == ENGINE_ENOMEM) out_string(c, "SERVER_ERROR out of memory");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);

        if (ret != ENGINE_DISCONNECT) {
            /* swallow the data line */
            c->sbytes = vlen;
            if (c->state == conn_write) {
                c->write_and_go = conn_swallow;
            } else { /* conn_new_cmd (by noreply) */
                conn_set_state(c, conn_swallow);
            }
        }
    }
}

#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
static void process_bop_prepare_nread_keys(conn *c, int cmd, uint32_t vlen, uint32_t kcnt)
{
    eitem *elem = NULL;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    int need_size = 0;

#ifdef SUPPORT_BOP_MGET
    if (cmd == OPERATION_BOP_MGET) {
        int bmget_count = c->coll_numkeys * c->coll_rcount;
        int eresult_array_size = c->coll_numkeys * sizeof(struct elems_result);
        int respon_hdr_size = c->coll_numkeys * ((UINT32_STR_LENG*2)+30);
        int respon_bdy_size = bmget_count * ((MAX_BKEY_LENG*2+2)+(MAX_EFLAG_LENG*2+2)+UINT32_STR_LENG+15);

        need_size = eresult_array_size + respon_hdr_size + respon_bdy_size;
    }
#endif
#ifdef SUPPORT_BOP_SMGET
    if (cmd == OPERATION_BOP_SMGET) {
#ifdef JHPARK_OLD_SMGET_INTERFACE
      if (c->coll_smgmode == 0) {
        int smget_count = c->coll_roffset + c->coll_rcount;
        int elem_array_size; /* elem pointer array where the found elements will be saved */
        int kmis_array_size; /* key index array where the missed key indexes are to be saved */
        int respon_hdr_size; /* the size of response head and tail */
        int respon_bdy_size; /* the size of response body */

        elem_array_size = smget_count * (sizeof(eitem*) + (2*sizeof(uint32_t)));
        kmis_array_size = c->coll_numkeys * sizeof(uint32_t);
        respon_hdr_size = (2*UINT32_STR_LENG) + 30; /* result head and tail size */
        respon_bdy_size = smget_count * ((MAX_BKEY_LENG*2+2)+(MAX_EFLAG_LENG*2+2)+(UINT32_STR_LENG*2)+5); /* result body size */

        need_size = elem_array_size + kmis_array_size + respon_hdr_size + respon_bdy_size;
      } else
#endif
      {
        int elem_array_size; /* smget element array size */
        int ehit_array_size; /* smget hitted elem array size */
        int emis_array_size; /* element missed keys array size */
        int respon_hdr_size; /* the size of response head and tail */
        int respon_bdy_size; /* the size of response body */

        elem_array_size = (c->coll_rcount + c->coll_numkeys) * sizeof(eitem*);
        ehit_array_size = c->coll_rcount * sizeof(smget_ehit_t);
        emis_array_size = c->coll_numkeys * sizeof(smget_emis_t);
        respon_hdr_size = (3*UINT32_STR_LENG) + 50; /* result head and tail size */
        respon_bdy_size = (c->coll_rcount * ((MAX_BKEY_LENG*2+2)+(MAX_EFLAG_LENG*2+2)+(UINT32_STR_LENG*2)+10))
                        + (c->coll_numkeys * ((MAX_EFLAG_LENG*2+2) + 5)); /* result body size */
        need_size = elem_array_size + ehit_array_size + emis_array_size
                  + respon_hdr_size + respon_bdy_size;
     }
    }
#endif
    assert(need_size > 0);

    if ((elem = (eitem *)malloc(need_size)) == NULL) {
        ret = ENGINE_ENOMEM;
    } else {
        /* allocate memory blocks needed */
        if (mblck_list_alloc(&c->thread->mblck_pool, 1, vlen, &c->memblist) < 0) {
            free((void*)elem);
            ret = ENGINE_ENOMEM;
        }
    }
    if (ret == ENGINE_SUCCESS) {
        c->coll_strkeys = (void*)&c->memblist;
        ritem_set_first(c, CONN_RTYPE_MBLCK, vlen);
        c->coll_eitem  = (void *)elem;
        c->coll_ecount = 0;
        c->coll_op = cmd;
        conn_set_state(c, conn_nread);
    } else {
#ifdef SUPPORT_BOP_MGET
        if (cmd == OPERATION_BOP_MGET)
            STATS_CMD_NOKEY(c, bop_mget);
#endif
#ifdef SUPPORT_BOP_SMGET
        if (cmd == OPERATION_BOP_SMGET)
            STATS_CMD_NOKEY(c, bop_smget);
#endif
        /* ret == ENGINE_ENOMEM */
        out_string(c, "SERVER_ERROR out of memory");

        /* swallow the data line */
        assert(c->state == conn_write);
        c->sbytes = vlen;
        c->write_and_go = conn_swallow;
    }
}
#endif

static void process_bop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *key = tokens[BOP_KEY_TOKEN].value;
    size_t nkey = tokens[BOP_KEY_TOKEN].length;
    int subcommid;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    c->coll_key = key;
    c->coll_nkey = nkey;

    if ((ntokens >= 6 && ntokens <= 14) &&
        ((strcmp(subcommand,"insert") == 0 && (subcommid = (int)OPERATION_BOP_INSERT)) ||
         (strcmp(subcommand,"upsert") == 0 && (subcommid = (int)OPERATION_BOP_UPSERT)) ))
    {
        unsigned char bkey[MAX_BKEY_LENG];
        unsigned char eflag[MAX_EFLAG_LENG];
        int      nbkey, neflag;
        int32_t  vlen;
        int      read_ntokens = BOP_KEY_TOKEN+1;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        c->coll_getrim = false;
        if (c->noreply == false) {
            if (strcmp(tokens[ntokens-2].value, "getrim") == 0) {
                /* The getrim flag in bop insert/upsert command
                 * If an element is trimmed by maxcount overflow,
                 * the trimmed element must be gotten by clients.
                 */
                c->coll_getrim = true;
            }
        }

        nbkey = get_bkey_from_str(tokens[read_ntokens++].value, bkey);
        neflag = 0;
        if (ntokens > 6 && strncmp(tokens[read_ntokens].value, "0x", 2) == 0) {
            neflag = get_eflag_from_str(tokens[read_ntokens++].value, eflag);
        }
        if (nbkey == -1 || neflag == -1) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if ((! safe_strtol(tokens[read_ntokens++].value, &vlen)) ||
            (vlen < 0 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        vlen += 2;

        int post_ntokens = 1 + ((c->noreply || c->coll_getrim) ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 2) {
            if (strcmp(tokens[read_ntokens].value, "create") != 0 ||
                get_coll_create_attr_from_tokens(&tokens[read_ntokens+1], rest_ntokens-1,
                                                 ITEM_TYPE_BTREE, &c->coll_attr_space) != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = &c->coll_attr_space; /* create if not exist */
        } else {
            if (rest_ntokens != 0) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            c->coll_attrp = NULL;
        }

        if (check_and_handle_pipe_state(c)) {
            process_bop_prepare_nread(c, subcommid, key, nkey, bkey, nbkey, eflag, neflag, vlen);
        } else { /* pipe error */
            c->sbytes = vlen;
            conn_set_state(c, conn_swallow);
        }
    }
    else if ((ntokens >= 7 && ntokens <= 10) && (strcmp(subcommand, "create") == 0))
    {
        set_noreply_maybe(c, tokens, ntokens);

        int read_ntokens = BOP_KEY_TOKEN+1;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (get_coll_create_attr_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                             ITEM_TYPE_BTREE, &c->coll_attr_space) != 0) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        c->coll_attrp = &c->coll_attr_space;
        process_bop_create(c, key, nkey, c->coll_attrp);
    }
    else if ((ntokens >= 6 && ntokens <= 10) && (strcmp(subcommand, "update") == 0))
    {
        int32_t  vlen;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        /* Only single bkey supported */
        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange) != 0 ||
            c->coll_bkrange.to_nbkey != BKEY_NULL) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = BOP_KEY_TOKEN + 2;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens > 1) {
            uint32_t offset;
            int      length;
            if (rest_ntokens > 2) {
                if (! safe_strtoul(tokens[read_ntokens].value, &offset) || offset >= MAX_EFLAG_LENG) {
                    print_invalid_command(c, tokens, ntokens);
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
                c->coll_eupdate.fwhere = (uint8_t)offset;
                c->coll_eupdate.bitwop = get_bitwise_op_from_str(tokens[read_ntokens+1].value);
                if (c->coll_eupdate.bitwop == BITWISE_OP_MAX) {
                    print_invalid_command(c, tokens, ntokens);
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
                read_ntokens += 2;
                rest_ntokens -= 2;
            } else {
                c->coll_eupdate.bitwop = BITWISE_OP_MAX;
            }
            if (strncmp(tokens[read_ntokens].value, "0x", 2) == 0) {
                length = get_eflag_from_str(tokens[read_ntokens].value, c->coll_eupdate.eflag);
                if (length < 0) {
                    print_invalid_command(c, tokens, ntokens);
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            } else {
                if (! safe_strtol(tokens[read_ntokens].value, &length) || length != 0) {
                    print_invalid_command(c, tokens, ntokens);
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            }
            c->coll_eupdate.neflag = (uint8_t)length;
            read_ntokens += 1;
            rest_ntokens -= 1;
        } else {
            c->coll_eupdate.neflag = EFLAG_NULL;
        }

        if ((rest_ntokens != 1) ||
            (! safe_strtol(tokens[read_ntokens].value, &vlen)) ||
            (vlen < -1 || vlen > (INT_MAX-2))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (vlen == -1) {
            if (check_and_handle_pipe_state(c)) {
                if (c->coll_eupdate.neflag == EFLAG_NULL) {
                    /* Nothing to update */
                    //out_string(c, "CLIENT_ERROR nothing to update");
                    out_string(c, "NOTHING_TO_UPDATE");
                    return;
                }
                c->coll_op   = OPERATION_BOP_UPDATE;
                process_bop_update_complete(c);
            } else { /* pipe error */
                conn_set_state(c, conn_new_cmd);
            }
        } else { /* vlen >= 0 */
            vlen += 2;
            if (check_and_handle_pipe_state(c)) {
                process_bop_update_prepare_nread(c, (int)OPERATION_BOP_UPDATE, key, nkey, vlen);
            } else { /* pipe error */
                c->sbytes = vlen;
                conn_set_state(c, conn_swallow);
            }
        }
    }
    else if ((ntokens >= 5 && ntokens <= 13) && (strcmp(subcommand, "delete") == 0))
    {
        uint32_t count = 0;
        bool     drop_if_empty = false;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = BOP_KEY_TOKEN + 2;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 3) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens += used_ntokens;
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if (rest_ntokens > 0) {
            if (strcmp(tokens[read_ntokens+rest_ntokens-1].value, "drop")==0) {
                drop_if_empty = true;
                rest_ntokens -= 1;
            }
        }

        if (rest_ntokens > 0) {
            if ((rest_ntokens > 1) ||
                (! safe_strtoul(tokens[read_ntokens].value, &count))) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        if (check_and_handle_pipe_state(c)) {
            process_bop_delete(c, key, nkey, &c->coll_bkrange,
                               (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                               count, drop_if_empty);
        } else { /* pipe error */
            conn_set_state(c, conn_new_cmd);
        }
    }
    else if ((ntokens >= 6 && ntokens <= 9) && (strcmp(subcommand, "incr") == 0 || strcmp(subcommand, "decr") == 0))
    {
        uint64_t delta;
        uint64_t initial = 0;
        bool     incr = (strcmp(subcommand, "incr") == 0 ? true : false);
        bool     create = false;;
        eflag_t  eflagspc;
        eflag_t *eflagptr = NULL;

        set_pipe_noreply_maybe(c, tokens, ntokens);

        if ((get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange) != 0) ||
            (c->coll_bkrange.to_nbkey != BKEY_NULL) ||
            (! safe_strtoull(tokens[BOP_KEY_TOKEN+2].value, &delta)) || (delta < 1)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = BOP_KEY_TOKEN + 3;
        int post_ntokens = 1 + (c->noreply ? 1 : 0);
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens > 0) {
            if (! safe_strtoull(tokens[BOP_KEY_TOKEN+3].value, &initial)) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            if (rest_ntokens > 1) {
                int neflag = get_eflag_from_str(tokens[BOP_KEY_TOKEN+4].value, eflagspc.val);
                if (neflag == -1) {
                    print_invalid_command(c, tokens, ntokens);
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
                eflagspc.len = neflag;
                eflagptr = &eflagspc;
            }
            create = true;
        }

        if (check_and_handle_pipe_state(c)) {
            process_bop_arithmetic(c, key, nkey, &c->coll_bkrange, incr,
                                   create, delta, initial, eflagptr);
        } else { /* pipe error */
            conn_set_state(c, conn_new_cmd);
        }
    }
    else if ((ntokens >= 5 && ntokens <= 13) && (strcmp(subcommand, "get") == 0))
    {
        uint32_t offset = 0;
        uint32_t count  = 0;
        bool delete = false;
        bool drop_if_empty = false;

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = BOP_KEY_TOKEN + 2;
        int post_ntokens = 1; /* "\r\n" */
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 3 && strncmp(tokens[read_ntokens+2].value, "0x", 2)==0) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens += used_ntokens;
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if (rest_ntokens > 0) {
            if (strcmp(tokens[read_ntokens+rest_ntokens-1].value, "delete")==0 ||
                strcmp(tokens[read_ntokens+rest_ntokens-1].value, "drop")==0) {
                delete = true;
                if (strlen(tokens[read_ntokens+rest_ntokens-1].value) == 4)
                    drop_if_empty = true;
                rest_ntokens -= 1;
            }
        }

        if (rest_ntokens > 0) {
            if (rest_ntokens == 1) {
                if (! safe_strtoul(tokens[read_ntokens].value, &count)) {
                    print_invalid_command(c, tokens, ntokens);
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            } else if (rest_ntokens == 2) {
                if ((! safe_strtoul(tokens[read_ntokens].value, &offset)) ||
                    (! safe_strtoul(tokens[read_ntokens+1].value, &count))) {
                    print_invalid_command(c, tokens, ntokens);
                    out_string(c, "CLIENT_ERROR bad command line format");
                    return;
                }
            } else {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }

        process_bop_get(c, key, nkey, &c->coll_bkrange,
                        (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter),
                        offset, count,
                        delete, drop_if_empty);
    }
    else if ((ntokens >= 5 && ntokens <= 10) && (strcmp(subcommand, "count") == 0))
    {
        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = BOP_KEY_TOKEN + 2;
        int post_ntokens = 1; /* "\r\n" */
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens >= 3) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if (rest_ntokens != 0) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_bop_count(c, key, nkey, &c->coll_bkrange,
                          (c->coll_efilter.ncompval==0 ? NULL : &c->coll_efilter));
    }
#if defined(SUPPORT_BOP_MGET) || defined(SUPPORT_BOP_SMGET)
    else if ((ntokens >= 7 && ntokens <= 14) &&
             ((strcmp(subcommand, "mget") == 0  && (subcommid = (int)OPERATION_BOP_MGET)) ||
              (strcmp(subcommand, "smget") == 0 && (subcommid = (int)OPERATION_BOP_SMGET)) ))
    {
        uint32_t count, offset = 0;
        uint32_t lenkeys, numkeys;
#ifdef JHPARK_OLD_SMGET_INTERFACE
        int smgmode = subcommid == OPERATION_BOP_SMGET ? set_smget_mode_maybe(c, tokens, ntokens) : 0;
#else
        bool unique = subcommid == OPERATION_BOP_SMGET ? set_unique_maybe(c, tokens, ntokens) : false;
#endif

        if ((! safe_strtoul(tokens[BOP_KEY_TOKEN].value, &lenkeys)) ||
            (! safe_strtoul(tokens[BOP_KEY_TOKEN+1].value, &numkeys)) ||
            (lenkeys > (UINT_MAX-2)) || (lenkeys == 0) || (numkeys == 0)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+2].value, &c->coll_bkrange)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        int read_ntokens = BOP_KEY_TOKEN + 3;
        int post_ntokens = 1; /* "\r\n" */
#ifdef JHPARK_OLD_SMGET_INTERFACE
        if (smgmode > 0) {
            post_ntokens += 1;
        }
#else
        if (unique) {
            post_ntokens += 1;
        }
#endif
        int rest_ntokens = ntokens - read_ntokens - post_ntokens;

        if (rest_ntokens > 3) {
            int used_ntokens = get_efilter_from_tokens(&tokens[read_ntokens], rest_ntokens,
                                                       &c->coll_efilter);
            if (used_ntokens == -1) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            read_ntokens += used_ntokens;
            rest_ntokens -= used_ntokens;
        } else {
            c->coll_efilter.ncompval = 0;
        }

        if ((rest_ntokens < 1) || (rest_ntokens > 2) ||
            (rest_ntokens > 1 && !safe_strtoul(tokens[read_ntokens++].value, &offset)) ||
            (rest_ntokens > 0 && !safe_strtoul(tokens[read_ntokens++].value, &count))) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        /* validation checking on arguments */
        if (numkeys > ((lenkeys/2) + 1) ||
            lenkeys > ((numkeys*KEY_MAX_LENGTH) + numkeys-1) ||
            count == 0) {
            /* ENGINE_EBADVALUE */
            out_string(c, "CLIENT_ERROR bad value");
            c->sbytes = lenkeys + 2;
            c->write_and_go = conn_swallow;
            return;
        }
        lenkeys += 2;
#ifdef SUPPORT_BOP_MGET
        if (subcommid == OPERATION_BOP_MGET) {
            if (numkeys > MAX_BMGET_KEY_COUNT || count > MAX_BMGET_ELM_COUNT) {
                /* ENGINE_EBADVALUE */
                out_string(c, "CLIENT_ERROR bad value");
                c->sbytes = lenkeys;
                c->write_and_go = conn_swallow;
                return;
            }
        }
#endif
#ifdef SUPPORT_BOP_SMGET
        if (subcommid == OPERATION_BOP_SMGET) {
            if (numkeys > MAX_SMGET_KEY_COUNT || (offset+count) > MAX_SMGET_REQ_COUNT) {
                /* ENGINE_EBADVALUE */
                out_string(c, "CLIENT_ERROR bad value");
                c->sbytes = lenkeys;
                c->write_and_go = conn_swallow;
                return;
            }
        }
#endif

        c->coll_numkeys = numkeys;
        c->coll_lenkeys = lenkeys;
        c->coll_roffset = offset;
        c->coll_rcount  = count;
#ifdef JHPARK_OLD_SMGET_INTERFACE
        c->coll_smgmode = smgmode;
#else
        c->coll_unique  = unique;
#endif

        process_bop_prepare_nread_keys(c, subcommid, lenkeys, numkeys);
    }
#endif
    else if ((ntokens == 6) && (strcmp(subcommand, "position") == 0))
    {
        ENGINE_BTREE_ORDER order;

        if ((get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange) != 0) ||
            (c->coll_bkrange.to_nbkey != BKEY_NULL)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "asc") == 0) {
            order = BTREE_ORDER_ASC;
        } else if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "desc") == 0) {
            order = BTREE_ORDER_DESC;
        } else {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_bop_position(c, key, nkey, &c->coll_bkrange, order);
    }
    else if ((ntokens == 6 || ntokens == 7) && (strcmp(subcommand, "pwg") == 0))
    {
        ENGINE_BTREE_ORDER order;
        uint32_t count = 0;

        if ((get_bkey_range_from_str(tokens[BOP_KEY_TOKEN+1].value, &c->coll_bkrange) != 0) ||
            (c->coll_bkrange.to_nbkey != BKEY_NULL)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "asc") == 0) {
            order = BTREE_ORDER_ASC;
        } else if (strcmp(tokens[BOP_KEY_TOKEN+2].value, "desc") == 0) {
            order = BTREE_ORDER_DESC;
        } else {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (ntokens == 7) {
            if (! safe_strtoul(tokens[BOP_KEY_TOKEN+3].value, &count)) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            if (count > 100) { /* max limit on count: 100 */
                out_string(c, "CLIENT_ERROR too large count value");
                return;
            }
        }

        process_bop_pwg(c, key, nkey, &c->coll_bkrange, order, count);
    }
    else if ((ntokens == 6) && (strcmp(subcommand, "gbp") == 0))
    {
        uint32_t from_posi, to_posi;
        ENGINE_BTREE_ORDER order;

        if (strcmp(tokens[BOP_KEY_TOKEN+1].value, "asc") == 0) {
            order = BTREE_ORDER_ASC;
        } else if (strcmp(tokens[BOP_KEY_TOKEN+1].value, "desc") == 0) {
            order = BTREE_ORDER_DESC;
        } else {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (get_position_range_from_str(tokens[BOP_KEY_TOKEN+2].value, &from_posi, &to_posi)) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        process_bop_gbp(c, key, nkey, order, from_posi, to_posi);
    }
    else
    {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#endif

#if ATTR_PROCESS || 1
/** getattr */
static size_t attr_to_printable_buffer(char *ptr, ENGINE_ITEM_ATTR attr_id, item_attr *attr_datap)
{
    if (attr_id == ATTR_TYPE)
        sprintf(ptr, "ATTR type=%s\r\n", get_item_type_str(attr_datap->type));
    else if (attr_id == ATTR_FLAGS)
        sprintf(ptr, "ATTR flags=%u\r\n", htonl(attr_datap->flags));
    else if (attr_id == ATTR_EXPIRETIME)
        sprintf(ptr, "ATTR expiretime=%d\r\n", (int32_t)human_readable_time(attr_datap->exptime));
    else if (attr_id == ATTR_COUNT)
        sprintf(ptr, "ATTR count=%d\r\n", attr_datap->count);
    else if (attr_id == ATTR_MAXCOUNT)
        sprintf(ptr, "ATTR maxcount=%d\r\n", attr_datap->maxcount);
    else if (attr_id == ATTR_OVFLACTION)
        sprintf(ptr, "ATTR overflowaction=%s\r\n", get_ovflaction_str(attr_datap->ovflaction));
    else if (attr_id == ATTR_READABLE)
        sprintf(ptr, "ATTR readable=%s\r\n", (attr_datap->readable ? "on" : "off"));
    else if (attr_id == ATTR_MAXBKEYRANGE) {
        if (attr_datap->maxbkeyrange.len == BKEY_NULL) {
            sprintf(ptr, "ATTR maxbkeyrange=0\r\n");
        } else {
            if (attr_datap->maxbkeyrange.len == 0) {
                uint64_t bkey_temp;
                memcpy((unsigned char*)&bkey_temp, attr_datap->maxbkeyrange.val, sizeof(uint64_t));
                sprintf(ptr, "ATTR maxbkeyrange=%"PRIu64"\r\n", bkey_temp);
                //sprintf(ptr, "ATTR maxbkeyrange=%"PRIu64"\r\n", *(uint64_t*)attr_datap->maxbkeyrange.val);
            } else {
                char *ptr_temp = ptr;
                sprintf(ptr_temp, "ATTR maxbkeyrange=0x");
                ptr_temp += strlen(ptr_temp);
                safe_hexatostr(attr_datap->maxbkeyrange.val, attr_datap->maxbkeyrange.len, ptr_temp);
                ptr_temp += strlen(ptr_temp);
                sprintf(ptr_temp, "\r\n");
            }
        }
    }
    else if (attr_id == ATTR_MINBKEY) {
        if (attr_datap->count > 0) {
            if (attr_datap->minbkey.len == 0) {
                uint64_t bkey_temp;
                memcpy((unsigned char*)&bkey_temp, attr_datap->minbkey.val, sizeof(uint64_t));
                sprintf(ptr, "ATTR minbkey=%"PRIu64"\r\n", bkey_temp);
                //sprintf(ptr, "ATTR minbkey=%"PRIu64"\r\n", *(uint64_t*)attr_datap->minbkey.val);
            } else {
                char *ptr_temp = ptr;
                sprintf(ptr_temp, "ATTR minbkey=0x");
                ptr_temp += strlen(ptr_temp);
                safe_hexatostr(attr_datap->minbkey.val, attr_datap->minbkey.len, ptr_temp);
                ptr_temp += strlen(ptr_temp);
                sprintf(ptr_temp, "\r\n");
            }
        } else {
            sprintf(ptr, "ATTR minbkey=-1\r\n");
        }
    }
    else if (attr_id == ATTR_MAXBKEY) {
        if (attr_datap->count > 0) {
            if (attr_datap->maxbkey.len == 0) {
                uint64_t bkey_temp;
                memcpy((unsigned char*)&bkey_temp, attr_datap->maxbkey.val, sizeof(uint64_t));
                sprintf(ptr, "ATTR maxbkey=%"PRIu64"\r\n", bkey_temp);
                //sprintf(ptr, "ATTR maxbkey=%"PRIu64"\r\n", *(uint64_t*)attr_datap->maxbkey.val);
            } else {
                char *ptr_temp = ptr;
                sprintf(ptr_temp, "ATTR maxbkey=0x");
                ptr_temp += strlen(ptr_temp);
                safe_hexatostr(attr_datap->maxbkey.val, attr_datap->maxbkey.len, ptr_temp);
                ptr_temp += strlen(ptr_temp);
                sprintf(ptr_temp, "\r\n");
            }
        } else {
            sprintf(ptr, "ATTR maxbkey=-1\r\n");
        }
    }
    else if (attr_id == ATTR_TRIMMED)
        sprintf(ptr, "ATTR trimmed=%u\r\n", (attr_datap->trimmed != 0 ? 1 : 0));

    return strlen(ptr);
}

static void process_getattr_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char   *key = tokens[KEY_TOKEN].value;
    size_t nkey = tokens[KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    item_attr attr_data;
    ENGINE_ITEM_ATTR attr_ids[ATTR_END];
    uint32_t attr_count = 0;
    int i;

    if (ntokens > 3) {
        for (i = KEY_TOKEN+1; i < ntokens-1; i++) {
            char *name = tokens[i].value;
            if (strcmp(name, "flags")==0)               attr_ids[attr_count++] = ATTR_FLAGS;
            else if (strcmp(name, "expiretime")==0)     attr_ids[attr_count++] = ATTR_EXPIRETIME;
            else if (strcmp(name, "type")==0)           attr_ids[attr_count++] = ATTR_TYPE;
            else if (strcmp(name, "count")==0)          attr_ids[attr_count++] = ATTR_COUNT;
            else if (strcmp(name, "maxcount")==0)       attr_ids[attr_count++] = ATTR_MAXCOUNT;
            else if (strcmp(name, "overflowaction")==0) attr_ids[attr_count++] = ATTR_OVFLACTION;
            else if (strcmp(name, "readable")==0)       attr_ids[attr_count++] = ATTR_READABLE;
            else if (strcmp(name, "maxbkeyrange")==0)   attr_ids[attr_count++] = ATTR_MAXBKEYRANGE;
            else if (strcmp(name, "minbkey")==0)        attr_ids[attr_count++] = ATTR_MINBKEY;
            else if (strcmp(name, "maxbkey")==0)        attr_ids[attr_count++] = ATTR_MAXBKEY;
            else if (strcmp(name, "trimmed")==0)        attr_ids[attr_count++] = ATTR_TRIMMED;
            else {
                ret = ENGINE_EBADATTR; break;
            }
        }
    }

    if (ret == ENGINE_SUCCESS) {
        ret = mc_engine.v1->getattr(mc_engine.v0, c, key, nkey,
                                    attr_ids, attr_count, &attr_data, 0);
        if (settings.detail_enabled) {
            stats_prefix_record_getattr(key, nkey);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        {
        char buffer[1024];
        char *str = &buffer[0];
        char *ptr = str; *ptr = '\0';

        STATS_HITS(c, getattr, key, nkey);

        if (attr_count > 0) {
            for (i = 0; i < attr_count; i++) {
                ptr += attr_to_printable_buffer(ptr, attr_ids[i], &attr_data);
            }
        } else { /* attr_count == 0 */
            ptr += attr_to_printable_buffer(ptr, ATTR_TYPE, &attr_data);
            ptr += attr_to_printable_buffer(ptr, ATTR_FLAGS, &attr_data);
            ptr += attr_to_printable_buffer(ptr, ATTR_EXPIRETIME, &attr_data);
            if (attr_data.type != ITEM_TYPE_KV) { /* collection_item */
                ptr += attr_to_printable_buffer(ptr, ATTR_COUNT, &attr_data);
                ptr += attr_to_printable_buffer(ptr, ATTR_MAXCOUNT, &attr_data);
                ptr += attr_to_printable_buffer(ptr, ATTR_OVFLACTION, &attr_data);
                ptr += attr_to_printable_buffer(ptr, ATTR_READABLE, &attr_data);
            }
            if (attr_data.type == ITEM_TYPE_BTREE) {
                ptr += attr_to_printable_buffer(ptr, ATTR_MAXBKEYRANGE, &attr_data);
                ptr += attr_to_printable_buffer(ptr, ATTR_MINBKEY, &attr_data);
                ptr += attr_to_printable_buffer(ptr, ATTR_MAXBKEY, &attr_data);
                ptr += attr_to_printable_buffer(ptr, ATTR_TRIMMED, &attr_data);
            }
        }
        sprintf(ptr, "END");
        out_string(c, str);
        }
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISSES(c, getattr, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_CMD_NOKEY(c, getattr);
        if (ret == ENGINE_EBADATTR) out_string(c, "ATTR_ERROR not found");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** setattr */
static void process_setattr_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    assert(c->ewouldblock == false);
    char *key = tokens[KEY_TOKEN].value;
    size_t nkey = tokens[KEY_TOKEN].length;

    if (nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    item_attr attr_data;
    ENGINE_ITEM_ATTR attr_ids[ATTR_END];
    uint32_t attr_count = 0;
    int i;
    char *name, *value, *equal;

    for (i = KEY_TOKEN+1; i < ntokens-1; i++) {
        if ((equal = strchr(tokens[i].value, '=')) == NULL) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        *equal = '\0';
        name = tokens[i].value; value = equal + 1;

        if (strcmp(name, "expiretime")==0) {
            int64_t exptime;
            attr_ids[attr_count++] = ATTR_EXPIRETIME;
            if (! safe_strtoll(value, &exptime)) {
                ret = ENGINE_EBADVALUE;
                break;
            }
            attr_data.exptime = realtime(exptime);
        } else if (strcmp(name, "maxcount")==0) {
            attr_ids[attr_count++] = ATTR_MAXCOUNT;
            if (! safe_strtol(value, &attr_data.maxcount)) {
                ret = ENGINE_EBADVALUE;
                break;
            }
        } else if (strcmp(name, "overflowaction")==0) {
            attr_ids[attr_count++] = ATTR_OVFLACTION;
            if (strcmp(value, "error")==0)
                attr_data.ovflaction = OVFL_ERROR;
            else if (strcmp(value, "head_trim")==0)
                attr_data.ovflaction = OVFL_HEAD_TRIM;
            else if (strcmp(value, "tail_trim")==0)
                attr_data.ovflaction = OVFL_TAIL_TRIM;
            else if (strcmp(value, "smallest_trim")==0)
                attr_data.ovflaction = OVFL_SMALLEST_TRIM;
            else if (strcmp(value, "largest_trim")==0)
                attr_data.ovflaction = OVFL_LARGEST_TRIM;
            else if (strcmp(value, "smallest_silent_trim")==0)
                attr_data.ovflaction = OVFL_SMALLEST_SILENT_TRIM;
            else if (strcmp(value, "largest_silent_trim")==0)
                attr_data.ovflaction = OVFL_LARGEST_SILENT_TRIM;
            else {
                ret = ENGINE_EBADVALUE;
                break;
            }
        } else if (strcmp(name, "readable")==0) {
            attr_ids[attr_count++] = ATTR_READABLE;
            if (strcmp(value, "on")==0)
                attr_data.readable = 1;
            else {
                ret = ENGINE_EBADVALUE;
                break;
            }
        } else if (strcmp(name, "maxbkeyrange")==0) {
            int length;
            length = get_bkey_from_str(value, attr_data.maxbkeyrange.val);
            if (length == -1) {
                ret = ENGINE_EBADVALUE;
                break;
            }
            attr_data.maxbkeyrange.len = (uint8_t)length;
            if (attr_data.maxbkeyrange.len == 0) {
                uint64_t bkey_temp;
                memcpy((unsigned char*)&bkey_temp, attr_data.maxbkeyrange.val, sizeof(uint64_t));
                if (bkey_temp == 0) attr_data.maxbkeyrange.len = BKEY_NULL; /* reset maxbkeyrange */
            }
            //if (attr_data.maxbkeyrange.len == 0 && *(uint64_t*)attr_data.maxbkeyrange.val == 0)
            //    attr_data.maxbkeyrange.len = BKEY_NULL; /* reset maxbkeyrange */
            attr_ids[attr_count++] = ATTR_MAXBKEYRANGE;
        } else {
            break;
        }
    }
    if (i < ntokens-1 && ret == ENGINE_SUCCESS) {
        ret = ENGINE_EBADATTR;
    }

    if (ret == ENGINE_SUCCESS) {
        ret = mc_engine.v1->setattr(mc_engine.v0, c, key, nkey,
                                    attr_ids, attr_count, &attr_data, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
        if (settings.detail_enabled) {
            stats_prefix_record_setattr(key, nkey);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, setattr, key, nkey);
        out_string(c, "OK");
        break;
    case ENGINE_KEY_ENOENT:
        STATS_MISSES(c, setattr, key, nkey);
        out_string(c, "NOT_FOUND");
        break;
    default:
        STATS_CMD_NOKEY(c, setattr);
        if (ret == ENGINE_EBADATTR)       out_string(c, "ATTR_ERROR not found");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "ATTR_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

#endif

#if PIPE_PROCESS || 1
static int pipe_response_save(conn *c, const char *str, size_t len)
{
    if (c->pipe_state == PIPE_STATE_ON) {
        if (c->pipe_count == 0) {
            /* skip the memory of response head string: "RESPONSE %d\r\n" */
            c->pipe_reslen = PIPE_RES_HEAD_SIZE;
            c->pipe_resptr = &c->pipe_response[c->pipe_reslen];
        }
        if ((c->pipe_reslen + (len+2)) < (PIPE_RES_MAX_SIZE-PIPE_RES_TAIL_SIZE)) {
            sprintf(c->pipe_resptr, "%s\r\n", str);
            c->pipe_reslen += (len+2);
            c->pipe_resptr = &c->pipe_response[c->pipe_reslen];
            c->pipe_count++;
            if (c->pipe_count >= PIPE_CMD_MAX_COUNT && c->noreply == true) {
                /* c->noreply == true: There are remaining pipe operations. */
                c->pipe_state = PIPE_STATE_ERR_CFULL; /* pipe count overflow */
                return -1;
            }
            else if ((strncmp(str, "CLIENT_ERROR", 12) == 0) ||
                     (strncmp(str, "SERVER_ERROR", 12) == 0) ||
                     (strncmp(str, "ERROR", 5) == 0)) { /* severe error */
                c->pipe_state = PIPE_STATE_ERR_BAD; /* bad error in pipelining */
                return -1;
            }
        } else {
            c->pipe_state = PIPE_STATE_ERR_MFULL; /* pipe memory overflow */
            return -1;
        }
    } else {
        /* A response message has come here before pipe error is reset.
         * Maybe, clients may not send all the commands of the pipelining.
         * So, force to reset the current pipelining.
         */
        mc_logger->log(EXTENSION_LOG_INFO, c,
                       "%d: response message before pipe error is reset. %s\n",
                       c->sfd, str);
        pipe_state_clear(c);
    }
    return 0;
}

static void pipe_response_done(conn *c, bool end_of_pipelining)
{
    char headbuf[PIPE_RES_HEAD_SIZE];
    int headlen;
    int headidx;

    /* pipe response head string */
    headlen = sprintf(headbuf, "RESPONSE %d\r\n", c->pipe_count);
    assert(headlen > 0 && headlen <= PIPE_RES_HEAD_SIZE);
    headidx = PIPE_RES_HEAD_SIZE - headlen;
    memcpy(&c->pipe_response[headidx], headbuf, headlen);

    /* pipe response tail string */
    if (c->pipe_state == PIPE_STATE_ON) {
        sprintf(c->pipe_resptr, "END\r\n");
        c->pipe_reslen += 5;

        pipe_state_clear(c);
    } else {
        if (c->pipe_state == PIPE_STATE_ERR_CFULL) {
            sprintf(c->pipe_resptr, "PIPE_ERROR command overflow\r\n");
            c->pipe_reslen += 29;
        } else if (c->pipe_state == PIPE_STATE_ERR_MFULL) {
            sprintf(c->pipe_resptr, "PIPE_ERROR memory overflow\r\n");
            c->pipe_reslen += 28;
        } else { /* PIPE_STATE_ERR_BAD */
            sprintf(c->pipe_resptr, "PIPE_ERROR bad error\r\n");
            c->pipe_reslen += 22;
        }
        if (end_of_pipelining) {
            pipe_state_clear(c);
        }
        /* The pipe_state will be cleared
         * after swallowing the remaining data.
         */
    }

    c->wbytes = c->pipe_reslen - headidx;
    c->wcurr = &c->pipe_response[headidx];

    conn_set_state(c, conn_write);
    c->write_and_go = conn_new_cmd;
}

#endif

#if SCAN_PROCESS || 1
#ifdef SCAN_COMMAND
/* scan command limits */
#define SCAN_COUNT_MAX           2000
#define SCAN_CURSOR_MAX_LENGTH   31
/* pattern constraints is necessary to prevent string_pattern_match()
 * from taking a long time to compare string.
 */
#define SCAN_PATTERN_MAX_STARCNT 4
#define SCAN_PATTERN_MAX_LENGTH  64
static bool scan_validate_pattern(char *pattern)
{
    int starcnt = 0;
    int patlen = strlen(pattern);
    if (patlen <= 0 || patlen > SCAN_PATTERN_MAX_LENGTH) {
        return false;
    }
    int i;
    for (i = 0; i < patlen; i++) {
        if (pattern[i] == '\\') { /* backslash next is glob (*, ?, \\) ? */
            if (i == (patlen-1)) break; /* invalid, ex) 'aaa\', '\\\' */
            if (pattern[i+1] != '*' && pattern[i+1] != '?' && pattern[i+1] != '\\') break; /* invalid */
            i++; /* skip the next character */
        }
        else if (pattern[i] == '*') { /* except normal star(\*) */
            if (++starcnt > SCAN_PATTERN_MAX_STARCNT) break; /* invalid */
        }
    }
    if (i < patlen) {
        return false;
    }
    return true;
}

static bool scan_validate_type(char *type_str, ENGINE_ITEM_TYPE *ittype)
{
    if (strlen(type_str) != 1) {
        return false;
    }
    switch (type_str[0]) {
        case 'A':
            *ittype = ITEM_TYPE_MAX; /* all item type */
            break;
        case 'K':
            *ittype = ITEM_TYPE_KV;
            break;
        case 'L':
            *ittype = ITEM_TYPE_LIST;
            break;
        case 'S':
            *ittype = ITEM_TYPE_SET;
            break;
        case 'M':
            *ittype = ITEM_TYPE_MAP;
            break;
        case 'B':
            *ittype = ITEM_TYPE_BTREE;
            break;
        default:
            return false;
    }
    return true;
}

#define KEYSCAN_RESPONSE_HEADER_MAX_LENGTH SCAN_CURSOR_MAX_LENGTH + 24
#define KEYSCAN_RESPONSE_ATTR_MAX_LENGTH 18

#define PREFIXSCAN_RESPONSE_HEADER_MAX_LENGTH SCAN_CURSOR_MAX_LENGTH + 28
#define PREFIXSCAN_RESPONSE_ATTR_MAX_LENGTH 64

static void process_prefixscan_command(conn *c, token_t *tokens, const size_t ntokens)
{
    /* prefixscan command format : scan prefix <cursor> [count <count>] [match <pattern>] */
    char     cursor[SCAN_CURSOR_MAX_LENGTH+1];
    uint32_t count   = 20;
    char    *pattern = NULL;

    /* read <cursor> */
    if (tokens[2].length > SCAN_CURSOR_MAX_LENGTH) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad cursor value");
        return;
    }
    strcpy(cursor, tokens[2].value);

    /* read optional fields (<count>, <pattern>) */
    int i = 0;
    int optcnt = ntokens - 4; /* except (scan, prefix, <cursor>, CRLF) */
    char *err_response = NULL;
    while (optcnt >= 2) {
        char *optk = tokens[3+i].value;
        char *optv = tokens[4+i].value;
        if (strcmp(optk, "count") == 0) {
            if (!safe_strtoul(optv, &count) ||
                count == 0                  ||
                count > SCAN_COUNT_MAX) {
                err_response = "CLIENT_ERROR bad count value";
                break;
            }
        } else if (strcmp(optk, "match") == 0) {
            if (!scan_validate_pattern(optv)) {
                err_response = "CLIENT_ERROR bad pattern string";
                break;
            }
            pattern = optv;
        } else {
            break;
        }
        optcnt -= 2;
        i += 2;
    }
    if (err_response != NULL) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, err_response);
        return;
    }

    if (optcnt != 0) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    int need_size = count + 100; /* may scan more than count in engine. */
    if (need_size > c->isize) {
        int new_size = c->isize * 2;
        while (need_size > new_size) {
            new_size *= 2;
        }
        item **new_list = malloc(sizeof(item *) * new_size);
        if (new_list == NULL) {
            out_string(c, "SERVER_ERROR out of memory.");
            return;
        }
        free(c->ilist);
        c->ilist = new_list;
        c->isize = new_size;
    }

    int item_count = 0;
    /* call engine prefixscan API */
    ENGINE_ERROR_CODE ret = mc_engine.v1->prefixscan(cursor, count, pattern, c->ilist, c->isize, &item_count);
    if (ret == ENGINE_SUCCESS) {
        char *response = NULL;
        do {
            response = (char*)malloc(PREFIXSCAN_RESPONSE_HEADER_MAX_LENGTH +
                                     (PREFIXSCAN_RESPONSE_ATTR_MAX_LENGTH * item_count));
            if (response == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            char *header = response;
            sprintf(header, "PREFIXES %d %s\r\n", item_count, cursor);
            if (add_iov(c, header, strlen(header)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            char *attrptr = response + PREFIXSCAN_RESPONSE_HEADER_MAX_LENGTH;
            for (i = 0; i < item_count; i++) {
                item *it = (item *)c->ilist[i];
                prefix_info pinfo;
                if (!mc_engine.v1->get_prefix_info(mc_engine.v0, c, it, &pinfo)) {
                    ret = ENGINE_ENOMEM; break;
                }
                sprintf(attrptr, " %llu %llu %04d%02d%02d%02d%02d%02d\r\n",
                        (unsigned long long)pinfo.total_item_count, (unsigned long long)pinfo.total_item_bytes,
                        pinfo.create_time.tm_year+1900, pinfo.create_time.tm_mon+1, pinfo.create_time.tm_mday,
                        pinfo.create_time.tm_hour, pinfo.create_time.tm_min, pinfo.create_time.tm_sec);
                if (add_iov(c, pinfo.name, pinfo.name_length) != 0 ||
                    add_iov(c, attrptr, strlen(attrptr)) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
                attrptr += PREFIXSCAN_RESPONSE_ATTR_MAX_LENGTH;
            }
            if (ret != ENGINE_SUCCESS) break;
            if (add_iov(c, "END\r\n", 5) != 0 ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM;
            }
        } while (0);
        if (ret == ENGINE_SUCCESS) {
            c->pcurr = c->ilist;
            c->pleft = item_count;
            c->write_and_free = response;
        } else {
            for (i = 0; i < item_count; i++) {
                mc_engine.v1->prefix_release(mc_engine.v0, c, c->ilist[i]);
            }
            if (response != NULL) {
                free(response);
                response = NULL;
            }
        }
    }

    switch (ret) {
        case ENGINE_SUCCESS:
            conn_set_state(c, conn_mwrite);
            c->msgcurr = 0;
            break;
        case ENGINE_EINVAL:
            out_string(c, "CLIENT_ERROR invalid cursor.");
            break;
        case ENGINE_ENOMEM:
            out_string(c, "SERVER_ERROR out of memory.");
            break;
        default:
            handle_unexpected_errorcode_ascii(c, __func__, ret);
            break;
    }
}

static void process_keyscan_command(conn *c, token_t *tokens, const size_t ntokens)
{
    /* keyscan command format : scan key <cursor> [count <count>] [match <pattern>] [type <type>] */
    char     cursor[SCAN_CURSOR_MAX_LENGTH+1];
    uint32_t count   = 20;
    char    *pattern = NULL;
    ENGINE_ITEM_TYPE ittype = ITEM_TYPE_MAX; /* all item type */

    /* read <cursor> */
    if (tokens[2].length > SCAN_CURSOR_MAX_LENGTH) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad cursor value");
        return;
    }
    strcpy(cursor, tokens[2].value);

    /* read optional fields (<count>, <pattern>, <type>) */
    int i = 0;
    int optcnt = ntokens - 4; /* except (scan, key, <cursor>, CRLF) */
    char *err_response = NULL;
    while (optcnt >= 2) {
        char *optk = tokens[3+i].value;
        char *optv = tokens[4+i].value;
        if (strcmp(optk, "count") == 0) {
            if (!safe_strtoul(optv, &count) ||
                count == 0                  ||
                count > SCAN_COUNT_MAX) {
                err_response = "CLIENT_ERROR bad count value";
                break;
            }
        } else if (strcmp(optk, "match") == 0) {
            if (!scan_validate_pattern(optv)) {
                err_response = "CLIENT_ERROR bad pattern string";
                break;
            }
            pattern = optv;
        } else if (strcmp(optk, "type") == 0) {
            if (!scan_validate_type(optv, &ittype)) {
                err_response = "CLIENT_ERROR bad item type";
                break;
            }
        } else {
            break;
        }
        optcnt -= 2;
        i += 2;
    }
    if (err_response != NULL) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, err_response);
        return;
    }

    if (optcnt != 0) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    int need_size = count + 100; /* may scan more than count in engine. */
    if (need_size > c->isize) {
        int new_size = c->isize * 2;
        while (need_size > new_size) {
            new_size *= 2;
        }
        item **new_list = malloc(sizeof(item *) * new_size);
        if (new_list == NULL) {
            out_string(c, "SERVER_ERROR out of memory.");
            return;
        }
        free(c->ilist);
        c->ilist = new_list;
        c->isize = new_size;
    }

    int item_count = 0;
    /* call engine keyscan API */
    ENGINE_ERROR_CODE ret = mc_engine.v1->keyscan(cursor, count, pattern, ittype, c->ilist, c->isize, &item_count);
    if (ret == ENGINE_SUCCESS) {
        char *response = NULL;
        do {
            response = (char*)malloc(KEYSCAN_RESPONSE_HEADER_MAX_LENGTH +
                                     (KEYSCAN_RESPONSE_ATTR_MAX_LENGTH * item_count));
            if (response == NULL) {
                ret = ENGINE_ENOMEM; break;
            }
            char *header = response;
            sprintf(header, "KEYS %d %s\r\n", item_count, cursor);
            if (add_iov(c, header, strlen(header)) != 0) {
                ret = ENGINE_ENOMEM; break;
            }
            char *attrptr = response + KEYSCAN_RESPONSE_HEADER_MAX_LENGTH;
            for (i = 0; i < item_count; i++) {
                item *it = (item *)c->ilist[i];
                if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &c->hinfo)) {
                    ret = ENGINE_ENOMEM; break;
                }
                sprintf(attrptr, " %c %d\r\n",
                        get_item_type_char(c->hinfo.type),
                        human_readable_time(c->hinfo.exptime));
                if (add_iov(c, c->hinfo.key, c->hinfo.nkey) != 0 ||
                    add_iov(c, attrptr, strlen(attrptr)) != 0) {
                    ret = ENGINE_ENOMEM; break;
                }
                attrptr += KEYSCAN_RESPONSE_ATTR_MAX_LENGTH;
            }
            if (ret != ENGINE_SUCCESS) break;
            if (add_iov(c, "END\r\n", 5) != 0 ||
                (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
                ret = ENGINE_ENOMEM;
            }
        } while (0);
        if (ret == ENGINE_SUCCESS) {
            c->icurr = c->ilist;
            c->ileft = item_count;
            c->write_and_free = response;
        } else {
            c->ileft = 0;
            for (i = 0; i < item_count; i++) {
                mc_engine.v1->release(mc_engine.v0, c, c->ilist[i]);
            }
            if (response != NULL) {
                free(response);
                response = NULL;
            }
        }
    }

    switch (ret) {
        case ENGINE_SUCCESS:
            conn_set_state(c, conn_mwrite);
            c->msgcurr = 0;
            break;
        case ENGINE_EINVAL:
            out_string(c, "CLIENT_ERROR invalid cursor.");
            break;
        case ENGINE_ENOMEM:
            out_string(c, "SERVER_ERROR out of memory.");
            break;
        default:
            handle_unexpected_errorcode_ascii(c, __func__, ret);
            break;
    }
}

static void process_scan_command(conn *c, token_t *tokens, const size_t ntokens)
{
    /* keyscan command format : scan key <cursor> [count <count>] [match <pattern>] [type <type>] */
    if (strcmp(tokens[1].value, "key") == 0) {
        process_keyscan_command(c, tokens, ntokens);
        return;
    }

    /* prefixscan command format : scan prefix <cursor> [count <count>] [match <pattern>] */
    if (strcmp(tokens[1].value, "prefix") == 0) {
        process_prefixscan_command(c, tokens, ntokens);
        return;
    }

    print_invalid_command(c, tokens, ntokens);
    out_string(c, "CLIENT_ERROR bad command line format");
}
#endif
#endif

#if ADMIN_PROCESS || 1
/** flush */
static void process_flush_command(conn *c, token_t *tokens, const size_t ntokens, bool flush_all)
{
    assert(c->ewouldblock == false);
    int64_t exptime = 0; /* default delay value */
    bool delay_flag;
    ENGINE_ERROR_CODE ret;

    set_noreply_maybe(c, tokens, ntokens);

    if (flush_all) {
        /* flush_all [<delay>] [noreply]\r\n */
        delay_flag = (ntokens == (c->noreply ? 4 : 3));
    } else {
        /* flush_prefix <prefix> [<delay>] [noreply]\r\n */
        if (tokens[PREFIX_TOKEN].length > PREFIX_MAX_LENGTH) {
            out_string(c, "CLIENT_ERROR too long prefix name");
            return;
        }
        delay_flag = (ntokens == (c->noreply ? 5 : 4));
    }
    if (delay_flag) {
        int delay_idx = (flush_all ? 1 : 2);
        if (! safe_strtoll(tokens[delay_idx].value, &exptime) || exptime < 0) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    }

    if (flush_all) {
        ret = mc_engine.v1->flush(mc_engine.v0, c, NULL, -1, realtime(exptime));
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);

        STATS_CMD_NOKEY(c, flush);
        if (ret == ENGINE_SUCCESS) {
            out_string(c, "OK");
        } else {
            handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    } else { /* flush_prefix */
        char *prefix = tokens[PREFIX_TOKEN].value;
        int nprefix = tokens[PREFIX_TOKEN].length;
        if (nprefix == 6 && strncmp(prefix, "<null>", 6) == 0) {
            /* flush null prefix */
            prefix = NULL;
            nprefix = 0;
        }

        ret = mc_engine.v1->flush(mc_engine.v0, c, prefix, nprefix, realtime(exptime));
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);

        if (settings.detail_enabled) {
            if (ret == ENGINE_SUCCESS || ret == ENGINE_PREFIX_ENOENT) {
                if (stats_prefix_delete(prefix, nprefix) == 0) { /* found */
                    ret = ENGINE_SUCCESS;
                }
            }
        }

        STATS_CMD_NOKEY(c, flush_prefix);
        if (ret == ENGINE_SUCCESS) {
            out_string(c, "OK");
        } else {
            if (ret == ENGINE_PREFIX_ENOENT) out_string(c, "NOT_FOUND");
            else handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }
}

/** stats */
/**
 * Append a key-value pair to the stats output buffer. This function assumes
 * that the output buffer is big enough.
 */
static void append_ascii_stats(const char *key, const uint16_t klen,
                               const char *val, const uint32_t vlen,
                               const void *cookie)
{
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return;
    }

    conn *c = (conn*)cookie;
    size_t needed =  vlen + klen + 10; // 10 == "STAT = \r\n"
    if (!grow_dynamic_buffer(c, needed)) {
        return;
    }

    char *pos = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
    uint32_t nbytes = 5; /* "END\r\n" or "STAT " */

    if (klen == 0 && vlen == 0) {
        memcpy(pos, "END\r\n", 5);
    } else {
        memcpy(pos, "STAT ", 5);
        memcpy(pos + nbytes, key, klen);
        nbytes += klen;
        if (vlen != 0) {
            pos[nbytes] = ' ';
            ++nbytes;
            memcpy(pos + nbytes, val, vlen);
            nbytes += vlen;
        }
        memcpy(pos + nbytes, "\r\n", 2);
        nbytes += 2;
    }

    c->dynamic_buffer.offset += nbytes;
    assert(c->dynamic_buffer.offset <= c->dynamic_buffer.size);
}

inline static void process_stats_detail(conn *c, const char *command)
{
    assert(c != NULL);

    if (settings.allow_detailed) {
        if (strcmp(command, "on") == 0) {
            settings.detail_enabled = 1;
            out_string(c, "OK");
        }
        else if (strcmp(command, "off") == 0) {
            settings.detail_enabled = 0;
            out_string(c, "OK");
        }
        else if (strcmp(command, "dump") == 0) {
            int len;
            char *stats = stats_prefix_dump(NULL, 0, &len);
            if (stats == NULL) {
                out_string(c, "SERVER_ERROR no more memory");
                return;
            }
            write_and_free(c, stats, len);
        }
        else {
            out_string(c, "CLIENT_ERROR usage: stats detail on|off|dump");
        }
    } else {
        out_string(c, "CLIENT_ERROR detailed stats disabled");
    }
}

static void process_stats_cachedump(conn *c, token_t *tokens, const size_t ntokens)
{
    char *buf = NULL;
    unsigned int bytes = 0;
    unsigned int id;
    unsigned int limit = 0;
    bool forward = true;
    bool sticky = false;
    bool valid = false;

    do {
        if (ntokens < 5 || ntokens > 7) break;
        if (!safe_strtoul(tokens[2].value, &id)) break;
        if (!safe_strtoul(tokens[3].value, &limit)) break;
        if (ntokens >= 6) {
            if (strcmp(tokens[4].value, "forward")==0) forward = true;
            else if (strcmp(tokens[4].value, "backward")==0) forward = false;
            else break;
        }
        if (ntokens == 7) {
            if (strcmp(tokens[5].value, "sticky")==0) sticky = true;
            else break;
        }
        valid = true;
    } while(0);

    if (valid == false) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }
    if (id > POWER_LARGEST) {
        out_string(c, "CLIENT_ERROR Illegal slab id");
        return;
    }

    if (limit == 0)  limit = 50;
    if (limit > 200) limit = 200;

    buf = mc_engine.v1->cachedump(mc_engine.v0, c, id, limit,
                                  forward, sticky, &bytes);
    write_and_free(c, buf, bytes);
}

static void process_stats_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL && ntokens >= 2);
    const char *subcommand = tokens[SUBCOMMAND_TOKEN].value;

    if (ntokens == 2) {
        server_stats(&append_ascii_stats, c, false);
        (void)mc_engine.v1->get_stats(mc_engine.v0, c, NULL, 0,
                                      &append_ascii_stats);
    } else if (strcmp(subcommand, "reset") == 0) {
        stats_reset(c);
        out_string(c, "RESET");
        return;
    } else if (strcmp(subcommand, "detail") == 0) {
        /* NOTE: how to tackle detail with binary? */
        if (ntokens < 4)
            process_stats_detail(c, "");  /* outputs the error message */
        else
            process_stats_detail(c, tokens[2].value);
        /* Output already generated */
        return;
    } else if (strcmp(subcommand, "settings") == 0) {
        process_stats_settings(&append_ascii_stats, c);
#ifdef ENABLE_ZK_INTEGRATION
    } else if (strcmp(subcommand, "zookeeper") == 0) {
        process_stats_zookeeper(&append_ascii_stats, c);
#endif
    } else if (strcmp(subcommand, "cachedump") == 0) {
        process_stats_cachedump(c, tokens, ntokens);
        return;
    } else if (strcmp(subcommand, "aggregate") == 0) {
        server_stats(&append_ascii_stats, c, true);
    } else if (strcmp(subcommand, "topkeys") == 0) {
        if (default_topkeys == NULL) {
            out_string(c, "NOT_SUPPORTED");
            return;
        }
        topkeys_stats(default_topkeys, c, get_current_time(), append_ascii_stats);
    } else if (strcmp(subcommand, "prefixes") == 0) {
        int len;
        char *stats = mc_engine.v1->prefix_dump_stats(mc_engine.v0, c, NULL, 0, &len);
        if (stats == NULL) {
            if (len == -1)
                out_string(c, "NOT_SUPPORTED");
            else
                out_string(c, "SERVER_ERROR no more memory");
            return;
        }
        write_and_free(c, stats, len);
        return; /* Output already generated */
    } else if (strcmp(subcommand, "prefixlist") == 0) {
        int len;
        char *stats;
        token_t *prefixes = ntokens > 4 ? &tokens[3] : NULL;
        size_t nprefixes = ntokens > 4 ? ntokens-4 : 0;

        if (ntokens < 4) {
            out_string(c, "CLIENT_ERROR subcommand(item|operation) is required");
            return;
        } else if (strcmp(tokens[2].value, "item") == 0) {
            stats = mc_engine.v1->prefix_dump_stats(mc_engine.v0, c, prefixes, nprefixes, &len);
        } else if (strcmp(tokens[2].value, "operation") == 0) {
            stats = stats_prefix_dump(prefixes, nprefixes, &len);
        } else {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (stats == NULL) {
            if (len == -1)
                out_string(c, "NOT_SUPPORTED");
            else
                out_string(c, "SERVER_ERROR no more memory");
            return;
        }
        write_and_free(c, stats, len);
        return; /* Output already generated */
    } else if (strcmp(subcommand, "prefix") == 0) {
        /* command: stats prefix <prefix>\r\n */
        if (ntokens != 4) {
            print_invalid_command(c, tokens, ntokens);
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
        if (tokens[2].length > PREFIX_MAX_LENGTH) {
            out_string(c, "CLIENT_ERROR too long prefix name");
            return;
        }
        if (strcmp(tokens[2].value, "<null>") == 0) { /* reserved keyword */
            (void)mc_engine.v1->prefix_get_stats(mc_engine.v0, c, NULL, 0,
                                                 append_ascii_stats);
            stats_prefix_get(NULL, 0, append_ascii_stats, c);
        } else {
            (void)mc_engine.v1->prefix_get_stats(mc_engine.v0, c,
                                                 tokens[2].value, tokens[2].length,
                                                 append_ascii_stats);
            stats_prefix_get(tokens[2].value, tokens[2].length, append_ascii_stats, c);
        }
    } else {
        /* getting here means that the subcommand is either engine specific or
           is invalid. query the engine and see. */
        ENGINE_ERROR_CODE ret;
        int nb;
        char buf[1024];

        nb = detokenize(&tokens[1], ntokens - 2, buf, 1024);
        if (nb <= 0) {
            /* no matching stat */
            ret = ENGINE_KEY_ENOENT;
        } else {
            ret = mc_engine.v1->get_stats(mc_engine.v0, c, buf, nb,
                                          append_ascii_stats);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            append_ascii_stats(NULL, 0, NULL, 0, c);
            write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
            c->dynamic_buffer.buffer = NULL;
            break;
        case ENGINE_ENOMEM:
            out_string(c, "SERVER_ERROR out of memory writing stats");
            break;
        case ENGINE_KEY_ENOENT:
            out_string(c, "ERROR no matching stat");
            break;
        default:
            handle_unexpected_errorcode_ascii(c, __func__, ret);
            break;
        }
        return;
    }

    /* append terminator and start the transfer */
    append_ascii_stats(NULL, 0, NULL, 0, c);

    if (c->dynamic_buffer.buffer == NULL) {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    } else {
        write_and_free(c, c->dynamic_buffer.buffer, c->dynamic_buffer.offset);
        c->dynamic_buffer.buffer = NULL;
    }
}

/** config */
static void process_verbosity_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    unsigned int level;

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "verbosity %d\r\nEND", settings.verbose);
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(config_val, &level)) {
        if (level > MAX_VERBOSITY_LEVEL) {
            out_string(c, "SERVER_ERROR cannot change the verbosity over the limit");
            return;
        }
        LOCK_SETTING();
        mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&level);
        settings.verbose = level;
        perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
        UNLOCK_SETTING();
        out_string(c, "END");
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_memlimit_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    unsigned int mlimit;

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "memlimit %u\r\nEND", (int)(settings.maxbytes / (1024 * 1024)));
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(config_val, &mlimit)) {
        ENGINE_ERROR_CODE ret;
        size_t new_maxbytes = (size_t)mlimit * 1024 * 1024;
        LOCK_SETTING();
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&new_maxbytes);
        if (ret == ENGINE_SUCCESS) {
            settings.maxbytes = new_maxbytes;
        }
        UNLOCK_SETTING();
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#ifdef ENABLE_ZK_INTEGRATION
static void process_zkfailstop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "zkfailstop %s\r\nEND", arcus_zk_get_failstop() ? "on" : "off");
        out_string(c, buf);
    } else if (ntokens == 4) {
        const char *config = tokens[COMMAND_TOKEN+2].value;
        bool zkfailstop;
        if (strcmp(config, "on") == 0)
            zkfailstop = true;
        else if (strcmp(config, "off") == 0)
            zkfailstop = false;
        else {
            out_string(c, "CLIENT_ERROR bad value");
            return;
        }
        arcus_zk_set_failstop(zkfailstop);
        out_string(c, "END");
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_hbtimeout_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    unsigned int hbtimeout;

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "hbtimeout %d\r\nEND", arcus_hb_get_timeout());
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(tokens[SUBCOMMAND_TOKEN+1].value, &hbtimeout)) {
        if (arcus_hb_set_timeout((int)hbtimeout) == 0)
            out_string(c, "END");
        else
            out_string(c, "CLIENT_ERROR bad value");
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_hbfailstop_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    unsigned int hbfailstop;

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "hbfailstop %d\r\nEND", arcus_hb_get_failstop());
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(tokens[SUBCOMMAND_TOKEN+1].value, &hbfailstop)) {
        if (arcus_hb_set_failstop((int)hbfailstop) == 0)
            out_string(c, "END");
        else
            out_string(c, "CLIENT_ERROR bad value");
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}
#endif

static void process_maxconns_command(conn *c, token_t *tokens, const size_t ntokens)
{
    int new_max;

    if (ntokens == 3) {
        char buf[32];
        sprintf(buf, "maxconns %d\r\nEND", settings.maxconns);
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtol(tokens[SUBCOMMAND_TOKEN+1].value, &new_max)) {
        struct rlimit rlim;
        int curr_conns;
        int extra_nfiles = ADMIN_MAX_CONNECTIONS + ZK_CONNECTIONS;
        if (settings.port != 0) {
            extra_nfiles += 2;
        }
        if (settings.udpport != 0) {
            extra_nfiles += settings.num_threads * 2;
        }
        LOCK_STATS();
        curr_conns = mc_stats.curr_conns;
        UNLOCK_STATS();
        if (new_max + extra_nfiles < (int)(curr_conns * 1.1) || new_max + extra_nfiles > 1000000) {
            out_string(c, "CLIENT_ERROR the value is out of range");
            return;
        }
        if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            out_string(c, "SERVER_ERROR failed to get RLIMIT_NOFILE");
            return;
        }
        if ((rlim.rlim_cur != RLIM_INFINITY) && (new_max + extra_nfiles > (int)rlim.rlim_cur)) {
            out_string(c, "SERVER_ERROR cannot change to the maxconns over the soft limit");
            return;
        }
        LOCK_SETTING();
        settings.maxconns = new_max;
        UNLOCK_SETTING();
        out_string(c, "END");
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_maxcollsize_command(conn *c, token_t *tokens, const size_t ntokens,
                                        int coll_type)
{
    assert(c != NULL && coll_type != ITEM_TYPE_KV);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    int32_t maxsize;

    if (ntokens == 3) {
        char buf[50];
        switch (coll_type) {
        case ITEM_TYPE_LIST:
            sprintf(buf, "max_list_size %u\r\nEND", settings.max_list_size);
            break;
        case ITEM_TYPE_SET:
            sprintf(buf, "max_set_size %u\r\nEND", settings.max_set_size);
            break;
        case ITEM_TYPE_MAP:
            sprintf(buf, "max_map_size %u\r\nEND", settings.max_map_size);
            break;
        case ITEM_TYPE_BTREE:
            sprintf(buf, "max_btree_size %u\r\nEND", settings.max_btree_size);
            break;
        }
        out_string(c, buf);
    }
    else if (ntokens == 4 && safe_strtol(config_val, &maxsize)) {
        ENGINE_ERROR_CODE ret;

        LOCK_SETTING();
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&maxsize);
        if (ret == ENGINE_SUCCESS) {
            switch (coll_type) {
            case ITEM_TYPE_LIST:
                settings.max_list_size = maxsize;
                break;
            case ITEM_TYPE_SET:
                settings.max_set_size = maxsize;
                break;
            case ITEM_TYPE_MAP:
                settings.max_map_size = maxsize;
                break;
            case ITEM_TYPE_BTREE:
                settings.max_btree_size = maxsize;
                break;
            }
        }
        UNLOCK_SETTING();
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
    else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_maxelembytes_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    uint32_t new_maxelembytes;

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "max_element_bytes %u\r\nEND", settings.max_element_bytes);
        out_string(c, buf);
    }
    else if (ntokens == 4 && safe_strtoul(config_val, &new_maxelembytes)) {
        ENGINE_ERROR_CODE ret;
        LOCK_SETTING();
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&new_maxelembytes);
        if (ret == ENGINE_SUCCESS) {
           settings.max_element_bytes = new_maxelembytes;
        }
        UNLOCK_SETTING();
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
    else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_scrubcount_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    uint32_t new_scrub_count;

    if (ntokens == 3) {
        char buf[32];
        sprintf(buf, "scrub_count %u\r\nEND", settings.scrub_count);
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(config_val, &new_scrub_count)) {
        ENGINE_ERROR_CODE ret;
        LOCK_SETTING();
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&new_scrub_count);
        if (ret == ENGINE_SUCCESS) {
            settings.scrub_count = new_scrub_count;
        }
        UNLOCK_SETTING();
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

#ifdef ENABLE_STICKY_ITEM
static void process_stickylimit_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    unsigned int sticky_limit;

    if (ntokens == 3) {
        char buf[50];
        sprintf(buf, "sticky_limit %u\r\nEND", (int)(settings.sticky_limit / (1024 * 1024)));
        out_string(c, buf);
    } else if (ntokens == 4 && safe_strtoul(config_val, &sticky_limit)) {
        ENGINE_ERROR_CODE ret;
        size_t new_sticky_limit = (size_t)sticky_limit * 1024 * 1024;
        LOCK_SETTING();
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&new_sticky_limit);
        if (ret == ENGINE_SUCCESS) {
            settings.sticky_limit = new_sticky_limit;
        }
        UNLOCK_SETTING();
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}
#endif

#ifdef ENABLE_PERSISTENCE
static void process_chkpt_interval_pct_snapshot_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    unsigned int chkpt_interval_pct_snapshot;
    ENGINE_ERROR_CODE ret;

    if (ntokens == 3) {
        ret = mc_engine.v1->get_config(mc_engine.v0, NULL, "chkpt_interval_pct_snapshot",
                                       (void*)&chkpt_interval_pct_snapshot);
        if (ret == ENGINE_SUCCESS) {
            char buf[50];
            sprintf(buf, "chkpt_interval_pct_snapshot %u\r\nEND", (unsigned int)chkpt_interval_pct_snapshot);
            out_string(c, buf);
        } else {
            out_string(c, "NOT_SUPPORTED");
        }
    } else if (ntokens == 4 && safe_strtoul(config_val, &chkpt_interval_pct_snapshot)) {
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&chkpt_interval_pct_snapshot);
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_chkpt_interval_min_logsize_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    unsigned int chkpt_interval_min_logsize;
    ENGINE_ERROR_CODE ret;

    if (ntokens == 3) {
        ret = mc_engine.v1->get_config(mc_engine.v0, NULL, "chkpt_interval_min_logsize",
                                       (void*)&chkpt_interval_min_logsize);
        if (ret == ENGINE_SUCCESS) {
            char buf[50];
            sprintf(buf, "chkpt_interval_min_logsize %u\r\nEND", (unsigned int)chkpt_interval_min_logsize);
            out_string(c, buf);
        } else {
            out_string(c, "NOT_SUPPORTED");
        }
    } else if (ntokens == 4 && safe_strtoul(config_val, &chkpt_interval_min_logsize)) {
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&chkpt_interval_min_logsize);
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

static void process_async_logging_command(conn *c, token_t *tokens, const size_t ntokens)
{
    assert(c != NULL);
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;
    char *config_val = tokens[SUBCOMMAND_TOKEN+1].value;
    bool  async_logging;
    ENGINE_ERROR_CODE ret;

    if (ntokens == 3) {
        ret = mc_engine.v1->get_config(mc_engine.v0, NULL, "async_logging", (void*)&async_logging);
        if (ret == ENGINE_SUCCESS) {
            char buf[50];
            sprintf(buf, "async_logging %s\r\nEND", (bool)async_logging ? "on" : "off");
            out_string(c, buf);
        } else {
            out_string(c, "NOT_SUPPORTED");
        }
    } else if (ntokens == 4) {
        bool new_async_logging;
        if (strcmp(config_val, "on") == 0)
            new_async_logging = true;
        else if (strcmp(config_val, "off") == 0)
            new_async_logging = false;
        else {
            out_string(c, "CLIENT_ERROR bad value");
            return;
        }
        ret = mc_engine.v1->set_config(mc_engine.v0, c, config_key, (void*)&new_async_logging);
        if (ret == ENGINE_SUCCESS)        out_string(c, "END");
        else if (ret == ENGINE_EBADVALUE) out_string(c, "CLIENT_ERROR bad value");
        else handle_unexpected_errorcode_ascii(c, __func__, ret);
    } else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}
#endif

static void process_config_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *config_key = tokens[SUBCOMMAND_TOKEN].value;

    if (ntokens < 3 || ntokens > 4) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (strcmp(config_key, "maxconns") == 0) {
        process_maxconns_command(c, tokens, ntokens);
    }
    else if (strcmp(config_key, "memlimit") == 0) {
        process_memlimit_command(c, tokens, ntokens);
    }
    else if (strcmp(config_key, "max_list_size") == 0) {
        process_maxcollsize_command(c, tokens, ntokens, ITEM_TYPE_LIST);
    }
    else if (strcmp(config_key, "max_set_size") == 0) {
        process_maxcollsize_command(c, tokens, ntokens, ITEM_TYPE_SET);
    }
    else if (strcmp(config_key, "max_map_size") == 0) {
        process_maxcollsize_command(c, tokens, ntokens, ITEM_TYPE_MAP);
    }
    else if (strcmp(config_key, "max_btree_size") == 0) {
        process_maxcollsize_command(c, tokens, ntokens, ITEM_TYPE_BTREE);
    }
    else if (strcmp(config_key, "max_element_bytes") == 0) {
        process_maxelembytes_command(c, tokens, ntokens);
    }
    else if (strcmp(config_key, "scrub_count") == 0) {
        process_scrubcount_command(c, tokens, ntokens);
    }
#ifdef ENABLE_ZK_INTEGRATION
    else if (strcmp(config_key, "zkfailstop") == 0) {
        process_zkfailstop_command(c, tokens, ntokens);
    }
    else if (strcmp(config_key, "hbtimeout") == 0) {
        process_hbtimeout_command(c, tokens, ntokens);
    }
    else if (strcmp(config_key, "hbfailstop") == 0) {
        process_hbfailstop_command(c, tokens, ntokens);
    }
#endif
#ifdef ENABLE_STICKY_ITEM
    else if (strcmp(config_key, "sticky_limit") == 0) {
        process_stickylimit_command(c, tokens, ntokens);
    }
#endif
    else if (strcmp(config_key, "verbosity") == 0) {
        process_verbosity_command(c, tokens, ntokens);
    }
#ifdef ENABLE_PERSISTENCE
    else if (strcmp(config_key, "chkpt_interval_pct_snapshot") == 0) {
        process_chkpt_interval_pct_snapshot_command(c, tokens, ntokens);
    }
    else if (strcmp(config_key, "chkpt_interval_min_logsize") == 0) {
        process_chkpt_interval_min_logsize_command(c, tokens, ntokens);
    }
    else if (strcmp(config_key, "async_logging") == 0) {
        process_async_logging_command(c, tokens, ntokens);
    }
#endif
    else {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}

/** cmdlog */
#ifdef COMMAND_LOGGING
static void process_logging_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *type = tokens[COMMAND_TOKEN+1].value;
    bool already_check = false;

    if (ntokens > 2 && strcmp(type, "start") == 0) {
        char *fpath = NULL;
        if (ntokens > 3) {
            if (tokens[SUBCOMMAND_TOKEN+1].length > CMDLOG_DIRPATH_LENGTH) {
                out_string(c, "\tcommand logging failed to start, path exceeds 128.\n");
                cmdlog_in_use = false;
                return;
            }
            fpath = tokens[SUBCOMMAND_TOKEN+1].value;
        }

        int ret = cmdlog_start(fpath, &already_check);
        if (already_check) {
            out_string(c, "\tcommand logging already started.\n");
            return;
        }
        if (ret == 0) {
            out_string(c, "\tcommand logging started.\n");
            cmdlog_in_use = true;
        } else {
            out_string(c, "\tcommand logging failed to start.\n");
            cmdlog_in_use = false;
        }
    } else if (ntokens > 2 && strcmp(type, "stop") == 0) {
        cmdlog_stop(&already_check);
        if (already_check) {
            out_string(c, "\tcommand logging already stopped.\n");
        } else {
            out_string(c, "\tcommand logging stopped.\n");
            cmdlog_in_use = false;
        }
    } else if (ntokens > 2 && strcmp(type, "stats") == 0) {
        char *str = cmdlog_stats();
        if (str) {
            write_and_free(c, str, strlen(str));
        } else {
            out_string(c, "\tcommand logging failed to get stats memory.\n");
        }
    } else {
        out_string(c, "\t* Usage: cmdlog [start [path] | stop | stats]\n");
    }
}
#endif

/** lqdetect */
#ifdef DETECT_LONG_QUERY
static void lqdetect_show(conn *c)
{
    int ret = 0, size = 0;
    c->lq_result = lqdetect_result_get(&size);
    if (c->lq_result == NULL) {
        out_string(c, "SERVER ERROR out of memory");
        return;
    }

    for (int i = 0; i < size; i++) {
        if (add_iov(c, c->lq_result[i].value, c->lq_result[i].length) != 0) {
            ret = -1; break;
        }
    }

    if (ret == 0) {
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
    } else {
        out_string(c, "SERVER ERROR out of memory writing show response");
        lqdetect_result_release(c->lq_result);
        c->lq_result = NULL;
    }
}

static void process_lqdetect_command(conn *c, token_t *tokens, size_t ntokens)
{
    char *type = tokens[COMMAND_TOKEN+1].value;
    bool already_check = false;

    if (ntokens > 2 && strcmp(type, "start") == 0) {
        uint32_t threshold = 0;
        if (ntokens > 3) {
            if (! safe_strtoul(tokens[SUBCOMMAND_TOKEN+1].value, &threshold)) {
                print_invalid_command(c, tokens, ntokens);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
        }
        int ret = lqdetect_start(threshold, &already_check);
        if (ret == 0) {
            if (already_check) {
                out_string(c, "\tlong query detection already started.\n");
            } else {
                out_string(c, "\tlong query detection started.\n");
                lqdetect_in_use = true;
            }
        } else {
            out_string(c, "\tlong query detection failed to start.\n");
        }
    } else if (ntokens > 2 && strcmp(type, "stop") == 0) {
        lqdetect_stop(&already_check);
        if (already_check) {
            out_string(c, "\tlong query detection already stopped.\n");
        } else {
            out_string(c, "\tlong query detection stopped.\n");
            lqdetect_in_use = false;
        }
    } else if (ntokens > 2 && strcmp(type, "show") == 0) {
        lqdetect_show(c);
    } else if (ntokens > 2 && strcmp(type, "stats") == 0) {
        char *str = lqdetect_stats();
        if (str) {
            write_and_free(c, str, strlen(str));
        } else {
            out_string(c, "\tlong query detection failed to get stats memory.\n");
        }
    } else {
        out_string(c,
        "\t" "* Usage: lqdetect [start [standard] | stop | show | stats]" "\n"
        );
    }
}
#endif

/** dump */
static void process_dump_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    char *modestr;
    char *filepath;
    char *prefix = NULL;
    int  nprefix = -1; /* all prefixes */
    bool valid = false;

    /* dump ascii command
     * dump start <mode> [<prefix>] filepath\r\n
     *   <mode> : key
     * dump stop\r\n
     */
    if (ntokens == 3) {
        if (memcmp(subcommand, "stop", 4) == 0) {
            modestr = filepath = NULL;
            valid = true;
        }
    } else if (ntokens == 5 || ntokens == 6) {
        if (memcmp(subcommand, "start", 5) == 0) {
            modestr = tokens[2].value;
            if (ntokens == 5) {
                filepath = tokens[3].value;
            } else {
                prefix = tokens[3].value;
                nprefix = tokens[3].length;
                if (nprefix > PREFIX_MAX_LENGTH) {
                    out_string(c, "CLIENT_ERROR too long prefix name");
                    return;
                }
                if (nprefix == 6 && strncmp(prefix, "<null>", 6) == 0) {
                    /* dump null prefix */
                    prefix = NULL;
                    nprefix = 0;
                }
                filepath = tokens[4].value;
            }
            valid = true;
        }
    }
    if (valid == false) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    ENGINE_ERROR_CODE ret;
    ret = mc_engine.v1->dump(mc_engine.v0, c, subcommand, modestr,
                             prefix, nprefix, filepath);
    if (ret == ENGINE_SUCCESS) {
        out_string(c, "OK");
    } else if (ret == ENGINE_FAILED) {
        out_string(c, "SERVER_ERROR failed. refer to the reason in server log.");
    } else {
        handle_unexpected_errorcode_ascii(c, __func__, ret);
    }
}

/** zkensemble */
#ifdef ENABLE_ZK_INTEGRATION
static void process_zkensemble_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    bool valid = false;

    if (arcus_zk_cfg == NULL) {
        out_string(c, "ERROR not using ZooKeeper");
        return;
    }

    if (ntokens == 3) {
        if (strcmp(subcommand, "get") == 0) {
            char buf[1024];
            if (arcus_zk_get_ensemble(buf, sizeof(buf)-16) != 0) {
                out_string(c, "ERROR failed to get the ensemble address");
            } else {
                strcat(buf, "\r\n\n");
                out_string(c, buf);
            }
            valid = true;
        } else if (strcmp(subcommand, "rejoin") == 0) {
            if (arcus_zk_rejoin_ensemble() != 0) {
                out_string(c, "ERROR failed to rejoin ensemble");
            } else {
                out_string(c, "Successfully rejoined");
            }
            valid = true;
        }
    } else if (ntokens == 4) {
        if (strcmp(subcommand, "set") == 0) {
            /* The ensemble is a comma separated list of host:port addresses.
             * host1:port1,host2:port2,...
             */
            if (arcus_zk_set_ensemble(tokens[SUBCOMMAND_TOKEN+1].value) != 0) {
                out_string(c, "ERROR failed to set the new ensemble address (check logs)");
            } else {
                out_string(c, "OK");
            }
            valid = true;
        }
    }
    if (valid == false) {
        print_invalid_command(c, tokens, ntokens);
        out_string(c, "CLIENT_ERROR bad command line format");
    }
}
#endif

#endif

#if ETC_PROCESS || 1
static void process_help_command(conn *c, token_t *tokens, const size_t ntokens)
{
    char *type = tokens[COMMAND_TOKEN+1].value;

    if (ntokens > 2 && strcmp(type, "kv") == 0) {
        out_string(c,
        "\t" "set|add|replace <key> <flags> <exptime> <bytes> [noreply]\\r\\n<data>\\r\\n" "\n"
        "\t" "append|prepend <key> <flags> <exptime> <bytes> [noreply]\\r\\n<data>\\r\\n" "\n"
        "\t" "cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\\r\\n<data>\\r\\n" "\n"
        "\t" "get <key>[ <key> ...]\\r\\n" "\n"
        "\t" "gets <key>[ <key> ...]\\r\\n" "\n"
        "\t" "mget <lenkeys> <numkeys>\\r\\n<\"space separated keys\">\\r\\n" "\n"
        "\t" "incr|decr <key> <delta> [<flags> <exptime> <initial>] [noreply]\\r\\n" "\n"
        "\t" "delete <key> [<time>] [noreply]\\r\\n" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "list") == 0) {
        out_string(c,
        "\t" "lop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "lop insert <key> <index> <bytes> [create <attributes>] [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "lop delete <key> <index or range> [drop] [noreply|pipe]\\r\\n" "\n"
        "\t" "lop get <key> <index or range> [delete|drop]\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "set") == 0) {
        out_string(c,
        "\t" "sop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "sop insert <key> <bytes> [create <attributes>] [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "sop delete <key> <bytes> [drop] [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "sop get <key> <count> [delete|drop]\\r\\n" "\n"
        "\t" "sop exist <key> <bytes> [pipe]\\r\\n<data>\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "map") == 0) {
        out_string(c,
        "\t" "mop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "mop insert <key> <field> <bytes> [create <attributes>] [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "mop update <key> <field> <bytes> [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "mop delete <key> <lenfields> <numfields> [drop] [noreply|pipe]\\r\\n[<\"space separated fields\">]\\r\\n" "\n"
        "\t" "mop get <key> <lenfields> <numfields> [delete|drop]\\r\\n[<\"space separated fields\">]\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "btree") == 0) {
        out_string(c,
        "\t" "bop create <key> <attributes> [noreply]\\r\\n" "\n"
        "\t" "bop insert|upsert <key> <bkey> [<eflag>] <bytes> [create <attributes>] [noreply|pipe|getrim]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop update <key> <bkey> [<eflag_update>] <bytes> [noreply|pipe]\\r\\n<data>\\r\\n" "\n"
        "\t" "bop delete <key> <bkey or \"bkey range\"> [<eflag_filter>] [<count>] [drop] [noreply|pipe]\\r\\n" "\n"
        "\t" "bop get <key> <bkey or \"bkey range\"> [<eflag_filter>] [[<offset>] <count>] [delete|drop]\\r\\n" "\n"
        "\t" "bop count <key> <bkey or \"bkey range\"> [<eflag_filter>] \\r\\n" "\n"
        "\t" "bop incr|decr <key> <bkey> <delta> [<initial> [<eflag>]] [noreply|pipe]\\r\\n" "\n"
        "\t" "bop mget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] [<offset>] <count>\\r\\n<\"space separated keys\">\\r\\n" "\n"
        "\t" "bop smget <lenkeys> <numkeys> <bkey or \"bkey range\"> [<eflag_filter>] <count> [duplicate|unique]\\r\\n<\"space separated keys\">\\r\\n" "\n"
        "\t" "bop position <key> <bkey> <order>\\r\\n" "\n"
        "\t" "bop pwg <key> <bkey> <order> [<count>]\\r\\n" "\n"
        "\t" "bop gbp <key> <order> <position or \"position range\">\\r\\n" "\n"
        "\n"
        "\t" "* <attributes> : <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]" "\n"
        "\t" "* <eflag_update> : [<fwhere> <bitwop>] <fvalue>" "\n"
        "\t" "* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>" "\n"
        "\t" "                 : <fwhere> [<bitwop> <foperand>] EQ|NE <comma separated fvalue list>" "\n"
        "\t" "* <bitwop> : &, |, ^" "\n"
        "\t" "* <compop> : EQ, NE, LT, LE, GT, GE" "\n"
        );
    } else if (ntokens > 2 && strcmp(type, "attr") == 0) {
        out_string(c,
        "\t" "getattr <key> [<attribute name> ...]\\r\\n" "\n"
        "\t" "setattr <key> <name>=<value> [<name>=value> ...]\\r\\n" "\n"
        );
#ifdef SCAN_COMMAND
    } else if (ntokens > 2 && strcmp(type, "scan") == 0) {
        out_string(c,
        "\t" "scan key <cursor> [count <count>] [match <pattern>] [type <type>]\\r\\n" "\n"
        "\t" "scan prefix <cursor> [count <count>] [match <pattern>]\\r\\n" "\n"
        );
#endif
    } else if (ntokens > 2 && strcmp(type, "admin") == 0) {
        out_string(c,
        "\t" "flush_all [<delay>] [noreply]\\r\\n" "\n"
        "\t" "flush_prefix <prefix> [<delay>] [noreply]\\r\\n" "\n"
        "\n"
        "\t" "scrub [stale]\\r\\n" "\n"
        "\n"
        "\t" "stats\\r\\n" "\n"
        "\t" "stats settings\\r\\n" "\n"
        "\t" "stats items\\r\\n" "\n"
        "\t" "stats slabs\\r\\n" "\n"
        "\t" "stats prefixes\\r\\n" "\n"
        "\t" "stats detail [on|off|dump]\\r\\n" "\n"
#ifdef ENABLE_ZK_INTEGRATION
        "\t" "stats zookeeper\\r\\n" "\n"
#endif
        "\t" "stats scrub\\r\\n" "\n"
        "\t" "stats dump\\r\\n" "\n"
        "\t" "stats cachedump <slab_clsid> <limit> [forward|backward [sticky]]\\r\\n" "\n"
        "\t" "stats reset\\r\\n" "\n"
#ifdef ENABLE_PERSISTENCE
        "\t" "stats persistence\\r\\n" "\n"
#endif
#ifdef COMMAND_LOGGING
        "\n"
        "\t" "cmdlog start [<file_path>]\\r\\n" "\n"
        "\t" "cmdlog stop\\r\\n" "\n"
        "\t" "cmdlog stats\\r\\n" "\n"
#endif
#ifdef DETECT_LONG_QUERY
        "\n"
        "\t" "lqdetect start [<detect_standard>]\\r\\n" "\n"
        "\t" "lqdetect stop\\r\\n" "\n"
        "\t" "lqdetect show\\r\\n" "\n"
        "\t" "lqdetect stats\\r\\n" "\n"
#endif
        "\n"
        "\t" "dump start <mode> [<prefix>] <filepath>\\r\\n" "\n"
        "\t" "  * <mode> : key" "\n"
        "\t" "dump stop\\r\\n" "\n"
#ifdef ENABLE_ZK_INTEGRATION
        "\n"
        "\t" "zkensemble set <ensemble_list>\\r\\n" "\n"
        "\t" "zkensemble get\\r\\n" "\n"
        "\t" "zkensemble rejoin\\r\\n" "\n"
#endif
        "\n"
        "\t" "config verbosity [<verbose>]\\r\\n" "\n"
        "\t" "config memlimit [<memsize(MB)>]\\r\\n" "\n"
#ifdef ENABLE_STICKY_ITEM
        "\t" "config sticky_limit [<stickylimit(MB)>]\\r\\n" "\n"
#endif
        "\t" "config maxconns [<maxconn>]\\r\\n" "\n"
        "\t" "config max_list_size [<maxsize>]\\r\\n" "\n"
        "\t" "config max_set_size [<maxsize>]\\r\\n" "\n"
        "\t" "config max_map_size [<maxsize>]\\r\\n" "\n"
        "\t" "config max_btree_size [<maxsize>]\\r\\n" "\n"
        "\t" "config max_element_bytes [<maxbytes>]\\r\\n" "\n"
        "\t" "config scrub_count [<count>]\\r\\n" "\n"
#ifdef ENABLE_ZK_INTEGRATION
        "\t" "config hbtimeout [<hbtimeout>]\\r\\n" "\n"
        "\t" "config hbfailstop [<hbfailstop>]\\r\\n" "\n"
        "\t" "config zkfailstop [on|off]\\r\\n" "\n"
#endif
#ifdef ENABLE_PERSISTENCE
        "\t" "config chkpt_interval_pct_snapshot [<percentage(%)>]\r\n"
        "\t" "config chkpt_interval_min_logsize [<minsize(MB)>]\r\n"
        "\t" "config async_logging [on|off]\r\n"
#endif
        );
    } else {
        char *cmd_types[] = { "kv", "list", "set", "map", "btree", "attr",
#ifdef SCAN_COMMAND
                              "scan",
#endif
                              "admin", NULL };
        int cmd_index;
        char buffer[256];
        char *ptr = &buffer[0];

        ptr += sprintf(ptr, "\tUsage: help [");
        cmd_index = 0;
        while (cmd_types[cmd_index]) {
            ptr += sprintf(ptr, "%s | ", cmd_types[cmd_index]);
            cmd_index += 1;
        }
        ptr -= 3; // remove the last " | " string
        ptr += sprintf(ptr, "]\n");
        out_string(c, buffer);
    }
}

static void process_extension_command(conn *c, token_t *tokens, size_t ntokens)
{
    EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *cmd;
    size_t nbytes = 0;
    char *ptr;

    if (ntokens > 0) {
        if (ntokens == MAX_TOKENS) {
            out_string(c, "ERROR too many arguments");
            return;
        }
        if (tokens[ntokens - 1].length == 0) {
            --ntokens;
        }
    }
    /* ntokens must be larger than 0 in order to avoid segfault in the next for statement. */
    if (ntokens == 0) {
        out_string(c, "ERROR no arguments");
        return;
    }

    for (cmd = settings.extensions.ascii; cmd != NULL; cmd = cmd->next) {
        if (cmd->accept(cmd->cookie, c, ntokens, tokens, &nbytes, &ptr)) {
            break;
        }
    }
    if (cmd == NULL) {
        out_string(c, "ERROR no matching command");
        return;
    }
    if (nbytes == 0) {
        if (!cmd->execute(cmd->cookie, c, ntokens, tokens,
                          ascii_response_handler)) {
            conn_set_state(c, conn_closing);
        } else {
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                conn_set_state(c, conn_new_cmd);
            }
        }
    } else {
        c->ritem = ptr;
        c->rlbytes = nbytes;
        c->rltotal = 0;
        c->ascii_cmd = cmd;
        /* NOT SUPPORTED YET! */
        conn_set_state(c, conn_nread);
    }
}

#endif

#if COMMAND_DISPATCH || 1
static void process_command_ascii(conn *c, char *command, int cmdlen)
{
    /* One more token is reserved in tokens strucure
     * for keeping the length of untokenized command.
     */
    token_t tokens[MAX_TOKENS+1];
    size_t ntokens;
    int comm;

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

    if (settings.verbose > 1) {
        mc_logger->log(EXTENSION_LOG_DEBUG, c,
                       "<%d %s\n", c->sfd, command);
    }

    /*
     * for commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        out_string(c, "SERVER_ERROR out of memory preparing response");
        return;
    }

#ifdef COMMAND_LOGGING
    if (cmdlog_in_use) {
        if (cmdlog_write(c->client_ip, command) == false) {
            cmdlog_in_use = false;
        }
    }
#endif

    ntokens = tokenize_command(command, cmdlen, tokens, MAX_TOKENS);

    if ((ntokens >= 3) && ((strcmp(tokens[COMMAND_TOKEN].value, "get" ) == 0) ||
                           (strcmp(tokens[COMMAND_TOKEN].value, "bget") == 0)))
    {
        process_get_command(c, tokens, ntokens, false);
    }
    else if ((ntokens >= 3) && (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0))
    {
        process_get_command(c, tokens, ntokens, true);
    }
    else if ((ntokens == 4) && (strcmp(tokens[COMMAND_TOKEN].value, "mget") == 0))
    {
        process_mget_command(c, tokens, ntokens, false);
    }
    else if ((ntokens == 4) && (strcmp(tokens[COMMAND_TOKEN].value, "mgets") == 0))
    {
        process_mget_command(c, tokens, ntokens, true);
    }
    else if ((ntokens == 6 || ntokens == 7) &&
        ((strcmp(tokens[COMMAND_TOKEN].value, "add"    ) == 0 && (comm = (int)OPERATION_ADD)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "set"    ) == 0 && (comm = (int)OPERATION_SET)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "replace") == 0 && (comm = (int)OPERATION_REPLACE)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "prepend") == 0 && (comm = (int)OPERATION_PREPEND)) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "append" ) == 0 && (comm = (int)OPERATION_APPEND)) ))
    {
        process_update_command(c, tokens, ntokens, (ENGINE_STORE_OPERATION)comm, false);
    }
    else if ((ntokens == 7 || ntokens == 8) &&
         (strcmp(tokens[COMMAND_TOKEN].value, "cas"    ) == 0 && (comm = (int)OPERATION_CAS)))
    {
        process_update_command(c, tokens, ntokens, (ENGINE_STORE_OPERATION)comm, true);
    }
    else if ((ntokens == 4 || ntokens == 5 || ntokens == 7 || ntokens == 8) &&
        (strcmp(tokens[COMMAND_TOKEN].value, "incr") == 0))
    {
        process_arithmetic_command(c, tokens, ntokens, 1);
    }
    else if ((ntokens == 4 || ntokens == 5 || ntokens == 7 || ntokens == 8) &&
        (strcmp(tokens[COMMAND_TOKEN].value, "decr") == 0))
    {
        process_arithmetic_command(c, tokens, ntokens, 0);
    }
    else if ((ntokens >= 3 && ntokens <= 5) && (strcmp(tokens[COMMAND_TOKEN].value, "delete") == 0))
    {
        process_delete_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 5 && ntokens <= 13) && (strcmp(tokens[COMMAND_TOKEN].value, "lop") == 0))
    {
        process_lop_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 5 && ntokens <= 12) && (strcmp(tokens[COMMAND_TOKEN].value, "sop") == 0))
    {
        process_sop_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 6 && ntokens <= 13) && (strcmp(tokens[COMMAND_TOKEN].value, "mop") == 0))
    {
        process_mop_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 5 && ntokens <= 14) && (strcmp(tokens[COMMAND_TOKEN].value, "bop") == 0))
    {
        process_bop_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 3 && ntokens <= 14) && (strcmp(tokens[COMMAND_TOKEN].value, "getattr") == 0))
    {
        process_getattr_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 4 && ntokens <=  8) && (strcmp(tokens[COMMAND_TOKEN].value, "setattr") == 0))
    {
        process_setattr_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "stats") == 0))
    {
        process_stats_command(c, tokens, ntokens);
    }
    else if ((ntokens >= 2 && ntokens <= 4) && (strcmp(tokens[COMMAND_TOKEN].value, "flush_all") == 0))
    {
        process_flush_command(c, tokens, ntokens, true);
    }
    else if ((ntokens >= 3 && ntokens <= 5) && (strcmp(tokens[COMMAND_TOKEN].value, "flush_prefix") == 0))
    {
        process_flush_command(c, tokens, ntokens, false);
    }
    else if ((ntokens >= 3) && (strcmp(tokens[COMMAND_TOKEN].value, "config") == 0))
    {
        process_config_command(c, tokens, ntokens);
    }
#ifdef ENABLE_ZK_INTEGRATION
    else if ((ntokens >= 3) && (strcmp(tokens[COMMAND_TOKEN].value, "zkensemble") == 0))
    {
        process_zkensemble_command(c, tokens, ntokens);
    }
#endif
    else if ((ntokens == 2) && (strcmp(tokens[COMMAND_TOKEN].value, "version") == 0))
    {
        out_string(c, "VERSION " VERSION);
    }
    else if ((ntokens >= 3) && (strcmp(tokens[COMMAND_TOKEN].value, "dump") == 0))
    {
        process_dump_command(c, tokens, ntokens);
    }
    else if ((ntokens == 2) && (strcmp(tokens[COMMAND_TOKEN].value, "quit") == 0))
    {
        LOCK_STATS();
        mc_stats.quit_conns++;
        UNLOCK_STATS();
        conn_set_state(c, conn_closing);
    }
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "help") == 0))
    {
        process_help_command(c, tokens, ntokens);
    }
#ifdef SCAN_COMMAND
    else if ((ntokens >= 4) && (strcmp(tokens[COMMAND_TOKEN].value, "scan") == 0))
    {
        process_scan_command(c, tokens, ntokens);
    }
#endif
#ifdef COMMAND_LOGGING
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "cmdlog") == 0))
    {
        process_logging_command(c, tokens, ntokens);
    }
#endif
#ifdef DETECT_LONG_QUERY
    else if ((ntokens >= 2) && (strcmp(tokens[COMMAND_TOKEN].value, "lqdetect") == 0))
    {
        process_lqdetect_command(c, tokens, ntokens);
    }
#endif
    else if ((ntokens == 2) && (strcmp(tokens[COMMAND_TOKEN].value, "ready") == 0))
    {
        char *response = "READY";
#ifdef ENABLE_ZK_INTEGRATION
        if (arcus_zk_cfg) {
            arcus_zk_stats zk_stats;
            arcus_zk_get_stats(&zk_stats);
            if (!zk_stats.zk_ready) response = "NOT_READY";
        }
#endif
        out_string(c, response);
    }
    else /* no matching command */
    {
        if (settings.extensions.ascii != NULL) {
            process_extension_command(c, tokens, ntokens);
        } else {
            out_string(c, "ERROR unknown command");
        }
    }
}

static void complete_update_ascii(conn *c)
{
    assert(c != NULL);
    assert(c->ewouldblock == false);

    /* The condition of 'c->coll_strkeys != NULL' is given for map collection.
     * See process_mop_delete_complete() and process_mop_get_complete().
     */
    if (c->coll_eitem != NULL || c->coll_strkeys != NULL) {
        if (c->coll_op == OPERATION_LOP_INSERT)  process_lop_insert_complete(c);
        else if (c->coll_op == OPERATION_SOP_INSERT) process_sop_insert_complete(c);
        else if (c->coll_op == OPERATION_SOP_DELETE) process_sop_delete_complete(c);
        else if (c->coll_op == OPERATION_SOP_EXIST) process_sop_exist_complete(c);
        else if (c->coll_op == OPERATION_MOP_INSERT) process_mop_insert_complete(c);
        else if (c->coll_op == OPERATION_MOP_UPDATE) process_mop_update_complete(c);
        else if (c->coll_op == OPERATION_MOP_DELETE) process_mop_delete_complete(c);
        else if (c->coll_op == OPERATION_MOP_GET) process_mop_get_complete(c);
        else if (c->coll_op == OPERATION_BOP_INSERT ||
                 c->coll_op == OPERATION_BOP_UPSERT) process_bop_insert_complete(c);
        else if (c->coll_op == OPERATION_BOP_UPDATE) process_bop_update_complete(c);
#ifdef SUPPORT_BOP_MGET
        else if (c->coll_op == OPERATION_BOP_MGET) process_bop_mget_complete(c);
#endif
#ifdef SUPPORT_BOP_SMGET
        else if (c->coll_op == OPERATION_BOP_SMGET) process_bop_smget_complete(c);
#endif
        else if (c->coll_op == OPERATION_MGET) process_mget_complete(c, false);
        else if (c->coll_op == OPERATION_MGETS) process_mget_complete(c, true);
        return;
    }

    item *it = c->item;
    ENGINE_ERROR_CODE ret;
    if (!mc_engine.v1->get_item_info(mc_engine.v0, c, it, &c->hinfo)) {
        mc_logger->log(EXTENSION_LOG_WARNING, c,
                       "%d: Failed to get item info\n", c->sfd);
        out_string(c, "SERVER_ERROR out of memory for getting item info");
        ret = ENGINE_ENOMEM;
    } else if (hinfo_check_ascii_tail_string(&c->hinfo) != 0) { /* check "\r\n" */
        out_string(c, "CLIENT_ERROR bad data chunk");
        ret = ENGINE_EBADVALUE;
    } else {
        ret = mc_engine.v1->store(mc_engine.v0, c, it, &c->cas, c->store_op, 0);
        CONN_CHECK_AND_SET_EWOULDBLOCK(ret, c);
#ifdef ENABLE_DTRACE
        switch (c->store_op) {
        case OPERATION_ADD:
            MEMCACHED_COMMAND_ADD(c->sfd, c->hinfo.key, c->hinfo.nkey,
                                  (ret == ENGINE_SUCCESS) ? c->hinfo.nbytes : -1, c->cas);
            break;
        case OPERATION_REPLACE:
            MEMCACHED_COMMAND_REPLACE(c->sfd, c->hinfo.key, c->hinfo.nkey,
                                      (ret == ENGINE_SUCCESS) ? c->hinfo.nbytes : -1, c->cas);
            break;
        case OPERATION_APPEND:
            MEMCACHED_COMMAND_APPEND(c->sfd, c->hinfo.key, c->hinfo.nkey,
                                     (ret == ENGINE_SUCCESS) ? c->hinfo.nbytes : -1, c->cas);
            break;
        case OPERATION_PREPEND:
            MEMCACHED_COMMAND_PREPEND(c->sfd, c->hinfo.key, c->hinfo.nkey,
                                      (ret == ENGINE_SUCCESS) ? c->hinfo.nbytes : -1, c->cas);
            break;
        case OPERATION_SET:
            MEMCACHED_COMMAND_SET(c->sfd, c->hinfo.key, c->hinfo.nkey,
                                  (ret == ENGINE_SUCCESS) ? c->hinfo.nbytes : -1, c->cas);
            break;
        case OPERATION_CAS:
            MEMCACHED_COMMAND_CAS(c->sfd, c->hinfo.key, c->hinfo.nkey, c->hinfo.nbytes, c->cas);
            break;
        }
#endif

        switch (ret) {
        case ENGINE_SUCCESS:
            out_string(c, "STORED");
            break;
        case ENGINE_KEY_EEXISTS:
            out_string(c, "EXISTS");
            break;
        case ENGINE_KEY_ENOENT:
            out_string(c, "NOT_FOUND");
            break;
        case ENGINE_NOT_STORED:
            out_string(c, "NOT_STORED");
            break;
        case ENGINE_PREFIX_ENAME:
            out_string(c, "CLIENT_ERROR invalid prefix name");
            break;
        case ENGINE_ENOMEM:
            out_string(c, "SERVER_ERROR out of memory");
            break;
        case ENGINE_EINVAL:
            out_string(c, "CLIENT_ERROR invalid arguments");
            break;
        case ENGINE_E2BIG:
            out_string(c, "CLIENT_ERROR value too big");
            break;
        case ENGINE_EACCESS:
            out_string(c, "CLIENT_ERROR access control violation");
            break;
        case ENGINE_NOT_MY_VBUCKET:
            out_string(c, "SERVER_ERROR not my vbucket");
            break;
        case ENGINE_EBADTYPE:
            out_string(c, "TYPE_MISMATCH");
            break;
        case ENGINE_FAILED:
            out_string(c, "SERVER_ERROR failure");
            break;
        default:
            handle_unexpected_errorcode_ascii(c, __func__, ret);
        }
    }

    if (c->store_op == OPERATION_CAS) {
        update_stat_cas(c, ret);
    } else {
        STATS_CMD(c, set, c->hinfo.key, c->hinfo.nkey);
    }

    mc_engine.v1->release(mc_engine.v0, c, c->item);
    c->item = 0;
}

#endif

#if GLOBAL || 1
void out_string(conn *c, const char *str)
{
    assert(c != NULL);
    size_t len = strlen(str);
    bool original_noreply = c->noreply;

    if (settings.verbose > 1) {
        if (c->noreply)
            mc_logger->log(EXTENSION_LOG_DEBUG, c, ">%d NOREPLY %s\n", c->sfd, str);
        else
            mc_logger->log(EXTENSION_LOG_DEBUG, c, ">%d %s\n", c->sfd, str);
    }

    if (c->pipe_state != PIPE_STATE_OFF) {
        if (pipe_response_save(c, str, len) < 0) { /* PIPE_STATE_ERR.. */
            c->noreply = false; /* stop pipelining */
        }
    }

    if (c->noreply) {
        c->noreply = false;
       /* Clear the ewouldblock so that the next read command from
        * the same connection does not falsely block and time out.
        *
        * It's better not to set the ewouldblock if noreply exists
        * when write operations are performed.
        */
        if (c->ewouldblock) {
            c->ewouldblock = false;
#ifdef MULTI_NOTIFY_IO_COMPLETE
            mc_logger->log(EXTENSION_LOG_WARNING, c,
                    "[FATAL] Unexpected ewouldblock in noreply processing.\n");
#endif
        }
        conn_set_state(c, conn_new_cmd);
        return;
    }

    /* Nuke a partial output... */
    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    add_msghdr(c);

    if (c->pipe_state != PIPE_STATE_OFF) {
        pipe_response_done(c, !original_noreply);
        return;
    }

    if ((len + 2) > c->wsize) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }

    memcpy(c->wbuf, str, len);
    memcpy(c->wbuf + len, "\r\n", 2);
    c->wbytes = len + 2;
    c->wcurr = c->wbuf;

    conn_set_state(c, conn_write);
    c->write_and_go = conn_new_cmd;
}

int try_read_command_ascii(conn *c)
{
    char *el, *cont;

    if (c->rbytes == 0)
        return 0;

    el = memchr(c->rcurr, '\n', c->rbytes);
    if (!el) {
        if (c->rbytes > 1024) {
            /*
             * We didn't have a '\n' in the first k. This _has_ to be a
             * large multiget, if not we should just nuke the connection.
             */
            char *ptr = c->rcurr;
            while (*ptr == ' ') { /* ignore leading whitespaces */
                ++ptr;
            }
            if (ptr - c->rcurr > 100) {
                mc_logger->log(EXTENSION_LOG_WARNING, c,
                    "%d: Too many leading whitespaces(%d). Close the connection.\n",
                    c->sfd, (int)(ptr - c->rcurr));
                conn_set_state(c, conn_closing);
                return 1;
            }
            /* Check KEY_MAX_LENGTH and eflag filter length
             *  - KEY_MAX_LENGTH : 16000
             *  - IN eflag filter : > 6400 (64*100)
             */
            if (c->rbytes > ((16+8)*1024)) {
                /* The length of "stats prefixes" command cannot exceed 24 KB. */
                if (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5)) {
                    char buffer[16];
                    memcpy(buffer, ptr, 15); buffer[15] = '\0';
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                        "%d: Too long ascii command(%s). Close the connection. client_ip: %s\n",
                        c->sfd, buffer, c->client_ip);
                    conn_set_state(c, conn_closing);
                    return 1;
                }
            }
        }
        return 0;
    }
    cont = el + 1;
    if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
        el--;
    }
    *el = '\0';

    assert(cont <= (c->rcurr + c->rbytes));

    process_command_ascii(c, c->rcurr, el - c->rcurr);

    c->rbytes -= (cont - c->rcurr);
    c->rcurr = cont;

    assert(c->rcurr <= (c->rbuf + c->rsize));

    return 1;
}

void complete_nread_ascii(conn *c)
{
    if (c->ascii_cmd != NULL) {
        if (!c->ascii_cmd->execute(c->ascii_cmd->cookie, c, 0, NULL,
                                   ascii_response_handler)) {
            conn_set_state(c, conn_closing);
        } else {
            if (c->dynamic_buffer.buffer != NULL) {
                write_and_free(c, c->dynamic_buffer.buffer,
                               c->dynamic_buffer.offset);
                c->dynamic_buffer.buffer = NULL;
            } else {
                conn_set_state(c, conn_new_cmd);
            }
        }
    } else {
        complete_update_ascii(c);
    }
}

#endif

#endif
