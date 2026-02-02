#include "json_parsing.h"
#include <errno.h>
#include <math.h>
#include <limits.h>
#include <assert.h>

static struct {
    int levels;
    union {
        ENGINE_HANDLE *v0;
        ENGINE_HANDLE_V1 *v1;
    } engine;                   // mc engine
    bool    init;               // is this structure initialized?
} parsing_conf;

static inline void _push_node(json_parser_context *jpctx, eitem *n)
{
    jpctx->nodes[jpctx->nlen] = n;
    jpctx->nlen++;
}

static inline eitem *_pop_node(json_parser_context *jpctx)
{
    jpctx->nlen--;
    return jpctx->nodes[jpctx->nlen];
}

static int _allowed_escapes[0x80];
static int _is_allowed_whitespace(unsigned c);

inline static int error_callback(jsonsl_t jsn, jsonsl_error_t err,
                                 struct jsonsl_state_st *state, char *errat)
{
    json_parser_context *jpctx = (json_parser_context*)jsn->data;
    jpctx->errpos = jsn->pos; //json parsing error position
    jpctx->err = err;   //json parsing error code
    jsonsl_stop(jsn);
    return 0;
}

inline static void push_callback(jsonsl_t jsn, jsonsl_action_t action,
                                 struct jsonsl_state_st *state, const jsonsl_char_t *at)
{
    json_parser_context *jpctx = (json_parser_context*)jsn->data;
    eitem *n = NULL;
    json_value value;
    ENGINE_ERROR_CODE err = ENGINE_SUCCESS;

    switch (state->type) {
      case JSONSL_T_OBJECT:
           value.intval = 1;
           err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                         jpctx->conn->coll_nkey, &n, N_DICT, &value);
           _push_node(jpctx, n);
           break;
      case JSONSL_T_LIST:
           value.intval = 1;
           err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                         jpctx->conn->coll_nkey, &n, N_ARRAY, &value);
           _push_node(jpctx, n);
           break;
      default:
           break;
    }
    if (err != ENGINE_SUCCESS) error_callback(jsn, JSONSL_ERROR_ENOMEM, state, NULL);
}

inline static void pop_callback(jsonsl_t jsn, jsonsl_action_t action,
                                struct jsonsl_state_st *state, const jsonsl_char_t *at)
{
    json_parser_context *jpctx = (json_parser_context*)jsn->data;
    const char *pos = jsn->base + state->pos_begin;
    size_t len = state->pos_cur - state->pos_begin;
    eitem *n;
    json_value json_val;
    ENGINE_ERROR_CODE err = ENGINE_SUCCESS;

    if (JSONSL_T_STRING == state->type || JSONSL_T_HKEY == state->type) {
        char *buffer = NULL;
        pos++;
        len--;

        if (state->nescapes) {
            jsonsl_error_t json_err;
            size_t newlen;
            buffer = (char*)malloc(len * sizeof(char));
            newlen = jsonsl_util_unescape(pos, buffer, len, _allowed_escapes, &json_err);

            if (!newlen) {
                free(buffer);
                error_callback(jsn, json_err, state, NULL);
                return ;
            }
            pos = buffer;
            len = newlen;
        }

        json_val.strval.pos = pos;
        json_val.strval.len = len;

        if (JSONSL_T_STRING == state->type) {
            err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                         jpctx->conn->coll_nkey, &n, N_STRING, &json_val);
        } else {
            err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                         jpctx->conn->coll_nkey, &n, N_KEYVAL, &json_val);
        }

        if (err != ENGINE_SUCCESS) {
            error_callback(jsn, JSONSL_ERROR_ENOMEM, state, NULL);
            return ;
        }
        _push_node(jpctx, n);

        if (buffer) free(buffer);
    }
    if (JSONSL_T_SPECIAL == state->type) {
        if (state->special_flags & JSONSL_SPECIALf_NUMERIC) {
            if (state->special_flags & (JSONSL_SPECIALf_FLOAT | JSONSL_SPECIALf_EXPONENT)) {
                double value;
                char *eptr;

                errno = 0;
                value = strtod(pos, &eptr);
                if ((errno == ERANGE && (value == HUGE_VAL || value == -HUGE_VAL)) ||
                   (errno != 0 && value == 0) || isnan(value) || (eptr != pos + len)) {
                    error_callback(jsn, JSONSL_ERROR_INVALID_NUMBER, state, NULL);
                    return ;
                }
                json_val.numval = value;
                err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                              jpctx->conn->coll_nkey, &n, N_NUMBER, &json_val);
                if (err == ENGINE_SUCCESS) _push_node(jpctx, n);
            } else {
                long long value;
                char *eptr;

                errno = 0;
                value = strtoll(pos, &eptr, 10);

                if ((errno == ERANGE && (value == LLONG_MAX || value == LLONG_MIN)) ||
                   (errno != 0 && value == 0) || (eptr != pos + len)) {
                    error_callback(jsn, JSONSL_ERROR_INVALID_NUMBER, state, NULL);
                    return ;
                }
                json_val.intval = (int64_t)value;
                err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                              jpctx->conn->coll_nkey, &n, N_INTEGER, &json_val);
                if (err == ENGINE_SUCCESS) _push_node(jpctx, n);
            }
        } else if (state->special_flags & JSONSL_SPECIALf_BOOLEAN) {
            json_val.boolval = state->special_flags & JSONSL_SPECIALf_TRUE;
            err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                          jpctx->conn->coll_nkey, &n, N_BOOLEAN, &json_val);
            if (err == ENGINE_SUCCESS) _push_node(jpctx, n);
        } else if (state->special_flags & JSONSL_SPECIALf_NULL) {
            err = parsing_conf.engine.v1->json_elem_alloc(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key,
                                                          jpctx->conn->coll_nkey, &n, N_NULL, NULL);
            if (err == ENGINE_SUCCESS) _push_node(jpctx, n);
        }

        if (err != ENGINE_SUCCESS) {
            error_callback(jsn, JSONSL_ERROR_ENOMEM, state, NULL);
            return ;
        }
    }
    if (jpctx->nlen > 1 && state->type != JSONSL_T_HKEY) {
        json_node_type type;
        eitem *n_temp = jpctx->nodes[jpctx->nlen - 2];
        err = parsing_conf.engine.v1->json_elem_get_type(n_temp, &type);

        if (err != ENGINE_SUCCESS) {
            error_callback(jsn, JSONSL_ERROR_ENOMEM, state, NULL);
            return ;
        }

        switch (type) {
          case N_DICT:
          case N_ARRAY:
               n = _pop_node(jpctx);
               err = parsing_conf.engine.v1->json_elem_append(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key, jpctx->conn->coll_nkey,
                                                              &jpctx->nodes[jpctx->nlen - 1], n, NULL, type, 0);
               break;
          case N_KEYVAL:
               n_temp = _pop_node(jpctx);
               n = _pop_node(jpctx);
               err = parsing_conf.engine.v1->json_elem_append(parsing_conf.engine.v0, jpctx->conn, jpctx->conn->coll_key, jpctx->conn->coll_nkey,
                                                              &jpctx->nodes[jpctx->nlen - 1], n, n_temp, type, 0);
               break;
          default:
               break;
        }
        if (err != ENGINE_SUCCESS) {
            error_callback(jsn, JSONSL_ERROR_ENOMEM, state, NULL);
            return ;
        }
    }
}

static void reset_json_parser(json_parser_context *jpctx, conn* c)
{
    c->coll_json_parser->data = jpctx;
    jpctx->err = JSONSL_ERROR_SUCCESS;
    jpctx->errpos = 0;
    jpctx->nlen = 0;
    c->coll_json_parser->stack[0].nelem = 0;
    jsonsl_reset(c->coll_json_parser);
    jpctx->conn = c;
}


int create_item_from_json(const char *buf, size_t len, eitem **node, conn *c, char **err)
{
    size_t _off = 0, _len = len;
    char *_buf = (char *)buf;
    int is_scalar = 0;
    int ret = JSONOBJECT_OK;
    json_parser_context jpctx;

    do {
        if (!parsing_conf.init) {
            ret = JSONOBJECT_ERROR;
            break;
        }

        while (_off < _len && _is_allowed_whitespace(_buf[_off])) _off++;

        if ((is_scalar = ('{' != buf[_off]) && ('[' != _buf[_off]) && _off < _len)) {
            _len = _len - _off + 2;
            _buf = (char*)malloc(_len * sizeof(char));
            _buf[0] = '[';
            _buf[_len - 1] = ']';
            memcpy(&_buf[1], &buf[_off], len - _off);
        }

        reset_json_parser(&jpctx, c);
        jsonsl_feed(c->coll_json_parser, _buf, _len);

        if (jpctx.err != JSONSL_ERROR_SUCCESS) {
            sprintf(*err, "JSON PARSING ERROR lexer error %s at position %zd",
                    jsonsl_strerror(jpctx.err), jpctx.errpos + 1);
            ret = JSONOBJECT_ERROR;
            break;
        } else if (c->coll_json_parser->level) {
            sprintf(*err, "JSON PARSING ERROR value incomplete - 0 containers unterminated");
            ret = JSONOBJECT_ERROR;
            break;
        } else if (!c->coll_json_parser->stack[0].nelem) {
            sprintf(*err, "JSON PARSING ERROR value not found");
            ret = JSONOBJECT_ERROR;
            break;
        }

        if (is_scalar) {
            eitem *e = _pop_node(&jpctx);
            free(_buf);
            parsing_conf.engine.v1->json_elem_scalar(parsing_conf.engine.v0, &e, node);
        } else {
            *node = _pop_node(&jpctx);
        }
    } while(0);

    if (ret != JSONOBJECT_OK) {
        while (jpctx.nlen) {
            eitem *e = _pop_node(&jpctx);
            parsing_conf.engine.v1->json_elem_unlink(parsing_conf.engine.v0, &e);
        }
        if (is_scalar) free(_buf);
    }
    return ret;
}

void new_json_parser(conn *c)
{
    if (!parsing_conf.init) return ;
    c->coll_json_parser = jsonsl_new(parsing_conf.levels);
    c->coll_json_parser->error_callback = error_callback;
    c->coll_json_parser->action_callback_POP = pop_callback;
    c->coll_json_parser->action_callback_PUSH = push_callback;
    jsonsl_enable_all_callbacks(c->coll_json_parser);
}

void json_parser_init(int levels, ENGINE_HANDLE_V1 *engine)
{
    assert(!parsing_conf.init);
    if (0 >= levels || JSONSL_MAX_LEVELS < levels) {
        parsing_conf.levels = JSONSL_MAX_LEVELS;
    } else {
        parsing_conf.levels = levels;
    }
    parsing_conf.engine.v1 = engine;
    parsing_conf.init = true;
}

void free_json_parser(conn *c)
{
    if (c->coll_json_parser) {
        jsonsl_destroy(c->coll_json_parser);
    }
}

static int _allowed_whitespace[0x100] = {
    0,0,0,0,0,0,0,0,0,
    1,
    1,
    0,0,
    1,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    1,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
};

static int _allowed_escapes[0x80] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,
    1,
    0,0,0,0,0,0,0,0,0,0,0,0,
    1,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,
    1,
    0,0,0,0,0,
    1,
    0,0,0,
    1,
    0,0,0,0,0,0,0,
    1,
    0,0,0,
    1,
    0,
    1,
    1,
    0,0,0,0,0,
};

static int _is_allowed_whitespace(unsigned c) {
    return c == ' ' || _allowed_whitespace[c&0xff];
}
