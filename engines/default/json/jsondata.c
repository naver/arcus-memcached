#include <assert.h>
#include "jsondata.h"
#include "engines/default/item_base.h"
#include "engines/default/default_engine.h"

static json_elem_item *_new_item(json_node_type t, uint32_t real_nbytes,
                                const void *cookie, struct default_engine *engine)
{
    size_t ntotal = sizeof(json_elem_item) + real_nbytes;

    unsigned int clsid = slabs_clsid(ntotal);
    if(clsid == 0) return NULL;
    json_elem_item *elem = do_item_mem_alloc(ntotal, clsid, cookie);

    if (elem != NULL) {
        memset(elem, 0, sizeof(json_elem_item));
        elem->slabs_clsid = clsid;
        assert(elem->slabs_clsid > 0);

        elem->refcount = 1;
        elem->nbytes = ntotal;
        elem->type = t;
        elem->status = JSON_ITEM_STATUS_UNLINK;
    }
    return elem;
}

json_elem_item *new_null_item(const void *cookie, struct default_engine *engine)
{
    return _new_item(N_NULL, 0, cookie, engine);
}

json_elem_item *new_bool_item(int val, const void *cookie, struct default_engine *engine)
{
    json_elem_item *ret = _new_item(N_BOOLEAN, 0, cookie, engine);
    ret->value.boolval = val != 0;
    return ret;
}

json_elem_item *new_double_item(double val, const void *cookie, struct default_engine *engine)
{
    json_elem_item *ret = _new_item(N_NUMBER, 0, cookie, engine);
    ret->value.numval = val;
    return ret;
}

json_elem_item *new_int_item(int64_t val, const void *cookie, struct default_engine *engine)
{
    json_elem_item *ret = _new_item(N_INTEGER, 0, cookie, engine);
    ret->value.intval = val;
    return ret;
}

json_elem_item *new_string_item(const char *s, uint32_t len, const void *cookie, struct default_engine *engine)
{
    json_elem_item *ret = _new_item(N_STRING, len+1, cookie, engine);
    if (ret){
        char *ptr = (char *)ret + sizeof(json_elem_item);
        memcpy(ptr, s, len);
        ptr[len] = '\0';

        ret->value.strval.len = len;
        ret->value.strval.data = ptr;

    }
    return ret;
}

json_elem_item *new_cstring_item(const char *s, const void *cookie, struct default_engine *engine)
{
    return new_string_item(s, strlen(s), cookie, engine);
}

json_elem_item *new_keyval_item(const char *key, uint32_t len, json_elem_item *n, const void *cookie, struct default_engine *engine)
{
    json_elem_item *ret = _new_item(N_KEYVAL, len+1, cookie, engine);
    if(ret){
        memset(&ret->value, 0, sizeof(ret->value));
        char *ptr = (char*)(ret + 1);
        memcpy(ptr, key, len);
        ptr[len] = '\0';
        ret->value.kvval.key = ptr;
        ret->value.kvval.val = n;
    }
    return ret;
}

json_elem_item *new_array_item(uint32_t cap, const void *cookie, struct default_engine *engine)
{
    json_elem_item *ret = _new_item(N_ARRAY, 0, cookie, engine);
    if(ret){
        ret->value.arrval.cap = MAX_CONTAINER_SIZE;
        ret->value.arrval.len = 0;
        ret->value.arrval.alloc_size = INITIAL_CONTAINER_SIZE;
        ret->value.arrval.entries = (json_elem_item**)malloc(sizeof(json_elem_item*) * INITIAL_CONTAINER_SIZE);
        if (ret->value.arrval.entries) {
            memset(ret->value.arrval.entries, 0, sizeof(json_elem_item*) * INITIAL_CONTAINER_SIZE);
        } else {
            free(ret);
            return NULL;
        }
    }
    return ret;
}

json_elem_item *new_dict_item(uint32_t cap, const void *cookie, struct default_engine *engine)
{
    json_elem_item *ret = _new_item(N_DICT, 0, cookie, engine);
    if(ret){
        ret->value.dictval.cap = MAX_CONTAINER_SIZE;
        ret->value.dictval.len = 0;
        ret->value.dictval.alloc_size = INITIAL_CONTAINER_SIZE;
        ret->value.dictval.entries = (json_elem_item**)malloc(sizeof(json_elem_item*) * INITIAL_CONTAINER_SIZE);
        if (ret->value.dictval.entries) {
            memset(ret->value.dictval.entries, 0, sizeof(json_elem_item*) * INITIAL_CONTAINER_SIZE);
        } else {
            free(ret);
            return NULL;
        }
    }
    return ret;
}

static int item_array_make_room_for(json_elem_item *arr, uint32_t addlen)
{
    t_array *a = &arr->value.arrval;
    uint32_t required = a->len + addlen;

    if (a->alloc_size >= required) return OBJ_OK;
    if (required > a->cap) return OBJ_ERR;

    uint32_t next_alloc = a->alloc_size * 2;
    if (next_alloc > a->cap) next_alloc = a->cap;
    if (next_alloc < required) next_alloc = required;

    void *new_entries = realloc(a->entries, next_alloc * sizeof(json_elem_item*));
    if (!new_entries) return OBJ_ERR;

    a->entries = (json_elem_item**)new_entries;
    a->alloc_size = next_alloc;
    return OBJ_OK;
}

int item_array_append(json_elem_item *arr, json_elem_item *n)
{
    t_array *a = &arr->value.arrval;
    if(item_array_make_room_for(arr, 1) != OBJ_OK){
        return OBJ_ERR;
    }
    a->entries[a->len++] = n;

    return OBJ_OK;
}

int item_array_set(json_elem_item *arr, int index, json_elem_item *n, json_elem_item **old)
{
    t_array *a = &arr->value.arrval;

    if (index<0 || index >= a->len) {
        return OBJ_ERR;
    }
    if (old) {
        *old = a->entries[index];
    }
    a->entries[index] = n;

    return OBJ_OK;
}

int item_array_item(json_elem_item *arr, int index, json_elem_item **n)
{
    t_array *a = &arr->value.arrval;

    if (index < 0 || index >= a->len) {
        *n = NULL;
        return OBJ_ERR;
    }
    *n = a->entries[index];
    return OBJ_OK;
}

json_elem_item *_obj_find(t_dict *o, const char *key, int *idx)
{
    for (int i = 0; i < o->len; i++) {
        if (!strcmp(key, o->entries[i]->value.kvval.key)) {
            if (idx) *idx = i;
            return o->entries[i];
        }
    }
    return NULL;
}

static int _obj_insert(t_dict *o, json_elem_item *n)
{
    uint32_t required = o->len + 1;

    if (required > o->alloc_size) {

        if (required > o->cap) {
            return OBJ_ERR;
        }
        uint32_t next_alloc = o->alloc_size * 2;
        if (next_alloc > o->cap) next_alloc = o->cap;

        void *new_ptr = realloc(o->entries, next_alloc * sizeof(json_elem_item*));
        if (!new_ptr) return OBJ_ERR;

        o->entries = (json_elem_item**)new_ptr;
        o->alloc_size = next_alloc;
    }
    o->entries[o->len++] = n;
    return OBJ_OK;
}

int item_dict_set(json_elem_item *obj, const char *key, json_elem_item *n,
                  const void *cookie, struct default_engine *engine, json_elem_item **old_node)
{
    t_dict *o = &obj->value.dictval;
    int idx = -1;

    if (key == NULL) return OBJ_ERR;
    if (old_node) *old_node = NULL;

    json_elem_item *kv = _obj_find(o, key, &idx);
    if (kv) {
        if (old_node) *old_node = kv->value.kvval.val;
        kv->value.kvval.val = n;
        return OBJ_OK;
    }

    json_elem_item *new_kv = new_keyval_item(key, strlen(key),n, cookie, engine);
    if (!new_kv) return OBJ_ERR;

    if (_obj_insert(o, new_kv) != OBJ_OK) {
        return OBJ_ERR;
    }
    return OBJ_OK;
}

int item_dict_set_keyval(json_elem_item *obj, json_elem_item *kv, json_elem_item **old)
{
    t_dict *o = &obj->value.dictval;

    if (kv->value.kvval.key == NULL) return OBJ_ERR;

    int idx;
    json_elem_item *_kv = _obj_find(o, kv->value.kvval.key, &idx);
    if (_kv) {
        o->entries[idx] = kv;
        *old = _kv;
        return OBJ_OK;
    }
    return _obj_insert(o, kv);
}

int item_dict_get(json_elem_item *obj, const char *key, json_elem_item **val)
{
    if (key == NULL) return OBJ_ERR;

    t_dict *o = &obj->value.dictval;

    int idx = -1;
    json_elem_item *kv = _obj_find(o, key, &idx);

    if (!kv) return OBJ_ERR;

    *val = kv->value.kvval.val;
    return OBJ_OK;
}

/* Path */
static json_elem_item *path_item_eval(path_item *pn, json_elem_item *n, path_error *err) {
    *err = E_OK;
    if (!n) {
        goto badtype;
    }

    if (n->type == N_ARRAY) {
        json_elem_item *rn = NULL;
        int index = 0;

        if (NT_INDEX == pn->type) {
            index = pn->value.index;

            if (index < 0) {
                index = n->value.arrval.len + index;
            }
            int rc = item_array_item(n, index, &rn);
            if (rc != OBJ_OK) {
                *err = E_NOINDEX;
            }

        } else {
            goto badtype;
        }
        return rn;
    }
    if (n->type == N_DICT) {
        if (pn->type != NT_KEY) {
            goto badtype;
        }
        json_elem_item *rn = NULL;
        int rc = item_dict_get(n, pn->value.key, &rn);
        if (rc != OBJ_OK) {
            *err = E_NOKEY;
        }
        return rn;
    }
badtype:
    *err = E_BADTYPE;
    return NULL;
}

path_error search_path_find_ex(search_path *path, json_elem_item *root,
                               json_elem_item **n, json_elem_item **p)
{
    json_elem_item *current = root;
    json_elem_item *prev = NULL;
    path_error ret;

    for (int i = 0; i < path->len; i++) {
        if (path->nodes[i].type == NT_ROOT) {
            continue;
        }
        prev = current;
        current = path_item_eval(&path->nodes[i], current, &ret);
        if (ret != E_OK) {
            *p = prev;
            *n = NULL;
            return ret;
        }
    }
    *p = prev;
    *n = current;
    return E_OK;
}

search_path new_search_path(void)
{
    search_path sp;
    sp.len = 0;
    sp.cap = JSON_MAX_PATH;
    sp.has_leading_dot = 0;
    return sp;
}

static void _search_path_append(search_path *p, path_item pn)
{
    if (p->len >= p->cap) {
        p->cap = p->cap ? (p->cap * 2 < 1024 ? p->cap * 2 : 1024) : 1;
    }
    p->nodes[p->len++] = pn;
}

static void search_path_append_index(search_path *p, int idx)
{
    path_item pn;
    pn.type = NT_INDEX;
    pn.value.index = idx;
    _search_path_append(p, pn);
}

static void search_path_append_key(search_path *p, const char *key, const size_t len)
{
    path_item pn;
    pn.type = NT_KEY;
    pn.value.key = strndup(key, len);
    _search_path_append(p, pn);
}

static void search_path_append_root(search_path *p)
{
    path_item pn;
    pn.type = NT_ROOT;
    _search_path_append(p, pn);
}

static int _tokenize_path(const char *json, size_t len, search_path *path)
{
    tokenizer_state st = S_NULL;
    size_t offset = 0;
    char *pos = (char *)json;
    token tok;
    tok.len = 0;
    tok.s = pos;
    tok.type = T_KEY;

    while (offset <= len) {
        char c = (offset < len) ? *pos : '\0';

        switch (st) {
          case S_NULL:
               if (c == '$') {
                   tok.s = pos; tok.len = 1; tok.type = T_KEY;
                   st = S_IDENT;
               } else if (c == '.') {
                   st = S_ROOT;
               } else if (c == '[') {
                   st = S_BRACKET;
               } else if (isalpha(c) || c == '_') {
                   tok.s = pos; tok.len = 1; tok.type = T_KEY;
                   st = S_IDENT;
               } else if (c == '\0') {
                   // 빈 문자열 처리
               } else goto syntaxerror;
               break;

          case S_ROOT:
          case S_DOT:
               if (isalpha(c) || c == '$' || c == '_') {
                   tok.s = pos; tok.len = 1; st = S_IDENT;
               } else goto syntaxerror;
               break;

          case S_IDENT:
               if (c == '.' || c == '[' || c == '\0') {
                   if (tok.len == 1 && tok.s[0] == '$') search_path_append_root(path);
                   else if (tok.len > 0) search_path_append_key(path, tok.s, tok.len);

                   if (c == '.') st = S_DOT;
                   else if (c == '[') st = S_BRACKET;
                   else st = S_NULL;
                   tok.len = 0;
               } else if (isalnum(c) || c == '_' || c == '$') {
                   tok.len++;
               } else goto syntaxerror;
               break;

          case S_BRACKET:
               if (isdigit(c)) {
                   tok.s = pos; tok.len = 1; tok.type = T_INDEX; st = S_NUMBER;
               } else if (c == '-') {
                   tok.s = pos; tok.len = 1; tok.type = T_INDEX; st = S_MINUS;
               } else if (c == '"' || c == '\'') st = S_DKEY;
               else goto syntaxerror;
               break;

          case S_MINUS: // 마이너스 부호 처리
               if (isdigit(c)) {
                   tok.len++;
                   st = S_NUMBER;
               } else goto syntaxerror;
               break;

          case S_NUMBER:
               if (isdigit(c)) {
                   tok.len++;
               } else if (c == ']') {
                   int64_t num = 0;
                   int i = (tok.s[0] == '-') ? 1 : 0;
                   for (; i < (int)tok.len; i++) num = num * 10 + (tok.s[i] - '0');
                   if (tok.s[0] == '-') num = -num;
                   search_path_append_index(path, (int)num);
                   st = S_NULL;
                   tok.len = 0;
               } else goto syntaxerror;
               break;

          case S_DKEY:
          case S_SKEY:
               if (c == '"' || c == '\'') {
                   if (tok.len > 0) search_path_append_key(path, tok.s, tok.len);
                   st = S_NULL;
               } else {
                   if (tok.len == 0) tok.s = pos;
                   tok.len++;
               }
               break;
        }

        if (offset < len) { offset++; pos++; }
        else break;
    }
    return (st == S_NULL || st == S_IDENT) ? PARSE_OK : PARSE_ERR;

syntaxerror:
    return PARSE_ERR;
}

int parse_json_path(const char *json_path, size_t len,
                    search_path *path)
{
    path->len = 0;

    // 루트 경로($) 직접 처리
    if (len == 1 && json_path[0] == '$') {
        path_item pn;
        pn.type = NT_ROOT;
        if (path->len < path->cap) {
            path->nodes[path->len++] = pn;
        }
        return PARSE_OK;
    }

    return _tokenize_path(json_path, len, path);
}

int search_path_is_root_path(const search_path *sp)
{
    return (sp->len == 1 && sp->nodes[0].type == NT_ROOT);
}
