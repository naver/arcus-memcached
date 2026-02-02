#pragma once
#include <memcached/json_type.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <string.h>
#include <limits.h>
struct default_engine;
/* json item status */
#define JSON_ITEM_STATUS_USED   2
#define JSON_ITEM_STATUS_UNLINK 1
#define JSON_ITEM_STATUS_FREE   0

/* Object */
#define OBJ_OK 0
#define OBJ_ERR 1

/* MAX CAPACITY */ //todo max?
#define INITIAL_CONTAINER_SIZE 4
#define MAX_CONTAINER_SIZE 50000

#define NODE_IS_SCALAR(n) (!n ? 1 : (int)(n->type & (N_STRING | N_NUMBER | N_INTEGER | N_BOOLEAN )))

struct _json_elem_item;

typedef struct {
    const char *data;
    uint32_t len;
} t_string;

typedef struct {
    struct _json_elem_item **entries;
    uint32_t len;
    uint32_t cap;
    uint32_t alloc_size;
} t_array;

typedef struct {
    const char *key;
    struct _json_elem_item *val;
} t_keyval;

typedef struct {
    struct _json_elem_item **entries;
    uint32_t len;
    uint32_t cap;
    uint32_t alloc_size;
} t_dict;

typedef struct _json_elem_item {
    json_node_type type;
    uint16_t refcount;
    uint8_t  slabs_clsid;    /* which slab class we're in */
    uint32_t nbytes;         /**< The total size of the data (in bytes) */
    uint8_t status;          /* 3(used), 2(insert mark), 1(delete_mark), 0(free) */
    union
    {
        int boolval;
        double numval;
        int64_t intval;
        t_string strval;
        t_array arrval;
        t_dict dictval;
        t_keyval kvval;
    } value;

} json_elem_item;

json_elem_item *new_null_item(const void *cookie, struct default_engine *engine);
json_elem_item *new_keyval_item(const char *key, uint32_t len, json_elem_item *n, const void *cookie, struct default_engine *engine);
json_elem_item *new_bool_item(int val, const void *cookie, struct default_engine *engine);
json_elem_item *new_double_item(double val, const void *cookie, struct default_engine *engine);
json_elem_item *new_int_item(int64_t val, const void *cookie, struct default_engine *engine);
json_elem_item *new_string_item(const char *s, uint32_t len, const void *cookie, struct default_engine *engine);
json_elem_item *new_cstring_item(const char *s, const void *cookie, struct default_engine *engine);
json_elem_item *new_array_item(uint32_t cap, const void *cookie, struct default_engine *engine);
json_elem_item *new_dict_item(uint32_t cap, const void *cookie, struct default_engine *engine);

int item_array_append(json_elem_item *arr, json_elem_item *n);
int item_array_set(json_elem_item *arr, int index, json_elem_item *n, json_elem_item **old);
int item_array_item(json_elem_item *arr, int index, json_elem_item **n);
int item_dict_set(json_elem_item *obj, const char *key, json_elem_item *n, const void *cookie, struct default_engine *engine, json_elem_item **old_node);
int item_dict_set_keyval(json_elem_item *obj, json_elem_item *kv, json_elem_item **old);
int item_dict_get(json_elem_item *obj, const char *key, json_elem_item **val);

json_elem_item *_obj_find(t_dict *o, const char *key, int *idx);
/* Path */
#define JSON_MAX_PATH 1024

typedef enum {
    NT_ROOT = 0,
    NT_KEY,
    NT_INDEX,
} path_item_type;

typedef enum {
    E_OK,
    E_NOKEY,
    E_NOINDEX,
    E_BADTYPE,
} path_error;

typedef struct {
    path_item_type type;
    union {
        int index;
        const char *key;
    } value;
} path_item;

typedef struct {
    path_item nodes[JSON_MAX_PATH];
    uint32_t len;
    uint32_t cap;
    int has_leading_dot;
} search_path;

search_path new_search_path(void);
path_error search_path_find_ex(search_path *path, json_elem_item *root,
                               json_elem_item **n, json_elem_item **p);

#define PARSE_OK 0
#define PARSE_ERR 1

typedef enum {
    T_KEY,
    T_INDEX,
} token_type;

typedef enum {
    S_NULL,
    S_ROOT,
    S_IDENT,
    S_NUMBER,
    S_DKEY,
    S_SKEY,
    S_BRACKET,
    S_DOT,
    S_MINUS,
} tokenizer_state;

typedef struct {
    token_type type;
    char *s;
    size_t len;
} token;

typedef struct json_path_node_t {
    const char *spath;
    size_t spathlen;
    json_elem_item *n;
    json_elem_item *p;
    search_path sp;
} json_path_node;

int parse_json_path(const char *json_path, size_t len, search_path *path);
int search_path_is_root_path(const search_path *sp);


/* Json Meta Info */

typedef struct _json_meta_info {
    int32_t mcnt;
    int32_t ccnt;
    uint8_t ovflact;
    uint8_t mflags;
    uint16_t itdist;
    uint32_t stotal;
    json_elem_item *root;
} json_meta_info;


/* Json item */
typedef struct _json_get_elem {
    size_t cap;
    char *buffer;
} json_get_elem;
