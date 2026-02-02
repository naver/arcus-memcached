#include <jsonsl/jsonsl.h>
#include <memcached/json_type.h>
#include "memcached.h"

#define JSONOBJECT_OK 0
#define JSONOBJECT_ERROR 1

typedef struct _json_parser_context {
    jsonsl_error_t err;
    size_t errpos;
    eitem *nodes[JSONSL_MAX_LEVELS];
    uint16_t nlen;
    conn *conn;
} json_parser_context;

int create_item_from_json(const char *buf, size_t len, eitem **node, conn *c, char **err);
void json_parser_init(int levels, ENGINE_HANDLE_V1 *v1);
void new_json_parser(conn *c);
void free_json_parser(conn *c);
