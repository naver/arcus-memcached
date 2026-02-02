#ifndef JSON_TYPE
#define JSON_TYPE

#include <stdio.h>
#include <stdint.h>

typedef enum {
    N_NULL = 0x1,
    N_STRING = 0x2,
    N_NUMBER = 0x4,
    N_INTEGER = 0x8,
    N_BOOLEAN = 0x10,
    N_DICT = 0x20,
    N_ARRAY = 0x40,
    N_KEYVAL = 0x80
} json_node_type;

typedef union _json_value {
    int boolval;
    double numval;
    int64_t intval;
    struct {
        const char *pos;
        size_t len;
    } strval;
} json_value;
#endif
