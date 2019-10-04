#ifndef __CMD_IN_SECOND_
#define __CMD_IN_SECOND_
#endif

#include <stdbool.h>
#include <stdint.h>
#include <sys/time.h>
#include <include/memcached/extension.h>

typedef enum {
    CMD_IN_SECOND_START,
    CMD_IN_SECOND_STARTED_ALREADY,
    CMD_IN_SECOND_NO_MEM,
    CMD_IN_SECOND_THREAD_FAILED,
    CMD_IN_SECOND_FILE_FAILED,
} CMD_IN_SECOND_START_CODE;

void cmd_in_second_init(EXTENSION_LOGGER_DESCRIPTOR *mc_logger);
int cmd_in_second_start(int operation, const char cmd[], int bulk_limit);
void cmd_in_second_write(int operation, const char* key, const char* client_ip);
void cmd_in_second_stop(bool* already_stop);
