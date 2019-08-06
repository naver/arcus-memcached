#ifndef __CMD_IN_SECOND_
#define __CMD_IN_SECOND_
#endif

#include <stdbool.h>
#include <stdint.h>
#include  <sys/time.h>

#define CMD_IN_SECOND_START 0
#define CMD_IN_SECOND_STARTED_ALREADY 1
#define CMD_IN_SECOND_NO_MEM 2

void cmd_in_second_init(void);
int32_t cmd_in_second_start(const char* collection_name, const char* cmd, const int32_t bulk_limit);
bool cmd_in_second_write(const char* collection_name, const char* cmd, const char* key, const char* client_ip);
