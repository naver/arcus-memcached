#ifndef SASL_DEFS_H
#define SASL_DEFS_H 1

#include <stdint.h>
const char *sasl_engine_string(void);
uint16_t arcus_sasl_authz(const char *username);

#if defined(ENABLE_SASL)

#include <sasl/sasl.h>
#include "memcached/types.h"

int init_sasl(void);
void shutdown_sasl(void);
void sasl_get_auth_data(sasl_conn_t *conn, auth_data_t *data);

#if defined(ENABLE_ZK_INTEGRATION)
void reload_sasl(void);
#endif

#elif defined(ENABLE_ISASL)

#include "isasl.h"
#include "memcached/types.h"

int init_sasl(void);
void shutdown_sasl(void);
void sasl_get_auth_data(sasl_conn_t *conn, auth_data_t *data);

#endif /* End of SASL support */

#endif /* SASL_DEFS_H */
