#ifndef SASL_DEFS_H
#define SASL_DEFS_H 1

#include <stdint.h>
const char *sasl_engine_string(void);
uint16_t arcus_sasl_authz(const char *username);

#if defined(ENABLE_SASL)

#include <sasl/sasl.h>
#include "memcached/extension.h"
#include "memcached/types.h"

int init_sasl(EXTENSION_LOGGER_DESCRIPTOR *logger);
void shutdown_sasl(void);
int reload_sasl(void);
void sasl_get_auth_data(sasl_conn_t *conn, auth_data_t *data);


#elif defined(ENABLE_ISASL)

#include "isasl.h"
#include "memcached/extension.h"
#include "memcached/types.h"

int init_sasl(EXTENSION_LOGGER_DESCRIPTOR *logger);
void shutdown_sasl(void);
int reload_sasl(void);
void sasl_get_auth_data(sasl_conn_t *conn, auth_data_t *data);

#endif /* End of SASL support */

#endif /* SASL_DEFS_H */
