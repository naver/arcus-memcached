#ifndef SASL_DEFS_H
#define SASL_DEFS_H 1

// Longest one I could find was ``9798-U-RSA-SHA1-ENC''
#define MAX_SASL_MECH_LEN 32


#if defined(ENABLE_SASL)

#include <sasl/sasl.h>
int init_sasl(void);
void shutdown_sasl(void);
uint16_t arcus_sasl_authz(const char *username);
const char *sasl_engine_string(void);

#if defined(ENABLE_ZK_INTEGRATION)
void reload_sasl(void);
#endif

#elif defined(ENABLE_ISASL)

#include "isasl.h"
int init_sasl(void);
void shutdown_sasl(void);
uint16_t arcus_sasl_authz(const char *username);
const char *sasl_engine_string(void);

#else /* End of SASL support */

typedef void* sasl_conn_t;

#define shutdown_sasl()
#define init_sasl() 0
#define arcus_sasl_authz(a) 0
#define sasl_engine_string() "none"
#define sasl_dispose(x) {}
#define sasl_server_new(a, b, c, d, e, f, g, h) 1
#define sasl_listmech(a, b, c, d, e, f, g, h) 1
#define sasl_server_start(a, b, c, d, e, f) 1
#define sasl_server_step(a, b, c, d, e) 1
#define sasl_getprop(a, b, c) {}
#define sasl_errstring(a, b, c) ""

#define SASL_CONTINUE  1
#define SASL_OK        0
#define SASL_FAIL     -1

#endif /* sasl compat */

#endif /* SASL_DEFS_H */
