#ifndef SASL_AUXPROP_H
#define SASL_AUXPROP_H

#ifdef ENABLE_SASL
#ifdef ENABLE_ZK_INTEGRATION

#include <sasl/sasl.h>
#include <sasl/saslplug.h>

int arcus_auxprop_plug_init(const sasl_utils_t *utils,
                            int max_version,
                            int *out_version,
                            sasl_auxprop_plug_t **plug,
                            const char *plugname);

int arcus_getdata(const char *user, char *out, const size_t max_out);

void arcus_auxprop_wakeup(void);

#endif /* ENABLE_ZK_INTEGRATION */
#endif /* ENABLE_SASL */

#endif /* SASL_AUXPROP_H */
