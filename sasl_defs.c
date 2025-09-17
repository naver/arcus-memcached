/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "memcached.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sasl_auxprop.h"

#if defined(ENABLE_SASL) && defined(ENABLE_ZK_INTEGRATION)
static bool use_acl_zookeeper = false;
#endif

#if defined(HAVE_SASL_CB_GETCONF) || defined(HAVE_SASL_CB_GETCONFPATH)
/* The locations we may search for a SASL config file if the user didn't
 * specify one in the environment variable SASL_CONF_PATH
 */
const char * const locations[] = {
    "/etc/sasl/memcached.conf",
    "/etc/sasl2/memcached.conf",
    NULL
};
#endif

#ifdef ENABLE_SASL_PWDB
#define MAX_ENTRY_LEN 256

static const char *memcached_sasl_pwdb;

static int sasl_server_userdb_checkpass(sasl_conn_t *conn,
                                        void *context,
                                        const char *user,
                                        const char *pass,
                                        unsigned passlen,
                                        struct propctx *propctx)
{
    size_t unmlen = strlen(user);
    if ((passlen + unmlen) > (MAX_ENTRY_LEN - 4)) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                       "WARNING: Failed to authenticate <%s> due to too long password (%u)\n",
                       user, passlen);
        return SASL_NOAUTHZ;
    }

    FILE *pwfile = fopen(memcached_sasl_pwdb, "r");
    if (pwfile == NULL) {
        if (settings.verbose) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                           "WARNING: Failed to open sasl database <%s>",
                           memcached_sasl_pwdb);
        }
        return SASL_NOAUTHZ;
    }

    char buffer[MAX_ENTRY_LEN];
    bool ok = false;

    while ((fgets(buffer, sizeof(buffer), pwfile)) != NULL) {
        if (memcmp(user, buffer, unmlen) == 0 && buffer[unmlen] == ':') {
            /* This is the correct user */
            ++unmlen;
            if (memcmp(pass, buffer + unmlen, passlen) == 0 &&
                (buffer[unmlen + passlen] == ':' || /* Additional tokens */
                 buffer[unmlen + passlen] == '\n' || /* end of line */
                 buffer[unmlen + passlen] == '\r'|| /* dos format? */
                 buffer[unmlen + passlen] == '\0')) { /* line truncated */
                ok = true;
            }

            break;
        }
    }
    (void)fclose(pwfile);
    if (ok) {
        return SASL_OK;
    }

    mc_logger->log(EXTENSION_LOG_WARNING, NULL, "WARNING: User <%s> failed to authenticate\n", user);

    return SASL_NOAUTHZ;
}
#endif

#if defined(HAVE_SASL_CB_GETCONF) || defined(HAVE_SASL_CB_GETCONFPATH)
static int sasl_getconf(void *context, const char **path)
{
    *path = getenv("SASL_CONF_PATH");

    if (*path == NULL) {
        for (int i = 0; locations[i] != NULL; ++i) {
            if (access(locations[i], F_OK) == 0) {
                *path = locations[i];
                break;
            }
        }
    }

    if (settings.verbose) {
        if (*path != NULL) {
            mc_logger->log(EXTENSION_LOG_INFO, NULL, "Reading configuration from: <%s>\n", *path);
        } else {
            mc_logger->log(EXTENSION_LOG_INFO, NULL, "Failed to locate a config path\n");
        }

    }

    return (*path != NULL) ? SASL_OK : SASL_FAIL;
}
#endif

#ifdef ENABLE_SASL
#ifdef ENABLE_ZK_INTEGRATION
static int sasl_getopt(void *context __attribute__((unused)),
                       const char *plugin_name __attribute__((unused)),
                       const char *option,
                       const char **result, unsigned *len)
{
    if (strcmp(option, "auxprop_plugin") == 0) {
        *result = "arcus";
        if (len) *len = (unsigned)strlen(*result);
        return SASL_OK;
    }
    if (strcmp(option, "mech_list") == 0) {
        *result = "scram-sha-256";
        if (len) *len = (unsigned)strlen(*result);
        return SASL_OK;
    }

    return SASL_FAIL;
}
#endif

static int sasl_log(void *context, int level, const char *message)
{
    bool log = true;

    switch (level) {
    case SASL_LOG_NONE:
        log = false;
        break;
    case SASL_LOG_PASS:
    case SASL_LOG_TRACE:
    case SASL_LOG_DEBUG:
    case SASL_LOG_NOTE:
        if (settings.verbose < 2) {
            log = false;
        }
        break;
    case SASL_LOG_WARN:
    case SASL_LOG_FAIL:
        if (settings.verbose < 1) {
            log = false;
        }
        break;
    default:
        /* This is an error */
        ;
    }

    if (log) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "SASL (severity %d): %s\n", level, message);
    }

    return SASL_OK;
}
#endif

uint16_t arcus_sasl_authz(const char *username)
{
    uint16_t ret = AUTHZ_ALL;

#if defined(ENABLE_SASL) && defined(ENABLE_ZK_INTEGRATION)
    if (use_acl_zookeeper) {
        char value[1024];
        if (arcus_getdata(username, value, sizeof(value)) == SASL_OK) {
            char *saveptr;
            char *token = strtok_r(value, ",", &saveptr);
            ret = AUTHZ_NONE;
            while (token != NULL) {
                if      (strcmp(token, "kv")    == 0) ret |= AUTHZ_KV | AUTHZ_DELETE;
                else if (strcmp(token, "list")  == 0) ret |= AUTHZ_LIST | AUTHZ_DELETE;
                else if (strcmp(token, "set")   == 0) ret |= AUTHZ_SET | AUTHZ_DELETE;
                else if (strcmp(token, "map")   == 0) ret |= AUTHZ_MAP | AUTHZ_DELETE;
                else if (strcmp(token, "btree") == 0) ret |= AUTHZ_BTREE | AUTHZ_DELETE;
                else if (strcmp(token, "scan")  == 0) ret |= AUTHZ_SCAN;
                else if (strcmp(token, "flush") == 0) ret |= AUTHZ_FLUSH;
                else if (strcmp(token, "attr")  == 0) ret |= AUTHZ_ATTR;
                else if (strcmp(token, "admin") == 0) ret |= AUTHZ_ADMIN;
                token = strtok_r(NULL, ",", &saveptr);
            }
        } else {
            ret = AUTHZ_FAIL;
        }
    }
#endif

    return ret;
}

static sasl_callback_t sasl_callbacks[5];

int init_sasl(void)
{
    int i = 0;
#ifdef ENABLE_SASL
    sasl_callbacks[i++] = (sasl_callback_t){ SASL_CB_LOG, (int(*)(void))sasl_log, NULL };

#ifdef ENABLE_SASL_PWDB
    memcached_sasl_pwdb = getenv("MEMCACHED_SASL_PWDB");
    if (memcached_sasl_pwdb == NULL) {
        if (settings.verbose) {
            mc_logger->log(EXTENSION_LOG_INFO, NULL,
                           "INFO: MEMCACHED_SASL_PWDB not specified. "
                           "Internal passwd database disabled\n");
        }
    } else {
        sasl_callbacks[i++] = (sasl_callback_t){ SASL_CB_SERVER_USERDB_CHECKPASS, (int(*)(void))sasl_server_userdb_checkpass, NULL };
    }
#elif defined(ENABLE_ZK_INTEGRATION)
    use_acl_zookeeper = (getenv("ARCUS_ACL_ZOOKEEPER") != NULL);
    if (use_acl_zookeeper) {
        sasl_callbacks[i++] = (sasl_callback_t){ SASL_CB_GETOPT, (int(*)(void))&sasl_getopt, NULL };
    }
#endif

#ifdef HAVE_SASL_CB_GETCONF
    sasl_callbacks[i++] = (sasl_callback_t){ SASL_CB_GETCONF, (int(*)(void))sasl_getconf, NULL },
#else
#ifdef HAVE_SASL_CB_GETCONFPATH
    sasl_callbacks[i++] = (sasl_callback_t){ SASL_CB_GETCONFPATH, (int(*)(void))sasl_getconf, NULL },
#endif
#endif

#endif
    sasl_callbacks[i] = (sasl_callback_t){ SASL_CB_LIST_END, NULL, NULL };

    if (sasl_server_init(sasl_callbacks, "memcached") != SASL_OK) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "Error initializing sasl.\n");
        return -1;
    }

#if defined(ENABLE_SASL) && defined(ENABLE_ZK_INTEGRATION)
    if (use_acl_zookeeper) {
        if (sasl_auxprop_add_plugin("arcus", &arcus_auxprop_plug_init) != SASL_OK) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL, "Error to SASL auxprop plugin.\n");
            return -1;
        }
    }
#endif

    if (settings.verbose) {
        mc_logger->log(EXTENSION_LOG_INFO, NULL, "Initialized SASL.\n");
    }
    return 0;
}

void shutdown_sasl(void)
{
    sasl_done();
}
