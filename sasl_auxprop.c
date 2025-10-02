#include "config.h"

#ifdef ENABLE_SASL
#ifdef ENABLE_ZK_INTEGRATION

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <zookeeper/zookeeper.h>

#include "sasl_auxprop.h"

/* cache table's size */
#define SASL_TABLE_SIZE 16
#define REFRESH_PERIOD 24 * 60 * 60 /* sec */
#define GROUP_MAXLEN 32
#define USERNAME_MAXLEN 32
#define PROPNAME_MAXLEN 32
#define VALUE_MAXLEN 8192 /* from Cyrus SASL's sasldb auxprop plugin */

static EXTENSION_LOGGER_DESCRIPTOR *mc_logger = NULL;

static const char *ensemble_list;
static char group_zpath[16 + GROUP_MAXLEN];

struct sasl_entry {
    struct sasl_entry *next;
    char key[sizeof(group_zpath) + USERNAME_MAXLEN + PROPNAME_MAXLEN];
    size_t value_len;
    char value[];
};

struct sasl_entry **g_sasltable;
static pthread_mutex_t g_sasltable_lock;

/* acl refresh thread */
static volatile bool acl_thread_running = false;
static volatile bool acl_thread_stopreq = false;
static pthread_mutex_t acl_thread_lock;
static pthread_cond_t acl_thread_cond;

static unsigned long hash_function(const char *str)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) hash = ((hash << 5) + hash) + c;
    return hash;
}

static bool _table_insert(struct sasl_entry **table, const char *key,
                          const char* value, int value_len)
{
    if (value_len < 0) {
        return false;
    }
    struct sasl_entry *entry = malloc(sizeof(struct sasl_entry) + value_len);
    if (!entry) {
        return false;
    }

    snprintf(entry->key, sizeof(entry->key), "%s", key);
    entry->value_len = value_len;
    memcpy(entry->value, value, value_len);

    unsigned long idx = hash_function(key) % SASL_TABLE_SIZE;
    entry->next = table[idx];
    table[idx] = entry;

    return true;
}

static void _table_free(struct sasl_entry **table)
{
    struct sasl_entry *entry;
    for (int i = 0; i < SASL_TABLE_SIZE; i++) {
        while (table[i]) {
            entry = table[i];
            table[i] = entry->next;
            free(entry);
        }
    }
    free(table);
}

static struct sasl_entry** get_arcus_acl_table(void)
{
    struct sasl_entry **table = NULL;
    zhandle_t *zh = NULL;
    struct String_vector users;
    struct String_vector props;
    char user_zpath[sizeof(group_zpath) + USERNAME_MAXLEN];
    char prop_zpath[sizeof(user_zpath) + PROPNAME_MAXLEN];
    char value[VALUE_MAXLEN];
    int value_len;
    int ret;

    table = calloc(SASL_TABLE_SIZE, sizeof(struct sasl_entry *));
    if (!table) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "ACL table alloc failed.\n");
        return NULL;
    }

    zh = zookeeper_init(ensemble_list, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        free(table);
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "ACL zookeeper init failed.\n");
        return NULL;
    }

    ret = zoo_get_children(zh, group_zpath, 0, &users);
    if (ret != ZOK) {
        free(table);
        zookeeper_close(zh);
        mc_logger->log(EXTENSION_LOG_WARNING, NULL,
            "ACL zoo_get_children(%s) failed: %s\n", group_zpath, zerror(ret));
        return NULL;
    }

    for (int i = 0; i < users.count; i++) {
        snprintf(user_zpath, sizeof(user_zpath), "%s/%s", group_zpath, users.data[i]);
        value_len = sizeof(value);
        ret = zoo_get(zh, user_zpath, 0, value, &value_len, NULL);
        if (ret != ZOK) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "ACL zoo_get(%s) failed: %s\n", user_zpath, zerror(ret));
            break;
        }
        if (!_table_insert(table, user_zpath, value, value_len)) {
            ret = -1;
            break;
        }

        ret = zoo_get_children(zh, user_zpath, 0, &props);
        if (ret != ZOK) {
            mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                "ACL zoo_get_children(%s) failed: %s\n", user_zpath, zerror(ret));
            break;
        }

        for (int j = 0; j < props.count; j++) {
            snprintf(prop_zpath, sizeof(prop_zpath), "%s/%s", user_zpath, props.data[j]);
            value_len = sizeof(value);
            ret = zoo_get(zh, prop_zpath, 0, value, &value_len, NULL);
            if (ret != ZOK) {
                mc_logger->log(EXTENSION_LOG_WARNING, NULL,
                    "ACL zoo_get(%s) failed: %s\n", prop_zpath, zerror(ret));
                break;
            }
            if (!_table_insert(table, prop_zpath, value, value_len)) {
                ret = -1;
                break;
            }
        }
        deallocate_String_vector(&props);
        if (ret != ZOK) {
            break;
        }
    }
    deallocate_String_vector(&users);
    zookeeper_close(zh);

    if (ret != ZOK) {
        _table_free(table);
        table = NULL;
    }
    return table;
}

static void* acl_refresh_thread(void *arg)
{
    struct timespec ts;
    struct sasl_entry **old_table;
    struct sasl_entry **new_table;

    clock_gettime(CLOCK_REALTIME, &ts);
    srand(ts.tv_sec);
    ts.tv_sec += rand() % REFRESH_PERIOD;

    mc_logger->log(EXTENSION_LOG_INFO, NULL, "ACL refresh thread is running.\n");

    acl_thread_running = true;
    while (!acl_thread_stopreq) {
        ts.tv_sec += REFRESH_PERIOD;
        pthread_mutex_lock(&acl_thread_lock);
        if (!acl_thread_stopreq) {
            pthread_cond_timedwait(&acl_thread_cond, &acl_thread_lock, &ts);
        }
        pthread_mutex_unlock(&acl_thread_lock);
        if (acl_thread_stopreq) {
            break;
        }

        mc_logger->log(EXTENSION_LOG_INFO, NULL, "ACL refresh started.\n");
        new_table = get_arcus_acl_table();
        if (new_table != NULL) {
            pthread_mutex_lock(&g_sasltable_lock);
            old_table = g_sasltable;
            g_sasltable = new_table;
            pthread_mutex_unlock(&g_sasltable_lock);

            _table_free(old_table);
        }
        mc_logger->log(EXTENSION_LOG_INFO, NULL, "ACL refresh %s.\n", new_table ? "completed" : "failed");
    }

    pthread_mutex_lock(&g_sasltable_lock);
    old_table = g_sasltable;
    g_sasltable = NULL;
    pthread_mutex_unlock(&g_sasltable_lock);

    _table_free(old_table);

    acl_thread_running = false;
    mc_logger->log(EXTENSION_LOG_INFO, NULL, "ACL refresh thread is stopped.\n");

    return NULL;
}

static int _arcus_getdata(const char *user,
                          const char *propName,
                          char *out, const size_t max_out,
                          size_t *out_len)
{
    char key[sizeof(group_zpath) + USERNAME_MAXLEN + PROPNAME_MAXLEN];
    if (propName) {
        snprintf(key, sizeof(key), "%s/%s/%s", group_zpath, user, propName);
    } else {
        snprintf(key, sizeof(key), "%s/%s", group_zpath, user);
    }

    int ret = SASL_NOUSER;
    unsigned long index = hash_function(key) % SASL_TABLE_SIZE;
    struct sasl_entry *entry;

    pthread_mutex_lock(&g_sasltable_lock);
    if (g_sasltable) {
        entry = g_sasltable[index];
        while (entry) {
            if (strcmp(entry->key, key) == 0) break;
            entry = entry->next;
        }

        if (entry && entry->value_len <= max_out) {
            memcpy(out, entry->value, entry->value_len);
            if (out_len) *out_len = entry->value_len;
            ret = SASL_OK;
        }
    }
    pthread_mutex_unlock(&g_sasltable_lock);

    return ret;
}

static int arcus_auxprop_lookup(void *glob_context __attribute__((unused)),
                                sasl_server_params_t *sparams,
                                unsigned flags,
                                const char *user,
                                unsigned ulen)
{
    const struct propval *to_fetch, *cur;
    char value[VALUE_MAXLEN];
    size_t value_len;
    bool saw_user_password = false;

    if (!sparams || !user) return SASL_BADPARAM;

    to_fetch = sparams->utils->prop_get(sparams->propctx);
    if (!to_fetch) {
        return SASL_NOMEM;
    }

    int ret = SASL_CONTINUE;
    for (cur = to_fetch; cur->name; cur++) {
        int cur_ret;
        const char *realname = cur->name;

        if (flags & SASL_AUXPROP_AUTHZID) {
            if (cur->name[0] == '*') continue;
        } else {
            if (cur->name[0] != '*') continue;
            else realname = cur->name + 1;
        }

        if (cur->values) {
            if ((flags & SASL_AUXPROP_OVERRIDE) ||
                ((flags & SASL_AUXPROP_VERIFY_AGAINST_HASH) &&
                 strcasecmp(realname, SASL_AUX_PASSWORD_PROP) == 0)) {
                sparams->utils->prop_erase(sparams->propctx, cur->name);
            } else {
                continue;
            }
        }

        if (strcasecmp(realname, SASL_AUX_PASSWORD_PROP) == 0) {
            saw_user_password = true;
        }

        cur_ret = _arcus_getdata(user, realname, value, sizeof(value), &value_len);

        if (ret == SASL_CONTINUE ||
            ret == SASL_NOUSER ||
            (ret == SASL_OK && cur_ret != SASL_NOUSER)) {
            ret = cur_ret;
        }

        if (cur_ret == SASL_OK) {
            sparams->utils->prop_set(sparams->propctx, cur->name, value, (unsigned) value_len);
        }
    }

    /* [Keep in sync with LDAPDB, SQL]
       If ret is SASL_CONTINUE, it means that no properties were requested
       (or maybe some were requested, but they already have values and
       SASL_AUXPROP_OVERRIDE flag is not set).
       Always return SASL_OK in this case. */
    if (ret == SASL_CONTINUE) ret = SASL_OK;

    if (flags & SASL_AUXPROP_AUTHZID) {
        if (ret == SASL_NOUSER) {
            /* This is a lie, but the caller can't handle
               when we return SASL_NOUSER for authorization identity lookup. */
            ret = SASL_OK;
        }
    } else if (ret == SASL_NOUSER && !saw_user_password) {
        ret = _arcus_getdata(user, SASL_AUX_PASSWORD_PROP, value, sizeof(value), &value_len);
    }

    return ret;
}

static void arcus_auxprop_free(void *glob_context,
                               const sasl_utils_t *utils)
{
    pthread_mutex_lock(&acl_thread_lock);
    acl_thread_stopreq = true;
    pthread_cond_signal(&acl_thread_cond);
    pthread_mutex_unlock(&acl_thread_lock);

    /* wait a maximum of 1000 msec */
    int elapsed_msec = 0;
    while (acl_thread_running) {
        usleep(10000); // 10ms wait
        elapsed_msec += 10;
        if (elapsed_msec > 1000)
            break;
    }
}

void arcus_auxprop_init_logger(EXTENSION_LOGGER_DESCRIPTOR *logger)
{
    mc_logger = logger;
}

static sasl_auxprop_plug_t arcus_auxprop_plugin = {
    .name = "arcus",
    .features = 0,
    .spare_int1 = 0,
    .glob_context = NULL,
    .auxprop_lookup = arcus_auxprop_lookup,
    .auxprop_store = NULL,
    .auxprop_free = arcus_auxprop_free
};

/* initialize arcus auxprop plugin (custom) */
int arcus_auxprop_plug_init(const sasl_utils_t *utils,
                            int max_version,
                            int *out_version,
                            sasl_auxprop_plug_t **plug,
                            const char *plugname __attribute__((unused)))
{
    pthread_t tid;

    if (!utils || !out_version || !plug) return SASL_BADPARAM;
    if (max_version < SASL_AUXPROP_PLUG_VERSION) return SASL_BADVERS;

    *out_version = SASL_AUXPROP_PLUG_VERSION;
    *plug = &arcus_auxprop_plugin;

    if (mc_logger == NULL) {
        utils->log(utils->conn, SASL_LOG_ERR, "mc_logger is not set");
        return SASL_FAIL;
    }

    ensemble_list = getenv("ARCUS_ACL_ZOOKEEPER");
    if (ensemble_list == NULL) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "ARCUS_ACL_ZOOKEEPER environment is not set\n");
        return SASL_FAIL;
    }

    const char *acl_group = getenv("ARCUS_ACL_GROUP");
    if (acl_group == NULL) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "ARCUS_ACL_GROUP environment is not set\n");
        return SASL_FAIL;
    }
    snprintf(group_zpath, sizeof(group_zpath), "/arcus_acl/%s", acl_group);

    g_sasltable = get_arcus_acl_table();
    if (!g_sasltable) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "Failed to initialize SASL table\n");
        return SASL_FAIL;
    }

    pthread_mutex_init(&g_sasltable_lock, NULL);
    pthread_mutex_init(&acl_thread_lock, NULL);
    pthread_cond_init(&acl_thread_cond, NULL);

    int ret = pthread_create(&tid, NULL, acl_refresh_thread, NULL);
    if (ret != 0) {
        mc_logger->log(EXTENSION_LOG_WARNING, NULL, "Failed to create ACL refresh thread\n");
        return SASL_FAIL;
    }

    return SASL_OK;
}

int arcus_getdata(const char *user, char *out, const size_t max_out)
{
    return _arcus_getdata(user, NULL, out, max_out, NULL);
}

void arcus_auxprop_wakeup(void)
{
    pthread_mutex_lock(&acl_thread_lock);
    pthread_cond_signal(&acl_thread_cond);
    pthread_mutex_unlock(&acl_thread_lock);
}

#endif /* ENABLE_ZK_INTEGRATION */
#endif /* ENABLE_SASL */
