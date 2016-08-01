/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <config.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include "cluster_config.h"

#define PROTOTYPES 1
#include "rfc1321/md5c.c"
#undef  PROTOTYPES

#define MAX_NODE_NAME_LENGTH 128

#define NUM_OF_HASHES 40
#define NUM_PER_HASH  4

/* 40 hashes, 4 numbers per hash = 160 hash points per node */
#define NUM_NODE_HASHES (NUM_OF_HASHES * NUM_PER_HASH)

struct server_item {
    char *hostport;
};

struct continuum_item {
    uint32_t index;    // server index
    uint32_t point;    // point on the ketama continuum
};

struct cluster_config {
    uint32_t self_id;                    // server index for this memcached
    char     *self_hostport;             // host:port string for this memcached
    struct continuum_item self_continuum[NUM_NODE_HASHES];

    int      num_servers;                // number of memcached servers in cluster
    int      num_continuum;              // number of continuum
    struct   server_item *servers;       // server list
    struct   continuum_item *continuum;  // continuum list

    pthread_mutex_t lock;                // cluster lock
    EXTENSION_LOGGER_DESCRIPTOR *logger; // memcached logger
    int      verbose;                    // log level
    bool     is_valid;                   // is this configuration valid?
};


static void hash_md5(const char *key, size_t key_length, unsigned char *result)
{
    MD5_CTX ctx;

    MD5Init(&ctx);
    MD5Update(&ctx, (unsigned char *)key, key_length);
    MD5Final(result, &ctx);
}

static uint32_t hash_ketama(const char *key, size_t key_length)
{
    unsigned char digest[16];

    hash_md5(key, key_length, digest);
    return (uint32_t)((digest[3] << 24)
                     |(digest[2] << 16)
                     |(digest[1] << 8)
                     | digest[0]);
}

static int continuum_item_cmp(const void *t1, const void *t2)
{
    const struct continuum_item *ct1 = t1, *ct2 = t2;

    if      (ct1->point == ct2->point) return  0;
    else if (ct1->point  > ct2->point) return  1;
    else                               return -1;
}

static bool ketama_continuum_generate(struct cluster_config *config,
                                      const struct server_item *servers, size_t num_servers,
                                      struct continuum_item **continuum, size_t *continuum_len)
{
    char nodename[MAX_NODE_NAME_LENGTH] = "";
    int  nodenlen;
    int pp, hh, ss, nn;
    unsigned char digest[16];

    *continuum = calloc(NUM_NODE_HASHES * num_servers, sizeof(struct continuum_item));
    if (*continuum == NULL) {
        config->logger->log(EXTENSION_LOG_WARNING, NULL, "calloc failed: continuum\n");
        return false;
    }

    for (ss=0, pp=0; ss<num_servers; ss++) {
        for (hh=0; hh<NUM_OF_HASHES; hh++) {
            nodenlen = snprintf(nodename, MAX_NODE_NAME_LENGTH, "%s-%u", servers[ss].hostport, hh);
            hash_md5(nodename, nodenlen, digest);
            for (nn=0; nn<NUM_PER_HASH; nn++, pp++) {
                (*continuum)[pp].index = ss;
                (*continuum)[pp].point = ((uint32_t) (digest[3 + nn * NUM_PER_HASH] & 0xFF) << 24)
                                       | ((uint32_t) (digest[2 + nn * NUM_PER_HASH] & 0xFF) << 16)
                                       | ((uint32_t) (digest[1 + nn * NUM_PER_HASH] & 0xFF) <<  8)
                                       | (           (digest[0 + nn * NUM_PER_HASH] & 0xFF)      );
            }
        }
    }

    qsort(*continuum, pp, sizeof(struct continuum_item), continuum_item_cmp);
    *continuum_len = pp;
    return true;
}

static void server_item_free(struct server_item *servers, int num_servers)
{
    for (int i=0; i<num_servers; i++) {
        free(servers[i].hostport);
    }
}

static bool server_item_populate(struct cluster_config *config,
                                 char **server_list, size_t num_servers, const char *self_hostport,
                                 struct server_item **servers, uint32_t *self_id)
{
    assert(*servers == NULL);
    int i;

    *servers = calloc(num_servers, sizeof(struct server_item));
    if (*servers == NULL) {
        config->logger->log(EXTENSION_LOG_WARNING, NULL, "calloc failed: servers\n");
        return false;
    }

    for (i=0; i<num_servers; i++) {
        char *buf = NULL;
        char *tok = NULL;

        // filter characters after dash(-)
        for (tok=strtok_r(server_list[i], "-", &buf);
             tok;
             tok=strtok_r(NULL, "-", &buf)) {
            char *hostport = strdup(server_list[i]);
            if (hostport == NULL) {
                config->logger->log(EXTENSION_LOG_WARNING, NULL, "invalid server token\n");
                server_item_free(*servers, i);
                free(*servers); *servers = NULL;
                return false;
            }

            (*servers)[i].hostport = hostport;
            if (strcmp(self_hostport, hostport) == 0) {
                *self_id = i;
            }
            break;
        }
    }
    return true;
}

static void cluster_config_print_node_list(struct cluster_config *config)
{
    assert(config->num_servers > 0 && config->servers != NULL);

    config->logger->log(EXTENSION_LOG_INFO, NULL,
                        "cluster node list: count=%d\n", config->num_servers);
    for (int i = 0; i < config->num_servers; i++) {
        config->logger->log(EXTENSION_LOG_INFO, NULL, "node[%d]: %s\n",
                            i, config->servers[i].hostport);
    }
}

static void cluster_config_print_continuum(struct cluster_config *config)
{
    assert(config->num_continuum > 0 && config->continuum!= NULL);

    config->logger->log(EXTENSION_LOG_INFO, NULL,
                        "cluster continuum: count=%d\n", config->num_continuum);
    for (int i = 0; i < config->num_continuum; i++) {
        config->logger->log(EXTENSION_LOG_INFO, NULL, "continuum[%d]: sidx=%d, hash=%x\n",
                            i, config->continuum[i].index, config->continuum[i].point);
    }
}

static void build_self_continuum(struct continuum_item *continuum, const char *hostport)
{
    char nodename[MAX_NODE_NAME_LENGTH] = "";
    int  nodenlen;
    int  hh, nn, pp;
    unsigned char digest[16];

    /* build sorted hash map */
    pp = 0;
    for (hh=0; hh<NUM_OF_HASHES; hh++) {
        nodenlen= snprintf(nodename, MAX_NODE_NAME_LENGTH, "%s-%u", hostport, hh);
        hash_md5(nodename, nodenlen, digest);
        for (nn=0; nn<NUM_PER_HASH; nn++, pp++) {
            continuum[pp].index = 0;
            continuum[pp].point = ((uint32_t) (digest[3 + nn * NUM_PER_HASH] & 0xFF) << 24)
                                | ((uint32_t) (digest[2 + nn * NUM_PER_HASH] & 0xFF) << 16)
                                | ((uint32_t) (digest[1 + nn * NUM_PER_HASH] & 0xFF) <<  8)
                                | (           (digest[0 + nn * NUM_PER_HASH] & 0xFF)      );
        }
    }
    qsort(continuum, pp, sizeof(struct continuum_item), continuum_item_cmp);

    /* build hash slice index */
    for (pp=0; pp<NUM_NODE_HASHES; pp++) {
        continuum[pp].index = (uint32_t)pp;
        //fprintf(stderr, "continuum[%u] hash=%x\n", continuum[pp].index, continuum[pp].point);
    }
}

static uint32_t find_continuum(struct continuum_item *continuum, size_t continuum_len,
                               uint32_t hash_value)
{
    struct continuum_item *beginp, *endp, *midp, *highp, *lowp;

    beginp = lowp = continuum;
    endp = highp = continuum + continuum_len;
    while (lowp < highp)
    {
        midp = lowp + (highp - lowp) / 2;
        if (midp->point < hash_value)
            lowp = midp + 1;
        else
            highp = midp;
    }
    if (highp == endp)
        highp = beginp;
    return highp->index;
#if 0 // OLD_CODE
    uint32_t mid, prev;
    while (1) {
        // pick the middle point
        midp = lowp + (highp - lowp) / 2;

        if (midp == endp) {
            // if at the end, rollback to 0th
            server = beginp->index;
            break;
        }

        mid = midp->point;
        prev = (midp == beginp) ? 0 : (midp-1)->point;

        if (digest <= mid && digest > prev) {
            // found the nearest server
            server = midp->index;
            break;
        }

        // adjust the limits
        if (mid < digest)     lowp = midp + 1;
        else                 highp = midp - 1;

        if (lowp > highp) {
            server = beginp->index;
            break;
        }
    }
    return server;
#endif
}

struct cluster_config *cluster_config_init(const char *hostport, size_t hostport_len,
                                           EXTENSION_LOGGER_DESCRIPTOR *logger, int verbose)
{
    assert(hostport);
    assert(hostport_len > 0);
    struct cluster_config *config;
    int err;

    config = calloc(1, sizeof(struct cluster_config));
    if (config == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "calloc failed: cluster_config\n");
        return NULL;
    }

    err = pthread_mutex_init(&config->lock, NULL);
    assert(err == 0);

    config->self_hostport = strndup(hostport, hostport_len);
    build_self_continuum(config->self_continuum, config->self_hostport);

    config->logger = logger;
    config->verbose = verbose;
    config->is_valid = false;
    return config;
}

void cluster_config_final(struct cluster_config *config)
{
    if (config != NULL) {
        if (config->self_hostport) {
            free(config->self_hostport);
            config->self_hostport = NULL;
        }
        if (config->continuum) {
            free(config->continuum);
            config->continuum = NULL;
        }
        if (config->servers) {
            server_item_free(config->servers, config->num_servers);
            free(config->servers);
            config->servers = NULL;
        }
        free(config);
    }
}

uint32_t cluster_config_self_id(struct cluster_config *config)
{
    assert(config);
    return config->self_id;
}

int cluster_config_num_servers(struct cluster_config *config)
{
    assert(config);
    return config->num_servers;
}

int cluster_config_num_continuum(struct cluster_config *config)
{
    assert(config);
    return config->num_continuum;
}

int cluster_config_reconfigure(struct cluster_config *config,
                               char **server_list, size_t num_servers)
{
    assert(config);
    assert(server_list);

    uint32_t self_id = 0;
    size_t num_continuum = 0;
    struct server_item *servers = NULL;
    struct continuum_item *continuum = NULL;
    bool populated, generated;
    int ret = 0;

    do {
        populated = server_item_populate(config, server_list, num_servers,
                                         config->self_hostport, &servers, &self_id);
        if (!populated) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "reconfiguration failed: server_item_populate\n");
            ret = -1; break;
        }
        generated = ketama_continuum_generate(config, servers, num_servers,
                                              &continuum, &num_continuum);
        if (!generated) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "reconfiguration failed: ketama_continuum_generate\n");
            server_item_free(servers, num_servers);
            free(servers); servers = NULL;
            ret = -1; break;
        }
    } while(0);

    pthread_mutex_lock(&config->lock);
    if (ret == 0) {
        server_item_free(config->servers, config->num_servers);
        free(config->servers);
        free(config->continuum);

        config->self_id = self_id;
        config->num_servers = num_servers;
        config->servers = servers;
        config->continuum = continuum;
        config->num_continuum = num_continuum;
        config->is_valid = true;
    } else {
        config->is_valid = false;
    }
    pthread_mutex_unlock(&config->lock);

    if (config->is_valid && config->verbose > 2) {
        cluster_config_print_node_list(config);
        cluster_config_print_continuum(config);
    }
    return ret;
}

int cluster_config_key_is_mine(struct cluster_config *config,
                               const char *key, size_t nkey, bool *mine,
                               uint32_t *key_id, uint32_t *self_id)
{
    uint32_t server, self;
    uint32_t digest;
    int ret = 0;

    assert(config);
    assert(config->continuum);

    pthread_mutex_lock(&config->lock);
    if (config->is_valid) {
        /* cluster is valid */
        self = config->self_id;
        digest = hash_ketama(key, nkey);
        server = find_continuum(config->continuum, config->num_continuum, digest);
        *mine = (server == self ? true : false);
        if ( key_id)  *key_id = server;
        if (self_id) *self_id = self;
    } else {
        /* this case should not be happened. */
        ret = -1; /* unknown cluster */
    }
    pthread_mutex_unlock(&config->lock);

    return ret;
}

uint32_t cluster_config_ketama_hash(struct cluster_config *config,
                                    const char *key, size_t nkey)
{
    assert(config);
    return hash_ketama(key, nkey);
}

int cluster_config_ketama_hslice(struct cluster_config *config, uint32_t hvalue)
{
    assert(config);
    return (int)find_continuum(config->self_continuum, NUM_NODE_HASHES, hvalue);
}

/**** OLD CODE ****
uint32_t cluster_config_ketama_hash(struct cluster_config *config,
                                    const char *key, size_t nkey, int *hslice)
{
    assert(config);
    uint32_t digest = hash_ketama(key, nkey);
    if (hslice) {
        *hslice = (int)find_continuum(config->self_continuum, NUM_NODE_HASHES, digest);
        assert(*hslice >= 0 && *hslice < NUM_NODE_HASHES);
    }
    return digest;
}
*******************/
