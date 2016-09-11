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

#define MAX_NODE_NAME_LENGTH 127

/* 40 hashes, 4 numbers per hash = 160 hash points per node */
#define NUM_OF_HASHES   40
#define NUM_PER_HASH    4
#define NUM_NODE_HASHES 160

/* continuum item */
struct cont_item {
    uint32_t hpoint;  // hash point on the ketama continuum
    uint32_t nindex;  // node index
};

/* node item */
struct node_item {
    char    *ndname; // "ip:port" string or group name string
};

struct cluster_config {
    uint32_t self_id;            // node index for this memcached
    char    *self_name;          // ip:port string or group name string
    struct cont_item self_continuum[NUM_NODE_HASHES];

    uint32_t num_nodes;          // number of nodes in cluster
    uint32_t num_conts;          // number of continuum items.
    struct node_item *node_list; // node list
    struct cont_item *continuum; // continuum of hash slices, that is hash ring

    pthread_mutex_t lock;                // cluster lock
    EXTENSION_LOGGER_DESCRIPTOR *logger; // memcached logger
    int       verbose;                   // log level
    bool      is_valid;                  // is this configuration valid?
};


static void hash_md5(const char *key, uint32_t nkey, unsigned char *result)
{
    MD5_CTX ctx;

    MD5Init(&ctx);
    MD5Update(&ctx, (unsigned char *)key, nkey);
    MD5Final(result, &ctx);
}

static uint32_t hash_ketama(const char *key, uint32_t nkey)
{
    unsigned char digest[16];

    hash_md5(key, nkey, digest);
    return (uint32_t)((digest[3] << 24)
                     |(digest[2] << 16)
                     |(digest[1] << 8)
                     | digest[0]);
}

static void gen_node_continuum(struct cont_item *continuum,
                               const char *node_name, uint32_t node_index)
{
    char buffer[MAX_NODE_NAME_LENGTH+1] = "";
    int  length;
    int  hh, nn, pp;
    unsigned char digest[16];

    pp = 0;
    for (hh=0; hh<NUM_OF_HASHES; hh++) {
        length = snprintf(buffer, MAX_NODE_NAME_LENGTH, "%s-%u", node_name, hh);
        hash_md5(buffer, length, digest);
        for (nn=0; nn<NUM_PER_HASH; nn++, pp++) {
            continuum[pp].hpoint = ((uint32_t) (digest[3 + nn * NUM_PER_HASH] & 0xFF) << 24)
                                 | ((uint32_t) (digest[2 + nn * NUM_PER_HASH] & 0xFF) << 16)
                                 | ((uint32_t) (digest[1 + nn * NUM_PER_HASH] & 0xFF) <<  8)
                                 | (           (digest[0 + nn * NUM_PER_HASH] & 0xFF)      );
            continuum[pp].nindex = node_index;
        }
    }
}

static int compare_continuum_item(const void *t1, const void *t2)
{
    const struct cont_item *ct1 = t1, *ct2 = t2;
    if      (ct1->hpoint > ct2->hpoint) return  1;
    else if (ct1->hpoint < ct2->hpoint) return -1;
    else                                return  0;
}

static void build_self_continuum(struct cont_item *continuum, const char *self_name)
{
    gen_node_continuum(continuum, self_name, 0);
    qsort(continuum, NUM_NODE_HASHES, sizeof(struct cont_item), compare_continuum_item);

    /* build hash slice index */
    for (int i=0; i < NUM_NODE_HASHES; i++) {
        continuum[i].nindex = i;
        //fprintf(stderr, "continuum[%d] hash=%x\n", i, continuum[i].hpoint);
    }
}

static void node_item_free(struct node_item *node_list, uint32_t num_nodes)
{
    for (int i=0; i < num_nodes; i++) {
        free(node_list[i].ndname);
    }
    free(node_list);
}

static int node_item_populate(struct cluster_config *config,
                              char **node_strs, uint32_t num_nodes, const char *self_name,
                              struct node_item **nodes_ptr, uint32_t *self_id)
{
    assert(*nodes_ptr == NULL);
    struct node_item *node_list;
    char *buf = NULL;
    char *tok = NULL;
    int i, ret = 0;

    node_list = calloc(num_nodes, sizeof(struct node_item));
    if (node_list == NULL) {
        config->logger->log(EXTENSION_LOG_WARNING, NULL, "calloc failed: node_list\n");
        return -1;
    }

    for (i=0; i < num_nodes; i++) {
        // filter characters after dash(-)
        tok = strtok_r(node_strs[i], "-", &buf);
        while (tok != NULL) {
            if ((node_list[i].ndname = strdup(tok)) == NULL) {
                config->logger->log(EXTENSION_LOG_WARNING, NULL, "invalid node token\n");
                ret = -1; break;
            }
            if (strcmp(self_name, tok) == 0) {
                *self_id = i;
            }
            break;
            //tok=strtok_r(NULL, "-", &buf);
        }
        if (ret != 0) break;
    }
    if (ret != 0) {
        node_item_free(node_list, i);
        node_list = NULL;
    }
    *nodes_ptr = node_list;
    return ret;
}

static int ketama_continuum_generate(struct cluster_config *config,
                                     struct node_item *node_list, uint32_t num_nodes,
                                     struct cont_item **continuum_ptr)
{
    struct cont_item *continuum;
    int i, count = num_nodes * NUM_NODE_HASHES;

    if ((continuum = calloc(count, sizeof(struct cont_item))) == NULL) {
        config->logger->log(EXTENSION_LOG_WARNING, NULL, "calloc failed: continuum\n");
        return -1;
    }

    for (i=0; i < num_nodes; i++) {
        gen_node_continuum(&continuum[i*NUM_NODE_HASHES], node_list[i].ndname, i);
    }
    qsort(continuum, count, sizeof(struct cont_item), compare_continuum_item);

    *continuum_ptr = continuum;
    return 0;
}

static void cluster_config_print_node_list(struct cluster_config *config)
{
    config->logger->log(EXTENSION_LOG_INFO, NULL, "cluster node list: count=%d\n",
                        config->num_nodes);
    for (int i=0; i < config->num_nodes; i++) {
        config->logger->log(EXTENSION_LOG_INFO, NULL, "node[%d]: %s\n",
                            i, config->node_list[i].ndname);
    }
}

static void cluster_config_print_continuum(struct cluster_config *config)
{
    config->logger->log(EXTENSION_LOG_INFO, NULL, "cluster continuum: count=%d\n",
                       config->num_conts);
    for (int i=0; i < config->num_conts; i++) {
        config->logger->log(EXTENSION_LOG_INFO, NULL, "continuum[%d]: sidx=%d, hash=%x\n",
                            i, config->continuum[i].nindex, config->continuum[i].hpoint);
    }
}

static uint32_t find_continuum(struct cont_item *continuum, uint32_t num_conts, uint32_t hvalue)
{
    struct cont_item *beginp, *endp, *midp, *highp, *lowp;

    beginp = lowp = continuum;
    endp = highp = continuum + num_conts;
    while (lowp < highp)
    {
        midp = lowp + (highp - lowp) / 2;
        if (midp->hpoint < hvalue)
            lowp = midp + 1;
        else
            highp = midp;
    }
    if (highp == endp)
        highp = beginp;
    return highp->nindex;
#if 0 // OLD_CODE
    uint32_t mid, prev;
    while (1) {
        // pick the middle item
        midp = lowp + (highp - lowp) / 2;

        if (midp == endp) {
            // if at the end, rollback to 0th
            server = beginp->nindex;
            break;
        }

        mid = midp->hpoint;
        prev = (midp == beginp) ? 0 : (midp-1)->hpoint;

        if (digest <= mid && digest > prev) {
            // found the nearest server
            server = midp->nindex;
            break;
        }

        // adjust the limits
        if (mid < digest)     lowp = midp + 1;
        else                 highp = midp - 1;

        if (lowp > highp) {
            server = beginp->nindex;
            break;
        }
    }
    return server;
#endif
}

struct cluster_config *cluster_config_init(const char *node_name,
                                           EXTENSION_LOGGER_DESCRIPTOR *logger,
                                           int verbose)
{
    assert(node_name);
    struct cluster_config *config;
    int err;

    config = calloc(1, sizeof(struct cluster_config));
    if (config == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL, "calloc failed: cluster_config\n");
        return NULL;
    }

    err = pthread_mutex_init(&config->lock, NULL);
    assert(err == 0);

    config->self_name = strdup(node_name);
    build_self_continuum(config->self_continuum, config->self_name);

    config->logger = logger;
    config->verbose = verbose;
    config->is_valid = false;
    return config;
}

void cluster_config_final(struct cluster_config *config)
{
    if (config != NULL) {
        if (config->self_name) {
            free(config->self_name);
            config->self_name = NULL;
        }
        if (config->continuum) {
            free(config->continuum);
            config->continuum = NULL;
        }
        if (config->node_list) {
            node_item_free(config->node_list, config->num_nodes);
            config->node_list = NULL;
        }
        free(config);
    }
}

uint32_t cluster_config_self_id(struct cluster_config *config)
{
    assert(config);
    return config->self_id;
}

uint32_t cluster_config_node_count(struct cluster_config *config)
{
    assert(config);
    return config->num_nodes;
}

uint32_t cluster_config_continuum_size(struct cluster_config *config)
{
    assert(config);
    return config->num_conts;
}

int cluster_config_reconfigure(struct cluster_config *config,
                               char **node_strs, uint32_t num_nodes)
{
    assert(config && node_strs && num_nodes > 0);
    struct node_item *node_list = NULL;
    struct cont_item *continuum = NULL;
    uint32_t self_id = 0;
    int ret = 0;

    do {
        if (node_item_populate(config, node_strs, num_nodes,
                               config->self_name, &node_list, &self_id) < 0) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "reconfiguration failed: node_item_populate\n");
            ret = -1; break;
        }
        if (ketama_continuum_generate(config, node_list, num_nodes, &continuum) < 0) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "reconfiguration failed: ketama_continuum_generate\n");
            node_item_free(node_list, num_nodes);
            node_list = NULL;
            ret = -1; break;
        }
    } while(0);

    pthread_mutex_lock(&config->lock);
    if (ret == 0) {
        if (config->node_list != NULL) {
            node_item_free(config->node_list, config->num_nodes);
            config->node_list = NULL;
        }
        if (config->continuum != NULL) {
            free(config->continuum);
            config->continuum = NULL;
        }
        config->num_nodes = num_nodes;
        config->num_conts = num_nodes * NUM_NODE_HASHES;
        config->node_list = node_list;
        config->continuum = continuum;
        config->self_id = self_id;
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
                               const char *key, uint32_t nkey, bool *mine,
                               uint32_t *key_id, uint32_t *self_id)
{
    assert(config && config->continuum);
    uint32_t server;
    uint32_t digest;
    int ret = 0;

    pthread_mutex_lock(&config->lock);
    if (config->is_valid) {
        digest = hash_ketama(key, nkey);
        server = find_continuum(config->continuum, config->num_conts, digest);
        *mine = (server == config->self_id ? true : false);
        if ( key_id)  *key_id = server;
        if (self_id) *self_id = config->self_id;
    } else { /* this case must not be happened. */
        ret = -1; /* unknown cluster */
    }
    pthread_mutex_unlock(&config->lock);
    return ret;
}

uint32_t cluster_config_ketama_hash(struct cluster_config *config,
                                    const char *key, uint32_t nkey)
{
    assert(config);
    return hash_ketama(key, nkey);
}

uint32_t cluster_config_ketama_hslice(struct cluster_config *config, uint32_t hvalue)
{
    assert(config);
    return (uint32_t)find_continuum(config->self_continuum, NUM_NODE_HASHES, hvalue);
}

/**** OLD CODE ****
uint32_t cluster_config_ketama_hash(struct cluster_config *config,
                                    const char *key, uint32_t nkey, uint32_t *hslice)
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
