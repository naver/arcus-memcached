/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2015-2016 JaM2in Co., Ltd.
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

/* node state */
#define NSTATE_JOINING  0
#define NSTATE_LEAVING  1
#define NSTATE_EXISTING 2

/* hash slice state: related to node state */
#define SSTATE_NONE     0
#define SSTATE_LOCAL    1
#define SSTATE_NORMAL   2

/* continuum item */
struct cont_item {
    uint32_t hpoint;  // hash point on the ketama continuum
    uint16_t nindex;  // node index (old server index) in node array
    uint8_t  sindex;  // hash slice index: 0 ~ 159
    uint8_t  sstate;  // hash slice state: 0(none), 1(local), 2(normal)
};

/* node item */
struct node_item {
    char     ndname[MAX_NODE_NAME_LENGTH+1]; // "ip:port" string or group name string
    uint16_t nstate;         // node state: 0(joining), 1(leaving), 2(existing)
    uint16_t refcnt;         // reference count
    uint8_t  dup_hp;         // duplicate hash point exist
    uint8_t  flags;           // node flags
    struct node_item *next;  // next pointer
    struct cont_item hslice[NUM_NODE_HASHES]; // my hash continuum
};

struct cluster_config {
    struct node_item   self_node;   // self node
    int                self_id;     // self index in nodearray.
    uint32_t           free_size;   // number of free node_item entries
    uint32_t           num_conts;   // number of continuum items
    uint32_t           num_nodes;   // number of nodes (used node_item entries)
    struct node_item  *free_list;   // free node list
    struct node_item **nodearray;   // node pointer array
    struct cont_item **continuum;   // continuum of hash slices, that is hash ring

    uint32_t           cur_memlen;  // length of cur_memory
    uint32_t           old_memlen;  // length of old_memory
    void              *cur_memory;  // current memory for nodearray and continuum
    void              *old_memory;  // old     memory for nodearray and continuum

    pthread_mutex_t config_lock;         // config lock
    pthread_mutex_t ketama_lock;         // ketama hashring lock
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

static int compare_node_item_ptr(const void *t1, const void *t2)
{
    const struct node_item **nt1 = (const struct node_item **)t1;
    const struct node_item **nt2 = (const struct node_item **)t2;
    return strcmp((*nt1)->ndname, (*nt2)->ndname);
}

static int compare_cont_item_ptr(const void *t1, const void *t2)
{
    const struct cont_item **ct1 = (const struct cont_item **)t1;
    const struct cont_item **ct2 = (const struct cont_item **)t2;
    if      ((*ct1)->hpoint > (*ct2)->hpoint) return  1;
    else if ((*ct1)->hpoint < (*ct2)->hpoint) return -1;
    else if ((*ct1)->nindex > (*ct2)->nindex) return  1;
    else if ((*ct1)->nindex < (*ct2)->nindex) return -1;
    else                                      return  0;
}

static int compare_cont_item(const void *t1, const void *t2)
{
    const struct cont_item *ct1 = t1, *ct2 = t2;
    if      (ct1->hpoint > ct2->hpoint) return  1;
    else if (ct1->hpoint < ct2->hpoint) return -1;
    else                                return  0;
}

static int gen_node_continuum(struct cont_item *continuum, const char *node_name)
{
    char buffer[MAX_NODE_NAME_LENGTH+1] = "";
    int  length;
    unsigned int  hh, nn, pp;
    unsigned char digest[16];
    int duplicate = 0;

    pp = 0;
    for (hh=0; hh<NUM_OF_HASHES; hh++) {
        length = snprintf(buffer, MAX_NODE_NAME_LENGTH, "%s-%u", node_name, hh);
        hash_md5(buffer, length, digest);
        for (nn=0; nn<NUM_PER_HASH; nn++, pp++) {
            continuum[pp].hpoint = ((uint32_t) (digest[3 + nn * NUM_PER_HASH] & 0xFF) << 24)
                                 | ((uint32_t) (digest[2 + nn * NUM_PER_HASH] & 0xFF) << 16)
                                 | ((uint32_t) (digest[1 + nn * NUM_PER_HASH] & 0xFF) <<  8)
                                 | (           (digest[0 + nn * NUM_PER_HASH] & 0xFF)      );
            /* continuum[pp].nindex : will be set later */
        }
    }

    /* sort the continuum and set the slice index */
    qsort(continuum, NUM_NODE_HASHES, sizeof(struct cont_item), compare_cont_item);

    /* set slice index while checking duplicate hpoint */
    continuum[0].sindex = 0;
    for (pp=1; pp < NUM_NODE_HASHES; pp++) {
        continuum[pp].sindex = pp; /* slice index: 0 ~ 159 */
        if (continuum[pp].hpoint == continuum[pp-1].hpoint)
            duplicate = 1;
    }
    return duplicate;
}

/*
 * Node item management
 */
static int do_node_string_check(char **node_strs, uint32_t num_nodes)
{
    char *buf = NULL;
    char *tok = NULL;

    for (int i=0; i < num_nodes; i++) {
        /* filter characters after dash(-) */
        tok = strtok_r(node_strs[i], "-", &buf);
        if (tok == NULL) return -1;
    }
    return 0;
}

static struct node_item *do_node_item_alloc(struct cluster_config *config)
{
    if (config->free_list) {
        /* allocate it from the free node list */
        struct node_item *item = config->free_list;
        config->free_list = item->next;
        config->free_size -= 1;
        return item;
    }
    return NULL;
}

static void do_node_item_free(struct cluster_config *config, struct node_item *item)
{
    /* link it to the free node list */
    item->next = config->free_list;
    config->free_list = item;
    config->free_size += 1;
}

static int do_node_free_list_prepare(struct cluster_config *config, uint32_t count)
{
    struct node_item *item;
    int rc = 0;

    if ((config->free_size + config->num_nodes) < count) {
        /* We need to grow the free node list */
        int addition = count - (config->free_size + config->num_nodes);
        for (int i = 0; i < addition; i++) {
            item = (struct node_item *)malloc(sizeof(struct node_item));
            if (item == NULL) {
                rc = -1; /* Out of memory */
                break;
            }
            do_node_item_free(config, item);
        }
    }
    return rc;
}

static void do_node_free_list_destroy(struct cluster_config *config)
{
    struct node_item *item;

    while (config->free_list) {
        item = config->free_list;
        config->free_list = item->next;
        free(item);
    }
}

static void do_node_item_set_state(struct node_item *item,
                                   uint8_t node_state, uint8_t slice_state)
{
    item->nstate = node_state;
    for (int i = 0; i < NUM_NODE_HASHES; i++) {
        item->hslice[i].sstate = slice_state;
    }
}

static void do_node_item_init(struct node_item *item, const char *node_name,
                              uint8_t node_state, uint8_t slice_state)
{
    strncpy(item->ndname, node_name, MAX_NODE_NAME_LENGTH);
    item->refcnt = 0;
    item->dup_hp = gen_node_continuum(item->hslice, item->ndname);
    item->flags = 0;
    do_node_item_set_state(item, node_state, slice_state);
}

static void do_self_node_build(struct cluster_config *config, const char *node_name)
{
    struct node_item *item = &config->self_node;
    do_node_item_init(item, node_name, NSTATE_EXISTING, SSTATE_NORMAL);
    if (item->dup_hp) {
        config->logger->log(EXTENSION_LOG_INFO, NULL,
                "[CHECK] Duplicate hash point in self node.\n");
    }
    item->refcnt += 1; /* +1 refcnt for self node not to be freed */
}

static struct node_item *do_node_item_build(struct cluster_config *config,
                                            const char *node_name,
                                            uint8_t node_state, uint8_t slice_state)
{
    struct node_item *item = do_node_item_alloc(config);
    if (item) {
        do_node_item_init(item, node_name, node_state, slice_state);
        if (item->dup_hp) {
            config->logger->log(EXTENSION_LOG_INFO, NULL,
                    "[CHECK] Duplicate hssh point in %s node.\n", node_name);
        }
    }
    return item;
}

/*
 * Hash Ring Space management
 */
static int do_hashring_space_init(struct cluster_config *config, uint32_t num_nodes)
{
    int ret=0;

    do {
        /* init free node list */
        if (do_node_free_list_prepare(config, num_nodes) < 0) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "Failed to init free node list.\n");
            ret = -1; break;
        }

        /* init space for new nodearray and continuum */
        config->old_memlen = (num_nodes * sizeof(void*))
                           + (num_nodes * NUM_NODE_HASHES * sizeof(void*));
        config->old_memory = malloc(config->old_memlen);
        if (config->old_memory == NULL) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "Failed to init old memory.\n");
            ret = -1; break;
        }
        config->cur_memlen = config->old_memlen;
        config->cur_memory = malloc(config->cur_memlen);
        if (config->cur_memory == NULL) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "Failed to init cur memory.\n");
            ret = -1; break;
        }
    } while(0);

    if (ret != 0) {
        do_node_free_list_destroy(config);
        if (config->old_memory) {
            free(config->old_memory);
            config->old_memory = NULL;
        }
    }
    return ret;
}

static int do_hashring_space_prepare(struct cluster_config *config, uint32_t num_nodes)
{
    void    *new_memory;
    uint32_t new_memlen;

    /* prepare free node list */
    if (do_node_free_list_prepare(config, num_nodes) < 0) {
        config->logger->log(EXTENSION_LOG_WARNING, NULL,
                            "Failed to prepare free node list.\n");
        return -1;
    }

    /* prepare space for new nodearray and continuum */
    new_memlen = (num_nodes * sizeof(void*))
               + (num_nodes * NUM_NODE_HASHES * sizeof(void*));
    if (config->old_memlen < new_memlen) {
        if ((new_memory = realloc(config->old_memory, new_memlen)) == NULL) {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "Failed to prepare hash ring space.\n");
            return -1;
        }
        config->old_memory = new_memory;
        config->old_memlen = new_memlen;
    }
    return 0;
}

/*
 * Node Array management
 */
static int do_nodearray_find(struct node_item **array, uint32_t count,
                             const char *node_name)
{
    int left, right, mid, cmp;

    left = 0;
    right = count-1;
    while (left <= right) {
        mid = (left + right) / 2;
        cmp = strcmp(node_name, array[mid]->ndname);
        if (cmp == 0) break;
        if (cmp <  0) right = mid-1;
        else          left  = mid+1;
    }
    return (left <= right ? mid : -1);
}

static void do_nodearray_release(struct cluster_config *config,
                                 struct node_item **array, uint32_t count)
{
    for (int i=0; i < count; i++) {
        array[i]->refcnt -= 1;
        if (array[i]->refcnt == 0) {
            do_node_item_free(config, array[i]);
        }
    }
}

static struct node_item **
do_nodearray_build_for_replace(struct cluster_config *config,
                               char **node_strs, uint32_t num_nodes,
                               bool *is_same, int *self_id)
{
    struct node_item **array;
    struct node_item  *item;
    int id, nfound=0;

    /* initialize is_same flag */
    *is_same = false;

    /* prepare space for nodearray and continuum */
    if (do_hashring_space_prepare(config, num_nodes) < 0) {
        return NULL;
    }

    /* nodearray pointer */
    array = (struct node_item **)config->old_memory;
    assert(array != NULL);

    for (int i=0; i < num_nodes; i++) {
        item = NULL;
        if (config->num_nodes > 0) {
            id = do_nodearray_find(config->nodearray, config->num_nodes, node_strs[i]);
            if (id >= 0) {
                item = config->nodearray[id];
                nfound += 1;
            }
        }
        if (item == NULL) {
            if (strcmp(node_strs[i], config->self_node.ndname) == 0) {
                item = &config->self_node;
            } else {
                /* Following do_node_item_build() is always successful.
                 * Because, free node list is prepared in advance.
                 * See do_hashring_space_prepare()
                 */
                item = do_node_item_build(config, node_strs[i],
                                          NSTATE_EXISTING, SSTATE_NORMAL);
                assert(item != NULL);
            }
        }
        item->refcnt += 1;
        array[i] = item;
    }

    if (num_nodes == config->num_nodes && num_nodes == nfound) {
        do_nodearray_release(config, array, num_nodes);
        *is_same = true; /* the same nodearray */
        return NULL;
    }

    /* sort the nodearray according to node name */
    qsort(array, num_nodes, sizeof(struct node_item*), compare_node_item_ptr);

    /* find the self_node */
    *self_id = do_nodearray_find(array, num_nodes, config->self_node.ndname);

    return array; /* OK */
}

static void do_nodearray_print(struct cluster_config *config)
{
    struct node_item **nodearray = config->nodearray;

    config->logger->log(EXTENSION_LOG_INFO, NULL, "cluster nodearray: count=%d\n",
                        config->num_nodes);
    for (int i=0; i < config->num_nodes; i++) {
        config->logger->log(EXTENSION_LOG_INFO, NULL,
                            "node[%d]: name=%s state=%d\n", i,
                            nodearray[i]->ndname, nodearray[i]->nstate);
    }
}

/*
 * Continuum management
 */
static struct cont_item **
do_continuum_build(struct cluster_config *config, struct node_item **array, uint32_t count)
{
    struct cont_item **continuum = (struct cont_item **)(array + count);
    int i, j, num_conts=0;

    for (i = 0; i < count; i++) {
        for (j = 0; j < NUM_NODE_HASHES; j++) {
            array[i]->hslice[j].nindex = i; /* set the correct node index */
            continuum[num_conts++] = &array[i]->hslice[j];
        }
    }
    qsort(continuum, num_conts, sizeof(struct cont_item *), compare_cont_item_ptr);
    return continuum;
}

static void do_continuum_print(struct cluster_config *config)
{
    struct cont_item **continuum = config->continuum;

    config->logger->log(EXTENSION_LOG_INFO, NULL, "cluster continuum: count=%d\n",
                       config->num_conts);
    for (int i=0; i < config->num_conts; i++) {
        config->logger->log(EXTENSION_LOG_INFO, NULL,
                            "continuum[%d]: hpoint=%x nindex=%d sstate=%d\n", i,
                            continuum[i]->hpoint, continuum[i]->nindex,
                            continuum[i]->sstate);
    }
}

/*
 * Hash Ring management
 */
static void do_hashring_replace(struct cluster_config *config, struct cont_item **continuum,
                                struct node_item **nodearray, uint32_t num_nodes, int self_id)
{
    assert((void*)nodearray == config->old_memory);
    void    *tmp_memory;
    uint32_t tmp_memlen;
    uint32_t old_num_nodes;

    pthread_mutex_lock(&config->ketama_lock);
    /* save old hash ring info */
    tmp_memory = config->cur_memory;
    tmp_memlen = config->cur_memlen;
    old_num_nodes = config->num_nodes;

    /* add new hash ring info */
    config->nodearray = nodearray;
    config->continuum = continuum;
    config->num_nodes = num_nodes;
    config->num_conts = num_nodes * NUM_NODE_HASHES;
    config->self_id = self_id;
    config->is_valid = true;

    /* replace the hash ring memory */
    config->cur_memory = config->old_memory;
    config->cur_memlen = config->old_memlen;
    config->old_memory = tmp_memory;
    config->old_memlen = tmp_memlen;
    pthread_mutex_unlock(&config->ketama_lock);

    if (old_num_nodes > 0) {
        do_nodearray_release(config, config->old_memory, old_num_nodes);
    } else {
        for (int i=1; i < config->num_conts; i++) {
            if (continuum[i-1]->hpoint == continuum[i]->hpoint) {
                config->logger->log(EXTENSION_LOG_INFO, NULL,
                        "[CHECK] Duplicate hash point in (%s:%d) and (%s:%d).\n",
                        nodearray[continuum[i-1]->nindex]->ndname, continuum[i-1]->sindex,
                        nodearray[continuum[i]->nindex]->ndname, continuum[i]->sindex);
            }
        }
    }
}

static struct cont_item *
find_global_continuum(struct cont_item **continuum, uint32_t num_conts, uint32_t hvalue)
{
    struct cont_item **beginp, **endp, **midp, **highp, **lowp;

    beginp = lowp = continuum;
    endp = highp = continuum + num_conts;
    while (lowp < highp)
    {
        midp = lowp + (highp - lowp) / 2;
        if ((*midp)->hpoint < hvalue)
            lowp = midp + 1;
        else
            highp = midp;
    }
    if (highp == endp)
        highp = beginp;
    /* find the first node if duplicate hash points */
    while (highp != beginp && (*highp)->hpoint == (*(highp-1))->hpoint) {
        highp -= 1;
    }
    return (*highp);
}

static struct cont_item *
find_local_continuum(struct cont_item *continuum, uint32_t num_conts, uint32_t hvalue)
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
    return highp;
}

/*
 * External Functions
 */
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

    if (do_hashring_space_init(config, 10) < 0) {
        free(config);
        return NULL;
    }
    do_self_node_build(config, node_name);

    err = pthread_mutex_init(&config->config_lock, NULL);
    assert(err == 0);
    err = pthread_mutex_init(&config->ketama_lock, NULL);
    assert(err == 0);

    config->self_id = -1;
    config->is_valid = false;
    config->logger = logger;
    config->verbose = verbose;
    return config;
}

void cluster_config_final(struct cluster_config *config)
{
    if (config != NULL) {
        if (config->nodearray) {
            do_nodearray_release(config, config->nodearray, config->num_nodes);
            config->nodearray = NULL;
        }
        do_node_free_list_destroy(config);
        if (config->cur_memory) {
            free(config->cur_memory);
            config->cur_memory = NULL;
        }
        if (config->old_memory) {
            free(config->old_memory);
            config->old_memory = NULL;
        }
        free(config);
    }
}

int cluster_config_reconfigure(struct cluster_config *config,
                               char **node_strs, uint32_t num_nodes)
{
    assert(config);
    struct node_item **nodearray;
    struct cont_item **continuum;
    bool is_same;
    int self_id, ret=0;

    if (do_node_string_check(node_strs, num_nodes) < 0) {
        config->logger->log(EXTENSION_LOG_WARNING, NULL,
                            "reconfiguration failed: invalid node token found.\n");
        return -1;
    }

    pthread_mutex_lock(&config->config_lock);
    nodearray = do_nodearray_build_for_replace(config, node_strs, num_nodes,
                                            &is_same, &self_id);
    if (nodearray == NULL) {
        if (is_same) {
            /* the same nodearray : do nothing */
        } else {
            config->logger->log(EXTENSION_LOG_WARNING, NULL,
                                "reconfiguration failed: do_nodearray_build\n");
            config->is_valid = false; ret = -1;
        }
    } else {
        /* build continuuum */
        continuum = do_continuum_build(config, nodearray, num_nodes);
        /* replace hash ring */
        do_hashring_replace(config, continuum, nodearray, num_nodes, self_id);
    }
    pthread_mutex_unlock(&config->config_lock);

    if (config->is_valid && config->verbose > 2) {
        do_nodearray_print(config);
        do_continuum_print(config);
    }
    return ret;
}

int cluster_config_key_is_mine(struct cluster_config *config,
                               const char *key, uint32_t nkey, bool *mine,
                               uint32_t *key_id, uint32_t *self_id)
{
    assert(config && config->continuum);
    struct cont_item *item;
    uint32_t digest;
    int ret = 0;

    pthread_mutex_lock(&config->ketama_lock);
    if (config->is_valid) {
        digest = hash_ketama(key, nkey);
        item = find_global_continuum(config->continuum, config->num_conts, digest);
        *mine = (item->nindex == config->self_id ? true : false);
        if ( key_id)  *key_id = item->nindex;
        if (self_id) *self_id = config->self_id;
    } else { /* this case must not be happened. */
        ret = -1; /* unknown cluster */
    }
    pthread_mutex_unlock(&config->ketama_lock);
    return ret;
}

int cluster_config_ketama_hslice(struct cluster_config *config,
                                 const char *key, uint32_t nkey, uint32_t *hvalue)
{
    assert(config);
    struct cont_item *item;

    *hvalue = hash_ketama(key, nkey);

    item = find_local_continuum(config->self_node.hslice, NUM_NODE_HASHES, *hvalue);
    return item->sindex; /* slice index */
}
