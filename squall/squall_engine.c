/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2010-2014 NAVER Corp.
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
#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <stddef.h>
#include <inttypes.h>

#include "squall_engine.h"
#include "memcached/util.h"
#include "memcached/config_parser.h"

#include "sq_page.h"
#include "sq_buffer.h"
#include "sq_assoc.h"
#include "sq_items.h"
#include "sq_prefix.h"
#include "sq_log.h"

/*
 * vbucket related structures and function
 */
#define CMD_SET_VBUCKET 0x83
#define CMD_GET_VBUCKET 0x84
#define CMD_DEL_VBUCKET 0x85

union vbucket_info_adapter {
    char c;
    struct vbucket_info v;
};

static void set_vbucket_state(struct squall_engine *e, uint16_t vbid, enum vbucket_state to)
{
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    vi.v.state = to;
    e->vbucket_infos[vbid] = vi.c;
}

static enum vbucket_state get_vbucket_state(struct squall_engine *e, uint16_t vbid)
{
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    return vi.v.state;
}

static bool handled_vbucket(struct squall_engine *e, uint16_t vbid)
{
    return e->config.ignore_vbucket
        || (get_vbucket_state(e, vbid) == VBUCKET_STATE_ACTIVE);
}

/* mechanism for handling bad vbucket requests */
#define VBUCKET_GUARD(e, v) if (!handled_vbucket(e, v)) { return ENGINE_NOT_MY_VBUCKET; }

static const char const * vbucket_state_name(enum vbucket_state s)
{
    static const char const * vbucket_states[] = {
        "dead", "active", "replica", "pending"
    };
    return vbucket_states[s];
}

static void stats_vbucket(struct squall_engine *e, ADD_STAT add_stat, const void *cookie)
{
    for (int i = 0; i < NUM_VBUCKETS; i++) {
        enum vbucket_state state = get_vbucket_state(e, i);
        if (state != VBUCKET_STATE_DEAD) {
            char buf[16];
            snprintf(buf, sizeof(buf), "vb_%d", i);
            const char * state_name = vbucket_state_name(state);
            add_stat(buf, strlen(buf), state_name, strlen(state_name), cookie);
        }
    }
}

/*
 * other static functions
 */
static inline struct squall_engine* get_handle(ENGINE_HANDLE* handle)
{
   return (struct squall_engine*)handle;
}

static inline hash_item* get_real_item(item* item)
{
    return (hash_item*)item;
}

static ENGINE_ERROR_CODE initalize_configuration(struct squall_engine *se, const char *cfg_str)
{
    struct config_item items[] = {
        { .key = "use_cas",          .datatype = DT_BOOL,   .value.dt_bool = &se->config.use_cas },
        { .key = "verbose",          .datatype = DT_SIZE,   .value.dt_size = &se->config.verbose },
        { .key = "eviction",         .datatype = DT_BOOL,   .value.dt_bool = &se->config.evict_to_free },
        { .key = "num_threads",      .datatype = DT_SIZE,   .value.dt_size = &se->config.num_threads },
        { .key = "cache_size",       .datatype = DT_SIZE,   .value.dt_size = &se->config.maxbytes },
        { .key = "sticky_limit",     .datatype = DT_SIZE,   .value.dt_size = &se->config.sticky_limit},
        { .key = "junk_item_time",   .datatype = DT_SIZE,   .value.dt_size = &se->config.junk_item_time },
        { .key = "preallocate",      .datatype = DT_BOOL,   .value.dt_bool = &se->config.preallocate },
        { .key = "factor",           .datatype = DT_FLOAT,  .value.dt_float = &se->config.factor },
        { .key = "chunk_size",       .datatype = DT_SIZE,   .value.dt_size = &se->config.chunk_size },
        { .key = "item_size_max",    .datatype = DT_SIZE,   .value.dt_size = &se->config.item_size_max },
        { .key = "ignore_vbucket",   .datatype = DT_BOOL,   .value.dt_bool = &se->config.ignore_vbucket },
        { .key = "prefix_delimiter", .datatype = DT_CHAR,   .value.dt_char = &se->config.prefix_delimiter },
        { .key = "vb0",              .datatype = DT_BOOL,   .value.dt_bool = &se->config.vb0 },
        { .key = NULL}
    };
    struct config_item sq_items[] = {
        { .key = "config_file",      .datatype = DT_CONFIGFILE },
        { .key = "data_path",        .datatype = DT_STRING, .value.dt_string = &se->config.sq.data_path },
        { .key = "logs_path",        .datatype = DT_STRING, .value.dt_string = &se->config.sq.logs_path },
        //{ .key = "mesg_path",        .datatype = DT_STRING, .value.dt_string = &se->config.sq.mesg_path },
        { .key = "data_file_size",   .datatype = DT_SIZE,   .value.dt_size = &se->config.sq.data_file_size },
        { .key = "data_page_size",   .datatype = DT_SIZE,   .value.dt_size = &se->config.sq.data_page_size },
        { .key = "data_buffer_count", .datatype = DT_SIZE,  .value.dt_size = &se->config.sq.data_buffer_count },
        { .key = "log_file_size",    .datatype = DT_SIZE,   .value.dt_size = &se->config.sq.log_file_size },
        { .key = "log_buffer_size",  .datatype = DT_SIZE,   .value.dt_size = &se->config.sq.log_buffer_size },
        { .key = "chkpt_interval",   .datatype = DT_SIZE,   .value.dt_size = &se->config.sq.chkpt_interval },
        { .key = "tran_commit_mode", .datatype = DT_SIZE,   .value.dt_size = &se->config.sq.tran_commit_mode },
        { .key = "max_prefix_count", .datatype = DT_SIZE,   .value.dt_size = &se->config.sq.max_prefix_count },
        { .key = "use_recovery",     .datatype = DT_BOOL,   .value.dt_bool = &se->config.sq.use_recovery },
        { .key = "use_directio",     .datatype = DT_BOOL,   .value.dt_bool = &se->config.sq.use_directio },
        { .key = "use_dbcheck",      .datatype = DT_BOOL,   .value.dt_bool = &se->config.sq.use_dbcheck },
        { .key = NULL}
    };

    se->config.vb0 = true;

    /* parse engine general config */
    if (cfg_str != NULL) {
        if (se->server.core->parse_config(cfg_str, items, stderr) != 0) {
            return ENGINE_FAILED;
        }
    }
    /* parse squall engine specific config */
    if (se->server.core->parse_config("config_file=./squall/squall_engine.conf", sq_items, stderr) != 0) {
        return ENGINE_ENOCONFFILE;
    }

    if (se->config.vb0) {
        set_vbucket_state(se, 0, VBUCKET_STATE_ACTIVE);
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE check_configuration(struct squall_engine *se)
{
    struct config *conf = &se->config;

    /* check num_threads */
    if (conf->num_threads == 0) {
        fprintf(stderr, "squall engine - num_threads must be positive integer\n");
        return ENGINE_FAILED;
    }

    /* check data page size */
    if (conf->sq.data_page_size < SQUALL_MIN_PAGE_SIZE || conf->sq.data_page_size > SQUALL_MAX_PAGE_SIZE) {
        fprintf(stderr, "squall engine - data_page_size must be within the range of 4096 ~ 65536.\n");
        return ENGINE_FAILED;
    }
    size_t valid_page_size = SQUALL_MIN_PAGE_SIZE;
    while (valid_page_size <= SQUALL_MAX_PAGE_SIZE) {
        if (conf->sq.data_page_size == valid_page_size)
            break;
        valid_page_size *= 2;
    }
    if (valid_page_size > SQUALL_MAX_PAGE_SIZE) {
        fprintf(stderr, "squall engine - data_page_size must be one of 4096, 8192, 16384, 32768, 65536.\n");
        return ENGINE_FAILED;
    }

    /* check data file size */
    if (conf->sq.data_file_size < SQUALL_MIN_FILE_SIZE || conf->sq.data_page_size > SQUALL_MAX_FILE_SIZE) {
        fprintf(stderr, "squall engine - data_filee_size must be within the range of 1024 ~ 8192.\n");
        return ENGINE_FAILED;
    }
    if (conf->sq.data_file_size > conf->sq.data_page_size) {
        /* Page number in a file is represented with 20bits */
        /* So, the max # of pages in a file: 1024 * 1024 */
        /* data_file_size of MB must be smaller than or equal to data_page_size of bytes */
        fprintf(stderr, "squall engine - data_file_size is too large. A file can have max 1 million pages.\n");
        return ENGINE_FAILED;
    }

    return ENGINE_SUCCESS;
}

/*
 * SQUALL ENGINE API
 */
static const engine_info* Squall_get_info(ENGINE_HANDLE* handle)
{
    return &get_handle(handle)->info.engine_info;
}

static ENGINE_ERROR_CODE Squall_initialize(ENGINE_HANDLE* handle, const char* config_str)
{
    struct squall_engine* se = get_handle(handle);
    ENGINE_ERROR_CODE ret;

    ret = initalize_configuration(se, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    /* check some config values */
    ret = check_configuration(se);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    /* fixup feature_info */
    if (se->config.use_cas) {
        se->info.engine_info.features[se->info.engine_info.num_features++].feature = ENGINE_FEATURE_CAS;
    }
    fprintf(stderr, "[INIT] configuration - initialized\n");

    /* initialize page manager */
    page_mgr_init(&se->config, &se->server);

    /* initialize buffer manager */
    if (buf_mgr_init(&se->config, &se->server) != 0) {
        return ENGINE_FAILED;
    }
    fprintf(stderr, "[INIT] buffer manager - initialized\n");

    /* initialize prefix manager */
    if (pfx_mgr_init(&se->config, &se->server) != 0) {
        return ENGINE_FAILED;
    }
    fprintf(stderr, "[INIT] prefix manager - initialized\n");

    /* initialize item manager */
    if (item_mgr_init(&se->config, &se->server) != 0) {
        return ENGINE_FAILED;
    }
    fprintf(stderr, "[INIT] item manager - initialized\n");

    /* initialize assoc manager */
    if (assoc_mgr_init(&se->config, &se->server) != 0) {
        return ENGINE_FAILED;
    }
    fprintf(stderr, "[INIT] assoc manager - initialized\n");

    /* start page flush thread */
    if (buf_mgr_page_flusher_start() != 0) {
        return ENGINE_FAILED;
    }
    fprintf(stderr, "[INIT] daemon thread - buffer page flush thread started\n");

    /* start prefix gc thread */
    if (pfx_mgr_gc_thread_start() != 0) {
        return ENGINE_FAILED;
    }
    fprintf(stderr, "[INIT] daemon thread - prefix gc thread started\n");

    /* start item gc thread */
    if (item_mgr_gc_thread_start() != 0) {
        return ENGINE_FAILED;
    }
    fprintf(stderr, "[INIT] daemon thread - item gc thread started\n");

    if (se->config.sq.use_recovery) {
        /* initialize transaction manager */
        if (tran_mgr_init(&se->config, &se->server) != 0) {
            return ENGINE_FAILED;
        }
        fprintf(stderr, "[INIT] transaction manager - initialized\n");

        /* initialize log manager */
        if (log_mgr_init(&se->config, &se->server) != 0) {
            return ENGINE_FAILED;
        }
        fprintf(stderr, "[INIT] log & recovery manager - initialized\n");

        /* start log flush thread */
        if (log_mgr_flush_thread_start() != 0) {
            return ENGINE_FAILED;
        }
        fprintf(stderr, "[INIT] daemon thread - log flush thread started\n");

        /* do restart recovery */
        log_mgr_restart();

        /* start log chkpt thread */
        if (log_mgr_chkpt_thread_start() != 0) {
            return ENGINE_FAILED;
        }
        fprintf(stderr, "[INIT] daemon thread - checkpoint thread started\n");
    }

    if (se->config.sq.use_dbcheck) { /* check db consistency */
        if (item_dbcheck() != ENGINE_SUCCESS) {
            return ENGINE_FAILED;
        }
    }

    se->initialized = true;
    fprintf(stderr, "[INIT] squall engine initialized\n");
    return ENGINE_SUCCESS;
}

static void Squall_destroy(ENGINE_HANDLE* handle)
{
    struct squall_engine* se = get_handle(handle);
    if (se->initialized) {
        //sleep(30);
        /* final tasks */
        item_mgr_release(); /* IMPORTANT */
        fprintf(stderr, "[FINAL] remaining item references were released\n");
        item_mgr_gc_thread_stop();
        fprintf(stderr, "[FINAL] daemon thread - item gc thread stopped\n");
        pfx_mgr_gc_thread_stop();
        fprintf(stderr, "[FINAL] daemon thread - prefix gc thread stopped\n");

        if (se->config.sq.use_recovery) {
            log_mgr_chkpt_thread_stop();
            fprintf(stderr, "[FINAL] daemon thread - checkpoint thread stopped\n");
            log_mgr_flush_thread_stop();
            fprintf(stderr, "[FINAL] daemon thread - log flush thread stopped\n");
        }

        buf_mgr_page_flusher_stop();
        fprintf(stderr, "[FINAL] daemon thread - buffer page flush thread stopped\n");
        buf_flush_all(-1);
        fprintf(stderr, "[FINAL] all buffer pages were flushed\n");

        if (se->config.sq.use_recovery) {
            log_mgr_checkpoint();
            fprintf(stderr, "[FINAL] the last checkpoint was performed\n");
            log_mgr_final();
            fprintf(stderr, "[FINAL] log & recovery manager - destroyed\n");
            tran_mgr_final();
            fprintf(stderr, "[FINAL] transaction manager - destroyed\n");
        }

        assoc_mgr_final();
        fprintf(stderr, "[FINAL] assoc manager - destroyed\n");
        item_mgr_final();
        fprintf(stderr, "[FINAL] item manager - destroyed\n");
        pfx_mgr_final();
        fprintf(stderr, "[FINAL] prefix manager - destroyed\n");
        buf_mgr_final();
        fprintf(stderr, "[FINAL] buffer manager - destroyed\n");
        page_mgr_final();

        se->initialized = false;
        fprintf(stderr, "[FINAL] squall engine destroyed\n");

        free(se);
    }
}

static ENGINE_ERROR_CODE Squall_item_allocate(ENGINE_HANDLE *handle, const void *cookie, item **item,
                                              const void *key, const size_t nkey, const size_t nbytes,
                                              const int flags, const rel_time_t exptime)
{
    struct squall_engine* engine = get_handle(handle);
    rel_time_t rel_exptime = exptime;
    if (rel_exptime == (rel_time_t)-1) rel_exptime = 0; /* remove sticky setting */

    if ((nkey + nbytes) > engine->config.item_size_max) {
        return ENGINE_E2BIG;
    }
    return item_alloc(key, nkey, nbytes, flags, rel_exptime, (hash_item**)item);
}

static ENGINE_ERROR_CODE Squall_item_delete(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* key, const size_t nkey, uint64_t cas,
                                            uint16_t vbucket)
{
    struct squall_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return item_delete(key, nkey, cas);
}

static void Squall_item_release(ENGINE_HANDLE* handle, const void *cookie, item* item)
{
    item_release(get_real_item(item));
}

static ENGINE_ERROR_CODE Squall_get(ENGINE_HANDLE* handle, const void* cookie,
                                    item** item, const void* key, const int nkey,
                                    uint16_t vbucket)
{
    struct squall_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    hash_item *it;

    ENGINE_ERROR_CODE ret = item_get(key, nkey, &it);
    if (ret == ENGINE_SUCCESS) {
       /* The returned item is of simple kv type. It's guaranteed by item_get(). */
       *item = it;
    }
    /* other return values:
     *    ENGINE_KEY_ENOENT | ENGINE_EBADTYPE | ENGINE_ENOMEM
     *    ENGINE_FAILED
     */
    return ret;
}

/* get stats functions */
static void stats_config(struct squall_engine *engine, ADD_STAT add_stat, const void *cookie)
{
    struct config *config = &engine->config;
    char val[128];
    int  len;

    add_stat("data_path", strlen("data_path"), config->sq.data_path, strlen(config->sq.data_path), cookie);
    add_stat("logs_path", strlen("logs_path"), config->sq.logs_path, strlen(config->sq.logs_path), cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.data_file_size);
    add_stat("data_file_size", strlen("data_file_size"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.data_page_size);
    add_stat("data_page_size", strlen("data_page_size"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.data_buffer_count);
    add_stat("data_buffer_count", strlen("data_buffer_count"), val, len, cookie);
    len = snprintf(val, 127, "%s", (config->sq.use_directio ? "true" : "false"));
    add_stat("use_directio", strlen("use_directio"), val, len, cookie);
    len = snprintf(val, 127, "%s", (config->sq.use_recovery ? "true" : "false"));
    add_stat("use_recovery", strlen("use_recovery"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.log_file_size);
    add_stat("log_file_size", strlen("log_file_size"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.log_buffer_size);
    add_stat("log_buffer_size", strlen("log_buffer_size"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.chkpt_interval);
    add_stat("chkpt_interval", strlen("chkpt_interval"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.tran_commit_mode);
    add_stat("tran_commit_mode", strlen("tran_commit_mode"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, config->sq.max_prefix_count);
    add_stat("max_prefix_count", strlen("max_prefix_count"), val, len, cookie);
    add_stat("use_dbcheck", strlen("use_dbcheck"), val, len, cookie);
    len = snprintf(val, 127, "%s", (config->sq.use_dbcheck ? "true" : "false"));
}

static void stats_items(struct squall_engine *engine, ADD_STAT add_stat, const void *cookie)
{
    struct item_stat stats;
    char val[128];
    int  len;

    item_stats_get(ITEM_STATS_MAIN, &stats);

    len = snprintf(val, 127, "%"PRIu64, stats.evictions);
    add_stat("evictions", 9, val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.reclaimed);
    add_stat("reclaimed", 9, val, len, cookie);
    len = snprintf(val, 127,"%"PRIu64, stats.total_items);
    add_stat("total_items", 11, val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.curr_items);
    add_stat("curr_items", 10, val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.curr_bytes);
    add_stat("bytes", 5, val, len, cookie);
}

static void stats_sizes(struct squall_engine *engine, ADD_STAT add_stat, const void *cookie)
{
    struct item_size stats;

    item_stats_get(ITEM_STATS_SIZE, &stats);

    /* no sats on item sizes */
    assert(stats.num_buckets == 0);
}

static void stats_buffer(struct squall_engine *engine, ADD_STAT add_stat, const void *cookie)
{
    struct buffer_stat stats;
    char val[128];
    int  len;
    uint32_t lru_void_count;

    buf_stat_get(&stats);

    lru_void_count = stats.buffer_used_count - stats.lru_perm_count
                   - stats.lru_warm_count - stats.lru_middle_count
                   - stats.lru_cold_dirty_count - stats.lru_cold_clean_count;

    len = snprintf(val, 127, "%u", stats.hash_table_size);
    add_stat("hash_table_size", strlen("hash_table_size"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.hash_chain_max_size);
    add_stat("hash_chain_max_size", strlen("hash_chain_max_size"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.buffer_used_count);
    add_stat("buffer_used_count", strlen("buffer_used_count"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.buffer_free_count);
    add_stat("buffer_free_count", strlen("buffer_free_count"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.total_fix_count);
    add_stat("buffer_fix_count", strlen("buffer_fix_count"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.total_hit_count);
    add_stat("buffer_hit_count", strlen("buffer_hit_count"), val, len, cookie);
    len = snprintf(val, 127, "%f", (float)stats.total_hit_count/stats.total_fix_count);
    add_stat("buffer_hit_ratio", strlen("buffer_hit_ratio"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.fixed_buffer_count);
    add_stat("fixed_buffer_count", strlen("fixed_buffer_count"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.lru_perm_count);
    add_stat("lru_perm_count", strlen("lru_perm_count"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.lru_warm_count);
    add_stat("lru_warm_count", strlen("lru_warm_count"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.lru_middle_count);
    add_stat("lru_middle_count", strlen("lru_middle_count"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.lru_cold_dirty_count);
    add_stat("lru_cold_dirty_count", strlen("lru_cold_dirty_count"), val, len, cookie);
    len = snprintf(val, 127, "%u", stats.lru_cold_clean_count);
    add_stat("lru_cold_clean_count", strlen("lru_cold_clean_count"), val, len, cookie);
    len = snprintf(val, 127, "%u", lru_void_count);
    add_stat("lru_void_count", strlen("lru_void_count"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.buffer_replace_count);
    add_stat("buffer_replace_count", strlen("buffer_replace_count"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.disk_read_count);
    add_stat("disk_read_count", strlen("disk_read_count"), val, len, cookie);
    len = snprintf(val, 127, "%"PRIu64, stats.disk_write_count);
    add_stat("disk_write_count", strlen("disk_write_count"), val, len, cookie);
}

static inline char *stats_tran_state_text(uint8_t state)
{
    char *str = "unknown";
    switch (state) {
      case TRAN_STATE_FREE:
           str = "free"; break;
      case TRAN_STATE_ALLOC:
           str = "alloc"; break;
      case TRAN_STATE_BEGIN:
           str = "begin"; break;
      case TRAN_STATE_COMMIT:
           str = "commit"; break;
      case TRAN_STATE_ABORT:
           str = "abort"; break;
    }
    return str;
}

static void stats_tran(struct squall_engine *engine, ADD_STAT add_stat, const void *cookie)
{
    struct tran_stat stats;
    char val[128];
    int  len;

    if (engine->config.sq.use_recovery) {
        tran_stat_get(&stats);

        len = snprintf(val, 127, "%"PRIu64, stats.nxt_tranid);
        add_stat("next_tranid", strlen("next_tranid"), val, len, cookie);
        len = snprintf(val, 127, "%u", stats.cur_trans);
        add_stat("cur_tran_count", strlen("cur_tran_count"), val, len, cookie);
        len = snprintf(val, 127, "%u", stats.imp_trans);
        add_stat("imp_tran_count", strlen("imp_tran_count"), val, len, cookie);
        len = snprintf(val, 127, "%u", stats.num_trans);
        add_stat("shw_tran_count", strlen("shw_tran_count"), val, len, cookie);
        for (int i = 0; i < stats.num_trans; i++) {
            len = snprintf(val, 127, "%"PRIu64" [%u|%u] [%u|%u] %s %s",
                           stats.tran_info[i].tranid,
                           stats.tran_info[i].first_lsn.filenum, stats.tran_info[i].first_lsn.roffset,
                           stats.tran_info[i].last_lsn.filenum, stats.tran_info[i].last_lsn.roffset,
                           (stats.tran_info[i].implicit != 0 ? "imp" : "exp"),
                           stats_tran_state_text(stats.tran_info[i].state));
            add_stat("tran", strlen("tran"), val, len, cookie);
        }
    }
}

static void stats_log(struct squall_engine *engine, ADD_STAT add_stat, const void *cookie)
{
    struct log_stat stats;
    char val[128];
    int  len;

    if (engine->config.sq.use_recovery) {
        log_stat_get(&stats);

        len = snprintf(val, 127, "%u|%u", stats.log_write_lsn.filenum, stats.log_write_lsn.roffset);
        add_stat("log_write_lsn ", strlen("log_write_lsn"), val, len, cookie);
        len = snprintf(val, 127, "%u|%u", stats.log_flush_lsn.filenum, stats.log_flush_lsn.roffset);
        add_stat("log_flush_lsn ", strlen("log_flush_lsn"), val, len, cookie);
        len = snprintf(val, 127, "%u|%u", stats.log_fsync_lsn.filenum, stats.log_fsync_lsn.roffset);
        add_stat("log_fsync_lsn ", strlen("log_fsync_lsn"), val, len, cookie);
    }
}

static void stats_scrub(struct squall_engine *engine, ADD_STAT add_stat, const void *cookie)
{
    struct item_scrub stats;
    char val[128];
    int  len;

    item_scrub_get(&stats);

    if (stats.running) {
       add_stat("scrubber:status", 15, "running", 7, cookie);
    } else {
       add_stat("scrubber:status", 15, "stopped", 7, cookie);
    }
    if (stats.started != 0) {
       if (stats.runmode == SCRUB_MODE_NORMAL) {
           add_stat("scrubber:run_mode", 17, "scrub", 5, cookie);
       } else {
           add_stat("scrubber:run_mode", 17, "scrub stale", 11, cookie);
       }
       if (stats.stopped != 0) {
          time_t diff = stats.stopped - stats.started;
          len = sprintf(val, "%"PRIu64, (uint64_t)diff);
          add_stat("scrubber:last_run", 17, val, len, cookie);
       }
       len = sprintf(val, "%"PRIu64, stats.visited);
       add_stat("scrubber:visited", 16, val, len, cookie);
       len = sprintf(val, "%"PRIu64, stats.cleaned);
       add_stat("scrubber:cleaned", 16, val, len, cookie);
    }
}

static ENGINE_ERROR_CODE Squall_get_stats(ENGINE_HANDLE* handle, const void* cookie,
                                           const char* stat_key, int nkey, ADD_STAT add_stat)
{
    struct squall_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (stat_key == NULL) {
        /* no stats on engine */
    }
    else if (strncmp(stat_key, "config", 6) == 0) {
        stats_config(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "slabs", 5) == 0) {
        /* no stats on slabs */
    }
    else if (strncmp(stat_key, "items", 5) == 0) {
        stats_items(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "sizes", 5) == 0) {
        stats_sizes(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "buffer", 6) == 0) {
        stats_buffer(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "tran", 4) == 0) {
        stats_tran(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "log", 3) == 0) {
        stats_log(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "vbucket", 7) == 0) {
        stats_vbucket(engine, add_stat, cookie);
    }
    else if (strncmp(stat_key, "scrub", 5) == 0) {
        stats_scrub(engine, add_stat, cookie);
    }
    else {
        ret = ENGINE_KEY_ENOENT;
    }
    return ret;
}

static void Squall_reset_stats(ENGINE_HANDLE* handle, const void *cookie)
{
    item_stats_reset();
    buf_stat_reset();
}

static ENGINE_ERROR_CODE Squall_get_prefix_stats(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* prefix, const int nprefix,
                                                 void *prefix_stats)
{
#if 1 // ENABLE_SQUALL_ENGINE
    struct squall_engine *engine = get_handle(handle);
    struct prefix_stat  pfx_stat_buf;
    struct prefix_stat *pfx_stats = &pfx_stat_buf;
    struct tm ltm;
    char   tbuf[128];
    char  *sbuf;
    const char *format = "PREFIX %s itm %llu (k=%llu,l=%llu,s=%llu,b=%llu) tsz %llu (k=%llu,l=%llu,s=%llu,b=%llu) ctime %s\r\n";
    int i, count;
    uint32_t tot_prefix_name_len;
    uint32_t msize, pos, written;

    if (nprefix < 0) {
        /* get all prefix stats */
        pfx_stats = malloc(engine->config.sq.max_prefix_count * sizeof(struct prefix_stat));
        if (pfx_stats == NULL) {
            return ENGINE_ENOMEM;
        }
    }

    count = pfx_get_stats(prefix, nprefix, pfx_stats);
    if (count == 0) {
        if (nprefix >= 0) {
            return ENGINE_PREFIX_ENOENT;
        }
    }

    tot_prefix_name_len = 6; /* include "<null>" string in case of null prefix */
    for (i = 0; i < count; i++) {
        tot_prefix_name_len += strlen(pfx_stats[i].name);
    }
    msize = sizeof(uint32_t) /* response string length will be kept */
          + strlen(format) + tot_prefix_name_len
          + (count * (strlen(format) - (2*2) /* %s removed */ + (10*(20-4)) /* %llu replaced by 20-digit num */ + 19 /* create time */))
          + sizeof("END\r\n");

    if ((sbuf = malloc(msize)) == NULL) {
        if (nprefix < 0) free(pfx_stats);
        return ENGINE_ENOMEM;
    }
    pos = sizeof(uint32_t);
    for (i = 0; i < count; i++) {
        localtime_r(&pfx_stats[i].create_time, &ltm);
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", &ltm);
        written = snprintf(sbuf+pos, msize-pos, format,
                   (strlen(pfx_stats[i].name) == 0 ? "<null>" : pfx_stats[i].name),
                   (pfx_stats[i].num_items[0]+pfx_stats[i].num_items[1]+pfx_stats[i].num_items[2]+pfx_stats[i].num_items[3]),
                    pfx_stats[i].num_items[0],pfx_stats[i].num_items[1],pfx_stats[i].num_items[2],pfx_stats[i].num_items[3],
                   (pfx_stats[i].siz_items[0]+pfx_stats[i].siz_items[1]+pfx_stats[i].siz_items[2]+pfx_stats[i].siz_items[3]),
                    pfx_stats[i].siz_items[0],pfx_stats[i].siz_items[1],pfx_stats[i].siz_items[2],pfx_stats[i].siz_items[3],
                    tbuf);
        pos += written;
    }
    memcpy(sbuf+pos, "END\r\n", 6);
    *(uint32_t*)sbuf = pos + 5 - sizeof(uint32_t);

    *(char**)prefix_stats = sbuf;

    if (nprefix < 0) {
        free(pfx_stats);
    }

    return ENGINE_SUCCESS;
#else
#if 0
    struct squall_engine *engine = get_handle(handle);
    return assoc_get_prefix_stats(engine, key, nkey, prefix_data);
#endif
#endif
}

static ENGINE_ERROR_CODE Squall_store(ENGINE_HANDLE* handle, const void *cookie,
                                      item* item, uint64_t *cas, ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket)
{
    struct squall_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    return item_store(get_real_item(item), cas, operation);
}

static ENGINE_ERROR_CODE Squall_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                                           const void *key, const int nkey, const bool increment,
                                           const bool create, const uint64_t delta, const uint64_t initial,
                                           const int flags, const rel_time_t exptime,
                                           uint64_t *cas, uint64_t *result, uint16_t vbucket)
{
    struct squall_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    rel_time_t rel_exptime = engine->server.core->realtime(exptime);
    if (rel_exptime == (rel_time_t)-1) rel_exptime = 0; /* remove sticky setting */

    return item_arithmetic(key, nkey, increment, delta, create, initial, flags, rel_exptime, cas, result);
}

static ENGINE_ERROR_CODE Squall_flush(ENGINE_HANDLE* handle, const void* cookie, time_t when)
{
    return item_flush_prefix(NULL, -1, when);
}

static ENGINE_ERROR_CODE Squall_flush_prefix(ENGINE_HANDLE* handle, const void* cookie,
                                             const void* prefix, const int nprefix, time_t when)
{
    return item_flush_prefix(prefix, nprefix, when);
}

/*
 * List Collection API
 */
static ENGINE_ERROR_CODE Squall_list_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                   const void* key, const int nkey, item_attr *attrp,
                                                   uint16_t vbucket)
{
   struct squall_engine* engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return list_struct_create(engine, key, nkey, attrp, cookie);
#endif
}

static ENGINE_ERROR_CODE Squall_list_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                eitem** eitem, const size_t nbytes)
{
#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   list_elem_item *elem = list_elem_alloc(get_handle(handle), nbytes, cookie);
   if (elem != NULL) {
       *eitem = elem;
       return ENGINE_SUCCESS;
   } else {
       return ENGINE_ENOMEM;
   }
#endif
}

static void Squall_list_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                     eitem **eitem_array, const int eitem_count)
{
#if 1 // ENABLE_SQUALL_ENGINE
#else
   list_elem_release(get_handle(handle), (list_elem_item**)eitem_array, eitem_count);
#endif
}

static ENGINE_ERROR_CODE Squall_list_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey, int index,
                                                 eitem *eitem, item_attr *attrp, bool *created,
                                                 uint16_t vbucket)
{
   struct squall_engine* engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return list_elem_insert(engine, key, nkey, index, (list_elem_item *)eitem,
                           attrp, created, cookie);
#endif
}

static ENGINE_ERROR_CODE Squall_list_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey,
                                                 int from_index, int to_index, const bool drop_if_empty,
                                                 uint32_t* del_count, bool* dropped, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return list_elem_delete(engine, key, nkey, from_index, to_index, drop_if_empty,
                           del_count, dropped);
#endif
}

static ENGINE_ERROR_CODE Squall_list_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                              const void* key, const int nkey,
                                              int from_index, int to_index,
                                              const bool delete, const bool drop_if_empty,
                                              eitem** eitem_array, uint32_t* eitem_count,
                                              uint32_t* flags, bool* dropped, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return list_elem_get(engine, key, nkey, from_index, to_index, delete, drop_if_empty,
                        (list_elem_item**)eitem_array, eitem_count, flags, dropped);
#endif
}

/*
 * Set Collection API
 */
static ENGINE_ERROR_CODE Squall_set_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  item_attr *attrp, uint16_t vbucket)
{
   struct squall_engine* engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return set_struct_create(engine, key, nkey, attrp, cookie);
#endif
}

static ENGINE_ERROR_CODE Squall_set_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                               eitem** eitem, const size_t nbytes)
{
#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   set_elem_item *elem = set_elem_alloc(get_handle(handle), nbytes, cookie);
   if (elem != NULL) {
       *eitem = elem;
       return ENGINE_SUCCESS;
   } else {
       return ENGINE_ENOMEM;
   }
#endif
}

static void Squall_set_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                    eitem **eitem_array, const int eitem_count)
{
#if 1 // ENABLE_SQUALL_ENGINE
#else
   set_elem_release(get_handle(handle), (set_elem_item**)eitem_array, eitem_count);
#endif
}

static ENGINE_ERROR_CODE Squall_set_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                const void* key, const int nkey,
                                                eitem *eitem, item_attr *attrp,
                                                bool *created, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return set_elem_insert(engine, key, nkey, (set_elem_item*)eitem, attrp, created, cookie);
#endif
}

static ENGINE_ERROR_CODE Squall_set_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                const void* key, const int nkey,
                                                const void* value, const int nbytes,
                                                const bool drop_if_empty,
                                                bool *dropped, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return set_elem_delete(engine, key, nkey, value, nbytes, drop_if_empty, dropped);
#endif
}

static ENGINE_ERROR_CODE Squall_set_elem_exist(ENGINE_HANDLE* handle, const void* cookie,
                                               const void* key, const int nkey,
                                               const void* value, const int nbytes,
                                               bool *exist, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return set_elem_exist(engine, key, nkey, value, nbytes, exist);
#endif
}

static ENGINE_ERROR_CODE Squall_set_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                             const void* key, const int nkey, const uint32_t count,
                                             const bool delete, const bool drop_if_empty,
                                             eitem** eitem, uint32_t* eitem_count,
                                             uint32_t* flags, bool* dropped, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return set_elem_get(engine, key, nkey, count, delete, drop_if_empty,
                       (set_elem_item**)eitem, eitem_count, flags, dropped);
#endif
}

/*
 * B+Tree Collection API
 */
static ENGINE_ERROR_CODE Squall_btree_struct_create(ENGINE_HANDLE* handle, const void* cookie,
                                                    const void* key, const int nkey,
                                                    item_attr *attrp, uint16_t vbucket)
{
   struct squall_engine* engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return btree_struct_create(engine, key, nkey, attrp, cookie);
#endif
}

static ENGINE_ERROR_CODE Squall_btree_elem_alloc(ENGINE_HANDLE* handle, const void* cookie,
                                                 eitem** eitem, const size_t nbkey,
                                                 const size_t neflag, const size_t nbytes)
{
#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   btree_elem_item *elem = btree_elem_alloc(get_handle(handle), nbkey, neflag, nbytes, cookie);
   if (elem != NULL) {
       *eitem = elem;
       return ENGINE_SUCCESS;
   } else {
       return ENGINE_ENOMEM;
   }
#endif
}

static void Squall_btree_elem_release(ENGINE_HANDLE* handle, const void *cookie,
                                      eitem **eitem_array, const int eitem_count)
{
#if 1 // ENABLE_SQUALL_ENGINE
#else
   btree_elem_release(get_handle(handle), (btree_elem_item**)eitem_array, eitem_count);
#endif
}

static ENGINE_ERROR_CODE Squall_btree_elem_insert(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  eitem *eitem, const bool replace_if_exist, item_attr *attrp,
                                                  bool *replaced, bool *created, eitem_result *trimmed,
                                                  uint16_t vbucket)
{
    struct squall_engine* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    ENGINE_ERROR_CODE ret;

#if 1 // ENABLE_SQUALL_ENGINE
    ret = ENGINE_ENOTSUP;
#else
    if (trimmed == NULL) {
        ret = btree_elem_insert(engine, key, nkey, (btree_elem_item *)eitem, replace_if_exist, attrp,
                                replaced, created, NULL, NULL, NULL, cookie);
    } else {
        btree_elem_item *trimmed_elem = NULL;
        ret = btree_elem_insert(engine, key, nkey, (btree_elem_item *)eitem, replace_if_exist, attrp,
                                replaced, created, &trimmed_elem,
                                &trimmed->count, &trimmed->flags, cookie);
        trimmed->elems = trimmed_elem;
    }
#endif
    return ret;
}

static ENGINE_ERROR_CODE Squall_btree_elem_update(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  const bkey_range *bkrange,
                                                  const eflag_update *eupdate,
                                                  const void* value, const int nbytes,
                                                  uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return btree_elem_update(engine, key, nkey, bkrange, eupdate, value, nbytes, cookie);
#endif
}

static ENGINE_ERROR_CODE Squall_btree_elem_delete(ENGINE_HANDLE* handle, const void* cookie,
                                                  const void* key, const int nkey,
                                                  const bkey_range *bkrange,
                                                  const eflag_filter *efilter,
                                                  const uint32_t req_count,
                                                  const bool drop_if_empty,
                                                  uint32_t* del_count, bool* dropped,
                                                  uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return btree_elem_delete(engine, key, nkey, bkrange, efilter, req_count, drop_if_empty,
                            del_count, dropped);
#endif
}

static ENGINE_ERROR_CODE Squall_btree_elem_arithmetic(ENGINE_HANDLE* handle, const void* cookie,
                                                      const void* key, const int nkey,
                                                      const bkey_range *bkrange,
                                                      const bool increment, const bool create,
                                                      const uint64_t delta, const uint64_t initial,
                                                      const eflag_t *eflagp,
                                                      uint64_t *result, uint16_t vbucket)
{
    struct squall_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
    return btree_elem_arithmetic(engine, key, nkey, bkrange, increment, create,
                                 delta, initial, eflagp, result, cookie);
#endif
}


static ENGINE_ERROR_CODE Squall_btree_elem_get(ENGINE_HANDLE* handle, const void* cookie,
                                               const void* key, const int nkey,
                                               const bkey_range *bkrange,
                                               const eflag_filter *efilter,
                                               const uint32_t offset,
                                               const uint32_t req_count,
                                               const bool delete, const bool drop_if_empty,
                                               eitem** eitem_array, uint32_t* eitem_count,
                                               uint32_t* flags, bool* dropped_trimmed,
                                               uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return btree_elem_get(engine, key, nkey, bkrange, efilter, offset, req_count,
                         delete, drop_if_empty, (btree_elem_item**)eitem_array, eitem_count,
                         flags, dropped_trimmed);
#endif
}

static ENGINE_ERROR_CODE Squall_btree_elem_count(ENGINE_HANDLE* handle, const void* cookie,
                                                 const void* key, const int nkey,
                                                 const bkey_range *bkrange,
                                                 const eflag_filter *efilter,
                                                 uint32_t* eitem_count, uint32_t* flags,
                                                 uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return btree_elem_count(engine, key, nkey, bkrange, efilter, eitem_count, flags);
#endif
}

static ENGINE_ERROR_CODE Squall_btree_posi_find(ENGINE_HANDLE* handle, const void* cookie,
                                                const char *key, const size_t nkey, const bkey_range *bkrange,
                                                ENGINE_BTREE_ORDER order, int *position, uint16_t vbucket)
{
    struct squall_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
    return ENGINE_ENOTSUP;
#else
    return btree_posi_find(engine, key, nkey, bkrange, order, position);
#endif
}

static ENGINE_ERROR_CODE Squall_btree_elem_get_by_posi(ENGINE_HANDLE* handle, const void* cookie,
                                                const char *key, const size_t nkey,
                                                ENGINE_BTREE_ORDER order, int from_posi, int to_posi,
                                                eitem **eitem_array, uint32_t *eitem_count, uint32_t *flags,
                                                uint16_t vbucket)
{
    struct squall_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
    return ENGINE_ENOTSUP;
#else
    return btree_elem_get_by_posi(engine, key, nkey, order, from_posi, to_posi,
                                  (btree_elem_item**)eitem_array, eitem_count, flags);
#endif
}

#ifdef SUPPORT_BOP_SMGET
static ENGINE_ERROR_CODE Squall_btree_elem_smget(ENGINE_HANDLE* handle, const void* cookie,
                                                 token_t *karray, const int kcount,
                                                 const bkey_range *bkrange,
                                                 const eflag_filter *efilter,
                                                 const uint32_t offset, const uint32_t count,
                                                 eitem** eitem_array,
                                                 uint32_t* kfnd_array,
                                                 uint32_t* flag_array,
                                                 uint32_t* eitem_count,
                                                 uint32_t* missed_key_array,
                                                 uint32_t* missed_key_count,
                                                 bool *trimmed, bool *duplicated,
                                                 uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   return btree_elem_smget(engine, karray, kcount, bkrange, efilter, offset, count,
                           (btree_elem_item**)eitem_array, kfnd_array, flag_array, eitem_count,
                           missed_key_array, missed_key_count, trimmed, duplicated);
#endif
}
#endif


static ENGINE_ERROR_CODE Squall_getattr(ENGINE_HANDLE* handle, const void* cookie,
                                        const void* key, const int nkey,
                                        ENGINE_ITEM_ATTR *attr_ids,
                                        const uint32_t attr_count,
                                        item_attr *attr_data, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

   return item_getattr(key, nkey, attr_ids, attr_count, attr_data);
}

static ENGINE_ERROR_CODE Squall_setattr(ENGINE_HANDLE* handle, const void* cookie,
                                        const void* key, const int nkey,
                                        ENGINE_ITEM_ATTR *attr_ids, const uint32_t attr_count,
                                        item_attr *attr_data, uint16_t vbucket)
{
   struct squall_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

   return item_setattr(key, nkey, attr_ids, attr_count, attr_data);
}

static ENGINE_ERROR_CODE Squall_set_memlimit(ENGINE_HANDLE* handle, const void* cookie,
                                             const size_t memlimit, const int sticky_ratio)
{
#if 1 // ENABLE_SQUALL_ENGINE
   return ENGINE_ENOTSUP;
#else
   ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
   struct squall_engine* engine = get_handle(handle);
   ENGINE_ERROR_CODE ret;

   pthread_mutex_lock(&engine->cache_lock);
   ret = slabs_set_memlimit(engine, memlimit);
   if (ret == ENGINE_SUCCESS) {
       engine->config.maxbytes = memlimit;
#ifdef ENABLE_STICKY_ITEM
       if (sticky_ratio > 0) {
           engine->config.sticky_limit = (memlimit / 100) * sticky_ratio;
       }
#endif
   }
   pthread_mutex_unlock(&engine->cache_lock);
   return ret;
#endif
}

static void Squall_set_junktime(ENGINE_HANDLE* handle, const void* cookie,
                                const size_t junktime)
{
    struct squall_engine* engine = get_handle(handle);

    /* junk_item_time does not give effect in squall engine */
    //pthread_mutex_lock(&engine->cache_lock);
    engine->config.junk_item_time = junktime;
    //pthread_mutex_unlock(&engine->cache_lock);
}

static void Squall_set_verbose(ENGINE_HANDLE* handle, const void* cookie,
                               const size_t verbose)
{
    struct squall_engine* engine = get_handle(handle);

    //pthread_mutex_lock(&engine->cache_lock);
    engine->config.verbose = verbose;
    //pthread_mutex_unlock(&engine->cache_lock);
}

static char *Squall_cachedump(ENGINE_HANDLE* handle, const void* cookie,
                              const unsigned int slabs_clsid, const unsigned int limit,
                              const bool forward, const bool sticky, unsigned int *bytes)
{
#if 1 // ENABLE_SQUALL_ENGINE
    return item_cachedump(NULL, 0, true, 0, 10, bytes);
#else
    struct squall_engine* engine = get_handle(handle);
    return item_cachedump(slabs_clsid, limit, forward, sticky, bytes);
#endif
}

static protocol_binary_response_status
set_vbucket(struct squall_engine *e, protocol_binary_request_header *request, const char **msg)
{
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[32];
    char valz[32];

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    // Read the value.
    size_t bodylen = ntohl(req->message.header.request.bodylen)
        - ntohs(req->message.header.request.keylen);
    if (bodylen >= sizeof(valz)) {
        *msg = "Value is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(valz, (char*)request + sizeof(req->message.header)
           + keylen, bodylen);
    valz[bodylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    *msg = "Configured";

    enum vbucket_state state;
    if (strcmp(valz, "active") == 0) {
        state = VBUCKET_STATE_ACTIVE;
    } else if(strcmp(valz, "replica") == 0) {
        state = VBUCKET_STATE_REPLICA;
    } else if(strcmp(valz, "pending") == 0) {
        state = VBUCKET_STATE_PENDING;
    } else if(strcmp(valz, "dead") == 0) {
        state = VBUCKET_STATE_DEAD;
    } else {
        *msg = "Invalid state.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        set_vbucket_state(e, (uint16_t)vbucket, state);
    }

    return rv;
}

static protocol_binary_response_status
get_vbucket(struct squall_engine *e, protocol_binary_request_header *request, const char **msg)
{
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[8]; // stringy 2^16 int

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg   = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        *msg = vbucket_state_name(get_vbucket_state(e, (uint16_t)vbucket));
    }

    return rv;
}

static protocol_binary_response_status
rm_vbucket(struct squall_engine *e, protocol_binary_request_header *request, const char **msg)
{
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;
    assert(req);

    char keyz[8]; // stringy 2^16 int

    // Read the key.
    int keylen = ntohs(req->message.header.request.keylen);
    if (keylen >= (int)sizeof(keyz)) {
        *msg = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
    keyz[keylen] = 0x00;

    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    uint32_t vbucket = 0;
    if (!safe_strtoul(keyz, &vbucket) || vbucket > NUM_VBUCKETS) {
        *msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        set_vbucket_state(e, (uint16_t)vbucket, VBUCKET_STATE_DEAD);
    }

    assert(msg);
    return rv;
}

static protocol_binary_response_status
scrub_cmd(struct squall_engine *e, protocol_binary_request_header *request, const char **msg)
{
    bool res; /* true or false */
    if (request->request.opcode == PROTOCOL_BINARY_CMD_SCRUB) {
        res = item_scrub_start(SCRUB_MODE_NORMAL);
    } else { /* PROTOCOL_BINARY_CMD_SCRUB_STALE */
#ifdef ENABLE_CLUSTER_AWARE
        if (! e->server.core->is_zk_integrated())
#endif
        {
            return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
        }
        res = item_scrub_start(SCRUB_MODE_STALE);
    }
    return (res) ? PROTOCOL_BINARY_RESPONSE_SUCCESS : PROTOCOL_BINARY_RESPONSE_EBUSY;
}


static ENGINE_ERROR_CODE Squall_unknown_command(ENGINE_HANDLE* handle, const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response)
{
   struct squall_engine* e = get_handle(handle);

    bool handled = true;
    const char *msg = NULL;
    protocol_binary_response_status res =
        PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;

    switch(request->request.opcode) {
    case PROTOCOL_BINARY_CMD_SCRUB:
    case PROTOCOL_BINARY_CMD_SCRUB_STALE:
        res = scrub_cmd(e, request, &msg);
        break;
    case CMD_DEL_VBUCKET:
        res = rm_vbucket(e, request, &msg);
        break;
    case CMD_SET_VBUCKET:
        res = set_vbucket(e, request, &msg);
        break;
    case CMD_GET_VBUCKET:
        res = get_vbucket(e, request, &msg);
        break;
    default:
        handled = false;
        break;
    }

    bool sent = false;
    if (handled) {
        size_t msg_size = msg ? strlen(msg) : 0;
        sent = response(msg, (uint16_t)msg_size, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        (uint16_t)res, 0, cookie);
    } else {
        sent = response(NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0, cookie);
    }

    if (sent) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}


static void Squall_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                                item* item, uint64_t val)
{
    hash_item *it = get_real_item(item);
    item_set_cas(it->desc.ptr, val);
    //item_set_cas(get_real_item(item), val);
}

static bool Squall_get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                                 const item* item, item_info *item_info)
{
    return item_info_get((hash_item*)item, item_info);
}

#if 1 // ENABLE_SQUALL_ENGINE
static void Squall_get_list_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                                      const eitem* eitem, eitem_info *elem_info)
{
}
static void Squall_get_set_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                                     const eitem* eitem, eitem_info *elem_info)
{
}
static void Squall_get_btree_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                                       const eitem* eitem, eitem_info *elem_info)
{
}
#else
#if 0
static void get_list_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                               const eitem* eitem, eitem_info *elem_info)
{
    list_elem_item *elem = (list_elem_item*)eitem;
    elem_info->nbytes = elem->nbytes;
    elem_info->value  = elem->value;
}

static void get_set_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                              const eitem* eitem, eitem_info *elem_info)
{
    set_elem_item *elem = (set_elem_item*)eitem;
    elem_info->nbytes = elem->nbytes;
    elem_info->value  = elem->value;
}

static void get_btree_elem_info(ENGINE_HANDLE *handle, const void *cookie,
                                const eitem* eitem, eitem_info *elem_info)
{
    btree_elem_item *elem = (btree_elem_item*)eitem;
    elem_info->nscore = elem->nbkey;
    elem_info->neflag = elem->neflag;
    elem_info->nbytes = elem->nbytes;
    elem_info->score  = elem->data;
    if (elem->neflag > 0) {
        elem_info->eflag = elem->data + (elem->nbkey == 0 ? sizeof(uint64_t) : elem->nbkey);
        elem_info->value = (const char*)elem_info->eflag + elem->neflag;
    } else {
        elem_info->eflag = NULL;
        elem_info->value = (const char*)elem->data + (elem->nbkey == 0 ? sizeof(uint64_t) : elem->nbkey);
    }
}
#endif
#endif

static tap_event_t tap_always_pause(ENGINE_HANDLE *e,
                                    const void *cookie, item **itm, void **es,
                                    uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                                    uint32_t *seqno, uint16_t *vbucket) {
    return TAP_PAUSE;
}

static tap_event_t tap_always_disconnect(ENGINE_HANDLE *e,
                                         const void *cookie, item **itm, void **es,
                                         uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                                         uint32_t *seqno, uint16_t *vbucket) {
    return TAP_DISCONNECT;
}

static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                     const void* client, size_t nclient,
                                     uint32_t flags,
                                     const void* userdata, size_t nuserdata) {
    TAP_ITERATOR rv = tap_always_pause;
    if ((flags & TAP_CONNECT_FLAG_DUMP)
        || (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS)) {
        rv = tap_always_disconnect;
    }
    return rv;
}

/*
 * PUBLIC API
 */
ENGINE_ERROR_CODE create_instance(uint64_t interface, GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle)
{
   SERVER_HANDLE_V1 *api = get_server_api();
   if (interface != 1 || api == NULL) {
      return ENGINE_ENOTSUP;
   }

   struct squall_engine *engine = malloc(sizeof(*engine));
   if (engine == NULL) {
      return ENGINE_ENOMEM;
   }

   struct squall_engine squall_engine = {
      .engine = {
         .interface = {
            .interface = 1
         },
         .get_info              = Squall_get_info,
         .initialize            = Squall_initialize,
         .destroy               = Squall_destroy,
         .allocate              = Squall_item_allocate,
         .remove                = Squall_item_delete,
         .release               = Squall_item_release,
         .get                   = Squall_get,
         .get_stats             = Squall_get_stats,
         .reset_stats           = Squall_reset_stats,
         .get_prefix_stats      = Squall_get_prefix_stats,
         .store                 = Squall_store,
         .arithmetic            = Squall_arithmetic,
         .flush                 = Squall_flush,
         .flush_prefix          = Squall_flush_prefix,
         /* LIST functions */
         .list_struct_create    = Squall_list_struct_create,
         .list_elem_alloc       = Squall_list_elem_alloc,
         .list_elem_release     = Squall_list_elem_release,
         .list_elem_insert      = Squall_list_elem_insert,
         .list_elem_delete      = Squall_list_elem_delete,
         .list_elem_get         = Squall_list_elem_get,
         /* SET functions */
         .set_struct_create     = Squall_set_struct_create,
         .set_elem_alloc        = Squall_set_elem_alloc,
         .set_elem_release      = Squall_set_elem_release,
         .set_elem_insert       = Squall_set_elem_insert,
         .set_elem_delete       = Squall_set_elem_delete,
         .set_elem_exist        = Squall_set_elem_exist,
         .set_elem_get          = Squall_set_elem_get,
         /* B+Tree functions */
         .btree_struct_create   = Squall_btree_struct_create,
         .btree_elem_alloc      = Squall_btree_elem_alloc,
         .btree_elem_release    = Squall_btree_elem_release,
         .btree_elem_insert     = Squall_btree_elem_insert,
         .btree_elem_update     = Squall_btree_elem_update,
         .btree_elem_delete     = Squall_btree_elem_delete,
         .btree_elem_arithmetic = Squall_btree_elem_arithmetic,
         .btree_elem_get        = Squall_btree_elem_get,
         .btree_elem_count      = Squall_btree_elem_count,
         .btree_posi_find       = Squall_btree_posi_find,
         .btree_elem_get_by_posi = Squall_btree_elem_get_by_posi,
#ifdef SUPPORT_BOP_SMGET
         .btree_elem_smget      = Squall_btree_elem_smget,
#endif
         /* Common LIST/SET functions */
         .getattr               = Squall_getattr,
         .setattr               = Squall_setattr,

         .set_memlimit          = Squall_set_memlimit,
         .set_junktime          = Squall_set_junktime,
         .set_verbose           = Squall_set_verbose,
         .cachedump             = Squall_cachedump,
         .unknown_command       = Squall_unknown_command,

         .item_set_cas          = Squall_item_set_cas,
         .get_item_info         = Squall_get_item_info,
         .get_list_elem_info    = Squall_get_list_elem_info,
         .get_set_elem_info     = Squall_get_set_elem_info,
         .get_btree_elem_info   = Squall_get_btree_elem_info,
         .get_tap_iterator      = get_tap_iterator
      },
      .server = *api,
      .get_server_api = get_server_api,
      .initialized = false,
      .config = {
         /* engine general conf */
         .use_cas = true,
         .verbose = 0,
         .oldest_live = 0,
         .evict_to_free = true,
         .num_threads = 0,
         .maxbytes = 64 * 1024 * 1024,
         .sticky_limit = 0,
         .junk_item_time = 0,
         .preallocate = false,
         .factor = 1.25,
         .chunk_size = 48,
         .item_size_max= 1024 * 1024,
         .prefix_delimiter = ':',

         /* squall engine specific conf */
         .sq.data_path = NULL, /* data directory path */
         .sq.logs_path = NULL, /* logs directory path */
         .sq.mesg_path = NULL, /* mesg directory path [NOT USED] */
         .sq.data_file_size    = 2048,      /* default: 2048 MB */
         .sq.data_page_size    = 4096,      /* default: 4096 Bytes */
         .sq.data_buffer_count = 262144,    /* default: 262144(pages) * 4096(page_size) = 1024 MB */
         .sq.log_file_size     = 2048,      /* default: 2048 MB */
         .sq.log_buffer_size   = 10,        /* default: 10 MB */
         .sq.chkpt_interval    = 1800,      /* default: 1800 seconds */
         .sq.tran_commit_mode  = DISK_COMMIT_EVERY_COMMAND, /* default: disk commit every command */
         .sq.max_prefix_count  = 1024,      /* [INTERNAL USE ONLY] */
         .sq.dbidx             = 0,         /* [INTERNAL USE ONLY] */
         .sq.recovery_phase    = SQUALL_RECOVERY_NONE, /* [INTERNAL USE ONLY] */
         .sq.use_recovery      = true,      /* default: use recovery */
         .sq.use_directio      = true,      /* default: use direct io */
         .sq.use_dbcheck       = false      /* default: do not check db consistency */
       },
      .info.engine_info = {
           .description = "Squall engine v0.1",
           .num_features = 1,
           .features = {
               [0].feature = ENGINE_FEATURE_PERSISTENT_STORAGE
           }
       }
   };

   *engine = squall_engine;
   *handle = (ENGINE_HANDLE*)&engine->engine;
   return ENGINE_SUCCESS;
}
