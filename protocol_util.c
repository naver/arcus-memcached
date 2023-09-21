#include "protocol_util.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

extern union {
    ENGINE_HANDLE *v0;
    ENGINE_HANDLE_V1 *v1;
} mc_engine;
extern struct mc_stats mc_stats;

extern time_t process_started;
extern rel_time_t get_current_time(void);
extern rel_time_t realtime(const time_t exptime);
extern rel_time_t human_readable_time(const rel_time_t exptime);

extern const char *prot_text(enum protocol prot);
extern void perform_callbacks(ENGINE_EVENT_TYPE type,
                              const void *data, const void *c);
extern int add_iov(conn *c, const void *buf, int len);
extern int build_udp_headers(conn *c);
extern void out_string(conn *c, const char *str);
extern void write_bin_packet(conn *c, protocol_binary_response_status err, int swallow);

void ritem_set_first(conn *c, int rtype, int vleng)
{
    c->rtype = rtype;

    if (c->rtype == CONN_RTYPE_MBLCK) {
        c->membk = MBLCK_GET_HEADBLK(&c->memblist);
        c->ritem = MBLCK_GET_BODYPTR(c->membk);
        c->rlbytes = vleng < MBLCK_GET_BODYLEN(&c->memblist)
                   ? vleng : MBLCK_GET_BODYLEN(&c->memblist);
        c->rltotal = vleng;
    }
    else if (c->rtype == CONN_RTYPE_HINFO) {
        if (c->hinfo.naddnl == 0) {
            c->ritem = (char*)c->hinfo.value;
            c->rlbytes = vleng;
            c->rltotal = 0;
        } else {
            if (c->hinfo.nvalue > 0) {
                c->ritem = (char*)c->hinfo.value;
                c->rlbytes = vleng < c->hinfo.nvalue
                           ? vleng : c->hinfo.nvalue;
                c->rindex = 0;
            } else {
                c->ritem = c->hinfo.addnl[0]->ptr;
                c->rlbytes = vleng < c->hinfo.addnl[0]->len
                           ? vleng : c->hinfo.addnl[0]->len;
                c->rindex = 1;
            }
            c->rltotal = vleng;
        }
    }
    else if (c->rtype == CONN_RTYPE_EINFO) {
        if (c->einfo.naddnl == 0) {
            c->ritem = (char*)c->einfo.value;
            c->rlbytes = vleng;
            c->rltotal = 0;
        } else {
            if (c->einfo.nvalue > 0) {
                c->ritem = (char*)c->einfo.value;
                c->rlbytes = vleng < c->einfo.nvalue
                           ? vleng : c->einfo.nvalue;
                c->rindex = 0;
            } else {
                c->ritem = c->einfo.addnl[0]->ptr;
                c->rlbytes = vleng < c->einfo.addnl[0]->len
                           ? vleng : c->einfo.addnl[0]->len;
                c->rindex = 1;
            }
            c->rltotal = vleng;
        }
    }
}

static void aggregate_callback(void *in, void *out)
{
    struct thread_stats *out_thread_stats = out;
    struct thread_stats *in_thread_stats = in;
    threadlocal_stats_aggregate(in_thread_stats, out_thread_stats);
}

/* return server specific stats only */
void server_stats(ADD_STAT add_stats, conn *c, bool aggregate)
{
    pid_t pid = getpid();
    rel_time_t now = get_current_time();

    struct thread_stats thread_stats;
    threadlocal_stats_clear(&thread_stats);

    if (aggregate && mc_engine.v1->aggregate_stats != NULL) {
        mc_engine.v1->aggregate_stats(mc_engine.v0, (const void *)c,
                                      aggregate_callback, &thread_stats);
    } else {
        threadlocal_stats_aggregate(default_thread_stats, &thread_stats);
    }

#ifndef __WIN32__
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
#endif

#ifdef ENABLE_ZK_INTEGRATION
    arcus_hb_stats hb_stats;
    arcus_hb_get_stats(&hb_stats);
#endif

    LOCK_STATS();

    APPEND_STAT("pid", "%lu", (long)pid);
    APPEND_STAT("uptime", "%u", now);
    APPEND_STAT("time", "%ld", now + (long)process_started);
    APPEND_STAT("version", "%s", VERSION);
    APPEND_STAT("libevent", "%s", event_get_version());
    APPEND_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));
#ifdef ENABLE_ZK_INTEGRATION
    APPEND_STAT("hb_count", "%"PRIu64, hb_stats.count);
    APPEND_STAT("hb_latency", "%"PRIu64, hb_stats.latency);
#endif

#ifndef __WIN32__
    append_stat("rusage_user", add_stats, c, "%ld.%06ld",
                (long)usage.ru_utime.tv_sec,
                (long)usage.ru_utime.tv_usec);
    append_stat("rusage_system", add_stats, c, "%ld.%06ld",
                (long)usage.ru_stime.tv_sec,
                (long)usage.ru_stime.tv_usec);
#endif

    APPEND_STAT("daemon_connections", "%u", mc_stats.daemon_conns);
    APPEND_STAT("curr_connections", "%u", mc_stats.curr_conns);
    APPEND_STAT("quit_connections", "%u", mc_stats.quit_conns);
    APPEND_STAT("reject_connections", "%u", mc_stats.rejected_conns);
    APPEND_STAT("total_connections", "%u", mc_stats.total_conns);
    APPEND_STAT("connection_structures", "%u", mc_stats.conn_structs);
    APPEND_STAT("cmd_get", "%"PRIu64, thread_stats.cmd_get);
    APPEND_STAT("cmd_set", "%"PRIu64, thread_stats.cmd_set);
    APPEND_STAT("cmd_incr", "%"PRIu64, thread_stats.cmd_incr);
    APPEND_STAT("cmd_decr", "%"PRIu64, thread_stats.cmd_decr);
    APPEND_STAT("cmd_delete", "%"PRIu64, thread_stats.cmd_delete);
    APPEND_STAT("cmd_cas", "%"PRIu64, thread_stats.cmd_cas);
    APPEND_STAT("cmd_flush", "%"PRIu64, thread_stats.cmd_flush);
    APPEND_STAT("cmd_flush_prefix", "%"PRIu64, thread_stats.cmd_flush_prefix);
    APPEND_STAT("cmd_auth", "%"PRIu64, thread_stats.cmd_auth);
    APPEND_STAT("cmd_lop_create", "%"PRIu64, thread_stats.cmd_lop_create);
    APPEND_STAT("cmd_lop_insert", "%"PRIu64, thread_stats.cmd_lop_insert);
    APPEND_STAT("cmd_lop_delete", "%"PRIu64, thread_stats.cmd_lop_delete);
    APPEND_STAT("cmd_lop_get", "%"PRIu64, thread_stats.cmd_lop_get);
    APPEND_STAT("cmd_sop_create", "%"PRIu64, thread_stats.cmd_sop_create);
    APPEND_STAT("cmd_sop_insert", "%"PRIu64, thread_stats.cmd_sop_insert);
    APPEND_STAT("cmd_sop_delete", "%"PRIu64, thread_stats.cmd_sop_delete);
    APPEND_STAT("cmd_sop_get", "%"PRIu64, thread_stats.cmd_sop_get);
    APPEND_STAT("cmd_sop_exist", "%"PRIu64, thread_stats.cmd_sop_exist);
    APPEND_STAT("cmd_mop_create", "%"PRIu64, thread_stats.cmd_mop_create);
    APPEND_STAT("cmd_mop_insert", "%"PRIu64, thread_stats.cmd_mop_insert);
    APPEND_STAT("cmd_mop_update", "%"PRIu64, thread_stats.cmd_mop_update);
    APPEND_STAT("cmd_mop_delete", "%"PRIu64, thread_stats.cmd_mop_delete);
    APPEND_STAT("cmd_mop_get", "%"PRIu64, thread_stats.cmd_mop_get);
    APPEND_STAT("cmd_bop_create", "%"PRIu64, thread_stats.cmd_bop_create);
    APPEND_STAT("cmd_bop_insert", "%"PRIu64, thread_stats.cmd_bop_insert);
    APPEND_STAT("cmd_bop_update", "%"PRIu64, thread_stats.cmd_bop_update);
    APPEND_STAT("cmd_bop_delete", "%"PRIu64, thread_stats.cmd_bop_delete);
    APPEND_STAT("cmd_bop_get", "%"PRIu64, thread_stats.cmd_bop_get);
    APPEND_STAT("cmd_bop_count", "%"PRIu64, thread_stats.cmd_bop_count);
    APPEND_STAT("cmd_bop_position", "%"PRIu64, thread_stats.cmd_bop_position);
    APPEND_STAT("cmd_bop_pwg", "%"PRIu64, thread_stats.cmd_bop_pwg);
    APPEND_STAT("cmd_bop_gbp", "%"PRIu64, thread_stats.cmd_bop_gbp);
#ifdef SUPPORT_BOP_MGET
    APPEND_STAT("cmd_bop_mget", "%"PRIu64, thread_stats.cmd_bop_mget);
#endif
#ifdef SUPPORT_BOP_SMGET
    APPEND_STAT("cmd_bop_smget", "%"PRIu64, thread_stats.cmd_bop_smget);
#endif
    APPEND_STAT("cmd_bop_incr", "%"PRIu64, thread_stats.cmd_bop_incr);
    APPEND_STAT("cmd_bop_decr", "%"PRIu64, thread_stats.cmd_bop_decr);
    APPEND_STAT("cmd_getattr", "%"PRIu64, thread_stats.cmd_getattr);
    APPEND_STAT("cmd_setattr", "%"PRIu64, thread_stats.cmd_setattr);
    APPEND_STAT("get_hits", "%"PRIu64, thread_stats.get_hits);
    APPEND_STAT("get_misses", "%"PRIu64, thread_stats.get_misses);
    APPEND_STAT("incr_hits", "%"PRIu64, thread_stats.incr_hits);
    APPEND_STAT("incr_misses", "%"PRIu64, thread_stats.incr_misses);
    APPEND_STAT("decr_hits", "%"PRIu64, thread_stats.decr_hits);
    APPEND_STAT("decr_misses", "%"PRIu64, thread_stats.decr_misses);
    APPEND_STAT("delete_hits", "%"PRIu64, thread_stats.delete_hits);
    APPEND_STAT("delete_misses", "%"PRIu64, thread_stats.delete_misses);
    APPEND_STAT("cas_hits", "%"PRIu64, thread_stats.cas_hits);
    APPEND_STAT("cas_badval", "%"PRIu64, thread_stats.cas_badval);
    APPEND_STAT("cas_misses", "%"PRIu64, thread_stats.cas_misses);
    APPEND_STAT("auth_errors", "%"PRIu64, thread_stats.auth_errors);
    APPEND_STAT("lop_create_oks", "%"PRIu64, thread_stats.lop_create_oks);
    APPEND_STAT("lop_insert_misses", "%"PRIu64, thread_stats.lop_insert_misses);
    APPEND_STAT("lop_insert_hits", "%"PRIu64, thread_stats.lop_insert_hits);
    APPEND_STAT("lop_delete_misses", "%"PRIu64, thread_stats.lop_delete_misses);
    APPEND_STAT("lop_delete_elem_hits", "%"PRIu64, thread_stats.lop_delete_elem_hits);
    APPEND_STAT("lop_delete_none_hits", "%"PRIu64, thread_stats.lop_delete_none_hits);
    APPEND_STAT("lop_get_misses", "%"PRIu64, thread_stats.lop_get_misses);
    APPEND_STAT("lop_get_elem_hits", "%"PRIu64, thread_stats.lop_get_elem_hits);
    APPEND_STAT("lop_get_none_hits", "%"PRIu64, thread_stats.lop_get_none_hits);
    APPEND_STAT("sop_create_oks", "%"PRIu64, thread_stats.sop_create_oks);
    APPEND_STAT("sop_insert_misses", "%"PRIu64, thread_stats.sop_insert_misses);
    APPEND_STAT("sop_insert_hits", "%"PRIu64, thread_stats.sop_insert_hits);
    APPEND_STAT("sop_delete_misses", "%"PRIu64, thread_stats.sop_delete_misses);
    APPEND_STAT("sop_delete_elem_hits", "%"PRIu64, thread_stats.sop_delete_elem_hits);
    APPEND_STAT("sop_delete_none_hits", "%"PRIu64, thread_stats.sop_delete_none_hits);
    APPEND_STAT("sop_get_misses", "%"PRIu64, thread_stats.sop_get_misses);
    APPEND_STAT("sop_get_elem_hits", "%"PRIu64, thread_stats.sop_get_elem_hits);
    APPEND_STAT("sop_get_none_hits", "%"PRIu64, thread_stats.sop_get_none_hits);
    APPEND_STAT("sop_exist_misses", "%"PRIu64, thread_stats.sop_exist_misses);
    APPEND_STAT("sop_exist_hits", "%"PRIu64, thread_stats.sop_exist_hits);
    APPEND_STAT("mop_create_oks", "%"PRIu64, thread_stats.mop_create_oks);
    APPEND_STAT("mop_insert_misses", "%"PRIu64, thread_stats.mop_insert_misses);
    APPEND_STAT("mop_insert_hits", "%"PRIu64, thread_stats.mop_insert_hits);
    APPEND_STAT("mop_update_misses", "%"PRIu64, thread_stats.mop_update_misses);
    APPEND_STAT("mop_update_elem_hits", "%"PRIu64, thread_stats.mop_update_elem_hits);
    APPEND_STAT("mop_update_none_hits", "%"PRIu64, thread_stats.mop_update_none_hits);
    APPEND_STAT("mop_delete_misses", "%"PRIu64, thread_stats.mop_delete_misses);
    APPEND_STAT("mop_delete_elem_hits", "%"PRIu64, thread_stats.mop_delete_elem_hits);
    APPEND_STAT("mop_delete_none_hits", "%"PRIu64, thread_stats.mop_delete_none_hits);
    APPEND_STAT("mop_get_misses", "%"PRIu64, thread_stats.mop_get_misses);
    APPEND_STAT("mop_get_elem_hits", "%"PRIu64, thread_stats.mop_get_elem_hits);
    APPEND_STAT("mop_get_none_hits", "%"PRIu64, thread_stats.mop_get_none_hits);
    APPEND_STAT("bop_create_oks", "%"PRIu64, thread_stats.bop_create_oks);
    APPEND_STAT("bop_insert_misses", "%"PRIu64, thread_stats.bop_insert_misses);
    APPEND_STAT("bop_insert_hits", "%"PRIu64, thread_stats.bop_insert_hits);
    APPEND_STAT("bop_update_misses", "%"PRIu64, thread_stats.bop_update_misses);
    APPEND_STAT("bop_update_elem_hits", "%"PRIu64, thread_stats.bop_update_elem_hits);
    APPEND_STAT("bop_update_none_hits", "%"PRIu64, thread_stats.bop_update_none_hits);
    APPEND_STAT("bop_delete_misses", "%"PRIu64, thread_stats.bop_delete_misses);
    APPEND_STAT("bop_delete_elem_hits", "%"PRIu64, thread_stats.bop_delete_elem_hits);
    APPEND_STAT("bop_delete_none_hits", "%"PRIu64, thread_stats.bop_delete_none_hits);
    APPEND_STAT("bop_get_misses", "%"PRIu64, thread_stats.bop_get_misses);
    APPEND_STAT("bop_get_elem_hits", "%"PRIu64, thread_stats.bop_get_elem_hits);
    APPEND_STAT("bop_get_none_hits", "%"PRIu64, thread_stats.bop_get_none_hits);
    APPEND_STAT("bop_count_misses", "%"PRIu64, thread_stats.bop_count_misses);
    APPEND_STAT("bop_count_hits", "%"PRIu64, thread_stats.bop_count_hits);
    APPEND_STAT("bop_position_misses", "%"PRIu64, thread_stats.bop_position_misses);
    APPEND_STAT("bop_position_elem_hits", "%"PRIu64, thread_stats.bop_position_elem_hits);
    APPEND_STAT("bop_position_none_hits", "%"PRIu64, thread_stats.bop_position_none_hits);
    APPEND_STAT("bop_pwg_misses", "%"PRIu64, thread_stats.bop_pwg_misses);
    APPEND_STAT("bop_pwg_elem_hits", "%"PRIu64, thread_stats.bop_pwg_elem_hits);
    APPEND_STAT("bop_pwg_none_hits", "%"PRIu64, thread_stats.bop_pwg_none_hits);
    APPEND_STAT("bop_gbp_misses", "%"PRIu64, thread_stats.bop_gbp_misses);
    APPEND_STAT("bop_gbp_elem_hits", "%"PRIu64, thread_stats.bop_gbp_elem_hits);
    APPEND_STAT("bop_gbp_none_hits", "%"PRIu64, thread_stats.bop_gbp_none_hits);
#ifdef SUPPORT_BOP_MGET
    APPEND_STAT("bop_mget_oks", "%"PRIu64, thread_stats.bop_mget_oks);
#endif
#ifdef SUPPORT_BOP_SMGET
    APPEND_STAT("bop_smget_oks", "%"PRIu64, thread_stats.bop_smget_oks);
#endif
    APPEND_STAT("bop_incr_elem_hits", "%"PRIu64, thread_stats.bop_incr_elem_hits);
    APPEND_STAT("bop_incr_none_hits", "%"PRIu64, thread_stats.bop_incr_none_hits);
    APPEND_STAT("bop_incr_misses", "%"PRIu64, thread_stats.bop_incr_misses);
    APPEND_STAT("bop_decr_elem_hits", "%"PRIu64, thread_stats.bop_decr_elem_hits);
    APPEND_STAT("bop_decr_none_hits", "%"PRIu64, thread_stats.bop_decr_none_hits);
    APPEND_STAT("bop_decr_misses", "%"PRIu64, thread_stats.bop_decr_misses);
    APPEND_STAT("getattr_misses", "%"PRIu64, thread_stats.getattr_misses);
    APPEND_STAT("getattr_hits", "%"PRIu64, thread_stats.getattr_hits);
    APPEND_STAT("setattr_misses", "%"PRIu64, thread_stats.setattr_misses);
    APPEND_STAT("setattr_hits", "%"PRIu64, thread_stats.setattr_hits);
    APPEND_STAT("stat_prefixes", "%"PRIu64, stats_prefix_count());
    APPEND_STAT("bytes_read", "%"PRIu64, thread_stats.bytes_read);
    APPEND_STAT("bytes_written", "%"PRIu64, thread_stats.bytes_written);
    APPEND_STAT("limit_maxbytes", "%"PRIu64, settings.maxbytes);
    APPEND_STAT("limit_maxconns", "%d", settings.maxconns);
    APPEND_STAT("threads", "%d", settings.num_threads);
    APPEND_STAT("conn_yields", "%"PRIu64, thread_stats.conn_yields);
    UNLOCK_STATS();
}

void process_stats_settings(ADD_STAT add_stats, void *c)
{
    assert(add_stats);
#ifdef ENABLE_ZK_INTEGRATION
    arcus_hb_confs hb_confs;
    arcus_hb_get_confs(&hb_confs);
#endif
    APPEND_STAT("maxbytes", "%llu", (unsigned long long)settings.maxbytes);
    APPEND_STAT("maxconns", "%d", settings.maxconns);
    APPEND_STAT("tcpport", "%d", settings.port);
    APPEND_STAT("udpport", "%d", settings.udpport);
    APPEND_STAT("sticky_limit", "%llu", (unsigned long long)settings.sticky_limit);
    APPEND_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
    APPEND_STAT("verbosity", "%d", settings.verbose);
    APPEND_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
    APPEND_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
    APPEND_STAT("domain_socket", "%s",
                settings.socketpath ? settings.socketpath : "NULL");
    APPEND_STAT("umask", "%o", settings.access);
    APPEND_STAT("growth_factor", "%.2f", settings.factor);
    APPEND_STAT("chunk_size", "%d", settings.chunk_size);
    APPEND_STAT("num_threads", "%d", settings.num_threads);
    APPEND_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
    APPEND_STAT("detail_enabled", "%s",
                settings.detail_enabled ? "yes" : "no");
    APPEND_STAT("allow_detailed", "%s",
                settings.allow_detailed ? "yes" : "no");
    APPEND_STAT("reqs_per_event", "%d", settings.reqs_per_event);
    APPEND_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
    APPEND_STAT("tcp_backlog", "%d", settings.backlog);
    APPEND_STAT("binding_protocol", "%s",
                prot_text(settings.binding_protocol));
#ifdef SASL_ENABLED
    APPEND_STAT("auth_enabled_sasl", "%s", "yes");
#else
    APPEND_STAT("auth_enabled_sasl", "%s", "no");
#endif

#ifdef ENABLE_ISASL
    APPEND_STAT("auth_sasl_engine", "%s", "isasl");
#elif defined(ENABLE_SASL)
    APPEND_STAT("auth_sasl_engine", "%s", "cyrus");
#else
    APPEND_STAT("auth_sasl_engine", "%s", "none");
#endif
    APPEND_STAT("auth_required_sasl", "%s", settings.require_sasl ? "yes" : "no");
    APPEND_STAT("item_size_max", "%llu", settings.item_size_max);
    APPEND_STAT("max_list_size", "%u", settings.max_list_size);
    APPEND_STAT("max_set_size", "%u", settings.max_set_size);
    APPEND_STAT("max_map_size", "%u", settings.max_map_size);
    APPEND_STAT("max_btree_size", "%u", settings.max_btree_size);
    APPEND_STAT("max_element_bytes", "%u", settings.max_element_bytes);
    APPEND_STAT("scrub_count", "%u", settings.scrub_count);
    APPEND_STAT("topkeys", "%d", settings.topkeys);
#ifdef ENABLE_ZK_INTEGRATION
    APPEND_STAT("hb_timeout", "%u", hb_confs.timeout);
    APPEND_STAT("hb_failstop", "%u", hb_confs.failstop);
#endif

    for (EXTENSION_DAEMON_DESCRIPTOR *ptr = settings.extensions.daemons;
         ptr != NULL;
         ptr = ptr->next) {
        APPEND_STAT("extension", "%s", ptr->get_name());
    }

    APPEND_STAT("logger", "%s", mc_logger->get_name());

    for (EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ptr = settings.extensions.ascii;
         ptr != NULL;
         ptr = ptr->next) {
        APPEND_STAT("ascii_extension", "%s", ptr->get_name(ptr->cookie));
    }
}

#ifdef ENABLE_ZK_INTEGRATION
void process_stats_zookeeper(ADD_STAT add_stats, void *c)
{
    assert(add_stats);
    arcus_zk_confs zk_confs;
    arcus_zk_stats zk_stats;
    arcus_zk_get_confs(&zk_confs);
    arcus_zk_get_stats(&zk_stats);

    APPEND_STAT("zk_libversion", "%s", zk_confs.zk_libversion);
    APPEND_STAT("zk_timeout", "%u", zk_confs.zk_timeout);
    APPEND_STAT("zk_failstop", "%s", zk_confs.zk_failstop ? "on" : "off");
    APPEND_STAT("zk_connected", "%s", zk_stats.zk_connected ? "true" : "false");
#ifdef ENABLE_ZK_RECONFIG
    APPEND_STAT("zk_reconfig_needed", "%s", zk_stats.zk_reconfig_needed ? "on" : "off");
    if (zk_stats.zk_reconfig_needed) {
        APPEND_STAT("zk_reconfig_enabled", "%s", zk_stats.zk_reconfig_enabled ? "on" : "off");
        APPEND_STAT("zk_reconfig_version", "%" PRIx64, zk_stats.zk_reconfig_version);
    }
#endif
}
#endif

void update_stat_cas(conn *c, ENGINE_ERROR_CODE ret)
{
    switch (ret) {
    case ENGINE_SUCCESS:
        STATS_HITS(c, cas, c->hinfo.key, c->hinfo.nkey);
        break;
    case ENGINE_KEY_EEXISTS:
        STATS_BADVAL(c, cas, c->hinfo.key, c->hinfo.nkey);
        break;
    case ENGINE_KEY_ENOENT:
    case ENGINE_EBADTYPE:
        STATS_MISSES(c, cas, c->hinfo.key, c->hinfo.nkey);
        break;
    default:
        STATS_CMD_NOKEY(c, cas);
    }
}

void stats_reset(const void *cookie)
{
    LOCK_STATS();
    mc_stats.rejected_conns = 0;
    mc_stats.quit_conns = 0;
    mc_stats.total_conns = 0;
    stats_prefix_clear();
    UNLOCK_STATS();
    threadlocal_stats_reset(default_thread_stats);
    mc_engine.v1->reset_stats(mc_engine.v0, cookie);
}

/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
int add_msghdr(conn *c)
{
    assert(c != NULL);
    struct msghdr *msg;

    if (c->msgsize == c->msgused) {
        msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
        if (! msg)
            return -1;
        c->msglist = msg;
        c->msgsize *= 2;
    }

    msg = c->msglist + c->msgused;

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
     * msg_flags, the last 3 of which aren't defined on solaris:
     */
    memset(msg, 0, sizeof(struct msghdr));

    msg->msg_iov = &c->iov[c->iovused];

    if (c->request_addr_size > 0) {
        msg->msg_name = &c->request_addr;
        msg->msg_namelen = c->request_addr_size;
    }

    c->msgbytes = 0;
    c->msgused++;

    if (IS_UDP(c->transport)) {
        /* Leave room for the UDP header, which we'll fill in later. */
        return add_iov(c, NULL, UDP_HEADER_SIZE);
    }

    return 0;
}

/* set up a connection to write a buffer then free it, used for stats */
void write_and_free(conn *c, char *buf, int bytes)
{
    if (buf) {
        c->write_and_free = buf;
        c->wcurr = buf;
        c->wbytes = bytes;
        if (c->iovused == 0 || (IS_UDP(c->transport) && c->iovused == 1)) {
            if (add_iov(c, c->wcurr, c->wbytes) != 0) {
                if (settings.verbose > 0) {
                    mc_logger->log(EXTENSION_LOG_WARNING, c,
                                   "Couldn't build response in conn_write.\n");
                }
                conn_set_state(c, conn_closing);
        }
        conn_set_state(c, conn_mwrite);
        if(c->write_and_go == NULL) c->write_and_go = conn_new_cmd;
    }
    } else {
        if (c->protocol == binary_prot) {
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        } else {
            out_string(c, "SERVER_ERROR out of memory writing stats");
        }
    }
}

/* grow the dynamic buffer of the given connection */
bool grow_dynamic_buffer(conn *c, size_t needed)
{
    size_t nsize = c->dynamic_buffer.size;
    size_t available = nsize - c->dynamic_buffer.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->dynamic_buffer.buffer == NULL) {
        nsize = 1024;
        available = c->dynamic_buffer.size = c->dynamic_buffer.offset = 0;
    }

    while (needed > available) {
        assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->dynamic_buffer.offset;
    }

    if (nsize != c->dynamic_buffer.size) {
        char *ptr = realloc(c->dynamic_buffer.buffer, nsize);
        if (ptr) {
            c->dynamic_buffer.buffer = ptr;
            c->dynamic_buffer.size = nsize;
        } else {
            rv = false;
        }
    }

    return rv;
}
