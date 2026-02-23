#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <signal.h>
#include <sys/time.h>

#include "rdkafka.h"
#include <libmemcached/memcached.h>

#define ENABLE_PERSISTENCE 1
#include <memcached/engine.h>
#include "engines/default/default_engine.h"
#include "engines/default/item_base.h"
#include "engines/default/cmdlogrec.h"

#define BTREE_REAL_NBKEY(nbkey) ((nbkey)==0 ? sizeof(uint64_t) : (nbkey))
#define EXPIRED_REL_EXPTIME(exptime) ((exptime) == 1)

typedef struct _kafka_st {
    rd_kafka_t *rk;
    char *topic;
    char *brokers;
} kafka_st;

typedef struct snapshot_ctx {
    char keybuf[1024];
    uint16_t keylen;
    uint8_t ittype;
} snapshot_ctx;

/* global data */
static kafka_st kafka_anch;

static int CONVERT_REL_EXPTIME(rel_time_t exptime)
{
    if (exptime == 0 || exptime == (rel_time_t)(-1)) {
        return exptime; /* 0 (never), -1 (sticky) */
    }

    rel_time_t abstime = time(NULL);
    if (exptime > abstime) {
        return 2; /* not expired */
    } else {
        /* expired. */
        return 1;
    }
}

void kafka_st_init(char *topic, char *broker)
{
    kafka_anch.topic = "test-topic";
    kafka_anch.brokers = "localhost";
}

void consumer_init(int argc, char **argv) {
    rd_kafka_t *rk;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_partition_list_t *subscription;
    char errstr[512];

    conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", kafka_anch.brokers, errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    if (rd_kafka_conf_set(conf, "group.id", argv[2], errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    if (rd_kafka_conf_set(conf, "group.protocol", argv[3], errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false",
                            errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return;
    }

    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        exit(1);
    }
    conf = NULL;
    kafka_anch.rk = rk;

    rd_kafka_poll_set_consumer(rk);
    subscription = rd_kafka_topic_partition_list_new(argc-4);
    for (int i = 4; i<argc; i++)
        rd_kafka_topic_partition_list_add(subscription, argv[i], RD_KAFKA_PARTITION_UA);

    rd_kafka_resp_err_t err;
    err = rd_kafka_subscribe(rk, subscription);
    if (err) {
        fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n",
                        subscription->cnt, rd_kafka_err2str(err));
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(rk);
    }
}

static int lrec_to_it_link(memcached_st *mc, LogRec *logrec)
{
    memcached_return_t rc;
    ITLinkLog *log = (ITLinkLog*)logrec;
    ITLinkData *body = &log->body;
    struct lrec_item_common *cm = &body->cm;
    char *keyptr = body->data;

    if (CONVERT_REL_EXPTIME(cm->exptime) == 1) {
       fprintf(stderr, "expried tiem. key=%.*s\n", cm->keylen, keyptr);
       return 0;
    }

    if (cm->ittype == ITEM_TYPE_KV) {
        char *valptr = keyptr + cm->keylen;
        uint32_t bytes = cm->vallen-2;

        rc = memcached_set(mc, keyptr, cm->keylen, valptr, bytes, cm->exptime, cm->flags);
        if (memcached_failed(rc)) {
            fprintf(stderr, "Failed to memcached_set: %d(%s)\n",
                    rc, memcached_strerror(mc, rc));
            return -1;
        }
        // fprintf(stderr, "KV key=%.*s\n", cm->keylen, keyptr);
    } else {
        struct lrec_coll_meta *meta = (struct lrec_coll_meta*)&body->ptr.meta;
        memcached_coll_create_attrs_st attributes;
        memcached_coll_create_attrs_init(&attributes, cm->flags, cm->exptime, meta->mcnt);

        if (cm->ittype == ITEM_TYPE_LIST) {
            rc = memcached_lop_create(mc, keyptr, cm->keylen, &attributes);
            if (memcached_failed(rc)) {
                fprintf(stderr, "Failed to memcached_lop_create: %d(%s)\n",
                        rc, memcached_strerror(mc, rc));
                return -1;
            }
        } else if (cm->ittype == ITEM_TYPE_SET) {

        } else if (cm->ittype == ITEM_TYPE_MAP) {

        } else if (cm->ittype == ITEM_TYPE_BTREE) {

        }

    }
    assert(rc == MEMCACHED_SUCCESS);
    return 0;
}

static int lrec_to_it_unlink(memcached_st *mc, LogRec *logrec)
{
    memcached_return_t rc;
    ITUnlinkLog *log = (ITUnlinkLog*)logrec;
    ITUnlinkData *body = &log->body;
    char *keyptr = body->data;

    rc = memcached_delete(mc, keyptr, body->keylen, 0);
    if (memcached_failed(rc)) {
        fprintf(stderr, "Failed to memcached_delete: %d(%s)\n",
                rc, memcached_strerror(mc, rc));
        return -1;
    }

    assert(rc == MEMCACHED_SUCCESS);
    return 0;
}

static int lrec_to_list_elem_insert(memcached_st *mc, LogRec *logrec)
{
    memcached_return_t rc;
    memcached_coll_attrs_st attrs;
    ListElemInsLog *log = (ListElemInsLog*)logrec;
    ListElemInsData *body = &log->body;
    char *keyptr = body->data;
    char *valptr = keyptr+body->keylen;
    uint32_t bytes = body->vallen-2;

    rc = memcached_get_attrs(mc, keyptr, body->keylen, &attrs);
    if (body->create) {
        if (rc == MEMCACHED_SUCCESS) {
            fprintf(stderr, "Exist key\n");
            return 0;
        }

        memcached_coll_create_attrs_st attributes;
        lrec_attr_info *attrs_info = (lrec_attr_info*)(keyptr+body->keylen+body->vallen);
        memcached_coll_create_attrs_init(&attributes, attrs_info->flags,
                                         attrs_info->exptime, attrs_info->maxcount);
        rc = memcached_lop_create(mc, keyptr,body->keylen, &attributes);
        if (memcached_failed(rc)) {
            fprintf(stderr, "Failed to memcached_lop_create: %d(%s)\n",
                rc, memcached_strerror(mc, rc));
            return -1;
        }
    }

    if (rc == MEMCACHED_SUCCESS) {
        rc = memcached_lop_insert(mc, keyptr, body->keylen, body->eindex,
                                  valptr, bytes, NULL);
        if (memcached_failed(rc)) {
                fprintf(stderr, "Failed to memcached_lop_insert: %d(%s)\n",
                        rc, memcached_strerror(mc, rc));
                return -1;
        }
        assert(rc == MEMCACHED_SUCCESS);
    } else {
        fprintf(stderr, "Failed to memcached_lop_insert: %d(%s)\n",
                rc, memcached_strerror(mc, rc));
        return -1;
    }
    assert(rc == MEMCACHED_SUCCESS);
    return 0;
}

static int lrec_to_list_elem_delete(memcached_st *mc, LogRec *logrec)
{
    memcached_return_t rc;
    ListElemDelLog *log = (ListElemDelLog*)logrec;
    ListElemDelData *body = &log->body;
    char *keyptr = body->data;
    int32_t bgn_idx = body->eindex;
    int32_t end_idx = body->eindex+body->delcnt-1;

    if (body->delcnt > 1) {
        rc = memcached_lop_delete_by_range(mc, keyptr, body->keylen, bgn_idx, end_idx, body->drop);
    } else {
        rc = memcached_lop_delete(mc, keyptr, body->keylen, bgn_idx, body->drop);
    }

    if (memcached_failed(rc)) {
        fprintf(stderr, "Failed to memcached_lop_elem_delete: %d(%s)\n",
                rc, memcached_strerror(mc, rc));
        return -1;
    }
    assert(rc == MEMCACHED_SUCCESS);
    return 0;
}

static int lrec_to_ascii_command(memcached_st *mc, LogRec *logrec)
{
    int ret = -1;
    LogHdr *loghdr = &logrec->header;

    if (loghdr->logtype == LOG_IT_LINK) {
        ret = lrec_to_it_link(mc, logrec);
    } else if (loghdr->logtype == LOG_IT_UNLINK) {
        ret = lrec_to_it_unlink(mc, logrec);
    } else if (loghdr->logtype == LOG_LIST_ELEM_INSERT) {
        ret = lrec_to_list_elem_insert(mc, logrec);
    } else if (loghdr->logtype == LOG_LIST_ELEM_DELETE) {
        ret = lrec_to_list_elem_delete(mc, logrec);
    }
    return ret;
}

static int snapshot_elem_to_ascii(memcached_st *mc, snapshot_ctx *ctx, LogRec *logrec)
{
    memcached_return_t rc;
    memcached_coll_attrs_st attrs;
    SnapshotElemLog *log = (SnapshotElemLog*)logrec;
    SnapshotElemData *body = &log->body;
    char *valptr = body->data;
    int index;
    if (ctx->ittype == ITEM_TYPE_LIST) {
        rc = memcached_get_attrs(mc, ctx->keybuf, ctx->keylen, &attrs);
        index = attrs.count;
        rc = memcached_lop_insert(mc, ctx->keybuf, ctx->keylen, index, valptr, body->nbytes-2, NULL);
        if (memcached_failed(rc)) {
            fprintf(stderr, "Failed to memcached_lop_create: %d(%s)\n",
                rc, memcached_strerror(mc, rc));
            return -1;
        }
    }
    return 0;
}

static void update_snapshot_ctx(snapshot_ctx *ctx, LogRec *logrec)
{
    ITLinkLog *log = (ITLinkLog*)logrec;
    const ITLinkData *body = &log->body;
    const struct lrec_item_common *cm = (const struct lrec_item_common *)&body->cm;
    const char *keyptr = body->data;

    if (cm->ittype == ITEM_TYPE_BTREE) {
        const struct lrec_coll_meta *meta = (const struct lrec_coll_meta *)&body->ptr.meta;
        if (meta->maxbkrlen != BKEY_NULL) {
            keyptr += BTREE_REAL_NBKEY(meta->maxbkrlen);
        }
    }

    if (cm->ittype != ITEM_TYPE_KV) {
        ctx->keylen = cm->keylen;
        ctx->ittype = cm->ittype;
        if (cm->keylen > 0 && cm->keylen < sizeof(ctx->keybuf)) {
            memcpy(ctx->keybuf, keyptr, cm->keylen);
            ctx->keybuf[cm->keylen] = '\0';
        }
    }
}

static void commit_pending(rd_kafka_t *rk, rd_kafka_topic_partition_list_t **ppending)
{
    if (*ppending == NULL || (*ppending)->cnt == 0) return;
    rd_kafka_resp_err_t err = rd_kafka_commit(rk, *ppending, 1);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "Kafka batch commit failed: %s\n", rd_kafka_err2str(err));
    }

    rd_kafka_topic_partition_list_destroy(*ppending);
    *ppending = rd_kafka_topic_partition_list_new(0);
}

static int do_consum(memcached_st *mc) {
    kafka_st *ks = &kafka_anch;

    snapshot_ctx ctx;
    memset(&ctx, 0, sizeof(snapshot_ctx));

    rd_kafka_topic_partition_list_t *pending = rd_kafka_topic_partition_list_new(0);
    int success_since_commit = 0;

    while(1) {
        rd_kafka_message_t *rkm;

        // 메세지 큐에서 메세지를 하나 소비
        rkm = rd_kafka_consumer_poll(ks->rk, 500);

        if (rkm == NULL) {
            continue; // timeout
        }

        if (rkm->err) {
            rd_kafka_message_destroy(rkm);
            continue;
        }

        char buf[4096];
        memcpy(buf, rkm->payload, rkm->len);
        LogRec *logrec = (LogRec*)buf;
        LogHdr *loghdr = &logrec->header;

        if (rkm->len < sizeof(LogHdr) + loghdr->body_length) {
            rd_kafka_message_destroy(rkm);
            continue;
        }

        int ret = -1;
        if (loghdr->logtype == LOG_IT_LINK) {
            update_snapshot_ctx(&ctx, logrec);
            ret = lrec_to_ascii_command(mc, logrec);
        } else if (loghdr->logtype == LOG_SNAPSHOT_ELEM) {
            ret = snapshot_elem_to_ascii(mc, &ctx, logrec);
        } else if(loghdr->logtype == LOG_SNAPSHOT_DONE) {
            ret = 0;
        } else {
            ret = lrec_to_ascii_command(mc, logrec);
        }

        if (ret == 0) {
            const char *topic = rd_kafka_topic_name(rkm->rkt);
            int32_t partition = rkm->partition;
            int64_t next_offset = rkm->offset+1;

            rd_kafka_topic_partition_t *tp = rd_kafka_topic_partition_list_find(pending, topic, partition);
            if (tp == NULL) {
                tp = rd_kafka_topic_partition_list_add(pending, topic, partition);
            }
            tp->offset = next_offset;

            success_since_commit++;
            if (success_since_commit >= 200) {
                commit_pending(ks->rk, &pending);
                success_since_commit = 0;
            }
        } else {
            fprintf(stderr, "Skip commit (conversion/write failure).\n");
        }

        rd_kafka_message_destroy(rkm);
    }
}

int main(int argc, char **argv)
{
    memcached_st *mc = memcached_create(NULL);
    assert(mc);

    if (memcached_server_add(mc, "dev-3", 21212) != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Fail to add memcached server. Port=%d\n", 21212);
        return 0;
    }

    kafka_st_init(NULL, NULL);
    consumer_init(argc, argv);

    do_consum(mc);

    rd_kafka_consumer_close(kafka_anch.rk);
    rd_kafka_destroy(kafka_anch.rk);
    memcached_free(mc);
    return 0;
}
