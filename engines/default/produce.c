#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "rdkafka.h"

#include "default_engine.h"

#ifdef ENABLE_PERSISTENCE
#include "checkpoint.h"
#include "cmdlogfile.h"
#include "cmdlogbuf.h"
#include "cmdlogrec.h"
#include "cdclogfile.h"
#include "produce.h"

#define PRODUCE_CMDLOG 0
#define PRODUCE_SNAPSHOT 1

typedef struct _kafka_st {
    rd_kafka_t *rk;
    char *topic;
    char *brokers;
} kafka_st;

typedef struct _ms_producer {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    void *config;
    bool sleep;
    int mode;
    int snapshot_req;
    char snapshot_path[MAX_FILEPATH_LENGTH]; /* snapshot file path */
    char cmdlog_path[MAX_FILEPATH_LENGTH];   /* cmdlog file path */
    char *data_path;
    char *logs_path;
    volatile uint8_t running;
    volatile bool reqstop;
    volatile bool initialized;
} ms_producer;

/* global data */
static EXTENSION_LOGGER_DESCRIPTOR* logger = NULL;
static kafka_st kafka_anch;
static ms_producer producer;

// callback이 필요한지 검토
// static void
// dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {

// }

static int producer_init(void)
{
    kafka_st *ks = &kafka_anch;
    rd_kafka_t *rk;
    rd_kafka_conf_t *conf;
    char errstr[512];

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", ks->brokers, errstr,
                            sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "%s\n", errstr);
        return -1;
    }

    // callback이 필요한지 검토 후 활성화
    //rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create new producer: %s\n", errstr);
        return -1;
    }
    ks->rk = rk;
    return 1;
}

static int do_produce(char *buf, size_t size)
{
    kafka_st *ks = &kafka_anch;
    while(1) {
        rd_kafka_resp_err_t err;
        err = rd_kafka_producev(
                    ks->rk,
                    RD_KAFKA_V_TOPIC(ks->topic),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_VALUE(buf, size),
                    RD_KAFKA_V_OPAQUE(NULL),
                    RD_KAFKA_V_END);

        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            rd_kafka_poll(ks->rk, 0);
            return 1;
        }

        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            rd_kafka_poll(ks->rk, 10);
            continue;
        }

        return -1;
    }
}

static void producer_destory(void)
{
    kafka_st *ks = &kafka_anch;

    if (ks->rk == NULL) {
        return;
    }

    rd_kafka_poll(ks->rk, 0);

    /* 최대 10초 동안 남은 메세지 전송 대기 */
    rd_kafka_flush(ks->rk, 10*1000);

    /* flush 후에도 남아있다면 미전달 */
    int outq = rd_kafka_outq_len(ks->rk);
    if (outq > 0) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "%d message(s) were not delivered\n", outq);
    }

    rd_kafka_destroy(ks->rk);
    ks->rk = NULL;
}

static int produce_cmdlog(ms_producer *p, int *cmdlog_offset)
{
    int fd;
    pthread_mutex_lock(&p->lock);
    if ((fd=open(p->cmdlog_path, O_RDONLY)) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open cmdlog file. path=%s err=%s\n",
                    p->cmdlog_path, strerror(errno));
        return -1;
    }
    pthread_mutex_unlock(&p->lock);

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size, seek_offset;
    file_size = sb.st_size;
    seek_offset = lseek(fd, *cmdlog_offset, SEEK_SET);

    char buf[MAX_LOG_RECORD_SIZE];
    ssize_t nread;
    LogRec *logrec = (LogRec*)buf;
    LogHdr *loghdr = &logrec->header;

    while (1) {
        if (p->snapshot_req) {
            close(fd);
            return 1;
        }

        if (seek_offset >= file_size) {
            close(fd);
            if (cdclog_file_deletable()) {
                char path[MAX_FILEPATH_LENGTH];
                next_cdclog_path(p->cmdlog_path, path);
                fd = open(path, O_RDONLY);
                if (fd < 0) {
                    logger->log(EXTENSION_LOG_WARNING, NULL,
                                "Failed to open cmdlog file. path=%s err=%s\n",
                                path, strerror(errno));
                    *cmdlog_offset = seek_offset;
                    return -1;
                }
                cdclog_file_delete(p->cmdlog_path);
                snprintf(p->cmdlog_path, MAX_FILEPATH_LENGTH, "%s", path);

                fstat(fd, &sb);
                file_size = sb.st_size;
                seek_offset = 0;
                continue;
            }
            break;
        }

        nread = read(fd, loghdr, sizeof(LogHdr));
        if (nread != sizeof(LogHdr)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[PRODUCE] failed : read header data "
                        "nread(%zd) != header_length(%ld).\n", nread, sizeof(LogHdr));
            break;
        }

        size_t log_size = sizeof(LogHdr)+loghdr->body_length;
        if (loghdr->body_length > 0) {
            logrec->body = buf+sizeof(LogHdr);
            nread = read(fd, logrec->body, loghdr->body_length);
            if (nread != loghdr->body_length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[PRODUCE] failed : read body data "
                        "nread(%zd) != body_length(%d).\n", nread, loghdr->body_length);
                break;
            }
        }

        if (do_produce(buf, log_size) < 0) {
            break;
        }
        seek_offset += log_size;
    }

    *cmdlog_offset = seek_offset;
    return 1;
}

static int produce_snapshot(ms_producer *p)
{
    int fd;
    pthread_mutex_lock(&p->lock);
    if ((fd=open(p->snapshot_path, O_RDONLY)) < 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open snapshot file. path=%s err=%s\n",
                    p->snapshot_path, strerror(errno));
        return -1;
    }
    pthread_mutex_unlock(&p->lock);

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size, seek_offset;
    file_size = sb.st_size;
    seek_offset = 0;

    char buf[MAX_LOG_RECORD_SIZE];
    ssize_t nread;
    LogRec *logrec = (LogRec*)buf;
    LogHdr *loghdr = &logrec->header;

    while (seek_offset < file_size) {
        nread = read(fd, loghdr, sizeof(LogHdr));
        if (nread != sizeof(LogHdr)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[PRODUCE] failed : read header data "
                        "nread(%zd) != header_length(%ld).\n", nread, sizeof(LogHdr));
            break;
        }

        size_t log_size = sizeof(LogHdr)+loghdr->body_length;
        if (loghdr->body_length > 0) {
            logrec->body = buf+sizeof(LogHdr);
            nread = read(fd, logrec->body, loghdr->body_length);
            if (nread != loghdr->body_length) {
                logger->log(EXTENSION_LOG_WARNING, NULL,
                        "[PRODUCE] failed : read body data "
                        "nread(%zd) != body_length(%d).\n", nread, loghdr->body_length);
                break;
            }
        }

        if (do_produce(buf, log_size) < 0) {
            break;
        }
        seek_offset += log_size;
    }
    p->mode = PRODUCE_CMDLOG;
    return 1;
}

static int do_producer_thread_sleep(ms_producer *p, int sleep_sec)
{
    struct timeval tv;
    struct timespec to;

    gettimeofday(&tv, NULL);
    to.tv_sec = tv.tv_sec + sleep_sec;
    to.tv_nsec = tv.tv_usec * 1000;

    pthread_mutex_lock(&p->lock);
    p->sleep = true;
    pthread_cond_timedwait(&p->cond, &p->lock, &to);
    p->sleep = false;
    pthread_mutex_unlock(&p->lock);

    return sleep_sec;
}

static void do_producer_thread_wakeup(ms_producer *p)
{
    pthread_mutex_lock(&p->lock);
    if (p->sleep) {
        pthread_cond_signal(&p->cond);
    }
    pthread_mutex_unlock(&p->lock);
}

void set_cmdlog_path(const char *path)
{
    pthread_mutex_lock(&producer.lock);
    snprintf(producer.cmdlog_path, MAX_FILEPATH_LENGTH,
            "%s", path);
    pthread_mutex_unlock(&producer.lock);
}

void set_snapshot_path(const char *path)
{
    pthread_mutex_lock(&producer.lock);
    snprintf(producer.snapshot_path, MAX_FILEPATH_LENGTH,
            "%s", path);
    pthread_mutex_unlock(&producer.lock);
}

static void* producer_thread_main(void* arg)
{
    producer_init();

    ms_producer *p = (ms_producer *)arg;
    int cmdlog_offset = 0;

    p->running = RUNNING_STARTED;
    while(1) {
        do_producer_thread_sleep(p, 1);

        if (p->reqstop) {
            logger->log(EXTENSION_LOG_INFO, NULL, "Producer thread recognized stop request.\n");
            break;
        }

        pthread_mutex_lock(&p->lock);
        if (p->snapshot_path[0] == '\0' || p->cmdlog_path[0] == '\0') {
            continue;
        }
        pthread_mutex_unlock(&p->lock);

        if (p->mode == PRODUCE_SNAPSHOT) {
            produce_snapshot(p);
            cmdlog_offset = 0;
        }
        else if (p->mode == PRODUCE_CMDLOG) {
            produce_cmdlog(p, &cmdlog_offset);
        }
    }
    p->running = RUNNING_STOPPED;
    producer_destory();
    return NULL;
}

ENGINE_ERROR_CODE kafka_st_init(char *topic, char *broker)
{
    kafka_anch.topic = topic;
    kafka_anch.brokers = broker;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE ms_prod_init(struct default_engine* engine)
{
    logger = engine->server.log->get_logger();

    kafka_st_init("test-topic", "localhost");
    pthread_mutex_init(&producer.lock, NULL);
    pthread_cond_init(&producer.cond, NULL);
    producer.config = (void*)&engine->config;
    producer.sleep = false;
    producer.mode = PRODUCE_SNAPSHOT;
    producer.snapshot_req = 0;
    producer.snapshot_path[0] = '\0';
    producer.cmdlog_path[0] = '\0';
    producer.data_path = engine->config.data_path;
    producer.logs_path = engine->config.logs_path;
    producer.running = RUNNING_UNSTARTED;
    producer.reqstop = false;
    producer.initialized = true;
    logger->log(EXTENSION_LOG_INFO, NULL, "PRODUCER module initialized");
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE producer_thread_start(void)
{
    pthread_t tid;
    if (producer.running == RUNNING_STARTED) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "Producer_thread already started.\n");
        return ENGINE_FAILED;
    }

    producer.running = RUNNING_UNSTARTED;
    /* create producer thread */
    if (pthread_create(&tid, NULL, producer_thread_main, &producer) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
            "Failed to create producer thread. error=%s\n", strerror(errno));
        return ENGINE_FAILED;
    }

    /* wait unitl producer thread starts */
    while (producer.running == RUNNING_UNSTARTED) {
        usleep(5000);
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Producer thread started.\n");
    return ENGINE_SUCCESS;
}

void producer_thread_stop(void)
{
    if (producer.running == RUNNING_UNSTARTED) {
        return;
    }
    while (producer.running == RUNNING_STARTED) {
        producer.reqstop = true;
        do_producer_thread_wakeup(&producer);
        usleep(5000);
    }
    logger->log(EXTENSION_LOG_INFO, NULL, "Producer thread stopped.\n");
}

void producer_final(void)
{
    if (producer.initialized == false) {
        return;
    }
    pthread_mutex_destroy(&producer.lock);
    pthread_cond_destroy(&producer.cond);
    producer.initialized = false;
    logger->log(EXTENSION_LOG_INFO, NULL, "PRODUCER module destroyed.\n");
}
#endif
