/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * arcus-memcached - Arcus memory cache server
 * Copyright 2019 JaM2in Corp.
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
#ifndef MEMCACHED_LOGREC_H
#define MEMCACHED_LOGREC_H

enum log_action_type {
    ACT_KV_LINK = 0,
    ACT_KV_UNLINK
};

/* Log Record Structure */

typedef struct loghdr {
    uint32_t body_length; /* LogRec body length */
    uint8_t acttype;      /* operation type */
    uint8_t reserved[3];
} LogHdr;

typedef struct logrec {
    LogHdr header;
    char body[1];         /* specific log record data */
} LogRec;

/* Specific Log Record */

/* KV Link Log Record */
/* KVLinkData | *key | *value */
typedef struct _KV_link_data{
    uint16_t nkey;
    uint32_t flags;
    uint32_t exptime;
    uint32_t nbytes;
    uint64_t cas;
} KVLinkData;

typedef struct _KV_link_log {
    LogHdr header;
    KVLinkData body;
} KVLinkLog;

/* KV Unlink Log Record */
/* KVUnlinkData | *key */
typedef struct _KV_unlink_data{
    uint16_t nkey;
} KVUnlinkData;

typedef struct _KV_unlink_log{
    LogHdr header;
    KVUnlinkData body;
} KVUnlinkLog;

/* Log Record Function */
typedef struct _logrec_func {
    int (*write) (struct logrec *logrec, hash_item* it);
    ENGINE_ERROR_CODE (*redo) (struct logrec *logrec, struct default_engine* engine);
} LOGREC_FUNC;

LOGREC_FUNC logrec_func[2];

int lrec_KV_link_write(struct logrec *logRec, hash_item *it);
ENGINE_ERROR_CODE lrec_KV_link_redo(struct logrec *logRec, struct default_enginei *engine);
int lrec_KV_unlink_write(struct logrec *logRec, hash_item *it);
ENGINE_ERROR_CODE lrec_KV_unlink_redo(struct logrec *logRec, struct default_engine *engine);

int lrec_it_write(struct logrec *logRec, hash_item *it);
ENGINE_ERROR_CODE lrec_it_redo(struct logrec *logRec, struct default_engine *engine);

#endif
