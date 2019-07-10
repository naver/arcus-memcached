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

#include <string.h>
#include <ctype.h>

#include "default_engine.h"
#ifdef ENABLE_PERSISTENCE_03_KV_DATA_SNAPSHOT
#include "lrec.h"

LOGREC_FUNC logrec_func[2] = {
    { lrec_KV_link_write, lrec_KV_link_redo },
    { lrec_KV_unlink_write, lrec_KV_unlink_redo }
};

int lrec_KV_link_write(struct logrec *logRec, hash_item *it)
{
    KVLinkLog *log = (void*)logRec;
    KVLinkData *data = &log->body;

    data->nkey = it->nkey;
    data->flags = it->flags;
    data->exptime = it->exptime;
    data->nbytes = it->nbytes;
    data->cas = item_get_cas(it);

    char *ptr = (char*)(log + 1);
    memcpy(ptr, item_get_key(it), it->nkey);
    ptr += it->nkey;
    memcpy(ptr, item_get_data(it), it->nbytes);
    ptr += it->nbytes;

    return (int)(ptr - (char*)data);
}

ENGINE_ERROR_CODE lrec_KV_link_redo(struct logrec *logRec, struct default_engine *engine)
{
    ENGINE_ERROR_CODE ret;
    KVLinkLog *log = (void*)logRec;
    KVLinkData *data = &log->body;

    char *ptr = (char*)(log+1);
    char *kstr = ptr;
    char *value = kstr+(data->nkey);

    ret = ha_apply_simple_item_link(engine, kstr, data->nkey, data->flags,
                                    data->exptime, data->nbytes, value, data->cas);

    return ret;
}

int lrec_KV_unlink_write(struct logrec *logRec, hash_item *it)
{
    KVUnlinkLog *log = (void*)logRec;
    KVUnlinkData *data = &log->body;

    data->nkey = it->nkey;

    char *kptr = (char*)(log+1);
    memcpy(kptr, item_get_key(it), it->nkey);
    kptr += it->nkey;

    return (int)(kptr-(char*)data);
}

ENGINE_ERROR_CODE lrec_KV_unlink_redo(struct logrec *logRec, struct default_engine *engine)
{
    ENGINE_ERROR_CODE ret;
    KVUnlinkLog *log = (void*)logRec;
    KVUnlinkData *data = &log->body;

    char *kstr = (char*)(log+1);
    ret = ha_apply_item_unlink(engine, kstr, data->nkey);
    return ret;
}

int lrec_it_write(struct logrec *logRec, hash_item *it)
{
    return logrec_func[logRec->header.acttype].write(logRec, it);
}

ENGINE_ERROR_CODE lrec_it_redo(struct logrec *logRec, struct default_engine *engine)
{
    return logrec_func[logRec->header.acttype].redo(logRec, engine);
}
#endif
