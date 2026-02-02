#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sched.h>
#include <inttypes.h>

/* Dummy PERSISTENCE_ACTION Macros */
#define PERSISTENCE_ACTION_BEGIN(a, b)
#define PERSISTENCE_ACTION_END(a)

#include "default_engine.h"
#include "item_clog.h"

static struct default_engine *engine=NULL;
static struct engine_config  *config=NULL; // engine config
static EXTENSION_LOGGER_DESCRIPTOR *logger;

static inline void LOCK_CACHE(void)
{
    pthread_mutex_lock(&engine->cache_lock);
}

static inline void UNLOCK_CACHE(void)
{
    pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * JSON collection management
 */
static ENGINE_ERROR_CODE do_json_item_find(struct default_engine *engine,
                                           const void *key, const size_t nkey,
                                           bool do_update, hash_item **item)
{
    *item = NULL;
    hash_item *it = do_item_get(key, nkey, do_update);
    if (it == NULL) {
        return ENGINE_KEY_ENOENT;
    }
    if (IS_JSON_ITEM(it)) {
        *item = it;
        return ENGINE_SUCCESS;
    } else {
        do_item_release(it);
        return ENGINE_EBADTYPE;
    }
}

static int32_t do_json_real_maxcount(int32_t maxcount)
{
    int32_t real_maxcount = maxcount;

    if (maxcount < 0) {
        /* It has the max_map_size that can be increased in the future */
        real_maxcount = -1;
    } else if (maxcount == 0) {
        real_maxcount = DEFAULT_JSON_SIZE;
    } else if (maxcount > config->max_map_size) {
        real_maxcount = config->max_map_size;
    }
    return real_maxcount;
}

static hash_item *do_json_item_alloc(struct default_engine *engine,
                                     const void *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    uint32_t flags = (attrp != NULL) ? attrp->flags : 0;
    rel_time_t exptime = (attrp != NULL) ? attrp->exptime : 0;

    uint32_t nbytes = 2; /* "\r\n" */
    int real_nbytes = META_OFFSET_IN_ITEM(nkey,nbytes)
                    + sizeof(json_meta_info) - nkey;

    hash_item *it = do_item_alloc(key, nkey, flags, exptime,
                                  real_nbytes, cookie);
    if (it != NULL) {
        it->iflag |= ITEM_IFLAG_JSON;
        it->nbytes = nbytes; /* NOT real_nbytes */
        memcpy(item_get_data(it), "\r\n", nbytes);

        /* initialize json meta information */
        json_meta_info *info = (json_meta_info *)item_get_meta(it);
        info->mcnt = do_json_real_maxcount((attrp != NULL) ? attrp->maxcount : 0);
        info->ccnt = 0;
        info->ovflact = OVFL_ERROR;
        info->mflags = 0;
#ifdef ENABLE_STICKY_ITEM
        if (attrp != NULL && IS_STICKY_EXPTIME(attrp->exptime)){
            info->mflags |= COLL_META_FLAG_STICKY;
        }
#endif
        if (attrp != NULL && attrp->readable == 1) {
            info->mflags |= COLL_META_FLAG_READABLE;
        }
        info->itdist = (uint16_t)((size_t*)info-(size_t*)it);
        info->stotal = 0;
        info->root    = NULL;
        assert((hash_item*)COLL_GET_HASH_ITEM(info) == it);
    }

    return it;
}

static json_elem_item *do_json_elem_alloc(struct default_engine *engine, json_node_type type,
                                          json_value *value, const void *cookie)
{
    json_elem_item *elem_item = NULL;

    switch(type) {
      case N_DICT:
           elem_item = new_dict_item(value->intval, cookie, engine);
           break;
      case N_ARRAY:
           elem_item = new_array_item(value->intval, cookie, engine);
           break;
      case N_STRING:
           elem_item = new_string_item(value->strval.pos, value->strval.len, cookie, engine);
           break;
      case N_KEYVAL:
           elem_item = new_keyval_item(value->strval.pos, value->strval.len, NULL, cookie, engine);
           break;
      case N_NUMBER:
           elem_item = new_double_item(value->numval, cookie, engine);
           break;
      case N_INTEGER:
           elem_item = new_int_item(value->intval, cookie, engine);
           break;
      case N_BOOLEAN:
           elem_item = new_bool_item(value->boolval, cookie, engine);
           break;
      case N_NULL:
           elem_item = new_null_item(cookie, engine);
           break;
    }
    return elem_item;
}

//todo free 수정
static void     do_json_elem_free(json_elem_item **elem)
{
    json_elem_item *_elem = *elem;

    if (_elem == NULL) return ;

    switch (_elem->type) {
      case N_ARRAY:
           free(_elem->value.arrval.entries);
           _elem->value.arrval.entries = NULL;
           break;
      case N_DICT:
           free(_elem->value.dictval.entries);
           _elem->value.dictval.entries = NULL;
           break;
      case N_KEYVAL:
           //free((char*)_elem->value.kvval.key);
           _elem->value.kvval.key = NULL;
           break;
      case N_STRING:
           //free((char*)_elem->value.strval.data);
           _elem->value.strval.data = NULL;
           break;
      case N_NULL:
      case N_NUMBER:
      case N_INTEGER:
      case N_BOOLEAN:
           break;
    }
    //free(_elem);
    size_t ntotal = _elem->nbytes;
    do_item_mem_free(_elem, ntotal);
    *elem = NULL;
}

static void do_json_elem_sequential_release(json_elem_item **nodes, int count)
{
    if (nodes == NULL || count <= 0) return;

    for (int i = 0; i < count; i++) {

        if (nodes[i] == NULL) continue;

        json_elem_item *_elem = nodes[i];
        if (_elem->status == JSON_ITEM_STATUS_FREE) {
            nodes[i] = NULL;
            continue;
        }
        LOCK_CACHE();
        if (_elem->refcount > 0) {
            _elem->refcount--;
            if (_elem->refcount == 0 && _elem->status == JSON_ITEM_STATUS_UNLINK) {
                _elem->status = JSON_ITEM_STATUS_FREE;
            }
        }
        UNLOCK_CACHE();


        if (_elem->status == JSON_ITEM_STATUS_FREE) {
            do_json_elem_free(&nodes[i]);
        } else {
            nodes[i] = NULL;
        }
    }
}

static ENGINE_ERROR_CODE do_json_elem_get(json_path_node *jpn, json_elem_item *root,
                                          const char *path, const size_t npath)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    jpn->sp = new_search_path();
    jpn->spath = path;
    jpn->spathlen = npath;

    //char*로 받아온 패스값을 파싱하여 jpn의 sp에 저장
    if (parse_json_path(jpn->spath, jpn->spathlen, &jpn->sp) == PARSE_ERR) {
        ret = ENGINE_EBADVALUE;
    } else ret = ENGINE_SUCCESS;

    if (jpn == NULL) ret = ENGINE_EBADVALUE;

    do {
        if (ret != ENGINE_SUCCESS) break;

        if (!search_path_is_root_path(&jpn->sp)) {
            if (search_path_find_ex(&jpn->sp, root, &jpn->n, &jpn->p) !=  E_OK) {
                ret = ENGINE_EBADVALUE;
            }
        } else {
            jpn->p = NULL;
            jpn->n = root;
        }
    } while(0);
    return ret;
}

static ENGINE_ERROR_CODE do_json_elem_unlink(json_elem_item **elem)
{
    if (*elem == NULL) return ENGINE_EINVAL;
    (*elem)->status = JSON_ITEM_STATUS_UNLINK;
    json_elem_item *it = *elem;

    // 참조 카운트 감소
    // it->refcount--;

    // 더 이상 참조하는 곳이 없을 때만 실제로 해제 로직 수행
    if (it->refcount == 0) {
        json_elem_item **container = NULL;
        int i, len;

        // 상태를 UNLINK로 변경 (슬랩 반납 준비)
        //it->status = JSON_ITEM_STATUS_UNLINK;

        // 컨테이너 타입(Array, Dict)인 경우 자식들을 재귀적으로 해제
        switch (it->type) {
            case N_ARRAY:
                container = it->value.arrval.entries;
                len = it->value.arrval.len;
                if (container != NULL) {
                    for (i = 0; i < len; i++) {
                        do_json_elem_unlink(&container[i]);
                    }
                    free(container); // Arcus 내부 dict/array 할당 방식에 맞게 해제
                }
                break;

            case N_DICT:
                container = it->value.dictval.entries;
                len = it->value.dictval.len;
                if (container != NULL) {
                    for (i = 0; i < len; i++) {
                        do_json_elem_unlink(&container[i]);
                    }
                    free(container);
                }
                break;

            case N_KEYVAL:
                do_json_elem_unlink(&it->value.kvval.val);
                break;

            default:
                // 자식이 없으므로 종료
                break;
        }
        // 실제 슬랩 메모리 반납
        do_item_mem_free(it, it->nbytes);
    }
    *elem = NULL;
    return ENGINE_SUCCESS;
}

static void do_json_elem_link(json_elem_item *elem)
{
    json_elem_item **container = NULL;
    int i, len;

    if (elem == NULL) return ;

    //elem->refcount++;
    elem->status = JSON_ITEM_STATUS_USED;

    switch (elem->type) {
      case N_ARRAY:
      case N_DICT:
           if (elem->type == N_ARRAY) {
               container = elem->value.arrval.entries;
               len = elem->value.arrval.len;
           } else {
               container = elem->value.dictval.entries;
               len = elem->value.dictval.len;
           }

           if (container != NULL) {
               for (i = 0; i < len; i++) {
                   do_json_elem_link(container[i]);
               }
           }
           break;
      case N_KEYVAL:
           do_json_elem_link(elem->value.kvval.val);
           break;
      default:
           break;
    }
}

static ENGINE_ERROR_CODE do_json_elem_append(struct default_engine *engine, json_elem_item **dest,
                                             json_elem_item *e, json_elem_item *e_temp,
                                             json_node_type type, const void *cookie)
{
    json_elem_item *old = NULL;
    ENGINE_ERROR_CODE ret;

    switch(type) {
    case N_DICT:
        ret = item_dict_set_keyval(*dest, e, &old);
        if (old != NULL) do_json_elem_unlink(&old);
        break;
    case N_ARRAY:
        ret = item_array_append(*dest, e);
        break;
    case N_KEYVAL:
        e->value.kvval.val = e_temp;
        ret = item_dict_set_keyval(*dest, e, &old);
        if (old != NULL) do_json_elem_unlink(&old);
        break;
    default:
        ret = ENGINE_ENOMEM;
        break;
    }
    return ret;
}

static ENGINE_ERROR_CODE do_json_elem_set(struct default_engine *engine,
                                          hash_item *it, json_elem_item *elem_item,
                                          const char *path, const size_t npath,
                                          const void *cookie, json_elem_item **old_item)
{
    json_path_node jpn;
    ENGINE_ERROR_CODE ret;
    json_meta_info *info = (json_meta_info *)item_get_meta(it);

    LOCK_CACHE();
    //루트 노드를 삽입하는 경우
    if (npath == 1 && path[0] == '$') {
        *old_item=info->root;
        info->root=elem_item;
        UNLOCK_CACHE();
        return ENGINE_SUCCESS;
    }
    ret = do_json_elem_get(&jpn, info->root, path, npath);

    //타겟노드가 존재하고 이를 교체하는 경우 (upsert)
    if(ret == ENGINE_SUCCESS  && jpn.n != NULL) {
        //루트노드를 교체
        if (search_path_is_root_path(&jpn.sp)) {
            *old_item = info->root;
            info->root = elem_item;
            ret = ENGINE_SUCCESS;
        }
        //부모가 있는 노드를 교체
        else if (jpn.p !=NULL && jpn.p->status == JSON_ITEM_STATUS_USED) {
            int res = OBJ_ERR;
            //어레이인 경우
            if (jpn.p->type == N_ARRAY) {
                int index = jpn.sp.nodes[jpn.sp.len - 1].value.index;
                res = item_array_set(jpn.p, index, elem_item, old_item);
                ret = (res == OBJ_OK) ? ENGINE_SUCCESS : ENGINE_ENOMEM;
            }//딕셔너리인 경우
            else if (jpn.p->type == N_DICT) {
                const char *tmp_key = jpn.sp.nodes[jpn.sp.len - 1].value.key;
                res = item_dict_set(jpn.p, tmp_key, elem_item, cookie, engine, old_item);
                ret = (res == OBJ_OK) ? ENGINE_SUCCESS : ENGINE_ENOMEM;
            }
        } else {
            ret = ENGINE_ELEM_ENOENT;
        }
    }
    //부모는 있으나 타겟은 없는 경우 타겟노드 삽입
    else if (ret == ENGINE_EBADVALUE || (ret == ENGINE_SUCCESS && jpn.n == NULL)){
        if (jpn.p != NULL && jpn.p->type == N_DICT && jpn.p->status == JSON_ITEM_STATUS_USED) {
            const char *new_key = jpn.sp.nodes[jpn.sp.len - 1].value.key;
            int res_ = item_dict_set(jpn.p, new_key, elem_item, cookie, engine, old_item);
            ret = (res_ == OBJ_OK) ? ENGINE_SUCCESS : ENGINE_ENOMEM;
        } else{
            ret = ENGINE_ELEM_ENOENT;
        }
    }
    UNLOCK_CACHE();
    return ret;
}
static ENGINE_ERROR_CODE do_json_elem_insert_buffer(size_t *offset, json_get_elem *get_elem, const char *data,
                                                    size_t data_len, uint32_t indent)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    do {
        size_t new_len = *offset + data_len + indent * 2;
        if (new_len +1 >= get_elem->cap) {
            while(new_len > get_elem->cap) {
                if (get_elem->cap * 2  == 0) {  //overflow ck
                    ret = ENGINE_E2BIG; break;
                }
                get_elem->cap *= 2;
            }
            if (ret != ENGINE_SUCCESS) break;

            char *temp_buffer = (char*)realloc(get_elem->buffer, sizeof(char) * get_elem->cap);
            if (temp_buffer != NULL) get_elem->buffer = temp_buffer;
            else { ret = ENGINE_ENOMEM; break; }
        }
        char *cur = get_elem->buffer + *offset;
        char *input_data = (char*)malloc((data_len + indent * 2 + 1)*sizeof(char));
        if (input_data == NULL) {
            ret = ENGINE_ENOMEM; break;
        }
        if (indent > 0) {
            memset(input_data, ' ', indent * 2);
        }
        // 데이터 복사
        memcpy(input_data + (indent * 2), data, data_len);
        // 최종 버퍼에 복사
        memcpy(cur, input_data, indent * 2 + data_len);
        *offset += data_len + indent * 2;

        get_elem->buffer[*offset] = '\0';

        free(input_data);
    } while(0);

    return ret;
}

ENGINE_ERROR_CODE json_struct_create(const char *key, const size_t nkey,
                                     item_attr *attrp, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    hash_item *it;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_JSON_CREATE);

    LOCK_CACHE();
    it = do_item_get(key, nkey, DONT_UPDATE);
    if (it != NULL) {
        do_item_release(it);
        ret = ENGINE_KEY_EEXISTS;
    } else {
        it = do_json_item_alloc(engine, key, nkey, attrp, cookie);
        if (it == NULL) {
            ret = ENGINE_ENOMEM;
        } else {
            ret = do_item_link(it);
            do_item_release(it);
        }
    }
    UNLOCK_CACHE();

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

json_elem_item *json_elem_alloc(struct default_engine *engine, json_node_type type,
                                json_value *value, const void *cookie)
{
    json_elem_item *elem;
    elem = do_json_elem_alloc(engine, type, value, cookie);
    return elem;
}

ENGINE_ERROR_CODE json_elem_append(struct default_engine *engine, json_elem_item **dest,
                                   json_elem_item *e, json_elem_item *e_temp,
                                   json_node_type type, const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    LOCK_CACHE();
    ret = do_json_elem_append(engine, dest, e, e_temp, type, cookie);
    UNLOCK_CACHE();
    return ret;
}

ENGINE_ERROR_CODE json_elem_set(struct default_engine *engine,
                                const char *key, const size_t nkey,
                                const char *path, const size_t npath,
                                json_elem_item *elem_item, item_attr *attrp,
                                bool *created, const void *cookie)
{
    hash_item *it = NULL;
    json_elem_item *old_item = NULL;
    ENGINE_ERROR_CODE ret;
    PERSISTENCE_ACTION_BEGIN(cookie, UPD_JSON_ELEM_INSERT);

    *created = false;

    LOCK_CACHE();
    ret = do_json_item_find(engine, key, nkey, DONT_UPDATE, &it);

    if (ret == ENGINE_KEY_ENOENT) {
        it = do_json_item_alloc(engine, key, nkey, attrp, cookie);
        if (it != NULL) {
            ret = do_item_link(it);
            if (ret == ENGINE_SUCCESS) {
                *created = true;
            } else {
                do_item_release(it); // 링크 실패 시 해제
                it = NULL;
            }
        } else {
            ret = ENGINE_ENOMEM;
        }
    }
    UNLOCK_CACHE();
    if (ret == ENGINE_SUCCESS && it !=NULL) {
        do_json_elem_link(elem_item);
        ret = do_json_elem_set(engine, it, elem_item, path, npath, cookie,&old_item);

        // created_set 에서 it이 생성되었으나 set을 실패한 경우 it 언링크하여 삭제
        if (ret != ENGINE_SUCCESS && *created) {
            LOCK_CACHE();
            do_item_unlink(it, ITEM_UNLINK_NORMAL);
            UNLOCK_CACHE();
        }
    }
    if(old_item!=NULL){
        do_json_elem_unlink(&old_item);
    }
    if (it != NULL) do_item_release(it);

    PERSISTENCE_ACTION_END(ret);
    return ret;
}

ENGINE_ERROR_CODE json_elem_unlink(struct default_engine *engine, json_elem_item **e){
    return do_json_elem_unlink((json_elem_item**)e);
}
ENGINE_ERROR_CODE json_elem_delete(struct default_engine *engine,
                                   const char *key, const size_t nkey,
                                   const char *path, const size_t npath,
                                   const bool drop_if_empty, bool *dropped,
                                   const void *cookie)
{
    hash_item *it;
    json_path_node jpn;
    json_elem_item *old_node = NULL;
    ENGINE_ERROR_CODE ret;
    *dropped = false;
    PERSISTENCE_ACTION_BEGIN(cookie, (drop_if_empty ? UPD_JSON_ELEM_DELETE_DROP
                                                    : UPD_JSON_ELEM_DELETE));

    memset(&jpn, 0, sizeof(json_path_node));

    LOCK_CACHE();
    ret = do_json_item_find(engine, key, nkey, DONT_UPDATE, &it);

    if (ret == ENGINE_SUCCESS) {
        json_meta_info *info = (json_meta_info*)item_get_meta(it);
        ret = do_json_elem_get(&jpn, info->root, path, npath);

        if (ret == ENGINE_SUCCESS) {
            if (jpn.p != NULL) {
                if (jpn.p->status != JSON_ITEM_STATUS_USED) {
                    ret = ENGINE_ELEM_ENOENT;
                } else {
                    int index = -1;
                    json_elem_item **entries = NULL;
                    unsigned int *p_len = NULL;

                    if (jpn.p->type == N_ARRAY) {
                        index = jpn.sp.nodes[jpn.sp.len - 1].value.index;
                        entries = jpn.p->value.arrval.entries;
                        p_len = &jpn.p->value.arrval.len;
                    } else if (jpn.p->type == N_DICT) {
                        entries = jpn.p->value.dictval.entries;
                        p_len = &jpn.p->value.dictval.len;
                        for (int i = 0; i < (int)*p_len; i++) {
                            if (entries[i]->value.kvval.val == jpn.n) {
                                index = i; break;
                            }
                        }
                    }

                    if (index >= 0 && index < (int)*p_len) {
                        old_node = entries[index];
                        size_t new_len = --(*p_len);
                        if (index < (int)new_len) {
                            memmove(&entries[index], &entries[index + 1], sizeof(json_elem_item *) * (new_len - index));
                        }
                        entries[new_len] = NULL;

                        // 자식을 지웠는데 부모가 비었다면 부모도 UNLINK
                        if (drop_if_empty && *p_len == 0 && jpn.p != info->root) {
                            do_json_elem_unlink(&old_node); // 자식 해제
                            old_node = jpn.p;
                            old_node->status = JSON_ITEM_STATUS_UNLINK;
                        }
                    }
                }
            } else if (jpn.p == NULL && info->root == jpn.n) {
                old_node = info->root;
                info->root = NULL;
                if (drop_if_empty) {
                    do_item_unlink(it, ITEM_UNLINK_NORMAL);
                    *dropped = true;
                }
            }
            UNLOCK_CACHE();

            if (old_node) {
                do_json_elem_unlink(&old_node);
            }
            do_item_release(it);
        } else {
            UNLOCK_CACHE();
        }
    }
    PERSISTENCE_ACTION_END(ret);
    return ret;
}
static void json_elem_collect(json_elem_item *elem,
                              json_elem_item **node_array,
                              int *parent_idx_array,
                              int *count,
                              int p_idx)
{
    if (elem == NULL || *count >= 1024) return;

    // 현재 노드를 배열에 담기
    int curr_idx = *count;
    node_array[curr_idx] = elem;
    parent_idx_array[curr_idx] = p_idx;
    (*count)++;

    // refcount 추가
    elem->refcount++;

    // DFS
    json_elem_item **container = NULL;
    int i, len;

    switch (elem->type) {
        case N_ARRAY:
        case N_DICT:
            if (elem->type == N_ARRAY) {
                container = elem->value.arrval.entries;
                len = elem->value.arrval.len;
            } else {
                container = elem->value.dictval.entries;
                len = elem->value.dictval.len;
            }

            if (container != NULL) {
                for (i = 0; i < len; i++) {
                    // 자식들에게 현재 내 인덱스(curr_idx)를 부모로 알려줌
                    json_elem_collect(container[i], node_array, parent_idx_array, count, curr_idx);
                }
            }
            break;

        case N_KEYVAL:
            if (elem->value.kvval.val != NULL) {
                json_elem_collect(elem->value.kvval.val, node_array, parent_idx_array, count, curr_idx);
            }
            break;

        default:
            break;
    }
}

ENGINE_ERROR_CODE json_elem_get(struct default_engine *engine,
                                const char *key, const size_t nkey,
                                const char *path, const size_t npath,
                                json_elem_item **elem, void **it_ptr,
                                json_elem_item **node_array, int *parent_idx_array,
                                int *node_count)
{
    hash_item *it = NULL;
    ENGINE_ERROR_CODE ret;
    json_path_node jpn;
    printf("%s\n",path);

    LOCK_CACHE();
    //전체 해시맵에서 json 타입인 해시 아이템의 찾아옴
    ret = do_json_item_find(engine, key, nkey, DONT_UPDATE, &it);
    if (ret != ENGINE_SUCCESS) {
        UNLOCK_CACHE();
        return ret;
    }
    else {
        //그 키 뒤에 붙어있는 메타정보(트리정보) 가져옴
        json_meta_info *info = (json_meta_info *)item_get_meta(it);
        //path 경로를 따라 루트부터 타겟노드를 찾아옴
        ret = do_json_elem_get(&jpn, info->root, path, npath);
        if (ret == ENGINE_SUCCESS && jpn.n != NULL) {

            json_elem_collect(jpn.n, node_array, parent_idx_array, node_count, -1);

            *elem = jpn.n; //최종적으로 찾아낸 타겟노드의 포인터 반환
            *it_ptr=(void*)it;
            ret=ENGINE_SUCCESS;
        }
    }
    if(it!=NULL) do_item_release(it);
    UNLOCK_CACHE();
    return ret;
}

static ENGINE_ERROR_CODE do_json_elem_render (json_elem_item **nodes, int *parent_idxs, int node_count,
                                              size_t *offset, json_get_elem *get_elem)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    for (int i = 0; i < node_count; i++) {
        json_elem_item *elem = nodes[i];

        if (elem == NULL || elem->status == JSON_ITEM_STATUS_UNLINK) {
        continue;
        }

        int curr_p = parent_idxs[i];
        int next_p = -1;
        for (int j = i + 1; j < node_count; j++) {
        if (nodes[j] != NULL && nodes[j]->status != JSON_ITEM_STATUS_UNLINK) {
            next_p = parent_idxs[j];
            break;
        }
    }

        //Depth 계산
        uint32_t indent = 0;
        int temp_p = curr_p;
        while (temp_p != -1) {
            temp_p = parent_idxs[temp_p];
            indent++;
        }

        uint32_t actual_indent = indent;
        if (i > 0 && nodes[i-1]->type == N_KEYVAL) {
            actual_indent = 0;
        }

        char *data = NULL;
        int data_len = 0;
        char buf[64];

        switch (elem->type) {
            case N_ARRAY: data = "[\r\n"; data_len = 3; break;
            case N_DICT:  data = "{\r\n"; data_len = 3; break;
            case N_NULL:    data = "null";    data_len = 4; break;
            case N_BOOLEAN: data = elem->value.boolval ? "true" : "false"; data_len = strlen(data); break;
            case N_INTEGER: sprintf(buf, "%lld", elem->value.intval); data = buf; data_len = strlen(data); break;
            case N_NUMBER:  gcvt(elem->value.numval, 17, buf); data = buf; data_len = strlen(data); break;
            case N_STRING:
                ret = do_json_elem_insert_buffer(offset, get_elem, "\"", 1, actual_indent);
                ret = do_json_elem_insert_buffer(offset, get_elem, elem->value.strval.data, elem->value.strval.len, 0);
                ret = do_json_elem_insert_buffer(offset, get_elem, "\"", 1, 0);
                goto skip_generic_insert;
            case N_KEYVAL:
                sprintf(buf, "%s: ", elem->value.kvval.key);
                ret = do_json_elem_insert_buffer(offset, get_elem, buf, strlen(buf), actual_indent);
                goto skip_generic_insert;
        }
        if (data) {
            ret = do_json_elem_insert_buffer(offset, get_elem, data, data_len, actual_indent);
        }

skip_generic_insert:
        if (ret != ENGINE_SUCCESS) break;
        // 콤마 괄호 닫기
        // 다음 노드가 내 형제라면 콤마만 찍기
        if (next_p == curr_p && curr_p != -1) {
            if (elem->type != N_KEYVAL) {
                ret = do_json_elem_insert_buffer(offset, get_elem, ",\r\n", 3, 0);
            }
        }
        // 다음 노드가 내 부모보다 위쪽이거나 마지막 노드
        else if (next_p < curr_p) {
            int close_p = curr_p;
            int close_depth = indent;

            // 괄호 닫기
            while (close_p != -1 && close_p != next_p) {
                if (nodes[close_p]->type != N_KEYVAL) {
                    char *bracket = (nodes[close_p]->type == N_ARRAY) ? "]" : "}";
                    ret = do_json_elem_insert_buffer(offset, get_elem, "\r\n", 2, 0);
                    ret = do_json_elem_insert_buffer(offset, get_elem, bracket, 1, close_depth - 1);
                }
                // 닫은 부모가 다음 노드의 형제면 콤마
                int grand_p = parent_idxs[close_p];
                if (grand_p == next_p && next_p != -1) {
                    ret = do_json_elem_insert_buffer(offset, get_elem, ",\r\n", 3, 0);
                }

                close_p = grand_p;
                close_depth--;
            }
            if (next_p != -1 && nodes[curr_p]->type != N_KEYVAL) {
                ret = do_json_elem_insert_buffer(offset, get_elem, ",\r\n", 3, 0);
            }
        }
    }
    return ret;
}

ENGINE_ERROR_CODE json_elem_render(json_elem_item **nodes, int *parent_idxs,
                                    int node_count, char **buffer, size_t *len)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    size_t offset = *len;
    json_get_elem *get_elem = (json_get_elem*)malloc(sizeof(json_get_elem));
    get_elem->cap = 256;

    while(*len > get_elem->cap) {
        if (get_elem->cap * 2 == 0) {
            ret = ENGINE_E2BIG; break;
        }
        get_elem->cap *= 2;
    }
    do {
        if (ret == ENGINE_E2BIG) break;

        char *temp_buffer = (char*)realloc(*buffer , get_elem->cap * sizeof(char));
        if (temp_buffer != NULL) get_elem->buffer = temp_buffer;
        else { ret = ENGINE_ENOMEM; break; }

        ret = do_json_elem_render(nodes, parent_idxs, node_count, &offset, get_elem);

        if (ret != ENGINE_SUCCESS) break;

        //if (offset > *len) offset -= 3;
        ret = do_json_elem_insert_buffer(&offset, get_elem, "\r\nEND\r\n", 7, 0);

        *len = offset;
        *buffer = get_elem->buffer;
    } while(0);
    free(get_elem);

    return ret;
}

void json_elem_release(struct default_engine* engine, json_elem_item **nodes, int count)
{
    if (nodes == NULL || count <= 0) return;
    do_json_elem_sequential_release(nodes, count);

    //free(nodes);
}

void json_elem_scalar(struct default_engine *engine, json_elem_item **e, json_elem_item **dest)
{
    json_elem_item *old_in_array = NULL;
    json_elem_item *parent_to_free = *e; // 나중에 락 밖에서 unlink할 대상

    LOCK_CACHE();

    // 배열의 0번 인덱스 아이템을 가져옴
    item_array_item(*e, 0, dest);

    if (*dest != NULL) {
        do_json_elem_link(*dest);
        item_array_set(*e, 0, NULL, &old_in_array);
    }

    *e = NULL;

    UNLOCK_CACHE();

    // 부모 노드(e였던 것)를 해제
    if (parent_to_free != NULL) {
        do_json_elem_unlink(&parent_to_free);
    }
}

/*
 * External Functions
 */
ENGINE_ERROR_CODE item_json_coll_init(void *engine_ptr)
{
    /* initialize global variables */
    engine = engine_ptr;
    config = &engine->config;
    logger = engine->server.log->get_logger();

    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM json module initialized.\n");
    return ENGINE_SUCCESS;
}

void item_json_coll_final(void *engine_ptr)
{
    logger->log(EXTENSION_LOG_INFO, NULL, "ITEM json module destroyed.\n");
}
