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
/* stats */
void stats_prefix_init(char delimiter, void (*cb_when_prefix_overflow)(void));
void stats_prefix_clear(void);
int  stats_prefix_count(void);
int  stats_prefix_insert(const char *prefix, const size_t nprefix);
int  stats_prefix_delete(const char *prefix, const size_t nprefix);
void stats_prefix_record_get(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_delete(const char *key, const size_t nkey);
void stats_prefix_record_set(const char *key, const size_t nkey);
void stats_prefix_record_incr(const char *key, const size_t nkey);
void stats_prefix_record_decr(const char *key, const size_t nkey);
void stats_prefix_record_lop_create(const char *key, const size_t nkey);
void stats_prefix_record_lop_insert(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_lop_delete(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_lop_get(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_sop_create(const char *key, const size_t nkey);
void stats_prefix_record_sop_insert(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_sop_delete(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_sop_get(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_sop_exist(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_mop_create(const char *key, const size_t nkey);
void stats_prefix_record_mop_insert(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_mop_update(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_mop_delete(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_mop_get(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_create(const char *key, const size_t nkey);
void stats_prefix_record_bop_insert(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_update(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_delete(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_incr(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_decr(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_get(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_count(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_position(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_pwg(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_bop_gbp(const char *key, const size_t nkey, const bool is_hit);
void stats_prefix_record_getattr(const char *key, const size_t nkey);
void stats_prefix_record_setattr(const char *key, const size_t nkey);
/*@null@*/
char *stats_prefix_dump(token_t *tokens, const size_t ntokens, int *length);
void stats_prefix_get(const char *prefix, const size_t nprefix, ADD_STAT add_stat, void *cookie);
