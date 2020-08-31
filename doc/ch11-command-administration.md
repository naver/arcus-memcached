# Chapter 11. Admin & Monitoring 명령

- FLUSH 명령
- SCRUB 명령
- STATS 명령
- CONFIG 명령
- CMDLOG 명령
- LQDETECT 명령
- KEY DUMP 명령
- ZKENSEMBLE 명령
- HELP 명령

### Flush 명령

ARCUS cache server는 items을 invalidate 시키기 위한 두 가지 flush 명령을 제공한다.

- flush_all : 모든 items을 flush
- flush_prefix: 특정 prefix의 items들만 flush

Flush 작업은 items을 invalidate시키더라도 그 items이 차지한 메모리 공간을 즉각 반환하지 않는다. 
대신, ARCUS cache server의 global 정보로 flush 수행 시점 정보를 기록해 둠으로써,
그 시점 이전에 존재했던 items은 invalidated items이라는 것을 알 수 있게 한다.
따라서, item 접근할 때마다 invalidated item인지를 확인하여야 하는 부담이 있지만,
flush 작업 자체는 O(1) 시간에 그 수행이 완료된다.

flush_all 명령은 flush 수행 시점 정보만 기록해 두고, 전체 prefix들의 통계 정보는 그대로 남겨 둔다.
따라서, flush_all을 수행하더라도  prefix 관련한 통계 정보를 조회할 수 있다.
해당 prefix에 속한 items이 모두 제거되는 시점에, 그 prefix의 통계 정보는 함께 제거된다.
반면, flsuh_prefix 명령은 해당 prefix에 대한 flush 수행 시점 정보를 기록해 두면서,
그 prefix의 통계 정보를 모두 reset시켜 제거한다는 것이 차이가 있다.
따라서, flush_prefix 수행 이후에는 해당 prefix에 대한 통계 정보를 조회할 수 없게 된다.

두 flush 명령의 syntax는 아래와 같다.

```
flush_all [<delay>] [noreply]\r\n
flush_prefix <prefix> [<delay>] [noreply]\r\n
```

- \<prefix\> - prefix string. "\<null\>"을 사용하면, prefix string이 없는 item들을 invalidate시킨다.
- \<delay\> - 지연된 invalidation 요청 시에 명시하며, 그 지연 기간을 초(second) 단위로 지정한다.
- noreply - 명시하면, response string이 생략된다.

Response string과 그 의미는 아래와 같다.

- "OK" - 성공
- “NOT_FOUND” - prefix miss (flush_prefix 명령인 경우만 해당)
- CLIENT_ERROR bad command line format”	- protocol syntax 틀림

### Scrub 명령

ARCUS cache server에는 유효하지 않으면서 메모리를 차지하고 있는 items이 존재할 수 있다.
이 items은 아래 두 유형으로 구분된다.

- ARCUS cache server에서 어떤 items이 expired되더라도 그 items은 즉각 제거되지 않으며,
  flush 명령으로 어떤 items을 invalidate시키더라도 그 items은 즉각 제거되지 않는다.
  이들 items은 ARCUS cache server 내부에 메모리를 차지하면서 계속 존재하고 있다.
  어떤 이유이든 이 items에 대한 접근이 발생할 때
  ARCUS cache server는 expired/flushed 상태임을 알게 되며,
  그 items을 제거함으로써 그 items이 차지한 메모리를 반환한다.
- Cache cloud를 형성하고 consistent hashing의 key-to-node mapping을 사용하는 환경에서,
  그 cache cloud에 특정 node의 추가나 삭제에 의해 key-to-node remapping이 발생하게 된다.
  이러한 key-to-node remapping이 발생하면, 어떤 node에 있던 기존 items은 더 이상 사용되지 않게 된다.
  이러한 items을 stale items이라 한다. 이러한 stale items은 자연스럽게 expired 되기도 하지만,
  old data를 가지고 남아 있다가 그 이후의 key-to-node remapping에 의해
  유효한 items으로 다시 전환될 여지가 있다.
  따라서, 이러한 stale items은 cache cloud의 node list가 변경될 때마다 제거하여야 한다.

Scrub 기능이란 (1) expired item, flushed item과 같은 invalidated item들을 명시적으로 제거하는 기능과
(2) cache cloud에서의 key-to-node remapping으로 발생한 stale items들을 명시적으로 제거하는 기능을 의미한다.

이러한 scrub 기능은 daemon thread에 의해 background 작업으로 수행되며,
한 순간에 하나의 scrub 작업만 수행될 수 있다.
즉, scrub 작업이 진행 중인 상태에서 새로운 scrub 작업을 요청할 수 없다.

```
scrub [stale]\r\n
```
- stale - 명시하지 않으면 invalidated item을 제거하고, 명시하면 stale item을 제거한다.

Response string과 그 의미는 아래와 같다.

- “OK” - 성공
- “BUSY” - 현재 scrub 작업이 수행 중이어서 새로운 scrub 작업을 요청할 수 없음
- “NOT_SUPPORTED” - 지원되지 않는 scrub 명령
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림

참고 사항으로, scrub 명령은 ascii 명령의 extension 기능으로 구현되었기에,
ARCUS cache server 구동 시에 ascii_scrub.so 파일을 dynamic linking 하는
구동 옵션을 주어야 scrub 명령을 사용할 수 있다.


### Stats 명령

ARCUS cache server의 각종 통계 정보를 조회하거나 그 통계 정보를 reset한다.

```
stats [<args>]\r\n
```

\<args\>를 생략하거나, 어떤 값을 주느냐에 따라 stats 명령의 동작은 아래와 같이 달라진다.

```
 <args>             | stats 명령의 동작
 ------------------ | -------------------------------------------
                    | General purpose 통계 정보 조회
 settings           | Configuration 정보 조회
 items              | Item 통계 정보 조회
 slabs              | Slab 통계 정보 조회
 prefixes           | Prefix 별 item 통계 정보 조회
 detail on|off|dump | Prefix 별 수행 명령 통계 정보 조회 및 제어
 scrub              | scrub 수행 상태 조회
 cachedump          | slab class 별 cache key dump
 reset              | 모든 통계 정보를 reset
```

stats 명령은 직접 한번씩 수행해 보기를 권하며, 아래에서는 추가 설명이 필요한 부분들만 기술한다.

**General purpose 정보**

특정 분류에 국한되지 않은 일반적인 통계를 알기 위한 명령이다. 다음은 stats 명령 결과의 예이다.

```
STAT pid 3553
STAT uptime 6910
STAT time 1584942539
STAT version 1.11.7
STAT libevent 2.1.11-stable
STAT pointer_size 64
STAT rusage_user 1.241010
STAT rusage_system 2.843840
STAT daemon_connections 2
STAT curr_connections 1
STAT quit_connections 0
STAT reject_connections 0
STAT total_connections 3
STAT connection_structures 3
STAT cmd_get 0
STAT cmd_set 0
STAT cmd_incr 0
STAT cmd_decr 0
STAT cmd_delete 0
STAT cmd_flush 0
STAT cmd_flush_prefix 0
STAT cmd_cas 0
STAT cmd_lop_create 0
STAT cmd_lop_insert 0
STAT cmd_lop_delete 0
STAT cmd_lop_get 0
STAT cmd_sop_create 0
STAT cmd_sop_insert 0
STAT cmd_sop_delete 0
STAT cmd_sop_get 0
STAT cmd_sop_exist 0
STAT cmd_mop_create 0
STAT cmd_mop_insert 0
STAT cmd_mop_update 0
STAT cmd_mop_delete 0
STAT cmd_mop_get 0
STAT cmd_bop_create 0
STAT cmd_bop_insert 0
STAT cmd_bop_update 0
STAT cmd_bop_delete 0
STAT cmd_bop_get 0
STAT cmd_bop_count 0
STAT cmd_bop_position 0
STAT cmd_bop_pwg 0
STAT cmd_bop_gbp 0
STAT cmd_bop_mget 0
STAT cmd_bop_smget 0
STAT cmd_bop_incr 0
STAT cmd_bop_decr 0
STAT cmd_getattr 0
STAT cmd_setattr 0
STAT auth_cmds 0
STAT auth_errors 0
STAT get_hits 0
STAT get_misses 0
STAT delete_misses 0
STAT delete_hits 0
STAT incr_misses 0
STAT incr_hits 0
STAT decr_misses 0
STAT decr_hits 0
STAT cas_misses 0
STAT cas_hits 0
STAT cas_badval 0
STAT lop_create_oks 0
STAT lop_insert_misses 0
STAT lop_insert_hits 0
STAT lop_delete_misses 0
STAT lop_delete_elem_hits 0
STAT lop_delete_none_hits 0
STAT lop_get_misses 0
STAT lop_get_elem_hits 0
STAT lop_get_none_hits 0
STAT sop_create_oks 0
STAT sop_insert_misses 0
STAT sop_insert_hits 0
STAT sop_delete_misses 0
STAT sop_delete_elem_hits 0
STAT sop_delete_none_hits 0
STAT sop_get_misses 0
STAT sop_get_elem_hits 0
STAT sop_get_none_hits 0
STAT sop_exist_misses 0
STAT sop_exist_hits 0
STAT mop_create_oks 0
STAT mop_insert_misses 0
STAT mop_insert_hits 0
STAT mop_update_misses 0
STAT mop_update_elem_hits 0
STAT mop_update_none_hits 0
STAT mop_delete_misses 0
STAT mop_delete_elem_hits 0
STAT mop_delete_none_hits 0
STAT mop_get_misses 0
STAT mop_get_elem_hits 0
STAT mop_get_none_hits 0
STAT bop_create_oks 0
STAT bop_insert_misses 0
STAT bop_insert_hits 0
STAT bop_update_misses 0
STAT bop_update_elem_hits 0
STAT bop_update_none_hits 0
STAT bop_delete_misses 0
STAT bop_delete_elem_hits 0
STAT bop_delete_none_hits 0
STAT bop_get_misses 0
STAT bop_get_elem_hits 0
STAT bop_get_none_hits 0
STAT bop_count_misses 0
STAT bop_count_hits 0
STAT bop_position_misses 0
STAT bop_position_elem_hits 0
STAT bop_position_none_hits 0
STAT bop_pwg_misses 0
STAT bop_pwg_elem_hits 0
STAT bop_pwg_none_hits 0
STAT bop_gbp_misses 0
STAT bop_gbp_elem_hits 0
STAT bop_gbp_none_hits 0
STAT bop_mget_oks 0
STAT bop_smget_oks 0
STAT bop_incr_elem_hits 0
STAT bop_incr_none_hits 0
STAT bop_incr_misses 0
STAT bop_decr_elem_hits 0
STAT bop_decr_none_hits 0
STAT bop_decr_misses 0
STAT getattr_misses 0
STAT getattr_hits 0
STAT setattr_misses 0
STAT setattr_hits 0
STAT stat_prefixes 0
STAT bytes_read 23
STAT bytes_written 771
STAT limit_maxbytes 8589934592
STAT threads 6
STAT conn_yields 0
STAT curr_prefixes 0
STAT reclaimed 0
STAT evictions 0
STAT outofmemorys 0
STAT sticky_items 0
STAT curr_items 0
STAT total_items 0
STAT sticky_bytes 0
STAT bytes 0
STAT sticky_limit 0
STAT engine_maxbytes 8589934592
END
```

명령 별 주요 통계를 정리하면 다음과 같다.

- cmd_\<command_name\>: 해당 명령의 수행 횟수
- \<command_name\>_hits: 해당 명령의 key hit 횟수
- \<command_name\>_misses: 해당 명령의 key miss 횟수
- 콜렉션 명령의 key hit 횟수는 따로 제공하지 않으며, 아래 횟수의 합으로 계산할 수 있다.
  - \<collection_name\>_\<command_name\>_elem_hits: 콜렉션 명령의 key hit 그리고 element hit 횟수
  - \<collection_name\>_\<command_name\>_none_hits: 콜렉션 명령의 key hit 그러나 element miss 횟수

다음은 그 외의 개별 통계이다. 

| stats                 | 설명                                                         |
| --------------------- | ------------------------------------------------------------ |
| pid                   | 캐시 노드의 프로세스 id                                      |
| uptime                | 캐시 서버를 구동한 시간(초)                                  |
| time                  | 현재 시간 (unix time)                                        |
| version               | 현재 arcus-memcached 버전                                    |
| libevent              | 사용중인 libevent 버전                                       |
| pointer_size          | 포인터의 크기(bit 단위)                                      |
| rusage_user           | 프로세스의 누적 user time.                                   |
| rusage_system         | 프로세스의 누적 system time.                                 |
| daemon_connections    | 서버가 사용하는 daemon connection 개수                       |
| curr_connections      | 현재 열려있는 connection 개수                                |
| quit_connections      | 클라이언트가 quit 명령을 이용해 연결을 끊은 횟수             |
| reject_connections    | 클라이언트와의 연결을 거절한 횟수                            |
| total_connections     | 서버 구동 이후 누적 connection 총합                          |
| connection_structures | 서버가 할당한 connection 구조체 개수                         |
| auth_cmds             | sasl 인증 횟수                                               |
| auth_errors           | sasl 인증 실패 횟수                                          |
| cas_badval            | 키는 찾았으나 cas 값이 맞지 않은 요청의 횟수                 |
| bytes_read            | 서버가 네트워크에서 읽은 데이터 용량 총합(bytes)             |
| bytes_written         | 서버가 네트워크에 쓴 데이터 용량 총합(bytes)                 |
| limit_maxbytes        | 서버에 허용된 최대 메모리 용량(bytes)                        |
| threads               | worker thread 개수                                           |
| conn_yields           | 이벤트당 최대 요청 수의 제한                                 |
| curr_prefixes         | 현재 저장된 prefix 개수                                      |
| reclaimed             | expired된 아이템의 공간을 사용해 새로운 아이템을 저장한 횟수 |
| evictions             | eviction 횟수                                                |
| outofmemorys          | outofmemory (메모리가 부족한 상황에서 eviction이 허용되지 않거나 실패) 발생 횟수 |
| sticky_items          | 현재 sticky 아이템의 개수                                    |
| curr_items            | 현재 서버에 저장된 아이템의 개수                             |
| total_items           | 서버 구동 후 저장한 아이템의 누적 개수                       |
| sticky_bytes          | sticky 아이템이 차지하는 메모리 용량(bytes)                  |
| bytes                 | 현재 사용중인 메모리 용량(bytes)                             |
| sticky_limit          | sticky item을 저장할 수 있는 최대 메모리 용량(bytes)         |
| engine_maxbytes       | 엔진에 허용된 최대 저장 용량                                 |

**settings 통계 정보**

각종 설정값에 대한 통계 정보를 보는 명령이다. 다음은 stats settings 실행 결과의 예이다.

```
STAT maxbytes 8589934592
STAT maxconns 3000
STAT tcpport 11911
STAT udpport 0
STAT sticky_limit 0
STAT inter NULL
STAT verbosity 1
STAT oldest 0
STAT evictions on
STAT domain_socket NULL
STAT umask 700
STAT growth_factor 1.25
STAT chunk_size 48
STAT num_threads 6
STAT stat_key_prefix :
STAT detail_enabled yes
STAT allow_detailed yes
STAT reqs_per_event 5
STAT cas_enabled yes
STAT tcp_backlog 8192
STAT binding_protocol auto-negotiate
STAT auth_enabled_sasl no
STAT auth_sasl_engine none
STAT auth_required_sasl no
STAT item_size_max 1048576
STAT max_list_size 50000
STAT max_set_size 50000
STAT max_map_size 50000
STAT max_btree_size 50000
STAT max_element_bytes 16384
STAT topkeys 0
STAT logger syslog
STAT ascii_extension scrub
END
```

| stats              | 설명                                                         |
| ------------------ | ------------------------------------------------------------ |
| maxbytes           | 캐시 서버의 최대 저장 용량(byte)                             |
| maxconns           | 접속할 수 있는 클라이언트의 최대 개수                        |
| tcpport            | listen하고 있는 TCP port                                     |
| udpport            | listen하고 있는 UDP port                                     |
| sticky_limit       | sticky 아이템을 저장할 수 있는 최대 공간의 크기(bytes)       |
| inter              | listen interface                                             |
| verbosity          | 현재 verbosity 레벨(0~3)                                     |
| oldest             | 가장 오래된 아이템이 저장되고 지난 시간                      |
| evictions          | eviction의 허용 여부                                         |
| domain_socket      | domain socket의 경로                                         |
| umask              | domain socket의 umask                                        |
| growth_factor      | slab class의 chunk 크기 증가 팩터                            |
| chunk_size         | 아이템을 저장하기 위해 할당하는 최소의 공간(key + value + flags)의 크기(bytes) |
| stat_key_prefix    | prefix와 key를 구분하는 문자                                 |
| detail_enabled     | detailed stat(prefix별 통계) 수집 여부                       |
| allow_detailed     | stat detail 명령 허용 여부                                   |
| reqs_per_event     | io 이벤트에서 처리할 수 있는 최대 io 연산 수                 |
| cas_enabled        | cas 연산 허용 여부                                           |
| tcp_backlog        | tcp의 backlog 큐 크기                                        |
| binding_protocol   | 사용중인 프로토콜. ascii, binary, auto(negotiating) 세 가지임 |
| auth_enabled_sasl  | sasl 인증 사용 여부                                          |
| auth_sasl_engine   | sasl 인증에 사용할 엔진                                      |
| auth_required_sasl | sasl 인증 필수 여부                                          |
| item_size_max      | 아이템의 최대 사이즈                                         |
| max_list_size      | list collection의 최대 element 갯수                          |
| max_set_size       | set collection의 최대 element 갯수                           |
| max_map_size       | map collection의 최대 element 갯수                           |
| max_btree_size     | btree collection의 최대 element 갯수                         |
| max_element_bytes  | collection element 데이터의 최대 크기                        |
| topkeys            | 추적하고 있는 topkey 개수                                    |
| logger             | 사용 중인 logger extension                                   |
| ascii_extension    | 사용 중인 ascii protocol extension                           |

**items 통계 정보**

item에 대한 slab class 별 통계 정보를 조회하는 명령이다. 다음은 stats items 실행 결과의 예이다.

```
STAT items:0:number 2000002
STAT items:0:sticky 0
STAT items:0:age 5401
STAT items:0:evicted 0
STAT items:0:evicted_nonzero 0
STAT items:0:evicted_time 0
STAT items:0:outofmemory 0
STAT items:0:tailrepairs 0
STAT items:0:reclaimed 0
END
```

'items:' 옆에 표기된 숫자가 slab class id이다. 통계 정보의 의미는 다음과 같다.

| Stats           | 설명                                                         |
| --------------- | ------------------------------------------------------------ |
| number          | 해당 클래스에 저장된 아이템의 개수                           |
| sticky          | sticky로 설정된 아이템의 개수. [basic concept 문서](ch01-arcus-basic-concept.md#expiration-eviction-and-sticky) 참조 |
| age             | LRU 체인에서 가장 오래된 아이템이 생성되고 나서 지난 시간(초) |
| evicted         | evict된 아이템의 개수                                        |
| evicted_nonzero | evict된 아이템 중, expired time이 명시적인 양수 값으로 설정되어 있던 아이템의 개수 |
| evicted_time    | 가장 최근에 evict된 아이템에 마지막으로 접근하고 나서 지난 시간(초) |
| out_of_memory   | 메모리 부족으로 아이템을 저장하는데 실패한 횟수              |
| tailrepairs     | slab allocator를 refcount leak에서 복구한 횟수               |
| reclaimed       | expired된 아이템의 공간을 사용해 새로운 아이템을 저장한 횟수 |

**slabs 통계 정보**

각 slab 클래스의 통계 정보와 전체 클래스에 대한 메타 정보를 조회하는 명령이다. 다음은 stats slabs 실행 결과의 예이다.

```
STAT SM:free_min_classid 709
STAT SM:free_max_classid -1
STAT SM:used_total_space 472
STAT SM:used_01pct_space 288
STAT SM:free_small_space 0
STAT SM:free_bslot_space 261640
STAT SM:free_avail_space 261640
STAT SM:free_chunk_space 0
STAT SM:free_limit_space 0
STAT SM:space_shortage_level 0
STAT 0:chunk_size 262144
STAT 0:chunks_per_page 4
STAT 0:reserved_pages 0
STAT 0:total_pages 1
STAT 0:total_chunks 4
STAT 0:used_chunks 1
STAT 0:free_chunks 0
STAT 0:free_chunks_end 3
STAT 0:mem_requested 262144
STAT active_slabs 1
STAT memory_limit 8589934592
STAT total_malloced 1048576
END
```

콜론(:)앞의 문자는 slab 클래스 번호를 의미한다. 'SM'이라고 표기된 클래스는 작은 크기의 데이터를 관리하는 small manager 클래스이다.

일반 클래스의 통계 정보가 뜻하는 의미는 다음과 같다.

| stats           | 설명                                                         |
| --------------- | ------------------------------------------------------------ |
| chunk_size      | 각 chunk가 사용하는 메모리 공간들의 크기 합(bytes)           |
| chunks_per_page | 페이지 당 chunk의 개수                                       |
| reserved_pages  | 해당 클래스에 할당하기 위해 예약된 페이지 수                 |
| total_pages     | 해당 클래스에 할당된 페이지의 개수                           |
| total_chunks    | 해당 클래스에 할당된 청크의 개수                             |
| used_chunks     | 이미 아이템에게 할당된 chunk의 개수                          |
| free_chunks     | 아직 할당되지 않았거나 삭제 연산으로 인해 free된 chunk의 개수 |
| free_chunks_end | 마지막으로 할당된 페이지 끝에 남아있는 chunk 개수            |
| mem_requested   | 해당 클래스에 요청된 메모리 공간의 크기 합(bytes)            |

 small manager 클래스의 통계 정보가 뜻하는 의미는 다음과 같다.

| stats                | 설명                                                         |
| -------------------- | ------------------------------------------------------------ |
| free_min_classid     | free slot의 id 중 최소값                                     |
| free_max_classid     | free slot의 id 중 최대값(마지막 슬롯인 big free slot의 id는 제외) |
| used_total_space     | 사용중인 공간의 크기 합(bytes)                               |
| used_01pct_space     | 크기가 상위 1퍼센트에 속하는 슬롯들이 사용하는 공간의 크기 합(bytes) |
| free_small_space     | 할당되지 않았고, 할당될 가능성이 낮은 작은 메모리 공간의 크기 합(bytes) |
| free_bslot_space     | big free slot의 남은 공간의 크기 합(bytes)                   |
| free_avail_space     | 할당되지 않았고, 할당될 가능성이 높은 큰 메모리 공간의 크기 합(bytes) |
| free_chunk_space     | 메모리 블락(chunk)를 할당할 수 있는 공간의 크기 합(bytes)    |
| free_limit_space     | 항상 비운 채로 유지되어야 하는 최소한의 여유 공간의 크기 합(bytes) |
| space_shortage_level | 공간이 부족한 정도를 0~100 으로 수치화한 레벨.               |

space_shortage_level이 10 이상으로 올라가면, background에서 아이템을 evict 하는 별도의 쓰레드를 실행해 메모리 공간을 확보한다. LRU 체인의 끝부터 space_shortage_level 만큼 아이템을 삭제하게 된다. (ssl이 10이라면 10개의 아이템 삭제)

기타 메타 통계를 정리하면 다음과 같다.

| stats          | 설명                                            |
| -------------- | ----------------------------------------------- |
| active_slabs   | 할당된 slab class의 총 개수                     |
| memory_limit   | 캐시 서버의 최대 용량(bytes)                    |
| total_malloced | slab page에 할당된 메모리 공간의 크기 합(bytes) |



**Prefix 통계 정보**

모든 prefix들의 item 통계 정보는 "stats prefixes" 명령으로 조회하고,
모든 prefix들의 연산 통계 정보는 "stats detail dump" 명령으로 조회한다.
그리고, Prefix들의 연산 통계 정보에 한해,
통계 정보의 수집 여부를 on 또는 off 할 수 있다.

모든 prefix들의 item 통계 정보의 결과 예는 아래와 같다.
\<null\> prefix 통계는 prefix를 가지지 않는 items 통계이다.

```
PREFIX <null> itm 2 kitm 1 litm 1 sitm 0 mitm 0 bitm 0 tsz 144 ktsz 64 ltsz 80 stsz 0 mtsz 0 btsz 0 time 20121105152422
PREFIX a itm 5 kitm 5 litm 0 sitm 0 mitm 0 bitm 0 tsz 376 ktsz 376 ltsz 0 stsz 0 mtsz 0 btsz 0 time 20121105152422
PREFIX b itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0 tsz 144 ktsz 144 ltsz 0 stsz 0 mtsz 0 btsz 0 time 20121105152422
END
```

각 prefix의 item 통계 정보에
itm은 전체 item 수이고, kitm, litm, sitm, mitm, bitm은 각각 kv, list, set, map, b+tree item 수이며,
tsz(total size)는 전체 items이 차지하는 공간의 크기이고,
ktsz, ltsz, stsz, mtsz, btsz는 각각 kv, list, set, map, b+tree items이 차지하는 공간의 크기이다.
time은 prefix 생성 시간이다.

모든 prefix들의 연산 통계 정보의 결과 예는 아래와 같다.
각 PREFIX 라인은 실제로 하나의 line으로 표시되지만, 본 문서는 이해를 돕기 위해 여러 line으로 표시한다.


```
PREFIX <null> get 2 hit 2 set 2 del 0
       lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0
       scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0
       mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0
       bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0
       pfs 0 pfh 0 pgs 0 pgh 0
       gas 0 sas 0
PREFIX a get 5 hit 5 set 5 del 0 
       lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0
       scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0
       mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0
       bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0
       pfs 0 pfh 0 pgs 0 pgh 0
       gas 0 sas 0
PREFIX b get 2 hit 2 set 2 del 0 
       lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0
       scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0
       mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0
       bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0
       pfs 0 pfh 0 pgs 0 pgh 0
       gas 0 sas 0
END
```

각 prefix의 연산 통계 정보에서
get, hit, set, del은 kv 유형의 items에 대한 연산 통계이고,
'l', 's', 'm', 'b'로 시작하는 3 character는 각각 list, set, map, b+tree 유형의 items에 대한 연산 통계이며,
'p'로 시작하는 3 character는 특별히 b+tree에 대한 position 연산의 통계이다.
gas와 sas는 item attribute 연산의 통계이다.

연산 통계에서 각 3 character의 의미는 다음과 같다.

- list 연산 통계
  - lcs - lop create 수행 횟수
  - lis, lih - lop insert 수행 횟수와 hit 수
  - lds, ldh – lop delete 수행 횟수와 hit 수
  - lgs, lgh – lop get 수행 횟수와 hit 수
- set 연산 통계
  - scs - sop create 수행 횟수
  - sis, sih - sop insert 수행 횟수와 hit 수
  - sds, sdh – sop delete 수행 횟수와 hit 수
  - sgs, sgh – sop get 수행 횟수와 hit 수
  - ses, seh - sop exist 수행 횟수와 hit 수
- map 연산 통계
  - mcs - mop create 수행 횟수
  - mis, mih - mop insert 수행 횟수와 hit 수
  - mus, muh – mop update 수행 횟수와 hit 수
  - mds, mdh – mop delete 수행 횟수와 hit 수
  - mgs, mgh – mop get 수행 횟수와 hit 수
- b+tree 연산 통계
  - bcs – bop create 수행 횟수
  - bis, bih – bop insert/upsert 수행 횟수와 hit 수
  - bus, buh – bop update 수행 횟수와 hit 수
  - bps, bph – bop incr(plus 의미) 수행 횟수와 hit 수
  - bms, bmh - bop decr(minus 의미) 수행 횟수와 hit 수
  - bds, bdh – bop delete 수행 횟수와 hit 수
  - bgs, bgh – bop get 수행 횟수와 hit 수
  - bns, bnh – bop count 수행 횟수와 hit 수
- b+tree position 연산 통계
  - pfs, pfh - bop position 수행 횟수와 hit 수
  - pgs, pgh - bop gbp 수행 횟수와 hit 수
- item attribute 연산 통계
  - gas - getattr 수행 횟수
  - sas - setattr 수행 횟수
  
**Scrub 수행 상태**

Scrub 수행 상태를 조회한 결과 예는 다음과 같다.

```
STAT scrubber:status stopped
STAT scrubber:last_run 0
STAT scrubber:visited 0
STAT scrubber:cleaned 0
END
```

- status - 현재 scrub 작업이 running 중인지 stopped 상태인지를 나타낸다.
- last_run - 이전에 완료된 scrub 작업의 소요 시간을 초 단위로 나타낸다.
- visited - 현재 수행중인 또는 이전에 수행된 scrub에서 접근한 item들의 수를 나타낸다.
- cleaned - 현재 수행중인 또는 이전에 수행된 scrub에서 삭제한 item들의 수를 나타낸다.

**slab class 별 cache key dump**

slab class 별 LRU에 달려있는 item들의 cache key들을 dump하기 위하여,
아래의 stats cachedump 명령을 제공한다.

```
stats cachedump <slab_clsid> <limit> [ forward | backward [sticky] ]\r\n
```

- \<slab_clsid\>	- dump 대상 LRU를 지정하기 위한 slab class id이다.
- \<limit\>	- dump하고자 하는 item 개수로서 0 ~ 200 범위에서 지정이 가능하다.
0이면 default로 50개로 지정되며, 200 초과이면 200개만 dump한다.
해당 LRU의 head 또는 tail에서 시작하여 limit 개 item들의 cache key들을 dump한다.
- forward or backward - LRU의 head 또는 tail 중에 어디에서 dump를 시작할 것인지를 지정한다.
forward이면 head에서 시작하고, backward이면 tail에서 시작한다.
지정하지 않으면, default는 forward이다.
- sticky - 하나의 slab class에서 non-sticky item들의 LRU 리스트와 sticky item들의 LRU 리스트가 별도로 유지되어 있다.
sticky가 지정되면 sticky LRU에서 dump하고, 지정되지 않으면 non-sticky LRU에서 dump한다.

Cachedump 결과의 예는 아래와 같다.

```
ITEM a:bkey2
ITEM a:bkey1
ITEM b:bkey3
ITEM b:bkey1
ITEM b:bkey2
ITEM c:bkey1
ITEM c:bkey2
END
```

### Config 명령

ARCUS cache server는 특정 configuration에 대해 동적으로 변경하거나 현재의 값을 조회하는 기능을 제공한다.
동적으로 변경가능한 configuration들은 현재 아래만 지원한다.

- verbosity
- memlimit
- zkfailstop
- maxconns
- max_element_bytes

**config verbosity**

ARCUS cache server의 verbose log level을 동적으로(restart 없이) 변경/조회한다.

```
config verbosity [<verbose>]\r\n
```

\<verbose\>는 새로 지정할 verbose log level 값으로, 허용가능한 범위는 0 ~ 2이다.
이 인자가 생략되면 현재 설정되어 있는 verbose 값을 조회한다.

**config memlimit**

ARCUS cache server 구동 시에 -m 옵션으로 설정된 memory limit을 동적으로(restart 없이) 변경/조회한다.

```
config memlimit [<memsize>]\r\n
```

\<memsize\>는 새로 지정할 memory limit으로 MB 단위로 설정하며,
ARCUS cache server가 현재 사용 중인 메모리 크기인 tatal_malloced 보다 큰 크기로만 설정이 가능하다.
이 인자가 생략되면 현재 설정되어 있는 memory limit 값을 조회한다.

**config zkfailstop**

ARCUS cache server의 automatic failstop 기능을 on 또는 off 한다.

```
config zkfailstop [on|off]\r\n
```

Network failure 상태에서 정상적인 서비스를 진행하지 못하는 cache server가 cache cloud에 그대로 존재할 경우, 해당 cache server가 담당하고 있는 data 범위에 대한 요청이 모두 실패하고 DB에 부담을 주게 된다. 또한 이후에 ZooKeeper에 재연결 되더라도 old data를 가지고 있을 가능성이 있으며 이로 인해 응용에 오동작을 발생시킬 수 있다. ARCUS cache server는 이를 해결하기 위해 ZooKeeper session timeout이 발생할 경우 failed cache server를 cache cloud에서 자동으로 제거하는 automatic failstop 기능을 기본적으로 제공한다.

**config hbtimeout**

hbtimeout 값을 변경/조회한다.

```
config hbtimeout [<hbtimeout>]\r\n
```

ARCUS cache server에는 주기적으로 노드의 정상 작동 여부를 확인하는 heartbeat 연산이 존재한다. hbtimeout은 heartbeat 연산의 timeout 시간을 의미한다. hbtimeout으로 설정한 시간이 지나도 heartbeat 연산이 이루어지지 않으면 해당 heartbeat는 timeout된 것으로 간주한다. 최소 50ms, 최대 10000ms로 설정할 수 있으며 디폴트 값은 10000ms이다.

**config hbfailstop**

hbfailstop 값을 변경/조회한다.

```
config hbfailstop [hbfailstop]\r\n
```

ARCUS cache server는 heartbeat 지연이 계속될 경우 서버를 강제 종료할 수 있다. 연속된 timeout이 발생할 때마다 hbtimeout 값을 누적하여 더하고, 누적된 값이 hbfailstop 값을 넘길 경우 failstop을 수행한다. 예를 들어 hbfailstop이 30초, hbtimeout이 10초이면 hbtimeout이 연속으로 3번 발생하였을 경우 failstop이 발생한다. 최소 3000ms, 최대 300000ms로 설정할 수 있으며 디폴트 값은 60000ms이다.

**config maxconns**

ARCUS cache server 구동 시에 -c 옵션으로 설정된 최대 연결 수를 동적으로(restart 없이) 변경/조회한다.

```
config maxconns [<maxconn>]\r\n
```

\<maxconn\>는 새로 지정할 최대 연결 수로서, 현재의 연결 수보다 10% 이상의 큰 값으로만 설정이 가능하다.
이 인자가 생략되면 현재 설정되어 있는 최대 연결 수 값을 조회한다.

**config max_element_bytes**

Collection element가 가지는 value의 최대 크기를 byte 단위로 설정한다. 기본 설정은 16KB이며 1~32KB까지 설정 가능하다.

```
config max_element_bytes [<maxbytes>]\r\n
```

**config max_collection_size**

콜렉션 아이템의 최대 element 수를 조회/변경한다.

```
config max_<collection>_size [<max_size>]\r\n
* <collection>: list|set|btree|map
```

기본 설정은 50000개이며 최소 50000개, 최대 1000000개 까지 설정할 수 있다. 기존 값보다 작게 설정할 수 없다. 기본 설정보다 큰 값으로 설정하고 나서, 한 번에 많은 element 들을 조회한다면 조회 응답 속도가 느려질 뿐만 아니라 다른 연산의 응답 속도에도 부정적 영향을 미친다. 따라서, 주의 사항으로, 최대 element 수를 늘리더라도 응용은  한번에 적은 수의 element 만을 조회하는 요청을 반복하는 형태로 구현하여야 한다.

**config scrub_count**

ARCUS cache server에는 더 이상 유효하지 않은 아이템을 일괄 삭제하는 scrub 명령이 존재한다. config scrub_count 명령은 daemon thread가 scrub 명령을 수행할 때, 한 번의 연산마다 몇 개의 아이템을 지울지를 설정/조회한다. 기본 값은 96이며 최소 32, 최대 320으로 설정할 수 있다.

```
config scrub_count [<scrub_count>]\r\n
```

### Command Logging 명령

ARCUS cache server에 입력되는 command를 logging 한다.
start 명령을 시작으로 logging이 종료될 때 까지의 모든 command를 기록한다.
단, 성능유지를 위해 skip되는 command가 있을 수 있으며 stats 명령을 통해 그 수를 확인할 수 있다.
10MB log 파일 10개를 사용하며, 초과될 경우 자동 종료한다.

```
cmdlog [start [<log_file_path>] | stop | stats]\r\n
```

\<log_file_path\>는 logging 정보를 저장할 file의 path이다.
- path는 생략 가능하며, 생략할 경우 default로 지정된다.
  - default로 자동 지정할 경우 log file은 memcached구동위치/command_log 디렉터리 안에 생성된다.
  - command_log 디렉터리는 자동생성되지 않으며, memcached process가 구동된 위치에 생성해 주어야 한다.
  - 생성되는 log file의 파일명은 command_port_bgndate_bgntime_{n}.log 이다.
- path는 직접 지정할 경우 절대 path, 상대 path지정이 가능하다. 최종 파일이 생성될 디렉터리까지 지정해 주어야 한다.

start 명령의 결과로 log file에 출력되는 내용은 아래와 같다.

```
---------------------------------------
format : <time> <client_ip> <command>\n
---------------------------------------

19:14:45.530198 127.0.0.1 bop insert arcustest-Collection_Btree:vuRYyfqyeP0Egg8daGF72 0x626B65795F6279746541727279323239 0x45464C4147 80 create 0 600 4000
19:14:45.530387 127.0.0.1 lop insert arcustest-Collection_List:pGhEn6DFv5MixbYObBgp1 -1 64 create 0 600 4000
19:14:45.530221 127.0.0.1 lop insert arcustest-Collection_List:hhSAED2pFBH9xGqEgAeW1 -1 80 create 0 600 4000
19:14:45.530334 127.0.0.1 bop insert arcustest-Collection_Btree:RGSXLACxWpKwLPdC86qn0 0x626B65795F6279746541727279303331 0x45464C4147 80 create 0 600 4000
19:14:45.530385 127.0.0.1 lop insert arcustest-Collection_List:PwFTiFSEWlenireHcxNb2 -1 80 create 0 600 4000
19:14:45.530407 127.0.0.1 bop insert arcustest-Collection_Btree:P1lfJrJyVFyP0ogrw27h1 0x626B65795F6279746541727279313238 0x45464C4147 101 create 0 600 4000
19:14:45.530537 127.0.0.1 sop exist arcustest-Collection_Set:gTx8KDPBiufiGN9ArtgG3 81 pipe
19:14:45.530757 127.0.0.1 sop exist arcustest-Collection_Set:gTx8KDPBiufiGN9ArtgG3 81
```

stop 명령은 logging이 완료되기 전 중지하고 싶을 때 사용할 수 있다.

stats 명령은 가장 최근 수행된(수행 중인) command logging의 상태를 조회하고 결과는 아래와 같다.

```
Command logging stats : running                                      //Not started | stopped by causes(request or overflow or error) | running
The last running time : 20160126_192729 ~ 20160126_192742            //bgndate_bgntime ~ enddate_endtime
The number of entered commands : 146783                              //entered_commands
The number of skipped commands : 0                                   //skipped_commands
The number of log files : 1                                          //file_count
The log file name: /Users/temp/command_11211_20160126_192729_{n}.log //path/file_name
```

### Long query detect 명령

ARCUS cache server에서 collection item에 대한 요청 중에는 그 처리 시간이 오래 걸리는 요청이 존재한다.
이를 detect하기 위한 기능으로 lqdetect 명령을 제공한다.
start 명령을 시작으로 detection이 종료될 때 까지 long query 가능성이 있는 command에 대하여, 
그 command 처리에서 접근한 elements 수가 특정 기준 이상인 command를 추출,
command 별로 detect된 명령어 20개를 샘플로 저장한다.
long query 대상이 되는 모든 command에 대해 20개의 샘플 저장이 완료되면 자동 종료한다.
저장된 샘플은 show 명령을 통해 확인할 수 있다.

long query detection 대상이 되는 command는 아래와 같다.
```
1. sop get
2. lop insert
3. lop delete
4. lop get
5. bop delete
6. bop get
7. bop count
8. bop gbp
```

lqdetect command는 아래와 같다.
```
lqdetect [start [<detect_standard>] | stop | show | stats]\r\n
```
\<detect_standard\>는 long query로 분류하는 기준으로 해당 요청에서 접근하는 elements 수로 나타내며, 어떤 요청에서 detection 기준 이상으로 많은 elements를 접근하는 요청을 long query로 구분한다. 생략 시 default standard는 4000이다.

start 명령으로 detection을 시작할 수 있다.

stop 명령은 detection이 완료되기 전 중지하고 싶을 때 사용할 수 있다.

show 명령은 저장된 명령어 샘플을 출력하고 그 결과는 아래와 같다.

```
-----------------------------------------------------------
format : <time> <client_ip> <count> <command> <arguments>\n
-----------------------------------------------------------

sop get command entered count : 0

lop insert command entered count : 0

lop delete command entered count : 0

lop get command entered count : 92
17:56:33.276847 127.0.0.1 <46> lop get arcustest-Collection_List:YN8UCtNaoD4hHnMMwMJq1 0..44
17:56:33.278116 127.0.0.1 <43> lop get arcustest-Collection_List:orjTteJo7F0bWdXDDGcP0 0..41
17:56:33.279856 127.0.0.1 <48> lop get arcustest-Collection_List:r7ERYr3IdiD3RO8hLNvI3 0..46
17:56:33.304063 127.0.0.1 <45> lop get arcustest-Collection_List:0OWKNF3Z17NaTSaDTZG61 0..43

bop delete command entered count : 0

bop get command entered count : 81
17:56:33.142590 127.0.0.1 <47> bop get arcustest-Collection_Btree:0X6mqSiwBx6fEZVLuwKF0 0x626B65795F62797465417272793030..0x626B65795F6279746541727279303530 efilter 0 47
17:56:33.142762 127.0.0.1 <49> bop get arcustest-Collection_Btree:PiX8strLCv7iWywd1ZuE0 0x626B65795F62797465417272793030..0x626B65795F6279746541727279303530 efilter 0 49
17:56:33.143326 127.0.0.1 <46> bop get arcustest-Collection_Btree:PiX8strLCv7iWywd1ZuE1 0x626B65795F62797465417272793130..0x626B65795F6279746541727279313530 efilter 0 48

bop count command entered count : 0

bop gbp command entered count : 0
```

stats 명령은 가장 최근 수행된(수행 중인) long query detection의 상태를 조회하고 그 결과는 아래와 같다.
```
Long query detection stats : running              //stopped by causes(request or overflow) | running
The last running time : 20160126_175629 ~ 0_0     //bgndata_bgntime ~ enddate_endtime
The number of total long query commands : 1152    //detected_commands 
The detection standard : 43                       //standard
```

### Key dump 명령

ARCUS cache server의 key를 dump 한다.

dump ascii command는 아래와 같다.
```
dump start key [<prefix>] filepath\r\n
dump stop\r\n
stats dump\r\n
```
dump start명령.
- 첫번째 인자는 무조건 "key"이다.
  - 현재는 일단 key string만을 dump한다.
  - 향후에 key or item을 선택할 수 있게 하여, item인 경우 item 전체 내용을 dump할 수 있다.
- 두번째 인자는 \<prefix\>는 cache key의 prefix를 의미하며, 생략 가능하다.
  - 생략하면, 모든 key string을 dump한다.
  - "\<null\>"을 주면, prefix가 없는 key string을 dump한다.
  - 어떤 prefix를 주면, 그 prefix의 모든 key string을 dump한다.
- 세번째 인자는 \<file path\>이다.
  - 반드시 명시해야 한다.
  - 절대 path로 줄 수도 있으며, 상대 path도 가능하다.
  - 상대 path이면 memcached process가 구동된 위치에서의 상대 path이다.

dump stop은 혹시나 dump 작업이 너무 오려 걸려
중지하고 싶을 경우에 사용할 수 있다.

stats dump는 현재 진행중인 dump 또는 가장 최근에 수행된 dump의 stats을 보여 준다.

dump 파일은 무조건 하나의 file로 만들어 진다.
file 내용의 format은 아래와 같다.

```
<type> <key_string> <exptime>\n
...
<type> <key_string> <exptime>\n
DUMP SUMMARY: { prefix=<prefix>, count=<count>, total=<total> elapsed=<elapsed> }\n
```

위의 결과에서 각 의미는 아래와 같다.
- key dump 결과 부분
  - \<type\>은 item type으로 1 character로 표시한다.
     - "K" : kv 
     - "L" : list
     - "S" : set
     - "M" : map
     - "B" : b+tree
  - \<key_string\>은 cache server에 저장되어 있는 key string 이다.
  - \<exptime\>은 key의 exptime으로 아래 값들 중 하나이다.
    -  0 (exptime = 0인 경우)
    - -1 : sticky item (exptime = -1인 경우)
    - timestamp (exptime > 0인 경우)으로
      "time since the Epoch (00:00:00 UTC, January 1, 1970), measured in seconds" 이다.
- DUMP SUMMARY 부분
  - \<prefix\>는 prefix name이다.
    - 모든 key dump이면, "\<all\>"이 명시된다.
    - prefix 없는 key dump이면, "\<null\>"이 명시된다.
    - 특정 prefix의 key dump이면, 그 prefix name이 명시된다.
  - \<count\>는 dump한 key 개수이다.
  - \<total\>은 cache에 있는 전체 key 개수이다.
  - \<elapsed\>는 dump하는 데 소요된 시간(단위: 초) 이다.

### Zkensemble 명령

ARCUS cache server가 연결되어 있는 ZooKeeper ensemble 설정에 대한 명령을 제공한다.

```
zkensemble set <ensemble_list>\r\n
zkensemble get\r\n
zkensemble rejoin\r\n
```

set 명령은 ZK ensemble 주소를 변경한다. ensemble_list는 \<ip:port\>,...,\<ip:port\>  와 같은 ZK server 들의 list 형태 혹은 ZK ensemble의 도메인 주소 형태로 지정할 수 있다.

get 명령은 ZK ensemble 주소를 조회한다. 조회 결과는 \<ip:port\>,...,\<ip:port\> 형식으로 확인할 수 있다.

rejoin 명령은 ZK ensemble 과의 연결을 끊고 cache cloud에서 빠져 대기하는 cache server를 다시 ZK ensemble에 연결하도록 하는 명령이다. Cache cloud에서 cache server가 빠져나가는 경우는 아래와 같다.
- Failstop off 상태에서 ZooKeeper session timeout이 일어난 경우
- 운영자의 실수로 cache_list에 등록된 cache server의 ephemeral znode가 삭제된 경우


### Help 명령

ARCUS cache server의 acsii command syntax를 조회한다.

```
help [<subcommand>]\r\n
```

\<subcommand\>로는 kv, lop, sop, mop, bop, stats, flush, config, etc가 있으며,
\<subcommand\>가 생략되면, help 명령에서 사용가능한 subcommand 목록이 출력된다.
