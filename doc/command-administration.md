Admin & Monitoring 명령
-----------------------

- FLUSH 명령
- SCRUB 명령
- STATS 명령
- CONFIG 명령
- HELP 명령

### Flush 명령

Arcus cache server는 items을 invalidate 시키기 위한 두 가지 flush 명령을 제공한다.

- flush_all : 모든 items을 flush
- flush_prefix: 특정 prefix의 items들만 flush

Flush 작업은 items을 invalidate시키더라도 그 items이 차지한 메모리 공간을 즉각 반환하지 않는다. 
대신, Arcus cache server의 global 정보로 flush 수행 시점 정보를 기록해 둠으로써,
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

- \<prefix\> - prefix string. "null"을 사용하면, prefix string이 없는 item들을 invalidate시킨다.
- \<delay\> - 지연된 invalidation 요청 시에 명시하며, 그 지연 기간을 초(second) 단위로 지정한다.
- noreply - 명시하면, response string이 생략된다.

Response string과 그 의미는 아래와 같다.

- "OK" - 성공
- “NOT_FOUND” - prefix miss (flush_prefix 명령인 경우만 해당)
- CLIENT_ERROR bad command line format”	- protocol syntax 틀림

### Scrub 명령

Arcus cache server에는 유효하지 않으면서 메모리를 차지하고 있는 items이 존재할 수 있다.
이 items은 아래 두 유형으로 구분된다.

- Arcus cache server에서 어떤 items이 expired되더라도 그 items은 즉각 제거되지 않으며,
  flush 명령으로 어떤 items을 invalidate시키더라도 그 items은 즉각 제거되지 않는다.
  이들 items은 Arcus cache server 내부에 메모리를 차지하면서 계속 존재하고 있다.
  어떤 이유이든 이 items에 대한 접근이 발생할 때
  Arcus cache server는 expired/flushed 상태임을 알게 되며,
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
Arcus cache server 구동 시에 ascii_scrub.so 파일을 dynamic linking 하는
구동 옵션을 주어야 scrub 명령을 사용할 수 있다.


### Stats 명령

Arcus cache server의 각종 통계 정보를 조회하거나 그 통계 정보를 reset한다.

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
 cachedump          | slab calss 별 cache key dump
 reset              | 모든 통계 정보를 reset
``` 

stats 명령은 직접 한번씩 수행해 보기를 권하며, 아래에서는 추가 설명이 필요한 부분들만 기술한다.

**Prefix 통계 정보**

모든 prefix들의 item 통계 정보는 "stats prefixes" 명령으로 조회하고,
모든 prefix들의 연산 통계 정보는 "stats detail dump" 명령으로 조회한다.
그리고, Prefix들의 연산 통계 정보에 한해,
통계 정보의 수집 여부를 on 또는 off 할 수 있다.

모든 prefix들의 item 통계 정보의 결과 예는 아래와 같다.
\<null\> prefix 통계는 prefix를 가지지 않는 items 통계이다.

```
PREFIX <null> itm 2 kitm 1 litm 1 sitm 0 bitm 0 tsz 144 ktsz 64 ltsz 80 stsz 0 btsz 0 time 20121105152422
PREFIX a itm 5 kitm 5 litm 0 sitm 0 bitm 0 tsz 376 ktsz 376 ltsz 0 stsz 0 btsz 0 time 20121105152422
PREFIX b itm 2 kitm 2 litm 0 sitm 0 bitm 0 tsz 144 ktsz 144 ltsz 0 stsz 0 btsz 0 time 20121105152422
END
```

각 prefix의 item 통계 정보에
itm은 전체 item 수이고, kitm, litm, sitm, bitm은 각각 kv, list, set, b+tree item 수이며,
tsz(total size)는 전체 items이 차지하는 공간의 크기이고,
ktsz, ltsz, stsz, btsz는 각각 kv, list, set, b+tree items이 차지하는 공간의 크기이다.
time은 prefix 생성 시간이다.

모든 prefix들의 연산 통계 정보의 결과 예는 아래와 같다.
각 PREFIX 라인은 실제로 하나의 line으로 표시되지만, 본 문서는 이해를 돕기 위해 여러 line으로 표시한다.


```
PREFIX <null> get 2 hit 2 set 2 del 0
       lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0
       scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 
       bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0
       pfs 0 pfh 0 pgs 0 pgh 0
       gas 0 sas 0
PREFIX a get 5 hit 5 set 5 del 0 
       lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0
       scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 
       bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0
       pfs 0 pfh 0 pgs 0 pgh 0
       gas 0 sas 0
PREFIX b get 2 hit 2 set 2 del 0 
       lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0
       scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 
       bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0
       pfs 0 pfh 0 pgs 0 pgh 0
       gas 0 sas 0
END
```

각 prefix의 연산 통계 정보에서
get, hit, set, del은 kv 유형의 items에 대한 연산 통계이고,
'l', 's', b'로 시작하는 3 character는 각각 list, set, b+tree 유형의 items에 대한 연산 통계이며,
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

Arcus cache server는 특정 configuration에 대해 동적으로 변경하거나 현재의 값을 조회하는 기능을 제공한다.
동적으로 변경가능한 configuration들은 현재 아래만 지원한다.

- verbosity
- memlimit
- maxconns

**config verbosity**

Arcus cache server의 verbose log level을 동적으로(restart 없이) 변경/조회한다.

```
config verbosity [<verbose>]\r\n
```

\<verbose\>는 새로 지정할 verbose log level 값으로, 허용가능한 범위는 0 ~ 2이다.
이 인자가 생략되면 현재 설정되어 있는 verbose 값을 조회한다.

**config memlimit**

Arcus cache server 구동 시에 -m 옵션으로 설정된 memory limit을 동적으로(restart 없이) 변경/조회한다.

```
config memlimit [<memsize>]\r\n
```

\<memsize\>는 새로 지정할 memory limit으로 MB 단위로 설정하며,
Arcus cache server가 현재 사용 중인 메모리 크기인 tatal_malloced 보다 큰 크기로만 설정이 가능하다.
이 인자가 생략되면 현재 설정되어 있는 memory limit 값을 조회한다.

**config maxconns**

Arcus cache server 구동 시에 -c 옵션으로 설정된 최대 연결 수를 동적으로(restart 없이) 변경/조회한다.

```
config maxconns [<maxconn>]\r\n
```

\<maxconn\>는 새로 지정할 최대 연결 수로서, 현재의 연결 수보다 10% 이상의 큰 값으로만 설정이 가능하다.
이 인자가 생략되면 현재 설정되어 있는 최대 연결 수 값을 조회한다.

### Help 명령

Arcus cache server의 acsii command syntax를 조회한다.

```
help [<subcommand>]\r\n
```

\<subcommand\>로는 kv, lop, sop, bop, stats, flush, config, etc가 있으며,
\<subcommand\>가 생략되면, help 명령에서 사용가능한 subcommand 목록이 출력된다.
