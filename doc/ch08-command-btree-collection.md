# Chapter 8. B+Tree 명령

B+tree collection에 관한 명령은 아래와 같다.

- [B+tree collection 생성: bop create](#bop-create)
- B+tree collection 삭제: delete (기존 key-value item의 삭제 명령을 그대로 사용)

B+tree element에 관한 기본 명령은 아래와 같다.

- [B+tree element 삽입/대체: bop insert/upsert](#bop-insertupsert)
- [B+tree element 변경: bop update](#bop-update)
- [B+tree element 삭제: bop delete](#bop-delete)
- [B+tree element 조회: bop get](#bop-get)
- [B+tree element 개수 계산: bop count](#bop-count)
- [B+tree element 값의 증감: bop incr/decr](#bop-incrdecr)

ARCUS cache server는 다수의 b+tree들에 대한 조회 기능을 특별히 제공하며, 이들은 아래와 같다.

- [하나의 명령으로 여러 b+tree들에 대한 조회를 한번에 수행하는 기능:  bop mget](#bop-mget)
- [여러 b+tree들에서 조회 조건을 만족하는 elements를 sort merge하여 최종 결과를 얻는 기능: bop smget](#bop-smget)

ARCUS cache server는 bkey 기반의 element 조회 기능 외에도 b+tree position 기반의 element 조회 기능을 제공한다.
B+tree에서 특정 element의 position이란 b+teee에서의 그 element의 위치 정보로서,
bkey들의 정렬(ASC or DESC) 기준으로 봐서 몇 번째 위치한 element인지를 나타낸다.
B+tree position은 0-based index로 표현한다.
예를 들어, b+tree에 N개의 elements가 있다면 0부터 N-1까지의 index로 나타낸다.

ARCUS cache server에서 제공하는 b+tree position 관련 명령은 다음과 같다.

- [B+tree에서 특정 bkey의 position을 조회하는 기능 : bop position](#bop-position)
- [B+tree에서 하나의 position 또는 position range에 해당하는 element를 조회하는 기능 : bop gbp(get by position)](#bop-gbp)
- [B+tree에서 특정 bkey의 position과 element 그리고 그 위치 앞뒤의 element를 함께 조회하는 기능: bop pwg(position with get)](#bop-pwg)

B+tree position 기반의 조회가 필요한 예를 하나 들면, ranking 시스템이 있다.
Ranking 시스템에서는 특정 score를 bkey로 하여 해당 elements를 저장하고,
조회는 최고/최저 score 기준으로 몇번째 위치 또는 위치의 범위에 해당하는 element를 찾는 경우가 많다.


## bop create

B+tree collection을 empty 상태로 생성한다.

```
bop create <key> <attributes> [noreply]\r\n
* attributes: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<attributes\> - 설정할 item attributes.
[Item Attribute 설명](ch03-item-attributes.md)을 참조 바란다.
  - unreadable - 명시하면, readable 속성은 off로 설정됩니다.
- noreply - 명시하면, response string을 전달받지 않는다.

Response string과 그 의미는 아래와 같다.

| Response String                         | 설명                     |
|-----------------------------------------|------------------------ |
| "CREATED"                               | 성공
| "EXISTS"                                | 동일 key string을 가진 item이 이미 존재
| "NOT_SUPPORTED"                         | 지원하지 않음
| "CLIENT_ERROR bad command line format"  | protocol syntax 틀림
| "SERVER_ERROR out of memory"            | 메모리 부족

## bop insert/upsert

B+tree collection에 하나의 element를 추가하는 명령으로
(1) 하나의 element를 삽입하는 bop insert 명령과
(2) 현재 삽입하는 bkey를 가진 element가 없으면 현재의 element를 삽입하고
그 bkey를 가진 element가 있으면 현재의 element로 대체시키는 bop upsert 명령이 있다.
이들 명령 수행에서 b+tree collection을 생성하면서 하나의 element를 추가할 수도 있다.

```
bop insert <key> <bkey> [<eflag>] <bytes> [create <attributes>] [noreply|pipe|getrim]\r\n<data>\r\n
bop upsert <key> <bkey> [<eflag>] <bytes> [create <attributes>] [noreply|pipe|getrim]\r\n<data>\r\n
* attributes: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<bkey\> - 삽입할 element의 bkey
- \<eflag\> - 삽입할 element의 optional flag
- \<bytes\>와 \<data\> - 삽입할 element의 데이터의 길이와 데이터 그 자체 (최대 크기는 [기본제약사항](ch01-arcus-basic-concept.md#basic-constraints)을 참고)
- create \<attributes\> - b+tree collection 없을 시에 b+tree 생성 요청.
[Item Attribute 설명](ch03-item-attributes.md)을 참조 바란다.
  - unreadable - 명시하면, readable 속성은 off로 설정됩니다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다.
pipe 사용은 [Command Pipelining](ch09-command-pipelining.md)을 참조 바란다.
- getrim - 새로운 element 추가로 maxcount 제약에 의한 overflow trim이 발생할 경우,
trim된 element 정보를 가져온다.

Trimmed element 정보가 리턴되는 경우, 그 response string은 아래와 같다.

```
VALUE <flags> <count>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
END\r\n
```

그 외의 response string과 의미는 아래와 같다.

| Response String                         | 설명                     |
|-----------------------------------------|------------------------ |
| "STORED"                                | 성공 (element만 삽입)
| "CREATED_STORED"                        | 성공 (collection 생성하고 element 삽입)
| "REPLACED"                              | 성공 (element를 대체)
| "NOT_FOUND"                             | key miss
| "TYPE_MISMATCH"                         | 해당 item이 b+tree colleciton이 아님
| "BKEY_MISMATCH"                         | 삽입할 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "OVERFLOWED"                            | overflow 발생
| "OUT_OF_RANGE"                          | 새로운 element 삽입이 maxcount 또는 maxbkeyrange 제약을 위배하면서 그 element의 bkey 값이 overflowaction에 의해 자동 삭제되는 경우이어서 삽입이 실패하는 경우이다. 예를 들어, smallest_trim 상황에서 새로 삽입할 element의 bkey 값이 b+tree의 smallest bkey 보다 작으면서 maxcount 개의 elements가 이미 존재하거나 maxbkeyrange를 벗어나는 경우가 이에 해당된다.
| "ELEMENT_EXISTS"                        | 동일 bkey를 가진 element가 존재
| "NOT_SUPPORTED"                         | 지원하지 않음
| "CLIENT_ERROR bad command line format"  | protocol syntax 틀림
| "CLIENT_ERROR too large value"          | 삽입할 데이터가 element value의 최대 크기보다 큼
| "CLIENT_ERROR bad data chunk"           | 삽입할 데이터의 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
| "SERVER_ERROR out of memory"            | 메모리 부족

## bop update

B+tree collection에서 하나의 element에 대해 eflag 변경 그리고/또는 data 변경을 수행한다.
현재 다수 elements에 대한 변경 연산은 제공하지 않고 있다.

```
bop update <key> <bkey> [<eflag_update>] <bytes> [noreply|pipe]\r\n[<data>\r\n]
* eflag_update : [<fwhere> <bitwop>] <fvalue>
```

- \<key\> - 대상 item의 key string
- \<bkey\> - 대상 element의 bkey
- \<eflag_update\> - eflag update 명시.
[Collection 기본 개념](ch02-collection-items.md)에서 eflag update를 참조 바란다.
- \<bytes\>와 \<data\> - 새로 변경할 데이터의 길이와 데이터 그 자체. 데이터 변경을 원치 않으면 \<bytes\>를 -1로 하고 \<data\>를 생략하면 된다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다.
pipe 사용은 [Command Pipelining](ch09-command-pipelining.md)을 참조 바란다.

Response string과 그 의미는 아래와 같다.

| Response String                         | 설명                     |
|-----------------------------------------|------------------------ |
| "UPDATED"                               | 성공
| "NOT_FOUND"                             | key miss
| "NOT_FOUND_ELEMENT"                     | element miss (변경할 element가 없음)
| "TYPE_MISMATCH"                         | 해당 item이 b+tree colleciton이 아님
| "BKEY_MISMATCH"                         | 명령 인자로 주어진 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "EFLAG_MISMATCH"                        | 해당 element의 eflag 값에 대해 \<eflag_update\>를 적용할 수 없음. 예를 들어, 변경하고자 하는 eflag가 존재하지 않거나, 존재하더라도 \<eflag_update\> 조건으로 명시된 부분의 데이터를 가지지 않은 상태이다.
| "NOTHING_TO_UPDATE"                     | eflag 변경과 data 변경 중 어느 하나도 명시되지 않은 상태
| "NOT_SUPPORTED"                         | 지원하지 않음
| "CLIENT_ERROR bad command line format"  | protocol syntax 틀림
| "CLIENT_ERROR too large value"          | 변경할 데이터가 element value의 최대 크기보다 큼
| "CLIENT_ERROR bad data chunk"           | 변경할 데이터의 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
| "SERVER_ERROR out of memory"            | 메모리 부족

## bop delete

b+tree collection에서 하나의 bkey 또는 bkey range 조건과 eflag filter 조건을 만족하는
N 개의 elements를 삭제한다.

```
bop delete <key> <bkey or "bkey range"> [<eflag_filter>] [<count>] [drop] [noreply|pipe]\r\n
* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
```

- \<key\> - 대상 item의 key string
- \<bkey or "bkey range"\> - 하나의 bkey 또는 bkey range 조회 조건.
Bkey range는 "bkey1..bkey2" 형식으로 표현한다.
- \<eflag_filter\> - eflag filter 조건.
[Collection 기본 개념](ch02-collection-items.md)에서 eflag filter 참조 바란다.
- \<count\> - 삭제할 elements 개수 지정
- drop - element 삭제로 인해 empty b+tree가 될 경우, 그 b+tree를 drop할 것인지를 지정한다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다.
pipe 사용은 [Command Pipelining](ch09-command-pipelining.md)을 참조 바란다.

Response string과 그 의미는 아래와 같다.

| Response String                         | 설명                    |
|-----------------------------------------|------------------------ |
| "DELETED"                               | 성공 (element만 삭제)
| "DELETED_DROPPED"                       | 성공 (element 삭제하고 collection을 drop한 상태)
| "NOT_FOUND"                             | key miss
| "NOT_FOUND_ELEMENT"                     | element miss (삭제할 element가 없음)
| "TYPE_MISMATCH"                         | 해당 item이 b+tree colleciton이 아님
| "BKEY_MISMATCH"                         | 명령 인자의 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "NOT_SUPPORTED"                         | 지원하지 않음
| "CLIENT_ERROR bad command line format"  | protocol syntax 틀림

## bop get

B+tree collection에서 하나의 bkey 또는 bkey range 조건과 eflag filter 조건을 만족하는
elements에서 offset 개를 skip한 후 count 개의 elements를 조회한다.

```
bop get <key> <bkey or "bkey range"> [<eflag_filter>] [[<offset>] <count>] [delete|drop]\r\n
* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
```

- \<key\> - 대상 item의 key string
- \<bkey or "bkey range"\> - 하나의 bkey 또는 bkey range 조회 조건. Bkey range는 "bkey1..bkey2" 형식으로 표현한다.
- \<eflag_filter\> - eflag filter 조건. [Collection 기본 개념](ch02-collection-items.md)에서 eflag filter 참조 바란다.
- [\<offset\>] \<count\> - 조회 조건을 만족하는 elements에서 skip 개수와 실제 조회할 개수
- delete or drop - element 조회하면서 그 element를 delete할 것인지
그리고 delete로 인해 empty b+tree가 될 경우 그 b+tree를 drop할 것인지를 지정한다.

성공 시의 response string은 아래와 같다.

```
VALUE <flags> <count>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
…
END|TRIMMED|DELETED|DELETED_DROPPED\r\n
```

위 response string에 대한 설명은 다음과 같다.

VALUE 라인의 \<count\>는 조회된 element 개수를 나타내며,
그 다음 라인 부터 조회된 각 element의 bkey, flag, data가 나타낸다.
마지막 라인은 조회 상태로서 END, TRIMMED, DELETED, DELETED_DROPPED 중 하나를 가진다.
END, DELEETED, DELEETD_DROPPED은 각각
element 조회만 수행한 상태, element 조회하고 삭제한 상태,
element 조회 및 삭제한 후 empty b+tree collection도 drop한 상태를 의미한다.
TRIMMED는 특별한 의미로서, element 조회만 수행한 상태이면서
element 조회 조건이 b+tree의 overflowaction으로 trim된 bkey 영역과 overlap 되었음을 나타낸다.
이를 통해, 조회 조건을 만족하지만 overflow trim으로 조회되지 않은 elements가 있을 수 있음을
해당 응용이 알 수 있게 한다. 그러면, 해당 응용은 필요시,
back-end storage에서 조회되지 않은 나머지 elements를 다시 조회할 수 있다.
참고로, overflow action으로 smallest_silent_trim 또는 largest_silent_trim을 사용한다면,
b+tree collection 내부에 trim 발생 여부를 유지하지 않아 TRIMMED와 같은 trim 발생 상태를
알려주지 않게 된다. 이 경우, trim 발생 여부에 대한 검사는 응용에서 자체적으로 수행해야 한다.

실패 시의 response string과 그 의미는 아래와 같다.

| Response String                                       | 설명                    |
|-------------------------------------------------------|------------------------ |
| "NOT_FOUND"                                           | key miss
| "NOT_FOUND_ELEMENT"                                   | element miss (조회 조건을 만족하는 element가 없음)
| "OUT_OF_RANGE"                                        | 조회 조건을 만족하는 element가 없으며, 또한 주어진 bkey range가 b+tree의 overflowaction에 의해 trim된 bkey 영역과 overlap 되었음을 나타낸다.
| "TYPE_MISMATCH"                                       | 해당 item이 b+tree collection이 아님
| "BKEY_MISMATCH"                                       | 명령 인자로 주어진 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "UNREADABLE"                                          | 해당 item이 unreadable item임
| "NOT_SUPPORTED"                                       | 지원하지 않음
| "CLIENT_ERROR bad command line format"                | protocol syntax 틀림
| "SERVER_ERROR out of memory [writing get response]"   | 메모리 부족

## bop count

b+tree collection에서 하나의 bkey 또는 bkey range 조건과 eflag filter 조건을 만족하는
elements 개수를 구한다.

```
bop count <key> <bkey or "bkey range"> [<eflag_filter>]\r\n
* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
```

- \<key\> - 대상 item의 key string
- \<bkey or "bkey range"\> - 하나의 bkey 또는 bkey range 조회 조건.
Bkey range는 "bkey1..bkey2" 형식으로 표현한다.
- \<eflag_filter\> - eflag filter 조건.
[Collection 기본 개념](ch02-collection-items.md)에서 eflag filter 참조 바란다.

성공 시의 response string은 아래와 같다.

```
COUNT=<count>
```

실패 시의 return string과 그 의미는 아래와 같다.

| Response String                          | 설명                    |
|------------------------------------------|------------------------ |
| "NOT_FOUND"                              | key miss
| "TYPE_MISMATCH"                          | 해당 item이 b+tree collection이 아님
| "BKEY_MISMATCH"                          | 명령 인자로 주어진 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "UNREADABLE"                             | 해당 item이 unreadable item임
| "NOT_SUPPORTED"                          | 지원하지 않음
| "CLIENT_ERROR bad command line format"   | protocol syntax 틀림

## bop incr/decr

B+tree collection 특정 하나의 element에 있는 데이터를 increment 또는 decrement하고,
증감된 데이터를 반환한다.
이 명령은 key-value item에 대한 incr/decr 명령과 유사한 명령으로
이 명령을 수행할 b+tree element의 데이터는 증감이 가능한 숫자형 데이터이어야 한다.

```
bop incr <key> <bkey> <delta> [<initial> [<eflag>]] [noreply|pipe]\r\n
bop decr <key> <bkey> <delta> [<initial> [<eflag>]] [noreply|pipe]\r\n
```

- \<key\> - 대상 item의 key string
- \<bkey\> - 대상 element의 bkey
- \<delta\> - increment/decrement할 delta 값으로서, 0 보다 큰 숫자 값을 가져야 한다.
  - increment 연산으로 64bit unsigned integer가 overflow되면, wrap around되어 잔여 값으로 설정된다.
  - decrement 연산으로 64bit unsigned integer가 underflow되면, 새로운 값은 무조건 0으로 설정된다.
- \<initial\> - 대상 element가 없을 경우, 새로운 element를 생성하고 initial 값으로 설정한다.
  - \<eflag\>는 새로은 element에 eflag 값을 줄 경우에 명시할 수 있다.

성공 시의 response string은 아래와 같다.
Increment/decrement 수행 후의 데이터 값이다.

```
<value>\r\n
```

실패 시의 response string과 그 의미는 아래와 같다.

| Response String                                                 | 설명                    |
|-----------------------------------------------------------------|------------------------ |
| "NOT_FOUND"                                                     | key miss
| "NOT_FOUND_ELEMENT"                                             | element miss
| "TYPE_MISMATCH"                                                 | 해당 item이 b+tree collection이 아님
| "BKEY_MISMATCH"                                                 | 명령 인자로 주언진 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "OUT_OF_RANGE"                                                  | 새로운 element 삽입이 maxcount 또는 maxbkeyrange 제약을 위배하면서 그 element의 bkey 값이 overflowaction에 의해 자동 삭제되는 경우이어서 삽입이 실패하는 경우이다. 예를 들어, smallest_trim 상황에서 새로 삽입할 element의 bkey 값이 b+tree의 smallest bkey 보다 작으면서 maxcount 개의 elements가 이미 존재하거나 maxbkeyrange를 벗어나는 경우가 이에 해당된다.
| "OVERFLOWED"                                                    | overflow 발생
| "NOT_SUPPORTED"                                                 | 지원하지 않음
| "CLIENT_ERROR cannot increment or decrement non-numeric value"  | 해당 element의 데이터가 숫자형이 아님.
| "CLIENT_ERROR bad command line format"                          | protocol syntax 틀림
| "SERVER_ERROR out of memory [writing get response]"             | 메모리 부족

## bop mget

여러 b+tree들에 대해 동일 조회 조건(bkey range와 eflag filter)으로 element들을 한꺼번에 조회한다.
여러 b+tree들에 대한 동일 조회 조건을 사용하므로, 대상 b+tree들은 동일 bkey 유형을 가져야 한다.
그리고, eflag에 대해서도 동일 성격의 데이터를 사용하기를 권고한다.

```
bop mget <lenkeys> <numkeys> <bkey or "bkey range"> [<eflag_filter>] [<offset>] <count>\r\n
<”space separated keys”>\r\n
* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
```

- \<”space separated keys”\> - 대상 b+tree들의 key list로, 스페이스(' ')로 구분한다.
     - 하위 호환성(1.10.X 이하 버전)을 위해 콤마(,)도 지원하지만 권장하지 않는다.
- \<lenkeys\>과 \<numkeys> - key list 문자열의 길이와 key 개수를 나타낸다.
- \<bkey or "bkey range"\> - 하나의 bkey 또는 bkey range 조회 조건. Bkey range는 "bkey1..bkey2" 형식으로 표현한다.
- \<eflag_filter\> - eflag filter 조건. [Collection 기본 개념](ch02-collection-items.md)에서 eflag filter 참조 바란다.
- [\<offset\>] \<count\> - 조회 조건을 만족하는 elements에서 skip 개수와 실제 조회할 개수

bop mget 명령은 O(small N) 수행 원칙을 위하여 다음의 제약 사항을 가진다.
- key list에 지정 가능한 최대 key 수는 200이다.
- count의 최대 값은 50이다.


성공 시의 response string은 다음과 같다.

```
VALUE <key> <status> [<flags> <ecount>]\r\n
[ELEMENT <bkey> [<eflag>] <bytes> <data>\r\n
 ...
 ELEMENT <bkey> [<eflag>] <bytes> <data>\r\n]
VALUE <key> <status> [<flags> <ecount>]\r\n
[ELEMENT <bkey> [<eflag>] <bytes> <data>\r\n
 ...
 ELEMENT <bkey> [<eflag>] <bytes> <data>\r\n]

...

VALUE <key> <status> [<flags> <ecount>]\r\n
[ELEMENT <bkey> [<eflag>] <bytes> <data>\r\n
 ...
 ELEMENT <bkey> [<eflag>] <bytes> <data>\r\n]
END\r\n
```

조회한 대상 key마다 VALUE 라인이 있으며, 대상 key string과 조회 상태가 나타난다.
조회 상태는 아래 중의 하나가 되며, 각 의미는 bop get 명령의 response string을 참조 바란다.

- OK : 정상 조회
- TRIMMED : 정상 조회 But, trimmed element 존재
- NOT_FOUND
- NOT_FOUND_ELEMENT
- OUT_OF_RANGE
- TYPE_MISMATCH
- BKEY_MISMATCH
- UNREADABLE

조회 상태가 정상 조회된 상태인 "OK"와 "TRIMMED"이면,
그 key에 설정된 flags 값과 조회한 element 개수가 나오며,
다음 라인부터 조회한 각 element의 bkey  optional eflag, data 길이와 data 그 자체가 나온다.
그 외의 조회 상태는 해당 key에서 element 조회를 실패한 경우이므로,
flags와 ecount를 포함하여 조회된 element 정보가 생략된다.

실패 시의 response string과 그 의미는 다음과 같다.

| Response String                                       | 설명                    |
|-------------------------------------------------------|------------------------ |
| "NOT_SUPPORTED"                                       | 지원하지 않음
| "CLIENT_ERROR bad command line format"                | protocol syntax 틀림
| "CLIENT_ERROR bad data chunk"                         | space separated key list의 길이가 \<lenkeys\>와 다르거나 “\r\n”으로 끝나지 않음
| "CLIENT_ERROR bad value"                              | bop mget 명령의 제약 조건을 위배함.
| "SERVER_ERROR out of memory [writing get response]"   | 메모리 부족

## bop smget

여러 b+tree들에서 bkey range 조건과 eflag filter 조건을 모두 만족하는
elements를 sort merge 형태로 조회하면서 count 개의 elements를 가져온다.
즉, 여러 b+tree들을 하나의 large b+tree로 구성되어 있다고 보고,
이에 대한 element 조회 기능과 동일하다.

smget 동작은 조회 범위와 어떤 b+tree의 trim 영역과의 겹침에 대한 처리로,
아래 두 가지 동작 모드가 있다.

1) 기존 smget 동작 (1.8.X 이하 버전에서 동작하던 방식)
   - smget 조회 조건을 만족하는 첫번째 element가 trim된 b+tree가 하나라도 존재하면 OUT_OF_RANGE 응답을 보낸다.
     이 경우, 응용은 모든 key에 대해 백엔드 저장소인 DB에서 elements 조회한 후에
     응용에서 sort-merge 작업을 수행하여야 한다.
   - OUT_OF_RANGE가 없는 상황에서 smget을 수행하면서
     조회 조건을 만족하는 두번째 이후의 element가 trim된 b+tree를 만나게 되면,
     그 지점까지 조회한 elements를 최종 elements 결과로 하고
     smget 수행 상태는 TRIMMED로 하여 응답을 보낸다.
     이 경우, 응용은 모든 key에 대해 백엔드 저장소인 DB에서 trim 영역의 elements를 조회하여
     smget 결과에 반영하여야 한다.

2) 신규 smget 동작 (1.9.0 이후 버전에서 추가된 방식)
   - 기존의 OUT_OF_RANGE에 해당하는 b+tree를 missed keys로 분류하고
     나머지 b+tree들에 대해 smget을 계속 수행한다.
     따라서, 응용에서는 missed keys에 한해서만
     백엔드 저장소인 DB에서 elements를 조회하여 최종 smget 결과에 반영할 수 있다.
   - smget 조회 조건을 만족하는 두번째 이후의 element가 trim된 b+tree가 존재하더라도,
     그 지점에서 smget을 중지하는 것이 아니라, 그러한 b+tree를 trimmed keys로 분류하고
     원하는 개수의 elements를 찾을 때까지 smget을 계속 진행한다.
     따라서, 응용에서는 trimmed keys에 한하여
     백엔드 저장소인 DB에서 trim된 elements를 조회하여 최종 smget 결과에 반영할 수 있다.
   - bkey에 대한 unique 조회 기능을 지원한다.
     중복 bkey를 허용하여 조회하는 duplcate 조회 외에
     중복 bkey를 제거하고 unique bkey만을 조회하는 unique 조회를 지원한다.
   - 조회 조건에 offset 기능을 제거한다.

기존 smget 연산을 사용하더라도, offset 값은 항상 0으로 사용하길 권고한다.
양수의 offset을 사용하는 smget에서 missed keys가 존재하고
missed keys에 대한 DB 조회가 offset으로 skip된 element를 가지는 경우,
응용에서 정확한 offset 처리가 불가능해지기 때문이다.
이전의 조회 결과에 이어서 추가로 조회하고자 하는 경우,
이전에 조회된 bkey 값을 바탕으로 bkey range를 재조정하여 사용할 수 있다.

```
bop smget <lenkeys> <numkeys> <bkey or "bkey range"> [<eflag_filter>] <count> [duplicate|unique]\r\n
<"space separated keys">\r\n
* <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
```

- \<”space separated keys”\> - 대상 b+tree들의 key list로, 스페이스(' ')로 구분한다.
     - 하위 호환성(1.10.X 이하 버전)을 위해 콤마(,)도 지원하지만 권장하지 않는다.
- \<lenkeys\>과 \<numkeys> - key list 문자열의 길이와 key 개수를 나타낸다.
- \<bkey or "bkey range"\> - 하나의 bkey 또는 bkey range 조회 조건.
Bkey range는 "bkey1..bkey2" 형식으로 표현한다.
- \<eflag_filter\> - eflag filter 조건.
[Collection 기본 개념](ch02-collection-items.md)에서 eflag filter 참조 바란다.
- \<count\> - 조회할 element 개수
- [duplicate|unique] - smget 동작 방식을 지정한다.
  - 생략되면, 예전 smget 동작을 수행한다.
  - 지정되면, 신규 smget 동작을 수행한다. duplicate는 중복 bkey를 허용하고, unique는 중복 bkey를 제거한다.

bop smget 명령은 O(small N) 수행 원칙을 위하여 다음의 제약 사항을 가진다.
- key list에 지정 가능한 최대 key 수는 10000이다.
- count의 최대 값은 2000이다.

기존 smget 동작에서 성공 시의 response string은 다음과 같다.

```
VALUE <ecount>\r\n
<key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
<key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
<key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
...
MISSED_KEYS <kcount>\r\n
<key>\r\n
<key>\r\n
…
END|DUPLICATED|TRIMMED|DUPLICATRED_TRIMMED\r\n
```

위 response string에 대한 설명은 다음과 같다.

- VALUE 부분: 조회한 elements를 나타낸다.
  - Element 정보는 조회한 element가 속한 b+tree의 key string과 flags 정보
    그리고 그 element의 bkey, optional eflag, data로 구성된다.
  - Element 정보는 bkey 기준으로 정렬되며,
    동일 bkey를 가진 elements는 key string 기준으로 정렬된다.
- MISSED_KEYS 부분: smget 조회에 참여하지 못한 key list와 그 원인을 나타낸다.
  - \<key\>는 smget에 참여하지 못한 key string이다.
- 마지막 라인은 smget response string의 마지막을 나타낸다.
  - END: 조회 결과에 중복 bkey가 없음
  - DUPLICATED: 조회 결과에 중복 bkey가 있음.
  - TRIMMED: 조회 범위가 trim 영역과 겹치는 b+tree를 발견한 상태이다.
  - DUPLICATED_TRIMMED: DUPLICATED와 TRIMMED 의미를 모두 가진다.

신규 smget 동작에서 성공 시의 response string은 다음과 같다.

```
ELEMENTS <ecount>\r\n
<key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
<key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
<key> <flags> <bkey> [<eflag>] <bytes> <data>\r\n
...
MISSED_KEYS <kcount>\r\n
<key> <cause>\r\n
<key> <cause>\r\n
…
TRIMMED_KEYS <kcount>\r\n
<key> <bkey>\r\n
<key> <bkey>\r\n
…
END|DUPLICATED\r\n

* <cause> = NOT_FOUND | UNREADABLE | OUT_OF_RANGE
```

위 response string에 대한 설명은 다음과 같다.

- ELEMENTS 부분: 조회한 elements를 나타낸다.
  - Element 정보는 조회한 element가 속한 b+tree의 key string과 flags 정보
    그리고 그 element의 bkey, optional eflag, data로 구성된다.
  - Element 정보는 bkey 기준으로 정렬되며,
    동일 bkey를 가진 elements는 key string 기준으로 정렬된다.
- MISSED_KEYS 부분: smget 조회에 참여하지 못한 key list와 그 원인을 나타낸다.
  - \<key\>는 smget에 참여하지 못한 key string이다.
  - \<cause\>는 smget에 참여하지 못한 원인을 나타낸다.
    - NOT_FOUND: 그 key가 cache에 존재하지 않음
    - UNREADABLE: 그 key가 unreadable 상태에 있음
    - OUT_OF_RANGE: bkey range의 시작 부분이 그 key의 trim 영역과 겹쳐 있음
- TRIMMED_KEYS 부분: smget 조회 범위의 뒷 부분에서 trim이 발생한 key list이다.
  - \<key\>는 trim이 발생한 key string이다.
  - \<bkey\>는 trim 직전에 있던 마지막 bkey 이다.
  - Timmed keys 정보는 bkey 기준으로 정렬된다.
- 마지막 라인은 smget response string의 마지막을 나타낸다.
  - END: 조회 결과에 중복 bkey가 없음
  - DUPLICATED: 조회 결과에 중복 bkey가 있음.


smget 수행의 실패 시의 response string은 다음과 같다.

| Response String                                      | 설명                    |
|------------------------------------------------------|------------------------ |
| "TYPE_MISMATCH"                                      | 어떤 key가 b+tree type이 아님
| "BKEY_MISMATCH"                                      | smget에 참여된 b+tree들의 bkey 유형이 서로 다름.
| "OUT_OF_RANGE"                                       | 기존 smget 동작에서만 발생할 수 있는 실패 response string이다.
| "NOT_SUPPORTED"                                      | 지원하지 않음
| "CLIENT_ERROR bad command line format"               | protocol syntax 틀림
| "CLIENT_ERROR bad data chunk"                        | 주어진 key 리스트에 중복 key가 존재하거나 주어진 key 리스트의 길이가 \<lenkeys\> 길이와 다르거나 "\r\n"으로 끝나지 않음.
| "CLIENT_ERROR bad value"                             | 앞서 기술한 smget 연산의 제약 조건을 위배
| "SERVER_ERROR out of memory [writing get response"   | 메모리 부족


## bop position

b+tree collection에서 특정 element의 position을 조회한다.
Element의 position이란 b+tree에서의 위치 정보로서,
bkey들의 정렬(ASC or DESC) 기준으로 몇 번째 위치한 element인지를 나타내는
0부터 N-1까지의 index를 의미한다.

```
bop position <key> <bkey> <order>\r\n
* <order> = asc | desc
```

- \<key\> - 대상 item의 key string
- \<bkey\> - 대상 element의 bkey
- \<order\> - 어떤 bkey 정렬 기준으로 position을 얻을 것인지 명시

성공 시의 response string은 아래와 같다.

```
POSITION=<position>\r\n
```

실패 시의 response string과 그 의미는 아래와 같다.

| Response String                          | 설명                    |
|------------------------------------------|------------------------ |
| "NOT_FOUND"                              | key miss
| "NOT_FOUND_ELEMENT"                      | element miss
| "TYPE_MISMATCH"                          | b+tree collection 아님
| "BKEY_MISMATCH"                          | 명령 인자로 주어진 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "UNREADABLE"                             | 해당 item이 unreadable item임
| "NOT_SUPPORTED"                          | 지원하지 않음
| "CLIENT_ERROR bad command line format"   | protocol syntax 틀림

## bop gbp

B+tree collection에서 position 기반으로 elements를 조회한다.

```
bop gbp <key> <order> <position or "position range">\r\n
* <order> = asc | desc
```

- \<key\> - 대상 item의 key string
- \<order\> - 어떤 bkey 정렬 기준으로 position을 적용할 지를 명시
- \<position or "position range"\> - 조회할 elements의 하나의 position 또는 position range.
Position range는 "position1..position2" 형식으로 표현.

성공 시의 response string은 아래와 같다.
bop get 성공 시의 response string을 참조 바란다.

```
VALUE <flags> <count>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
…
END\r\n
```

실패 시의 response string과 그 의미는 아래와 같다.

| Response String                                      | 설명                    |
|------------------------------------------------------|------------------------ |
| "NOT_FOUND"                                          | key miss
| "NOT_FOUND_ELEMENT"                                  | element miss
| "TYPE_MISMATCH"                                      | b+tree collection 아님
| "UNREADABLE"                                         | 해당 item이 unreadable item임
| "NOT_SUPPORTED"                                      | 지원하지 않음
| "CLIENT_ERROR bad command line format"               | protocol syntax 틀림
| "SERVER_ERROR out of memory [writing get response]"  | 메모리 부족

## bop pwg
Available since 1.8.0

B+tree collection에서 특정 bkey의 position을 조회하면서,
그 bkey를 가진 element를 포함하여 앞뒤에(양방향) 위치한 element N개 씩을 한번에 조회한다.

```
bop pwg <key> <bkey> <order> [<count>]\r\n
* <order> = asc | desc
```

- \<key\> - 대상 item의 key string
- \<bkey\> - 대상 element의 bkey
- \<order\> - 어떤 bkey 정렬 기준으로 position을 얻을 것인지 명시
- \<count\> - 조회한 position의 앞뒤에서 각각 몇 개의 element를 조회할 것인지를 명시 (**최대 값은 100으로 제한**)
  - 0이면, 조회한 position의 element만 조회
  - 양수이면, 조회한 position의 element 외에 그 position의 앞뒤에서 각각 그 수만큼 element 조회

성공 시의 response string은 아래와 같다.

```
VALUE <position> <flags> <count> <index>\r\n
<bkey> [<eflag>] <bytes> <data>\r\n
...
<bkey> [<eflag>] <bytes> <data>\r\n
END\r\n
```

위의 VALUE 라인에서 각 값의 의미는 다음과 같다.
그 아래 라인들에서 element 값의 표현은 bop get 경우와 동일하다.

- \<position\> : 주어진 bkey의 position
- \<flags\> : b+tree item의 flags 속성값
- \<count\> : 조회한 전체 element 개수
- \<index\> : 전체 element list에서 주어진 bkey를 가진 element 위치 (0-based index)
  - 주어진 bkey의 position과 element만 조회하면, count는 1이 되고, index는 0이 된다.
  - 주어진 bkey의 position과 element 외에 양방향 10개 element 조회에서,
    그 position 앞에 5개 element가 존재하고 뒤에 10개 element가 존재한다면
    count는 (5 + 1 + 10) = 16이 되고, index는 5가 된다.

실패 시의 response string과 그 의미는 아래와 같다.

| Response String                                     | 설명                    |
|-----------------------------------------------------|-------------------------|
| "NOT_FOUND"                                         | key miss
| "NOT_FOUND_ELEMENT"                                 | element miss
| "TYPE_MISMATCH"                                     | b+tree collection 아님
| "BKEY_MISMATCH"                                     | 명령 인자로 주어진 bkey 유형과 대상 b+tree의 bkey 유형이 다름
| "UNREADABLE"                                        | 해당 item이 unreadable item임
| "NOT_SUPPORTED"                                     | 지원하지 않음
| "CLIENT_ERROR bad command line format"              | protocol syntax 틀림
| "SERVER_ERROR out of memory [writing get response]" | 메모리 부족
