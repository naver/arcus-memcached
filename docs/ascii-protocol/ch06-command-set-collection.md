# Chapter 6. SET 명령

Set collection에 관한 명령은 아래와 같다.

- [Set collection 생성: sop create](#sop-create)
- Set collection 삭제: delete (기존 key-value item의 삭제 명령을 그대로 사용)

Set element에 관한 명령은 아래와 같다.

- [Set element 삽입: sop insert](#sop-insert)
- [Set element 삭제: sop delete](#sop-delete)
- [Set element 조회: sop get](#sop-get)
- [Set element 존재유무 검사: sop exist](#sop-exist)

## sop create

Set collection을 empty 상태로 생성한다.

```
sop create <key> <attributes> [noreply]\r\n
* <attributes>: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<attributes\> - 설정할 item attributes. [Item Attribute 설명](ch03-item-attributes.md)을 참조 바란다.
  - unreadable - 명시하면, readable 속성은 off로 설정됩니다.
- noreply - 명시하면, response string을 전달받지 않는다.

Response string과 그 의미는 아래와 같다.

| Response String                          | 설명                    |
|------------------------------------------|------------------------ |
| "CREATED"                                | 성공
| "EXISTS"                                 | 동일 key string을 가진 item이 이미 존재
| "NOT_SUPPORTED"                          | 지원하지 않음
| "CLIENT_ERROR bad command line format"   | protocol syntax 틀림
| "SERVER_ERROR out of memory"             | 메모리 부족

## sop insert

Set collection에 하나의 element를 삽입한다.
Set collection을 생성하면서 하나의 element를 삽입할 수도 있다.

```
sop insert <key> <bytes> [create <attributes>] [noreply|pipe]\r\n<data>\r\n
* <attributes>: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<bytes\> - 삽입할 데이터 길이 (trailing 문자인 "\r\n"을 제외한 길이)
- create \<attributes\> - set collection 없을 시에 set 생성 요청.
[Item Attribute 설명](ch03-item-attributes.md)을 참조 바란다.
  - unreadable - 명시하면, readable 속성은 off로 설정됩니다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다.
pipe 사용은 [Command Pipelining](ch09-command-pipelining.md)을 참조 바란다.
- \<data\> - 삽입할 데이터 (최대 크기는 [기본제약사항](ch01-arcus-basic-concept.md#basic-constraints)을 참고)

Response string과 그 의미는 아래와 같다.

| Response String                          | 설명                    |
|------------------------------------------|------------------------ |
| "STORED"                                 | 성공 (element만 삽입)
| "CREATED_STORED"                         | 성공 (collection 생성하고 element 삽입)
| "NOT_FOUND"                              | key miss
| "TYPE_MISMATCH"                          | 해당 item이 set collection이 아님
| "OVERFLOWED"                             | overflow 발생
| "ELEMENT_EXISTS"                         | 동일 데이터를 가진 element가 존재. set uniqueness 위배
| "NOT_SUPPORTED"                          | 지원하지 않음
| "CLIENT_ERROR bad command line format"   | protocol syntax 틀림
| "CLIENT_ERROR too large value"           | 삽입할 데이터가 element value의 최대 크기보다 큼
| "CLIENT_ERROR bad data chunk"            | 삽입할 데이터 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
| "SERVER_ERROR out of memory"             | 메모리 부족

## sop delete

Set collection에서 하나의 element를 삭제한다.

```
sop delete <key> <bytes> [drop] [noreply|pipe]\r\n<data>\r\n
```

- \<key\> - 대상 item의 key string
- \<bytes\> - 삭제할 데이터 길이 (trailing 문자인 "\r\n"을 제외한 길이)
- drop - element 삭제로 인해 empty set이 될 경우, 그 set을 drop할 것인지를 지정한다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다.
pipe 사용은 [Command Pipelining](ch09-command-pipelining.md)을 참조 바란다.
- \<data\> - 삭제할 데이터

Response string과 그 의미는 아래와 같다.

| Response String                         | 설명                    |
|-----------------------------------------|------------------------ |
| "DELETED"                               | 성공 (element만 삭제)
| "DELETED_DROPPED"                       | 성공 (element 삭제하고 collection을 drop한 상태)
| "NOT_FOUND"                             | key miss
| "NOT_FOUND_ELEMENT"                     | element miss (삭제할 element가 없음)
| "TYPE_MISMATCH"                         | 해당 item이 set collection이 아님
| "NOT_SUPPORTED"                         | 지원하지 않음
| "CLIENT_ERROR bad command line format"  | protocol syntax 틀림
| "CLIENT_ERROR too large value"          | 삭제할 데이터가 element value의 최대 크기보다 큼
| "CLIENT_ERROR bad data chunk"           | 삭제할 데이터의 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음

## sop get

Set collection에서 N 개의 elements를 조회한다.

```
sop get <key> <count> [delete|drop]\r\n
```

- \<key\> - 대상 item의 key string
- \<count\> - 조회할 elements 개수를 지정. 0이면 전체 elements를 의미한다.
- delete or drop - element 조회하면서 그 element를 delete할 것인지,
그리고 delete로 인해 empty set이 될 경우 그 set을 drop할 것인지를 지정한다.

sop get 명령은 다음과 같은 특징을 갖는다.
- count 값은 0 또는 양수이며, 양수인 경우 elements를 랜덤하게 조회한다.
- long request를 방지하기 위해 count의 최대값은 1000으로 제한한다.

성공 시의 response string은 아래와 같다.
VALUE 라인의 \<count\>는 조회된 element 개수를 의미한다.
마지막 라인은 END, DELETED, DELETED_DROPPED 중의 하나를 가지며
각각 element 조회만 수행한 상태, element 조회하고 삭제한 상태,
element 조회 및 삭제하고 set을 drop한 상태를 의미한다.

```
VALUE <flags> <count>\r\n
<bytes> <data>\r\n
<bytes> <data>\r\n
<bytes> <data>\r\n
...
END|DELETED|DELETED_DROPPED\r\n
```

실패 시의 response string과 그 의미는 아래와 같다.

| Response String                                      | 설명                    |
|------------------------------------------------------|------------------------ |
| "NOT_FOUND"                                          | key miss
| "NOT_FOUND_ELEMENT"                                  | element miss (element가 존재하지 않는 상태임)
| "TYPE_MISMATCH"                                      | 해당 item이 set collection이 아님
| "UNREADABLE"                                         | 해당 item이 unreadable item임
| "NOT_SUPPORTED"                                      | 지원하지 않음
| "CLIENT_ERROR invalid: too many count"               | count 제약 개수를 초과함
| "CLIENT_ERROR bad command line format"               | protocol syntax 틀림
| "SERVER_ERROR out of memory [writing get response]"  | 메모리 부족

## sop exist

Set collection에 특정 element의 존재 유무를 검사한다.

```
sop exist <key> <bytes> [pipe]\r\n<data>\r\n
```

- \<key\> - 대상 item의 key string
- \<bytes\>와 \<data\> - 존재 유무를 검사할 데이터의 길이와 데이터 그 자체
- pipe - 명시하면, response string을 전달받지 않는다.
[Command Pipelining](ch09-command-pipelining.md)을 참조 바란다.

Response string과 그 의미는 아래와 같다.

| Response String                          | 설명                     |
|------------------------------------------|------------------------ |
| "EXIST"                                  | 성공 (주어진 데이터가 set에 존재)
| "NOT_EXIST"                              | 성공 (주어진 데이터가 set에 존재하지 않음)
| "NOT_FOUND"                              | key miss
| "TYPE_MISMATCH"                          | 해당 item이 set collection이 아님
| "UNREADABLE"                             | 해당 item이 unreadable item임
| "NOT_SUPPORTED"                          | 지원하지 않음
| "CLIENT_ERROR bad command line format"   | protocol syntax 틀림
| "CLIENT_ERROR too large value"           | 주어진 데이터가 element value의 최대 크기보다 큼
| "CLIENT_ERROR bad data chunk"            | 주어진 데이터의 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
