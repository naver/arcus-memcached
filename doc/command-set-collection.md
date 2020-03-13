SET 명령
--------

Set collection에 관한 명령은 아래와 같다.

- [Set collection 생성: sop create](command-set-collection.md#sop-create---set-collection-%EC%83%9D%EC%84%B1)
- Set collection 삭제: delete (기존 key-value item의 삭제 명령을 그대로 사용)

Set element에 관한 명령은 아래와 같다. 

- [Set element 삽입: sop insert](command-set-collection.md#sop-insert---set-element-%EC%82%BD%EC%9E%85)
- [Set element 삭제: sop delete](command-set-collection.md#sop-delete---set-element-%EC%82%AD%EC%A0%9C)
- [Set element 조회: sop get](command-set-collection.md#sop-get---set-element-%EC%A1%B0%ED%9A%8C)
- [Set element 존재유무 검사: sop exist](command-set-collection.md#sop-exist---set-element-%EC%A1%B4%EC%9E%AC%EC%9C%A0%EB%AC%B4-%EA%B2%80%EC%82%AC)

### sop create (Set Collection 생성)

Set collection을 empty 상태로 생성한다.

```
sop create <key> <attributes> [noreply]\r\n
* <attributes>: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<attributes\> - 설정할 item attributes. [Item Attribute 설명](/doc/arcus-item-attribute.md)을 참조 바란다.
- noreply - 명시하면, response string을 전달받지 않는다.

Response string과 그 의미는 아래와 같다.

- "CREATED" - 성공
- "EXISTS" - 동일 key string을 가진 item이 이미 존재
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- “SERVER_ERROR out of memory” - 메모리 부족

### sop insert (Set Element 삽입)

Set collection에 하나의 element를 삽입한다.
Set collection을 생성하면서 하나의 element를 삽입할 수도 있다.

```
sop insert <key> <bytes> [create <attributes>] [noreply|pipe]\r\n<data>\r\n
* <attributes>: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<bytes\> - 삽입할 데이터 길이 (trailing 문자인 "\r\n"을 제외한 길이)
- create \<attributes\> - set collection 없을 시에 set 생성 요청.
                    [Item Attribute 설명](/doc/arcus-item-attribute.md)을 참조 바란다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다. 
                    pipe 사용은 [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.
- \<data\> - 삽입할 데이터 (최대 4KB)

Response string과 그 의미는 아래와 같다.

- "STROED" - 성공 (element만 삽입)
- “CREATED_STORED” - 성공 (collection 생성하고 element 삽입)
- “NOT_FOUND” - key miss
- “TYPE_MISMATCH” - 해당 item이 set collection이 아님
- “OVERFLOWED” - overflow 발생
- "ELEMENT_EXISTS" - 동일 데이터를 가진 element가 존재. set uniqueness 위배
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- “CLIENT_ERROR too large value” - 삽입할 데이터가 4KB 보다 큼
- “CLIENT_ERROR bad data chunk” - 삽입할 데이터 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
- “SERVER_ERROR out of memory” - 메모리 부족

### sop delete (Set Element 삭제)

Set collection에서 하나의 element를 삭제한다.

```
sop delete <key> <bytes> [drop] [noreply|pipe]\r\n<data>\r\n
```

- \<key\> - 대상 item의 key string
- \<bytes\> - 삭제할 데이터 길이 (trailing 문자인 "\r\n"을 제외한 길이)
- drop - element 삭제로 인해 empty set이 될 경우, 그 set을 drop할 것인지를 지정한다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다. 
                    pipe 사용은 [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.
- \<data\> - 삭제할 데이터 (최대 4KB)

Response string과 그 의미는 아래와 같다.

- "DELETED" - 성공 (element만 삭제)
- “DELETED_DROPPED” - 성공 (element 삭제하고 collection을 drop한 상태)
- “NOT_FOUND” - key miss
- “NOT_FOUND_ELEMENT” - element miss (삭제할 element가 없음)
- “TYPE_MISMATCH” - 해당 item이 set collection이 아님
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- “CLIENT_ERROR too large value” - 삭제할 데이터가 4KB 보다 큼
- “CLIENT_ERROR bad data chunk” - 삭제할 데이터의 길이가 \<bytes\>와 다르거나 “\r\n”으로 끝나지 않음

### sop get (Set Element 조회)

Set collection에서 N 개의 elements를 조회한다.

```
sop get <key> <count> [delete|drop]\r\n
```

- \<key\> - 대상 item의 key string
- \<count\> - 조회할 elements 개수를 지정. 0이면 전체 elements를 의미한다.
- delete or drop - element 조회하면서 그 element를 delete할 것인지
                   그리고 delete로 인해 empty set이 될 경우 그 set을 drop할 것인지를 지정한다.

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

- “NOT_FOUND”	- key miss
- “NOT_FOUND_ELEMENT”	- element miss (element가 존재하지 않는 상태임)
- “TYPE_MISMATCH”	- 해당 item이 set collection이 아님
- “UNREADABLE” - 해당 item이 unreadable item임
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- "SERVER_ERROR out of memory [writing get response]”	- 메모리 부족

### sop exist (Set Element 존재유무 검사)

Set collection에 특정 element의 존재 유무를 검사한다.

```
sop exist <key> <bytes> [pipe]\r\n<data>\r\n
```

- \<key\> - 대상 item의 key string
- \<bytes\>와 \<data\> - 존재 유무를 검사할 데이터의 길이와 데이터 그 자체 (최대 4KB)
- pipe - 명시하면, response string을 전달받지 않는다. 
         [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.

Response string과 그 의미는 아래와 같다.

- “EXIST" - 성공 (주어진 데이터가 set에 존재)
- "NOT_EXIST" - 성공 (주어진 데이터가 set에 존재하지 않음)
- “NOT_FOUND”	- key miss
- “TYPE_MISMATCH”	- 해당 item이 set collection이 아님
- “UNREADABLE” - 해당 item이 unreadable item임
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- “CLIENT_ERROR too large value” : 주어진 데이터가 4KB 보다 큼
- “CLIENT_ERROR bad data chunk” : 주어진 데이터의 길이가 \<bytes\>와 다르거나 “\r\n”으로 끝나지 않음
 

