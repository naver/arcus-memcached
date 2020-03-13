LIST 명령
---------

List collection에 관한 명령은 아래와 같다.

- [List collection 생성: lop create](command-list-collection.md#lop-create---list-collection-%EC%83%9D%EC%84%B1)
- List collection 삭제: delete (기존 key-value item의 삭제 명령을 그대로 사용)

List element에 관한 명령은 아래와 같다.

- [List element 삽입: lop insert](command-list-collection.md#lop-insert---list-element-%EC%82%BD%EC%9E%85)
- [List element 삭제: lop delete](command-list-collection.md#lop-delete---list-element-%EC%82%AD%EC%A0%9C)
- [List element 조회: lop get](command-list-collection.md#lop-get---list-element-%EC%A1%B0%ED%9A%8C)

### lop create (List Collection 생성)

List collection을 empty 상태로 생성한다.

```
lop create <key> <attributes> [noreply]\r\n
* attributes: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
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

### lop insert (List Element 삽입)

List collection에 하나의 element를 삽입한다.
List collection을 생성하면서 하나의 element를 삽입할 수도 있다.

```
lop insert <key> <index> <bytes> [create <attributes>] [noreply|pipe]\r\n<data>\r\n
* attributes: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<index\> - 삽입 위치를 0-based index로 지정.
  - 0, 1, 2, ... : list의 앞에서 시작하여 각 element 위치를 나타냄
  - -1, -2, -3, ... : list의 뒤에서 시작하여 각 element 위치를 나타냄
- \<bytes\> - 삽입할 데이터 길이 (trailing 문자인 "\r\n"을 제외한 길이)
- create \<attributes\> - list collection 없을 시에 list 생성 요청.
                    [Item Attribute 설명](/doc/arcus-item-attribute.md)을 참조 바란다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다. 
                    pipe 사용은 [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.
- \<data\> - 삽입할 데이터 (최대 4KB)
 
Response string과 그 의미는 아래와 같다.

- "STROED" - 성공 (element만 삽입)
- “CREATED_STORED” - 성공 (collection 생성하고 element 삽입)
- “NOT_FOUND” - key miss
- “TYPE_MISMATCH” - 해당 item이 list collection이 아님
- “OVERFLOWED” - overflow 발생
- “OUT_OF_RANGE” - 삽입 위치가 list의 현재 element index 범위를 넘어섬,
                   예를 들어, 10개 element가 있는 상태에서 삽입 위치가 20인 경우임
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- “CLIENT_ERROR too large value” - 삽입할 데이터가 4KB 보다 큼
- “CLIENT_ERROR bad data chunk” - 삽입할 데이터 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
- “SERVER_ERROR out of memory” - 메모리 부족

### lop delete (List Element 삭제)

List collection에 하나의 index 또는 index range에 해당하는 elements를 삭제한다.

```
lop delete <key> <index or "index range"> [drop] [noreply|pipe]\r\n
lop delete 명령에서 각 인자의 설명은 아래와 같다.
```
- \<key\> - 대상 item의 key string
- \<index or "index range"\> - 삭제할 element의 index or index range.
  Element index는 "lop insert" 명령에서 소개한 바와 같이 0-based index 형태로 지정하며,
  index range는 index1..index2 형태로 표현하여, 그 예는 다음과 같다.
  - 0..-1: 첫째 element부터 마지막 element까지 (forward 순서)
  - 2..-2: 앞의 3번째 element부터 뒤의 2번째 element까지 (forward 순서)
  - -3..-1: 뒤의 3번째 element부터 뒤의 1번째 element까지 (forward 순서)
  - 4..2 : 앞의 5번째 element 부터 앞의 3번째 element까지 (backward 순서)
  - -1..0: 마지막 element 부터 첫째 element 까지 (backward 순서)
- drop - element 삭제로 인해 empty list가 될 경우, 그 list를 drop할 것인지를 지정한다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다. 
                    pipe 사용은 [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.

Response string과 그 의미는 아래와 같다.

- "DELETED" - 성공 (element만 삭제)
- “DELETED_DROPPED” - 성공 (element 삭제하고 list를 drop한 상태)
- “NOT_FOUND” - key miss
- “NOT_FOUND_ELEMENT” - element miss (single index or index range에 해당하는 element가 없음)
- “TYPE_MISMATCH” - 해당 item이 list collection이 아님
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림

### lop get (List Element 조회)

List collection에 하나의 index 또는 index range에 해당하는 elements를 조회한다.

```
lop get <key> <index or "index range"> [delete|drop]\r\n
```

- \<key\> - 대상 item의 key string
- \<index or "index range"\> - 조회할 element의 index or index range. "lop delete" 명령의 인자 참조
- delete or drop - element 조회하면서 그 element를 delete할 것인지
                   그리고 delete로 인해 empty list가 될 경우 그 list를 drop할 것인지를 지정한다.

성공 시의 response string은 아래와 같다.
VALUE 라인의 \<count\>는 조회된 element 개수를 의미한다.
마지막 라인은 END, DELETED, DELETED_DROPPED 중의 하나를 가지며,
각각 element 조회만 수행한 상태, element 조회하고 삭제한 상태,
element 조회 및 삭제하고 list를 drop한 상태를 의미한다.

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
- “NOT_FOUND_ELEMENT”	- element miss (index or index range에 해당하는 element가 없음)
- “TYPE_MISMATCH”	- 해당 item이 list collection이 아님
- “UNREADABLE” - 해당 item이 unreadable item임
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- "SERVER_ERROR out of memory [writing get response]”	- 메모리 부족

<!-- reference list -->
[item-attribute]: /doc/arcus-item-attribute.md "Item Attribute 설명"
[command-pipelining]: /doc/command-pipelining.md "Command Pipelining"
