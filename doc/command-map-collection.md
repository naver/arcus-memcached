MAP 명령
--------

Map collection에 관한 명령은 아래와 같다.

- [Map collection 생성: mop create](command-map-collection.md#mop-create---map-collection-생성)
- Map collection 삭제: delete (기존 key-value item의 삭제 명령을 그대로 사용)

Map element에 관한 명령은 아래와 같다. 

- [Map element 삽입: mop insert](command-map-collection.md#mop-insert---map-element-삽입)
- [Map element 변경: mop update](command-map-collection.md#mop-update---map-element-변경)
- [Map element 삭제: mop delete](command-map-collection.md#mop-delete---map-element-삭제)
- [Map element 조회: mop get](command-map-collection.md#mop-get---map-field-element-조회)

### mop create (Map Collection 생성)

Map collection을 empty 상태로 생성한다.

```
mop create <key> <attributes> [noreply]\r\n
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

### mop insert (Map Element 삽입)

Map collection에 하나의 field, element를 삽입한다.
Map collection을 생성하면서 \<field, value\>로 구성된 하나의 element를 삽입할 수도 있다.

```
mop insert <key> <field> <bytes> [create <attributes>] [noreply|pipe]\r\n<data>\r\n
* <attributes>: <flags> <exptime> <maxcount> [<ovflaction>] [unreadable]
```

- \<key\> - 대상 item의 key string
- \<field\> - 삽입할 element의 field string
- \<bytes\> - 삽입할 element의 데이터 길이 (trailing 문자인 "\r\n"을 제외한 길이)
- create \<attributes\> - 해당 map collection 없을 시에 map 생성 요청.
                    [Item Attribute 설명](/doc/arcus-item-attribute.md)을 참조 바란다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다. 
                    pipe 사용은 [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.
- \<data\> - 삽입할 데이터 (최대 4KB)

Response string과 그 의미는 아래와 같다.

- "STROED" - 성공 (field, element 삽입)
- “CREATED_STORED” - 성공 (collection 생성하고 field, element 삽입)
- “NOT_FOUND” - key miss
- “TYPE_MISMATCH” - 해당 item이 map collection이 아님
- “OVERFLOWED” - overflow 발생
- "ELEMENT_EXISTS" - 동일 이름의 field가 이미 존재. map field uniqueness 위배
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- “CLIENT_ERROR too large value” - 삽입할 데이터가 4KB 보다 큼
- “CLIENT_ERROR bad data chunk” - 삽입할 데이터 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
- “CLIENT_ERROR invalid prefix name” - 유효하지(존재하지) 않는 prefix 명
- “SERVER_ERROR out of memory” - 메모리 부족

### mop update (Map Element 변경)

Map collection에서 하나의 field에 대해 element 변경을 수행한다.
현재 다수 field에 대한 변경연산은 제공하지 않는다.

```
mop update <key> <field> <bytes> [noreply|pipe]\r\n<data>\r\n
```

- \<key\> - 대상 item의 key string
- \<field\> - 대상 element의 field string
- \<bytes\> - 변경할 데이터 길이 (trailing 문자인 "\r\n"을 제외한 길이)
- noreply or pipe - 명시하면, response string을 전달받지 않는다. 
                    pipe 사용은 [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.
- \<data\> - 변경할 데이터 자체 (최대 4KB)

Response string과 그 의미는 아래와 같다.

- "UPDATED" - 성공 (element 변경)
- “NOT_FOUND” - key miss
- "NOT_FOUND_ELEMENT" - field miss
- “TYPE_MISMATCH” - 해당 item이 map collection이 아님
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- “CLIENT_ERROR too large value” - 삽입할 데이터가 4KB 보다 큼
- “CLIENT_ERROR bad data chunk” - 삽입할 데이터 길이가 \<bytes\>와 다르거나 "\r\n"으로 끝나지 않음
- “SERVER_ERROR out of memory” - 메모리 부족

### mop delete (Map Element 삭제)

Map collection에서 하나 이상의 field 이름을 주어, 그에 해당하는 element를 삭제한다.

```
mop delete <key> <lenfields> <numfields> [drop] [noreply|pipe]\r\n
[<"space separated fields">]\r\n
```

- "space separated fields" - 대상 map의 field list로, 스페이스(' ')로 구분한다.
                           - 하위 호환성(1.10.X 이하 버전)을 위해 콤마(,)도 지원하지만 권장하지 않는다.
- \<key\> - 대상 item의 key string
- \<lenfields>\과 \<numfields>\ - field list 문자열의 길이와 field 개수를 나타낸다. 0이면 전체 field, element를 의미한다.
- drop - field, element 삭제로 인해 empty map이 될 경우, 그 map을 drop할 것인지를 지정한다.
- noreply or pipe - 명시하면, response string을 전달받지 않는다. 
                    pipe 사용은 [Command Pipelining](/doc/command-pipelining.md)을 참조 바란다.

Response string과 그 의미는 아래와 같다.

- "DELETED" - 성공 (전체 또는 일부 field, element 삭제)
- “DELETED_DROPPED” - 성공 (전체 또는 일부 field, element 삭제하고 collection을 drop한 상태)
- “NOT_FOUND” - key miss
- “NOT_FOUND_ELEMENT” - field miss (삭제할 field, element가 없음. 모든 field가 없을 시에만 리턴)
- “TYPE_MISMATCH” - 해당 item이 map collection이 아님
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림

### mop get (Map Field, Element 조회)

Map collection에서 하나 이상의 field 이름을 주어, 그에 해당하는 element를 조회한다.

```
mop get <key> <lenfields> <numfields> [delete|drop]\r\n
[<"space separated fields">]\r\n
```

- "space separated fields" - 대상 map의 field list로, 스페이스(' ')로 구분한다.
                           - 하위 호환성(1.10.X 이하 버전)을 위해 콤마(,)도 지원하지만 권장하지 않는다.
- \<key\> - 대상 item의 key string
- \<lenfields\> 과 \<numfields>\ - field list 문자열의 길이와 field 개수를 나타낸다. 0이면 전체 field, element를 의미한다.
- delete or drop - field, element 조회하면서 그 field, element를 delete할 것인지
                   그리고 delete로 인해 empty map이 될 경우 그 map을 drop할 것인지를 지정한다.

성공 시의 response string은 아래와 같다.
VALUE 라인의 \<count\>는 조회된 field 개수를 의미한다. 
마지막 라인은 END, DELETED, DELETED_DROPPED 중의 하나를 가지며
각각 field 조회만 수행한 상태, field 조회하고 삭제한 상태,
field 조회 및 삭제하고 map을 drop한 상태를 의미한다.

```
VALUE <flags> <count>\r\n
<field> <bytes> <data>\r\n
<field> <bytes> <data>\r\n
<field> <bytes> <data>\r\n
...
END|DELETED|DELETED_DROPPED\r\n
```

실패 시의 response string과 그 의미는 아래와 같다.

- “NOT_FOUND” - key miss
- “NOT_FOUND_ELEMENT” - field miss (주어진 field 이름들 중 하나라도 가진 element가 전혀 없는 상태임)
- “TYPE_MISMATCH” - 해당 item이 map collection이 아님
- “UNREADABLE” - 해당 item이 unreadable item임
- "NOT_SUPPORTED" - 지원하지 않음
- “CLIENT_ERROR bad command line format” - protocol syntax 틀림
- "SERVER_ERROR out of memory [writing get response]”	- 메모리 부족

