Item Attribute 설명
------------------

Arcus cache server는 collection 기능 지원으로 인해,
기존 key-value item 유형 외에 list, set, map, b+tree item 유형을 가진다.
각 item 유형에 따라 설정/조회 가능한 속성들(attributes)이 구분되며, 이들의 개요는 아래 표와 같다.
아래 표에서 각 속성이 적용되는 item 유형, 속성의 간단한 설명, 허용가능한 값들과 디폴트 값을 나타낸다.

```
|-----------------------------------------------------------------------------------------------------------------|
| Attribute Name | Item Type   | Description           | Allowed Values                | Default Value            |
|-----------------------------------------------------------------------------------------------------------------|
| flags          | all         | data specific flags   | 4 bytes unsigned integer      | 0                        |
|-----------------------------------------------------------------------------------------------------------------|
| expiretime     | all         | item expiration time  | 4 bytes singed integer        | 0                        |
|                |             |                       |  -1: sticky                   |                          |
|                |             |                       |   0: never expired            |                          |
|                |             |                       |  >0: expired in the future    |                          |
|-----------------------------------------------------------------------------------------------------------------|
| type           | all         | item type             | "kv", "list", "set", "map",   | N/A                      |
|                |             |                       | "b+tree"                      |                          |
|-----------------------------------------------------------------------------------------------------------------|
| count          | collection  | current # of elements | 4 bytes unsigned integer      | N/A                      |
|-----------------------------------------------------------------------------------------------------------------|
| maxcount       | collection  | maximum # of elements | 4 bytes unsigned integer      | 4000                     |
|-----------------------------------------------------------------------------------------------------------------|
| overflowaction | collection  | overflow action       | “error” - all collections     | list - "tail_trim"       |
|                |             |                       | “head_trim” – list only       | set - "error"            |
|                |             |                       | “tail_trim” – list only       | map - "error"            |
|                |             |                       | “smallest_trim” – b+tree only | b+tree = "smallest_trim" |
|                |             |                       | “largest_trim” – b+tree only  |                          |
|-----------------------------------------------------------------------------------------------------------------|
| readable       | collection  | readable/unreable     | “on”, “off”                   | "on"                     |
|-----------------------------------------------------------------------------------------------------------------|
| maxbkeyrange   | b+tree only | maximum bkey range    | 8 bytes unsigned integer or   | 0                        |
|                |             |                       | hexadecimal (max 31 bytes)    |                          |
|-----------------------------------------------------------------------------------------------------------------|
```

Arcus cache server는 item 속성들을 조회하거나 변경하는 용도의 getattr 명령과 setattr 명령을 제공한다.
이들 명령에 대한 자세한 설명은 [Item Attribute 명령](/doc/command-item-attribute.md)을 참고 바란다.


Item 속성들 중 정확한 이해를 돕기 위해 추가 설명이 필요한 속성들에 대해 아래에서 자세히 설명한다.

### flags 속성

Flags는 item의 data-specific 정보를 저장하기 위한 목적으로 사용된다.
예를 들어, Arcus java client는 어떤 java object를 cache server에 저장할 경우,
그 java object의 type에 따라 serialization(or marshalling)하여 저장할 data를 만들고, 
그 java object의 type 정보를 flags 값으로 하여 Arcus cache server에 요청하여 저장한다.
Data 조회 시에는 Arcus cache server로 부터 data와 함께 flags 정보를 함께 얻어와서,
해당 java object의 type에 따라 그 data를 de-serialization(or de-marshalling)하여 java object를 생성한다.

### expiretime 속성

Item의 expiretime 속성으로 그 item의 expiration time을 초(second) 단위로 설정한다.

Arcus cache server는 expire 되지 않고 메모리 부족 상황에서도 evict 되지 않는 sticky item 기능을 제공한다.
Sticky item 또한 expiretime 속성으로 지정한다.

- -1 : sticky item으로 설정
- 0	: never expired item으로 설정, 그러나 메모리 부족 시에 evict될 수 있다.
- X <= (60 * 60 * 24 * 30) : 30일 이하의 값이면, 실제 expiration time은 "현재 시간 + X(초)"로 결정된다.
                       -2 이하이면, 그 즉시 expire 된다.
- X > (60 * 60 * 24 * 30) : 30일 초과의 값이면, 실제 expiration time은 "X"로 결정된다.
                      이 경우, X를 unix time으로 인식하여 expiration time으로 설정하는 것이며,
                      X가 현재 시간보다 작으면 그 즉시 expire 된다.

### maxcount 속성

Collection item에만 유효한 속성으로, 하나의 collection에 저장할 수 있는 최대 element 수를 규정한다.

Maxcount 속성의 hard limit과 default(설정 생략 또는 0을 값으로 주는 경우) 값은 아래와 같다.
- hard limit : 50000
- default value : 4000

Maxcount 속성의 hard limit을 작게 규정한 이유는 O(small N)의 수행 비용을 가지도록 하기 위한 것이다.
Event-driven processing 모델에 따라
하나의 worker thread가 비동기 방식으로 여러 client requests를 처리해야 하는 상황에서,
한 request의 처리 비용이 가급적 작아야만 다른 request의 execution latency에 주는 영향을 최소화할 수 있다.

### overflowaction 속성

Collection의 maxcount를 초과하여 element 추가하면 overflow가 발생하게 되며, 이 경우에 취할 action을 지정한다.

- "error"는 새로운 element 추가를 허용하지 않고 overflow 오류를 리턴한다. 
- "head_trim"과 "tail_trim"은 list collection에 설정 가능한 overflow action으로
  새로운 element 추가를 허용하는 대신 list의 head 또는 tail에 있는 기존 element를 제거한다.
- "smallest_trim"과 "largest_trim"은 b+tree collecton에 설정 가능한 overflow action으로
  새로운 element 추가를 허용하는 대신 smallest bkey 또는largest bkey를 가진 기존 element를 제거한다.
  
참고로, 아래에 기술하는 maxbkeyrange 속성에 따라 element를 trim해야 할 경우가 발생하며,
이 경우에도 적용된다.

### readable 속성

Arcus cache server는 다수 element를 가진 collection을 atomic하게 생성하는 명령을 제공하지 않는다.
대신, 하나의 element를 추가하는 명령을 반복 수행함으로써 원하는 collection을 만들 수 있다.
이 경우, 하나의 collection이 완성되기 전의 incomplete collection이 응용에게 노출될 수 있는 문제가 있다.
예를 들어, 어떤 사용자의 SNS 친구 정보를 set collection 형태로 cache에 저장한다고 가정한다.
일부 친구 정보만 set collection에 저장된 상태에서 그 사용자의 전체 친구 정보를 조회하는 요청이 들어온다면,
incomplete 친구 정보가 응용에게 노출되게 된다.
이러한 문제를 방지하기 위해 collection 생성에 대해 read atomicity를 제공하는 기능이 필요하며,
이 기능의 구현을 위해 readable 속성을 제공한다.
  
처음 empty collection 생성 시에 readable 속성을 off 상태로 설정해서
그 collection에 대한 조회 요청은 UNREADABLE 오류를 발생시키게 하고,
그 collection에 모든 element들을 추가한 후에 마지막으로 readable 속성을 다시 on 상태로 변경함으로써
complete collection이 응용에 의해 조회될 수 있게 할 수 있다.

### maxbkeyrange 속성
  
B+tree only 속성으로 smallest bkey와 largest bkey의 최대 범위를 규정한다.
B+tree에 설정된 maxbkeyrange를 위배시키는 새로운 bkey를 가진 element를 삽입하는 경우,
b+tree의 overflow action 정책에 따라 오류를 내거나 smallest/largest bkey를 가진 elements를
trim함으로써 항상 maxbkeyrange 특성을 준수하게 한다.

maxbkeyrange의 사용 예로,
어떤 응용이 data 생성 시간을 bkey로 하여 그 data를 b+tree에 저장하고
최근 2일치 data 만을 b+tree에 유지하길 원한다고 가정한다.
초 단위의 시간 값을 bkey 값으로 사용한다면,
maxbkeyrange는 2일치에 해당하는 값인 172880(2 * 24 * 60 * 60)으로 지정하고,
최근 data만을 보관하기 위해 overflowaction은 "smallest_trim"으로 지정하면 된다.
이러한 지정으로, 새로운 data가 추가될 때마다 b+tree에서 2일치가 지난 data는
maxbkeyrange와 overflowaction에 의해 자동으로 제거된다.
만약, 이런 기능이 없다면, 응용에서 오래된(2일이 지난) data를 직접 제거하는 작업을 수행해야 한다.
  
maxbkeyrange 설정은 bkey의 데이터 유형에 맞게 설정하여야 하며,
maxbkeyrange 설정이 생략되거나 명시적으로 0을 줄 경우의 default 값은
bkey 데이터 유형에 무관하게 unlimited maxbkeyrange를 의미한다.
