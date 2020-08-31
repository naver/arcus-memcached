# Chapter 2. Collection Concept

### Collection 구조와 특징

Collection 유형과 그 구조 및 특징은 아래와 같다.

**List** - linked list

> Element들의 doubly linked list 구조를 가진다.
  Head element와 tail element 정보를 유지하면서, head/tail에서 시작하여 forward/backward 방향으로
  특정 위치에 있는 element를 접근할 수 있다.
  많은 element를 가진 list에서 중간 위치의 임의 element 접근 시에 성능 이슈가 있으므로,
  list를 queue 개념으로 사용하길 권한다.
  
**Set** - unordered set of unique value

> Set 자료 구조는 membership checking에 적합하다.
  Unordered set of unique value 저장을 위해 내부적으로 hash table 구조를 사용한다.
  하나의 set에 들어가는 elements 개수에 비례하여 hash table 전체 크기를 동적으로 조정하기 위해,
  일반적인 tree 구조와 유사하게 여러 depth로 구성되는 hash table 구조를 가진다.

**Map** - unordered set of \<field, value\>

> Map 자료 구조는 \<field, value\> 쌍을 저장한다.
  Field 값의 유일성 보장과 field 기준으로 해당 element 탐색을 빠르게 하기 위한 hash table 구조를 사용한다.
  하나의 map에 들어가는 elements 개수에 비례하여 hash table 전체 크기를 동적으로 조정하기 위해,
  일반적인 tree 구조와 유사하게 여러 depth로 구성되는 hash table 구조를 가진다.


**B+tree** - sorted map based on b+tree key

> 각 element 마다 unique key를 두고, 이를 기준으로 정렬된 elements 집합을 b+tree 구조로 저장하며,
  이러한 unique key 기반으로 forward/backward 방향의 range scan을 제공한다.
  Elements 수에 비례하여 동적으로 depth를 조정하는 b+tree 구조를 사용하여 메모리 사용을 최소화한다.
  그 외에, b+tree의 nonleaf node는 각 하위 node 중심의 sub-tree에 저장된 element 개수 정보를
  담고 있도록 해서, 특정 element의 position 조회 및 position 기반의 element 조회 기능도 제공한다.
  
Collection item은 \<key, "collection meta info"\> 구조를 가진다.
Collection meta info는 collection 유형에 따른 속성 정보를 가지며,
해당 collection의 elements에 신속히 접근하기 정보를 가진다.
예를 들어, list의 head/tail element 주소, set의 최상위 hash table 주소,
map의 최상위 hash table 구조, b+tree의 root node 주소가 이에 해당된다.

### Element 구조

Collection 유형에 따른 element 구조는 아래와 같다.

- list/set element : \<data\>

  각 element는 하나의 데이터 만을 가진다.

- map element : \<field(map element key), data\>

  map에서 각 element를 구분하기 위한 field를 필수적으로 가지며,
  field는 중복을 허용하지 않는다.
  
- b+tree element : \<bkey(b+tree key), eflag(element flag), data\>

  b+tree에서 elements를 어떤 기준으로 정렬하기 위한 bkey를 필수적으로 가지며,
  옵션 사항으로 bkey 기반의 scan 시에 특정 element를 filtering하기 위한 eflag를 가질 수 있으며,
  bkey에 종속되어 단순 저장/조회 용도로 사용되는 data를 가진다.


### BKey (B+Tree Key)

B+tree collection에서 사용가능한 bkey 데이터 유형은 아래 두 가지이다.

- 8 bytes unsigned integer

  0 ~ 18446744073709551615 범위의 값을 지정할 수 있다.
  이 유형이 성능 및 메모리 공간 관점에서 hexadecimal 유형보다 유리하므로, 이 유형의 bkey 사용을 권장한다.
  
- hexadecimal
 
  “0x”로 시작하는 짝수 개의 hexadecimal 문자열로 표현하며, 대소문자 모두 사용 가능하다.
  ARCUS cache server는 두 hexadecimal 문자를 1 byte로 저장하며,
  1 ~ 31 길이의 variable length byte array로 저장한다.
  
  hexadecimal 표현이 올바른 경우의 저장 바이트 수와 잘못된 경우의 이유는 아래와 같다.

  hexadecimal value | storage bytes | incorrect reason
  ----------------- | ------------- | ----------------
  0x34F40056        | 4 bytes       |
  0xabcd00778899    | 6 bytes       |
  34F40056          |               | 앞에 "0x"가 없음
  0x34F40           |               | 홀수 개의 hexadecimal 문자열
  0x34F40G          |               | 'G'가 hexadecimal 문자가 아님

bkey의 대소 비교는 8 bytes unsigned integer 유형의 값이면 두 integer 값의 단순한 비교 연산으로 수행하며, 
hexadecimal 유형의 값이면 아래와 같은 lexicographical order로 두 값을 비교한다.

- 두 hexadecimal의 첫째 바이트부터 차례로 바이트 단위로 대소를 비교하여, 차이나면 대소 비교를 종료한다.
- 두 hexadecimal 중 작은 길이만큼의 비교에서 두 값이 동일하면, 긴 길이의 hexadecimal 값이 크다고 판단한다.
- 두 hexadecimal의 길이도 같고 각 바이트의 값도 동일하면, 두 hexadecimal 값은 같다라고 판단한다.

### EFlag (Element Flag)

eflag는 현재 b+tree element에만 존재하는 필드이다.
eflag 데이터 유형은 hexadecimal 유형만 가능하며,
bkey의 hexadecimal 표현과 저장 방식을 그대로 따른다. 

### EFlag Filter

eflag에 대한 filter 조건은 아래와 같이 표현하며,
(1) eflag의 전체/부분 값과 특정 값과의 compare 연산이나
(2) eflag의 전체/부분 값에 대해 어떤 operand로 bitwise 연산을 취한 후의 결과와 특정 값과의 compare 연산이다.

```
eflag_filter: <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
```

- \<fwhere\> 
  - eflag 값에서 bitwise/compare 연산을 취할 시작 offset을 바이트 단위로 나타낸다.
    bitwise/compare 연산을 취할 데이터의 length는 \<fvalue\>의 length로 한다.
    예를 들어, eflag 전체 데이터를 선택한다면, \<fwhere\>는 0이어야 하고
    \<fvalue\>의 length는 eflag 전체 데이터의 length와 동일하여야 한다.
- [\<bitwop\> \<foperand\>]
  - 생략 가능하며, eflag에 대한 bitwise 연산을 지정한다.
  - bitwise 연산이 지정되면 이 연산의 결과가 compare 연산의 대상이 되며,
    생략된다면 eflag 값 자체가 compare 연산의 대상이 된다.
  - \<bitwop\>는 “&”(bitwise and), “|”(bitwise or), “^”(bitwise xor) 중의 하나로 bitwise 연산을 지정한다.
  - \<foperand\>는 bitwise 연산을 취할 operand로 hexadecimal로 표현한다.
    \<foperand\>의 길이는 compare 연산을 취한 \<fvalue\>의 길이와 동일하여야 한다.
- \<compop\> \<fvalue\>  
  - eflag에 대한 compare 연산을 지정한다.
  - \<compop\>는 "EQ", "NE', "LT", "LE", "GT", "GE" 중의 하나로 compare 연산을 지정하며,
    \<fvalue\>는 compare 연산을 취할 대상 값으로 마찬가지로 hexadecimal로 표현한다.
  - IN 또는 NOT IN 조건을 명시할 수도 있다. 
    IN 조건은 "EQ" 연산과 comma separated hexadecimal values로 명시하면 되고,
    NOT IN 조건은 "NE" 연산과 comma separated hexadecimal values로 명시하면 된다.
    이 경우, comma로 구분된 hexadecimal values의 최대 수는 100 개까지만 지원한다.
  
하나의 b+tree의 element에는 동일 길이의 element flag를 사용하길 권장한다.
하지만 응용이 필요하다면, 하나의 b+tree에 소속된 elements 이더라도
eflag가 생략될 수도 있고 서로 다른 길이의 eflag를 가질 수도 있다.
이 경우, 아래와 같이 eflag filtering이 애매모호해 질 수 있다.
이 상황에서는 filter 조건의 비교 연산이 “NE”이면 true로 판별하고, 그 외의 비교 연산이면 false로 판별한다.

- eflag가 없는 element에 eflag_filter 조건이 주어질 수 있다.
- eflag가 있지만 eflag_filter 조건에서 명시된 offset과 length의 데이터를 가지지 않을 수 있다.
  예를 들어, eflag가 4 bytes인 상황에서
  (1) eflag_filter 조건의 offset은 5인 경우이거나 
  (2) eflag_filter 조건의 offset은 3이고 length는 4인 경우가 있을 수 있다.

### EFlag Update

Eflag의 전체 또는 부분 값에 update 연산도 가능하며 아래와 같이 표현한다.
Eflag 전체 변경은 새로운 eflag 값으로 교체하는 것이며,
부분 변경은 eflag의 부분 데이터에 대해 bitwise 연산을 취한 결과로 교체한다.

```
eflag_update: [<fwhere> <bitwop>] <fvalue>
```

- [\<fwhere\> \<bitwop\>]
  - eflag를 부분 변경할 경우만 지정한다.
  - \<fwhere>은 eflag에서 부분 변경할 데이터의 시작 offset을 바이트 단위로 나타내며,
    이 경우, 부분 변경할 데이터의 length는 뒤에 명시되는 \<fvalue\>의 length로 결정된다.
  - \<bitwop\>는 부분 변경할 데이터에 대한 취할 bitwise 연산으로,
    “&”(bitwise and), “|”(bitwise or), “^”(bitwise xor) 중의 하나로 지정할 수 있다.
- \<fvalue\>
  - 변경할 new value를 나타낸다.
  - 앞서 기술한 \<fwhere\>과 \<bitwop\>가 생략되면, eflag의 전체 데이터를 \<fvalue\>로 변경한다.
    부분 변경을 위한 \<fwhere\>과 \<bitwop\>가 지정되면
    \<fvalue\>는 eflag 부분 데이터에 대해 bitwise 연산을 취할 operand로 사용되며,
    bitwise 연산의 결과가 eflag의 new value로 변경된다.
  

기존 eflag 값을 delete하여 eflag가 없는 상태로 변경할 수 있다.
이를 위해서는 \<fwhere\>과 \<bitwop\>를 생략하고 \<fvalue\> 값으로 0을 주면 된다.

