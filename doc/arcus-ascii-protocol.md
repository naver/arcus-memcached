Arcus memcached Ascii Protocol
==============================

Arcus cache server가 제공하는 명령들에 대한 ascii protocol을 기술한다.
Arcus cache server의 현재 버전은 1.7.0이며, 이 기준으로 작성한다.
향후 버전이 바뀌면, 그에 따라 수정할 것이다.
마지막으로, binary protocol은 현재 지원하지 않는 상태이므로, 논의에서 제외한다.

본 내용은 Arcus cache client 개발자를 주 대상으로 하며,
Arcus cache server에 관심있는 독자의 경우도 참고할 수 있다.

Collection Support
------------------

Ascii protocol 관점에서 Arcus cache server는 기존 memcached 기능 외에 collection 기능을 제공한다.
하나의 data만을 가지는 simple key-value 외에도 여러 data를 구조화된 형태로 저장/조회할 수 있으며,
제공하는 collection 유형은 아래와 같다.

- list : 데이터들의 linked list 구조
- set : 유일한 데이터들의 집합으로 membership 검사에 적합
- map : \<field, value\>쌍으로 구성된 데이터들의 집합으로 field 기준의 hash 구조
- b+tree : b+tree 키 기준으로 정렬된 데이터들의 집합으로 range scan 처리에 적합

Basic Concepts
--------------

Arcus cache server를 사용함에 있어 필요한 cache key, item, slab 등의 기본 용어와 개념은
[Arcus 기본 개념](/doc/arcus-basic-concept.md)에서 설명하므로, 이를 먼저 읽어보길 권한다.

Arcus cache server가 제공하는 collection과 element 구조, b+tree key, element flag 등의
중요 요소들은 [Collection 기본 개념](/doc/arcus-collection-concept.md)에서 소개한다.

Collection 기능의 제공에 따라 item 속성들이 많이 확장되었으며,
이들은 [Item 속성 설명](/doc/arcus-item-attribute.md)에서 자세히 다룬다.

Simple Key-Value 기능
---------------------

Arcus cache server는 memcached 1.4 기준의 key-value 명령을 그대로 제공하며, 일부에 대해 확장된 명령을 제공한다.
따라서, 기존 memcached 1.4에서 사용한 명령들은 Arcus cache server에서도 그대로 사용 가능하다.
[Key-Value 명령](/doc/command-key-value.md)에서 key-value 유형의 item에 대해 수행가능한 명령들을 소개한다.

Collection 기능
---------------

Collection 명령의 자세한 설명은 아래를 참고 바랍니다.

- [List collection 명령](/doc/command-list-collection.md)
- [Set collection 명령](/doc/command-set-collection.md)
- [Map collection 명령](/doc/command-map-collection.md)
- [B+tree collection 명령](/doc/command-btree-collection.md)

Collection 일부 명령들은 command pipelining 처리가 가능하며,
[Command Pipelining 기능](/doc/command-pipelining.md)에서 설명한다.

Item Attributes 기능
--------------------

Collection 지원으로 인해 item 유형이 다양해 졌으며, 다양한 item 유형에 따라 item attributes도 확장되었다.
이러한 item attributes를 조회하거나 변경하기 위하여
[Item Attribute 명령](/doc/command-item-attribute.md)을 제공한다.

Admin & Monitoring 기능
-----------------------

Arcus cache server의 운영 상에 필요한 기능들은
[Admin & Monitoring 명령](/doc/command-administration.md)으로 제공한다.

