# Chapter 1. ARCUS Basic Concept

ARCUS Cache Server는 하나의 데이터만을 value로 가지는 simple key-value 외에도
여러 데이터를 구조화된 형태로 저장하는 collection을 하나의 value로 가지는
확장된 key-value 데이터 모델을 제공한다.

## 기본 제약 사항

ARCUS Cache Server의 key-value 모델은 아래의 기본 제약 사항을 가진다.

- 기존 key-value 모델의 제약 사항
  - Key의 최대 크기는 16000 character이다. (arcus-memcached 1.11 이후 버전)
    - 기존 버전에서 key 최대 크기는 250 character이다.
  - Value의 최대 크기는 1MB(trailing 문자인 “\r\n” 포함한 길이) 이다.
- Collection 제약 사항
  - 하나의 collection에 들어갈 수 있는 최대 element 개수는 50,000개이다.
  - Collection의 각 element가 가지는 value의 최대 크기는 16KB(trailing 문자인 “\r\n” 포함한 길이)이며 이는 설정으로 변경 가능하다.

## Cache Key

Cache key는 ARCUS Cache Server에 저장할 데이터를 대표하는 코드이다. Cache key 형식은 아래와 같다.

```
  Cache Key : [<prefix>:]<subkey>
```

- \<prefix\> - Cache key의 앞에 붙는 namespace이다.
  - Prefix 단위로 Cache Server에 저장된 key들을 그룹화하여 flush하거나 통계 정보를 볼 수 있다.
  - Prefix를 생략할 수도 있지만, 가급적 사용하길 권한다.
- delimiter - Prefix와 subkey를 구분하는 문자로 default delimiter는 콜론(‘:’)이다.
- \<subkey\> - 일반적으로 응용에서 사용하는 Key이다.

Prefix와 subkey는 명명 규칙을 가지므로 주의하여야 한다.
Prefix는 영문 대소문자, 숫자, 언더바(_), 하이픈(-), 플러스(+), 점(.) 문자만으로 구성될 수 있으며,
이 중에 하이픈(-)은 prefix 명의 첫번째 문자로 올 수 없는 제약이 있다.
Subkey는 공백을 포함할 수 없으며, 기본적으로 alphanumeric만을 사용하길 권장한다.

## Cache Item

ARCUS Cache Server는 simple key-value 외에 collection 지원으로 다양한 item 유형을 가진다.

- simple key-value item - 기존 key-value item
- collection item
  - list item - 데이터들의 linked list을 value로 가지는 item
  - set item - 유일한 데이터들의 집합을 value로 가지는 item
  - map item - \<field, value\>쌍으로 구성된 데이터 집합을 value로 가지는 item
  - b+tree item - b+tree key 기반으로 정렬된 데이터 집합을 value로 가지는 item

## Expiration, Eviction, and Sticky

각 cache item은 expiration time 속성을 가지며,
이 값의 설정을 통해 expire되지 않는 item 또는 특정 시간 이후에 자동 expire될 item을 지정할 수 있다.
이에 대한 자세한 설명은 [Item Attribute 설명](ch03-item-attributes.md)을 참고 바란다.

ARCUS Cache Server는 memory cache이며, 한정된 메모리 공간을 사용하여 데이터를 caching한다.
메모리 공간이 모두 사용된 상태에서 새로운 item 저장 요청이 들어올 경우,
ARCUS Cache Server는 "out of memory" 오류를 내거나 LRU 기반의 eviction 방식
즉, 가장 오랫동안 접근되지 않은 item을 제거하고 새로운 item 저장을 허용하는 방식을 사용한다.
이러한 동작 방식은 ARCUS Cache Server의 -M 구동 옵션을 지정 가능하며,
default로는 LRU 기반의 eviction 방식을 사용한다.

특정 응용에서는 어떤 item이 expire & evict 대상이 되지 않기를 원하는 경우도 있다.
ARCUS Cache Server는 이러한 item을 sticky item이라 하며,
expiration time을 -1로 지정하면, sticky item으로 지원한다.
Sticky item의 삭제는 전적으로 응용에 의해 관리되어야 함을 주의해야 한다.

Sticky items은 일반적으로 많지 않을 것으로 예상하지만,
응용의 실수로 인해 sticky item들이 ARCUS 서버의 전체 메모리 공간을 차지하게 되는 경우를 방지하기 위하여,
전체 메모리 공간의 일부만이 sticky items에 의해 사용되도록 설정하는 -g(gummed or sticky) 구동 옵션을 제공한다.
Sticky items의 메모리 공간으로 사용될 메모리 비율이며, 0 ~ 100 범위의 값으로 지정가능하다.
디폴트인 0은 sticky items을 허용하지 않는다는 것이며,
100은 전체 메모리를 sticky items 저장 용도로 사용할 수 있음을 의미한다.

## Memory Allocator

ARCUS Cache Server는 item 메모리 공간의 할당과 반환을 효율적으로 관리할 목적으로
두 가지 memory allocator를 사용한다.

### Slab Allocator

Slab allocator는 메모리 크기 별로 메모리 공간을 나누어 관리하기 위해 slab class로 구분하고,
각 slab class에서 동일 크기의 메모리 공간들인 slab들을 free list 형태로 관리하면서
그 크기의 메모리 공간의 할당과 반환을 신속히 처리해 주는 memory allocator이다.
기존 memcached에서 사용되던 대표적인 memory allocator이다.

최대 slab 크기는 현재 1MB이다. 최소 slab 크기 즉, 첫 번째 slab class의 slab 크기와
그 다음 slab class들의 slab 크기는 아래의 ARCUS Cache Server 구동 옵션으로 설정한다.

- \-n \<bytes\> : minimum space allocated from key+value+flags (default: 48)
  - 최소 크기의 slab 크기를 결정한다.
- \-f \<factor\> : chunk size growth factor (default: 1.25)
  - Slab class 별로 slab 크기의 증가 정도를 지정하며, 1.0보다 큰 값으로 지정해야 한다.

### Small Memory Allocator

Collection 지원으로 인해 작은 메모리 공간의 할당과 반환 요청이 많아졌다.
이러한 작은 메모리 공간을 효율적으로 관리하기 위하여
small memory allocator를 새로 개발하여 사용하고 있다.
8000 바이트 이하의 메모리 공간은 small memory allocator가 담당하며,
8000 바이트 초과의 메모리 공간은 기존 slab allocator가 담당한다.

## Slab Class 별 LRU 리스트

ARCUS Cache Server는 slab class 별 LRU 리스트를 유지하고,
eviction 대상 item으로 오랫동안 접근되지 않은 item이 선택될 수 있게 한다.

Small memory allocator 추가로 인해, slab class 별 LRU 리스트에 변동 사항이 있다.
특별히, 0번 slab class를 두어 small memory allocator가 사용하고 있으며,
small memory allocator로 부터 메모리 공간을 할당받는
작은 크기의 key-value items과 collection items은 0번 LRU 리스트에 연결된다.
따라서, 8000 바이트 이하의 메모리 공간에 해당하는
기존 slab class의 LRU 리스트들은 empty가 상태가 된다.

## Automatic Scrub (added since v1.11.3)

ARCUS 캐시 클러스터에 새로운 캐시 노드를 추가하면, stale 데이터가 발생한다.
이유는 기존 캐시 노드들에 있던 일부 데이터는 consistent hashing에 의해 새로 추가된 캐시 노드로 담당 노드가 변경되기 때문이다.
이러한 stale 데이터는 기존 캐시 노드에 남아 있더라도 더 이상 접근되지 않으므로 대부분 문제가 없다.
하지만, 새로 추가한 캐시 노드가 어떤 이유로 다운되고 ARCUS 캐시 클러스터에서 제거된다면 stale 데이터가 다시 접근될 수 있는 문제가 발생한다.
결국, 이러한 stale 데이터를 제거하여야 한다.

Expire time이 짧은 stale 데이터는 자동으로 제거되지만, 그렇지 않은 stale 데이터가 항상 존재할 수 있다.
ARCUS 캐시 클러스터는 새로운 캐시 노드가 추가될 때마다 기존의 캐시 노드들은 모두 30초 후에 자신의 노드에서 stale 데이터를 찾아 제거하는 작업을 자동으로 수행한다.
이를 automatic scrub 작업이라 한다. 이를 위해, ARCUS 캐시 노드는 캐시 클라이언트와 마찬가지로 키 분배(캐시 데이터 분배)를 위한 consistent hashing 로직을 동일하게 가지며,
ZooKeeper Watcher를 통해 캐시 노드가 추가된 시점을 확인하면 background thread를 생성하여 자신 노드에 있는 모든 캐시 데이터를 순차 접근하면서 stale 데이터를 찾아 제거한다.

stale 데이터 노출 관점
- Automatic scrub 작업이 완료될 때까지 새로 추가한 캐시 노드가 다운되지 않는다면 stale 데이터는 노출되지 않는다.
- Automatic scrub 작업이 완료되기 전에 새로 추가한 캐시 노드가 다운되어 ARCUS 캐시 클러스터에서 제거된다면, stale 데이터가 노출될 수 있다.
- Automatic scrub 작업은 모든 캐시 데이터를 순차 접근하여 stale 여부를 검사하므로, 일정 시간이 소요된다.
하지만, Automatic scrub 작업의 완료 전에 새로 추가된 캐시 노드가 다운되는 경우는 극히 드물다.

참고로 명시적으로 수행할 수 있는 `scrub stale` 명령을 제공하고 있다. [Scrub 명령](ch12-command-administration.md#scrub-명령)
