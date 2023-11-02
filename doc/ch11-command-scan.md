# Chapter 11. Scan 명령

- [SCAN KEY 명령](#scan-key)
- [SCAN PREFIX 명령](#scan-prefix)

## Scan key

특정 조건을 만족하는 아이템들의 키 목록을 얻어올 수 있는 scan key 기능을 제공한다.
scan key 기능은 제한된 시간(5ms) 내에서 아이템들을 스캔하고 응용에게 찾은 키 스트링 목록과
다음 스캔할 위치를 응답해준다. 응용은 응답 받은 위치를 다시 주어 scan key 를 반복 호출하는
iteration 방식으로 전체 아이템을 스캔한다.
scan key 명령 요청 syntax는 아래와 같다.

```
scan key <cursor> [count <count>] [match <pattern>] [type <type>]\r\n
```

- \<cursor\> - 스캔 시작 위치.
처음부터 스캔한다면 0을, 이전 스캔이 끝난 다음 지점부터 이어서 한다면 이전 스캔에서 응답 받은 cursor 값을 그대로 지정한다.
- \<count\> - 스캔할 아이템 개수. 1~2000 범위에서 지정할 수 있고, 지정하지 않을 시 20 으로 설정된다.
지정한 수만큼의 아이템들을 스캔하여 그 중 조건(type, pattern)과 일치하는 아이템들의 키 목록을 응답한다.
응답한 키 목록의 개수는 scan key 내부 구현 로직 상 count 보다 적거나 많을 수 있다.
count 보다 적은 경우는 조건과 불일치한 아이템이 존재하거나 long query 방지를 위해 scan key 수행시간이 제한시간을 초과하여 수행을 종료한 경우이다.
count 보다 많은 경우는 스캔 동작이 해시테이블의 버켓 단위로 수행하기 때문에 하나의 버켓에 대해 스캔을 시작하면 마지막 지점까지 스캔을 완료하므로 count 보다 많은 수의 아이템을 스캔할 수 있다.
이렇게 설계한 이유는 버켓 중간에서 스캔을 중지할 경우 다음 스캔 요청이 들어올 때까지 그 버켓의 상태가 변경되지 않게 유지해야 하는
복잡성을 제거하여 stateless한 scan key api를 제공하기 위함이다.
- \<pattern\> - 키 문자열 패턴. 최대 문자열 길이: 64, 최대 '\*' 입력 개수: 4 . 지정하지 않을 시 모든 키 문자열을 조회한다.
glob style 패턴 문자열을 지정하여 해당 패턴과 일치하는 키 문자열을 갖는 아이템들을 찾는다. glob 문자는 '\*', '\?', '\\' 을 지원한다.
문자열 비교 알고리즘의 worst case 수행 시간이 오래 걸리는 것을 방지하기 위해 패턴 문자열에 길이와 '\*' 입력 개수에 제약을 두었다.
- \<type\> - 아이템 타입. 각 타입별 지정 값은 다음과 같다. 지정하지 않을 시 'A' 로 설정된다.
All type : 'A', KV : 'K', List : 'L', Set : 'S', Map : 'M', Btree : 'B'

scan key 명령 응답 syntax는 아래와 같다.

```
KEYS <key_count> <cursor>\r\n
key1 <type> <expiretime>\r\n
key2 <type> <expiretime>\r\n
key3 <type> <expiretime>\r\n
...
```

첫 번째 줄에는 KEYS 로 시작하는 응답 문자열이 나온다.
\<key_count\>는 조건과 일치한 아이템 개수로 두 번째 줄부터 키 문자열이 \<key_count\> 개 만큼 나온다.
\<cursor\>는 스캔할 다음 지점으로 이어서 스캔할 경우 이 값을 그대로 주어 scan key 호출하면 된다.
이 값이 0 이 나오면 전체 아이템을 스캔 완료했음을 나타낸다. 따라서 응용은 0이 나올때까지 반복 호출하면 된다.

두 번째 줄부터 scan된 key 목록이 나온다.
\<type\> - 아이템 타입. 각 타입별 지정 값은 다음과 같다.
KV : 'K', List : 'L', Set : 'S', Map : 'M', Btree : 'B'
\<expiretime\>은 아이템 만료 시간을 나타낸다.

scan key 사용 예시이다.
1000개의 아이템을 스캔하여 그 중 KV 타입이고, \*key1\* 패턴과 일치하는 키 목록을 조회한다.
```
scan key 0 count 1000 match *key1* type K
```

5개의 키가 조회되었고, 다음 스캔 지점 cursor 값은 8000이다.
0이 아니므로 전체 아이템을 스캔하지 못했음을 의미한다. 0이 나올때까지 응답받은 cursor 를 주어 scan key 를 반복 호출하면 된다.
```
KEYS 5 8000\r\n
akey12 K 0\r\n
bkey13 K 10\r\n
ccckey14 K 0\r\n
cckey16 K 0\r\n
keykey13 K 3600\r\n
```

scan key 명령 실패 시에 response string 은 다음과 같다.

| Response String                        | 설명                     |
|----------------------------------------|------------------------ |
| CLIENT_ERROR bad cursor value          | cursor 문자열 길이가 32 이상인 경우
| CLIENT_ERROR bad count value           | count 가 0 이거나 2000 보다 큰 경우
| CLIENT_ERROR bad pattern string        | 패턴 문자열 길이가 65 이상이거나 패턴 문자열에 * 을 5개 이상 주었거나 '\\' 다음에 패턴 문자('\\', '\*', '\?')가 없는 경우
| CLIENT_ERROR bad item type             | type 값을 잘못 준 경우
| CLIENT_ERROR invalid cursor            | cursor 에 숫자가 아닌 값을 준 경우
| CLIENT_ERROR bad command line format   | protocol syntax 틀림

## Scan prefix

특정 조건을 만족하는 Prefix 목록을 얻어올 수 있는 scan prefix 기능을 제공한다.
scan prefix 기능은 제한된 시간(5ms) 내에서 Prefix들을 스캔하고 응용에게 찾은 Prefix 목록과
다음 스캔할 위치를 응답해준다. 응용은 응답 받은 위치를 다시 주어 scan prefix 를 반복 호출하는
iteration 방식으로 전체 Prefix를 스캔한다.
scan prefix 명령 요청 syntax는 아래와 같다.

```
scan prefix <cursor> [count <count>] [match <pattern>]\r\n
```

- \<cursor\> - 스캔 시작 위치.
처음부터 스캔한다면 0을, 이전 스캔이 끝난 다음 지점부터 이어서 한다면 이전 스캔에서 응답 받은 cursor 값을 그대로 지정한다.
- \<count\> - 스캔할 Prefix 개수. 1~2000 범위에서 지정할 수 있고, 지정하지 않을 시 20 으로 설정된다.
지정한 수만큼의 Prefix들을 스캔하여 그 중 조건(pattern)과 일치하는 Prefix 목록을 응답한다.
응답한 Prefix 목록의 개수는 scan prefix 내부 구현 로직 상 count 보다 적거나 많을 수 있다.
count 보다 적은 경우는 조건과 불일치한 Prefix가 존재하거나 long query 방지를 위해 scan prefix 수행시간이 제한시간을 초과하여 수행을 종료한 경우이다.
count 보다 많은 경우는 스캔 동작이 해시테이블의 버켓 단위로 수행하기 때문에 하나의 버켓에 대해 스캔을 시작하면 마지막 지점까지 스캔을 완료하므로 count 보다 많은 수의 Prefix를 스캔할 수 있다.
이렇게 설계한 이유는 버켓 중간에서 스캔을 중지할 경우 다음 스캔 요청이 들어올 때까지 그 버켓의 상태가 변경되지 않게 유지해야 하는
복잡성을 제거하여 stateless한 scan prefix api를 제공하기 위함이다.
- \<pattern\> - 키 문자열 패턴. 최대 문자열 길이: 64, 최대 '\*' 입력 개수: 4 . 지정하지 않을 시 모든 키 문자열을 조회한다.
glob style 패턴 문자열을 지정하여 해당 패턴과 일치하는 키 문자열을 갖는 Prefix들을 찾는다. glob 문자는 '\*', '\?', '\\' 을 지원한다.
문자열 비교 알고리즘의 worst case 수행 시간이 오래 걸리는 것을 방지하기 위해 패턴 문자열에 길이와 '\*' 입력 개수에 제약을 두었다.

scan prefix 명령 응답 syntax는 아래와 같다.

```
PREFIXES <prefix_count> <cursor>\r\n
prefix1 <item_count> <item_bytes> <create_time>\r\n
prefix2 <item_count> <item_bytes> <create_time>\r\n
prefix3 <item_count> <item_bytes> <create_time>\r\n
...
```

첫 번째 줄에는 PREFIXES 로 시작하는 응답 문자열이 나온다.
\<prefix_count\>는 조건과 일치한 Prefix 개수로 두 번째 줄부터 Prefix 문자열이 \<prefix_count\> 개 만큼 나온다.
\<cursor\>는 스캔할 다음 지점으로 이어서 스캔할 경우 이 값을 그대로 주어 scan prefix 호출하면 된다.
이 값이 0 이 나오면 전체 Prefix를 스캔 완료했음을 나타낸다. 따라서 응용은 0이 나올때까지 반복 호출하면 된다.

두 번째 줄부터 scan된 prefix 목록이 나온다.
\<item_count\>는 해당 prefix의 아이템 개수를 나타낸다.
\<item_bytes\>는 해당 prefix의 아이템들이 차지하는 메모리 용량을 byte 단위로 나타낸다.
\<create_time\>은 해당 prefix가 생성된 시간을 나타낸다.

scan prefix 사용 예시이다.
1000개의 Prefix를 스캔하여 그 중 \*prefix1\* 패턴과 일치하는 Prefix 목록을 조회한다.
```
scan prefix 0 count 1000 match *prefix1*
```

5개의 키가 조회되었고, 다음 스캔 지점 cursor 값은 8이다.
0이 아니므로 전체 Prefix를 스캔하지 못했음을 의미한다. 0이 나올때까지 응답받은 cursor 를 주어 scan prefix 를 반복 호출하면 된다.
```
PREFIXES 5 8\r\n
aprefix12 1 96 20220621095219\r\n
bprefix13 12 2016 20220621095219\r\n
cccprefix14 25 2704 20220621095219\r\n
ccprefix16 2 256 20220621095219\r\n
prefixprefix13 3 288 20220621095219\r\n
```

scan prefix 명령 실패 시에 response string 은 다음과 같다.

| Response String                        | 설명                     |
|----------------------------------------|------------------------ |
| CLIENT_ERROR bad cursor value          | cursor 문자열 길이가 32 이상인 경우
| CLIENT_ERROR bad count value           | count 가 0 이거나 2000 보다 큰 경우
| CLIENT_ERROR bad pattern string        | 패턴 문자열 길이가 65 이상이거나 패턴 문자열에 * 을 5개 이상 주었거나 '\\' 다음에 패턴 문자('\\', '\*', '\?')가 없는 경우
| CLIENT_ERROR invalid cursor            | cursor 에 숫자가 아닌 값을 준 경우
| CLIENT_ERROR bad command line format   | protocol syntax 틀림
