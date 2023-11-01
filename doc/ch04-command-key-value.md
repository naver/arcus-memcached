# Chapter 4. Simple Key-Value 명령

ARCUS Cache Server는 memcached 1.4의 key-value 명령을 그대로 지원하며,
이에 추가하여 incr/decr 명령은 그 기능을 확장 지원한다.

Simple key-value 명령들의 요약은 아래와 같다.
이들 명령들의 자세한 정보는 [memcached 1.4의 기존 ASCII protocol](https://github.com/naver/arcus-memcached/blob/master/doc/protocol.txt)를 참고하기 바란다.

## storage 명령

set, add, replace, append, prepend, cas 명령이 있으며 syntax는 다음과 같다.

```
<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n<data>\r\n
```

Response string과 그 의미는 아래와 같다.

| Response String      | 설명                     |
|----------------------|------------------------ |
| "STORED"             | 성공
| "NOT_STORED"         | 연산 조건에 부합하지 않아 저장에 실패 함. ex) 이미 존재하는 key에 대해 add 연산,  존재하지 않는 key에 대해 replace, append, prepend 연산.
| "NOT_FOUND"          | cas 연산의 key miss.
| "EXISTS"             | cas 연산의 응답으로, 해당 아이템이 클라이언트의 마지막 fetch 이후 수정된 적이 있음을 뜻함.
| "TYPE_MISMATCH"      | 해당 아이템이 key-value 타입이 아님.
| "CLIENT_ERROR"       | 클라이언트에서 잘못된 질의를 했음을 의미. 이어 나오는 문자열을 통해 오류의 원인을 파악 가능. 예) bad command line format
| "SERVER ERROR"       | 서버 측의 오류로 저장하지 못했음을 의미. 이어 나오는 문자열을 통해 오류의 원인을 파악 가능. 예) out of memory

## retrieval 명령

하나의 cache item을 조회하는 get, gets 명령이 있으며, syntax는 다음과 같다.
get 명령은 value만 조회하는 반면 gets 명령은 value와 함께 cas value도 조회한다.

```
get <key>\r\n
gets <key>\r\n
```

한번에 여러 cache item들을 조회하기 위한 mget, mgets 명령이 있으며, syntax는 다음과 같다.
mget, mgets 명령은 get, gets 처럼 mget 명령은 value만 조회하고 mgets 명령은 value와 함께 cas value도 조회한다.
mget 명령은 1.11 버전부터 mgets 명령은 1.13 버전부터 제공한다.

```
mget <lenkeys> <numkeys>\r\n
<"space separated keys">\r\n
mgets <lenkeys> <numkeys>\r\n
<"space separated keys">\r\n
```
- \<”space separated keys”\> - key list로, 스페이스(' ')로 구분한다.
- \<lenkeys\>과 \<numkeys> - key list 문자열의 길이와 key 개수를 나타낸다.

retrieval 명령이 정상 수행되었을 경우, Response string은 아래와 같이 구성된다.

- key hit된 아이템 정보를 모두 출력
- key miss된 아이템은 별도 response 없이 생략
- 응답의 끝에 "END\r\n" 출력

```
VALUE <key> <flags> <bytes> [<cas unique>]\r\n
<data block>\r\n\
...
END\r\n
```

실패시 string은 아래와 같다.


| Response String      | 설명                     |
|----------------------|------------------------ |
| "CLIENT_ERROR"       | 클라이언트에서 잘못된 질의를 했음을 의미. 이어 나오는 문자열을 통해 오류의 원인을 파악 가능. 예) bad command line format
| "SERVER ERROR"       | 서버 측의 오류로 조회하지 못했음을 의미. 이어 나오는 문자열을 통해 오류의 원인을 파악 가능. 예) out of memory writing get response

mget 명령에서 메모리 부족으로 일부 key에 대해서만 정상 조회한 후 실패한 경우, 전체 연산을 서버 에러 처리한다.

## deletion 명령

delete 명령이 있으며 syntax는 다음과 같다.

```
delete <key> [noreply]\r\n
```

Response string과 그 의미는 아래와 같다.

| Response String      | 설명                     |
|----------------------|------------------------ |
| "DELETED"            | 성공
| "NOT_FOUND"          | key miss
| "CLIENT_ERROR"       | 클라이언트에서 잘못된 질의를 했음을 의미. 이어 나오는 문자열을 통해 오류의 원인을 파악 가능. 예) bad command line format

## Increment/Decrement 명령

incr, decr 명령이 있으며, syntax는 아래와 같다.
ARCUS Cache Server는 이 명령을 확장하여,
해당 key가 존재하지 않는 경우에 initial 값을 가지는 새로운 key-value item을 생성한다.

```
incr <key> <delta> [<flags> <exptime> <initial>] [noreply]\r\n
decr <key> <delta> [<flags> <exptime> <initial>] [noreply]\r\n
```

성공시 Response string으로 incr, decr 연산을 적용한 값이 출력 된다.

실패시 Response string과 의미는 아래와 같다.

| Response String      | 설명                     |
|----------------------|------------------------ |
| "NOT_FOUND"          | key miss
| "TYPE_MISMATCH"      | 해당 아이템이 key-value 타입이 아님
| "CLIENT_ERROR"       | 클라이언트에서 잘못된 질의를 했음을 의미. 이어 나오는 문자열을 통해 오류의 원인을 파악 가능. 예) invalid numeric delta argument, cannot increment or decrement non-numeric value
| "SERVER ERROR"       | 서버 측의 오류로 연산하지 못했음을 의미. 이어 나오는 문자열을 통해 오류의 원인을 파악 가능. 예) out of memory
