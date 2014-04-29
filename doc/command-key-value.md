Simple Key-Value 명령
---------------------

Arcus cache server는 memcached 1.4의 key-value 명령을 그대로 지원하며, 
이에 추가하여 incr/decr 명령은 그 기능을 확장 지원한다.

Simple key-value 명령들의 요약은 아래와 같다.
이들 명령들의 자세한 정보는 [memcached 1.4의 기존 ascii protocol](/doc/protocol.txt)를 참고하기 바란다.

**storage 명령**

set, add, replace, append, prepend, cas 명령이 있으며 syntax는 다음과 같다.

```
<command name> <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n<data>\r\n
```

**retrieval 명령**

get, gets 명령이 있으며, syntax는 다음과 같다.

```
get <key>\r\n
gets <key>\r\n
```

**deletion 명령**

delete 명령이 있으며 syntax는 다음과 같다.

```
delete <key> [<time>] [noreply]\r\n
```

**Increment/Decrement 명령**

incr, decr 명령이 있으며, syntax는 아래와 같다.
Arcus cache server는 이 명령을 확장하여,
해당 key가 존재하지 않는 경우에 initial 값을 가지는 새로운 key-value item을 생성한다.

```
incr <key> <delta> [<flags> <exptime> <initial>] [noreply]\r\n
decr <key> <delta> [<flags> <exptime> <initial>] [noreply]\r\n
```
