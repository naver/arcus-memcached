Arcus Telnet Interface
----------------------

Arcus cache server의 동작을 간단히 확인하는 방법으로, telnet interface를 이용할 수 있다.

### Telnet 사용법

OS prompt 상에서 아래와 같이 telnet 명령을 실행시킨다.
telnet 명령의 인자로는 연결하고자 하는 Arcus cache server인 memcached의 IP와 port number를 준다.

```
$ telnet {memcached-ip} {memcached-port}
```

### Telnet 연결

Localhost에 11211 포트 번호로 memcached가 구동되어 있다고 가정한다.
telnet 명령으로 해당 memcached에 연결하기 위해, 
OS prompt 상에서 아래의 명령을 수행한다.

```
$ telnet localhost 11211
Trying 127.0.0.1...
Connected to localhost.localdomain (127.0.0.1).
Escape character is '^]'.
```

telnet 명령으로 memcached에 연결한 이후에는 Arcus ASCII 명령을 직접 수행해 볼 수 있다.
아래에서 그 예들을 든다. Arcus ASCII 명령의 자세한 설명은 [Arcus cache server ascii protocol](/doc/arcus-ascii-protocol.md)을 참고하기 바란다.


### 예제 1.  get/set

하나의 key-value item으로 <"foo", "fooval">을 저장하기 위해, set 명령을 입력한다.

```
set foo 0 0 6
fooval
```

set 명령의 수행 결과로 정상적으로 key-value item이 저장되었다는 string이 리턴된다.

```
STORED
```

저장된 foo item을 조회하기 위해, get 명령을 입력한다.


```
get foo
```

get 명령으로 조회한 foo item 결과는 아래와 같다.


```
VALUE foo 0 6
fooval
END
```

### 예제 2.  b+tree

하나의 b+tree item을 생성하면서 5개의 elements를 추가하기 위해,
아래의 5개 bop insert 명령을 차례로 수행한다.

```
bop insert bkey1 90 6 create 11 0 0
datum9
bop insert bkey1 70 6
datum7
bop insert bkey1 50 6
datum5
bop insert bkey1 30 6
datum3
bop insert bkey1 10 6
datum1
```

5개 bop insert 명령의 수행 결과는 아래와 같다.


```
CREATED_STORED
STORED
STORED
STORED
STORED
```

b+tree에서 30부터 80까지의 bkey(b+tree key) range에 속하는 elements를 조회하기 위하여,
bop get 명령을 입력한다.


```
bop get bkey1 30..80
```

bop get 명령으로 조회한 결과는 아래와 같다.


```
VALUE 11 3
30 6 datum3
50 6 datum5
70 6 datum7
END
```

### Telnet 종료

현재의 telnet 연결을 종료하려면, quit 명령을 입력한다.

```
quit
```
