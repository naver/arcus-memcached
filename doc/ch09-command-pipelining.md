# Chapter 9. Command Pipelining

Command pipelining은
“pipe” 키워드를 통해 여러 collection 명령들을 pipelining하여 cache server에 전달하고,
cache server는 각 명령을 처리한 즉시 그 response를 client로 전달하는 것이 아니라
그 response를 reply queue에 보관해 두었다가, 마지막 명령 처리 후에 reply queue에 보관해 둔 전체 response를
한번에 client로 전달하는 기능이다.
기존에 N 번의 request – response를 전달하던 것에 비해
command pipelining은 한 번의 request stream과 한 번의 response stream을 전달할 수 있게 함으로써
network 비용을 상당히 줄일 수 있으며, 전체 latency를 줄일 수 있는 장점을 가진다.

Command pipelining은 현재 collection 명령들 중 일부에 한해서만 가능하다.
Command pipelining 가능한 명령은 아래와 같으며, 단순 response string을 가지는 명령만 이에 해당된다.
한번에 pipelining이 가능한 최대 명령의 수는 `500`개로 제한을 두고 있음을 주의하여야 한다.

* lop 명령들 - lop insert/delete
* sop 명령들 - sop insert/delete/exist
* mop 명령들 - mop insert/delete/update
* bop 명령들 - bop insert/upsert/delete/update/incr/decr

Command pipelining 수행 예로,
특정 list의 tail 쪽으로 10개 elements를 추가하고자 한다면,
아래와 같이 lop insert 명령을 연속하여 cache server로 보내면 된다.
첫 번째 명령부터 마지막 바로 이전 명령까지는 모두 “pipe” 인자를 사용하여 연결하여야 하고,
마지막 명령에서는 “pipe” 인자를 생략함으로써 pipelining의 끝임을 표현하여야 한다.

```
 lop insert lkey -1 6 pipe\r\ndatum0\r\n
 lop insert lkey -1 6 pipe\r\ndatum1\r\n
 lop insert lkey -1 6 pipe\r\ndatum2\r\n
 lop insert lkey -1 6 pipe\r\ndatum3\r\n
 lop insert lkey -1 6 pipe\r\ndatum4\r\n
 lop insert lkey -1 6 pipe\r\ndatum5\r\n
 lop insert lkey -1 6 pipe\r\ndatum6\r\n
 lop insert lkey -1 6 pipe\r\ndatum7\r\n
 lop insert lkey -1 6 pipe\r\ndatum8\r\n
 lop insert lkey -1 6\r\ndatum9\r\n
```

Command pipelining의 response string은 아래와 같다.

```
RESPONSE <count>\r\n
<STATUS of the 1st pipelined command>\r\n
<STATUS of the 2nd pipelined command>\r\n
...
<STATUS of the last pipelined command>\r\n
END|PIPE_ERROR <error_string>\r\n
```

RESPONSE 라인에서 \<count\>는 전체 결과 수를 나타내고,
그 다음 라인들은 각 명령의 수행 결과를 차례로 나타낸다.
각 명령의 결과는 각 명령마다 다르므로 각 명령에 대한 설명을 참조하여야 한다.
마지막 라인은 pipelining 수행 상태를 나타내며, 아래 중의 하나를 가진다.

- "END" - pipelining 연산이 정상 수행됨
- “PIPE_ERROR command overflow” - pipelining 가능한 최대 commands 수인 500개를 초과하였다.
  이 경우, 500개까지의 command들만 하나의 command pipelining으로 처리되고 하나의 response stream으로 리턴된다.
  그 이후의 commands들은 처리되지 않는다.
- “PIPE_ERROR memory overflow” - ARCUS cache server 내부에서 pipelining 처리를 위한
  메모리 공간이 부족한 상태를 의미한다. ARCUS cache server는 500개 commands의 result를 담아둘 공간을
  미리 확보하여 수행하므로 이 오류가 발생할 가능성은 거의 없다.
  단, 의도하지 않은 이유에 의한 경우를 대비하여 이 오류를 추가해 둔 것이다.
  이 오류가 발생하면, 그 시점에 command pipelining을 중지하고 그 즉시 response stream을 client에 전달한다.
  이 경우의 response stream에는 가장 마지막에 수행된 command의 response string이 생략된다.
  그리고, 그 이후의 commands들은 처리되지 않는다.
- “PIPE_ERROR bad error” - pipelining 으로 어떤 command를 수행 중
  “CLIENT_ERROR”와 “SERVER_ERROR”로 시작하는 중요 오류가 발생한 경우이다.
  이 경우에도, 그 즉시 command pipelining을 중지하고 현재까지의 response stream을 client에 전달한다.
  그리고, 그 이후의 commands들은 처리되지 않는다.
