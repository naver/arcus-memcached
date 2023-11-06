## 필수 옵션

| 옵션 | 설명 | 기타|
| --- | --- | --- |
| `-E` | Engine to load, (for example, -E .libs/default_engine.so) |  |

## 기본 옵션

| 옵션 | 설명 | 기본값 | 기타 |
| --- | --- | --- | --- |
| `-p <num>` | TCP 포트 번호 | 11211 |  |
| `-U <num>` | UDP 포트 번호 | 11211 | 0 is off |
| `-s <file>` | UNIX socket path |  |  |
| `-a <mask>` | mask for UNIX socket | 0700 | in octal |
| `-l <ip_addr>` | interface to listen on | INADDR_ANY(모든 주소) |  |
| `-d` | 백그라운드 실행 (`-E`와 `-X`의 인자를 절대경로 사용하거나 `-r`옵션과 함께 사용해야 함) |  |  |
| `-r` | maximize core file limit |  |  |
| `-u <username>` | `<username>`이 실행한 것으로 가정 |  | root 실행 시 |
| `-c` | max simultaneous connections | 1024 |  |
| `-k` | lock down all paged memory. Note that there is a limit on how much memory you may lock. Trying to allocate more than that would fail, so be sure you set the limit correctly for the user you started the daemon with (not for `-u <username>` user; under sh this is done with 'ulimit -S -l NUM_KB'). |  |  |
| `-v`, `-vv`, `-vvv` | verbose, very verbose, extremely verbose |  |  |
| `-h` | 도움말 출력 후 종료 |  |  |
| `-i` | memcached, libevent licence 출력 |  |  |
| `-P <file>` | save PID in `<file>` |  | only used with `-d` option |
| `-t <num>` | number of threads to use | 4 |  |
| `-R` | Maximum number of requests per event, limits the number of requests process for a given connection to prevent starvation | 20 |  |
| `-b` | Set the backlog queue limit | 1024 |  |
| `-B` | Binding protocol - one of ascii, binary, or auto | auto |  |
| `-e` | engine_config 옵션, `<key>=<value>;<key>=<value>` 형태로 직접 지정하거나, `config_file=</path>`로 파일 지정 가능 |  |  |

## engine config 관련 옵션

아래 옵션들을 사용하지 않더라도 -e 옵션을 활용해 engine 관련 설정을 지정할 수 있다.
예시. `-m 40 -M` 대신 `-e cache_size=40;eviction=false;`를 사용해도 동일한 동작 수행
다른 옵션과 `-e` 옵션을 중복해서 사용하는 경우 `-e` 옵션이 더 우선 적용된다.
또한 `-e <key>=<value1>;<key>=<value2>` 형태로 입력하는 경우 `key`의 값은 `value2`로 최종 적용된다.

| 옵션 | 설명 | 기본값 | 기타 |
| --- | --- | --- | --- |
| `-m <num>` | 최대 메모리 | 64 | MB 단위 |
| `-M` | 메모리 부족한 경우 (item 삭제 대신)에러 |  |  |
| `-g` | sticky(gummed) memory limit | 0 | MB 단위 |
| `-f <factor>` | chunk size growth factor | 1.25 |  |
| `-n <bytes>` | minimum space allocated for key+value+flags | 48 |  |
| `-D <char>` | Use `<char>` as the delimiter between key prefixes and IDs. This is used for per-prefix stats reporting. The default is ":" (colon). If this option is specified, stats collection is turned on automatically; if not, then it may be turned on by sending the "stats detail on" command to the server. (+ `detail_enabled = 1`) |  |  |
| `-L` | Try to use large memory pages (if available). Increasing the memory page size could reduce the number of TLB misses and improve the performance. In order to get large pages from the OS, memcached will allocate the total item-cache in one large chunk. |  |  |
| `-C` | Disable use of CAS |  |  |
| `-I` | Override the size of each slab page. Adjusts max item size | 1mb | min: 1k, max: 128m |

## 기타
현재 engine 관련 설정은 `-e` 옵션을 활용한 config file 지정, 다른 구동 옵션을 활용한 설정, 환경변수에서 읽어오는 방법 등 다양한 방법을 사용하고 있다.
현재 방법들 중 환경변수는 사용자 관점에서 관리하기가 어렵고, 구동 옵션에서는 엔진 관련 설정과 다른 설정이 명확히 드러나지 않는다는 문제가 있다.
따라서 `-e config_file=/config_file1.conf` 형태로 파일을 이용하여 engine 설정을 입력하는 것이 좋다.
