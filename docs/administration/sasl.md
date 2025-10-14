# SASL Authentication / Authorization

## Concept

### Authentication

- 현재 `SCRAM-SHA-256` 인증 방식만을 지원한다.
- ZooKeeper에 암호화된 형태의 비밀번호를 보관하며, ZooKeeper에 저장된 데이터를 확인하더라도 해당 계정으로 인증할 수 없다.
- Cyrus SASL 라이브러리 사용하며, ZooKeeper에 인증/권한 정보를 저장하기 위한 arcus auxprop plugin을 직접 구현한다.

### Permissions

각 계정에는 아래와 같은 권한을 부여할 수 있다.
- 특정 type의 item을 조회/수정할 수 있는 권한: `kv`, `list`, `set`, `map`, `btree`
- Item attribute 관련 명령의 수행 권한: `attr`
- Flush 계열 명령의 수행 권한: `flush`
- 설정/운영 관련 명령의 수행 권한: `admin`

### ACL Group

인증/권한 관련 규칙의 집합을 구분하는 단위이다.
- 하나의 ACL group에는 여러 사용자가 포함될 수 있으며, 각 사용자에게 권한을 부여할 수 있다.
- 서로 다른 ACL group에는 같은 이름을 갖는 사용자가 존재할 수 있으며, 독립적인 password와 권한을 갖게 된다.
- ARCUS Cache Server는 구동 시 하나의 ACL group을 선택하여 해당 ACL group의 인증/권한 정보에 따라 동작하게 된다.

### Manage

계정/권한 관리를 위한 도구로 [`arcusctl`](https://github.com/jam2in/arcusctl)을 제공하며, 아래와 같은 기능을 지원한다.
- ACL group 생성/제거
- ACL group 하위 사용자 생성/제거

arcusctl 사용하여 ACL group 정보를 ZooKeeper에 저장/조회할 수 있다.
아래는 2개의 ACL group(prod, dev)을 포함하는 znode 예시이다.

- `/arcus_acl/<group>` 하위 znode는 ZooKeeper ACL을 설정하여 권한을 가진 경우에만 수정할 수 있도록 한다.
- `/arcus_acl/<group>/<username>`에는 해당 user의 권한 정보를 보관한다.
- `/arcus_acl/<group>/<username>/authPassword`에는 해당 user의 비밀번호를 암호화된 형태로 보관한다.
- `*`으로 시작하는 사용자 계정(`*myadmin`, `*testadmin`)은, 서버에 연결해서 수행하는 모든 명령 이력을 감사 로그로 기록한다.

<img width="2422" height="1353" alt="image" src="https://github.com/user-attachments/assets/6ecbe90e-688a-4f48-827f-f253e19d2e2a" />


## Cache Server

### Build

ARCUS Cache Server 빌드 시 configure option으로 `--enable-sasl` 지정 시 SASL 인증 관련 기능을 사용할 수 있다.

```
./configure ... --enable-sasl [--with-sasl=<cyrus-sasl_install_path>]
```

### Running

ZooKeeper에 저장된 인증/권한 정보를 ARCUS Cache Server에서 사용하기 위하여 아래와 같이 환경변수를 설정해야 한다.

- `ARCUS_ACL_ZOOKEEPER`: ACL group 정보가 저장된 ZooKeeper 주소
- `ARCUS_ACL_GROUP`: ACL group 이름

Cache Server 구동 시 `-S` 옵션을 지정하여 클라이언트 연결에 대한 SASL 인증을 요구할 수 있으며, 실행 명령 예시는 아래와 같다.

```
ARCUS_ACL_ZOOKEEPER="127.0.0.1:2181" ARCUS_ACL_GROUP="prod"\
 $INSTALL_PATH/bin/memcached ... -S
```

`-S` 옵션 지정하지 않고 구동하더라도 동적으로 SASL 인증을 활성화하기 위한 `config auth on` 기능을 제공한다.
이 경우 `ARCUS_ACL_ZOOKEEPER`, `ARCUS_ACL_GROUP` 환경변수는 설정된 상태여야 정상 동작한다.
반대로, SASL 인증을 동적으로 비활성화하는 기능은 제공하지 않는다.

ARCUS Cache Server는 ZooKeeper에 저장된 인증/권한 정보를 가져와서 캐싱하여 사용하며, 24시간 주기로 갱신한다.
따라서 ACL 사용자 정보를 추가/수정/제거하는 경우 캐시에 반영되어 사용되기까지 최대 24시간이 소요될 수 있다.
필요 시 빠른 반영을 위해 `reload auth` 명령을 제공한다.

### Commands

`--enable-sasl` 설정 시 활성화되는 명령(ascii protocol 기준)은 다음과 같다.

- [`sasl mech`](../ascii-protocol/ch12-command-sasl.md#sasl-mech): 캐시 서버가 지원하는 인증 방식 목록 조회
- [`sasl auth`](../ascii-protocol/ch12-command-sasl.md#sasl-auth): 특정 인증 방식으로 인증 과정 수행
- [`config auth on`](../ascii-protocol/ch13-command-administration.md#config-auth): 동적으로 SASL 인증을 활성화
- [`reload auth`](../ascii-protocol/ch13-command-administration.md#reload): 외부에 저장된 인증/권한 정보를 로딩하여 캐싱

### Audit Log

감사 로그는 `SECURITY_EVENT` keyword로 시작하며, 아래 상황에서 기록한다.

- 인증 성공/실패
```
SECURITY_EVENT client=10.0.0.1 user=myapp authentication succeeded
SECURITY_EVENT client=10.0.0.1 user=myapp authentication failed(no mechanism available)
```

- 명령 수행 이력
  - 일반 user: 권한 없는 명령 수행 시도 이력
  - `*` 문자로 시작하는 user: 모든 명령 수행 이력

```
SECURITY_EVENT client=10.0.0.1 user=myapp cmd="mop create 0 100 10" unauthorized
SECURITY_EVENT client=10.0.0.1 user= cmd="set kv 0 60 5" unauthorized
SECURITY_EVENT client=10.0.0.1 user=*myadmin cmd="set kv 0 60 5" unauthorized
SECURITY_EVENT client=10.0.0.1 user=*myadmin cmd="config memlimit" authorized
```
