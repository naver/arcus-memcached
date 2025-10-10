# SASL Authentication / Authorization

## Build

ARCUS Cache Server 빌드 시 configure option으로 `--enable-sasl` 지정 시 SASL 인증 관련 기능을 사용할 수 있다.

```
./configure ... --enable-sasl [--with-sasl=<cyrus-sasl_install_path>]
```

## Environment variable

ZooKeeper에 저장된 인증/권한 정보를 ARCUS Cache Server에서 사용하기 위하여 아래와 같이 환경변수를 설정해야 한다.

```
ARCUS_ACL_ZOOKEEPER="127.0.0.1:2181" ARCUS_ACL_GROUP="test"\
 $INSTALL_PATH/bin/memcached ... -S
```
- `ARCUS_ACL_ZOOKEEPER`: 인증/권한 정보가 저장된 ZooKeeper 주소
- `ARCUS_ACL_GROUP`: 인증/권한 정보 집합의 식별자

## Commands

`--enable-sasl` 설정 시 활성화되는 명령(ascii protocol 기준)은 다음과 같다.

- [`sasl mech`](../ascii-protocol/ch12-command-sasl.md#sasl-mech), [`sasl auth`](../ascii-protocol/ch12-command-sasl.md#sasl-auth)
- [`config auth on`](../ascii-protocol/ch13-command-administration.md#config-auth)
- [`reload auth`](../ascii-protocol/ch13-command-administration.md#reload)
