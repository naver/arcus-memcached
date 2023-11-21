# Build & Install
## 패키지 설치
```
yum install gcc make which libtool (CentOS / Redhat)
apt-get install build-essential libtool (Ubuntu)
brew install autoconf automake libtool (OSX)
```
## Source 가져오기
```
wget https://github.com/naver/arcus-memcached/releases/download/<version>/arcus-memcached-<version>.tar.gz
tar -zxvf arcus-memcached-<version>.tar.gz
cd arcus-memcached-<version>
```
또는
```
git clone https://github.com/naver/arcus-memcached.git
cd arcus-memcached
git switch -d <version>
./config/autorun.sh
```
## Compile
[libevent](https://github.com/libevent/libevent), [arcus-zookeeper](https://github.com/naver/arcus-zookeeper) 설치
```
./deps/install.sh <arcus_install_path>
```
arcus-memcached 설치
```
./configure
make
make install
```
`./configure` 수행 시 다음과 같은 option을 사용할 수 있습니다.
- `--prefix=<arcus_install_path>`: arcus-memcached가 설치될 경로 지정. (default=`/usr/local`)
- `--with-libevent=<arcus_install_path>`: libevent가 설치된 경로 지정 (default=`<prefix>` 또는 `/usr/local`)
- `--enable-zk-integration`: zookeeper based clustering 사용
- `--enable-zk-reconfig`: zookeeper reconfig 기능 사용
- `--with-zookeeper=<arcus_install_path>`: arcus-zookeeper가 설치된 경로 지정 (default=`<prefix>` 또는 `/usr/local`)

# Test
```
yum install perl-core
```
unit test
```
make test
```
특정 테스트만 수행
```
perl ./t/<test_script>.t
```
동시성 관련 문제(used port 등)로 테스트 실패하는 경우
```perl
# run_test.pl
my $opt = '--job 1'; # --job N : run N test jobs in parallel
```
