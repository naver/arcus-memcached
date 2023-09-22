## Compile
- 의존성 모듈 설치
  - Install tools

    ```
    (CentOS) sudo yum install gcc gcc-c++ make autoconf automake libtool pkgconfig cppunit-devel perl-Test-Harness perl-Test-Simple
    (Ubuntu) sudo apt-get install build-essential autoconf automake libtool libcppunit-dev
    (OSX)    brew install autoconf automake libtool pkg-config cppunit
    ```
  - Install libevent

    ```
    $ wget https://github.com/downloads/libevent/libevent/libevent-2.0.21-stable.tar.gz
    $ tar xfz libevent-2.0.21-stable.tar.gz
    $ pushd libevent-2.0.21-stable
    $ ./autogen.sh
    $ ./configure --prefix=/path/to/arcus-directory
    $ make
    $ sudo make install
    $ popd
    ```
  - Install [arcus-zookeeper](https://github.com/naver/arcus-zookeeper)
- tag 정보가 없어 autorun.sh 가 실패하는 경우

    ```
    fatal: No names found, cannot describe anything.
    Can't find recent tag from current commit.
    If you forked the repository, the tag might not be included.
    You need to fetch tags from upstream repository. at config/version.pl line 22.
    Failed to run config/version.pl
    ```
    git clone이 아닌 직접 파일을 다운로드했거나 fork를 한 저장소로부터 처음 설치한 경우에 위의 에러가 발생한다.

    설치된 폴더를 git에 연결하고, repository로부터 tag 정보를 가져와야 한다.
    ```
    $ git init
    $ git remote add upstream https://github.com/naver/arcus-memcached.git
    $ git fetch upstream --tags
    ```

## Test
- 개별 테스트를 수행하는 방법

  ```
  perl t/${test_script_name}.t
  ```
- 여러 테스트가 동시 수행되어 used port로 실패하는 경우 재수행하거나 동시성을 1로 하여 수행.

  ```
  //run_test.pl 수정
  my $opt = '--job 1'; # --job N : run N test jobs in parallel
  ```
- perl Test::More.pm 모듈이 없어서 실패하는 경우

  ```
  Can't locate Test/More.pm in @INC (@INC contains: /usr/local/lib64/perl5 /usr/local/share/perl5 /usr/lib64/perl5/vendor_perl
  /usr/share/perl5/vendor_perl /usr/lib64/perl5 /usr/share/perl5 .) at -e line 1.
  ```
  위 에러를 보이게 되며, 테스트 스크립트에서 필요로 하는 perl 모듈인 Test::More.pm 이 시스템에 설치되어 있지 않아서 발생한다.
  CPAN 을 이용해 Test::More.pm 모듈을 설치한다.
  ```
  # Install CPAN
  (CentOS) sudo yum install cpan
  (Ubuntu) sudo apt-get install libpath-tiny-perl

  # Install Test::More
  cpan Test::More
  ```
