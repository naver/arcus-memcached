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
