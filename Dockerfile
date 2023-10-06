ARG \
    libevent_version="2.1.12-stable" \
    zookeeper_version="3.5.9-p3" \
    configure_options="--enable-zk-integration"

FROM centos:7 AS builder
RUN yum update -y
RUN yum install -y gcc gcc-c++ make m4 perl-core autoconf automake libtool pkgconfig cppunit-devel ant which git net-tools cpan
ARG libevent_version zookeeper_version configure_options
## Libevent
WORKDIR /tmp
ADD https://github.com/libevent/libevent/releases/download/release-${libevent_version}/libevent-${libevent_version}.tar.gz .
RUN tar -zxf libevent-${libevent_version}.tar.gz
WORKDIR libevent-${libevent_version}
RUN ./configure --disable-openssl --prefix=/arcus
RUN make && make install
## ZooKeeper C client
WORKDIR /tmp
RUN git clone https://github.com/naver/arcus-zookeeper.git -b ${zookeeper_version}
WORKDIR arcus-zookeeper
RUN ant compile_jute
WORKDIR zookeeper-client/zookeeper-client-c
RUN autoreconf -if
RUN ./configure --prefix=/arcus
RUN make && make install
## Memcached
COPY . /tmp/arcus-memcached
WORKDIR /tmp/arcus-memcached
RUN ./config/autorun.sh
RUN ./configure --prefix=/arcus --with-libevent=/arcus --with-zookeeper=/arcus ${configure_options}
RUN make && make install

FROM centos:7 AS base
ARG libevent_version zookeeper_version configure_options
LABEL \
    libevent="$libevent_version" \
    zookeeper="$zookeeper_version" \
    configure="$configure_options"
#   document="https://github.com/naver/arcus-memcached"
COPY --from=builder /arcus /arcus
ENV PATH ${PATH}:/arcus/bin

ENTRYPOINT ["memcached",\
 "-E", "/arcus/lib/default_engine.so",\
 "-X", "/arcus/lib/ascii_scrub.so",\
 "-u", "root",\
 "-D", ":",\
 "-r"]
CMD [\
 "-v",\
 "-p", "11211",\
 "-m", "100",\
 "-c", "100"]

# for arcus-operator
FROM base
RUN yum update -y
RUN yum install -y bind-utils
RUN yum clean all -y
ENV MEMCACHED_DIR /arcus-memcached
ENV PATH ${PATH}:${MEMCACHED_DIR}
ENV ARCUS_USER root
WORKDIR ${MEMCACHED_DIR}
RUN ln -s /arcus/lib ${MEMCACHED_DIR}/.libs
