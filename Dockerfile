FROM alpine:3.4

RUN addgroup -S memcached
RUN adduser -S -D -H -s /sbin/nologin -G memcached -g memcached memcached

COPY . /arcus-memcached

RUN apk add --no-cache libevent-dev \
    autoconf automake libtool cppunit build-base \
    perl git \
    && cd /arcus-memcached \
    && tar xzf zookeeper-3.4.5.tar.gz \
    && cd zookeeper-3.4.5/src/c \
    && ./configure && make && make install \
    && cd /arcus-memcached \
    && ./config/autorun.sh \
    && ./configure --enable-zk-integration \
    && make && make install \
    && apk del autoconf automake libtool cppunit build-base perl git libevent-dev \
    && cd / && rm -rf /arcus-memcached \
    && apk add --no-cache libevent

CMD ["memcached", "-u", "memcached", "-E", "/usr/local/lib/default_engine.so"]
