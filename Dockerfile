FROM centos:7 AS builder
RUN yum update -y
RUN yum install -y make libtool which git
# Copy all files from the repository not included in .dockerignore to /src in the image.
COPY . /src
WORKDIR /src
RUN ./deps/install.sh /arcus
RUN ./config/autorun.sh
RUN ./configure --prefix=/arcus --enable-zk-integration
RUN make && make install

FROM centos:7 AS base
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
