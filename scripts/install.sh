set -eo pipefail
BASEDIR=$(dirname $(readlink -f $0))/..

LOG_FILE="${BASEDIR}/scripts/install.log"
LIBEVENT_VERSION="2.1.12-stable"
ZOOKEEPER_VERSION="3.5.9-p3"

function run() {
    echo ">>> $@ (${PWD})" | tee -a ${LOG_FILE}
    $@ 2>&1 | tee -a ${LOG_FILE}
}

# Set target path
if [ -n "$1" ]; then
    run mkdir -p $1
    PREFIX=$(readlink -f $1)
    cat /dev/null > ${LOG_FILE}
    echo "TARGET PATH: ${PREFIX}" | tee -a ${LOG_FILE}
else
    echo "[WARNING] No installation target path was given."
    echo "[WARNING] arcus-memcached will be installed in system path and will need root privileges."
    echo "[WARNING] Otherwise, you can consider run below:"
    echo -e "\n        $0 /target/path\n"
    read -p "Do you want to continue installing to system path? [yN]: " yn
    case $yn in
        [Yy]* ) ;;
        * ) exit -1;;
    esac
    cat /dev/null > ${LOG_FILE}
    echo "TARGET PATH: (system)" | tee -a ${LOG_FILE}
fi

# install libevent
cd ${BASEDIR}/deps
run tar -zxf "libevent-${LIBEVENT_VERSION}.tar.gz"
cd "libevent-${LIBEVENT_VERSION}"
if [ -n "${PREFIX}" ]; then configure_option=" --prefix=${PREFIX}"
else configure_option=""
fi
run ./configure --disable-openssl ${configure_option}
run make
run make install

# install libzookeeper
cd ${BASEDIR}/deps
run tar -zxf "arcus-zookeeper-${ZOOKEEPER_VERSION}.tar.gz"
cd "arcus-zookeeper-${ZOOKEEPER_VERSION}"
if [ -n "${PREFIX}" ]; then configure_option=" --prefix=${PREFIX}"
else configure_option=""
fi
run ./configure ${configure_option}
run make
run make install

# install memcached
cd ${BASEDIR}
if [ -n "${PREFIX}" ]; then configure_option=" --prefix=${PREFIX} --with-libevent=${PREFIX} --with-zookeeper=${PREFIX}"
else configure_option=""
fi
run ./configure --enable-zk-integration ${configure_option}
run make
run make install

echo -e "\n\narcus-memcached installation complete. Now you can run the command below:\n"
echo "    ${PREFIX}/bin/memcached -E ${PREFIX}/lib/default_engine.so -v"
