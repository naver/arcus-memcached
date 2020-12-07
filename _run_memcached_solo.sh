#!/bin/sh

CURRENT_DIR_PATH=`pwd`

 ./memcached -E $CURRENT_DIR_PATH/.libs/default_engine.so -X $CURRENT_DIR_PATH/.libs/syslog_logger.so -X $CURRENT_DIR_PATH/.libs/ascii_scrub.so $@
