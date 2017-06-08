#!/bin/bash

./memcached -E .libs/default_engine.so -X .libs/syslog_logger.so -X .libs/ascii_scrub.so $@
