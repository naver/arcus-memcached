#!/usr/bin/perl

use strict;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

if ("$engine" eq "default" || "$engine" eq "") {
    plan tests => 95;
} else {
    plan tests => 87;
}

my $cmd;
my $val;
my $rst;

mem_cmd_is($sock, "get kvkey", "", "END");
mem_cmd_is($sock, "get bkey1", "", "END");
mem_cmd_is($sock, "get bkey2", "", "END");

# create kvkey, bkey1, bkey2
mem_cmd_is($sock, "set kvkey 19 5 10", "some value", "STORED");
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 90 6", "datum9", "STORED");
mem_cmd_is($sock, "bop insert bkey1 70 6", "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey1 60 6", "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey1 50 6", "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey1 40 6", "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey1 30 6", "datum3", "STORED");
mem_cmd_is($sock, "bop insert bkey1 10 6", "datum1", "STORED");
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 100 7", "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 80 6", "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 70 6", "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey2 60 6", "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 50 6", "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey2 40 6", "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey2 20 6", "datum2", "STORED");
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 7
10 6 datum1
30 6 datum3
40 6 datum4
50 6 datum5
60 6 datum6
70 6 datum7
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..100";
$rst = "VALUE 12 7
20 6 datum2
40 6 datum4
50 6 datum5
60 6 datum6
70 6 datum7
80 6 datum8
100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0..100 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey1 11 10 6 datum1
bkey2 12 20 6 datum2
bkey1 11 30 6 datum3
bkey1 11 40 6 datum4
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 50 6 datum5
bkey1 11 60 6 datum6
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 10
bkey1 11 10 6 datum1
bkey2 12 20 6 datum2
bkey1 11 30 6 datum3
bkey1 11 40 6 datum4
bkey1 11 50 6 datum5
bkey1 11 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
bkey1 11 90 6 datum9
bkey2 12 100 7 datum10
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 100..0 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey2 12 70 6 datum7
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 60 6 datum6
bkey2 12 50 6 datum5
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 10
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey2 12 70 6 datum7
bkey2 12 60 6 datum6
bkey2 12 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
bkey1 11 10 6 datum1
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 70..0 4"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 4
bkey2 12 70 6 datum7
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 60 6 datum6
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 4
bkey2 12 70 6 datum7
bkey2 12 60 6 datum6
bkey2 12 50 6 datum5
bkey2 12 40 6 datum4
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 40..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst = "ELEMENTS 6
bkey1 11 40 6 datum4
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 50 6 datum5
bkey1 11 60 6 datum6
bkey2 12 60 6 datum6
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 6
bkey1 11 40 6 datum4
bkey1 11 50 6 datum5
bkey1 11 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
bkey1 11 90 6 datum9
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 30..0 10"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 3
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
bkey1 11 10 6 datum1
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 3
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
bkey1 11 10 6 datum1
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# NEW smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0..100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 6
bkey1 11 30 6 datum3
bkey1 11 40 6 datum4
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 50 6 datum5
bkey1 11 60 6 datum6
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 6
bkey1 11 30 6 datum3
bkey1 11 40 6 datum4
bkey1 11 50 6 datum5
bkey1 11 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 90..30 3 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 60 6 datum6
bkey2 12 50 6 datum5
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 4
bkey2 12 60 6 datum6
bkey2 12 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 200..300 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 0
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 0
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 0..100 4 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst = "ELEMENTS 6
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 50 6 datum5
bkey1 11 60 6 datum6
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 6
bkey1 11 50 6 datum5
bkey1 11 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
bkey1 11 90 6 datum9
bkey2 12 100 7 datum10
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
}

# NEW smget test: failures
$cmd = "bop smget 23 2 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# delete kvkey, bkey1, bkey2
mem_cmd_is($sock, "delete kvkey", "", "DELETED");
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# create bkey1, bkey2
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 100 7", "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 80 6", "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 60 6", "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 40 6", "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey2 20 6", "datum2", "STORED");
$cmd = "bop get bkey1 0..100"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..100";
$rst = "VALUE 12 5
20 6 datum2
40 6 datum4
60 6 datum6
80 6 datum8
100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0..100 5"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey2 12 20 6 datum2
bkey2 12 40 6 datum4
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 5
bkey2 12 20 6 datum2
bkey2 12 40 6 datum4
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 146 21 0..100000 10";
$val = "KEY_11 KEY_12 KEY_13 KEY_14 KEY_15 KEY_16 KEY_17 "
     . "KEY_18 KEY_19 KEY_20 KEY_21 KEY_22 KEY_23 KEY_24 "
     . "KEY_25 KEY_26 KEY_27 KEY_28 KEY_29 KEY_30 KEY_16";
$rst = "ELEMENTS 0
MISSED_KEYS 21
KEY_11 NOT_FOUND
KEY_12 NOT_FOUND
KEY_13 NOT_FOUND
KEY_14 NOT_FOUND
KEY_15 NOT_FOUND
KEY_16 NOT_FOUND
KEY_17 NOT_FOUND
KEY_18 NOT_FOUND
KEY_19 NOT_FOUND
KEY_20 NOT_FOUND
KEY_21 NOT_FOUND
KEY_22 NOT_FOUND
KEY_23 NOT_FOUND
KEY_24 NOT_FOUND
KEY_25 NOT_FOUND
KEY_26 NOT_FOUND
KEY_27 NOT_FOUND
KEY_28 NOT_FOUND
KEY_29 NOT_FOUND
KEY_30 NOT_FOUND
KEY_16 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# delete bkey1, bkey2
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# NEW smget unique test 1
# - bkey1: 10, 6, 2
# - bkey2: 10, 4
# - bkey3: 9, 5, 1
# - bkey4: 7, 3

# create bkey1, bkey2, bkey3, bkey4
mem_cmd_is($sock, "bop create bkey1 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 10 7", "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey1 6 6",  "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey1 2 6",  "datum2", "STORED");
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 10 7", "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 4 6",  "datum4", "STORED");
mem_cmd_is($sock, "bop create bkey3 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey3 9 6",  "datum9", "STORED");
mem_cmd_is($sock, "bop insert bkey3 5 6",  "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey3 1 6",  "datum1", "STORED");
mem_cmd_is($sock, "bop create bkey4 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey4 7 6",  "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey4 3 6",  "datum3", "STORED");

# NEW smget test
$cmd = "bop smget 23 4 10..0 3"; $val = "bkey1 bkey2 bkey3 bkey4";
$rst = "ELEMENTS 3
bkey2 12 10 7 datum10
bkey1 12 10 7 datum10
bkey3 12 9 6 datum9
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 3
bkey2 12 10 7 datum10
bkey3 12 9 6 datum9
bkey4 12 7 6 datum7
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# NEW smget unique test 2
# - bkey1: 10, 4
# - bkey2: 10, 6, 2
# - bkey3: 9, 5, 1
# - bkey4: 7, 3

# re-create bkey1, bkey2
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");
mem_cmd_is($sock, "bop create bkey1 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 10 7", "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey1 4 6",  "datum4", "STORED");
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 10 7", "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 6 6",  "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 2 6",  "datum2", "STORED");

$cmd = "bop smget 23 4 10..0 3"; $val = "bkey1 bkey2 bkey3 bkey4";
$rst = "ELEMENTS 3
bkey2 12 10 7 datum10
bkey1 12 10 7 datum10
bkey3 12 9 6 datum9
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 3
bkey2 12 10 7 datum10
bkey3 12 9 6 datum9
bkey4 12 7 6 datum7
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# delete bkey1, bkey2, bkey3, bkey4
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");
mem_cmd_is($sock, "delete bkey3", "", "DELETED");
mem_cmd_is($sock, "delete bkey4", "", "DELETED");

# after test
release_memcached($engine, $server);
