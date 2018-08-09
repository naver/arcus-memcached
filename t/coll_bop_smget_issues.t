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
    plan tests => 136;
} else {
    plan tests => 119;
}

my $cmd;
my $val;
my $rst;
my $num;
my $maxcount = 10;

# Initialize
mem_cmd_is($sock, "get key0", "", "END");
mem_cmd_is($sock, "get key1", "", "END");
mem_cmd_is($sock, "get key2", "", "END");

#
# 1st SMGet Test
#

# create keys
$num = 1;
$cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 2; $num <= 12; $num++) {
    $cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}
$num = 1;
$cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 2; $num <= 5; $num++) {
    $cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}
$num = 6;
$cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 7; $num <= 8; $num++) {
    $cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

# key list (maxcount = 10)
# - key0: 12, 11, 10,  9,  8,  7,  6,  5,  4,  3, trim
# - key1:  5,  4,  3,  2,  1
# - key2:  8,  7,  6

# NEW smget: ascending order (count = 10)
$cmd = "bop smget 14 3 0..5 10"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 1 5 value
key1 0 2 5 value
key1 0 3 5 value
key1 0 4 5 value
key1 0 5 5 value
MISSED_KEYS 1
key0 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget: ascending order (count = 10)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 14 3 0..5 10"; $val = "key0 key1 key2";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget: descending order (count = 10, 6, 5, 3, 1)
# duplicate
$cmd = "bop smget 14 3 5..0 10 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 8
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
key1 0 2 5 value
key1 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key0 3
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 6 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 6
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 5 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 3 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 1 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# NEW smget: descending order (count = 10, 6, 5, 3, 1)
# unique
$cmd = "bop smget 14 3 5..0 10 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
key1 0 2 5 value
key1 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key0 3
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 6 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
key1 0 2 5 value
key1 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key0 3
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 5 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
key1 0 2 5 value
key1 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key0 3
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 3 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 1 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget: descending order (count = 10, 6, 5, 3, 1)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 14 3 5..0 10"; $val = "key0 key1 key2";
$rst = "VALUE 6
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
MISSED_KEYS 0
DUPLICATED_TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 6"; $val = "key0 key1 key2";
$rst = "VALUE 6
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 5"; $val = "key0 key1 key2";
$rst = "VALUE 5
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 3"; $val = "key0 key1 key2";
$rst = "VALUE 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 1"; $val = "key0 key1 key2";
$rst = "VALUE 1
key1 0 5 5 value
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete keys
mem_cmd_is($sock, "delete key0", "", "DELETED");
mem_cmd_is($sock, "delete key1", "", "DELETED");
mem_cmd_is($sock, "delete key2", "", "DELETED");

#
# 2nd SMGet Test
#

# create keys
$num = 1;
$cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 2; $num <= 12; $num++) {
    $cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}
$num = 1;
$cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 2; $num <= 5; $num++) {
    $cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}
$num = 6;
$cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 7; $num <= 8; $num++) {
    $cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

# key list (maxcount = 10)
# - key0:  5,  4,  3,  2,  1
# - key1: 12, 11, 10,  9,  8,  7,  6,  5,  4,  3, trim
# - key2:  8,  7,  6

# NEW smget: ascending order (count = 10)
$cmd = "bop smget 14 3 0..5 10"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key0 0 1 5 value
key0 0 2 5 value
key0 0 3 5 value
key0 0 4 5 value
key0 0 5 5 value
MISSED_KEYS 1
key1 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget: ascending order (count = 10)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 14 3 0..5 10"; $val = "key0 key1 key2";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget: descending order (count = 10, 6, 5, 3, 1)
# duplicate
$cmd = "bop smget 14 3 5..0 10 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 8
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
key0 0 2 5 value
key0 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key1 3
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 6 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 6
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 5 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 3 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 1 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# NEW smget: descending order (count = 10, 6, 5, 3, 1)
# unique
$cmd = "bop smget 14 3 5..0 10 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
key0 0 2 5 value
key0 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key1 3
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 6 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
key0 0 2 5 value
key0 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key1 3
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 5 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 5
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
key0 0 2 5 value
key0 0 1 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key1 3
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 3 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 1 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget: descending order (count = 10, 6, 5, 3, 1)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 14 3 5..0 10"; $val = "key0 key1 key2";
$rst = "VALUE 6
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
MISSED_KEYS 0
DUPLICATED_TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 6"; $val = "key0 key1 key2";
$rst = "VALUE 6
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
key0 0 3 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 5"; $val = "key0 key1 key2";
$rst = "VALUE 5
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 3"; $val = "key0 key1 key2";
$rst = "VALUE 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 14 3 5..0 1"; $val = "key0 key1 key2";
$rst = "VALUE 1
key1 0 5 5 value
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete keys
mem_cmd_is($sock, "delete key0", "", "DELETED");
mem_cmd_is($sock, "delete key1", "", "DELETED");
mem_cmd_is($sock, "delete key2", "", "DELETED");

# create keys
$num = 0;
$cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 1; $num < 30; $num++) {
    $cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}
$num = 0;
$cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
for ($num = 1; $num < 10; $num++) {
    $cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

# key list (maxcount = 10)
# - key0: 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, trim
# - key1:  9,  8,  7,  6,  5,  4,  3,  2,  1

# NEW smget: descending order (20..10)
$cmd = "bop smget 9 2 20..10 100"; $val = "key0 key1";
$rst = "ELEMENTS 1
key0 0 20 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key0 20
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget: descending order (20..10)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 9 2 20..10 100"; $val = "key0 key1";
$rst = "VALUE 1
key0 0 20 5 value
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget: descending order (20..10, offset=5, value=100)
$cmd = "bop smget 9 2 20..10 5 100"; $val = "key0 key1";
$rst = "VALUE 0
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget: descending order (22..20, offset=5, value=100)
$cmd = "bop smget 9 2 22..20 5 100"; $val = "key0 key1";
$rst = "VALUE 0
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget: descending order (50..40, offset=0, value=100)
$cmd = "bop smget 9 2 50..40 0 100"; $val = "key0 key1";
$rst = "VALUE 0
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget: descending order (9..5)
$cmd = "bop smget 9 2 9..5 100"; $val = "key0 key1";
$rst = "ELEMENTS 5
key1 0 9 5 value
key1 0 8 5 value
key1 0 7 5 value
key1 0 6 5 value
key1 0 5 5 value
MISSED_KEYS 1
key0 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget: descending order (9..5)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 9 2 9..5 100"; $val = "key0 key1";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete keys
mem_cmd_is($sock, "delete key0", "", "DELETED");
mem_cmd_is($sock, "delete key1", "", "DELETED");

# after test
release_memcached($engine, $server);
