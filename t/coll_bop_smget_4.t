#!/usr/bin/perl

use strict;
use Test::More tests => 85;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;
my $num;
my $maxcount = 10;

# Initialize
$cmd = "get key0"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get key1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get key2"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

#
# 1st SMGet Test
#

# create keys
$num = 1;
$cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 2; $num <= 12; $num++) {
    $cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}
$num = 1;
$cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 2; $num <= 5; $num++) {
    $cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}
$num = 6;
$cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 7; $num <= 8; $num++) {
    $cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}

# key list (maxcount = 10)
# - key0: 12, 11, 10,  9,  8,  7,  6,  5,  4,  3, trim
# - key1:  5,  4,  3,  2,  1
# - key2:  8,  7,  6

# smget: ascending order (count = 10)
$cmd = "bop smget 14 3 0..5 10"; $val = "key0 key1 key2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 14 3 0..5 10 duplicate"; $val = "key0 key1 key2";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 0..5 10 unique"; $val = "key0 key1 key2";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (count = 10, 6, 5, 3, 1)
# old smget
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 5"; $val = "key0 key1 key2";
$rst = "VALUE 5
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 3"; $val = "key0 key1 key2";
$rst = "VALUE 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 1"; $val = "key0 key1 key2";
$rst = "VALUE 1
key1 0 5 5 value
MISSED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (count = 10, 6, 5, 3, 1)
# new smget: duplicate
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 3 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 1 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (count = 10, 6, 5, 3, 1)
# new smget: unique
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 3 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 1 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# delete keys
$cmd = "delete key0"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete key1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete key2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

#
# 2nd SMGet Test
#

# create keys
$num = 1;
$cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 2; $num <= 12; $num++) {
    $cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}
$num = 1;
$cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 2; $num <= 5; $num++) {
    $cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}
$num = 6;
$cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 7; $num <= 8; $num++) {
    $cmd = "bop insert key2 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}

# key list (maxcount = 10)
# - key0:  5,  4,  3,  2,  1
# - key1: 12, 11, 10,  9,  8,  7,  6,  5,  4,  3, trim
# - key2:  8,  7,  6

# smget: ascending order (count = 10)
$cmd = "bop smget 14 3 0..5 10"; $val = "key0 key1 key2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 14 3 0..5 10 duplicate"; $val = "key0 key1 key2";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 0..5 10 unique"; $val = "key0 key1 key2";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (count = 10, 6, 5, 3, 1)
# old smget
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 5"; $val = "key0 key1 key2";
$rst = "VALUE 5
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
key0 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 3"; $val = "key0 key1 key2";
$rst = "VALUE 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
DUPLICATED";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 1"; $val = "key0 key1 key2";
$rst = "VALUE 1
key1 0 5 5 value
MISSED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (count = 10, 6, 5, 3, 1)
# new smget: duplicate
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 3 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key0 0 5 5 value
key1 0 4 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 1 duplicate"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (count = 10, 6, 5, 3, 1)
# new smget: unique
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 3 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 3
key1 0 5 5 value
key1 0 4 5 value
key1 0 3 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 14 3 5..0 1 unique"; $val = "key0 key1 key2";
$rst = "ELEMENTS 1
key1 0 5 5 value
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# delete keys
$cmd = "delete key0"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete key1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete key2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# after test
release_memcached($engine, $server);
