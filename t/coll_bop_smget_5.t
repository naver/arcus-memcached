#!/usr/bin/perl

use strict;
use Test::More tests => 53;
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

# create keys
$num = 0;
$cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 1; $num < 30; $num++) {
    $cmd = "bop insert key0 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}
$num = 0;
$cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
for ($num = 1; $num < 10; $num++) {
    $cmd = "bop insert key1 $num 5 create 0 0 $maxcount"; $val = "value"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
}

# key list (maxcount = 10)
# - key0: 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, trim
# - key1:  9,  8,  7,  6,  5,  4,  3,  2,  1

# smget: descending order (20..10)
$cmd = "bop smget 9 2 20..10 100"; $val = "key0 key1";
$rst = "VALUE 1
key0 0 20 5 value
MISSED_KEYS 0
TRIMMED";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 9 2 20..10 100 duplicate"; $val = "key0 key1";
$rst = "ELEMENTS 1
key0 0 20 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key0 20
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 9 2 20..10 100 unique"; $val = "key0 key1";
$rst = "ELEMENTS 1
key0 0 20 5 value
MISSED_KEYS 0
TRIMMED_KEYS 1
key0 20
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (20..10, offset=5, value=100)
$cmd = "bop smget 9 2 20..10 5 100"; $val = "key0 key1";
$rst = "VALUE 0
MISSED_KEYS 0
TRIMMED";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (22..20, offset=5, value=100)
$cmd = "bop smget 9 2 22..20 5 100"; $val = "key0 key1";
$rst = "VALUE 0
MISSED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (50..40, offset=0, value=100)
$cmd = "bop smget 9 2 50..40 0 100"; $val = "key0 key1";
$rst = "VALUE 0
MISSED_KEYS 0
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# smget: descending order (9..5)
$cmd = "bop smget 9 2 9..5 100"; $val = "key0 key1"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 9 2 9..5 100 duplicate"; $val = "key0 key1";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 9 2 9..5 100 unique"; $val = "key0 key1";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

# delete keys
$cmd = "delete key0"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete key1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine, $server);
