#!/usr/bin/perl

use strict;
use Test::More tests => 47;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
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

# smget: descending order (count = 10)
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

# delete keys
$cmd = "delete key0"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete key1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

