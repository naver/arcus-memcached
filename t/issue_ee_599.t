#!/usr/bin/perl

use strict;
use Test::More tests => 8;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $size;
my $stats;

$size = 10;
$cmd = "set key1 1 0 $size"; $val = "x"x$size; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$size = 3;
$cmd = "set key2 1 0 $size"; $val = "x"x$size; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$size = 20;
$cmd = "set key3 1 0 $size"; $val = "x"x$size; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$size = 3;
$cmd = "set key4 1 0 $size"; $val = "x"x$size; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# make free space
$cmd = "delete key1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

sleep(0.01);
$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:free_small_space"} != 0, 1,"$cmd confirm that free_small_space is not 0");

$cmd = "delete key3"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

sleep(0.01);
$stats = mem_stats($sock, "slabs");
# Previously, there was a phenomenon in
# which free_small_space was calculated to be larger than 0
is ($stats->{"SM:free_small_space"}, 0, "$cmd confirm free_small_space");

# after test
release_memcached($engine, $server);
