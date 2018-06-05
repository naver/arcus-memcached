#!/usr/bin/perl

use strict;
use Test::More tests => 27;
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

sleep(0.01);
$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_min_classid"}, 12, "$cmd confirm used_min_classid");
is ($stats->{"SM:used_max_classid"}, 12, "$cmd confirm used_max_classid");
is ($stats->{"SM:used_01pct_classid"}, 12, "$cmd confirm used_01pct_classid");
is ($stats->{"SM:free_min_classid"}, 709, "$cmd confirm free_min_classid");
is ($stats->{"SM:free_max_classid"}, -1, "$cmd confirm free_max_classid");
is ($stats->{"SM:free_small_space"}, 0, "$cmd confirm free_small_space");

$size = 3;
$cmd = "set key2 1 0 $size"; $val = "x"x$size; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

sleep(0.01);
$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_min_classid"}, 11, "$cmd confirm used_min_classid");
is ($stats->{"SM:used_max_classid"}, 12, "$cmd confirm used_max_classid");

$size = 20;
$cmd = "set key3 1 0 $size"; $val = "x"x$size; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

sleep(0.01);
$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_min_classid"}, 11, "$cmd confirm used_min_classid");
is ($stats->{"SM:used_max_classid"}, 13, "$cmd confirm used_max_classid");

$size = 3;
$cmd = "set key4 1 0 $size"; $val = "x"x$size; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);


# make free space
$cmd = "delete key1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

sleep(0.01);
$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_min_classid"}, 11, "$cmd confirm used_min_classid");
is ($stats->{"SM:used_max_classid"}, 13, "$cmd confirm used_max_classid");
is ($stats->{"SM:used_01pct_classid"}, 13, "$cmd confirm used_01pct_classid");
is ($stats->{"SM:free_min_classid"}, 12, "$cmd confirm free_min_classid");
is ($stats->{"SM:free_max_classid"}, 12, "$cmd confirm free_max_classid");

$cmd = "delete key3"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

sleep(0.01);
$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_min_classid"}, 11, "$cmd confirm used_min_classid");
is ($stats->{"SM:used_max_classid"}, 11, "$cmd confirm used_max_classid");
is ($stats->{"SM:used_01pct_classid"}, 11, "$cmd confirm used_01pct_classid");
is ($stats->{"SM:free_min_classid"}, 12, "$cmd confirm free_min_classid");
is ($stats->{"SM:free_max_classid"}, 13, "$cmd confirm free_max_classid");
# Previously, there was a phenomenon in
# which free_small_space was calculated to be larger than 0
is ($stats->{"SM:free_small_space"}, 0, "$cmd confirm free_small_space");

# after test
release_memcached($engine, $server);
