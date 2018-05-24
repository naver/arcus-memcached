#!/usr/bin/perl

use strict;
use Test::More tests => 157;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

# assuming max slab is 1M and default mem is 64M
my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $msg;

# create a big value for the largest slab
my $max = 1024 * 1024;
my $big = 'x' x (1023 * 1024 - 250);

ok(length($big) > 512 * 1024, "buffer is bigger than 512k");
ok(length($big) < 1024 * 1024, "buffer is less than 1m");

# test that an even bigger value is rejected while we're here
my $too_big = $big . $big . $big;
my $len = length($too_big);
$cmd = "set too_big 0 0 $len"; $val = "$too_big";
$rst = "SERVER_ERROR object too large for cache"; $msg = "too_big not stored";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

# set the big value
my $len = length($big);
$cmd = "set big 0 0 $len"; $val = "$big"; $rst = "STORED"; $msg = "stored big";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);
$cmd = "get big";
$rst = "VALUE big 0 $len
$big
END";
mem_cmd_is($sock, $cmd, "", $rst);

# no evictions yet
my $stats = mem_stats($sock);
is($stats->{"evictions"}, "0", "no evictions to start");

# set many big items, enough to get evictions
for (my $i = 0; $i < 100; $i++) {
  $cmd = "set item_$i 0 0 $len"; $val = "$big"; $rst = "STORED"; $msg = "stored item_$i";
  mem_cmd_is($sock, $cmd, $val, $rst, $msg);
}

# some evictions should have happened
my $stats = mem_stats($sock);
my $evictions = int($stats->{"evictions"});
ok($evictions == 45, "some evictions happened");

# the first big value should be gone
$cmd = "get big"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# the earliest items should be gone too
for (my $i = 0; $i < $evictions - 1; $i++) {
  $cmd = "get item_$i"; $rst = "END";
  mem_cmd_is($sock, $cmd, "", $rst);
}

# check that the non-evicted are the right ones
for (my $i = $evictions - 1; $i < $evictions + 4; $i++) {
  $cmd = "get item_$i";
  $rst = "VALUE item_$i 0 $len\n"
       . "$big\n"
       . "END";
  mem_cmd_is($sock, $cmd, "", $rst);
}

# after test
release_memcached($engine, $server);
