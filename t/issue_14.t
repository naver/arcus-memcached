#!/usr/bin/perl

use strict;
use Test::More tests => 21;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val = "B"x66560;
my $rst;
my $key = 0;

for ($key = 0; $key < 10; $key++) {
    $cmd = "set key$key 0 2 66560"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

#print $sock "stats slabs"
my $first_stats  = mem_stats($sock, "slabs");
my $first_malloc = $first_stats->{total_malloced};

sleep(4);

for ($key = 10; $key < 20; $key++) {
    $cmd = "set key$key 0 2 66560"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

my $second_stats  = mem_stats($sock, "slabs");
my $second_malloc = $second_stats->{total_malloced};

is ($second_malloc, $first_malloc, "Memory grows..");

# after test
release_memcached($engine, $server);
