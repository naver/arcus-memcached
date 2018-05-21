#!/usr/bin/perl

use strict;
### [ARCUS] CHANGED FOLLOWING TEST ###
# Arcus-memcached allowed more memory to be allocated.
#use Test::More tests => 84;
use Test::More tests => 104;
######################################
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

### [ARCUS] CHANGED FOLLOWING TEST ###
# ENABLE_MIGRATION: hash_item structure has more fields in migration.
#my $server = get_memcached($engine, "-m 3");
my $engine = shift;
my $server = get_memcached($engine, "-m 3 -n 32");
######################################
my $sock = $server->sock;
my $cmd;
my $val = "B"x77320;
my $rst;
my $key = 0;

### [ARCUS] CHANGED FOLLOWING TEST ###
# Arcus-memcached allowed more memory to be allocated.
#for ($key = 0; $key < 40; $key++) {
for ($key = 0; $key < 60; $key++) {
######################################
    $cmd = "set key$key 0 0 77320"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

my $first_stats  = mem_stats($sock, "items");
my $first_evicted = $first_stats->{"items:31:evicted"};
# I get 1 eviction on a 32 bit binary, but 4 on a 64 binary..
# Just check that I have evictions...
isnt ($first_evicted, "0", "check evicted");

$cmd = "stats reset"; $rst = "RESET";
mem_cmd_is($sock, $cmd, "", $rst);

my $second_stats  = mem_stats($sock, "items");
my $second_evicted = $second_stats->{"items:31:evicted"};
is ($second_evicted, "0", "check evicted");

### [ARCUS] CHANGED FOLLOWING TEST ###
# Arcus-memcached allowed more memory to be allocated.
#for ($key = 40; $key < 80; $key++) {
for ($key = 60; $key < 100; $key++) {
######################################
    $cmd = "set key$key 0 0 77320"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

my $last_stats  = mem_stats($sock, "items");
my $last_evicted = $last_stats->{"items:31:evicted"};
is ($last_evicted, "40", "check evicted");

# after test
release_memcached($engine, $server);
