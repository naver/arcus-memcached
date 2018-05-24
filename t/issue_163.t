#!/usr/bin/perl

use strict;
use Test::More tests => 7;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

### [ARCUS] CHANGED FOLLOWING TEST ###
## ENABLE_MIGRATION: hash_item structure has more fields in migration.
#my $server = get_memcached($engine);
my $engine = shift;
my $server = get_memcached($engine, "-n 32");
######################################
my $sock = $server->sock;
my $cmd;
my $val1 = "A"x77320;
my $val2 = "B"x77330;
my $rst;

$cmd = "set key 0 1 77320"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val1, $rst);

my $stats  = mem_stats($sock, "slabs");
my $requested = $stats->{"0:mem_requested"};
isnt ($requested, "0", "We should have requested some memory");

sleep(3);
$cmd = "set key 0 0 77330"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val2, $rst);

my $stats  = mem_stats($sock, "items");
my $reclaimed = $stats->{"items:0:reclaimed"};
is ($reclaimed, "1", "Objects should be reclaimed");

$cmd = "delete key"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set key 0 0 77320"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val1, $rst);

my $stats  = mem_stats($sock, "slabs");
my $requested2 = $stats->{"0:mem_requested"};
is ($requested2, $requested, "we've not allocated and freed the same amont");

# after test
release_memcached($engine, $server);
