#!/usr/bin/perl
# Test the 'stats items' evictions counters.

use strict;
use Test::More tests => 92;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine, "-m 3");
my $sock = $server->sock;
my $cmd;
my $val = "B"x66560;
my $rst;
my $key = 0;

# These aren't set to expire.
for ($key = 0; $key < 40; $key++) {
    $cmd = "set key$key 0 0 66560"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

# These ones would expire in 600 seconds.
for ($key = 0; $key < 50; $key++) {
    $cmd = "set key$key 0 600 66560"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

my $stats  = mem_stats($sock, "items");
my $evicted = $stats->{"items:31:evicted"};
isnt($evicted, "0", "check evicted");
my $evicted_nonzero = $stats->{"items:31:evicted_nonzero"};
isnt($evicted_nonzero, "0", "check evicted_nonzero");

# after test
release_memcached($engine, $server);
