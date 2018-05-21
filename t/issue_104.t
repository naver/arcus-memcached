#!/usr/bin/perl

use strict;
use Test::More tests => 6;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;

# first get should miss
$cmd = "get foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Now set and get (should hit)
$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

my $stats = mem_stats($sock);
is($stats->{cmd_get}, 2, "Should have 2 get requests");
is($stats->{get_hits}, 1, "Should have 1 hit");
is($stats->{get_misses}, 1, "Should have 1 miss");

# after test
release_memcached($engine, $server);
