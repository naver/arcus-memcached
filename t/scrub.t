#!/usr/bin/perl

use strict;
use Test::More tests => 83;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine, "-X .libs/ascii_scrub.so");
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $key = 0;

for ($key = 0; $key < 40; $key++) {
    $cmd = "set key$key 0 0 5"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

for ($key = 40; $key < 80; $key++) {
    $cmd = "set key$key 0 1 5"; $val = "value"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

sleep(1.5);
$cmd = "scrub"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst, "scrub started");
sleep(1.0);
my $stats  = mem_stats($sock, "scrub");
my $visited = $stats->{"scrubber:visited"};
my $cleaned = $stats->{"scrubber:cleaned"};

is ($visited, "80", "visited");
is ($cleaned, "40", "cleaned");

# after test
release_memcached($engine, $server);
