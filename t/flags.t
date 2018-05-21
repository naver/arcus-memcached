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

# set foo (and should get it)
for my $flags (0, 123, 2**16-1) {
    $cmd = "set foo $flags 0 6"; $val = "fooval"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "get foo";
    $rst = "VALUE foo $flags 6\n"
         . "fooval\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
}

# after test
release_memcached($engine, $server);
