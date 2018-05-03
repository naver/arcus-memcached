#!/usr/bin/perl

use strict;
use Test::More tests => 100000;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $rst;

$cmd = "set foo 1 0 3\r\nabc";
$rst = "STORED";

for (1..100000) {
    mem_cmd_is($sock, $cmd, $rst);
}
