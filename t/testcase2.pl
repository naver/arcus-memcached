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


$cmd = "get bkey1"; $rst = "END";

for (1..100000) {
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
}
