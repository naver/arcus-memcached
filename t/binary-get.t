#!/usr/bin/perl

use strict;
use Test::More tests => 8;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $count = 1;
my $cmd;
my $val;
my $rst;

foreach my $blob ("mooo\0", "mumble\0\0\0\0\r\rblarg", "\0", "\r") {
    my $key = "foo$count";
    my $len = length($blob);
    $cmd = "set $key 0 0 $len"; $val = $blob; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    mem_get_is($sock, $key, $blob);
    $count++;
}

# after test
release_memcached($engine, $server);
