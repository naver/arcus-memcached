#!/usr/bin/perl

use strict;
use Test::More tests => 996;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $msg;

for (my $keyi = 1; $keyi < 250; $keyi++) {
    my $key = "x" x $keyi;
    $cmd = "set $key 0 0 1"; $val = "9"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "get $key";
    $rst = "VALUE $key 0 1\n"
         . "9\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "incr $key 1"; $rst = "10"; $msg = "incr $key to 10";
    mem_cmd_is($sock, $cmd, "", $rst, $msg);
    $cmd = "get $key";
    $rst = "VALUE $key 0 2\n"
         . "10\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
}

# after test
release_memcached($engine, $server);
