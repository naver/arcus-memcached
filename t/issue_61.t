#!/usr/bin/perl

use strict;
use Test::More tests => 2;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine, "-R 1");
my $sock = $server->sock;
my $cmd;
my $rst;

$cmd = "set foobar 0 0 5\r\nBubba\r\nset foobar 0 0 5\r\nBubba\r\nset foobar 0 0 5\r\nBubba\r\nset foobar 0 0 5\r\nBubba\r\nset foobar 0 0 5\r\nBubba\r\nset foobar 0 0 5\r\nBubba";
$rst = "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED";
mem_cmd_is($sock, $cmd, "", $rst);
my $stats = mem_stats($sock);
is ($stats->{"conn_yields"}, "5", "Got a decent number of yields");

# after test
release_memcached($engine, $server);
