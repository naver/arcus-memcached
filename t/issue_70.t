#!/usr/bin/perl

use strict;
use Test::More tests => 4;
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

$cmd = "set issue70 0 0 0\r\n"; $rst = "STORED"; $msg = "stored issue70";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set issue70 0 0 -1"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set issue70 0 0 4294967295"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set issue70 0 0 2147483647"; $val = "scoobyscoobydoo";
$rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, $val, $rst);

# after test
release_memcached($engine, $server);
