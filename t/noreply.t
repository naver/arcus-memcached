#!/usr/bin/perl

use strict;
use Test::More tests => 20;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;

# Test that commands can take 'noreply' parameter.
$rst = "";
$cmd = "flush_all noreply";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "flush_all 0 noreply";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "add noreply:foo 0 0 1 noreply"; $val = "1";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "get noreply:foo";
$rst = "VALUE noreply:foo 0 1
1
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set noreply:foo 0 0 1 noreply"; $val = "2"; $rst = "";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get noreply:foo"; $val = "2";
$rst = "VALUE noreply:foo 0 1
2
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set noreply:foo 0 0 1 noreply"; $val = "3"; $rst = "";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get noreply:foo"; $val = "3";
$rst = "VALUE noreply:foo 0 1
3
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set noreply:foo 0 0 1 noreply"; $val = "4"; $rst = "";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get noreply:foo"; $val = "4";
$rst = "VALUE noreply:foo 0 1
4
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set noreply:foo 0 0 1 noreply"; $val = "5"; $rst = "";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get noreply:foo"; $val = "5";
$rst = "VALUE noreply:foo 0 1
5
END";
mem_cmd_is($sock, $cmd, "", $rst);

my @result = mem_gets($sock, "noreply:foo");

$cmd = "cas noreply:foo 0 0 1 $result[0] noreply"; $val = "6"; $rst = "";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get noreply:foo"; $val = "6";
$rst = "VALUE noreply:foo 0 1
6
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "incr noreply:foo 3 noreply"; $rst = "";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get noreply:foo"; $val = "9";
$rst = "VALUE noreply:foo 0 1
9
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "decr noreply:foo 2 noreply"; $rst = "";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get noreply:foo"; $val = "7";
$rst = "VALUE noreply:foo 0 1
7
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete noreply:foo noreply"; $rst = "";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get noreply:foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
