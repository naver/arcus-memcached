#!/usr/bin/perl

use strict;
use Test::More tests => 33;
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

# Bug 21
$cmd = "set bug21 0 0 19"; $val = "9223372036854775807"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "incr bug21 1"; $rst = "9223372036854775808"; $msg = "bug21 incr 1";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "incr bug21 1"; $rst = "9223372036854775809"; $msg = "bug21 incr 2";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "decr bug21 1"; $rst = "9223372036854775808"; $msg = "bug21 decr";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set num 0 0 1"; $val = "1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get num";
$rst = "VALUE num 0 1
1
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "incr num 1"; $rst = "2"; $msg = "+ 1 = 2";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get num";
$rst = "VALUE num 0 1
2
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "incr num 8"; $rst = "10"; $msg = "+ 8 = 10";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get num";
$rst = "VALUE num 0 2
10
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "decr num 1"; $rst = "9"; $msg = "- 1 = 9";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "decr num 9"; $rst = "0"; $msg = "- 9 = 0";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "decr num 5"; $rst = "0"; $msg = "- 5 = 0";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set num 0 0 10"; $val = "4294967296"; $rst = "STORED"; $msg = "stored 2**32";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

$cmd = "incr num 1"; $rst = "4294967297"; $msg = "4294967296 + 1 = 4294967297";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "set num 0 0 20"; $val = "18446744073709551615"; $rst = "STORED";
$msg = "stored 2**64-1";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

$cmd = "incr num 1"; $rst = "0"; $msg = "(2**64 - 1) + 1 = 0";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "decr bogus 5"; $rst = "NOT_FOUND"; $msg = "can't decr bogus key";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "decr incr 5"; $rst = "NOT_FOUND"; $msg = "can't incr bogus key";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "set bigincr 0 0 1"; $val = "0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "incr bigincr 18446744073709551610"; $rst = "18446744073709551610";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set text 0 0 2"; $val = "hi"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "incr text 1"; $rst = "CLIENT_ERROR cannot increment or decrement non-numeric value";
$msg = "hi - 1 = 0";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "delete noexists"; $rst = "NOT_FOUND"; $msg = "deleted noexists, but not found";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "incr noexists 1 0 0 20"; $rst = "20"; $msg = "init value : 20";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get noexists";
$rst = "VALUE noexists 0 2
20
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "incr noexists 1 0 0 20"; $rst = "21"; $msg = "20 + 1-= 21";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get noexists";
$rst = "VALUE noexists 0 2
21
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete noexists2"; $rst = "NOT_FOUND"; $msg = "deleted noexists2, but not found";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "decr noexists 1 0 0 20"; $rst = "20"; $msg = "init value : 20";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get noexists";
$rst = "VALUE noexists 0 2
20
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "decr noexists 1"; $rst = "19"; $msg = "20 - 1-= 19";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get noexists";
$rst = "VALUE noexists 0 2
19
END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
