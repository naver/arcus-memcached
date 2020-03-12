#!/usr/bin/perl

use strict;
use Test::More tests => 19;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $rst;

my $max16 = 16*1024;
my $val16len = $max16 - 2;
my $val16;
for (1..$val16len) { $val16 .= chr( int(rand(25) + 65) ); }

my $max32 = 32*1024;
my $val32len = $max32 - 2;
my $val32;
for (1..$val32len) { $val32 .= chr( int(rand(25) + 65) ); }

$cmd = "config max_element_bytes $max16"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$rst = "CREATED_STORED";
$cmd = "lop insert lkey1 0 $val16len create 11 0 0";
mem_cmd_is($sock, $cmd, $val16, $rst);
$cmd = "sop insert skey1 $val16len create 11 0 0";
mem_cmd_is($sock, $cmd, $val16, $rst);
$cmd = "mop insert mkey1 0 $val16len create 11 0 0";
mem_cmd_is($sock, $cmd, $val16, $rst);
$cmd = "bop insert bkey1 0 $val16len create 11 0 0";
mem_cmd_is($sock, $cmd, $val16, $rst);
$rst = "CLIENT_ERROR too large value";
$cmd = "lop insert lkey2 0 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);
$cmd = "sop insert skey2 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);
$cmd = "mop insert mkey2 0 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);
$cmd = "bop insert bkey2 0 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);

$cmd = "config max_element_bytes $max32"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$rst = "CREATED_STORED";
$cmd = "lop insert lkey3 0 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);
$cmd = "sop insert skey3 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);
$cmd = "mop insert mkey3 0 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);
$cmd = "bop insert bkey3 0 $val32len create 11 0 0";
mem_cmd_is($sock, $cmd, $val32, $rst);

$val16 = "";
for (1..$val16len) { $val16 .= 'a' ; }

$val32 = "";
for (1..$val32len) { $val32 .= 'b'; }

$cmd = "bop create bkey4 11 0 0"; $rst="CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$rst = "STORED";
$cmd = "bop insert bkey4 0 $val16len";
mem_cmd_is($sock, $cmd, $val16, $rst);
$cmd = "bop insert bkey4 1 $val32len";
mem_cmd_is($sock, $cmd, $val32, $rst);
$cmd = "bop get bkey4 0";
$rst = "VALUE 11 1
0 $val16len $val16
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey4 1";
$rst = "VALUE 11 1
1 $val32len $val32
END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
