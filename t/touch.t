#!/usr/bin/perl

use strict;
use Test::More tests =>19;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;
my $expire;

# Initialize
$cmd = "set key 0 0 5"; $val = "datum"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop create lkey 0 0 1 error"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop create skey 0 0 1 error"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "mop create mkey 0 0 1 error"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create bkey 0 0 1 error"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
# key value
$cmd = "touch key 1"; $rst = "TOUCHED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr key expiretime";
$rst = "ATTR expiretime=1\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
# list
$cmd = "touch lkey 1"; $rst = "TOUCHED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey expiretime";
$rst = "ATTR expiretime=1\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
# set
$cmd = "touch skey 1"; $rst = "TOUCHED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr skey expiretime";
$rst = "ATTR expiretime=1\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
# map
$cmd = "touch mkey 1"; $rst = "TOUCHED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr mkey expiretime";
$rst = "ATTR expiretime=1\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
#btree
$cmd = "touch bkey 1"; $rst = "TOUCHED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey expiretime";
$rst = "ATTR expiretime=1\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Fail Cases
# bad value
$cmd = "set key 0 0 5"; $val = "datum"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "touch key str"; $rst = "CLIENT_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
# not exist key
$expire = time() - 1;
$cmd = "touch key $expire"; $rst = "TOUCHED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "touch key 1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
