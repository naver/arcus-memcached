#!/usr/bin/perl

use strict;
use Test::More tests =>13;
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

# Success Cases
# key value
$cmd = "gat 1 key";
$rst = "VALUE key 0 5\n"
     . "datum\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr key expiretime";
$rst = "ATTR expiretime=1\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
# gats command
$cmd = "gats 1 key";
$rst = "VALUE key 0 5 1\n"
     . "datum\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr key expiretime";
$rst = "ATTR expiretime=1\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Fail Cases
# bad value
$cmd = "set key 0 0 5"; $val = "datum"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "gat str key"; $rst = "CLIENT_ERROR invalid exptime argument";
mem_cmd_is($sock, $cmd, "", $rst);
# exist key and not exist key
$cmd = "gat 1 key key1";
$rst = "VALUE key 0 5\n"
     . "datum\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
# not exist key
$expire = time() - 1;
$cmd = "gat $expire key";
$rst = "VALUE key 0 5\n"
     . "datum\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "gat 1 key"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
# collection type
$cmd = "lop create lkey 0 0 5"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "gat 1 lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey expiretime";
$rst = "ATTR expiretime=0\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
