#!/usr/bin/perl

use strict;
use Test::More tests => 18;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
sop insert skey 6 create 13 60 1000
datum1
sop insert skey 6
datum2
sop insert skey 6
datum3
sop insert skey 6
datum4
sop insert skey 6
datum5
sop exist skey 6
datum0
sop exist skey 6
datum1
sop exist skey 6
datum2
sop exist skey 6
datum3
sop exist skey 6
datum4
sop exist skey 6
datum5
sop exist skey 6
datum6
sop exist skey 6
datum7
sop exist skey 6
datum8
sop exist skey 6
datum9
sop exist skey 6 pipe
datum0
sop exist skey 6 pipe
datum1
sop exist skey 6 pipe
datum2
sop exist skey 6 pipe
datum3
sop exist skey 6 pipe
datum4
sop exist skey 6 pipe
datum5
sop exist skey 6 pipe
datum6
sop exist skey 6 pipe
datum7
sop exist skey 6 pipe
datum8
sop exist skey 6
datum9
sop delete skey 6 pipe
datum1
sop delete skey 6 pipe
datum2
sop delete skey 6 pipe
datum3
sop delete skey 6 pipe
datum4
sop delete skey 6 pipe
datum5
sop delete skey 6
datum6
delete skey
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6 create 13 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum0"; $rst = "NOT_EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum1"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum2"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum3"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum4"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum5"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum6"; $rst = "NOT_EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum7"; $rst = "NOT_EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum8"; $rst = "NOT_EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6"; $val = "datum9"; $rst = "NOT_EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 6 pipe\r\ndatum0\r\n"
     . "sop exist skey 6 pipe\r\ndatum1\r\n"
     . "sop exist skey 6 pipe\r\ndatum2\r\n"
     . "sop exist skey 6 pipe\r\ndatum3\r\n"
     . "sop exist skey 6 pipe\r\ndatum4\r\n"
     . "sop exist skey 6 pipe\r\ndatum5\r\n"
     . "sop exist skey 6 pipe\r\ndatum6\r\n"
     . "sop exist skey 6 pipe\r\ndatum7\r\n"
     . "sop exist skey 6 pipe\r\ndatum8\r\n"
     . "sop exist skey 6\r\ndatum9";
$rst = "RESPONSE 10\n"
     . "NOT_EXIST\n"
     . "EXIST\n"
     . "EXIST\n"
     . "EXIST\n"
     . "EXIST\n"
     . "EXIST\n"
     . "NOT_EXIST\n"
     . "NOT_EXIST\n"
     . "NOT_EXIST\n"
     . "NOT_EXIST\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop delete skey 6 pipe\r\ndatum1\r\n"
     . "sop delete skey 6 pipe\r\ndatum2\r\n"
     . "sop delete skey 6 pipe\r\ndatum3\r\n"
     . "sop delete skey 6 pipe\r\ndatum4\r\n"
     . "sop delete skey 6 pipe\r\ndatum5\r\n"
     . "sop delete skey 6\r\ndatum6";
$rst = "RESPONSE 6\n"
     . "DELETED\n"
     . "DELETED\n"
     . "DELETED\n"
     . "DELETED\n"
     . "DELETED\n"
     . "NOT_FOUND_ELEMENT\n"
     . "END";
$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
