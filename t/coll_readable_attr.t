#!/usr/bin/perl

use strict;
use Test::More tests => 28;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
bop create bkey1 11 0 0 unreadable
bop insert bkey1 90 6
datum9
bop insert bkey1 70 6
datum7
bop insert bkey1 50 6
datum5
bop insert bkey1 30 6
datum3
bop insert bkey1 10 6
datum1
bop get bkey1 10..90
getattr bkey1 readable
setattr bkey1 readable=on
getattr bkey1 readable
setattr bkey1 readable=off
bop get bkey1 10..90
delete bkey1

bop create bkey1 11 0 0 unreadable
bop insert bkey1 0x0fff 6
datum9
bop insert bkey1 0x0FFA 6
datum7
bop insert bkey1 0x00FF 6
datum5
bop insert bkey1 0x000F 6
datum3
bop insert bkey1 0x0000 6
datum1
bop get bkey1 0x00..0xFF
getattr bkey1 readable
setattr bkey1 readable=on
getattr bkey1 readable
setattr bkey1 readable=off
bop get bkey1 0x00..0xFF
delete bkey1
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Case 1
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create bkey1 11 0 0 unreadable"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 90 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 70 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 50 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 10 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 10..90"; $rst = "UNREADABLE";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey1 readable";
$rst = "ATTR readable=off\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey1 readable=on"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey1 readable";
$rst = "ATTR readable=on\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey1 readable=off"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 10..90";
$rst = "VALUE 11 5
10 6 datum1
30 6 datum3
50 6 datum5
70 6 datum7
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# Case 2
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create bkey1 11 0 0 unreadable"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0fff 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0FFA 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x00FF 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x000F 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0000 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0x00..0xFF"; $rst = "UNREADABLE";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey1 readable";
$rst = "ATTR readable=off\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey1 readable=on"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey1 readable";
$rst = "ATTR readable=on\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey1 readable=off"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 5
0x0000 6 datum1
0x000F 6 datum3
0x00FF 6 datum5
0x0FFA 6 datum7
0x0FFF 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
