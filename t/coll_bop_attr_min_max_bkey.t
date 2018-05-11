#!/usr/bin/perl

use strict;
use Test::More tests => 24;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
ok 1 - get minbkey1:
ok 2 - bop create minbkey1 1 0 100:
ok 3 - getattr minbkey1 minbkey maxbkey:
ok 4 - bop insert minbkey1 90 6:datum9
ok 5 - bop insert minbkey1 70 6:datum7
ok 6 - bop insert minbkey1 50 6:datum5
ok 7 - bop insert minbkey1 30 6:datum3
ok 8 - bop insert minbkey1 10 6:datum1
ok 9 - getattr minbkey1 minbkey maxbkey:
ok 10 - bop delete minbkey1 10:
ok 11 - getattr minbkey1 minbkey maxbkey:
ok 12 - delete minbkey1:
ok 13 - get minbkey1:
ok 14 - bop create minbkey1 1 0 100:
ok 15 - getattr minbkey1 minbkey maxbkey:
ok 16 - bop insert minbkey1 0xffaa2211 6:datum9
ok 17 - bop insert minbkey1 0xee0122 6:datum7
ok 18 - bop insert minbkey1 0x110491 6:datum5
ok 19 - bop insert minbkey1 0x00019A 6:datum3
ok 20 - bop insert minbkey1 0x019ACC 6:datum1
ok 21 - getattr minbkey1 minbkey maxbkey:
ok 22 - bop delete minbkey1 0xFFAA2211:
ok 23 - getattr minbkey1 minbkey maxbkey:
ok 24 - delete minbkey1:
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Case 1
$cmd = "get minbkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create minbkey1 1 0 100"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr minbkey1 minbkey maxbkey";
$rst =
"ATTR minbkey=-1
ATTR maxbkey=-1
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert minbkey1 90 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 70 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 50 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 10 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr minbkey1 minbkey maxbkey";
$rst =
"ATTR minbkey=10
ATTR maxbkey=90
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop delete minbkey1 10"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr minbkey1 minbkey maxbkey";
$rst =
"ATTR minbkey=30
ATTR maxbkey=90
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete minbkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);



# Case 2
$cmd = "get minbkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create minbkey1 1 0 100"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr minbkey1 minbkey maxbkey";
$rst =
"ATTR minbkey=-1
ATTR maxbkey=-1
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert minbkey1 0xffaa2211 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 0xee0122 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 0x110491 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 0x00019A 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert minbkey1 0x019ACC 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr minbkey1 minbkey maxbkey";
$rst =
"ATTR minbkey=0x00019A
ATTR maxbkey=0xFFAA2211
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete minbkey1 0xFFAA2211"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr minbkey1 minbkey maxbkey";
$rst =
"ATTR minbkey=0x00019A
ATTR maxbkey=0xEE0122
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete minbkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
