#!/usr/bin/perl

use strict;
use Test::More tests => 42;
=head
use Test::More tests => 54;
=cut
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

=head
bop insert bkey1 90 6 create 11 0 0
datum9
bop insert bkey1 70 6
datum7
bop insert bkey1 50 6
datum5
bop insert bkey1 30 6
datum3
bop insert bkey1 10 6
datum1
bop insert bkey2 100 7 create 11 0 0
datum10
bop insert bkey2 80 6
datum8
bop insert bkey2 60 6
datum6
bop insert bkey2 40 6
datum4
bop insert bkey2 20 6
datum2
bop get bkey1 0..100
bop get bkey2 0..100
bop smget 11 2 0..100 5
bkey1,bkey2
bop smget 23 4 0..100 2 6
bkey2,bkey3,bkey1,bkey4
bop smget 23 4 90..30 2 9
bkey2,bkey3,bkey1,bkey4
bop smget 23 4 200..300 2 6
bkey2,bkey3,bkey1,bkey4
set keyx 0 0 6
datumx
bop smget 28 5 0..100 2 6
bkey2,bkey3,bkey1,bkey4,keyx
bop smget 29 5 0..100 2 6
bkey2,bkey3,bkey1,bkey4,bkey1
bop smget 29 5 0..100 2 6
bkey2,bkey3,bkey1,bkey4,bkey3
bop smget 23 2 0..100 2 6
bkey2,bkey3,bkey1,bkey4

delete bkey1
delete bkey2
=cut


# testBOPSMGetSimple
$cmd = "get bkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey2"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create bkey1 11 0 0"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey1 90 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 70 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 50 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 10 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_get_is($sock, "bkey1 0..100", 11, 5, "10,30,50,70,90", "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop create bkey2 12 0 0"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 80 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 60 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 40 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_get_is($sock, "bkey2 0..100", 12, 5, "20,40,60,80,100", "datum2,datum4,datum6,datum8,datum10", "END");
bop_new_smget_is($sock, "11 2 0..100 5", "bkey1,bkey2",
5,
"bkey1 11 10 6 datum1
,bkey2 12 20 6 datum2
,bkey1 11 30 6 datum3
,bkey2 12 40 6 datum4
,bkey1 11 50 6 datum5",
0, "",
0, "",
"END");
bop_new_smget_is($sock, "11 2 100..0 10", "bkey1,bkey2",
10,
"bkey2 12 100 7 datum10
,bkey1 11 90 6 datum9
,bkey2 12 80 6 datum8
,bkey1 11 70 6 datum7
,bkey2 12 60 6 datum6
,bkey1 11 50 6 datum5
,bkey2 12 40 6 datum4
,bkey1 11 30 6 datum3
,bkey2 12 20 6 datum2
,bkey1 11 10 6 datum1",
0, "",
0, "",
"END");
bop_new_smget_is($sock, "23 4 0..100 2 6", "bkey2,bkey3,bkey1,bkey4",
6,
"bkey1 11 30 6 datum3
,bkey2 12 40 6 datum4
,bkey1 11 50 6 datum5
,bkey2 12 60 6 datum6
,bkey1 11 70 6 datum7
,bkey2 12 80 6 datum8",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "23 4 90..30 2 9", "bkey2,bkey3,bkey1,bkey4",
5,
"bkey1 11 70 6 datum7
,bkey2 12 60 6 datum6
,bkey1 11 50 6 datum5
,bkey2 12 40 6 datum4
,bkey1 11 30 6 datum3",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "23 4 200..300 2 6", "bkey2,bkey3,bkey1,bkey4",
0, "",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
=head
bop_smget_is($sock, "11 2 0..100 5", "bkey1,bkey2",
             5, "bkey1,bkey2,bkey1,bkey2,bkey1", "11,12,11,12,11",
             "10,20,30,40,50", "datum1,datum2,datum3,datum4,datum5", 0, "", "END");
bop_smget_is($sock, "11 2 100..0 10", "bkey1,bkey2",
             10, "bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1",
             "12,11,12,11,12,11,12,11,12,11", "100,90,80,70,60,50,40,30,20,10",
             "datum10,datum9,datum8,datum7,datum6,datum5,datum4,datum3,datum2,datum1", 0, "", "END");
bop_smget_is($sock, "23 4 0..100 2 6", "bkey2,bkey3,bkey1,bkey4",
             6, "bkey1,bkey2,bkey1,bkey2,bkey1,bkey2", "11,12,11,12,11,12",
             "30,40,50,60,70,80", "datum3,datum4,datum5,datum6,datum7,datum8", 2, "bkey3,bkey4", "END");
bop_smget_is($sock, "23 4 90..30 2 9", "bkey2,bkey3,bkey1,bkey4",
             5, "bkey1,bkey2,bkey1,bkey2,bkey1", "11,12,11,12,11",
             "70,60,50,40,30", "datum7,datum6,datum5,datum4,datum3", 2, "bkey3,bkey4", "END");
bop_smget_is($sock, "23 4 200..300 2 6", "bkey2,bkey3,bkey1,bkey4",
             0, "", "", "", "", 2, "bkey3,bkey4", "END");
=cut
$cmd = "set keyx 19 5 10"; $val = "some value"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 23 2 0..100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 28 5 0..100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,keyx"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 29 5 0..100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey1"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_new_smget_is($sock, "29 5 0..100 2 6", "bkey2,bkey3,bkey1,bkey4,bkey3",
6,
"bkey1 11 30 6 datum3
,bkey2 12 40 6 datum4
,bkey1 11 50 6 datum5
,bkey2 12 60 6 datum6
,bkey1 11 70 6 datum7
,bkey2 12 80 6 datum8",
3,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND
,bkey3 NOT_FOUND",
0, "",
"END");
=head
bop_smget_is($sock, "29 5 0..100 2 6", "bkey2,bkey3,bkey1,bkey4,bkey3",
             6, "bkey1,bkey2,bkey1,bkey2,bkey1,bkey2", "11,12,11,12,11,12",
             "30,40,50,60,70,80", "datum3,datum4,datum5,datum6,datum7,datum8", 3, "bkey3,bkey4,bkey3", "END");
=cut

$cmd = "delete keyx"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

$cmd = "bop create bkey1 11 0 0"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create bkey2 12 0 0"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 80 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 60 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 40 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");

$cmd = "bop get bkey1 0..100"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey2 0..100", 12, 5, "20,40,60,80,100", "datum2,datum4,datum6,datum8,datum10", "END");
bop_new_smget_is($sock, "11 2 0..100 5", "bkey1,bkey2",
5,
"bkey2 12 20 6 datum2
,bkey2 12 40 6 datum4
,bkey2 12 60 6 datum6
,bkey2 12 80 6 datum8
,bkey2 12 100 7 datum10",
0, "",
0, "",
"END");
bop_new_smget_is($sock, "146 21 0..100000 10", "KEY_11,KEY_12,KEY_13,KEY_14,KEY_15,KEY_16,KEY_17,KEY_18,KEY_19,KEY_20,KEY_21,KEY_22,KEY_23,KEY_24,KEY_25,KEY_26,KEY_27,KEY_28,KEY_29,KEY_30,KEY_16",
0, "",
21,
"KEY_11 NOT_FOUND
,KEY_12 NOT_FOUND
,KEY_13 NOT_FOUND
,KEY_14 NOT_FOUND
,KEY_15 NOT_FOUND
,KEY_16 NOT_FOUND
,KEY_17 NOT_FOUND
,KEY_18 NOT_FOUND
,KEY_19 NOT_FOUND
,KEY_20 NOT_FOUND
,KEY_21 NOT_FOUND
,KEY_22 NOT_FOUND
,KEY_23 NOT_FOUND
,KEY_24 NOT_FOUND
,KEY_25 NOT_FOUND
,KEY_26 NOT_FOUND
,KEY_27 NOT_FOUND
,KEY_28 NOT_FOUND
,KEY_29 NOT_FOUND
,KEY_30 NOT_FOUND
,KEY_16 NOT_FOUND",
0, "",
"END");
=head
bop_smget_is($sock, "11 2 0..100 5", "bkey1,bkey2",
             5, "bkey2,bkey2,bkey2,bkey2,bkey2", "12,12,12,12,12", "20,40,60,80,100", "datum2,datum4,datum6,datum8,datum10",
             0, "", "END");
bop_smget_is($sock, "146 21 0..100000 10", "KEY_11,KEY_12,KEY_13,KEY_14,KEY_15,KEY_16,KEY_17,KEY_18,KEY_19,KEY_20,KEY_21,KEY_22,KEY_23,KEY_24,KEY_25,KEY_26,KEY_27,KEY_28,KEY_29,KEY_30,KEY_16",
             0, "", "", "", "",
             21, "KEY_11,KEY_12,KEY_13,KEY_14,KEY_15,KEY_16,KEY_17,KEY_18,KEY_19,KEY_20,KEY_21,KEY_22,KEY_23,KEY_24,KEY_25,KEY_26,KEY_27,KEY_28,KEY_29,KEY_30,KEY_16", "END");
=cut
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
