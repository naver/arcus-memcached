#!/usr/bin/perl

use strict;
use Test::More tests => 59;
=head
use Test::More tests => 65;
=cut
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1
get bkey2
get kvkey
set kvkey 0 0 6
datumx

bop insert bkey1 0x0090 6 create 11 0 0
datum9
bop insert bkey1 0x0070 6
datum7
bop insert bkey1 0x0050 6
datum5
bop insert bkey1 0x0030 6
datum3
bop insert bkey1 0x0010 6
datum1
bop insert bkey2 0x0100 7 create 12 0 0
datum10
bop insert bkey2 0x0080 6
datum8
bop insert bkey2 0x0060 6
datum6
bop insert bkey2 0x0040 6
datum4
bop insert bkey2 0x0020 6
datum2
bop get bkey1 0x00..0x1000
bop get bkey2 0x0000..0xFFFFFFFF

bop smget 11 2 0x0000..0x0100 5
bkey1 bkey2
bop smget 23 4 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4
bop smget 23 4 0x0090..0x0030 2 9
bkey2 bkey3 bkey1 bkey4
bop smget 23 4 0x0200..0x0300 2 6
bkey2 bkey3 bkey1 bkey4
bop smget 29 5 0x0000..0x0100 1 6
bkey2 bkey3 bkey1 bkey4 bkey3

bop smget 28 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 kvkey
bop smget 29 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 bkey1
bop smget 23 2 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4

delete bkey1
delete bkey2

bop insert bkey1 0x0090 6 create 11 0 0
datum9
bop insert bkey1 0x00000070 6
datum7
bop insert bkey1 0x000000000050 6
datum5
bop insert bkey1 0x0000000000000030 6
datum3
bop insert bkey1 0x00000000000000000010 6
datum1
bop insert bkey2 0x01 7 create 11 0 0
datum10
bop insert bkey2 0x000080 6
datum8
bop insert bkey2 0x0000000060 6
datum6
bop insert bkey2 0x00000000000040 6
datum4
bop insert bkey2 0x000000000000000020 6
datum2
bop get bkey1 0x00..0xFF
bop get bkey2 0x00..0xFFFFFFFFFFFFFFFFFFFF


bop smget 11 2 0x00..0xFF 5
bkey1 bkey2
bop smget 23 4 0x00..0xFFFF 2 6
bkey2 bkey3 bkey1 bkey4
bop smget 23 4 0x0090..0x0000000000000030 2 9
bkey2 bkey3 bkey1 bkey4
bop smget 23 4 0x0200..0x0300 2 6
bkey2 bkey3 bkey1 bkey4
bop smget 29 5 0x0000..0x0100 1 6
bkey2 bkey3 bkey1 bkey4 bkey3

bop smget 28 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 kvkey
bop smget 29 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 bkey1
bop smget 23 2 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4

delete bkey1
delete bkey2
delete kvkey
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get bkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey2"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get kvkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
# Prepare Keys
$cmd = "bop insert bkey1 0x0090 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0070 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0050 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0x1000",
               11, 5, "0x0010,0x0030,0x0050,0x0070,0x0090", ",,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey2 0x0100 7 create 12 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0080 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0060 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0040 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0020 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF",
               12, 5, "0x0020,0x0040,0x0060,0x0080,0x0100", ",,,,",
               "datum2,datum4,datum6,datum8,datum10", "END");
# smgets
bop_new_smget_is($sock, "11 2 0x0000..0x0100 5 duplicate", "bkey1 bkey2",
5,
"bkey1 11 0x0010 6 datum1
,bkey2 12 0x0020 6 datum2
,bkey1 11 0x0030 6 datum3
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5",
0, "",
0, "",
"END");
bop_new_smget_is($sock, "23 4 0x0000..0x0100 2 6 duplicate", "bkey2 bkey3 bkey1 bkey4",
6,
"bkey1 11 0x0030 6 datum3
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "23 4 0x0090..0x0030 2 9 duplicate", "bkey2 bkey3 bkey1 bkey4",
5,
"bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0030 6 datum3",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "23 4 0x0200..0x0300 2 6 duplicate", "bkey2 bkey3 bkey1 bkey4",
0, "",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "29 5 0x0000..0x0100 1 6 duplicate", "bkey2 bkey3 bkey1 bkey4 bkey3",
6,
"bkey2 12 0x0020 6 datum2
,bkey1 11 0x0030 6 datum3
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7",
3,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND
,bkey3 NOT_FOUND",
0, "",
"END");
# OLD smget test : Use comma separated keys
bop_old_smget_is($sock, "11 2 0x0000..0x0100 5", "bkey1,bkey2",
5,
"bkey1 11 0x0010 6 datum1
,bkey2 12 0x0020 6 datum2
,bkey1 11 0x0030 6 datum3
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5",
0, "",
"END");
bop_old_smget_is($sock, "23 4 0x0000..0x0100 2 6", "bkey2,bkey3,bkey1,bkey4",
6,
"bkey1 11 0x0030 6 datum3
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8",
2,
"bkey3,bkey4",
"END");
bop_old_smget_is($sock, "23 4 0x0090..0x0030 2 9", "bkey2,bkey3,bkey1,bkey4",
5,
"bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0030 6 datum3",
2,
"bkey3,bkey4",
"END");
bop_old_smget_is($sock, "23 4 0x0200..0x0300 2 6", "bkey2,bkey3,bkey1,bkey4",
0, "",
2,
"bkey3,bkey4",
"END");
bop_old_smget_is($sock, "29 5 0x0000..0x0100 1 6", "bkey2,bkey3,bkey1,bkey4,bkey3",
6,
"bkey2 12 0x0020 6 datum2
,bkey1 11 0x0030 6 datum3
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7",
3,
"bkey3,bkey4,bkey3",
"END");
=head
bop_ext_smget_is($sock, "11 2 0x0000..0x0100 5", "bkey1,bkey2",
                 5, "bkey1,bkey2,bkey1,bkey2,bkey1", "11,12,11,12,11",
                 "0x0010,0x0020,0x0030,0x0040,0x0050", ",,,,", "datum1,datum2,datum3,datum4,datum5",
                 0, "", "END");
bop_ext_smget_is($sock, "23 4 0x0000..0x0100 2 6", "bkey2,bkey3,bkey1,bkey4",
                 6, "bkey1,bkey2,bkey1,bkey2,bkey1,bkey2", "11,12,11,12,11,12",
                 "0x0030,0x0040,0x0050,0x0060,0x0070,0x0080", ",,,,,", "datum3,datum4,datum5,datum6,datum7,datum8",
                 2, "bkey3,bkey4", "END");
bop_ext_smget_is($sock, "23 4 0x0090..0x0030 2 9", "bkey2,bkey3,bkey1,bkey4",
                 5, "bkey1,bkey2,bkey1,bkey2,bkey1", "11,12,11,12,11",
                 "0x0070,0x0060,0x0050,0x0040,0x0030", ",,,,", "datum7,datum6,datum5,datum4,datum3",
                 2, "bkey3,bkey4", "END");
bop_ext_smget_is($sock, "23 4 0x0200..0x0300 2 6", "bkey2,bkey3,bkey1,bkey4",
                 0, "", "", "", "", "",
                 2, "bkey3,bkey4", "END");
bop_ext_smget_is($sock, "29 5 0x0000..0x0100 1 6", "bkey2,bkey3,bkey1,bkey4,bkey3",
                 6, "bkey2,bkey1,bkey2,bkey1,bkey2,bkey1", "12,11,12,11,12,11",
                 "0x0020,0x0030,0x0040,0x0050,0x0060,0x0070", ",,,,,", "datum2,datum3,datum4,datum5,datum6,datum7",
                 3, "bkey3,bkey4,bkey3", "END");
=cut

# fails
$cmd = "bop smget 29 5 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 29 5 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 23 2 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# initialzie
$cmd = "bop insert bkey1 0x0090 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x00000070 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x000000000050 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0000000000000030 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x00000000000000000010 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0xFF",
               11, 5, "0x00000000000000000010,0x0000000000000030,0x000000000050,0x00000070,0x0090", ",,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey2 0x01 7 create 12 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x000080 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0000000060 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x00000000000040 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x000000000000000020 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x00..0xFFFFFFFFFFFFFFFFFF",
               12, 5, "0x000000000000000020,0x00000000000040,0x0000000060,0x000080,0x01", ",,,,",
               "datum2,datum4,datum6,datum8,datum10", "END");
# smgets : Use comma sperated keys for backward compatibility check
bop_new_smget_is($sock, "11 2 0x00..0xFF 5 duplicate", "bkey1,bkey2",
5,
"bkey1 11 0x00000000000000000010 6 datum1
,bkey2 12 0x000000000000000020 6 datum2
,bkey1 11 0x0000000000000030 6 datum3
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x000000000050 6 datum5",
0, "",
0, "",
"END");
bop_new_smget_is($sock, "23 4 0x00..0xFFFF 2 6 duplicate", "bkey2,bkey3,bkey1,bkey4",
6,
"bkey1 11 0x0000000000000030 6 datum3
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x000000000050 6 datum5
,bkey2 12 0x0000000060 6 datum6
,bkey1 11 0x00000070 6 datum7
,bkey2 12 0x000080 6 datum8",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "23 4 0x0090..0x0000000000000030 2 9 duplicate", "bkey2,bkey3,bkey1,bkey4",
5,
"bkey1 11 0x00000070 6 datum7
,bkey2 12 0x0000000060 6 datum6
,bkey1 11 0x000000000050 6 datum5
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x0000000000000030 6 datum3",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "23 4 0x0200..0x0300 2 6 duplicate", "bkey2,bkey3,bkey1,bkey4",
0, "",
2,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND",
0, "",
"END");
bop_new_smget_is($sock, "29 5 0x0000..0x0100 1 6 duplicate", "bkey2,bkey3,bkey1,bkey4,bkey3",
6,
"bkey2 12 0x000000000000000020 6 datum2
,bkey1 11 0x0000000000000030 6 datum3
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x000000000050 6 datum5
,bkey2 12 0x0000000060 6 datum6
,bkey1 11 0x00000070 6 datum7",
3,
"bkey3 NOT_FOUND
,bkey4 NOT_FOUND
,bkey3 NOT_FOUND",
0, "",
"END");
# OLD smget test : Use comma separated keys
bop_old_smget_is($sock, "11 2 0x00..0xFF 5", "bkey1,bkey2",
5,
"bkey1 11 0x00000000000000000010 6 datum1
,bkey2 12 0x000000000000000020 6 datum2
,bkey1 11 0x0000000000000030 6 datum3
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x000000000050 6 datum5",
0, "",
"END");
bop_old_smget_is($sock, "23 4 0x00..0xFFFF 2 6", "bkey2,bkey3,bkey1,bkey4",
6,
"bkey1 11 0x0000000000000030 6 datum3
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x000000000050 6 datum5
,bkey2 12 0x0000000060 6 datum6
,bkey1 11 0x00000070 6 datum7
,bkey2 12 0x000080 6 datum8",
2,
"bkey3,bkey4",
"END");
bop_old_smget_is($sock, "23 4 0x0090..0x0000000000000030 2 9", "bkey2,bkey3,bkey1,bkey4",
5,
"bkey1 11 0x00000070 6 datum7
,bkey2 12 0x0000000060 6 datum6
,bkey1 11 0x000000000050 6 datum5
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x0000000000000030 6 datum3",
2,
"bkey3,bkey4",
"END");
bop_old_smget_is($sock, "23 4 0x0200..0x0300 2 6", "bkey2,bkey3,bkey1,bkey4",
0, "",
2,
"bkey3,bkey4",
"END");
bop_old_smget_is($sock, "29 5 0x0000..0x0100 1 6", "bkey2,bkey3,bkey1,bkey4,bkey3",
6,
"bkey2 12 0x000000000000000020 6 datum2
,bkey1 11 0x0000000000000030 6 datum3
,bkey2 12 0x00000000000040 6 datum4
,bkey1 11 0x000000000050 6 datum5
,bkey2 12 0x0000000060 6 datum6
,bkey1 11 0x00000070 6 datum7",
3,
"bkey3,bkey4,bkey3",
"END");
=head
bop_ext_smget_is($sock, "11 2 0x00..0xFF 5", "bkey1,bkey2",
                 5, "bkey1,bkey2,bkey1,bkey2,bkey1", "11,12,11,12,11",
                 "0x00000000000000000010,0x000000000000000020,0x0000000000000030,0x00000000000040,0x000000000050",
                 ",,,,", "datum1,datum2,datum3,datum4,datum5",
                 0, "", "END");
bop_ext_smget_is($sock, "23 4 0x00..0xFFFF 2 6", "bkey2,bkey3,bkey1,bkey4",
                 6, "bkey1,bkey2,bkey1,bkey2,bkey1,bkey2", "11,12,11,12,11,12",
                 "0x0000000000000030,0x00000000000040,0x000000000050,0x0000000060,0x00000070,0x000080",
                 ",,,,,", "datum3,datum4,datum5,datum6,datum7,datum8",
                 2, "bkey3,bkey4", "END");
bop_ext_smget_is($sock, "23 4 0x0090..0x0000000000000030 2 9", "bkey2,bkey3,bkey1,bkey4",
                 5, "bkey1,bkey2,bkey1,bkey2,bkey1", "11,12,11,12,11",
                 "0x00000070,0x0000000060,0x000000000050,0x00000000000040,0x0000000000000030",
                 ",,,,", "datum7,datum6,datum5,datum4,datum3",
                 2, "bkey3,bkey4", "END");
bop_ext_smget_is($sock, "23 4 0x0200..0x0300 2 6", "bkey2,bkey3,bkey1,bkey4",
                 0, "", "", "", "", "",
                 2, "bkey3,bkey4", "END");
bop_ext_smget_is($sock, "29 5 0x0000..0x0100 1 6", "bkey2,bkey3,bkey1,bkey4,bkey3",
                 6, "bkey2,bkey1,bkey2,bkey1,bkey2,bkey1", "12,11,12,11,12,11",
                 "0x000000000000000020,0x0000000000000030,0x00000000000040,0x000000000050,0x0000000060,0x00000070",
                 ",,,,,", "datum2,datum3,datum4,datum5,datum6,datum7",
                 3, "bkey3,bkey4,bkey3", "END");
=cut

# fails
$cmd = "bop smget 29 5 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 29 5 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop smget 23 2 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete kvkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");




# after test
release_memcached($engine);
