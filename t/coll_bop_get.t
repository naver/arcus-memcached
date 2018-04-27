#!/usr/bin/perl

use strict;
#use Test::More tests => 65;
use Test::More tests => 61;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1: END
get bkey2: END

bop insert bkey1 0x0090 0x11FF 6 create 11 0 0 datum9: CREATED_STORED
bop insert bkey1 0x0070 0x01FF 6 datum7: STORED
bop insert bkey1 0x0050 0x00FF 6 datum5: STORED
bop insert bkey1 0x0030 0x000F 6 datum3: STORED
bop insert bkey1 0x0010 6 datum1: STORED
bop get bkey1 0x00..0x1000 == 11 5 ebkeys eflags values
bop insert bkey2 0x0100 0x11FF 7 datum10: STORED
bop insert bkey2 0x0080 0x01FF 6 datum8: STORED
bop insert bkey2 0x0060 0x00FF 6 datum6: STORED
bop insert bkey2 0x0040 0x000F 6 datum4: STORED
bop insert bkey2 0x0020 0x0000 6 datum2: STORED
bop get bkey2 0x0000..0xFFFFFFFF == 11 5 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 EQ 0x000F == 11 1 ebkeys eflags values
# bop get bkey1 0x00..0x1000 0 EQ 0x000F,0x00FF == 11 2 ebkeys eflags values
# bop get bkey1 0x00..0x1000 0 & 0xFFFF EQ 0x000F,0x00FF == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 NE 0x0000 == 11 5 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 LT 0x00FF == 11 1 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 LE 0x00FF == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 GT 0x00FF == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 GE 0x00FF == 11 3 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 & 0x0100 EQ 0x0100 == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 | 0x0110 NE 0x0110 == 11 5 ebkeys eflags values
bop get bkey1 0x00..0x1000 1 ^ 0xF0   EQ 0xFF == 11 1 ebkeys eflags values
bop get bkey1 0x00..0x1000 1 & 0xFF   EQ 0x0F == 11 1 ebkeys eflags values
bop get bkey1 0x0050..0x0050 0 & 0xFFFF EQ 0x00FF 0 1 delete == 11 1 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 EQ 0x0000 == 11 1 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 NE 0x0000 == 11 4 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 LT 0x00FF == 11 2 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 LE 0x00FF == 11 3 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 GT 0x00FF == 11 2 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 GE 0x00FF == 11 3 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 & 0x0100 EQ 0x0100 == 11 2 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 | 0x0110 NE 0x0110 == 11 4 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0   EQ 0xFF == 11 1 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0   EQ 0xFF == 11 1 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 EQ 0x0000: NOT_FOUND_ELEMENT
bop get bkey1 0..0x1000: CLIENT_ERROR bad command line format
# bop get bkey1 0..0x1000 0 EQ 0x000F,0x000FF: CLIENT_ERROR bad command line format
bop get bkey1 0..1000: BKEY_MISMATCH
bop get kvkey 0..1000: NOT_FOUND
bop get kvkey 0..1000: NOT_FOUND
set kvkey 0 0 6 datumx: STORED
bop get kvkey 0..1000: TYPE_MISMATCH

delete kvkey: DELETED
delete bkey2: DELETED

bop insert bkey2 0x00 6 create 13 0 0 datum1: CREATED_STORED
bop insert bkey2 0x0000 0x0202 7 datum22: STORED
bop insert bkey2 0x000000 8 datum333: STORED
bop insert bkey2 0x01 0x01 9 datum4444: STORED
bop insert bkey2 0x0001 10 datum55555: STORED
bop insert bkey2 0x000001 0x030303 11 datum666666: STORED
bop insert bkey2 0x02 12 datum7777777: STORED
bop insert bkey2 0x0002 0x0202 13 datum88888888: STORED
bop insert bkey2 0x000002 14 datum999999999: STORED
bop insert bkey2 0x0101 0x00 5 datum: STORED

bop get bkey2 0x01..0x02 == 13 3 ebkeys eflags values
bop get bkey2 0x0001..0x0002 == 13 2 ebkeys eflags values
bop get bkey2 0x00..0x0002 == 13 7 ebkeys eflags values
bop get bkey2 0x02..0x0000 == 13 9 ebkeys eflags values

delete bkey1: DELETED
delete bkey2: DELETED
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
# Prepare Keys
$cmd = "bop insert bkey1 0x0090 0x11FF 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0070 0x01FF 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0050 0x00FF 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0030 0x000F 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");

$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0x1000",
               11, 5, "0x0010,0x0030,0x0050,0x0070,0x0090", ",0x000F,0x00FF,0x01FF,0x11FF",
               "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop create bkey2 11 0 0"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey2 0x0100 0x11FF 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0080 0x01FF 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0060 0x00FF 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0040 0x000F 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0020 0x0000 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF",
               11, 5, "0x0020,0x0040,0x0060,0x0080,0x0100", "0x0000,0x000F,0x00FF,0x01FF,0x11FF",
               "datum2,datum4,datum6,datum8,datum10", "END");
# Success cases
bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 EQ 0x000F",
               11, 1, "0x0030", "0x000F", "datum3", "END");
# bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 EQ 0x000F,0x00FF",
#               11, 2, "0x0030,0x0050", "0x000F,0x00FF", "datum3,datum5", "END");
# bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 & 0xFFFF EQ 0x000F,0x00FF",
#                11, 2, "0x0030,0x0050", "0x000F,0x00FF", "datum3,datum5", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 NE 0x000F",
               11, 4, "0x0010,0x0050,0x0070,0x0090", ",0x00FF,0x01FF,0x11FF",
               "datum1,datum5,datum7,datum9", "END");

bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 LT 0x00FF",
               11, 1, "0x0030", "0x000F", "datum3", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 LE 0x00FF",
               11, 2, "0x0030,0x0050", "0x000F,0x00FF", "datum3,datum5", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 GT 0x00FF",
               11, 2, "0x0070,0x0090", "0x01FF,0x11FF", "datum7,datum9", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 GE 0x00FF",
               11, 3, "0x0050,0x0070,0x0090", "0x00FF,0x01FF,0x11FF", "datum5,datum7,datum9", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 & 0x0100 EQ 0x0100",
               11, 2, "0x0070,0x0090", "0x01FF,0x11FF", "datum7,datum9", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 0 | 0x0110 NE 0x0110",
               11, 5, "0x0010,0x0030,0x0050,0x0070,0x0090", ",0x000F,0x00FF,0x01FF,0x11FF",
               "datum1,datum3,datum5,datum7,datum9", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 1 ^ 0xF0   EQ 0xFF",
               11, 1, "0x0030", "0x000F", "datum3", "END");
bop_ext_get_is($sock, "bkey1 0x00..0x1000 1 & 0xFF   EQ 0x0F",
               11, 1, "0x0030", "0x000F", "datum3", "END");
# bop_ext_get_is($sock, "bkey1 0x0030..0x0090 0 NE 0x000F,0x11FF",
#                11, 2, "0x0050,0x0070", "0x00FF,0x01FF",
#                "datum5,datum7", "END");
bop_ext_get_is($sock, "bkey1 0x0050..0x0050 0 & 0xFFFF EQ 0x00FF 0 1 delete",
               11, 1, "0x0050", "0x00FF", "datum5", "DELETED");

bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 EQ 0x0000",
               11, 1, "0x0020", "0x0000", "datum2", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 NE 0x0000",
               11, 4, "0x0040,0x0060,0x0080,0x0100", "0x000F,0x00FF,0x01FF,0x11FF",
               "datum4,datum6,datum8,datum10", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 LT 0x00FF",
               11, 2, "0x0020,0x0040", "0x0000,0x000F", "datum2,datum4", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 LE 0x00FF",
               11, 3, "0x0020,0x0040,0x0060", "0x0000,0x000F,0x00FF", "datum2,datum4,datum6", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 GT 0x00FF",
               11, 2, "0x0080,0x0100", "0x01FF,0x11FF", "datum8,datum10", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 GE 0x00FF",
               11, 3, "0x0060,0x0080,0x0100", "0x00FF,0x01FF,0x11FF", "datum6,datum8,datum10", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 & 0x0100 EQ 0x0100",
               11, 2, "0x0080,0x0100", "0x01FF,0x11FF", "datum8,datum10", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 0 | 0x0110 NE 0x0110",
               11, 4, "0x0040,0x0060,0x0080,0x0100", "0x000F,0x00FF,0x01FF,0x11FF", "datum4,datum6,datum8,datum10", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0   EQ 0xFF",
               11, 1, "0x0040", "0x000F", "datum4", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0   EQ 0xFF",
               11, 1, "0x0040", "0x000F", "datum4", "END");

# Fail Cases
$cmd = "bop get bkey1 0x00..0x1000 0 EQ 0x0000"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 0..0x1000"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# $cmd = "bop get bkey1 0..0x1000 0 EQ 0x000F,0x000FF"; $rst = "CLIENT_ERROR bad command line format";
# print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 0..1000"; $rst = "BKEY_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get kvkey 0..1000"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get kvkey 0..1000"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop get kvkey 0..1000"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete kvkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# Other Cases
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey2 0x00 6 create 13 0 0"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0000 0x0202 7"; $val = "datum22"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x000000 8"; $val = "datum333"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x01 0x01 9"; $val = "datum4444"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0001 10"; $val = "datum55555"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x000001 0x030303 11"; $val = "datum666666"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x02 12"; $val = "datum7777777"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0002 0x0202 13"; $val = "datum88888888"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x000002 14"; $val = "datum999999999"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0101 0x00 5"; $val = "datum"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x01..0x02",
               13, 3, "0x01,0x0101,0x02", "0x01,0x00,", "datum4444,datum,datum7777777", "END");
bop_ext_get_is($sock, "bkey2 0x0001..0x0002",
               13, 2, "0x0001,0x0002", ",0x0202", "datum55555,datum88888888", "END");
bop_ext_get_is($sock, "bkey2 0x00..0x0002",
               13, 7,
               "0x00,0x0000,0x000000,0x000001,0x000002,0x0001,0x0002", ",0x0202,,0x030303,,,0x0202",
               "datum1,datum22,datum333,datum666666,datum999999999,datum55555,datum88888888", "END");
bop_ext_get_is($sock, "bkey2 0x02..0x0000",
               13, 9,
               "0x02,0x0101,0x01,0x0002,0x0001,0x000002,0x000001,0x000000,0x0000", ",0x00,0x01,0x0202,,,0x030303,,0x0202",
               "datum7777777,datum,datum4444,datum88888888,datum55555,datum999999999,datum666666,datum333,datum22", "END");
# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine);
