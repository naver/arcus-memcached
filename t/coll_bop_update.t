#!/usr/bin/perl

use strict;
use Test::More tests => 60;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1
get bkey2

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
bop insert bkey2 0x0100 0x11FF 7 create 11 0 0
datum10
bop insert bkey2 0x0080 0x01FF 6
datum8
bop insert bkey2 0x0060 0x00FF 6
datum6
bop insert bkey2 0x0040 0x000F 6
datum4
bop insert bkey2 0x0020 0x0000 6
datum2
bop get bkey1 0..100
bop get bkey2 0x0000..0xFFFFFFFF

bop update bkey1 90 8
datum999
bop update bkey1 70 8
datum777
bop update bkey1 50 10
datum55555
bop update bkey1 30 10
datum33333
bop update bkey1 10 6
datum0
bop get bkey1 0..100
bop update bkey1 10 10
datum11111
bop get bkey1 0..100

bop update bkey1 60 8
datum666
bop update bkey1 60 -1
bop update bkey1 0x60 8
datum666

bop update bkey2 0x0100 0x000011ff -1
bop update bkey2 0x0080 0x000001ff -1
bop update bkey2 0x0060 0x000000ff -1
bop update bkey2 0x0040 0x0000000f -1
bop update bkey2 0x0020 0x00000000 -1
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0100 0xbb0011ff -1
bop update bkey2 0x0080 0xbb0001ff -1
bop update bkey2 0x0060 0xbb0000ff -1
bop update bkey2 0x0040 0xbb00000f -1
bop update bkey2 0x0020 0xbb000000 -1
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0100 1 | 0xaa -1
bop update bkey2 0x0080 1 | 0xaa -1
bop update bkey2 0x0060 1 | 0xaa -1
bop update bkey2 0x0040 1 | 0xaa -1
bop update bkey2 0x0020 1 | 0xaa -1
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0100 0 & 0xF0 9
datum1010
bop update bkey2 0x0080 0 & 0xF0 8
datum888
bop update bkey2 0x0060 0 & 0xF0 3
666
bop update bkey2 0x0040 0 & 0xF0 13
datumdatum444
bop update bkey2 0x0020 0 & 0xF0 6
datum0
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0020 0 6
datum2
bop get bkey2 0x0000..0xFFFFFFFF

bop update bkey2 0x0030 6
datum2
bop update bkey2 0030 6
datum2
bop update bkey2 30 6
datum2
bop update bkey2 0x20 1 | 0xFF -1
bop update bkey2 0x0020 1 | 0xFF -1
bop update bkey2 0x0040 4 | 0xFF -1
bop update bkey2 0x0040 32 | 0xFF -1

delete bkey1
delete bkey2
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
$cmd = "bop insert bkey1 90 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 70 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 50 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 10 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0100 0x11FF 7 create 11 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0080 0x01FF 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0060 0x00FF 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0040 0x000F 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0020 0x0000 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0..100", 11, 5,
               "10,30,50,70,90", ",,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF", 11, 5,
               "0x0020,0x0040,0x0060,0x0080,0x0100", "0x0000,0x000F,0x00FF,0x01FF,0x11FF",
               "datum2,datum4,datum6,datum8,datum10", "END");
# Success Cases
$cmd = "bop update bkey1 90 8"; $val = "datum999"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey1 70 8"; $val = "datum777"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey1 50 10"; $val = "datum55555"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey1 30 10"; $val = "datum33333"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey1 10 6"; $val = "datum0"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0..100", 11, 5,
               "10,30,50,70,90", ",,,,",
               "datum0,datum33333,datum55555,datum777,datum999", "END");
$cmd = "bop update bkey1 10 10"; $val = "datum11111"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0..100", 11, 5,
               "10,30,50,70,90", ",,,,",
               "datum11111,datum33333,datum55555,datum777,datum999", "END");
# Fail Cases
$cmd = "bop update bkey1 60 8"; $val = "datum666"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey1 60 -1"; $rst = "NOTHING_TO_UPDATE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey1 0x60 8"; $val = "datum666"; $rst = "BKEY_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
# Success Cases
$cmd = "bop update bkey2 0x0100 0x000011ff -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0080 0x000001ff -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0060 0x000000ff -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0040 0x0000000f -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0020 0x00000000 -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF", 11, 5,
               "0x0020,0x0040,0x0060,0x0080,0x0100", "0x00000000,0x0000000F,0x000000FF,0x000001FF,0x000011FF",
               "datum2,datum4,datum6,datum8,datum10", "END");
$cmd = "bop update bkey2 0x0100 0xbb0011ff -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0080 0xbb0001ff -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0060 0xbb0000ff -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0040 0xbb00000f -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0020 0xbb000000 -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF", 11, 5,
               "0x0020,0x0040,0x0060,0x0080,0x0100", "0xBB000000,0xBB00000F,0xBB0000FF,0xBB0001FF,0xBB0011FF",
               "datum2,datum4,datum6,datum8,datum10", "END");
$cmd = "bop update bkey2 0x0100 1 | 0xaa -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0080 1 | 0xaa -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0060 1 | 0xaa -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0040 1 | 0xaa -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0020 1 | 0xaa -1"; $rst = "UPDATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF", 11, 5,
               "0x0020,0x0040,0x0060,0x0080,0x0100", "0xBBAA0000,0xBBAA000F,0xBBAA00FF,0xBBAA01FF,0xBBAA11FF",
               "datum2,datum4,datum6,datum8,datum10", "END");
$cmd = "bop update bkey2 0x0100 0 & 0xF0 9"; $val = "datum1010"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey2 0x0080 0 & 0xF0 8"; $val = "datum888"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey2 0x0060 0 & 0xF0 3"; $val = "666"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey2 0x0040 0 & 0xF0 13"; $val = "datumdatum444"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey2 0x0020 0 & 0xF0 6"; $val = "datum0"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF", 11, 5,
               "0x0020,0x0040,0x0060,0x0080,0x0100", "0xB0AA0000,0xB0AA000F,0xB0AA00FF,0xB0AA01FF,0xB0AA11FF",
               "datum0,datumdatum444,666,datum888,datum1010", "END");
$cmd = "bop update bkey2 0x0020 0 6"; $val = "datum2"; $rst = "UPDATED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF", 11, 5,
               "0x0020,0x0040,0x0060,0x0080,0x0100", ",0xB0AA000F,0xB0AA00FF,0xB0AA01FF,0xB0AA11FF",
               "datum2,datumdatum444,666,datum888,datum1010", "END");
# Fail Cases
$cmd = "bop update bkey2 0x0030 6"; $val = "datum2"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey2 0030 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey2 30 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop update bkey2 0x20 1 | 0xFF -1"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0020 1 | 0xFF -1"; $rst = "EFLAG_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0040 4 | 0xFF -1"; $rst = "EFLAG_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop update bkey2 0x0040 32 | 0xFF -1"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine, $server);
