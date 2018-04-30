#!/usr/bin/perl

use strict;
use Test::More tests => 111;
=head
use Test::More tests => 115;
=cut
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1
get bkey2

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
$cmd = "setattr bkey1 maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 maxcount", "maxcount=5");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=smallest_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey1 0x0000..0x0200",
               11, 5, "0x0010,0x0030,0x0050,0x0070,0x0090", ",,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey1 0x0110 7"; $val = "datum11"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=1");
$cmd = "bop insert bkey1 0x0130 7"; $val = "datum13"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=1");
bop_ext_get_is($sock, "bkey1 0x0000..0x0200",
               11, 5, "0x0050,0x0070,0x0090,0x0110,0x0130", ",,,,",
               "datum5,datum7,datum9,datum11,datum13", "TRIMMED");
bop_ext_get_is($sock, "bkey1 0x0200..0x0000",
               11, 5, "0x0130,0x0110,0x0090,0x0070,0x0050", ",,,,",
               "datum13,datum11,datum9,datum7,datum5", "TRIMMED");
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
$cmd = "setattr bkey2 maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey2 maxcount", "maxcount=5");
getattr_is($sock, "bkey2 overflowaction", "overflowaction=smallest_trim");
getattr_is($sock, "bkey2 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey2 0x0000..0x0200",
               12, 5, "0x0020,0x0040,0x0060,0x0080,0x0100", ",,,,",
               "datum2,datum4,datum6,datum8,datum10", "END");
# smgets
bop_new_smget_is($sock, "11 2 0x0000..0x0200 10 duplicate", "bkey1 bkey2",
5,
"bkey2 12 0x0020 6 datum2
,bkey2 12 0x0040 6 datum4
,bkey2 12 0x0060 6 datum6
,bkey2 12 0x0080 6 datum8
,bkey2 12 0x0100 7 datum10",
1,
"bkey1 OUT_OF_RANGE",
0, "",
"END");
bop_new_smget_is($sock, "11 2 0x0200..0x0000 10 duplicate", "bkey1 bkey2",
10,
"bkey1 11 0x0130 7 datum13
,bkey1 11 0x0110 7 datum11
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0040 6 datum4
,bkey2 12 0x0020 6 datum2",
0, "",
1,
"bkey1 0x0050",
"END");
# OLD smget test : Use comma separated keys
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1,bkey2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_old_smget_is($sock, "11 2 0x0200..0x0000 10", "bkey1,bkey2",
8,
"bkey1 11 0x0130 7 datum13
,bkey1 11 0x0110 7 datum11
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5",
0, "",
"TRIMMED");
=head
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1,bkey2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_smget_is($sock, "11 2 0x0200..0x0000 10", "bkey1,bkey2",
                 8, "bkey1,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1", "11,11,12,11,12,11,12,11",
                 "0x0130,0x0110,0x0100,0x0090,0x0080,0x0070,0x0060,0x0050", ",,,,,,,",
                 "datum13,datum11,datum10,datum9,datum8,datum7,datum6,datum5",
                 0, "", "TRIMMED");
=cut

# prepare keys 2
$cmd = "setattr bkey1 overflowaction=largest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 maxcount", "maxcount=5");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=largest_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey1 0x0000..0x0200",
               11, 5, "0x0050,0x0070,0x0090,0x0110,0x0130", ",,,,",
               "datum5,datum7,datum9,datum11,datum13", "END");
$cmd = "bop insert bkey1 0x0150 7"; $val = "datum15"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=1");
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=1");

$cmd = "bop insert bkey2 0x0120 7"; $val = "datum12"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey2 trimmed", "trimmed=1");
$cmd = "bop insert bkey2 0x0140 7"; $val = "datum14"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey2 trimmed", "trimmed=1");
$cmd = "setattr bkey2 overflowaction=largest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey2 maxcount", "maxcount=5");
getattr_is($sock, "bkey2 overflowaction", "overflowaction=largest_trim");
getattr_is($sock, "bkey2 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey2 0x0000..0x0200",
               12, 5, "0x0060,0x0080,0x0100,0x0120,0x0140", ",,,,",
               "datum6,datum8,datum10,datum12,datum14", "END");

# smgets 2
bop_new_smget_is($sock, "11 2 0x0000..0x0200 10 duplicate", "bkey1 bkey2",
10,
"bkey1 11 0x0010 6 datum1
,bkey1 11 0x0030 6 datum3
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0100 7 datum10
,bkey2 12 0x0120 7 datum12
,bkey2 12 0x0140 7 datum14",
0, "",
1,
"bkey1 0x0090",
"END");
bop_new_smget_is($sock, "11 2 0x0200..0x0000 10 duplicate", "bkey1 bkey2",
5,
"bkey2 12 0x0140 7 datum14
,bkey2 12 0x0120 7 datum12
,bkey2 12 0x0100 7 datum10
,bkey2 12 0x0080 6 datum8
,bkey2 12 0x0060 6 datum6",
1,
"bkey1 OUT_OF_RANGE",
0, "",
"END");
# OLD smget test : Use comma separated keys
bop_old_smget_is($sock, "11 2 0x0000..0x0200 10", "bkey1,bkey2",
7,
"bkey1 11 0x0010 6 datum1
,bkey1 11 0x0030 6 datum3
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0090 6 datum9",
0, "",
"TRIMMED");
$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1 bkey2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
=head
bop_ext_smget_is($sock, "11 2 0x0000..0x0200 10", "bkey1,bkey2",
                 7, "bkey1,bkey1,bkey1,bkey2,bkey1,bkey2,bkey1", "11,11,11,12,11,12,11",
                 "0x0010,0x0030,0x0050,0x0060,0x0070,0x0080,0x0090", ",,,,,,",
                 "datum1,datum3,datum5,datum6,datum7,datum8,datum9",
                 0, "", "TRIMMED");
$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1,bkey2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
=cut

# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# prepare keys
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
$cmd = "setattr bkey1 maxcount=5 overflowaction=smallest_silent_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 maxcount", "maxcount=5");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=smallest_silent_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey1 0x0000..0x0200",
               11, 5, "0x0010,0x0030,0x0050,0x0070,0x0090", ",,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey1 0x0110 7"; $val = "datum11"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop insert bkey1 0x0130 7"; $val = "datum13"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey1 0x0000..0x0200",
               11, 5, "0x0050,0x0070,0x0090,0x0110,0x0130", ",,,,",
               "datum5,datum7,datum9,datum11,datum13", "END");
bop_ext_get_is($sock, "bkey1 0x0200..0x0000",
               11, 5, "0x0130,0x0110,0x0090,0x0070,0x0050", ",,,,",
               "datum13,datum11,datum9,datum7,datum5", "END");
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
$cmd = "setattr bkey2 maxcount=5 overflowaction=smallest_silent_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey2 maxcount", "maxcount=5");
getattr_is($sock, "bkey2 overflowaction", "overflowaction=smallest_silent_trim");
bop_ext_get_is($sock, "bkey2 0x0000..0x0200",
               12, 5, "0x0020,0x0040,0x0060,0x0080,0x0100", ",,,,",
               "datum2,datum4,datum6,datum8,datum10", "END");
# smgets
bop_new_smget_is($sock, "11 2 0x0000..0x0200 10 duplicate", "bkey1 bkey2",
10,
"bkey2 12 0x0020 6 datum2
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0110 7 datum11
,bkey1 11 0x0130 7 datum13",
0, "",
0, "",
"END");
bop_new_smget_is($sock, "11 2 0x0200..0x0000 10 duplicate", "bkey1 bkey2",
10,
"bkey1 11 0x0130 7 datum13
,bkey1 11 0x0110 7 datum11
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0040 6 datum4
,bkey2 12 0x0020 6 datum2",
0, "",
0, "",
"END");
# OLD smget test : Use comma separated keys
bop_old_smget_is($sock, "11 2 0x0000..0x0200 10", "bkey1,bkey2",
10,
"bkey2 12 0x0020 6 datum2
,bkey2 12 0x0040 6 datum4
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0110 7 datum11
,bkey1 11 0x0130 7 datum13",
0, "",
"END");
bop_old_smget_is($sock, "11 2 0x0200..0x0000 10", "bkey1,bkey2",
10,
"bkey1 11 0x0130 7 datum13
,bkey1 11 0x0110 7 datum11
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0040 6 datum4
,bkey2 12 0x0020 6 datum2",
0, "",
"END");
=head
bop_ext_smget_is($sock, "11 2 0x0000..0x0200 10", "bkey1,bkey2",
                 10, "bkey2,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey1",
                 "12,12,11,12,11,12,11,12,11,11",
                 "0x0020,0x0040,0x0050,0x0060,0x0070,0x0080,0x0090,0x0100,0x0110,0x0130", ",,,,,,,,,",
                 "datum2,datum4,datum5,datum6,datum7,datum8,datum9,datum10,datum11,datum13",
                 0, "", "END");
bop_ext_smget_is($sock, "11 2 0x0200..0x0000 10", "bkey1,bkey2",
                 10, "bkey1,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey2",
                 "11,11,12,11,12,11,12,11,12,12",
                 "0x0130,0x0110,0x0100,0x0090,0x0080,0x0070,0x0060,0x0050,0x0040,0x0020", ",,,,,,,,,",
                 "datum13,datum11,datum10,datum9,datum8,datum7,datum6,datum5,datum4,datum2",
                 0, "", "END");
=cut

# prepare keys 2
$cmd = "setattr bkey1 overflowaction=largest_silent_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 maxcount", "maxcount=5");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=largest_silent_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey1 0x0000..0x0200",
               11, 5, "0x0050,0x0070,0x0090,0x0110,0x0130", ",,,,",
               "datum5,datum7,datum9,datum11,datum13", "END");
$cmd = "bop insert bkey1 0x0150 7"; $val = "datum15"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");

$cmd = "bop insert bkey2 0x0120 7"; $val = "datum12"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey2 trimmed", "trimmed=0");
$cmd = "bop insert bkey2 0x0140 7"; $val = "datum14"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey2 trimmed", "trimmed=0");
$cmd = "setattr bkey2 overflowaction=largest_silent_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey2 maxcount", "maxcount=5");
getattr_is($sock, "bkey2 overflowaction", "overflowaction=largest_silent_trim");
getattr_is($sock, "bkey2 trimmed", "trimmed=0");
bop_ext_get_is($sock, "bkey2 0x0000..0x0200",
               12, 5, "0x0060,0x0080,0x0100,0x0120,0x0140", ",,,,",
               "datum6,datum8,datum10,datum12,datum14", "END");

# smgets 2
bop_new_smget_is($sock, "11 2 0x0000..0x0200 10 duplicate", "bkey1 bkey2",
10,
"bkey1 11 0x0010 6 datum1
,bkey1 11 0x0030 6 datum3
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0100 7 datum10
,bkey2 12 0x0120 7 datum12
,bkey2 12 0x0140 7 datum14",
0, "",
0, "",
"END");
bop_new_smget_is($sock, "11 2 0x0200..0x0000 10 duplicate", "bkey1 bkey2",
10,
"bkey2 12 0x0140 7 datum14
,bkey2 12 0x0120 7 datum12
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5
,bkey1 11 0x0030 6 datum3
,bkey1 11 0x0010 6 datum1",
0, "",
0, "",
"END");
# OLD smget test : Use comma separated keys
bop_old_smget_is($sock, "11 2 0x0000..0x0200 10", "bkey1,bkey2",
10,
"bkey1 11 0x0010 6 datum1
,bkey1 11 0x0030 6 datum3
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0100 7 datum10
,bkey2 12 0x0120 7 datum12
,bkey2 12 0x0140 7 datum14",
0, "",
"END");
bop_old_smget_is($sock, "11 2 0x0200..0x0000 10", "bkey1,bkey2",
10,
"bkey2 12 0x0140 7 datum14
,bkey2 12 0x0120 7 datum12
,bkey2 12 0x0100 7 datum10
,bkey1 11 0x0090 6 datum9
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0050 6 datum5
,bkey1 11 0x0030 6 datum3
,bkey1 11 0x0010 6 datum1",
0, "",
"END");
=head
bop_ext_smget_is($sock, "11 2 0x0000..0x0200 10", "bkey1,bkey2",
                 10, "bkey1,bkey1,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey2,bkey2",
                 "11,11,11,12,11,12,11,12,12,12",
                 "0x0010,0x0030,0x0050,0x0060,0x0070,0x0080,0x0090,0x0100,0x0120,0x0140", ",,,,,,,,,",
                 "datum1,datum3,datum5,datum6,datum7,datum8,datum9,datum10,datum12,datum14",
                 0, "", "END");
bop_ext_smget_is($sock, "11 2 0x0200..0x0000 10", "bkey1,bkey2",
                 10, "bkey2,bkey2,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey1,bkey1",
                 "12,12,12,11,12,11,12,11,11,11",
                 "0x0140,0x0120,0x0100,0x0090,0x0080,0x0070,0x0060,0x0050,0x0030,0x0010", ",,,,,,,,,",
                 "datum14,datum12,datum10,datum9,datum8,datum7,datum6,datum5,datum3,datum1",
                 0, "", "END");
=cut

# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# after test
release_memcached($engine, $server);
