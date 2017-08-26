#!/usr/bin/perl

use strict;
use Test::More tests => 62;
=head
use Test::More tests => 63;
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
bop insert bkey1 10 7 create 12 0 1000
datum01
bop insert bkey1 30 7
datum03
bop insert bkey1 50 7
datum05
bop insert bkey1 70 7
datum07
bop insert bkey1 90 7
datum09
setattr bkey1 maxcount=5
getattr bkey1
bop insert bkey1 110 7
datum11
getattr bkey1
bop get bkey1 0..1000
bop get bkey1 1000..0
bop get bkey1 0..20
bop get bkey1 20..0
bop get bkey1 20..1000
bop get bkey1 1000..20
bop get bkey1 20
bop get bkey1 30..1000
bop get bkey1 1000..30
bop get bkey1 30
bop get bkey1 40..1000
bop get bkey1 1000..40
bop get bkey1 40
bop get bkey1 200..1000
bop get bkey1 1000..200
bop get bkey1 200
bop insert bkey2 20 7 create 12 0 1000
datum02
bop insert bkey2 40 7
datum04
bop insert bkey2 60 7
datum06
bop insert bkey2 80 7
datum08
bop insert bkey2 100 7
datum10
setattr bkey2 maxcount=5 overflowaction=largest_trim
getattr bkey2
bop insert bkey2 10 7
datum01
getattr bkey2
bop get bkey2 0..1000
bop get bkey2 1000..0
bop get bkey2 90..0
bop get bkey2 0..90
bop get bkey2 90
bop get bkey2 80..0
bop get bkey2 0..80
bop get bkey2 80
bop get bkey2 70..0
bop get bkey2 0..70
bop get bkey2 70
bop get bkey2 0..5
bop get bkey2 5..0
bop get bkey2 5
setattr bkey2 overflowaction=smallest_trim
getattr bkey2
bop insert bkey2 100 7
datum10
bop insert bkey2 120 7
datum12
getattr bkey2
bop smget 11 2 120..40 10
bkey1 bkey2
delete bkey1
delete bkey2
=cut

$cmd = "get bkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey2"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

$cmd = "bop insert bkey1 10 7 create 12 0 1000"; $val = "datum01"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 30 7"; $val = "datum03"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 50 7"; $val = "datum05"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 70 7"; $val = "datum07"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 90 7"; $val = "datum09"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr bkey1 maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 maxcount", "maxcount=5");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=smallest_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop insert bkey1 110 7"; $val = "datum11"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed minbkey maxbkey", "trimmed=1 minbkey=30 maxbkey=110");
bop_get_is($sock, "bkey1 0..1000", 12, 5, "30,50,70,90,110",
                  "datum03,datum05,datum07,datum09,datum11", "TRIMMED");
bop_get_is($sock, "bkey1 1000..0", 12, 5, "110,90,70,50,30",
                  "datum11,datum09,datum07,datum05,datum03", "TRIMMED");
$cmd = "bop get bkey1 0..20"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 20..0"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey1 20..1000", 12, 5, "30,50,70,90,110",
                  "datum03,datum05,datum07,datum09,datum11", "TRIMMED");
bop_get_is($sock, "bkey1 1000..20", 12, 5, "110,90,70,50,30",
                  "datum11,datum09,datum07,datum05,datum03", "TRIMMED");
$cmd = "bop get bkey1 20"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey1 30..1000", 12, 5, "30,50,70,90,110",
                  "datum03,datum05,datum07,datum09,datum11", "END");
bop_get_is($sock, "bkey1 1000..30", 12, 5, "110,90,70,50,30",
                  "datum11,datum09,datum07,datum05,datum03", "END");
bop_get_is($sock, "bkey1 30", 12, 1, "30", "datum03", "END");
bop_get_is($sock, "bkey1 40..1000", 12, 4, "50,70,90,110",
                  "datum05,datum07,datum09,datum11", "END");
bop_get_is($sock, "bkey1 1000..40", 12, 4, "110,90,70,50",
                  "datum11,datum09,datum07,datum05", "END");
$cmd = "bop get bkey1 40"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 200..1000"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 1000..200"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 200"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey2 20 7 create 12 0 1000"; $val = "datum02"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 40 7"; $val = "datum04"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 60 7"; $val = "datum06"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 80 7"; $val = "datum08"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr bkey2 maxcount=5 overflowaction=largest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey2 maxcount", "maxcount=5");
getattr_is($sock, "bkey2 overflowaction", "overflowaction=largest_trim");
getattr_is($sock, "bkey2 trimmed", "trimmed=0");
$cmd = "bop insert bkey2 10 7"; $val = "datum01"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey2 trimmed minbkey maxbkey", "trimmed=1 minbkey=10 maxbkey=80");
bop_get_is($sock, "bkey2 0..1000", 12, 5, "10,20,40,60,80",
                  "datum01,datum02,datum04,datum06,datum08", "TRIMMED");
bop_get_is($sock, "bkey2 1000..0", 12, 5, "80,60,40,20,10",
                  "datum08,datum06,datum04,datum02,datum01", "TRIMMED");
bop_get_is($sock, "bkey2 90..0", 12, 5, "80,60,40,20,10",
                  "datum08,datum06,datum04,datum02,datum01", "TRIMMED");
bop_get_is($sock, "bkey2 0..90", 12, 5, "10,20,40,60,80",
                  "datum01,datum02,datum04,datum06,datum08", "TRIMMED");
$cmd = "bop get bkey2 90"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey2 80..0", 12, 5, "80,60,40,20,10",
                  "datum08,datum06,datum04,datum02,datum01", "END");
bop_get_is($sock, "bkey2 0..80", 12, 5, "10,20,40,60,80",
                  "datum01,datum02,datum04,datum06,datum08", "END");
bop_get_is($sock, "bkey2 80", 12, 1, "80", "datum08", "END");
bop_get_is($sock, "bkey2 70..0", 12, 4, "60,40,20,10",
                  "datum06,datum04,datum02,datum01", "END");
bop_get_is($sock, "bkey2 0..70", 12, 4, "10,20,40,60",
                  "datum01,datum02,datum04,datum06", "END");
$cmd = "bop get bkey2 70"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey2 0..5"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey2 5..0"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey2 5"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr bkey2 overflowaction=smallest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey2 trimmed minbkey maxbkey", "trimmed=0 minbkey=10 maxbkey=80");
$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 120 7"; $val = "datum12"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_new_smget_is($sock, "11 2 120..40 10 duplicate", "bkey1 bkey2",
9,
"bkey2 12 120 7 datum12
,bkey1 12 110 7 datum11
,bkey2 12 100 7 datum10
,bkey1 12 90 7 datum09
,bkey2 12 80 7 datum08
,bkey1 12 70 7 datum07
,bkey2 12 60 7 datum06
,bkey1 12 50 7 datum05
,bkey2 12 40 7 datum04",
0,"",
0,"",
"END");
# OLD smget test : Use comma separated keys
bop_old_smget_is($sock, "11 2 120..40 10", "bkey1,bkey2",
9,
"bkey2 12 120 7 datum12
,bkey1 12 110 7 datum11
,bkey2 12 100 7 datum10
,bkey1 12 90 7 datum09
,bkey2 12 80 7 datum08
,bkey1 12 70 7 datum07
,bkey2 12 60 7 datum06
,bkey1 12 50 7 datum05
,bkey2 12 40 7 datum04",
0,"",
"END");
=head
bop_smget_is($sock, "11 2 120..40 10", "bkey1,bkey2",
             9, "bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2,bkey1,bkey2", "12,12,12,12,12,12,12,12,12",
             "120,110,100,90,80,70,60,50,40",
             "datum12,datum11,datum10,datum09,datum08,datum07,datum06,datum05,datum04",
             0, "", "END");
=cut
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
