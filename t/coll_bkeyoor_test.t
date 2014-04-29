#!/usr/bin/perl

use strict;
use Test::More tests => 77;
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
setattr bkey1 maxcount=5
getattr bkey1
bop get bkey1 0
bop get bkey1 20
bop get bkey1 60
bop get bkey1 100
bop get bkey1 1..5
bop get bkey1 5..1
bop get bkey1 20..25
bop get bkey1 25..20
bop get bkey1 82..87
bop get bkey1 87..82
bop get bkey1 100..110
bop get bkey1 110..100
setattr bkey1 overflowaction=largest_trim
getattr bkey1
bop get bkey1 0
bop get bkey1 20
bop get bkey1 60
bop get bkey1 100
bop get bkey1 1..5
bop get bkey1 5..1
bop get bkey1 20..25
bop get bkey1 25..20
bop get bkey1 82..87
bop get bkey1 87..82
bop get bkey1 100..110
bop get bkey1 110..100
setattr bkey1 overflowaction=smallest_trim
getattr bkey1
bop insert bkey1 100 7
datum10
bop insert bkey1 10 7
datum01
getattr bkey1
bop get bkey1 10
bop get bkey1 0..10
bop get bkey1 110
bop get bkey1 120..110
setattr bkey1 overflowaction=largest_trim
getattr bkey1
bop get bkey1 10
bop get bkey1 110
bop insert bkey1 10 7
datum01
bop insert bkey1 100 7
datum10
getattr bkey1
bop get bkey1 100
bop get bkey1 100..110
setattr bkey1 overflowaction=smallest_silent_trim
getattr bkey1
bop insert bkey1 100 7
datum10
bop insert bkey1 10 7
datum01
getattr bkey1
bop get bkey1 10
bop get bkey1 0..10
bop get bkey1 110
bop get bkey1 120..110
setattr bkey1 overflowaction=largest_silent_trim
getattr bkey1
bop get bkey1 10
bop get bkey1 110
bop insert bkey1 10 7
datum01
bop insert bkey1 100 7
datum10
getattr bkey1
bop get bkey1 100
bop get bkey1 100..110
delete bkey1
=cut


# testBOPSMGetSimple
$cmd = "get bkey1"; $rst = "END";
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
$cmd = "setattr bkey1 maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 maxcount", "maxcount=5");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=smallest_trim");
# $cmd = "bop get bkey1 0"; $rst = "OUT_OF_RANGE";
$cmd = "bop get bkey1 0"; $rst = "NOT_FOUND_ELEMENT"; # [ARCUS-137] improve the accuracy of trimmed status
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 20"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 60"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 100"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# $cmd = "bop get bkey1 0..5"; $rst = "OUT_OF_RANGE";
$cmd = "bop get bkey1 0..5"; $rst = "NOT_FOUND_ELEMENT"; # [ARCUS-137] improve the accuracy of trimmed status
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
#$cmd = "bop get bkey1 5..0"; $rst = "OUT_OF_RANGE";
$cmd = "bop get bkey1 5..0"; $rst = "NOT_FOUND_ELEMENT"; # [ARCUS-137] improve the accuracy of trimmed status
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 20..25"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 25..20"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 82..87"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 87..82"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 100..110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 110..100"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr bkey1 overflowaction=largest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=largest_trim");
$cmd = "bop get bkey1 0"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 20"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 60"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# $cmd = "bop get bkey1 100"; $rst = "OUT_OF_RANGE";
$cmd = "bop get bkey1 100"; $rst = "NOT_FOUND_ELEMENT"; # [ARCUS-137] improve the accuracy of trimmed status
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 0..5"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 5..0"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 20..25"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 25..20"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 82..87"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 87..82"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# $cmd = "bop get bkey1 100..110"; $rst = "OUT_OF_RANGE";
$cmd = "bop get bkey1 100..110"; $rst = "NOT_FOUND_ELEMENT"; # [ARCUS-137] improve the accuracy of trimmed status
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# $cmd = "bop get bkey1 110..100"; $rst = "OUT_OF_RANGE";
$cmd = "bop get bkey1 110..100"; $rst = "NOT_FOUND_ELEMENT"; # [ARCUS-137] improve the accuracy of trimmed status
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

$cmd = "setattr bkey1 overflowaction=smallest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=smallest_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop insert bkey1 100 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 10 7"; $val = "datum00"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=1");
$cmd = "bop get bkey1 10"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 0..10"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 120..110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

$cmd = "setattr bkey1 overflowaction=largest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=largest_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop get bkey1 10"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey1 10 7"; $val = "datum01"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 100 7"; $val = "datum10"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=1");
$cmd = "bop get bkey1 100"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 100..110"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

$cmd = "setattr bkey1 overflowaction=smallest_silent_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=smallest_silent_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop insert bkey1 100 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 10 7"; $val = "datum00"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop get bkey1 10"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 0..10"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 120..110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

$cmd = "setattr bkey1 overflowaction=largest_silent_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 overflowaction", "overflowaction=largest_silent_trim");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop get bkey1 10"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey1 10 7"; $val = "datum01"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 100 7"; $val = "datum10"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey1 trimmed", "trimmed=0");
$cmd = "bop get bkey1 100"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 100..110"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

