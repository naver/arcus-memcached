#!/usr/bin/perl

use strict;
use Test::More tests => 30;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
ok 1 - get minbkey1: END
ok 2 - bop create minbkey1 1 0 100: CREATED
ok 3 - getattr minbkey1 minbkey == 'minbkey=-1'
ok 4 - getattr minbkey1 maxbkey == 'maxbkey=-1'
ok 5 - bop insert minbkey1 90 6 datum9: STORED
ok 6 - bop insert minbkey1 70 6 datum7: STORED
ok 7 - bop insert minbkey1 50 6 datum5: STORED
ok 8 - bop insert minbkey1 30 6 datum3: STORED
ok 9 - bop insert minbkey1 10 6 datum1: STORED
ok 10 - getattr minbkey1 minbkey == 'minbkey=10'
ok 11 - getattr minbkey1 maxbkey == 'maxbkey=90'
ok 12 - bop delete minbkey1 10: DELETED
ok 13 - getattr minbkey1 minbkey == 'minbkey=30'
ok 14 - getattr minbkey1 maxbkey == 'maxbkey=90'
ok 15 - delete minbkey1: DELETED
ok 16 - get minbkey1: END
ok 17 - bop create minbkey1 1 0 100: CREATED
ok 18 - getattr minbkey1 minbkey == 'minbkey=-1'
ok 19 - getattr minbkey1 maxbkey == 'maxbkey=-1'
ok 20 - bop insert minbkey1 0xffaa2211 6 datum9: STORED
ok 21 - bop insert minbkey1 0xee0122 6 datum7: STORED
ok 22 - bop insert minbkey1 0x110491 6 datum5: STORED
ok 23 - bop insert minbkey1 0x00019A 6 datum3: STORED
ok 24 - bop insert minbkey1 0x019ACC 6 datum1: STORED
ok 25 - getattr minbkey1 minbkey == 'minbkey=0x00019A'
ok 26 - getattr minbkey1 maxbkey == 'maxbkey=0xFFAA2211'
ok 27 - bop delete minbkey1 0xFFAA2211: DELETED
ok 28 - getattr minbkey1 minbkey == 'minbkey=0x00019A'
ok 29 - getattr minbkey1 maxbkey == 'maxbkey=0xEE0122'
ok 30 - delete minbkey1: DELETED
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Case 1
$cmd = "get minbkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create minbkey1 1 0 100"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "minbkey1 minbkey", "minbkey=-1");
getattr_is($sock, "minbkey1 maxbkey", "maxbkey=-1");
$cmd = "bop insert minbkey1 90 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 70 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 50 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 10 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "minbkey1 minbkey", "minbkey=10");
getattr_is($sock, "minbkey1 maxbkey", "maxbkey=90");

$cmd = "bop delete minbkey1 10"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "minbkey1 minbkey", "minbkey=30");
getattr_is($sock, "minbkey1 maxbkey", "maxbkey=90");

$cmd = "delete minbkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");



# Case 2
$cmd = "get minbkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create minbkey1 1 0 100"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "minbkey1 minbkey", "minbkey=-1");
getattr_is($sock, "minbkey1 maxbkey", "maxbkey=-1");
$cmd = "bop insert minbkey1 0xffaa2211 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 0xee0122 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 0x110491 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 0x00019A 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert minbkey1 0x019ACC 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "minbkey1 minbkey", "minbkey=0x00019A");
getattr_is($sock, "minbkey1 maxbkey", "maxbkey=0xFFAA2211");
$cmd = "bop delete minbkey1 0xFFAA2211"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "minbkey1 minbkey", "minbkey=0x00019A");
getattr_is($sock, "minbkey1 maxbkey", "maxbkey=0xEE0122");


$cmd = "delete minbkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# after test
release_memcached($engine);
