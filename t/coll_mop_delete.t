#!/usr/bin/perl

use strict;
use Test::More tests => 22;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get mkey1: END
mop insert mkey1 f9 6 create 11 0 0 datum9: CREATED_STORED
mop insert mkey1 f7 6 datum7: STORED
mop insert mkey1 f5 6 datum5: STORED
mop insert mkey1 f3 6 datum3: STORED
mop insert mkey1 f1 6 datum1: STORED
mop insert mkey1 f2 6 datum2: STORED
mop insert mkey1 f4 6 datum4: STORED
mop insert mkey1 f6 6 datum6: STORED
mop insert mkey1 f8 6 datum8: STORED
mop get mkey1 26 9
f1,f2,f3,f4,f5,f6,f7,f8,f9

mop delete mkey1 5: CLIENT_ERROR bad command line format
mop delete mkey1 3 1 f10: NOT_FOUND_ELEMENT
mop delete mkey1 7 2 f11,f12: NOT_FOUND_ELEMENT
mop delete mkey1 -1 -1: CLIENT_ERROR bad command line format
mop delete mkey1 10 -1: CLIENT_ERROR bad command line format
mop dleete mkey1 -1 10: CLIENT_ERROR bad command line format

mop delete mkey1 2 1 f2: DELETED
mop get mkey 26 9
f1,f2,f3,f4,f5,f6,f7,f8,f9
mop delete mkey1 5 2 f9,f5: DELETED
mop get mkey1 26 9
f1,f2,f3,f4,f5,f6,f7,f8,f9
mop delete mkey1 0 0 true: DELETED_DROPPED
get mkey1: END
=cut

my $server = new_memcached();
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;
my $flist;

# Initialize
$cmd = "get mkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Success Cases
$cmd = "mop insert mkey1 f9 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f7 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f5 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f3 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f1 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f2 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f4 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f6 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f8 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
mop_get_is($sock, "mkey1 26 9", 11, 9, 9, "f1,f2,f3,f4,f5,f6,f7,f8,f9", "f1,f2,f3,f4,f5,f6,f7,f8,f9",
           "datum1,datum2,datum3,datum4,datum5,datum6,datum7,datum8,datum9", "END");
# Fail Cases
$cmd = "mop delete mkey1 3 1"; $flist = "f10"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$flist\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop delete mkey1 7 2"; $flist = "f11,f12"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$flist\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop delete mkey1 -1 -1"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "mop delete mkey1 10 -1"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "mop delete mkey1 -1 10"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Success Cases
$cmd = "mop delete mkey1 2 1"; $flist = "f3"; $rst = "DELETED";
print $sock "$cmd\r\n$flist\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
mop_get_is($sock, "mkey1 26 9", 11, 9, 8, "f1,f2,f3,f4,f5,f6,f7,f8,f9", "f1,f2,f4,f5,f6,f7,f8,f9",
           "datum1,datum2,datum4,datum5,datum6,datum7,datum8,datum9", "END");
$cmd = "mop delete mkey1 5 2"; $flist = "f9,f5"; $rst = "DELETED";
print $sock "$cmd\r\n$flist\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
mop_get_is($sock, "mkey1 26 9", 11, 9, 6, "f1,f2,f3,f4,f5,f6,f7,f8,f9", "f1,f2,f4,f6,f7,f8",
           "datum1,datum2,datum4,datum6,datum7,datum8", "END");
$cmd = "mop delete mkey1 0 0 drop"; $rst = "DELETED_DROPPED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Finalize
$cmd = "get mkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


