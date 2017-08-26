#!/usr/bin/perl

use strict;
use Test::More tests => 14;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get mkey1
get mkey2
mop insert mkey1 f7 6 create 11 0 0
datum7
mop insert mkey1 f6 6
datum6
mop insert mkey1 f5 6
datum5
mop insert mkey1 f4 6
datum4
mop insert mkey1 f3 6
datum3
mop insert mkey1 f2 6
datum2
mop insert mkey1 f1 6
datum1
mop get mkey1 20 7
f1 f2 f3 f4 f5 f6 f7

mop get mkey1 2 1
f4
mop get meky1 11 4
f3 f7 f1 f5
mop get mkey1 11 4
f1 f2 f8 f3

delete mkey1
=cut

my $server = new_memcached();
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get mkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get mkey2"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Prepare Keys
$cmd = "mop insert mkey1 f7 6 create 11 0 0"; $val = "datum7"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f6 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f5 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f4 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f3 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f2 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "mop insert mkey1 f1 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
mop_get_is($sock, "mkey1 20 7", 11, 7, 7, "f1 f2 f3 f4 f5 f6 f7", "f1,f2,f3,f4,f5,f6,f7",
           "datum1,datum2,datum3,datum4,datum5,datum6,datum7","END");
# Success Cases
mop_get_is($sock, "mkey1 2 1", 11, 1, 1, "f4", "f4",
           "datum4","END");
mop_get_is($sock, "mkey1 11 4", 11, 4, 4, "f3 f7 f1 f5", "f3,f7,f1,f5",
           "datum3,datum7,datum1,datum5","END");
mop_get_is($sock, "mkey1 11 4", 11, 4, 3, "f1 f2 f8 f3", "f1,f2,f3",
           "datum1,datum2,datum3","END");
# Finalize
$cmd = "delete mkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
