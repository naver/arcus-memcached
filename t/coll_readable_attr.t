#!/usr/bin/perl

use strict;
use Test::More tests => 28;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
bop create bkey1 11 0 0 unreadable
bop insert bkey1 90 6
datum9
bop insert bkey1 70 6
datum7
bop insert bkey1 50 6
datum5
bop insert bkey1 30 6
datum3
bop insert bkey1 10 6
datum1
bop get bkey1 10..90
getattr bkey1 readable
setattr bkey1 readable=on
getattr bkey1 readable
setattr bkey1 readable=off
bop get bkey1 10..90
delete bkey1

bop create bkey1 11 0 0 unreadable
bop insert bkey1 0x0fff 6
datum9
bop insert bkey1 0x0FFA 6
datum7
bop insert bkey1 0x00FF 6
datum5
bop insert bkey1 0x000F 6
datum3
bop insert bkey1 0x0000 6
datum1
bop get bkey1 0x00..0xFF
getattr bkey1 readable
setattr bkey1 readable=on
getattr bkey1 readable
setattr bkey1 readable=off
bop get bkey1 0x00..0xFF
delete bkey1
=cut

my $server = new_memcached();
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Case 1
$cmd = "get bkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create bkey1 11 0 0 unreadable"; $rst = "CREATED";
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
$cmd = "bop get bkey1 10..90"; $rst = "UNREADABLE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 readable", "readable=off");
$cmd = "setattr bkey1 readable=on"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 readable", "readable=on");
$cmd = "setattr bkey1 readable=off"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_ext_get_is($sock, "bkey1 10..90",
               11, 5, "10,30,50,70,90", ",,,,", "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# Case 2
$cmd = "get bkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create bkey1 11 0 0 unreadable"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey1 0x0fff 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0FFA 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x00FF 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x000F 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0000 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop get bkey1 0x00..0xFF"; $rst = "UNREADABLE";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 readable", "readable=off");
$cmd = "setattr bkey1 readable=on"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey1 readable", "readable=on");
$cmd = "setattr bkey1 readable=off"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0xFF",
               11, 5, "0x0000,0x000F,0x00FF,0x0FFA,0x0FFF", ",,,,", "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

