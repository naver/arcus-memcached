#!/usr/bin/perl

use strict;
use Test::More tests => 62;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1

bop insert bkey1 0x0010 1 create 11 0 0
1
bop insert bkey1 0x0020 1
2
bop insert bkey1 0x0030 1
3
bop insert bkey1 0x0040 1
4
bop insert bkey1 0x0050 1
a
bop count bkey1 0x00..0x1000

bop incr bkey1 0x0010 1
bop get bkey1 0x0010
bop incr bkey1 0x0020 25
bop get bkey1 0x0020
bop decr bkey1 0x0020 20
bop get bkey1 0x0020
bop decr bkey1 0x0030 2
bop get bkey1 0x0030
bop decr bkey1 0x0040 10
bop get bkey1 0x0040
bop incr bkey1 0x0050 10
bop get bkey1 0x0050

bop incr bkey1 0x0060 10
bop incr bkey1 1 10
bop incr bkey2 0 10

delete bkey1
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

# Prepare Keys
$cmd = "bop insert bkey1 0x0010 1 create 11 0 0"; $val = "1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0020 1"; $val = "2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0030 1"; $val = "3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0040 1"; $val = "4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0050 1"; $val = "a"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");

$cmd = "bop count bkey1 0x0010..0x0050"; $rst = "COUNT=5";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

print $sock "bop incr bkey1 0x0010 1\r\n";
is(scalar <$sock>, "2\r\n", "+ 1 = 2");
bop_ext_get_is($sock, "bkey1 0x0010", 11, 1, "0x0010", "", "2", "END");

print $sock "bop incr bkey1 0x0020 25\r\n";
is(scalar <$sock>, "27\r\n", "+ 25 = 27");
bop_ext_get_is($sock, "bkey1 0x0020", 11, 1, "0x0020", "", "27", "END");

print $sock "bop decr bkey1 0x0020 20\r\n";
is(scalar <$sock>, "7\r\n", "- 20 = 7");
bop_ext_get_is($sock, "bkey1 0x0020", 11, 1, "0x0020", "", "7", "END");

print $sock "bop decr bkey1 0x0030 2\r\n";
is(scalar <$sock>, "1\r\n", "- 2 = 1");
bop_ext_get_is($sock, "bkey1 0x0030", 11, 1, "0x0030", "", "1", "END");

print $sock "bop decr bkey1 0x0040 10\r\n";
is(scalar <$sock>, "0\r\n", "- 10 = 0");
bop_ext_get_is($sock, "bkey1 0x0040", 11, 1, "0x0040", "", "0", "END");

print $sock "bop incr bkey1 0x0050 10\r\n";
is(scalar <$sock>,
   "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
   "a + 10 = 0");

bop_ext_get_is($sock, "bkey1 0x0050", 11, 1, "0x0050", "", "a", "END");

print $sock "bop incr bkey1 0x0060 10\r\n";
$rst = "NOT_FOUND_ELEMENT";
is(scalar <$sock>, "$rst\r\n", "$rst");

print $sock "bop incr bkey1 0x0060 10 6\r\n";
is(scalar <$sock>, "6\r\n", "initail = 6");
bop_ext_get_is($sock, "bkey1 0x0060", 11, 1, "0x0060", "", "6", "END");

print $sock "bop incr bkey1 0x0060 10 6\r\n";
is(scalar <$sock>, "16\r\n", "+ 10 = 16");
bop_ext_get_is($sock, "bkey1 0x0060", 11, 1, "0x0060", "", "16", "END");

print $sock "bop decr bkey1 0x0070 10 7 0xFF\r\n";
is(scalar <$sock>, "7\r\n", "initial = 7 0xFF");
bop_ext_get_is($sock, "bkey1 0x0070", 11, 1, "0x0070", "0xFF", "7", "END");

print $sock "bop decr bkey1 0x0070 5 7 0xFF\r\n";
is(scalar <$sock>, "2\r\n", "- 5 = 2");
bop_ext_get_is($sock, "bkey1 0x0070", 11, 1, "0x0070", "0xFF", "2", "END");

print $sock "bop decr bkey1 0x0070 10 7 0xFF\r\n";
is(scalar <$sock>, "0\r\n", "- 10 = 0");
bop_ext_get_is($sock, "bkey1 0x0070", 11, 1, "0x0070", "0xFF", "0", "END");

print $sock "bop incr bkey1 0x0080 0 987654321 0xF0F0\r\n";
is(scalar <$sock>, "CLIENT_ERROR bad command line format\r\n", "delta = 0");

print $sock "bop incr bkey1 0x0080 1 987654321 0xF0F0\r\n";
is(scalar <$sock>, "987654321\r\n", "initial = 987654321 0xF0F0");
bop_ext_get_is($sock, "bkey1 0x0080", 11, 1, "0x0080", "0xF0F0", "987654321", "END");

print $sock "bop decr bkey1 0x0080 1\r\n";
is(scalar <$sock>, "987654320\r\n", "- 1 = 987654320");
bop_ext_get_is($sock, "bkey1 0x0080", 11, 1, "0x0080", "0xF0F0", "987654320", "END");

print $sock "bop decr bkey1 0x0080 900000000\r\n";
is(scalar <$sock>, "87654320\r\n", "- 900000000 = 87654320");
bop_ext_get_is($sock, "bkey1 0x0080", 11, 1, "0x0080", "0xF0F0", "87654320", "END");

print $sock "bop decr bkey1 0x0080 80000000 10\r\n";
is(scalar <$sock>, "7654320\r\n", "- 80000000 = 7654320");
bop_ext_get_is($sock, "bkey1 0x0080", 11, 1, "0x0080", "0xF0F0", "7654320", "END");

print $sock "bop decr bkey1 0x0080 7654000 10 0xFFFF\r\n";
is(scalar <$sock>, "320\r\n", "- 7654000 = 320");
bop_ext_get_is($sock, "bkey1 0x0080", 11, 1, "0x0080", "0xF0F0", "320", "END");

print $sock "bop incr bkey1 0x0080 680 10 0xFFFF\r\n";
is(scalar <$sock>, "1000\r\n", "+ 680 = 1000");
bop_ext_get_is($sock, "bkey1 0x0080", 11, 1, "0x0080", "0xF0F0", "1000", "END");

print $sock "bop incr bkey1 1 10\r\n";
$rst = "BKEY_MISMATCH";
is(scalar <$sock>, "$rst\r\n", "$rst");

print $sock "bop incr bkey2 0 10\r\n";
$rst = "NOT_FOUND";
is(scalar <$sock>, "$rst\r\n", "$rst");

# additional tests
$cmd = "bop insert bkey1 0x0100 0"; $val = ""; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0110 2"; $val = "-1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0120 20"; $val = "18446744073709551615"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0130 20"; $val = "18446744073709551614"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0140 20"; $val = "18446744073709551616"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");

print $sock "bop incr bkey1 0x0100 1\r\n";
is(scalar <$sock>,
   "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
   "incr1 on empty");
print $sock "bop decr bkey1 0x0100 1\r\n";
is(scalar <$sock>,
   "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
   "decr 1 on empty");

print $sock "bop incr bkey1 0x0110 1\r\n";
is(scalar <$sock>,
   "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
   "incr 1 on -1");
print $sock "bop decr bkey1 0x0110 1\r\n";
is(scalar <$sock>,
   "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
   "incr 1 on -1");

print $sock "bop incr bkey1 0x0120 1\r\n";
is(scalar <$sock>, "0\r\n", "incr 1 on 18446744073709551615");
print $sock "bop decr bkey1 0x0120 1\r\n";
is(scalar <$sock>, "0\r\n", "decr 1 on 0");

print $sock "bop incr bkey1 0x0130 1\r\n";
is(scalar <$sock>, "18446744073709551615\r\n", "incr 1 on 18446744073709551614");
print $sock "bop decr bkey1 0x0130 1\r\n";
is(scalar <$sock>, "18446744073709551614\r\n", "decr 1 on 18446744073709551615");
print $sock "bop decr bkey1 0x0130 1\r\n";
is(scalar <$sock>, "18446744073709551613\r\n", "decr 1 on 18446744073709551614");

print $sock "bop incr bkey1 0x0140 1\r\n";
is(scalar <$sock>,
   "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
   "incr 1 on 18446744073709551616");
print $sock "bop decr bkey1 0x0140 1\r\n";
is(scalar <$sock>,
   "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
   "decr 1 on 18446744073709551616");

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine, $server);
