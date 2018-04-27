#!/usr/bin/perl

use strict;
use Test::More tests => 34;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get kvkey
get bkey1
bop upsert bkey1 0x090909090909090909 6 create 11 0 0
datum9
bop upsert bkey1 0x07070707070707 6
datum7
bop upsert bkey1 0x0505050505 6
datum5
bop upsert bkey1 0x030303 0x0303 6
datum3
bop upsert bkey1 0x01 0x01 6
datum1
bop get bkey1 0x00..0xFF
bop
bop upsert bkey2 0x0202 6
datum2
set kvkey 0 0 6
datumx
bop upsert kvkey 0x0202 6
datum2
bop upsert bkey1 02 6
datum2
bop upsert bkey1 00 6
datum2
bop upsert bkey1 2 6
datum2
setattr bkey1 maxcount=5 overflowaction=error
bop upsert bkey1 0x0202 6
datum2
setattr bkey1 overflowaction=smallest_trim
bop upsert bkey1 0x00 6
datum0
bop upsert bkey1 0x01 6
datum1
setattr bkey1 maxcount=4000
bop upsert bkey1 0x020 6
datum2
bop upsert bkey1 0x0202020202020202020202020202020202020202020202020202020202020202 6
datum2
bop upsert bkey1 0x0202 0x020 6
datum2
bop upsert bkey1 0x0202 0x0202020202020202020202020202020202020202020202020202020202020202 6
datum2
bop upsert bkey1 0x0202 02 6
datum2
bop upsert bkey1 0x0202 00 6
datum2
bop upsert bkey1 0x0202 2 6
datum2
bop upsert bkey1 0x01 10
datum11111
bop upsert bkey1 0x030303 0x03 10
datum33333
bop upsert bkey1 0x0505050505 0x05 10
datum55555
bop upsert bkey1 0x07070707070707 0x07 10
datum77777
bop upsert bkey1 0x090909090909090909 0x09 10
datum99999
bop get bkey1 0x00..0xFF
delete kvkey
delete bkey1
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get kvkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Success Cases
$cmd = "bop upsert bkey1 0x090909090909090909 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x07070707070707 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x0505050505 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x030303 0x0303 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x01 0x01 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0xFF", 11, 5,
               "0x01,0x030303,0x0505050505,0x07070707070707,0x090909090909090909", "0x01,0x0303,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
# Fail Cases
$cmd = "bop upsert bkey2 0x0202 6"; $val = "datum2"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert kvkey 0x0202 6"; $val = "datum2"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 02 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 00 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 2 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr bkey1 maxcount=5 overflowaction=error"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x0202 6"; $val = "datum2"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr bkey1 overflowaction=smallest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x00 6"; $val = "datum0"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr bkey1 maxcount=4000"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x020 6"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x0202020202020202020202020202020202020202020202020202020202020202 6"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x0202 0x020 6"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x0202 0x0202020202020202020202020202020202020202020202020202020202020202 6"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x0202 02 6"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x0202 00 6"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop upsert bkey1 0x0202 2 6"; $rst = "CLIENT_ERROR bad command line format";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Success Cases
$cmd = "bop upsert bkey1 0x01 10"; $val = "datum11111"; $rst = "REPLACED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x030303 0x03 10"; $val = "datum33333"; $rst = "REPLACED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x0505050505 0x05 10"; $val = "datum55555"; $rst = "REPLACED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x07070707070707 0x07 10"; $val = "datum77777"; $rst = "REPLACED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop upsert bkey1 0x090909090909090909 0x09 10"; $val = "datum99999"; $rst = "REPLACED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0xFF", 11, 5,
               "0x01,0x030303,0x0505050505,0x07070707070707,0x090909090909090909",
               ",0x03,0x05,0x07,0x09",
               "datum11111,datum33333,datum55555,datum77777,datum99999", "TRIMMED");
# Finalize
$cmd = "delete kvkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine);
