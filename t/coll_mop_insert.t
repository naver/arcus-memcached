#!/usr/bin/perl

use strict;
use Test::More tests => 13;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get kvkey
get mkey1
mop insert mkey1 field1 6 create 11 0 0
datum9
mop insert mkey1 field2 6
datum7
mop get mkey1 13 2
field1 field2

mop insert mkey2 field1 6
datum2
set kvkey 0 0 6
datumx
mop insert kvkey field1 6
datum2
setattr mkey1 maxcount=5
mop insert mkey1 field1 6
datum0
setattr mkey1 maxcount=4000

delete kvkey
delete mkey1
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
$cmd = "get mkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Success Cases
$cmd = "mop insert mkey1 field1 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "mop insert mkey1 field2 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
mop_get_is($sock, "mkey1 13 2", 11, 2, 2, "field1 field2", "field1,field2",
           "datum9,datum7","END");
# Fail Cases
$cmd = "mop insert mkey2 field1 6"; $val = "datum2"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "mop insert kvkey field1 6"; $val = "datum2"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr mkey1 maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "mop insert mkey1 field1 6"; $val = "datum0"; $rst = "ELEMENT_EXISTS";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr mkey1 maxcount=4000"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
# Finalize
$cmd = "delete kvkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete mkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# after test
release_memcached($engine);
