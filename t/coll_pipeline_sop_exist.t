#!/usr/bin/perl

use strict;
use Test::More tests => 37;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
sop insert skey 6 create 13 60 1000
datum1
sop insert skey 6
datum2
sop insert skey 6
datum3
sop insert skey 6
datum4
sop insert skey 6
datum5
sop exist skey 6
datum0
sop exist skey 6
datum1
sop exist skey 6
datum2
sop exist skey 6
datum3
sop exist skey 6
datum4
sop exist skey 6
datum5
sop exist skey 6
datum6
sop exist skey 6
datum7
sop exist skey 6
datum8
sop exist skey 6
datum9
sop exist skey 6 pipe
datum0
sop exist skey 6 pipe
datum1
sop exist skey 6 pipe
datum2
sop exist skey 6 pipe
datum3
sop exist skey 6 pipe
datum4
sop exist skey 6 pipe
datum5
sop exist skey 6 pipe
datum6
sop exist skey 6 pipe
datum7
sop exist skey 6 pipe
datum8
sop exist skey 6
datum9
sop delete skey 6 pipe
datum1
sop delete skey 6 pipe
datum2
sop delete skey 6 pipe
datum3
sop delete skey 6 pipe
datum4
sop delete skey 6 pipe
datum5
sop delete skey 6
datum6
delete skey
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6 create 13 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum0"; $rst = "NOT_EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum1"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum2"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum3"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum4"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum5"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum6"; $rst = "NOT_EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum7"; $rst = "NOT_EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum8"; $rst = "NOT_EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 6"; $val = "datum9"; $rst = "NOT_EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
print $sock "sop exist skey 6 pipe\r\ndatum0\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum1\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum2\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum3\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum4\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum5\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum6\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum7\r\n";
print $sock "sop exist skey 6 pipe\r\ndatum8\r\n";
print $sock "sop exist skey 6\r\ndatum9\r\n";
is(scalar <$sock>, "RESPONSE  10\r\n", "pipeline head: 10");
is(scalar <$sock>, "NOT_EXIST\r\n",    "NOT_EXIST");
is(scalar <$sock>, "EXIST\r\n",        "EXIST");
is(scalar <$sock>, "EXIST\r\n",        "EXIST");
is(scalar <$sock>, "EXIST\r\n",        "EXIST");
is(scalar <$sock>, "EXIST\r\n",        "EXIST");
is(scalar <$sock>, "EXIST\r\n",        "EXIST");
is(scalar <$sock>, "NOT_EXIST\r\n",    "NOT_EXIST");
is(scalar <$sock>, "NOT_EXIST\r\n",    "NOT_EXIST");
is(scalar <$sock>, "NOT_EXIST\r\n",    "NOT_EXIST");
is(scalar <$sock>, "NOT_EXIST\r\n",    "NOT_EXIST");
is(scalar <$sock>, "END\r\n",          "pipeline tail: END");
print $sock "sop delete skey 6 pipe\r\ndatum1\r\n";
print $sock "sop delete skey 6 pipe\r\ndatum2\r\n";
print $sock "sop delete skey 6 pipe\r\ndatum3\r\n";
print $sock "sop delete skey 6 pipe\r\ndatum4\r\n";
print $sock "sop delete skey 6 pipe\r\ndatum5\r\n";
print $sock "sop delete skey 6\r\ndatum6\r\n";
is(scalar <$sock>, "RESPONSE   6\r\n",      "pipeline head: 6");
is(scalar <$sock>, "DELETED\r\n",           "DELETED");
is(scalar <$sock>, "DELETED\r\n",           "DELETED");
is(scalar <$sock>, "DELETED\r\n",           "DELETED");
is(scalar <$sock>, "DELETED\r\n",           "DELETED");
is(scalar <$sock>, "DELETED\r\n",           "DELETED");
is(scalar <$sock>, "NOT_FOUND_ELEMENT\r\n", "NOT_FOUND_ELEMENT");
is(scalar <$sock>, "END\r\n",               "pipeline tail: END");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine);
