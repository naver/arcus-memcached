#!/usr/bin/perl

use strict;
use Test::More tests => 20;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

# SOP test sub routines
sub sop_insert {
    my ($key, $data_prefix, $count, $create, $flags, $exptime, $maxcount) = @_;
    my $i=0;
    my $value = "$data_prefix$i";
    my $vleng = length($value);
    my $cmd;
    my $msg;

    $cmd = "sop insert $key $vleng $create $flags $exptime $maxcount\r\n$value\r\n";
    $msg = "sop insert $key $vleng $create $flags $exptime $maxcount : $value";
    print $sock "$cmd";
    is(scalar <$sock>, "CREATED_STORED\r\n", "$msg");

    for ($i = 1; $i < $count; $i++) {
        $value = "$data_prefix$i";
        $vleng = length($value);
        $cmd = "sop insert $key $vleng\r\n$value\r\n";
        $msg = "sop insert $key $vleng : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "STORED\r\n", "$msg");
    }
}

# SOP test global variables
my $cnt;
my $cmd;
my $val;
my $rst;

# testSOPSegFault : Basic
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
#sop_insert("skey", "datum", 10, "create", 13, 0, 0);
$cmd = "sop insert skey 6 create 13 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_get_is($sock, "skey 0",
           13, 10, "datum0,datum1,datum2,datum3,datum4,datum5,datum6,datum7,datum8,datum9");
$cmd = "sop delete skey 6"; $val="datum1"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6"; $val="datum3"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6"; $val="datum5"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6"; $val="datum7"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6"; $val="datum9"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_get_is($sock, "skey 0",
           13, 5, "datum0,datum2,datum4,datum6,datum8");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine);
