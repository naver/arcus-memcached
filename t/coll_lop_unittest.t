#!/usr/bin/perl

use strict;
use Test::More tests => 188;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

# LOB test sub routines
sub lop_insert {
    my ($key, $data_prefix, $count, $create, $flags, $exptime, $maxcount) = @_;
    my $i=0;
    my $value = "$data_prefix$i";
    my $vleng = length($value);
    my $cmd;
    my $msg;

    $cmd = "lop insert $key $i $vleng $create $flags $exptime $maxcount\r\n$value\r\n";
    $msg = "lop insert $key $i $vleng $create $flags $exptime $maxcount : $value";
    print $sock "$cmd";
    is(scalar <$sock>, "CREATED_STORED\r\n", "$msg");

    for ($i = 1; $i < $count; $i++) {
        $value = "$data_prefix$i";
        $vleng = length($value);
        $cmd = "lop insert $key $i $vleng\r\n$value\r\n";
        $msg = "lop insert $key $i $vleng : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "STORED\r\n", "$msg");
    }
}

# LOP test global variables
my $flags = 11;
my $default_list_size = 4000;
my $maximum_list_size = 50000;
my $cmd;
my $val;
my $rst;

# testLOPInsertGet
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
lop_insert("lkey", "datum", 5, "create", 17, 0, 0);
getattr_is($sock, "lkey count maxcount", "count=5 maxcount=$default_list_size");
lop_get_is($sock, "lkey 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");
lop_get_is($sock, "lkey 0..2147483647",  17, 5, "datum0,datum1,datum2,datum3,datum4");
lop_get_is($sock, "lkey -2147483648..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");
lop_get_is($sock, "lkey 0..2",   17, 3, "datum0,datum1,datum2");
lop_get_is($sock, "lkey 2..8",   17, 3, "datum2,datum3,datum4");
lop_get_is($sock, "lkey -1..-2", 17, 2, "datum4,datum3");
lop_get_is($sock, "lkey -3..3",  17, 2, "datum2,datum3");
lop_get_is($sock, "lkey 0..-2",  17, 4, "datum0,datum1,datum2,datum3");
lop_get_is($sock, "lkey 6..-3",  17, 3, "datum4,datum3,datum2");
lop_get_is($sock, "lkey 1..1",   17, 1, "datum1");
lop_get_is($sock, "lkey 1",      17, 1, "datum1");
$cmd = "lop get lkey 6..8"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop get lkey -10..-8"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPInsertDeletePop
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
lop_insert("lkey", "datum", 10, "create", 17, 0, -1);
getattr_is($sock, "lkey count maxcount", "count=10 maxcount=$maximum_list_size");
$cmd = "lop delete lkey 8..-3"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete lkey 1..3"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete lkey 7..9"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete lkey -9..-8"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "lkey count maxcount", "count=5 maxcount=$maximum_list_size");
lop_get_is($sock, "lkey 0..-1",        17, 5, "datum0,datum4,datum5,datum6,datum9");
lop_get_is($sock, "lkey 9..-2 delete", 17, 2, "datum9,datum6");
lop_get_is($sock, "lkey 1 delete",     17, 1, "datum4");
$cmd = "lop delete lkey 4..5"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete lkey -5..-7"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "lkey count maxcount", "count=2 maxcount=$maximum_list_size");
lop_get_is($sock, "lkey 0..-1", 17, 2, "datum0,datum5");
$cmd = "lop delete lkey 0..-1 drop"; $rst = "DELETED_DROPPED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testEmptyCollectionOfListType
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop create lkey 17 0 -1"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "lkey count maxcount", "count=5 maxcount=$maximum_list_size");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum0,datum1,datum2,datum3,datum4");
$cmd = "lop delete lkey 0..2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
lop_get_is($sock, "lkey 0..-1 delete", 17, 2, "datum3,datum4");
$cmd = "lop delete lkey 0..-1"; $rst = "NOT_FOUND_ELEMENT";
$cmd = "lop get lkey 0..-1"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "lkey count maxcount", "count=0 maxcount=$maximum_list_size");
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1 drop", 17, 3, "datum0,datum1,datum2");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPInsertFailCheck
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey 0 6"; $val = "datum0"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 0 6 create 17 0 1000"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 2 6"; $val = "datum2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -3 6"; $val = "datum2"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 1 9"; $val = "datum_new"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 1 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPOverflowCheck
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey -1 6 create 17 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "lkey count maxcount", "count=1 maxcount=1000");
$cmd = "setattr lkey maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "lkey count maxcount overflowaction", "count=1 maxcount=5 overflowaction=tail_trim");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum1,datum2,datum3,datum4,datum5");
$cmd = "lop insert lkey 5 6"; $val = "datum6"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -6 6"; $val = "datum6"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 2 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 0 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 0 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum6,datum1,datum2,datum0,datum8");
$cmd = "lop insert lkey 4 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum6,datum1,datum2,datum0,datum9");
$cmd = "lop insert lkey -5 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum6,datum3,datum1,datum2,datum0");
$cmd = "setattr lkey overflowaction=head_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "lkey overflowaction", "overflowaction=head_trim");
$cmd = "lop insert lkey 2 6"; $val = "datums"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum3,datums,datum1,datum2,datum0");
$cmd = "lop insert lkey 0 6"; $val = "datumt"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datumt,datum3,datums,datum1,datum2");
$cmd = "setattr lkey overflowaction=error"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "lkey overflowaction", "overflowaction=error");
$cmd = "lop insert lkey 2 6"; $val = "datumu"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey 0 6"; $val = "datumu"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datumu"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datumt,datum3,datums,datum1,datum2");
$cmd = "setattr lkey overflowaction=smallest_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr lkey overflowaction=largest_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr lkey overflow=largest_trim"; $rst = "ATTR_ERROR not found";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPTrimCheck-1
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop create lkey 17 0 5 head_trim"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum0,datum1,datum2,datum3,datum4");
$cmd = "lop insert lkey 1 6"; $val = "datums"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datums,datum1,datum2,datum3,datum4");
$cmd = "lop insert lkey 4 6"; $val = "datumt"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum1,datum2,datum3,datumt,datum4");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPTrimCheck-2
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop create lkey 17 0 5 tail_trim"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum0,datum1,datum2,datum3,datum4");
$cmd = "lop insert lkey -2 6"; $val = "datums"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum0,datum1,datum2,datum3,datums");
$cmd = "lop insert lkey -5 6"; $val = "datumt"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1", 17, 5, "datum0,datumt,datum1,datum2,datum3");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPNotFoundError
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop get lkey 0"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete lkey 0"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPNotLISTError
$cmd = "set x 19 5 10"; $val = "some value"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6 create 13 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 1 6 create 15 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert x -1 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert skey -1 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert bkey -1 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop get x 0..-1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop get skey 0..-1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop get bkey 0..-1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete x 0..-1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete skey 0..-1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop delete bkey 0..-1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPNotKVError
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey -1 6 create 17 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "set lkey 19 5 10"; $val = "some value"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "replace lkey 19 5 $flags"; $val = "other value"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "prepend lkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "append lkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "incr lkey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "decr lkey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPExpire
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey -1 6 create 17 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr lkey expiretime=2"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
lop_get_is($sock, "lkey 0..-1",  17, 3, "datum1,datum2,datum3");
sleep(2.1);
$cmd = "lop get lkey 0..-1"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testLOPFlush
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey -1 6 create 17 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
lop_get_is($sock, "lkey 0..-1",  17, 3, "datum1,datum2,datum3");
$cmd = "flush_all"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop get lkey 0..-1"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");



# after test
release_memcached($engine, $server);
