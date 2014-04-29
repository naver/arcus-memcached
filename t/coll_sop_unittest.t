#!/usr/bin/perl

use strict;
use Test::More tests => 83007;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;

# SOP test sub routines
sub sop_insert {
    my ($key, $from, $to, $create, $flags, $exptime, $maxcount) = @_;
    my $i=$from;
    my $value = "datum$i";
    my $vleng = length($value);
    my $cmd;
    my $msg;

    $cmd = "sop insert $key $vleng $create $flags $exptime $maxcount\r\n$value\r\n";
    $msg = "sop insert $key $vleng $create $flags $exptime $maxcount : $value";
    print $sock "$cmd";
    is(scalar <$sock>, "CREATED_STORED\r\n", "$msg");

    for ($i = $from+1; $i <= $to; $i++) {
        $value = "datum$i";
        $vleng = length($value);
        $cmd = "sop insert $key $vleng\r\n$value\r\n";
        $msg = "sop insert $key $vleng : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "STORED\r\n", "$msg");
    }
}

sub sop_delete {
    my ($key, $from, $to) = @_;
    my $i;
    my $value;
    my $vleng;
    my $cmd;
    my $msg;

    for ($i = $from; $i <= $to; $i++) {
        $value = "datum$i";
        $vleng = length($value);
        $cmd = "sop delete $key $vleng\r\n$value\r\n";
        $msg = "sop delete $key $vleng : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "DELETED\r\n", "$msg");
    }
}

sub assert_sop_get {
    my ($args, $flags, $ecount, $from, $to) = @_;
    my $i;
    my $value;
    my @res_data = ();

    for ($i = $from; $i <= $to; $i++) {
        $value = "datum$i";
        push(@res_data, $value);
    }
    my $data_list = join(",", @res_data);

    sop_get_is($sock, $args, $flags, $ecount, $data_list);
}

# SOP test global variables
my $cnt;
my $cmd;
my $val;
my $rst;

# testSOPInsertGet
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
sop_insert("skey", 0, 2848, "create", 13, 0, 0);
getattr_is($sock, "skey count maxcount", "count=2849 maxcount=4000");
assert_sop_get("skey 2849", 13, 2849, 0, 2848);
assert_sop_get("skey 0",    13, 2849, 0, 2848);
$cmd = "sop exist skey 6"; $val="datum6"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 7"; $val="datum66"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 8"; $val="datum666"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 9"; $val="datum6666"; $rst = "NOT_EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testSOPInsertDeletePop
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
sop_insert("skey", 0, 9, "create", 13, 0, -1);
$cmd = "sop insert skey 6"; $val="datum3"; $rst = "ELEMENT_EXISTS";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "skey count maxcount", "count=10 maxcount=50000");
sop_get_is($sock, "skey 10",
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
$cmd = "sop delete skey 6"; $val="datum3"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 7"; $val="datum10"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_get_is($sock, "skey 10 delete",
           13, 5, "datum0,datum2,datum4,datum6,datum8");
$cmd = "sop insert skey 6"; $val="datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6 drop"; $val="datum3"; $rst = "DELETED_DROPPED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
sop_insert("skey", 0, 49999, "create", 13, 0, -1);
$cmd = "sop insert skey 10"; $val="datum12345"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "skey count maxcount", "count=50000 maxcount=50000");
assert_sop_get("skey 50000", 13, 50000, 0, 49999);
assert_sop_get("skey 0",     13, 50000, 0, 49999);
$cmd = "sop delete skey 6"; $val="datum4"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 7"; $val="datum44"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 8"; $val="datum444"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 9"; $val="datum4444"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 10"; $val="datum44444"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 11"; $val="datum444444"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 8"; $val="datum444"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "skey count maxcount", "count=49995 maxcount=50000");
$cmd = "sop insert skey 10"; $val="datum44444"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 9"; $val="datum4444"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 8"; $val="datum444"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 7"; $val="datum44"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val="datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "skey count maxcount", "count=50000 maxcount=50000");
sop_delete("skey", 30000, 39999);
sop_delete("skey", 0, 9999);
getattr_is($sock, "skey count maxcount", "count=30000 maxcount=50000");
$cmd = "sop exist skey 9"; $val="datum6666"; $rst = "NOT_EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist skey 10"; $val="datum26666"; $rst = "EXIST";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_delete("skey", 40000, 49999);
assert_sop_get("skey 20000 drop", 13, 20000, 10000, 29999);
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testEmptyCollectionOfSetType
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop create skey 13 0 -1"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "skey count maxcount", "count=5 maxcount=50000");
sop_get_is($sock, "skey 0",
           13, 5, "datum0,datum1,datum2,datum3,datum4");
$cmd = "sop delete skey 6"; $val="datum0"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6"; $val="datum2"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6"; $val="datum4"; $rst = "DELETED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_get_is($sock, "skey 0 delete",
           13, 2, "datum1,datum3");
$cmd = "sop delete skey 6"; $val="datum3"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop get skey 1"; $rst = "NOT_FOUND_ELEMENT";
getattr_is($sock, "skey count maxcount", "count=0 maxcount=50000");
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_get_is($sock, "skey 0 drop",
           13, 3, "datum0,datum1,datum2");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testSOPInsertFailCheck
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6 create 13 0 1000"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "ELEMENT_EXISTS";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
#$cmd = "sop insert skey 9"; $val = "datum_new"; $rst = "LENGTH_MISMATCH";
$cmd = "sop insert skey 9"; $val = "datum_new"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testSOPOverflowCheck
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6 create 13 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "skey count maxcount", "count=1 maxcount=1000");
$cmd = "setattr skey maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "skey count maxcount overflowaction", "count=1 maxcount=5 overflowaction=error");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_get_is($sock, "skey 5",
           13, 5, "datum1,datum2,datum3,datum4,datum5");
$cmd = "sop insert skey 6"; $val = "datum6"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr skey overflowaction=head_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr skey overflowaction=tail_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr skey overflowaction=smallest_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr skey overflowaction=largest_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr skey overflow=largest_trim"; $rst = "ATTR_ERROR not found";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testSOPNotFoundError
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop get skey 1"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop exist skey 6"; $val = "datum1"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete skey 6"; $val = "datum1"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");

# testSOPNotLISTError
$cmd = "set x 19 5 10"; $val = "some value"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey 0 6 create 17 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 1 6 create 15 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert x 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert lkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert bkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete x 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete lkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop delete bkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist x 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist lkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop exist bkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop get x 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop get lkey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop get bkey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testSOPNotKVError
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6 create 13 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "set skey 19 5 10"; $val = "some value"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "replace skey 19 5 11"; $val = "other value"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "prepend skey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "append skey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "incr skey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "decr skey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testSOPExpire
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6 create 13 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr skey expiretime=2"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
sop_get_is($sock, "skey 5",  13, 3, "datum1,datum2,datum3");
sleep(2.1);
$cmd = "sop get skey 5"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testSOPFlush
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop insert skey 6 create 13 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
sop_get_is($sock, "skey 5",  13, 3, "datum1,datum2,datum3");
$cmd = "flush_all"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "sop get skey 5"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


