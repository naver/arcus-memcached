#!/usr/bin/perl

use strict;
use Test::More tests => 21530;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;

# BOP test sub routines
sub bop_insert {
    my ($key, $from_bkey, $to_bkey, $width, $create, $flags, $exptime, $maxcount) = @_;
    my $data = "_data_";
    my $bkey;
    my $value;
    my $vleng;
    my $cmd;
    my $msg;

    $value = "$key$data$from_bkey";
    $vleng = length($value);
    if ($create eq "create") {
        $cmd = "bop insert $key $from_bkey $vleng $create $flags $exptime $maxcount\r\n$value\r\n";
        $msg = "bop insert $key $from_bkey $vleng $create $flags $exptime $maxcount : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "CREATED_STORED\r\n", "$msg");
    } else {
        $cmd = "bop insert $key $from_bkey $vleng\r\n$value\r\n";
        $msg = "bop insert $key $from_bkey $vleng : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "STORED\r\n", "$msg");
    }

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey + $width; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $value = "$key$data$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $vleng\r\n$value\r\n";
            $msg = "bop insert $key $bkey $vleng : $value";
            print $sock "$cmd";
            is(scalar <$sock>, "STORED\r\n", "$msg");
        }
    } else {
        for ($bkey = $from_bkey - $width; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $value = "$key$data$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $vleng\r\n$value\r\n";
            $msg = "bop insert $key $bkey $vleng : $value";
            print $sock "$cmd";
            is(scalar <$sock>, "STORED\r\n", "$msg");
        }
    }
}

sub assert_bop_get {
    my ($key, $width, $flags, $from_bkey, $to_bkey, $offset, $count, $delete, $tailstr) = @_;
    my $saved_from_bkey = $from_bkey;
    my $saved_to_bkey   = $to_bkey;
    my $data = "_data_";
    my $bkey;
    my $value;
    my $valcnt = 0;
    my @res_bkey = ();
    my @res_data = ();

    if ($from_bkey <= $to_bkey) {
        if (($from_bkey % $width) ne 0) {
            $from_bkey = $from_bkey + ($width - ($from_bkey % $width));
        }
        if ($offset > 0) {
            $from_bkey = $from_bkey + ($offset * $width);
        }
        if (($to_bkey % $width) ne 0) {
            $to_bkey = $to_bkey - ($to_bkey % $width);
        }
        for ($bkey = $from_bkey; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $value = "$key$data$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $value);
            $valcnt = $valcnt + 1;
            if (($count > 0) and ($valcnt >= $count)) {
                last;
            }
        }
    } else {
        if (($from_bkey % $width) ne 0) {
            $from_bkey = $from_bkey - ($from_bkey % $width)
        }
        if ($offset > 0) {
            $from_bkey = $from_bkey - ($offset * $width);
        }
        if (($to_bkey % $width) ne 0) {
            $to_bkey = $to_bkey + ($width - ($to_bkey % $width))
        }
        for ($bkey = $from_bkey; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $value = "$key$data$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $value);
            $valcnt = $valcnt + 1;
            if (($count > 0) and ($valcnt >= $count)) {
                last;
            }
        }
    }
    my $range = "$saved_from_bkey..$saved_to_bkey";
    my $bkey_list = join(",", @res_bkey);
    my $data_list = join(",", @res_data);
    my $args = "$key $range $offset $count $delete";
    bop_get_is($sock, $args, $flags, $valcnt, $bkey_list, $data_list, $tailstr);
}

# BOP test global variables
my $flags = 11;
my $default_btree_size = 4000;
my $maximum_btree_size = 50000;
my $min = 10;
my $max = 10000;
my $width = 10;
my $cnt;
my $cmd;
my $val;
my $rst;

# testBOPInsertGet
$cnt = 0;
for (0..6) {
    $cmd = "get bkey"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    if ($cnt == 0) {
        bop_insert("bkey", $min, $max, $width, "create", $flags, 0, 0);
    } elsif ($cnt == 1) {
        bop_insert("bkey", $max, $min, $width, "create", $flags, 0, 0);
    } elsif ($cnt == 2) {
        bop_insert("bkey", $min, $max, ($width*2), "create", $flags, 0, 0);
        bop_insert("bkey", ($min+$width), $max, ($width*2), "", $flags, 0, 0);
    } elsif ($cnt == 3) {
        bop_insert("bkey", $max, $min, ($width*2), "create", $flags, 0, 0);
        bop_insert("bkey", ($max-$width), $min, ($width*2), "", $flags, 0, 0);
    } elsif ($cnt == 4) {
        bop_insert("bkey", ($min+(0*$width)), $max, ($width*3), "create", $flags, 0, 0);
        bop_insert("bkey", ($min+(1*$width)), $max, ($width*3), "", $flags, 0, 0);
        bop_insert("bkey", ($min+(2*$width)), $max, ($width*3), "", $flags, 0, 0);
    } elsif ($cnt == 5) {
        bop_insert("bkey", ($max-(0*$width)), $min, ($width*3), "create", $flags, 0, 0);
        bop_insert("bkey", ($max-(1*$width)), $min, ($width*3), "", $flags, 0, 0);
        bop_insert("bkey", ($max-(2*$width)), $min, ($width*3), "", $flags, 0, 0);
    } else {
        bop_insert("bkey", $min, $max, ($width*4), "create", $flags, 0, 0);
        bop_insert("bkey", (($min+$max)-(1*$width)), $min, ($width*4), "", $flags, 0, 0);
        bop_insert("bkey", (($min)+(2*$width)), $max, ($width*4), "", $flags, 0, 0);
        bop_insert("bkey", (($min+$max)-(3*$width)), $min, ($width*4), "", $flags, 0, 0);
    }
    getattr_is($sock, "bkey count maxcount", "count=1000 maxcount=$default_btree_size");
    assert_bop_get("bkey", $width, $flags, 6500, 6500, 0, 0, "", "END");
    assert_bop_get("bkey", $width, $flags, 2300, 2300, 0, 0, "", "END");
    assert_bop_get("bkey", $width, $flags, 2000, 2255, 0, 0, "", "END");
    assert_bop_get("bkey", $width, $flags, 2000, 2255, 10, 20, "", "END");
    assert_bop_get("bkey", $width, $flags, 2000, 2255, 10, 0, "", "END");
    assert_bop_get("bkey", $width, $flags, 8700, 150, 0, 0, "", "END");
    assert_bop_get("bkey", $width, $flags, 7690, 8870, 0, 50, "", "END");
    assert_bop_get("bkey", $width, $flags, 6540, 2300, 0, 80, "", "END");
    assert_bop_get("bkey", $width, $flags, 6540, 2300, 40, 40, "", "END");
    assert_bop_get("bkey", $width, $flags, 6540, 2300, 40, 0, "", "END");
    assert_bop_get("bkey", $width, $flags, 5, 1000, 0, 20, "", "END");
    assert_bop_get("bkey", $width, $flags, 10005, 9000, 0, 30, "", "END");
    $cmd = "bop get bkey 655"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 0..5"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 20000..15000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "delete bkey"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "get bkey"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cnt = $cnt+1;
}

# testBOPInsertDeletePop
$max = 20000;
$cnt = 0;
for (0..6) {
    $cmd = "get bkey"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    if ($cnt == 0) {
        bop_insert("bkey", $min, $max, $width, "create", $flags, 0, -1);
    } elsif ($cnt == 1) {
        bop_insert("bkey", $max, $min, $width, "create", $flags, 0, -1);
    } elsif ($cnt == 2) {
        bop_insert("bkey", $min, $max, ($width*2), "create", $flags, 0, -1);
        bop_insert("bkey", ($min+$width), $max, ($width*2), "", $flags, 0, -1);
    } elsif ($cnt == 3) {
        bop_insert("bkey", $max, $min, ($width*2), "create", $flags, 0, -1);
        bop_insert("bkey", ($max-$width), $min, ($width*2), "", $flags, 0, -1);
    } elsif ($cnt == 4) {
        bop_insert("bkey", ($min+(0*$width)), $max, ($width*3), "create", $flags, 0, -1);
        bop_insert("bkey", ($min+(1*$width)), $max, ($width*3), "", $flags, 0, -1);
        bop_insert("bkey", ($min+(2*$width)), $max, ($width*3), "", $flags, 0, -1);
    } elsif ($cnt == 5) {
        bop_insert("bkey", ($max-(0*$width)), $min, ($width*3), "create", $flags, 0, -1);
        bop_insert("bkey", ($max-(1*$width)), $min, ($width*3), "", $flags, 0, -1);
        bop_insert("bkey", ($max-(2*$width)), $min, ($width*3), "", $flags, 0, -1);
    } else {
        bop_insert("bkey", $min, $max, ($width*4), "create", $flags, 0, -1);
        bop_insert("bkey", (($min+$max)-(1*$width)), $min, ($width*4), "", $flags, 0, -1);
        bop_insert("bkey", (($min)+(2*$width)), $max, ($width*4), "", $flags, 0, -1);
        bop_insert("bkey", (($min+$max)-(3*$width)), $min, ($width*4), "", $flags, 0, -1);
    }
    getattr_is($sock, "bkey count maxcount", "count=2000 maxcount=$maximum_btree_size");
    $cmd = "bop delete bkey 1000..1999"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 10000..0 100"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 15550..17000 50"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 8700..4350 50"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 0..2000 100"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 2020..2020"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 22000..2000 100"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    getattr_is($sock, "bkey count maxcount", "count=1499 maxcount=$maximum_btree_size");
    assert_bop_get("bkey", $width, $flags, 3000, 5000, 0, 100, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 13000, 11000, 50, 50, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 13000, 11000, 0, 50, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 13400, 15300, 0, 50, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 7200, 5980, 0, 50, "delete", "DELETED");
    getattr_is($sock, "bkey count maxcount", "count=1199 maxcount=$maximum_btree_size");
    assert_bop_get("bkey", $width, $flags, 5800, 6200, 0, 40, "", "END");
    assert_bop_get("bkey", $width, $flags, 5820, 610, 0, 70, "", "END");
    assert_bop_get("bkey", $width, $flags, 2100, 3200, 0, 60, "", "END");
    assert_bop_get("bkey", $width, $flags, 15000, 14000, 0, 30, "", "END");
    assert_bop_get("bkey", $width, $flags, 14200, 14400, 0, 100, "", "END");
    assert_bop_get("bkey", $width, $flags, 14200, 14900, 0, 100, "", "END");
    getattr_is($sock, "bkey count maxcount", "count=1199 maxcount=$maximum_btree_size");
    $cmd = "bop count bkey 2010"; $rst = "COUNT=1";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    bop_get_is($sock, "bkey 2010", $flags, 1, "2010", "bkey_data_2010", "END");
    $cmd = "bop count bkey 10010..9000"; $rst = "COUNT=2";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    bop_get_is($sock, "bkey 10010..9000", $flags, 2, "10010,9000", "bkey_data_10010,bkey_data_9000", "END");
    $cmd = "bop count bkey 0..900"; $rst = "COUNT=0";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 0..900"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop count bkey 20000..19100"; $rst = "COUNT=0";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 20000..19100"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 0..900"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 0..900 delete"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop delete bkey 0..100000000 drop"; $rst = "DELETED_DROPPED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "get bkey"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cnt = $cnt+1;
}

# testEmptyCollectionOfB+TreeType
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create bkey $flags 0 -1"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 40 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey count maxcount", "count=5 maxcount=$maximum_btree_size");
$cmd = "bop count bkey 0..40"; $rst = "COUNT=5";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..40", $flags, 5, "0,10,20,30,40", "datum0,datum1,datum2,datum3,datum4", "END");
$cmd = "bop delete bkey 0..20"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop count bkey 20..40"; $rst = "COUNT=2";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 20..40 delete", $flags, 2, "30,40", "datum3,datum4", "DELETED");
$cmd = "bop delete bkey 0..20"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey 0..20"; $rst = "NOT_FOUND_ELEMENT";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey count maxcount", "count=0 maxcount=$maximum_btree_size");
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count bkey 0..50"; $rst = "COUNT=3";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..50 drop", $flags, 3, "0,10,20", "datum0,datum1,datum2", "DELETED_DROPPED");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testBOPInsertFailCheck
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 10 6 create $flags 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "ELEMENT_EXISTS";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 15 9"; $val = "datum_new"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "delete bkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testBOPOverflowCheck
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey 10 6 create $flags 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey count maxcount", "count=1 maxcount=1000");
$cmd = "setattr bkey maxcount=5"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey count maxcount overflowaction",
           "count=1 maxcount=5 overflowaction=smallest_trim");
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 50 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 70 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 90 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey count maxcount", "count=5 maxcount=5");
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..1000", $flags, 5, "10,30,50,70,90","datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey 80 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 60 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..1000 0", $flags, 5, "50,60,70,80,90","datum5,datum6,datum7,datum8,datum9", "TRIMMED");
$cmd = "setattr bkey overflowaction=largest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey overflowaction", "overflowaction=largest_trim");
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 40 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 90 6"; $val = "datum1"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..1000 0 0", $flags, 5, "30,40,50,60,70","datum3,datum4,datum5,datum6,datum7", "TRIMMED");
$cmd = "setattr bkey overflowaction=error"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey overflowaction", "overflowaction=error");
$cmd = "bop insert bkey 20 6"; $val = "datum1"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 80 6"; $val = "datum1"; $rst = "OVERFLOWED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr bkey overflowaction=head_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr bkey overflowaction=tail_trim"; $rst = "ATTR_ERROR bad value";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr bkey overflow=tail_trim"; $rst = "ATTR_ERROR not found";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

#testBOPMaxBKeyRangeCheck
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey 10 6 create $flags 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey count maxcount maxbkeyrange", "count=1 maxcount=1000 maxbkeyrange=0");
$cmd = "setattr bkey maxbkeyrange=80"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey count maxcount maxbkeyrange overflowaction",
           "count=1 maxcount=1000 maxbkeyrange=80 overflowaction=smallest_trim");
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 50 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 70 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 90 6"; $val = "datum9"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
getattr_is($sock, "bkey count maxcount", "count=5 maxcount=1000");
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..1000", $flags, 5, "10,30,50,70,90","datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey 80 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_get_is($sock, "bkey 0..1000", $flags, 6, "10,30,50,70,80,90","datum1,datum3,datum5,datum7,datum8,datum9", "END");
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 100 7"; $val = "datum10"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_get_is($sock, "bkey 0..1000", $flags, 6, "30,50,70,80,90,100","datum3,datum5,datum7,datum8,datum9,datum10", "END");
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=6";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "setattr bkey overflowaction=largest_trim"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey maxbkeyrange overflowaction", "maxbkeyrange=80 overflowaction=largest_trim");
$cmd = "bop insert bkey 40 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_get_is($sock, "bkey 0..1000", $flags, 7, "30,40,50,70,80,90,100",
           "datum3,datum4,datum5,datum7,datum8,datum9,datum10", "END");
$cmd = "bop insert bkey 120 7"; $val = "datum12"; $rst = "OUT_OF_RANGE";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=7";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..1000", $flags, 7, "10,30,40,50,70,80,90",
           "datum1,datum3,datum4,datum5,datum7,datum8,datum9", "END");
$cmd = "setattr bkey maxbkeyrange=0"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey maxbkeyrange", "maxbkeyrange=0");
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 60 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 120 7"; $val = "datum12"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_get_is($sock, "bkey 0..1000", $flags, 10, "0,10,30,40,50,60,70,80,90,120",
           "datum0,datum1,datum3,datum4,datum5,datum6,datum7,datum8,datum9,datum12", "END");
$cmd = "delete bkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testBOPNotFoundError
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey 0..100"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop delete bkey 0..100"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop count bkey 0..100"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testBOPNotLISTError
$cmd = "set x 19 5 10"; $val = "some value"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "lop insert lkey 0 6 create 13 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "sop insert skey 6 create 15 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert x 10 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert lkey 10 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert skey 10 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop delete x 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop delete lkey 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop delete skey 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get x 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get lkey 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get skey 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop count x 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop count lkey 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop count skey 0..100"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete lkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete skey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get lkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get skey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testBOPNotKVError
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey 10 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "set bkey 19 5 10"; $val = "some value"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "replace bkey 19 5 $flags"; $val = "other value"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "prepend bkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "append bkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "incr bkey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "decr bkey 1"; $rst = "TYPE_MISMATCH";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testBOPExpire
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey 10 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "setattr bkey expiretime=2"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop count bkey 0..100"; $rst = "COUNT=3";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..100 0 0", $flags, 3, "10,20,30", "datum1,datum2,datum3", "END");
sleep(2.1);
$cmd = "bop get bkey 0..100"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# testBOPFlush
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop insert bkey 10 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count bkey 0..100"; $rst = "COUNT=3";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
bop_get_is($sock, "bkey 0..100",  $flags, 3, "10,20,30", "datum1,datum2,datum3", "END");
$cmd = "flush_all"; $rst = "OK";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey 0..100"; $rst = "NOT_FOUND";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
