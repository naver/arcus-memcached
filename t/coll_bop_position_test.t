#!/usr/bin/perl

use strict;
use Test::More tests => 700819;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;

# BOP test sub routines
sub bop_insert {
    my ($key, $from_bkey, $to_bkey, $width, $create, $flags, $exptime, $maxcount) = @_;
    my $bkey;
    my $eflag = "";
    my $value;
    my $vleng;
    my $cmd;
    my $msg;
    my @array = ("0x0000", "0x0001", "0x0002", "0x0003", "0x0004", "0x0005", "0x0006", "0x0007", "0x0008", "0x0009");

    $value = "$key-data-$from_bkey";
    $vleng = length($value);
    $eflag = $array[$from_bkey%10];
    if ($create eq "create") {
        $cmd = "bop insert $key $from_bkey $eflag $vleng $create $flags $exptime $maxcount\r\n$value\r\n";
        $msg = "bop insert $key $from_bkey $eflag $vleng $create $flags $exptime $maxcount : $value";
        print $sock "$cmd"; is(scalar <$sock>, "CREATED_STORED\r\n", "$msg");
        $cmd = "setattr bkey maxcount=50000\r\n"; $msg = "OK";
        print $sock "$cmd"; is(scalar <$sock>, "OK\r\n", "$cmd: $msg");
    } else {
        $cmd = "bop insert $key $from_bkey $eflag $vleng\r\n$value\r\n";
        $msg = "bop insert $key $from_bkey $eflag $vleng : $value";
        print $sock "$cmd"; is(scalar <$sock>, "STORED\r\n", "$msg");
    }

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey + $width; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $eflag = $array[$bkey%10];
            $value = "$key-data-$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $eflag $vleng\r\n$value\r\n";
            $msg = "bop insert $key $bkey $eflag $vleng : $value";
            print $sock "$cmd"; is(scalar <$sock>, "STORED\r\n", "$msg");
        }
    } else {
        for ($bkey = $from_bkey - $width; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $eflag = $array[$bkey%10];
            $value = "$key-data-$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $eflag $vleng\r\n$value\r\n";
            $msg = "bop insert $key $bkey $eflag $vleng : $value";
            print $sock "$cmd"; is(scalar <$sock>, "STORED\r\n", "$msg");
        }
    }
}

sub assert_bop_get {
    my ($key, $width, $flags, $from_bkey, $to_bkey, $offset, $count, $delete, $tailstr) = @_;
    my $ecnt = 0;
    my $bkey;
    my $data;
    my @res_bkey = ();
    my @res_data = ();

    if ($from_bkey <= $to_bkey) {
        if ($offset eq 0) {
            $bkey = $from_bkey;
        } else {
            $bkey = $from_bkey + ($offset * $width);
        }
        for ( ; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
            $ecnt = $ecnt + 1;
            if (($count > 0) and ($ecnt >= $count)) {
                last;
            }
        }
    } else {
        if ($offset eq 0) {
            $bkey = $from_bkey;
        } else {
            $bkey = $from_bkey - ($offset * $width);
        }
        for ( ; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
            $ecnt = $ecnt + 1;
            if (($count > 0) and ($ecnt >= $count)) {
                last;
            }
        }
    }
    my $bkey_list = join(",", @res_bkey);
    my $data_list = join(",", @res_data);
    my $args = "$key $from_bkey..$to_bkey $offset $count $delete";
    bop_get_is($sock, $args, $flags, $ecnt, $bkey_list, $data_list, $tailstr);
}

# bop gbp (get by position)
sub assert_bop_gbp {
    my ($key, $order, $from_posi, $to_posi, $from_bkey, $to_bkey, $width, $flags, $tailstr) = @_;
    my $ecnt = 0;
    my $bkey;
    my $data;
    my @res_bkey = ();
    my @res_data = ();

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
            $ecnt = $ecnt + 1;
        }
    } else {
        for ($bkey = $from_bkey; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
            $ecnt = $ecnt + 1;
        }
    }
    my $bkey_list = join(",", @res_bkey);
    my $data_list = join(",", @res_data);
    my $args = "$key $order $from_posi..$to_posi";
    bop_gbp_is($sock, $args, $flags, $ecnt, $bkey_list, $data_list, $tailstr);
}

# BOP test global variables
my $width = 2;
my $min = 2;
my $max = 100000;
my $cnt = 0;
my $cmd;
my $val;
my $rst;

for (0..6) {
    $cmd = "get bkey"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    if ($cnt == 0) {
        bop_insert("bkey", $min, $max, $width, "create", 11, 0, -1);
    } elsif ($cnt == 1) {
        bop_insert("bkey", $max, $min, $width, "create", 11, 0, -1);
    } elsif ($cnt == 2) {
        bop_insert("bkey", $min, $max, ($width*2), "create", 11, 0, 0);
        bop_insert("bkey", ($min+$width), $max, ($width*2), "", 11, 0, 0);
    } elsif ($cnt == 3) {
        bop_insert("bkey", $max, $min, ($width*2), "create", 11, 0, 0);
        bop_insert("bkey", ($max-$width), $min, ($width*2), "", 11, 0, 0);
    } elsif ($cnt == 4) {
        bop_insert("bkey", ($min+(0*$width)), $max, ($width*3), "create", 11, 0, -1);
        bop_insert("bkey", ($min+(1*$width)), $max, ($width*3), "", 11, 0, -1);
        bop_insert("bkey", ($min+(2*$width)), $max, ($width*3), "", 11, 0, -1);
    } elsif ($cnt == 5) {
        bop_insert("bkey", ($max-(0*$width)), $min, ($width*3), "create", 11, 0, -1);
        bop_insert("bkey", ($max-(1*$width)), $min, ($width*3), "", 11, 0, -1);
        bop_insert("bkey", ($max-(2*$width)), $min, ($width*3), "", 11, 0, -1);
    } else {
        bop_insert("bkey", $min, $max, ($width*4), "create", 11, 0, -1);
        bop_insert("bkey", (($min+$max)-(1*$width)), $min, ($width*4), "", 11, 0, -1);
        bop_insert("bkey", (($min)+(2*$width)), $max, ($width*4), "", 11, 0, -1);
        bop_insert("bkey", (($min+$max)-(3*$width)), $min, ($width*4), "", 11, 0, -1);
    }
    getattr_is($sock, "bkey count maxcount", "count=50000 maxcount=50000");
    # bop get
    assert_bop_get("bkey", $width, 11, 22222, 22222, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 88888, 88888, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 20000, 20222, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 20000, 20222, 10, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 20000, 20222, 50, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 20000, 20222, 100, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 80222, 80000, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 80222, 80000, 10, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 80222, 80000, 50, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 80222, 80000, 100, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 10000, 90000, 0, 100, "", "END");
    assert_bop_get("bkey", $width, 11, 10000, 90000, 2000, 100, "", "END");
    assert_bop_get("bkey", $width, 11, 90000, 10000, 0, 100, "", "END");
    assert_bop_get("bkey", $width, 11, 90000, 10000, 2000, 100, "", "END");
    $cmd = "bop get bkey 55555"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 0..1"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 110000..100001"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    # bop position
    $cmd = "bop position bkey 2 asc"; $rst = "POSITION=0";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 2 desc"; $rst = "POSITION=49999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 100000 asc"; $rst = "POSITION=49999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 100000 desc"; $rst = "POSITION=0";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 20000 asc"; $rst = "POSITION=9999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 20000 desc"; $rst = "POSITION=40000";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 80000 asc"; $rst = "POSITION=39999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 80000 desc"; $rst = "POSITION=10000";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 33333 asc"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 77777 desc"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    # bop bgp(get bp position)
    $cmd = "bop gbp bkey asc -1"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey asc -1..-100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey asc -1..100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey desc -1"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey desc -1..-100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey desc -1..100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey asc 200000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey asc 200000..300000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey asc 300000..200000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey desc 200000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey desc 200000..300000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp bkey desc 300000..200000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    assert_bop_gbp("bkey", "asc", 0, 99, 2, 200, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 99, 0, 200, 2, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 0, 100, 100000, 99800, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 100, 0, 99800, 100000, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 10000, 10099, 20002, 20200, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 10099, 10000, 20200, 20002, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 10000, 10100, 80000, 79800, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 10100, 10000, 79800, 80000, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 25000, 25000, 50002, 50002, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 25000, 25000, 50000, 50000, 2, 11, "END");
    if ($cnt == 0 or $cnt == 2) {
        $cmd = "bop delete bkey 10002..19998"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 79998..50002"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop position bkey 10000 asc"; $rst = "POSITION=4999";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop position bkey 90000 desc"; $rst = "POSITION=5000";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_gbp("bkey", "asc", 4999, 5000, 10000, 20000, 10000, 11, "END");
        assert_bop_gbp("bkey", "desc", 10000, 10001, 80000, 50000, 30000, 11, "END");
        assert_bop_gbp("bkey", "asc", 100, 199, 202, 400, 2, 11, "END");
        assert_bop_gbp("bkey", "asc", 199, 100, 400, 202, 2, 11, "END");
        assert_bop_gbp("bkey", "desc", 0, 100, 100000, 99800, 2, 11, "END");
        assert_bop_gbp("bkey", "desc", 100, 0, 99800, 100000, 2, 11, "END");
        $cmd = "bop delete bkey 0..100000 0 EQ 0x0002,0x0004,0x0006,0x0008"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("bkey", 1, 49999, $width, "", 11, 0, -1);
        $cmd = "bop delete bkey 0..100000 0 EQ 0x0000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("bkey", 99999, 50001, $width, "", 11, 0, -1);
    } elsif ($cnt == 1 or $cnt == 3) {
        $cmd = "bop delete bkey 0..50000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop position bkey 50002 asc"; $rst = "POSITION=0";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_gbp("bkey", "asc", 5000, 5100, 60002, 60202, 2, 11, "END");
        assert_bop_gbp("bkey", "desc", 5000, 5100, 90000, 89800, 2, 11, "END");
        $cmd = "bop delete bkey 100000..0 0 NE 0x0002,0x0004"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 90000..10000 0 EQ 0x0002"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("bkey", 1, 19999, $width, "", 11, 0, -1);
        $cmd = "bop delete bkey 90000..10000 0 EQ 0x0004"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 90000..20000"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 0..100000 0 EQ 0x0002,0x0004"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("bkey", 99999, 80001, $width, "", 11, 0, -1);
        bop_insert("bkey", 40001, 49999, $width, "", 11, 0, -1);
        bop_insert("bkey", 59999, 50001, $width, "", 11, 0, -1);
        bop_insert("bkey", 20001, 39999, $width, "", 11, 0, -1);
        bop_insert("bkey", 79999, 60001, $width, "", 11, 0, -1);
    } else {
        my $small;
        my $large;
        for ($small = 0; $small < 100000; $small = $small + 10000) {
            $large = $small + int(rand(9000));
            $cmd = "bop delete bkey $small..$large"; $rst = "DELETED";
            print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        }
        $cmd = "bop delete bkey 0..50000 0 GT 0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 100000..50000 0 GT 0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop get bkey 0..100000 0 GE 0x0002"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("bkey", 99999, 50001, $width, "", 11, 0, -1);
        $cmd = "bop delete bkey 0..100000 0 EQ 0x0000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("bkey", 1, 19999, $width, "", 11, 0, -1);
        bop_insert("bkey", 49999, 20001, $width, "", 11, 0, -1);
    }
    # the second phase: get, position, gbp
    getattr_is($sock, "bkey count maxcount", "count=50000 maxcount=50000");
    $cmd = "bop delete bkey 0..100000 0 EQ 0x0000,0x0002,0x0004,0x0006,0x0008"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    # bop get
    assert_bop_get("bkey", $width, 11, 33333, 33333, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 77777, 77777, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 10001, 10223, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 10001, 10223, 10, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 10001, 10223, 50, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 10001, 10223, 100, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 10001, 10223, 100, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 90223, 90001, 0, 0, "", "END");
    assert_bop_get("bkey", $width, 11, 90223, 90001, 10, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 90223, 90001, 50, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 90223, 90001, 100, 50, "", "END");
    assert_bop_get("bkey", $width, 11, 11111, 77777, 0, 100, "", "END");
    assert_bop_get("bkey", $width, 11, 11111, 77777, 2000, 100, "", "END");
    assert_bop_get("bkey", $width, 11, 77777, 11111, 0, 100, "", "END");
    assert_bop_get("bkey", $width, 11, 77777, 11111, 2000, 100, "", "END");
    $cmd = "bop get bkey 50000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 0..0"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get bkey 100001..100000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    # bop position
    $cmd = "bop position bkey 1 asc"; $rst = "POSITION=0";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 1 desc"; $rst = "POSITION=49999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 99999 asc"; $rst = "POSITION=49999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 99999 desc"; $rst = "POSITION=0";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 20001 asc"; $rst = "POSITION=10000";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 20001 desc"; $rst = "POSITION=39999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 79999 asc"; $rst = "POSITION=39999";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 79999 desc"; $rst = "POSITION=10000";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 20000 desc"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop position bkey 80000 desc"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    # bop bgp(get by position)
    assert_bop_gbp("bkey", "asc", 0, 99, 1, 199, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 99, 0, 199, 1, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 0, 100, 99999, 99799, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 100, 0, 99799, 99999, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 10000, 10099, 20001, 20199, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 10099, 10000, 20199, 20001, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 10000, 10100, 79999, 79799, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 10100, 10000, 79799, 79999, 2, 11, "END");
    assert_bop_gbp("bkey", "asc", 25000, 25000, 50001, 50001, 2, 11, "END");
    assert_bop_gbp("bkey", "desc", 25000, 25000, 49999, 49999, 2, 11, "END");
    if ($cnt == 0 or $cnt == 1) {
        $cmd = "bop delete bkey 10001..19999"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 79999..50001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop position bkey 9999 asc"; $rst = "POSITION=4999";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop position bkey 89999 desc"; $rst = "POSITION=5000";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_gbp("bkey", "asc", 4999, 5000, 9999, 20001, 10002, 11, "END");
        assert_bop_gbp("bkey", "desc", 9999, 10000, 80001, 49999, 30002, 11, "END");
        assert_bop_gbp("bkey", "asc", 100, 199, 201, 399, 2, 11, "END");
        assert_bop_gbp("bkey", "asc", 199, 100, 399, 201, 2, 11, "END");
        assert_bop_gbp("bkey", "desc", 0, 100, 99999, 99799, 2, 11, "END");
        assert_bop_gbp("bkey", "desc", 100, 0, 99799, 99999, 2, 11, "END");
        $cmd = "bop delete bkey 0..100000 0 NE 0x0005,0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    } elsif ($cnt == 2 or $cnt == 3) {
        $cmd = "bop delete bkey 0..50000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop position bkey 50001 asc"; $rst = "POSITION=0";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_gbp("bkey", "asc", 5000, 5100, 60001, 60201, 2, 11, "END");
        assert_bop_gbp("bkey", "desc", 5000, 5100, 89999, 89799, 2, 11, "END");
        $cmd = "bop delete bkey 100000..0 0 NE 0x0001,0x0003"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 90000..10000 0 EQ 0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 90000..10000 0 EQ 0x0003"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 90000..20000"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    } else {
        my $small;
        my $large;
        for ($small = 0; $small < 100000; $small = $small + 10000) {
            $large = $small + int(rand(9000));
            $cmd = "bop delete bkey $small..$large"; $rst = "DELETED";
            print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        }
        $cmd = "bop delete bkey 0..50000 0 GT 0x0002"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete bkey 100000..50000 0 GT 0x0002"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop get bkey 0..100000 0 GE 0x0003"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    }
    $cmd = "bop delete bkey 0..100000000 drop"; $rst = "DELETED_DROPPED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "get bkey"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cnt = $cnt+1;
}
