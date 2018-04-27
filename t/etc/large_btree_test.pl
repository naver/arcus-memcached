#!/usr/bin/perl

use strict;
use Test::More tests => 1751209;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use MemcachedTest;

# set environment variable
$ENV{'ARCUS_MAX_BTREE_SIZE'}='1000000';

my $max_btree_size = 200000;
#my $max_btree_size = 1000000;
my $server = get_memcached($engine, "-v -m 128 -X .libs/syslog_logger.so");
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
        $cmd = "setattr $key maxcount=$max_btree_size\r\n"; $msg = "OK";
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

# bop position
sub assert_bop_pos {
    my ($key, $bkey, $order, $minbk, $maxbk, $width) = @_;
    my $posi;
    my $exec;
    my $resp;

    $exec = "bop position $key $bkey $order";
    if (($bkey < $minbk) or ($bkey > $maxbk) or ((($bkey - $minbk) % $width) ne 0)) {
        $resp = "NOT_FOUND_ELEMENT";
    } else {
        if ($order eq "asc") {
            $posi = ($bkey - $minbk) / $width;
        } else {
            $posi = ($maxbk - $bkey) / $width;
        }
        $resp = "POSITION=$posi";
    }
    print $sock "$exec\r\n";
    is(scalar <$sock>, "$resp\r\n", "$exec: $resp");
}

# bop gbp (get by position)
sub assert_bop_gbp {
    my ($key, $order, $from_posi, $to_posi, $flags, $from_bkey, $to_bkey, $width) = @_;
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
    my $reshead = "$flags $ecnt";
    bop_gbp_is($sock, $args, $reshead, $bkey_list, $data_list);
}

# bop pwg (position with get)
sub assert_bop_pwg {
    my ($key, $find_bkey, $order, $count, $flags, $minbk, $maxbk, $width) = @_;
    my $posi;
    my $ridx;
    my $ecnt;
    my $mcnt;
    my $bkey;
    my $data;
    my $from_bkey;
    my $to_bkey;
    my @res_bkey = ();
    my @res_data = ();

    if ($order eq "asc") {
        $posi = ($find_bkey - $minbk) / $width;
    } else {
        $posi = ($maxbk - $find_bkey) / $width;
    }
    if ($count == 0) {
        $ridx = 0;
        $ecnt = 1;
    } else {
        if ($posi >= $count) {
            $ridx = $count;
        } else {
            $ridx = $posi;
        }
        $mcnt = (($maxbk - $minbk) / $width) + 1;
        if ($posi <= ($mcnt - $count - 1)) {
            $ecnt = $ridx + 1 + $count;
        } else {
            $ecnt = $ridx + 1 + ($mcnt - $posi - 1);
        }
    }

    if ($order eq "asc") {
        $from_bkey = ($find_bkey - ($ridx * $width));
        $to_bkey   = ($find_bkey + (($ecnt - $ridx - 1) * $width));
        for ($bkey = $from_bkey; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
        }
    } else {
        $from_bkey = ($find_bkey + ($ridx * $width));
        $to_bkey   = ($find_bkey - (($ecnt - $ridx - 1) * $width));
        for ($bkey = $from_bkey; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
        }
    }
    my $bkey_list = join(",", @res_bkey);
    my $data_list = join(",", @res_data);
    my $args = "$key $find_bkey $order $count";
    my $reshead = "$posi $flags $ecnt $ridx";
    bop_pwg_is($sock, $args, $reshead, $bkey_list, $data_list);
}

# BOP test global variables
my $flags = 11;
my $width = 2;
my $min;
my $max;
my $cnt = 0;
my $cmd;
my $val;
my $rst;
my $key;

my $order;
my $bkey_A;
my $bkey_B;
# Used in pwg operation.
my $ecount;
my $rindex;
my $rcount;

for (0..6) {
    $key = "joon:key$cnt";
    $cmd = "get $key"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

    # set min bkey & max bkey
    $min = 2;
    $max = $max_btree_size * $width;

    if ($cnt == 0) {
        bop_insert("$key", $min, $max, $width, "create", 11, 0, -1);
    } elsif ($cnt == 1) {
        bop_insert("$key", $max, $min, $width, "create", 11, 0, -1);
    } elsif ($cnt == 2) {
        bop_insert("$key", $min, $max, ($width*2), "create", 11, 0, 0);
        bop_insert("$key", ($min+$width), $max, ($width*2), "", 11, 0, 0);
    } elsif ($cnt == 3) {
        bop_insert("$key", $max, $min, ($width*2), "create", 11, 0, 0);
        bop_insert("$key", ($max-$width), $min, ($width*2), "", 11, 0, 0);
    } elsif ($cnt == 4) {
        bop_insert("$key", ($min+(0*$width)), $max, ($width*3), "create", 11, 0, -1);
        bop_insert("$key", ($min+(1*$width)), $max, ($width*3), "", 11, 0, -1);
        bop_insert("$key", ($min+(2*$width)), $max, ($width*3), "", 11, 0, -1);
    } elsif ($cnt == 5) {
        bop_insert("$key", ($max-(0*$width)), $min, ($width*3), "create", 11, 0, -1);
        bop_insert("$key", ($max-(1*$width)), $min, ($width*3), "", 11, 0, -1);
        bop_insert("$key", ($max-(2*$width)), $min, ($width*3), "", 11, 0, -1);
    } else {
        bop_insert("$key", $min, $max, ($width*4), "create", 11, 0, -1);
        bop_insert("$key", (($min+$max)-(1*$width)), $min, ($width*4), "", 11, 0, -1);
        bop_insert("$key", (($min)+(2*$width)), $max, ($width*4), "", 11, 0, -1);
        bop_insert("$key", (($min+$max)-(3*$width)), $min, ($width*4), "", 11, 0, -1);
    }
    getattr_is($sock, "$key count maxcount", "count=$max_btree_size maxcount=$max_btree_size");
    # bop get
    assert_bop_get("$key", $width, 11,  22222,  22222,    0,   0, "", "END");
    assert_bop_get("$key", $width, 11,  88888,  88888,    0,   0, "", "END");
    assert_bop_get("$key", $width, 11,  20000,  20222,    0,   0, "", "END");
    assert_bop_get("$key", $width, 11,  20000,  20222,   10,  50, "", "END");
    assert_bop_get("$key", $width, 11,  20000,  20222,   50,   0, "", "END");
    assert_bop_get("$key", $width, 11,  20000,  20222,  100,   0, "", "END");
    assert_bop_get("$key", $width, 11, 180222, 180000,    0,   0, "", "END");
    assert_bop_get("$key", $width, 11, 180222, 180000,   10,  50, "", "END");
    assert_bop_get("$key", $width, 11, 180222, 180000,   50,  50, "", "END");
    assert_bop_get("$key", $width, 11, 180222, 180000,  100,  50, "", "END");
    assert_bop_get("$key", $width, 11,  10000, 190000,    0, 100, "", "END");
    assert_bop_get("$key", $width, 11,  10000, 190000, 2000, 100, "", "END");
    assert_bop_get("$key", $width, 11, 190000,  10000,    0, 100, "", "END");
    assert_bop_get("$key", $width, 11, 190000,  10000, 2000, 100, "", "END");
    $cmd = "bop get $key 55555"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get $key 0..1"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get $key 2010000..2000001"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

    # bop position
    assert_bop_pos($key, $min, "asc",  $min, $max, $width);
    assert_bop_pos($key, $min, "desc", $min, $max, $width);
    assert_bop_pos($key, $max, "asc",  $min, $max, $width);
    assert_bop_pos($key, $max, "desc", $min, $max, $width);
    assert_bop_pos($key, 2000, "asc",  $min, $max, $width);
    assert_bop_pos($key, 2000, "desc", $min, $max, $width);
    assert_bop_pos($key, $max-2000, "asc",  $min, $max, $width);
    assert_bop_pos($key, $max-2000, "desc", $min, $max, $width);
    assert_bop_pos($key, 3333, "asc",  $min, $max, $width);
    assert_bop_pos($key, 3333, "desc", $min, $max, $width);
    assert_bop_pos($key, $max-3333, "asc",  $min, $max, $width);
    assert_bop_pos($key, $max-3333, "desc", $min, $max, $width);

    # bop gbp (get by position)
    $cmd = "bop gbp $key asc -1"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key asc -1..-100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key asc -1..100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key desc -1"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key desc -1..-100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key desc -1..100"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key asc 2400000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key asc 2400000..2500000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key asc 2500000..2400000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key desc 2400000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key desc 2400000..2500000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop gbp $key desc 2500000..2400000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    assert_bop_gbp("$key", "asc", 0, 99, $flags,
                   $min, ($min + (99 * $width)), $width);
    assert_bop_gbp("$key", "asc", 99, 0, $flags,
                   ($min + (99 * $width)), $min, $width);
    assert_bop_gbp("$key", "desc", 0, 99, $flags,
                   $max, ($max - (99 * $width)), $width);
    assert_bop_gbp("$key", "desc", 99, 0, $flags,
                   ($max - (99 * $width)), $max, $width);
    assert_bop_gbp("$key", "asc", 10000, 10099, $flags,
                   ($min + (10000 * $width)), ($min + (10099 * $width)), $width);
    assert_bop_gbp("$key", "asc", 10099, 10000, $flags,
                   ($min + (10099 * $width)), ($min + (10000 * $width)), $width);
    assert_bop_gbp("$key", "desc", 10000, 10099, $flags,
                   ($max - (10000 * $width)), ($max - (10099 * $width)), $width);
    assert_bop_gbp("$key", "desc", 10099, 10000, $flags,
                   ($max - (10099 * $width)), ($max - (10000 * $width)), $width);
    assert_bop_gbp("$key", "asc", 25000, 25000, $flags,
                   ($min + (25000 * $width)), ($min + (25000 * $width)), $width);
    assert_bop_gbp("$key", "desc", 25000, 25000, $flags,
                   ($max - (25000 * $width)), ($max - (25000 * $width)), $width);

    # bop pwg(position with get)
    $cmd = "bop pwg $key 10 asc -1"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop pwg $key 10 both"; $rst = "CLIENT_ERROR bad command line format";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop pwg $key 33333 asc"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop pwg $key 77777 desc"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop pwg $key 33333 asc 10"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop pwg $key 77777 desc 10"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    assert_bop_pwg("$key", $min,   "asc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $min,  "desc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $max,   "asc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $max,  "desc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 44444,  "asc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 44444, "desc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $min,   "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $min,  "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $max,   "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $max,  "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 44444,  "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 44444, "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $min+(5*$width),   "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $min+(5*$width),  "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $max-(5*$width),   "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", $max-(5*$width),  "desc", 10, $flags, $min, $max, $width);

    if ($cnt == 0 or $cnt == 2) {
        $cmd = "bop delete $key 10001..19999"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_pos($key, 10000, "asc",  $min, $max, $width);
        assert_bop_gbp("$key", "asc", 4999, 5000, $flags, 10000, 20000, 10000);
        $bkey_A = $max-20001;
        $bkey_B = $max-49999;
        $cmd = "bop delete $key $bkey_A..$bkey_B"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_pos($key, $max-10000, "desc", $min, $max, $width);
        assert_bop_gbp("$key", "desc", 10000, 10001, $flags, $max-20000, $max-50000, 30000);

        # bop get by position
        assert_bop_gbp("$key",  "asc",   100,   199, $flags,
                       ($min + (100 * $width)), ($min + (199 * $width)), $width);
        assert_bop_gbp("$key",  "asc",   199,   100, $flags,
                       ($min + (199 * $width)), ($min + (100 * $width)), $width);
        assert_bop_gbp("$key", "desc",     0,   100, $flags,
                       $max, ($max - (100 * $width)), $width);
        assert_bop_gbp("$key", "desc",   100,     0, $flags,
                       ($max - (100 * $width)), $max, $width);
        $cmd = "bop delete $key 0..$max 0 EQ 0x0002,0x0004,0x0006,0x0008"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("$key", 1, 49999, $width, "", 11, 0, -1);
        $cmd = "bop delete $key 0..$max 0 EQ 0x0000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("$key", 99999, 50001, $width, "", 11, 0, -1);
    } elsif ($cnt == 1 or $cnt == 3) {
        $cmd = "bop delete $key 0..50000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        # bop position (new min bkey: 50002)
        assert_bop_pos($key, 50122, "asc",  50002, $max, $width);
        assert_bop_pos($key, 50122, "desc", 50002, $max, $width);
        # bop get by position (new min bkey: 50002)
        assert_bop_gbp("$key",  "asc", 5000, 5100, $flags,
                       (50002 + (5000 * $width)), (50002 + (5100 * $width)), $width);
        assert_bop_gbp("$key", "desc", 5000, 5100, $flags,
                       ($max - (5000 * $width)), ($max - (5100 * $width)), $width);
        $cmd = "bop delete $key 100001..$max"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 100000..0 0 NE 0x0002,0x0004"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 90000..10000 0 EQ 0x0002"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("$key", 1, 19999, $width, "", 11, 0, -1);
        $cmd = "bop delete $key 90000..10000 0 EQ 0x0004"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 90000..20000"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 0..100000 0 EQ 0x0002,0x0004"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("$key", 99999, 80001, $width, "", 11, 0, -1);
        bop_insert("$key", 40001, 49999, $width, "", 11, 0, -1);
        bop_insert("$key", 59999, 50001, $width, "", 11, 0, -1);
        bop_insert("$key", 20001, 39999, $width, "", 11, 0, -1);
        bop_insert("$key", 79999, 60001, $width, "", 11, 0, -1);
    } else {
        my $small;
        my $large;
        for ($small = 0; $small < $max; $small = $small + 10000) {
            $large = $small + int(rand(9000));
            $cmd = "bop delete $key $small..$large"; $rst = "DELETED";
            print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        }
        $cmd = "bop delete $key 0..100000 0 GT 0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key $max..100000 0 GT 0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop get $key 0..$max 0 GE 0x0002"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("$key", 99999, 50001, $width, "", 11, 0, -1);
        $cmd = "bop delete $key 0..$max 0 EQ 0x0000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        bop_insert("$key", 1, 19999, $width, "", 11, 0, -1);
        bop_insert("$key", 49999, 20001, $width, "", 11, 0, -1);
    }

    # the second phase: get, position, gbp
    getattr_is($sock, "$key count maxcount", "count=50000 maxcount=$max_btree_size");
    $cmd = "bop delete $key 0..100000 0 EQ 0x0000,0x0002,0x0004,0x0006,0x0008"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

    # reset $min & $max
    $min = 1;
    $max = $min + (49999 * $width);

    # bop get
    assert_bop_get("$key", $width, 11, 33333, 33333,    0,    0, "", "END");
    assert_bop_get("$key", $width, 11, 77777, 77777,    0,    0, "", "END");
    assert_bop_get("$key", $width, 11, 10001, 10223,    0,    0, "", "END");
    assert_bop_get("$key", $width, 11, 10001, 10223,   10,   50, "", "END");
    assert_bop_get("$key", $width, 11, 10001, 10223,   50,   50, "", "END");
    assert_bop_get("$key", $width, 11, 10001, 10223,  100,   50, "", "END");
    assert_bop_get("$key", $width, 11, 10001, 10223,  100,   50, "", "END");
    assert_bop_get("$key", $width, 11, 90223, 90001,    0,    0, "", "END");
    assert_bop_get("$key", $width, 11, 90223, 90001,   10,   50, "", "END");
    assert_bop_get("$key", $width, 11, 90223, 90001,   50,   50, "", "END");
    assert_bop_get("$key", $width, 11, 90223, 90001,  100,   50, "", "END");
    assert_bop_get("$key", $width, 11, 11111, 77777,    0,  100, "", "END");
    assert_bop_get("$key", $width, 11, 11111, 77777, 2000,  100, "", "END");
    assert_bop_get("$key", $width, 11, 77777, 11111,    0,  100, "", "END");
    assert_bop_get("$key", $width, 11, 77777, 11111, 2000,  100, "", "END");

    $cmd = "bop get $key 50000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get $key 0..0"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "bop get $key 100001..100000"; $rst = "NOT_FOUND_ELEMENT";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

    # bop position
    assert_bop_pos($key, $min,  "asc", $min, $max, $width);
    assert_bop_pos($key, $min, "desc", $min, $max, $width);
    assert_bop_pos($key, 99999,  "asc", $min, $max, $width);
    assert_bop_pos($key, 99999, "desc", $min, $max, $width);
    assert_bop_pos($key, 20001,  "asc", $min, $max, $width);
    assert_bop_pos($key, 20001, "desc", $min, $max, $width);
    assert_bop_pos($key, 56789,  "asc", $min, $max, $width);
    assert_bop_pos($key, 56789, "desc", $min, $max, $width);
    assert_bop_pos($key, 20000,  "asc", $min, $max, $width);
    assert_bop_pos($key, 20000, "desc", $min, $max, $width);
    assert_bop_pos($key, 80000,  "asc", $min, $max, $width);
    assert_bop_pos($key, 80000, "desc", $min, $max, $width);

    # bop bgp(get by position)
    assert_bop_gbp("$key", "asc",      0,    99, $flags,
                   $min, ($min + (99 * $width)), $width);
    assert_bop_gbp("$key", "asc",     99,     0, $flags,
                   ($min + (99 * $width)), $min, $width);
    assert_bop_gbp("$key", "desc",     0,   100, $flags,
                   $max, ($max - (100 * $width)), $width);
    assert_bop_gbp("$key", "desc",   100,     0, $flags,
                   ($max - (100 * $width)), $max, $width);
    assert_bop_gbp("$key", "asc",  10000, 10099, $flags,
                   ($min + (10000 * $width)), ($min + (10099 * $width)), $width);
    assert_bop_gbp("$key", "asc",  10099, 10000, $flags,
                   ($min + (10099 * $width)), ($min + (10000 * $width)), $width);
    assert_bop_gbp("$key", "desc", 10000, 10100, $flags,
                   ($max - (10000 * $width)), ($max - (10100 *$width)), $width);
    assert_bop_gbp("$key", "desc", 10100, 10000, $flags,
                   ($max - (10100 * $width)), ($max - (10000 *$width)), $width);
    assert_bop_gbp("$key", "asc",  25000, 25000, $flags,
                   ($min + (25000 * $width)), ($min + (25000 * $width)), $width);
    assert_bop_gbp("$key", "desc", 25000, 25000, $flags,
                   ($max - (25000 * $width)), ($max - (25000 *$width)), $width);

    # bop pwg(position with get)
    assert_bop_pwg("$key",  $min,  "asc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key",  $min, "desc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key",  $max,  "asc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key",  $max, "desc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 33333,  "asc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 33333, "desc",  0, $flags, $min, $max, $width);
    assert_bop_pwg("$key",  $min,  "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key",  $min, "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key",  $max,  "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key",  $max, "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 33333,  "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 33333, "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key",     9,  "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key",     9, "desc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 99989,  "asc", 10, $flags, $min, $max, $width);
    assert_bop_pwg("$key", 99989, "desc", 10, $flags, $min, $max, $width);

    if ($cnt == 0 or $cnt == 1) {
        $cmd = "bop delete $key 10001..19999"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_pos($key, 9999, "asc", $min, $max, $width);
        assert_bop_gbp("$key", "asc", 4999, 5000, $flags,  9999, 20001, 10002);
        $cmd = "bop delete $key 79999..50001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        assert_bop_pos($key, 89999, "desc", $min, $max, $width);
        assert_bop_gbp("$key", "desc", 9999, 10000, $flags, 80001, 49999, 30002);

        # bop get by position
        assert_bop_gbp("$key", "asc", 100, 199, $flags,
                       ($min + (100 * $width)), ($min + (199 * $width)), $width);
        assert_bop_gbp("$key", "asc", 199, 100, $flags,
                       ($min + (199 * $width)), ($min + (100 * $width)), $width);
        assert_bop_gbp("$key", "desc", 1, 100, $flags,
                       ($max - (1 * $width)), ($max - (100 * $width)), $width);
        assert_bop_gbp("$key", "desc", 100, 1, $flags,
                       ($max - (100 * $width)), ($max - (1* $width)), $width);
        $cmd = "bop delete $key 0..100000 0 NE 0x0005,0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    } elsif ($cnt == 2 or $cnt == 3) {
        $cmd = "bop delete $key 0..50000"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        # bop position (new min bkey: 50001)
        assert_bop_pos($key, 50121,  "asc", 50001, $max, $width);
        assert_bop_pos($key, 50121, "desc", 50001, $max, $width);
        # bop get by position (new min bkey: 50001)
        assert_bop_gbp("$key", "asc", 5000, 5100, $flags,
                       (50001 + (5000 * $width)), (50001 + (5100 * $width)), $width);
        assert_bop_gbp("$key", "desc", 5000, 5100, $flags,
                       ($max - (5000 * $width)), ($max - (5100 * $width)), $width);
        $cmd = "bop delete $key 100000..0 0 NE 0x0001,0x0003"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 90000..10000 0 EQ 0x0001"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 90000..10000 0 EQ 0x0003"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 90000..20000"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    } else {
        my $small;
        my $large;
        for ($small = 0; $small < 100000; $small = $small + 10000) {
            $large = $small + int(rand(9000));
            $cmd = "bop delete $key $small..$large"; $rst = "DELETED";
            print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        }

        $cmd = "bop delete $key 0..50000 0 GT 0x0002"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
        $cmd = "bop delete $key 100000..50000 0 GT 0x0002"; $rst = "DELETED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

        $cmd = "bop get $key 0..100000 0 GE 0x0003"; $rst = "NOT_FOUND_ELEMENT";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    }
    $cmd = "bop delete $key 0..100000000 drop"; $rst = "DELETED_DROPPED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "get $key"; $rst = "END";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cnt = $cnt+1;
}

# unset environment variable
delete $ENV{'ARCUS_MAX_BTREE_SIZE'};
