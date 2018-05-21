#!/usr/bin/perl

use strict;
use Test::More tests => 21522;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

# BOP test sub routines
sub bop_insert {
    my ($key, $from_bkey, $to_bkey, $width, $create, $flags, $exptime, $maxcount) = @_;
    my $data = "_data_";
    my $bkey;
    my $val;
    my $rst;
    my $vleng;
    my $cmd;

    $val = "$key$data$from_bkey";
    $vleng = length($val);
    if ($create eq "create") {
        $cmd = "bop insert $key $from_bkey $vleng $create $flags $exptime $maxcount";
        $rst = "CREATED_STORED";
    } else {
        $cmd = "bop insert $key $from_bkey $vleng"; $rst = "STORED";
    }
    mem_cmd_is($sock, $cmd, $val, $rst);

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey + $width; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $val = "$key$data$bkey";
            $vleng = length($val);
            $cmd = "bop insert $key $bkey $vleng"; $rst = "STORED";
            mem_cmd_is($sock, $cmd, $val, $rst);
        }
    } else {
        for ($bkey = $from_bkey - $width; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $val = "$key$data$bkey";
            $vleng = length($val);
            $cmd = "bop insert $key $bkey $vleng"; $rst = "STORED";
            mem_cmd_is($sock, $cmd, $val, $rst);
        }
    }
}

sub assert_bop_get {
    my ($key, $width, $flags, $from_bkey, $to_bkey, $offset, $count, $delete, $tailstr) = @_;
    my $saved_from_bkey = $from_bkey;
    my $saved_to_bkey   = $to_bkey;
    my $data = "_data_";
    my $bkey;
    my $val;
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
            $val = "$key$data$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $val);
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
            $val = "$key$data$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $val);
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
    mem_cmd_is($sock, $cmd, "", $rst);
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
    $cmd = "getattr bkey count maxcount";
    $rst = "ATTR count=1000\n"
         . "ATTR maxcount=$default_btree_size\n"
         . "END";
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
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop get bkey 0..5"; $rst = "NOT_FOUND_ELEMENT";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop get bkey 20000..15000"; $rst = "NOT_FOUND_ELEMENT";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "delete bkey"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "get bkey"; $rst = "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cnt = $cnt+1;
}

# testBOPInsertDeletePop
$max = 20000;
$cnt = 0;
for (0..6) {
    $cmd = "get bkey"; $rst = "END";
    mem_cmd_is($sock, $cmd, "", $rst);
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
    $cmd = "getattr bkey count maxcount";
    $rst = "ATTR count=2000\n"
         . "ATTR maxcount=$maximum_btree_size\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 1000..1999"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 10000..0 100"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 15550..17000 50"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 8700..4350 50"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 0..2000 100"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 2020..2020"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 22000..2000 100"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "getattr bkey count maxcount";
    $rst = "ATTR count=1499\n"
         . "ATTR maxcount=$maximum_btree_size\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    assert_bop_get("bkey", $width, $flags, 3000, 5000, 0, 100, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 13000, 11000, 50, 50, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 13000, 11000, 0, 50, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 13400, 15300, 0, 50, "delete", "DELETED");
    assert_bop_get("bkey", $width, $flags, 7200, 5980, 0, 50, "delete", "DELETED");
    $cmd = "getattr bkey count maxcount";
    $rst = "ATTR count=1199\n"
         . "ATTR maxcount=$maximum_btree_size\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    assert_bop_get("bkey", $width, $flags, 5800, 6200, 0, 40, "", "END");
    assert_bop_get("bkey", $width, $flags, 5820, 610, 0, 70, "", "END");
    assert_bop_get("bkey", $width, $flags, 2100, 3200, 0, 60, "", "END");
    assert_bop_get("bkey", $width, $flags, 15000, 14000, 0, 30, "", "END");
    assert_bop_get("bkey", $width, $flags, 14200, 14400, 0, 100, "", "END");
    assert_bop_get("bkey", $width, $flags, 14200, 14900, 0, 100, "", "END");
    $cmd = "getattr bkey count maxcount";
    $rst = "ATTR count=1199\n"
         . "ATTR maxcount=$maximum_btree_size\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop count bkey 2010"; $rst = "COUNT=1";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop get bkey 2010";
    $rst = "VALUE $flags 1\n"
         . "2010 14 bkey_data_2010\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop count bkey 10010..9000"; $rst = "COUNT=2";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop get bkey 10010..9000";
    $rst = "VALUE $flags 2\n"
         . "10010 15 bkey_data_10010\n"
         . "9000 14 bkey_data_9000\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop count bkey 0..900"; $rst = "COUNT=0";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop get bkey 0..900"; $rst = "NOT_FOUND_ELEMENT";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop count bkey 20000..19100"; $rst = "COUNT=0";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop get bkey 20000..19100"; $rst = "NOT_FOUND_ELEMENT";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 0..900"; $rst = "NOT_FOUND_ELEMENT";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop get bkey 0..900 delete"; $rst = "NOT_FOUND_ELEMENT";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete bkey 0..100000000 drop"; $rst = "DELETED_DROPPED";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "get bkey"; $rst = "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cnt = $cnt+1;
}

# testEmptyCollectionOfB+TreeType
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create bkey $flags 0 -1"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 40 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey count maxcount";
$rst = "ATTR count=5
ATTR maxcount=$maximum_btree_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count bkey 0..40"; $rst = "COUNT=5";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..40";
$rst = "VALUE $flags 5
0 6 datum0
10 6 datum1
20 6 datum2
30 6 datum3
40 6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey 0..20"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count bkey 20..40"; $rst = "COUNT=2";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 20..40 delete";
$rst = "VALUE $flags 2
30 6 datum3
40 6 datum4
DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey 0..20"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..20"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey count maxcount";
$rst = "ATTR count=0
ATTR maxcount=$maximum_btree_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count bkey 0..50"; $rst = "COUNT=3";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..50 drop";
$rst = "VALUE $flags 3
0 6 datum0
10 6 datum1
20 6 datum2
DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testBOPInsertFailCheck
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 10 6 create $flags 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "ELEMENT_EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 15 9"; $val = "datum_new"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "delete bkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testBOPOverflowCheck
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 10 6 create $flags 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey count maxcount";
$rst = "ATTR count=1
ATTR maxcount=1000
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "setattr bkey maxcount=5"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey count maxcount overflowaction";
$rst = "ATTR count=1
ATTR maxcount=5
ATTR overflowaction=smallest_trim
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 50 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 70 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 90 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey count maxcount";
$rst = "ATTR count=5
ATTR maxcount=5
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..1000";
$rst = "VALUE $flags 5
10 6 datum1
30 6 datum3
50 6 datum5
70 6 datum7
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 80 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 60 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..1000 0";
$rst = "VALUE $flags 5
50 6 datum5
60 6 datum6
70 6 datum7
80 6 datum8
90 6 datum9
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey overflowaction=largest_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey overflowaction";
$rst = "ATTR overflowaction=largest_trim
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 40 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 90 6"; $val = "datum1"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..1000 0 0";
$rst = "VALUE $flags 5
30 6 datum3
40 6 datum4
50 6 datum5
60 6 datum6
70 6 datum7
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey overflowaction=error"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey overflowaction";
$rst = "ATTR overflowaction=error
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 20 6"; $val = "datum1"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 80 6"; $val = "datum1"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey overflowaction=head_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey overflowaction=tail_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey overflow=tail_trim"; $rst = "ATTR_ERROR not found";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

#testBOPMaxBKeyRangeCheck
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 10 6 create $flags 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey count maxcount maxbkeyrange";
$rst = "ATTR count=1
ATTR maxcount=1000
ATTR maxbkeyrange=0
END";
$cmd = "setattr bkey maxbkeyrange=80"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey count maxcount maxbkeyrange overflowaction";
$rst = "ATTR count=1
ATTR maxcount=1000
ATTR maxbkeyrange=80
ATTR overflowaction=smallest_trim
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 50 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 70 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 90 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey count maxcount";
$rst = "ATTR count=5
ATTR maxcount=1000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=5";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..1000";
$rst = "VALUE $flags 5
10 6 datum1
30 6 datum3
50 6 datum5
70 6 datum7
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 80 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey 0..1000";
$rst = "VALUE $flags 6
10 6 datum1
30 6 datum3
50 6 datum5
70 6 datum7
80 6 datum8
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 100 7"; $val = "datum10"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey 0..1000";
$rst = "VALUE $flags 6
30 6 datum3
50 6 datum5
70 6 datum7
80 6 datum8
90 6 datum9
100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=6";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey overflowaction=largest_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey maxbkeyrange overflowaction";
$rst = "ATTR maxbkeyrange=80
ATTR overflowaction=largest_trim
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop insert bkey 40 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey 0..1000";
$rst = "VALUE $flags 7
30 6 datum3
40 6 datum4
50 6 datum5
70 6 datum7
80 6 datum8
90 6 datum9
100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 120 7"; $val = "datum12"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 10 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count bkey 0..1000"; $rst = "COUNT=7";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..1000";
$rst = "VALUE $flags 7
10 6 datum1
30 6 datum3
40 6 datum4
50 6 datum5
70 6 datum7
80 6 datum8
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey maxbkeyrange=0"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey maxbkeyrange";
$rst = "ATTR maxbkeyrange=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 0 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 60 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 120 7"; $val = "datum12"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey 0..1000";
$rst = "VALUE $flags 10
0 6 datum0
10 6 datum1
30 6 datum3
40 6 datum4
50 6 datum5
60 6 datum6
70 6 datum7
80 6 datum8
90 6 datum9
120 7 datum12
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testBOPNotFoundError
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..100"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey 0..100"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count bkey 0..100"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);

# testBOPNotLISTError
$cmd = "set x 19 5 10"; $val = "some value"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 0 6 create 13 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6 create 15 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert x 10 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert lkey 10 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert skey 10 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop delete x 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete lkey 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete skey 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get x 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get lkey 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get skey 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count x 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count lkey 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count skey 0..100"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testBOPNotKVError
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 10 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set bkey 19 5 10"; $val = "some value"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "replace bkey 19 5 $flags"; $val = "other value"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "prepend bkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "append bkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "incr bkey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "decr bkey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testBOPExpire
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 10 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey expiretime=2"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop count bkey 0..100"; $rst = "COUNT=3";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..100 0 0";
$rst = "VALUE $flags 3
10 6 datum1
20 6 datum2
30 6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
sleep(2.1);
$cmd = "bop get bkey 0..100"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testBOPFlush
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey 10 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count bkey 0..100"; $rst = "COUNT=3";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..100";
$rst = "VALUE $flags 3
10 6 datum1
20 6 datum2
30 6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "flush_all"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 0..100"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
