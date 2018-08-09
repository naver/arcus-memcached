#!/usr/bin/perl

use strict;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

if ("$engine" eq "default" || "$engine" eq "") {
    plan tests => 54;
} else {
    plan tests => 37;
}

my $cmd;
my $val;
my $rst;

sub bop_insert {
    my ($key, $from_bkey, $to_bkey, $width, $create) = @_;
    my $bkey;
    my $valid; # value id
    my $value;
    my $vleng;

    $bkey = $from_bkey;
    $valid = $bkey / 10;
    $value = "datum" . "$valid";
    $vleng = length($value);
    if ($create ne "") {
        $cmd = "bop insert $key $bkey $vleng $create";
        $rst = "CREATED_STORED";
    } else {
        $cmd = "bop insert $key $bkey $vleng";
        $rst = "STORED";
    }
    mem_cmd_is($sock, $cmd, $value, $rst);

    if ($from_bkey <= $to_bkey) {
        for ($bkey += $width; $bkey <= $to_bkey; $bkey += $width) {
            $valid = $bkey / 10;
            $value = "datum" . "$valid";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $vleng"; $rst = "STORED";
            mem_cmd_is($sock, $cmd, $value, $rst);
        }
    } else {
        for ($bkey -= $width; $bkey >= $to_bkey; $bkey -= $width) {
            $valid = $bkey / 10;
            $value = "datum" . "$valid";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $vleng"; $rst = "STORED";
            mem_cmd_is($sock, $cmd, $value, $rst);
        }
    }
}

# [ Basic SMGET Test ]
mem_cmd_is($sock, "get kvkey", "", "END");
mem_cmd_is($sock, "get bkey1", "", "END");
mem_cmd_is($sock, "get bkey2", "", "END");

# create kvkey, bkey1, bkey2
mem_cmd_is($sock, "set kvkey 19 5 10", "some value", "STORED");
bop_insert("bkey1",  90, 10, 20, "create 11 0 0");
bop_insert("bkey2", 100, 20, 20, "create 12 0 0");

# NEW smget test
$cmd = "bop smget 11 2 0..100 5"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey1 11 10 6 datum1
bkey2 12 20 6 datum2
bkey1 11 30 6 datum3
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 100..0 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
bkey1 11 10 6 datum1
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 70..0 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 7
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
bkey1 11 10 6 datum1
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0..100 5"; $val = "bkey1,bkey2";
$rst = "VALUE 5
bkey1 11 10 6 datum1
bkey2 12 20 6 datum2
bkey1 11 30 6 datum3
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 100..0 10"; $val = "bkey1,bkey2";
$rst = "VALUE 10
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
bkey1 11 10 6 datum1
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 70..0 10"; $val = "bkey1,bkey2";
$rst = "VALUE 7
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
bkey1 11 10 6 datum1
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0..100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 6
bkey1 11 30 6 datum3
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 90..30 2 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 5
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 200..300 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 0
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
}

# OLD smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0..100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 6
bkey1 11 30 6 datum3
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 90..30 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 5
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 200..300 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 0
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test: missed
$cmd = "bop smget 29 5 30..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst = "ELEMENTS 6
bkey1 11 30 6 datum3
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test: missed
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 29 5 30..100 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey3";
$rst = "VALUE 6
bkey1 11 30 6 datum3
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
MISSED_KEYS 3
bkey3
bkey4
bkey3
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# create bkey3 : empty collection
mem_cmd_is($sock, "bop create bkey3 13 0 0", "", "CREATED");

# NEW smget test : empty collection
$cmd = "bop smget 11 2 0..100 5"; $val = "bkey2 bkey3";
$rst = "ELEMENTS 5
bkey2 12 20 6 datum2
bkey2 12 40 6 datum4
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 146 21 0..100000 10";
$val = "KEY_11,KEY_12,KEY_13,KEY_14,KEY_15,KEY_16,KEY_17,"
     . "KEY_18,KEY_19,KEY_20,KEY_21,KEY_22,KEY_23,KEY_24,"
     . "KEY_25,KEY_26,KEY_27,KEY_28,KEY_29,KEY_30,KEY_16";
$rst = "ELEMENTS 0
MISSED_KEYS 21
KEY_11 NOT_FOUND
KEY_12 NOT_FOUND
KEY_13 NOT_FOUND
KEY_14 NOT_FOUND
KEY_15 NOT_FOUND
KEY_16 NOT_FOUND
KEY_17 NOT_FOUND
KEY_18 NOT_FOUND
KEY_19 NOT_FOUND
KEY_20 NOT_FOUND
KEY_21 NOT_FOUND
KEY_22 NOT_FOUND
KEY_23 NOT_FOUND
KEY_24 NOT_FOUND
KEY_25 NOT_FOUND
KEY_26 NOT_FOUND
KEY_27 NOT_FOUND
KEY_28 NOT_FOUND
KEY_29 NOT_FOUND
KEY_30 NOT_FOUND
KEY_16 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : empty collection
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0..100 5"; $val = "bkey2 bkey3";
$rst = "VALUE 5
bkey2 12 20 6 datum2
bkey2 12 40 6 datum4
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 146 21 0..100000 10";
$val = "KEY_11,KEY_12,KEY_13,KEY_14,KEY_15,KEY_16,KEY_17,"
     . "KEY_18,KEY_19,KEY_20,KEY_21,KEY_22,KEY_23,KEY_24,"
     . "KEY_25,KEY_26,KEY_27,KEY_28,KEY_29,KEY_30,KEY_16";
$rst = "VALUE 0
MISSED_KEYS 21
KEY_11
KEY_12
KEY_13
KEY_14
KEY_15
KEY_16
KEY_17
KEY_18
KEY_19
KEY_20
KEY_21
KEY_22
KEY_23
KEY_24
KEY_25
KEY_26
KEY_27
KEY_28
KEY_29
KEY_30
KEY_16
END";
}

# NEW smget test: failure
$cmd = "bop smget 23 2 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test: failure
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 2 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete items
mem_cmd_is($sock, "delete kvkey", "", "DELETED");
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");
mem_cmd_is($sock, "delete bkey3", "", "DELETED");

# after test
release_memcached($engine, $server);
