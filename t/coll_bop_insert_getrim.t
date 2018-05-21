#!/usr/bin/perl

use strict;
use Test::More tests => 207;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

# BOP test sub routines
sub bop_insert_getrim_load {
    my ($key, $from_bkey, $to_bkey, $count) = @_;
    my $bkey;
    my $eflag = "";
    my $val;
    my $vleng;
    my $cmd;
    my $rst;

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey; $bkey <= $to_bkey; $bkey = $bkey + 1) {
            $val = "$key-data-$bkey";
            $vleng = length($val);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim"; $rst = "STORED";
            mem_cmd_is($sock, $cmd, $val, $rst);
        }
    } else {
        for ($bkey = $from_bkey; $bkey >= $to_bkey; $bkey = $bkey - 1) {
            $val = "$key-data-$bkey";
            $vleng = length($val);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim"; $rst = "STORED";
            mem_cmd_is($sock, $cmd, $val, $rst);
        }
    }
}

sub bop_insert_getrim_trim {
    my ($key, $from_bkey, $to_bkey, $count, $flags) = @_;
    my $bkey;
    my $eflag = "";
    my $val;
    my $vleng;
    my $trim_bkey;
    my $trim_value;
    my $trim_vleng;
    my $cmd;
    my $rst;

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey; $bkey <= $to_bkey; $bkey = $bkey + 1) {
            $val = "$key-data-$bkey";
            $vleng = length($val);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim";
            $trim_bkey = $bkey - $count;
            $trim_value = "$key-data-$trim_bkey";
            $trim_vleng = length($trim_value);
            $rst = "VALUE $flags 1\n"
                 . "$trim_bkey $trim_vleng $trim_value\n"
                 . "TRIMMED";
            mem_cmd_is($sock, $cmd, $val, $rst);
        }
    } else {
        for ($bkey = $from_bkey; $bkey >= $to_bkey; $bkey = $bkey - 1) {
            $val = "$key-data-$bkey";
            $vleng = length($val);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim";
            $trim_bkey = $bkey - $count;
            $trim_value = "$key-data-$trim_bkey";
            $trim_vleng = length($trim_value);
            $rst = "VALUE $flags 1\n"
                 . "$trim_bkey $trim_vleng $trim_value\n"
                 . "TRIMMED";
            mem_cmd_is($sock, $cmd, $val, $rst);
        }
    }
}

# BOP test global variables
my $count = 100;
my $cmd;
my $val;
my $rst;
my $i;

$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create bkey 11 0 $count"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey count maxcount";
$rst =
"ATTR count=0
ATTR maxcount=$count
END";
mem_cmd_is($sock, $cmd, "", $rst);
bop_insert_getrim_load("bkey", 1, $count, $count);
$cmd = "getattr bkey count maxcount trimmed";
$rst =
"ATTR count=$count
ATTR maxcount=$count
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
bop_insert_getrim_trim("bkey", $count+1, $count*2, $count, 11);
$cmd = "getattr bkey count maxcount trimmed";
$rst =
"ATTR count=$count
ATTR maxcount=$count
ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey 101..200 0 0";
$rst = "VALUE 11 100\n";
for ($i = 101; $i <= 200; $i++) {
    $rst .= "$i 13 bkey-data-$i\n";
}
$rst .= "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
