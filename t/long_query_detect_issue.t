#!/usr/bin/perl

use strict;
use Test::More tests => 107;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

sub do_btree_prepare {
    my ($kstr) = @_;
    my $eidx;

    $cmd = "bop create $kstr 0 0 0"; $rst = "CREATED";
    mem_cmd_is($sock, $cmd, "", $rst);

    for ($eidx = 0; $eidx < 100; $eidx += 1) {
        my $bkstr = "0x" . sprintf "%062d", $eidx;
        my $eflag = "0x" . sprintf "%062d", $eidx;
        my $val   = "element_value_$eidx";
        my $vleng = length($val);

        $cmd = "bop insert $kstr $bkstr $eflag $vleng"; $rst = "STORED";
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

sub do_btree_efilter {
    my ($kstr) = @_;
    my $bkrange;
    my $efilter;
    my $eidx;

    # longest bkrange
    $bkrange = "0x" . "0"x62 . ".." . "0x" . "F"x62;

    # prepare for making efilter.
    my $foperand = "0x" . "F"x62;
    my $fvallist = "0x" . sprintf "%062d", 1;
    for ($eidx = 2; $eidx <= 100; $eidx += 1) {
        $fvallist .= ",0x" . sprintf "%062d", $eidx;
    }
    # longest efilter case 1
    $efilter = "0 & $foperand EQ $fvallist";

    # bop count
    $cmd = "bop count $kstr $bkrange $efilter"; $rst = "COUNT=99";
    mem_cmd_is($sock, $cmd, "", $rst);

    # bop get
    $cmd = "bop get $kstr $bkrange $efilter 0 100";
    $rst = "VALUE 0 99\n";
    for ($eidx = 1; $eidx < 100; $eidx += 1) {
        my $bkstr = "0x" . sprintf "%062d", $eidx;
        my $eflag = "0x" . sprintf "%062d", $eidx;
        my $val   = "element_value_$eidx";
        my $vleng = length($val);
        $rst .= "$bkstr $eflag $vleng $val\n"
    }
    $rst .= "END";
    mem_cmd_is($sock, $cmd, "", $rst);

    # longest efilter case 2
    $efilter = "0 & $foperand NE $fvallist";

    # bop count
    $cmd = "bop count $kstr $bkrange $efilter"; $rst = "COUNT=1";
    mem_cmd_is($sock, $cmd, "", $rst);

    # bop get
    $cmd = "bop get $kstr $bkrange $efilter 0 100";
    $rst = "VALUE 0 1\n";
    for ($eidx = 0; $eidx < 1; $eidx += 1) {
        my $bkstr = "0x" . sprintf "%062d", $eidx;
        my $eflag = "0x" . sprintf "%062d", $eidx;
        my $val   = "element_value_$eidx";
        my $vleng = length($val);
        $rst .= "$bkstr $eflag $vleng $val\n"
    }
    $rst .= "END";
    mem_cmd_is($sock, $cmd, "", $rst);

    # bop get (single index)
    my $bkey = "0x" . "0"x62;
    $cmd = "bop get $kstr $bkey $efilter 0 100";
    $rst = "VALUE 0 1\n";
    for ($eidx = 0; $eidx < 1; $eidx += 1) {
        my $bkstr = "0x" . sprintf "%062d", $eidx;
        my $eflag = "0x" . sprintf "%062d", $eidx;
        my $val   = "element_value_$eidx";
        my $vleng = length($val);
        $rst .= "$bkstr $eflag $vleng $val\n"
    }
    $rst .= "END";
    mem_cmd_is($sock, $cmd, "", $rst);
}

# do test
my $key = "AA";
my $line;

print $sock "lqdetect start 10\r\n";
$line = scalar <$sock>;
$line = scalar <$sock>;
do_btree_prepare($key);
do_btree_efilter($key);
print $sock "lqdetect stop\r\n";
$line = scalar <$sock>;
$line = scalar <$sock>;
mem_cmd_is($sock, "delete $key", "", "DELETED");

# after test
release_memcached($engine, $server);
