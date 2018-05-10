#!/usr/bin/perl

use strict;
use Test::More tests => 212;
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
    my $cnt;

    $cmd = "bop create $kstr 0 0 0"; $rst = "CREATED";
    mem_cmd_is($sock, $cmd, "", $rst);

    for ($cnt = 0; $cnt < 100; $cnt += 1) {
        my $bkstr = "0x" . sprintf "%062d", $cnt;
        my $eflag = "0x" . sprintf "%062d", $cnt;
        my $val   = "element_value_$cnt";
        my $vleng = length($val);

        $cmd = "bop insert $kstr $bkstr $eflag $vleng"; $rst = "STORED";
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

sub do_btree_efilter {
    my ($kstr) = @_;
    my $bkrange;
    my $efilter;
    my $cnt;

    # lonest bkrange
    $bkrange = "0x" . "0"x62 . ".." . "0x" . "F"x62;

    # longest efilter 1
    my $foperand = "0x" . "F"x62;
    my $fvallist = "0x" . sprintf "%062d", 1;
    for ($cnt = 2; $cnt <= 100; $cnt += 1) {
        $fvallist .= ",0x" . sprintf "%062d", $cnt;
    }
    $efilter = "0 & $foperand EQ $fvallist";

    # bop count
    $cmd = "bop count $kstr $bkrange $efilter"; $rst = "COUNT=99";
    mem_cmd_is($sock, $cmd, "", $rst);

    # bop get
    $rst = "VALUE 0 99\n";
    for ($cnt = 1; $cnt < 100; $cnt += 1) {
        my $bkstr = "0x" . sprintf "%062d", $cnt;
        my $eflag = "0x" . sprintf "%062d", $cnt;
        my $val   = "element_value_$cnt";
        my $vleng = length($val);
        $rst .= "$bkstr $eflag $vleng $val\n"
    }
    $rst .= "END";
    $cmd = "bop get $kstr $bkrange $efilter 0 100";
    mem_cmd_is($sock, $cmd, "", $rst);

    # longest efilter 2
    $efilter = "0 & $foperand NE $fvallist";

    # bop count
    $cmd = "bop count $kstr $bkrange $efilter"; $rst = "COUNT=1";
    mem_cmd_is($sock, $cmd, "", $rst);

    # bop get
    $rst = "VALUE 0 1\n";
    for ($cnt = 0; $cnt < 1; $cnt += 1) {
        my $bkstr = "0x" . sprintf "%062d", $cnt;
        my $eflag = "0x" . sprintf "%062d", $cnt;
        my $val   = "element_value_$cnt";
        my $vleng = length($val);
        $rst .= "$bkstr $eflag $vleng $val\n"
    }
    $rst .= "END";
    $cmd = "bop get $kstr $bkrange $efilter 0 100";
    mem_cmd_is($sock, $cmd, "", $rst);
}

# do test
my $key;

# KEY_MAX_LENGTH = 250
$key = "A" x 250;
do_btree_prepare($key);
do_btree_efilter($key);
$cmd = "delete $key"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# KEY_MAX_LENGTH = 32000: long key test
$key = "B" x 32000;
do_btree_prepare($key);
do_btree_efilter($key);
$cmd = "delete $key"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
