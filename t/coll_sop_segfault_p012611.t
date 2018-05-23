#!/usr/bin/perl

use strict;
use Test::More tests => 20;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# SOP test sub routines
sub sop_insert {
    my ($key, $from, $to, $create) = @_;
    my $index;
    my $vleng;

    for ($index = $from; $index <= $to; $index++) {
        $val = "datum$index";
        $vleng = length($val);
        if ($index == $from) {
            $cmd = "sop insert $key $vleng $create";
            $rst = "CREATED_STORED";
        } else {
            $cmd = "sop insert $key $vleng";
            $rst = "STORED";
        }
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

# testSOPSegFault : Basic
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

sop_insert("skey", 0, 9, "create 13 0 0");

$cmd = "sop get skey 0";
$rst = "VALUE 13 10
6 datum9
6 datum8
6 datum3
6 datum2
6 datum1
6 datum0
6 datum7
6 datum6
6 datum5
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "sop delete skey 6"; $val="datum1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6"; $val="datum3"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6"; $val="datum5"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6"; $val="datum7"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6"; $val="datum9"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "sop get skey 0";
$rst = "VALUE 13 5
6 datum8
6 datum2
6 datum0
6 datum6
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
