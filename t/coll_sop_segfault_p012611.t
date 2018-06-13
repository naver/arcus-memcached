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
sop_get_is($sock, "skey 0", 13, 10,
           "datum0,datum1,datum2,datum3,datum4,datum5,datum6,datum7,datum8,datum9");

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
sop_get_is($sock, "skey 0", 13, 5,
           "datum0,datum2,datum4,datum6,datum8");

$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
