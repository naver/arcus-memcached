#!/usr/bin/perl

use strict;
use Test::More tests => 20;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $rst;

# SOP test sub routines
sub sop_insert {
    my ($key, $data_prefix, $count, $create, $flags, $exptime, $maxcount) = @_;
    my $i=0;
    my $val = "$data_prefix$i";
    my $vleng = length($val);
    my $cmd;
    my $msg;

    $cmd = "sop insert $key $vleng $create $flags $exptime $maxcount"; $rst = "CREATED_STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);

    for ($i = 1; $i < $count; $i++) {
        $val = "$data_prefix$i";
        $vleng = length($val);
        $cmd = "sop insert $key $vleng\r\n$val\r\n"; $rst = "STORED";
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

# SOP test global variables
my $cnt;
my $cmd;
my $val;
my $rst;

# testSOPSegFault : Basic
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
#sop_insert("skey", "datum", 10, "create", 13, 0, 0);
$cmd = "sop insert skey 6 create 13 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
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
