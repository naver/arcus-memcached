#!/usr/bin/perl

use strict;
use Test::More tests => 21922;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# LOB test sub routines
sub lop_insert {
    my ($key, $from, $to, $create) = @_;
    my $index;
    my $vleng;

    for ($index = $from; $index <= $to; $index++) {
        $val = "datum$index";
        $vleng = length($val);
        if ($index == $from && $create ne "") {
            $cmd = "lop insert $key $index $vleng $create";
            $rst = "CREATED_STORED";
        } else {
            $cmd = "lop insert $key $index $vleng";
            $rst = "STORED";
        }
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

# LOP test global variables
my $flags = 11;
my $default_list_size = 4000;
my $maximum_list_size = 50000;

# testLargeListCollection 1
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
lop_insert("lkey", 0, 1999, "create 17 0 10000");
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=2000
ATTR maxcount=10000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 100..999"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 999..100"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=200
ATTR maxcount=10000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 95..104 delete";
$rst = "VALUE 17 10
7 datum95
7 datum96
7 datum97
7 datum98
7 datum99
9 datum1900
9 datum1901
9 datum1902
9 datum1903
9 datum1904
DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 90..99 delete";
$rst = "VALUE 17 10
7 datum90
7 datum91
7 datum92
7 datum93
7 datum94
9 datum1905
9 datum1906
9 datum1907
9 datum1908
9 datum1909
DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0..89"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 89..10"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey -1..0";
$rst = "VALUE 17 10
9 datum1919
9 datum1918
9 datum1917
9 datum1916
9 datum1915
9 datum1914
9 datum1913
9 datum1912
9 datum1911
9 datum1910
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0..-1 drop"; $rst = "DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);


# testLargeListCollection 2
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
lop_insert("lkey", 0, 9999, "create 17 0 10000");
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=10000
ATTR maxcount=10000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 100..9999"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=100
ATTR maxcount=10000
END";
mem_cmd_is($sock, $cmd, "", $rst);
lop_insert("lkey", 100, 9999, "");
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=10000
ATTR maxcount=10000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0..9989"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=10
ATTR maxcount=10000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey -1..0";
$rst = "VALUE 17 10
9 datum9999
9 datum9998
9 datum9997
9 datum9996
9 datum9995
9 datum9994
9 datum9993
9 datum9992
9 datum9991
9 datum9990
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0..-1 drop"; $rst = "DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);



# after test
release_memcached($engine, $server);
