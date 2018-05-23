#!/usr/bin/perl

use strict;
use Test::More tests => 189;
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
        if ($index == $from) {
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

# testLOPInsertGet
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
lop_insert("lkey", 0, 4, "create 17 0 0");
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=5
ATTR maxcount=$default_list_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..2147483647";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey -2147483648..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..2";
$rst = "VALUE 17 3
6 datum0
6 datum1
6 datum2
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 2..8";
$rst = "VALUE 17 3
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey -1..-2";
$rst = "VALUE 17 2
6 datum4
6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey -3..3";
$rst = "VALUE 17 2
6 datum2
6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-2";
$rst = "VALUE 17 4
6 datum0
6 datum1
6 datum2
6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 6..-3";
$rst = "VALUE 17 3
6 datum4
6 datum3
6 datum2
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 1..1";
$rst = "VALUE 17 1
6 datum1
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 1";
$rst = "VALUE 17 1
6 datum1
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 6..8"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey -10..-8"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPInsertDeletePop
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
lop_insert("lkey", 0, 9, "create 17 0 -1");
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=10
ATTR maxcount=$maximum_list_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 8..-3"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 1..3"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 7..9"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey -9..-8"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=5
ATTR maxcount=$maximum_list_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum4
6 datum5
6 datum6
6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 9..-2 delete";
$rst = "VALUE 17 2
6 datum9
6 datum6
DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 1 delete";
$rst = "VALUE 17 1
6 datum4
DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 4..5"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey -5..-7"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=2
ATTR maxcount=$maximum_list_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 2
6 datum0
6 datum5
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0..-1 drop"; $rst = "DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testEmptyCollectionOfListType
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop create lkey 17 0 -1"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=5
ATTR maxcount=$maximum_list_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0..2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1 delete";
$rst = "VALUE 17 2
6 datum3
6 datum4
DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0..-1"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=0
ATTR maxcount=$maximum_list_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1 drop";
$rst = "VALUE 17 3
6 datum0
6 datum1
6 datum2
DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPInsertFailCheck
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 0 6"; $val = "datum0"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 0 6 create 17 0 1000"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 2 6"; $val = "datum2"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -3 6"; $val = "datum2"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 1 9"; $val = "datum_new"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPOverflowCheck
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6 create 17 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr lkey count maxcount";
$rst = "ATTR count=1
ATTR maxcount=1000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr lkey maxcount=5"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey count maxcount overflowaction";
$rst = "ATTR count=1
ATTR maxcount=5
ATTR overflowaction=tail_trim
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum1
6 datum2
6 datum3
6 datum4
6 datum5
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 5 6"; $val = "datum6"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -6 6"; $val = "datum6"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 2 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 0 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 0 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum6
6 datum1
6 datum2
6 datum0
6 datum8
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 4 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum6
6 datum1
6 datum2
6 datum0
6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -5 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum6
6 datum3
6 datum1
6 datum2
6 datum0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr lkey overflowaction=head_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey overflowaction";
$rst = "ATTR overflowaction=head_trim
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 2 6"; $val = "datums"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum3
6 datums
6 datum1
6 datum2
6 datum0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 0 6"; $val = "datumt"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datumt
6 datum3
6 datums
6 datum1
6 datum2
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr lkey overflowaction=error"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr lkey overflowaction";
$rst = "ATTR overflowaction=error
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "lop insert lkey 2 6"; $val = "datumu"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey 0 6"; $val = "datumu"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datumu"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datumt
6 datum3
6 datums
6 datum1
6 datum2
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr lkey overflowaction=smallest_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr lkey overflowaction=largest_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr lkey overflow=largest_trim"; $rst = "ATTR_ERROR not found";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPTrimCheck-1
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop create lkey 17 0 5 head_trim"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 1 6"; $val = "datums"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datums
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 4 6"; $val = "datumt"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum1
6 datum2
6 datum3
6 datumt
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPTrimCheck-2
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop create lkey 17 0 5 tail_trim"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -2 6"; $val = "datums"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datums
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -5 6"; $val = "datumt"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datumt
6 datum1
6 datum2
6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPNotFoundError
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete lkey 0"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPNotLISTError
$cmd = "set x 19 5 10"; $val = "some value"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6 create 13 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 1 6 create 15 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert x -1 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert skey -1 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get x 0..-1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get skey 0..-1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get bkey 0..-1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete x 0..-1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete skey 0..-1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop delete bkey 0..-1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPNotKVError
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6 create 17 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set lkey 19 5 10"; $val = "some value"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "replace lkey 19 5 $flags"; $val = "other value"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "prepend lkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "append lkey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "incr lkey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "decr lkey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPExpire
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6 create 17 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr lkey expiretime=2"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 3
6 datum1
6 datum2
6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
sleep(2.1);
$cmd = "lop get lkey 0..-1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testLOPFlush
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey -1 6 create 17 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop insert lkey -1 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "lop get lkey 0..-1";
$rst = "VALUE 17 3
6 datum1
6 datum2
6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "flush_all"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop get lkey 0..-1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
