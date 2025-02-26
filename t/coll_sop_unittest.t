#!/usr/bin/perl

use strict;
use Test::More tests => 83005;
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

sub sop_delete {
    my ($key, $from, $to) = @_;
    my $index;
    my $vleng;

    for ($index = $from; $index <= $to; $index++) {
        $val = "datum$index";
        $vleng = length($val);
        $cmd = "sop delete $key $vleng"; $rst = "DELETED";
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

sub assert_sop_get {
    my ($args, $flags, $ecount, $from, $to) = @_;
    my $index;
    my @res_data = ();

    for ($index = $from; $index <= $to; $index++) {
        $val = "datum$index";
        push(@res_data, $val);
    }
    my $data_list = join(",", @res_data);

    sop_get_is($sock, $args, $flags, $ecount, $data_list);
}

# SOP test global variables
my $flags = 13;
my $default_set_size = 4000;
my $maximum_set_size = 50000;
my $cnt;

# testSOPInsertGet
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
sop_insert("skey", 0, 2848, "create $flags 0 0");
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=2849
ATTR maxcount=$default_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
assert_sop_get("skey 0",    $flags, 2849, 0, 2848);
$cmd = "sop exist skey 6"; $val="datum6"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 7"; $val="datum66"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 8"; $val="datum666"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 9"; $val="datum6666"; $rst = "NOT_EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testSOPInsertDeletePop
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
sop_insert("skey", 0, 9, "create $flags 0 -1");
$cmd = "sop insert skey 6"; $val="datum3"; $rst = "ELEMENT_EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=10
ATTR maxcount=$maximum_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
sop_get_is($sock, "skey 0", $flags, 10,
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
$cmd = "sop delete skey 6"; $val="datum3"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 7"; $val="datum10"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
sop_get_is($sock, "skey 0 delete", $flags, 5,
           "datum0,datum2,datum4,datum6,datum8");
$cmd = "sop insert skey 6"; $val="datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6 drop"; $val="datum3"; $rst = "DELETED_DROPPED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
sop_insert("skey", 0, 49999, "create $flags 0 -1");
$cmd = "sop insert skey 10"; $val="datum12345"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=50000
ATTR maxcount=$maximum_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
assert_sop_get("skey 0", $flags, 50000, 0, 49999);
$cmd = "sop delete skey 6"; $val="datum4"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 7"; $val="datum44"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 8"; $val="datum444"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 9"; $val="datum4444"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 10"; $val="datum44444"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 11"; $val="datum444444"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 8"; $val="datum444"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=49995
ATTR maxcount=$maximum_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 10"; $val="datum44444"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 9"; $val="datum4444"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 8"; $val="datum444"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 7"; $val="datum44"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val="datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=50000
ATTR maxcount=$maximum_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
sop_delete("skey", 30000, 39999);
sop_delete("skey", 0, 9999);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=30000
ATTR maxcount=$maximum_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop exist skey 9"; $val="datum6666"; $rst = "NOT_EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist skey 10"; $val="datum26666"; $rst = "EXIST";
mem_cmd_is($sock, $cmd, $val, $rst);
sop_delete("skey", 40000, 49999);
assert_sop_get("skey 0 drop", $flags, 20000, 10000, 29999);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testEmptyCollectionOfSetType
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop create skey $flags 0 -1"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=5
ATTR maxcount=$maximum_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
sop_get_is($sock, "skey 0", $flags, 5,
           "datum0,datum1,datum2,datum3,datum4");
$cmd = "sop delete skey 6"; $val="datum0"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6"; $val="datum2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6"; $val="datum4"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
sop_get_is($sock, "skey 0 delete", $flags, 2, "datum1,datum3");
$cmd = "sop delete skey 6"; $val="datum3"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop get skey 1"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=0
ATTR maxcount=$maximum_set_size
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
sop_get_is($sock, "skey 0 drop", $flags, 3, "datum0,datum1,datum2");
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testSOPInsertFailCheck
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6 create $flags 0 1000"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum0"; $rst = "ELEMENT_EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 9"; $val = "datum_new"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testSOPOverflowCheck
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6 create $flags 0 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr skey count maxcount";
$rst = "ATTR count=1
ATTR maxcount=1000
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr skey maxcount=5"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr skey count maxcount overflowaction";
$rst = "ATTR count=1
ATTR maxcount=5
ATTR overflowaction=error
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
sop_get_is($sock, "skey 0", $flags, 5,
           "datum1,datum2,datum3,datum4,datum5");
$cmd = "sop insert skey 6"; $val = "datum6"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr skey overflowaction=head_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr skey overflowaction=tail_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr skey overflowaction=smallest_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr skey overflowaction=largest_trim"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr skey overflow=largest_trim"; $rst = "ATTR_ERROR not found";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testSOPNotFoundError
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop get skey 1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop exist skey 6"; $val = "datum1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete skey 6"; $val = "datum1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);

# testSOPNotLISTError
$cmd = "set x 19 5 10"; $val = "some value"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey 0 6 create 17 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey 1 6 create 15 0 0"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert x 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert lkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert bkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete x 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete lkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop delete bkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist x 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist lkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop exist bkey 6"; $val = "datum1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop get x 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop get lkey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop get bkey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get lkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testSOPNotKVError
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set skey 19 5 10"; $val = "some value"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "replace skey 19 5 11"; $val = "other value"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "prepend skey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "append skey 19 5 5"; $val = "thing"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "incr skey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "decr skey 1"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete skey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testSOPExpire
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr skey expiretime=2"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
sop_get_is($sock, "skey 0",  $flags, 3, "datum1,datum2,datum3");
sleep(2.1);
$cmd = "sop get skey 5"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# testSOPFlush
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop insert skey 6 create $flags 60 1000"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "sop insert skey 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
sop_get_is($sock, "skey 0",  $flags, 3, "datum1,datum2,datum3");
$cmd = "flush_all"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "sop get skey 5"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get skey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
