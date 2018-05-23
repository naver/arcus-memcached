#!/usr/bin/perl

use strict;
use Test::More tests => 154;

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

## Insert some items without prefix
#initial stats
stats_prefixes_is($sock, "");

$cmd = "set stats_prefixes_aa 0 0 2"; $val = "hi"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get stats_prefixes_aa";
$rst = "VALUE stats_prefixes_aa 0 2
hi
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "set stats_prefixes_bb 0 0 3"; $val = "bye"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get stats_prefixes_bb";
$rst = "VALUE stats_prefixes_bb 0 3
bye
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count stats_prefixes_btree 0..20";
$rst = "COUNT=1";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert stats_prefixes_map field 6 create 19 0 0"; $val = "datum7";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("stats_prefixes_list", 0, 4, "create 17 0 0");
$cmd = "lop get stats_prefixes_list 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

lop_insert("stats_prefixes_list2", 0, 4, "create 17 0 0");
$cmd = "lop get stats_prefixes_list2 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("stats_prefixes_set", 0, 2, "create 13 0 0");
stats_prefixes_is($sock, "PREFIX <null> itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");

# delete
$cmd = "delete stats_prefixes_aa"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 6 kitm 1 litm 2 sitm 1 mitm 1 bitm 1");

$cmd = "delete stats_prefixes_bb"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 5 kitm 0 litm 2 sitm 1 mitm 1 bitm 1");

$cmd = "delete stats_prefixes_btree"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 4 kitm 0 litm 2 sitm 1 mitm 1 bitm 0");

$cmd = "delete stats_prefixes_list"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 3 kitm 0 litm 1 sitm 1 mitm 1 bitm 0");

$cmd = "delete stats_prefixes_list2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 2 kitm 0 litm 0 sitm 1 mitm 1 bitm 0");

$cmd = "delete stats_prefixes_set"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX <null> itm 1 kitm 0 litm 0 sitm 0 mitm 1 bitm 0");

$cmd = "delete stats_prefixes_map"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

#check again
stats_prefixes_is($sock, "");

## Insert some items have prefix
#initial stats
stats_prefixes_is($sock, "");

$cmd = "set aa:stats_prefixes_aa 0 0 2"; $val = "hi"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get aa:stats_prefixes_aa";
$rst = "VALUE aa:stats_prefixes_aa 0 2
hi
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "set aa:stats_prefixes_bb 0 0 3"; $val = "bye"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get aa:stats_prefixes_bb";
$rst = "VALUE aa:stats_prefixes_bb 0 3
bye
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert aa:stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count aa:stats_prefixes_btree 0..20";
$rst = "COUNT=1";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert aa:stats_prefixes_map field 6 create 19 0 0"; $val = "datum7";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("aa:stats_prefixes_list", 0, 4, "create 17 0 0");
$cmd = "lop get aa:stats_prefixes_list 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

lop_insert("aa:stats_prefixes_list2", 0, 4, "create 17 0 0");
$cmd = "lop get aa:stats_prefixes_list2 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("aa:stats_prefixes_set", 0, 2, "create 13 0 0");
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");


# delete
$cmd = "delete aa:stats_prefixes_aa"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 1 litm 2 sitm 1 mitm 1 bitm 1");

$cmd = "delete aa:stats_prefixes_bb"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 5 kitm 0 litm 2 sitm 1 mitm 1 bitm 1");

$cmd = "delete aa:stats_prefixes_btree"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 0 litm 2 sitm 1 mitm 1 bitm 0");

$cmd = "delete aa:stats_prefixes_list"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 0 litm 1 sitm 1 mitm 1 bitm 0");

$cmd = "delete aa:stats_prefixes_list2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 0 litm 0 sitm 1 mitm 1 bitm 0");

$cmd = "delete aa:stats_prefixes_set"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 1 kitm 0 litm 0 sitm 0 mitm 1 bitm 0");

$cmd = "delete aa:stats_prefixes_map"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

#check again
stats_prefixes_is($sock, "");

## Insert some items have prefix
#initial stats
stats_prefixes_is($sock, "");

$cmd = "set aa:stats_prefixes_aa 0 0 2"; $val = "hi"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get aa:stats_prefixes_aa";
$rst = "VALUE aa:stats_prefixes_aa 0 2
hi
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "set aa:stats_prefixes_bb 0 0 3"; $val = "bye"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get aa:stats_prefixes_bb";
$rst = "VALUE aa:stats_prefixes_bb 0 3
bye
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert aa:stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count aa:stats_prefixes_btree 0..20";
$rst = "COUNT=1";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert aa:stats_prefixes_map field 6 create 19 0 0"; $val = "datum7";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("aa:stats_prefixes_list", 0, 4, "create 17 0 0");
$cmd = "lop get aa:stats_prefixes_list 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

lop_insert("aa:stats_prefixes_list2", 0, 4, "create 17 0 0");
$cmd = "lop get aa:stats_prefixes_list2 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("aa:stats_prefixes_set", 0, 2, "create 13 0 0");
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");


$cmd = "set bb:stats_prefixes_aa 0 0 2"; $val = "hi"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get bb:stats_prefixes_aa";
$rst = "VALUE bb:stats_prefixes_aa 0 2
hi
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "set bb:stats_prefixes_bb 0 0 3"; $val = "bye"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get bb:stats_prefixes_bb";
$rst = "VALUE bb:stats_prefixes_bb 0 3
bye
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,"
                       . "PREFIX bb itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert bb:stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop count bb:stats_prefixes_btree 0..20";
$rst = "COUNT=1";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,"
                       . "PREFIX bb itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert bb:stats_prefixes_map field 6 create 19 0 0"; $val = "datum7";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,"
                       . "PREFIX bb itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("bb:stats_prefixes_list", 0, 4, "create 17 0 0");
$cmd = "lop get aa:stats_prefixes_list 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

lop_insert("bb:stats_prefixes_list2", 0, 4, "create 17 0 0");
$cmd = "lop get bb:stats_prefixes_list2 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,"
                       . "PREFIX bb itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("bb:stats_prefixes_set", 0, 2, "create 13 0 0");
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,"
                       . "PREFIX bb itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");

# delete
$cmd = "delete aa:stats_prefixes_aa"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete aa:stats_prefixes_bb"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete aa:stats_prefixes_btree"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete aa:stats_prefixes_map"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete aa:stats_prefixes_list"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete aa:stats_prefixes_list2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete aa:stats_prefixes_set"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete bb:stats_prefixes_aa"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bb:stats_prefixes_bb"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bb:stats_prefixes_btree"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bb:stats_prefixes_map"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bb:stats_prefixes_list"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bb:stats_prefixes_list2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bb:stats_prefixes_set"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

#check again
stats_prefixes_is($sock, "");

# after test
release_memcached($engine, $server);
