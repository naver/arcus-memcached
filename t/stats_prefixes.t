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
    my ($key, $data_prefix, $count, $create, $flags, $exptime, $maxcount) = @_;
    my $i=0;
    my $value = "$data_prefix$i";
    my $vleng = length($value);
    my $cmd;
    my $msg;

    $cmd = "lop insert $key $i $vleng $create $flags $exptime $maxcount\r\n$value\r\n";
    $msg = "lop insert $key $i $vleng $create $flags $exptime $maxcount : $value";
    print $sock "$cmd";
    is(scalar <$sock>, "CREATED_STORED\r\n", "$msg");

    for ($i = 1; $i < $count; $i++) {
        $value = "$data_prefix$i";
        $vleng = length($value);
        $cmd = "lop insert $key $i $vleng\r\n$value\r\n";
        $msg = "lop insert $key $i $vleng : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "STORED\r\n", "$msg");
    }
}

sub sop_insert {
    my ($key, $from, $to, $create, $flags, $exptime, $maxcount) = @_;
    my $i=$from;
    my $value = "datum$i";
    my $vleng = length($value);
    my $cmd;
    my $msg;

    $cmd = "sop insert $key $vleng $create $flags $exptime $maxcount\r\n$value\r\n";
    $msg = "sop insert $key $vleng $create $flags $exptime $maxcount : $value";
    print $sock "$cmd";
    is(scalar <$sock>, "CREATED_STORED\r\n", "$msg");

    for ($i = $from+1; $i <= $to; $i++) {
        $value = "datum$i";
        $vleng = length($value);
        $cmd = "sop insert $key $vleng\r\n$value\r\n";
        $msg = "sop insert $key $vleng : $value";
        print $sock "$cmd";
        is(scalar <$sock>, "STORED\r\n", "$msg");
    }
}

## Insert some items without prefix
#initial stats
stats_prefixes_is($sock, "");

print $sock "set stats_prefixes_aa 0 0 2\r\nhi\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a");
mem_get_is($sock, "stats_prefixes_aa", "hi");
print $sock "set stats_prefixes_bb 0 0 3\r\nbye\r\n";
is(scalar <$sock>, "STORED\r\n", "stored b");
mem_get_is($sock, "stats_prefixes_bb", "bye");
stats_prefixes_is($sock, "PREFIX <null> itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count stats_prefixes_btree 0..20"; $rst = "COUNT=1";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
stats_prefixes_is($sock, "PREFIX <null> itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert stats_prefixes_map field 6 create 19 0 0"; $val = "datum7"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
stats_prefixes_is($sock, "PREFIX <null> itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("stats_prefixes_list", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "stats_prefixes_list 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");

lop_insert("stats_prefixes_list2", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "stats_prefixes_list2 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");
stats_prefixes_is($sock, "PREFIX <null> itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("stats_prefixes_set", 0, 2, "create", 13, 0, 0);
stats_prefixes_is($sock, "PREFIX <null> itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");


# delete
print $sock "delete stats_prefixes_aa\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted stats_prefixes_aa");
stats_prefixes_is($sock, "PREFIX <null> itm 6 kitm 1 litm 2 sitm 1 mitm 1 bitm 1");

print $sock "delete stats_prefixes_bb\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted stats_prefixes_bb");
stats_prefixes_is($sock, "PREFIX <null> itm 5 kitm 0 litm 2 sitm 1 mitm 1 bitm 1");

print $sock "delete stats_prefixes_btree\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted stats_prefixes_btree");
stats_prefixes_is($sock, "PREFIX <null> itm 4 kitm 0 litm 2 sitm 1 mitm 1 bitm 0");

print $sock "delete stats_prefixes_list\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted stats_prefixes_list");
stats_prefixes_is($sock, "PREFIX <null> itm 3 kitm 0 litm 1 sitm 1 mitm 1 bitm 0");

print $sock "delete stats_prefixes_list2\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted stats_prefixes_list2");
stats_prefixes_is($sock, "PREFIX <null> itm 2 kitm 0 litm 0 sitm 1 mitm 1 bitm 0");

print $sock "delete stats_prefixes_set\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted stats_prefixes_set");
stats_prefixes_is($sock, "PREFIX <null> itm 1 kitm 0 litm 0 sitm 0 mitm 1 bitm 0");

print $sock "delete stats_prefixes_map\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted stats_prefixes_map");
#check again
stats_prefixes_is($sock, "");



## Insert some items have prefix
#initial stats
stats_prefixes_is($sock, "");

print $sock "set aa:stats_prefixes_aa 0 0 2\r\nhi\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a");
mem_get_is($sock, "aa:stats_prefixes_aa", "hi");
print $sock "set aa:stats_prefixes_bb 0 0 3\r\nbye\r\n";
is(scalar <$sock>, "STORED\r\n", "stored b");
mem_get_is($sock, "aa:stats_prefixes_bb", "bye");
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert aa:stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count aa:stats_prefixes_btree 0..20"; $rst = "COUNT=1";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert aa:stats_prefixes_map field 6 create 19 0 0"; $val = "datum7"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("aa:stats_prefixes_list", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "aa:stats_prefixes_list 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");

lop_insert("aa:stats_prefixes_list2", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "aa:stats_prefixes_list2 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");
stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("aa:stats_prefixes_set", 0, 2, "create", 13, 0, 0);
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");


# delete
print $sock "delete aa:stats_prefixes_aa\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_aa");
stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 1 litm 2 sitm 1 mitm 1 bitm 1");

print $sock "delete aa:stats_prefixes_bb\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_bb");
stats_prefixes_is($sock, "PREFIX aa itm 5 kitm 0 litm 2 sitm 1 mitm 1 bitm 1");

print $sock "delete aa:stats_prefixes_btree\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_btree");
stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 0 litm 2 sitm 1 mitm 1 bitm 0");

print $sock "delete aa:stats_prefixes_list\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_list");
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 0 litm 1 sitm 1 mitm 1 bitm 0");

print $sock "delete aa:stats_prefixes_list2\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_list2");
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 0 litm 0 sitm 1 mitm 1 bitm 0");

print $sock "delete aa:stats_prefixes_set\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_set");
stats_prefixes_is($sock, "PREFIX aa itm 1 kitm 0 litm 0 sitm 0 mitm 1 bitm 0");

print $sock "delete aa:stats_prefixes_map\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_map");
#check again
stats_prefixes_is($sock, "");



## Insert some items have prefix
#initial stats
stats_prefixes_is($sock, "");

print $sock "set aa:stats_prefixes_aa 0 0 2\r\nhi\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a");
mem_get_is($sock, "aa:stats_prefixes_aa", "hi");
print $sock "set aa:stats_prefixes_bb 0 0 3\r\nbye\r\n";
is(scalar <$sock>, "STORED\r\n", "stored b");
mem_get_is($sock, "aa:stats_prefixes_bb", "bye");
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert aa:stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count aa:stats_prefixes_btree 0..20"; $rst = "COUNT=1";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert aa:stats_prefixes_map field 6 create 19 0 0"; $val = "datum7"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("aa:stats_prefixes_list", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "aa:stats_prefixes_list 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");

lop_insert("aa:stats_prefixes_list2", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "aa:stats_prefixes_list2 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");
stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("aa:stats_prefixes_set", 0, 2, "create", 13, 0, 0);
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");


print $sock "set bb:stats_prefixes_aa 0 0 2\r\nhi\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a");
mem_get_is($sock, "bb:stats_prefixes_aa", "hi");
print $sock "set bb:stats_prefixes_bb 0 0 3\r\nbye\r\n";
is(scalar <$sock>, "STORED\r\n", "stored b");
mem_get_is($sock, "bb:stats_prefixes_bb", "bye");
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,PREFIX bb itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert bb:stats_prefixes_btree 1 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop count bb:stats_prefixes_btree 0..20"; $rst = "COUNT=1";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,PREFIX bb itm 3 kitm 2 litm 0 sitm 0 mitm 0 bitm 1");

# insert map
$cmd = "mop insert bb:stats_prefixes_map field 6 create 19 0 0"; $val = "datum7"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,PREFIX bb itm 4 kitm 2 litm 0 sitm 0 mitm 1 bitm 1");

# insert list
lop_insert("bb:stats_prefixes_list", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "aa:stats_prefixes_list 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");

lop_insert("bb:stats_prefixes_list2", "datum", 5, "create", 17, 0, 0);
lop_get_is($sock, "bb:stats_prefixes_list2 0..-1",  17, 5, "datum0,datum1,datum2,datum3,datum4");
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,PREFIX bb itm 6 kitm 2 litm 2 sitm 0 mitm 1 bitm 1");

# insert set
sop_insert("bb:stats_prefixes_set", 0, 2, "create", 13, 0, 0);
stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1,PREFIX bb itm 7 kitm 2 litm 2 sitm 1 mitm 1 bitm 1");


# delete
print $sock "delete aa:stats_prefixes_aa\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_aa");
print $sock "delete aa:stats_prefixes_bb\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_bb");
print $sock "delete aa:stats_prefixes_btree\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_btree");
print $sock "delete aa:stats_prefixes_map\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_map");
print $sock "delete aa:stats_prefixes_list\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_list");
print $sock "delete aa:stats_prefixes_list2\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_list2");
print $sock "delete aa:stats_prefixes_set\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted aa:stats_prefixes_set");

print $sock "delete bb:stats_prefixes_aa\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted bb:stats_prefixes_aa");
print $sock "delete bb:stats_prefixes_bb\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted bb:stats_prefixes_bb");
print $sock "delete bb:stats_prefixes_btree\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted bb:stats_prefixes_btree");
print $sock "delete bb:stats_prefixes_map\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted bb:stats_prefixes_map");
print $sock "delete bb:stats_prefixes_list\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted bb:stats_prefixes_list");
print $sock "delete bb:stats_prefixes_list2\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted bb:stats_prefixes_list2");
print $sock "delete bb:stats_prefixes_set\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted bb:stats_prefixes_set");

#check again
stats_prefixes_is($sock, "");


# after test
release_memcached($engine, $server);
