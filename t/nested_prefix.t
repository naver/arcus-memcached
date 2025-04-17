#!/usr/bin/perl

use strict;
use Test::More tests => 54;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $size;
my $csize;
my $sbsize;
my $count;
my $msg;
my $expire;
my $prefix_size = 3;
my $cprefix_size = 4;
my $subkey_size = 2;
my $total_prefix_size = 3;

sub nested_prefix_insert {
    for ($size = 0; $size < $prefix_size; $size++) {
        $cmd = "set pname$size:foo 0 0 7"; $val = "fooval$size"; $rst = "STORED";
        mem_cmd_is($sock, $cmd, $val, $rst);
        # for ($csize = 0; $csize < $cprefix_size; $csize++) {
        #     $cmd = "set pname$size:cpname$csize:foo 0 0 8"; $val = "fooval$size$csize"; $rst = "STORED";
        #     mem_cmd_is($sock, $cmd, $val, $rst);
        # }
    }
}

sub nested_item_get_hit {
    for ($size = 0; $size < $prefix_size; $size++) {
        $cmd = "get pname$size:foo";
        $rst = "VALUE pname$size:foo 0 7\nfooval$size\nEND";
        mem_cmd_is($sock, $cmd, "", $rst);
        # for ($csize = 0; $csize < $cprefix_size; $csize++) {
        #     $cmd = "get pname$size:cpname$csize:foo";
        #     $rst = "VALUE pname$size:cpname$csize:foo 0 8\nfooval$size$csize\nEND";
        #     mem_cmd_is($sock, $cmd, "", $rst);
        # }
    }
}

sub count_total_prefix_exist {
  print $sock "stats detail dump\r\n";
  my $line = scalar <$sock>;
  $count = 0;

  while ($line =~ /^PREFIX/) {
    $count = $count + 1;
    $line = scalar <$sock>;
  }
  if ($count != $prefix_size) {
    croak("The number of prefixes is incorrect.");
  }
}

sub prefix_flush {
  for ($size = 0; $size < $prefix_size; $size++) {
    $cmd = "flush_prefix pname$size"; $rst = "OK";
    mem_cmd_is($sock, $cmd, "", $rst);
  }
}

sub child_prefix_flush {
  for ($size = 0; $size < $prefix_size; $size++) {
    # for ($csize = 0; $csize < $cprefix_size; $csize++) {
    #   $cmd = "flush_prefix pname$size:cpname$csize"; $rst = "OK";
    #     mem_cmd_is($sock, $cmd, "", $rst);
    # }
  }
}

sub check_all_child_prefixes_flushed {
    for ($size = 0; $size < $prefix_size; $size++) {
        # for ($csize = 0; $csize < $cprefix_size; $csize++) {
        #     $cmd = "get pname$size:cpname$csize:foo";
        #     $rst = "END";
        #     mem_cmd_is($sock, $cmd, "", $rst);
        # }
    }
}

sub check_all_prefixes_flushed {
    for ($size = 0; $size < $prefix_size; $size++) {
        $cmd = "get pname$size:foo";
        $rst = "END";
        mem_cmd_is($sock, $cmd, "", $rst);
    }
}

sub check_prefix_count_0 {
  print $sock "stats detail dump\r\n";
  my $line = scalar <$sock>;
  $count = 0;
  while ($line =~ /^PREFIX/) {
    $count = $count + 1;
    $line = scalar <$sock>;
  }
  if ($count != 0) {
    croak("The number of prefixes is incorrect.");
  }
}

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

# stats nested prefix invalid name test
# $cmd = "set -pname:cpname:foo 0 0 6"; $val = "fooval";
# $rst = "INVALID bad prefix name";
# mem_cmd_is($sock, $cmd, $val, $rst);

# $cmd = "set pname:-cpname:foo 0 0 6"; $val = "fooval";
# $rst = "INVALID bad prefix name";
# mem_cmd_is($sock, $cmd, $val, $rst);

# stats prefix flush test
$cmd = "stats detail on"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);

# stats prefixes flush test
nested_prefix_insert();
nested_item_get_hit();
count_total_prefix_exist();

child_prefix_flush();
check_all_child_prefixes_flushed();
prefix_flush();
check_all_prefixes_flushed();

$cmd = "flush_all"; $rst = "OK"; $msg = "did flush_all";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

# stats prefix detail dump flush test
nested_prefix_insert();
nested_item_get_hit();
count_total_prefix_exist();

prefix_flush();
check_prefix_count_0();

# stats nested prefix operation test

#initialization
$cmd = "flush_all"; $rst = "OK"; $msg = "did flush_all";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "stats detail off"; $rst = "OK"; $msg = "detail collection turned off";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

stats_prefixes_is($sock, "");

$cmd = "set aa:foo 0 0 2"; $val = "hi"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
stats_prefixes_is($sock, "PREFIX aa itm 1 kitm 1 litm 0 sitm 0 mitm 0 bitm 0");

$cmd = "get aa:foo";
$rst = "VALUE aa:foo 0 2
hi
END";
mem_cmd_is($sock, $cmd, "", $rst);

# nested prefix test
# $cmd = "set aa:cc:foo 0 0 3"; $val = "bye"; $rst = "STORED";
# mem_cmd_is($sock, $cmd, $val, $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 2 litm 0 sitm 0 mitm 0 bitm 0");

# $cmd = "get aa:cc:foo";
# $rst = "VALUE aa:cc:foo 0 3
# bye
# END";
# mem_cmd_is($sock, $cmd, "", $rst);

# $cmd = "set aa:dd:foo 0 0 3"; $val = "bye"; $rst = "STORED";
# mem_cmd_is($sock, $cmd, $val, $rst);
# $cmd = "get aa:dd:foo";
# $rst = "VALUE aa:dd:foo 0 3
# bye
# END";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 3 litm 0 sitm 0 mitm 0 bitm 0");

# insert btree
$cmd = "bop insert aa:foo_btree 1 6 create 11 0 0"; $val = "datum9";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 1 litm 0 sitm 0 mitm 0 bitm 1");
# stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 3 litm 0 sitm 0 mitm 0 bitm 1");

$cmd = "bop count aa:foo_btree 0..20";
$rst = "COUNT=1";
mem_cmd_is($sock, $cmd, "", $rst);

# $cmd = "bop insert aa:bb:foo_btree 1 6 create 11 0 0"; $val = "datum9";
# $rst = "CREATED_STORED";
# mem_cmd_is($sock, $cmd, $val, $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 5 kitm 3 litm 0 sitm 0 mitm 0 bitm 2");

# $cmd = "bop count aa:bb:foo_btree 0..20";
# $rst = "COUNT=1";
# mem_cmd_is($sock, $cmd, "", $rst);

# insert map
$cmd = "mop insert aa:foo_map field 6 create 19 0 0"; $val = "datum7";
$rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 1 litm 0 sitm 0 mitm 1 bitm 1");
# stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 3 litm 0 sitm 0 mitm 1 bitm 2");

# $cmd = "mop insert aa:cc:foo_map field 6 create 19 0 0"; $val = "datum7";
# $rst = "CREATED_STORED";
# mem_cmd_is($sock, $cmd, $val, $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 3 litm 0 sitm 0 mitm 2 bitm 2");

# insert list
lop_insert("aa:foo_list", 0, 4, "create 17 0 0");
$cmd = "lop get aa:foo_list 0..-1";
$rst = "VALUE 17 5
6 datum0
6 datum1
6 datum2
6 datum3
6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

# lop_insert("aa:dd:foo_list", 0, 4, "create 17 0 0");
# $cmd = "lop get aa:dd:foo_list 0..-1";
# $rst = "VALUE 17 5
# 6 datum0
# 6 datum1
# 6 datum2
# 6 datum3
# 6 datum4
# END";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 9 kitm 3 litm 2 sitm 0 mitm 2 bitm 2");

# insert set
sop_insert("aa:foo_set", 0, 2, "create 13 0 0");
stats_prefixes_is($sock, "PREFIX aa itm 5 kitm 1 litm 1 sitm 1 mitm 1 bitm 1");
# stats_prefixes_is($sock, "PREFIX aa itm 10 kitm 3 litm 2 sitm 1 mitm 2 bitm 2");

# sop_insert("aa:gg:foo_set", 0, 2, "create 13 0 0");
# stats_prefixes_is($sock, "PREFIX aa itm 11 kitm 3 litm 2 sitm 2 mitm 2 bitm 2");

# delete
$cmd = "delete aa:foo"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 0 litm 1 sitm 1 mitm 1 bitm 1");
# stats_prefixes_is($sock, "PREFIX aa itm 10 kitm 2 litm 2 sitm 2 mitm 2 bitm 2");

# $cmd = "delete aa:cc:foo"; $rst = "DELETED";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 9 kitm 1 litm 2 sitm 2 mitm 2 bitm 2");

# $cmd = "delete aa:dd:foo"; $rst = "DELETED";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 8 kitm 0 litm 2 sitm 2 mitm 2 bitm 2");

$cmd = "delete aa:foo_btree"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 0 litm 1 sitm 1 mitm 1 bitm 0");
# stats_prefixes_is($sock, "PREFIX aa itm 7 kitm 0 litm 2 sitm 2 mitm 2 bitm 1");

# $cmd = "delete aa:bb:foo_btree"; $rst = "DELETED";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 6 kitm 0 litm 2 sitm 2 mitm 2 bitm 0");

$cmd = "delete aa:foo_list"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 0 litm 0 sitm 1 mitm 1 bitm 0");
# stats_prefixes_is($sock, "PREFIX aa itm 5 kitm 0 litm 1 sitm 2 mitm 2 bitm 0");

# $cmd = "delete aa:dd:foo_list"; $rst = "DELETED";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 4 kitm 0 litm 0 sitm 2 mitm 2 bitm 0");

$cmd = "delete aa:foo_map"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "PREFIX aa itm 1 kitm 0 litm 0 sitm 1 mitm 0 bitm 0");
# stats_prefixes_is($sock, "PREFIX aa itm 3 kitm 0 litm 0 sitm 2 mitm 1 bitm 0");

# $cmd = "delete aa:cc:foo_map"; $rst = "DELETED";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "PREFIX aa itm 2 kitm 0 litm 0 sitm 2 mitm 0 bitm 0");

$cmd = "delete aa:foo_set"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
stats_prefixes_is($sock, "");
# stats_prefixes_is($sock, "PREFIX aa itm 1 kitm 0 litm 0 sitm 1 mitm 0 bitm 0");

# $cmd = "delete aa:gg:foo_set"; $rst = "DELETED";
# mem_cmd_is($sock, $cmd, "", $rst);
# stats_prefixes_is($sock, "");

# after test
release_memcached($engine, $server);
