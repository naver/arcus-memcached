#!/usr/bin/perl

use strict;
use Test::More tests => 2002;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# BOP test sub routines
sub prepare_kv_mget {
    my ($key_cnt) = @_;
    my $kcnt;
    my $key;

    for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        $key = "megt_test:mget_test_key_$kcnt";
        $val = "megt_test_value_$kcnt";
        my $len = length($val);
        $cmd = "set $key 0 0 $len"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
    }
}

sub assert_kv_mget_old {
    my ($key_len, $key_cnt, $key_str) = @_;
    my $kcnt;
    my $key;

    $rst = "";
    for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        $key = "megt_test:mget_test_key_$kcnt";
        $val = "megt_test_value_$kcnt";
        my $len = length($val);
        $rst = "$rst VALUE $key 0 $len\r\n$val\r\n";
    }
    $rst = "$rst END";

    $cmd = "get $key_str"; $rst = "END";
    print $sock "$cmd\r\n";
    is(scalar <$sock>, "$rst\r\n", "old mget test");
}

sub assert_kv_mget_new {
    my ($key_len, $key_cnt, $key_str) = @_;
    my $kcnt;
    my $key;

    $rst = "";
    for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        $key = "megt_test:mget_test_key_$kcnt";
        $val = "megt_test_value_$kcnt";
        my $len = length($val);
        $rst = "$rst VALUE $key 0 $len\r\n$val\r\n";
    }
    $rst = "$rst END";

    $cmd = "mget $key_len $key_cnt"; $val = "$key_str"; $rst = "END";
    print $sock "$cmd\r\n$val\r\n";
    is(scalar <$sock>, "$rst\r\n", "new mget test");
}

# testKVMGet
my $key_cnt = 2000;
my $key_str;
my $key_len;
my $kcnt;
prepare_kv_mget($key_cnt);
for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
    if ($kcnt == 0) {
        $key_str = "mget_test:mget_test_key_$kcnt";
    } else {
        $key_str = "$key_str mget_test:mget_test_key_$kcnt";
    }
}
$key_len = length($key_str);
# kv mget old
assert_kv_mget_old($key_len, $key_cnt, $key_str);
# kv mget new
assert_kv_mget_new($key_len, $key_cnt, $key_str);
