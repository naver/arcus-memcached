#!/usr/bin/perl

use strict;
use Test::More tests => 6002;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
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
        $key = "mget_test:mget_test_key_$kcnt";
        $val = "mget_test_value_$kcnt";
        my $len = length($val);
        $cmd = "set $key 0 0 $len"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
    }
}

sub assert_kv_mget_old {
    my ($key_len, $key_cnt, $key_str) = @_;
    my $kcnt;

    # old kv mget command
    print $sock "get $key_str\r\n";

    for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        my $kstr = "mget_test:mget_test_key_$kcnt";
        my $data = "mget_test_value_$kcnt";
        my $vlen = length($data);
        my $head = scalar <$sock>;
        my $body = scalar <$sock>;
        Test::More::is("$head $body", "VALUE $kstr 0 $vlen\r\n $data\r\n", "mget $kcnt item");
    }
    my $tail = scalar <$sock>;
    Test::More::is("$tail", "END\r\n", "mget END");
}

sub assert_kv_mget_new {
    my ($key_len, $key_cnt, $key_str) = @_;
    my $kcnt;

    # new kv mget command
    print $sock "mget $key_len $key_cnt\r\n$key_str\r\n";

    for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        my $kstr = "mget_test:mget_test_key_$kcnt";
        my $data = "mget_test_value_$kcnt";
        my $vlen = length($data);
        my $head = scalar <$sock>;
        my $body = scalar <$sock>;
        Test::More::is("$head $body", "VALUE $kstr 0 $vlen\r\n $data\r\n", "mget $kcnt item");
    }
    my $tail = scalar <$sock>;
    Test::More::is("$tail", "END\r\n", "mget END");
}

# testKVMGet
my $key_cnt = 2000;
my $key_str;
my $key_len;
my $kcnt;

prepare_kv_mget($key_cnt);

$key_str = "mget_test:mget_test_key_0";
for ($kcnt = 1; $kcnt < $key_cnt; $kcnt += 1) {
    $key_str = "$key_str mget_test:mget_test_key_$kcnt";
}
$key_len = length($key_str);

# kv mget old
assert_kv_mget_old($key_len, $key_cnt, $key_str);

# kv mget new
assert_kv_mget_new($key_len, $key_cnt, $key_str);

# after test
release_memcached($engine);
