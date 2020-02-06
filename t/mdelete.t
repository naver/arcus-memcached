#!/usr/bin/perl

use strict;
use Test::More tests => 3014;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

sub bad_command {

    my $key_str = "key1 key2 key3 key4";
    my $key_len = length($key_str);
    my $key_cnt = 4;

    my $unknown_command = "ERROR unknown command";
    #to many or few tokens
    mem_cmd_is($sock, "mdelete 10 10 10", "", $unknown_command);
    mem_cmd_is($sock, "mdelete 10", "", $unknown_command);

    my $bad_format = "CLIENT_ERROR bad command line format";
    mem_cmd_is($sock, "mdelete $key_len -1", "", $bad_format);
    mem_cmd_is($sock, "mdelete 0.5 $key_len", "", $bad_format);
    mem_cmd_is($sock, "mdelete not_number $key_len", "", $bad_format);

    my $bad_data_chunk = "CLIENT_ERROR bad data chunk";
    #wrong key_count
    mem_cmd_is($sock, "mdelete $key_len 10", $key_str, $bad_data_chunk);
    mem_cmd_is($sock, "mdelete $key_len 2", $key_str, $bad_data_chunk);

    my $too_long_key = 'a' x 35000;
    mem_cmd_is($sock, "mdelete 35011 2", "$too_long_key normal_key", $bad_data_chunk);

    my $bad_value = "CLIENT_ERROR bad value";
    mem_cmd_is($sock, "mdelete $key_len 0", "", $bad_value);
    mem_cmd_is($sock, "mdelete 0 10", "", $bad_value);
    mem_cmd_is($sock, "mdelete 0 0", "", $bad_value);
}

sub prepare_kv_mdelete {
    my ($key_cnt) = @_;

    for (my $kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        my $val = "value_$kcnt";
        my $len = length($val);
        my $cmd = "set key_$kcnt 0 0 $len";
        mem_cmd_is($sock, $cmd, $val, "STORED");
    }
}

sub kv_mdelete_hits {
    my ($key_len, $key_cnt, $key_str) = @_;

    my $rst = "";

    for (my $kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        $rst .= "key_$kcnt DELETED\n";
    }

    my $cmd = "mdelete $key_len $key_cnt";
    mem_cmd_is($sock, $cmd, $key_str, $rst);
}

sub kv_mdelete_not_found {

    my ($key_cnt) = @_;

    my $val = "not_found_0";
    my $rst = "not_found_0 NOT_FOUND\n";

    for (my $kcnt = 1; $kcnt < $key_cnt; $kcnt += 1) {
        $val = "$val not_found_$kcnt";
        $rst .= "not_found_$kcnt NOT_FOUND\n";
    }

    my $key_len = length($val);
    my $cmd = "mdelete $key_len $key_cnt";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

# mixed case(DELETED || NOT_FOUND)
sub kv_mdelete_mixed {
    my ($key_cnt) = @_;

    my $val = "not_found_0";
    my $rst = "not_found_0 NOT_FOUND\n";

    for (my $kcnt = 1; $kcnt < $key_cnt; $kcnt += 1) {
        if ($kcnt % 2 == 0 ) {
            $val = "$val not_found_$kcnt";
            $rst .= "not_found_$kcnt NOT_FOUND\n";
            next;
        }
        $val = "$val key_$kcnt";
        $rst .= "key_$kcnt DELETED\n";
    }

    my $key_len = length($val);
    my $cmd = "mdelete $key_len $key_cnt";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

bad_command();

my $key_cnt = 1000;
my $key_str;
my $key_len;

$key_str = "key_0";
for (my $kcnt = 1; $kcnt < $key_cnt; $kcnt += 1) {
    $key_str = "$key_str key_$kcnt";
}
$key_len = length($key_str);

prepare_kv_mdelete($key_cnt);
kv_mdelete_hits($key_len, $key_cnt, $key_str);

prepare_kv_mdelete($key_cnt);
kv_mdelete_not_found($key_cnt);

prepare_kv_mdelete($key_cnt);
kv_mdelete_mixed($key_cnt);

# after test
release_memcached($engine, $server);
