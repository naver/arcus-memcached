#!/usr/bin/perl

use strict;
use Test::More tests => 109997;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# BOP test sub routines
sub prepare_bop_smget {
    my ($key_cnt, $data_cnt_per_key) = @_;
    my $dat_cnt = ($key_cnt * $data_cnt_per_key);
    my $kcnt;
    my $dcnt;
    my $key;
    for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
        $key = "KEY_$kcnt";
        $cmd = "bop create $key 11 0 0"; $rst = "CREATED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    }
    for ($kcnt = 0, $dcnt = 0; $dcnt < $dat_cnt; $dcnt += 1) {
        $key  = "KEY_$kcnt";
        $val  = "DATA_$dcnt";
        my $len = length($val);
        $cmd = "bop insert $key $dcnt $len"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
        $kcnt += 1;
        if ($kcnt == $key_cnt) {
            $kcnt = 0;
        }
    }
}

sub assert_bop_smget {
    my ($key_len, $key_cnt, $key_str, $min, $max, $from, $to, $offset, $count) = @_;
    my $range = "$from..$to";
    my $res_count;
    my @res_bkey = ();
    my @res_data = ();
    my $bkey;
    my $data;
    my $mis_count = 1;
    my $mis_keys = "KEY_absent NOT_FOUND";
=head
    my $mis_keys = "KEY_absent";
=cut

    my $range_valid = 1;
    if ($from <= $to) {
        if (($from < $min and $to < $min) or ($from > $max)) {
           $range_valid = 0;
        } else {
           if ($from < $min) {
               $from = $min;
           }
           if ($to > $max) {
               $to = $max;
           }
           if ($offset > 0) {
               $from += $offset;
               if ($from > $to) {
                   $range_valid = 0;
               }
           }
        }
        if ($range_valid == 1) {
            $res_count = $to - $from + 1;
            if ($res_count > $count) {
                $res_count = $count;
            }
            for ($bkey = $from; $bkey < ($from + $res_count); $bkey += 1) {
                $data = "DATA_$bkey";
                push(@res_bkey, $bkey);
                push(@res_data, $data);
            }
        }
    } else {
        if (($from > $max and $to > $max) or ($from < $min)) {
            $range_valid = 0;
        } else {
            if ($from > $max) {
                $from = $max;
            }
            if ($to < $min) {
                $to = $min;
            }
            if ($offset > 0) {
                $from -= $offset;
                if ($from < $to) {
                    $range_valid = 0;
                }
            }
        }
        if ($range_valid == 1) {
            $res_count = $from - $to + 1;
            if ($res_count > $count) {
                $res_count = $count;
            }
            for ($bkey = $from; $bkey > ($from - $res_count); $bkey -= 1) {
                $data = "DATA_$bkey";
                push(@res_bkey, $bkey);
                push(@res_data, $data);
            }
        }
    }

    my $args = "$key_len $key_cnt $range $offset $count";
    if ($range_valid == 1) {
        my $bkey_list = join(",", @res_bkey);
        my $data_list = join(",", @res_data);
        bop_smget_is($sock, $args, $key_str,
                     $res_count, "", "", $bkey_list, $data_list, $mis_count, $mis_keys, "END");
    } else {
        bop_smget_is($sock, $args, $key_str,
                     0, "", "", "", "", $mis_count, $mis_keys, "END");
    }
}

# testBOPSMGetComplex
my $key_cnt = 9999;
my $data_cnt_per_key = 10;
my $dat_cnt = ($key_cnt * $data_cnt_per_key);
my $key_str;
my $key_len;
my $kcnt;
prepare_bop_smget($key_cnt, $data_cnt_per_key);
$key_str = "KEY_absent";
for ($kcnt = 0; $kcnt < $key_cnt; $kcnt += 1) {
    $key_str = "$key_str,KEY_$kcnt";
}
$key_len = length($key_str);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, 0, $dat_cnt-1, 0, 20);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, 0, $dat_cnt-1, 10, 20);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, $dat_cnt/2, $dat_cnt-1, 10, 20);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, $dat_cnt-1, 0, 0, 20);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, $dat_cnt-1, 0, 10, 20);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, $dat_cnt/2, 0, 10, 20);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, $dat_cnt, $dat_cnt+100, 0, 20);
assert_bop_smget($key_len, $key_cnt+1, $key_str, 0, $dat_cnt-1, $dat_cnt+100, $dat_cnt, 0, 20);

