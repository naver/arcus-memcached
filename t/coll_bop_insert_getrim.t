#!/usr/bin/perl

use strict;
use Test::More tests => 407;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

# BOP test sub routines
sub bop_insert_getrim_load {
    my ($key, $from_bkey, $to_bkey, $count) = @_;
    my $bkey;
    my $eflag = "";
    my $value;
    my $vleng;
    my $cmd;
    my $msg;

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey; $bkey <= $to_bkey; $bkey = $bkey + 1) {
            $value = "$key-data-$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim\r\n$value\r\n";
            $msg = "bop insert $key $bkey $eflag $vleng getrim: $value";
            print $sock "$cmd"; is(scalar <$sock>, "STORED\r\n", "$msg");
        }
    } else {
        for ($bkey = $from_bkey; $bkey >= $to_bkey; $bkey = $bkey - 1) {
            $value = "$key-data-$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim\r\n$value\r\n";
            $msg = "bop insert $key $bkey $eflag $vleng getrim: $value";
            print $sock "$cmd"; is(scalar <$sock>, "STORED\r\n", "$msg");
        }
    }
}

sub bop_insert_getrim_trim {
    my ($key, $from_bkey, $to_bkey, $count, $flags) = @_;
    my $bkey;
    my $eflag = "";
    my $value;
    my $vleng;
    my $trim_bkey;
    my $trim_value;
    my $trim_vleng;
    my $cmd;
    my $msg;

    if ($from_bkey <= $to_bkey) {
        for ($bkey = $from_bkey; $bkey <= $to_bkey; $bkey = $bkey + 1) {
            $value = "$key-data-$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim\r\n$value\r\n";
            $msg = "bop insert $key $bkey $eflag $vleng getrim: $value";
            $trim_bkey = $bkey - $count;
            $trim_value = "$key-data-$trim_bkey";
            $trim_vleng = length($trim_value);
            print $sock "$cmd";
            is(scalar <$sock>, "VALUE $flags 1\r\n", "head");
            is(scalar <$sock>, "$trim_bkey $trim_vleng $trim_value\r\n", "body");
            is(scalar <$sock>, "TRIMMED\r\n", "tail - $msg");
        }
    } else {
        for ($bkey = $from_bkey; $bkey >= $to_bkey; $bkey = $bkey - 1) {
            $value = "$key-data-$bkey";
            $vleng = length($value);
            $cmd = "bop insert $key $bkey $eflag $vleng getrim\r\n$value\r\n";
            $msg = "bop insert $key $bkey $eflag $vleng getrim: $value";
            $trim_bkey = $bkey - $count;
            $trim_value = "$key-data-$trim_bkey";
            $trim_vleng = length($trim_value);
            print $sock "$cmd";
            is(scalar <$sock>, "VALUE $flags 1\r\n", "head");
            is(scalar <$sock>, "$trim_bkey $trim_vleng $trim_value\r\n", "body");
            is(scalar <$sock>, "TRIMMED\r\n", "tail - $msg");
        }
    }
}

sub assert_bop_get {
    my ($key, $width, $flags, $from_bkey, $to_bkey, $offset, $count, $delete, $tailstr) = @_;
    my $ecnt = 0;
    my $bkey;
    my $data;
    my @res_bkey = ();
    my @res_data = ();

    if ($from_bkey <= $to_bkey) {
        if ($offset eq 0) {
            $bkey = $from_bkey;
        } else {
            $bkey = $from_bkey + ($offset * $width);
        }
        for ( ; $bkey <= $to_bkey; $bkey = $bkey + $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
            $ecnt = $ecnt + 1;
            if (($count > 0) and ($ecnt >= $count)) {
                last;
            }
        }
    } else {
        if ($offset eq 0) {
            $bkey = $from_bkey;
        } else {
            $bkey = $from_bkey - ($offset * $width);
        }
        for ( ; $bkey >= $to_bkey; $bkey = $bkey - $width) {
            $data = "$key-data-$bkey";
            push(@res_bkey, $bkey);
            push(@res_data, $data);
            $ecnt = $ecnt + 1;
            if (($count > 0) and ($ecnt >= $count)) {
                last;
            }
        }
    }
    my $bkey_list = join(",", @res_bkey);
    my $data_list = join(",", @res_data);
    my $args = "$key $from_bkey..$to_bkey $offset $count $delete";
    bop_get_is($sock, $args, $flags, $ecnt, $bkey_list, $data_list, $tailstr);
}


# BOP test global variables
my $count = 100;
my $cmd;
my $val;
my $rst;

$cmd = "get bkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop create bkey 11 0 $count"; $rst = "CREATED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
getattr_is($sock, "bkey count maxcount", "count=0 maxcount=$count");
bop_insert_getrim_load("bkey", 1, $count, $count);
getattr_is($sock, "bkey count maxcount trimmed", "count=$count maxcount=$count trimmed=0");
bop_insert_getrim_trim("bkey", $count+1, $count*2, $count, 11);
getattr_is($sock, "bkey count maxcount trimmed", "count=$count maxcount=$count trimmed=1");
assert_bop_get("bkey", 1, 11, $count+1, $count*2, 0, 0, "", "END");
$cmd = "delete bkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine);
