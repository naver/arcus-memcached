#!/usr/bin/perl

use strict;
use Test::More tests => 51001;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $flags = 13;
my $default_set_size = 4000;
my $maximum_set_size = 50000;
my $cnt;

sub sop_forced_insert_over_maxcount {
    my ($key, $create) = @_;
    my $index;

    for ($index = 0; $index < $maximum_set_size; $index++) {
        my $val = "datum$index";
        my $vleng = length($val);
        my $cmd;
        my $rst;

        if ($index == 0) {
            $cmd = "sop insert $key $vleng $create force";
            $rst = "CREATED_STORED";
        }

        else {
            $cmd = "sop insert $key $vleng force";
            $rst = "STORED";
        }
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}


sub sop_forced_insert_over_hardlimit{
    my ($key, $from, $to) = @_;
    my $index;

    for($index = $from; $index <= $to; $index++){
        my $val = "datum$index";
        my $vleng = length($val);
        my $cmd = "sop insert $key $vleng force";
        my $rst = "OVERFLOWED";

        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

sop_forced_insert_over_maxcount("skey", "create $flags 0 1");
sop_forced_insert_over_hardlimit("skey", $maximum_set_size, $maximum_set_size+1000);

# after test
release_memcached($engine, $server);
