#!/usr/bin/perl

use strict;
use Test::More tests => 243;
use Time::HiRes qw(gettimeofday time);
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

#mem_cmd_is($sock, $cmd, $val, $rst);
#
sub request_log{
    my ($whole_cmd, $cnt) = @_;
    my $rst = "$whole_cmd will be logged";
    mem_cmd_is($sock, "cmd_in_second $whole_cmd $cnt", "", $rst);
}

sub do_bulk_insert {

    my ($collection, $cmd, $count) = @_;
    my $start_time = time;

    my $key = "mykey";

    for (my $index=0; $index < $count; $index++) {
        my $val = "datum$index";
        my $vleng = length($val);

        my $whole_cmd = "$collection $cmd $key $vleng";

        if ($collection eq "bop") {
            $whole_cmd = "$collection $cmd $key $index $vleng";
        }

        my $rst = "STORED";

        if ($index == 0) {
            my $create = "create 13 0 0";

            $whole_cmd = "$collection $cmd $key $vleng $create";

            if($collection eq "bop") {
                $whole_cmd = "$collection $cmd $key $index $vleng $create";
            }

            $rst = "CREATED_STORED";
        }

        mem_cmd_is($sock, $whole_cmd, $val, $rst);

        #sleep(0.001);
    }


    my $end_time = time;

    cmp_ok($end_time - $start_time, "<=", 1000, "1 second");
}

unlink "../cmd_in_second.log";
request_log("bop insert", 2000);
do_bulk_bop_insert("bop", "insert", 2100);

open(my $file_handle, "cmd_in_second.log") or die "log file not exist\n";

release_memcached($engine, $server);
