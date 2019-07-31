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

sub do_bulk_coll_insert {

    my ($collection, $count) = @_;
    my $start_time = time;

    my $cmd = "insert";
    my $key = "mykey";

    for (my $index=0; $index < $count; $index++) {
        my $val = "datum$index";
        my $vleng = length($val);

        my $whole_cmd;

        if ($collection eq "sop") {
            $whole_cmd = "$collection $cmd $key $vleng";
        }

        elsif ($collection eq "bop" or $collection eq "lop") {
            $whole_cmd = "$collection $cmd $key $index $vleng";
        }

        elsif ($collection eq "mop") {
            my $field = "f$index";
            $whole_cmd = "$collection $cmd $key $field $vleng";
        }


        my $rst = "STORED";

        if ($index == 0) {
            my $create = "create 13 0 0";

            if ($collection eq "sop") {
                $whole_cmd = "$collection $cmd $key $vleng $create";
            }

            elsif ($collection eq "bop" or $collection eq "lop") {
                $whole_cmd = "$collection $cmd $key $index $vleng $create";
            }

            elsif ($collection eq "mop") {
                my $field = "f$index";
                $whole_cmd = "$collection $cmd $key $field $vleng $create";
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
request_log("bop insert", 50);
do_bulk_coll_insert("bop", 51);

my $file_handle;
open($file_handle, "cmd_in_second.log") or die "log file not exist\n";
close($file_handle);

sleep(1);
mem_cmd_is($sock, "bop insert mykey 1000 9", "datum1000", "STORED");
=pod

unlink "../cmd_in_second.log";
request_log("mop insert", 10);
do_bulk_coll_insert("mop", 11);

open($file_handle, "cmd_in_second.log") or die "log file not exist\n";
close($file_handle);
=cut

release_memcached($engine, $server);

