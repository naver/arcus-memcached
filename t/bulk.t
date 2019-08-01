#!/usr/bin/perl

use strict;
use Test::More tests => 20781;
use Time::HiRes qw(gettimeofday time);
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;
use Cwd;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $start = 0;
my $on_logging = 1;

my $bulk_size = 3461;

#mem_cmd_is($sock, $cmd, $val, $rst);
#
sub request_log{
    my ($whole_cmd, $cnt, $state) = @_;

    if ($cnt <= 0) {
        my $rst= "CLIENT_ERROR bad command line format";
        mem_cmd_is($sock, "cmd_in_second $whole_cmd $cnt", "", $rst);
        return;
    }

    if ($state == $start){
        my $rst = "$whole_cmd will be logged";
        mem_cmd_is($sock, "cmd_in_second $whole_cmd $cnt", "", $rst);
        return;
    }

    if ($state == $on_logging) {
        my $rst = "cmd in second already started";
        mem_cmd_is($sock, "cmd_in_second $whole_cmd $cnt", "", $rst);
    }
}

sub do_bulk_coll_insert {

    if (-e "cmd_in_second.log") {
        unlink "cmd_in_second.log" or die "can't remove old file\n";
    }

    my ($collection, $key, $count) = @_;
    my $start_time = time;

    my $cmd = "insert";

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

        if ($count >= 10) {
            if ($index % (int($count/10)) == 0) {
                request_log("mop insert", 1000, $on_logging);
            }
        }

        mem_cmd_is($sock, $whole_cmd, $val, $rst) or die;

    }


    my $end_time = time;

    cmp_ok($end_time - $start_time, "<=", 1000, "all commands are done in a second");

    my $file_handle;
    open($file_handle, "cmd_in_second.log") or die "log file not exist\n";
    close($file_handle);
}


sub wrong_cmd_test{

    request_log("sop insert", 0, $start);
    request_log("sop insert", -1, $start);

    my $bad_format = "CLIENT_ERROR bad command line format";
    mem_cmd_is($sock, "cmd_in_second lop upsert 1000", "", $bad_format);
    mem_cmd_is($sock, "cmd_in_second sop upsert 1000", "", $bad_format);
    mem_cmd_is($sock, "cmd_in_second mop upsert 1000", "", $bad_format);
    mem_cmd_is($sock, "cmd_in_second bop cas 1000", "", $bad_format);
    mem_cmd_is($sock, "cmd_in_second bop insert", "", $bad_format);
    mem_cmd_is($sock, "cmd_in_second bop insert blahblah", "", $bad_format);
    mem_cmd_is($sock, "cmd_in_second set 1000 blahblah", "", $bad_format);

    my $unknown = "ERROR unknown command";
    mem_cmd_is($sock, "cmd_in_second bop", "", $unknown);
    mem_cmd_is($sock, "cmd_in_second bop insert 1000 blahblah", "", $unknown);
    mem_cmd_is($sock, "cmd_in_second bop insert 1000 1000", "", $unknown);

}

sub non_bulk_cmd {

    my $request_cnt = 10000;
    request_log("sop exist", $request_cnt, $start);
    unlink "cmd_in_second.log" or die "can't remove old file\n";

    for (my $i=0; $i < 5; $i++) {
        for (my $j = 0; $j < $bulk_size; $j++) {
            my $val = "datum$j";
            my $vleng = length($val);

            mem_cmd_is($sock, "sop exist skey $vleng", $val, "EXIST");
            sleep(0.001);
        }
    }

    if (-e "cmd_in_second.log") {
        die "log made by non-bulk command";
    }
}


wrong_cmd_test();
sleep(3);
#extremely small cases
=pod
request_log("bop insert", 1, $start);
do_bulk_coll_insert("bop", "bkey", 1);
request_log("bop insert", 9, $start);
do_bulk_coll_insert("bop", "bkey2", 9);
=cut

request_log("sop insert", $bulk_size, $start);
do_bulk_coll_insert("sop", "skey", $bulk_size+1);

sleep(1);

#non_bulk_cmd();

release_memcached($engine, $server);
