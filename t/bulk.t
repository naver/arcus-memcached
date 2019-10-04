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
sub request_log {
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

sub stop_log {
    request_log("bop insert", 1000, $start);
    mem_cmd_is($sock, "cmd_in_second stop", "", "cmd_in_second stopped");
    mem_cmd_is($sock, "cmd_in_second stop", "", "cmd_in_second already stopped");
}

sub do_bulk_coll_insert {


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

        if ($count >= 10 and $count < $bulk_size) {
            if ($index % (int($count/10)) == 0) {
                request_log("mop insert", 1000, $on_logging);
            }
        }

        mem_cmd_is($sock, $whole_cmd, $val, $rst);
    }


    my $end_time = time;

    cmp_ok($end_time - $start_time, "<=", 1000, "all commands are done in a second");
    sleep(1);

    file_check()
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
    mem_cmd_is($sock, "cmd_in_second bop", "", $bad_format);
    mem_cmd_is($sock, "cmd_in_second stop blahblah", "", $bad_format);

    my $unknown = "ERROR unknown command";
    mem_cmd_is($sock, "cmd_in_second bop insert 1000 blahblah", "", $unknown);
    mem_cmd_is($sock, "cmd_in_second bop insert 1000 1000", "", $unknown);

}


sub slow_cmd {

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

sub file_check {

    my $file_handle;
    open($file_handle, "cmd_in_second.log") or die "log file not exist\n";

    if (-s "cmd_in_second.log" == 0) {
        die "empty log";
    }

    my $line_cnt;
    close($file_handle);
}


wrong_cmd_test();
sleep(0.3);

stop_log();

sleep(0.3);
#extremely small cases
request_log("bop insert", 1, $start);
do_bulk_coll_insert("bop", "bkey1", 1);
sleep(0.3);

request_log("bop insert", 9, $start);
do_bulk_coll_insert("bop", "bkey2", 9);

sleep(0.3);
request_log("bop insert", $bulk_size, $start);
do_bulk_coll_insert("bop", "bkey3", $bulk_size);

#slow_cmd();
release_memcached($engine, $server);
