#!/usr/bin/perl

use strict;
use warnings;

use IO::Socket::INET;
use IO::Select;
use POSIX;
use FileHandle;

use Test::More tests => 1;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

use Time::HiRes qw(gettimeofday tv_interval);

my $test_count = 5000;

my $n_threads = 4;

my $engine = shift;

if(@ARGV == 1){
    $n_threads = shift;
}

my $server = get_memcached($engine);

# creating thread
my @childs;

my $read_set = IO::Select->new();

for(my $count = 1; $count <= $n_threads; $count++){
    my $h_from_child = IO::Socket->new();
    my $h_to_parent = IO::Socket->new();
    socketpair($h_from_child, $h_to_parent, AF_UNIX, SOCK_STREAM, PF_UNSPEC);

    my $sock = IO::Socket::INET->new(PeerAddr => "$server->{host}:$server->{port}");

    my $pid = fork();

    if($pid){
        # parent
        push(@childs, $pid);
        shutdown($h_to_parent, 0);

        $read_set->add($h_from_child);

    } elsif($pid == 0){
        # child
        sleep(1);
        shutdown($h_from_child, 1);

        my $ret = stress($count, $sock);
        $h_to_parent->send("$ret\n");

        sleep(1000); # indeed, wait parent kill me
        exit 0;
    } else {
        die "ERROR: couldn't fork.\n";
    }
}

my $child_success = 0;

my $start_time = [gettimeofday];
my $end_time = $n_threads * 2 + 1;

my $elapsed = 0;

while(1){
    my @rh_set = $read_set->can_read(1);

    foreach my $rh (@rh_set){
        my $buf = <$rh>;

        if($buf){
            if($buf == $test_count * 2){
                $child_success++;
            }
        } else {
            # close by peer
            print "ERR: close by peer\n";
        }

        $read_set->remove($rh);
    }

    $elapsed = tv_interval($start_time);
    last if($elapsed > $end_time);
}

# kill children
foreach(@childs) {
   kill 9, $_;
}

is($child_success, $n_threads, "all set/incr operations are completed");
# END

sub stress {
    my $id = shift;
    my $sock = shift;

    my $n_incr_success = 0;
    my $n_incr_fail = 0;

    my $n_set_success = 0;
    my $n_set_fail = 0;

    for(my $i = 0; $i < $test_count; $i++){
        print $sock "incr a 100\r\n";
        my $ret_incr = <$sock>;
        if(defined $ret_incr){
            $n_incr_success++;
        } else {
            $n_incr_fail++;
        }

        print $sock "set a 0 0 1\r\n1\r\n";
        my $ret_set = <$sock>;
        if(defined $ret_set){
            $n_set_success++;
        } else {
            $n_set_fail++;
        }
    }

    print "$id: incr($n_incr_success/$n_incr_fail), set($n_set_success/$n_set_fail)\n";
    return $n_incr_success + $n_set_success;
}

# after test
release_memcached($engine);
