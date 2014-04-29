#!/usr/bin/perl

use strict;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use MemcachedTest;

#my $server = new_memcached_engine("squall", '-t 16 -M -m 1');

#sleep 20;
sleep 1;

my $threads = 128;
#my $threads = 32;
#my $threads = 4;
my $running = 0;

while ($running < $threads) {
#    my $sock = IO::Socket::INET->new(PeerAddr => "$server->{host}:$server->{port}");
    my $sock = IO::Socket::INET->new(PeerAddr => "localhost:11211");
    my $cpid = fork();
    if ($cpid) {
        $running++;
        print "Launched $cpid.  Running $running threads.\n";
    } else {
        stress($sock);
        exit 0;
    }
}
while ($running > 0) {
    wait();
    print "stopped. Running $running threads.\n";
    $running--;
}

=head
while (1) {
    if ($running < $threads) {
        my $cpid = fork();
        if ($cpid) {
            $running++;
            print "Launched $cpid.  Running $running threads.\n";
        } else {
            stress();
            exit 0;
        }
    } else {
        wait();
        print "stopped. Running $running threads.\n";
        $running--;
    }
}
=cut


sub stress {
    my $sock = shift;
    my $i;
    for ($i = 0; $i < 5000000; $i++) {
#        my $keyrand = int(rand(1000000000));
#        my $valrand = int(rand(50000));
#        my $keyrand = int(rand(3000000));
        my $keyrand = int(rand(1000000));
        my $valrand = int(rand(5000));
        my $key = "dash$keyrand";
        my $val = "B" x $valrand;
        my $len = length($val);
        my $res;
        my $meth = int(rand(10));
        if (($meth ge 0) and ($meth le 1)) {
            print $sock "set $key 0 0 $len\r\n$val\r\n";
            $res = scalar <$sock>;
            if ($res ne "STORED\r\n") {
                print "set $key $len: $res\r\n";
            }
        } elsif (($meth ge 2) and ($meth le 8)) {
            print $sock "get $key\r\n";
            $res = scalar <$sock>;
            if ($res =~ /^VALUE/) {
                $res .= scalar(<$sock>) . scalar(<$sock>);
            }
#            print $sock "add $key 0 0 $len\r\n$val\r\n";
#            $res = scalar <$sock>;
#            if (($res ne "STORED\r\n") and ($res ne "NOT_STORED\r\n")) {
#                print "add $key $len: $res\r\n";
#            }
        } else {
            print $sock "delete $key\r\n";
            $res = scalar <$sock>;
            if (($res ne "DELETED\r\n") and ($res ne "NOT_FOUND\r\n")) {
                print "delete $key: $res\r\n";
            }
        }
    }
    print "stress end\n";
}

#undef $server;

