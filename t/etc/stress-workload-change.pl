#!/usr/bin/perl

use strict;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use MemcachedTest;

#my $server = new_memcached('-m 1024');

my $threads = 256;
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


sub stress {
    my $sock = shift;
    my $i;
    for ($i = 0; $i < 60; $i++) {
        my $keyrand = int(rand(1000000));
        my $valrand = 50000;
        my $key = "key1$keyrand";
        my $val = "B" x $valrand;
        my $len = length($val);
        my $res;
        print $sock "set $key 0 0 $len\r\n$val\r\n";
        $res = scalar <$sock>;
        if ($res ne "STORED\r\n") {
            print "set $key $len: $res\r\n";
        }
    }
    for ($i = 0; $i < 2000; $i++) {
        my $keyrand = int(rand(1000000));
        my $valrand = 200;
        my $key = "key2$keyrand";
        my $val = "B" x $valrand;
        my $len = length($val);
        my $res;
        print $sock "set $key 0 0 $len\r\n$val\r\n";
        $res = scalar <$sock>;
        if ($res ne "STORED\r\n") {
            print "set $key $len: $res\r\n";
        }
    }
    for ($i = 0; $i < 5000; $i++) {
        my $keyrand = int(rand(1000000));
        my $valrand = int(rand(5000));
        my $key = "dash$keyrand";
        my $val = "B" x $valrand;
        my $len = length($val);
        my $res;
        print $sock "set $key 0 0 $len\r\n$val\r\n";
        $res = scalar <$sock>;
        if ($res ne "STORED\r\n") {
            print "set $key $len: $res\r\n";
        }
    }
    print "stress end\n";
}

