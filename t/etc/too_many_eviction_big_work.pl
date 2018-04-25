#!/usr/bin/perl

use strict;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use MemcachedTest;

sleep 1;

my $threads = 3;
my $running = 0;

# default engine start option : -m 512
# example) ./memcached -E .libs/default_engine.so -X .libs/ascii_scrub.so -m 512

while ($running < $threads) {
#    my $sock = IO::Socket::INET->new(PeerAddr => "$server->{host}:$server->{port}");
    my $sock = IO::Socket::INET->new(PeerAddr => "localhost:11211");
    my $cpid = fork();
    if ($cpid) {
        $running++;
        print "Launched $cpid.  Running $running threads.\n";
    } else {
        data_work($sock);
        exit 0;
    }
}

while ($running > 0) {
    wait();
    print "stopped. Running $running threads.\n";
    $running--;
}

sub data_work {
    my $sock = shift;
    my $i;
    my $expire = 86400;
    for ($i = 0; $i < 800; $i++) {
        my $keyrand = int(rand(900000000));
        my $valrand = 7900;
        my $key = "dash$keyrand";
        my $val = "B" x $valrand;
        my $len = length($val);
        sleep(0.01);
        print $sock "set $key 0 $expire $len\r\n$val\r\n";
        my $res = scalar <$sock>;
        if ($res ne "STORED\r\n") {
            print "set $key $len: $res\r\n";
        }
        if (($i % 100) == 99) {
            print "$i added\n";
        }
    }
    print "data_work end\n";
}

#undef $server;
