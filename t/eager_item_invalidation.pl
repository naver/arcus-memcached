#!/usr/bin/perl

use strict;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

sleep 1;

my $threads = 128;
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
    my $expire;
    for ($i = 0; $i < 5000000; $i++) {
        my $keyrand = int(rand(2000000));
        my $valrand = int(rand(500));
        my $key = "dash$keyrand";
        my $val = "B" x $valrand;
        my $len = length($val);
        my $res;
        my $meth = int(rand(10));
        if (($meth ge 0) and ($meth le 7)) {
            if ($meth le 4) {
                $expire = 30 + int(rand(30));
            } elsif (($meth ge 5) and ($meth le 6)) {
                $expire = 120 + int(rand(300));
            } else {
                $expire = 0;
            }
            print $sock "set $key 0 $expire $len\r\n$val\r\n";
            $res = scalar <$sock>;
            if ($res ne "STORED\r\n") {
                print "set $key $len: $res\r\n";
            }
        } elsif (($meth ge 8) and ($meth le 9)) {
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
