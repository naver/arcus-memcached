#!/usr/bin/perl

use strict;
use Test::More tests => 258;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;


my $engine = shift;
my $server = get_memcached($engine, "-f 1.04");
my $sock = $server->sock;

# Test sets up to a large size around 1MB.
# Everything up to 1MB - 1k should succeed, everything 1MB +1k should fail.

my $len = 4096;
while ($len < 1024*1028) {
    my $val = "B"x$len;
    if ($len >= (1024*1024)) {
        # Ensure causing a memory overflow doesn't leave stale data.
        print $sock "set foo_$len 0 0 3\r\nMOO\r\n";
        is(scalar <$sock>, "STORED\r\n");
        print $sock "set foo_$len 0 0 $len\r\n$val\r\n";
        is(scalar <$sock>, "SERVER_ERROR object too large for cache\r\n", "failed to store size $len");
        mem_get_is($sock, "foo_$len");
    } else {
        print $sock "set foo_$len 0 0 $len\r\n$val\r\n";
        is(scalar <$sock>, "STORED\r\n", "stored size $len");
    }
    $len += 4096;
}

# after test
release_memcached($engine);
