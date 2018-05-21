#!/usr/bin/perl

use strict;
use Test::More tests => 258;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;


my $engine = shift;
my $server = get_memcached($engine, "-f 1.04");
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;

# Test sets up to a large size around 1MB.
# Everything up to 1MB - 1k should succeed, everything 1MB +1k should fail.

my $len = 4096;
while ($len < 1024*1028) {
    $val = "B"x$len;
    if ($len >= (1024*1024)) {
        # Ensure causing a memory overflow doesn't leave stale data.
        $cmd = "set foo_$len 0 0 3"; $rst = "STORED";
        mem_cmd_is($sock, $cmd, "MOO", $rst);
        $cmd = "set foo_$len 0 0 $len"; $rst = "SERVER_ERROR object too large for cache";
        mem_cmd_is($sock, $cmd, $val, $rst, "failed to store size $len");
        $cmd = "get foo_$len"; $rst = "END";
        mem_cmd_is($sock, $cmd, "", $rst);
    } else {
        $cmd = "set foo_$len 0 0 $len"; $rst = "STORED";
        mem_cmd_is($sock, $cmd, $val, $rst, "stored size $len");
    }
    $len += 4096;
}

# after test
release_memcached($engine, $server);
