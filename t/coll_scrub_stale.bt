#!/usr/bin/perl

use strict;
use Test::More tests => 110006;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
#my $server = get_memcached($engine, "-m 512 -X .libs/ascii_scrub.so -z 127.0.0.1:2181");
#my $another = get_memcached($engine, "-m 512 -X .libs/ascii_scrub.so -z 127.0.0.1:2181");
=cut
my $engine = shift;
my $server = get_memcached($engine, "-m 512 -X .libs/ascii_scrub.so");
my $sock = $server->sock;
my $kcnt = 10000;
my $dcnt = 10;
my $kidx;
my $didx;
my $val;
my $len;
my $prefix;

$prefix = "PrefixA";
for ($kidx = 0; $kidx < $kcnt; $kidx += 2) {
    print $sock "bop create $prefix:KEY_$kidx 11 0 0\r\n";
    is (scalar <$sock>, "CREATED\r\n", "B+Tree create: $prefix:KEY_$kidx");
    for ($didx = 0; $didx < $dcnt; $didx++) {
        $val = "$prefix:KEY_$kidx DATA_$didx";
        $len = length($val);
        print $sock "bop insert $prefix:KEY_$kidx $didx $len\r\n$val\r\n";
        is(scalar <$sock>, "STORED\r\n", "B+tree insert: $val");
    }
}

$prefix = "PrefixB";
for ($kidx = 1; $kidx < $kcnt; $kidx += 2) {
    print $sock "bop create $prefix:KEY_$kidx 11 0 0\r\n";
    is (scalar <$sock>, "CREATED\r\n", "B+Tree create: $prefix:KEY_$kidx");
    for ($didx = 0; $didx < $dcnt; $didx++) {
        $val = "$prefix:KEY_$kidx DATA_$didx";
        $len = length($val);
        print $sock "bop insert $prefix:KEY_$kidx $didx $len\r\n$val\r\n";
        is(scalar <$sock>, "STORED\r\n", "B+tree insert: $val");
    }
}

$prefix = "PrefixA";
print $sock "flush_prefix $prefix\r\n";
is(scalar <$sock>, "OK\r\n", "did flush_prefix $prefix");

print $sock "scrub stale\r\n";
is (scalar <$sock>, "NOT_SUPPORTED\r\n", "scrub stale NOT supported");
=head
is (scalar <$sock>, "OK\r\n", "scrub stale NOT supported");
sleep(1);
my $stats  = mem_stats($sock, "scrub");
my $status  = $stats->{"scrubber:status"};
my $lastrun = $stats->{"scrubber:last_run"};
my $visited = $stats->{"scrubber:visited"};
my $cleaned = $stats->{"scrubber:cleaned"};
print "status   = $status\r\n";
print "last_run = $lastrun\r\n";
print "visited  = $visited\r\n";
print "cleaned  = $visited\r\n";
=cut

print $sock "scrub\r\n";
is (scalar <$sock>, "OK\r\n", "scrub started");
sleep(1);
my $stats  = mem_stats($sock, "scrub");
my $status  = $stats->{"scrubber:status"};
my $lastrun = $stats->{"scrubber:last_run"};
my $visited = $stats->{"scrubber:visited"};
my $cleaned = $stats->{"scrubber:cleaned"};
print "last_run = $lastrun\r\n";
is ($status,  "stopped", "stopped");
is ($visited, $kcnt, "visited");
is ($cleaned, $kcnt/2, "cleaned");

# after test
release_memcached($engine);
