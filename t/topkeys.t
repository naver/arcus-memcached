#!/usr/bin/perl

use strict;
use Test::More tests => 252;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server;
my $sock;
my $cmd;
my $val;
my $rst;

delete $ENV{"MEMCACHED_TOP_KEYS"};
$server = get_memcached($engine);
$sock = $server->sock;

$cmd = "stats topkeys"; $rst = "NOT_SUPPORTED";
mem_cmd_is($sock, $cmd, "", $rst, "No topkeys without command line option.");

$ENV{"MEMCACHED_TOP_KEYS"} = "100";
$server = get_memcached($engine);
$sock = $server->sock;

$cmd = "stats topkeys"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst, "No top keys yet.");

# Do some operations

$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

sub parse_stats {
    my ($stats) = @_;
    my %ret = ();
    my $key;
    foreach $key (keys %$stats) {
        my %h = split /[,=]/,$stats->{$key};
        $ret{$key} = \%h;
    }
    return \%ret;
}


my $stats = parse_stats(mem_stats($sock, 'topkeys'));

is($stats->{'foo'}->{'cmd_set'}, '1');
is($stats->{'foo'}->{'get_hits'}, '1');

foreach my $key (qw(get_misses incr_hits incr_misses decr_hits decr_misses delete_hits delete_misses evictions)) {
    is($stats->{'foo'}->{$key}, 0, "all stats except cmd_set are zero");
}

$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set bar 0 0 6"; $val = "barval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get bar";
$rst = "VALUE bar 0 6
barval
END";
mem_cmd_is($sock, $cmd, "", $rst);

$stats = parse_stats(mem_stats($sock, 'topkeys'));

is($stats->{'foo'}->{'cmd_set'}, '2');
is($stats->{'bar'}->{'cmd_set'}, '1');

$cmd = "delete foo"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$stats = parse_stats(mem_stats($sock, 'topkeys'));
is($stats->{'foo'}->{'delete_hits'}, 1);
is($stats->{'foo'}->{'delete_misses'}, 0);
is($stats->{'foo'}->{'cmd_set'}, 2);

sub check_incr_stats {
    my ($key, $ih, $im, $dh, $dm) = @_;
    my $stats = parse_stats(mem_stats($sock, 'topkeys'));

    is($stats->{$key}->{'incr_hits'}, $ih);
    is($stats->{$key}->{'incr_misses'}, $im);
    is($stats->{$key}->{'decr_hits'}, $dh);
    is($stats->{$key}->{'decr_misses'}, $dm);
}

$cmd = "incr i 1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst, "shouldn't incr a missing thing");
check_incr_stats("i", 0, 1, 0, 0);

$cmd = "decr d 1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst, "shouldn't decr a missing thing");
check_incr_stats("d", 0, 0, 0, 1);

$cmd = "set n 0 0 1"; $val = "0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "incr n 3"; $rst = "3";
mem_cmd_is($sock, $cmd, "", $rst, "incr works");
check_incr_stats("n", 1, 0, 0, 0);

$cmd = "decr n 1"; $rst = "2";
mem_cmd_is($sock, $cmd, "", $rst, "decr works");
check_incr_stats("n", 1, 0, 1, 0);

$cmd = "decr n 1"; $rst = "1";
mem_cmd_is($sock, $cmd, "", $rst, "decr works");
check_incr_stats("n", 1, 0, 2, 0);

my $i;
# Make sure older keys fall out of the LRU
for ($i = 0; $i < 200; $i++) {
    $cmd = "set foo$i 0 0 6"; $val = "fooval"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
}

$stats = parse_stats(mem_stats($sock, 'topkeys'));
is($stats->{'foo99'}->{'cmd_set'}, undef);
is($stats->{'foo100'}->{'cmd_set'}, 1);
is($stats->{'foo199'}->{'cmd_set'}, 1);

# after test
release_memcached($engine, $server);
