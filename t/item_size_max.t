#!/usr/bin/perl

use strict;
use Test::More tests => 7;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $stats = mem_stats($sock, ' settings');

# Ensure default still works.
is($stats->{item_size_max}, 1024 * 1024);
$server->stop();

### [ARCUS] CHANGED FOLLOWING TEST ###
# The small memory allocator allocates chunk memory from existing slab allocator.
# And, the chunk memory size is about 17MB.
# Therefore, the minimum item size is changed from 1KB to 20KB.
######################################

# Should die.
### [ARCUS] CHANGED FOLLOWING TEST ###
#eval {
#    $server = get_memcached($engine, '-I 1000');
#};
#ok($@ && $@ =~ m/^Failed/, "Shouldn't start with < 1k item max");
eval {
    $server = get_memcached($engine, '-I 20000');
};
ok($@ && $@ =~ m/^Failed/, "Shouldn't start with < 20k item max");
######################################

eval {
    $server = get_memcached($engine, '-I 256m');
};
ok($@ && $@ =~ m/^Failed/, "Shouldn't start with > 128m item max");

# Minimum.
### [ARCUS] CHANGED FOLLOWING TEST ###
#$server = get_memcached($engine, '-I 1024');
$server = get_memcached($engine, '-I 20480');
my $stats = mem_stats($server->sock, ' settings');
#is($stats->{item_size_max}, 1024);
is($stats->{item_size_max}, 20480);
$server->stop();
######################################

# Reasonable but unreasonable.
$server = get_memcached($engine, '-I 1049600');
my $stats = mem_stats($server->sock, ' settings');
is($stats->{item_size_max}, 1049600);
$server->stop();

# Suffix kilobytes.
$server = get_memcached($engine, '-I 512k');
my $stats = mem_stats($server->sock, ' settings');
is($stats->{item_size_max}, 524288);
$server->stop();

# Suffix megabytes.
$server = get_memcached($engine, '-I 32m');
my $stats = mem_stats($server->sock, ' settings');
is($stats->{item_size_max}, 33554432);
$server->stop();


# after test
release_memcached($engine, $server);
