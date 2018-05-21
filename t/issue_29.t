#!/usr/bin/perl

use strict;
use Test::More tests => 4;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $rst;
my $msg;

$cmd = "set issue29 0 0 0\r\n"; $rst = "STORED"; $msg = "stored issue29";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

my $first_stats  = mem_stats($sock, "slabs");
### [ARCUS] CHANGED FOLLOWING TEST ###
# Arcus uses small memory allocator.
# So, the used chunk is the chunk of the small memory allocator.
#my $first_used = $first_stats->{"1:used_chunks"};

#is(1, $first_used, "Used one");
######################################
my $first_used = $first_stats->{"0:used_chunks"};

is(1, $first_used, "Used one chunk");
######################################

$cmd = "set issue29_b 0 0 0\r\n"; $rst = "STORED"; $msg = "stored issue29_b";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

my $second_stats  = mem_stats($sock, "slabs");
### [ARCUS] CHANGED FOLLOWING TEST ###
# Arcus uses small memory allocator.
#my $second_used = $second_stats->{"1:used_chunks"};

#is(2, $second_used, "Used two")
######################################
my $second_used = $second_stats->{"0:used_chunks"};

is(1, $second_used, "Used still one chunk");
######################################

# after test
release_memcached($engine, $server);
