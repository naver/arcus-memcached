#!/usr/bin/perl

use strict;
use Test::More tests => 2;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;

# key string longer than KEY_MAX_LENGTH
my $key = "a"x32001;

$cmd = "set a 1 0 1"; $val = "a"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "get a $key"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
