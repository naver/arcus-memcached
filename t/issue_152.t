#!/usr/bin/perl

use strict;
use Test::More tests => 2;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

# key string longer than KEY_MAX_LENGTH
my $key = "a"x32001;

print $sock "set a 1 0 1\r\na\r\n";
is (scalar <$sock>, "STORED\r\n", "Stored key");

print $sock "get a $key\r\n";
is (scalar <$sock>, "CLIENT_ERROR bad command line format\r\n", "illegal key");

# after test
release_memcached($engine);
