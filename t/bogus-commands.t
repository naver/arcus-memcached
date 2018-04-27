#!/usr/bin/perl

use strict;
use Test::More tests => 1;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

print $sock "boguscommand slkdsldkfjsd\r\n";
is(scalar <$sock>, "ERROR unknown command\r\n", "got error back");

# after test
release_memcached($engine);
