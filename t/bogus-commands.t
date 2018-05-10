#!/usr/bin/perl

use strict;
use Test::More tests => 1;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $rst;

$cmd = "boguscommand slkdsldkfjsd";
$rst = "ERROR unknown command";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
