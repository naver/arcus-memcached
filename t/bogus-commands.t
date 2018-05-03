#!/usr/bin/perl

use strict;
use Test::More tests => 3;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $rst;
my $msg;

$cmd = "boguscommand slkdsldkfjsd\r\n";
$rst = "ERROR unknown command";
$msg = "got error back";

# case 1
mem_cmd_is($sock, $cmd, $rst, $msg);

# case 2
mem_cmd_is($sock, $cmd, $rst);

print $sock "boguscommand slkdsldkfjsd\r\n";
is(scalar <$sock>, "ERROR unknown command\r\n", "got error back");

# after test
release_memcached($engine, $server);
