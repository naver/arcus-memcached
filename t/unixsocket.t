#!/usr/bin/perl

use strict;
use Test::More tests => 3;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $filename = "/tmp/memcachetest$$";

my $engine = shift;
my $server = get_memcached($engine, "-s $filename");
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;

ok(-S $filename, "creating unix domain socket $filename");

# set foo (and should get it)
$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

unlink($filename);

## Just some basic stuff for now...

# after test
release_memcached($engine, $server);
