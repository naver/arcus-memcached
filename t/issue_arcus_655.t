#!/usr/bin/perl

use strict;
use Test::More tests => 2;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $expire = time() - 1; # immediate expiration
my $cmd = "bop insert bkey 0 6 create 0 $expire 10";
my $val = "datum0";
my $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop get bkey 0";
$rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);

release_memcached($engine, $server)
