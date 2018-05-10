#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 5;

use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine, '-X .libs/example_protocol.so');
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;


ok(defined($sock), 'Connection 0');

$cmd = "noop"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "echo foo bar"; $rst = "echo [foo] [bar]";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "echo 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7";
$rst = "echo [1] [2] [3] [4] [5] [6] [7] [8] [9] [0] "
     . "[1] [2] [3] [4] [5] [6] [7] [8] [9] [0] [1] [2] [3] [4] [5] [6] [7]";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "echo 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8";
$rst = "ERROR too many arguments";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
