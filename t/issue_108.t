#!/usr/bin/perl

use strict;
use Test::More tests => 4;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $key = "del_key";
my $cmd;
my $val;
my $rst;
my $msg;

$cmd = "add $key 0 0 1"; $val = "x"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "delete $key 0"; $rst = "DELETED"; $msg = "Properly deleted with 0";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "add $key 0 0 1"; $val = "x"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

print $sock "delete $key 0 noreply\r\n";
# will not reply, but a subsequent add will succeed

$cmd = "add $key 0 0 1"; $val = "x"; $rst = "STORED";
$msg = "Add succeeded after quiet deletion.";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

# after test
release_memcached($engine, $server);
