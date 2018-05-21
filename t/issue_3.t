#!/usr/bin/perl

use strict;
use Test::More tests => 11;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $msg;
my $key = "del_key";

$cmd = "delete $key"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete $key 10"; $rst = "CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "add $key 0 0 1"; $val = "x"; $rst = "STORED"; $msg = "Add before a broken delete.";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

$cmd = "delete $key 10 noreply"; $rst = "";
mem_cmd_is($sock, $cmd, "", $rst);
# Does not reply
# is (scalar <$sock>, "ERROR\r\n", "Even more invalid delete");

$cmd = "add $key 0 0 1"; $val = "x"; $rst = "NOT_STORED";
$msg = "Failed to add after failed silent delete.";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

$cmd = "delete $key noreply"; $rst = "";
mem_cmd_is($sock, $cmd, "", $rst);
# Will not reply, so let's do a set and check that.

$cmd = "set $key 0 0 1"; $val = "x"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "delete $key"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set $key 0 0 1"; $val = "x"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "delete $key noreply"; $rst = "";
mem_cmd_is($sock, $cmd, "", $rst);

# will not reply, but a subsequent add will succeed

$cmd = "add $key 0 0 1"; $val = "x"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# after test
release_memcached($engine, $server);
