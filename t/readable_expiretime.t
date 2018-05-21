#!/usr/bin/perl

use strict;
use Test::More tests => 10;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get foo
set foo 0 0 6
fooval
getattr foo expiretime
delete foo

get foo
set foo 0 999 6
fooval
getattr foo expiretime
delete foo
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Case 1
$cmd = "get foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "getattr foo expiretime";
$rst = "ATTR expiretime=0
END";
mem_cmd_is($sock, $cmd, "", $rst, "expiretime=0 ok");

$cmd = "delete foo"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# Case 2
$cmd = "get foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "set foo 0 999 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "getattr foo expiretime";
$rst = "ATTR expiretime=999
END";
mem_cmd_is($sock, $cmd, "", $rst, "expiretime=999 ok");

$cmd = "delete foo"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
