#!/usr/bin/perl

use strict;
use Test::More tests => 14;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $expire;
my $cmd;
my $val;
my $rst;
my $msg;

$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "flush_all"; $rst = "OK"; $msg = "did flush_all";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Test flush_all with zero delay.
$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "flush_all 0"; $rst = "OK"; $msg = "did flush_all";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# check that flush_all doesn't blow away items that immediately get set
$cmd = "set foo 0 0 3"; $val = "new"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 3
new
END";
mem_cmd_is($sock, $cmd, "", $rst);

# and the other form, specifying a flush_all time...
my $expire = time() + 2;
$cmd = "flush_all $expire"; $rst = "OK"; $msg = "did flush_all in future";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "set foo 0 0 4"; $val = "1234"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 4
1234
END";
mem_cmd_is($sock, $cmd, "", $rst);
sleep(2.2);
$cmd = "get foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
