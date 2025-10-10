#!/usr/bin/perl

use strict;
use Test::More tests => 40;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1
get kvkey

set kvkey 0 0 10
1
set kvkey 0 0 10
22

set kvkey 0 0 10
0000000000

set kvkey 0 0 10
11111111111
set kvkey 0 0 10
222222222222

set kvkey 0 10
0000000000
set kvkey 0 0 10 10
0000000000
set kvkey 0 0 10 10 10
0000000000

bop insert bkey1 10 5 create 11 0 0
datum

bop insert bkey1 13 5
333
bop insert bkey1 14 5
4444

bop insert bkey1 15 5
55555

bop insert bkey1 16 5
666666
bop insert bkey1 17 5
7777777
bop insert bkey1 18 5
88888888
bop insert bkey1 19 5
999999999

bop insert bkey1 21
0000000000
bop insert bkey1 22 10
0000000000
bop insert bkey1 23 10 10
0000000000

delete bkey1
delete kvkey
=cut

my $cmd;
my $val;
my $rst;
my $rst2;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

# Initialize
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get kvkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Test
$cmd = "set kvkey 0 0 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10"; $val = "11111111111";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10"; $val = "222222222222";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 10"; $val = "0000000000";
$rst =
"ERROR unknown command
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10 10 10"; $val = "0000000000";
$rst =
"ERROR unknown command
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop insert bkey1 10 5 create 11 0 0"; $val = "datum"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop insert bkey1 15 5"; $val = "55555"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 16 5"; $val = "666666";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 17 5"; $val = "7777777";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 18 5"; $val = "88888888";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 19 5"; $val = "999999999";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop insert bkey1 21"; $val = "0000000000";
$rst =
"CLIENT_ERROR bad command line format
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 22 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 23 10 10"; $val = "0000000000";
$rst =
"CLIENT_ERROR bad command line format
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete kvkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$server->stop();

#########################

my $server = get_memcached($engine, "-X .libs/ascii_scrub.so");
my $sock = $server->sock;

# Initialize
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get kvkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Test
$cmd = "set kvkey 0 0 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "set kvkey 0 0 10"; $val = "11111111111";
$rst =
"CLIENT_ERROR bad data chunk
ERROR no arguments";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10"; $val = "222222222222";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "set kvkey 0 10"; $val = "0000000000";
$rst =
"ERROR unknown command
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 10 10 10"; $val = "0000000000";
$rst =
"ERROR unknown command
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop insert bkey1 10 5 create 11 0 0"; $val = "datum"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop insert bkey1 15 5"; $val = "55555"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 16 5"; $val = "666666";
$rst =
"CLIENT_ERROR bad data chunk
ERROR no arguments";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 17 5"; $val = "7777777";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 18 5"; $val = "88888888";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 19 5"; $val = "999999999";
$rst =
"CLIENT_ERROR bad data chunk
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop insert bkey1 21"; $val = "0000000000";
$rst =
"CLIENT_ERROR bad command line format
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 22 10"; $val = "0000000000"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 23 10 10"; $val = "0000000000";
$rst =
"CLIENT_ERROR bad command line format
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete kvkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$server->stop();

# after test
release_memcached($engine, $server);
