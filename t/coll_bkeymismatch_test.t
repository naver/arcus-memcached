#!/usr/bin/perl

use strict;
use Test::More tests => 17;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1
bop create bkey1 1 0 0
setattr bkey1 maxbkeyrange=0x1000
bop insert bkey1 1 6
datum2
delete bkey1

bop create bkey1 10 0 0
bop insert bkey1 1 6
datum2
setattr bkey1 maxbkeyrange=0x1000
delete bkey1

bop create bkey1 1 0 0
setattr bkey1 maxbkeyrange=10
bop insert bkey1 0x0001 6
datum2
delete bkey1

bop create bkey1 10 0 0
bop insert bkey1 0x0011 6
datum2
setattr bkey1 maxbkeyrange=10
delete bkey1
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "bop create bkey1 1 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey1 maxbkeyrange=0x1000"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 1 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop create bkey1 10 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 1 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 maxbkeyrange=0x1000"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);


$cmd = "bop create bkey1 1 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey1 maxbkeyrange=10"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0001 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop create bkey1 10 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0011 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 maxbkeyrange=10"; $rst = "ATTR_ERROR bad value";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
