#!/usr/bin/perl
use strict;
use Test::More tests => 8;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;

$cmd = "config verbosity foo bar my"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst, "Illegal number of arguments");

$cmd = "config verbosity noreply"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst, "Illegal noreply");

$cmd = "config verbosity 0"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst, "Correct syntax");

my $settings = mem_stats($sock, 'settings');
is('0', $settings->{'verbosity'}, "Verify settings");

$cmd = "config verbosity foo"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst, "Not a numeric argument");

$cmd = "config verbosity 1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst,"Correct syntax");

$settings = mem_stats($sock, 'settings');
is('1', $settings->{'verbosity'}, "Verify settings");

$cmd = "config verbosity 100"; $rst = "SERVER_ERROR cannot change the verbosity over the limit";
mem_cmd_is($sock, $cmd, "", $rst, "Over the max value");

# after test
release_memcached($engine, $server);
