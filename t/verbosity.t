#!/usr/bin/perl
use strict;
use Test::More tests => 8;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

print $sock "config verbosity foo bar my\r\n";
is(scalar <$sock>, "CLIENT_ERROR bad command line format\r\n", "Illegal number of arguments");

print $sock "config verbosity noreply\r\n";
is(scalar <$sock>, "CLIENT_ERROR bad command line format\r\n", "Illegal noreply");

print $sock "config verbosity 0\r\n";
is(scalar <$sock>, "END\r\n", "Correct syntax");

my $settings = mem_stats($sock, 'settings');
is('0', $settings->{'verbosity'}, "Verify settings");

print $sock "config verbosity foo\r\n";
is(scalar <$sock>, "CLIENT_ERROR bad command line format\r\n", "Not a numeric argument");

print $sock "config verbosity 1\r\n";
is(scalar <$sock>, "END\r\n", "Correct syntax");

$settings = mem_stats($sock, 'settings');
is('1', $settings->{'verbosity'}, "Verify settings");

print $sock "config verbosity 100\r\n";
is(scalar <$sock>, "SERVER_ERROR cannot change the verbosity over the limit\r\n", "Over the max value");

# after test
release_memcached($engine, $server);
