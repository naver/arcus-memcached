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
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

print $sock "set foo 0 0 6\r\nfooval\r\n";
is(scalar <$sock>, "STORED\r\n", "stored foo");
mem_get_is($sock, "foo", "fooval");

print $sock "getattr foo expiretime\r\n";
my $line = scalar <$sock> . scalar <$sock>;
like($line, qr/ATTR expiretime=0[^0-9]/, "expiretime=0 ok");

print $sock "delete foo\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted foo");

# Case 2
$cmd = "get foo"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

print $sock "set foo 0 999 6\r\nfooval\r\n";
is(scalar <$sock>, "STORED\r\n", "stored foo");
mem_get_is($sock, "foo", "fooval");

print $sock "getattr foo expiretime\r\n";
my $line = scalar <$sock> . scalar <$sock>;
like($line, qr/ATTR expiretime=9[0-9][0-9][^0-9]/, "expiretime=999 ok");

print $sock "delete foo\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted foo");

# after test
release_memcached($engine, $server);
