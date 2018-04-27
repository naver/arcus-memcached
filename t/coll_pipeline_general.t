#!/usr/bin/perl

use strict;
use Test::More tests => 28;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
lop insert lkey1 0 6 create 11 0 0 pipe
datum0
lop insert lkey1 0 6 pipe
datum1
lop insert lkey1 0 6 pipe
datum2
lop insert lkey1 0 6 pipe
datum3
lop insert lkey1 0 6 pipe
datum4
lop insert lkey1 0 6 pipe
datum5
lop insert lkey1 0 6 pipe
datum6
lop insert lkey1 0 6
datum7
delete lkey1

lop insert lkey2 0 6 create 11 0 0 pipe
datum0
lop insert lkey2 0 6 pipe
datum1111
lop insert lkey2 0 6 pipe
datum2222
lop insert lkey2 0 9 pipe
datum3333
lop insert lkey2 0 9 pipe
datum4444
lop insert lkey2 0 9 pipe
datum5
lop insert lkey2 0 9
datum6666
delete lkey2
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Case 1
print $sock "lop insert lkey1 0 6 create 11 0 0 pipe\r\ndatum0\r\n";
print $sock "lop insert lkey1 0 6 pipe\r\ndatum1\r\n";
print $sock "lop insert lkey1 0 6 pipe\r\ndatum2\r\n";
print $sock "lop insert lkey1 0 6 pipe\r\ndatum3\r\n";
print $sock "lop insert lkey1 0 6 pipe\r\ndatum4\r\n";
print $sock "lop insert lkey1 0 6 pipe\r\ndatum5\r\n";
print $sock "lop insert lkey1 0 6 pipe\r\ndatum6\r\n";
print $sock "lop insert lkey1 0 6\r\ndatum7\r\n";
is(scalar <$sock>, "RESPONSE   8\r\n",   "pipeline head: 8");
is(scalar <$sock>, "CREATED_STORED\r\n", "CREATED_STORED");
is(scalar <$sock>, "STORED\r\n",         "STORED");
is(scalar <$sock>, "STORED\r\n",         "STORED");
is(scalar <$sock>, "STORED\r\n",         "STORED");
is(scalar <$sock>, "STORED\r\n",         "STORED");
is(scalar <$sock>, "STORED\r\n",         "STORED");
is(scalar <$sock>, "STORED\r\n",         "STORED");
is(scalar <$sock>, "STORED\r\n",         "STORED");
is(scalar <$sock>, "END\r\n",            "pipeline tail: END");
$cmd = "delete lkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

print $sock "lop insert lkey2 0 6 create 11 0 0 pipe\r\ndatum0\r\n";
print $sock "lop insert lkey2 0 6 pipe\r\ndatum1111\r\n";
is(scalar <$sock>, "RESPONSE   2\r\n",                "pipeline head: 2");
is(scalar <$sock>, "CREATED_STORED\r\n",              "CREATED_STORED");
is(scalar <$sock>, "CLIENT_ERROR bad data chunk\r\n", "CLIENT_ERROR bad data chunk");
is(scalar <$sock>, "PIPE_ERROR bad error\r\n",        "pipeline tail: PIPE_ERROR bad error");
is(scalar <$sock>, "ERROR unknown command\r\n", "Remaining characters: 1");
print $sock "lop insert lkey2 0 6 pipe\r\ndatum22222\r\n";
is(scalar <$sock>, "RESPONSE   1\r\n",                "pipeline head: 1");
is(scalar <$sock>, "CLIENT_ERROR bad data chunk\r\n", "CLIENT_ERROR bad data chunk");
is(scalar <$sock>, "PIPE_ERROR bad error\r\n",        "pipeline tail: PIPE_ERROR bad error");
is(scalar <$sock>, "ERROR unknown command\r\n", "Remaining characters: 22");
print $sock "lop insert lkey2 0 9 pipe\r\ndatum3333\r\n";
print $sock "lop insert lkey2 0 9 pipe\r\ndatum4444\r\n";
print $sock "lop insert lkey2 0 9 pipe\r\ndatum5\r\n";
print $sock "lop insert lkey2 0 9 pipe\r\ndatum6666\r\n";
is(scalar <$sock>, "RESPONSE   3\r\n",                "pipeline head: 3");
is(scalar <$sock>, "STORED\r\n",                      "STORED");
is(scalar <$sock>, "STORED\r\n",                      "STORED");
is(scalar <$sock>, "CLIENT_ERROR bad data chunk\r\n", "CLIENT_ERROR bad data chunk");
is(scalar <$sock>, "PIPE_ERROR bad error\r\n",        "pipeline tail: PIPE_ERROR bad error");
is(scalar <$sock>, "ERROR unknown command\r\n", "Remaining characters: op insert lkey2 0 9 pipe");
is(scalar <$sock>, "ERROR unknown command\r\n", "Remaining characters: datum6666");
$cmd = "delete lkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");


# after test
release_memcached($engine);
