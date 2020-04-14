#!/usr/bin/perl

use strict;
use Test::More tests => 6;
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
$cmd = "lop insert lkey1 0 6 create 11 0 0 pipe\r\ndatum0\r\n"
     . "lop insert lkey1 0 6 pipe\r\ndatum1\r\n"
     . "lop insert lkey1 0 6 pipe\r\ndatum2\r\n"
     . "lop insert lkey1 0 6 pipe\r\ndatum3\r\n"
     . "lop insert lkey1 0 6 pipe\r\ndatum4\r\n"
     . "lop insert lkey1 0 6 pipe\r\ndatum5\r\n"
     . "lop insert lkey1 0 6 pipe\r\ndatum6\r\n"
     . "lop insert lkey1 0 6\r\ndatum7";
$rst = "RESPONSE 8\n"
     . "CREATED_STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "lop insert lkey2 0 6 create 11 0 0 pipe\r\ndatum0\r\n"
     . "lop insert lkey2 0 6 pipe\r\ndatum1111";
$rst = "RESPONSE 2\n"
     . "CREATED_STORED\n"
     . "CLIENT_ERROR bad data chunk\n"
     . "PIPE_ERROR bad error\n"
     . "ERROR unknown command";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey2 0 6 pipe\r\ndatum22222";
$rst = "RESPONSE 1\n"
     . "CLIENT_ERROR bad data chunk\n"
     . "PIPE_ERROR bad error\n"
     . "ERROR unknown command";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "lop insert lkey2 0 9 pipe\r\ndatum3333\r\n"
     . "lop insert lkey2 0 9 pipe\r\ndatum4444\r\n"
     . "lop insert lkey2 0 9 pipe\r\ndatum5\r\n"
     . "lop insert lkey2 0 9 pipe\r\ndatum6666";
$rst = "RESPONSE 3\n"
     . "STORED\n"
     . "STORED\n"
     . "CLIENT_ERROR bad data chunk\n"
     . "PIPE_ERROR bad error\n"
     . "ERROR unknown command\n"
     . "ERROR unknown command";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete lkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
