#!/usr/bin/perl

use strict;
use Test::More tests => 15;
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

lop create 0 0 0
lop insert lkey3 4 pipe
data
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

mem_cmd_is($sock, "lop create lkey3 0 0 0", "", "CREATED");
my $max_pipe_operation = 500;

$cmd = "";
for (my $i = 0; $i < $max_pipe_operation; $i++) {
    $cmd .= "lop insert lkey3 0 4 pipe\r\ndata\r\n";
}
$cmd .= "lop insert lkey3 0 4\r\ndata";

$rst = "RESPONSE 500\n";
for (my $i = 0; $i < $max_pipe_operation; $i++) {
    $rst .= "STORED\n";
}
$rst .= "PIPE_ERROR command overflow";

mem_cmd_is($sock, $cmd, "", $rst);

# PR#622 TEST : "FIX: clear pipe_state at the end of the pipelining to avoid swallowing the next command."
# old server swallows the next command after pipelining error
$cmd = "lop insert lkey3 0 9 pipe\r\ndatum3333\r\n"
    . "lop insert lkey3 0 9 pipe\r\ndatum4444\r\n"
    . "lop insert lkey3 0 x";
$rst = "RESPONSE 3\n"
    . "STORED\n"
    . "STORED\n"
    . "CLIENT_ERROR bad command line format\n"
    . "PIPE_ERROR bad error";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "lop insert lkey3 0 9\r\ndatum3333";
$rst = "STORED";
mem_cmd_is($sock, $cmd, "", $rst);

# Failure test of single line command pipelining
$cmd = "bop insert bkey1 10 1 create 11 0 0 pipe\r\n1\r\n"
     . "bop insert bkey1 20 1 pipe\r\n2\r\n"
     . "bop insert bkey1 30 1 pipe\r\n3\r\n"
     . "bop insert bkey1 40 1 pipe\r\n4\r\n"
     . "bop insert bkey1 50 1\r\n5";
$rst = "RESPONSE 5\n"
     . "CREATED_STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "STORED\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 5\n"
     . "10 1 1\n"
     . "20 1 2\n"
     . "30 1 3\n"
     . "40 1 4\n"
     . "50 1 5\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop incr bkey1 10 10 pipe\r\n"
     . "bop incr bkey1 20 10 pipe\r\n"
     . "bop incr bkey1 30 data_10 pipe\r\n"
     . "bop incr bkey1 40 10 pipe\r\n"
     . "bop incr bkey1 50 10";
$rst = "RESPONSE 3\n"
     . "11\n"
     . "12\n"
     . "CLIENT_ERROR bad command line format\n"
     . "PIPE_ERROR bad error";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 5\n"
     . "10 2 11\n"
     . "20 2 12\n"
     . "30 1 3\n"
     . "40 1 4\n"
     . "50 1 5\n"
     . "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);


# after test
release_memcached($engine, $server);
