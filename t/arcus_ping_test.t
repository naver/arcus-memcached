#!/usr/bin/perl

use strict;
use Test::More tests => 32;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get arcus-ping:qos_kv
set arcus-ping:qos_kv 0 3 4
DATA
get arcus-ping:qos_kv
lop insert arcus-ping:qos_lop 0 4 create 0 3 10
DATA
lop get arcus-ping:qos_lop 0..10
lop delete arcus-ping:qos_lop 0..10 drop
sop insert arcus-ping:qos_sop 4 create 0 3 10
DATA
sop exist arcus-ping:qos_sop 4
DATA
sop delete arcus-ping:qos_sop 4 drop
DATA
bop insert arcus-ping:qos_bop 1 4 create 0 3 10
DATA
bop get arcus-ping:qos_bop 1..10
bop delete arcus-ping:qos_bop 1..10 10 drop
delete arcus-ping:qos_kv
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

for (0..1) {
    $cmd = "get arcus-ping:qos_kv"; $rst = "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "set arcus-ping:qos_kv 0 3 4"; $val = "DATA"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    mem_get_is($sock, "arcus-ping:qos_kv", "DATA");

    # lop test
    $cmd = "lop insert arcus-ping:qos_lop 0 4 create 0 3 10"; $val = "DATA"; $rst = "CREATED_STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "lop get arcus-ping:qos_lop 0..10";
    $rst =
    "VALUE 0 1\n"
  . "4 DATA\n"
  . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "lop delete arcus-ping:qos_lop 0..10 drop"; $rst = "DELETED_DROPPED";
    mem_cmd_is($sock, $cmd, "", $rst);

    # sop test
    $cmd = "sop insert arcus-ping:qos_sop 4 create 0 3 10"; $val = "DATA"; $rst = "CREATED_STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "sop exist arcus-ping:qos_sop 4"; $val="DATA"; $rst = "EXIST";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "sop delete arcus-ping:qos_sop 4 drop"; $val="DATA"; $rst = "DELETED_DROPPED";
    mem_cmd_is($sock, $cmd, $val, $rst);


    # mop test
    $cmd = "mop insert arcus-ping:qos_mop 2 4 create 0 3 10"; $val = "DATA"; $rst = "CREATED_STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "mop update arcus-ping:qos_mop 2 10"; $val = "UPDATEDATA"; $rst = "UPDATED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "mop delete arcus-ping:qos_mop 1 1 drop"; $val = "2"; $rst = "DELETED_DROPPED";
    mem_cmd_is($sock, $cmd, $val, $rst);

    # bop test
    $cmd = "bop insert arcus-ping:qos_bop 1 4 create 0 3 10"; $val = "DATA"; $rst = "CREATED_STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
    $cmd = "bop get arcus-ping:qos_bop 1..10";
    $rst =
    "VALUE 0 1\n"
  . "1 4 DATA\n"
  . "END";
    mem_cmd_is($sock, $cmd, "", $rst);
    $cmd = "bop delete arcus-ping:qos_bop 1..10 10 drop"; $rst = "DELETED_DROPPED";
    mem_cmd_is($sock, $cmd, "", $rst);

    $cmd = "delete arcus-ping:qos_kv"; $rst = "DELETED";
    mem_cmd_is($sock, $cmd, "", $rst);
}

# after test
release_memcached($engine, $server);
