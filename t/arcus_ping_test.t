#!/usr/bin/perl

use strict;
use Test::More tests => 26;
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
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
    $cmd = "set arcus-ping:qos_kv 0 3 4"; $val = "DATA"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
    mem_get_is($sock, "arcus-ping:qos_kv", "DATA");

    $cmd = "lop insert arcus-ping:qos_lop 0 4 create 0 3 10"; $val = "DATA"; $rst = "CREATED_STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
    lop_get_is($sock, "arcus-ping:qos_lop 0..10",  0, 1, "DATA");
    $cmd = "lop delete arcus-ping:qos_lop 0..10 drop"; $rst = "DELETED_DROPPED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

    $cmd = "sop insert arcus-ping:qos_sop 4 create 0 3 10"; $val = "DATA"; $rst = "CREATED_STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
    $cmd = "sop exist arcus-ping:qos_sop 4"; $val="DATA"; $rst = "EXIST";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
    $cmd = "sop delete arcus-ping:qos_sop 4 drop"; $val="DATA"; $rst = "DELETED_DROPPED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");

    $cmd = "bop insert arcus-ping:qos_bop 1 4 create 0 3 10"; $val = "DATA"; $rst = "CREATED_STORED";
    print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
    bop_get_is($sock, "arcus-ping:qos_bop 1..10", 0, 1, "1", "DATA", "END");
    $cmd = "bop delete arcus-ping:qos_bop 1..10 10 drop"; $rst = "DELETED_DROPPED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

    $cmd = "delete arcus-ping:qos_kv"; $rst = "DELETED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
}

# after test
release_memcached($engine, $server);
