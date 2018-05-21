#!/usr/bin/perl

use strict;
use Test::More tests => 536;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;


my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $msg;

# set foo (and should get it)
$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

# add bar (and should get it)
$cmd = "add bar 0 0 6"; $val = "barval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get bar";
$rst = "VALUE bar 0 6
barval
END";
mem_cmd_is($sock, $cmd, "", $rst);

# add foo (but shouldn't get new value)
$cmd = "add foo 0 0 5"; $val = "foov2"; $rst = "NOT_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

# replace bar (should work)
$cmd = "replace bar 0 0 6"; $val = "barva2"; $rst = "STORED";
$msg = "replaced barval 2";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

# replace notexist (shouldn't work)
$cmd = "replace notexist 0 0 6"; $val = "barva2"; $rst = "NOT_STORED";
$msg = "didn't replace notexist";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

# delete foo.
$cmd = "delete foo"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# delete foo again.  not found this time.
$cmd = "delete foo"; $rst = "NOT_FOUND"; $msg = "deleted foo, but not found";
mem_cmd_is($sock, $cmd, "", $rst);

# add moo
$cmd = "add moo 0 0 6"; $val = "mooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get moo";
$rst = "VALUE moo 0 6
mooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

# check-and-set (cas) failure case, try to set value with incorrect cas unique val
$cmd = "cas moo 0 0 6 0"; $val = "MOOVAL"; $rst = "EXISTS";
$msg = "check and set with invalid id";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);

# test "gets", grab unique ID
my @result = mem_gets($sock, "moo");
ok($result[0] != "", "sock - gets foo1 is not empty");
$cmd = "cas moo 0 0 6 0"; $val = "MOOVAL"; $rst = "EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "cas moo 0 0 6 $result[0]"; $val = "MOOVAL"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get moo";
$rst = "VALUE moo 0 6
MOOVAL
END";
mem_cmd_is($sock, $cmd, "", $rst);

# pipeling is okay
$cmd = "set foo 0 0 6\r\nfooval\r\ndelete foo\r\nset foo 0 0 6\r\nfooval\r\ndelete foo";
$rst = "STORED\n"
     . "DELETED\n"
     . "STORED\n"
     . "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);


# Test sets up to a large size around 1MB.
# Everything up to 1MB - 1k should succeed, everything 1MB +1k should fail.

my $len = 1024;
while ($len < 1024*1028) {
    my $val = "B"x$len;
    if ($len > (1024*1024)) {
        # Ensure causing a memory overflow doesn't leave stale data.
        $cmd = "set foo_$len 0 0 3"; $rst = "STORED";
        mem_cmd_is($sock, $cmd, "MOO", $rst);
        $cmd = "set foo_$len 0 0 $len"; $rst = "SERVER_ERROR object too large for cache";
        $msg = "failed to store size $len";
        mem_cmd_is($sock, $cmd, $val, $rst, $msg);
        $cmd = "get foo_$len";
        $rst = "END";
        mem_cmd_is($sock, $cmd, "", $rst);
    } else {
        $cmd = "set foo_$len 0 0 $len"; $rst = "STORED";
        $msg = "stored size $len";
        mem_cmd_is($sock, $cmd, $val, $rst);
    }
    $len += 2048;
}

# after test
release_memcached($engine, $server);
