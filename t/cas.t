#!/usr/bin/perl

use strict;
use Test::More tests => 30;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;


my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $sock2 = $server->new_sock;

my $cmd;
my $val;
my $rst;
my $msg;
my @result;
my @result2;

ok($sock != $sock2, "have two different connections open");
$rst = "CLIENT_ERROR bad command line format";
$cmd = "cas bad blah 0 0 0"; $msg = "bad flags";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "cas bad 0 blah 0 0"; $msg = "bad exp";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "cas bad 0 0 blah 0"; $msg = "bad cas";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "cas bad 0 0 0 blah"; $msg = "bad size";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

# gets foo (should not exist)
$cmd = "gets foo"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# set foo
$cmd = "set foo 0 0 6"; $val = "barval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# gets foo and verify identifier exists
@result = mem_gets($sock, "foo");
$cmd = "gets foo";
$rst =
"VALUE foo 0 6 $result[0]
barval
END";
mem_cmd_is($sock, $cmd, "", $rst);

# cas fail
$cmd = "cas foo 0 0 6 123"; $val = "barva2"; $rst = "EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);

# gets foo - success
@result = mem_gets($sock, "foo");
$cmd = "gets foo";
$rst =
"VALUE foo 0 6 $result[0]
barval
END";
mem_cmd_is($sock, $cmd, "", $rst);

# cas success
$cmd = "cas foo 0 0 6 $result[0]"; $val = "barva2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# cas failure (reusing the same key)
$cmd = "cas foo 0 0 6 $result[0]"; $val = "barva2"; $rst = "EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);

# delete foo
$cmd = "delete foo"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# cas missing
$cmd = "cas foo 0 0 6 $result[0]"; $val = "barva2"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);

# cas empty
# cant parse barval2\r\n
$cmd = "cas foo 0 0 6"; $val = "barva2";
$rst =
"ERROR unknown command
ERROR unknown command";
mem_cmd_is($sock, $cmd, $val, $rst);

# set foo1
$cmd = "set foo1 0 0 1"; $val = "1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
# set foo2
$cmd = "set foo2 0 0 1"; $val = "2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# gets foo1 check
@result = mem_gets($sock, "foo1");
$cmd = "gets foo1";
$rst =
"VALUE foo1 0 1 $result[0]
1
END";
mem_cmd_is($sock, $cmd, "", $rst);

# gets foo2 check
@result2 = mem_gets($sock, "foo2");
$cmd = "gets foo2";
$rst =
"VALUE foo2 0 1 $result2[0]
2
END";
mem_cmd_is($sock, $cmd, "", $rst);

# validate foo1 != foo2
ok($result[0] != $result2[0],"foo1 != foo2 single-gets success");

# multi-gets
$cmd = "gets foo1 foo2";
$rst =
"VALUE foo1 0 1 $result[0]
1
VALUE foo2 0 1 $result2[0]
2
END";

# validate foo1 != foo2
ok($result[0] != $result2[0],"foo1 != foo2 single-gets success");

### simulate race condition with cas

# gets foo1 - success
@result = mem_gets($sock, "foo1");
ok($result[0] != "", "sock - gets foo1 is not empty");

# gets foo2 - success
@result2 = mem_gets($sock2, "foo1");
ok($result2[0] != "","sock2 - gets foo1 is not empty");

$cmd = "cas foo1 0 0 6 $result[0]"; $val = "barva2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "cas foo1 0 0 5 $result2[0]"; $val = "apple"; $rst = "EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);

### bug 15: http://code.google.com/p/memcached/issues/detail?id=15

# set foo
$cmd = "set bug15 0 0 1"; $val = "0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# Check out the first gets.
@result = mem_gets($sock, "bug15");
$cmd = "gets bug15";
$rst =
"VALUE bug15 0 1 $result[0]
0
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Increment
$cmd = "incr bug15 1"; $rst = "1";
mem_cmd_is($sock, $cmd, "", $rst);

# Validate a changed CAS
@result2 = mem_gets($sock, "bug15");
$cmd = "gets bug15";
$rst =
"VALUE bug15 0 1 $result2[0]
1
END";
mem_cmd_is($sock, $cmd, "", $rst);

ok($result[0] != $result2[0], "CAS changed");

# after test
release_memcached($engine, $server);
