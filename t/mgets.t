#!/usr/bin/perl

use strict;
use Test::More tests => 14;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# set
$cmd = "set key1 0 0 6"; $val = "value1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set key2 0 0 6"; $val = "value2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set key3 0 0 6"; $val = "value3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set key4 0 0 6"; $val = "value4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set key5 0 0 6"; $val = "value5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);


# mgets (NEW command)
$cmd = "mgets 24 5"; $val = "key1 key2 key3 key4 key5";
$rst = "VALUE key1 0 6 1
value1
VALUE key2 0 6 2
value2
VALUE key3 0 6 3
value3
VALUE key4 0 6 4
value4
VALUE key5 0 6 5
value5
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mgets 24 5"; $val = "key5 key4 key3 key2 key1";
$rst = "VALUE key5 0 6 5
value5
VALUE key4 0 6 4
value4
VALUE key3 0 6 3
value3
VALUE key2 0 6 2
value2
VALUE key1 0 6 1
value1
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mgets 29 6"; $val = "key1 key2 key3 key4 key5 key6";
$rst = "VALUE key1 0 6 1
value1
VALUE key2 0 6 2
value2
VALUE key3 0 6 3
value3
VALUE key4 0 6 4
value4
VALUE key5 0 6 5
value5
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mgets 29 6"; $val = "key6 key5 key4 key3 key2 key1";
$rst = "VALUE key5 0 6 5
value5
VALUE key4 0 6 4
value4
VALUE key3 0 6 3
value3
VALUE key2 0 6 2
value2
VALUE key1 0 6 1
value1
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# delete
$cmd = "delete key1"; $rst ="DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete key2"; $rst ="DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete key3"; $rst ="DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete key4"; $rst ="DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete key5"; $rst ="DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);


