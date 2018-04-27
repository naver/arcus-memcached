#!/usr/bin/perl

use strict;
use Test::More tests => 18;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
set key1 0 0 6
value1
set key2 0 0 6
value2
set key3 0 0 6
value3
set key4 0 0 6
value4
set key5 0 0 6
value5

get key1 key2 key3 key4 key5
get key5 key4 key3 key2 key1
get key1 key2 key3 key4 key5 key6
get key6 key5 key4 key3 key2 key1

mget 24 5
key1 key2 key3 key4 key5
mget 24 5
key5 key4 key3 key2 key1
mget 29 6
key1 key2 key3 key4 key5 key6
mget 29 6
key6 key5 key4 key3 key2 key1

delete key1
delete key2
delete key3
delete key4
delete key5
=cut


my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# set
print $sock "set key1 0 0 6\r\nvalue1\r\n";
is(scalar <$sock>, "STORED\r\n", "stored key1");
print $sock "set key2 0 0 6\r\nvalue2\r\n";
is(scalar <$sock>, "STORED\r\n", "stored key2");
print $sock "set key3 0 0 6\r\nvalue3\r\n";
is(scalar <$sock>, "STORED\r\n", "stored key3");
print $sock "set key4 0 0 6\r\nvalue4\r\n";
is(scalar <$sock>, "STORED\r\n", "stored key4");
print $sock "set key5 0 0 6\r\nvalue5\r\n";
is(scalar <$sock>, "STORED\r\n", "stored key5");

# mget (OLD command)
$cmd = "get key1 key2 key3 key4 key5"; $val = "";
$rst = "VALUE key1 0 6
value1
VALUE key2 0 6
value2
VALUE key3 0 6
value3
VALUE key4 0 6
value4
VALUE key5 0 6
value5
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "get key5 key4 key3 key2 key1"; $val = "";
$rst = "VALUE key5 0 6
value5
VALUE key4 0 6
value4
VALUE key3 0 6
value3
VALUE key2 0 6
value2
VALUE key1 0 6
value1
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "get key1 key2 key3 key4 key5 key6"; $val = "";
$rst = "VALUE key1 0 6
value1
VALUE key2 0 6
value2
VALUE key3 0 6
value3
VALUE key4 0 6
value4
VALUE key5 0 6
value5
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "get key6 key5 key4 key3 key2 key1"; $val = "";
$rst = "VALUE key5 0 6
value5
VALUE key4 0 6
value4
VALUE key3 0 6
value3
VALUE key2 0 6
value2
VALUE key1 0 6
value1
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# mget (NEW command)
$cmd = "mget 24 5"; $val = "key1 key2 key3 key4 key5";
$rst = "VALUE key1 0 6
value1
VALUE key2 0 6
value2
VALUE key3 0 6
value3
VALUE key4 0 6
value4
VALUE key5 0 6
value5
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "mget 24 5"; $val = "key5 key4 key3 key2 key1";
$rst = "VALUE key5 0 6
value5
VALUE key4 0 6
value4
VALUE key3 0 6
value3
VALUE key2 0 6
value2
VALUE key1 0 6
value1
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "mget 29 6"; $val = "key1 key2 key3 key4 key5 key6";
$rst = "VALUE key1 0 6
value1
VALUE key2 0 6
value2
VALUE key3 0 6
value3
VALUE key4 0 6
value4
VALUE key5 0 6
value5
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);
$cmd = "mget 29 6"; $val = "key6 key5 key4 key3 key2 key1";
$rst = "VALUE key5 0 6
value5
VALUE key4 0 6
value4
VALUE key3 0 6
value3
VALUE key2 0 6
value2
VALUE key1 0 6
value1
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

# delete
print $sock "delete key1\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted key1");
print $sock "delete key2\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted key2");
print $sock "delete key3\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted key3");
print $sock "delete key4\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted key4");
print $sock "delete key5\r\n";
is(scalar <$sock>, "DELETED\r\n", "deleted key5");

# after test
release_memcached($engine);
