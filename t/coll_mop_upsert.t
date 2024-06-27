#!/usr/bin/perl

use strict;
use Test::More tests => 14;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get kvkey
get mkey1
mop upsert mkey1 field1 6 create 11 0 0
datum9
mop upsert mkey1 field2 6
datum7
setattr mkey1 maxcount=2
mop upsert mkey1 field1 6
datum5
mop get mkey1 13 2
field1 field2

mop upsert mkey2 field1 6
datum2
set kvkey 0 0 6
datumx
mop upsert kvkey field1 6
datum2
mop upsert mkey1 field1 6
datum4
setattr mkey1 maxcount=4000

delete kvkey
delete mkey1
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get kvkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get mkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "mop upsert mkey1 field1 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop upsert mkey1 field2 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr mkey1 maxcount=2"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "mop upsert mkey1 field1 6"; $val = "datum0"; $rst = "REPLACED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 13 2"; $val = "field1 field2";
$rst = "VALUE 11 2
field1 6 datum0
field2 6 datum7
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# Fail Cases
$cmd = "mop upsert mkey2 field1 6"; $val = "datum2"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop upsert kvkey field1 6"; $val = "datum2"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop upsert mkey1 field3 6"; $val = "datum5"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr mkey1 maxcount=4000"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "delete kvkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete mkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);

