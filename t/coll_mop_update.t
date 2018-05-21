#!/usr/bin/perl

use strict;
use Test::More tests => 11;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get mkey1

mop insert mkey1 f3 6 create 11 0 0
datum3
mop insert mkey1 f2 6
datum2
mop insert mkey1 f1 6
datum1
mop get mkey1 8 3
f3 f2 f1

mop update mkey1 f3 8
datum333
mop update mkey1 f2 8
datum222
mop update mkey1 f1 6
datum0
mop get mkey1 8 3
f3 f2 f1

mop update mket1 f4 9
datum444

delete mkey1
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get mkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "mop insert mkey1 f3 6 create 11 0 0"; $val = "datum3"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f2 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 8 3"; $val = "f3 f2 f1";
$rst = "VALUE 11 3
f3 6 datum3
f2 6 datum2
f1 6 datum1
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# Success Cases
$cmd = "mop update mkey1 f3 8"; $val = "datum333"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop update mkey1 f2 8"; $val = "datum222"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop update mkey1 f1 6"; $val = "datum0"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 8 3"; $val = "f3 f2 f1";
$rst = "VALUE 11 3
f3 8 datum333
f2 8 datum222
f1 6 datum0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# Fail Cases
$cmd = "mop update mkey1 f4 8"; $val = "datum444"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);

# Finalize
$cmd = "delete mkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
