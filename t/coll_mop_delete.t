#!/usr/bin/perl

use strict;
use Test::More tests => 25;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get mkey1: END
mop insert mkey1 f9 6 create 11 0 0 datum9: CREATED_STORED
mop insert mkey1 f7 6 datum7: STORED
mop insert mkey1 f5 6 datum5: STORED
mop insert mkey1 f3 6 datum3: STORED
mop insert mkey1 f1 6 datum1: STORED
mop insert mkey1 f2 6 datum2: STORED
mop insert mkey1 f4 6 datum4: STORED
mop insert mkey1 f6 6 datum6: STORED
mop insert mkey1 f8 6 datum8: STORED
mop get mkey1 26 9
f1 f2 f3 f4 f5 f6 f7 f8 f9

mop delete mkey1 5: CLIENT_ERROR bad command line format
mop delete mkey1 3 1 f10: NOT_FOUND_ELEMENT
mop delete mkey1 7 2 f11 f12: NOT_FOUND_ELEMENT
mop delete mkey1 -1 -1: CLIENT_ERROR bad command line format
mop delete mkey1 10 -1: CLIENT_ERROR bad command line format
mop dleete mkey1 -1 10: CLIENT_ERROR bad command line format

mop delete mkey1 2 1 f2: DELETED
mop get mkey 26 9
f1 f2 f3 f4 f5 f6 f7 f8 f9
mop delete mkey1 5 2 f9 f5: DELETED
mop get mkey1 26 9
f1 f2 f3 f4 f5 f6 f7 f8 f9
mop delete mkey1 0 0 true: DELETED_DROPPED
get mkey1: END
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;
my $flist;

# Initialize
$cmd = "get mkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
# Success Cases
$cmd = "mop insert mkey1 f9 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f7 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f5 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f3 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f2 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f4 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f6 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f8 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 26 9"; $val = "f1 f2 f3 f4 f5 f6 f7 f8 f9";
$rst = "VALUE 11 9
f1 6 datum1
f2 6 datum2
f3 6 datum3
f4 6 datum4
f5 6 datum5
f6 6 datum6
f7 6 datum7
f8 6 datum8
f9 6 datum9
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# Fail Cases
$cmd = "mop delete mkey1 3 1"; $val = "f10"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop delete mkey1 7 2"; $val = "f11 f12"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop delete mkey1 -1 -1"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "mop delete mkey1 10 -1"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "mop delete mkey1 -1 10"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "mop delete mkey1 2 1"; $val = "f3"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 26 9"; $val = "f1 f2 f3 f4 f5 f6 f7 f8 f9";
$rst = "VALUE 11 8
f1 6 datum1
f2 6 datum2
f4 6 datum4
f5 6 datum5
f6 6 datum6
f7 6 datum7
f8 6 datum8
f9 6 datum9
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop delete mkey1 5 2"; $val = "f9 f5"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 26 9"; $val = "f1 f2 f3 f4 f5 f6 f7 f8 f9";
$rst = "VALUE 11 6
f1 6 datum1
f2 6 datum2
f4 6 datum4
f6 6 datum6
f7 6 datum7
f8 6 datum8
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop delete mkey1 0 0"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "mop delete mkey1 0 0"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "mop insert mkey1 f1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop delete mkey1 0 0 drop"; $rst = "DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "get mkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
