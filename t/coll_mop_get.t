#!/usr/bin/perl

use strict;
use Test::More tests => 23;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get mkey1
get mkey2
mop insert mkey1 f7 6 create 11 0 0
datum7
mop insert mkey1 f6 6
datum6
mop insert mkey1 f5 6
datum5
mop insert mkey1 f4 6
datum4
mop insert mkey1 f3 6
datum3
mop insert mkey1 f2 6
datum2
mop insert mkey1 f1 6
datum1
mop get mkey1 20 7
f1 f2 f3 f4 f5 f6 f7

mop get mkey1 2 1
f4
mop get meky1 11 4
f3 f7 f1 f5
mop get mkey1 11 4
f1 f2 f8 f3

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
$cmd = "get mkey2"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
# Prepare Keys: mkey1
$cmd = "mop insert mkey1 f7 6 create 11 0 0"; $val = "datum7"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f6 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f5 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f4 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f3 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f2 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey1 f1 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 20 7"; $val = "f1 f2 f3 f4 f5 f6 f7";
$rst = "VALUE 11 7
f1 6 datum1
f2 6 datum2
f3 6 datum3
f4 6 datum4
f5 6 datum5
f6 6 datum6
f7 6 datum7
END";
mem_cmd_is($sock, $cmd, $val, $rst);
# Prepare Keys: mkey2
my $data = "a"x4000;
my $vlen = 4001;
$cmd = "mop insert mkey2 f7 $vlen create 11 0 0"; $val = "$data" . "7"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey2 f6 $vlen"; $val = "$data" . "6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey2 f5 $vlen"; $val = "$data" . "5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey2 f4 $vlen"; $val = "$data" . "4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey2 f3 $vlen"; $val = "$data" . "3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey2 f2 $vlen"; $val = "$data" . "2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop insert mkey2 f1 $vlen"; $val = "$data" . "1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey2 20 7"; $val = "f1 f2 f3 f4 f5 f6 f7";
$rst = "VALUE 11 7
f1 $vlen $data" . "1
f2 $vlen $data" . "2
f3 $vlen $data" . "3
f4 $vlen $data" . "4
f5 $vlen $data" . "5
f6 $vlen $data" . "6
f7 $vlen $data" . "7
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# Success Cases
$cmd = "mop get mkey1 2 1"; $val = "f4";
$rst = "VALUE 11 1
f4 6 datum4
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 11 4"; $val = "f3 f7 f1 f5";
$rst = "VALUE 11 4
f3 6 datum3
f7 6 datum7
f1 6 datum1
f5 6 datum5
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "mop get mkey1 11 4"; $val = "f1 f2 f8 f3";
$rst = "VALUE 11 3
f1 6 datum1
f2 6 datum2
f3 6 datum3
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# Finalize
$cmd = "delete mkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete mkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
