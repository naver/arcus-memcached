#!/usr/bin/perl

use strict;
use Test::More tests => 34;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get kvkey
get bkey1
bop insert bkey1 0x090909090909090909 6 create 11 0 0
datum9
bop insert bkey1 0x07070707070707 6
datum7
bop insert bkey1 0x0505050505 6
datum5
bop insert bkey1 0x030303 0x0303 6
datum3
bop insert bkey1 0x01 0x01 6
datum1
bop get bkey1 0x00..0xFF
bop insert bkey2 0x0202 6
datum2
set kvkey 0 0 6
datumx
bop insert kvkey 0x0202 6
datum2
bop insert bkey1 02 6
datum2
bop insert bkey1 00 6
datum2
bop insert bkey1 2 6
datum2
setattr bkey1 maxcount=5 overflowaction=error
bop insert bkey1 0x0202 6
datum2
setattr bkey1 overflowaction=smallest_trim
bop insert bkey1 0x00 6
datum0
bop insert bkey1 0x01 6
datum1
setattr bkey1 maxcount=4000
bop insert bkey1 0x020 6
datum2
bop insert bkey1 0x0202020202020202020202020202020202020202020202020202020202020202 6
datum2
bop insert bkey1 0x0202 0x020 6
datum2
bop insert bkey1 0x0202 0x0202020202020202020202020202020202020202020202020202020202020202 6
datum2
bop insert bkey1 0x0202 02 6
datum2
bop insert bkey1 0x0202 00 6
datum2
bop insert bkey1 0x0202 2 6
datum2
bop insert bkey1 0x0202 0x02 6
datum2
bop insert bkey1 0x04040404 0x0404 6
datum4
bop insert bkey1 0x060606060606 6
datum6
bop insert bkey1 0x0808080808080808 6
datum8
bop get bkey1 0x00..0xFF
delete kvkey
delete bkey1
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
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "bop insert bkey1 0x090909090909090909 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x07070707070707 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0505050505 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x030303 0x0303 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x01 0x01 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 5
0x01 0x01 6 datum1
0x030303 0x0303 6 datum3
0x0505050505 6 datum5
0x07070707070707 6 datum7
0x090909090909090909 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Fail Cases
$cmd = "bop insert bkey2 0x0202 6"; $val = "datum2"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert kvkey 0x0202 6"; $val = "datum2"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 02 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 00 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 2 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 maxcount=5 overflowaction=error"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0202 6"; $val = "datum2"; $rst = "OVERFLOWED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 overflowaction=smallest_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x00 6"; $val = "datum0"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x01 6"; $val = "datum1"; $rst = "ELEMENT_EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 maxcount=4000"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x020 6"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0202020202020202020202020202020202020202020202020202020202020202 6"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0202 0x020 6"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0202 0x0202020202020202020202020202020202020202020202020202020202020202 6"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0202 02 6"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0202 00 6"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0202 2 6"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "bop insert bkey1 0x0202 0x02 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x04040404 0x0404 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x060606060606 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0808080808080808 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 9
0x01 0x01 6 datum1
0x0202 0x02 6 datum2
0x030303 0x0303 6 datum3
0x04040404 0x0404 6 datum4
0x0505050505 6 datum5
0x060606060606 6 datum6
0x07070707070707 6 datum7
0x0808080808080808 6 datum8
0x090909090909090909 6 datum9
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "delete kvkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);


# after test
release_memcached($engine, $server);
