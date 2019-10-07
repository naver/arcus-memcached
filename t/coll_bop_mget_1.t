#!/usr/bin/perl

use strict;
use Test::More tests => 52;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
#my $server = get_memcached($engine, "", "11333");
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

=head
get bkey1
get bkey2

bop insert bkey1 90 6 create 11 0 0
datum9
bop insert bkey1 70 6
datum7
bop insert bkey1 50 6
datum5
bop insert bkey1 30 6
datum3
bop insert bkey1 10 6
datum1
bop insert bkey2 100 7 create 12 0 0
datum10
bop insert bkey2 80 6
datum8
bop insert bkey2 60 6
datum6
bop insert bkey2 40 6
datum4
bop insert bkey2 20 6
datum2
bop get bkey1 0..100
bop get bkey2 0..100

bop mget 11 2 0..100 5
bkey1 bkey2
bop mget 23 4 0..100 2 6
bkey2 bkey3 bkey1 bkey4
bop mget 23 4 90..30 2 9
bkey2 bkey3 bkey1 bkey4
bop mget 23 4 200..300 2 6
bkey2 bkey3 bkey1 bkey4

set keyx 0 0 6
datumx

bop mget 28 5 0..100 2 6
bkey2 bkey3 bkey1 bkey4 keyx
bop mget 29 5 0..100 2 6
bkey2 bkey3 bkey1 bkey4 bkey1
bop mget 29 5 0..100 2 6
bkey2 bkey3 bkey1 bkey4 bkey3
bop mget 23 2 0..100 2 6
bkey2 bkey3 bkey1 bkey4

delete bkey1
delete bkey2
delete keyx

bop create bkey1 11 0 0
bop create bkey2 12 0 0
bop insert bkey2 100 7
datum10
bop insert bkey2 80 6
datum8
bop insert bkey2 60 6
datum6
bop insert bkey2 40 6
datum4
bop insert bkey2 20 6
datum2

delete bkey1
delete bkey2

bop insert bkey1 0x0090 0x11FF 6 create 3 0 0 datum9: CREATED_STORED
bop insert bkey1 0x0070 0x01FF 6 datun7: STORED
bop insert bkey1 0x0050 0X00FF 6 datum5: STORED
bop insert bkey2 0x0080 0x11FF 6 create 3 0 0 datum8: CREATED_STORED
bop insert bkey2 0x0060 0x01FF 6 datum6: STORED
bop insert kbey2 0x0040 0x00FF 6 datum4: STORED
bop mget 11 2 0x00..0x1000 ebkeys eflags[IN] values
bop mget 11 2 0x00..0x1000 ebkeys eflags[NOT IN] values

delete bkey1
delete bkey2
=cut

#
# testBOPMGetSimple
#
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey2"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop create bkey1 11 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 90 6"; $val = "datum9"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 70 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 50 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 10 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop create bkey2 12 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 80 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 60 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 40 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop get bkey1 0..100";
$rst =
"VALUE 11 5
10 6 datum1
30 6 datum3
50 6 datum5
70 6 datum7
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop get bkey2 0..100";
$rst =
"VALUE 12 5
20 6 datum2
40 6 datum4
60 6 datum6
80 6 datum8
100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop mget 11 2 0..100 5";
$val = "bkey1 bkey2";
$rst =
"VALUE bkey1 OK 11 5
ELEMENT 10 6 datum1
ELEMENT 30 6 datum3
ELEMENT 50 6 datum5
ELEMENT 70 6 datum7
ELEMENT 90 6 datum9
VALUE bkey2 OK 12 5
ELEMENT 20 6 datum2
ELEMENT 40 6 datum4
ELEMENT 60 6 datum6
ELEMENT 80 6 datum8
ELEMENT 100 7 datum10
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 11 2 100..0 10";
$val = "bkey1 bkey2";
$rst =
"VALUE bkey1 OK 11 5
ELEMENT 90 6 datum9
ELEMENT 70 6 datum7
ELEMENT 50 6 datum5
ELEMENT 30 6 datum3
ELEMENT 10 6 datum1
VALUE bkey2 OK 12 5
ELEMENT 100 7 datum10
ELEMENT 80 6 datum8
ELEMENT 60 6 datum6
ELEMENT 40 6 datum4
ELEMENT 20 6 datum2
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0..100 2 6";
$val = "bkey2 bkey3 bkey1 bkey4";
$rst =
"VALUE bkey2 OK 12 3
ELEMENT 60 6 datum6
ELEMENT 80 6 datum8
ELEMENT 100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 50 6 datum5
ELEMENT 70 6 datum7
ELEMENT 90 6 datum9
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 90..30 2 9";
$val = "bkey2 bkey3 bkey1 bkey4";
$rst =
"VALUE bkey2 OK 12 1
ELEMENT 40 6 datum4
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 2
ELEMENT 50 6 datum5
ELEMENT 30 6 datum3
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 200..300 2 6";
$val = "bkey2 bkey3 bkey1 bkey4";
$rst =
"VALUE bkey2 NOT_FOUND_ELEMENT
VALUE bkey3 NOT_FOUND
VALUE bkey1 NOT_FOUND_ELEMENT
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "set keyx 19 5 10";
$val = "some value";
$rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 2 0..100 2 6";
$val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 28 5 0..100 2 6";
$val = "bkey2 bkey3 bkey1 bkey4 keyx";
$rst =
"VALUE bkey2 OK 12 3
ELEMENT 60 6 datum6
ELEMENT 80 6 datum8
ELEMENT 100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 50 6 datum5
ELEMENT 70 6 datum7
ELEMENT 90 6 datum9
VALUE bkey4 NOT_FOUND
VALUE keyx TYPE_MISMATCH
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0..100 2 6";
$val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst =
"VALUE bkey2 OK 12 3
ELEMENT 60 6 datum6
ELEMENT 80 6 datum8
ELEMENT 100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 50 6 datum5
ELEMENT 70 6 datum7
ELEMENT 90 6 datum9
VALUE bkey4 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 50 6 datum5
ELEMENT 70 6 datum7
ELEMENT 90 6 datum9
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0..100 2 6";
$val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst =
"VALUE bkey2 OK 12 3
ELEMENT 60 6 datum6
ELEMENT 80 6 datum8
ELEMENT 100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 50 6 datum5
ELEMENT 70 6 datum7
ELEMENT 90 6 datum9
VALUE bkey4 NOT_FOUND
VALUE bkey3 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "delete keyx"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

#
# bop mget test 2
#
$cmd = "bop create bkey1 11 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create bkey2 12 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 80 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 60 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 40 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 20 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop get bkey1 0..100";
$rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop get bkey2 0..100";
$rst =
"VALUE 12 5
20 6 datum2
40 6 datum4
60 6 datum6
80 6 datum8
100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop mget 11 2 0..100 5";
$val = "bkey1 bkey2";
$rst =
"VALUE bkey1 NOT_FOUND_ELEMENT
VALUE bkey2 OK 12 5
ELEMENT 20 6 datum2
ELEMENT 40 6 datum4
ELEMENT 60 6 datum6
ELEMENT 80 6 datum8
ELEMENT 100 7 datum10
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 146 21 0..100000 10";
$val = "KEY_11 KEY_12 KEY_13 KEY_14 KEY_15 "
     . "KEY_16 KEY_17 KEY_18 KEY_19 KEY_20 "
     . "KEY_21 KEY_22 KEY_23 KEY_24 KEY_25 "
     . "KEY_26 KEY_27 KEY_28 KEY_29 KEY_30 KEY_16";
$rst =
"VALUE KEY_11 NOT_FOUND
VALUE KEY_12 NOT_FOUND
VALUE KEY_13 NOT_FOUND
VALUE KEY_14 NOT_FOUND
VALUE KEY_15 NOT_FOUND
VALUE KEY_16 NOT_FOUND
VALUE KEY_17 NOT_FOUND
VALUE KEY_18 NOT_FOUND
VALUE KEY_19 NOT_FOUND
VALUE KEY_20 NOT_FOUND
VALUE KEY_21 NOT_FOUND
VALUE KEY_22 NOT_FOUND
VALUE KEY_23 NOT_FOUND
VALUE KEY_24 NOT_FOUND
VALUE KEY_25 NOT_FOUND
VALUE KEY_26 NOT_FOUND
VALUE KEY_27 NOT_FOUND
VALUE KEY_28 NOT_FOUND
VALUE KEY_29 NOT_FOUND
VALUE KEY_30 NOT_FOUND
VALUE KEY_16 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

#
# bop mget with eflag filtering
#
$cmd = "bop insert bkey1 0x0090 0x11FF 6 create 3 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0070 0x01FF 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0050 0x00FF 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0080 0x11FF 6 create 3 0 0"; $val = "datum8"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0060 0x01FF 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0040 0x00FF 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 11 2 0x00..0x1000 0 EQ 0x11FF,0x01FF 6";
$val = "bkey1 bkey2";
$rst =
"VALUE bkey1 OK 3 2
ELEMENT 0x0070 0x01FF 6 datum7
ELEMENT 0x0090 0x11FF 6 datum9
VALUE bkey2 OK 3 2
ELEMENT 0x0060 0x01FF 6 datum6
ELEMENT 0x0080 0x11FF 6 datum8
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 11 2 0x00..0x1000 0 NE 0x00FF,0x11FF 6";
$val = "bkey1 bkey2";
$rst =
"VALUE bkey1 OK 3 1
ELEMENT 0x0070 0x01FF 6 datum7
VALUE bkey2 OK 3 1
ELEMENT 0x0060 0x01FF 6 datum6
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
