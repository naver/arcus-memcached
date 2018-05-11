#!/usr/bin/perl

use strict;
#use Test::More tests => 65;
use Test::More tests => 61;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1: END
get bkey2: END

bop insert bkey1 0x0090 0x11FF 6 create 11 0 0 datum9: CREATED_STORED
bop insert bkey1 0x0070 0x01FF 6 datum7: STORED
bop insert bkey1 0x0050 0x00FF 6 datum5: STORED
bop insert bkey1 0x0030 0x000F 6 datum3: STORED
bop insert bkey1 0x0010 6 datum1: STORED
bop get bkey1 0x00..0x1000 == 11 5 ebkeys eflags values
bop insert bkey2 0x0100 0x11FF 7 datum10: STORED
bop insert bkey2 0x0080 0x01FF 6 datum8: STORED
bop insert bkey2 0x0060 0x00FF 6 datum6: STORED
bop insert bkey2 0x0040 0x000F 6 datum4: STORED
bop insert bkey2 0x0020 0x0000 6 datum2: STORED
bop get bkey2 0x0000..0xFFFFFFFF == 11 5 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 EQ 0x000F == 11 1 ebkeys eflags values
# bop get bkey1 0x00..0x1000 0 EQ 0x000F,0x00FF == 11 2 ebkeys eflags values
# bop get bkey1 0x00..0x1000 0 & 0xFFFF EQ 0x000F,0x00FF == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 NE 0x0000 == 11 5 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 LT 0x00FF == 11 1 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 LE 0x00FF == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 GT 0x00FF == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 GE 0x00FF == 11 3 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 & 0x0100 EQ 0x0100 == 11 2 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 | 0x0110 NE 0x0110 == 11 5 ebkeys eflags values
bop get bkey1 0x00..0x1000 1 ^ 0xF0   EQ 0xFF == 11 1 ebkeys eflags values
bop get bkey1 0x00..0x1000 1 & 0xFF   EQ 0x0F == 11 1 ebkeys eflags values
bop get bkey1 0x0050..0x0050 0 & 0xFFFF EQ 0x00FF 0 1 delete == 11 1 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 EQ 0x0000 == 11 1 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 NE 0x0000 == 11 4 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 LT 0x00FF == 11 2 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 LE 0x00FF == 11 3 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 GT 0x00FF == 11 2 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 GE 0x00FF == 11 3 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 & 0x0100 EQ 0x0100 == 11 2 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 0 | 0x0110 NE 0x0110 == 11 4 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0   EQ 0xFF == 11 1 ebkeys eflags values
bop get bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0   EQ 0xFF == 11 1 ebkeys eflags values
bop get bkey1 0x00..0x1000 0 EQ 0x0000: NOT_FOUND_ELEMENT
bop get bkey1 0..0x1000: CLIENT_ERROR bad command line format
# bop get bkey1 0..0x1000 0 EQ 0x000F,0x000FF: CLIENT_ERROR bad command line format
bop get bkey1 0..1000: BKEY_MISMATCH
bop get kvkey 0..1000: NOT_FOUND
bop get kvkey 0..1000: NOT_FOUND
set kvkey 0 0 6 datumx: STORED
bop get kvkey 0..1000: TYPE_MISMATCH

delete kvkey: DELETED
delete bkey2: DELETED

bop insert bkey2 0x00 6 create 13 0 0 datum1: CREATED_STORED
bop insert bkey2 0x0000 0x0202 7 datum22: STORED
bop insert bkey2 0x000000 8 datum333: STORED
bop insert bkey2 0x01 0x01 9 datum4444: STORED
bop insert bkey2 0x0001 10 datum55555: STORED
bop insert bkey2 0x000001 0x030303 11 datum666666: STORED
bop insert bkey2 0x02 12 datum7777777: STORED
bop insert bkey2 0x0002 0x0202 13 datum88888888: STORED
bop insert bkey2 0x000002 14 datum999999999: STORED
bop insert bkey2 0x0101 0x00 5 datum: STORED

bop get bkey2 0x01..0x02 == 13 3 ebkeys eflags values
bop get bkey2 0x0001..0x0002 == 13 2 ebkeys eflags values
bop get bkey2 0x00..0x0002 == 13 7 ebkeys eflags values
bop get bkey2 0x02..0x0000 == 13 9 ebkeys eflags values

delete bkey1: DELETED
delete bkey2: DELETED
=cut

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey2"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Prepare Keys
$cmd = "bop insert bkey1 0x0090 0x11FF 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0070 0x01FF 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0050 0x00FF 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0030 0x000F 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0x00..0x1000";
$rst = "VALUE 11 5
0x0010 6 datum1
0x0030 0x000F 6 datum3
0x0050 0x00FF 6 datum5
0x0070 0x01FF 6 datum7
0x0090 0x11FF 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop create bkey2 11 0 0"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 0x0100 0x11FF 7"; $val = "datum10"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0080 0x01FF 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0060 0x00FF 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0040 0x000F 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0020 0x0000 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 11 5
0x0020 0x0000 6 datum2
0x0040 0x000F 6 datum4
0x0060 0x00FF 6 datum6
0x0080 0x01FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Success cases
$cmd = "bop get bkey1 0x00..0x1000 0 EQ 0x000F";
$rst = "VALUE 11 1
0x0030 0x000F 6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 0 NE 0x000F";
$rst = "VALUE 11 4
0x0010 6 datum1
0x0050 0x00FF 6 datum5
0x0070 0x01FF 6 datum7
0x0090 0x11FF 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop get bkey1 0x00..0x1000 0 LT 0x00FF";
$rst = "VALUE 11 1
0x0030 0x000F 6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 0 LE 0x00FF";
$rst = "VALUE 11 2
0x0030 0x000F 6 datum3
0x0050 0x00FF 6 datum5
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 0 GT 0x00FF";
$rst = "VALUE 11 2
0x0070 0x01FF 6 datum7
0x0090 0x11FF 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 0 GE 0x00FF";
$rst = "VALUE 11 3
0x0050 0x00FF 6 datum5
0x0070 0x01FF 6 datum7
0x0090 0x11FF 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 0 & 0x0100 EQ 0x0100";
$rst = "VALUE 11 2
0x0070 0x01FF 6 datum7
0x0090 0x11FF 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 0 | 0x0110 NE 0x0110";
$rst = "VALUE 11 5
0x0010 6 datum1
0x0030 0x000F 6 datum3
0x0050 0x00FF 6 datum5
0x0070 0x01FF 6 datum7
0x0090 0x11FF 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 1 ^ 0xF0   EQ 0xFF";
$rst = "VALUE 11 1
0x0030 0x000F 6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0x1000 1 & 0xFF   EQ 0x0F";
$rst = "VALUE 11 1
0x0030 0x000F 6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0050..0x0050 0 & 0xFFFF EQ 0x00FF 0 1 delete";
$rst = "VALUE 11 1
0x0050 0x00FF 6 datum5
DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 EQ 0x0000";
$rst = "VALUE 11 1
0x0020 0x0000 6 datum2
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 NE 0x0000";
$rst = "VALUE 11 4
0x0040 0x000F 6 datum4
0x0060 0x00FF 6 datum6
0x0080 0x01FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 LT 0x00FF";
$rst = "VALUE 11 2
0x0020 0x0000 6 datum2
0x0040 0x000F 6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 LE 0x00FF";
$rst = "VALUE 11 3
0x0020 0x0000 6 datum2
0x0040 0x000F 6 datum4
0x0060 0x00FF 6 datum6
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 GT 0x00FF";
$rst = "VALUE 11 2
0x0080 0x01FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 GE 0x00FF";
$rst = "VALUE 11 3
0x0060 0x00FF 6 datum6
0x0080 0x01FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 & 0x0100 EQ 0x0100";
$rst = "VALUE 11 2
0x0080 0x01FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 0 | 0x0110 NE 0x0110";
$rst = "VALUE 11 4
0x0040 0x000F 6 datum4
0x0060 0x00FF 6 datum6
0x0080 0x01FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0 EQ 0xFF";
$rst = "VALUE 11 1
0x0040 0x000F 6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF 1 ^ 0xF0   EQ 0xFF";
$rst = "VALUE 11 1
0x0040 0x000F 6 datum4
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Fail Cases
$cmd = "bop get bkey1 0x00..0x1000 0 EQ 0x0000"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..0x1000"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
# $cmd = "bop get bkey1 0..0x1000 0 EQ 0x000F,0x000FF"; $rst = "CLIENT_ERROR bad command line format";
# print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "bop get bkey1 0..1000"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get kvkey 0..1000"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get kvkey 0..1000"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get kvkey 0..1000"; $rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete kvkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# Other Cases
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 0x00 6 create 13 0 0"; $val = "datum1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0000 0x0202 7"; $val = "datum22"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x000000 8"; $val = "datum333"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x01 0x01 9"; $val = "datum4444"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0001 10"; $val = "datum55555"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x000001 0x030303 11"; $val = "datum666666"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x02 12"; $val = "datum7777777"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0002 0x0202 13"; $val = "datum88888888"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x000002 14"; $val = "datum999999999"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0101 0x00 5"; $val = "datum"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey2 0x01..0x02";
$rst = "VALUE 13 3
0x01 0x01 9 datum4444
0x0101 0x00 5 datum
0x02 12 datum7777777
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0001..0x0002";
$rst = "VALUE 13 2
0x0001 10 datum55555
0x0002 0x0202 13 datum88888888
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x00..0x0002";
$rst = "VALUE 13 7
0x00 6 datum1
0x0000 0x0202 7 datum22
0x000000 8 datum333
0x000001 0x030303 11 datum666666
0x000002 14 datum999999999
0x0001 10 datum55555
0x0002 0x0202 13 datum88888888
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x02..0x0000";
$rst = "VALUE 13 9
0x02 12 datum7777777
0x0101 0x00 5 datum
0x01 0x01 9 datum4444
0x0002 0x0202 13 datum88888888
0x0001 10 datum55555
0x000002 14 datum999999999
0x000001 0x030303 11 datum666666
0x000000 8 datum333
0x0000 0x0202 7 datum22
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
