#!/usr/bin/perl

use strict;
use Test::More tests => 49;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1: END
bop insert bkey1 0x090909090909090909 6 create 11 0 0 datum9: CREATED_STORED
bop insert bkey1 0x07070707070707 6 datum7: STORED
bop insert bkey1 0x0505050505 6 datum5: STORED
bop insert bkey1 0x030303 0x0303 6 datum3: STORED
bop insert bkey1 0x01 0x01 6 datum1: STORED
bop insert bkey1 0x0202 0x02 6 datum2: STORED
bop insert bkey1 0x04040404 0x0404 6 datum4: STORED
bop insert bkey1 0x060606060606 6 datum6: STORED
bop insert bkey1 0x0808080808080808 6 datum8: STORED
bop get bkey1 0x00..0xFF == 11 9 ebkeys eflags values
bop delete bkey1 5: BKEY_MISMATCH
bop delete bkey1 0..9: BKEY_MISMATCH
bop delete bkey1 0..0xFF: CLIENT_ERROR bad command line format
bop delete bkey1 0x00..0xFFF: CLIENT_ERROR bad command line format
bop delete bkey1 0x11..0xFFFF: NOT_FOUND_ELEMENT
bop delete bkey1 0x00..0xFF 1 EQ 0x05: NOT_FOUND_ELEMENT
bop delete bkey1 0x00..0xFF 32 EQ 0x05: CLIENT_ERROR bad command line format
bop delete bkey1 0x00..0xFF 1 XX 0x05: CLIENT_ERROR bad command line format
bop delete bkey1 0x00..0xFF 0 & 0xFFFFFF EQ 0x03: CLIENT_ERROR bad command line format
bop delete bkey1 0x00..0xFF 0 & 0xFFFFFF EQ 0x030303: NOT_FOUND_ELEMENT
bop delete bkey1 0x00..0xFF 1 * 0x00 EQ 0x03 1 1: CLIENT_ERROR bad command line format
bop delete bkey1 0x00..0xFF 3 GT 0x00: NOT_FOUND_ELEMENT
bop delete bkey1 0x0505050505 0 & 0x00 EQ 0x00: NOT_FOUND_ELEMENT
bop delete bkey1 0x00..0xFF 1 & 0xFF EQ 0x03: DELETED
bop get bkey1 0x00..0xFF == 11 8 ebkeys eflags values
bop delete bkey1 0x00..0xFF 0 ^ 0x10 EQ 0x11: DELETED
bop get bkey1 0x00..0xFF == 11 7 ebkeys eflags values
bop delete bkey1 0x00..0xFF 0 LE 0x04: DELETED
bop get bkey1 0x00..0xFF == 11 5 ebkeys eflags values
bop delete bkey1 0xFFFF..0x0000 2 drop: DELETED
bop get bkey1 0x00..0xFF == 11 3 ebkeys eflags values
bop delete bkey1 0x0505050505 0 & 0x00 NE 0x00 drop: DELETED
bop get bkey1 0x00..0xFF == 11 2 ebkeys eflags values
bop delete bkey1 0xFF..0x00 drop: DELETED_DROPPED
get bkey1: END
bop insert bkey1 0 0x0011 6 create 11 0 10 datum0: CREATED_STORED
bop insert bkey1 1 0x0022 6 datum1: STORED
bop insert bkey1 2 0x0A11 6 datum2: STORED
bop insert bkey1 3 0x0AFF 6 datum3: STORED
bop insert bkey1 4 0xBB77 6 datum4: STORED
bop insert bkey1 5 0xCC 6 datum5: STORED
bop get bkey1 0..10 == 11 6 ebkeys eflags values
bop delete bkey1 0..10 0 EQ 0x0011,0xBB77: DELETED
bop get bkey1 0..10 == 11 4 ebkeys eflags values
bop delete bkey1 0..10 0 NE 0x0022,0x0A11: DELETED
bop get bkey1 0..10 == 11 2 ebkeys eflags values
bop delete bkey1 0..10 drop: DELETED_DROPPED
get bkey1: END
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
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Fail Cases
$cmd = "bop delete bkey1 5"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0..9"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0..0xFF"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFFF"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x11..0xFFFF"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 1 EQ 0x05"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 32 EQ 0x05"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 1 XX 0x05"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 0 & 0xFFFFFF EQ 0x03"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 0 & 0xFFFFFF EQ 0x030303"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 1 * 0x00 EQ 0x03 1 1"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 3 GT 0x00"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x0505050505 0 & 0x00 EQ 0x00"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "bop delete bkey1 0x00..0xFF 1 & 0xFF EQ 0x03"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 8
0x01 0x01 6 datum1
0x0202 0x02 6 datum2
0x04040404 0x0404 6 datum4
0x0505050505 6 datum5
0x060606060606 6 datum6
0x07070707070707 6 datum7
0x0808080808080808 6 datum8
0x090909090909090909 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 0 ^ 0x10 EQ 0x11"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 7
0x0202 0x02 6 datum2
0x04040404 0x0404 6 datum4
0x0505050505 6 datum5
0x060606060606 6 datum6
0x07070707070707 6 datum7
0x0808080808080808 6 datum8
0x090909090909090909 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x00..0xFF 0 LE 0x04"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 5
0x0505050505 6 datum5
0x060606060606 6 datum6
0x07070707070707 6 datum7
0x0808080808080808 6 datum8
0x090909090909090909 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0xFFFF..0x0000 2 drop"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 3
0x0505050505 6 datum5
0x060606060606 6 datum6
0x07070707070707 6 datum7
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0x0505050505 0 & 0x00 NE 0x00 drop"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 2
0x060606060606 6 datum6
0x07070707070707 6 datum7
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0xFF..0x00 drop"; $rst = "DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);
# Finalize
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "bop insert bkey1 0 0x0011 6 create 11 0 10"; $val = "datum0"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 1 0x0022 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 2 0x0A11 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 3 0x0AFF 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 4 0xBB77 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 5 0xCC 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# check
$cmd = "bop get bkey1 0..10";
$rst = "VALUE 11 6
0 0x0011 6 datum0
1 0x0022 6 datum1
2 0x0A11 6 datum2
3 0x0AFF 6 datum3
4 0xBB77 6 datum4
5 0xCC 6 datum5
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0..10 0 EQ 0x0011,0xBB77"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..10";
$rst = "VALUE 11 4
1 0x0022 6 datum1
2 0x0A11 6 datum2
3 0x0AFF 6 datum3
5 0xCC 6 datum5
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0..10 0 NE 0x0022,0x0A11"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..10";
$rst = "VALUE 11 2
1 0x0022 6 datum1
2 0x0A11 6 datum2
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop delete bkey1 0..10 drop"; $rst = "DELETED_DROPPED";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
