#!/usr/bin/perl

use strict;
use Test::More tests => 69;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

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
bop insert bkey2 0x0100 0x11FF 7 create 11 0 0
datum10
bop insert bkey2 0x0080 0x01FF 6
datum8
bop insert bkey2 0x0060 0x00FF 6
datum6
bop insert bkey2 0x0040 0x000F 6
datum4
bop insert bkey2 0x0020 0x0000 6
datum2
bop get bkey1 0..100
bop get bkey2 0x0000..0xFFFFFFFF

bop update bkey1 90 8
datum999
bop update bkey1 70 8
datum777
bop update bkey1 50 10
datum55555
bop update bkey1 30 10
datum33333
bop update bkey1 10 6
datum0
bop get bkey1 0..100
bop update bkey1 10 10
datum11111
bop get bkey1 0..100

bop update bkey1 60 8
datum666
bop update bkey1 60 -1
bop update bkey1 0x60 8
datum666

bop update bkey2 0x0100 0x000011ff -1
bop update bkey2 0x0080 0x000001ff -1
bop update bkey2 0x0060 0x000000ff -1
bop update bkey2 0x0040 0x0000000f -1
bop update bkey2 0x0020 0x00000000 -1
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0100 0xbb0011ff -1
bop update bkey2 0x0080 0xbb0001ff -1
bop update bkey2 0x0060 0xbb0000ff -1
bop update bkey2 0x0040 0xbb00000f -1
bop update bkey2 0x0020 0xbb000000 -1
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0100 1 | 0xaa -1
bop update bkey2 0x0080 1 | 0xaa -1
bop update bkey2 0x0060 1 | 0xaa -1
bop update bkey2 0x0040 1 | 0xaa -1
bop update bkey2 0x0020 1 | 0xaa -1
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0100 0 & 0xF0 9
datum1010
bop update bkey2 0x0080 0 & 0xF0 8
datum888
bop update bkey2 0x0060 0 & 0xF0 3
666
bop update bkey2 0x0040 0 & 0xF0 13
datumdatum444
bop update bkey2 0x0020 0 & 0xF0 6
datum0
bop get bkey2 0x0000..0xFFFFFFFF
bop update bkey2 0x0020 0 6
datum2
bop get bkey2 0x0000..0xFFFFFFFF

bop update bkey2 0x0030 6
datum2
bop update bkey2 0030 6
datum2
bop update bkey2 30 6
datum2
bop update bkey2 0x20 1 | 0xFF -1
bop update bkey2 0x0020 1 | 0xFF -1
bop update bkey2 0x0040 4 | 0xFF -1
bop update bkey2 0x0040 64 | 0xFF -1

delete bkey1
delete bkey2
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
$cmd = "bop insert bkey1 90 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 70 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 50 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 30 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 10 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0100 0x11FF 7 create 11 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0080 0x01FF 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0060 0x00FF 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0040 0x000F 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0020 0x0000 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 5
10 6 datum1
30 6 datum3
50 6 datum5
70 6 datum7
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 11 5
0x0020 0x0000 6 datum2
0x0040 0x000F 6 datum4
0x0060 0x00FF 6 datum6
0x0080 0x01FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Success Cases
$cmd = "bop update bkey1 90 8"; $val = "datum999"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey1 70 8"; $val = "datum777"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey1 50 10"; $val = "datum55555"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey1 30 10"; $val = "datum33333"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey1 10 6"; $val = "datum0"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 5
10 6 datum0
30 10 datum33333
50 10 datum55555
70 8 datum777
90 8 datum999
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey1 10 10"; $val = "datum11111"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 5
10 10 datum11111
30 10 datum33333
50 10 datum55555
70 8 datum777
90 8 datum999
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Fail Cases
$cmd = "bop update bkey1 60 8"; $val = "datum666"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey1 60 -1"; $rst = "NOTHING_TO_UPDATE";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey1 70 8"; $val = "datum7777";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$rst = "ERROR unknown command";
Test::More::is(scalar(<$sock>), "$rst\r\n", "bop update : $rst");
$cmd = "bop update bkey1 70 8"; $val = "datum77777";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$rst = "ERROR unknown command";
Test::More::is(scalar(<$sock>), "$rst\r\n", "bop update : $rst");
$cmd = "bop update bkey1 70 8"; $val = "datum777777";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$rst = "ERROR unknown command";
Test::More::is(scalar(<$sock>), "$rst\r\n", "bop update : $rst");
$cmd = "bop update bkey1 70 8"; $val = "datum7777777";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$rst = "ERROR unknown command";
Test::More::is(scalar(<$sock>), "$rst\r\n", "bop update : $rst");
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 5
10 10 datum11111
30 10 datum33333
50 10 datum55555
70 8 datum777
90 8 datum999
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey1 0x60 8"; $val = "datum666"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);

# Success Cases
$cmd = "bop update bkey2 0x0100 0x000011ff -1"; $rst = "UPDATED";
$cmd = "bop update bkey2 0x0080 0x000001ff -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0060 0x000000ff -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0040 0x0000000f -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0020 0x00000000 -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 11 5
0x0020 0x00000000 6 datum2
0x0040 0x0000000F 6 datum4
0x0060 0x000000FF 6 datum6
0x0080 0x000001FF 6 datum8
0x0100 0x11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0100 0xbb0011ff -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0080 0xbb0001ff -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0060 0xbb0000ff -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0040 0xbb00000f -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0020 0xbb000000 -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 11 5
0x0020 0xBB000000 6 datum2
0x0040 0xBB00000F 6 datum4
0x0060 0xBB0000FF 6 datum6
0x0080 0xBB0001FF 6 datum8
0x0100 0xBB0011FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0100 1 | 0xaa -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0080 1 | 0xaa -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0060 1 | 0xaa -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0040 1 | 0xaa -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0020 1 | 0xaa -1"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 11 5
0x0020 0xBBAA0000 6 datum2
0x0040 0xBBAA000F 6 datum4
0x0060 0xBBAA00FF 6 datum6
0x0080 0xBBAA01FF 6 datum8
0x0100 0xBBAA11FF 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0100 0 & 0xF0 9"; $val = "datum1010"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey2 0x0080 0 & 0xF0 8"; $val = "datum888"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey2 0x0060 0 & 0xF0 3"; $val = "666"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey2 0x0040 0 & 0xF0 13"; $val = "datumdatum444"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey2 0x0020 0 & 0xF0 6"; $val = "datum0"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 11 5
0x0020 0xB0AA0000 6 datum0
0x0040 0xB0AA000F 13 datumdatum444
0x0060 0xB0AA00FF 3 666
0x0080 0xB0AA01FF 8 datum888
0x0100 0xB0AA11FF 9 datum1010
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0020 0 6"; $val = "datum2"; $rst = "UPDATED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 11 5
0x0020 6 datum2
0x0040 0xB0AA000F 13 datumdatum444
0x0060 0xB0AA00FF 3 666
0x0080 0xB0AA01FF 8 datum888
0x0100 0xB0AA11FF 9 datum1010
END";
mem_cmd_is($sock, $cmd, "", $rst);

# Fail Cases
$cmd = "bop update bkey2 0x0030 6"; $val = "datum2"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey2 0030 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey2 30 6"; $val = "datum2"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop update bkey2 0x20 1 | 0xFF -1"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0020 1 | 0xFF -1"; $rst = "EFLAG_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0040 4 | 0xFF -1"; $rst = "EFLAG_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop update bkey2 0x0040 64 | 0xFF -1"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);


# after test
release_memcached($engine, $server);
