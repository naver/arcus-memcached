#!/usr/bin/perl

use strict;
use Test::More tests => 62;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1

bop insert bkey1 0x0010 1 create 11 0 0
1
bop insert bkey1 0x0020 1
2
bop insert bkey1 0x0030 1
3
bop insert bkey1 0x0040 1
4
bop insert bkey1 0x0050 1
a
bop count bkey1 0x00..0x1000

bop incr bkey1 0x0010 1
bop get bkey1 0x0010
bop incr bkey1 0x0020 25
bop get bkey1 0x0020
bop decr bkey1 0x0020 20
bop get bkey1 0x0020
bop decr bkey1 0x0030 2
bop get bkey1 0x0030
bop decr bkey1 0x0040 10
bop get bkey1 0x0040
bop incr bkey1 0x0050 10
bop get bkey1 0x0050

bop incr bkey1 0x0060 10
bop incr bkey1 1 10
bop incr bkey2 0 10

delete bkey1
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

# Prepare Keys
$cmd = "bop insert bkey1 0x0010 1 create 11 0 0"; $val = "1"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0020 1"; $val = "2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0030 1"; $val = "3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0040 1"; $val = "4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0050 1"; $val = "a"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop count bkey1 0x0010..0x0050"; $rst = "COUNT=5";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop incr bkey1 0x0010 1"; $rst = "2";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0010";
$rst = "VALUE 11 1
0x0010 1 2
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 0x0020 25"; $rst = "27";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0020";
$rst = "VALUE 11 1
0x0020 2 27
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0020 20"; $rst = "7";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0020";
$rst = "VALUE 11 1
0x0020 1 7
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0030 2"; $rst = "1";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0030";
$rst = "VALUE 11 1
0x0030 1 1
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0040 10"; $rst = "0";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0040";
$rst = "VALUE 11 1
0x0040 1 0
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 0x0050 10";
$rst = "INVALID incr or decr on non-numeric value";
mem_cmd_is($sock, $cmd, "", $rst, "a + 10 = 0");

$cmd = "bop get bkey1 0x0050";
$rst = "VALUE 11 1
0x0050 1 a
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 0x0060 10"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 0x0060 10 6"; $rst = "6";
mem_cmd_is($sock, $cmd, "", $rst, "initail = 6");
$cmd = "bop get bkey1 0x0060";
$rst = "VALUE 11 1
0x0060 1 6
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 0x0060 10 6"; $rst = "16";
mem_cmd_is($sock, $cmd, "", $rst, "+ 10 = 16");
$cmd = "bop get bkey1 0x0060";
$rst = "VALUE 11 1
0x0060 2 16
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0070 10 7 0xFF"; $rst = "7";
mem_cmd_is($sock, $cmd, "", $rst, "initial = 7 0xFF");
$cmd = "bop get bkey1 0x0070";
$rst = "VALUE 11 1
0x0070 0xFF 1 7
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0070 5 7 0xFF"; $rst = "2";
mem_cmd_is($sock, $cmd, "", $rst, "- 5 = 2");
$cmd = "bop get bkey1 0x0070";
$rst = "VALUE 11 1
0x0070 0xFF 1 2
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0070 10 7 0xFF"; $rst = "0";
mem_cmd_is($sock, $cmd, "", $rst, "- 10 = 0");
$cmd = "bop get bkey1 0x0070";
$rst = "VALUE 11 1
0x0070 0xFF 1 0
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 0x0080 0 987654321 0xF0F0";
$rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst, "delta = 0");

$cmd = "bop incr bkey1 0x0080 1 987654321 0xF0F0"; $rst = "987654321";
mem_cmd_is($sock, $cmd, "", $rst, "initial = 987654321 0xF0F0");
$cmd = "bop get bkey1 0x0080";
$rst = "VALUE 11 1
0x0080 0xF0F0 9 987654321
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0080 1"; $rst = "987654320";
mem_cmd_is($sock, $cmd, "", $rst, "- 1 = 987654320");
$cmd = "bop get bkey1 0x0080";
$rst = "VALUE 11 1
0x0080 0xF0F0 9 987654320
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0080 900000000"; $rst = "87654320";
mem_cmd_is($sock, $cmd, "", $rst, "- 900000000 = 87654320");
$cmd = "bop get bkey1 0x0080";
$rst = "VALUE 11 1
0x0080 0xF0F0 8 87654320
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0080 80000000 10"; $rst = "7654320";
mem_cmd_is($sock, $cmd, "", $rst, "- 80000000 = 7654320");
$cmd = "bop get bkey1 0x0080";
$rst = "VALUE 11 1
0x0080 0xF0F0 7 7654320
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop decr bkey1 0x0080 7654000 10 0xFFFF"; $rst = "320";
mem_cmd_is($sock, $cmd, "", $rst, "- 7654000 = 320");
$cmd = "bop get bkey1 0x0080";
$rst = "VALUE 11 1
0x0080 0xF0F0 3 320
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 0x0080 680 10 0xFFFF"; $rst = "1000";
mem_cmd_is($sock, $cmd, "", $rst, "+ 680 = 1000");
$cmd = "bop get bkey1 0x0080";
$rst = "VALUE 11 1
0x0080 0xF0F0 4 1000
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey1 1 10"; $rst = "BKEY_MISMATCH";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop incr bkey2 0 10"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst);

# additional tests
$cmd = "bop insert bkey1 0x0100 0\r\n"; $val = ""; $rst = "STORED"; # 0 value test
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0110 2"; $val = "-1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0120 20"; $val = "18446744073709551615"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0130 20"; $val = "18446744073709551614"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0140 20"; $val = "18446744073709551616"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$rst = "INVALID incr or decr on non-numeric value";
$cmd = "bop incr bkey1 0x0100 1";
mem_cmd_is($sock, $cmd, "", $rst, "incr1 on empty");
$cmd = "bop decr bkey1 0x0100 1";
mem_cmd_is($sock, $cmd, "", $rst, "decr 1 on empty");
$cmd = "bop incr bkey1 0x0110 1";
mem_cmd_is($sock, $cmd, "", $rst, "incr 1 on -1");
$cmd = "bop decr bkey1 0x0110 1";
mem_cmd_is($sock, $cmd, "", $rst, "incr 1 on -1");

$cmd = "bop incr bkey1 0x0120 1"; $rst = "0";
mem_cmd_is($sock, $cmd, "", $rst, "incr 1 on 18446744073709551615");
$cmd = "bop decr bkey1 0x0120 1"; $rst = "0";
mem_cmd_is($sock, $cmd, "", $rst, "decr 1 on 0");

$cmd = "bop incr bkey1 0x0130 1"; $rst = "18446744073709551615";
mem_cmd_is($sock, $cmd, "", $rst, "incr 1 on 18446744073709551614");
$cmd = "bop decr bkey1 0x0130 1"; $rst = "18446744073709551614";
mem_cmd_is($sock, $cmd, "", $rst, "decr 1 on 18446744073709551615");
$cmd = "bop decr bkey1 0x0130 1"; $rst = "18446744073709551613";
mem_cmd_is($sock, $cmd, "", $rst, "decr 1 on 18446744073709551614");

$rst = "INVALID incr or decr on non-numeric value";
$cmd = "bop incr bkey1 0x0140 1";
mem_cmd_is($sock, $cmd, "", $rst, "incr 1 on 18446744073709551616");
$cmd = "bop decr bkey1 0x0140 1";
mem_cmd_is($sock, $cmd, "", $rst, "decr 1 on 18446744073709551616");

# Finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
