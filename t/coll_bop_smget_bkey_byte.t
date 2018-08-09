#!/usr/bin/perl

use strict;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

if ("$engine" eq "default" || "$engine" eq "") {
    plan tests => 102;
} else {
    plan tests => 67;
}

my $cmd;
my $val;
my $rst;

mem_cmd_is($sock, "get kvkey", "", "END");
mem_cmd_is($sock, "get bkey1", "", "END");
mem_cmd_is($sock, "get bkey2", "", "END");

# create kvkey, bkey1, bkey2 (fixed length byte-array bkey)
mem_cmd_is($sock, "set kvkey 19 5 10", "some value", "STORED");
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 0x0090 6", "datum9", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0070 6", "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0050 6", "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0030 6", "datum3", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0010 6", "datum1", "STORED");
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 0x0100 7", "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0080 6", "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0060 6", "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0040 6", "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0020 6", "datum2", "STORED");

# NEW smget test
$cmd = "bop smget 11 2 0x0000..0x0100 5"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey1 11 0x0010 6 datum1
bkey2 12 0x0020 6 datum2
bkey1 11 0x0030 6 datum3
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 0x0080..0x0000 5"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey2 12 0x0040 6 datum4
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0x0000..0x0100 5"; $val = "bkey1,bkey2";
$rst = "VALUE 5
bkey1 11 0x0010 6 datum1
bkey2 12 0x0020 6 datum2
bkey1 11 0x0030 6 datum3
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0080..0x0000 5"; $val = "bkey1,bkey2";
$rst = "VALUE 5
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey2 12 0x0040 6 datum4
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 6
bkey1 11 0x0030 6 datum3
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 0x0090..0x0030 2 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 5
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey2 12 0x0040 6 datum4
bkey1 11 0x0030 6 datum3
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 0x0200..0x0300 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 0
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 0x0000..0x0100 1 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst = "ELEMENTS 6
bkey2 12 0x0020 6 datum2
bkey1 11 0x0030 6 datum3
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
}

# OLD smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 6
bkey1 11 0x0030 6 datum3
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 0x0090..0x0030 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 5
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey2 12 0x0040 6 datum4
bkey1 11 0x0030 6 datum3
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 0x0200..0x0300 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 0
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 29 5 0x0000..0x0100 1 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey3";
$rst = "VALUE 6
bkey2 12 0x0020 6 datum2
bkey1 11 0x0030 6 datum3
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
MISSED_KEYS 3
bkey3
bkey4
bkey3
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test: failure
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 23 2 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test: failure
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 23 2 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete items
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# create bkey1, bkey2 (variable length byte-array bkey)
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 0x0090 6",                 "datum9", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x00000070 6",             "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x000000000050 6",         "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0000000000000030 6",     "datum3", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x00000000000000000010 6", "datum1", "STORED");
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 0x01 7",                   "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x000080 6",               "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0000000060 6",           "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x00000000000040 6",       "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x000000000000000020 6",   "datum2", "STORED");
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 5
0x00000000000000000010 6 datum1
0x0000000000000030 6 datum3
0x000000000050 6 datum5
0x00000070 6 datum7
0x0090 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x00..0xFFFFFFFFFFFFFFFFFF";
$rst = "VALUE 12 5
0x000000000000000020 6 datum2
0x00000000000040 6 datum4
0x0000000060 6 datum6
0x000080 6 datum8
0x01 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0x00..0xFF 5"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey1 11 0x00000000000000000010 6 datum1
bkey2 12 0x000000000000000020 6 datum2
bkey1 11 0x0000000000000030 6 datum3
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x000000000050 6 datum5
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0x00..0xFF 5"; $val = "bkey1,bkey2";
$rst = "VALUE 5
bkey1 11 0x00000000000000000010 6 datum1
bkey2 12 0x000000000000000020 6 datum2
bkey1 11 0x0000000000000030 6 datum3
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x000000000050 6 datum5
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0x00..0xFFFF 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 6
bkey1 11 0x0000000000000030 6 datum3
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x000000000050 6 datum5
bkey2 12 0x0000000060 6 datum6
bkey1 11 0x00000070 6 datum7
bkey2 12 0x000080 6 datum8
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 0x0090..0x0000000000000030 2 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 5
bkey1 11 0x00000070 6 datum7
bkey2 12 0x0000000060 6 datum6
bkey1 11 0x000000000050 6 datum5
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x0000000000000030 6 datum3
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 0x0200..0x0300 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 0
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 0x0000..0x0100 1 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst = "ELEMENTS 6
bkey2 12 0x000000000000000020 6 datum2
bkey1 11 0x0000000000000030 6 datum3
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x000000000050 6 datum5
bkey2 12 0x0000000060 6 datum6
bkey1 11 0x00000070 6 datum7
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
}

# OLD smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0x00..0xFFFF 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 6
bkey1 11 0x0000000000000030 6 datum3
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x000000000050 6 datum5
bkey2 12 0x0000000060 6 datum6
bkey1 11 0x00000070 6 datum7
bkey2 12 0x000080 6 datum8
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 0x0090..0x0000000000000030 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 5
bkey1 11 0x00000070 6 datum7
bkey2 12 0x0000000060 6 datum6
bkey1 11 0x000000000050 6 datum5
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x0000000000000030 6 datum3
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 0x0200..0x0300 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 0
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 29 5 0x0000..0x0100 1 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey3";
$rst = "VALUE 6
bkey2 12 0x000000000000000020 6 datum2
bkey1 11 0x0000000000000030 6 datum3
bkey2 12 0x00000000000040 6 datum4
bkey1 11 0x000000000050 6 datum5
bkey2 12 0x0000000060 6 datum6
bkey1 11 0x00000070 6 datum7
MISSED_KEYS 3
bkey3
bkey4
bkey3
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test : failures
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 23 2 0x0000..0x0100 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : failures
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2,bkey3,bkey1,bkey4,kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 29 5 0x0000..0x0100 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 23 2 0x0000..0x0100 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete bkey1, bkey2
mem_cmd_is($sock, "delete kvkey", "", "DELETED");
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# create bkey1, bkey2 (for EFlag Filter test)
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 0x0090 0x11FF 6", "datum9", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0070 0x01FF 6", "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0050 0x00FF 6", "datum5", "STORED");
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 0x0080 0x11FF 6", "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0060 0x01FF 6", "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0040 0x00FF 6", "datum4", "STORED");

# NEW smget test
$cmd = "bop smget 11 2 0x0000..0xFFFF 0 EQ 0x11FF,0x01FF 6"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 4
bkey2 12 0x0060 0x01FF 6 datum6
bkey1 11 0x0070 0x01FF 6 datum7
bkey2 12 0x0080 0x11FF 6 datum8
bkey1 11 0x0090 0x11FF 6 datum9
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 0x0000..0xFFFF 0 NE 0x00FF,0x11FF 6"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 2
bkey2 12 0x0060 0x01FF 6 datum6
bkey1 11 0x0070 0x01FF 6 datum7
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0x0000..0xFFFF 0 EQ 0x11FF,0x01FF 6"; $val = "bkey1 bkey2";
$rst = "VALUE 4
bkey2 12 0x0060 0x01FF 6 datum6
bkey1 11 0x0070 0x01FF 6 datum7
bkey2 12 0x0080 0x11FF 6 datum8
bkey1 11 0x0090 0x11FF 6 datum9
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0000..0xFFFF 0 NE 0x00FF,0x11FF 6"; $val = "bkey1 bkey2";
$rst = "VALUE 2
bkey2 12 0x0060 0x01FF 6 datum6
bkey1 11 0x0070 0x01FF 6 datum7
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete bkey1, bkey2
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# after test
release_memcached($engine, $server);
