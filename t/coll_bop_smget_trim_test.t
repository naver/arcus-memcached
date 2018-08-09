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
    plan tests => 263;
} else {
    plan tests => 206;
}

my $cmd;
my $val;
my $rst;

mem_cmd_is($sock, "get kvkey", "", "END");
mem_cmd_is($sock, "get bkey1", "", "END");
mem_cmd_is($sock, "get bkey2", "", "END");

# create kvkey
mem_cmd_is($sock, "set kvkey 19 5 10", "some value", "STORED");

# create bkey1
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "setattr bkey1 maxcount=5", "", "OK");
mem_cmd_is($sock, "bop insert bkey1 10 6", "datum1", "STORED");
mem_cmd_is($sock, "bop insert bkey1 20 6", "datum2", "STORED");
mem_cmd_is($sock, "bop insert bkey1 30 6", "datum3", "STORED");
mem_cmd_is($sock, "bop insert bkey1 40 6", "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey1 50 6", "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey1 70 6", "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey1 90 6", "datum9", "STORED");
$cmd = "getattr bkey1 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=smallest_trim
ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..100";
$rst = "VALUE 11 5
30 6 datum3
40 6 datum4
50 6 datum5
70 6 datum7
90 6 datum9
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 100..0";
$rst = "VALUE 11 5
90 6 datum9
70 6 datum7
50 6 datum5
40 6 datum4
30 6 datum3
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 30..100";
$rst = "VALUE 11 5
30 6 datum3
40 6 datum4
50 6 datum5
70 6 datum7
90 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 100..30";
$rst = "VALUE 11 5
90 6 datum9
70 6 datum7
50 6 datum5
40 6 datum4
30 6 datum3
END";
mem_cmd_is($sock, $cmd, "", $rst);

# create bkey2
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "setattr bkey2 maxcount=5", "", "OK");
mem_cmd_is($sock, "bop insert bkey2 20 6", "datum2", "STORED");
mem_cmd_is($sock, "bop insert bkey2 40 6", "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey2 60 6", "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 80 6", "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 100 7", "datum10", "STORED");
$cmd = "getattr bkey2 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=smallest_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..100";
$rst = "VALUE 12 5
20 6 datum2
40 6 datum4
60 6 datum6
80 6 datum8
100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 100..0";
$rst = "VALUE 12 5
100 7 datum10
80 6 datum8
60 6 datum6
40 6 datum4
20 6 datum2
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0..100 5"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey2 12 20 6 datum2
bkey2 12 40 6 datum4
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 1
bkey1 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 100..0 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
MISSED_KEYS 0
TRIMMED_KEYS 1
bkey1 30
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 9
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
MISSED_KEYS 0
TRIMMED_KEYS 1
bkey1 30
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 100..30 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 9
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 0
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 8
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0..100 5"; $val = "bkey1,bkey2";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 100..0 10"; $val = "bkey1,bkey2";
$rst = "VALUE 9
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 0
DUPLICATED_TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 100..30 10"; $val = "bkey1,bkey2";
$rst = "VALUE 9
bkey2 12 100 7 datum10
bkey1 11 90 6 datum9
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0..100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 3
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 3
bkey3 NOT_FOUND
bkey1 OUT_OF_RANGE
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 90..30 2 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 6
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 5
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 30..90 2 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 6
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
bkey1 11 90 6 datum9
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 5
bkey1 11 50 6 datum5
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
bkey1 11 90 6 datum9
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 100..0 2 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 8
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 1
bkey1 30
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 7
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 30 6 datum3
bkey2 12 20 6 datum2
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 1
bkey1 30
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 23 4 200..300 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "ELEMENTS 0
MISSED_KEYS 2
bkey3 NOT_FOUND
bkey4 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 40..0 4 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 0
MISSED_KEYS 0
TRIMMED_KEYS 1
bkey1 30
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 0..100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "ELEMENTS 3
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 4
bkey3 NOT_FOUND
bkey1 OUT_OF_RANGE
bkey4 NOT_FOUND
bkey1 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 0..100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst = "ELEMENTS 3
bkey2 12 60 6 datum6
bkey2 12 80 6 datum8
bkey2 12 100 7 datum10
MISSED_KEYS 4
bkey3 NOT_FOUND
bkey1 OUT_OF_RANGE
bkey4 NOT_FOUND
bkey3 NOT_FOUND
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
}

# OLD smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 0..100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 90..30 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 6
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3
bkey4
DUPLICATED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 30..90 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 6
bkey2 12 40 6 datum4
bkey1 11 50 6 datum5
bkey2 12 60 6 datum6
bkey1 11 70 6 datum7
bkey2 12 80 6 datum8
bkey1 11 90 6 datum9
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 100..0 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst ="VALUE 7
bkey2 12 80 6 datum8
bkey1 11 70 6 datum7
bkey2 12 60 6 datum6
bkey1 11 50 6 datum5
bkey2 12 40 6 datum4
bkey1 11 40 6 datum4
bkey1 11 30 6 datum3
MISSED_KEYS 2
bkey3
bkey4
DUPLICATED_TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 23 4 200..300 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE 0
MISSED_KEYS 2
bkey3
bkey4
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 40..0 4 10"; $val = "bkey1,bkey2";
$rst = "VALUE 0
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 29 5 0..100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey1";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 29 5 0..100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey3";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test: failures
$cmd = "bop smget 23 2 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey1 bkey4 kvkey";
$rst = "TYPE_MISMATCH";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2 bkey3 bkey2 bkey4 bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test: failures
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 2 0..100 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2,bkey3,bkey1,bkey4,kvkey";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 29 5 0..100 6"; $val = "bkey2,bkey3,bkey2,bkey4,bkey1";
$rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete kvkey, bkey1, bkey2
mem_cmd_is($sock, "delete kvkey", "", "DELETED");
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# create bkey1, bkey2, bkey3, bkey4, bkey5
mem_cmd_is($sock, "bop create bkey1 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop create bkey2 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop create bkey3 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop create bkey4 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop create bkey5 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 11 0x01 7",  "datum11", "STORED");
mem_cmd_is($sock, "bop insert bkey2 12 0x02 7",  "datum12", "STORED");
mem_cmd_is($sock, "bop insert bkey3 13 0x01 7",  "datum13", "STORED");
mem_cmd_is($sock, "bop insert bkey4 14 0x02 7",  "datum14", "STORED");
mem_cmd_is($sock, "bop insert bkey5 15 0x01 7",  "datum15", "STORED");
mem_cmd_is($sock, "bop insert bkey1 16 0x02 7",  "datum16", "STORED");
mem_cmd_is($sock, "bop insert bkey2 17 0x01 7",  "datum17", "STORED");
mem_cmd_is($sock, "bop insert bkey3 18 0x02 7",  "datum18", "STORED");
mem_cmd_is($sock, "bop insert bkey4 19 0x01 7",  "datum19", "STORED");
mem_cmd_is($sock, "bop insert bkey5 20 0x02 7",  "datum20", "STORED");
mem_cmd_is($sock, "bop insert bkey1 21 0x03 7",  "datum21", "STORED");
mem_cmd_is($sock, "bop insert bkey2 22 0x03 7",  "datum22", "STORED");
mem_cmd_is($sock, "bop insert bkey3 23 0x03 7",  "datum23", "STORED");
mem_cmd_is($sock, "bop insert bkey4 24 0x03 7",  "datum24", "STORED");

# NEW smget test
$cmd = "bop smget 29 5 24..11 14"; $val = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 10
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
bkey5 0 20 0x02 7 datum20
bkey4 0 19 0x01 7 datum19
bkey3 0 18 0x02 7 datum18
bkey2 0 17 0x01 7 datum17
bkey1 0 16 0x02 7 datum16
bkey5 0 15 0x01 7 datum15
MISSED_KEYS 0
TRIMMED_KEYS 4
bkey4 19
bkey3 18
bkey2 17
bkey1 16
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 24..11 0 EQ 0x01 14"; $val = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 3
bkey4 0 19 0x01 7 datum19
bkey2 0 17 0x01 7 datum17
bkey5 0 15 0x01 7 datum15
MISSED_KEYS 0
TRIMMED_KEYS 4
bkey4 19
bkey3 18
bkey2 17
bkey1 16
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 24..11 0 EQ 0x02 14"; $rst = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 3
bkey5 0 20 0x02 7 datum20
bkey3 0 18 0x02 7 datum18
bkey1 0 16 0x02 7 datum16
MISSED_KEYS 0
TRIMMED_KEYS 4
bkey4 19
bkey3 18
bkey2 17
bkey1 16
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 24..11 0 EQ 0x03 14"; $rst = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 4
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
MISSED_KEYS 0
TRIMMED_KEYS 4
bkey4 19
bkey3 18
bkey2 17
bkey1 16
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 24..17 0 EQ 0x03 14"; $rst = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 4
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
MISSED_KEYS 0
TRIMMED_KEYS 2
bkey4 19
bkey3 18
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 24..11 0 EQ 0x03 4"; $rst = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 4
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 11..24 14"; $val = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 2
bkey5 0 15 0x01 7 datum15
bkey5 0 20 0x02 7 datum20
MISSED_KEYS 4
bkey1 OUT_OF_RANGE
bkey2 OUT_OF_RANGE
bkey3 OUT_OF_RANGE
bkey4 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 29 5 24..11 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "VALUE 6
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
bkey5 0 20 0x02 7 datum20
bkey4 0 19 0x01 7 datum19
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

# problem) the first scan of <bkey3> is trim. so, return OUT_OF_RANGE.
$cmd = "bop smget 29 5 24..11 0 EQ 0x01 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);

# problem) the first scan of <bkey4> is trim. so, return OUT_OF_RANGE.
$cmd = "bop smget 29 5 24..11 0 EQ 0x02 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);

# problem) the next scan of <bkey4, 24> is trim. so, stop smget.
$cmd = "bop smget 29 5 24..11 0 EQ 0x03 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "VALUE 1
bkey4 0 24 0x03 7 datum24
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

# problem) the next scan of <bkey4, 24> is trim. so, stop smget.
$cmd = "bop smget 29 5 24..17 0 EQ 0x03 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "VALUE 1
bkey4 0 24 0x03 7 datum24
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

# problem) the next scan of <bkey4, 24> is trim. so, stop smget.
$cmd = "bop smget 29 5 24..11 0 EQ 0x03 4"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "VALUE 1
bkey4 0 24 0x03 7 datum24
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 29 5 11..24 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 29 5 24..11 0 EQ 0x03 2 2"; $val = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 2
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 24..11 0 EQ 0x03 2 3"; $val = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 2
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
MISSED_KEYS 0
TRIMMED_KEYS 4
bkey4 19
bkey3 18
bkey2 17
bkey1 16
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
}

# OLD smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
# problem) the next scan of <bkey4, 24> is trim. so, stop smget.
$cmd = "bop smget 29 5 24..11 0 EQ 0x03 2 2"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "VALUE 0
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

# problem) the next scan of <bkey4, 24> is trim. so, stop smget.
$cmd = "bop smget 29 5 24..11 0 EQ 0x03 2 3"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "VALUE 0
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# add an element into bkey1, bkey2
mem_cmd_is($sock, "bop insert bkey1 23 0x03 7",  "datum23", "STORED");
mem_cmd_is($sock, "bop insert bkey2 23 0x03 7",  "datum23", "STORED");

# NEW smget test
$cmd = "bop smget 29 5 24..11 14"; $val = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 10
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 23 0x03 7 datum23
bkey1 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
bkey5 0 20 0x02 7 datum20
bkey4 0 19 0x01 7 datum19
bkey3 0 18 0x02 7 datum18
bkey5 0 15 0x01 7 datum15
MISSED_KEYS 0
TRIMMED_KEYS 4
bkey2 22
bkey1 21
bkey4 19
bkey3 18
DUPLICATED";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
$rst = "ELEMENTS 8
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
bkey1 0 21 0x03 7 datum21
bkey5 0 20 0x02 7 datum20
bkey4 0 19 0x01 7 datum19
bkey3 0 18 0x02 7 datum18
bkey5 0 15 0x01 7 datum15
MISSED_KEYS 0
TRIMMED_KEYS 4
bkey2 22
bkey1 21
bkey4 19
bkey3 18
END";
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 29 5 11..24 14"; $val = "bkey1 bkey2 bkey3 bkey4 bkey5";
$rst = "ELEMENTS 2
bkey5 0 15 0x01 7 datum15
bkey5 0 20 0x02 7 datum20
MISSED_KEYS 4
bkey1 OUT_OF_RANGE
bkey2 OUT_OF_RANGE
bkey3 OUT_OF_RANGE
bkey4 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 29 5 24..11 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "VALUE 5
bkey4 0 24 0x03 7 datum24
bkey3 0 23 0x03 7 datum23
bkey2 0 23 0x03 7 datum23
bkey1 0 23 0x03 7 datum23
bkey2 0 22 0x03 7 datum22
MISSED_KEYS 0
DUPLICATED_TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 29 5 11..24 14"; $val = "bkey1,bkey2,bkey3,bkey4,bkey5";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete bkey1, bkey2, bkey3, bkey4, bkey5
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");
mem_cmd_is($sock, "delete bkey3", "", "DELETED");
mem_cmd_is($sock, "delete bkey4", "", "DELETED");
mem_cmd_is($sock, "delete bkey5", "", "DELETED");

# SMGET TRIM ISSUE 1

# create bkey1, bkey2, bkey3, bkey4
mem_cmd_is($sock, "bop create bkey1 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop create bkey2 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop create bkey3 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop create bkey4 0 0 2", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 1 0x01 6",  "datum1", "STORED");
mem_cmd_is($sock, "bop insert bkey1 6 0x02 6",  "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey1 7 0x01 6",  "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey2 2 0x01 6",  "datum2", "STORED");
mem_cmd_is($sock, "bop insert bkey2 5 0x01 6",  "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey2 8 0x01 6",  "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey3 3 0x01 6",  "datum3", "STORED");
mem_cmd_is($sock, "bop insert bkey3 4 0x01 6",  "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey4 0 0x01 6",  "datum0", "STORED");
mem_cmd_is($sock, "bop insert bkey4 1 0x01 6",  "datum1", "STORED");
mem_cmd_is($sock, "bop insert bkey4 2 0x01 6",  "datum2", "STORED");

# NEW smget test
$cmd = "bop smget 23 4 8..0 3 EQ 0x01 2"; $val = "bkey1 bkey2 bkey3 bkey4";
$rst = "ELEMENTS 0
MISSED_KEYS 0
TRIMMED_KEYS 3
bkey1 6
bkey2 5
bkey4 1
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 8..0 3 EQ 0x01 2"; $val = "bkey1,bkey2,bkey3,bkey4";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# NEW smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 8..0 0 EQ 0x01 3 2"; $val = "bkey1 bkey2 bkey3 bkey4";
$rst = "ELEMENTS 2
bkey3 0 4 0x01 6 datum4
bkey3 0 3 0x01 6 datum3
MISSED_KEYS 0
TRIMMED_KEYS 2
bkey1 6
bkey2 5
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);
}

# OLD smget test (offset is used)
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 23 4 8..0 0 EQ 0x01 3 2"; $val = "bkey1,bkey2,bkey3,bkey4";
$rst = "VALUE 0
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete bkey1, bkey2, bkey3, bkey4, bkey5
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");
mem_cmd_is($sock, "delete bkey3", "", "DELETED");
mem_cmd_is($sock, "delete bkey4", "", "DELETED");

# SMGET TRIM OTHERS

# create bkey1 (smallest_trim)
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 0x0090 6",  "datum9", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0070 6",  "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0050 6",  "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0030 6",  "datum3", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0010 6",  "datum1", "STORED");
mem_cmd_is($sock, "setattr bkey1 maxcount=5", "", "OK");
$cmd = "getattr bkey1 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=smallest_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0000..0x0200";
$rst = "VALUE 11 5
0x0010 6 datum1
0x0030 6 datum3
0x0050 6 datum5
0x0070 6 datum7
0x0090 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);

# make trim on bkey1
mem_cmd_is($sock, "bop insert bkey1 0x0110 7",  "datum11", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);
mem_cmd_is($sock, "bop insert bkey1 0x0130 7",  "datum13", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0000..0x0200";
$rst = "VALUE 11 5
0x0050 6 datum5
0x0070 6 datum7
0x0090 6 datum9
0x0110 7 datum11
0x0130 7 datum13
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0200..0x0000";
$rst = "VALUE 11 5
0x0130 7 datum13
0x0110 7 datum11
0x0090 6 datum9
0x0070 6 datum7
0x0050 6 datum5
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);

# create bkey2 (smallest_trim)
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 0x0100 7",  "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0080 6",  "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0060 6",  "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0040 6",  "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0020 6",  "datum2", "STORED");
mem_cmd_is($sock, "setattr bkey2 maxcount=5", "", "OK");
$cmd = "getattr bkey2 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=smallest_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0x0200";
$rst = "VALUE 12 5
0x0020 6 datum2
0x0040 6 datum4
0x0060 6 datum6
0x0080 6 datum8
0x0100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey2 12 0x0020 6 datum2
bkey2 12 0x0040 6 datum4
bkey2 12 0x0060 6 datum6
bkey2 12 0x0080 6 datum8
bkey2 12 0x0100 7 datum10
MISSED_KEYS 1
bkey1 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey1 11 0x0130 7 datum13
bkey1 11 0x0110 7 datum11
bkey2 12 0x0100 7 datum10
bkey1 11 0x0090 6 datum9
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey2 12 0x0040 6 datum4
bkey2 12 0x0020 6 datum2
MISSED_KEYS 0
TRIMMED_KEYS 1
bkey1 0x0050
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1,bkey2";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1,bkey2";
$rst = "VALUE 8
bkey1 11 0x0130 7 datum13
bkey1 11 0x0110 7 datum11
bkey2 12 0x0100 7 datum10
bkey1 11 0x0090 6 datum9
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# setattr largest_trim on bkey1
mem_cmd_is($sock, "setattr bkey1 overflowaction=largest_trim", "", "OK");
$cmd = "getattr bkey1 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=largest_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0000..0x0200";
$rst = "VALUE 11 5
0x0050 6 datum5
0x0070 6 datum7
0x0090 6 datum9
0x0110 7 datum11
0x0130 7 datum13
END";
mem_cmd_is($sock, $cmd, "", $rst);

# make trim on bkey1
mem_cmd_is($sock, "bop insert bkey1 0x0150 7",  "datum15", "OUT_OF_RANGE");
mem_cmd_is($sock, "bop insert bkey1 0x0030 6",  "datum3", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);
mem_cmd_is($sock, "bop insert bkey1 0x0010 6",  "datum1", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);

# make trim on bkey2
mem_cmd_is($sock, "bop insert bkey2 0x0120 7",  "datum12", "STORED");
$cmd = "getattr bkey2 trimmed";
$rst = "ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);
mem_cmd_is($sock, "bop insert bkey2 0x0140 7",  "datum14", "STORED");
$cmd = "getattr bkey2 trimmed";
$rst = "ATTR trimmed=1
END";
mem_cmd_is($sock, $cmd, "", $rst);

# setattr largest_trim on bkey2
mem_cmd_is($sock, "setattr bkey2 overflowaction=largest_trim", "", "OK");
$cmd = "getattr bkey2 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=largest_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0x0200";
$rst = "VALUE 12 5
0x0060 6 datum6
0x0080 6 datum8
0x0100 7 datum10
0x0120 7 datum12
0x0140 7 datum14
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey1 11 0x0010 6 datum1
bkey1 11 0x0030 6 datum3
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
bkey1 11 0x0090 6 datum9
bkey2 12 0x0100 7 datum10
bkey2 12 0x0120 7 datum12
bkey2 12 0x0140 7 datum14
MISSED_KEYS 0
TRIMMED_KEYS 1
bkey1 0x0090
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 5
bkey2 12 0x0140 7 datum14
bkey2 12 0x0120 7 datum12
bkey2 12 0x0100 7 datum10
bkey2 12 0x0080 6 datum8
bkey2 12 0x0060 6 datum6
MISSED_KEYS 1
bkey1 OUT_OF_RANGE
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1,bkey2";
$rst = "VALUE 7
bkey1 11 0x0010 6 datum1
bkey1 11 0x0030 6 datum3
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
bkey1 11 0x0090 6 datum9
MISSED_KEYS 0
TRIMMED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1 bkey2";
$rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete bkey1, bkey2
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# create bkey1 (smallest_silent_trim)
mem_cmd_is($sock, "bop create bkey1 11 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey1 0x0090 6",  "datum9", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0070 6",  "datum7", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0050 6",  "datum5", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0030 6",  "datum3", "STORED");
mem_cmd_is($sock, "bop insert bkey1 0x0010 6",  "datum1", "STORED");
mem_cmd_is($sock, "setattr bkey1 maxcount=5 overflowaction=smallest_silent_trim", "", "OK");
$cmd = "getattr bkey1 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=smallest_silent_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0000..0x0200";
$rst = "VALUE 11 5
0x0010 6 datum1
0x0030 6 datum3
0x0050 6 datum5
0x0070 6 datum7
0x0090 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);

# make trim on bkey1
mem_cmd_is($sock, "bop insert bkey1 0x0110 7",  "datum11", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
mem_cmd_is($sock, "bop insert bkey1 0x0130 7",  "datum13", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0000..0x0200";
$rst = "VALUE 11 5
0x0050 6 datum5
0x0070 6 datum7
0x0090 6 datum9
0x0110 7 datum11
0x0130 7 datum13
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0200..0x0000";
$rst = "VALUE 11 5
0x0130 7 datum13
0x0110 7 datum11
0x0090 6 datum9
0x0070 6 datum7
0x0050 6 datum5
END";
mem_cmd_is($sock, $cmd, "", $rst);

# create bkey2 (smallest_silent_trim)
mem_cmd_is($sock, "bop create bkey2 12 0 0", "", "CREATED");
mem_cmd_is($sock, "bop insert bkey2 0x0100 7",  "datum10", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0080 6",  "datum8", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0060 6",  "datum6", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0040 6",  "datum4", "STORED");
mem_cmd_is($sock, "bop insert bkey2 0x0020 6",  "datum2", "STORED");
mem_cmd_is($sock, "setattr bkey2 maxcount=5 overflowaction=smallest_silent_trim", "", "OK");
$cmd = "getattr bkey2 maxcount overflowaction";
$rst = "ATTR maxcount=5
ATTR overflowaction=smallest_silent_trim
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0x0200";
$rst = "VALUE 12 5
0x0020 6 datum2
0x0040 6 datum4
0x0060 6 datum6
0x0080 6 datum8
0x0100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey2 12 0x0020 6 datum2
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
bkey1 11 0x0090 6 datum9
bkey2 12 0x0100 7 datum10
bkey1 11 0x0110 7 datum11
bkey1 11 0x0130 7 datum13
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey1 11 0x0130 7 datum13
bkey1 11 0x0110 7 datum11
bkey2 12 0x0100 7 datum10
bkey1 11 0x0090 6 datum9
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey2 12 0x0040 6 datum4
bkey2 12 0x0020 6 datum2
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1,bkey2";
$rst = "VALUE 10
bkey2 12 0x0020 6 datum2
bkey2 12 0x0040 6 datum4
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
bkey1 11 0x0090 6 datum9
bkey2 12 0x0100 7 datum10
bkey1 11 0x0110 7 datum11
bkey1 11 0x0130 7 datum13
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1,bkey2";
$rst = "VALUE 10
bkey1 11 0x0130 7 datum13
bkey1 11 0x0110 7 datum11
bkey2 12 0x0100 7 datum10
bkey1 11 0x0090 6 datum9
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey2 12 0x0040 6 datum4
bkey2 12 0x0020 6 datum2
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# setattr largest_silent_trim on bkey1
mem_cmd_is($sock, "setattr bkey1 overflowaction=largest_silent_trim", "", "OK");
$cmd = "getattr bkey1 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=largest_silent_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0x0000..0x0200";
$rst = "VALUE 11 5
0x0050 6 datum5
0x0070 6 datum7
0x0090 6 datum9
0x0110 7 datum11
0x0130 7 datum13
END";
mem_cmd_is($sock, $cmd, "", $rst);

# make trim on bkey1
mem_cmd_is($sock, "bop insert bkey1 0x0150 7",  "datum15", "OUT_OF_RANGE");
mem_cmd_is($sock, "bop insert bkey1 0x0030 6",  "datum3", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
mem_cmd_is($sock, "bop insert bkey1 0x0010 6",  "datum1", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);

# make trim on bkey2
mem_cmd_is($sock, "bop insert bkey2 0x0120 7",  "datum12", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
mem_cmd_is($sock, "bop insert bkey2 0x0140 7",  "datum14", "STORED");
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);

# setattr largest_silent_trim on bkey2
mem_cmd_is($sock, "setattr bkey2 overflowaction=largest_silent_trim", "", "OK");
$cmd = "getattr bkey2 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=largest_silent_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0x0000..0x0200";
$rst = "VALUE 12 5
0x0060 6 datum6
0x0080 6 datum8
0x0100 7 datum10
0x0120 7 datum12
0x0140 7 datum14
END";
mem_cmd_is($sock, $cmd, "", $rst);

# NEW smget test
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey1 11 0x0010 6 datum1
bkey1 11 0x0030 6 datum3
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
bkey1 11 0x0090 6 datum9
bkey2 12 0x0100 7 datum10
bkey2 12 0x0120 7 datum12
bkey2 12 0x0140 7 datum14
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1 bkey2";
$rst = "ELEMENTS 10
bkey2 12 0x0140 7 datum14
bkey2 12 0x0120 7 datum12
bkey2 12 0x0100 7 datum10
bkey1 11 0x0090 6 datum9
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey1 11 0x0030 6 datum3
bkey1 11 0x0010 6 datum1
MISSED_KEYS 0
TRIMMED_KEYS 0
END";
mem_cmd_is($sock, $cmd . " duplicate", $val, $rst);
mem_cmd_is($sock, $cmd . " unique", $val, $rst);

# OLD smget test : Use comma separated keys
if ("$engine" eq "default" || "$engine" eq "") {
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1,bkey2";
$rst = "VALUE 10
bkey1 11 0x0010 6 datum1
bkey1 11 0x0030 6 datum3
bkey1 11 0x0050 6 datum5
bkey2 12 0x0060 6 datum6
bkey1 11 0x0070 6 datum7
bkey2 12 0x0080 6 datum8
bkey1 11 0x0090 6 datum9
bkey2 12 0x0100 7 datum10
bkey2 12 0x0120 7 datum12
bkey2 12 0x0140 7 datum14
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1,bkey2";
$rst = "VALUE 10
bkey2 12 0x0140 7 datum14
bkey2 12 0x0120 7 datum12
bkey2 12 0x0100 7 datum10
bkey1 11 0x0090 6 datum9
bkey2 12 0x0080 6 datum8
bkey1 11 0x0070 6 datum7
bkey2 12 0x0060 6 datum6
bkey1 11 0x0050 6 datum5
bkey1 11 0x0030 6 datum3
bkey1 11 0x0010 6 datum1
MISSED_KEYS 0
END";
mem_cmd_is($sock, $cmd, $val, $rst);
}

# delete bkey1, bkey2
mem_cmd_is($sock, "delete bkey1", "", "DELETED");
mem_cmd_is($sock, "delete bkey2", "", "DELETED");

# after test
release_memcached($engine, $server);
