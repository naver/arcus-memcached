#!/usr/bin/perl

use strict;
use Test::More tests => 95;
=head
use Test::More tests => 115;
=cut
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1
get bkey2

bop insert bkey1 0x0090 6 create 11 0 0
datum9
bop insert bkey1 0x0070 6
datum7
bop insert bkey1 0x0050 6
datum5
bop insert bkey1 0x0030 6
datum3
bop insert bkey1 0x0010 6
datum1
bop insert bkey2 0x0100 7 create 12 0 0
datum10
bop insert bkey2 0x0080 6
datum8
bop insert bkey2 0x0060 6
datum6
bop insert bkey2 0x0040 6
datum4
bop insert bkey2 0x0020 6
datum2

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

# Prepare Keys
$cmd = "bop insert bkey1 0x0090 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0070 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0050 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 maxcount=5"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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

$cmd = "bop insert bkey1 0x0110 7"; $val = "datum11"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1\nEND";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop insert bkey1 0x0130 7"; $val = "datum13"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1\nEND";
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
$cmd = "bop insert bkey2 0x0100 7 create 12 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0080 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0060 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0040 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0020 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey2 maxcount=5"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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

# smgets
$cmd = "bop smget 11 2 0x0000..0x0200 10 duplicate"; $val = "bkey1 bkey2";
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
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10 duplicate"; $val = "bkey1 bkey2";
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
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget test : Use comma separated keys
$cmd = "bop smget 11 2 0x0000..0x0200 10"; $val = "bkey1,bkey2"; $rst = "OUT_OF_RANGE";
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

# prepare keys 2
$cmd = "setattr bkey1 overflowaction=largest_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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
$cmd = "bop insert bkey1 0x0150 7"; $val = "datum15"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=1\nEND";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop insert bkey2 0x0120 7"; $val = "datum12"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey2 trimmed";
$rst = "ATTR trimmed=1\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 0x0140 7"; $val = "datum14"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey2 trimmed";
$rst = "ATTR trimmed=1\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey2 overflowaction=largest_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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

# smgets 2
$cmd = "bop smget 11 2 0x0000..0x0200 10 duplicate"; $val = "bkey1 bkey2";
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
mem_cmd_is($sock, $cmd, $val, $rst);

bop_new_smget_is($sock, "11 2 0x0200..0x0000 10 duplicate", "bkey1 bkey2",
5,
"bkey2 12 0x0140 7 datum14
,bkey2 12 0x0120 7 datum12
,bkey2 12 0x0100 7 datum10
,bkey2 12 0x0080 6 datum8
,bkey2 12 0x0060 6 datum6",
1,
"bkey1 OUT_OF_RANGE",
0, "",
"END");
# OLD smget test : Use comma separated keys
bop_old_smget_is($sock, "11 2 0x0000..0x0200 10", "bkey1,bkey2",
7,
"bkey1 11 0x0010 6 datum1
,bkey1 11 0x0030 6 datum3
,bkey1 11 0x0050 6 datum5
,bkey2 12 0x0060 6 datum6
,bkey1 11 0x0070 6 datum7
,bkey2 12 0x0080 6 datum8
,bkey1 11 0x0090 6 datum9",
0, "",
"TRIMMED");
$cmd = "bop smget 11 2 0x0200..0x0000 10"; $val = "bkey1 bkey2"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);

# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# prepare keys
$cmd = "bop insert bkey1 0x0090 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0070 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0050 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 maxcount=5 overflowaction=smallest_silent_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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
$cmd = "bop insert bkey1 0x0110 7"; $val = "datum11"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0130 7"; $val = "datum13"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0\nEND";
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
$cmd = "bop insert bkey2 0x0100 7 create 12 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0080 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0060 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0040 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0020 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey2 maxcount=5 overflowaction=smallest_silent_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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

# smgets
$cmd = "bop smget 11 2 0x0000..0x0200 10 duplicate"; $val = "bkey1 bkey2";
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
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop smget 11 2 0x0200..0x0000 10 duplicate"; $val = "bkey1 bkey2";
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
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget test : Use comma separated keys
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

# prepare keys 2
$cmd = "setattr bkey1 overflowaction=largest_silent_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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
$cmd = "bop insert bkey1 0x0150 7"; $val = "datum15"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed";
$rst = "ATTR trimmed=0\nEND";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop insert bkey2 0x0120 7"; $val = "datum12"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey2 trimmed";
$rst = "ATTR trimmed=0\nEND";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 0x0140 7"; $val = "datum14"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey2 trimmed";
$rst = "ATTR trimmed=0\nEND";
$cmd = "setattr bkey2 overflowaction=largest_silent_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
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

# smgets 2
$cmd = "bop smget 11 2 0x0000..0x0200 10 duplicate"; $val = "bkey1 bkey2";
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
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop smget 11 2 0x0200..0x0000 10 duplicate"; $val = "bkey1 bkey2";
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
mem_cmd_is($sock, $cmd, $val, $rst);

# OLD smget test : Use comma separated keys
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

# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
