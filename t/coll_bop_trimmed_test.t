#!/usr/bin/perl

use strict;
use Test::More tests => 58;
=head
use Test::More tests => 63;
=cut
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

=head
bop insert bkey1 10 7 create 12 0 1000
datum01
bop insert bkey1 30 7
datum03
bop insert bkey1 50 7
datum05
bop insert bkey1 70 7
datum07
bop insert bkey1 90 7
datum09
setattr bkey1 maxcount=5
getattr bkey1
bop insert bkey1 110 7
datum11
getattr bkey1
bop get bkey1 0..1000
bop get bkey1 1000..0
bop get bkey1 0..20
bop get bkey1 20..0
bop get bkey1 20..1000
bop get bkey1 1000..20
bop get bkey1 20
bop get bkey1 30..1000
bop get bkey1 1000..30
bop get bkey1 30
bop get bkey1 40..1000
bop get bkey1 1000..40
bop get bkey1 40
bop get bkey1 200..1000
bop get bkey1 1000..200
bop get bkey1 200
bop insert bkey2 20 7 create 12 0 1000
datum02
bop insert bkey2 40 7
datum04
bop insert bkey2 60 7
datum06
bop insert bkey2 80 7
datum08
bop insert bkey2 100 7
datum10
setattr bkey2 maxcount=5 overflowaction=largest_trim
getattr bkey2
bop insert bkey2 10 7
datum01
getattr bkey2
bop get bkey2 0..1000
bop get bkey2 1000..0
bop get bkey2 90..0
bop get bkey2 0..90
bop get bkey2 90
bop get bkey2 80..0
bop get bkey2 0..80
bop get bkey2 80
bop get bkey2 70..0
bop get bkey2 0..70
bop get bkey2 70
bop get bkey2 0..5
bop get bkey2 5..0
bop get bkey2 5
setattr bkey2 overflowaction=smallest_trim
getattr bkey2
bop insert bkey2 100 7
datum10
bop insert bkey2 120 7
datum12
getattr bkey2
bop get bkey2 120..0 10
bop get bkey2 120..40 10
delete bkey1
delete bkey2
=cut

$cmd = "get bkey1"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "get bkey2"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "bop insert bkey1 10 7 create 12 0 1000"; $val = "datum01"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 30 7"; $val = "datum03"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 50 7"; $val = "datum05"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 70 7"; $val = "datum07"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 90 7"; $val = "datum09"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey1 maxcount=5"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey1 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=smallest_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey1 110 7"; $val = "datum11"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey1 trimmed minbkey maxbkey";
$rst = "ATTR trimmed=1
ATTR minbkey=30
ATTR maxbkey=110
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..1000";
$rst = "VALUE 12 5
30 7 datum03
50 7 datum05
70 7 datum07
90 7 datum09
110 7 datum11
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 1000..0";
$rst = "VALUE 12 5
110 7 datum11
90 7 datum09
70 7 datum07
50 7 datum05
30 7 datum03
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 0..20"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 20..0"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 20..1000";
$rst = "VALUE 12 5
30 7 datum03
50 7 datum05
70 7 datum07
90 7 datum09
110 7 datum11
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 1000..20";
$rst = "VALUE 12 5
110 7 datum11
90 7 datum09
70 7 datum07
50 7 datum05
30 7 datum03
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 20"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 30..1000";
$rst = "VALUE 12 5
30 7 datum03
50 7 datum05
70 7 datum07
90 7 datum09
110 7 datum11
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 1000..30";
$rst = "VALUE 12 5
110 7 datum11
90 7 datum09
70 7 datum07
50 7 datum05
30 7 datum03
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 30";
$rst = "VALUE 12 1
30 7 datum03
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 40..1000";
$rst = "VALUE 12 4
50 7 datum05
70 7 datum07
90 7 datum09
110 7 datum11
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 1000..40";
$rst = "VALUE 12 4
110 7 datum11
90 7 datum09
70 7 datum07
50 7 datum05
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 40"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 200..1000"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 1000..200"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey1 200"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 20 7 create 12 0 1000"; $val = "datum02"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 40 7"; $val = "datum04"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 60 7"; $val = "datum06"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 80 7"; $val = "datum08"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "setattr bkey2 maxcount=5 overflowaction=largest_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey2 maxcount overflowaction trimmed";
$rst = "ATTR maxcount=5
ATTR overflowaction=largest_trim
ATTR trimmed=0
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 10 7"; $val = "datum01"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "getattr bkey2 trimmed minbkey maxbkey";
$rst = "ATTR trimmed=1
ATTR minbkey=10
ATTR maxbkey=80
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..1000";
$rst = "VALUE 12 5
10 7 datum01
20 7 datum02
40 7 datum04
60 7 datum06
80 7 datum08
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 1000..0";
$rst = "VALUE 12 5
80 7 datum08
60 7 datum06
40 7 datum04
20 7 datum02
10 7 datum01
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 90..0";
$rst = "VALUE 12 5
80 7 datum08
60 7 datum06
40 7 datum04
20 7 datum02
10 7 datum01
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..90";
$rst = "VALUE 12 5
10 7 datum01
20 7 datum02
40 7 datum04
60 7 datum06
80 7 datum08
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 90"; $rst = "OUT_OF_RANGE";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 80..0";
$rst = "VALUE 12 5
80 7 datum08
60 7 datum06
40 7 datum04
20 7 datum02
10 7 datum01
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..80";
$rst = "VALUE 12 5
10 7 datum01
20 7 datum02
40 7 datum04
60 7 datum06
80 7 datum08
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 80", 12, 1, "80";
$rst = "VALUE 12 1
80 7 datum08
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 70..0";
$rst = "VALUE 12 4
60 7 datum06
40 7 datum04
20 7 datum02
10 7 datum01
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..70";
$rst = "VALUE 12 4
10 7 datum01
20 7 datum02
40 7 datum04
60 7 datum06
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 70"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 0..5"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 5..0"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 5"; $rst = "NOT_FOUND_ELEMENT";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "setattr bkey2 overflowaction=smallest_trim"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "getattr bkey2 trimmed minbkey maxbkey";
$rst = "ATTR trimmed=0
ATTR minbkey=10
ATTR maxbkey=80
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 100 7"; $val = "datum10"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 120 7"; $val = "datum12"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey2 120..0 10";
$rst = "VALUE 12 5
120 7 datum12
100 7 datum10
80 7 datum08
60 7 datum06
40 7 datum04
TRIMMED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop get bkey2 120..40 10";
$rst = "VALUE 12 5
120 7 datum12
100 7 datum10
80 7 datum08
60 7 datum06
40 7 datum04
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
