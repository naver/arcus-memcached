#!/usr/bin/perl

use strict;
use Test::More tests => 67;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

=head
get bkey1
get bkey2
get kvkey
set kvkey 0 0 6
datumx

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
bop get bkey1 0x00..0x1000
bop get bkey2 0x0000..0xFFFFFFFF

bop mget 11 2 0x0000..0x0100 5
bkey1 bkey2
bop mget 23 4 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4
bop mget 23 4 0x0090..0x0030 2 9
bkey2 bkey3 bkey1 bkey4
bop mget 23 4 0x0200..0x0300 2 6
bkey2 bkey3 bkey1 bkey4
bop mget 29 5 0x0000..0x0100 1 6
bkey2 bkey3 bkey1 bkey4 bkey3

bop mget 28 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 kvkey
bop mget 29 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 bkey1
bop mget 23 2 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4

delete bkey1
delete bkey2

bop insert bkey1 0x0090 6 create 11 0 0
datum9
bop insert bkey1 0x00000070 6
datum7
bop insert bkey1 0x000000000050 6
datum5
bop insert bkey1 0x0000000000000030 6
datum3
bop insert bkey1 0x00000000000000000010 6
datum1
bop insert bkey2 0x01 7 create 12 0 0
datum10
bop insert bkey2 0x000080 6
datum8
bop insert bkey2 0x0000000060 6
datum6
bop insert bkey2 0x00000000000040 6
datum4
bop insert bkey2 0x000000000000000020 6
datum2
bop get bkey1 0x00..0xFF
bop get bkey2 0x00..0xFFFFFFFFFFFFFFFFFFFF

bop mget 11 2 0x00..0xFF 5
bkey1 bkey2
bop mget 23 4 0x00..0xFFFF 2 6
bkey2 bkey3 bkey1 bkey4
bop mget 23 4 0x0090..0x0000000000000030 2 9
bkey2 bkey3 bkey1 bkey4
bop mget 23 4 0x0200..0x0300 2 6
bkey2 bkey3 bkey1 bkey4
bop mget 29 5 0x0000..0x0100 1 6
bkey2 bkey3 bkey1 bkey4 bkey3

bop mget 28 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 kvkey
bop mget 29 5 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4 bkey1
bop mget 23 2 0x0000..0x0100 2 6
bkey2 bkey3 bkey1 bkey4

delete bkey1
delete bkey2
delete kvkey
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
$cmd = "get kvkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

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
$cmd = "bop get bkey1 0x00..0x1000";
$rst = "VALUE 11 5
0x0010 6 datum1
0x0030 6 datum3
0x0050 6 datum5
0x0070 6 datum7
0x0090 6 datum9
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
$cmd = "bop get bkey2 0x0000..0xFFFFFFFF";
$rst = "VALUE 12 5
0x0020 6 datum2
0x0040 6 datum4
0x0060 6 datum6
0x0080 6 datum8
0x0100 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# mgets
$cmd = "bop mget 11 2 0x0000..0x0100 5"; $val = "bkey1 bkey2";
$rst = "VALUE bkey1 OK 11 5
ELEMENT 0x0010 6 datum1
ELEMENT 0x0030 6 datum3
ELEMENT 0x0050 6 datum5
ELEMENT 0x0070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey2 OK 12 5
ELEMENT 0x0020 6 datum2
ELEMENT 0x0040 6 datum4
ELEMENT 0x0060 6 datum6
ELEMENT 0x0080 6 datum8
ELEMENT 0x0100 7 datum10
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "VALUE bkey2 OK 12 3
ELEMENT 0x0060 6 datum6
ELEMENT 0x0080 6 datum8
ELEMENT 0x0100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x0050 6 datum5
ELEMENT 0x0070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0090..0x0030 2 9"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "VALUE bkey2 OK 12 1
ELEMENT 0x0040 6 datum4
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 2
ELEMENT 0x0050 6 datum5
ELEMENT 0x0030 6 datum3
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0200..0x0300 2 6"; $val = "bkey2 bkey3 bkey1 bkey4";
$rst = "VALUE bkey2 NOT_FOUND_ELEMENT
VALUE bkey3 NOT_FOUND
VALUE bkey1 NOT_FOUND_ELEMENT
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0x0000..0x0100 1 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey3";
$rst = "VALUE bkey2 OK 12 4
ELEMENT 0x0040 6 datum4
ELEMENT 0x0060 6 datum6
ELEMENT 0x0080 6 datum8
ELEMENT 0x0100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 4
ELEMENT 0x0030 6 datum3
ELEMENT 0x0050 6 datum5
ELEMENT 0x0070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
VALUE bkey3 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# fails
$cmd = "bop mget 29 5 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 krkey";
$rst = "VALUE bkey2 OK 12 3
ELEMENT 0x0060 6 datum6
ELEMENT 0x0080 6 datum8
ELEMENT 0x0100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x0050 6 datum5
ELEMENT 0x0070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
VALUE krkey NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4 bkey1";
$rst = "VALUE bkey2 OK 12 3
ELEMENT 0x0060 6 datum6
ELEMENT 0x0080 6 datum8
ELEMENT 0x0100 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x0050 6 datum5
ELEMENT 0x0070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x0050 6 datum5
ELEMENT 0x0070 6 datum7
ELEMENT 0x0090 6 datum9
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 2 0x0000..0x0100 2 6"; $val = "bkey2 bkey3 bkey1 bkey4"; $rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);

# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# initialzie
$cmd = "bop insert bkey1 0x0090 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x00000070 6"; $val = "datum7"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x000000000050 6"; $val = "datum5"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x0000000000000030 6"; $val = "datum3"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey1 0x00000000000000000010 6"; $val = "datum1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey1 0x00..0xFF";
$rst = "VALUE 11 5
0x00000000000000000010 6 datum1
0x0000000000000030 6 datum3
0x000000000050 6 datum5
0x00000070 6 datum7
0x0090 6 datum9
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "bop insert bkey2 0x01 7 create 12 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x000080 6"; $val = "datum8"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x0000000060 6"; $val = "datum6"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x00000000000040 6"; $val = "datum4"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop insert bkey2 0x000000000000000020 6"; $val = "datum2"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop get bkey2 0x00..0xFFFFFFFFFFFFFFFFFF";
$rst = "VALUE 12 5
0x000000000000000020 6 datum2
0x00000000000040 6 datum4
0x0000000060 6 datum6
0x000080 6 datum8
0x01 7 datum10
END";
mem_cmd_is($sock, $cmd, "", $rst);

# mgets : Use comma separated keys for backward compatibility check
$cmd = "bop mget 11 2 0x00..0xFF 5"; $val = "bkey1,bkey2";
$rst = "VALUE bkey1 OK 11 5
ELEMENT 0x00000000000000000010 6 datum1
ELEMENT 0x0000000000000030 6 datum3
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x00000070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey2 OK 12 5
ELEMENT 0x000000000000000020 6 datum2
ELEMENT 0x00000000000040 6 datum4
ELEMENT 0x0000000060 6 datum6
ELEMENT 0x000080 6 datum8
ELEMENT 0x01 7 datum10
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x00..0xFFFF 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE bkey2 OK 12 3
ELEMENT 0x0000000060 6 datum6
ELEMENT 0x000080 6 datum8
ELEMENT 0x01 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x00000070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0090..0x0000000000000030 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE bkey2 OK 12 1
ELEMENT 0x00000000000040 6 datum4
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 2
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x0000000000000030 6 datum3
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0200..0x0300 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE bkey2 NOT_FOUND_ELEMENT
VALUE bkey3 NOT_FOUND
VALUE bkey1 NOT_FOUND_ELEMENT
VALUE bkey4 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0x0000..0x0100 1 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey3";
$rst = "VALUE bkey2 OK 12 4
ELEMENT 0x00000000000040 6 datum4
ELEMENT 0x0000000060 6 datum6
ELEMENT 0x000080 6 datum8
ELEMENT 0x01 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 4
ELEMENT 0x0000000000000030 6 datum3
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x00000070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
VALUE bkey3 NOT_FOUND
END";
mem_cmd_is($sock, $cmd, $val, $rst);

# fails
$cmd = "bop mget 29 5 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,kvkey";
$rst = "VALUE bkey2 OK 12 3
ELEMENT 0x0000000060 6 datum6
ELEMENT 0x000080 6 datum8
ELEMENT 0x01 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x00000070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
VALUE kvkey TYPE_MISMATCH
END";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey1";
$rst = "VALUE bkey2 OK 12 3
ELEMENT 0x0000000060 6 datum6
ELEMENT 0x000080 6 datum8
ELEMENT 0x01 7 datum10
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x00000070 6 datum7
ELEMENT 0x0090 6 datum9
VALUE bkey4 NOT_FOUND
VALUE bkey1 OK 11 3
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x00000070 6 datum7
ELEMENT 0x0090 6 datum9
END";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "bop mget 23 2 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4"; $rst = "CLIENT_ERROR bad data chunk";
mem_cmd_is($sock, $cmd, $val, $rst);

# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete bkey2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete kvkey"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

# long key test
my $key1;
my $key2;

$cmd = "bop mget 600291 2 0..1000 2";
$key1 = "a"x300145;
$key2 = "b"x300145;
$val = "$key1 $key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad value");
$val = "$key1,$key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad value");
$val = "$key1?$key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad value");

$cmd = "bop mget 66001 2 0..1000 2";
$key1 = "a"x33000;
$key2 = "b"x33000;
$val = "$key1 $key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad value");
$val = "$key1,$key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad value");
$val = "$key1?$key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad value");

$cmd = "bop mget 60001 3 0..1000 2";
$key1 = "a"x30000;
$key2 = "b"x30000;
$val = "$key1 $key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad data chunk");
$val = "$key1,$key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad data chunk");
$val = "$key1?$key2";
mem_cmd_is($sock, $cmd, $val, "CLIENT_ERROR bad data chunk");

$cmd = "bop mget 60001 2 0..1000 2";
$key1 = "a"x30000;
$key2 = "b"x30000;
$rst =
"VALUE $key1 NOT_FOUND
VALUE $key2 NOT_FOUND
END";
$val = "$key1 $key2";
mem_cmd_is($sock, $cmd, $val, $rst);
$val = "$key1,$key2";
mem_cmd_is($sock, $cmd, $val, $rst);
$val = "$key1?$key2";
mem_cmd_is($sock, $cmd, $val,  "CLIENT_ERROR bad data chunk");

# case) the last char is delimiter in mblock
$cmd = "bop mget 49139 3 0..1000 2";
$key1 = "a"x16379;
$rst =
"VALUE $key1 NOT_FOUND
VALUE $key1 NOT_FOUND
VALUE $key1 NOT_FOUND
END";
$val = "$key1 $key1 $key1";
mem_cmd_is($sock, $cmd, $val, $rst);
$val = "$key1,$key1,$key1";
mem_cmd_is($sock, $cmd, $val, $rst);
$val = "$key1?$key1?$key1";
mem_cmd_is($sock, $cmd, $val,  "CLIENT_ERROR bad data chunk");

# case) the first char is delimiter in mblock
$cmd = "bop mget 49140 3 0..1000 2";
$key1 = "a"x16379;
$key2 = "b"x16380;
$rst =
"VALUE $key1 NOT_FOUND
VALUE $key2 NOT_FOUND
VALUE $key1 NOT_FOUND
END";
$val = "$key1 $key2 $key1";
mem_cmd_is($sock, $cmd, $val, $rst);
$val = "$key1,$key2,$key1";
mem_cmd_is($sock, $cmd, $val, $rst);
$val = "$key1?$key2?$key1";
mem_cmd_is($sock, $cmd, $val,  "CLIENT_ERROR bad data chunk");

# after test
release_memcached($engine, $server);
