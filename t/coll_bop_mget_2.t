#!/usr/bin/perl

use strict;
use Test::More tests => 49;
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
bkey1,bkey2
bop mget 23 4 0x0000..0x0100 2 6
bkey2,bkey3,bkey1,bkey4
bop mget 23 4 0x0090..0x0030 2 9
bkey2,bkey3,bkey1,bkey4
bop mget 23 4 0x0200..0x0300 2 6
bkey2,bkey3,bkey1,bkey4
bop mget 29 5 0x0000..0x0100 1 6
bkey2,bkey3,bkey1,bkey4,bkey3

bop mget 28 5 0x0000..0x0100 2 6
bkey2,bkey3,bkey1,bkey4,kvkey
bop mget 29 5 0x0000..0x0100 2 6
bkey2,bkey3,bkey1,bkey4,bkey1
bop mget 23 2 0x0000..0x0100 2 6
bkey2,bkey3,bkey1,bkey4

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
bkey1,bkey2
bop mget 23 4 0x00..0xFFFF 2 6
bkey2,bkey3,bkey1,bkey4
bop mget 23 4 0x0090..0x0000000000000030 2 9
bkey2,bkey3,bkey1,bkey4
bop mget 23 4 0x0200..0x0300 2 6
bkey2,bkey3,bkey1,bkey4
bop mget 29 5 0x0000..0x0100 1 6
bkey2,bkey3,bkey1,bkey4,bkey3

bop mget 28 5 0x0000..0x0100 2 6
bkey2,bkey3,bkey1,bkey4,kvkey
bop mget 29 5 0x0000..0x0100 2 6
bkey2,bkey3,bkey1,bkey4,bkey1
bop mget 23 2 0x0000..0x0100 2 6
bkey2,bkey3,bkey1,bkey4

delete bkey1
delete bkey2
delete kvkey
=cut

my $server = new_memcached();
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# Initialize
$cmd = "get bkey1"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get bkey2"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "get kvkey"; $rst = "END";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "set kvkey 0 0 6"; $val = "datumx"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
# Prepare Keys
$cmd = "bop insert bkey1 0x0090 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0070 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0050 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0030 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0010 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0x1000",
               11, 5, "0x0010,0x0030,0x0050,0x0070,0x0090", ",,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey2 0x0100 7 create 12 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0080 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0060 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0040 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0020 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x0000..0xFFFFFFFF",
               12, 5, "0x0020,0x0040,0x0060,0x0080,0x0100", ",,,,",
               "datum2,datum4,datum6,datum8,datum10", "END");

# mgets
$cmd = "bop mget 11 2 0x0000..0x0100 5"; $val = "bkey1,bkey2";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0090..0x0030 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE bkey2 OK 12 1
ELEMENT 0x0040 6 datum4
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 2
ELEMENT 0x0050 6 datum5
ELEMENT 0x0030 6 datum3
VALUE bkey4 NOT_FOUND
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0200..0x0300 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE bkey2 NOT_FOUND_ELEMENT
VALUE bkey3 NOT_FOUND
VALUE bkey1 NOT_FOUND_ELEMENT
VALUE bkey4 NOT_FOUND
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0x0000..0x0100 1 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey3";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

# fails
$cmd = "bop mget 29 5 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,krkey";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 29 5 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4,bkey1";
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 2 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");

# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

# initialzie
$cmd = "bop insert bkey1 0x0090 6 create 11 0 0"; $val = "datum9"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x00000070 6"; $val = "datum7"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x000000000050 6"; $val = "datum5"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x0000000000000030 6"; $val = "datum3"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey1 0x00000000000000000010 6"; $val = "datum1"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey1 0x00..0xFF",
               11, 5, "0x00000000000000000010,0x0000000000000030,0x000000000050,0x00000070,0x0090", ",,,,",
               "datum1,datum3,datum5,datum7,datum9", "END");
$cmd = "bop insert bkey2 0x01 7 create 12 0 0"; $val = "datum10"; $rst = "CREATED_STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x000080 6"; $val = "datum8"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x0000000060 6"; $val = "datum6"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x00000000000040 6"; $val = "datum4"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
$cmd = "bop insert bkey2 0x000000000000000020 6"; $val = "datum2"; $rst = "STORED";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
bop_ext_get_is($sock, "bkey2 0x00..0xFFFFFFFFFFFFFFFFFF",
               12, 5, "0x000000000000000020,0x00000000000040,0x0000000060,0x000080,0x01", ",,,,",
               "datum2,datum4,datum6,datum8,datum10", "END");
# mgets
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
mem_cmd_val_is($sock, $cmd, $val, $rst);

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
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0090..0x0000000000000030 2 9"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE bkey2 OK 12 1
ELEMENT 0x00000000000040 6 datum4
VALUE bkey3 NOT_FOUND
VALUE bkey1 OK 11 2
ELEMENT 0x000000000050 6 datum5
ELEMENT 0x0000000000000030 6 datum3
VALUE bkey4 NOT_FOUND
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 4 0x0200..0x0300 2 6"; $val = "bkey2,bkey3,bkey1,bkey4";
$rst = "VALUE bkey2 NOT_FOUND_ELEMENT
VALUE bkey3 NOT_FOUND
VALUE bkey1 NOT_FOUND_ELEMENT
VALUE bkey4 NOT_FOUND
END";
mem_cmd_val_is($sock, $cmd, $val, $rst);

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
mem_cmd_val_is($sock, $cmd, $val, $rst);

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
mem_cmd_val_is($sock, $cmd, $val, $rst);

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
mem_cmd_val_is($sock, $cmd, $val, $rst);

$cmd = "bop mget 23 2 0x0000..0x0100 2 6"; $val = "bkey2,bkey3,bkey1,bkey4"; $rst = "CLIENT_ERROR bad data chunk";
print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd $val: $rst");
# finalize
$cmd = "delete bkey1"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete bkey2"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");
$cmd = "delete kvkey"; $rst = "DELETED";
print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "$cmd: $rst");

