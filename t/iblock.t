#!/usr/bin/perl

use strict;
use Test::More tests => 89;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $msg;

my $block_size = 32768; # data size refer IVALUE_PER_DTA
my $header_size = 4;    # refer IVALUE_PER_HDR
$block_size -= $header_size;
my $half = $block_size/2;
my $size1;
my $size2;
my $retsize;
my $retval;
my $stats;

$size1 = $half;
$cmd = "set key1 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set key2 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# append (key1 + "2"x$size2)
$size2 = ($block_size * 4) - 2; # full block
$cmd = "append key1 0 0 $size2"; $val = "2"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, "[append] fullblock + half block");
$retsize = $size1 + $size2;
$cmd = "get key1"; $retval = "1"x$half . $val;
$rst = "VALUE key1 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, "[ap+get] fullblock + half block");

# prepend ("2"x$size2 + key2)
$cmd = "prepend key2 0 0 $size2"; $val = "2"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, "[prepend] half block + fullblock");
$retsize = $size1 + $size2;
$cmd = "get key2"; $retval = $val . "1"x$half;
$rst = "VALUE key2 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, "[pr+get] half block + fullblock");

# delete key & check slab
$cmd = "delete key1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete key2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_max_classid"}, -1, "confirm slab initialization");


# store block full block + "\r\n" block
$size1 = $block_size;
$cmd = "set key1 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# append (key1 + "\r\n")
$cmd = "append key1 0 0 0\r\n"; $rst = "STORED";
mem_cmd_is($sock, $cmd, "", $rst, "[append] \\r\\n");
$cmd = "get key1";
$rst = "VALUE key1 0 $size1
$val
END";
mem_cmd_is($sock, $cmd, "", $rst, "[ap+get] \\r\\n");

# append (combine frt last block + bck first block)
$size2 = $half;
$cmd = "append key1 1 0 $size2"; $val = "A"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, "[append] combine frt + bck block");
$retsize = $size1 + $size2;
$cmd = "get key1"; $retval = "1"x$size1 . $val;
$rst = "VALUE key1 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, "[ap+get] combine frt + bck block");

# store block full block + "\r\n" block
$size1 = $block_size;
$cmd = "set key2 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# prepend ("\r\n" + key2)
$cmd = "prepend key2 0 0 0\r\n"; $rst = "STORED";
mem_cmd_is($sock, $cmd, "", $rst, "[prepend] \\r\\n");
$cmd = "get key2";
$rst = "VALUE key2 0 $size1
$val
END";
mem_cmd_is($sock, $cmd, "", $rst, "[pre+get] \\r\\n");

# prepend (not combine block)
$size2 = $half;
$cmd = "prepend key2 0 0 $size2"; $val = "2"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, '[prepend] combine bck block[0], [1]');
$retsize = $size1 + $size2;
$cmd = "get key2"; $retval = $val . "1"x$size1;
$rst = "VALUE key2 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, '[pre+get] combine bck block[0], [1]');

# delete key & check slab
$cmd = "delete key1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete key2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_max_classid"}, -1, "confirm slab initialization");

################
## split case ##
################

$size1 = $block_size - 1;
$cmd = "set split1 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set split2 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# append split case 1
$size2 = $block_size + 1;
$cmd = "append split1 0 0 $size2"; $val = "2"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, '[append] combine \n split case 1');
$retsize = $size1 + $size2;
$cmd = "get split1"; $retval = "1"x$size1 . $val;
$rst = "VALUE split1 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, '[ap+get] combine \n split case 1');

# prepend split case 1
$size2 = $block_size*2 + $block_size - 1;
$cmd = "prepend split1 0 0 $size2"; $val = "x"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, '[prepend] combine \n split case 1');
$retsize = $retsize + $size2;
$cmd = "get split1"; $retval = $val . $retval;
$rst = "VALUE split1 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, '[pre+get] combine \n split case 1');

# append split case 2
$size2 = $block_size*5;
$cmd = "append split2 0 0 $size2"; $val = "2"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, '[append] combine \n split case 2');
$retsize = $size1 + $size2;
$cmd = "get split2"; $retval = "1"x$size1 . $val;
$rst = "VALUE split2 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, '[ap+get] combine \n split case 2');

# prepend split case 2
$size2 = $block_size - 1;
$cmd = "prepend split2 0 0 $size2"; $val = "j"x$size2; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, '[prepend] combine \n split case 2');
$retsize = $retsize + $size2;
$cmd = "get split2"; $retval = $val . $retval;
$rst = "VALUE split2 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, '[pre+get] combine \n split case 2');

$size1 = $block_size - 1;
$cmd = "set split3 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "set split4 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

# append split case 3
$cmd = "append split3 0 0 $size1"; $val = "2"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, '[append] combine \n split case 3');
$retsize = $size1*2;
$cmd = "get split3"; $retval = "1"x$size1 . $val;
$rst = "VALUE split3 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, '[ap+get] combine \n split case 3');

# prepend split case 4
$cmd = "prepend split4 0 0 $size1"; $val = "j"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, '[prepend] combine \n split case 3');
$retsize = $size1*2;
$cmd = "get split4"; $retval = $val . "1"x$size1;
$rst = "VALUE split4 0 $retsize
$retval
END";
mem_cmd_is($sock, $cmd, "", $rst, '[pre+get] combine \n split case 3');

# delete key & check slab
$cmd = "delete split1"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete split2"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete split3"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete split4"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_max_classid"}, -1, "confirm slab initialization");

###############
## loop test ##
###############

my $cnt;

$size1 = $block_size - 1;
$cmd = "set aploop 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$retsize = $size1;
$retval = "1"x$size1;
for ($cnt = 1; $cnt <= 10; $cnt++) {
    $size2 = 1024*$cnt;
    $cmd = "append aploop 0 0 $size2"; $val = "2"x($size2 - 1) . "s"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst, "[append] loop test - $cnt");
    $retsize += $size2;
    $cmd = "get aploop"; $retval .= $val;
    $rst = "VALUE aploop 0 $retsize\n"
         . "$retval\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst, "[ap+get] loop test - $cnt");
}

$size1 = $block_size - 1;
$cmd = "set preloop 0 0 $size1"; $val = "1"x$size1; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$retsize = $size1;
$retval = "1"x$size1;
for ($cnt = 1; $cnt <= 10; $cnt++) {
    $size2 = 1024*$cnt;
    $cmd = "prepend preloop 0 0 $size2"; $val = "x"x($size2 - 1) . "s"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst, "[prepend] loop test - $cnt");
    $retsize += $size2;
    $cmd = "get preloop"; $retval = $val . $retval;
    $rst = "VALUE preloop 0 $retsize\n"
         . "$retval\n"
         . "END";
    mem_cmd_is($sock, $cmd, "", $rst, "[pre+get] loop test - $cnt");
}

# delete key & check slab
$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_max_classid"} != -1, 1, "confirm used slab");

$cmd = "delete aploop"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "delete preloop"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

$stats = mem_stats($sock, "slabs");
is ($stats->{"SM:used_max_classid"}, -1, "confirm slab initialization");

# after test
release_memcached($engine, $server);
