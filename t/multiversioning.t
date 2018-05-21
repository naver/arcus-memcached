#!/usr/bin/perl

use strict;
use Test::More tests => 13;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock  = $server->sock;
my $sock2 = $server->new_sock;
my $cmd;
my $val;
my $rst;
my $msg;

ok($sock != $sock2, "have two different connections open");

# set large value
my $size   = 256 * 1024;  # 256 kB
my $bigval = "0123456789abcdef" x ($size / 16);
$bigval =~ s/^0/\[/; $bigval =~ s/f$/\]/;
my $bigval2 = uc($bigval);

$cmd = "set big 0 0 $size"; $val = "$bigval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get big"; $msg = "big value got correctly";
$rst = "VALUE big 0 $size
$bigval
END";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

print $sock "get big\r\n";
my $buf;
is(read($sock, $buf, $size / 2), $size / 2, "read half the answer back");
like($buf, qr/VALUE big/, "buf has big value header in it");
like($buf, qr/abcdef/, "buf has some data in it");
unlike($buf, qr/abcde\]/, "buf doesn't yet close");

# sock2 interrupts (maybe sock1 is slow) and deletes stuff:
$cmd = "delete big"; $rst = "DELETED"; $msg = "deleted big from sock2 while sock1's still reading it";
mem_cmd_is($sock2, $cmd, "", $rst, $msg);
$cmd = "get big"; $rst = "END"; $msg = "nothing from sock2 now.  gone from namespace.";
mem_cmd_is($sock2, $cmd, "", $rst, $msg);
$cmd = "set big 0 0 $size"; $val = "$bigval2"; $rst = "STORED";
$msg = "stored big w/ val2";
mem_cmd_is($sock2, $cmd, $val, $rst, $msg);
$cmd = "get big"; $msg = "big value2 got correctly";
$rst = "VALUE big 0 $size
$bigval2
END";
mem_cmd_is($sock2, $cmd, "", $rst, $msg);

# sock1 resumes reading...
$buf .= <$sock>;
$buf .= <$sock>;
like($buf, qr/abcde\]/, "buf now closes");

# and if sock1 reads again, it's the uppercase version:
$cmd = "get big"; $msg = "big value2 got correctly from sock1";
$rst = "VALUE big 0 $size
$bigval2
END";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

# after test
release_memcached($engine, $server);
