#!/usr/bin/perl

use strict;
use Test::More tests => 15;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $expire;
my $cmd;
my $val;
my $rst;
my $msg;

sub wait_for_early_second {
    my $have_hires = eval "use Time::HiRes (); 1";
    if ($have_hires) {
        my $tsh = Time::HiRes::time();
        my $ts = int($tsh);
        return if ($tsh - $ts) < 0.5;
    }

    my $ts = int(time());
    while (1) {
        my $t = int(time());
        return if $t != $ts;
        select undef, undef, undef, 0.10;  # 1/10th of a second sleeps until time changes.
    }
}

wait_for_early_second();

$cmd = "set foo 0 1 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);
sleep(1.5);
$cmd = "get foo";
$rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

$expire = time() - 1;
$cmd = "set foo 0 $expire 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo"; $rst = "END"; $msg = "already expired";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$expire = time() + 1;
$cmd = "set foo 0 $expire 6"; $val = "foov+1"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
foov+1
END";
mem_cmd_is($sock, $cmd, "", $rst);
sleep(2.2);
$cmd = "get foo"; $rst = "END"; $msg = "now expired";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$expire = time() - 20;
$cmd = "set boo 0 $expire 6"; $val = "booval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get boo"; $rst = "END"; $msg = "now expired";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "add add 0 2 6"; $val = "addval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get add";
$rst = "VALUE add 0 6
addval
END";
mem_cmd_is($sock, $cmd, "", $rst);
# second add fails
$cmd = "add add 0 2 7"; $val = "addval2"; $rst = "NOT_STORED"; $msg = "add failure";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);
sleep(2.3);
$cmd = "add add 0 2 7"; $val = "addval3"; $rst = "STORED"; $msg = "stored add again";
mem_cmd_is($sock, $cmd, $val, $rst, $msg);
$cmd = "get add";
$rst = "VALUE add 0 7
addval3
END";
mem_cmd_is($sock, $cmd, "", $rst);

# after test
release_memcached($engine, $server);
