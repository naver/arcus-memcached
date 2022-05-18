#!/usr/bin/perl

use strict;
use Test::More; # see done_testing()
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $key;
my $rst;
my $val = "val";
my $vlen = length($val);
my @keyarr1 = ();
my @keyarr2 = ();

# SET ITEMS
$key = "aa\\*aaa";
$cmd = "set $key 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@keyarr1, $key);
$key = "bb\\*aab";
$cmd = "set $key 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@keyarr1, $key);
$key = "c?\\*aaba";
$cmd = "set $key 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@keyarr1, $key);
push(@keyarr2, $key);
$key = "d?\\*abba";
$cmd = "set $key 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@keyarr1, $key);
push(@keyarr2, $key);
$key = "a?\\*aaaaa";
$cmd = "bop create $key 0 0 3"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
push(@keyarr1, $key);
$key = "a?\\*aaaa";
$cmd = "sop create $key 0 0 3"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
push(@keyarr1, $key);

# KEYSCAN
my @scankeys = keyscan($sock, "0", 2000, "*", "A");
Test::More::is(scalar(@scankeys), scalar(@keyarr1));
my %keyset = map { $_ => 1 } @keyarr1;
foreach $_ ( @scankeys ) {
    Test::More::ok(exists($keyset{$_}));
}

@scankeys = keyscan($sock, "0", 2000, "?\\?\\\\\\**a", "K");
Test::More::is(scalar(@scankeys), scalar(@keyarr2));
%keyset = map { $_ => 1 } @keyarr2;
foreach $_ ( @scankeys ) {
    print "$_\n";
    Test::More::ok(exists($keyset{$_}));
}

# FAIL CASES
$cmd = "scan key a"; $rst = "CLIENT_ERROR invalid cursor.";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan key 0 count 2001"; $rst = "CLIENT_ERROR bad count value";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan key 0 type V"; $rst = "CLIENT_ERROR bad item type";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan key 0 match ***asdds\\***"; $rst = "CLIENT_ERROR bad pattern string";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan key 0 match \\"; $rst = "CLIENT_ERROR bad pattern string";
mem_cmd_is($sock, $cmd, "", $rst);

my $key = "a" x 65;
$cmd = "scan key 0 match $key"; $rst = "CLIENT_ERROR too long pattern string";
mem_cmd_is($sock, $cmd, "", $rst);

# It's difficult to calculate how many tests were run in keyscan()
done_testing();

# after test
release_memcached($engine, $server);
