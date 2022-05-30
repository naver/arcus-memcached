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
my $prefix;
my $rst;
my $val = "val";
my $vlen = length($val);
my @prefixarr1 = ();
my @prefixarr2 = ();

# SET ITEMS
$prefix = "aaaaaab";
$cmd = "set $prefix:$prefix 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@prefixarr1, $prefix);
$prefix = "bbbaaab";
$cmd = "set $prefix:$prefix 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@prefixarr1, $prefix);
$prefix = "cccaabba";
$cmd = "set $prefix:$prefix 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@prefixarr1, $prefix);
push(@prefixarr2, $prefix);
$prefix = "ddddabba";
$cmd = "set $prefix:$prefix 0 0 $vlen"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
push(@prefixarr1, $prefix);
push(@prefixarr2, $prefix);
$prefix = "aaaaaaaab";
$cmd = "bop create $prefix:$prefix 0 0 3"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
push(@prefixarr1, $prefix);
$prefix = "aaaaaaab";
$cmd = "sop create $prefix:$prefix 0 0 3"; $rst = "CREATED";
mem_cmd_is($sock, $cmd, "", $rst);
push(@prefixarr1, $prefix);

# PREFIXSCAN
my @scanprefixes = prefixscan($sock, "0", 2000, "*");
Test::More::is(scalar(@scanprefixes), scalar(@prefixarr1));
my %prefixset = map { $_ => 1 } @prefixarr1;
foreach $_ ( @scanprefixes ) {
    Test::More::ok(exists($prefixset{$_}));
}

@scanprefixes = prefixscan($sock, "0", 2000, "*a");
Test::More::is(scalar(@scanprefixes), scalar(@prefixarr2));
%prefixset = map { $_ => 1 } @prefixarr2;
foreach $_ ( @scanprefixes ) {
    print "$_\n";
    Test::More::ok(exists($prefixset{$_}));
}

# FAIL CASES
$cmd = "scan prefix a"; $rst = "CLIENT_ERROR invalid cursor.";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan prefix 0 count 2001"; $rst = "CLIENT_ERROR bad count value";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan prefix 0 type V"; $rst = "CLIENT_ERROR bad command line format";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan prefix 0 match ***asdds\\***"; $rst = "CLIENT_ERROR bad pattern string";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "scan prefix 0 match \\"; $rst = "CLIENT_ERROR bad pattern string";
mem_cmd_is($sock, $cmd, "", $rst);

my $prefix = "a" x 65;
$cmd = "scan prefix 0 match $prefix"; $rst = "CLIENT_ERROR bad pattern string";
mem_cmd_is($sock, $cmd, "", $rst);

# It's difficult to calculate how many tests were run in prefixscan()
done_testing();

# after test
release_memcached($engine, $server);
