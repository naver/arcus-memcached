#!/usr/bin/perl

use strict;
use Test::More tests => 24;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $server = new_memcached();
my $sock = $server->sock;
my $expire;

### [ARCUS] CHANGED BELOW TESTS ######
# Arcus extended the result of stats detail dump command.
######################################

print $sock "stats detail dump\r\n";
is(scalar <$sock>, "END\r\n", "verified empty stats at start");

print $sock "stats detail on\r\n";
is(scalar <$sock>, "OK\r\n", "detail collection turned on");

print $sock "set foo:123 0 0 6\r\nfooval\r\n";
is(scalar <$sock>, "STORED\r\n", "stored foo");

print $sock "stats detail dump\r\n";
#is(scalar <$sock>, "PREFIX foo get 0 hit 0 set 1 del 0\r\n", "details after set");
is(scalar <$sock>, "PREFIX foo get 0 hit 0 set 1 del 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gas 0 sas 0\r\n", "details after set");
is(scalar <$sock>, "END\r\n", "end of details");

mem_get_is($sock, "foo:123", "fooval");
print $sock "stats detail dump\r\n";
#is(scalar <$sock>, "PREFIX foo get 1 hit 1 set 1 del 0\r\n", "details after get with hit");
is(scalar <$sock>, "PREFIX foo get 1 hit 1 set 1 del 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gas 0 sas 0\r\n", "details after get with hit");
is(scalar <$sock>, "END\r\n", "end of details");

mem_get_is($sock, "foo:124", undef);

print $sock "stats detail dump\r\n";
#is(scalar <$sock>, "PREFIX foo get 2 hit 1 set 1 del 0\r\n", "details after get without hit");
is(scalar <$sock>, "PREFIX foo get 2 hit 1 set 1 del 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gas 0 sas 0\r\n", "details after get without hit");
is(scalar <$sock>, "END\r\n", "end of details");

print $sock "delete foo:125\r\n";
is(scalar <$sock>, "NOT_FOUND\r\n", "sent delete command");

print $sock "stats detail dump\r\n";
#is(scalar <$sock>, "PREFIX foo get 2 hit 1 set 1 del 1\r\n", "details after delete");
is(scalar <$sock>, "PREFIX foo get 2 hit 1 set 1 del 1 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gas 0 sas 0\r\n", "details after delete");
is(scalar <$sock>, "END\r\n", "end of details");

print $sock "stats reset\r\n";
is(scalar <$sock>, "RESET\r\n", "stats cleared");

print $sock "stats detail dump\r\n";
is(scalar <$sock>, "END\r\n", "empty stats after clear");

mem_get_is($sock, "foo:123", "fooval");
print $sock "stats detail dump\r\n";
#is(scalar <$sock>, "PREFIX foo get 1 hit 1 set 0 del 0\r\n", "details after clear and get");
is(scalar <$sock>, "PREFIX foo get 1 hit 1 set 0 del 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gas 0 sas 0\r\n", "details after clear and get");
is(scalar <$sock>, "END\r\n", "end of details");

print $sock "stats detail off\r\n";
is(scalar <$sock>, "OK\r\n", "detail collection turned off");

mem_get_is($sock, "foo:124", undef);

mem_get_is($sock, "foo:123", "fooval");
print $sock "stats detail dump\r\n";
#is(scalar <$sock>, "PREFIX foo get 1 hit 1 set 0 del 0\r\n", "details after stats turned off");
is(scalar <$sock>, "PREFIX foo get 1 hit 1 set 0 del 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gas 0 sas 0\r\n", "details after stats turned off");
is(scalar <$sock>, "END\r\n", "end of details");
