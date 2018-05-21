#!/usr/bin/perl

use strict;
use Test::More tests => 18;
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
my $expire;

### [ARCUS] CHANGED BELOW TESTS ######
# Arcus extended the result of stats detail dump command.
######################################

$cmd = "stats detail dump"; $rst = "END"; $msg = "verified empty stats at start";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "stats detail on"; $rst = "OK"; $msg = "detail collection turned on";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "set foo:123 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "stats detail dump";
$rst = "PREFIX foo get 0 hit 0 set 1 del 0 inc 0 dec 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gps 0 gph 0 gas 0 sas 0
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "get foo:123";
$rst = "VALUE foo:123 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "stats detail dump";
$rst = "PREFIX foo get 1 hit 1 set 1 del 0 inc 0 dec 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gps 0 gph 0 gas 0 sas 0
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "get foo:124"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "stats detail dump"; $msg = "details after get without hit";
$rst = "PREFIX foo get 2 hit 1 set 1 del 0 inc 0 dec 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gps 0 gph 0 gas 0 sas 0
END";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "delete foo:125"; $rst = "NOT_FOUND"; $msg = "sent delete command";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "stats detail dump"; $msg = "details after delete";
$rst = "PREFIX foo get 2 hit 1 set 1 del 1 inc 0 dec 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gps 0 gph 0 gas 0 sas 0
END";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "stats reset"; $rst = "RESET"; $msg = "stats cleared";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "stats detail dump"; $rst = "END"; $msg = "empty stats after clear";
mem_cmd_is($sock, $cmd, "", $rst, $msg);
$cmd = "get foo:123";
$rst = "VALUE foo:123 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "stats detail dump"; $msg = "details after clear and get";
$rst = "PREFIX foo get 1 hit 1 set 0 del 0 inc 0 dec 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gps 0 gph 0 gas 0 sas 0
END";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "stats detail off"; $rst = "OK"; $msg = "detail collection turned off";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

$cmd = "get foo:124"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

$cmd = "get foo:123";
$rst = "VALUE foo:123 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);
$cmd = "stats detail dump"; $msg = "details after stats turned off";
$rst = "PREFIX foo get 1 hit 1 set 0 del 0 inc 0 dec 0 lcs 0 lis 0 lih 0 lds 0 ldh 0 lgs 0 lgh 0 scs 0 sis 0 sih 0 sds 0 sdh 0 sgs 0 sgh 0 ses 0 seh 0 mcs 0 mis 0 mih 0 mus 0 muh 0 mds 0 mdh 0 mgs 0 mgh 0 bcs 0 bis 0 bih 0 bus 0 buh 0 bds 0 bdh 0 bps 0 bph 0 bms 0 bmh 0 bgs 0 bgh 0 bns 0 bnh 0 pfs 0 pfh 0 pgs 0 pgh 0 gps 0 gph 0 gas 0 sas 0
END";
mem_cmd_is($sock, $cmd, "", $rst, $msg);

# after test
release_memcached($engine, $server);
