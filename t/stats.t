#!/usr/bin/perl

use strict;
use Test::More tests => 80;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;

## Output looks like this:
##
## STAT pid 22969
## STAT uptime 13
## STAT time 1259170891
## STAT version 1.4.3
## STAT libevent 1.4.13-stable.
## STAT pointer_size 32
## STAT rusage_user 0.001198
## STAT rusage_system 0.003523
## STAT curr_connections 10
## STAT total_connections 11
## STAT connection_structures 11
## STAT cmd_get 0
## STAT cmd_set 0
## STAT cmd_flush 0
## STAT get_hits 0
## STAT get_misses 0
## STAT delete_misses 0
## STAT delete_hits 0
## STAT incr_misses 0
## STAT incr_hits 0
## STAT decr_misses 0
## STAT decr_hits 0
## STAT cas_misses 0
## STAT cas_hits 0
## STAT cas_badval 0
## STAT auth_cmds 0
## STAT auth_errors 0
## STAT auth_unknowns 0
## STAT bytes_read 7
## STAT bytes_written 0
## STAT limit_maxbytes 67108864
## STAT threads 4
## STAT conn_yields 0
## STAT bytes 0
## STAT curr_items 0
## STAT total_items 0
## STAT evictions 0
## STAT reclaimed 0

# note that auth stats are tested in auth specfic tests

### [ARCUS] NEW STATS RESULT #########
# New stats result has 97 items like below.
## STAT pid 13006
## STAT uptime 10
## STAT time 1299724760
## STAT version UNKNOWN
## STAT libevent 1.4.13-stable
## STAT pointer_size 64
## STAT rusage_user 0.001999
## STAT rusage_system 0.002999
## STAT daemon_connections 5
## STAT curr_connections 6
## STAT total_connections 6
## STAT connection_structures 6
## STAT cmd_get 0
## STAT cmd_set 0
## STAT cmd_flush 0
## STAT cmd_flush_prefix 0
## STAT cmd_lop_create 0
## STAT cmd_lop_insert 0
## STAT cmd_lop_delete 0
## STAT cmd_lop_get 0
## STAT cmd_sop_create 0
## STAT cmd_sop_insert 0
## STAT cmd_sop_delete 0
## STAT cmd_sop_get 0
## STAT cmd_sop_exist 0
## STAT cmd_bop_create 0
## STAT cmd_bop_insert 0
## STAT cmd_bop_update 0
## STAT cmd_bop_delete 0
## STAT cmd_bop_get 0
## STAT cmd_bop_count 0
## STAT cmd_bop_mget 0
## STAT cmd_bop_smget 0
## STAT cmd_bop_incr 0
## STAT cmd_bop_decr 0
## STAT cmd_getattr 0
## STAT cmd_setattr 0
## STAT auth_cmds 0
## STAT auth_errors 0
## STAT get_hits 0
## STAT get_misses 0
## STAT delete_misses 0
## STAT delete_hits 0
## STAT incr_misses 0
## STAT incr_hits 0
## STAT decr_misses 0
## STAT decr_hits 0
## STAT cas_misses 0
## STAT cas_hits 0
## STAT cas_badval 0
## STAT lop_create_oks 0
## STAT lop_insert_misses 0
## STAT lop_insert_hits 0
## STAT lop_delete_misses 0
## STAT lop_delete_elem_hits 0
## STAT lop_delete_none_hits 0
## STAT lop_get_misses 0
## STAT lop_get_elem_hits 0
## STAT lop_get_none_hits 0
## STAT sop_create_oks 0
## STAT sop_insert_misses 0
## STAT sop_insert_hits 0
## STAT sop_delete_misses 0
## STAT sop_delete_elem_hits 0
## STAT sop_delete_none_hits 0
## STAT sop_get_misses 0
## STAT sop_get_elem_hits 0
## STAT sop_get_none_hits 0
## STAT sop_exist_misses 0
## STAT sop_exist_hits 0
## STAT bop_create_oks 0
## STAT bop_insert_misses 0
## STAT bop_insert_hits 0
## STAT bop_update_misses 0
## STAT bop_update_elem_hits 0
## STAT bop_update_none_hits 0
## STAT bop_delete_misses 0
## STAT bop_delete_elem_hits 0
## STAT bop_delete_none_hits 0
## STAT bop_get_misses 0
## STAT bop_get_elem_hits 0
## STAT bop_get_none_hits 0
## STAT bop_count_misses 0
## STAT bop_count_hits 0
## STAT bop_mget_oks 0
## STAT bop_smget_oks 0
## STAT bop_incr_elem_hits 0
## STAT bop_incr_none_hits 0
## STAT bop_incr_misses 0
## STAT bop_decr_elem_hits 0
## STAT bop_decr_none_hits 0
## STAT bop_decr_misses 0
## STAT getattr_misses 0
## STAT getattr_hits 0
## STAT setattr_misses 0
## STAT setattr_hits 0
## STAT bytes_read 7
## STAT bytes_written 0
## STAT limit_maxbytes 67108864
## STAT rejected_conns 0
## STAT threads 4
## STAT conn_yields 0
## STAT evictions 0
## STAT sticky_items 0
## STAT curr_items 0
## STAT total_items 0
## STAT sticky_bytes 0
## STAT bytes 0
## STAT reclaimed 0
## STAT sticky_limit 0
## STAT engine_maxbytes 67108864
######################################

my $stats = mem_stats($sock);

my $sasl_enabled = 0;
# Test number of keys
if ($stats->{'auth_sasl_enabled'} == 'yes') {
    $sasl_enabled = 1;
}

### [ARCUS] CHANGED FOLLOWING TEST ###
# Arcus extended stats values.
#is(scalar(keys(%$stats)), 40, "40 stats values");
#is(scalar(keys(%$stats)), 103, "103 stats values");
#is(scalar(keys(%$stats)), 111, "111 stats values");
#is(scalar(keys(%$stats)), 112, "112 stats values"); # add zk_timeout
#is(scalar(keys(%$stats)), 120, "120 stats values"); # add bop_position/bop_gbp
######################################

# Test initial state
foreach my $key (qw(curr_items total_items bytes cmd_get cmd_set get_hits evictions get_misses
                 bytes_written delete_hits delete_misses incr_hits incr_misses decr_hits decr_misses)) {
    is($stats->{$key}, 0, "initial $key is zero");
}

# Do some operations

$cmd = "set foo 0 0 6"; $val = "fooval"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
$cmd = "get foo";
$rst = "VALUE foo 0 6
fooval
END";
mem_cmd_is($sock, $cmd, "", $rst);

my $stats = mem_stats($sock);

is($stats->{'engine_maxbytes'}, 64<<20, "engine_maxbytes is " .. 64<<20);

foreach my $key (qw(total_items curr_items cmd_get cmd_set get_hits)) {
    is($stats->{$key}, 1, "after one set/one get $key is 1");
}

# FUTURE: cachedump not implemented
#my $cache_dump = mem_stats($sock, " cachedump 1 100");
#ok(defined $cache_dump->{'foo'}, "got foo from cachedump");

$cmd = "delete foo"; $rst = "DELETED";
mem_cmd_is($sock, $cmd, "", $rst);

my $stats = mem_stats($sock);
is($stats->{delete_hits}, 1);
is($stats->{delete_misses}, 0);

$cmd = "delete foo"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst, "shouldn't delete foo again");


my $stats = mem_stats($sock);
is($stats->{delete_hits}, 1);
is($stats->{delete_misses}, 1);

# incr stats

sub check_incr_stats {
    my ($ih, $im, $dh, $dm) = @_;
    my $stats = mem_stats($sock);

    is($stats->{incr_hits}, $ih);
    is($stats->{incr_misses}, $im);
    is($stats->{decr_hits}, $dh);
    is($stats->{decr_misses}, $dm);
}

$cmd = "incr i 1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst, "shouldn't incr a missing thing");
check_incr_stats(0, 1, 0, 0);

$cmd = "decr d 1"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, "", $rst, "shouldn't decr a missing thing");
check_incr_stats(0, 1, 0, 1);

$cmd = "set n 0 0 1"; $val = "0"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);

$cmd = "incr n 3"; $rst = "3";
mem_cmd_is($sock, $cmd, "", $rst, "incr works");
check_incr_stats(1, 1, 0, 1);

$cmd = "decr n 1"; $rst = "2";
mem_cmd_is($sock, $cmd, "", $rst, "decr works");
check_incr_stats(1, 1, 1, 1);

# cas stats

sub check_cas_stats {
    my ($ch, $cm, $cb) = @_;
    my $stats = mem_stats($sock);

#    is($stats->{cas_hits}, $ch);
#    is($stats->{cas_misses}, $cm);
#    is($stats->{cas_badval}, $cb);
}

check_cas_stats(0, 0, 0);

$cmd = "cas c 0 0 1 99999999"; $val = "z"; $rst = "NOT_FOUND";
mem_cmd_is($sock, $cmd, $val, $rst, "missed cas");
check_cas_stats(0, 1, 0);

$cmd = "set c 0 0 1"; $val = "x"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst);
my ($id, $v) = mem_gets($sock, 'c');
is('x', $v, 'got the expected value');

$cmd = "cas c 0 0 1 99999999"; $val = "z"; $rst = "EXISTS";
mem_cmd_is($sock, $cmd, $val, $rst, "missed cas");
check_cas_stats(0, 1, 1);
my ($newid, $v) = mem_gets($sock, 'c');
is('x', $v, 'got the expected value');

$cmd = "cas c 0 0 1 $id"; $val = "z"; $rst = "STORED";
mem_cmd_is($sock, $cmd, $val, $rst, "good cas");
check_cas_stats(1, 1, 1);
my ($newid, $v) = mem_gets($sock, 'c');
is('z', $v, 'got the expected value');

my $settings = mem_stats($sock, ' settings');
is(1024, $settings->{'maxconns'});
is('NULL', $settings->{'domain_socket'});
is('on', $settings->{'evictions'});
is('yes', $settings->{'cas_enabled'});
is('no', $settings->{'auth_required_sasl'});

$cmd = "stats reset"; $rst = "RESET";
mem_cmd_is($sock, $cmd, "", $rst, "good stats reset");

my $stats = mem_stats($sock);
is(0, $stats->{'cmd_get'});
is(0, $stats->{'cmd_set'});
is(0, $stats->{'get_hits'});
is(0, $stats->{'get_misses'});
is(0, $stats->{'delete_misses'});
is(0, $stats->{'delete_hits'});
is(0, $stats->{'incr_misses'});
is(0, $stats->{'incr_hits'});
is(0, $stats->{'decr_misses'});
is(0, $stats->{'decr_hits'});
is(0, $stats->{'cas_misses'});
is(0, $stats->{'cas_hits'});
is(0, $stats->{'cas_badval'});
is(0, $stats->{'evictions'});
is(0, $stats->{'reclaimed'});

$cmd = "flush_all"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst, "flushed");

my $stats = mem_stats($sock);
is($stats->{cmd_flush}, 1, "after one flush cmd_flush is 1");

# after test
release_memcached($engine, $server);
