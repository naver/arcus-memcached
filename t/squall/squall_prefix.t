#!/usr/bin/perl

use strict;
##### prefix depth : 2 => 1 #####
use Test::More tests => 40;
#################################
#use Test::More tests => 55;
#################################
use FindBin qw($Bin);
use lib "$Bin/../lib";
use MemcachedTest;

=head
set a 0 0 5
Etiam
get a
set a:b 0 0 9
Venenatis
get a:b
set a:b:c 0 0 11
Consectetur
get a:b:c
set a:b:c:d 0 0 9
Tristique
get a:b:c:d

stats noprefix
stats prefix a
stats prefix a:b

set a:bc 0 0 6
Ornare
get a:bc

flush_prefix a:b
stats prefix a:b

get a
get a:b

set a:b:d 0 0 6
Tortor
get a:b:d

get a:b:c
get a:b:c:d
get a:bc

stats noprefix
stats prefix a
stats prefix a:b

flush_prefix a

stats noprefix
stats prefix a
stats prefix a:b

get a
get a:b
get a:b:c
get a:b:c:d

set a:b 0 0 7
Dapibus
get a:b

set a:b:c 0 0 4
Nibh
get a:b:c

stats noprefix
stats prefix a
stats prefix a:b

set b:d:e 0 0 6
Moliis
get b:d:e

flush_prefix b

sleep 1

set b:d:e 0 0 5
Fusce
get b:d:e

flush_prefix a 2
stats prefix a

set a:c 0 0 7
Egestas

stats prefix a
get a:c

stats noprefix
stats prefix a
stats prefix a:b
sleep 2.2
get a:c

=cut

my $server = new_memcached_engine("squall");
my $sock = $server->sock;
my $expire;

#initial stats
##### stats prefixes command ####
stats_prefixes_is($sock, "");
#################################
#stats_noprefix_is($sock, "hash_items=0 prefix_items=0 tot_prefix_items=0");
#################################

# Insert some items having prefix
print $sock "set a 0 0 5\r\nEtiam\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a");
mem_get_is($sock, "a", "Etiam");

print $sock "set a:b 0 0 9\r\nVenenatis\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:b");
mem_get_is($sock, "a:b", "Venenatis");

print $sock "set a:b:c 0 0 11\r\nConsectetur\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:b:c");
mem_get_is($sock, "a:b:c", "Consectetur");

print $sock "set a:b:c:d 0 0 9\r\nTristique\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:b:c:d");
mem_get_is($sock, "a:b:c:d", "Tristique");

# prefix stats
##### stats prefixes command ####
stats_prefixes_is($sock, "PREFIX <null> itm 1,PREFIX a itm 3");
##### prefix depth : 2 => 1 #####
#stats_noprefix_is($sock, "hash_items=1 prefix_items=1 tot_prefix_items=1");
#stats_prefix_is($sock, "a", "hash_items=3 prefix_items=0");
#################################
#stats_noprefix_is($sock, "hash_items=1 prefix_items=1 tot_prefix_items=2");
#stats_prefix_is($sock, "a", "hash_items=1 prefix_items=1");
#stats_prefix_is($sock, "a:b", "hash_items=2 prefix_items=0");
#################################

# test flush_prefix 2nd level prefix with zero delay
print $sock "set a:bc 0 0 6\r\nOrnare\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:bc");
mem_get_is($sock, "a:bc", "Ornare");

##### prefix depth : 2 => 1 #####
#print $sock "flush_prefix a:b\r\n";
#is(scalar <$sock>, "OK\r\n", "did flush_prefix a:b");
#################################

##### prefix depth : 2 => 1 #####
#stats_prefix_is($sock, "a:b", '');
#################################

mem_get_is($sock, "a", "Etiam");
mem_get_is($sock, "a:b", "Venenatis");

print $sock "set a:b:d 0 0 6\r\nTortor\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:b:d");
mem_get_is($sock, "a:b:d", "Tortor");

##### prefix depth : 2 => 1 #####
#mem_get_is($sock, "a:b:c", undef);
#mem_get_is($sock, "a:b:c:d", undef);
#mem_get_is($sock, "a:bc", "Ornare");
#################################

# prefix stats
##### stats prefixes command ####
stats_prefixes_is($sock, "PREFIX <null> itm 1,PREFIX a itm 5");
##### prefix depth : 2 => 1 #####
#stats_noprefix_is($sock, "hash_items=1 prefix_items=1 tot_prefix_items=1");
#stats_prefix_is($sock, "a", "hash_items=5 prefix_items=0");
#################################
#stats_noprefix_is($sock, "hash_items=1 prefix_items=1 tot_prefix_items=2");
#stats_prefix_is($sock, "a", "hash_items=2 prefix_items=1");
#stats_prefix_is($sock, "a:b", "hash_items=1 prefix_items=0");
#################################

# flush_prefix 1st level prefix
print $sock "flush_prefix a\r\n";
is(scalar <$sock>, "OK\r\n", "did flush_prefix a");

##### stats prefixes command ####
stats_prefixes_is($sock, "PREFIX <null> itm 1");
##### prefix depth : 2 => 1 #####
#stats_noprefix_is($sock, "hash_items=1 prefix_items=0 tot_prefix_items=0");
#stats_prefix_is($sock, "a", '');
#################################
#stats_noprefix_is($sock, "hash_items=1 prefix_items=0 tot_prefix_items=0");
#stats_prefix_is($sock, "a", '');
#stats_prefix_is($sock, "a:b", '');
#################################

mem_get_is($sock, "a", "Etiam");
mem_get_is($sock, "a:b", undef);
mem_get_is($sock, "a:b:c", undef);
mem_get_is($sock, "a:b:c:d", undef);

# check that flush_prefix doesn't blow away items that immediately get set
print $sock "set a:b 0 0 7\r\nDapibus\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:b");
mem_get_is($sock, "a:b", "Dapibus");

print $sock "set a:b:c 0 0 4\r\nNibh\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:b:c");
mem_get_is($sock, "a:b:c", "Nibh");

##### stats prefixes command ####
stats_prefixes_is($sock, "PREFIX <null> itm 1,PREFIX a itm 2");
##### prefix depth : 2 => 1 #####
#stats_noprefix_is($sock, "hash_items=1 prefix_items=1 tot_prefix_items=1");
#stats_prefix_is($sock, "a", "hash_items=2 prefix_items=0");
#################################
#stats_noprefix_is($sock, "hash_items=1 prefix_items=1 tot_prefix_items=2");
#stats_prefix_is($sock, "a", "hash_items=1 prefix_items=1");
#stats_prefix_is($sock, "a:b", "hash_items=1 prefix_items=0");
#################################

# check that flush_prefix doesn't blow away items after sleep
print $sock "set b:d:e 0 0 6\r\nMollis\r\n";
is(scalar <$sock>, "STORED\r\n", "stored b:d:e");
mem_get_is($sock, "b:d:e", "Mollis");

print $sock "flush_prefix b\r\n";
is(scalar <$sock>, "OK\r\n", "did flush_prefix b");

sleep(1);

print $sock "set b:d:e 0 0 5\r\nFusce\r\n";
is(scalar <$sock>, "STORED\r\n", "stored b:d:e");
mem_get_is($sock, "b:d:e", "Fusce");

=head
# and the other form, specifying a flush_prefix time...
my $expire = time() + 2;
print $sock "flush_prefix a $expire\r\n";
is(scalar <$sock>, "OK\r\n", "did flush_prefix a in future");

##### stats prefixes command ####
stats_prefixes_is($sock, "PREFIX <null> itm 1,PREFIX a itm 2,PREFIX b itm 1");
##### prefix depth : 2 => 1 #####
#stats_prefix_is($sock, "a", "hash_items=2 prefix_items=0");
#################################
#stats_prefix_is($sock, "a", "hash_items=1 prefix_items=1");
#################################

print $sock "set a:c 0 0 7\r\nEgestas\r\n";
is(scalar <$sock>, "STORED\r\n", "stored a:c = 'Egestas'");

##### stats prefixes command ####
stats_prefixes_is($sock, "PREFIX <null> itm 1,PREFIX a itm 3,PREFIX b itm 1");
##### prefix depth : 2 => 1 #####
#stats_prefix_is($sock, "a", "hash_items=3 prefix_items=0");
#################################
#stats_prefix_is($sock, "a", "hash_items=2 prefix_items=1");
#################################
mem_get_is($sock, "a:c", 'Egestas');

##### stats prefixes command ####
stats_prefixes_is($sock, "PREFIX <null> itm 1,PREFIX a itm 3,PREFIX b itm 1");
##### prefix depth : 2 => 1 #####
#stats_noprefix_is($sock, "hash_items=1 prefix_items=2 tot_prefix_items=2");
#stats_prefix_is($sock, "a", "hash_items=3 prefix_items=0");
#################################
#stats_noprefix_is($sock, "hash_items=1 prefix_items=2 tot_prefix_items=4");
#stats_prefix_is($sock, "a", "hash_items=2 prefix_items=1");
#stats_prefix_is($sock, "a:b", "hash_items=1 prefix_items=0");
#################################

sleep(2.2);
mem_get_is($sock, "a:c", undef);
=cut
