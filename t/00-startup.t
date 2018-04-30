#!/usr/bin/perl

use strict;
use Test::More tests => 18;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
eval {
    my $server = get_memcached($engine);
    ok($server, "started the server");
};
is($@, '', 'Basic startup works');

eval {
    my $server = get_memcached($engine, "-l fooble");
};
ok($@, "Died with illegal -l args");

eval {
    my $server = get_memcached($engine, "-l 127.0.0.1");
};
is($@,'', "-l 127.0.0.1 works");

eval {
    my $server = get_memcached($engine, '-C');
    my $stats = mem_stats($server->sock, 'settings');
    is('no', $stats->{'cas_enabled'});
};
is($@, '', "-C works");

eval {
    my $server = get_memcached($engine, '-b 8675');
    my $stats = mem_stats($server->sock, 'settings');
    is('8675', $stats->{'tcp_backlog'});
};
is($@, '', "-b works");

foreach my $val ('auto', 'ascii') {
    eval {
        my $server = get_memcached($engine, "-B $val");
        my $stats = mem_stats($server->sock, 'settings');
        ok($stats->{'binding_protocol'} =~ /$val/, "$val works");
    };
    is($@, '', "$val works");
}

# For the binary test, we just verify it starts since we don't have an easy bin client.
eval {
    my $server = get_memcached($engine, "-B binary");
};
is($@, '', "binary works");

eval {
    my $server = get_memcached($engine, "-vv -B auto");
};
is($@, '', "auto works");

eval {
    my $server = get_memcached($engine, "-vv -B ascii");
};
is($@, '', "ascii works");


# For the binary test, we just verify it starts since we don't have an easy bin client.
eval {
    my $server = get_memcached($engine, "-vv -B binary");
};
is($@, '', "binary works");


# Should blow up with something invalid.
eval {
    my $server = get_memcached($engine, "-B http");
};
ok($@, "Died with illegal -B arg.");

# Should not allow -t 0
eval {
    my $server = get_memcached($engine, "-t 0");
};
ok($@, "Died with illegal 0 thread count");

# after test
release_memcached($engine, $server);
