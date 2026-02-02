#!/usr/bin/perl

use strict;
use Test::More tests => //;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $val;
my $rst;

# JOP test sub routines
sub jop_set {
    my ($key, $from, $to) = @_;
    my $index;
    my $vleng;
    my $path;

    my $root_val="{}";
    mem_cmd_is($sock, "jop set $key \$ 2","{}","CREATED_STORED");

    for ($index = $from; $index <= $to; $index++) {
        $val = "\"datum$index\"";
        $vleng = length($val);
        $path = "\$.field$index";

        $cmd = "jop set $key $path $vleng";
        $rst = "STORED";

        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}

sub jop_delete {
    my ($key, $from, $to) = @_;
    my $index;
    my $path;
    if($to < 2) {return;}
    for ($index = $from; $index <= $to-1; $index++) {
        $path = "\$.field$index";

        $cmd = "jop delete $key $path"; $rst = "DELETED";
        mem_cmd_is($sock, $cmd, "", $rst);
    }
    $cmd = "jop delete $key \$"; $rst = "DELETED_DROPPED";
    mem_cmd_is($sock, $cmd, "", $rst);
}

sub jop_get {
    my ($key, $from, $to) = @_;
    my $index;
    my $path;

    for ($index = $from; $index <= $to; $index++) {
        $path = "\$.field$index";
        $val = "datum$index";
        $cmd = "jop get $key $path";
        $rst="VALUE KEY: jkey, PATH: $path\r\n".
             "$val\r\n".
             "END";

        mem_cmd_is($sock, $cmd, $val, $rst);
    }
}


# JOP test global variables
my $flags = 13;
my $default_set_size = 4000;
my $maximum_set_size = 50000;
my $cnt;

$cmd = "get jkey"; $rst = "END";
mem_cmd_is($sock, $cmd, "", $rst);

jop_set("jkey",1,10);
$cmd = "jop get $key \$.10"; $rst = "datum10";
mem_cmd_is($sock, $cmd, $val, $rst);
