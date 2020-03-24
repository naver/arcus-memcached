#!/usr/bin/perl

use strict;
use Test::More tests => 48;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

my $cmd;
my $rst;

sub binary_key_insert {
    my ($key, $bkeys_ref) = @_;
    my @bkeys = @{$bkeys_ref};

    for (my $i = 0; $i < $#bkeys+1; $i++) {
        my $cmd = "bop insert $key $bkeys[$i] 6";
        my $val = "datum$i";
        mem_cmd_is($sock, $cmd, $val, "STORED");
    }
}

#case1. all binary(hex) bkeys have same length
mem_cmd_is($sock, "bop create fixed_binary_bkey_len 0 0 0", "", "CREATED");
mem_cmd_is($sock, "setattr fixed_binary_bkey_len maxbkeyrange=0x32", "", "OK");
my @fixed_length_bkey = ("0x64", "0x6E", "0x78", "0x82", "0x8C", "0x96");
binary_key_insert("fixed_binary_bkey_len", \@fixed_length_bkey);
mem_cmd_is($sock, "setattr fixed_binary_bkey_len overflowaction=smallest_trim", "", "OK");
mem_cmd_is($sock, "bop insert fixed_binary_bkey_len 0x00 6", "datum6", "OUT_OF_RANGE");
mem_cmd_is($sock, "setattr fixed_binary_bkey_len overflowaction=largest_trim", "", "OK");
mem_cmd_is($sock, "bop insert fixed_binary_bkey_len 0xFF 6", "datum7", "OUT_OF_RANGE");

#case2. all binary(hex) bkeys have different length
mem_cmd_is($sock, "bop create variable_binary_bkey_len 0 0 0", "", "CREATED");
mem_cmd_is($sock, "setattr variable_binary_bkey_len maxbkeyrange=0x5500", "", "OK");
my @variable_length_bkey = ("0xFF", "0xEE00", "0xDD0000", "0xCC000000", "0xBB00000000", "0xAA000000000000");
binary_key_insert("variable_binary_bkey_len", \@variable_length_bkey);
mem_cmd_is($sock, "setattr variable_binary_bkey_len overflowaction=smallest_trim", "", "OK");
mem_cmd_is($sock, "bop insert variable_binary_bkey_len 0x00 6", "datum6", "OUT_OF_RANGE");
mem_cmd_is($sock, "setattr variable_binary_bkey_len overflowaction=largest_trim", "", "OK");
mem_cmd_is($sock, "bop insert variable_binary_bkey_len 0xFFFF 6", "datum7", "OUT_OF_RANGE");

#case3. test for trimming
mem_cmd_is($sock, "bop create trimming 0 0 0", "", "CREATED");
mem_cmd_is($sock, "setattr trimming maxbkeyrange=0x5555555555 overflowaction=smallest_trim", "", "OK");
mem_cmd_is($sock, "bop insert trimming 0x11 6", "datum1", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 1
0x11 6 datum1
END");
mem_cmd_is($sock, "bop insert trimming 0x2222222222 6", "datum2", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 2
0x11 6 datum1
0x2222222222 6 datum2
END");
mem_cmd_is($sock, "bop insert trimming 0x333333333333333333 6", "datum3", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 3
0x11 6 datum1
0x2222222222 6 datum2
0x333333333333333333 6 datum3
END");
mem_cmd_is($sock, "bop insert trimming 0x444444 6", "datum4", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 4
0x11 6 datum1
0x2222222222 6 datum2
0x333333333333333333 6 datum3
0x444444 6 datum4
END");
mem_cmd_is($sock, "bop insert trimming 0x55555555555555 6", "datum5", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 5
0x11 6 datum1
0x2222222222 6 datum2
0x333333333333333333 6 datum3
0x444444 6 datum4
0x55555555555555 6 datum5
END");
mem_cmd_is($sock, "bop insert trimming 0x6622 6", "datum6", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 6
0x11 6 datum1
0x2222222222 6 datum2
0x333333333333333333 6 datum3
0x444444 6 datum4
0x55555555555555 6 datum5
0x6622 6 datum6
END");
mem_cmd_is($sock, "bop insert trimming 0x6666 6", "datum7", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 6
0x2222222222 6 datum2
0x333333333333333333 6 datum3
0x444444 6 datum4
0x55555555555555 6 datum5
0x6622 6 datum6
0x6666 6 datum7
END");
mem_cmd_is($sock, "bop insert trimming 0x7777 6", "datum8", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 7
0x2222222222 6 datum2
0x333333333333333333 6 datum3
0x444444 6 datum4
0x55555555555555 6 datum5
0x6622 6 datum6
0x6666 6 datum7
0x7777 6 datum8
END");
mem_cmd_is($sock, "bop insert trimming 0x777777777777 6", "datum9", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 8
0x2222222222 6 datum2
0x333333333333333333 6 datum3
0x444444 6 datum4
0x55555555555555 6 datum5
0x6622 6 datum6
0x6666 6 datum7
0x7777 6 datum8
0x777777777777 6 datum9
END");
mem_cmd_is($sock, "bop insert trimming 0x88888888 7", "datum10", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 8
0x333333333333333333 6 datum3
0x444444 6 datum4
0x55555555555555 6 datum5
0x6622 6 datum6
0x6666 6 datum7
0x7777 6 datum8
0x777777777777 6 datum9
0x88888888 7 datum10
END");
mem_cmd_is($sock, "bop insert trimming 0x9999999999999999 6", "datum9", "STORED");
mem_cmd_is($sock, "bop get trimming 0x00..0xFF", "", "VALUE 0 7
0x55555555555555 6 datum5
0x6622 6 datum6
0x6666 6 datum7
0x7777 6 datum8
0x777777777777 6 datum9
0x88888888 7 datum10
0x9999999999999999 6 datum9
END");
release_memcached($engine, $server);
