#!/usr/bin/perl

use strict;
use Test::More tests => 6;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;
my $cmd;
my $val;
my $rst;
my $size;
my $csize;
my $gsize;
my $count;
my $prefix_size = 3;
my $cprefix_size = 3;
my $gprefix_size = 2;

sub prefix_insert {
  for ($size = 0; $size < $prefix_size; $size++) {
    $cmd = "set pname$size:foo 0 0 6"; $val = "fooval"; $rst = "STORED";
    mem_cmd_is($sock, $cmd, $val, $rst);
  }
}

sub item_get_hit {
  for ($size = 0; $size < $prefix_size; $size++) {
    $cmd = "get pname$size:foo";
    $rst = "VALUE pname$size:foo 0 6\nfooval\nEND";
    mem_cmd_is($sock, $cmd, "", $rst);
  }
}

sub nested_prefix_insert {
    for($size = 0; $size < $prefix_size; $size++){
        for($csize = 0; $csize < $cprefix_size; $csize++){
            for($gsize = 0; $gsize < $gprefix_size; $gsize++){
                $cmd = "set pname$size:cpname$csize:foo$gsize 0 0 9"; $val = "fooval$size$csize$gsize"; $rst = "STORED";
                mem_cmd_is($sock, $cmd, $val, $rst);
            }
        }
    }

    # for($nestsize = 0; $nestsize < $nest_prefix_size; $nestsize++){
    #     $cmd = "set pname:npname$nestsize:foo 0 0 6"; $val = "fooval$nestsize"; $rst = "STORED";
    #     mem_cmd_is($sock, $cmd, $val, $rst);
    # }
}

sub nested_item_get_hit {  
    for($size = 0; $size < $prefix_size; $size++){
        for($csize = 0; $csize < $cprefix_size; $csize++){
            for($gsize = 0; $gsize < $gprefix_size; $gsize++){
                $cmd = "get pname$size:cpname$csize:foo$gsize";
                $rst = "VALUE pname$size:cpname$csize:foo$gsize 0 9\nfooval$size$csize$gsize\nEND";
                mem_cmd_is($sock, $cmd, "", $rst);
            }
        }
    }

    # for($nestsize = 0; $nestsize < $nest_prefix_size; $nestsize++){
    #     $cmd = "get pname:npname$nestsize:foo";
    #     $rst = "VALUE pname:npname$nestsize:foo 0 6\nfooval$nestsize\nEND";
    #     mem_cmd_is($sock, $cmd, "", $rst);
    # }
}



#prefix_insert();
#item_get_hit();
nested_prefix_insert();
nested_item_get_hit();

# after test
release_memcached($engine, $server);
