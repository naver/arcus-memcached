#!/usr/bin/perl

use strict;
use Test::More tests => 34;
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
my $sbsize;
my $count;
my $prefix_size = 3;
my $cprefix_size = 4;
my $subkey_size = 2;
my $total_prefix_size = 3;

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
            $cmd = "set pname$size:cpname$csize:foo 0 0 8"; $val = "fooval$size$csize"; $rst = "STORED";
            mem_cmd_is($sock, $cmd, $val, $rst);
        }
    }
}

sub nested_item_get_hit {  
    for($size = 0; $size < $prefix_size; $size++){
        for($csize = 0; $csize < $cprefix_size; $csize++){
            $cmd = "get pname$size:cpname$csize:foo";
            $rst = "VALUE pname$size:cpname$csize:foo 0 8\nfooval$size$csize\nEND";
            mem_cmd_is($sock, $cmd, "", $rst);
        }
    }
}

sub count_prefix_exist {
  print $sock "stats prefixes\r\n";
  my $line = scalar <$sock>;
  $count = 0;

  while ($line =~ /^PREFIX/) {
    $count = $count + 1;
    $line = scalar <$sock>;
  }
  if ($count != $prefix_size)
  {
    croak("The number of prefixes is incorrect.");
  }
}

sub count_total_prefix_exist {
  print $sock "stats prefixes\r\n";
  my $line = scalar <$sock>;
  $count = 0;

  while ($line =~ /^PREFIX/) {
    $count = $count + 1;
    $line = scalar <$sock>;
  }
  #Must be $count != $prefix_size*$cprefix_size
  if ($count != $prefix_size + $prefix_size*$cprefix_size)
  {
    croak("The number of prefixes is incorrect.");
  }
}

sub prefix_flush {
  for ($size = 0; $size < $prefix_size; $size++) {
    $cmd = "flush_prefix pname$size"; $rst = "OK";
    mem_cmd_is($sock, $cmd, "", $rst);
  }
}
sub count_prefix_empty {
  print $sock "stats prefixes\r\n";
  my $line = scalar <$sock>;
  $count = 0;
  while ($line =~ /^PREFIX/) {
    $count = $count + 1;
    $line = scalar <$sock>;
  }
  if ($count != 0)
  {
    croak("The number of prefixes is incorrect.");
  }
}

$cmd = "stats detail on"; $rst = "OK";
mem_cmd_is($sock, $cmd, "", $rst);

prefix_insert();
count_prefix_exist();
item_get_hit();
nested_prefix_insert();
nested_item_get_hit();
count_total_prefix_exist();
prefix_flush();
#count_prefix_empty();

# after test
release_memcached($engine, $server);
