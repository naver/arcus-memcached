#!/usr/bin/perl

use strict;
use Test::More tests => 2846;
use FindBin qw($Bin);
use lib "$Bin/lib";
use MemcachedTest;

#my $server = get_memcached($engine, "-X /home/jhpark/work/arcus-memcached/.libs/syslog_logger.so -m 1024 -vv -r");
my $engine = shift;
my $server = get_memcached($engine);
my $sock = $server->sock;

#my $ksname = "keyset";
my $ksname = "a"x32000;
my $prefix = "longkey";
my $kcount = 200;
my @keyarr = ();
my $keylen;
my $keystr;
my $kcnt;
my $klen;
my $cmd;
my $val;
my $rst;

sub prepare_keyset_with_btree {
    my ($keycnt) = @_;

    $cmd = "bop create $ksname 0 0 $kcount"; $rst = "CREATED";
    print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "keyset created");

    for ($kcnt = 0; $kcnt < $keycnt; $kcnt += 1) {
        $keylen = 2000 + int(rand(2000));
        $keystr = "$prefix:";
        for ($klen = length($keystr); $klen < $keylen; $klen += 1) {
             $keystr .= chr( int(rand(25) + 65) );
        }
        $cmd = "bop insert $ksname $kcnt $keylen"; $val="$keystr"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "$kcnt key created");
    }

    for ($kcnt = 0; $kcnt < $kcount; $kcnt++) {
        $keyarr[$kcnt] = get_key_from_keyset($kcnt);
    }
}

sub delete_keyset_with_btree {
    my ($keycnt) = @_;

    for ($kcnt = 0; $kcnt < $kcount; $kcnt++) {
        $keystr = $keyarr[$kcnt];
        print $sock "delete $keystr\r\n";
        is(scalar <$sock>, "DELETED\r\n", "$kcnt kv item deleted");
    }
    print $sock "delete $ksname\r\n";
    is(scalar <$sock>, "DELETED\r\n", "keyset deleted");
}

sub prepare_keyset_in_memory {
    my ($keycnt) = @_;

    for ($kcnt = 0; $kcnt < $keycnt; $kcnt += 1) {
        $keylen = 2000 + int(rand(30000));
        $keystr = "$prefix:";
        for ($klen = length($keystr); $klen < $keylen; $klen += 1) {
             $keystr .= chr( int(rand(25) + 65) );
        }
        $keyarr[$kcnt] = $keystr;
    }
}

sub delete_keyset_in_memory {
    my ($keycnt) = @_;

    for ($kcnt = 0; $kcnt < $kcount; $kcnt++) {
        $keystr = $keyarr[$kcnt];
        print $sock "delete $keystr\r\n";
        is(scalar <$sock>, "DELETED\r\n", "$kcnt kv item deleted");
    }
}

sub get_key_from_keyset {
    my ($kidx) = @_;
    my $bkey;
    my $leng;
    my $klen;
    my $kstr;

    print $sock "bop get $ksname $kidx\r\n";

    my $head = scalar <$sock>; # skip head response
    my $line = scalar <$sock>; # get the first body line
    while ($line !~ /^END/) {
        $bkey = substr $line, 0, index($line,' ');
        $leng = length($bkey) + 1;
        $klen = substr $line, $leng, index($line,' ',$leng)-$leng;
        $leng = $leng + length($klen) + 1;
        $kstr = substr $line, $leng, length($line)-$leng-2;
        $line = scalar <$sock>; # get the next body line
    }
    my $tail = $line;
    Test::More::is("$head $tail", "VALUE 0 1\r\n END\r\n", "$kidx key retrieved");
    return $kstr;
}

sub assert_kv_mget_old {
    my ($keycnt) = @_;

    # prepare space seperated keys */
    $keystr = $keyarr[0];
    for ($kcnt = 1; $kcnt < $keycnt; $kcnt += 1) {
        $keystr .= " $keyarr[$kcnt]";
    }
    # old kv mget command
    print $sock "get $keystr\r\n";

    for ($kcnt = 0; $kcnt < $keycnt; $kcnt += 1) {
        my $kstr = $keyarr[$kcnt];
        my $data = "data";
        my $vlen = length($data);
        my $head = scalar <$sock>;
        my $body = scalar <$sock>;
        Test::More::is("$head $body", "VALUE $kstr 0 $vlen\r\n $data\r\n", "old mget $kcnt item");
    }
    my $tail = scalar <$sock>;
    Test::More::is("$tail", "END\r\n", "old mget END");
}

sub assert_kv_mget_new {
    my ($keycnt) = @_;

    # prepare space seperated keys */
    $keystr = $keyarr[0];
    for ($kcnt = 1; $kcnt < $keycnt; $kcnt += 1) {
        $keystr .= " $keyarr[$kcnt]";
    }
    # new kv mget command
    $keylen = length($keystr);
    print $sock "mget $keylen $keycnt\r\n$keystr\r\n";

    for ($kcnt = 0; $kcnt < $keycnt; $kcnt += 1) {
        my $kstr = $keyarr[$kcnt];
        my $data = "data";
        my $vlen = length($data);
        my $head = scalar <$sock>;
        my $body = scalar <$sock>;
        Test::More::is("$head $body", "VALUE $kstr 0 $vlen\r\n $data\r\n", "new mget $kcnt item");
    }
    my $tail = scalar <$sock>;
    Test::More::is("$tail", "END\r\n", "new mget END");
}

sub assert_collection_test {
    my ($keycnt) = @_;

    for ($kcnt = 0; $kcnt < 10; $kcnt += 1) {
        $keystr = $keyarr[$kcnt];
        $cmd = "bop create $keystr 0 0 0"; $rst = "CREATED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "bop created");
        $cmd = "bop insert $keystr 1 10"; $val="bop_data_1"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "bop inserted");
        $cmd = "bop insert $keystr 2 10"; $val="bop_data_2"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "bop inserted");
        $cmd = "bop insert $keystr 3 10"; $val="bop_data_3"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "bop inserted");
        #bop_get_is($sock, "$keystr 0..100", 0, 3,
        #           "1,2,3", "bop_data_1,bop_data_2,bop_data_3", "END");
        #getattr_is($sock, "$keystr count", "count=3");
    }
    for ($kcnt = 10; $kcnt < 20; $kcnt += 1) {
        $keystr = $keyarr[$kcnt];
        $cmd = "lop create $keystr 0 0 0"; $rst = "CREATED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "lop created");
        $cmd = "lop insert $keystr -1 10"; $val="lop_data_1"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "lop inserted");
        $cmd = "lop insert $keystr -1 10"; $val="lop_data_2"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "lop inserted");
        $cmd = "lop insert $keystr -1 10"; $val="lop_data_3"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "lop inserted");
        #lop_get_is($sock, "$keystr 0..-1", 0, 3,
        #           "lop_data_1,lop_data_2,lop_data_3", "END");
        #getattr_is($sock, "$keystr count", "count=3");
    }
    for ($kcnt = 20; $kcnt < 30; $kcnt += 1) {
        $keystr = $keyarr[$kcnt];
        $cmd = "sop create $keystr 0 0 0"; $rst = "CREATED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "sop created");
        $cmd = "sop insert $keystr 10"; $val="sop_data_1"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "sop inserted");
        $cmd = "sop insert $keystr 10"; $val="sop_data_2"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "sop inserted");
        $cmd = "sop insert $keystr 10"; $val="sop_data_3"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "sop inserted");
        #sop_get_is($sock, "$keystr 3", 0, 3,
        #           "sop_data_1,sop_data_2,sop_data_3", "END");
        #getattr_is($sock, "$keystr count", "count=3");
        $cmd = "sop exist $keystr 10"; $val="sop_data_3"; $rst = "EXIST";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "sop exist");
        $cmd = "sop exist $keystr 10"; $val="sop_data_4"; $rst = "NOT_EXIST";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "sop not exist");
    }
    for ($kcnt = 30; $kcnt < 40; $kcnt += 1) {
        $keystr = $keyarr[$kcnt];
        $cmd = "mop create $keystr 0 0 0"; $rst = "CREATED";
        print $sock "$cmd\r\n"; is(scalar <$sock>, "$rst\r\n", "mop created");
        $cmd = "mop insert $keystr field_1 10"; $val="mop_data_1"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "mop inserted");
        $cmd = "mop insert $keystr field_2 10"; $val="mop_data_2"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "mop inserted");
        $cmd = "mop insert $keystr field_3 10"; $val="mop_data_3"; $rst = "STORED";
        print $sock "$cmd\r\n$val\r\n"; is(scalar <$sock>, "$rst\r\n", "mop inserted");
        #mop_get_is($sock, "$keystr 23 3", 0, 3, 3, "field_1 field_2 field_3",
        #           "field_1,field_2,filed_3", "mop_data_1,mop_data_2,mop_data_3", "END");
        #getattr_is($sock, "$keystr count", "count=3");
    }
    for ($kcnt = 0; $kcnt < 40; $kcnt += 1) {
        $keystr = $keyarr[$kcnt];
        print $sock "delete $keystr\r\n";
        is(scalar <$sock>, "DELETED\r\n", "collection item deleted");
    }
}

# prepare keyset with b+tree collection
prepare_keyset_with_btree($kcount);
# set
for ($kcnt = 0; $kcnt < $kcount; $kcnt++) {
    $keystr = $keyarr[$kcnt];
    $cmd = "set $keystr 0 0 4"; $val = "data"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n";
    is(scalar <$sock>, "$rst\r\n", "set $kcnt kv item");
}
# get
for ($kcnt = 0; $kcnt < $kcount; $kcnt++) {
    $keystr = $keyarr[$kcnt];
    print $sock "get $keystr\r\n";
    my $expt = "VALUE $keystr 0 4\r\ndata\r\nEND\r\n";
    my $resp = scalar(<$sock>);
    if (!$resp || $resp =~ /^END/) {
    } else {
        $resp .= scalar(<$sock>) . scalar(<$sock>);
    }
    Test::More::is($resp, $expt, "get $kcnt kv item");
}
# mget old
assert_kv_mget_old($kcount);
# mget new
assert_kv_mget_new($kcount);
# delete
delete_keyset_with_btree($kcount);
# collectoin test
assert_collection_test($kcount);


# prepare keyset with in-memory
prepare_keyset_in_memory($kcount);
# set
for ($kcnt = 0; $kcnt < $kcount; $kcnt++) {
    $keystr = $keyarr[$kcnt];
    $cmd = "set $keystr 0 0 4"; $val = "data"; $rst = "STORED";
    print $sock "$cmd\r\n$val\r\n";
    is(scalar <$sock>, "$rst\r\n", "set $kcnt kv item");
}
# get
for ($kcnt = 0; $kcnt < $kcount; $kcnt++) {
    $keystr = $keyarr[$kcnt];
    print $sock "get $keystr\r\n";
    my $expt = "VALUE $keystr 0 4\r\ndata\r\nEND\r\n";
    my $resp = scalar(<$sock>);
    if (!$resp || $resp =~ /^END/) {
    } else {
        $resp .= scalar(<$sock>) . scalar(<$sock>);
    }
    Test::More::is($resp, $expt, "get $kcnt kv item");
}
# mget old
assert_kv_mget_old($kcount);
# mget new
assert_kv_mget_new($kcount);
# delete
delete_keyset_in_memory($kcount);
# collectoin test
assert_collection_test($kcount);

# after test
release_memcached($engine);
