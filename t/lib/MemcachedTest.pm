package MemcachedTest;
use strict;
use IO::Socket::INET;
use IO::Socket::UNIX;
use Exporter 'import';
use Carp qw(croak);
use vars qw(@EXPORT);

# Instead of doing the substitution with Autoconf, we assume that
# cwd == builddir.
use Cwd;
my $builddir = getcwd;


@EXPORT = qw(get_memcached sleep release_memcached
             mem_get_is mem_gets mem_gets_is mem_stats mem_cmd_val_is mem_cmd_is
             getattr_is lop_get_is sop_get_is mop_get_is bop_get_is bop_gbp_is bop_pwg_is bop_smget_is
             bop_ext_get_is bop_ext_smget_is bop_new_smget_is bop_old_smget_is
             stats_prefixes_is stats_noprefix_is stats_prefix_is
             supports_sasl free_port);

sub sleep {
    my $n = shift;
    select undef, undef, undef, $n;
}

sub mem_stats {
    my ($sock, $type) = @_;
    $type = $type ? " $type" : "";
    print $sock "stats$type\r\n";
    my $stats = {};
    while (<$sock>) {
        last if /^(\.|END)/;
        /^(STAT|ITEM) (\S+)\s+([^\r\n]+)/;
        #print " slabs: $_";
        $stats->{$2} = $3;
    }
    return $stats;
}

sub mem_cmd_is {
    my ($sock_opts, $cmd, $val, $rst, $msg) = @_;
    my @response_list;
    my @response_error = ("ATTR_ERROR", "CLIENT_ERROR", "ERROR", "PREFIX_ERROR", "SERVER_ERROR");

    my @prdct_response = split('\n', $rst);
    my @cmd_pipeline = split('\r\n', $cmd);
    my $rst_type = 0;
    my $count;

    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    # send command
    if ("$val" eq "") {
        print $sock "$cmd\r\n";
    } else {
        print $sock "$cmd\r\n$val\r\n";
    }

    # msg string
    if ("$msg" eq "") {
        if (length($val) <= 40) {
            $msg = $cmd . ":" . $val;
        } else {
            $msg = $cmd . ":" . substr($val, 0, 40);
        }
    }

    my $resp = "";
    my $line;

    if ($cmd =~ /noreply$/) {
        if ("$rst" ne "") {
            $rst_type = 3;
        }
    } elsif ($#cmd_pipeline > 1) {
        $rst_type = 3;
    } else {
        $line = scalar <$sock>;
        $resp = $resp . (substr $line, 0, length($line)-2);

        if ($line =~ /^VALUE/) {
            $rst_type = 1;
            @response_list = ("DELETED", "DELETED_DROPPED", "DUPLICATED", "DUPLICATED_TRIMMED", "END", "TRIMMED");
        } elsif ($line =~ /^ELEMENTS/) {
            $rst_type = 1;
            @response_list = ("DUPLICATED", "END");
        } elsif (grep $resp =~ /^$_/, @response_error) {
            $rst_type = 2;
        } elsif ($line =~ /^RESPONSE/) { # pipe command
            $rst_type = 2;
        } elsif ($line =~ /^(ATTR|PREFIX)/) {
            $rst_type = 1;
            @response_list = ("END");
        } else {
            # single line response
#            @response_list = ("ATTR_MISMATCH", "BKEY_MISMATCH", "CREATED", "CREATED_STORED"
#                , "DELETED", "DELETED_DROPPED", "DUPLICATED", "DUPLICATED_TRIMMED"
#                , "EFLAG_MISMATCH", "ELEMENT_EXISTS", "END", "EXIST", "EXISTS", "NOT_EXIST", "NOT_FOUND"
#                , "NOT_FOUND_ELEMENT", "NOT_STORED", "NOT_SUPPORTED", "NOTHING_TO_UPDATE", "OK"
#                , "OUT_OF_RANGE", "OVERFLOWED", "REPLACED", "RESET", "STORED", "TRIMMED"
#                , "TYPE_MISMATCH", "UNREADABLE", "UPDATED"
#                , "ATTR_ERROR", "CLIENT_ERROR", "ERROR", "PREFIX_ERROR", "SERVER_ERROR"
#                , "COUNT=", "POSITION=");
        }
    }

    if ($rst_type eq 1) {
        do {
            $resp = $resp . "\n";
            $line = scalar <$sock>;
            $resp = $resp . (substr $line, 0, length($line)-2);
        } while (!(grep $line =~ /^$_/, @response_list));
    } elsif ($rst_type eq 2) {
        $count = $#prdct_response;
        while ($count--) {
            $resp = $resp . "\n";
            $line = scalar <$sock>;
            $resp = $resp . (substr $line, 0, length($line)-2);
        }
    } elsif ($rst_type eq 3) {
        $count = $#prdct_response + 1;
        while ($count--) {
            $line = scalar <$sock>;
            $resp = $resp . (substr $line, 0, length($line)-2);
            if ($count eq 0) {
                last;
            }
            $resp = $resp . "\n";
        }
    }
    Test::More::is("$resp", "$rst", $msg);
}

sub mem_get_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $key, $val, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    my $expect_flags = $opts->{flags} || 0;
    my $dval = defined $val ? "'$val'" : "<undef>";
    $msg ||= "$key == $dval";

    print $sock "get $key\r\n";
    if (! defined $val) {
        my $line = scalar <$sock>;
        if ($line =~ /^VALUE/) {
            $line .= scalar(<$sock>) . scalar(<$sock>);
        }
        Test::More::is($line, "END\r\n", $msg);
    } else {
        my $len = length($val);
        my $body = scalar(<$sock>);
        my $expected = "VALUE $key $expect_flags $len\r\n$val\r\nEND\r\n";
        if (!$body || $body =~ /^END/) {
            Test::More::is($body, $expected, $msg);
            return;
        }
        $body .= scalar(<$sock>) . scalar(<$sock>);
        Test::More::is($body, $expected, $msg);
    }
}

sub mem_gets {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $key) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;
    my $val;
    my $expect_flags = $opts->{flags} || 0;

    print $sock "gets $key\r\n";
    my $response = <$sock>;
    if ($response =~ /^END/) {
        return "NOT_FOUND";
    }
    else
    {
        $response =~ /VALUE (.*) (\d+) (\d+) (\d+)/;
        my $flags = $2;
        my $len = $3;
        my $identifier = $4;
        read $sock, $val , $len;
        # get the END
        $_ = <$sock>;
        $_ = <$sock>;

        return ($identifier,$val);
    }

}
sub mem_gets_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $identifier, $key, $val, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    my $expect_flags = $opts->{flags} || 0;
    my $dval = defined $val ? "'$val'" : "<undef>";
    $msg ||= "$key == $dval";

    print $sock "gets $key\r\n";
    if (! defined $val) {
        my $line = scalar <$sock>;
        if ($line =~ /^VALUE/) {
            $line .= scalar(<$sock>) . scalar(<$sock>);
        }
        Test::More::is($line, "END\r\n", $msg);
    } else {
        my $len = length($val);
        my $body = scalar(<$sock>);
        my $expected = "VALUE $key $expect_flags $len $identifier\r\n$val\r\nEND\r\n";
        if (!$body || $body =~ /^END/) {
            Test::More::is($body, $expected, $msg);
            return;
        }
        $body .= scalar(<$sock>) . scalar(<$sock>);
        Test::More::is($body, $expected, $msg);
    }
}

# COLLECTION: common
sub mem_cmd_val_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $cmd, $val, $rst, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    if ($val eq "") {
        $msg ||= "$cmd";
        print $sock "$cmd\r\n";
    } else {
        $msg ||= "$cmd: $val";
        print $sock "$cmd\r\n$val\r\n";
    }

    my $resp = "";
    my $line = scalar <$sock>;
#   while ($line !~ /^END/) {
    while ($line !~ /^END\r\n/ and $line !~ /^DUPLICATED\r\n/ and
           $line !~ /^TRIMMED\r\n/ and $line !~ /^DUPLICATED_TRIMMED\r\n/) {
        $resp = $resp . (substr $line, 0, length($line)-2) . "\n";
        $line = scalar <$sock>;
    }
    $resp = $resp . (substr $line, 0, length($line)-2);
    Test::More::is("$resp", "$rst", $msg);
}

# COLLECTION
sub getattr_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $val, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    my $dval = defined $val ? "'$val'" : "<undef>";
    $msg ||= "getattr $args == $dval";

    print $sock "getattr $args\r\n";

    my $expected = $val;
    my @res_array = ();
    my $line = scalar <$sock>;
    while ($line =~ /^ATTR/) {
        push(@res_array, substr $line, 5, length($line)-7);
        $line = scalar <$sock>;
    }
    my $response = join(" ", @res_array);
    Test::More::is($response, $expected, $msg);
}

# COLLECTION
sub lop_get_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $flags, $ecount, $values, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    my $dval = defined $values ? "'$values'" : "<undef>";
    $msg ||= "lop get $args == $flags $ecount $dval";

    print $sock "lop get $args\r\n";

    my $expected_head = "VALUE $flags $ecount\r\n";
    my $expected_body = $values;

    my $response_head = scalar <$sock>;
    my @value_array = ();
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^DELETED/ and $line !~ /^DELETED_DROPPED/) {
        $vleng = substr $line, 0, index($line, ' ');
        $rleng = length($vleng) + 1;
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $response_body = join(",", @value_array);

    Test::More::is("$response_head $response_body", "$expected_head $expected_body", $msg);
}

# COLLECTION
sub sop_get_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $flags, $ecount, $values, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    my $dval = defined $values ? "'$values'" : "<undef>";
    $msg ||= "sop get $args == $flags $ecount $dval";

    print $sock "sop get $args\r\n";

    my $expected_head = "VALUE $flags $ecount\r\n";
    my $expected_body = join(",", sort(split(",", $values)));

    my $response_head = scalar <$sock>;
    my @value_array = ();
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^DELETED/ and $line !~ /^DELETED_DROPPED/) {
        $vleng = substr $line, 0, index($line, ' ');
        $rleng = length($vleng) + 1;
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $response_body = join(",", sort(@value_array));

    Test::More::is("$response_head $response_body", "$expected_head $expected_body", $msg);
}

# COLLECTION
sub mop_get_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $flags, $numfields, $flist, $ecount, $values, $tailstr, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "mop get $args == $flags $ecount field data";

    my $expected_head = "VALUE $flags $ecount\r\n";
    my $expected_field;
    my $expected_body;
    my $expected_tail = "$tailstr\r\n";

    if ($numfields > 0) {
        print $sock "mop get $args\r\n$flist\r\n";
        $expected_field = $flist;
        $expected_body = $values;
    } else {
        print $sock "mop get $args\r\n";
        $expected_field = join(" ", sort(split(" ",$flist)));
        $expected_body = join(" ", sort(split(" ",$values)));
    }

    my $response_head = scalar <$sock>;
    my @field_array = ();
    my @value_array = ();
    my $field;
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^DELETED/ and $line !~ /DELETED_DROPPED/) {
        $field = substr $line, 0, index($line, ' ');
        $rleng = length($field) + 1;
        $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($vleng) + 1;
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@field_array, $field);
        push(@value_array, $value);
        $line = scalar <$sock>;
    }
    my $response_field;
    my $response_body;
    my $response_tail = $line;

    if ($numfields == 0) {
        @field_array = sort @field_array;
        @value_array = sort @value_array;
    }
    $response_field = join(" ", @field_array);
    $response_body = join(" ", @value_array);

    Test::More::is("$response_head $response_field $response_body $response_tail",
                   "$expected_head $expected_field $expected_body $expected_tail", $msg);
}

# COLLECTION
sub bop_get_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $flags, $ecount, $ebkeys, $values, $tailstr, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop get $args == $flags $ecount bkeys data";

    print $sock "bop get $args\r\n";

    my $expected_head = "VALUE $flags $ecount\r\n";
    my $expected_bkey = $ebkeys;
    my $expected_body = $values;
    my $expected_tail = "$tailstr\r\n";

    my $response_head = scalar <$sock>;
    my @ebkey_array = ();
    my @value_array = ();
    my $ebkey;
    my $eflag;
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^TRIMMED/ and $line !~ /^DELETED/ and $line !~ /^DELETED_DROPPED/) {
        $ebkey = substr $line, 0, index($line,' ');
        $rleng = length($ebkey) + 1;
        $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($vleng) + 1;
        if ((substr $vleng , 0, 2) eq "0x") {
            $eflag = $vleng;
            $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
            $rleng = $rleng + length($vleng) + 1;
        }
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@ebkey_array, $ebkey);
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $response_bkey = join(",", @ebkey_array);
    my $response_body = join(",", @value_array);
    my $response_tail = $line;

    Test::More::is("$response_head $response_bkey $response_body $response_tail",
                   "$expected_head $expected_bkey $expected_body $expected_tail", $msg);
}

# COLLECTION
sub bop_ext_get_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $flags, $ecount, $ebkeys, $eflags, $values, $tailstr, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop get $args == $flags $ecount ebkeys eflags values";

    print $sock "bop get $args\r\n";

    my $expected_head = "VALUE $flags $ecount\r\n";
    my $expected_bkey = $ebkeys;
    my $expected_eflg = $eflags;
    my $expected_body = $values;
    my $expected_tail = "$tailstr\r\n";

    my $response_head = scalar <$sock>;
    my @ebkey_array = ();
    my @eflag_array = ();
    my @value_array = ();
    my $ebkey;
    my $eflag;
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^TRIMMED/ and $line !~ /^DELETED/ and $line !~ /^DELETED_DROPPED/) {
        $ebkey = substr $line, 0, index($line,' ');
        $rleng = length($ebkey) + 1;
        if ((substr $line, $rleng, 2) eq "0x") {
            $eflag = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
            $rleng = $rleng + length($eflag) + 1;
        } else {
            $eflag = "";
        }
        $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($vleng) + 1;
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@ebkey_array, $ebkey);
        push(@eflag_array, $eflag);
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $response_bkey = join(",", @ebkey_array);
    my $response_eflg = join(",", @eflag_array);
    my $response_body = join(",", @value_array);
    my $response_tail = $line;

    Test::More::is("$response_head $response_bkey $response_eflg $response_body $response_tail",
                   "$expected_head $expected_bkey $expected_eflg $expected_body $expected_tail", $msg);
}

# COLLECTION
sub bop_gbp_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $reshead, $ebkeys, $values, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop gbp $args == $reshead bkeys data";

    print $sock "bop gbp $args\r\n";

    my $expected_head = "VALUE $reshead\r\n";
    my $expected_bkey = $ebkeys;
    my $expected_body = $values;
    my $expected_tail = "END\r\n";

    my $response_head = scalar <$sock>;
    my @ebkey_array = ();
    my @value_array = ();
    my $ebkey;
    my $eflag;
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^END/) {
        $ebkey = substr $line, 0, index($line,' ');
        $rleng = length($ebkey) + 1;
        $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($vleng) + 1;
        if ((substr $vleng , 0, 2) eq "0x") {
            $eflag = $vleng;
            $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
            $rleng = $rleng + length($vleng) + 1;
        }
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@ebkey_array, $ebkey);
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $response_bkey = join(",", @ebkey_array);
    my $response_body = join(",", @value_array);
    my $response_tail = $line;

    Test::More::is("$response_head $response_bkey $response_body $response_tail",
                   "$expected_head $expected_bkey $expected_body $expected_tail", $msg);
}

# COLLECTION
sub bop_pwg_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $reshead, $ebkeys, $values, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop pwg $args == $reshead bkeys data";

    print $sock "bop pwg $args\r\n";

    my $expected_head = "VALUE $reshead\r\n";
    my $expected_bkey = $ebkeys;
    my $expected_body = $values;
    my $expected_tail = "END\r\n";

    my $response_head = scalar <$sock>;
    my @ebkey_array = ();
    my @value_array = ();
    my $ebkey;
    my $eflag;
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^END/) {
        $ebkey = substr $line, 0, index($line,' ');
        $rleng = length($ebkey) + 1;
        $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($vleng) + 1;
        if ((substr $vleng , 0, 2) eq "0x") {
            $eflag = $vleng;
            $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
            $rleng = $rleng + length($vleng) + 1;
        }
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@ebkey_array, $ebkey);
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $response_bkey = join(",", @ebkey_array);
    my $response_body = join(",", @value_array);
    my $response_tail = $line;

    Test::More::is("$response_head $response_bkey $response_body $response_tail",
                   "$expected_head $expected_bkey $expected_body $expected_tail", $msg);
}

# COLLECTION
sub bop_smget_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $keystr, $ecount, $keys, $flags, $ebkeys, $values, $miss_kcnt, $miss_keys, $tailstr, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop smget $args keystr == $ecount {key flags bkey value} $miss_kcnt {missed key}";

    print $sock "bop smget $args\r\n$keystr\r\n";

    my $newapi;
    if (index($miss_keys,' ') >= 0) {
       $newapi = 1;
    } else {
       $newapi = 0;
    }

    my $exp_elem_head;
    if ($newapi > 0) {
        $exp_elem_head = "ELEMENTS $ecount\r\n";
    } else {
        $exp_elem_head = "VALUE $ecount\r\n";
    }
    my $exp_elem_keys = $keys;
    my $exp_elem_flgs = $flags;
    my $exp_elem_bkey = $ebkeys;
    my $exp_elem_vals = $values;
    my $exp_mkey_head = "MISSED_KEYS $miss_kcnt\r\n";
    my $exp_mkey_vals = $miss_keys;
    my $exp_tkey_head = "TRIMMED_KEYS 0\r\n";
    my $exp_tkey_vals = "";
    my $exp_elem_tail = "$tailstr\r\n";

    my $res_elem_head = scalar <$sock>;
    my @itkey_array = ();
    my @itflg_array = ();
    my @ebkey_array = ();
    my @value_array = ();
    my $itkey;
    my $itflg;
    my $ebkey;
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^MISSED_KEYS/) {
        $itkey = substr $line, 0, index($line,' ');
        $rleng = length($itkey) + 1;
        $itflg = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($itflg) + 1;
        $ebkey = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($ebkey) + 1;
        $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($vleng) + 1;
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@itkey_array, $itkey);
        push(@itflg_array, $itflg);
        push(@ebkey_array, $ebkey);
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $res_elem_keys = join(",", @itkey_array);
    my $res_elem_flgs = join(",", @itflg_array);
    my $res_elem_bkey = join(",", @ebkey_array);
    my $res_elem_vals = join(",", @value_array);

    if ($newapi > 0) {
        my $res_mkey_head = $line;
        my @mskey_array = ();
        my $mskey;
        $line = scalar <$sock>;
        while ($line !~ /^TRIMMED_KEYS/) {
            $mskey = substr $line, 0, length($line)-2;
            push(@mskey_array, $mskey);
            $line = scalar <$sock>;
        }
        my $res_mkey_vals = join(",", @mskey_array);

        my $res_tkey_head = $line;
        my @trkey_array = ();
        my $trkey;
        $line = scalar <$sock>;
        while ($line !~ /^END/ and $line !~ /^DUPLICATED/) {
            $trkey = substr $line, 0, length($line)-2;
            push(@trkey_array, $trkey);
            $line = scalar <$sock>;
        }
        my $res_tkey_vals = join(",", @trkey_array);
        my $res_elem_tail = $line;

        Test::More::is("$res_elem_head $res_elem_bkey $res_elem_vals $res_mkey_head $res_mkey_vals $res_tkey_head $res_tkey_vals $res_elem_tail",
                       "$exp_elem_head $exp_elem_bkey $exp_elem_vals $exp_mkey_head $exp_mkey_vals $exp_tkey_head $exp_tkey_vals $exp_elem_tail", $msg);
    } else {
        my $res_mkey_head = $line;
        my @mskey_array = ();
        my $mskey;
        $line = scalar <$sock>;
        while ($line !~ /^END/ and $line !~ /^TRIMMED/ and $line !~ /^DUPLICATED/ and $line !~ /^DUPLICATED_TRIMMED/) {
            $mskey = substr $line, 0, length($line)-2;
            push(@mskey_array, $mskey);
            $line = scalar <$sock>;
        }
        my $res_mkey_vals = join(",", @mskey_array);
        my $res_elem_tail = $line;
        Test::More::is("$res_elem_head $res_elem_bkey $res_elem_vals $res_mkey_head $res_mkey_vals $res_elem_tail",
                       "$exp_elem_head $exp_elem_bkey $exp_elem_vals $exp_mkey_head $exp_mkey_vals $exp_elem_tail", $msg);
    }
    if ($exp_elem_keys ne "") {
        Test::More::is("$res_elem_keys", "$exp_elem_keys", $msg);
    }
    if ($exp_elem_flgs ne "") {
        Test::More::is("$res_elem_flgs", "$exp_elem_flgs", $msg);
    }
}

# COLLECTION
sub bop_ext_smget_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $keystr, $ecount, $keys, $flags, $ebkeys, $eflags, $values, $miss_kcnt, $miss_keys, $tailstr, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop smget $args keystr == $ecount {key flags bkey [eflag] value} $miss_kcnt {missed key}";

    print $sock "bop smget $args\r\n$keystr\r\n";

    my $exp_elem_head = "ELEMENTS $ecount\r\n";
    my $exp_elem_keys = $keys;
    my $exp_elem_flgs = $flags;
    my $exp_elem_bkey = $ebkeys;
    my $exp_elem_eflg = $eflags;
    my $exp_elem_vals = $values;
    my $exp_mkey_head = "MISSED_KEYS $miss_kcnt\r\n";
    my $exp_mkey_vals = $miss_keys;
    my $exp_elem_tail = "$tailstr\r\n";

    my $res_elem_head = scalar <$sock>;
    my @itkey_array = ();
    my @itflg_array = ();
    my @ebkey_array = ();
    my @eflag_array = ();
    my @value_array = ();
    my $itkey;
    my $itflg;
    my $ebkey;
    my $eflag;
    my $vleng;
    my $value;
    my $rleng;
    my $line = scalar <$sock>;
    while ($line !~ /^MISSED_KEYS/) {
        $itkey = substr $line, 0, index($line,' ');
        $rleng = length($itkey) + 1;
        $itflg = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($itflg) + 1;
        $ebkey = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($ebkey) + 1;
        if ((substr $line, $rleng, 2) eq "0x") {
            $eflag = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
            $rleng = $rleng + length($eflag) + 1;
        } else {
            $eflag = "";
        }
        $vleng = substr $line, $rleng, index($line,' ',$rleng)-$rleng;
        $rleng = $rleng + length($vleng) + 1;
        $value = substr $line, $rleng, length($line)-$rleng-2;
        push(@itkey_array, $itkey);
        push(@itflg_array, $itflg);
        push(@ebkey_array, $ebkey);
        push(@eflag_array, $eflag);
        push(@value_array, $value);
        $line  = scalar <$sock>;
    }
    my $res_elem_keys = join(",", @itkey_array);
    my $res_elem_flgs = join(",", @itflg_array);
    my $res_elem_bkey = join(",", @ebkey_array);
    my $res_elem_eflg = join(",", @eflag_array);
    my $res_elem_vals = join(",", @value_array);

    my $res_mkey_head = $line;
    my @mskey_array = ();
    my $mskey;
    $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^TRIMMED/ and $line !~ /^DUPLICATED/ and $line !~ /^DUPLICATED_TRIMMED/) {
        $mskey = substr $line, 0, length($line)-2;
        push(@mskey_array, $mskey);
        $line = scalar <$sock>;
    }
    my $res_mkey_vals = join(",", @mskey_array);
    my $res_elem_tail = $line;

    Test::More::is("$res_elem_head $res_elem_bkey $res_elem_eflg $res_elem_vals $res_mkey_head $res_mkey_vals $res_elem_tail",
                   "$exp_elem_head $exp_elem_bkey $exp_elem_eflg $exp_elem_vals $exp_mkey_head $exp_mkey_vals $exp_elem_tail", $msg);
    if ($exp_elem_keys ne "") {
        Test::More::is("$res_elem_keys", "$exp_elem_keys", $msg);
    }
    if ($exp_elem_flgs ne "") {
        Test::More::is("$res_elem_flgs", "$exp_elem_flgs", $msg);
    }
}

# COLLECTION
sub bop_new_smget_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $keys, $ecount, $elems, $mcount, $mkeys, $tcount, $tkeys, $resp, $msg) = @_;
#    my ($sock_opts, $args, $keys, $ecount, $elems, $mcount, $mkeys, $resp, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop smget $args keys == $ecount {<key> [trim] <flags> <bkey> [<eflag>] <value>} $mcount {<key> <cause>}";
    print $sock "bop smget $args\r\n$keys\r\n";

    my $line;
    my $elem_data;
    my $mkey_data;
    my $tkey_data;

    my $exp_elem_head = "ELEMENTS $ecount\r\n";
    my $exp_mkey_head = "MISSED_KEYS $mcount\r\n";
    my $exp_tkey_head = "TRIMMED_KEYS $tcount\r\n";
    my $exp_resp_tail = "$resp\r\n";

    my @elem_arr = ();
    my @elem_set = split(",", $elems);
    foreach $elem_data ( @elem_set )
    {
       $elem_data =~ s/^\s+|\s+$//g;
       $elem_data =~ s/[\r\n]//sg;
       push(@elem_arr, $elem_data);
    }
    my @mkey_arr = ();
    my @mkey_set = split(",", $mkeys);
    foreach $mkey_data ( @mkey_set )
    {
       $mkey_data =~ s/^\s+|\s+$//g;
       $mkey_data =~ s/[\r\n]//sg;
       push(@mkey_arr, $mkey_data);
    }
    my @tkey_arr = ();
    my @tkey_set = split(",", $tkeys);
    foreach $tkey_data ( @tkey_set )
    {
       $tkey_data =~ s/^\s+|\s+$//g;
       $tkey_data =~ s/[\r\n]//sg;
       push(@tkey_arr, $tkey_data);
    }
    my $exp_elem_body = join(",", @elem_arr);
    my $exp_mkey_body = join(",", @mkey_arr);
    my $exp_tkey_body = join(",", @tkey_arr);

    @elem_arr = ();
    @mkey_arr = ();
    @tkey_arr = ();

    my $res_elem_head = scalar <$sock>;
    $line = scalar <$sock>;
    while ($line !~ /^MISSED_KEYS/) {
        push(@elem_arr, (substr $line, 0, length($line)-2));
        $line  = scalar <$sock>;
    }
    my $res_elem_body = join(",", @elem_arr);

    my $res_mkey_head = $line;
    $line = scalar <$sock>;
    while ($line !~ /^TRIMMED_KEYS/) {
        push(@mkey_arr, (substr $line, 0, length($line)-2));
        $line  = scalar <$sock>;
    }
    my $res_mkey_body = join(",", @mkey_arr);

    my $res_tkey_head = $line;
    $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^DUPLICATED/) {
        push(@tkey_arr, (substr $line, 0, length($line)-2));
        $line = scalar <$sock>;
    }
    my $res_tkey_body = join(",", @tkey_arr);
    my $res_resp_tail = $line;

    Test::More::is("$res_elem_head $res_elem_body $res_mkey_head $res_mkey_body $res_tkey_head $res_tkey_body $res_resp_tail",
                   "$exp_elem_head $exp_elem_body $exp_mkey_head $exp_mkey_body $exp_tkey_head $exp_tkey_body $exp_resp_tail", $msg);
}

sub bop_old_smget_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $args, $keys, $ecount, $elems, $mcount, $mkeys, $resp, $msg) = @_;
#    my ($sock_opts, $args, $keys, $ecount, $elems, $mcount, $mkeys, $resp, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "bop smget $args keys == $ecount {<key> [trim] <flags> <bkey> [<eflag>] <value>} $mcount {<key> <cause>}";
    print $sock "bop smget $args\r\n$keys\r\n";

    my $line;
    my $elem_data;
    my $mkey_data;

    my $exp_elem_head = "VALUE $ecount\r\n";
    my $exp_mkey_head = "MISSED_KEYS $mcount\r\n";
    my $exp_resp_tail = "$resp\r\n";

    my @elem_arr = ();
    my @elem_set = split(",", $elems);
    foreach $elem_data ( @elem_set )
    {
       $elem_data =~ s/^\s+|\s+$//g;
       $elem_data =~ s/[\r\n]//sg;
       push(@elem_arr, $elem_data);
    }
    my @mkey_arr = ();
    my @mkey_set = split(",", $mkeys);
    foreach $mkey_data ( @mkey_set )
    {
       $mkey_data =~ s/^\s+|\s+$//g;
       $mkey_data =~ s/[\r\n]//sg;
       push(@mkey_arr, $mkey_data);
    }
    my $exp_elem_body = join(",", @elem_arr);
    my $exp_mkey_body = join(",", @mkey_arr);

    @elem_arr = ();
    @mkey_arr = ();

    my $res_elem_head = scalar <$sock>;
    $line = scalar <$sock>;
    while ($line !~ /^MISSED_KEYS/) {
        push(@elem_arr, (substr $line, 0, length($line)-2));
        $line  = scalar <$sock>;
    }
    my $res_elem_body = join(",", @elem_arr);

    my $res_mkey_head = $line;
    $line = scalar <$sock>;
    while ($line !~ /^END/ and $line !~ /^TRIMMED/ and $line !~ /^DUPLICATED/ and $line !~ /^DUPLICATED_TRIMMED/) {
        push(@mkey_arr, (substr $line, 0, length($line)-2));
        $line  = scalar <$sock>;
    }
    my $res_mkey_body = join(",", @mkey_arr);
    my $res_resp_tail = $line;

    Test::More::is("$res_elem_head $res_elem_body $res_mkey_head $res_mkey_body $res_resp_tail",
                   "$exp_elem_head $exp_elem_body $exp_mkey_head $exp_mkey_body $exp_resp_tail", $msg);
}

# DELETE_BY_PREFIX
sub stats_prefixes_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $val, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "stats prefixes";

    print $sock "stats prefixes\r\n";

    my $expected = join(",", sort(split(",", $val)));
    my @res_array = ();
    my $line = scalar <$sock>;
    my $subline;

    while ($line !~ /^END/) {
        $subline = substr $line, 0, index($line, ' tsz');
        push(@res_array, $subline);
        $line = scalar <$sock>;
    }
    my $response = join(",", sort(@res_array));
    Test::More::is("$response $line", "$expected END\r\n", $msg);
}

# DELETE_BY_PREFIX
sub stats_noprefix_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $val, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "stats noprefix";

    print $sock "stats noprefix\r\n";
    my $expected = $val;
    my @res_array = ();
    my $line = scalar <$sock>;

    while ($line =~ /^PREFIX/) {
        my $subline = substr $line, 7, length($line) - 9;

        unless($subline =~ /^hash_items_bytes/ or $subline =~ /^name/) {
            push(@res_array, $subline);
        }

        $line = scalar <$sock>;
    }
    my $response = join(" ", @res_array);
    Test::More::is($response, $expected, $msg);
}

# DELETE_BY_PREFIX
sub stats_prefix_is {
    # works on single-line values only.  no newlines in value.
    my ($sock_opts, $prefix, $val, $msg) = @_;
    my $opts = ref $sock_opts eq "HASH" ? $sock_opts : {};
    my $sock = ref $sock_opts eq "HASH" ? $opts->{sock} : $sock_opts;

    $msg ||= "stats prefix $prefix";

    print $sock "stats prefix $prefix\r\n";
    my $expected = $val;
    my @res_array = ();
    my $line = scalar <$sock>;

    while ($line =~ /^PREFIX/) {
        my $subline = substr $line, 7, length($line) - 9;

        unless($subline =~ /^hash_items_bytes/ or $subline =~ /^name/ or $subline =~ /^tot_prefix_items/) {
            push(@res_array, $subline);
        }

        $line = scalar <$sock>;
    }
    my $response = join(" ", @res_array);
    Test::More::is($response, $expected, $msg);
}

sub free_port {
    my $type = shift || "tcp";
    my $sock;
    my $port;
    while (!$sock) {
        $port = int(rand(20000)) + 30000;
        $sock = IO::Socket::INET->new(LocalAddr => '127.0.0.1',
                                      LocalPort => $port,
                                      Proto     => $type,
                                      ReuseAddr => 1);
    }
    return $port;
}

sub supports_udp {
    my $output = `$builddir/memcached -h`;
    return 0 if $output =~ /^memcached 1\.1\./;
    return 1;
}

sub supports_sasl {
    my $output = `$builddir/memcached -h`;
    return 1 if $output =~ /sasl/i;
    return 0;
}

sub get_memcached {
    my ($engine, $args, $port) = @_;
    if ("$engine" eq "default" || "$engine" eq "") {
        return new_memcached($args, $port);
    } else {
        croak("Failed to get memcached server. engine_name : \"$engine\"");
    }
}

sub new_memcached {
    my ($args, $passed_port) = @_;
    my $port = $passed_port || free_port();
    my $host = '127.0.0.1';

    if ($ENV{T_MEMD_USE_DAEMON}) {
        my ($host, $port) = ($ENV{T_MEMD_USE_DAEMON} =~ m/^([^:]+):(\d+)$/);
        my $conn = IO::Socket::INET->new(PeerAddr => "$host:$port");
        if ($conn) {
            return Memcached::Handle->new(conn => $conn,
                                          host => $host,
                                          port => $port);
        }
        croak("Failed to connect to specified memcached server.") unless $conn;
    }

    my $udpport = free_port("udp");
    $args .= " -p $port";
    if (supports_udp()) {
        $args .= " -U $udpport";
    }
    if ($< == 0) {
        $args .= " -u root";
    }
    $args .= " -E $builddir/.libs/default_engine.so";

    my $exe = "$builddir/memcached";
    croak("memcached binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
    croak("memcached binary not executable\n") unless -x _;

    my $childpid = fork();

    unless ($childpid) {
        exec "$builddir/timedrun 600 $exe $args";
        exit; # never gets here.
    }

    # unix domain sockets
    if ($args =~ /-s (\S+)/) {
        sleep 1;
        my $filename = $1;
        my $conn = IO::Socket::UNIX->new(Peer => $filename) ||
            croak("Failed to connect to unix domain socket: $! '$filename'");

        return Memcached::Handle->new(pid  => $childpid,
                                      conn => $conn,
                                      domainsocket => $filename,
                                      host => $host,
                                      port => $port);
    }

    # try to connect / find open port, only if we're not using unix domain
    # sockets

    for (1..20) {
        my $conn = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port");
        if ($conn) {
            return Memcached::Handle->new(pid  => $childpid,
                                          conn => $conn,
                                          udpport => $udpport,
                                          host => $host,
                                          port => $port);
        }
        select undef, undef, undef, 0.10;
    }
    croak("Failed to startup/connect to memcached server.");
}

sub new_memcached_engine {
    my ($engine, $args, $passed_port) = @_;
    my $port = $passed_port || free_port();
    my $host = '127.0.0.1';

    if ($ENV{T_MEMD_USE_DAEMON}) {
        my ($host, $port) = ($ENV{T_MEMD_USE_DAEMON} =~ m/^([^:]+):(\d+)$/);
        my $conn = IO::Socket::INET->new(PeerAddr => "$host:$port");
        if ($conn) {
            return Memcached::Handle->new(conn => $conn,
                                          host => $host,
                                          port => $port);
        }
        croak("Failed to connect to specified memcached server.") unless $conn;
    }

    my $udpport = free_port("udp");
    $args .= " -p $port";
    if (supports_udp()) {
        $args .= " -U $udpport";
    }
    if ($< == 0) {
        $args .= " -u root";
    }
    $args .= " -E $builddir/.libs/$engine\_engine.so";

    my $exe = "$builddir/memcached";
    croak("memcached binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
    croak("memcached binary not executable\n") unless -x _;

    my $childpid = fork();

    unless ($childpid) {
        exec "$builddir/timedrun 600 $exe $args";
        exit; # never gets here.
    }

    # unix domain sockets
    if ($args =~ /-s (\S+)/) {
        sleep 1;
        my $filename = $1;
        my $conn = IO::Socket::UNIX->new(Peer => $filename) ||
            croak("Failed to connect to unix domain socket: $! '$filename'");

        return Memcached::Handle->new(pid  => $childpid,
                                      conn => $conn,
                                      domainsocket => $filename,
                                      host => $host,
                                      port => $port);
    }

    # try to connect / find open port, only if we're not using unix domain
    # sockets

    for (1..20) {
        my $conn = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port");
        if ($conn) {
            return Memcached::Handle->new(pid  => $childpid,
                                          conn => $conn,
                                          udpport => $udpport,
                                          host => $host,
                                          port => $port);
        }
        select undef, undef, undef, 0.10;
    }
    croak("Failed to startup/connect to memcached server.");
}

sub release_memcached {
    # does nothing in the community version
}

############################################################################
package Memcached::Handle;
sub new {
    my ($class, %params) = @_;
    return bless \%params, $class;
}

sub DESTROY {
    my $self = shift;
    kill 2, $self->{pid};
}

sub stop {
    my $self = shift;
    kill 15, $self->{pid};
}

sub host { $_[0]{host} }
sub port { $_[0]{port} }
sub udpport { $_[0]{udpport} }

sub sock {
    my $self = shift;

    if ($self->{conn} && ($self->{domainsocket} || getpeername($self->{conn}))) {
        return $self->{conn};
    }
    return $self->new_sock;
}

sub new_sock {
    my $self = shift;
    if ($self->{domainsocket}) {
        return IO::Socket::UNIX->new(Peer => $self->{domainsocket});
    } else {
        return IO::Socket::INET->new(PeerAddr => "$self->{host}:$self->{port}");
    }
}

sub new_udp_sock {
    my $self = shift;
    return IO::Socket::INET->new(PeerAddr => '127.0.0.1',
                                 PeerPort => $self->{udpport},
                                 Proto    => 'udp',
                                 LocalAddr => '127.0.0.1',
                                 LocalPort => MemcachedTest::free_port('udp'),
        );

}

1;
