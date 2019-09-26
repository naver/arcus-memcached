#!/usr/bin/perl
use strict;
use Cwd;

my $engine_name = shift;
my $script_type = shift;
my @engine_list = ("default");
my $opt = '--job 2'; # --job N : run N test jobs in parallel
my $srcdir = getcwd;
my $ext = "s";

if ($script_type eq "big") {
    $ext = "b";
}

# default engine specific test script : ./t/default
# common engine test script : ./t

if (grep $_ eq $engine_name, @engine_list) {
    my $returnCode = system("prove $opt - < $srcdir/t/tlist/engine_$engine_name\_$ext\.txt :: $engine_name");
    if ($returnCode != 0) {
        exit(1);
    }
} elsif ("$engine_name" eq "") {
    # default engine test
    my $returnCode = system("prove $opt - < $srcdir/t/tlist/engine_default_$ext\.txt :: default");
    if ($returnCode != 0) {
        exit(1);
    }
} else {
    system("echo -e \'\033[31mmake test [TYPE=<small || big>] [ENGINE=<@engine_list>]\033[0m\n\'");
    exit(1);
}
