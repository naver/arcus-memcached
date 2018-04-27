#!/usr/bin/perl
use strict;
use Cwd;

my $engine_name = shift;
my $script_type = shift;
my @engine_list = ("default");
my $opt = '--job 5'; # --job N : run N test jobs in parallel
my $srcdir = getcwd;
my $ext = ".t";

if ($script_type eq "big") {
    $ext = "t";
}

# default engine specific test script : ./t/default
# common engine test script : ./t

if (grep $_ eq $engine_name, @engine_list) {
    #system("prove $opt --ext '$ext' $srcdir/t/$engine_name $srcdir/t :: $engine_name");
    system("prove $opt --ext '$ext' $srcdir/t :: $engine_name");
} elsif ("$engine_name" eq "") {
    # default engine test
    #system("prove $opt --ext '$ext' $srcdir/t/default $srcdir/t :: default");
    system("prove $opt --ext '$ext' $srcdir/t :: default");
} else {
    system('echo -e \'\033[31mmake test [TYPE=<small || big>] [ENGINE=<default>]\033[0m\n\'');
    exit(1);
}
