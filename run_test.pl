#!/usr/bin/perl
use strict;
use Cwd;

my $engine_name = shift;
my $script_type = shift;
my @engine_list = ("default");
my $opt = '--job 5'; # --job N : run N test jobs in parallel
my $srcdir = getcwd;
my $ext = "s";

if ($script_type eq "big") {
    $ext = "b";
}

# default engine specific test script : ./t/default
# common engine test script : ./t

if (grep $_ eq $engine_name, @engine_list) {
    system("prove $opt - < $srcdir/t/tlist/engine_$engine_name\_$ext\.txt :: $engine_name");
} elsif ("$engine_name" eq "") {
    # default engine test
    system("prove $opt - < $srcdir/t/tlist/engine_default_$ext\.txt :: default");
} else {
    system('echo -e \'\033[31mmake test [TYPE=<small || big>] [ENGINE=<default>]\033[0m\n\'');
    exit(1);
}
