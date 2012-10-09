#!/usr/bin/env perl
use strict;
#use autodie;
use File::Basename;
use File::Spec;
use Cwd qw(abs_path);

my $test = abs_path($ARGV[0]);

-e $test or die "No such test: $test\n";

my $module = basename($test);

my $module = File::Spec->catfile(dirname(dirname($test)), $module);

-e $module or die "The test doesn't have a module to load: $module";

(my $result = $test) =~ s/\.[^.]+$/.result/;
(my $reject = $test) =~ s/\.[^.]+$/.reject/;

system "echo 'lua dofile(\"$module\")' | tarantool > /dev/null";

system "cat $test | tarantool > $reject";

system "diff -u $result $reject";

print "Success!\n" if $_ == 0;
