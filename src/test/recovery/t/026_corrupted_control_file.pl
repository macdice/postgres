
# Copyright (c) 2021, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use Config;

plan tests => 8;

# find $pat in logfile of $node after $off-th byte
# XXX: function copied from t/019_repslot_limit.pl
sub find_in_log
{
    my ($node, $pat, $off) = @_;

    $off = 0 unless defined $off;
    my $log = TestLib::slurp_file($node->logfile);
    return 0 if (length($log) <= $off);

    $log = substr($log, $off);

    return $log =~ m/$pat/;
}

my $psql_timeout = IPC::Run::timer(60);

my $node = get_new_node('test');
my $control_file = $node->data_dir . "/global/pg_control";
$node->init();

# We start up without warnings if there is no corruption.
my $logstart = -s $node->logfile;
$node->start();
$node->stop();
ok(!find_in_log($node, "incorrect checksum"),
   "both copies ok #1");

# Corrupt the first copy of the control data... we can still start up, using
# the second copy.
open(my $file, '+<', $control_file);
print $file "................";
close($file);
$logstart = -s $node->logfile;
$node->start();
$node->stop();
ok(find_in_log($node, "incorrect checksum for first copy of control data", $logstart),
   "first copy bad");

# There should be no corruption now.
$logstart = -s $node->logfile;
$node->start();
$node->stop();
ok(!find_in_log($node, "incorrect checksum", $logstart),
   "both copies ok #2");

# Corrupt the second copy of the control data.  We don't want to hard-code its
# location in this test, so we'll just grab a chunk of bytes from the front of
# the control file, and then search for the second instance of that string and
# corrupt it.
my $control_file_contents = slurp_file($control_file);
my $header = substr $control_file_contents, 0, 64;
my $index = rindex $control_file_contents, $header;
ok($index > 0, "second copy exists");
open($file, '+<', $control_file);
seek($file, $index, 0);
print $file "................";
close($file);
$logstart = -s $node->logfile;
$node->start();
$node->stop();
ok(find_in_log($node, "incorrect checksum for second copy of control data, correcting", $logstart),
   "second copy bad");

# There should be no corruption now.
$logstart = -s $node->logfile;
$node->start();
$node->stop();
ok(!find_in_log($node, "incorrect checksum", $logstart),
   "both copies ok #3");

# Corrupt both copies.  Now we can't start up!
my $control_file_size = -s $control_file;
open($file, '+<', $control_file);
print $file ("." x $control_file_size);
close($file);
$logstart = -s $node->logfile;
$node->start(fail_ok => 1);
ok(find_in_log($node, "incorrect checksum for both copies of control data", $logstart),
   "both copies bad 1/2");
ok(find_in_log($node, "FATAL:  database files are incompatible with server", $logstart),
   "both copies bad 2/2");
