# Copyright (c) 2022-2023, PostgreSQL Global Development Group

# Exercise various modes of recovery.

# XXX here's what I have so far
#
# crash
# crash + standby
#
# backup
# backup + extra checkpoint
# backup + has_restoring


use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Compare;

my $result;
my $log;

# Set up a cluster.
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(has_archiving => 1, allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', 'log_min_messages = DEBUG2');
$node_primary->start;
$node_primary->psql('postgres', "CREATE TABLE foo(i int)");
$node_primary->stop; # make a shutdown checkpoint

$node_primary->start;
$node_primary->psql('postgres', "INSERT INTO foo VALUES (1)");
$node_primary->stop('immediate'); # crash

$node_primary->start();
$result = $node_primary->safe_psql('postgres', "SELECT max(i) FROM foo;");
is($result, "1", "check table contents after simple crash recovery");

# Because we restarted from a shutdown checkpoint, we should not have replayed
# any checkpoint records.
$log = slurp_file($node_primary->logfile());
ok($log !~ "potential restart point at", "no potential restart points");
ok($log !~ "skipping restart point at", "no skipped restart points");

$node_primary->psql('postgres', "checkpoint"); # make an online checkpoint
$node_primary->psql('postgres', "INSERT INTO foo VALUES (2)");
$node_primary->stop('immediate'); # crash

$node_primary->start();
$result = $node_primary->safe_psql('postgres', "SELECT max(i) FROM foo;");
is($result, "2", "check table contents after simple crash recovery");

# Because we started from an online checkpoint, we should have seen the
# checkpoint record, but skipped the opportunity to create a restart point,
# because we were still in crash recovery.
$log = slurp_file($node_primary->logfile());
ok($log !~ "potential restart point at", "no potential restart points");
ok($log =~ "skipping restart point at", "skipped restart point");

# Take a backup.
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# For a later step, also create some archived WAL.
$node_primary->psql('postgres', "CHECKPOINT"); # another checkpoint
$node_primary->psql('postgres', "INSERT INTO foo VALUES (3)");
$node_primary->psql('postgres', "SELECT pg_switch_wal()"); # archive that

# Start the from the backup, ignoring the archive.
my $node_recover1 = PostgreSQL::Test::Cluster->new('recover1');
$node_recover1->init_from_backup(
	$node_primary, $backup_name,
	standby       => 0,
	has_streaming => 0,
	has_archiving => 0,
	has_restoring => 0);
$node_recover1->append_conf('postgresql.conf', 'log_min_messages = DEBUG2');
$node_recover1->start;

# We started from an online checkpoint created by the base backup, so we should
# have seen the checkpoint record, but skipped it because we're still in crash
# recovery.
$log = slurp_file($node_recover1->logfile());
ok($log !~ "potential restart point at", "no potential restart points");
ok($log =~ "skipping restart point at", "skipped restart point");

=begin comment

# Because we restarted from a shutdown checkpoint, we should not have seen
# any checkpoint records while replaying
$log = slurp_file($node->logfile());
ok($log !~ "potential restart point at", "no potential restart point");



# Simple crash recovery should have seen, and skipped, the restart point
# opportunity (it would be ignored anyway because its redo point matches the
# starting checkpoint, but this verifies that it skipped even earlier)
# XXX so here check for "skipping" message


# Check we can see the expected data.
my $result = $node_primary->safe_psql('postgres', "SELECT max(i) FROM foo;");
is($result, qq{1}, "check table contents after simple crash recovery");


# Create another online checkpoint and some more data.
$node_primary->psql(
	'postgres', qq{
CHECKPOINT;
INSERT INTO foo VALUES (2);
});

# Take a backup.
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Do some work that logs more checkpoints, and archive the WAL segment.
$node_primary->psql(
	'postgres', qq{
CHECKPOINT;
INSERT INTO foo VALUES(3);
SELECT pg_switch_wal();
});

# Start the from the backup, ignoring the archive.
my $node_recover1 = PostgreSQL::Test::Cluster->new('recover1');
$node_recover1->init_from_backup(
	$node_primary, $backup_name,
	standby       => 0,
	has_streaming => 0,
	has_archiving => 0,
	has_restoring => 0);
$node_recover1->append_conf('postgresql.conf', 'log_min_messages = DEBUG2');
$node_recover1->start;

# XXX We should have skipped the restart point.

# We should see data from the moment of the backup, but not the archived changes.
$result = $node_recover1->safe_psql('postgres', "SELECT max(i) FROM foo;");
is($result, qq{2}, "check table contents after simple backup recovery");

# Start the from the backup, including the archive.
my $node_recover2 = PostgreSQL::Test::Cluster->new('recover2');
$node_recover2->init_from_backup(
	$node_primary, $backup_name,
	standby       => 0,
	has_streaming => 0,
	has_archiving => 0,
	has_restoring => 1);
$node_recover2->append_conf('postgresql.conf', 'log_min_messages = DEBUG2');
$node_recover2->start;

# We should see data from the moment of the backup, and also the archived changes.
$result = $node_recover2->safe_psql('postgres', "SELECT max(i) FROM foo;");
is($result, qq{3}, "check table contents after backup recovery + archive");

=cut

done_testing();
