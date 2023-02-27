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

# Initialize and start primary node with WAL archiving
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(has_archiving => 1, allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', 'log_min_messages = DEBUG2');
$node_primary->start;

# Make the last checkpoint 'online', so the checkpoint record is replayed.
# Then create some data we can check.
$node_primary->psql(
	'postgres', qq{
CHECKPOINT;
CREATE TABLE foo(i int);
INSERT INTO foo VALUES (1);
});

# Crash
$node_primary->stop('immediate');
$node_primary->start();

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

done_testing();
