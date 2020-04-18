# Simple test of early pruning and snapshot-too-old errors.
use strict;
use warnings;
use PostgresNode;
use PsqlSession;
use TestLib;
use Test::More tests => 5;

my $node = get_new_node('master');
$node->init;
$node->append_conf("postgresql.conf", "timezone = UTC");
$node->append_conf("postgresql.conf", "old_snapshot_threshold=10");
$node->start;
$node->psql('postgres', 'create extension old_snapshot');
$node->psql('postgres', 'create table t (i int)');
$node->psql('postgres', 'insert into t select generate_series(1, 42)');

# start an interactive session that we can use to interleave statements
my $session = PsqlSession->new($node, "postgres");
$session->send("\\set PROMPT1 ''\n", 2);
$session->send("\\set PROMPT2 ''\n", 1);

my @lines;
my $command_tag;
my $result;

# begin a session that we can interleave with vacuum activity
@lines = $session->send("begin transaction isolation level repeatable read;\n", 2);
shift @lines;
$command_tag = shift @lines;
is($command_tag, "BEGIN");

# take a snapshot at time 0
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:00:00Z')");
@lines = $session->send("select * from t order by i limit 1;\n", 2);
shift @lines;
$result = shift @lines;
is($result, "1");

# advance time by 10 minutes, then UPDATE and VACUUM the table
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:10:00Z')");
$node->psql('postgres', "update t set i = 1001 where i = 1");
$node->psql('postgres', "vacuum t");

# our snapshot is not too old yet, so we can still use it
@lines = $session->send("select * from t order by i limit 1;\n", 2);
shift @lines;
$result = shift @lines;
is($result, "1");

# advance time by 10 more minutes, then VACUUM again
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:20:00Z')");
$node->psql('postgres', "vacuum t");

# our snapshot is not too old yet, so we can still use it
@lines = $session->send("select * from t order by i limit 1;\n", 2);
shift @lines;
$result = shift @lines;
is($result, "1");

# advance time by just one more minute, then VACUUM again
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:21:00Z')");
$node->psql('postgres', "vacuum t");

# our snapshot is too old!  the thing it wants to see has been removed
@lines = $session->send("select * from t order by i limit 1;\n", 2);
shift @lines;
$result = shift @lines;
is($result, "ERROR:  snapshot too old");

$session->close;
$node->stop;
