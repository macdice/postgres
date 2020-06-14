# Like src/test/isolation/specs/read-only-anomaly-3.spec, except that
# s3 runs on a streaming replica server.

use strict;
use warnings;

use PostgresNode;
use PsqlSession;
use TestLib;
use Test::More tests => 13;

my $node_primary = get_new_node('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

$node_primary->safe_psql('postgres',
	"CREATE TABLE bank_account (id TEXT PRIMARY KEY, balance DECIMAL NOT NULL)");
$node_primary->safe_psql('postgres',
	"INSERT INTO bank_account (id, balance) VALUES ('X', 0), ('Y', 0)");

my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

my $node_replica = get_new_node('replica');
$node_replica->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_replica->start;

# We need three sessions.  s1 and s2 are on the primary, s3 is on the replica.
my $s1 = PsqlSession->new($node_primary, "postgres");
$s1->send("\\set PROMPT1 ''\n", 2);
$s1->send("\\set PROMPT2 ''\n", 1);
my $s2 = PsqlSession->new($node_primary, "postgres");
$s2->send("\\set PROMPT1 ''\n", 2);
$s2->send("\\set PROMPT2 ''\n", 1);
my $s3 = PsqlSession->new($node_replica, "postgres");
$s3->send("\\set PROMPT1 ''\n", 2);
$s3->send("\\set PROMPT2 ''\n", 1);

my @lines;
my $result;
@lines = $s1->send("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n", 2);
shift @lines;
is(shift @lines, "BEGIN");
@lines = $s2->send("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n", 2);
shift @lines;
is(shift @lines, "BEGIN");
@lines = $s3->send("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n", 2);
shift @lines;
is(shift @lines, "BEGIN");

# s2rx
@lines = $s2->send("SELECT balance FROM bank_account WHERE id = 'X';\n", 2);
shift @lines;
is(shift @lines, "0");

# s2ry
@lines = $s2->send("SELECT balance FROM bank_account WHERE id = 'Y';\n", 2);
shift @lines;
is(shift @lines, "0");

# s1ry
@lines = $s1->send("SELECT balance FROM bank_account WHERE id = 'Y';\n", 2);
shift @lines;
is(shift @lines, "0");

# s1wy
@lines = $s1->send("UPDATE bank_account SET balance = 20 WHERE id = 'Y';\n", 2);
shift @lines;
is(shift @lines, "UPDATE 1");

# s1c
@lines = $s1->send("COMMIT;\n", 2);
shift @lines;
is(shift @lines, "COMMIT");

# now, we want to wait until the replica has replayed s1c
my $until_lsn =
  $node_primary->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
$node_replica->poll_query_until('postgres',
    "SELECT (pg_last_wal_replay_lsn() - '$until_lsn'::pg_lsn) >= 0")
  or die "standby never caught up";

# s3r begins...
@lines = $s3->send("SELECT id, balance FROM bank_account WHERE id IN ('X', 'Y') ORDER BY id;\n", 1);
$s3->check_is_blocked();

# s2wx
@lines = $s2->send("UPDATE bank_account SET balance = -11 WHERE id = 'X';\n", 2);
shift @lines;
is(shift @lines, "UPDATE 1");

# s2c
@lines = $s2->send("COMMIT;\n", 2);
shift @lines;
is(shift @lines, "COMMIT");

# ... s3r completes
@lines = $s3->send("", 2);
is(shift @lines, "X|-11");
is(shift @lines, "Y|20");

# s3c
@lines = $s3->send("COMMIT;\n", 2);
shift @lines;
is(shift @lines, "COMMIT");

