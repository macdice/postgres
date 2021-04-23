# Run the standard regression tests with streaming replication
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 3;

# Initialize primary node
my $node_primary = get_new_node('primary');
# A specific role is created to perform some tests related to replication,
# and it needs proper authentication configuration.
$node_primary->init(
	allows_streaming => 1,
	auth_extra       => [ '--create-role', 'repl_role' ]);
$node_primary->append_conf('postgresql.conf', 'wal_consistency_checking = all');
$node_primary->start;
is( $node_primary->psql(
        'postgres',
        qq[SELECT pg_create_physical_replication_slot('standby_1');]),
    0,
    'physical slot created on primary');
my $backup_name = 'my_backup';

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby_1->append_conf('postgresql.conf',
    "primary_slot_name = standby_1");
$node_standby_1->start;

# Create some content on primary and check its presence in standby 1
$node_primary->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1,1002) AS a");

# Wait for standby to catch up
$node_primary->wait_for_catchup($node_standby_1, 'replay',
	$node_primary->lsn('insert'));

my $result =
  $node_standby_1->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 1: $result\n";
is($result, qq(1002), 'check streamed content on standby 1');

# XXX The tablespace tests don't currently work when the standby shares a
# filesystem with the primary due to colliding absolute paths.  We'll skip
# that for now.

# Run the regression tests against the primary.
system_or_bail($ENV{MAKE}, '-C', '../regress', 'installcheck',
			   'PGPORT=' . $node_primary->port,
			   'EXTRA_REGRESS_OPTS=--skip-tests=tablespace --outputdir=' . TestLib::tempdir);

# Wait for standby to catch up and do a sanity check
$node_primary->safe_psql('postgres', "INSERT INTO tab_int VALUES (42)");
$node_primary->wait_for_catchup($node_standby_1, 'replay',
	$node_primary->lsn('insert'));
$result =
  $node_standby_1->safe_psql('postgres', "SELECT count(*) FROM tab_int");
print "standby 1: $result\n";
is($result, qq(1003), 'check streamed content on standby 1');

$node_standby_1->stop;
$node_primary->stop;
