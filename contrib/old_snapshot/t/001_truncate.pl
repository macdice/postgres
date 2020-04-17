# Test truncation of the old snapshot time mapping, to check
# that we can't get into trouble when xids wrap around.
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 6;

my $node = get_new_node('master');
$node->init;
$node->append_conf("postgresql.conf", "timezone = UTC");
$node->append_conf("postgresql.conf", "old_snapshot_threshold=10");
$node->append_conf("postgresql.conf", "max_prepared_transactions=10");
$node->start;
$node->psql('postgres', 'update pg_database set datallowconn = true');
$node->psql('postgres', 'create extension old_snapshot');

note "check time map is truncated when CLOG is";

# build up a time map with 4 entries
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:00:00Z')");
$node->psql('postgres', "select pg_current_xact_id()");
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:01:00Z')");
$node->psql('postgres', "select pg_current_xact_id()");
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:02:00Z')");
$node->psql('postgres', "select pg_current_xact_id()");
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:03:00Z')");
$node->psql('postgres', "select pg_current_xact_id()");
my $count;
$node->psql('postgres', "select count(*) from pg_old_snapshot_time_mapping()", stdout => \$count);
is($count, 4, "expected to have 4 entries in the old snapshot time map");

# now cause frozen XID to advance
$node->psql('postgres', 'vacuum freeze');
$node->psql('template0', 'vacuum freeze');
$node->psql('template1', 'vacuum freeze');

# we expect all XIDs to have been truncated
$node->psql('postgres', "select count(*) from pg_old_snapshot_time_mapping()", stdout => \$count);
is($count, 0, "expected to have 0 entries in the old snapshot time map");

# put two more in the map
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:04:00Z')");
$node->psql('postgres', "select pg_current_xact_id()");
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:05:00Z')");
$node->psql('postgres', "select pg_current_xact_id()");
$node->psql('postgres', "select count(*) from pg_old_snapshot_time_mapping()", stdout => \$count);
is($count, 2, "expected to have 2 entries in the old snapshot time map");

# prepare a transaction, to stop xmin from getting further ahead
$node->psql('postgres', "begin; select pg_current_xact_id(); prepare transaction 'tx1'");

# add 16 more minutes (this tests wrapping around the mapping array, which is of size 10 + 10)...
$node->psql('postgres', "select pg_clobber_current_snapshot_timestamp('3000-01-01 00:21:00Z')");
$node->psql('postgres', "select pg_current_xact_id()");
$node->psql('postgres', "select count(*) from pg_old_snapshot_time_mapping()", stdout => \$count);
is($count, 18, "expected to have 18 entries in the old snapshot time map");

# now cause frozen XID to advance
$node->psql('postgres', 'vacuum freeze');
$node->psql('template0', 'vacuum freeze');
$node->psql('template1', 'vacuum freeze');

# this should leave just 16
$node->psql('postgres', "select count(*) from pg_old_snapshot_time_mapping()", stdout => \$count);
is($count, 16, "expected to have 16 entries in the old snapshot time map");

# commit tx1, and then freeze again to get rid of all of them
$node->psql('postgres', "commit prepared 'tx1'");

# now cause frozen XID to advance
$node->psql('postgres', 'vacuum freeze');
$node->psql('template0', 'vacuum freeze');
$node->psql('template1', 'vacuum freeze');

# we should now be back to empty
$node->psql('postgres', "select count(*) from pg_old_snapshot_time_mapping()", stdout => \$count);
is($count, 0, "expected to have 0 entries in the old snapshot time map");

$node->stop;
