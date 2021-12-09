# Simple tablespace tests that can't be replicated on the same host
# due to the use of absolute paths, so we keep them out of the regular
# regression tests.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More tests => 6;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# Create a directory to hold the tablespace.
my $LOCATION = $node->basedir() . "/testtablespace";
mkdir($LOCATION);

my $result;

# Create a tablespace with an absolute path
$result = $node->psql('postgres',
	"CREATE TABLESPACE regress_tablespace LOCATION '$LOCATION'");
ok($result == 0, 'create tablespace with absolute path');

# Can't create a tablespace where there is one already
$result = $node->psql('postgres',
	"CREATE TABLESPACE regress_tablespace LOCATION '$LOCATION'");
ok($result != 0, 'clobber tablespace with absolute path');

# Create table in it
$result = $node->psql('postgres',
	"CREATE TABLE t () TABLESPACE regress_tablespace");
ok($result == 0, 'create table in tablespace with absolute path');

# Can't drop a tablespace that still has a table in it
$result = $node->psql('postgres',
	"DROP TABLESPACE regress_tablespace LOCATION '$LOCATION'");
ok($result != 0, 'drop tablespace with absolute path');

# Drop the table
$result = $node->psql('postgres', "DROP TABLE t");
ok($result == 0, 'drop table in tablespace with absolute path');

# Drop the tablespace
$result = $node->psql('postgres', "DROP TABLESPACE regress_tablespace");
ok($result == 0, 'drop tablespace with absolute path');

$node->stop;
