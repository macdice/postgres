
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test CREATE INDEX CONCURRENTLY with concurrent prepared-xact modifications
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Psql;
use PostgreSQL::Test::Utils;

use Test::More;

Test::More->builder->todo_start('filesystem bug')
  if PostgreSQL::Test::Utils::has_wal_read_bug;

my ($node, $result);

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('CIC_2PC_test');
$node->init;
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 10');
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->start;

# We'll use three separate PSQL sessions
my $main_psql = PostgreSQL::Test::Psql->new($node);
my $side_psql = PostgreSQL::Test::Psql->new($node);
my $cic_psql = PostgreSQL::Test::Psql->new($node);

$main_psql->query(q(CREATE EXTENSION amcheck));
$main_psql->query(q(CREATE TABLE tbl(i int)));


#
# Run 3 overlapping 2PC transactions with CIC
#

$main_psql->query(q(
BEGIN;
INSERT INTO tbl VALUES(0);
));

$cic_psql->query_start(q(
CREATE INDEX CONCURRENTLY idx ON tbl(i);
));

$main_psql->query(q(
PREPARE TRANSACTION 'a';
));

$main_psql->query(q(
BEGIN;
INSERT INTO tbl VALUES(0);
));

$side_psql->query(q(COMMIT PREPARED 'a';));

$main_psql->query(q(
PREPARE TRANSACTION 'b';
BEGIN;
INSERT INTO tbl VALUES(0);
));

$side_psql->query(q(COMMIT PREPARED 'b';));

$main_psql->query(q(
PREPARE TRANSACTION 'c';
COMMIT PREPARED 'c';
));

$cic_psql->query_complete;

$result = $main_psql->query_success(q(SELECT bt_index_check('idx',true);));
is($result, '0', 'bt_index_check after overlapping 2PC');


#
# Server restart shall not change whether prepared xact blocks CIC
#

$main_psql->query(q(
BEGIN;
INSERT INTO tbl VALUES(0);
PREPARE TRANSACTION 'spans_restart';
BEGIN;
CREATE TABLE unused ();
PREPARE TRANSACTION 'persists_forever';
));

$main_psql->finish;
$side_psql->finish;
$cic_psql->finish;

$node->restart;

$main_psql = PostgreSQL::Test::Psql->new($node);
$side_psql = PostgreSQL::Test::Psql->new($node);

$main_psql->query_start(q(
DROP INDEX CONCURRENTLY idx;
CREATE INDEX CONCURRENTLY idx ON tbl(i);
));

$side_psql->query("COMMIT PREPARED 'spans_restart'");
$main_psql->query_complete;

$result = $main_psql->query_success(q(SELECT bt_index_check('idx',true)));
is($result, '0', 'bt_index_check after 2PC and restart');


#
# Stress CIC+2PC with pgbench
#
# pgbench might try to launch more than one instance of the CIC
# transaction concurrently.  That would deadlock, so use an advisory
# lock to ensure only one CIC runs at a time.

# Fix broken index first
$main_psql->query(q(REINDEX TABLE tbl;));

# Run pgbench.
$node->pgbench(
	'--no-vacuum --client=5 --transactions=100',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent INSERTs w/ 2PC and CIC',
	{
		'003_pgbench_concurrent_2pc' => q(
			BEGIN;
			INSERT INTO tbl VALUES(0);
			PREPARE TRANSACTION 'c:client_id';
			COMMIT PREPARED 'c:client_id';
		  ),
		'003_pgbench_concurrent_2pc_savepoint' => q(
			BEGIN;
			SAVEPOINT s1;
			INSERT INTO tbl VALUES(0);
			PREPARE TRANSACTION 'c:client_id';
			COMMIT PREPARED 'c:client_id';
		  ),
		'003_pgbench_concurrent_cic' => q(
			SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
			\if :gotlock
				DROP INDEX CONCURRENTLY idx;
				CREATE INDEX CONCURRENTLY idx ON tbl(i);
				SELECT bt_index_check('idx',true);
				SELECT pg_advisory_unlock(42);
			\endif
		  ),
		'004_pgbench_concurrent_ric' => q(
			SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
			\if :gotlock
				REINDEX INDEX CONCURRENTLY idx;
				SELECT bt_index_check('idx',true);
				SELECT pg_advisory_unlock(42);
			\endif
		  )
	});

$main_psql->finish;
$side_psql->finish;
$node->stop;
done_testing();
