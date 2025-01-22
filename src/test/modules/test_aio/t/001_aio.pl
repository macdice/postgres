# Copyright (c) 2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;


###
# Test io_method=worker
###
my $node_worker = create_node('worker');
$node_worker->start();

run_generic_test('worker', $node_worker);
SKIP:
{
	skip 'Injection points not supported by this build', 1
	  unless $ENV{enable_injection_points} eq 'yes';
	test_inject_worker('worker', $node_worker);
}

$node_worker->stop();


###
# Test io_method=io_uring
###

if ($ENV{with_liburing} eq 'yes')
{
	my $node_uring = create_node('io_uring');
	$node_uring->start();
	run_generic_test('io_uring', $node_uring);
	$node_uring->stop();
}


###
# Test io_method=sync
###

my $node_sync = create_node('sync');

# just to have one test not use the default auto-tuning

$node_sync->append_conf('postgresql.conf', qq(
io_max_concurrency=4
));

$node_sync->start();
run_generic_test('sync', $node_sync);
$node_sync->stop();

done_testing();


###
# Test Helpers
###


sub create_node
{
	my $io_method = shift;

	my $node = PostgreSQL::Test::Cluster->new($io_method);

	# Want to test initdb for each IO method, otherwise we could just reuse
	# the cluster.
	$node->init(extra => ['-c', "io_method=$io_method"]);

	$node->append_conf('postgresql.conf', qq(
shared_preload_libraries=test_aio
log_min_messages = 'DEBUG3'
log_statement=all
restart_after_crash=false
));

	ok(1, "$io_method: initdb");

	return $node;
}


sub psql_like
{
	my $io_method = shift;
	my $psql = shift;
	my $name = shift;
	my $sql = shift;
	my $expected_stdout = shift;
	my $expected_stderr = shift;
	my ($cmdret, $output);

	($output, $cmdret) = $psql->query($sql);

	like($output, $expected_stdout, "$io_method: $name: expected stdout");
	like($psql->{stderr}, $expected_stderr, "$io_method: $name: expected stderr");
	$psql->{stderr} = '';
}


sub test_handle
{
	my $io_method = shift;
	my $node = shift;

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# leak warning: implicit xact
	psql_like($io_method, $psql,
			  "handle_get() leak in implicit xact",
			  qq(SELECT handle_get()),
			  qr/^$/,
			  qr/leaked AIO handle/, "$io_method: leaky handle_get() warns");

	# leak warning: explicit xact
	psql_like($io_method, $psql,
			  "handle_get() leak in explicit xact",
			  qq(BEGIN; SELECT handle_get(); COMMIT),
			  qr/^$/,
			  qr/leaked AIO handle/);


	# leak warning: explicit xact, rollback
	psql_like($io_method, $psql,
			  "handle_get() leak in explicit xact, rollback",
			  qq(BEGIN; SELECT handle_get(); ROLLBACK;),
			  qr/^$/,
			  qr/leaked AIO handle/);

	# leak warning: subtrans
	psql_like($io_method, $psql,
			  "handle_get() leak in subxact",
			  qq(BEGIN; SAVEPOINT foo; SELECT handle_get(); COMMIT;),
			  qr/^$/,
			  qr/leaked AIO handle/);

	# leak warning + error: released in different command (thus resowner)
	psql_like($io_method, $psql,
			  "handle_release() in different command",
			  qq(BEGIN; SELECT handle_get(); SELECT handle_release_last(); COMMIT;),
			  qr/^$/,
			  qr/leaked AIO handle.*release in unexpected state/ms);

	# no leak, release in same command
	psql_like($io_method, $psql,
			  "handle_release() in same command",
			  qq(BEGIN; SELECT handle_get() UNION ALL SELECT handle_release_last(); COMMIT;),
			  qr/^$/,
			  qr/^$/);

	# normal handle use
	psql_like($io_method, $psql,
			  "handle_get_release()",
			  qq(SELECT handle_get_release()),
			  qr/^$/,
			  qr/^$/);

	# should error out, API violation
	psql_like($io_method, $psql,
			  "handle_get_twice()",
			  qq(SELECT handle_get_release()),
			  qr/^$/,
			  qr/^$/);

	# recover after error in implicit xact
	psql_like($io_method, $psql,
			  "handle error recovery in implicit xact",
			  qq(SELECT handle_get_and_error(); SELECT 'ok', handle_get_release()),
			  qr/^|ok$/,
			  qr/ERROR.*as you command/);

	# recover after error in implicit xact
	psql_like($io_method, $psql,
			  "handle error recovery in explicit xact",
			  qq(BEGIN; SELECT handle_get_and_error(); SELECT handle_get_release(), 'ok'; COMMIT;),
			  qr/^|ok$/,
			  qr/ERROR.*as you command/);

	# recover after error in subtrans
	psql_like($io_method, $psql,
			  "handle error recovery in explicit subxact",
			  qq(BEGIN; SAVEPOINT foo; SELECT handle_get_and_error(); ROLLBACK TO SAVEPOINT foo; SELECT handle_get_release(); ROLLBACK;),
			  qr/^|ok$/,
			  qr/ERROR.*as you command/);

	$psql->quit();
}


sub test_batch
{
	my $io_method = shift;
	my $node = shift;

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# leak warning & recovery: implicit xact
	psql_like($io_method, $psql,
			  "batch_start() leak & cleanup in implicit xact",
			  qq(SELECT batch_start()),
			  qr/^$/,
			  qr/open AIO batch at end/, "$io_method: leaky batch_start() warns");

	# leak warning & recovery: explicit xact
	psql_like($io_method, $psql,
			  "batch_start() leak & cleanup in explicit xact",
			  qq(BEGIN; SELECT batch_start(); COMMIT;),
			  qr/^$/,
			  qr/open AIO batch at end/, "$io_method: leaky batch_start() warns");


	# leak warning & recovery: explicit xact, rollback
	#
	# FIXME: This doesn't fail right now, due to not getting a chance to do
	# something at transaction command commit. That's not a correctness issue,
	# it just means it's a bit harder to find buggy code.
	#psql_like($io_method, $psql,
	#		  "batch_start() leak & cleanup after abort",
	#		  qq(BEGIN; SELECT batch_start(); ROLLBACK;),
	#		  qr/^$/,
	#		  qr/open AIO batch at end/, "$io_method: leaky batch_start() warns");

	# no warning, batch closed in same command
	psql_like($io_method, $psql,
			  "batch_start(), batch_end() works",
			  qq(SELECT batch_start() UNION ALL SELECT batch_end()),
			  qr/^$/,
			  qr/^$/, "$io_method: batch_start(), batch_end()");

	$psql->quit();
}

sub test_io_error
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# verify the error is reported in custom C code
	($output, $ret) = $psql->query(qq(SELECT read_corrupt_rel_block('tbl_a', 1);));
	is($ret, 1, "$io_method: read_corrupt_rel_block() fails");
	like($psql->{stderr}, qr/invalid page in block 1 of relation base\/.*/,
		 "$io_method: read_corrupt_rel_block() reports error");
	$psql->{stderr} = '';

	# verify the error is reported for bufmgr reads
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_a WHERE ctid = '(1, 1)'));
	is($ret, 1, "$io_method: tid scan reading corrupt block fails");
	like($psql->{stderr}, qr/invalid page in block 1 of relation base\/.*/,
		 "$io_method: tid scan reading corrupt block reports error");
	$psql->{stderr} = '';

	# verify the error is reported for bufmgr reads
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_a WHERE ctid = '(1, 1)'));
	is($ret, 1, "$io_method: sequential scan reading corrupt block fails");
	like($psql->{stderr}, qr/invalid page in block 1 of relation base\/.*/,
		 "$io_method: sequential scan reading corrupt block reports error");
	$psql->{stderr} = '';

	$psql->quit();
}


sub test_inject
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# injected what we'd expect
	$psql->query_safe(qq(SELECT inj_io_short_read_attach(8192);));
	$psql->query_safe(qq(SELECT invalidate_rel_block('tbl_b', 2);));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_b WHERE ctid = '(2, 1)';));
	is($ret, 0, "$io_method: injection point not triggering failure succeeds");

	# injected a read shorter than a single block, expecting error
	$psql->query_safe(qq(SELECT inj_io_short_read_attach(17);));
	$psql->query_safe(qq(SELECT invalidate_rel_block('tbl_b', 2);));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_b WHERE ctid = '(2, 1)';));
	is($ret, 1, "$io_method: single block short read fails");
	like($psql->{stderr}, qr/ERROR:.*could not read blocks 2\.\.2 in file "base\/.*": read only 0 of 8192 bytes/,
		 "$io_method: single block short read reports error");
	$psql->{stderr} = '';

	# shorten multi-block read to a single block, should retry
	$psql->query_safe(qq(
SELECT invalidate_rel_block('tbl_b', 0);
SELECT invalidate_rel_block('tbl_b', 1);
SELECT invalidate_rel_block('tbl_b', 2);
SELECT inj_io_short_read_attach(8192);
    ));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_b;));
	is($ret, 0, "$io_method: multi block short read is retried");

	# verify that page verification errors are detected even as part of a
	# shortened multi-block read (tbl_a, block 1 is corrupted)
	$psql->query_safe(qq(
SELECT invalidate_rel_block('tbl_a', 0);
SELECT invalidate_rel_block('tbl_a', 1);
SELECT invalidate_rel_block('tbl_a', 2);
SELECT inj_io_short_read_attach(8192);
    ));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_a WHERE ctid < '(2, 1)'));
	is($ret, 1, "$io_method: shortened multi-block read detects invalid page");
	like($psql->{stderr}, qr/ERROR:.*invalid page in block 1 of relation base\/.*/,
		 "$io_method: shortened multi-block reads reports invalid page");
	$psql->{stderr} = '';

	# trigger a hard error, should error out
	$psql->query_safe(qq(
SELECT inj_io_short_read_attach(-errno_from_string('EIO'));
SELECT invalidate_rel_block('tbl_b', 2);
    ));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_b; SELECT 1;));
	is($ret, 1, "$io_method: hard IO error is detected");
	like($psql->{stderr}, qr/ERROR:.*could not read blocks 2..3 in file \"base\/.*\": Input\/output error/,
		 "$io_method: hard IO error is reported");
	$psql->{stderr} = '';

	$psql->query_safe(qq(
SELECT inj_io_short_read_detach();
	));

	$psql->quit();
}


sub test_inject_worker
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# trigger a failure to reopen, should error out, but should recover
	$psql->query_safe(qq(
SELECT inj_io_reopen_attach();
SELECT invalidate_rel_block('tbl_b', 1);
    ));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_b;));
	is($ret, 1, "$io_method: failure to open is detected");
	like($psql->{stderr}, qr/ERROR:.*could not read blocks 1..2 in file "base\/.*": No such file or directory/,
		 "$io_method: failure to open is reported");
	$psql->{stderr} = '';

	$psql->query_safe(qq(
SELECT inj_io_reopen_detach();
	));

	# check that we indeed recover
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_b;));
	is($ret, 0, "$io_method: recovers from failure to open ");


	$psql->quit();
}


sub run_generic_test
{
	my $io_method = shift;
	my $node = shift;

	is($node->safe_psql('postgres', 'SHOW io_method'),
	   $io_method,
	   "$io_method: io_method set correctly");

	$node->safe_psql('postgres', qq(
CREATE EXTENSION test_aio;
CREATE TABLE tbl_a(data int not null) WITH (AUTOVACUUM_ENABLED = false);
CREATE TABLE tbl_b(data int not null) WITH (AUTOVACUUM_ENABLED = false);

INSERT INTO tbl_a SELECT generate_series(1, 10000);
INSERT INTO tbl_b SELECT generate_series(1, 10000);
SELECT grow_rel('tbl_a', 500);
SELECT grow_rel('tbl_b', 550);

SELECT corrupt_rel_block('tbl_a', 1);
));

	test_handle($io_method, $node);
	test_io_error($io_method, $node);
	test_batch($io_method, $node);

  SKIP:
  {
	  skip 'Injection points not supported by this build', 1
		unless $ENV{enable_injection_points} eq 'yes';
	  test_inject($io_method, $node);
  }
}
