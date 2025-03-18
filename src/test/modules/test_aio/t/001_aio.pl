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

test_generic('worker', $node_worker);
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
	test_generic('io_uring', $node_uring);
	$node_uring->stop();
}


###
# Test io_method=sync
###

my $node_sync = create_node('sync');

# just to have one test not use the default auto-tuning

$node_sync->append_conf(
	'postgresql.conf', qq(
io_max_concurrency=4
));

$node_sync->start();
test_generic('sync', $node_sync);
$node_sync->stop();

done_testing();


###
# Test Helpers
###

sub create_node
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my $io_method = shift;

	my $node = PostgreSQL::Test::Cluster->new($io_method);

	# Want to test initdb for each IO method, otherwise we could just reuse
	# the cluster.
	#
	# Unfortunately Cluster::init() puts PG_TEST_INITDB_EXTRA_OPTS after the
	# options specified by ->extra, if somebody puts -c io_method=xyz in
	# PG_TEST_INITDB_EXTRA_OPTS it would break this test. Fix that up if we
	# detect it.
	local $ENV{PG_TEST_INITDB_EXTRA_OPTS} = $ENV{PG_TEST_INITDB_EXTRA_OPTS};
	if (defined $ENV{PG_TEST_INITDB_EXTRA_OPTS} &&
		$ENV{PG_TEST_INITDB_EXTRA_OPTS} =~ m/io_method=/)
	{
		$ENV{PG_TEST_INITDB_EXTRA_OPTS} .= " -c io_method=$io_method";
	}

	$node->init(extra => [ '-c', "io_method=$io_method" ]);

	$node->append_conf(
		'postgresql.conf', qq(
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
	like($psql->{stderr}, $expected_stderr,
		"$io_method: $name: expected stderr");
	$psql->{stderr} = '';
}

sub query_wait_block
{
	my $io_method = shift;
	my $node = shift;
	my $psql = shift;
	my $name = shift;
	my $sql = shift;
	my $waitfor = shift;

	my $pid = $psql->query_safe('SELECT pg_backend_pid()');

	$psql->{stdin} .= qq($sql;\n);
	$psql->{run}->pump_nb();
	ok(1, "$io_method: $name: issued sql");

	$node->poll_query_until('postgres',
		qq(SELECT wait_event FROM pg_stat_activity WHERE pid = $pid),
		$waitfor,);
	ok(1, "$io_method: $name: observed $waitfor wait event");
}


###
# Sub-tests
###

sub test_handle
{
	my $io_method = shift;
	my $node = shift;

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# leak warning: implicit xact
	psql_like(
		$io_method,
		$psql,
		"handle_get() leak in implicit xact",
		qq(SELECT handle_get()),
		qr/^$/,
		qr/leaked AIO handle/,
		"$io_method: leaky handle_get() warns");

	# leak warning: explicit xact
	psql_like(
		$io_method, $psql,
		"handle_get() leak in explicit xact",
		qq(BEGIN; SELECT handle_get(); COMMIT),
		qr/^$/, qr/leaked AIO handle/);


	# leak warning: explicit xact, rollback
	psql_like(
		$io_method,
		$psql,
		"handle_get() leak in explicit xact, rollback",
		qq(BEGIN; SELECT handle_get(); ROLLBACK;),
		qr/^$/,
		qr/leaked AIO handle/);

	# leak warning: subtrans
	psql_like(
		$io_method,
		$psql,
		"handle_get() leak in subxact",
		qq(BEGIN; SAVEPOINT foo; SELECT handle_get(); COMMIT;),
		qr/^$/,
		qr/leaked AIO handle/);

	# leak warning + error: released in different command (thus resowner)
	psql_like(
		$io_method,
		$psql,
		"handle_release() in different command",
		qq(BEGIN; SELECT handle_get(); SELECT handle_release_last(); COMMIT;),
		qr/^$/,
		qr/leaked AIO handle.*release in unexpected state/ms);

	# no leak, release in same command
	psql_like(
		$io_method,
		$psql,
		"handle_release() in same command",
		qq(BEGIN; SELECT handle_get() UNION ALL SELECT handle_release_last(); COMMIT;),
		qr/^$/,
		qr/^$/);

	# normal handle use
	psql_like($io_method, $psql, "handle_get_release()",
		qq(SELECT handle_get_release()),
		qr/^$/, qr/^$/);

	# should error out, API violation
	psql_like($io_method, $psql, "handle_get_twice()",
		qq(SELECT handle_get_release()),
		qr/^$/, qr/^$/);

	# recover after error in implicit xact
	psql_like(
		$io_method,
		$psql,
		"handle error recovery in implicit xact",
		qq(SELECT handle_get_and_error(); SELECT 'ok', handle_get_release()),
		qr/^|ok$/,
		qr/ERROR.*as you command/);

	# recover after error in implicit xact
	psql_like(
		$io_method,
		$psql,
		"handle error recovery in explicit xact",
		qq(BEGIN; SELECT handle_get_and_error(); SELECT handle_get_release(), 'ok'; COMMIT;),
		qr/^|ok$/,
		qr/ERROR.*as you command/);

	# recover after error in subtrans
	psql_like(
		$io_method,
		$psql,
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
	psql_like(
		$io_method,
		$psql,
		"batch_start() leak & cleanup in implicit xact",
		qq(SELECT batch_start()),
		qr/^$/,
		qr/open AIO batch at end/,
		"$io_method: leaky batch_start() warns");

	# leak warning & recovery: explicit xact
	psql_like(
		$io_method,
		$psql,
		"batch_start() leak & cleanup in explicit xact",
		qq(BEGIN; SELECT batch_start(); COMMIT;),
		qr/^$/,
		qr/open AIO batch at end/,
		"$io_method: leaky batch_start() warns");


	# leak warning & recovery: explicit xact, rollback
	#
	# XXX: This doesn't fail right now, due to not getting a chance to do
	# something at transaction command commit. That's not a correctness issue,
	# it just means it's a bit harder to find buggy code.
	#psql_like($io_method, $psql,
	#		  "batch_start() leak & cleanup after abort",
	#		  qq(BEGIN; SELECT batch_start(); ROLLBACK;),
	#		  qr/^$/,
	#		  qr/open AIO batch at end/, "$io_method: leaky batch_start() warns");

	# no warning, batch closed in same command
	psql_like(
		$io_method,
		$psql,
		"batch_start(), batch_end() works",
		qq(SELECT batch_start() UNION ALL SELECT batch_end()),
		qr/^$/,
		qr/^$/,
		"$io_method: batch_start(), batch_end()");

	$psql->quit();
}

sub test_io_error
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# verify the error is reported in custom C code
	($output, $ret) = $psql->query(
		qq(SELECT read_rel_block_ll('tbl_corr', 1, wait_complete=>true);));
	is($ret, 1, "$io_method: read_rel_block_ll() of tbl_corr page fails");
	like(
		$psql->{stderr},
		qr/invalid page in block 1 of relation base\/.*/,
		"$io_method: read_rel_block_ll() of tbl_corr page reports error");
	$psql->{stderr} = '';

	# verify the error is reported for bufmgr reads
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_corr WHERE ctid = '(1, 1)'));
	is($ret, 1, "$io_method: tid scan reading tbl_corr block fails");
	like(
		$psql->{stderr},
		qr/invalid page in block 1 of relation base\/.*/,
		"$io_method: tid scan reading tbl_corr block reports error");
	$psql->{stderr} = '';

	# verify the error is reported for bufmgr reads
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_corr WHERE ctid = '(1, 1)'));
	is($ret, 1, "$io_method: sequential scan reading tbl_corr block fails");
	like(
		$psql->{stderr},
		qr/invalid page in block 1 of relation base\/.*/,
		"$io_method: sequential scan reading tbl_corr block reports error");
	$psql->{stderr} = '';

	$psql->quit();
}

# Test interplay between StartBufferIO and TerminateBufferIO
sub test_startwait_io
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql_a = $node->background_psql('postgres', on_error_stop => 0);
	my $psql_b = $node->background_psql('postgres', on_error_stop => 0);

	# create a buffer we can play around with
	($output, $ret) =
	  $psql_a->query(qq(SELECT buffer_create_toy('tbl_ok', 1);));
	is($ret, 0, "$io_method: create toy succeeds");
	like($output, qr/^\d+$/, "$io_method: create toy returns numeric");
	my $buf_id = $output;

	# check that one backend can perform StartBufferIO
	psql_like(
		$io_method,
		$psql_a,
		"first StartBufferIO",
		qq(SELECT buffer_call_start_io($buf_id, for_input=>true, nowait=>false);),
		qr/^t$/,
		qr/^$/);

	# but not twice on the same buffer (non-waiting)
	psql_like(
		$io_method,
		$psql_a,
		"second StartBufferIO fails, same session",
		qq(SELECT buffer_call_start_io($buf_id, for_input=>true, nowait=>true);),
		qr/^f$/,
		qr/^$/);
	psql_like(
		$io_method,
		$psql_b,
		"second StartBufferIO fails, other session",
		qq(SELECT buffer_call_start_io($buf_id, for_input=>true, nowait=>true);),
		qr/^f$/,
		qr/^$/);

	# start io in a different session, will block
	query_wait_block(
		$io_method,
		$node,
		$psql_b,
		"blocking start buffer io",
		qq(SELECT buffer_call_start_io($buf_id, for_input=>true, nowait=>false);),
		"BufferIo");

	# Terminate the IO, without marking it as success, this should trigger the
	# waiting session to be able to start the io
	($output, $ret) = $psql_a->query(
		qq(SELECT buffer_call_terminate_io($buf_id, for_input=>true, succeed=>false, io_error=>false, syncio=>true);)
	);
	is($ret, 0,
		"$io_method: blocking start buffer io, terminating io, not valid");

	# Because the IO was terminated, but not marked as valid, second session should get the right to start io
	pump_until($psql_b->{run}, $psql_b->{timeout}, \$psql_b->{stdout}, qr/t/);
	ok(1, "$io_method: blocking start buffer io, can start io");

	# terminate the IO again
	$psql_b->query_safe(
		qq(SELECT buffer_call_terminate_io($buf_id, for_input=>true, succeed=>false, io_error=>false, syncio=>true);)
	);


	# same as the above scenario, but mark IO as having succeeded
	($output, $ret) =
	  $psql_a->query(qq(SELECT buffer_call_start_io($buf_id, true, false);));
	is($ret, 0,
		"$io_method: blocking buffer io w/ success: first start buffer io succeeds"
	);
	is($output, "t",
		"$io_method: blocking buffer io w/ success: first start buffer io returns true"
	);


	# start io in a different session, will block
	query_wait_block(
		$io_method,
		$node,
		$psql_b,
		"blocking start buffer io",
		qq(SELECT buffer_call_start_io($buf_id, for_input=>true, nowait=>false);),
		"BufferIo");

	($output, $ret) = $psql_a->query(
		qq(SELECT buffer_call_terminate_io($buf_id, for_input=>true, succeed=>true, io_error=>false, syncio=>true);)
	);
	is($ret, 0,
		"$io_method: blocking start buffer io, terminating IO, valid");

	# Because the IO was terminated, and marked as valid, second session should complete but not need io
	pump_until($psql_b->{run}, $psql_b->{timeout}, \$psql_b->{stdout}, qr/f/);
	ok(1, "$io_method: blocking start buffer io, no need to start io");


	# buffer is valid now, make it invalid again
	$buf_id = $psql_a->query_safe(qq(SELECT buffer_create_toy('tbl_ok', 1);));

	$psql_a->quit();
	$psql_b->quit();
}

# Test that if the backend issuing a read doesn't wait for the IO's
# completion, another backend can complete the IO
sub test_complete_foreign
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql_a = $node->background_psql('postgres', on_error_stop => 0);
	my $psql_b = $node->background_psql('postgres', on_error_stop => 0);

	# Issue IO without waiting for completion, then sleep
	$psql_a->query_safe(
		qq(SELECT read_rel_block_ll('tbl_ok', 1, wait_complete=>false);));

	# Check that another backend can read the relevant block
	psql_like(
		$io_method,
		$psql_b,
		"completing read started by sleeping backend",
		qq(SELECT count(*) FROM tbl_ok WHERE ctid = '(1,1)' LIMIT 1),
		qr/^1$/,
		qr/^$/);

	# Issue IO without waiting for completion, then exit
	$psql_a->query_safe(
		qq(SELECT read_rel_block_ll('tbl_ok', 1, wait_complete=>false);));
	$psql_a->reconnect_and_clear();

	# Check that another backend can read the relevant block
	psql_like(
		$io_method,
		$psql_b,
		"completing read started by exited backend",
		qq(SELECT count(*) FROM tbl_ok WHERE ctid = '(1,1)' LIMIT 1),
		qr/^1$/,
		qr/^$/);

	# Read a tbl_corr block, then sleep. The other session will retry the IO
	# and also fail. The easiest thing to verify that seems to be to check
	# that both are in the log.
	my $log_location = -s $node->logfile;
	$psql_a->query_safe(
		qq(SELECT read_rel_block_ll('tbl_corr', 1, wait_complete=>false);));

	psql_like(
		$io_method,
		$psql_b,
		"completing read of tbl_corr block started by other backend",
		qq(SELECT count(*) FROM tbl_corr WHERE ctid = '(1,1)' LIMIT 1),
		qr/^$/,
		qr/invalid page in block/);

	# The log message issued for the read_rel_block_ll() should be logged as a LOG
	$node->wait_for_log(qr/LOG[^\n]+invalid page in/, $log_location);
	ok(1,
		"$io_method: completing read of tbl_corr block started by other backend: LOG message for background read"
	);

	# But for the SELECT, it should be an ERROR
	$log_location =
	  $node->wait_for_log(qr/ERROR[^\n]+invalid page in/, $log_location);
	ok(1,
		"$io_method: completing read of tbl_corr block started by other backend: ERROR message for foreground read"
	);

	$psql_a->quit();
	$psql_b->quit();
}

sub test_inject
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# injected what we'd expect
	$psql->query_safe(qq(SELECT inj_io_short_read_attach(8192);));
	$psql->query_safe(qq(SELECT invalidate_rel_block('tbl_ok', 2);));
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_ok WHERE ctid = '(2, 1)';));
	is($ret, 0,
		"$io_method: injection point not triggering failure succeeds");

	# injected a read shorter than a single block, expecting error
	$psql->query_safe(qq(SELECT inj_io_short_read_attach(17);));
	$psql->query_safe(qq(SELECT invalidate_rel_block('tbl_ok', 2);));
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_ok WHERE ctid = '(2, 1)';));
	is($ret, 1, "$io_method: single block short read fails");
	like(
		$psql->{stderr},
		qr/ERROR:.*could not read blocks 2\.\.2 in file "base\/.*": read only 0 of 8192 bytes/,
		"$io_method: single block short read reports error");
	$psql->{stderr} = '';

	# shorten multi-block read to a single block, should retry
	my $inval_query = qq(SELECT invalidate_rel_block('tbl_ok', 0);
SELECT invalidate_rel_block('tbl_ok', 1);
SELECT invalidate_rel_block('tbl_ok', 2);
SELECT invalidate_rel_block('tbl_ok', 3);
/* gap */
SELECT invalidate_rel_block('tbl_ok', 5);
SELECT invalidate_rel_block('tbl_ok', 6);
SELECT invalidate_rel_block('tbl_ok', 7);
SELECT invalidate_rel_block('tbl_ok', 8););

	$psql->query_safe($inval_query);
	$psql->query_safe(qq(SELECT inj_io_short_read_attach(8192);));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_ok;));
	is($ret, 0, "$io_method: multi block short read (1 block) is retried");
	is($output, 10000, "$io_method: multi block short read (1 block) has correct result");

	# shorten multi-block read to two blocks, should retry
	$psql->query_safe($inval_query);
	$psql->query_safe(qq(SELECT inj_io_short_read_attach(8192*2);));

	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_ok;));
	is($ret, 0, "$io_method: multi block short read (2 blocks) is retried");
	is($output, 10000, "$io_method: multi block short read (2 blocks) has correct result");

	# verify that page verification errors are detected even as part of a
	# shortened multi-block read (tbl_corr, block 1 is tbl_corred)
	$psql->query_safe(
		qq(
SELECT invalidate_rel_block('tbl_corr', 0);
SELECT invalidate_rel_block('tbl_corr', 1);
SELECT invalidate_rel_block('tbl_corr', 2);
SELECT inj_io_short_read_attach(8192);
    ));
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_corr WHERE ctid < '(2, 1)'));
	is($ret, 1,
		"$io_method: shortened multi-block read detects invalid page");
	like(
		$psql->{stderr},
		qr/ERROR:.*invalid page in block 1 of relation base\/.*/,
		"$io_method: shortened multi-block reads reports invalid page");
	$psql->{stderr} = '';

	# trigger a hard error, should error out
	$psql->query_safe(
		qq(
SELECT inj_io_short_read_attach(-errno_from_string('EIO'));
SELECT invalidate_rel_block('tbl_ok', 2);
    ));
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_ok;));
	is($ret, 1, "$io_method: first hard IO error is detected");
	like(
		$psql->{stderr},
		qr/ERROR:.*could not read blocks 2\.\.2 in file \"base\/.*\": Input\/output error/,
		"$io_method: first hard IO error is reported");
	$psql->{stderr} = '';

	# trigger a second hard error, should error out again
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_ok;));
	is($ret, 1, "$io_method: second hard IO error is detected");
	like(
		$psql->{stderr},
		qr/ERROR:.*could not read blocks 2\.\.2 in file \"base\/.*\": Input\/output error/,
		"$io_method: second hard IO error is reported");
	$psql->{stderr} = '';

	$psql->query_safe(
		qq(
SELECT inj_io_short_read_detach();
	));

	# now the IO should be ok.
	($output, $ret) =
	  $psql->query(qq(SELECT count(*) FROM tbl_ok;));
	is($ret, 0, "$io_method: recovers after hard error");
	is($output, 10000, "$io_method: recovers after hard error, query result ok");

	$psql->quit();
}

sub test_inject_worker
{
	my $io_method = shift;
	my $node = shift;
	my ($ret, $output);

	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# trigger a failure to reopen, should error out, but should recover
	$psql->query_safe(
		qq(
SELECT inj_io_reopen_attach();
SELECT invalidate_rel_block('tbl_ok', 1);
    ));
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_ok;));
	is($ret, 1, "$io_method: failure to open: detected");
	like(
		$psql->{stderr},
		qr/ERROR:.*could not read blocks 1\.\.1 in file "base\/.*": No such file or directory/,
		"$io_method: failure to open is reported");
	$psql->{stderr} = '';

	$psql->query_safe(
		qq(
SELECT inj_io_reopen_detach();
	));

	# check that we indeed recover
	($output, $ret) = $psql->query(qq(SELECT count(*) FROM tbl_ok;));
	is($ret, 0, "$io_method: failure to open: recovers");
	is($output, 10000, "$io_method: failure to open: next query ok");

	$psql->quit();
}

sub test_generic
{
	my $io_method = shift;
	my $node = shift;

	is($node->safe_psql('postgres', 'SHOW io_method'),
		$io_method, "$io_method: io_method set correctly");

	$node->safe_psql(
		'postgres', qq(
CREATE EXTENSION test_aio;
CREATE TABLE tbl_corr(data int not null) WITH (AUTOVACUUM_ENABLED = false);
CREATE TABLE tbl_ok(data int not null) WITH (AUTOVACUUM_ENABLED = false);

INSERT INTO tbl_corr SELECT generate_series(1, 10000);
INSERT INTO tbl_ok SELECT generate_series(1, 10000);
SELECT grow_rel('tbl_corr', 16);
SELECT grow_rel('tbl_ok', 16);

SELECT corrupt_rel_block('tbl_corr', 1);
CHECKPOINT;
));

	test_handle($io_method, $node);
	test_io_error($io_method, $node);
	test_batch($io_method, $node);
	test_startwait_io($io_method, $node);
	test_complete_foreign($io_method, $node);

  SKIP:
	{
		skip 'Injection points not supported by this build', 1
		  unless $ENV{enable_injection_points} eq 'yes';
		test_inject($io_method, $node);
	}
}
