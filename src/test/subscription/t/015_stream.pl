
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test streaming of simple large transaction
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Psql;
use PostgreSQL::Test::Utils;
use Test::More;

# Create publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'logical_decoding_work_mem = 64kB');
$node_publisher->start;

# Create subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# Connect to both nodes
my $psql_publisher1 = PostgreSQL::Test::Psql->new($node_publisher);
my $psql_publisher2 = PostgreSQL::Test::Psql->new($node_publisher);
my $psql_subscriber = PostgreSQL::Test::Psql->new($node_subscriber);

# Create some preexisting content on publisher
$psql_publisher1->query("CREATE TABLE test_tab (a int primary key, b varchar)");
$psql_publisher1->query("INSERT INTO test_tab VALUES (1, 'foo'), (2, 'bar')");

# Setup structure on subscriber
$psql_subscriber->query(
	"CREATE TABLE test_tab (a int primary key, b text, c timestamptz DEFAULT now(), d bigint DEFAULT 999)"
);

# Setup logical replication
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$psql_publisher1->query("CREATE PUBLICATION tap_pub FOR TABLE test_tab");

my $appname = 'tap_sub';
$psql_subscriber->query(
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr application_name=$appname' PUBLICATION tap_pub WITH (streaming = on)"
);

$node_publisher->wait_for_catchup($appname);

# Also wait for initial table sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

my $result =
  $psql_subscriber->query_one(
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(2|2|2), 'check initial data was copied to subscriber');

# Interleave a pair of transactions, each exceeding the 64kB limit.
my $in  = '';
my $out = '';

my $timer = IPC::Run::timeout($PostgreSQL::Test::Utils::timeout_default);

$psql_publisher1->query(q{
BEGIN;
INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(3, 5000) s(i);
UPDATE test_tab SET b = md5(b) WHERE mod(a,2) = 0;
DELETE FROM test_tab WHERE mod(a,3) = 0;
});

$psql_publisher2->query(q{
BEGIN;
INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(5001, 9999) s(i);
DELETE FROM test_tab WHERE a > 5000;
COMMIT;
});

$psql_publisher1->query("COMMIT");

$node_publisher->wait_for_catchup($appname);

$result = $psql_subscriber->query_one(
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(3334|3334|3334), 'check extra columns contain local defaults');

# Test the streaming in binary mode
$psql_subscriber->query(
	"ALTER SUBSCRIPTION tap_sub SET (binary = on)");

# Insert, update and delete enough rows to exceed the 64kB limit.
$psql_publisher1->query(q{
BEGIN;
INSERT INTO test_tab SELECT i, md5(i::text) FROM generate_series(5001, 10000) s(i);
UPDATE test_tab SET b = md5(b) WHERE mod(a,2) = 0;
DELETE FROM test_tab WHERE mod(a,3) = 0;
COMMIT;
});

$psql_publisher1->wait_for_catchup($appname);

$result =
  $psql_subscriber->query_one(
	"SELECT count(*), count(c), count(d = 999) FROM test_tab");
is($result, qq(6667|6667|6667), 'check extra columns contain local defaults');

# Change the local values of the extra columns on the subscriber,
# update publisher, and check that subscriber retains the expected
# values. This is to ensure that non-streaming transactions behave
# properly after a streaming transaction.
$psql_subscriber->query(
	"UPDATE test_tab SET c = 'epoch'::timestamptz + 987654321 * interval '1s'"
);
$psql_publisher1->query(
	"UPDATE test_tab SET b = md5(a::text)");

$psql_publisher1->wait_for_catchup($appname);

$result = $psql_subscriber->query_one(
	"SELECT count(*), count(extract(epoch from c) = 987654321), count(d = 999) FROM test_tab"
);
is($result, qq(6667|6667|6667),
	'check extra columns contain locally changed data');

$psql_publisher2->finish;
$psql_publisher1->finish;
$psql_subscriber->finish;

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
