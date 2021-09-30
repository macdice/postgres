
# Copyright (c) 2021, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 11;

program_help_ok('pg_waldump');
program_version_ok('pg_waldump');
program_options_handling_ok('pg_waldump');

# Test: check if pg_waldump correctly skips over the contiulation
# pages while seeking for the first record.
my $node = PostgresNode->new('primary');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', 'wal_keep_size=1GB');
$node->start;

my $start_lsn = $node->safe_psql('postgres', "SELECT pg_current_wal_lsn()");
my $start_file = $node->safe_psql('postgres', "SELECT pg_walfile_name(pg_current_wal_lsn())");

# insert a record spans over multiple pages
$node->safe_psql('postgres',
	qq{SELECT pg_logical_emit_message(true, 'test 026', repeat('xyzxz', 123456))}
);

# run pg_waldump from the second byte of a record
my ($file, $off) = split(/\//, $start_lsn);
my $target_lsn = sprintf("%X/%X", hex($file), hex($off) + 1);
my ($stdout, $stderr) =
  run_command(["pg_waldump", '-s', $target_lsn,
			  $node->basedir . "/pgdata/pg_wal/" . $start_file]);

ok ($stdout =~
	/first record is after ([0-9A-F\/]+), at ([0-9A-F\/]+), skipping over ([0-9]+) bytes/,
	'output contains required information');
my $echoed_target = $1;
my $first_record = $2;
my $skipped_bytes = $3;

ok ($echoed_target eq $target_lsn, 'target LSN is correct');
ok ($skipped_bytes > 8192, 'skipped more than a page');
