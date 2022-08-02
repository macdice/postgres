# Copyright (c) 2021-2022, PostgreSQL Global Development Group

=pod

=head1 NAME

PostgreSQL::Test::Psql - class representing psql session

=head1 SYNOPSIS

  use PostgreSQL::Test::Psql;

  my $psql = PostgreSQL::Test::Psql->new($cluster, 'postgres');
  my $output = $psql->query("SELECT 42");
  $psql->finish;

=cut

package PostgreSQL::Test::Psql;

use strict;
use warnings;

use Carp;
use Config;
use Test::More;
use Time::HiRes qw(usleep);
use Scalar::Util qw(blessed);

sub new
{
	my ($self, $cluster, $dbname) = @_;

	if (!$dbname) {
		$dbname = "postgres";
	}

	# Since harnesses need references to variables for the
	# input/output streams, we'll create a bunch of lexically scoped
	# variables and return an anonymous function that captures them.
	my $stdin = "";
	my $stdout = "";
	my $stderr = "";
	my $last_query = "";
	my $timer = IPC::Run::timeout(180);
	my $harness = $cluster->background_psql($dbname,
											\$stdin,
											\$stdout,
											\$stderr,
											$timer,
											on_error_stop => 0);
	my $closure = sub {
		my ($command, $argument) = @_;
		if ($command eq "QUERY_START") {
			# Add trailing semi-colon if necessary
			if (!($argument =~ /;$/)) {
				$argument .= ";"
			}
			# Add trailing newline if necessary
			if (!($argument =~ /\n$/)) {
				$argument .= "\n";
			}
			$last_query = $argument;
			$stdin = $argument . "\n\\echo ___SYNC___\n";
			$harness->pump_nb;
		} elsif ($command eq "QUERY_COMPLETE") {
			$stdout = "";
			$stderr = "";
			$harness->pump until $stdout =~ /___SYNC___\n/;
			$stdout =~ s/___SYNC___\n//;
			return $stdout;
		} elsif ($command eq "LAST_QUERY") {
			return $last_query;
		} elsif ($command eq "STDERR") {
			return $stderr;
		} elsif ($command eq "CLUSTER") {
			return $cluster;
		} elsif ($command eq "FINISH") {
			#print "XXX begin psql->finish";
			$stdout = undef;
			$stdin = undef;
			$harness->pump_nb;
			#print "XXX before $harness->finish\n";
			#$harness->finish;
			#print "XXX after $harness->finish\n";
			#print "XXX end psql->finish";
		}
	};

	bless $closure, "PostgreSQL::Test::Psql";
	return $closure;
}

sub query_start
{
	my ($self, $query) = @_;
	($self)->("QUERY_START", $query);
}

sub query_complete
{
	my ($self) = @_;
	my $stdout = ($self)->("QUERY_COMPLETE");
	my $stderr = stderr($self);
	my $query = ($self)->("LAST_QUERY");
	if ($stderr =~ "ERROR:") {
		BAIL_OUT("query \"$query\" failed with: $stderr");
	}
	return $stdout;
}

sub query
{
	my ($self, $query) = @_;
	($self)->query_start($query);
	return ($self)->query_complete();
}

sub query_success
{
	my ($self, $query) = @_;
	($self)->("QUERY_START", $query);
	my $stdout = ($self)->("QUERY_COMPLETE", $query);
	my $stderr = $self->stderr;
	if ($stderr =~ "ERROR:") {
		return 1;
	}
	return 0;
}

sub query_one
{
	my ($self, $query) = @_;
	my $stdout = $self->query($query);
	my $first_line = (split /\n/, $stdout)[0];
	chomp($first_line);
	return $first_line;
}

sub stderr
{
	my ($self) = @_;
	return ($self)->("STDERR");
}

sub cluster
{
	my ($self) = @_;
	return ($self)->("CLUSTER");
}

sub finish
{
	my ($self) = @_;
	print "XXX i will finish\n";
	return ($self)->("FINISH");
}

sub lsn
{
	my ($self, $mode) = @_;
	my %modes = (
		'insert'  => 'pg_current_wal_insert_lsn()',
		'flush'   => 'pg_current_wal_flush_lsn()',
		'write'   => 'pg_current_wal_lsn()',
		'receive' => 'pg_last_wal_receive_lsn()',
		'replay'  => 'pg_last_wal_replay_lsn()');

	$mode = '<undef>' if !defined($mode);
	croak "unknown mode for 'lsn': '$mode', valid modes are "
	  . join(', ', keys %modes)
	  if !defined($modes{$mode});

	my $result = $self->query_one("SELECT $modes{$mode}");
	if ($result eq '')
	{
		return;
	}
	else
	{
		return $result;
	}
}

sub poll_query_until
{
	my ($self, $query, $expected) = @_;

	$expected = 't' unless defined($expected);    # default value

	my ($stdout, $stderr);
	my $max_attempts = 180 * 10;
	my $attempts     = 0;

	while ($attempts < $max_attempts)
	{
		my $stdout = $self->query_one($query);
		$stderr = $self->stderr;
		chomp($stderr);

		if ($stdout eq $expected && $stderr eq '')
		{
			return 1;
		}

		# Wait 0.1 second before retrying.
		usleep(100_000);

		$attempts++;
	}

	# The query result didn't change in 180 seconds. Give up. Print the
	# output from the last attempt, hopefully that's useful for debugging.
	diag qq(poll_query_until timed out executing this query:
$query
expecting this output:
$expected
last actual query output:
$stdout
with stderr:
$stderr);
	return 0;
}

sub wait_for_catchup
{
	my ($self, $standby_name, $mode, $target_lsn) = @_;
	$mode = defined($mode) ? $mode : 'replay';
	my %valid_modes =
	  ('sent' => 1, 'write' => 1, 'flush' => 1, 'replay' => 1);
	croak "unknown mode $mode for 'wait_for_catchup', valid modes are "
	  . join(', ', keys(%valid_modes))
	  unless exists($valid_modes{$mode});

	# Allow passing of a PostgreSQL::Test::Cluster instance as shorthand
	if (blessed($standby_name) && $standby_name->isa("PostgreSQL::Test::Cluster"))
	{
		$standby_name = $standby_name->name;
	}
	if (!defined($target_lsn))
	{
		$target_lsn = $self->lsn('write');
	}
	print "Waiting for replication conn "
	  . $standby_name . "'s "
	  . $mode
	  . "_lsn to pass "
	  . $target_lsn . " on "
	  . $self->cluster->name . "\n";
	my $query =
	  qq[SELECT '$target_lsn' <= ${mode}_lsn AND state = 'streaming' FROM pg_catalog.pg_stat_replication WHERE application_name = '$standby_name';];
	$self->poll_query_until($query)
	  or croak "timed out waiting for catchup";
	print "done\n";
	return;
}

1;
