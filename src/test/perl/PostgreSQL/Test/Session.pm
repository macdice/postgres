package PostgreSQL::Test::Session;

use strict;
use warnings FATAL => 'all';

use Carp;
use Time::HiRes qw(usleep);

use PostgreSQL::PqFFI;

my $setup_ok;

sub setup
{
	return if $setup_ok;
	my $libdir = shift;
	PostgreSQL::PqFFI::setup($libdir);
	$setup_ok = 1;
}

# can pass either a PostgreSQL::Test::Cluster instance or an explicit
# directory location for libpq.{so, dll, whatever} plus a connstr
sub new
{
	my $class = shift;
	my $self = {};
	bless $self, $class;
	my %args = @_;
	my $node = $args{node};
	my $dbname = $args{dbname} || 'postgres';
	my $libdir = $args{libdir};
	my $connstr = $args{connstr};
	unless ($setup_ok)
	{
		unless ($libdir)
		{
			croak "bad node" unless $node->isa("PostgreSQL::Test::Cluster");
			$libdir = $node->config_data('--libdir');
		}
		setup($libdir);
	}
	unless ($connstr)
	{
		croak "bad node" unless $node->isa("PostgreSQL::Test::Cluster");
		$connstr = $node->connstr($dbname);
	}
	$self->{connstr} = $connstr;
	$self->{conn} = PQconnectdb($connstr);
	# The destructor will clean up for us even if we fail
	return (PQstatus($self->{conn}) == CONNECTION_OK) ? $self : undef;
}

sub close
{
	my $self = shift;
	PQfinish($self->{conn});
	delete $self->{conn};
}

sub DESTROY
{
	my $self = shift;
	$self->close if exists $self->{conn};
}

sub reconnect
{
	my $self = shift;
	$self->close if exists $self->{conn};
	$self->{conn} = PQconnectdb($self->{connstr});
	return PQstatus($self->{conn});
}

sub conn_status
{
	my $self = shift;
	return exists $self->{conn} ? PQstatus($self->{conn}) : undef;
}

# run some sql which doesn't return tuples

sub do
{
	my $self = shift;
	my $conn = $self->{conn};
	my $status;
	foreach my $sql (@_)
	{
		my $result = PQexec($conn, $sql);
		$status = PQresultStatus($result);
		PQclear($result);
		return $status unless $status == PGRES_COMMAND_OK;
	}
	return $status;
}

sub do_async
{
	my $self = shift;
	my $conn = $self->{conn};
	my $sql = shift;
	my $result = PQsendQuery($conn, $sql);
	return $result; # 1 or 0
}

# set password for user
sub set_password
{
	my $self = shift;
	my $user = shift;
	my $password = shift;
	my $conn = $self->{conn};
	my $result = PQchangePassword($conn, $user, $password);
	my $ret = _get_result_data($result);
	PQclear($result);
	return $ret;
}

# get the next resultset from some aync commands
# wait if necessary
# c.f. libpqsrv_get_result
sub _get_result
{
	my $conn = shift;
	while (PQisBusy($conn))
	{
		usleep(100_000);
		last if PQconsumeInput($conn) == 0;
	}
	return PQgetResult($conn);
}

# wait for all the resultsets and clear them
# c.f. libpqsrv_get_result_last
sub wait_for_completion
{
	my $self = shift;
	my $conn = $self->{conn};
	while (my $res = _get_result($conn))
	{
		PQclear($res);
	}
}

# Run some sql that does return tuples
# Returns a hash with status, names, types and rows fields. names and types
# are arrays, rows is an array of arrays. If there is an error processing
# the query then result will also contain an error_message field, and names,
# types and rows will be empty.

sub _get_result_data
{
	my $result = shift;
	my $conn = shift;
	my $status = PQresultStatus($result);
	my $res = {	status => $status, names => [], types => [], rows => [],
			psqlout => ""};
	unless ($status == PGRES_TUPLES_OK || $status == PGRES_COMMAND_OK)
	{
		$res->{error_message} = PQerrorMessage($conn);
		return $res;
	}
	if ($status == PGRES_COMMAND_OK)
	{
		return $res;
	}
	my $ntuples = PQntuples($result);
	my $nfields = PQnfields($result);
	# assuming here that the strings returned by PQfname and PQgetvalue
	# are mapped into perl space using setsvpv or similar and thus won't
	# be affect by us calling PQclear on the result object.
	foreach my $field (0 .. $nfields-1)
	{
		push(@{$res->{names}}, PQfname($result, $field));
		push(@{$res->{types}}, PQftype($result, $field));
	}
	my @textrows;
	foreach my $nrow (0 .. $ntuples - 1)
	{
		my $row = [];
		foreach my $field ( 0 .. $nfields - 1)
		{
			my $val = PQgetvalue($result, $nrow, $field);
			if (($val // "") eq "")
			{
				$val = undef if PQgetisnull($result, $nrow, $field);
			}
			push(@$row, $val);
		}
		push(@{$res->{rows}}, $row);
		no warnings qw(uninitialized);
		push(@textrows, join('|', @$row));
	}
	$res->{psqlout} = join("\n",@textrows) if $ntuples;
	return $res;
}

sub query
{
	my $self = shift;
	my $sql = shift;
	my $conn = $self->{conn};
	my $result = PQexec($conn, $sql);
	my $res = _get_result_data($result, $conn);
	PQclear($result);
	return $res;
}

# Return a single value for a query. The query must return exactly one columns
# and exactly one row unless missing_ok is set, in which case it can also
# return zero rows. Any other case results in an error.
# If the result is NULL, or if missing_ok is set and there are zero rows,
# undef is returned. Otherwise the value from the query is returned.

sub query_oneval
{
	my $self = shift;
	my $sql = shift;
	my $missing_ok = shift; # default is not ok
	my $conn = $self->{conn};
	my $result = PQexec($conn, $sql);
	my $status = PQresultStatus($result);
	unless  ($status == PGRES_TUPLES_OK)
	{
		PQclear($result) if $result;
		croak PQerrorMessage($conn);
	}
	my $ntuples = PQntuples($result);
	return undef if ($missing_ok && !$ntuples);
	my $nfields = PQnfields($result);
	croak "$ntuples tuples != 1 or $nfields fields != 1"
	  if $ntuples != 1 || $nfields != 1;
	my $val = PQgetvalue($result, 0, 0);
	if ($val eq "")
	{
		$val = undef if PQgetisnull($result, 0, 0);
	}
	PQclear($result);
	return $val;
}

# return tuples like psql's -A -t mode.
# An empty resultset is represented by nothing, because that's the way psql does
# it, and putting out a line with '--empty' breaks at least one test.

sub query_tuples
{
	my $self = shift;
	my @results;
	foreach my $sql (@_)
	{
		my $res = $self->query($sql);
		croak $res->{error_message}
		  unless $res->{status} == PGRES_TUPLES_OK;
		my $rows = $res->{rows};
		unless (@$rows)
		{
			# push(@results,"-- empty");
			next;
		}
		# join will render undef as an empty string here
		no warnings qw(uninitialized);
		my @tuples = map { join('|', @$_); } @$rows;
		push(@results, join("\n",@tuples));
	}
	return join("\n",@results);
}


1;
