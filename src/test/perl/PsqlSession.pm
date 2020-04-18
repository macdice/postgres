=pod

=head1 NAME

PsqlSession - class representing psql connection

=head1 SYNOPSIS

  use PsqlSession;

  my $node = PostgresNode->get_new_node('mynode');
  my $session = PsqlSession->new($node, "dbname");

  # send simple query and wait for one line response
  my $result = $session->send("SELECT 42;", 1);

  # close connection
  $session->close();

=head1 DESCRIPTION

PsqlSession allows for tests of interleaved operations, similar to
isolation tests.

=cut

package PsqlSession;

use strict;
use warnings;

use PostgresNode;
use TestLib;
use IPC::Run qw(pump finish timer);

our @EXPORT = qw(
  new
  send
  close
);

=pod

=head1 METHODS

=over

=item PsqlSession::new($class, $node, $dbname)

Create a new PsqlSession instance, connected to a database.

=cut

sub new
{
	my ($class, $node, $dbname) = @_;
	my $timer = timer(5);
	my $stdin = '';
	my $stdout = '';
	my $harness = $node->interactive_psql($dbname, \$stdin, \$stdout, $timer);
	my $self = {
		_harness => $harness,
		_stdin => \$stdin,
		_stdout => \$stdout,
		_timer => $timer
	};
	bless $self, $class;
	return $self;
}

=pod

=item $session->send($input, $lines)

Send the given input to psql, and then wait for the given number of lines
of output, or a timeout.

=cut

sub count_lines
{
	my ($s) = @_;
	return $s =~ tr/\n//;
}

sub send
{
	my ($self, $statement, $lines) = @_;
	${$self->{_stdout}} = '';
	${$self->{_stdin}} .= $statement;
	$self->{_timer}->start(5);
	pump $self->{_harness} until count_lines(${$self->{_stdout}}) == $lines || $self->{_timer}->is_expired;
	die "expected ${lines} lines but after timeout, received only: ${$self->{_stdout}}" if $self->{_timer}->is_expired;
	my @result = split /\n/, ${$self->{_stdout}};
	chop(@result);
	return @result;
}

=pod

=item $session->close()

Close a PsqlSession connection.

=cut

sub close
{
	my ($self) = @_;
	$self->{_timer}->start(5);
	${$self->{_stdin}} .= "\\q\n";
	finish $self->{_harness} or die "psql returned $?";
	$self->{_timer}->reset;
}

=pod

=back

=cut

1;
