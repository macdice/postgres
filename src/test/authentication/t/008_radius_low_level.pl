
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;
use IO::Socket  qw(AF_UNIX SOCK_STREAM IPPROTO_TCP);
use Digest::MD5 qw(md5);

sub make_random_string
{
	my $s = "";
	$s .= chr(ord("A") + rand(26)) for 1..16;
	return $s;
}

# Functions for connecting to PostgreSQL with a raw socket.

sub begin_password_auth
{
	my ($node, $database, $user, $password) = @_;

	my $sock = IO::Socket->new(
		Domain => AF_UNIX,
		Type => SOCK_STREAM,
		Proto => IPPROTO_TCP,		
		Peer => $node->host . "/.s.PGSQL." . $node->port,
		Blocking => 1) or die "cannot connect to server";

	my $startup_message = pack("NZ*Z*Z*Z*x",
							   0x030000, # protocol 3.0
							   "user", $user,
							   "database", $database);
	$sock->send(pack("Na*",
					 length($startup_message) + 4,
					 $startup_message));

	# XXX should poll() on a nonblocking socket so we can time out
	my $response;
	$sock->recv($response, 8192);
	
	$response eq pack("aNN", 'R', 8, 3)
	  or die "expected AuthenticationCleartextPassword (R) message, but got: $response";

	my $password_message = pack("Z*", $password);
	$sock->send(pack("aNa*",
					 "p", # PasswordMessage
					 length($password_message) + 5,
					 $password_message));

	return $sock;
}

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->start;

$node->safe_psql('postgres', 'CREATE USER test1;');
$node->safe_psql('postgres', 'CREATE USER test2;');

my $shared_secret = make_random_string();
my $password = make_random_string();
my $radius_port = PostgreSQL::Test::Cluster::get_free_port();

unlink($node->data_dir . '/pg_hba.conf');
$node->append_conf(
	'pg_hba.conf',
	qq{
local all test2 radius radiusservers="127.0.0.1" radiussecrets="$shared_secret" radiusports="$radius_port"
}
);
$node->restart;

my $client_sock = begin_password_auth($node, 'postgres', 'test2', 'foo');
#sleep(5);
done_testing();
