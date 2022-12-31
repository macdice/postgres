
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

my $radiusd_dir = "${PostgreSQL::Test::Utils::tmp_check}/radiusd_data";
my $radiusd_conf = "radiusd.conf";
my $radiusd_users = "users.txt";
my $radiusd_prefix;
my $radiusd;

if ($ENV{PG_TEST_EXTRA} !~ /\bradius\b/)
{
	plan skip_all => 'radius not enabled in PG_TEST_EXTRA';
}
elsif ($^O eq 'freebsd')
{
	$radiusd = '/usr/local/sbin/radiusd';
}
elsif ($^O eq 'linux' && -f '/usr/sbin/freeradius')
{
	$radiusd = '/usr/sbin/freeradius';
}
elsif ($^O eq 'linux')
{
	$radiusd = '/usr/sbin/radiusd';
}
elsif ($^O eq 'darwin' && -d '/opt/local')
{
	# typical path for MacPorts
	$radiusd = '/opt/local/sbin/radiusd';
	$radiusd_prefix = '/opt/local';
}
elsif ($^O eq 'darwin' && -d '/opt/homebrew')
{
	# typical path for Homebrew on ARM
	$radiusd = '/opt/homebrew/bin/radiusd';
	$radiusd_prefix = '/opt/homebrew';
}
elsif ($^O eq 'darwin' && -d '/usr/local')
{
	# typical path for Homebrew on Intel
	$radiusd = '/usr/local/bin/radiusd';
	$radiusd_prefix = '/usr/local';
}

if (!defined($radiusd) || ! -f $radiusd)
{
	plan skip_all =>
	  "radius tests not supported on $^O or dependencies not installed";
}

sub make_random_string
{
	my $length = 8 + int(rand(16));
	my $s = "";
	$s .= chr(ord("A") + rand(26)) for 1..$length;
	return $s;
}

my $shared_secret = make_random_string();
my $password = make_random_string();

note "setting up radiusd";

my $radius_port = PostgreSQL::Test::Cluster::get_free_port();

mkdir $radiusd_dir or die "cannot create $radiusd_dir";

append_to_file("$radiusd_dir/$radiusd_users",
	qq{test2 Cleartext-Password := "$password"});

my $conf = qq{
client default {
  ipaddr = "127.0.0.1"
  secret = "$shared_secret"
}

modules {
  files {
    filename = "$radiusd_dir/users.txt"
  }
  pap {
  }
}

server default {
  listen {
    type   = "auth"
    ipv4addr = "127.0.0.1"
    port = "$radius_port"
  }
  authenticate {
    Auth-Type PAP {
      pap
    }
  }
  authorize {
    files
    pap
  }
}

log {
  destination = "files"
  localstatedir = "$radiusd_dir"
  logdir = "$radiusd_dir"
  file = "$radiusd_dir/radius.log"
}

pidfile = "$radiusd_dir/radiusd.pid"
};

if ($radiusd_prefix)
{
	$conf .= "prefix=\"$radiusd_prefix\"\n";
}

append_to_file("$radiusd_dir/$radiusd_conf", $conf);

system_or_bail $radiusd, '-xx', '-d', $radiusd_dir;

END
{
	kill 'INT', `cat $radiusd_dir/radiusd.pid`
	  if -f "$radiusd_dir/radiusd.pid";
}

note "setting up PostgreSQL instance";

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->start;

$node->safe_psql('postgres', 'CREATE USER test1;');
$node->safe_psql('postgres', 'CREATE USER test2;');

unlink($node->data_dir . '/pg_hba.conf');
$node->append_conf(
	'pg_hba.conf',
	qq{
local all test2 radius radiusservers="127.0.0.1" radiussecrets="$shared_secret" radiusports="$radius_port"
}
);
$node->restart;

note "running tests";

sub test_access
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $role, $expected_res, $test_name, %params) = @_;
	my $connstr = "user=$role";

	if ($expected_res eq 0)
	{
		$node->connect_ok($connstr, $test_name, %params);
	}
	else
	{
		# No checks of the error message, only the status code.
		$node->connect_fails($connstr, $test_name, %params);
	}
}

$ENV{"PGPASSWORD"} = 'wrong';
test_access(
	$node, 'test1', 2,
	'authentication fails if user not found in RADIUS',
	log_unlike => [qr/connection authenticated:/]);
test_access(
	$node, 'test2', 2,
	'authentication fails with wrong password',
	log_unlike => [qr/connection authenticated:/]);

$ENV{"PGPASSWORD"} = $password;
test_access($node, 'test2', 0, 'authentication succeeds with right password',
	log_like =>
	  [qr/connection authenticated: identity="test2" method=radius/],);

done_testing();
