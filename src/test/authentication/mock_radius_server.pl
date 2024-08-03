#!/bin/env perl
#
# A mock RADIUS server, to test authentication when FreeRADIUS isn't available
# (for example on Windows).

use strict;
use warnings FATAL => 'all';
use IO::Socket  qw(AF_INET SOCK_DGRAM IPPROTO_UDP SOL_SOCKET SO_REUSEADDR);
use Digest::MD5 qw(md5);

my $port = $ARGV[0];

# Only requests for this user/password will be accepted
my $shared_secret = "secret";
my $user_name = "test2";
my $user_password = "secret2";
my $nas_identifier = "postgresql";

print "starting mock RADIUS server on port $port\n";
my $sock = IO::Socket->new(
	Domain => AF_INET,
	LocalPort => $port,
	Type => SOCK_DGRAM,
	Proto => IPPROTO_UDP,
	Blocking => 1) or die "cannot create UDP socket on port $port";
$sock->setsockopt(SOL_SOCKET, SO_REUSEADDR, 1);

my $buffer = "";
while (my $peer = $sock->recv($buffer, 8192, 0))
{
	# Parse Access-Request header
	# https://datatracker.ietf.org/doc/html/rfc2865#section-4.1
	if (length($buffer) < 20)
	{
		print "error: header too small for Access-Request\n";
		next;
	}
	my $header = substr $buffer, 0, 20;
	my ($code, $identifier, $length, $req_authenticator) =
	  unpack("CCna[16]", $header);
	if ($code != 1)
	{
		print "error: expected Code = 1 (Access-Request), got $code\n";
		next;
	}
	if ($length != length($buffer))
	{
		print "error: header said there were $length bytes, but datagram has "
		  . length($buffer)
		  . " bytes\n";
		next;
	}

	# Now parse the attributes that follow the header
	# https://datatracker.ietf.org/doc/html/rfc2865#section-5
	my $attributes = substr $buffer, length($header);
	my $req_service_type;
	my $req_user_name;
	my $req_user_password;
	my $req_nas_identifier;
	while (length($attributes) >= 2)
	{
		my ($type, $length) = unpack("CC", substr $attributes, 0, 2);
		if (length($attributes) < $length)
		{
			print
			  "error: attribute type $type declares length $length but there are only "
			  . length($attributes)
			  . " bytes remaining in datagram\n";
			last;
		}
		my $value = substr $attributes, 2, $length - 2;

		if ($type == 1)
		{
			$req_user_name = $value;
		}
		elsif ($type == 2)
		{
			$req_user_password = $value;
		}
		elsif ($type == 6)
		{
			$req_service_type = unpack("N", $value);
		}
		elsif ($type == 32)
		{
			$req_nas_identifier = $value;
		}

		# advance to next attribute
		$attributes = substr $attributes, $length;
	}
	if (length($attributes) > 0)
	{
		print "error: trailing data after attributes\n";
		next;
	}

	# The request was well formed.  Time to reply.
	print "Received Access-Request from user $req_user_name\n";

	# Compute the expected encoded password
	# https://datatracker.ietf.org/doc/html/rfc2865#section-5.2
	my $digest = md5($shared_secret, $req_authenticator);
	my $padded_password =
	  $user_password . ("\000" x (16 - length($user_password)));
	my $encoded_password = "";
	for (my $i = 0; $i < 16; $i++)
	{
		$encoded_password .= chr(
			ord(substr($digest, $i, 1)) ^
			  ord(substr($padded_password, $i, 1)));
	}

	if (   $req_nas_identifier eq $nas_identifier
		&& $req_user_name eq $user_name
		&& $req_user_password eq $encoded_password
		&& $req_service_type == 8)
	{
		# Send Access-Accept
		# https://datatracker.ietf.org/doc/html/rfc2865#section-4.2
		print "Sending Access-Accept\n";
		my $response = pack("CCn", 2, $identifier, 20);
		$response .= md5($response, $req_authenticator, $shared_secret);
		$sock->send($response, 0, $peer);
	}
	else
	{
		# Send Access-Reject
		# https://datatracker.ietf.org/doc/html/rfc2865#section-4.3
		print "Sending Access-Reject\n";
		my $response = pack("CCn", 3, $identifier, 20);
		$response .= md5($response, $req_authenticator, $shared_secret);
		$sock->send($response, 0, $peer);
	}
}
