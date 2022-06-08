# Copyright (c) 2022, PostgreSQL Global Development Group

# This test requires a second major version of ICU installed in the usual
# system library search path.  That is, not the one PostgreSQL was linked
# against.  It also assumes that ucol_getVersion() for locale "en" will change
# between the two library versions.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{with_icu} ne 'yes')
{
	plan skip_all => 'ICU not supported by this build';
}

if (!($ENV{PG_TEST_EXTRA} =~ /\bicu=([0-9]+)\b/))
{
	plan skip_all => 'PG_TEST_EXTRA not configured to test an alternative ICU library version';
}
my $alt_major_version = $1;

my $node1 = PostgreSQL::Test::Cluster->new('node1');
$node1->init;
$node1->start;

my $linked_major_version = $node1->safe_psql('postgres', 'select pg_icu_library_version(-1)::decimal::int');

print "linked_major_version = $linked_major_version\n";
print "alt_major_version = $alt_major_version\n";

if ($alt_major_version ge $linked_major_version)
{
	BAIL_OUT("can't run multi-version tests because ICU major version selected via PG_TEST_EXTRA is not lower than the major version the executable is linked against ($linked_major_version)");
}

# Sanity check that when we load a library, its u_getVersion() function tells
# us it has the major version we expect.  The result is a string eg "71.1", so
# we get the major part by casting.
is($node1->safe_psql('postgres', "select pg_icu_library_version($alt_major_version)::decimal::int"),
	$alt_major_version,
	"alt library reports expected major version");

sub set_default_icu_library_version
{
	my $major_version = shift;
	$node1->safe_psql('postgres', "alter system set default_icu_library_version = $major_version; select pg_reload_conf()");
}

my $ret;
my $stderr;

# Create a collation that doesn't specify the ICU version to use.  Which
# library we load depends on the GUC default_icu_library_version.  Here it uses
# the linked version because it's set to 0 (default value in a new cluster).
set_default_icu_library_version(0);
$node1->safe_psql('postgres', "create collation c1 (provider=icu, locale='en')");

# No warning by default.
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c1", stderr => \$stderr);
is($ret, 0, "can use collation");
unlike($stderr, qr/WARNING/, "no warning for default");

# No warning if we explicitly select the linked version.
set_default_icu_library_version($linked_major_version);
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c1", stderr => \$stderr);
unlike($stderr, qr/WARNING/, "no warning for explicit match");

# If we use a different major version explicitly, we get a warning that
# includes a hint that we might be able to install and select a different ICU
# version.
set_default_icu_library_version($alt_major_version);
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c1", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING/, "warning for incorrect major version");
like($stderr, qr/HINT:  Install another version of ICU/, "warning suggests installing another ICU version");

# Create a collation using the alt version without specifying it explicitly.
# This simulates a collation that was created by a different build linked
# against an older ICU.
$node1->safe_psql('postgres', "create collation c2 (provider=icu, locale='en')");

# Warning if we try to use it with default setttings.
set_default_icu_library_version(0);
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING/, "warning for incorrect major version");
like($stderr, qr/HINT:  Install another version of ICU/, "warning suggests installing another ICU version");

# No warning if we explicitly activate the alt version.
set_default_icu_library_version($alt_major_version);
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning for explicit match");

# Refresh the version...  this will update it from the linked version (or
# whatever default_icu_library_version points to, here it's 0 and thus the
# linked version), because c2 is not explicitly pinned to an ICU major version.
set_default_icu_library_version(0);
$ret = $node1->psql('postgres', "alter collation c2 refresh version", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/NOTICE:  changing version/, "version changes");

# Now no warning.
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "warning has gone away after refresh");

# Create a collation that is pinned to a specific version of ICU.
$node1->safe_psql('postgres', "create collation c3 (provider=icu, locale='$alt_major_version:en')");

# No warnings expected, no matter what default_icu_library_version says, because
# we always load that exact library.
set_default_icu_library_version($linked_major_version);
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c3", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning for explicit lib");
set_default_icu_library_version($alt_major_version);
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c3", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning for explicit lib");
set_default_icu_library_version(0);
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c3", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning for explicit lib");

# Similar tests using the database default.

set_default_icu_library_version(0);
$node1->safe_psql('postgres', "create database db2 locale_provider = icu template = template0 icu_locale = 'en'");

# No warning.
$ret = $node1->psql('db2', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# Warning when you log into the database.
set_default_icu_library_version($alt_major_version);
$ret = $node1->psql('db2', "select", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING/, "warning for incorrect major version");
like($stderr, qr/HINT:  Install another version of ICU/, "warning suggests installing another ICU version");

# One way to clear the warning is to REFRESH.
$ret = $node1->psql('postgres', "alter database db2 refresh collation version", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/NOTICE:  changing version/, "version changes");

# Now the warning is gone.
$ret = $node1->psql('db2', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# Now we go back to using the linked version, and we'll see the warning again.
# Perhaps this case simulates the most likely real-world experience, when
# moving to a new OS that has PostgreSQL packages linked against a later ICU
# version, using all defaults.
set_default_icu_library_version(0);
$ret = $node1->psql('db2', "select", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING/, "warning for incorrect major version");
like($stderr, qr/HINT:  Install another version of ICU/, "warning suggests installing another ICU version");

# Option 1 is to get rid of the warning by installing the library and setting
# the GUC.
set_default_icu_library_version($alt_major_version);
$ret = $node1->psql('db2', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning after setting GUC");

# Option 2 is to rebuild indexes etc and use REFRESH.
set_default_icu_library_version(0);
$ret = $node1->psql('postgres', "alter database db2 refresh collation version", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/NOTICE:  changing version/, "version changes");
$ret = $node1->psql('db2', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning after refresh");

# None of this applies if you explicitly pinned your database to an specific
# ICU major version in the first place, so we ignore the GUC.
set_default_icu_library_version(0);
$node1->safe_psql('postgres', "create database db3 locale_provider = icu template = template0 icu_locale = '$alt_major_version:en'");

# No warning with all GUC settings.
set_default_icu_library_version($alt_major_version);
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning with pinned library version");
set_default_icu_library_version($linked_major_version);
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning with pinned library version");
set_default_icu_library_version(0);
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning with pinned library version");

$node1->stop;

done_testing();
