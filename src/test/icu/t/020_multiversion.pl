# Copyright (c) 2022, PostgreSQL Global Development Group
#
# If one or more extra ICU versions is installed in the standard system library
# search path, this test will detect them and run.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{with_icu} ne 'yes')
{
	plan skip_all => 'ICU not supported by this build';
}

my $node1 = PostgreSQL::Test::Cluster->new('node1');
$node1->init;
$node1->start;

# Check which ICU versions are installed.
my $highest_version = $node1->safe_psql('postgres', 'select max(icu_version::decimal) from pg_icu_library_versions()');
my $lowest_version = $node1->safe_psql('postgres', 'select min(icu_version::decimal) from pg_icu_library_versions()');
my $highest_major_version = int($highest_version);
my $lowest_major_version = int($lowest_version);

if ($highest_major_version == $lowest_major_version)
{
	$node1->stop;
	plan skip_all => 'no extra ICU library versions found';
}

sub set_default_icu_library_version
{
	my $icu_version = shift;
	$node1->safe_psql('postgres', "alter system set default_icu_library_version = '$icu_version'; select pg_reload_conf()");
}

sub set_icu_library_versions
{
	my $icu_versions = shift;
	$node1->safe_psql('postgres', "alter system set icu_library_versions = '$icu_versions'");
	$node1->restart;
}

my $ret;
my $stderr;

# === DATABASE objects ===

# ===== Scenario 1: user creates database with all default settings

$node1->safe_psql('postgres', "create database db2 locale_provider = icu template = template0 icu_locale = 'en'");

# No warning when logging into this database.
$ret = $node1->psql('db2', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# ===== Scenario 2: user wants to use an old library

# Create a database using the older library by changing the default.  This
# might be done for compatibility with some other system, but it also simulates
# a database that was created with all default settings when the binary was
# linked against the older version.
set_default_icu_library_version($lowest_major_version);
$node1->safe_psql('postgres', "create database db3 locale_provider = icu template = template0 icu_locale = 'en'");

isnt($node1->safe_psql('postgres', "select datcollversion from pg_database where datname = 'db2'"),
     $node1->safe_psql('postgres', "select datcollversion from pg_database where datname = 'db3'"),
     'db2 and db3 should have different datcollversion');

# ===== Scenario 3: user has the old library avaliable, is happy to keep using it

# Unset the default ICU library version (meaning use the linked version for
# newly created databases).  No warning, because we can still find that older
# version via dlopen().  User can happily go on using that old version in this
# database for the rest of time.
set_default_icu_library_version("");
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# ===== Scenario 4: user doesn't have the old library, installs after warnings

# Hide the old library version.  This simulates a system that doesn't have that
# version installed yet, by making it unavailable.  We get a warning.
set_icu_library_versions("$highest_major_version");
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING:  database "db3" has a collation version mismatch/, "warning for incorrect datcollversion");
like($stderr, qr/HINT:  Install a version of ICU that provides/, "warning suggests installing another ICU version");

# Make the old version available again, this time explicitly (whereas before it
# worked becuase the default is * which would find it automatically).
set_icu_library_versions("$lowest_major_version,$highest_major_version");
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# It also works if you use a major.minor version explicitly.
set_icu_library_versions("$lowest_version,$highest_major_version");
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# Or *, the default value that we started with.
set_icu_library_versions("*");
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# ===== Scenario 5: user doesn't have the old library, rebuilds after warnings

# Hide the old library version again, and we get the warning again.
set_icu_library_versions("$highest_major_version");
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING:  database "db3" has a collation version mismatch/, "warning for incorrect datcollversion");
like($stderr, qr/HINT:  Install a version of ICU that provides/, "warning suggests installing another ICU version");

# If we don't want to install a new library, we have the option of clobbering
# the version.  It's the administrator's job to rebuild any database objects
# that depend on the collation (most interestingly indexes) before doing so.
# In this scenario, the REFRESH command can be run before or *after* rebuilding
# indexes, because either way we're already using the default ICU library (due
# to failure to find the named version).
$ret = $node1->psql('postgres', "alter database db3 refresh collation version", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/NOTICE:  changing version/, "version changes");

# Now no warning.
$ret = $node1->psql('db3', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning after refresh");

# ===== Scenario 6: user has the old library, but eventually decides to rebuild/upgrade

# Make a new database with the old version active
set_default_icu_library_version($lowest_major_version);
$node1->safe_psql('postgres', "create database db4 locale_provider = icu template = template0 icu_locale = 'en'");

# No warning, it just load the old version.
$ret = $node1->psql('db4', "select", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning after refresh");
my $old_datcollversion = $node1->safe_psql('postgres', "select datcollversion from pg_database where datname = 'db4'");

# The user would now like to upgrade to the new library.  Presumably people
# will want to do this eventually to avoid running very old unmaintained copies
# of ICU.  Unlike scenario 3, here it's actually a requirement to REFRESH
# *before* doing all the rebuilds of indexes etc, which may be a little
# confusing (not shown here).  REFRESH is necessary to change datcollversion,
# which is required to make us start opening the newer library.
#
# XXX Currently you also need to reconnect all sessions too, because the
# default locale is cached and now out of date.
set_default_icu_library_version("");
$ret = $node1->psql('postgres', "alter database db4 refresh collation version", stderr => \$stderr);
my $new_datcollversion = $node1->safe_psql('postgres', "select datcollversion from pg_database where datname = 'db4'");

isnt($old_datcollversion, $new_datcollversion, "datcollversion changed");


# === COLLATION objects ===

# The same scenarios, this time with COLLATIONs.

# ===== Scenario 1: user creates database with all default settings

set_default_icu_library_version("");
set_icu_library_versions("*");
$node1->safe_psql('postgres', "create collation c1 (provider = icu, locale = 'en')");

# No warning when using it.
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c1", stderr => \$stderr);
is($ret, 0, "can use collation");
unlike($stderr, qr/WARNING/, "no warning for default");

# ===== Scenario 2: user wants to use an old library

# Simulates a collation in a database that migrated from an older binary, or a
# collation set up explicitly to match some other system.
set_default_icu_library_version($lowest_major_version);
$node1->safe_psql('postgres', "create collation c2 (provider = icu, locale = 'en')");

isnt($node1->safe_psql('postgres', "select collversion from pg_collation where collname = 'c1'"),
     $node1->safe_psql('postgres', "select collversion from pg_collation where collname = 'c2'"),
     'c1 and c2 should have different collversion');

# ===== Scenario 3: user has the old library avaliable, is happy to keep using it

$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "can use collation");
unlike($stderr, qr/WARNING/, "no warning when using old library collation");

# ===== Scenario 4: user doesn't have the old library, installs after warnings

# Hide the old library version.
set_default_icu_library_version("");
set_icu_library_versions("$highest_major_version");
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING:  collation "c2" version mismatch/, "warning for incorrect collversion");
like($stderr, qr/HINT:  Install a version of ICU that provides/, "warning suggests installing another ICU version");

# Make the old version available again.
set_icu_library_versions("$lowest_major_version,$highest_major_version");
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# It also works if you use a major.minor version explicitly.
set_icu_library_versions("$lowest_version,$highest_major_version");
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# Or *, the default value that we started with.
set_icu_library_versions("*");
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# ===== Scenario 5: user doesn't have the old library, rebuilds after warnings

# Hide the old library version again.
set_icu_library_versions("$highest_major_version");
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/WARNING:  collation "c2" version mismatch/, "warning for incorrect collversion");
like($stderr, qr/HINT:  Install a version of ICU that provides/, "warning suggests installing another ICU version");

# Rebuild things, and refresh.
$ret = $node1->psql('postgres', "alter collation c2 refresh version", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/NOTICE:  changing version/, "version changes");

# Now no warning.
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c2", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

# ===== Scenario 6: user has the old library, but eventually decides to rebuild/upgrade

set_default_icu_library_version($lowest_major_version);
$node1->safe_psql('postgres', "create collation c3 (provider = icu, locale = 'en')");

# No warning.
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c3", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

my $old_collversion = $node1->safe_psql('postgres', "select collversion from pg_collation where collname = 'c3'");

# Rebuild things, and refresh.  As with database scenario 6, we need to refresh
# *before* rebuilding dependent objects (not shown here).
set_default_icu_library_version("");
$ret = $node1->psql('postgres', "alter collation c3 refresh version", stderr => \$stderr);
is($ret, 0, "success");
like($stderr, qr/NOTICE:  changing version/, "version changes");

# No warning.
$ret = $node1->psql('postgres', "select 'x' < 'y' collate c3", stderr => \$stderr);
is($ret, 0, "success");
unlike($stderr, qr/WARNING/, "no warning");

my $new_collversion = $node1->safe_psql('postgres', "select collversion from pg_collation where collname = 'c3'");

isnt($old_collversion, $new_collversion, "collversion changed");

$node1->stop;

done_testing();
