#!/bin/sh
#
# Run the test suite or selected executables under valgrind.  Example that runs
# memcheck on backend code (-b):
#
#   ../src/tools/valgrind.sh -b meson test
#
# The defaults can be overridden or supplemented with the usual valgrind
# options:
#
#   valgrind.sh -b --leak-check=full --verbose make check
#
# Different executables can be selected with special options, but most options
# are passed straight through to valgrind.  Note that PostgreSQL should be
# compiled with the macro USE_VALGRIND defined for full effect.
#
# XXX For now you can only select postgres and initdb, but it would be good to
# support frontend programs, ecpg tests, etc, which probably requires teaching
# Cluster.pm to understand more PG_TEST_WRAP_XXX variables first.

set -e

show_usage_and_die()
{
cat >&2 <<EOF
Usage: $0 [options...] command ...

Only a few options are interpreted by the script itself, beginning with the
prefix --pg- to avoid confusion with valgrind's own options.  Since valgrind
doesn't use short options, we also use those for convenience.  Those options
are:

  -e=name|--pg-executable=name
     A class of executable that should be run under valgrind.  May be given
     multiple times.  It will be communicated to the test harness scripts
     through environment variables to tell them to wrap qualifying executables
     with another invocation of this script.  If this option is not given then
     the whole command is run under valgrind at the top level (not
     recommended due to slowness).  Recognized executable names:

       postgres -- postgres server (but not when run under initdb)
       initdb   -- initdb and single-user postgres subprocess

  -b|--pg-backend
     Short for -e=postgres -e=initdb.  initdb isn't really a "backend" but it
     is included because it also runs postgres and only test-harness code
     understands the PG_TEST_WRAP_POSTGRES environment variable.

  --pg-no-quiet
     Don't pass --quiet to valgrind (also implied by --verbose etc).

  -n|--pg-dry-run
     Don't run anything, just display what would be done.

All other options are passed directly through to valgrind, though note that
special shell characters and spaces are unlikely to work correctly in the
current implementation.

The following options are passed to valgrind automatically, unless conflicting
options are provided explicitly:

  --log-file=/path/to/valgrind-logs/%p.log
  --supressions=/path/to/src/tools/valgrind.supp
  --time-stamp=yes
  --trace-children=yes
  --error-exitcode=128
  --quiet

If this implicit default --log-file option is used, the directory
"valgrind-logs" is created in the current working directory.  If this implicit
default --suppressions is used, "valgrind.supp" is expected to be located in
the same directory as this script.

The script may be invoked via relative or absolute path directly in the source
tree, or installed in a new location, but in that last case the valgrind.supp
file should be placed in the same directory, or --suppressions=... should be
provided explicitly.

Examples of use:

  valgrind.sh -e=postgres meson test
  valgrind.sh -b meson test
  valgrind.sh -b --leak-check=full meson test
  valgrind.sh -b --exit-on-first-error=yes make world-check
EOF
  exit 1
}

if [ "$PG_VALGRIND_OPTIONS" = "" ] ; then
  # At top level, being invoked by a user.  Scan command line switches.
  self_abs_path="$(dirname "$(readlink -f "$0")")/$(basename "$0")"
  while [ $# -ge 1 ] ; do
    case "$1" in
      --pg-executable=*|-e=*)
        no_toplevel_valgrind=1
        name="$(echo "$1" | sed 's/^[^=]*=//')"
        case "$name" in
          postgres) export PG_TEST_WRAP_POSTGRES="$self_abs_path" ;;
          initdb)   export PG_TEST_WRAP_INITDB="$self_abs_path" ;;
          *)        echo "Unrecognized executable name: $name" >&2
                    exit 1
        esac
        shift
        ;;
      --pg-backend|-b)
        no_toplevel_valgrind=1
        export PG_TEST_WRAP_POSTGRES="$self_abs_path"
        export PG_TEST_WRAP_INITDB="$self_abs_path"
        shift
        ;;
      --pg-no-quiet)
        no_quiet=1
        shift
        ;;
      --pg-dry-run|-n)
        no_run=1
        shift
        ;;
      --pg-*)
        show_usage_and_die
        ;;
      -*)
        # Pass everything else through to valgrind.
        valgrind_options="$valgrind_options $1"
        # Remember to suppress conflicting defaults if we see any of these.
        case $1 in
          --log-file=*)            no_log_file=1 ;;
          --error-exitcode=*)      no_error_exitcode=1 ;;
          --verbose|-v|--quiet|-q) no_quiet=1 ;;
          --suppressions=*)        no_suppressions=1 ;;
          --trace-children=*)      no_trace_children=1 ;;
          --time-stamp=*)          no_time_stamp=1 ;;
        esac
        shift
        ;;
    esac
  done

  # No command given?  Show help rather than valgrind's error.
  if [ $# -eq 0 ] ; then
    show_usage_and_die
  fi

  # Add defaults that haven't been suppressed by an explicit option.
  if [ "$no_suppressions" = "" ] ; then
    valgrind_options="$valgrind_options --suppressions=$(dirname "$self_abs_path")/valgrind.supp"
  fi
  if [ "$no_log_file" = "" ] ; then
    current_directory="$(pwd)"
    if [ "$no_run" = "" ] ; then
      mkdir -p "$current_directory/valgrind-logs"
    fi
    valgrind_options="$valgrind_options --log-file=$current_directory/valgrind-logs/%p.log"
  fi
  if [ "$no_trace_children" = "" ] ; then
    valgrind_options="$valgrind_options --trace-children=yes"
  fi
  if [ "$no_time_stamp" = "" ] ; then
    valgrind_options="$valgrind_options --time-stamp=yes"
  fi
  if [ "$no_error_exitcode" = "" ] ; then
    valgrind_options="$valgrind_options --error-exitcode=128"
  fi
  if [ "$no_quiet" = "" ] ; then
    valgrind_options="$valgrind_options --quiet"
  fi
  PG_VALGRIND_OPTIONS="$valgrind_options"
  if [ "$no_run" != "" ] ; then
    if [ "$no_toplevel_valgrind" != "" ] ; then
      set | grep '^PG_(TEST_WRAP_[^=]*|VALGRIND_OPTIONS)=' >&2
      echo "Would run: $@"
    else
      echo "Would run: valgrind $PG_VALGRIND_OPTIONS $@"
    fi
    exit 0
  fi
fi

# If asked to trace named executables then run the top-level command normally.
# This script will be invoked again via PG_TEST_WRAP_XXX to run valgrind for
# specific programs, and will receive PG_VALGRIND_OPTIONS from this script.
if [ "$no_toplevel_valgrind" != "" ] ; then
  export PG_VALGRIND_OPTIONS
  exec $@
fi

# Either running valgrind on the whole command at top level, or being invoked
# by Cluster.pm or pg_regress.c with a PG_VALGRIND_OPTIONS exported by a
# top-level valgrind.sh.
exec valgrind $PG_VALGRIND_OPTIONS $@
