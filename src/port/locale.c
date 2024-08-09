/*-------------------------------------------------------------------------
 *
 * locale.c
 *		Helper routines for thread-safe system locale usage.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/locale.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#ifndef WIN32
#include <pthread.h>
#else
#include <synchapi.h>
#endif

/* A process-lifetime singleton, allocated on first need. */
static locale_t c_locale;

#ifndef WIN32
static void
init_c_locale_once(void)
{
	c_locale = newlocale(LC_ALL, "C", NULL);
}
#else
static BOOL
init_c_locale_once(PINIT_ONCE once, PVOID parameter, PVOID *context)
{
	c_locale = _create_locale(LC_ALL, "C");
	return true;
}
#endif

/*
 * Access a process-lifetime singleton locale_t object.  Use the macro
 * PG_C_LOCALE instead of calling this directly, as it can skip the function
 * call on some systems.
 */
locale_t
pg_get_c_locale(void)
{
	/*
	 * Fast path if already initialized.  This assumes that we can read a
	 * locale_t (in practice, a pointer) without tearing in a multi-threaded
	 * program.
	 */
	if (c_locale != (locale_t) 0)
		return c_locale;

	/* Make a locale_t.  It will live until process exit. */
	{
#ifndef WIN32
		static pthread_once_t once = PTHREAD_ONCE_INIT;

		pthread_once(&once, init_c_locale_once);
#else
		static INIT_ONCE once;
		InitOnceExecuteOnce(&once, init_c_locale_once, NULL, NULL);
#endif
	}

	/*
	 * It's possible that the allocation of the locale failed due to low
	 * memory, and then (locale_t) 0 will be returned.  Users of PG_C_LOCALE
	 * should defend against that by checking pg_ensure_c_locale() at a
	 * convenient time, so that they can treat it as a simple constant after
	 * that.
	 *
	 * Note that macOS, NetBSD and FreeBSD don't reach this code at all (see
	 * src/include/port.h, PG_C_LOCALE really is a constant).  Glibc, Musl and
	 * Solaris (but not illumos) appear to have special cases for the "C"
	 * locale that always return the same static object and thus cannot fail,
	 * but that's not documented and we don't rely on it.  Other supported
	 * systems might actually report failure in pg_ensure_c_locale() on low
	 * memory, so users of PG_C_LOCALE must find a good place to check that.
	 */

	return c_locale;
}
