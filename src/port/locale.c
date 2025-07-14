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

#ifdef PG_C_LOCALE_ALLOCATE
#ifdef FRONTEND
#ifndef WIN32
#include <pthread.h>
#else
#include <synchapi.h>
#endif
#endif
#endif

/* Assertion support. */
bool		pg_ensure_c_locale_called;

#ifdef PG_C_LOCALE_ALLOCATE
locale_t	pg_c_locale;

#ifndef WIN32
static void
pg_c_locale_once(void)
{
	pg_c_locale = newlocale(LC_ALL, "C", NULL);
}
#else
static BOOL
pg_c_locale_once(PINIT_ONCE once, PVOID parameter, PVOID *context)
{
	pg_c_locale = _create_locale(LC_ALL, "C");
	return true;
}
#endif
#endif

/*
 * Initialize pg_c_locale if required on this platform.  This should be called
 * at a convenient time in program/library initialization before any use of the
 * macro PG_C_LOCALE.  If it returns false, the caller should report an
 * out-of-memory error.
 *
 * Does nothing at all on systems where PG_C_LOCALE is a constant (macOS,
 * NetBSD, FreeBSD).  On some other libc implementations it cannot fail due to
 * implementation of the "C" locale as a static object in libc (Glibc, Musl,
 * Solaris), though we don't count on that.  On other systems it might fail due
 * to low memory, and if pg_ensure_c_locale() hasn't been called then attempts
 * to pass PG_C_LOCALE to libc functions will crash.
 *
 * On all platforms, PG_C_LOCALE contains an assertion that this function has
 * been called.
 */
bool
pg_ensure_c_locale(void)
{
#ifdef PG_C_LOCALE_ALLOCATE
#ifdef FRONTEND
#ifndef WIN32
	static pthread_once_t once = PTHREAD_ONCE_INIT;
	pthread_once(&once, pg_c_locale_once);
#else
	static INIT_ONCE once;
	InitOnceExecuteOnce(&once, pg_c_locale_once, NULL, NULL);
#endif
#else /* !FRONTEND */
	if (pg_c_locale == 0)
	{
#ifndef WIN32
		pg_c_locale_once();
#else
		pg_c_locale_once(0, 0, 0);
#endif
	}
#endif

	if (pg_c_locale != 0)
		pg_ensure_c_locale_called = true;
	return pg_c_locale != 0;
#else /* !PG_C_LOCALE_ALLOCATE */
	/* This platform doesn't need to do anything except support assertions. */
	pg_ensure_c_locale_called = true;
	return true;
#endif
}
