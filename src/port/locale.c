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

#include <locale.h>
#ifdef HAVE_XLOCALE_H
#include <xlocale.h>
#endif

#ifndef WIN32
#include <pthread.h>
#else
#include <synchapi.h>
#endif

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
	 * It is remotely possible that the locale couldn't be allocated, due to
	 * lack of memory.  In that case (locale_t) 0 will be returned.  A caller
	 * can test for that condition with pg_ensure_c_locale() at a convenient
	 * time for error reporting.
	 */

	return c_locale;
}
