/*
 * We would like to be able to convert between strings and doubles using the C
 * locale.  We can set the thread's current locale with uselocale() and use
 * snprintf() and strtod() according to the POSIX standard, but some systems
 * haven't implemented uselocale() yet.  We can use snprintf_l() and
 * strtod_l() on some systems, but POSIX hasn't standardized those yet.  All
 * systems we target can do one or the other, so provide a simple abstraction.
 */

#ifndef PGTYPES_FORMAT_H
#define PGTYPES_FORMAT_H

/*
 * Use wcstombs_l's header as a clue about where to find the other extra _l
 * functions.
 */
#if defined(LOCALE_T_IN_XLOCALE) || defined(WCSTOMBS_L_IN_XLOCALE)
#include <xlocale.h>
#else
#include <locale.h>
#endif

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

extern PGDLLIMPORT locale_t PGTYPESclocale;

/*
 * If any of these functions are missing, we need to mess with the
 * thread-local locale.
 */
#if !defined(HAVE_STRTOD_L) || !defined(HAVE_VSPRINTF_L) || !defined(HAVE_VSNPRINTF_L)
#if defined(WIN32)
/* Windows has all of these in slightly scrambled form. */
#else
/* At least one of these functions is missing.  We'll do the save/restore. */
#define PGTYPES_MUST_SAVE_RESTORE_THREAD_LOCALE
#endif
#endif

/*
 * Before running code that might do conversions, call this to change the
 * thread's current locale on platforms that need it.  This is done as a
 * separate function rather than inside individual conversion wrappers for
 * some batching effect, avoiding repeated save/restore.
 *
 * PGTYPESinit() must have been called, or this will have no effect.
 */
static inline int
pgtypes_begin_clocale(locale_t *old_locale)
{
#ifdef PGTYPES_MUST_SAVE_RESTORE_THREAD_LOCALE
	/*
	 * On this platform, at least one of the functions below expects us to
	 * have changed the thread's locale.  The caller provides space for us to
	 * store the current locale, so we can change it back later.
	 */
	*old_locale = uselocale(PGTYPESclocale);
	return (*old_locale == (locale_t) 0) ? -1 : 0;
#else
	/*
	 * Dummy value.  We have _l() variants of the functions we need, and we
	 * might not even have uselocale() on this platform.
	 */
	*old_locale = (locale_t) 0;
	return 0;
#endif
}

/*
 * Restore the current thread's locale.  Call with the value returned by
 * pgtypes_begin_clocale().  Does nothing on platforms with all the required
 * _l() functions.
 */
static inline void
pgtypes_end_clocale(locale_t old_locale)
{
#ifdef PGTYPES_MUST_SAVE_RESTORE_THREAD_LOCALE
	/* Put it back. */
	uselocale(old_locale);
#endif
}

static inline double
pgtypes_strtod(const char *str, char **endptr)
{
#if defined(WIN32)
	return _strtod_l(str, endptr, PGTYPESclocale);
#elif defined(HAVE_STRTOD_L)
	return strtod_l(str, endptr, PGTYPESclocale);
#else
	return strtod(str, endptr);
#endif
}

static inline int pgtypes_sprintf(char *str, const char *format, ...) pg_attribute_printf(2, 3);

static inline int
pgtypes_sprintf(char *str, const char *format, ...)
{
	va_list args;

	va_start(args, format);

#if defined(WIN32)
	return _vsprintf_l(str, format, PGTYPESclocale, args);
#elif defined(HAVE_VSPRINTF_L)
	return vsprintf_l(str, PGTYPESclocale, format, args);
#else
	return vsprintf(str, format, args);
#endif
}

static inline int pgtypes_snprintf(char *str, size_t size, const char *format, ...) pg_attribute_printf(3, 4);

static inline int
pgtypes_snprintf(char *str, size_t size, const char *format, ...)
{
	va_list args;

	va_start(args, format);

#if defined(WIN32)
	return _vsnprintf_l(str, size, format, PGTYPESclocale, args);
#elif defined(HAVE_VSPRINTF_L)
	return vsnprintf_l(str, size, PGTYPESclocale, format, args);
#else
	return vsnprintf(str, size, format, args);
#endif
}

#endif
