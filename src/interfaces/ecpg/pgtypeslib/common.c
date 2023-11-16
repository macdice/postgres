/* src/interfaces/ecpg/pgtypeslib/common.c */

#include "postgres_fe.h"

#include "pgtypes.h"
#include "pgtypes_format.h"
#include "pgtypeslib_extern.h"

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

static locale_t	PGTYPESclocale = (locale_t) 0;

int
PGTYPESinit(void)
{
	/* Already called? */
	if (PGTYPESclocale != (locale_t) 0)
		return 0;

#ifdef WIN32
	PGTYPESclocale = _create_locale(LC_ALL, "C");
#else
	PGTYPESclocale = newlocale(LC_ALL_MASK, "C", (locale_t) 0);
#endif
	if (PGTYPESclocale == (locale_t) 0)
		return -1;
	return -0;
}

/*
 * If any of these _l() functions are missing, we need to mess with the
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
int
PGTYPESbegin_clocale(locale_t *old_locale)
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
 * PGTYPESbegin_clocale().  Does nothing on platforms with all the required
 * _l() functions.
 */
void
PGTYPESend_clocale(locale_t old_locale)
{
#ifdef PGTYPES_MUST_SAVE_RESTORE_THREAD_LOCALE
	/* Put it back. */
	uselocale(old_locale);
#endif
}

double
PGTYPESstrtod(const char *str, char **endptr)
{
#if defined(WIN32)
	return _strtod_l(str, endptr, PGTYPESclocale);
#elif defined(HAVE_STRTOD_L)
	return strtod_l(str, endptr, PGTYPESclocale);
#else
	return strtod(str, endptr);
#endif
}

int
PGTYPESsprintf(char *str, const char *format, ...)
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

int
PGTYPESsnprintf(char *str, size_t size, const char *format, ...)
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

/* Return value is zero-filled. */
char *
pgtypes_alloc(long size)
{
	char	   *new = (char *) calloc(1L, size);

	if (!new)
		errno = ENOMEM;
	return new;
}

char *
pgtypes_strdup(const char *str)
{
	char	   *new = (char *) strdup(str);

	if (!new)
		errno = ENOMEM;
	return new;
}

int
pgtypes_fmt_replace(union un_fmt_comb replace_val, int replace_type, char **output, int *pstr_len)
{
	/*
	 * general purpose variable, set to 0 in order to fix compiler warning
	 */
	int			i = 0;

	switch (replace_type)
	{
		case PGTYPES_TYPE_NOTHING:
			break;
		case PGTYPES_TYPE_STRING_CONSTANT:
		case PGTYPES_TYPE_STRING_MALLOCED:
			i = strlen(replace_val.str_val);
			if (i + 1 <= *pstr_len)
			{
				/* include trailing terminator in what we copy */
				memcpy(*output, replace_val.str_val, i + 1);
				*pstr_len -= i;
				*output += i;
				if (replace_type == PGTYPES_TYPE_STRING_MALLOCED)
					free(replace_val.str_val);
				return 0;
			}
			else
				return -1;
			break;
		case PGTYPES_TYPE_CHAR:
			if (*pstr_len >= 2)
			{
				(*output)[0] = replace_val.char_val;
				(*output)[1] = '\0';
				(*pstr_len)--;
				(*output)++;
				return 0;
			}
			else
				return -1;
			break;
		case PGTYPES_TYPE_DOUBLE_NF:
		case PGTYPES_TYPE_INT64:
		case PGTYPES_TYPE_UINT:
		case PGTYPES_TYPE_UINT_2_LZ:
		case PGTYPES_TYPE_UINT_2_LS:
		case PGTYPES_TYPE_UINT_3_LZ:
		case PGTYPES_TYPE_UINT_4_LZ:
			{
				char	   *t = pgtypes_alloc(PGTYPES_FMT_NUM_MAX_DIGITS);

				if (!t)
					return ENOMEM;
				switch (replace_type)
				{
					case PGTYPES_TYPE_DOUBLE_NF:
						i = PGTYPESsnprintf(t, PGTYPES_FMT_NUM_MAX_DIGITS,
											"%0.0g", replace_val.double_val);
						break;
					case PGTYPES_TYPE_INT64:
						i = snprintf(t, PGTYPES_FMT_NUM_MAX_DIGITS,
									 INT64_FORMAT, replace_val.int64_val);
						break;
					case PGTYPES_TYPE_UINT:
						i = snprintf(t, PGTYPES_FMT_NUM_MAX_DIGITS,
									 "%u", replace_val.uint_val);
						break;
					case PGTYPES_TYPE_UINT_2_LZ:
						i = snprintf(t, PGTYPES_FMT_NUM_MAX_DIGITS,
									 "%02u", replace_val.uint_val);
						break;
					case PGTYPES_TYPE_UINT_2_LS:
						i = snprintf(t, PGTYPES_FMT_NUM_MAX_DIGITS,
									 "%2u", replace_val.uint_val);
						break;
					case PGTYPES_TYPE_UINT_3_LZ:
						i = snprintf(t, PGTYPES_FMT_NUM_MAX_DIGITS,
									 "%03u", replace_val.uint_val);
						break;
					case PGTYPES_TYPE_UINT_4_LZ:
						i = snprintf(t, PGTYPES_FMT_NUM_MAX_DIGITS,
									 "%04u", replace_val.uint_val);
						break;
				}

				if (i < 0 || i >= PGTYPES_FMT_NUM_MAX_DIGITS)
				{
					free(t);
					return -1;
				}
				i = strlen(t);
				*pstr_len -= i;

				/*
				 * if *pstr_len == 0, we don't have enough space for the
				 * terminator and the conversion fails
				 */
				if (*pstr_len <= 0)
				{
					free(t);
					return -1;
				}
				strcpy(*output, t);
				*output += i;
				free(t);
			}
			break;
		default:
			break;
	}
	return 0;
}

/* Functions declared in pgtypes.h. */

/* Just frees memory (mostly needed for Windows) */
void
PGTYPESchar_free(char *ptr)
{
	free(ptr);
}
