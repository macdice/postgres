/*-----------------------------------------------------------------------
 *
 * PostgreSQL locale utilities
 *
 * src/include/utils/pg_locale.h
 *
 * Copyright (c) 2002-2022, PostgreSQL Global Development Group
 *
 *-----------------------------------------------------------------------
 */

#ifndef _PG_LOCALE_
#define _PG_LOCALE_

#if defined(LOCALE_T_IN_XLOCALE) || defined(WCSTOMBS_L_IN_XLOCALE)
#include <xlocale.h>
#endif
#ifdef USE_ICU
#include <unicode/ucol.h>
#endif

#include "utils/guc.h"

#ifdef USE_ICU
/*
 * ucol_strcollUTF8() was introduced in ICU 50, but it is buggy before ICU 53.
 * (see
 * <https://www.postgresql.org/message-id/flat/f1438ec6-22aa-4029-9a3b-26f79d330e72%40manitou-mail.org>)
 */
#if U_ICU_VERSION_MAJOR_NUM >= 53
#define HAVE_UCOL_STRCOLLUTF8 1
#else
#undef HAVE_UCOL_STRCOLLUTF8
#endif
#endif

/* use for libc locale names */
#define LOCALE_NAME_BUFLEN 128

/* GUC settings */
extern PGDLLIMPORT char *locale_messages;
extern PGDLLIMPORT char *locale_monetary;
extern PGDLLIMPORT char *locale_numeric;
extern PGDLLIMPORT char *locale_time;

/* lc_time localization cache */
extern PGDLLIMPORT char *localized_abbrev_days[];
extern PGDLLIMPORT char *localized_full_days[];
extern PGDLLIMPORT char *localized_abbrev_months[];
extern PGDLLIMPORT char *localized_full_months[];


extern bool check_locale_messages(char **newval, void **extra, GucSource source);
extern void assign_locale_messages(const char *newval, void *extra);
extern bool check_locale_monetary(char **newval, void **extra, GucSource source);
extern void assign_locale_monetary(const char *newval, void *extra);
extern bool check_locale_numeric(char **newval, void **extra, GucSource source);
extern void assign_locale_numeric(const char *newval, void *extra);
extern bool check_locale_time(char **newval, void **extra, GucSource source);
extern void assign_locale_time(const char *newval, void *extra);

extern bool check_locale(int category, const char *locale, char **canonname);
extern char *pg_perm_setlocale(int category, const char *locale);
extern void check_strxfrm_bug(void);

extern bool lc_collate_is_c(Oid collation);
extern bool lc_ctype_is_c(Oid collation);

/*
 * Return the POSIX lconv struct (contains number/money formatting
 * information) with locale information for all categories.
 */
extern struct lconv *PGLC_localeconv(void);

extern void cache_locale_time(void);

#ifdef USE_ICU

/*
 * We don't want to call into dlopen'd ICU libraries that are newer than the
 * one we were compiled and linked against, just in case there is an
 * incompatible API change.
 */
#define PG_MAX_ICU_MAJOR_VERSION U_ICU_VERSION_MAJOR_NUM

/* An old ICU release that we know has the right API. */
#define PG_MIN_ICU_MAJOR_VERSION 54

/*
 * In a couple of places we use an array of possible versions as a fast
 * associative table, which isn't too big for now.
 */
#define PG_NUM_ICU_MAJOR_VERSIONS								\
	(PG_MAX_ICU_MAJOR_VERSION - PG_MIN_ICU_MAJOR_VERSION + 1)
#define PG_ICU_SLOT(major_version)					\
	((major_version) - PG_MIN_ICU_MAJOR_VERSION)

/*
 * An ICU library version that we're either linked against or have loaded at
 * runtime.
 */
typedef struct pg_icu_library
{
	void	   *handle;			/* if loaded with dlopen() */
	int			major_version;	/* major version of ICU */
	UCollator *(*open)(const char *loc, UErrorCode *status);
	void (*close)(UCollator *coll);
	void (*getVersion)(const UCollator *coll, UVersionInfo info);
	void (*versionToString)(const UVersionInfo versionArray,
							char *versionString);
	UCollationResult (*strcoll)(const UCollator *coll,
								const UChar *source,
								int32_t sourceLength,
								const UChar *target,
								int32_t targetLength);
	UCollationResult (*strcollUTF8)(const UCollator *coll,
									const char *source,
									int32_t sourceLength,
									const char *target,
									int32_t targetLength,
									UErrorCode *status);
	int32_t (*getSortKey)(const UCollator *coll,
						  const UChar *source,
						  int32_t sourceLength,
						  uint8_t *result,
						  int32_t resultLength);
	int32_t (*nextSortKeyPart)(const UCollator *coll,
							   UCharIterator *iter,
							   uint32_t state[2],
							   uint8_t *dest,
							   int32_t count,
							   UErrorCode *status);
	const char *(*errorName)(UErrorCode code);
} pg_icu_library;

extern pg_icu_library *current_icu_library;

#endif

/*
 * We define our own wrapper around locale_t so we can keep the same
 * function signatures for all builds, while not having to create a
 * fake version of the standard type locale_t in the global namespace.
 * pg_locale_t is occasionally checked for truth, so make it a pointer.
 */
struct pg_locale_struct
{
	char		provider;
	bool		deterministic;
	union
	{
#ifdef HAVE_LOCALE_T
		locale_t	lt;
#endif
#ifdef USE_ICU
		struct
		{
			const char *locale;
			UCollator  *ucol[PG_NUM_ICU_MAJOR_VERSIONS];
		}			icu;
#endif
		int			dummy;		/* in case we have neither LOCALE_T nor ICU */
	}			info;
};

typedef struct pg_locale_struct *pg_locale_t;

#ifdef USE_ICU
/*
 * Get a collator for 'loc' suitable for use with ICU library 'lib'.
 */
static inline UCollator *
pg_icu_collator(pg_icu_library *lib, pg_locale_t loc)
{
	int major_version = lib->major_version;
	UCollator *collator = loc->info.icu.ucol[PG_ICU_SLOT(major_version)];

	if (unlikely(!collator))
	{
		UErrorCode status;

		collator =lib->open(loc->info.icu.locale, &status);
		if (U_FAILURE(status))
			ereport(ERROR,
					(errmsg("could not open collator for locale \"%s\", ICU major version %d: %s",
							loc->info.icu.locale,
							major_version,
							lib->errorName(status))));
		loc->info.icu.ucol[PG_ICU_SLOT(major_version)] = collator;
	}

	return collator;
}

extern void pg_icu_activate_major_version(int major_version);
#endif

extern PGDLLIMPORT struct pg_locale_struct default_locale;

extern void make_icu_collator(const char *iculocstr,
							  struct pg_locale_struct *resultp);

extern pg_locale_t pg_newlocale_from_collation(Oid collid);

extern char *get_collation_actual_version(char collprovider, const char *collcollate);

#ifdef USE_ICU
extern int32_t icu_to_uchar(UChar **buff_uchar, const char *buff, size_t nbytes);
extern int32_t icu_from_uchar(char **result, const UChar *buff_uchar, int32_t len_uchar);
#endif
extern void check_icu_locale(const char *icu_locale);

/* These functions convert from/to libc's wchar_t, *not* pg_wchar_t */
extern size_t wchar2char(char *to, const wchar_t *from, size_t tolen,
						 pg_locale_t locale);
extern size_t char2wchar(wchar_t *to, size_t tolen,
						 const char *from, size_t fromlen, pg_locale_t locale);

#endif							/* _PG_LOCALE_ */
