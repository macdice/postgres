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
#include <unicode/ubrk.h>
#endif

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
extern PGDLLIMPORT char *icu_library_path;
extern PGDLLIMPORT char *icu_library_versions;
extern PGDLLIMPORT char *default_icu_library_version;

/* lc_time localization cache */
extern PGDLLIMPORT char *localized_abbrev_days[];
extern PGDLLIMPORT char *localized_full_days[];
extern PGDLLIMPORT char *localized_abbrev_months[];
extern PGDLLIMPORT char *localized_full_months[];


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
 * An ICU library version that we're either linked against or have loaded at
 * runtime.
 */
typedef struct pg_icu_library
{
	int			major_version;
	int			minor_version;
	void		(*getICUVersion) (UVersionInfo info);
	void		(*getUnicodeVersion) (UVersionInfo into);
	void		(*getCLDRVersion) (UVersionInfo info, UErrorCode *status);
	UCollator  *(*open) (const char *loc, UErrorCode *status);
	void		(*close) (UCollator *coll);
	void		(*getCollatorVersion) (const UCollator *coll, UVersionInfo info);
	void		(*getUCAVersion) (const UCollator *coll, UVersionInfo info);
	void		(*versionToString) (const UVersionInfo versionArray,
									char *versionString);
	UCollationResult (*strcoll) (const UCollator *coll,
								 const UChar *source,
								 int32_t sourceLength,
								 const UChar *target,
								 int32_t targetLength);
	UCollationResult (*strcollUTF8) (const UCollator *coll,
									 const char *source,
									 int32_t sourceLength,
									 const char *target,
									 int32_t targetLength,
									 UErrorCode *status);
	int32_t		(*getSortKey) (const UCollator *coll,
							   const UChar *source,
							   int32_t sourceLength,
							   uint8_t *result,
							   int32_t resultLength);
	int32_t		(*nextSortKeyPart) (const UCollator *coll,
									UCharIterator *iter,
									uint32_t state[2],
									uint8_t *dest,
									int32_t count,
									UErrorCode *status);
	void		(*setUTF8) (UCharIterator *iter,
							const char *s,
							int32_t length);
	const char *(*errorName) (UErrorCode code);
	int32_t		(*strToUpper) (UChar *dest,
							   int32_t destCapacity,
							   const UChar *src,
							   int32_t srcLength,
							   const char *locale,
							   UErrorCode *pErrorCode);
	int32_t		(*strToLower) (UChar *dest,
							   int32_t destCapacity,
							   const UChar *src,
							   int32_t srcLength,
							   const char *locale,
							   UErrorCode *pErrorCode);
	int32_t		(*strToTitle) (UChar *dest,
							   int32_t destCapacity,
							   const UChar *src,
							   int32_t srcLength,
							   UBreakIterator *titleIter,
							   const char *locale,
							   UErrorCode *pErrorCode);
	void		(*setAttribute) (UCollator *coll,
								 UColAttribute attr,
								 UColAttributeValue value,
								 UErrorCode *status);
	struct pg_icu_library *next;
} pg_icu_library;

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
			char	   *locale;
			UCollator  *ucol;
			pg_icu_library *lib;
		}			icu;
#endif
		int			dummy;		/* in case we have neither LOCALE_T nor ICU */
	}			info;
};

#ifdef USE_ICU
#define PG_ICU_LIB(x) ((x)->info.icu.lib)
#define PG_ICU_COL(x) ((x)->info.icu.ucol)
#endif

typedef struct pg_locale_struct *pg_locale_t;

extern PGDLLIMPORT struct pg_locale_struct default_locale;

extern bool make_icu_collator(const char *iculocstr,
							  const char *collversion,
							  struct pg_locale_struct *resultp);

extern pg_locale_t pg_newlocale_from_collation(Oid collid);

extern char *get_collation_actual_version(char collprovider, const char *collcollate);

#ifdef USE_ICU
extern int32_t icu_to_uchar(UChar **buff_uchar, const char *buff, size_t nbytes);
extern int32_t icu_from_uchar(char **result, const UChar *buff_uchar, int32_t len_uchar);
#endif
extern void check_icu_locale(const char *icu_locale);

extern void invalidate_cached_collations(void);

/* These functions convert from/to libc's wchar_t, *not* pg_wchar_t */
extern size_t wchar2char(char *to, const wchar_t *from, size_t tolen,
						 pg_locale_t locale);
extern size_t char2wchar(wchar_t *to, size_t tolen,
						 const char *from, size_t fromlen, pg_locale_t locale);

#endif							/* _PG_LOCALE_ */
