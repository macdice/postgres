#include "postgres.h"

#ifdef USE_ICU

#include <unicode/ucasemap.h>
#include <unicode/ucnv.h>
#include <unicode/ucol.h>
#include <unicode/uiter.h>
#include <unicode/ustring.h>

#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_locale.h"
#include "utils/pg_locale_icu.h"
#include "utils/pg_locale_internal.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

#include "pg_locale_router.h"

#include <dlfcn.h>

/* Names used by ICU on different platforms. */
#if defined(WIN32)
#define DEFAULT_LIBICUUC "icuuc@VERSION@" DLSUFFIX
#define DEFAULT_LIBICUI18N "icuin@VERSION@" DLSUFFIX
#elif defined(__APPLE__)
#define DEFAULT_LIBICUUC "libicuuc.@VERSION@" DLSUFFIX
#define DEFAULT_LIBICUI18N "libicui18n.@VERSION@" DLSUFFIX
#elif defined(_AIX)
/*
 * XXX It looks like icu4c/source/config/mh-aix-gcc puts the version number
 * here, but is it .so, .a, or both using RLTD_MEMBER syntax?
 */
#define DEFAULT_LIBICUUC "libicuuc@VERSION@" DLSUFFIX
#define DEFAULT_LIBICUI18N "libicui18n@VERSION@" DLSUFFIX
#else
#define DEFAULT_LIBICUUC "libicuuc" DLSUFFIX ".@VERSION@"
#define DEFAULT_LIBICUI18N "libicui18n" DLSUFFIX ".@VERSION@"
#endif

/*
 * Only ICU versions in this range can be opened, because they have passed the
 * cross-checks for ABI compatibility.
 */
#define PG_LOCALE_ROUTER_ICU_MAX U_ICU_VERSION_MAJOR_NUM
#define PG_LOCALE_ROUTER_ICU_MIN 55

static_assert(PG_LOCALE_ROUTER_ICU_MIN <= PG_LOCALE_ROUTER_ICU_MAX,
			  "unsupported ICU version");

/*
 * We're using values, types and functions from the compile-time library for
 * everything except the dyn_ functions below.  These are the library-defined
 * values and types that cross that boundary, so they *must* match.  If any of
 * these assertions fail for a future ICU version, we'll need to devise a
 * version-sensitive coping strategy.
 */
static_assert(sizeof(UErrorCode) == sizeof(int), "ABI break");
static_assert(alignof(UErrorCode) == alignof(int), "ABI break");
static_assert(sizeof(UVersionInfo) == 4, "ABI break");
static_assert(U_MAX_VERSION_STRING_LENGTH == 20, "ABI break");
static_assert(U_MAX_VERSION_LENGTH == 4, "ABI break");
static_assert(U_ZERO_ERROR == 0, "ABI break");
static_assert(UCOL_DEFAULT == -1, "ABI break");
typedef UCharIterator UCharIterator_MAX;
typedef struct UCharIterator_MIN
{
	/* Simplified from ICU 55's uiter.h (unchanged since ICU 2.6) */
	const void *context;
	int32_t		length;
	int32_t		start;
	int32_t		index;
	int32_t		limit;
	int32_t		reservedField;
	int32_t		(*getIndex) (UCharIterator *, UCharIteratorOrigin);
	int32_t		(*move) (UCharIterator *, int32_t, UCharIteratorOrigin);
	UBool		(*hasNext) (UCharIterator *);
	UBool		(*hasPrevious) (UCharIterator *);
	UChar32		(*current) (UCharIterator *);
	UChar32		(*next) (UCharIterator *);
	UChar32		(*previous) (UCharIterator *);
	UChar32		(*reservedFn) (UCharIterator *, int32_t);
	uint32_t	(*getState) (const UCharIterator *);
	void		(*setState) (UCharIterator *, uint32_t, UErrorCode *);
}			UCharIterator_MIN;

static_assert(sizeof(UCharIterator_MIN) == sizeof(UCharIterator_MAX), "ABI break");
static_assert(alignof(UCharIterator_MIN) == alignof(UCharIterator_MAX), "ABI break");
#define ASSERT_ITER_MEMBER(member)										\
	static_assert(offsetof(UCharIterator_MIN, member) ==				\
				  offsetof(UCharIterator_MAX, member),					\
				  "ABI break");											\
	static inline bool assert_iter_##member(UCharIterator_MIN *min,		\
											UCharIterator_MAX *max)		\
	{ return &min->member == &max->member; /* pointer type check */ }
ASSERT_ITER_MEMBER(context);
ASSERT_ITER_MEMBER(length);
ASSERT_ITER_MEMBER(start);
ASSERT_ITER_MEMBER(index);
ASSERT_ITER_MEMBER(limit);
ASSERT_ITER_MEMBER(reservedField);
ASSERT_ITER_MEMBER(getIndex);
ASSERT_ITER_MEMBER(move);
ASSERT_ITER_MEMBER(hasNext);
ASSERT_ITER_MEMBER(hasPrevious);
ASSERT_ITER_MEMBER(current);
ASSERT_ITER_MEMBER(next);
ASSERT_ITER_MEMBER(previous);
ASSERT_ITER_MEMBER(reservedFn);
ASSERT_ITER_MEMBER(getState);
ASSERT_ITER_MEMBER(setState);

/*
 * Functions looked up in an ICU library.  The dyn_ prefixes avoid collision
 * with the function names, which ICU defines as macros, and is also a visual
 * reminder of which functions take rloc->dyn_collator.
 *
 * For everything but these, we use the linked ICU library.
 */
typedef struct pg_locale_router_icu_library
{
	int			major_version;
	int			missing_symbols;
	int			reference_count;
	dlist_node	node;

	char	   *lib_uc_name;
	void	   *lib_uc;
	const char *(*dyn_u_errorName) (UErrorCode code);
	void		(*dyn_u_getUnicodeVersion) (UVersionInfo info);
	void		(*dyn_u_getVersion) (UVersionInfo info);
	void		(*dyn_u_versionToString) (const UVersionInfo versionArray,
										  char *versionString);
	void		(*dyn_uenum_close) (UEnumeration * en);
	const char *(*dyn_uenum_next) (UEnumeration * en,
								   int32_t *resutLength,
								   UErrorCode *status);

	char	   *lib_i18n_name;
	void	   *lib_i18n;
	void		(*dyn_ucol_close) (UCollator *coll);
	void		(*dyn_ucol_getUCAVersion) (const UCollator *coll,
										   UVersionInfo info);
	const UChar *(*dyn_ucol_getRules) (const UCollator *coll,
									   int32_t *length);
	int32_t		(*dyn_ucol_getSortKey) (const UCollator *coll,
										const UChar *source,
										int32_t sourceLength,
										uint8_t *result,
										int32_t resultLength);
	void		(*dyn_ucol_getVersion) (const UCollator *coll,
										UVersionInfo info);
	int32_t		(*dyn_ucol_nextSortKeyPart) (const UCollator *coll,
											 UCharIterator *iter,
											 uint32_t state[2],
											 uint8_t *dest,
											 int32_t count,
											 UErrorCode *status);
	UCollator  *(*dyn_ucol_open) (const char *loc, UErrorCode *status);
	UEnumeration *(*dyn_ucol_openAvailableLocales) (UErrorCode *status);
	UCollator  *(*dyn_ucol_openRules) (const UChar *rules,
									   int32_t rulesLength,
									   UColAttributeValue normalizationMode,
									   UCollationStrength strength,
									   UParseError *parseError,
									   UErrorCode *status);
	UCollationResult (*dyn_ucol_strcollIter) (const UCollator *coll,
											  UCharIterator *sIter,
											  UCharIterator *tIter,
											  UErrorCode *status);
	UCollationResult (*dyn_ucol_strcollUTF8) (const UCollator *coll,
											  const char *source,
											  int32_t sourceLength,
											  const char *target,
											  int32_t targetLength,
											  UErrorCode *status);
}			pg_locale_router_icu_library;

typedef struct pg_locale_router_icu_locale
{
	struct pg_locale_struct locale;

	pg_locale_t std_locale;

	UCollator  *dyn_collator;
	pg_locale_router_icu_library *library;
}			pg_locale_router_icu_locale;

/* GUCs */
static char *libicuuc = DEFAULT_LIBICUUC;
static char *libicui18n = DEFAULT_LIBICUI18N;

static dlist_head loaded_icu_libraries;

static void *
get_sym(pg_locale_router_icu_library * lib, const char *name)
{
	char		full_name[80];
	void	   *handle;
	void	   *sym;

	/* Assume U_DISABLE_RENAMING not defined when building ICU. */
	snprintf(full_name, sizeof(full_name), "%s_%d", name, lib->major_version);

	handle = strncmp(name, "ucol_", 5) == 0 ? lib->lib_i18n : lib->lib_uc;
	sym = dlsym(handle, full_name);
	if (!sym)
	{
		if (lib->missing_symbols == 0)
			elog(DEBUG1,
				 "pg_locale_router: ICU version %d is missing required symbol %s, skipping",
				 lib->major_version,
				 full_name);
		lib->missing_symbols++;
	}

	return sym;
}

/*
 * Replace @VERSION@ with major_version in configured library name or path.
 */
static void
make_library_name(char dst[MAXPGPATH], const char *libname, int major_version)
{
	const char *v = strstr(libname, "@VERSION@");

	if (v)
		snprintf(dst, MAXPGPATH, "%.*s%d%s",
				 (int) (v - libname), libname,
				 major_version,
				 v + 9);
	else
		snprintf(dst, MAXPGPATH, "%s", libname);
}

/*
 * Find or load a library and increment its reference count.  Returns NULL if
 * not found, symbols are missing or dlopen() failed for some other reason.
 * Sets *out_of_memory to indicate whether allocation failed.
 */
static pg_locale_router_icu_library *
get_icu_library(int major_version, bool load, bool *out_of_memory)
{
	pg_locale_router_icu_library *lib;
	dlist_iter	iter;

	*out_of_memory = false;

	/* Do we already have it loaded? */
	dlist_foreach(iter, &loaded_icu_libraries)
	{
		lib = dlist_container(pg_locale_router_icu_library,
							  node,
							  iter.cur);
		if (lib->major_version == major_version)
		{
			lib->reference_count++;
			return lib;
		}
	}

	if (!load)
		return NULL;

	if (major_version == U_ICU_VERSION_MAJOR_NUM)
	{
		/*
		 * The version we compiled and linked against.  No need to dlopen.
		 *
		 * This serves as a compile-time check that headers match our expected
		 * function pointers.  If this fails to compile on a future version of
		 * ICU, we'll need to write some version-sensitive trampoline
		 * functions.
		 *
		 * This library will never be selected by pg_locale_router as the core
		 * ICU support will be preferred, but it's useful to be able to see
		 * details about it in the output of pg_locale_router_libraries().
		 */
		lib = MemoryContextAllocExtended(TopMemoryContext,
										 sizeof(pg_locale_router_icu_library),
										 MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
		if (lib == NULL)
		{
			*out_of_memory = true;
			return NULL;
		}

		lib->major_version = major_version;
		lib->dyn_u_errorName = u_errorName;
		lib->dyn_u_getUnicodeVersion = u_getUnicodeVersion;
		lib->dyn_u_getVersion = u_getVersion;
		lib->dyn_u_versionToString = u_versionToString;
		lib->dyn_uenum_close = uenum_close;
		lib->dyn_uenum_next = uenum_next;
		lib->dyn_ucol_close = ucol_close;
		lib->dyn_ucol_getUCAVersion = ucol_getUCAVersion;
		lib->dyn_ucol_getRules = ucol_getRules;
		lib->dyn_ucol_getSortKey = ucol_getSortKey;
		lib->dyn_ucol_getVersion = ucol_getVersion;
		lib->dyn_ucol_nextSortKeyPart = ucol_nextSortKeyPart;
		lib->dyn_ucol_open = ucol_open;
		lib->dyn_ucol_openAvailableLocales = ucol_openAvailableLocales;
		lib->dyn_ucol_openRules = ucol_openRules;
		lib->dyn_ucol_strcollIter = ucol_strcollIter;
		lib->dyn_ucol_strcollUTF8 = ucol_strcollUTF8;
	}
	else
	{
		char		lib_uc_name[MAXPGPATH];
		char		lib_i18n_name[MAXPGPATH];
		void	   *lib_uc;
		void	   *lib_i18n;
		char	   *trailing_space;

		/* Can we find the two libraries? */
		make_library_name(lib_uc_name, libicuuc, major_version);
		make_library_name(lib_i18n_name, libicui18n, major_version);
		if (!(lib_uc = dlopen(lib_uc_name, RTLD_NOW | RTLD_GLOBAL)))
			return NULL;
		if (!(lib_i18n = dlopen(lib_i18n_name, RTLD_NOW | RTLD_GLOBAL)))
		{
			dlclose(lib_uc);
			return NULL;
		}

		lib = MemoryContextAllocExtended(TopMemoryContext,
										 sizeof(pg_locale_router_icu_library) +
										 strlen(lib_uc_name) + 1 +
										 strlen(lib_i18n_name) + 1,
										 MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
		if (lib == NULL)
		{
			*out_of_memory = true;
			dlclose(lib_uc);
			dlclose(lib_i18n);
			return NULL;
		}

		/* Show library names in pg_locale_router_icu_libraries() output. */
		trailing_space = (char *) lib + sizeof(*lib);
		lib->lib_uc_name = trailing_space;
		lib->lib_i18n_name = trailing_space + strlen(lib_uc_name) + 1;
		strcpy(lib->lib_uc_name, lib_uc_name);
		strcpy(lib->lib_i18n_name, lib_i18n_name);

		lib->lib_uc = lib_uc;
		lib->lib_i18n = lib_i18n;
		lib->major_version = major_version;
		lib->dyn_u_errorName = get_sym(lib, "u_errorName");
		lib->dyn_u_getUnicodeVersion = get_sym(lib, "u_getUnicodeVersion");
		lib->dyn_u_getVersion = get_sym(lib, "u_getVersion");
		lib->dyn_u_versionToString = get_sym(lib, "u_versionToString");
		lib->dyn_uenum_close = get_sym(lib, "uenum_close");
		lib->dyn_uenum_next = get_sym(lib, "uenum_next");
		lib->dyn_ucol_close = get_sym(lib, "ucol_close");
		lib->dyn_ucol_getUCAVersion = get_sym(lib, "ucol_getUCAVersion");
		lib->dyn_ucol_getRules = get_sym(lib, "ucol_getRules");
		lib->dyn_ucol_getSortKey = get_sym(lib, "ucol_getSortKey");
		lib->dyn_ucol_getVersion = get_sym(lib, "ucol_getVersion");
		lib->dyn_ucol_nextSortKeyPart = get_sym(lib, "ucol_nextSortKeyPart");
		lib->dyn_ucol_open = get_sym(lib, "ucol_open");
		lib->dyn_ucol_openAvailableLocales = get_sym(lib, "ucol_openAvailableLocales");
		lib->dyn_ucol_openRules = get_sym(lib, "ucol_openRules");
		lib->dyn_ucol_strcollIter = get_sym(lib, "ucol_strcollIter");
		lib->dyn_ucol_strcollUTF8 = get_sym(lib, "ucol_strcollUTF8");
		if (lib->missing_symbols > 0)
		{
			dlclose(lib_uc);
			dlclose(lib_i18n);
			pfree(lib);
			return NULL;
		}
	}

	lib->reference_count = 1;
	dlist_push_tail(&loaded_icu_libraries, &lib->node);

	return lib;
}

/*
 * Decrement reference count and free if it reaches zero.
 */
static void
release_icu_library(pg_locale_router_icu_library * lib)
{
	Assert(lib->reference_count > 0);
	if (--lib->reference_count == 0)
	{
		if (lib->lib_uc)
		{
			dlclose(lib->lib_uc);
			dlclose(lib->lib_i18n);
		}
		dlist_delete(&lib->node);
		pfree(lib);
	}
}

static pg_locale_router_icu_locale *
get_rloc(pg_locale_t locale)
{
	return (pg_locale_router_icu_locale *) locale;
}

static int
pg_locale_router_icu_strncoll_utf8(const char *arg1, size_t len1,
								   const char *arg2, size_t len2,
								   pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;
	int			result;
	UErrorCode	status;

	status = U_ZERO_ERROR;
	result = lib->dyn_ucol_strcollUTF8(rloc->dyn_collator,
									   arg1, len1,
									   arg2, len2,
									   &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("collation failed: %s",
						lib->dyn_u_errorName(status))));

	return result;
}

static int
pg_locale_router_icu_strcoll_utf8(const char *arg1,
								  const char *arg2,
								  pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;
	int			result;
	UErrorCode	status;

	status = U_ZERO_ERROR;
	result = lib->dyn_ucol_strcollUTF8(rloc->dyn_collator,
									   arg1, -1,
									   arg2, -1,
									   &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("collation failed: %s",
						lib->dyn_u_errorName(status))));

	return result;
}

static int
pg_locale_router_icu_strcoll_internal(const char *arg1, ssize_t len1,
									  const char *arg2, ssize_t len2,
									  pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;
	PgUCharIteratorMultibyteContext context1;
	PgUCharIteratorMultibyteContext context2;
	UCharIterator iter1;
	UCharIterator iter2;
	UErrorCode	status;
	int			result;

	Assert(GetDatabaseEncoding() != PG_UTF8);

	pg_uiter_setDbEncodingString(&iter1, &context1, arg1, len1);
	pg_uiter_setDbEncodingString(&iter2, &context2, arg2, len2);

	status = U_ZERO_ERROR;
	result = lib->dyn_ucol_strcollIter(rloc->dyn_collator,
									   &iter1,
									   &iter2,
									   &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("%s failed: %s", "ucol_strcollIter",
						lib->dyn_u_errorName(status))));

	pg_uiter_close(&iter1);
	pg_uiter_close(&iter2);

	return result;
}

static int
pg_locale_router_icu_strncoll(const char *arg1, size_t len1,
							  const char *arg2, size_t len2,
							  pg_locale_t locale)
{
	return pg_locale_router_icu_strcoll_internal(arg1, len1,
												 arg2, len2,
												 locale);
}

static int
pg_locale_router_icu_strcoll(const char *arg1,
							 const char *arg2,
							 pg_locale_t locale)
{
	return pg_locale_router_icu_strcoll_internal(arg1, -1,
												 arg2, -1,
												 locale);
}

static size_t
pg_locale_router_icu_strnxfrm_internal(char *dest, size_t destsize,
									   const char *src, ssize_t srclen,
									   pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;
	UConverter *icu_converter = pg_icu_dbencoding_converter();
	UChar		sbuf[1024 / sizeof(UChar)];
	UChar	   *uchar = sbuf;
	bool		overflow;
	int32_t		ulen;
	Size		result_bsize;

	ulen = pg_uchar_convert(icu_converter, uchar, lengthof(sbuf), src, srclen,
							&overflow);
	if (overflow)
	{
		uchar = palloc_array(UChar, ulen + 1);
		ulen = pg_uchar_convert(icu_converter, uchar, ulen, src, srclen, NULL);
	}

	result_bsize = lib->dyn_ucol_getSortKey(rloc->dyn_collator,
											uchar, ulen,
											(uint8_t *) dest, destsize);

	/*
	 * ucol_getSortKey() counts the nul-terminator in the result length, but
	 * this function should not.
	 */
	Assert(result_bsize > 0);
	result_bsize--;

	if (uchar != sbuf)
		pfree(uchar);

	/* if dest is defined, it should be nul-terminated */
	Assert(result_bsize >= destsize || dest[result_bsize] == '\0');

	return result_bsize;
}

static size_t
pg_locale_router_icu_strnxfrm(char *dest, size_t destsize,
							  const char *src, size_t srclen,
							  pg_locale_t locale)
{
	return pg_locale_router_icu_strnxfrm_internal(dest, destsize,
												  src, srclen,
												  locale);
}

static size_t
pg_locale_router_icu_strxfrm(char *dest, size_t destsize,
							 const char *src,
							 pg_locale_t locale)
{
	return pg_locale_router_icu_strnxfrm_internal(dest, destsize,
												  src, -1,
												  locale);
}

static size_t
pg_locale_router_icu_strnxfrm_prefix_utf8_internal(char *dest, size_t destsize,
												   const char *src, ssize_t srclen,
												   pg_locale_t locale)
{
	size_t		result;
	UCharIterator iter;
	uint32_t	state[2];
	UErrorCode	status;

	Assert(GetDatabaseEncoding() == PG_UTF8);

	uiter_setUTF8(&iter, src, srclen);
	state[0] = state[1] = 0;	/* won't need that again */
	status = U_ZERO_ERROR;
	result = ucol_nextSortKeyPart(locale->icu.ucol,
								  &iter,
								  state,
								  (uint8_t *) dest,
								  destsize,
								  &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("sort key generation failed: %s",
						u_errorName(status))));

	return result;
}

static size_t
pg_locale_router_icu_strnxfrm_prefix_utf8(char *dest, size_t destsize,
										  const char *src, size_t srclen,
										  pg_locale_t locale)
{
	return pg_locale_router_icu_strnxfrm_prefix_utf8_internal(dest,
															  destsize,
															  src,
															  srclen,
															  locale);
}

static size_t
pg_locale_router_icu_strxfrm_prefix_utf8(char *dest, size_t destsize,
										 const char *src,
										 pg_locale_t locale)
{
	return pg_locale_router_icu_strnxfrm_prefix_utf8_internal(dest,
															  destsize,
															  src,
															  -1,
															  locale);
}

static size_t
pg_locale_router_icu_strnxfrm_prefix_internal(char *dest, size_t destsize,
											  const char *src, ssize_t srclen,
											  pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;
	PgUCharIteratorMultibyteContext context;
	UCharIterator iter;
	uint32_t	state[2];
	UErrorCode	status;
	Size		result_bsize;

	Assert(GetDatabaseEncoding() != PG_UTF8);

	pg_uiter_setDbEncodingString(&iter, &context, src, srclen);
	state[0] = state[1] = 0;	/* won't need that again */
	status = U_ZERO_ERROR;
	result_bsize = lib->dyn_ucol_nextSortKeyPart(rloc->dyn_collator,
												 &iter,
												 state,
												 (uint8_t *) dest,
												 destsize,
												 &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("sort key generation failed: %s",
						lib->dyn_u_errorName(status))));
	pg_uiter_close(&iter);

	return result_bsize;
}

static size_t
pg_locale_router_icu_strnxfrm_prefix(char *dest, size_t destsize,
									 const char *src, size_t srclen,
									 pg_locale_t locale)
{
	return pg_locale_router_icu_strnxfrm_prefix_internal(dest,
														 destsize,
														 src,
														 srclen,
														 locale);
}

static size_t
pg_locale_router_icu_strxfrm_prefix(char *dest, size_t destsize,
									const char *src,
									pg_locale_t locale)
{
	return pg_locale_router_icu_strnxfrm_prefix_internal(dest,
														 destsize,
														 src,
														 -1,
														 locale);
}

static const struct collate_methods pg_locale_router_icu_collate_methods_utf8 = {
	.strncoll = pg_locale_router_icu_strncoll_utf8,
	.strcoll = pg_locale_router_icu_strcoll_utf8,
	.strnxfrm = pg_locale_router_icu_strnxfrm,
	.strxfrm = pg_locale_router_icu_strxfrm,
	.strnxfrm_prefix = pg_locale_router_icu_strnxfrm_prefix_utf8,
	.strxfrm_prefix = pg_locale_router_icu_strxfrm_prefix_utf8,
	.strxfrm_is_safe = true,
};

static const struct collate_methods pg_locale_router_icu_collate_methods = {
	.strncoll = pg_locale_router_icu_strncoll,
	.strcoll = pg_locale_router_icu_strcoll,
	.strnxfrm = pg_locale_router_icu_strnxfrm,
	.strxfrm = pg_locale_router_icu_strxfrm,
	.strnxfrm_prefix = pg_locale_router_icu_strnxfrm_prefix,
	.strxfrm_prefix = pg_locale_router_icu_strxfrm_prefix,
	.strxfrm_is_safe = true,
};

static void
pg_locale_router_icu_freelocale(pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;

	/* Free the pg_locale that was being used for ctype. */
	pg_freelocale(rloc->std_locale);

	lib->dyn_ucol_close(rloc->dyn_collator);
	release_icu_library(lib);
	pfree(locale);
}

static const struct locale_methods pg_locale_router_icu_locale_methods = {
	.freelocale = pg_locale_router_icu_freelocale,
};

void
pg_locale_router_icu_init(void)
{
	DefineCustomStringVariable("pg_locale_router.icu.libicuuc",
							   "Library name or path for ICU common components.",
							   NULL,
							   &libicuuc,
							   DEFAULT_LIBICUUC,
							   PGC_SUSET,
							   0,
							   NULL,
							   NULL,
							   NULL);
	DefineCustomStringVariable("pg_locale_router.icu.libicui18n",
							   "Library name or path for ICU internationalization components.",
							   NULL,
							   &libicui18n,
							   DEFAULT_LIBICUI18N,
							   PGC_SUSET,
							   0,
							   NULL,
							   NULL,
							   NULL);
}

pg_locale_t
pg_locale_router_icu_newlocale(const locale_descriptor *descriptor,
							   int flags,
							   MemoryContext context,
							   pg_newlocale_function std_newlocale)
{
	pg_locale_router_icu_library *lib;
	pg_locale_router_icu_locale *result;
	UCollator  *dyn_collator;
	pg_locale_t std_result;
	char		info_buffer[64];
	char	   *info_space;

	/*
	 * Open it using the standard core routine, which uses the ICU library
	 * that PostgreSQL was compiled and linked against.
	 */
	std_result = std_newlocale(descriptor, flags, context);
	if (!std_result)
		return std_result;

	/*
	 * If there is no version in the catalog (not expected) then no rerouting
	 * is possible, but let the core code complain about that.
	 */
	if (descriptor->collate_version == NULL)
		return std_result;

	/* If the version matches, we can use it directly. */
	if (std_result->collate_version &&
		strcmp(descriptor->collate_version,
			   std_result->collate_version) == 0)
		return std_result;

	/*
	 * Search for a library that reports the version we want.  No point in
	 * testing the maximum version as that's the same as std_locale, so start
	 * the search at max - 1.
	 */
	lib = NULL;
	for (int major_version = PG_LOCALE_ROUTER_ICU_MAX - 1;
		 major_version >= PG_LOCALE_ROUTER_ICU_MIN;
		 major_version--)
	{
		bool		out_of_memory;
		UErrorCode	status;

		lib = get_icu_library(major_version, true, &out_of_memory);
		if (out_of_memory)
		{
			pg_freelocale(std_result);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}
		if (lib == NULL)
			continue;

		status = U_ZERO_ERROR;
		dyn_collator = lib->dyn_ucol_open(descriptor->locale, &status);
		if (U_SUCCESS(status))
		{
			UVersionInfo version;
			char		version_string[U_MAX_VERSION_STRING_LENGTH];

			lib->dyn_ucol_getVersion(dyn_collator, version);
			lib->dyn_u_versionToString(version, version_string);
			if (strcmp(version_string, descriptor->collate_version) == 0)
				break;
			lib->dyn_ucol_close(dyn_collator);
		}
		release_icu_library(lib);
		lib = NULL;
	}

	/* If we didn't find a match, let core complain about versions. */
	if (lib == NULL)
		return std_result;

	/* Append rules, if required. */
	if (descriptor->icurules)
	{
		const UChar *std_rules;
		UChar	   *all_rules;
		int32_t		my_rules_len;
		int32_t		std_rules_len;
		int32_t		all_rules_len;
		int32_t		icurules_len;
		UErrorCode	status;

		/* Allocate a UChar string for standard rules + icurules. */
		std_rules = lib->dyn_ucol_getRules(dyn_collator, &std_rules_len);
		icurules_len = strlen(descriptor->icurules);
		status = U_ZERO_ERROR;
		my_rules_len = ucnv_toUChars(pg_icu_dbencoding_converter(),
									 NULL, 0,
									 descriptor->icurules, icurules_len,
									 &status);
		if (U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR)
		{
			lib->dyn_ucol_close(dyn_collator);
			pg_freelocale(std_result);
			ereport(ERROR,
					(errmsg("%s failed: %s", "ucnv_fromUChars",
							u_errorName(status))));
		}
		all_rules_len = std_rules_len + my_rules_len;
		all_rules = palloc_array_extended(UChar,
										  all_rules_len, MCXT_ALLOC_NO_OOM);
		if (!all_rules)
		{
			lib->dyn_ucol_close(dyn_collator);
			release_icu_library(lib);
			pg_freelocale(std_result);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		/* Write out standard rules then icurules. */
		u_strncpy(all_rules, std_rules, std_rules_len);
		status = U_ZERO_ERROR;
		ucnv_toUChars(pg_icu_dbencoding_converter(),
					  all_rules + std_rules_len, my_rules_len,
					  descriptor->icurules, icurules_len,
					  &status);
		if (U_FAILURE(status))
		{
			lib->dyn_ucol_close(dyn_collator);
			release_icu_library(lib);
			pg_freelocale(std_result);
			ereport(ERROR,
					(errmsg("%s failed: %s", "ucnv_fromUChars",
							u_errorName(status))));
		}

		/* Reopen dyn_collator with the rules appended. */
		lib->dyn_ucol_close(dyn_collator);
		status = U_ZERO_ERROR;
		dyn_collator = lib->dyn_ucol_openRules(all_rules, all_rules_len,
											   UCOL_DEFAULT, UCOL_DEFAULT,
											   NULL, &status);
		if (U_FAILURE(status))
		{
			lib->dyn_ucol_close(dyn_collator);
			release_icu_library(lib);
			pg_freelocale(std_result);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not open collator for locale \"%s\" with rules \"%s\": %s",
							descriptor->locale,
							descriptor->icurules,
							lib->dyn_u_errorName(status))));
		}
	}

	/*
	 * Build an informational message to show in the "collate" column of the
	 * pg_stat_collations view, to show that the locale has been redirected.
	 *
	 * XXX Perhaps there should be a column reserved for such messages?  Many
	 * columns already...
	 */
	snprintf(info_buffer, sizeof(info_buffer), "(ICU %d)", lib->major_version);

	result = MemoryContextAllocExtended(context,
										sizeof(pg_locale_router_icu_locale) +
										strlen(info_buffer) + 1,
										MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!result)
	{
		lib->dyn_ucol_close(dyn_collator);
		release_icu_library(lib);
		pg_freelocale(std_result);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/*
	 * Copy the internal state of std_result.  That avoids the need to forward
	 * all the ctype functions to "std_locale" with an extra hop, or to
	 * reimplement them all.  The core ctype_methods functions will receive a
	 * pointer to this copy.
	 *
	 * XXX Alternatively we could define a new ctype_methods that uses the
	 * dynamically loaded ICU library, but versioning for ctype behavior is
	 * not currently in scope.
	 */
	result->locale = *std_result;

	/* Store informational message as artificial "collate" string. */
	info_space = (char *) result + sizeof(pg_locale_router_icu_locale);
	strcpy(info_space, info_buffer);
	result->locale.descriptor.collate = info_space;

	/*
	 * Replace the freelocale routine, which also needs the std_locale pointer
	 * to free it when the time comes.
	 */
	result->locale.locale = &pg_locale_router_icu_locale_methods;
	result->std_locale = std_result;

	/*
	 * Replace the collate functions.  Note that these work with dyn_collator,
	 * never icu.ucol (which came from the wrong library).
	 */
	result->library = lib;
	result->dyn_collator = dyn_collator;
	result->locale.collate_version = result->locale.descriptor.collate_version;
	if (GetDatabaseEncoding() == PG_UTF8)
		result->locale.collate = &pg_locale_router_icu_collate_methods_utf8;
	else
		result->locale.collate = &pg_locale_router_icu_collate_methods;

	return &result->locale;
}

#endif

PG_FUNCTION_INFO_V1(pg_locale_router_icu_libraries);
PG_FUNCTION_INFO_V1(pg_locale_router_icu_locales);

Datum
pg_locale_router_icu_libraries(PG_FUNCTION_ARGS)
{
#ifndef USE_ICU
	InitMaterializedSRF(fcinfo, 0);
	return (Datum) 0;
#else
	bool		show_all = PG_GETARG_BOOL(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	volatile	pg_locale_router_icu_library *lib;

	InitMaterializedSRF(fcinfo, 0);
	PG_TRY();
	{
		for (int major_version = PG_LOCALE_ROUTER_ICU_MAX;
			 major_version >= PG_LOCALE_ROUTER_ICU_MIN;
			 major_version--)
		{
			Datum		values[6];
			bool		nulls[6] = {0};
			UVersionInfo icu_version;
			UVersionInfo unicode_version;
			char		icu_version_string[U_MAX_VERSION_STRING_LENGTH];
			char		unicode_version_string[U_MAX_VERSION_STRING_LENGTH];
			bool		out_of_memory;

			lib = get_icu_library(major_version, show_all, &out_of_memory);
			if (out_of_memory)
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			if (lib == NULL)
				continue;

			lib->dyn_u_getVersion(icu_version);
			lib->dyn_u_versionToString(icu_version, icu_version_string);
			lib->dyn_u_getUnicodeVersion(unicode_version);
			lib->dyn_u_versionToString(unicode_version, unicode_version_string);

			values[0] = Int32GetDatum(lib->major_version);
			values[1] = CStringGetTextDatum(icu_version_string);
			values[2] = CStringGetTextDatum(unicode_version_string);
			values[3] = Int32GetDatum(lib->reference_count - 1 /* my temp ref */ );
			if (lib->lib_uc_name)
				values[4] = CStringGetTextDatum(lib->lib_uc_name);
			else
				nulls[4] = true;
			if (lib->lib_i18n_name)
				values[5] = CStringGetTextDatum(lib->lib_i18n_name);
			else
				nulls[5] = true;

			release_icu_library(unvolatize(pg_locale_router_icu_library *, lib));
			lib = NULL;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
	PG_FINALLY();
	{
		if (lib)
			release_icu_library(unvolatize(pg_locale_router_icu_library *, lib));
	}
	PG_END_TRY();

	return (Datum) 0;
#endif
}

Datum
pg_locale_router_icu_locales(PG_FUNCTION_ARGS)
{
#ifndef USE_ICU
	InitMaterializedSRF(fcinfo, 0);
	return (Datum) 0;
#else
	int			major_version = PG_GETARG_INT32(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	UErrorCode	status;
	bool		out_of_memory;
	volatile	pg_locale_router_icu_library *lib;
	volatile	UEnumeration *en;

	lib = get_icu_library(major_version, true, &out_of_memory);
	if (out_of_memory)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	if (lib == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not load ICU version %d",
						major_version)));

	PG_TRY();
	{
		status = U_ZERO_ERROR;
		en = lib->dyn_ucol_openAvailableLocales(&status);
		if (U_FAILURE(status))
			ereport(ERROR,
					(errmsg("%s failed: %s", "ucol_openAvailableLocales",
							lib->dyn_u_errorName(status))));

		InitMaterializedSRF(fcinfo, 0);
		for (;;)
		{
			Datum		values[3];
			bool		nulls[3] = {0};
			UVersionInfo collate_version;
			char		collate_version_string[U_MAX_VERSION_STRING_LENGTH];
			UVersionInfo uca_version;
			char		uca_version_string[U_MAX_VERSION_STRING_LENGTH];
			const char *locale;
			UCollator  *collator;

			status = U_ZERO_ERROR;
			locale = lib->dyn_uenum_next(unvolatize(UEnumeration *, en),
										 NULL,
										 &status);
			if (U_FAILURE(status))
				ereport(ERROR,
						(errmsg("%s failed: %s", "uenum_next",
								lib->dyn_u_errorName(status))));
			if (locale == NULL)
				break;

			status = U_ZERO_ERROR;
			collator = lib->dyn_ucol_open(locale, &status);
			if (U_FAILURE(status))
				ereport(ERROR,
						(errmsg("%s failed: %s", "ucol_open",
								lib->dyn_u_errorName(status))));
			lib->dyn_ucol_getVersion(collator, collate_version);
			lib->dyn_ucol_getUCAVersion(collator, uca_version);
			lib->dyn_ucol_close(collator);
			lib->dyn_u_versionToString(collate_version, collate_version_string);
			lib->dyn_u_versionToString(uca_version, uca_version_string);

			values[0] = CStringGetTextDatum(locale);
			values[1] = CStringGetTextDatum(collate_version_string);
			values[2] = CStringGetTextDatum(uca_version_string);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}
	}
	PG_FINALLY();
	{
		if (en)
			lib->dyn_uenum_close(unvolatize(UEnumeration *, en));
		release_icu_library(unvolatize(pg_locale_router_icu_library *, lib));
	}
	PG_END_TRY();

	return (Datum) 0;
#endif
}
