#include "postgres.h"

#ifdef USE_ICU

#include <unicode/ucasemap.h>
#include <unicode/ucnv.h>
#include <unicode/ucol.h>
#include <unicode/ustring.h>

#include "fmgr.h"
#include "utils/pg_locale.h"
#include "utils/pg_locale_internal.h"

#include "pg_locale_router.h"

#include <dlfcn.h>

typedef struct pg_locale_router_icu_library
{
	void	   *library_handle;
	int			major_version;
	
	UCollator *(*dyn_ucol_open)(const char *loc, UErrorCode *status);
	void (*dyn_ucol_close)(UCollator *coll);
	void (*dyn_ucol_getVersion)(const UCollator *coll, UVersionInfo info);
	void (*dyn_u_versionToString)(const UVersionInfo versionArray,
								  char *versionString);
	UCollationResult (*dyn_ucol_strcoll)(const UCollator *coll,
										 const UChar *source,
										 int32_t sourceLength,
										 const UChar *target,
										 int32_t targetLength);
	UCollationResult (*dyn_ucol_strcollUTF8)(const UCollator *coll,
											 const char *source,
											 int32_t sourceLength,
											 const char *target,
											 int32_t targetLength,
											 UErrorCode *status);
	int32_t (*dyn_ucol_getSortKey)(const UCollator *coll,
								   const UChar *source,
								   int32_t sourceLength,
								   uint8_t *result,
								   int32_t resultLength);
	int32_t (*dyn_ucol_nextSortKeyPart)(const UCollator *coll,
										UCharIterator *iter,
										uint32_t state[2],
										uint8_t *dest,
										int32_t count,
										UErrorCode *status);
	const char *(*dyn_u_errorName)(UErrorCode code);
	
	struct pg_locale_router_icu_library *next;
} pg_locale_router_icu_library;

typedef struct pg_locale_router_icu_locale
{
	struct pg_locale_struct locale;
	
	pg_locale_t std_locale;

	UCollator *dyn_collator;
	pg_locale_router_icu_library *library;	
} pg_locale_router_icu_locale;

#if 0
/*
 * Macros to generate functions that forward ctype-related queries to a core
 * PostgreSQL locale.
 *
 * XXX We could generate functions that use these for the target ICU library
 * too.
 */
#define GEN_ISCLASS(class)												\
	static bool															\
	pg_locale_router_fwd_is##class(pg_wchar wc, pg_locale_t locale)		\
	{																	\
		pg_locale_router_icu_locale *rloc;								\
		pg_locale_t ctype_locale;										\
		rloc = (pg_locale_router_icu_locale *) locale;					\
		ctype_locale = rloc->ctype_locale;								\
		return ctype_locale->ctype->wc_is##class(wc, ctype_locale);		\
	}
#define GEN_TOCLASS(class)												\
	static pg_wchar														\
	pg_locale_router_fwd_to##class(pg_wchar wc, pg_locale_t locale)		\
	{																	\
		pg_locale_router_icu_locale *rloc;								\
		pg_locale_t ctype_locale;										\
		rloc = (pg_locale_router_icu_locale *) locale;					\
		ctype_locale = rloc->ctype_locale;								\
		return ctype_locale->ctype->wc_to##class(wc, ctype_locale);		\
	}
#define GEN_STRFUN(strfun)												\
	static size_t														\
	pg_locale_router_fwd_##strfun(char *dest,							\
								  size_t destsize,						\
								  const char *src,						\
								  size_t srclen,						\
								  pg_locale_t locale)					\
	{																	\
		pg_locale_router_icu_locale *rloc;								\
		pg_locale_t ctype_locale;										\
		rloc = (pg_locale_router_icu_locale *) locale;					\
		ctype_locale = rloc->ctype_locale;								\
		return ctype_locale->ctype->strfun(dest, destsize, src, srclen,	\
										   ctype_locale);				\
	}

GEN_ISCLASS(digit);
GEN_ISCLASS(alpha);
GEN_ISCLASS(alnum);
GEN_ISCLASS(upper);
GEN_ISCLASS(lower);
GEN_ISCLASS(graph);
GEN_ISCLASS(print);
GEN_ISCLASS(punct);
GEN_ISCLASS(space);
GEN_ISCLASS(isxdigit);
GEN_ISCLASS(cased);
GEN_TOCLASS(upper);
GEN_TOCLASS(lower);
GEN_STRFUN(strlower);
GEN_STRFUN(strupper);
GEN_STRFUN(strtitle);
GEN_STRFUN(strfold);

static const struct ctype_methods pg_locale_router_icu_ctype_method = {
	.strlower = pg_locale_router_icu_strlower,
	.strtitle = pg_locale_router_icu_strtitle,
	.strupper = pg_locale_router_icu_strupper,
	.strfold = pg_locale_router_icu_strfold,
	.downcase_ident = NULL,
	.wc_isdigit = pg_locale_router_icu_isdigit,
	.wc_isalpha = pg_locale_router_icu_isalpha,
	.wc_isalnum = pg_locale_router_icu_isalnum,
	.wc_isupper = pg_locale_router_icu_isupper,
	.wc_islower = pg_locale_router_icu_tolower,
	.wc_isgraph = pg_locale_router_icu_isgraph,
	.wc_isprint = pg_locale_router_icu_isprint,
	.wc_ispunct = pg_locale_router_icu_ispunct,
	.wc_isspace = pg_locale_router_icu_isspace,
	.wc_isxdigit = pg_locale_router_icu_isxdigit,
	.wc_iscased = pg_locale_router_icu_iscased,
	.wc_toupper = pg_locale_router_icu_toupper,
	.wc_tolower = pg_locale_router_icu_tolower,
};
#endif

static pg_locale_router_icu_locale *
get_rloc(pg_locale_t locale)
{
	return (pg_locale_router_icu_locale *) locale;
}

/*
 * Like init_icu_converter() in pg_locale_icu.c.
 */
static void
pg_locale_router_icu_init_converter(void)
{
	const char *icu_encoding_name;
	UErrorCode	status;
	UConverter *conv;

	if (icu_converter)
		return;					/* already done */

	icu_encoding_name = get_encoding_name_for_icu(GetDatabaseEncoding());
	if (!icu_encoding_name)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("encoding \"%s\" not supported by ICU",
						pg_encoding_to_char(GetDatabaseEncoding()))));

	status = U_ZERO_ERROR;
	conv = ucnv_open(icu_encoding_name, &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("could not open ICU converter for encoding \"%s\": %s",
						icu_encoding_name, u_errorName(status))));

	icu_converter = conv;
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

static size_t
pg_locale_router_icu_strnxfrm_internal(char *dest, size_t destsize,
									   const char *src, ssize_t srclen,
									   pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;
	UChar		sbuf[1024 / sizeof(UChar)];
	UChar	   *uchar = sbuf;
	int32_t		ulen;
	Size		result_bsize;
	UErrorCode	status;

	/* Convert in one pass, if possible. */
	status = U_ZERO_ERROR;
	u_strFromUTF8(sbuf, lengthof(sbuf), &ulen, src, srclen, &status);
	if (status == U_BUFFER_OVERFLOW_ERROR)
	{
		/* Allocate a bigger buffer and try again. */
		uchar = palloc_array(UChar, ulen);
		status = U_ZERO_ERROR;
		u_strFromUTF8(uchar, ulen, &ulen, src, srclen, &status);
	}
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("%s failed: %s", "u_strFromUTF8",
						u_errorName(status))));
	
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

static const struct collate_methods pg_locale_router_icu_collate_methods_utf8 = {
	.strncoll = pg_locale_router_icu_strncoll_utf8,
	.strcoll = pg_locale_router_icu_strcoll_utf8,
	.strnxfrm = pg_locale_router_icu_strnxfrm,
	.strxfrm = pg_locale_router_icu_strxfrm,
	.strnxfrm_prefix = pg_locale_router_strnxfrm_prefix_utf8,
	.strxfrm_prefix = pg_locale_router_strxfrm_prefix_utf8,
	.strxfrm_is_safe = true,
};

static const struct collate_methods pg_locale_router_icu_collate_methods = {
	.strncoll = pg_locale_router_icu_strncoll,
	.strcoll = pg_locale_router_icu_strcoll,
	.strnxfrm = pg_locale_router_icu_strnxfrm,
	.strxfrm = pg_locale_router_icu_strxfrm,
	.strnxfrm_prefix = pg_locale_router_strnxfrm_prefix,
	.strxfrm_prefix = pg_locale_router_strxfrm_prefix_utf8,
	.strxfrm_is_safe = true,
};

static void
pg_locale_router_icu_freelocale(pg_locale_t locale)
{
	pg_locale_router_icu_locale *rloc = get_rloc(locale);
	pg_locale_router_icu_library *lib = rloc->library;

	pg_freelocale(rloc->ctype_locale);
	lib->ucol_close(rloc->collator);
	pfree(locale);
}

static const struct locale_methods pg_locale_router_icu_locale_methods = {
	.freelocale = pg_locale_router_icu_freelocale,
};

static dlist_head icu_libraries;

static void
first_icu_library(void)
{
	
}

pg_locale_t
pg_locale_router_newlocale_icu(const locale_descriptor *descriptor,
							   int flags,
							   MemoryContext context,
							   pg_newlocale_function std_newlocale)
{
	UCollator *dyn_collator;
	pg_locale_t std_result;
	pg_locale_router_icu_locale *result;

	/*
	 * Open it using the standard core routine, which uses the ICU library
	 * that PostgreSQL was compiled and linked against.
	 */
	std_result = std_newlocale(descriptor, flags, context);
	if (!std_result)
		return std_result;

	/*
	 * Currently only UTF8 is supported, so if another encoding is in use then
	 * let the core code complain about collate_version differences.
	 */
	if (GetDatabaseEncoding() != PG_UTF8)
		return std_result;
	
	/*
	 * If there is no version in the catalog (not expected) then no
	 * rerouting is possible.
	 */
	if (descriptor->collate_version == NULL)
		return std_result;

	/* If the version matches, we can use it directly. */
	if (std_result->collate_version &&
		strcmp(descriptor->collate_version,
			   std_result->collate_version) == 0)
		return std_result;
	
	/*
	 * Try to find another library that reports the version from the catalog.
	 */
	library = first_icu_library();
	while (library)
	{
		UErrorCode error;

		error = U_ZERO_ERROR;
		dyn_collator = library->ucol_open(descriptor->collate, &status);
		if (U_SUCCESS(status))
		{
			UVersionInfo version;
			char version_string[U_MAX_VERSION_STRING_LENGTH];
			
			library->ucol_getVersion(dyn_collator, version);
			library->u_versionToString(version, version_string);
			if (strcmp(version_string, descriptor->collate_version) == 0)
				break;
			library->ucol_close(dyn_collator);
		}
		library = library->next;
	}

	/* If we didn't find a match, let core generate a warning. */
	if (library == NULL)
		return std_error;

	/* Append rules, if required. */
	if (descriptor->icurules)
	{
		const UChar *std_rules;
		UChar *my_rules;
		UChar *all_rules;
		int32_t length;
		int32_t total;

		icu_to_uchar(&my_rules,
					 descriptor->icu_rules,
					 strlen(descriptor->icu_rules));
		std_rules = lib->ucol_getRules(dyn_collator, &length);
		total = u_strlen(std_rules) + u_strlen(my_rules) + 1;
		all_rules = palloc_array_extended(UChar, total, total, MCXT_ALLOC_NO_OOM);
		if (!all_rules)
		{
			lib->ucol_close(dyn_collator);
			pg_freelocale(std_result);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));			
		}
		u_strcpy(all_rules, std_rules);
		u_strcat(all_rules, my_rules);
		lib->ucol_close(dyn_collator);

		status = U_ZERO_ERROR;
		dyn_collator = lib->ucol_openRules(all_rules, u_strlen(all_rules),
										  UCOL_DEFAULT, UCOL_DEFAULT,
										  NULL, &status);
		if (U_FAILURE(status))
		{
			pg_freelocale(std_result);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not open collator for locale \"%s\" with rules \"%s\": %s",
							descriptor->locale,
							descriptor->icurules,
							lib->u_errorName(status))));			
		}
	}
	
	result = MemoryContextAllocExtended(context,
										sizeof(pg_locale_router_icu_locale) +
										descriptor_size +
										U_MAX_VERSION_STRING_LENGTH,
										MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!result)
	{
		lib->ucol_close(dyn_collator);
		pg_freelocale(std_result);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/*
	 * Copy the internal state of std_result.  This liberty avoids the need to
	 * forward all the ctype functions, with associated runtime and
	 * maintenance costs.  The ctype_methods functions will receive a pointer
	 * to this copy.
	 *
	 * XXX Alternatively we could define a new ctype_methods that uses the
	 * dynamically loaded ICU library, but that seems a lot less valueable and
	 * harder to maintain.
	 */
	result->locale = *std_result;
	result->ctype_locale = std_result;

	/*
	 * Replace the collation and destruction functions.  Note that these work
	 * with result->dynamic_collator, not result->locale.icu.ucol.
	 */
	result->locale = &pg_locale_router_icu_locale_methods;
	result->collate = &pg_locale_router_icu_collate_methods;

	return &result->locale;
}

#endif
