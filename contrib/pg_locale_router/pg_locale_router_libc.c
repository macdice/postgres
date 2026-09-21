#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"
#include "utils/pg_locale.h"
#include "utils/pg_locale_internal.h"

#include "pg_locale_router.h"

#include <langinfo.h>

/* GUCs. */
static bool search_version_modifier = true;
static char *search_modifiers = "";

void
pg_locale_router_libc_init(void)
{
	DefineCustomBoolVariable("pg_locale_router.libc.search_version_modifier",
							 "Whether to try to open locale@version.",
							 NULL,
							 &search_version_modifier,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomStringVariable("pg_locale_router.libc.search_modifiers",
							   "Comma-separated list of extra locale modifiers to try.",
							   NULL,
							   &search_modifiers,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);
}

/*
 * It's important to check that you've really opened the locale you think you
 * have, because libc is allowed to throw away modifiers it can't find and
 * silently open the bare locale name.  Unfortunately there isn't a standard
 * way to do that (short of hijacking unrelated langinfo strings like YESNO),
 * or another reliable way to detect silent fallback behavior.
 */
static const char *
query_compiled_collate_version(pg_locale_t locale)
{
#if defined(LC_VERSION_MASK)
	/* FreeBSD's localedef -V stores CLDR version here. */
	return querylocale(LC_VERSION_MASK | LC_COLLATE_MASK, locale->lt);
#elif defined(__GLIBC__)
	locale_t	newloc;

	/*
	 * glibc has a special category LC_IDENTIFICATION that contains a
	 * revision, but it usually just reports "1.0".  We invent a new
	 * convention: when cross-compiling locales with localedef, the
	 * LC_IDENTIFICATION "revision" field should be modified to state the
	 * glibc localedata version: "...; localedata=2.41".
	 *
	 * Since we have to open the category and replace locale->lt (which
	 * governs the lifetime of the returned string), this operation has a
	 * side-effect, but it's harmless as no code in PostgreSQL cares about
	 * LC_IDENTIFICATION.
	 */
	newloc = newlocale(LC_IDENTIFICATION_MASK,
					   locale->descriptor.collate,
					   locale->lt);
	if (newloc)
	{
		const char *revision;

		locale->lt = newloc;
		revision = nl_langinfo_l(_NL_IDENTIFICATION_REVISION, locale->lt);
		if (revision)
		{
			const char *localedata;

			localedata = strstr(revision, "; localedata=");
			return localedata ? localedata + 13 : NULL;
		}
	}
#endif

	return NULL;
}

static bool
set_and_check_collate_version(pg_locale_t locale)
{
	locale->collate_version = query_compiled_collate_version(locale);
	if (locale->collate_version == NULL)
		return false;

	return strcmp(locale->collate_version,
				  locale->descriptor.collate_version) == 0;
}

pg_locale_t
pg_locale_router_libc_newlocale(const locale_descriptor *descriptor,
								int flags,
								MemoryContext context,
								pg_newlocale_function std_newlocale)
{
	pg_locale_t result = NULL;
	pg_locale_t alt_result;
	locale_descriptor alt_descriptor;
	char		alt_collate[LOCALE_NAME_BUFLEN];
	const char *modifier;

	/*
	 * Try to open it using the standard routine, ie using the locale names
	 * from the catalog directly.
	 */
	result = std_newlocale(descriptor,
						   PG_NEWLOCALE_FLAGS_NOT_FOUND_OK,
						   context);

	/*
	 * If found, and if there is either no version in the catalog or the
	 * version matches, then no more work is needed.
	 */
	if (result &&
		(descriptor->collate_version == NULL ||
		 (result->collate_version &&
		  strcmp(descriptor->collate_version, result->collate_version) == 0)))
		return result;

	if (search_version_modifier)
	{
		/*
		 * Try to open the locale with the version as a modifier appended to
		 * the locale name.
		 */
		snprintf(alt_collate,
				 sizeof(alt_collate),
				 "%s@%s",
				 descriptor->collate,
				 descriptor->collate_version);
		alt_descriptor = *descriptor;
		alt_descriptor.collate = alt_collate;
		alt_result = std_newlocale(&alt_descriptor,
								   PG_NEWLOCALE_FLAGS_NOT_FOUND_OK,
								   context);

		if (alt_result)
		{
			if (set_and_check_collate_version(alt_result))
			{
				if (result)
					pg_freelocale(result);
				return alt_result;
			}
			pg_freelocale(alt_result);
		}
	}

	/* Try a list of configured modifiers. */
	modifier = search_modifiers;
	while (*modifier)
	{
		const char *comma = strchr(modifier, ',');
		int			modifier_len = comma ? comma - modifier : strlen(modifier);

		/* Skip @ if user wrote it that way. */
		if (*modifier == '@')
		{
			modifier++;
			modifier_len--;
		}

		snprintf(alt_collate,
				 sizeof(alt_collate),
				 "%s@%.*s",
				 descriptor->collate,
				 modifier_len,
				 modifier);
		alt_descriptor = *descriptor;
		alt_descriptor.collate = alt_collate;
		alt_result = std_newlocale(&alt_descriptor,
								   PG_NEWLOCALE_FLAGS_NOT_FOUND_OK,
								   context);

		if (alt_result)
		{
			if (set_and_check_collate_version(alt_result))
			{
				if (result)
					pg_freelocale(result);
				return alt_result;
			}
			pg_freelocale(alt_result);
		}

		if (!comma)
			break;
		modifier = comma + 1;
		while (*modifier == ' ')
			modifier++;
	}

	if (!result && !(flags & PG_NEWLOCALE_FLAGS_NOT_FOUND_OK))
		report_newlocale_failure(descriptor->collate);

	return result;
}
