#include "postgres.h"

#include "fmgr.h"
#include "utils/pg_locale.h"
#include "utils/pg_locale_internal.h"

#include "pg_locale_router.h"

pg_locale_t
pg_locale_router_newlocale_libc(const locale_descriptor *descriptor,
								int flags,
								MemoryContext context,
								pg_newlocale_function std_newlocale)
{
	pg_locale_t result;
	pg_locale_t alt_result;
	locale_descriptor alt_descriptor;
	char		alt_collate[LOCALE_NAME_BUFLEN];

	/*
	 * Try to open it using the standard routine, ie using the locale names
	 * from the catalog.
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

	/*
	 * Try to open the locale with a version modifier appended to the locale
	 * name used for LC_COLLATE.  (For this to actually work, the user must
	 * make such a locale available to libc, eg by compiling it with POSIX
	 * localedef.)
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

	/*
	 * If not found, give up and let pg_locale.c complain about the version
	 * mismatch.
	 */
	if (!alt_result)
	{
		/*
		 * If we didn't find either of them, complain about the unmodified
		 * version.
		 */
		if (!result && (flags & PG_NEWLOCALE_FLAGS_NOT_FOUND_OK) == 0)
			report_newlocale_failure(descriptor->collate);

		return result;
	}

	/*
	 * Take the modifier as the authoritative version.
	 *
	 * XXX Could do other things instead...
	 */
	alt_result->collate_version = alt_result->descriptor.collate_version;

	/* Log this redirection. */
	elog(DEBUG1,
		 "pg_locale_router: collation \"%s\": using libc locale \"%s\" (version: %s) instead of instead of \"%s\" (%s%s) for LC_COLLATE",
		 alt_result->descriptor.name,
		 alt_result->descriptor.collate,
		 alt_result->descriptor.collate_version,
		 descriptor->collate,
		 result ? "version: " : "not found",
		 result ? result->collate_version : "");

	/* We don't need the original, if we found one. */
	if (result)
		pg_freelocale(result);

	return alt_result;
}
