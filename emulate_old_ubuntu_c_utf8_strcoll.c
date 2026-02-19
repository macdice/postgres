/*
 * Kludge to teach PostgreSQL to emulate the strange sort order produced by the
 * defunct C.UTF-8 locale historically shipped by Debian bullseye and Ubuntu
 * 18.04 and older.  Though its definition *looked* OK (it listed every
 * codepoint, in order), but somewhere between localedef and libc it seemed to
 * get very confused and show the weird striping artefacts visible in the table
 * below.
 *
 * Modern glibc and localedef can't seem to reproduce the same order, either by
 * compiling that old "C" definition with modern localedef (which appears to
 * work pretty well for other locales), or compiling a new locale definition
 * containing the table below explicitly.  Messing with the charmap doesn't
 * seem to help either.
 *
 * This replacement can be installed as:
 *
 * session_preload_libraries='emulate_old_ubuntu_c_utf8_strcoll.so'
 *
 * The, whenever someone connects to a database using C.UTF-8 as its default
 * locale, the locale's strncoll function will be monkey-patched by _PG_init()
 * below, affecting the rest of the session.  It doesn't affect other locales.
 *
 * This is a fairly extreme short-term emergency solution to a bizarre problem
 * (the entire point of C.UTF8 was to sort in codepoint order, people used it
 * to avoid upgrade problem due to unstable sorts :-/).
 *
 * Note that modern glibc has proper built-in support for C.UTF-8, and sorts by
 * codepoint order without the huge explicit list.
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/pg_locale.h"

#include <langinfo.h>
#include <limits.h>

struct broken_strcoll_range
{
	uint32_t codepoints[2];
	int order;
};

/*
 * This table was computed from the output of Ubuntu 18.04's /usr/bin/sort with
 * LC_COLLATE set to C.UTF-8, which itself has been committed to this repo for
 * posterity.  The input was from print_all_codepoints.py.  The Makefile runs
 * the sorted file through make_c_table.py to generate codepoint_table.h.
 */
static const struct broken_strcoll_range broken_strcoll_table[] = {
#include "codepoint_table.h"
};

/* Search for the range covering 'codepoint' and return its order. */
static int
lookup(char32_t codepoint)
{
    int low = 0;
    int high = lengthof(broken_strcoll_table);
    int mid = high / 2;
    while (low <= high)
    {
		const struct broken_strcoll_range *range;

        mid = (low + high) / 2;
		range = &broken_strcoll_table[mid];

		if (range->codepoints[0] <= codepoint &&
			range->codepoints[1] < codepoint)
			low = mid + 1;
		else if (range->codepoints[0] > codepoint)
			high = mid - 1;
		else
		{
			Assert(range->codepoints[0] <= codepoint);
			Assert(range->codepoints[1] >= codepoint);
			return range->order + (codepoint - range->codepoints[0]);
		}
    }
    return INT_MAX;
}

/* A function to replace the standard collation function. */
static int
broken_strncoll(const char *begin1, ssize_t maybe_size1,
				const char *begin2, ssize_t maybe_size2,
				pg_locale_t locale)
{
	const size_t size1 = maybe_size1 < 0 ? strlen(begin1) : maybe_size1;
	const size_t size2 = maybe_size2 < 0 ? strlen(begin2) : maybe_size2;
	const char *end1 = begin1 + size1;
	const char *end2 = begin2 + size2;

	/* Compare using our order lookup table. */
	while (begin1 < end1 && begin2 < end2)
	{
		int mblen1 = pg_mblen_range(begin1, end1);
		int mblen2 = pg_mblen_range(begin2, end2);
		int order1 = lookup(utf8_to_unicode((const unsigned char *) begin1));
		int order2 = lookup(utf8_to_unicode((const unsigned char *) begin2));

		if (order1 < order2)
			return -1;
		else if (order1 > order2)
			return 1;

		begin1 += mblen1;
		begin2 += mblen2;
	}

	/* Equal so far.  Tie-break using length. */
	if (begin2 < end2)
		return -1;
	else if (begin1 < end1)
		return 1;

	/* Equal. */
	return 0;
}

PG_MODULE_MAGIC_EXT(
                    .name = "emulate_old_ubuntu_c_utf8_strcoll",
                    .version = PG_VERSION
);

void
_PG_init(void)
{
	static struct collate_methods patched_collate_methods;
	pg_locale_t locale = pg_database_locale();

	/*
	 * Use available clues to detect the libc provider (builtin wouldn't set
	 * collate, and ICU would set strxfrm_is_safe).
	 */
	if (locale->collate && !locale->collate->strxfrm_is_safe)
	{
		const char *locale_name;

		/* A glibc kludge to extract the name (getlocalename_l() is too new). */
		locale_name = nl_langinfo_l(NL_LOCALE_NAME(LC_COLLATE), locale->lt);

		/* Name is case-insensitive, with or without hyphen. */
		if (strcasecmp(locale_name, "C.UTF-8") == 0 ||
			strcasecmp(locale_name, "C.UTF8") == 0)
		{
			/*
			 * Photocopy its internal function table, replace the strncoll
			 * pointer with our own, and inject it back into the locale.  It
			 * remains in place for the lifetime of this backend.
			 */
			patched_collate_methods = *locale->collate;
			patched_collate_methods.strncoll = broken_strncoll;
			locale->collate = &patched_collate_methods;
			elog(LOG, "emulate_old_ubuntu_c_utf8_strcoll.c: active for default locale \"%s\"", locale_name);
		}
	}
}
