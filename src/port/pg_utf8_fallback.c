/*-------------------------------------------------------------------------
 *
 * pg_utf8_fallback.c
 *	  Validate UTF-8 using plain C.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_utf8_fallback.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include "port/pg_utf8.h"


int
pg_utf8_verifychar(const unsigned char *s, int len)
{
	return pg_utf8_verifychar_internal(s, len);
}

/*
 * See the comment in common/wchar.c under "multibyte sequence validators".
 */
int
pg_validate_utf8_fallback(const unsigned char *s, int len)
{
	const unsigned char *start = s;

	/*
	 * Fast path for when we have enough bytes left in the string to give
	 * check_ascii() a chance to advance the pointer. This also allows the
	 * functions in this loop to skip length checks.
	 */
	while (len >= sizeof(uint64))
	{
		int			l;

		/* fast path for ASCII-subset characters */
		l = check_ascii(s, sizeof(uint64));
		if (l)
		{
			s += l;
			len -= l;
			continue;
		}

		/*
		 * Found non-ASCII or zero above, so verify a single character. By
		 * passing length as constant, the compiler should optimize away the
		 * length-checks in pg_utf8_verifychar_internal.
		 */
		l = pg_utf8_verifychar_internal(s, sizeof(uint64));
		if (l == -1)
			goto end;

		s += l;
		len -= l;
	}

	/* Slow path to handle the last few bytes in the string */
	while (len > 0)
	{
		int			l;

		l = pg_utf8_verifychar(s, len);
		if (l == -1)
			goto end;

		s += l;
		len -= l;
	}

end:
	return s - start;
}
