/*-------------------------------------------------------------------------
 *
 * string_iterator.h
 *	  Tools for iterating over text strings as char, char16_t or char32_t.
 *
 * Support for UTF-16 and UTF-32 is degraded if the database encoding is not
 * UTF8: only the 8-bit subset (LATIN1) or 7-bit subset of Unicode that can be
 * cast directly is supported, and out-of-range codepoints raise errors.
 *
 * XXX Data provision via callbacks could be investigated as a way to support
 * incremental or deferred detoasting with centralized infrastructure, to
 * avoid the need to open-code detoasting optimizations at every site.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/mb/string_iterator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STRING_ITERATOR_H
#define STRING_ITERATOR_H

#include "mb/pg_wchar.h"
#include "mb/unicode_types.h"

/* A device for iterating over multibyte strings. */
typedef struct mb_iterator
{
	const char *p;
	const char *end;
	int			encoding;
	char16_t	surrogate;
} mb_iterator;

/* An iterator for iterating over UTF-16 strings. */
typedef struct char16_iterator
{
	const storage_char16_t *p;
	const storage_char16_t *end;
} char16_iterator;

/* Static initializers for the above. */
#define MB_ITERATOR_INIT(data, size, encoding) \
	{(data), (data) + (size), (encoding)}
#define MB_ITERATOR_INIT_LOCAL(data, size) \
	{(data), (data) + (size), GetDatabaseEncoding()}
#define CHAR16_ITERATOR_INIT(data, size) {(data), (data) + (size)}

/* Function pointer types useful for creating specializations. */
typedef char32_t (*mb_iterator_next_char32_t_fn) (mb_iterator *);
typedef size_t (*mb_iterator_store_char16_fn) (mb_iterator *, storage_char16_t *);
typedef char32_t (*char16_iterator_next_char32_t_fn) (char16_iterator *);
typedef size_t (*char16_iterator_store_mb_fn) (char16_iterator *, char *, int encoding);

static inline void
mb_iterator_begin(mb_iterator *iterator, const char *data, size_t size)
{
	iterator->p = data;
	iterator->end = data + size;
	iterator->surrogate = 0;
}

static inline bool
mb_iterator_has_more(mb_iterator *iterator)
{
	return iterator->p < iterator->end || iterator->surrogate;
}

static inline void
mb_iterator_check_ascii_range(mb_iterator *iterator,
							  const char *target_encoding,
							  unsigned char c)
{
	if (unlikely(c > 0x7f))
		elog(ERROR,
			 "no conversion from \"%s\" to \"%s\" is available for the sequence beginning 0x%02x",
			 pg_encoding_to_char(iterator->encoding),
			 target_encoding,
			 c);
}

/* Store a character at dst and return byte count. */
static inline size_t
mb_iterator_store(mb_iterator *iterator, char *dst)
{
	size_t		size;

	Assert(mb_iterator_has_more(iterator));
	size = pg_mblen_range(iterator->p, iterator->end);
	memcpy(dst, iterator->p, size);
	iterator->p += size;
	return size;
}

static inline size_t
mb_iterator_store__sb(mb_iterator *iterator, char *dst)
{
	Assert(pg_encoding_max_length(iterator->encoding) == 1);
	Assert(mb_iterator_has_more(iterator));
	*dst = *iterator->p++;
	return 1;
}

/* Store a character in memory before dst and return byte count. */
static inline size_t
mb_iterator_store_before(mb_iterator *iterator, char *dst)
{
	size_t		size;

	Assert(mb_iterator_has_more(iterator));
	size = pg_mblen_range(iterator->p, iterator->end);
	dst -= size;
	memcpy(dst, iterator->p, size);
	iterator->p += size;
	return size;
}

static inline size_t
mb_iterator_store_before__sb(mb_iterator *iterator, char *dst)
{
	Assert(pg_encoding_max_length(iterator->encoding) == 1);
	Assert(mb_iterator_has_more(iterator));
	dst--;
	*dst = *iterator->p++;
	return 1;
}

static inline char32_t
mb_iterator_next_char32_t__ascii(mb_iterator *iterator)
{
	unsigned char c;

	Assert(mb_iterator_has_more(iterator));

	/* ASCII can be cast directly to char32_t, after 7-bit range check. */
	c = *iterator->p++;
	mb_iterator_check_ascii_range(iterator, "UTF-32", c);
	return c;
}

static inline char32_t
mb_iterator_next_char32_t__latin1(mb_iterator *iterator)
{
	Assert(iterator->encoding == PG_LATIN1);
	Assert(mb_iterator_has_more(iterator));

	/* LATIN1 (unsigned) can be cast directly to char32_t. */
	return (unsigned char) *iterator->p++;
}

static inline char32_t
mb_iterator_next_char32_t__utf8(mb_iterator *iterator)
{
	const char *p = iterator->p;
	size_t		size;

	Assert(iterator->encoding == PG_UTF8);
	Assert(mb_iterator_has_more(iterator));

	size = utf8_len_from_lead_byte(*p);
	if (p + size > iterator->end)
		report_invalid_encoding(iterator->encoding, p, iterator->end - p);
	iterator->p += size;

	return utf8_to_unicode((unsigned char *) p);
}

static inline char32_t
mb_iterator_next_char32_t(mb_iterator *iterator)
{
	switch (iterator->encoding)
	{
		case PG_UTF8:
			return mb_iterator_next_char32_t__utf8(iterator);
		case PG_LATIN1:
			return mb_iterator_next_char32_t__latin1(iterator);
		default:
			return mb_iterator_next_char32_t__ascii(iterator);
	}
}

static inline char16_t
mb_iterator_next_char16_t__ascii(mb_iterator *iterator)
{
	unsigned char c;

	Assert(mb_iterator_has_more(iterator));

	/* ASCII can be cast directly to char16_t after 7-bit range check. */
	c = *iterator->p++;
	mb_iterator_check_ascii_range(iterator, "UTF-16", c);
	return *iterator->p++;
}

static inline char16_t
mb_iterator_next_char16_t__latin1(mb_iterator *iterator)
{
	unsigned char c;

	Assert(mb_iterator_has_more(iterator));
	c = *iterator->p++;

	/* LATIN1 (unsigned) can be cast directly to char16_t. */
	return c;
}

static inline char16_t
mb_iterator_next_char16_t__utf8(mb_iterator *iterator)
{
	char32_t	codepoint;

	Assert(iterator->encoding == PG_UTF8);
	Assert(mb_iterator_has_more(iterator));

	if (unlikely(iterator->surrogate))
	{
		char16_t	result = iterator->surrogate;

		iterator->surrogate = 0;
		return result;
	}

	codepoint = mb_iterator_next_char32_t__utf8(iterator);
	if (unlikely(codepoint_has_surrogate_pair(codepoint)))
	{
		char16_t	result;

		codepoint_to_surrogate_pair(&result, &iterator->surrogate, codepoint);
		return result;
	}

	return codepoint;
}

static inline char16_t
mb_iterator_next_char16_t(mb_iterator *iterator)
{
	switch (iterator->encoding)
	{
		case PG_UTF8:
			return mb_iterator_next_char16_t__utf8(iterator);
		case PG_LATIN1:
			return mb_iterator_next_char16_t__latin1(iterator);
		default:
			return mb_iterator_next_char16_t__ascii(iterator);
	}
}

static inline size_t
mb_iterator_store_char16__ascii(mb_iterator *iterator, storage_char16_t *dst)
{
	unsigned char c;

	Assert(mb_iterator_has_more(iterator));

	/* ASCII can be cast directly to char16_t after 7-bit range check. */
	c = *iterator->p++;
	mb_iterator_check_ascii_range(iterator, "UTF-16", c);
	char16_store(dst, c);
	return 1;
}

static inline size_t
mb_iterator_store_char16__latin1(mb_iterator *iterator, storage_char16_t *dst)
{
	unsigned char c;

	Assert(iterator->encoding == PG_LATIN1);
	Assert(mb_iterator_has_more(iterator));

	/* LATIN1 (unsigned) can be cast directly to char16_t. */
	c = *iterator->p++;
	char16_store(dst, c);
	return 1;
}

static inline size_t
mb_iterator_store_char16__utf8(mb_iterator *iterator, storage_char16_t *dst)
{
	char32_t	codepoint;

	Assert(iterator->encoding == PG_UTF8);
	Assert(mb_iterator_has_more(iterator));

	codepoint = mb_iterator_next_char32_t__utf8(iterator);
	if (unlikely(codepoint_has_surrogate_pair(codepoint)))
	{
		char16_t	codepoint1;
		char16_t	codepoint2;

		codepoint_to_surrogate_pair(&codepoint1, &codepoint2, codepoint);
		char16_store(&dst[0], codepoint1);
		char16_store(&dst[1], codepoint2);
		return 2;
	}

	char16_store(dst, codepoint);
	return 1;
}

static inline char16_t
mb_iterator_store_char16(mb_iterator *iterator, storage_char16_t *dst)
{
	switch (iterator->encoding)
	{
		case PG_UTF8:
			return mb_iterator_store_char16__utf8(iterator, dst);
		case PG_LATIN1:
			return mb_iterator_store_char16__latin1(iterator, dst);
		default:
			return mb_iterator_store_char16__ascii(iterator, dst);
	}
}

static inline void
char16_iterator_begin(char16_iterator *iterator,
					  const storage_char16_t *data,
					  size_t size)
{
	iterator->p = data;
	iterator->end = data + size;
}

static inline bool
char16_iterator_has_more(char16_iterator *iterator)
{
	return iterator->p < iterator->end;
}

static inline void
char16_iterator_report_short_pair(char16_t codepoint1)
{
	elog(ERROR, "invalid UTF-16 sequence 0x%04x", codepoint1);
}

static inline void
char16_iterator_report_bad_pair(char16_t codepoint1, char16_t codepoint2)
{
	elog(ERROR, "invalid UTF-16 sequence 0x%04x 0x%04x",
		 codepoint1, codepoint2);
}

static inline char32_t
char16_iterator_next_char32_t(char16_iterator *iterator)
{
	char32_t	codepoint;

	Assert(char16_iterator_has_more(iterator));
	codepoint = char16_load(iterator->p++);

	if (unlikely(is_utf16_surrogate_first(codepoint)))
	{
		if (!char16_iterator_has_more(iterator))
			char16_iterator_report_short_pair(codepoint);
		codepoint = surrogate_pair_to_codepoint(codepoint,
												char16_load(iterator->p++));
	}

	return codepoint;
}

static inline char16_t
char16_iterator_next_char16_t(char16_iterator *iterator)
{
	Assert(char16_iterator_has_more(iterator));
	return char16_load(iterator->p++);
}

static pg_attribute_always_inline size_t
char16_iterator_store_mb__sb(char16_iterator *iterator, char *dst,
							 unsigned char max_char, int encoding)
{
	char16_t	codepoint;

	Assert(char16_iterator_has_more(iterator));

	codepoint = char16_load(iterator->p++);
	if (unlikely(codepoint > max_char))
		elog(ERROR,
			 "no conversion from \"UTF-16\" to \"%s\" is available for the codepoint %04x",
			 pg_encoding_to_char(encoding),
			 codepoint);
	*dst = codepoint;

	return 1;
}

static inline size_t
char16_iterator_store_mb__ascii(char16_iterator *iterator, char *dst,
								int encoding)
{
	/* Unicode can be cast to ASCII after 7-bit range check. */
	return char16_iterator_store_mb__sb(iterator, dst, 0x7f, encoding);
}

static inline size_t
char16_iterator_store_mb__latin1(char16_iterator *iterator, char *dst,
								 int encoding)
{
	/* Unicode can be cast to LATIN1 after 8-bit range check. */
	Assert(encoding == PG_LATIN1);
	return char16_iterator_store_mb__sb(iterator, dst, 0xff, encoding);
}

static inline size_t
char16_iterator_store_mb__utf8(char16_iterator *iterator, char *dst,
							   int encoding)
{
	char32_t	codepoint;

	Assert(encoding == PG_UTF8);
	Assert(char16_iterator_has_more(iterator));

	codepoint = char16_load(iterator->p++);

	/* Start of a surrogate pair? */
	if (unlikely(is_utf16_surrogate_first(codepoint)))
	{
		char16_t	codepoint2;

		if (unlikely(!char16_iterator_has_more(iterator)))
			char16_iterator_report_short_pair(codepoint);
		codepoint2 = char16_load(iterator->p++);
		if (unlikely(!is_utf16_surrogate_second(codepoint2)))
			char16_iterator_report_bad_pair(codepoint, codepoint2);
		codepoint = surrogate_pair_to_codepoint(codepoint, codepoint2);
	}

	unicode_to_utf8(codepoint, (unsigned char *) dst);
	return unicode_utf8len(codepoint);
}

/*
 * The destination must have space for MAX_MB_LEN_PER_UTF32_CODEPOINT bytes,
 * because UTF-16 surrogate pairs are combined to UTF-32.
 *
 * Call one of the specializations directly to avoid dispatching overhead.
 */
static inline size_t
char16_iterator_store_mb(char16_iterator *iterator, char *dst, int encoding)
{
	switch (encoding)
	{
		case PG_UTF8:
			return char16_iterator_store_mb__utf8(iterator, dst, encoding);
		case PG_LATIN1:
			return char16_iterator_store_mb__latin1(iterator, dst, encoding);
		default:
			return char16_iterator_store_mb__ascii(iterator, dst, encoding);
	}
}

/*
 * char16_iterator_store_mb() for database encoding.
 */
static inline size_t
char16_iterator_store_local(char16_iterator *iterator, char *dst)
{
	return char16_iterator_store_mb(iterator, dst, GetDatabaseEncoding());
}

/*
 * Skip one UTF-32 codepoint.  The iterator must not be exhausted.
 */
static inline void
char16_iterator_advance(char16_iterator *iterator)
{
	char16_t	codepoint;

	Assert(char16_iterator_has_more(iterator));
	codepoint = char16_load(iterator->p++);
	if (unlikely(is_utf16_surrogate_first(codepoint)))
	{
		if (!char16_iterator_has_more(iterator))
			char16_iterator_report_short_pair(codepoint);
		iterator->p++;
	}
}

/*
 * Skip as many UTF-32 codepoints as possible, returning the number that were
 * skipped before the string ended.
 */
static inline size_t
char16_iterator_advance_n(char16_iterator *iterator, size_t n)
{
	size_t		distance = 0;

	while (n > 0 && char16_iterator_has_more(iterator))
	{
		char16_iterator_advance(iterator);
		distance++;
		n--;
	}

	return distance;
}

#endif							/* STRING_ITERATOR_H */
