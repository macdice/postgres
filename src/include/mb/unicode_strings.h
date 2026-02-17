/*-------------------------------------------------------------------------
 *
 * unicode_strings.h
 *	  Support functions for converting and comparing Unicode encodings.
 *
 * Limited support is available in all database encodings, but only the ASCII
 * or LATIN1 range that maps directly to Unicode.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/mb/unicode_strings.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNICODE_STRINGS_H
#define UNICODE_STRINGS_H

#include "mb/string_iterator.h"
#include "mb/unicode_types.h"

/*
 * Constants used for worst-case buffer management for conversions.  Single
 * UTF-16 codepoints map to 1, 2 or 3-byte UTF-8 sequences (basic plane).
 * UTF-16 surrogate pairs map to 4-byte UTF-8 sequences, but that works out to
 * 2 bytes of UTF-8 per UTF-16 codepoint.
 */
#define MAX_CHAR16_CODEPOINTS_PER_UTF8_BYTE 1
#define MAX_CHAR16_CODEPOINTS_PER_UTF8_CHAR 2
#define MAX_UTF8_LEN_PER_CHAR16_CODEPOINT   3
#define MAX_UTF8_LEN_PER_CHAR32_CODEPOINT   4

/*
 * Since we don't currently support transcodings other than ASCII and LATIN1
 * (which are strict subsets of Unicode by definition), the corresponding
 * values for other encodings are 1.  We still define and use these more
 * general macro names, in anticipation of potential transcoding support.
 */
#define MAX_MB_LEN_PER_CHAR16_CODEPOINT   MAX_UTF8_LEN_PER_CHAR16_CODEPOINT
#define MAX_MB_LEN_PER_CHAR32_CODEPOINT   MAX_UTF8_LEN_PER_CHAR32_CODEPOINT
#define MAX_CHAR16_CODEPOINTS_PER_MB_BYTE MAX_CHAR16_CODEPOINTS_PER_UTF8_BYTE
#define MAX_CHAR32_CODEPOINTS_PER_MB_BYTE MAX_CHAR32_CODEPOINTS_PER_UTF8_BYTE

/*
 * How many UTF-16 codepoints might the given database encoding string occupy?
 * TODO: Also provide an _exact_size() function?
 */
static inline size_t
mb_to_char16_max_size(size_t size)
{
	return size * MAX_CHAR16_CODEPOINTS_PER_MB_BYTE;
}

/*
 * How many bytes of database encoding might the given UTF-16 string occupy?
 * TODO: Also provide an _exact_size() function?
 */
static inline size_t
char16_to_mb_max_size(size_t size)
{
	return size * MAX_MB_LEN_PER_CHAR16_CODEPOINT;
}

static pg_attribute_always_inline size_t
mb_to_char16__template(storage_char16_t *dst,
					   const char *src, size_t src_size, int src_encoding,
					   mb_iterator_store_char16_fn store_char16)
{
	mb_iterator iter = MB_ITERATOR_INIT(src, src_size, src_encoding);
	storage_char16_t *p = dst;

	while (mb_iterator_has_more(&iter))
		p += store_char16(&iter, p);

	return p - dst;
}

#define GENERATE_MB_TO_CHAR16(encoding) \
static inline size_t \
mb_to_char16__##encoding(storage_char16_t *dst, \
						const char *src, size_t src_size, int src_encoding) \
{ \
	return mb_to_char16__template(dst, src, src_size, src_encoding, \
								 mb_iterator_store_char16__##encoding); \
}
GENERATE_MB_TO_CHAR16(utf8);
GENERATE_MB_TO_CHAR16(latin1);
GENERATE_MB_TO_CHAR16(ascii);

static inline size_t
mb_to_char16(storage_char16_t *dst,
			 const char *src, size_t src_size, int src_encoding)
{
	switch (src_encoding)
	{
		case PG_UTF8:
			return mb_to_char16__utf8(dst, src, src_size, src_encoding);
		case PG_LATIN1:
			return mb_to_char16__latin1(dst, src, src_size, src_encoding);
		default:
			return mb_to_char16__ascii(dst, src, src_size, src_encoding);
	}
}

static inline size_t
local_to_char16(storage_char16_t *dst, const char *src, size_t src_size)
{
	return mb_to_char16(dst, src, src_size, GetDatabaseEncoding());
}

static pg_attribute_always_inline size_t
char16_to_mb__template(char *dst, int dst_encoding,
					   const storage_char16_t *src, size_t src_size,
					   char16_iterator_store_mb_fn store_mb)
{
	char16_iterator iter = CHAR16_ITERATOR_INIT(src, src_size);
	char	   *p = dst;

	while (char16_iterator_has_more(&iter))
		p += store_mb(&iter, p, dst_encoding);

	return p - dst;
}

/*
 * Try to inline char16_iterator_store_mb__XXX specializations into
 * char16_to_local__XXX specializations.
 */
#define GENERATE_CHAR16_TO_MB(encoding) \
static inline size_t \
char16_to_mb__##encoding(char *dst, int dst_encoding, \
						const storage_char16_t *src, size_t src_size) \
{ \
	return char16_to_mb__template(dst, dst_encoding, src, src_size, \
								 char16_iterator_store_mb__##encoding); \
}
GENERATE_CHAR16_TO_MB(ascii);
GENERATE_CHAR16_TO_MB(latin1);
GENERATE_CHAR16_TO_MB(utf8);

static inline size_t
char16_to_mb(char *dst, int dst_encoding,
			 const storage_char16_t *src, size_t src_size)
{
	switch (dst_encoding)
	{
		case PG_UTF8:
			return char16_to_mb__utf8(dst, dst_encoding, src, src_size);
		case PG_LATIN1:
			return char16_to_mb__latin1(dst, dst_encoding, src, src_size);
		default:
			return char16_to_mb__ascii(dst, dst_encoding, src, src_size);
	}
}

static inline size_t
char16_to_local(char *dst, const storage_char16_t *src, size_t src_size)
{
	return char16_to_mb(dst, GetDatabaseEncoding(), src, src_size);
}

static inline size_t
char16_to_local_cstr(char *dst, const storage_char16_t *src, size_t src_size)
{
	size_t		size = char16_to_local(dst, src, src_size);

	dst[size] = 0;
	return size;
}

static pg_attribute_always_inline int
char16_mb_cmp__template(const storage_char16_t *data1, size_t size1,
						const char *data2, size_t size2, int encoding2,
						mb_iterator_next_char32_t_fn next_char32_t)
{
	char16_iterator iter1 = CHAR16_ITERATOR_INIT(data1, size1);
	mb_iterator iter2 = MB_ITERATOR_INIT(data2, size2, encoding2);

	while (char16_iterator_has_more(&iter1) &&
		   mb_iterator_has_more(&iter2))
	{
		char32_t	codepoint1 = char16_iterator_next_char32_t(&iter1);
		char32_t	codepoint2 = next_char32_t(&iter2);

		if (codepoint1 < codepoint2)
			return -1;
		else if (codepoint1 > codepoint2)
			return 1;
	}

	if (mb_iterator_has_more(&iter2))
		return -1;
	else if (char16_iterator_has_more(&iter1))
		return 1;

	return 0;
}

/*
 * Try to inline char16_iterator_store_mb__XXX specializations into
 * char16_mb_cmp__XXX specializations.
 */
#define GENERATE_CHAR16_MB_CMP(encoding) \
static inline size_t \
char16_mb_cmp__##encoding(const storage_char16_t *data1, size_t size1, \
						 const char *data2, size_t size2, int encoding2) \
{ \
	return char16_mb_cmp__template(data1, size1, data2, size2, encoding2, \
								  mb_iterator_next_char32_t__##encoding); \
}
GENERATE_CHAR16_MB_CMP(ascii);
GENERATE_CHAR16_MB_CMP(latin1);
GENERATE_CHAR16_MB_CMP(utf8);

static inline int
char16_mb_cmp(const storage_char16_t *data1, size_t size1,
			  const char *data2, size_t size2, int encoding2)
{
	switch (encoding2)
	{
		case PG_UTF8:
			return char16_mb_cmp__utf8(data1, size1, data2, size2, encoding2);
		case PG_LATIN1:
			return char16_mb_cmp__latin1(data1, size1, data2, size2, encoding2);
		default:
			return char16_mb_cmp__ascii(data1, size1, data2, size2, encoding2);
	}
}

static inline int
mb_char16_cmp(const char *data1, size_t size1, int encoding1,
			  const storage_char16_t *data2, size_t size2)
{
	int			result = char16_mb_cmp(data2, size2, data1, size1, encoding1);

	INVERT_COMPARE_RESULT(result);
	return result;
}

static inline int
char16_local_cmp(const storage_char16_t *data1, size_t size1,
				 const char *data2, size_t size2)
{
	return char16_mb_cmp(data1, size1, data2, size2, GetDatabaseEncoding());
}

static inline int
local_char16_cmp(const char *data1, size_t size1,
				 const storage_char16_t *data2, size_t size2)
{
	return mb_char16_cmp(data1, size1, GetDatabaseEncoding(), data2, size2);
}

#endif							/* UNICODE_STRINGS_H */
