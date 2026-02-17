/*-------------------------------------------------------------------------
 *
 * unicode_types.h
 *	  Types for representing Unicode.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/mb/unicode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNICODE_TYPES_H
#define UNICODE_TYPES_H

/*
 * The type used to represent UTF-16 codepoints in varlena objects.  All
 * access to storage_char16_t and knowledge of its layout should be contained
 * in this file.
 */
typedef struct storage_char16_t
{
	uint8_t		high;
	uint8_t		low;
} storage_char16_t;

static_assert(alignof(storage_char16_t) == 1, "bad alignment for varlena");
static_assert(sizeof(storage_char16_t) == sizeof(char16_t), "bad size");

/* Read char16_t from storage_char16_t. */
static inline char16_t
char16_load(const storage_char16_t *s)
{
	return (s->high << 8) | s->low;
}

/* Write char16_t to storage_char16_t. */
static inline void
char16_store(storage_char16_t *s, char16_t c)
{
	s->high = c >> 8;
	s->low = c;
}

/* Codepoint order of two storage_char16_t strings of equal size. */
static inline int
char16_cmp1(const storage_char16_t *s1,
			const storage_char16_t *s2,
			size_t size)
{
	/* Fast binary comparison, motivating big-endian representation. */
	return memcmp(s1, s2, sizeof(storage_char16_t) * size);
}

/* Tell ICU's UCharIterator how to read from storage_char16_t format. */
#define UITER_SET_STORAGE_CHAR_T(iterator, s, size) \
	uiter_setUTF16BE((iterator), \
					 (const char *) (s), \
					 (size) * sizeof(storage_char16_t))

#endif							/* UNICODE_TYPES_H */
