/*-------------------------------------------------------------------------
 *
 * pg_utf8.h
 *	  Routines for fast validation of UTF-8 text.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_utf8.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_UTF8_H
#define PG_UTF8_H


#if defined(USE_SIMD_UTF8)
/* Use Intel SSE4.2 instructions. */
#define UTF8_VERIFYSTR(s, len) \
	pg_validate_utf8_simd((s), (len))

extern int	pg_validate_utf8_simd(const unsigned char *s, int len);

#elif defined(USE_SIMD_UTF8_WITH_RUNTIME_CHECK)
/*
 * Use Intel SSE 4.2 instructions, but perform a runtime check first
 * to check that they are available.
 */
#define UTF8_VERIFYSTR(s, len) \
	pg_validate_utf8((s), (len))

extern int	(*pg_validate_utf8) (const unsigned char *s, int len);
extern int	pg_validate_utf8_simd(const unsigned char *s, int len);

#else
#define UTF8_VERIFYSTR(s, len) \
	pg_validate_utf8_fallback((s), (len))

#endif							/* USE_SSE42_UTF8 */

/* The following need to be visible everywhere. */

extern int	pg_utf8_verifychar(const unsigned char *s, int len);
extern int	pg_validate_utf8_fallback(const unsigned char *s, int len);

#define IS_CONTINUATION_BYTE(c)	(((c) & 0xC0) == 0x80)
#define IS_TWO_BYTE_LEAD(c)		(((c) & 0xE0) == 0xC0)
#define IS_THREE_BYTE_LEAD(c)	(((c) & 0xF0) == 0xE0)
#define IS_FOUR_BYTE_LEAD(c)	(((c) & 0xF8) == 0xF0)

/* Verify a chunk of bytes for valid ASCII including a zero-byte check. */
static inline int
check_ascii(const unsigned char *s, int len)
{
	uint64		chunk,
				highbits_set,
				highbit_carry;

	if (len >= sizeof(uint64))
	{
		memcpy(&chunk, s, sizeof(uint64));

		/* Check if any bytes in this chunk have the high bit set. */
		highbits_set = chunk & UINT64CONST(0x8080808080808080);
		if (highbits_set)
			return 0;

		/*
		 * Check if there are any zero bytes in this chunk.
		 *
		 * First, add 0x7f to each byte. This sets the high bit in each byte,
		 * unless it was a zero. We already checked that none of the bytes had
		 * the high bit set previously, so the max value each byte can have
		 * after the addition is 0x7f + 0x7f = 0xfe, and we don't need to
		 * worry about carrying over to the next byte.
		 */
		highbit_carry = chunk + UINT64CONST(0x7f7f7f7f7f7f7f7f);

		/* Then check that the high bit is set in each byte. */
		highbit_carry &= UINT64CONST(0x8080808080808080);
		if (highbit_carry == UINT64CONST(0x8080808080808080))
			return sizeof(uint64);
		else
			return 0;
	}
	else
		return 0;
}

/*
 * Workhorse for src/common/pg_utf8_verifychar(). Returns the length of
 * the character at *s in bytes, or -1 on invalid input or premature end
 * of input. Static inline for the benefit of pg_utf8_verifystr().
 */
static inline int
pg_utf8_verifychar_internal(const unsigned char *s, int len)
{
	int			l;
	unsigned char b1,
				b2,
				b3,
				b4;

	if (!IS_HIGHBIT_SET(*s))
	{
		if (*s == '\0')
			return -1;
		l = 1;
	}
	/* code points U+0080 through U+07FF */
	else if (IS_TWO_BYTE_LEAD(*s))
	{
		l = 2;
		if (len < l)
			return -1;

		b1 = *s;
		b2 = *(s + 1);

		if (!IS_CONTINUATION_BYTE(b2))
			return -1;

		/* check 2-byte overlong: 1100.000x.10xx.xxxx */
		if (b1 < 0xC2)
			return -1;
	}
	/* code points U+0800 through U+D7FF and U+E000 through U+FFFF */
	else if (IS_THREE_BYTE_LEAD(*s))
	{
		l = 3;
		if (len < l)
			return -1;

		b1 = *s;
		b2 = *(s + 1);
		b3 = *(s + 2);

		if (!IS_CONTINUATION_BYTE(b2) ||
			!IS_CONTINUATION_BYTE(b3))
			return -1;

		/* check 3-byte overlong: 1110.0000 1001.xxxx 10xx.xxxx */
		if (b1 == 0xE0 && b2 < 0xA0)
			return -1;

		/* check surrogate: 1110.1101 101x.xxxx 10xx.xxxx */
		if (b1 == 0xED && b2 > 0x9F)
			return -1;
	}
	/* code points U+010000 through U+10FFFF */
	else if (IS_FOUR_BYTE_LEAD(*s))
	{
		l = 4;
		if (len < l)
			return -1;

		b1 = *s;
		b2 = *(s + 1);
		b3 = *(s + 2);
		b4 = *(s + 3);

		if (!IS_CONTINUATION_BYTE(b2) ||
			!IS_CONTINUATION_BYTE(b3) ||
			!IS_CONTINUATION_BYTE(b4))
			return -1;

		/*
		 * check 4-byte overlong: 1111.0000 1000.xxxx 10xx.xxxx 10xx.xxxx
		 */
		if (b1 == 0xF0 && b2 < 0x90)
			return -1;

		/* check too large: 1111.0100 1001.xxxx 10xx.xxxx 10xx.xxxx */
		if ((b1 == 0xF4 && b2 > 0x8F) || b1 > 0xF4)
			return -1;
	}
	else
		/* invalid byte */
		return -1;

	return l;
}

#endif							/* PG_UTF8_H */
