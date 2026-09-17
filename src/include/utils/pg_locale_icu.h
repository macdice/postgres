#ifndef PG_LOCALE_ICU_H
#define PG_LOCALE_ICU_H

#ifdef USE_ICU

#include <unicode/ucnv.h>
#include <unicode/uiter.h>

/* Size of in-place buffer in char16_t units, to avoid allocation. */
#define PG_UCONV_SMALL_BUFFER_SIZE 32

/*
 * How many char16_t units to convert at a time.  Start small, but big enough
 * that strcoll() has a reasonable chance of finding a difference in the first
 * chunk.
 */
#define PG_UCONV_MIN_CONVERT_SIZE 8
#define PG_UCONV_MAX_CONVERT_SIZE 64 * 1024

typedef struct PgUCharConverter
{
	UCharIterator iterator;

    UConverter *converter;

    char16_t   *buf;
    int32_t		buf_capacity;
    int32_t		buf_convert_size;
    char16_t	buf_small[PG_UCONV_SMALL_BUFFER_SIZE];
} PgUCharConverter;

extern void pg_uconv_init(PgUCharConverter *uconv, UConverter *converter);

/*
 * Set the string to convert.
 */
static inline void
pg_uconv_begin(PgUCharConverter *uconv, const char *string, size_t length)
{
	uconv->iterator.context = string;
	uconv->iterator.start = 0;
	uconv->iterator.length = length;

	uconv->iterator.index = 0;
	uconv->iterator.limit = 0;

	uconv->buf = uconv->buf_small;
	uconv->buf_capacity = lengthof(uconv->buf_small);
	uconv->buf_convert_size = PG_UCONV_MIN_CONVERT_SIZE;

	/*
	 * A reusable uconv object might have undigested bytes in its converter
	 * after an earlier usage was abandoned early, so reset it.
	 */
	ucnv_reset(uconv->converter);
}

/*
 * Free resources.
 */
static inline void
pg_uconv_end(PgUCharConverter *uconv)
{
	if (uconv->buf != uconv->buf_small)
		pfree(uconv->buf);
}

/*
 * Return an iterator suitable for use by ICU collation routines.
 */
static inline UCharIterator *
pg_uconv_iterator(PgUCharConverter *uconv)
{
	return &uconv->iterator;
}

#endif

#endif
