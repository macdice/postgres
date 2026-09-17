#ifndef PG_LOCALE_ICU_H
#define PG_LOCALE_ICU_H

#ifdef USE_ICU

#include <unicode/ucnv.h>
#include <unicode/uiter.h>

/* Size of in-place buffer in char16_t units, to avoid allocation. */
#define PG_UITER_MB_SMALL_BUFFER_SIZE 32

/*
 * How many char16_t units to convert at a time.  Start small, but big enough
 * that strcoll() has a reasonable chance of finding a difference in the first
 * chunk.
 */
#define PG_UITER_MB_MIN_CONVERT_SIZE 8
#define PG_UITER_MB_MAX_CONVERT_SIZE 64 * 1024

typedef struct PgUCharIteratorMultibyteContext
{
	UConverter *converter;

	const char *src;

    char16_t   *buf;
    int32_t		buf_capacity;
    int32_t		buf_convert_size;
    char16_t	buf_small[PG_UITER_MB_SMALL_BUFFER_SIZE];
} PgUCharIteratorMultibyteContext;

extern void pg_uiter_setMultibyteString(UCharIterator *iter,
										PgUCharIteratorMultibyteContext *context,
										const char *string,
										size_t length);
extern void pg_uiter_endMultibytestring(UCharIterator *iter,
										PgUCharIteratorMultibyteContext *context);
#endif

#endif
