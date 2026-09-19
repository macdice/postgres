#ifndef PG_LOCALE_ICU_H
#define PG_LOCALE_ICU_H

#ifdef USE_ICU

#include <unicode/ucnv.h>
#include <unicode/uiter.h>

/* Size of in-place buffer in char16_t units, to avoid allocation. */
#define PG_UITER_MB_SMALL_BUFFER_SIZE 32

/* How many char16_t units to convert at a time. */
#define PG_UITER_MB_MIN_CONVERT_SIZE 4
#define PG_UITER_MB_MAX_CONVERT_SIZE 64 * 1024

typedef struct PgUCharIteratorMultibyteContext
{
	UConverter *converter;

	const char *src;
	bool		src_nul_terminated;

    char16_t   *buf;
    int32_t		buf_capacity;
    int32_t		buf_convert_size;
    char16_t	buf_small[PG_UITER_MB_SMALL_BUFFER_SIZE];
} PgUCharIteratorMultibyteContext;

/* pg_locale_icu_iter.c */
extern void pg_uiter_initMultibyteContext(UCharIterator *iter,
										  PgUCharIteratorMultibyteContext *context,
										  UConverter *converter);
extern void pg_uiter_openMultibyteString(UCharIterator *iter,
										 const char *string,
										 int32_t length);
extern void pg_uiter_close(UCharIterator *iter);

extern void pg_uiter_setDbEncodingString(UCharIterator *iter,
										 PgUCharIteratorMultibyteContext *context,
										 const char *string,
										 int32_t length);

/* pg_locale_icu.c */
extern UConverter *pg_icu_dbencoding_converter(void);

#endif

#endif
