/*-----------------------------------------------------------------------
 *
 * PostgreSQL locale-related ICU utilities.
 *
 * src/include/utils/pg_locale.h
 *
 * Copyright (c) 2002-2026, PostgreSQL Global Development Group
 *
 *-----------------------------------------------------------------------
 */
#ifndef PG_LOCALE_ICU_H
#define PG_LOCALE_ICU_H

#ifdef USE_ICU

#include <unicode/ucnv.h>
#include <unicode/uiter.h>

/*
 * How many char16_t units to convert at a time.  The starting size is a guess
 * at a data-dependent trade-off: we want a decent chance of finding a
 * difference with just one conversion, but if we don't then we'll grow
 * rapidly by doubling to amortize the per-conversion overheads.
 */
#define PG_UITER_MB_MIN_CONVERT_SIZE 4
#define PG_UITER_MB_MAX_CONVERT_SIZE 64 * 1024

/* Size of in-place buffer in char16_t units, to avoid allocation. */
#define PG_UITER_MB_SMALL_BUFFER_SIZE (4 + 8 + 16 + 32)

/*
 * State space needed for multibyte string iteration.
 */
typedef struct PgUCharIteratorMultibyteContext
{
	UConverter *converter;

	/*
	 * Source string of length iter->length, which is incrementally discovered
	 * if NUL-terminated.  iter->start is the index of the next byte to
	 * convert.
	 */
	const char *src;
	bool		src_nul_terminated;

	/*
	 * Converted string.  iter->limit is the amount converted so far.
	 * iter->index is the current position.
	 */
    char16_t   *buf;
    int32_t		buf_capacity;
    int32_t		buf_convert_size;
    char16_t	buf_small[PG_UITER_MB_SMALL_BUFFER_SIZE];
} PgUCharIteratorMultibyteContext;

/* pg_locale_icu.c */
extern UConverter *pg_icu_dbencoding_converter(void);
extern int32_t pg_uchar_convert(UConverter *converter,
								UChar *dest, int32_t destlen,
								const char *src, int32_t srclen,
								bool *overflow);

/* pg_locale_icu_iter.c */
extern void pg_uiter_initMultibyteContext(UCharIterator *iter,
										  PgUCharIteratorMultibyteContext *context,
										  UConverter *converter);

/*
 * Given UCharIterator pointer, get the associated context.
 *
 * XXX Would it be better to have a struct PgUCharIteratorMultibyte with
 * UCharIterator as first member so you could cast between the two types,
 * instead of using iter->context as ICU apparently intends?  Then
 * pg_uiter_initMultibyte() and pg_uiter_setDbEncodingString() would not need
 * a pointer to both of them, just a pointer to PgUCharIteratorMultibyte, and
 * when calling ucol_strcollIter() you'd pass it &mb_iter->iterator.
 */
static inline PgUCharIteratorMultibyteContext *
pg_uiter_mb_context(UCharIterator *iter)
{
    return (PgUCharIteratorMultibyteContext *) iter->context;
}

/*
 * Open a string in the configured multibyte encoding.
 * pg_uiter_initMultibyteContext() must have been called first.
 * pg_uiter_close() should be called after use.
 */
static inline void
pg_uiter_openMultibyteString(UCharIterator *iter,
							 const char *string,
							 int32_t length)
{
	PgUCharIteratorMultibyteContext *context = pg_uiter_mb_context(iter);

	/* Source string.  If NUL-terminated, iter->length is computed lazily. */
	context->src = string;
	context->src_nul_terminated = length < 0;

	/* Converted string.  Initially points to internal buffer. */
	context->buf = context->buf_small;
	context->buf_capacity = lengthof(context->buf_small);

	/* Amount to convert at a time (grows as iteration progresses). */
	context->buf_convert_size = PG_UITER_MB_MIN_CONVERT_SIZE;   

	/* Indexes into context->src. */
 	iter->start = 0;
	iter->length = length < 0 ? 0 : length;

	/* Indexes into context->buf. */
	iter->index = 0;
	iter->limit = 0;
}

/*
 * Free temporary buffer memory after using UCharIterator.
 */
static inline void
pg_uiter_close(UCharIterator *iter)
{
	PgUCharIteratorMultibyteContext *context = pg_uiter_mb_context(iter);

	if (context->buf != context->buf_small)
		pfree(context->buf);
}

/*
 * Initialize for database encoding and open a string in one step, for an API
 * more closely resembling uiter_setUTF8(), uiter_setString() etc.  See notes
 * for pg_uiter_openMultibyteString().
 *
 * Consider initializing once and then reusing "iter" and "context" for
 * multiple pg_uiter_openMultibyteString(), pg_uiter_close() sequences
 * instead, to skip some overheads.
 */
static inline void
pg_uiter_setDbEncodingString(UCharIterator *iter,
							 PgUCharIteratorMultibyteContext *context,
							 const char *string,
							 int32_t length)
{
	pg_uiter_initMultibyteContext(iter, context, pg_icu_dbencoding_converter());
	pg_uiter_openMultibyteString(iter, string, length);
}

#endif

#endif
