/*-------------------------------------------------------------------------
 *
 * pg_stack_alloc.h
 *		Allocator for objects that don't escape the current function.
 *
 * A palloc()-like interface for allocating memory on the stack.  The initial
 * implementation uses an array declared statically.
 *
 * Once stack space is exhausted, allocations silently fall back to using
 * palloc().  Memory should therefore still be freed explicitly with
 * pg_stack_free() or MemoryContext-level cleanup.  It is a no-op in the
 * common case that pfree() doesn't need to be called.
 *
 * XXX A space-limited version of alloca() could be added.
 *
 * XXX It might be possible to use something like "defer" or equivalent
 * compiler extensions to clean up palloc()'d memory automatically, in future
 * work, and then pg_stack_free() would not be necessary.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/pg_stack_alloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_STACK_ALLOC_H
#define PG_STACK_ALLOC_H

#include "utils/elog.h"
#include "utils/palloc.h"
#include "miscadmin.h"

#include <limits.h>
#include <unistd.h>


/* #define PG_STACK_USE_PALLOC_LOG "/tmp/pg_stack_alloc.csv" */

/* Choose which implementation to use, if not already defined manually. */
#if !defined(PG_STACK_USE_ARRAY) &&									\
	!defined(PG_STACK_USE_PALLOC) &&								\
	!defined(PG_STACK_USE_PALLOC_LOG)
#define PG_STACK_USE_ARRAY
#endif


/*-------------------------------------------------------------------------
 *
 * Public API.
 *
 *-------------------------------------------------------------------------
 */

/* This ensures that "align" is a constant and can't cause overflow. */
#define PG_STACK_MAX_ALIGN 4096

/* Declare a stack allocator with a default size limit. */
#define DECLARE_PG_STACK()												\
	DECLARE_PG_STACK_SIZE(128)

/*
 * As above, but with a caller-supplied limit on stack usage.  The default
 * should be preferred.
 */
#define DECLARE_PG_STACK_SIZE(size) \
	bool pg_stack_maybe_pfree pg_attribute_unused() = false;			\
	size_t pg_stack_let_size;		/* temp, avoids double eval */		\
	DECLARE_PG_STACK_IMPL(size)

/* Allocate memory, optionally with explicit alignment. */
#define pg_stack_alloc(size)											\
	pg_stack_alloc_aligned((size), MAXIMUM_ALIGNOF)
#define pg_stack_alloc_aligned(size, align)								\
	(pg_stack_sanity_checks(align),										\
	 pg_stack_let_size = (size),										\
	 pg_stack_alloc_aligned_impl(pg_stack_let_size, (align)))

/* As above, but also zero the memory. */
#define pg_stack_alloc0(size) \
	pg_stack_alloc0_aligned((size), MAXIMUM_ALIGNOF)
#define pg_stack_alloc0_aligned(size, align)							\
	(pg_stack_sanity_checks(align),										\
	 pg_stack_let_size = (size),										\
	 memset(pg_stack_alloc_aligned_impl(pg_stack_let_size, (align)),	\
			0,															\
			pg_stack_let_size))

/* As above, but for a given type T. */
#define pg_stack_alloc_object(T)										\
	pg_stack_alloc_array(T, 1)
#define pg_stack_alloc0_object(T)										\
	pg_stack_alloc0_array(T, 1)

/* As above, but for an array of objects of size T. */
#define pg_stack_alloc_array(T, n)										\
	(pg_stack_sanity_checks(alignof(T)), 								\
	 StaticAssertExpr(sizeof(n) <= sizeof(size_t), "n too wide"), 		\
	 pg_stack_let_size = pg_stack_T_mul_n(sizeof(T), sizeof(n), (n)),	\
	 pg_stack_alloc_aligned_impl(pg_stack_let_size, alignof(T)))
#define pg_stack_alloc0_array(T, n)										\
	(pg_stack_sanity_checks(alignof(T)), 								\
	 StaticAssertExpr(sizeof(n) <= sizeof(size_t), "n too wide"), 		\
	 pg_stack_let_size = pg_stack_T_mul_n(sizeof(T), sizeof(n), (n)),	\
	 pg_stack_alloc0_aligned(pg_stack_let_size, (alignof(T))))

/* Copy a string. */
#define pg_stack_strdup(cstr)											\
	pg_stack_strdup_with_len((cstr), strlen(cstr))
#define pg_stack_strndup(cstr, n)										\
	pg_stack_strdup_with_len((cstr), strnlen((cstr), (n)))
#define pg_stack_strdup_with_len(data, size)							\
	(pg_stack_sanity_checks(1),											\
	 pg_stack_let_size = (size),										\
	 pg_stack_strdup_with_len_impl(										\
		 pg_stack_alloc_aligned_impl(pg_stack_let_size + 1,				\
									 alignof(char)),					\
		 (data),														\
		 pg_stack_let_size))
#define pg_stack_text_to_cstring(text) \
	pg_stack_strdup_with_len(VARDATA_ANY(text), VARSIZE_ANY_EXHDR(text))
#define pg_stack_text_datum_to_cstring(datum)							\
	pg_stack_text_to_cstring((text *) DatumGetPointer(datum))

/*
 * Free memory allocated with the above interfaces.  We don't expect to
 * receive pointers allocated by palloc() directly and not via this API.  That
 * would break the pg_stack_maybe_pfree optimization, and might limit
 * future implementation techniques.
 */
#define pg_stack_free(ptr)												\
	do																	\
	{																	\
		void *pg_stack_let_ptr = (ptr);									\
		Assert(pg_stack_ptr_p(pg_stack_let_ptr) ||						\
			   pg_stack_maybe_pfree);									\
		if (unlikely(pg_stack_maybe_pfree) &&							\
			!pg_stack_ptr_p(pg_stack_let_ptr))							\
			pfree(pg_stack_let_ptr);									\
	}																	\
	while (0)


/*-------------------------------------------------------------------------
 *
 * Private helper code common to all implementations.
 *
 *-------------------------------------------------------------------------
 */

/*
 * Normal usage would have constant align values, so the first two checks
 * should ideally be static assertions, but they are done this way to allow
 * regress.c to loop through alignment sizes.
 */
#define pg_stack_sanity_checks(align)									\
	(AssertMacro((align) > 0 && (align) <= PG_STACK_MAX_ALIGN),			\
	 AssertMacro(((align) & ((align) - 1)) == 0 /* power-of-two? */))

/* For assertions. */
static inline bool
pg_stack_ptr_is_aligned_p(const void *p, size_t align)
{
	return (uintptr_t) p % align == 0;
}

/* Maximum value that can be stored in an unsigned integer of given size. */
static inline size_t
pg_stack_max_for_uint_size(size_t size)
{
	Assert(size <= sizeof(size_t));
	return SIZE_MAX >> ((sizeof(size_t) * CHAR_BIT) - size * CHAR_BIT);
}

/* Post-allocation part of pg_stack_strdup_with_len(). */
static inline char *
pg_stack_strdup_with_len_impl(char *dst, const char *data, size_t size)
{
	memcpy(dst, data, size);
	dst[size] = 0;
	return dst;
}

/* Compute sizeof(T) * n. */
static inline size_t
pg_stack_T_mul_n(size_t sizeof_T, size_t sizeof_n, size_t n)
{
	/* XXX Could overflow, which might allow the stack to be corrupted. */
	return sizeof_T * n;
}

/*
 * Fall back to palloc() or palloc_aligned() due to lack of space.  We waste a
 * register remembering if we've ever had to do this, to generate better
 * straight-line code for the case where we don't have to free anything.
 */
#define pg_stack_palloc_aligned(size, align)							\
	((pg_stack_maybe_pfree = true),										\
	 ((align) > MAXIMUM_ALIGNOF ?										\
	  palloc_aligned((size), (align), 0) :								\
	  palloc(size)))


/*-------------------------------------------------------------------------
 *
 * Low-level implementations below this point supply the following macros:
 *
 * 1. DECLARE_PG_STACK_IMPL(size)
 * 2. pg_stack_alloc_aligned(size, align)
 * 3. pg_stack_ptr_p(ptr)
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * Toy implementations for debugging.
 *
 *-------------------------------------------------------------------------
 */

/* Just use palloc. */
#ifdef PG_STACK_USE_PALLOC
#define DECLARE_PG_STACK_IMPL(size)
#define pg_stack_alloc_aligned_impl(size, align)						\
	pg_stack_palloc_aligned((size), (align))
#define pg_stack_ptr_p() false
#endif

/*
 * Same, but log "location,function,size,depth" entries to a file, using the
 * PG_STACK_USE_PALLOC_LOG macro's definition (a ""-quoted string literal) for
 * the path.
 */
#ifdef PG_STACK_USE_PALLOC_LOG
#define DECLARE_PG_STACK_IMPL(size)										\
	FILE *pg_stack_log													\
	__attribute__((cleanup(pg_stack_close_log))) =						\
	fopen(PG_STACK_USE_PALLOC_LOG, "a+")
#define pg_stack_alloc_aligned_impl(size, align)						\
	(fprintf(pg_stack_log,												\
			 "%s:%d,%s,%zu,%zu\n",										\
			 __FILE__,													\
			 __LINE__,													\
			 __func__,													\
			 (size_t) (size),											\
			 ((const char *) stack_base_ptr -							\
			  (const char *) __builtin_stack_address())),				\
	 pg_stack_palloc_aligned((size), (align)))
#define pg_stack_ptr_p(ptr) false
static inline void
pg_stack_close_log(FILE **f)
{
	fclose(*f);
}
#endif

/*-------------------------------------------------------------------------
 *
 * Array-based implementation.
 *
 * This is entirely standard C requiring no compiler extensions, but it leaves
 * a big hole in the stack when you call another function and has no ability
 * to respect the total stack size limit.
 *
 *-------------------------------------------------------------------------
 */
#ifdef PG_STACK_USE_ARRAY

/* Required interface macros. */

#define DECLARE_PG_STACK_IMPL(size)										\
	char pg_stack_array[(size)];										\
	char *pg_stack_p = pg_stack_array + (size)

#define pg_stack_alloc_aligned_impl(size, align)						\
	(pg_stack_alloc_aligned_from_array(&pg_stack_array[0],				\
									   &pg_stack_p,						\
									   (size),							\
									   (align)) ?						\
	 pg_stack_p :														\
	 pg_stack_palloc_aligned(size, align))

#define pg_stack_ptr_p(ptr)												\
	((char *) (ptr) >= &pg_stack_array[0] &&							\
	 (char *) (ptr) <= &pg_stack_array[sizeof(pg_stack_array)])


/* Implementation. */

static inline bool
pg_stack_alloc_aligned_from_array(char *array,
								  char **p,
								  size_t size,
								  size_t align)
{
	if (likely(size <= (uintptr_t) *p)) /* avoid overflow with huge size */
	{
		char	   *result = *p - size;

		if (align > 1)
			result = (char *) TYPEALIGN_DOWN(align, result);

		if (likely(result >= array))
		{
			*p = result;
			return true;
		}
	}
	return false;
}

#endif

#endif
