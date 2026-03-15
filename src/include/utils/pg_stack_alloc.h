/*-------------------------------------------------------------------------
 *
 * pg_stack_alloc.h
 *		Allocator for objects that don't escape the current lexical scope.
 *
 * A palloc()-like interface to alloca(), for allocating memory efficiently on
 * the stack.  Raw alloca() is usually considered dangerous because of its
 * inherent stack overflow risk, but this interface imposes limits on stack
 * size and falls back to regular palloc() when they would be exceeded.
 *
 * If alloca() is not available on this platform, a simple array-based
 * emulation is used.
 *
 * Memory should still be freed explicitly with pg_stack_free().  It is a
 * no-op in the common case that pfree() doesn't need to be called.
 *
 * XXX It might be possible to use something like "defer" or equivalent
 * compiler extensions to clean up palloc()'d memory automatically, in future
 * work, and then pg_stack_free() would not be necessary.
 *
 * XXX Support for stacks that grow up has not been tested.  We don't current
 * target any such system.
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
#include "utils/memutils.h"					/* for MaxAllocSize */
#include "utils/palloc.h"
#include "miscadmin.h"

#include <limits.h>
#include <unistd.h>


/* #define PG_STACK_USE_PALLOC_LOG "/tmp/pg_stack_alloc.csv" */
/* #define PG_STACK_USE_ARRAY */

/* Spelling and alignment of alloca(). */
#ifdef HAVE__BUILTIN_ALLOCA
#define pg_stack_alloca(size) __builtin_alloca(size)
#define	ALIGNOF_ALLOCA __BIGGEST_ALIGNMENT__
#define PG_STACK_USE_ALLOCA
#elif defined(_MSC_VER)
#include <malloc.h>
#define pg_stack_alloca(size) alloca(size)
#define ALIGNOF_ALLOCA 16
#define PG_STACK_USE_ALLOCA
#endif

/* Choose which implementation to use, if not already defined manually. */
#if !defined(PG_STACK_USE_ARRAY) &&									\
	!defined(PG_STACK_USE_ALLOC) &&									\
	!defined(PG_STACK_USE_PALLOC) &&								\
	!defined(PG_STACK_USE_PALLOC_LOG)
#if PG_STACK_DIRECTION < 0 && defined(pg_stack_alloca)
#define PG_STACK_USE_ALLOCA
#else
#define PG_STACK_USE_ARRAY
#endif
#endif


/*-------------------------------------------------------------------------
 *
 * Public API.
 *
 *-------------------------------------------------------------------------
 */

#define PG_STACK_MAX_ALIGN 4096

/*
 * Declare a stack allocator with a default size limit.  If using the
 * array-based fallback code, use a much smaller limit, because it leaves
 * holds in the stack and has no way to opt out of consuming stack space when
 * the stack is getting too big.  It should hopefully still be useful for
 * temporary values/nulls arrays and strings in common cases, though.
 */
#ifdef PG_STACK_USE_ARRAY
#define DECLARE_PG_STACK()												\
	DECLARE_PG_STACK_SIZE(128)
#else
#define DECLARE_PG_STACK()												\
	DECLARE_PG_STACK_SIZE(1024)
#endif

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
		Assert(pg_stack_addr_p(ptr) || pg_stack_maybe_pfree);			\
		if (unlikely(pg_stack_maybe_pfree) &&							\
			!pg_stack_addr_p(ptr))										\
			pfree(ptr);													\
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
	 AssertMacro(((align) & ((align) - 1)) == 0 /* power-of-two? */),	\
	 StaticAssertExpr(!pg_in_lexical_scope_p(PG_TRY),					\
					  "pg_stack API not allowed in PG_TRY"),			\
	 StaticAssertExpr(!pg_in_lexical_scope_p(PG_CATCH),					\
					  "pg_stack API not allowed in PG_CATCH"),			\
	 StaticAssertExpr(!pg_in_lexical_scope_p(PG_FINALLY),				\
					  "pg_stack API not allowed in PG_FINALLY"))

#define pg_stack_addr_p(ptr)											\
	((char *) (ptr) >= pg_stack_addr_low() &&							\
	 (char *) (ptr) <= pg_stack_addr_high())

/* For assertions. */
static inline bool
pg_stack_is_aligned_p(const void *p, size_t align)
{
	return (uintptr_t) p % align == 0;
}

/* For assertions. */
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

/* Is it impossible for sizeof(T) * maximum possible n to overflow size_t? */
static inline bool
pg_stack_T_mul_n_cannot_overflow_p(size_t sizeof_T, size_t sizeof_n)
{
	/* See static assertion that n is not wider than size_t. */
	if (sizeof_T == 1)
		return true;

	/* Can't overflow if both factors fit in the lower half of size_t. */
	if (sizeof_n <= sizeof(size_t) / 2 &&
		(sizeof_T <= pg_stack_max_for_uint_size(sizeof(size_t) / 2)))
		return true;

	return false;
}

/* Would sizeof(T) * n overflow? */
static inline bool
pg_stack_T_mul_n_overflows_p(size_t sizeof_T, size_t n)
{
	return n > SIZE_MAX / sizeof_T;
}

/* Compute sizeof(T) * n or raise an error if that would overflow size_t. */
static inline size_t
pg_stack_T_mul_n(size_t sizeof_T, size_t sizeof_n, size_t n)
{
	size_t		result;

	/*
	 * These functions are split up so that we can sanity-check them
	 * individually on 32-bit CI.  For the common case of sizeof(n) == 4 and
	 * sizeof(size_t) == 8, this should reduce to simple multiplication.
	 * 32-bit systems can only skip the runtime test for sizeof(T) == 1,
	 * sizeof(n) == 2 or constexpr n <= UINT16_MAX.
	 */
	if (pg_stack_T_mul_n_cannot_overflow_p(sizeof_T, sizeof_n) ||
		!pg_stack_T_mul_n_overflows_p(sizeof_T, n))
		result = sizeof_T * n;
	else
		elog(ERROR, "pg_stack_alloc: %zu * %zu would overflow size_t",
			 sizeof_T, n);

	/*
	 * Explain this is terms that GCC's -Werror=stringop-overflow understands,
	 * so it doesn't warn when passing the value to memset().
	 */
	pg_assume(result == n ||
			  result <= pg_stack_max_for_uint_size(sizeof_n));

	return result;
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
	  palloc(size)))			/* can't ask for smaller alignment */


/*-------------------------------------------------------------------------
 *
 * Low-level implementations below this point supply the following macros:
 *
 * 1. DECLARE_PG_STACK_IMPL(size)
 * 2. pg_stack_alloc_aligned(size, align)
 * 3. pg_stack_addr_low()
 * 4. pg_stack_addr_high()
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
#define pg_stack_alloc_aligned_impl(size, align)	\
	pg_stack_palloc_aligned((size), (align))
#define pg_stack_addr_low() NULL
#define pg_stack_addr_high() NULL
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
#define pg_stack_addr_low() NULL
#define pg_stack_addr_high() NULL
static inline void
pg_stack_close_log(FILE **f)
{
	fclose(*f);
}
#endif

/*-------------------------------------------------------------------------
 *
 * Array-based fallback implementation.
 *
 * This is entirely standard C requiring no compiler extensions, but it leaves
 * a big hole in the stack when you call another function and has no ability
 * to respect the total stack size limit so we have to be much more cautious
 * about sizing when we use it.
 *
 *-------------------------------------------------------------------------
 */
#ifdef PG_STACK_USE_ARRAY

#define DECLARE_PG_STACK_IMPL(size)										\
	char pg_stack_array[(size)];										\
	char *pg_stack_sp = pg_stack_array + (size)

#define pg_stack_alloc_aligned_impl(size, align)						\
	(pg_stack_alloc_aligned_from_array(&pg_stack_array[0],				\
									   &pg_stack_sp,					\
									   (size),							\
									   (align)) ?						\
	 pg_stack_sp :														\
	 pg_stack_palloc_aligned(size, align))

#define pg_stack_addr_low()	 (&pg_stack_array[0])
#define pg_stack_addr_high() (&pg_stack_array[sizeof(pg_stack_array)])


static inline bool
pg_stack_alloc_aligned_from_array(const char *array,
								  char **sp,
								  size_t size,
								  size_t align)
{
	if (likely(size <= (uintptr_t) *sp))	/* avoids overflow with huge size */
	{
		char	   *result = *sp - size;

		if (align > 1)
			result = (char *) TYPEALIGN_DOWN(align, result);

		if (likely(result >= array))
		{
			*sp = result;
			return true;
		}
	}
	return false;
}

#endif

/*-------------------------------------------------------------------------
 *
 * alloca()-based implementation.
 *
 *-------------------------------------------------------------------------
 */
#ifdef PG_STACK_USE_ALLOCA

/* Required interface macros. */

#define DECLARE_PG_STACK_IMPL(size)										\
	pg_stack_declare_impl												\
	const char *pg_stack_limit =										\
		Max((const char *) stack_soft_limit_ptr,						\
			 pg_stack_sp - (size))

#define pg_stack_alloc_aligned_impl(size, align)						\
	(likely(pg_stack_alloca_would_fit_p(pg_stack_sp,					\
										pg_stack_limit,					\
										(size), (align))) ?				\
	 pg_stack_alloca_aligned((size), (align)) :							\
	 pg_stack_palloc_aligned((size), (align)))

#define pg_stack_addr_low()	 pg_stack_sp
#define pg_stack_addr_high() pg_stack_base


/* Extra compiler features we can use to skip some work. */

#ifdef HAVE__BUILTIN_FRAME_ADDRESS
#define pg_stack_base ((const char *) __builtin_frame_address(0))
#endif

#if pg_has_builtin(__builtin_stack_addressXXXX)
#define pg_stack_sp ((const char *) __builtin_stack_address())
#elif 0
/* XXX MSVC, Clang? */
#define pg_stack_sp ((const char *) pg_stack_alloca(0))
#endif


/* Implementation. */

#if defined(pg_stack_sp)
/* Easy case: no need to declare our own stack pointer variable. */
#define pg_stack_declare_impl
/*
 * For default alignment, this collapses to plain alloca().  For stricter
 * alignment, padding is added and the result is realigned.
 */
#define pg_stack_alloca_aligned(size, align)							\
	pg_stack_realign(pg_stack_alloca(pg_stack_pad((size), (align))), (align))
#else
/*
 * Fallback case: declare a variable and compute the stack pointer from the
 * result of every alloca() call.
 *
 * As an initial guess, give it a pointer to a variable on the
 * stack with stack pointer alignment, so that pg_stack_limit gets that
 * alignment too, enabling a small optimization in pg_stack_pad().
 *
 * In practice, alloca()'s first result may be less deep than this initial
 * value (if eg alloca() scribbles over variables whose storage is not needed
 * because they are never spilled).  That's OK: pg_stack_alloca_would_fit_p()
 * will be a bit too conservative on the very first allocation, but after that
 * it'll have the true stack pointer.
 */
#define pg_stack_declare_impl											\
	char *pg_stack_sp =	pg_stack_alloca(0);
/*
 * Remember alloca()'s result as the lower bound of the stack, and return it,
 * realigned if requested.
 */
#define pg_stack_alloca_aligned(size, align)							\
	(pg_stack_sp = pg_stack_alloca(pg_stack_pad((size), (align))),		\
	 pg_stack_realign(pg_stack_sp, (align)))
#endif

#if !defined(pg_stack_base)
/*
 * We can't read the stack frame address, so we need to use something else for
 * bounds checking.  The address of a variable on the stack wouldn't work,
 * because if the compiler knows it will never spill to it, alloca() might
 * allocate on the wrong side of it.  Adding a fudge factor might help, but
 * there doesn't seem to be a principled way to pick one.  So we just use
 * check_stack_depth.c's base pointer, which was certainly placed on the stack
 * before anything this in this stack frame.
  */
#define pg_stack_base ((const char *) stack_base_ptr)
#endif

/* Add padding for pg_stack_realign(). */
static inline size_t
pg_stack_pad(size_t size, size_t align)
{
	if (align <= ALIGNOF_ALLOCA)
		return size;

	return TYPEALIGN(align, size + align - ALIGNOF_ALLOCA);
}

/* Relign alloca()'s result if necessary. */
static inline void *
pg_stack_realign(void *ptr, size_t align)
{
	Assert(pg_stack_is_aligned_p(ptr, ALIGNOF_ALLOCA));

	if (align  <= ALIGNOF_ALLOCA)
		return ptr;

	return (void *) TYPEALIGN(align, ptr);
}

/*
 * Estimate the new stack pointer after a proposed alloca(), for the purpose
 * of comparing it with pg_stack_limit.
 *
 * This assumes that alloca() doesn't move the stack pointer down any further
 * than it needs to for ALIGNOF_ALLOCA, but at least GCC seems to move it one
 * ALIGNOF_ALLOCA step further than it needs to.  That's OK for our purposes,
 * it just means that our memory limit is slightly fuzzy.  You can't overrun it
 * by much.
 */
static inline const char *
pg_stack_estimate_sp(const char *sp, size_t size, size_t align)
{
	Assert((uintptr_t) sp >= pg_stack_pad(size, align));

	return sp - pg_stack_pad(size, align);
}

/* Would we overflow pg_stack_estimate_sp()'s arithmetic? */
static inline bool
pg_stack_alloca_would_overflow_p(const char *sp, size_t size, size_t align)
{
	if (align <= ALIGNOF_ALLOCA)
	{
		Assert(pg_stack_pad(size, align) == size);
		return size > (uintptr_t) sp;
	}

	/* Don't let pg_stack_pad() overflow. */
	if (size > MaxAllocSize)
		return true;

	/* Don't let pg_stack_estimate_sp() underflow. */
	return pg_stack_pad(size, align) > (uintptr_t) sp;
}

/* Would a proposed alloca() call exceed our limit? */
static inline bool
pg_stack_alloca_would_fit_p(const char *sp, const char *limit,
							size_t size, size_t align)
{
	return !pg_stack_alloca_would_overflow_p(sp, size, align) &&
		pg_stack_estimate_sp(sp, size, align) >= limit;
}

#endif

#endif
