/*-------------------------------------------------------------------------
 *
 * stack_buffer.h
 *		Memory allocator for small non-escaping objects.
 *
 * This API a palloc()-like interface with alloca()-like space management.
 * Unlike alloca(), it only allocates up to a defined safe limit on the stack
 * before falling back to regular palloc().
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/stack_buffer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STACK_BUFFER_H
#define STACK_BUFFER_H

#include "utils/elog.h"
#include "utils/palloc.h"

#include <limits.h>

/* Choose which implementation to use. */
#if 0
#define STACK_BUFFER_USE_PALLOC
#elif defined(HAVE__BUILTIN_ALLOCA_WITH_ALIGN)
#define STACK_BUFFER_USE_ALLOCA
#elif defined(_MSC_VER)
#include <malloc.h>
#define STACK_BUFFER_USE_ALLOCA
#else
#define STACK_BUFFER_USE_ARRAY
#endif


/*
 * Large enough that common small arrays such as values and nulls lists will
 * fit, but small enough that we feel comfortable putting it on the stack in
 * potentially recursive code.
 */
#define STACK_BUFFER_DEFAULT 128

/*
 * This larger size is intended only for non-recursive uses including
 * conversions to C string format before calling standard library routines.
 * Avoid allowing more than one buffer of this size to be active on the stack
 * at a time.
 */
#define STACK_BUFFER_LARGE 1024

/* Declare a stack buffer of default size. */
#define DECLARE_STACK_BUFFER() \
	DECLARE_STACK_BUFFER_SIZE(STACK_BUFFER_DEFAULT)

/* As above, using the standard "large" size (see notes above). */
#define DECLARE_STACK_BUFFER_LARGE() \
	DECLARE_STACK_BUFFER_SIZE(STACK_BUFFER_LARGE)

/* As above, but with a caller-specified limit on stack usage. */
#define DECLARE_STACK_BUFFER_SIZE(size) \
	size_t stack_buffer_let_size;		/* temp, avoids double eval */	\
	DECLARE_STACK_BUFFER_IMPL(size)

/* Allocate memory, optionally with explicit alignment. */
#define stack_buffer_alloc(size)						\
	stack_buffer_alloc_aligned((size), MAXIMUM_ALIGNOF)
#define stack_buffer_alloc_aligned(size, align)							\
	(stack_buffer_sanity_checks(),										\
	 stack_buffer_let_size = (size),									\
	 stack_buffer_let_size = Max(stack_buffer_let_size, 1),				\
	 stack_buffer_alloc_aligned_impl(stack_buffer_let_size, (align)))

/* As above, but also zero the memory. */
#define stack_buffer_alloc0(size) \
	stack_buffer_alloc0_aligned((size), MAXIMUM_ALIGNOF)
#define stack_buffer_alloc0_aligned(size, align) \
	(stack_buffer_sanity_checks(),										\
	 stack_buffer_let_size = (size),									\
	 stack_buffer_let_size = Max(stack_buffer_let_size, 1),				\
	 memset(stack_buffer_alloc_aligned_impl(stack_buffer_let_size,		\
											(align)),					\
			0,															\
			stack_buffer_let_size))

/* As above, but for a give type T. */
#define stack_buffer_alloc_object(T)			\
	stack_buffer_alloc_array(T, 1)
#define stack_buffer_alloc_array(T, n)								\
	((T *) stack_buffer_alloc_aligned((n) * sizeof(T), alignof(T)))
#define stack_buffer_alloc0_object(T)			\
	stack_buffer_alloc0_array(T, 1)
#define stack_buffer_alloc0_array(T, n)									\
	((T *) stack_buffer_alloc0_aligned((n) * sizeof(T), alignof(T)))

/* Copy a string. */
#define stack_buffer_strdup(cstr)						\
	stack_buffer_strdup_with_len((cstr), strlen(cstr))
#define stack_buffer_strndup(cstr, n)							\
	stack_buffer_strdup_with_len((cstr), strnlen((cstr), (n)))
#define stack_buffer_strdup_with_len(data, size)						\
	(stack_buffer_sanity_checks(),										\
	 stack_buffer_let_size = (size),									\
	 stack_buffer_strdup_with_len_impl(stack_buffer_alloc_aligned_impl(stack_buffer_let_size + 1, \
																	   alignof(char)), \
									   (data),							\
									   stack_buffer_let_size))
#define stack_buffer_text_to_cstring(text) \
	stack_buffer_strdup_with_len(VARDATA_ANY(text), VARSIZE_ANY_EXHDR(text))
#define stack_buffer_text_datum_to_cstring(datum) \
	stack_buffer_text_to_cstring((text *) DatumGetPointer(datum))

/* Free memory allocated with any of the above, or palloc(). */
#ifdef USE_ASSERT_CHECKING
#define stack_buffer_free(ptr)						\
	do												\
	{												\
		*((char *) ptr) = 0xff;						\
		if (unlikely(!stack_buffer_stack_p(ptr)))	\
			pfree(ptr);								\
	}												\
	while (0)
#else
#define stack_buffer_free(ptr)						\
	do												\
	{												\
		if (unlikely(!stack_buffer_stack_p(ptr)))	\
			pfree(ptr);								\
	}												\
	while (0)
#endif

/* Cannot work across setjmp()/longjmp() due to stack cloberring. */
#define stack_buffer_sanity_checks()									\
	(StaticAssertExpr(!pg_in_lexical_scope_p(PG_TRY),					\
					  "stack buffer API not allowed in PG_TRY"),		\
	 StaticAssertExpr(!pg_in_lexical_scope_p(PG_CATCH),					\
					  "stack buffer API not allowed in PG_CATCH"),		\
	 StaticAssertExpr(!pg_in_lexical_scope_p(PG_FINALLY),				\
					  "stack buffer API not allowed in PG_FINALLY"))

/* Post-allocation part of stack_buffer_strdup_with_len(). */
static inline char *
stack_buffer_strdup_with_len_impl(char *dst, const char *data, size_t size)
{
	memcpy(dst, data, size);
	dst[size] = 0;
	return dst;
}

/* Allocate with palloc() or palloc_aligned(). */
#define stack_buffer_palloc_aligned(size, align) \
	((align) > MAXIMUM_ALIGNOF ? \
	 palloc_aligned((size), (align), 0) : \
	 palloc(size))				/* can't ask for smaller alignment */


/*-------------------------------------------------------------------------
 *
 * Implementations below this point supply the following function-like macros,
 * to declare a stack buffer, allocate memory and recognize a pointer that
 * should not be pfree()'d:
 *
 * DECLARE_STACK_BUFFER_IMPL(size)
 * stack_buffer_alloc_aligned_impl(size, align)
 * stack_buffer_stack_p(ptr)
 *
 *-------------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * No-op implementation: always forwards to palloc(), just for testing.
 *
 *-------------------------------------------------------------------------
 */
#ifdef STACK_BUFFER_USE_PALLOC

#define DECLARE_STACK_BUFFER_IMPL(size)

#define stack_buffer_alloc_aligned_impl(size, align) \
	stack_buffer_palloc_aligned((size), (align))

#define stack_buffer_stack_p(ptr) false

#endif

/*-------------------------------------------------------------------------
 *
 * Array-based implementation.
 *
 * Simple and effective, but leaves a big hole in the stack when you call
 * another function.
 *
 *-------------------------------------------------------------------------
 */
#ifdef STACK_BUFFER_USE_ARRAY

#define DECLARE_STACK_BUFFER_IMPL(size)									\
	char stack_buffer_array[(size)];									\
	char *stack_buffer_sp = stack_buffer_array + (size)

#define stack_buffer_stack_p(ptr)										\
	((char *) (ptr) >= &stack_buffer_array[0] &&						\
	 (char *) (ptr) < &stack_buffer_array[sizeof(stack_buffer_array)])

#define stack_buffer_alloc_aligned_impl(size, align)				\
	stack_buffer_alloc_aligned_from_array(&stack_buffer_array[0],	\
										  &stack_buffer_sp,			\
										  (size),					\
										  (align))

static inline void *
stack_buffer_alloc_aligned_from_array(const char *array,
									  char **sp,
									  size_t size,
									  size_t align)
{
	char	   *result = *sp - size;	/* XXX overflow? */

	if (align > 1)
		result = (char *) TYPEALIGN_DOWN(align, result);

	if (likely(result >= array))
	{
		*sp = result;
		return result;
	}

	return stack_buffer_palloc_aligned(size, align);
}

#endif


/*-------------------------------------------------------------------------
 *
 * alloca()-based implementation.
 *
 * This is straightforward when we can read the stack pointer and control
 * alignment with builtins.  Most of this deals with synthesizing those things
 * when they're missing.
 *
 *-------------------------------------------------------------------------
 */
#ifdef STACK_BUFFER_USE_ALLOCA

#ifdef HAVE__BUILTIN_STACK_ADDRESS	/* GCC, Clang 22+ */
#define stack_buffer_get_sp() ((const char *) __builtin_stack_address())
#else
#define STACK_BUFFER_NEED_SP_VARIABLE
#endif


#ifdef STACK_BUFFER_NEED_SP_VARIABLE
#define DECLARE_STACK_BUFFER_IMPL(size)									\
	const size_t stack_buffer_max_size = (size);						\
	const void *stack_buffer_sp pg_attribute_unused() = NULL;			\
	const void *stack_buffer_base pg_attribute_unused() = NULL
#else
#define DECLARE_STACK_BUFFER_IMPL(size)									\
	const size_t stack_buffer_max_size = (size);						\
	const void *stack_buffer_base pg_attribute_unused() = stack_buffer_get_sp()
#endif

#define stack_buffer_stack_p(ptr)			\
	((const char *) (ptr) >= stack_buffer_lower() &&	\
	 (const char *) (ptr) < stack_buffer_upper())

#define stack_buffer_alloc_aligned_impl(size, align)			\
	(likely(stack_buffer_has_space_p((size), (align))) ?		\
	 stack_buffer_alloca_aligned((size), (align)) :				\
	 stack_buffer_palloc_aligned((size), (align)))


#ifdef STACK_BUFFER_NEED_SP_VARIABLE
#define stack_buffer_get_sp() ((const char *) stack_buffer_sp)
static inline void *
stack_buffer_compute_sp(const void **base,
						const void **sp,
						void *p,
						size_t size)
{
	if (*base == NULL)
	{
		if (PG_STACK_DIRECTION < 0)
			*base = (const char *) p + TYPEALIGN(alignof(max_align_t), size);
		else
			*base = p;
	}
	if (PG_STACK_DIRECTION < 0)
		*sp = p;
	else
		*sp = (const char *) p + TYPEALIGN(alignof(max_align_t), size);
	return p;
}
#define stack_buffer_alloca_aligned(size, align)						\
	stack_buffer_compute_sp(&stack_buffer_base,							\
							&stack_buffer_sp,							\
							stack_buffer_alloca_aligned_impl((size),	\
															 (align)),	\
							(size))
#else
#define stack_buffer_alloca_aligned(size, align)						\
	stack_buffer_alloca_aligned_impl((size), (align))
#endif

#if PG_STACK_DIRECTION < 0
#define stack_buffer_lower() stack_buffer_get_sp()
#define stack_buffer_upper() ((const char *) stack_buffer_base)
#else
#define stack_buffer_lower() ((const char *) stack_buffer_base)
#define stack_buffer_upper() stack_buffer_get_sp()
#endif
#define stack_buffer_size()										\
	(AssertMacro(stack_buffer_upper() >= stack_buffer_lower()),	\
	 stack_buffer_upper() - stack_buffer_lower())
#define stack_buffer_has_space_p(size, align)				\
	(stack_buffer_size() + (size) <= stack_buffer_max_size)
#if defined(HAVE__BUILTIN_ALLOCA_WITH_ALIGN)
#define stack_buffer_alloca_aligned_impl(size, align)		\
	__builtin_alloca_with_align((size), (align) * CHAR_BIT)
#else
#define stack_buffer_alloca_aligned_impl(size, align)	\
	((align) > alignof(max_align_t) ?									\
	 ((void *) TYPEALIGN((align), alloca((size) + (align) - 1))) :		\
	 alloca(size))				/* can't ask for smaller alignment */
#endif
#endif

#endif
