/*-------------------------------------------------------------------------
 *
 * palloc.h
 *	  POSTGRES memory allocator definitions.
 *
 * This file contains the basic memory allocation interface that is
 * needed by almost every backend module.  It is included directly by
 * postgres.h, so the definitions here are automatically available
 * everywhere.  Keep it lean!
 *
 * Memory allocation occurs within "contexts".  Every chunk obtained from
 * palloc()/MemoryContextAlloc() is allocated within a specific context.
 * The entire contents of a context can be freed easily and quickly by
 * resetting or deleting the context --- this is both faster and less
 * prone to memory-leakage bugs than releasing chunks individually.
 * We organize contexts into context trees to allow fine-grain control
 * over chunk lifetime while preserving the certainty that we will free
 * everything that should be freed.  See utils/mmgr/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/palloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PALLOC_H
#define PALLOC_H

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.  Most users
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData *MemoryContext;

/*
 * A memory context can have callback functions registered on it.  Any such
 * function will be called once just before the context is next reset or
 * deleted.  The MemoryContextCallback struct describing such a callback
 * typically would be allocated within the context itself, thereby avoiding
 * any need to manage it explicitly (the reset/delete action will free it).
 */
typedef void (*MemoryContextCallbackFunction) (void *arg);

typedef struct MemoryContextCallback
{
	MemoryContextCallbackFunction func; /* function to call */
	void	   *arg;			/* argument to pass it */
	struct MemoryContextCallback *next; /* next in list of callbacks */
} MemoryContextCallback;

/*
 * CurrentMemoryContext is the default allocation context for palloc().
 * Avoid accessing it directly!  Instead, use MemoryContextSwitchTo()
 * to change the setting.
 */
extern PGDLLIMPORT MemoryContext CurrentMemoryContext;

/*
 * Flags for MemoryContextAllocExtended.
 */
#define MCXT_ALLOC_HUGE			0x01	/* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM		0x02	/* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO			0x04	/* zero allocated memory */

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
extern void *MemoryContextAlloc(MemoryContext context, Size size);
extern void *MemoryContextAllocZero(MemoryContext context, Size size);
extern void *MemoryContextAllocExtended(MemoryContext context,
										Size size, int flags);
extern void *MemoryContextAllocAligned(MemoryContext context,
									   Size size, Size alignto, int flags);

extern void *palloc(Size size);
extern void *palloc0(Size size);
extern void *palloc_extended(Size size, int flags);
extern void *palloc_aligned(Size size, Size alignto, int flags);
pg_nodiscard extern void *repalloc(void *pointer, Size size);
pg_nodiscard extern void *repalloc_extended(void *pointer,
											Size size, int flags);
pg_nodiscard extern void *repalloc0(void *pointer, Size oldsize, Size size);
extern void pfree(void *pointer);

#ifndef FRONTEND

/*
 * Variants with easier notation and more type safety, backend version that
 * support T with strict alignment.
 */

/*
 * Allocate space for one object of type "T"
 */
#define palloc_object(T) palloc_array(T, 1)
#define palloc0_object(T) palloc0_array(T, 1)

/*
 * Allocate space for "n" objects of type "T"
 */
#define palloc_array(T, n)												\
	((T *)																\
	 (alignof(T) > MAXIMUM_ALIGNOF ?									\
	  palloc_aligned(sizeof(T) * (n), alignof(T), 0) :					\
	  palloc(sizeof(T) * (n))))
#define palloc0_array(T, n)												\
	((T *)																\
	 (alignof(T) > MAXIMUM_ALIGNOF ?									\
	  palloc_aligned(sizeof(T) * (n), alignof(T), MCXT_ALLOC_ZERO) :	\
	  palloc0(sizeof(T) * (n))))

#define sizeof_flexible(T, FA, n)							\
	(offsetof(T, FA) + sizeof(((T *) 0)->FA[0]) * (n))

/*
 * Allocate space for one object of type "T" including its flexible array
 * member "FA" with space for "n" elements.
 */
#define palloc_flexible_object(T, FA, n)								\
	((T *)																\
	 (alignof(T) > MAXIMUM_ALIGNOF ?									\
	  palloc_aligned(sizeof_flexible(T, FA, (n)), alignof(T), 0) :		\
	  palloc(sizeof_flexible(T, FA, (n)))))
/* Variant that zeroes the object but not the flexible array. */
#define palloc0_flexible_object(T, FA, n)								\
	((T *)																\
	 (memset((alignof(T) > MAXIMUM_ALIGNOF ?							\
			  palloc_aligned(sizeof_flexible(T, FA, (n)),				\
							 alignof(T),								\
							 0) :										\
			  palloc(sizeof_flexible(T, FA, (n)))),						\
			 0,															\
			 offsetof(T, FA))))
/* Variant that also zeroes the flexible array. */
#define palloc00_flexible_object(T, FA, n)								\
	((T *)																\
	 (alignof(T) > MAXIMUM_ALIGNOF ?									\
	  palloc_aligned(sizeof_flexible(T, FA, (n)),						\
					 alignof(T),										\
					 MCXT_ALLOC_ZERO) :									\
	  palloc0(sizeof_flexible(T, FA, (n)))))

/*
 * Change size of allocation pointed to by "pointer" to have space for "n"
 * objects of type "T"
 */
#define repalloc_array(pointer, T, n)									\
	(StaticAssertExpr(alignof(T) <= MAXIMUM_ALIGNOF,					\
					  "strict-aligning repalloc_array unimplemented"),	\
	 ((T *) repalloc(pointer, sizeof(T) * (n))))
#define repalloc0_array(pointer, T, old_n, n)							\
	(StaticAssertExpr(alignof(T) <= MAXIMUM_ALIGNOF,					\
					  "strict-aligning repalloc0_array unimplemented"),	\
	 ((T *) repalloc0(pointer, sizeof(T) * (old_n), sizeof(T) * (n))))

/*
 * Change size of flexible object pointed to by "pointer" to have a flexible
 * array FA of size "n".
 */
#define repalloc_flexible_object(T, FA, pointer, n)						\
	(StaticAssertExpr(alignof(T) <= MAXIMUM_ALIGNOF,					\
					  "strict-aligning repalloc_flexible_object unimplemented"), \
	 ((T *) repalloc((pointer), sizeof_flexible(T, FA, (n)))))

#endif

/* Higher-limit allocators. */
extern void *MemoryContextAllocHuge(MemoryContext context, Size size);
pg_nodiscard extern void *repalloc_huge(void *pointer, Size size);

/*
 * Although this header file is nominally backend-only, certain frontend
 * programs like pg_controldata include it via postgres.h.  For some compilers
 * it's necessary to hide the inline definition of MemoryContextSwitchTo in
 * this scenario; hence the #ifndef FRONTEND.
 */

#ifndef FRONTEND
static inline MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
	MemoryContext old = CurrentMemoryContext;

	CurrentMemoryContext = context;
	return old;
}
#endif							/* FRONTEND */

/* Registration of memory context reset/delete callbacks */
extern void MemoryContextRegisterResetCallback(MemoryContext context,
											   MemoryContextCallback *cb);
extern void MemoryContextUnregisterResetCallback(MemoryContext context,
												 MemoryContextCallback *cb);

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
extern char *MemoryContextStrdup(MemoryContext context, const char *string);
extern char *pstrdup(const char *in);
extern char *pnstrdup(const char *in, Size len);

extern char *pchomp(const char *in);

/* sprintf into a palloc'd buffer --- these are in psprintf.c */
extern char *psprintf(const char *fmt,...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args) pg_attribute_printf(3, 0);

#endif							/* PALLOC_H */
