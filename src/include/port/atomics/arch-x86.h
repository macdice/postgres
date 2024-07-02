/*-------------------------------------------------------------------------
 *
 * arch-x86.h
 *	  Atomic operations considerations specific to intel x86
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES:
 *
 * src/include/port/atomics/arch-x86.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Both 32 and 64 bit x86 do not allow loads to be reordered with other loads,
 * or stores to be reordered with other stores, but a load can be performed
 * before a subsequent store.
 *
 * Technically, some x86-ish chips support uncached memory access and/or
 * special instructions that are weakly ordered.  In those cases we'd need
 * the read and write barriers to be lfence and sfence.  But since we don't
 * do those things, a compiler barrier should be enough.
 *
 * "lock; addl" has worked for longer than "mfence". It's also rumored to be
 * faster in many scenarios.
 */

#if defined(__GNUC__) || defined(__INTEL_COMPILER)
#if defined(__i386__) || defined(__i386)
#define pg_memory_barrier_impl()		\
	__asm__ __volatile__ ("lock; addl $0,0(%%esp)" : : : "memory", "cc")
#elif defined(__x86_64__)
#define pg_memory_barrier_impl()		\
	__asm__ __volatile__ ("lock; addl $0,0(%%rsp)" : : : "memory", "cc")
#endif
#endif /* defined(__GNUC__) || defined(__INTEL_COMPILER) */

#define pg_read_barrier_impl()		pg_compiler_barrier_impl()
#define pg_write_barrier_impl()		pg_compiler_barrier_impl()
