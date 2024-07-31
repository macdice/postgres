/*-------------------------------------------------------------------------
 *
 * spin_delay.h
 *	   Implementation of architecture-specific spinlock delay.
 *
 * Note to implementors: the default implementation does nothing.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	  src/include/port/spin_delay.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPIN_DELAY_H
#define SPIN_DELAY_H

static pg_attribute_always_inline void
pg_spin_delay(void)
{
#if defined(__GNUC__) || defined(__INTEL_COMPILER)
#ifdef __i386__					/* 32-bit i386 */
	/*
	 * This sequence is equivalent to the PAUSE instruction ("rep" is ignored
	 * by old IA32 processors if the following instruction is not a string
	 * operation); the IA-32 Architecture Software Developer's Manual, Vol. 3,
	 * Section 7.7.2 describes why using PAUSE in the inner loop of a spin
	 * lock is necessary for good performance:
	 *
	 * The PAUSE instruction improves the performance of IA-32 processors
	 * supporting Hyper-Threading Technology when executing spin-wait loops
	 * and other routines where one thread is accessing a shared lock or
	 * semaphore in a tight polling loop. When executing a spin-wait loop, the
	 * processor can suffer a severe performance penalty when exiting the loop
	 * because it detects a possible memory order violation and flushes the
	 * core processor's pipeline. The PAUSE instruction provides a hint to the
	 * processor that the code sequence is a spin-wait loop. The processor
	 * uses this hint to avoid the memory order violation and prevent the
	 * pipeline flush. In addition, the PAUSE instruction de-pipelines the
	 * spin-wait loop to prevent it from consuming execution resources
	 * excessively.
	 */
	__asm__ __volatile__(
						 " rep; nop			\n");
#endif							/* __i386__ */
#ifdef __x86_64__				/* AMD Opteron, Intel EM64T */

	/*
	 * Adding a PAUSE in the spin delay loop is demonstrably a no-op on
	 * Opteron, but it may be of some use on EM64T, so we keep it.
	 */
	__asm__ __volatile__(
						 " rep; nop			\n");
#endif							/* __x86_64__ */
#if defined(__aarch64__)

	/*
	 * Using an ISB instruction to delay in spinlock loops appears beneficial
	 * on high-core-count ARM64 processors.  It seems mostly a wash for
	 * smaller gear, and ISB doesn't exist at all on pre-v7 ARM chips.
	 */
	__asm__ __volatile__(
						 " isb;				\n");
#endif							/* __aarch64__ */
#endif							/* defined(__GNUC__) ||
								 * defined(__INTEL_COMPILER) */

#ifdef _MSC_VER

	/*
	 * If using Visual C++ on Win64, inline assembly is unavailable.  Use a
	 * _mm_pause intrinsic instead of rep nop.
	 */
#if defined(_WIN64)
	_mm_pause();
#else
	/* See comment for gcc code. Same code, MASM syntax */
	__asm		rep nop;
#endif
#endif							/* _MSC_VER */
}

/* Architectures on which a relaxed load is recommended while spinning. */
#if defined(__i386__) || defined(__x86_64__) || \
	defined(_M_IX86) || defined(_M_AMD64) || \
	defined(__ppc__) || defined(__powerpc__) || \
	defined(__ppc64__) || defined(__powerpc64__)
#define PG_SPIN_TRY_RELAXED
#endif

#endif							/* SPIN_DELAY_H */
