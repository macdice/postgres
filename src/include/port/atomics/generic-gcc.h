/*-------------------------------------------------------------------------
 *
 * generic-gcc.h
 *	  Compiler barriers for GCC (or compatible)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/atomics/generic-gcc.h
 *
 *-------------------------------------------------------------------------
 */

/* intentionally no include guards, should only be included by atomics.h */
#ifndef INSIDE_ATOMICS_H
#error "should be included via atomics.h"
#endif

/*
 * An empty asm block should be a sufficient compiler barrier.
 */
#define pg_compiler_barrier_impl()	__asm__ __volatile__("" ::: "memory")
