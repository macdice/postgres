/*-------------------------------------------------------------------------
 *
 * htm.h
 *	  Hardware transaction memory operations.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/htm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HTM_H
#define HTM_H

#ifdef FRONTEND
#error "htm.h may not be included from frontend code"
#endif

#ifndef HAVE_HTM

/*
 * Make this a constant, so that if you build without --enable-htm, all
 * relevant branches are removed by constant folding.
 */
#define have_htm_support false

#else

/*
 * We have to check if the microarchitecture supports HTM instructions
 * with a runtime check.  We'll store the result in a global variable.
 */
extern bool have_htm_support;

/* Function called at startup to set the above variable. */
extern void htm_init(void);

/*
 * A future version of the C programming language might standardize
 * the interface to transactional memory (see eg N1961), but for now we
 * must use compiler builtins that vary.
 */
#if (defined(__GNUC__) || defined(__INTEL_COMPILER) || defined(_MSC_VER)) && !(defined(__IBMC__) || defined(__IBMCPP__))
/* ICC, GCC, MSVC use Intel's _xbegin() interfaces for x86 instructions. */
/* TODO: Only tested on GCC; is the header the same on the others?  Is there a minimum version for each compiler? */
#include <immintrin.h>
#define pg_htm_begin() (_xbegin() == _XBEGIN_STARTED)
#define pg_htm_commit() _xend()
#define pg_htm_abort() _xabort(0)
#elif (defined(__IBMC__) || defined(__IBMCPP__))
/* IBM XLC uses __TM_begin() etc for POWER instructions. */
#error "IBM compiler support for HTM not yet implemented"
#else
#error "no hardware transactional memory support"
#endif

#endif							/* HAVE_HTM */

#endif							/* HTM_H */
