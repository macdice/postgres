/*-------------------------------------------------------------------------
 *
 * pg_cpu.h
 *	  Identification of CPUs and runtime feature checks.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_cpu.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CPU_H
#define PG_CPU_H

#ifdef HAVE_SCHED_GETCPU
#include <sched.h>
#endif
#ifdef HAVE_GETCPUID
#include <sys/processor.h>
#endif
#ifdef WIN32
#include <windows.h>
#endif

/* Type used to identify logical processors in system APIs. */
#ifndef WIN32

/* Unix systems refer to CPUs with a small zero-indexed integer. */
typedef int pg_cpu_t;
#define PG_CPU_FORMAT "%d"
#define PG_CPU_FORMAT_ARG(x) x
#define PG_CPU_NUMBER(x) x
#define PG_CPU_GROUP(x) 0
#define PG_CPU_AS_INT(x) x
#define PG_CPU_HAS_GROUP false

#else

/*
 * Windows APIs use a two-part identifier in the variants that support more
 * than 64 logical CPUs.  The "Number" part is 0-63, and the "Group" part is
 * 0-N.
 */
typedef PROCESSOR_NUMBER pg_cpu_t;
#define PG_CPU_FORMAT "%d:%d"
#define PG_CPU_FORMAT_ARG(x) (x).Group, (x).Number
#define PG_CPU_NUMBER(x) ((x).Number)
#define PG_CPU_GROUP(x) ((x).Group)
#define PG_CPU_AS_INT(x) (((x).Group << 6) | (x).Number)
#define PG_CPU_HAS_GROUP true

#endif

/*
 * Return the CPU that the caller is running on as of this very instant.
 * Returns 0 if we don't have support on this system.  This can't fail, and is
 * expected to have a fast implementation that doesn't enter the kernel (user
 * mode instructions, vDSO, thread local updated by kernel, etc).
 */
static inline pg_cpu_t
pg_cpu_current(void)
{
#if defined(HAVE_SCHED_GETCPU)
	return sched_getcpu();		/* Linux, FreeBSD */
#elif defined(GETCPUID)
	return getcpuid();			/* Solaris, illumos */
#elif defined(WIN32)
	pg_cpu_t	cpu;

	GetCurrentProcessorNumberEx(&cpu);
	return cpu;
#else
	return 0;
#endif
}

#if defined(USE_SSE2) || defined(__i386__)

typedef enum X86FeatureId
{
	/* Have we run feature detection? */
	INIT_PG_X86,

	/* scalar registers and 128-bit XMM registers */
	PG_SSE4_2,
	PG_POPCNT,

	/* 512-bit ZMM registers */
	PG_AVX512_BW,
	PG_AVX512_VL,
	PG_AVX512_VPCLMULQDQ,
	PG_AVX512_VPOPCNTDQ,
} X86FeatureId;
#define X86FeaturesSize (PG_AVX512_VPOPCNTDQ + 1)

extern PGDLLIMPORT bool X86Features[];

extern void set_x86_features(void);

static inline bool
x86_feature_available(X86FeatureId feature)
{
	if (X86Features[INIT_PG_X86] == false)
		set_x86_features();

	return X86Features[feature];
}

#endif							/* defined(USE_SSE2) || defined(__i386__) */

#endif							/* PG_CPU_H */
