/*-------------------------------------------------------------------------
 *
 * pg_cpuset.h
 *	  Portable interface for controlling CPU affinity.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/pg_cpuset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CPUSET_H
#define PG_CPUSET_H

#if defined(HAVE_SCHED_GETAFFINITY) || defined(HAVE_CPUSET_GETAFFINITY)
#include <sched.h>
#endif

#if defined(WIN32)
#include <windows.h>
#endif

#include "port/pg_cpu.h"


#if defined(HAVE_SCHED_GETAFFINITY)
typedef cpu_set_t pg_cpuset_t;	/* Linux */
#elif defined(HAVE_CPUSET_GETAFFINITY)
typedef cpuset_t pg_cpuset_t;	/* FreeBSD */
#elif defined(WIN32)
typedef struct pg_cpuset_t		/* Windows */
{
	USHORT		count;

	/*
	 * In theory we should call GetMaximumProcessorGroupCount(), but it is
	 * convenient to use a fixed-sized representation.  20 groups of 64 CPUs
	 * is enough for 1280 CPUs, similar to CPU_SETSIZE on the current versions
	 * of the other OSes.  pg_cpuset_getaffinity() will fail if this is
	 * insufficient.
	 */
	GROUP_AFFINITY masks[20];
} pg_cpuset_t;
#else
typedef int pg_cpuset_t;		/* no support */
#endif

typedef struct pg_cpuset_iterator
{
#if defined(CPU_SETSIZE)
	pg_cpuset_t empty;
	pg_cpuset_t remaining;
	pg_cpu_t	next;
#elif defined(WIN32)
	const pg_cpuset_t *cpuset;
	int			index;
	int			next_processor;
#else
	int			dummy;
#endif
} pg_cpuset_iterator;

extern int	pg_cpuset_get_process_affinity_self(pg_cpuset_t *cpuset);
extern int	pg_cpuset_set_process_affinity_self(const pg_cpuset_t *cpuset);

extern int	pg_cpuset_get_thread_affinity_self(pg_cpuset_t *cpuset);
extern int	pg_cpuset_set_thread_affinity_self(const pg_cpuset_t *cpuset);

extern void pg_cpuset_initialize(pg_cpuset_t *cpuset);
extern bool pg_cpuset_is_empty(const pg_cpuset_t *cpuset);
extern int	pg_cpuset_count(const pg_cpuset_t *cpuset);
extern bool pg_cpuset_contains(const pg_cpuset_t *cpuset, pg_cpu_t cpu);
extern void pg_cpuset_add(pg_cpuset_t *cpuset, pg_cpu_t cpu);
extern void pg_cpuset_remove(pg_cpuset_t *cpuset, pg_cpu_t cpu);
extern void pg_cpuset_and(pg_cpuset_t *a, const pg_cpuset_t *b);

extern void pg_cpuset_iterator_begin(const pg_cpuset_t *cpuset,
									 pg_cpuset_iterator *iter);
extern bool pg_cpuset_iterator_has_next(const pg_cpuset_iterator *iter);
extern pg_cpu_t pg_cpuset_iterator_next(pg_cpuset_iterator *iter);

#endif							/* PG_CPUSET_H */
