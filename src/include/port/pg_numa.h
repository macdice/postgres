/*-------------------------------------------------------------------------
 *
 * pg_numa.h
 *	  Basic NUMA portability routines
 *
 *
 * Copyright (c) 2025-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 	src/include/port/pg_numa.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_NUMA_H
#define PG_NUMA_H

#ifdef HAVE_GETCPU
#include <sched.h>
#endif
#ifdef WIN32
#include <windows.h>
#endif

#include "port/pg_cpu.h"
#include "port/pg_cpuset.h"

#if defined(USE_LIBNUMA) || defined(WIN32)
#define PG_NUMA_HAVE_WORKING_GET_NODE_FOR_CPU
#endif

extern PGDLLIMPORT int pg_numa_init(void);
extern PGDLLIMPORT int pg_numa_query_pages(int pid, unsigned long count, void **pages, int *status);
extern PGDLLIMPORT int pg_numa_get_max_node(void);
extern PGDLLIMPORT int pg_numa_get_node_for_cpu(pg_cpu_t cpu);
extern PGDLLIMPORT int pg_numa_get_cpus_for_node(int node, pg_cpuset_t *cpuset);

typedef struct pg_numa_opaque_policy
{
	char		buffer[128];
} pg_numa_opaque_policy;

extern PGDLLIMPORT int pg_numa_save_policy(pg_numa_opaque_policy *policy);
extern PGDLLIMPORT int pg_numa_restore_policy(const pg_numa_opaque_policy *policy);

extern PGDLLIMPORT int pg_numa_set_policy_prefer(int node);
extern PGDLLIMPORT int pg_numa_set_policy_interleave(void);
extern PGDLLIMPORT int pg_numa_set_policy_local(void);

#ifdef USE_LIBNUMA

/*
 * This is required on Linux, before pg_numa_query_pages() as we
 * need to page-fault before move_pages(2) syscall returns valid results.
 */
static inline void
pg_numa_touch_mem_if_required(void *ptr)
{
	volatile uint64 touch pg_attribute_unused();

	touch = *(volatile uint64 *) ptr;
}

#else

#define pg_numa_touch_mem_if_required(ptr) \
	do {} while(0)

#endif

/*
 * Report which node the caller is currently running on.  This can't fail, and
 * is expected to have a fast implementation that doesn't enter the kernel
 * (user mode instructions, vDSO, thread local updated by kernel, etc).
 */
static inline int
pg_numa_get_current_node(void)
{
#ifdef HAVE_GETCPU
#define PG_NUMA_HAVE_WORKING_GET_CURRENT_NODE
	unsigned int node;

	/* The only specified error is EFAULT. */
	getcpu(NULL, &node);
	return node;
#elif defined(WIN32)
#define PG_NUMA_HAVE_WORKING_GET_CURRENT_NODE
	pg_cpu_t	cpu = pg_cpu_current();
	USHORT		node;

	return GetNumaProcessorNodeEx(&cpu, &node) ? node : 0;
#else
	return 0;
#endif
}

#endif							/* PG_NUMA_H */
