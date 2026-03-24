/*-------------------------------------------------------------------------
 *
 * cpu_affinity.c
 *	  manage CPU affinity
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/cpu_affinity.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/cpu_affinity.h"

/* GUC */
int			cpu_affinity = 1;

/*
 * This is read-only after initialization, so it is shared between child
 * processes without consuming more physical memory (except in EXEC_BACKEND
 * builds).  It could also be placed in shared memory, but it seems better to
 * have it fully configured while other shared memory is being sized.
 */
cpu_table	cpu_affinity_table = {0};

static bool
cpu_affinity_initialized(void)
{
	return cpu_table_num_cpu_sets(&cpu_affinity_table) != 0;
}

void
cpu_affinity_initialize(void)
{
	if (!cpu_affinity_initialized())
		cpu_table_build(&cpu_affinity_table,
						IsBootstrapProcessingMode() ? 0 : cpu_affinity);
}

/*
 * How many CPU sets are configured?  This number can be used to partition
 * contended resources.
 */
int
cpu_affinity_num_cpu_sets(void)
{
	Assert(cpu_affinity_initialized());
	return cpu_table_num_cpu_sets(&cpu_affinity_table);
}

/*
 * Which NUMA node is a CPU set in?  Intended for logging etc.
 */
int
cpu_affinity_get_numa_node(int cpu_set_number)
{
	Assert(cpu_affinity_initialized());
	return cpu_table_get_numa_node(&cpu_affinity_table, cpu_set_number);
}

/*
 * Which CPU set is a CPU in?  Intended for logging, views etc.
 */
int
cpu_affinity_get_cpu_set_for_cpu(pg_cpu_t cpu)
{
	Assert(cpu_affinity_initialized());
	return cpu_table_get_cpu_set_for_cpu(&cpu_affinity_table, cpu);
}

/*
 * Pin the caller to a given CPU set.  Use -1 to unpin, ie return to initial
 * affinity.
 */
void
cpu_affinity_run_on_cpu_set(int cpu_set_number)
{
	Assert(cpu_affinity_initialized());

	if (MyBackendType == B_BACKEND)
		elog(ERROR, "regular backends should not be pinned to CPU sets");

	cpu_table_run_on_cpu_set(&cpu_affinity_table, cpu_set_number);
}
