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
#include "storage/shmem.h"

/* GUC */
int			cpu_affinity = 1;

CpuAffinityControl *cpu_affinity_control;

size_t
CpuAffinityShmemSize(void)
{
	return sizeof(CpuAffinityControl);
}

void
CpuAffinityShmemInit(void)
{
	size_t		size = CpuAffinityShmemSize();
	bool		found;

	cpu_affinity_control = ShmemInitStruct("CpuAffinityControl", size, &found);
	if (!found)
		pg_cpuset_table_build(&cpu_affinity_control->table, cpu_affinity);
}

/*
 * How many CPU sets are configured?  This number can be used to partition
 * contended resources.
 */
int
cpu_affinity_cpu_sets(void)
{
	return pg_cpuset_table_count(&cpu_affinity_control->table);
}

/*
 * Which NUMA node is a CPU set running on?  This could be used for naming
 * worker processes.
 */
int
cpu_affinity_numa_node_for_cpu_set(int n)
{
	return pg_cpuset_table_numa_node(&cpu_affinity_control->table, n);
}

/*
 * Tell the operating system to run the caller on a given CPU set.
 *
 * XXX Currently this may not be called more than once and there is no way to
 * undo its effects.
 */
void
cpu_affinity_run_on_cpu_set(int n)
{
	if (MyBackendType == B_BACKEND)
		elog(ERROR, "regular backends cannot not be pinned to CPU sets");

	pg_cpuset_table_pin(&cpu_affinity_control->table, n);
}
