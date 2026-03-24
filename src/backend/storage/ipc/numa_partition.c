/*-------------------------------------------------------------------------
 *
 * numa_partition.c
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/numa_partition.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/numa_partition.h"

/* GUC */
int			numa_partitions = 1;

/*
 * This is read-only after initialization, so it is shared between child
 * processes without consuming more physical memory (except in EXEC_BACKEND
 * builds).  It could also be placed in shared memory, but it seems better to
 * have it fully configured while other shared memory is being sized.
 */
cpu_table	numa_partition_table = {0};

static bool
numa_partition_table_initialized(void)
{
	return cpu_table_partitions(&numa_partition_table) != 0;
}

void
numa_partition_initialize(void)
{
	if (!numa_partition_table_initialized())
		cpu_table_build(&numa_partition_table,
						IsBootstrapProcessingMode() ? 0 : numa_partitions);
}

int
numa_partition_count(void)
{
	Assert(numa_partition_table_initialized());
	return cpu_table_partitions(&numa_partition_table);
}

int
numa_partition_count_per_numa_node(void)
{
	Assert(numa_partition_table_initialized());
	return cpu_table_partitions_per_numa_node(&numa_partition_table);
}

/*
 * Which NUMA node is a CPU set in?  Intended for logging etc.
 */
int
numa_partition_to_numa_node(int partition)
{
	Assert(numa_partition_table_initialized());
	return cpu_table_partition_to_numa_node(&numa_partition_table, partition);
}

/*
 * Which CPU set is a CPU in?  Intended for logging, views etc.
 */
int
numa_partition_for_cpu(pg_cpu_t cpu)
{
	Assert(numa_partition_table_initialized());
	return cpu_table_cpu_to_partition(&numa_partition_table, cpu);
}

/*
 * Pin the caller to a given CPU set.  Use -1 to unpin, ie return to initial
 * affinity.
 */
void
numa_partition_pin_worker(int partition)
{
	Assert(numa_partition_table_initialized());

	if (MyBackendType == B_BACKEND)
		elog(ERROR, "regular backends should not be pinned to CPUs");

	cpu_table_run_on_partition(&numa_partition_table, partition);
}
