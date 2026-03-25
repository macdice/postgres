/*-------------------------------------------------------------------------
 *
 * cpu_table.c
 *	  Routing mechanism that partitions CPUs for efficient IPC.
a *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/cpu_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "lib/stringinfo.h"
#include "port/pg_bitutils.h"
#include "port/pg_cpu.h"
#include "port/pg_cpuset.h"
#include "port/pg_numa.h"
#include "storage/cpu_table.h"

static int
cpu_table_build_empty(cpu_table *table)
{
	table->format = CPU_TABLE_FORMAT_EMPTY;
	table->partitions = 1;
	return table->partitions;
}

#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
static int
cpu_table_build_numa_direct(cpu_table *table, int num_numa_nodes)
{
	table->format = CPU_TABLE_FORMAT_NUMA_DIRECT;
	table->partitions = num_numa_nodes;
	return table->partitions;
}
#endif

static void
cpu_table_map_set(cpu_table *table, pg_cpu_t cpu, uint8_t partition)
{
	uint8_t		hash;
	uint8_t		nibbles;
	int			shift;

	hash = cpu_table_hash_cpu(table, cpu);
	nibbles = table->map[hash / CPU_TABLE_SUBENTRIES];
	if (hash % CPU_TABLE_SUBENTRIES)
		shift = CPU_TABLE_SUBENTRY_BITS;
	else
		shift = 0;

	nibbles &= ~(CPU_TABLE_SUBENTRY_MASK << shift);
	nibbles |= partition << shift;

	table->map[hash / CPU_TABLE_SUBENTRIES] = nibbles;

	Assert(cpu_table_get(table, cpu) == partition);
}

int
cpu_table_build(cpu_table *table, int partitions_per_numa_node)
{
	pg_cpuset_t numa_nodes[CPU_TABLE_MAX_PARTITIONS];
	pg_cpuset_t partitions[CPU_TABLE_MAX_PARTITIONS];
	int			num_numa_nodes;
	int			num_partitions;
	bool		success;

	table->partitions_per_numa_node = partitions_per_numa_node;

	/* Partitioning explicitly disabled. */
	if (partitions_per_numa_node == 0)
		return cpu_table_build_empty(table);

	/* Check argument validity. */
	if (partitions_per_numa_node < 0 ||
		partitions_per_numa_node > CPU_TABLE_MAX_PARTITIONS)
		elog(ERROR, "unsupported number of partitions per NUMA node: %d",
			 partitions_per_numa_node);

	/* How many NUMA nodes? */
	num_numa_nodes = pg_numa_get_max_node() + 1;
	Assert(num_numa_nodes > 0);

	/* Use empty table if that means one set. */
	if (num_numa_nodes == 1 && partitions_per_numa_node == 1)
		return cpu_table_build_empty(table);

	/* Check validity of arguments now that we have num_numa_nodes. */
	if (num_numa_nodes > CPU_TABLE_MAX_PARTITIONS ||
		partitions_per_numa_node * num_numa_nodes > CPU_TABLE_MAX_PARTITIONS)
		ereport(ERROR,
				(errmsg("unsupported number of NUMA partitions: %d",
						partitions_per_numa_node * num_numa_nodes)));

	/* Record the set of CPUs we can run on. */
	pg_cpuset_initialize(&table->initial_affinity);
	if (pg_cpuset_get_process_affinity_self(&table->initial_affinity) < 0)
		elog(ERROR, "pg_cpuset_get_process_affinity_self() failed: %m");

	if (num_numa_nodes == 1)
	{
		/* No NUMA.  Use all the CPUs we can run on. */
		numa_nodes[0] = table->initial_affinity;
	}
	else
	{
		/* Get the CPUs for each node, then remove CPUs we can't run on. */
		for (int i = 0; i < num_numa_nodes; ++i)
		{
			if (pg_numa_get_cpus_for_node(i, &numa_nodes[i]) < 0)
				elog(ERROR, "pg_numa_get_cpus_for_node(%d) failed: %m", i);
			pg_cpuset_and(&numa_nodes[i], &table->initial_affinity);
		}
	}

#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
	/* Can we skip building a map and just use NUMA node numbers directly? */
	if (partitions_per_numa_node == 1)
	{
		int			can_use_numa_direct = true;

		/*
		 * We don't want users of this API to waste memory on partitioned
		 * resources for NUMA nodes that we aren't actually allowed to run on
		 * due to external pinning, OS configuration etc, or worse, try to pin
		 * workers there.
		 */
		for (int i = 0; i < num_numa_nodes; ++i)
			if (pg_cpuset_is_empty(&numa_nodes[i]))
				can_use_numa_direct = false;

		if (can_use_numa_direct)
			return cpu_table_build_numa_direct(table, num_numa_nodes);
	}
#endif

	/* Partition the node(s) as requested. */
	for (int i = 0; i < lengthof(partitions); ++i)
		pg_cpuset_initialize(&partitions[i]);

	num_partitions = 0;
	for (int i = 0; i < num_numa_nodes; ++i)
	{
		pg_cpuset_iterator iter;
		double		cpus_per_numa_node;
		double		cpus_per_partition;
		int			subset;
		int			cpus;

		/* Skip nodes we aren't allowed to run on. */
		cpus_per_numa_node = pg_cpuset_count(&numa_nodes[i]);
		if (cpus_per_numa_node == 0)
			continue;

		/* Floating point division to handle awkward numbers. */
		cpus_per_partition = cpus_per_numa_node / partitions_per_numa_node;
		subset = 0;
		cpus = 0;

		pg_cpuset_iterator_begin(&numa_nodes[i], &iter);
		while (pg_cpuset_iterator_has_next(&iter))
		{
			pg_cpu_t	cpu = pg_cpuset_iterator_next(&iter);

			cpus++;
			pg_cpuset_add(&partitions[num_partitions + subset], cpu);
			table->partition_to_numa_node[num_partitions + subset] = i;

			/* Spread division remainder out... */
			while (subset < partitions_per_numa_node - 1 &&
				   cpus >= cpus_per_partition * (subset + 1))
				subset++;
		}

		Assert(cpus == cpus_per_numa_node);
		num_partitions += partitions_per_numa_node;
	}

	/* Sanity check. */
	if (num_partitions == 0)
		elog(ERROR, "no CPUs found");

	/* Object if that produced any empty sets. */
	for (int i = 0; i < num_partitions; ++i)
		if (pg_cpuset_is_empty(&partitions[i]))
			ereport(ERROR,
					(errmsg("dividing %d NUMA node(s) by %d produced empty partitions",
							num_numa_nodes,
							partitions_per_numa_node)));

	/* Try to find a shift value that results in a perfect hash. */
	success = false;
	for (int shift = 0; shift < 8; shift++)
	{
		bool		conflict = false;
		bool		occupied[CPU_TABLE_ENTRIES *
							 CPU_TABLE_SUBENTRIES] = {0};

		/* Build candidate table. */
		table->format = CPU_TABLE_FORMAT_MAP;
		table->partitions = num_partitions;
		table->map_shift = shift;

		/*
		 * Default all entries to 0.  Any CPUs that magicaly become available
		 * after we observed initial_affinity (ie a user changes our affinity
		 * with external tools) will map to *some* CPU set, but not one that
		 * our perfect hash knows about.
		 */
		memset(table->map, 0, sizeof(table->map));

		for (int i = 0; i < num_partitions && !conflict; ++i)
		{
			pg_cpuset_iterator iter;

			pg_cpuset_iterator_begin(&partitions[i], &iter);
			while (pg_cpuset_iterator_has_next(&iter))
			{
				pg_cpu_t	cpu;
				uint8_t		hash;

				cpu = pg_cpuset_iterator_next(&iter);
				hash = cpu_table_hash_cpu(table, cpu);
				if (occupied[hash])
				{
					if (cpu_table_get(table, cpu) != i)
					{
						conflict = true;
						break;
					}
				}
				else
				{
					cpu_table_map_set(table, cpu, i);
					occupied[hash] = true;
				}
			}
		}
		if (!conflict)
		{
			success = true;
			break;
		}
	}

	if (success)
		return table->partitions;

	/*
	 * Provide fallback rather than error if we failed only because our CPU
	 * hashing scheme is not good enough.
	 */
	elog(LOG, "could not build CPU map, using single partition");
	return cpu_table_build_empty(table);
}

int
cpu_table_partitions(const cpu_table *table)
{
	return table->partitions;
}

int
cpu_table_partitions_per_numa_node(const cpu_table *table)
{
	return table->partitions_per_numa_node;
}

int
cpu_table_partition_to_numa_node(const cpu_table *table, int partition)
{
#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
	if (table->format == CPU_TABLE_FORMAT_NUMA_DIRECT)
		return partition;
#endif

	if (table->format == CPU_TABLE_FORMAT_MAP)
		return table->partition_to_numa_node[partition];

	Assert(table->format == CPU_TABLE_FORMAT_EMPTY);

	return 0;
}

int
cpu_table_cpu_to_partition(const cpu_table *table, pg_cpu_t cpu)
{
#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
	if (table->format == CPU_TABLE_FORMAT_NUMA_DIRECT)
		return pg_numa_get_node_for_cpu(cpu);
#endif

	if (table->format == CPU_TABLE_FORMAT_MAP)
	{
		/* CPU that we filtered out of the table? */
		if (!pg_cpuset_contains(&table->initial_affinity, cpu))
			return -1;

		return cpu_table_get(table, cpu);
	}

	Assert(table->format == CPU_TABLE_FORMAT_EMPTY);

	return 0;
}

void
cpu_table_run_on_partition(const cpu_table *table, int partition)
{
	if (partition == -1)
	{
		/*
		 * Revert to initial affinity, assumed to be all the CPUs we are
		 * allowed to run on.
		 */
		if (table->format != CPU_TABLE_FORMAT_EMPTY)
			if (pg_cpuset_set_process_affinity_self(&table->initial_affinity) < 0)
				elog(ERROR, "pg_cpuset_set_process_affinity_self() failed: %m");
		return;
	}

	/* Set affinity to one CPU set. */
	if (table->format == CPU_TABLE_FORMAT_MAP)
	{
		pg_cpuset_iterator iter;
		pg_cpuset_t affinity;

		pg_cpuset_initialize(&affinity);

		pg_cpuset_iterator_begin(&table->initial_affinity, &iter);
		while (pg_cpuset_iterator_has_next(&iter))
		{
			pg_cpu_t	cpu = pg_cpuset_iterator_next(&iter);

			/* Take all CPUs from initial affinity that map to this partition. */
			if (cpu_table_get(table, cpu) == partition)
				pg_cpuset_add(&affinity, cpu);
		}

		if (pg_cpuset_set_process_affinity_self(&affinity) < 0)
			elog(ERROR, "pg_cpuset_set_process_affinity_self() failed: %m");
	}
#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
	else if (table->format == CPU_TABLE_FORMAT_NUMA_DIRECT)
	{
		pg_cpuset_t cpuset;

		/*
		 * This is effectively numa_run_on_node(), but Windows needs it to be
		 * done with an explicit CPU set and it's easier to share the code.
		 */
		if (pg_numa_get_cpus_for_node(partition, &cpuset) < 0)
			elog(ERROR, "pg_numa_cpus_for_node(%d) failed: %m", partition);
		if (pg_cpuset_set_process_affinity_self(&cpuset) < 0)
			elog(ERROR, "pg_cpuset_set_process_affinity_self() failed: %m");
	}
#endif
	else
	{
		Assert(table->format == CPU_TABLE_FORMAT_EMPTY);
	}
}
