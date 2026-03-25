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
	table->num_sets = 1;
	return table->num_sets;
}

#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
static int
cpu_table_build_numa_direct(cpu_table *table, int num_numa_nodes)
{
	table->format = CPU_TABLE_FORMAT_NUMA_DIRECT;
	table->num_sets = num_numa_nodes;
	return table->num_sets;
}
#endif

static void
cpu_table_map_set(cpu_table *table, pg_cpu_t cpu, uint8_t cpu_set_number)
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
	nibbles |= cpu_set_number << shift;

	table->map[hash / CPU_TABLE_SUBENTRIES] = nibbles;

	Assert(cpu_table_get(table, cpu) == cpu_set_number);
}

int
cpu_table_build(cpu_table *table, int cpu_sets_per_numa_node)
{
	pg_cpuset_t numa_nodes[CPU_TABLE_MAX_CPU_SETS];
	pg_cpuset_t sets[CPU_TABLE_MAX_CPU_SETS];
	int			num_numa_nodes;
	int			nsets;
	bool		success;

	/* Partitioning explicitly disabled. */
	if (cpu_sets_per_numa_node == 0)
		return cpu_table_build_empty(table);

	/* Check argument validity. */
	if (cpu_sets_per_numa_node < 0 ||
		cpu_sets_per_numa_node > CPU_TABLE_MAX_CPU_SETS)
		elog(ERROR, "unsupported number of CPU sets per NUMA node: %d",
			 cpu_sets_per_numa_node);

	/* How many NUMA nodes? */
	num_numa_nodes = pg_numa_get_max_node() + 1;
	Assert(num_numa_nodes > 0);

	/* Use empty table if that means one set. */
	if (num_numa_nodes == 1 && cpu_sets_per_numa_node == 1)
		return cpu_table_build_empty(table);

	/* Check validity of arguments now that we have num_numa_nodes. */
	if (num_numa_nodes > CPU_TABLE_MAX_CPU_SETS ||
		cpu_sets_per_numa_node * num_numa_nodes > CPU_TABLE_MAX_CPU_SETS)
		ereport(ERROR,
				(errmsg("unsupported number of CPU sets: %d",
						cpu_sets_per_numa_node * num_numa_nodes)));

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
	if (cpu_sets_per_numa_node == 1)
	{
		int			can_use_numa_direct = true;

		/*
		 * We don't want users of this API to waste memory on partitioned
		 * resources for NUMA nodes that we aren't actually allowed to run on
		 * due to external pinning, OS configuration etc, or worse, try to pin
		 * workers there, which would surely fail.  If there are any such
		 * holes, we need to use map format instead so that we have 0-base CPU
		 * set numbers without holes.
		 */
		for (int i = 0; i < num_numa_nodes; ++i)
			if (pg_cpuset_is_empty(&numa_nodes[i]))
				can_use_numa_direct = false;

		if (can_use_numa_direct)
			return cpu_table_build_numa_direct(table, num_numa_nodes);
	}
#endif

	/* Partition the node(s) as requested. */
	for (int i = 0; i < lengthof(sets); ++i)
		pg_cpuset_initialize(&sets[i]);

	nsets = 0;
	for (int i = 0; i < num_numa_nodes; ++i)
	{
		pg_cpuset_iterator iter;
		double		node_cpu_count;
		double		cpus_per_set;
		int			subset;
		int			cpus;

		/* Skip nodes we aren't allowed to run on. */
		node_cpu_count = pg_cpuset_count(&numa_nodes[i]);
		if (node_cpu_count == 0)
			continue;

		/* Floating point division to handle awkward numbers. */
		cpus_per_set = node_cpu_count / cpu_sets_per_numa_node;
		subset = 0;
		cpus = 0;

		pg_cpuset_iterator_begin(&numa_nodes[i], &iter);
		while (pg_cpuset_iterator_has_next(&iter))
		{
			pg_cpu_t	cpu = pg_cpuset_iterator_next(&iter);

			cpus++;
			pg_cpuset_add(&sets[nsets + subset], cpu);
			table->cpu_set_to_numa_node[nsets + subset] = i;

			/* Spread division remainder out... */
			while (subset < cpu_sets_per_numa_node - 1 &&
				   cpus >= cpus_per_set * (subset + 1))
				subset++;
		}

		Assert(cpus == node_cpu_count);
		nsets += cpu_sets_per_numa_node;
	}

	/* Sanity check. */
	if (nsets == 0)
		elog(ERROR, "no CPUs found");

	/* Object if that produced any empty sets. */
	for (int i = 0; i < nsets; ++i)
		if (pg_cpuset_is_empty(&sets[i]))
			ereport(ERROR,
					(errmsg("dividing %d NUMA node(s) by %d produced empty CPU sets",
							num_numa_nodes,
							cpu_sets_per_numa_node)));

	/* Try to find a shift value that results in a perfect hash. */
	success = false;
	for (int shift = 0; shift < 8; shift++)
	{
		bool		conflict = false;
		bool		occupied[CPU_TABLE_ENTRIES *
							 CPU_TABLE_SUBENTRIES] = {0};

		/* Build candidate table. */
		table->format = CPU_TABLE_FORMAT_MAP;
		table->num_sets = nsets;
		table->map_shift = shift;

		/*
		 * Default all entries to 0.  Any CPUs that magicaly become available
		 * after we observed initial_affinity (ie a user changes our affinity
		 * with external tools) will map to *some* CPU set, but not one that
		 * our perfect hash knows about.
		 */
		memset(table->map, 0, sizeof(table->map));

		for (int i = 0; i < nsets && !conflict; ++i)
		{
			pg_cpuset_iterator iter;

			pg_cpuset_iterator_begin(&sets[i], &iter);
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
		return table->num_sets;

	/*
	 * Provide fallback rather than error if we failed only because our CPU
	 * hashing scheme is not good enough.
	 */
	elog(LOG, "could not build CPU map, using single CPU set");
	return cpu_table_build_empty(table);
}

int
cpu_table_num_cpu_sets(const cpu_table *table)
{
	return table->num_sets;
}

int
cpu_table_get_numa_node(const cpu_table *table, int cpu_set_number)
{
#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
	if (table->format == CPU_TABLE_FORMAT_NUMA_DIRECT)
		return cpu_set_number;
#endif

	if (table->format == CPU_TABLE_FORMAT_MAP)
		return table->cpu_set_to_numa_node[cpu_set_number];

	Assert(table->format == CPU_TABLE_FORMAT_EMPTY);

	return 0;
}

int
cpu_table_get_cpu_set_for_cpu(const cpu_table *table, pg_cpu_t cpu)
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
cpu_table_run_on_cpu_set(const cpu_table *table, int cpu_set_number)
{
	if (cpu_set_number == -1)
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

			/* Take all CPUs from initial affinity that map to this CPU set. */
			if (cpu_table_get(table, cpu) == cpu_set_number)
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
		if (pg_numa_get_cpus_for_node(cpu_set_number, &cpuset) < 0)
			elog(ERROR, "pg_numa_cpus_for_node(%d) failed: %m", cpu_set_number);
		if (pg_cpuset_set_process_affinity_self(&cpuset) < 0)
			elog(ERROR, "pg_cpuset_set_process_affinity_self() failed: %m");
	}
#endif
	else
	{
		Assert(table->format == CPU_TABLE_FORMAT_EMPTY);
	}
}
