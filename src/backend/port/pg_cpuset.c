/*-------------------------------------------------------------------------
 *
 * pg_cpuset.c
 *	  Portable interface for controlling CPU affinity.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/port/pg_cpuset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "lib/stringinfo.h"
#include "port/pg_bitutils.h"
#include "port/pg_cpuset.h"
#include "port/pg_numa.h"

/* An arbitrary number used for a couple of stack arrays. */
#define PG_CPUSET_MAX_CPUS_PER_NUMA_NODE 1024

/* Native representation of CPU sets.  Not currently exposed. */
#if defined(HAVE_SCHED_GETAFFINITY)
typedef cpu_set_t pg_cpuset_t;
#else
typedef void pg_cpuset_t;
#endif

/*
 * De facto compatible cpu_set_t manipulation macros are found in many OSes
 * and APIs (<sched.h>, <pthread.h> extensions, Linux, *BSD, ...).
 */
#if defined(HAVE_SCHED_GETAFFINITY)
#define HAVE_CPU_SET_MACROS
#endif

static void
pg_cpuset_add(pg_cpuset_t *cpuset, pg_cpu_t cpu)
{
#ifdef HAVE_CPU_SET_MACROS
	CPU_SET(cpu, cpuset);
#endif
}

static void
pg_cpuset_remove(pg_cpuset_t *cpuset, pg_cpu_t cpu)
{
#ifdef HAVE_CPU_SET_MACROS
	CPU_CLR(cpu, cpuset);
#endif
}

static void
pg_cpuset_and(pg_cpuset_t *a, const pg_cpuset_t *b)
{
#ifdef HAVE_CPU_SET_MACROS
	CPU_AND(a, a, b);
#endif
}

static int
pg_cpuset_count(const pg_cpuset_t *cpuset)
{
#ifdef HAVE_CPU_SET_MACROS
	return CPU_COUNT(cpuset);
#else
	return 0;
#endif
}

static bool
pg_cpuset_is_empty(const pg_cpuset_t *cpuset)
{
#ifdef HAVE_CPU_SET_MACROS
	return CPU_COUNT(cpuset) == 0;
#else
	return true;
#endif
}

static pg_cpuset_t *
pg_cpuset_make(const pg_cpu_t *cpus, int n)
{
#ifdef HAVE_CPU_SET_MACROS
	pg_cpuset_t *result = palloc_object(pg_cpuset_t);

	CPU_ZERO(result);
	for (int i = 0; i < n; ++i)
		pg_cpuset_add(result, cpus[i]);
	return result;
#else
	return NULL;
#endif
}

static pg_cpuset_t *
pg_cpuset_make_empty(void)
{
	return pg_cpuset_make(NULL, 0);
}

static void
pg_cpuset_free(pg_cpuset_t *set)
{
#if defined(HAVE_CPU_SET_MACROS)
	pfree(set);
#endif
}

typedef struct pg_cpuset_iterator
{
#ifdef HAVE_CPU_SET_MACROS
	pg_cpuset_t empty;
	pg_cpuset_t remaining;
	pg_cpu_t	next;
#else
	int			dummy;
#endif
} pg_cpuset_iterator;

static void
pg_cpuset_iterator_begin(const pg_cpuset_t *cpuset, pg_cpuset_iterator *iter)
{
#ifdef HAVE_CPU_SET_MACROS
	CPU_ZERO(&iter->empty);
	iter->remaining = *cpuset;
	iter->next = 0;
#endif
}

static bool
pg_cpuset_iterator_has_next(const pg_cpuset_iterator *iter)
{
#ifdef HAVE_CPU_SET_MACROS
	return !CPU_EQUAL(&iter->empty, &iter->remaining);
#else
	return false;
#endif
}

static pg_cpu_t
pg_cpuset_iterator_next(pg_cpuset_iterator *iter)
{
#ifdef HAVE_CPU_SET_MACROS
	Assert(pg_cpuset_iterator_has_next(iter));
	while (!CPU_ISSET(iter->next, &iter->remaining))
		iter->next++;
	CPU_CLR(iter->next, &iter->remaining);
	return iter->next++;
#else
	return 0;
#endif
}

static void
pg_cpuset_to_stringinfo(const pg_cpuset_t *cpuset, StringInfoData *s)
{
	pg_cpuset_iterator iter;

	pg_cpuset_iterator_begin(cpuset, &iter);
	while (pg_cpuset_iterator_has_next(&iter))
	{
		pg_cpu_t	cpu = pg_cpuset_iterator_next(&iter);

		appendStringInfo(s, PG_CPU_FORMAT, PG_CPU_FORMAT_ARG(cpu));
		if (pg_cpuset_iterator_has_next(&iter))
			appendStringInfo(s, ", ");
	}
}

static pg_cpuset_t *
pg_cpuset_get_affinity(void)
{
#if defined(HAVE_SCHED_GETAFFINITY)
	pg_cpuset_t *result = palloc_object(pg_cpuset_t);

	if (sched_getaffinity(getpid(), sizeof(*result), result) == 0)
		return result;

	pfree(result);
	return NULL;
#else
	errno = ENOSYS;
	return NULL;
#endif
}

static int
pg_cpuset_set_affinity(const pg_cpuset_t *set)
{
	int			result = -1;

#if defined(HAVE_SCHED_SETAFFINITY)
	if (sched_setaffinity(getpid(), sizeof(*set), set) == 0)
		result = 0;
#else
	errno = ENOSYS;
#endif

	return result;
}

/* Building a tables. */

static int
pg_cpuset_table_build_empty(pg_cpuset_table *table)
{
	table->format = PG_CPUSET_TABLE_FORMAT_EMPTY;
	table->sets = 1;
	return table->sets;
}

#if defined(PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT)
static int
pg_cpuset_table_build_numa_direct(pg_cpuset_table *table, int num_numa_nodes)
{
	table->format = PG_CPUSET_TABLE_FORMAT_NUMA_DIRECT;
	table->sets = num_numa_nodes;
	return table->sets;
}
#endif

static void
pg_cpuset_table_map_set(pg_cpuset_table *table,
						pg_cpu_t cpu,
						pg_cpuset_table_index_t value)
{
	pg_cpuset_table_map_hash_t hash;
	pg_cpuset_table_index_t nibbles;
	int			shift;

	hash = pg_cpuset_table_map_hash_cpu(table, cpu);
	nibbles = table->map[hash / PG_CPUSET_TABLE_MAP_SUBENTRIES];
	if (hash % PG_CPUSET_TABLE_MAP_SUBENTRIES)
		shift = PG_CPUSET_TABLE_MAP_SUBENTRY_BITS;
	else
		shift = 0;

	nibbles &= ~(PG_CPUSET_TABLE_MAP_SUBENTRY_MASK << shift);
	nibbles |= value << shift;

	table->map[hash / PG_CPUSET_TABLE_MAP_SUBENTRIES] = nibbles;

	Assert(pg_cpuset_table_map_get(table, cpu) == value);
}

int
pg_cpuset_table_build(pg_cpuset_table *table, int sets_per_node)
{
	pg_cpuset_t *numa_nodes[PG_CPUSET_TABLE_MAP_MAX_CPUSETS];
	pg_cpuset_t *sets[PG_CPUSET_TABLE_MAP_MAX_CPUSETS] = {0};
	int			num_numa_nodes;
	int			nsets;
	bool		success;

	if (sets_per_node == 0)
		return pg_cpuset_table_build_empty(table);

	/* Check argument validity. */
	if (pg_popcount32(sets_per_node) != 1 ||
		sets_per_node < 0 ||
		sets_per_node > PG_CPUSET_TABLE_MAP_MAX_CPUSETS)
		elog(ERROR, "unsupported number of CPU sets per NUMA node: %d",
			 sets_per_node);

	/* How many NUMA nodes? */
	num_numa_nodes = pg_numa_get_max_node() + 1;
	if (num_numa_nodes == 1 && sets_per_node == 1)
		return pg_cpuset_table_build_empty(table);

#if defined(PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT)
	/* Skip building a map if we can use direct NUMA routing. */
	if (sets_per_node == 1)
		return pg_cpuset_table_build_numa_direct(table, num_numa_nodes);
#endif

	/* Check validity of arguments now that we have num_numa_nodes. */
	if (num_numa_nodes > PG_CPUSET_TABLE_MAP_MAX_CPUSETS ||
		sets_per_node * num_numa_nodes > PG_CPUSET_TABLE_MAP_MAX_CPUSETS)
		elog(ERROR, "too many NUMA nodes to subdivide: %d",
			 sets_per_node);

	/* Get the set of CPUs for each node. */
	for (int i = 0; i < num_numa_nodes; ++i)
	{
		pg_cpu_t	cpus[PG_CPUSET_MAX_CPUS_PER_NUMA_NODE];
		int			ncpus;

		/*
		 * On a system without support, we get no CPUs, but we'll handle that
		 * further down.
		 */
		ncpus = pg_numa_get_cpus_for_node(i, cpus, lengthof(cpus));
		if (ncpus < 0)
			elog(ERROR, "pg_numa_get_cpus_for_node(%d) failed: %m", i);
		numa_nodes[i] = pg_cpuset_make(cpus, ncpus);
	}

	if (num_numa_nodes <= 1)
	{
		/* No NUMA or one node, use the set of visible CPUs instead. */
		if (numa_nodes[0])
			pg_cpuset_free(numa_nodes[0]);

		numa_nodes[0] = pg_cpuset_get_affinity();
		if (numa_nodes[0] == NULL)
			elog(ERROR, "pg_cpuset_get_affinity() failed: %m");

		num_numa_nodes = 1;
	}
	else
	{
		/*
		 * Filter out CPUs that are not currently visible.  We don't want to
		 * create sets that contain only CPUs that you can't actually run on.
		 */
		pg_cpuset_t *visible = pg_cpuset_get_affinity();

		if (visible == NULL)
			elog(ERROR, "pg_cpuset_get_affinity() failed: %m");
		for (int i = 0; i < num_numa_nodes; ++i)
			pg_cpuset_and(numa_nodes[i], visible);
		pg_cpuset_free(visible);
	}

	/* Partition the node(s) as requested. */
	nsets = 0;
	for (int i = 0; i < num_numa_nodes; ++i)
	{
		int			node_cpus = pg_cpuset_count(numa_nodes[i]);
		int			set_cpus = node_cpus / sets_per_node;
		pg_cpuset_iterator iter;

		/* Skip nodes hidden by external pinning. */
		if (node_cpus == 0)
			continue;

		/* Divide into sets, skipping empty ones. */
		pg_cpuset_iterator_begin(numa_nodes[i], &iter);
		for (int s = 0; s < sets_per_node; ++s)
		{
			table->numa_nodes[s] = i;
			if (sets[nsets] == NULL)
				sets[nsets] = pg_cpuset_make_empty();
			for (int c = 0; c < set_cpus; ++c)
				pg_cpuset_add(sets[nsets], pg_cpuset_iterator_next(&iter));
			if (!pg_cpuset_is_empty(sets[nsets]))
				nsets++;
		}
	}

	/* Remove any trailing empty set. */
	if (nsets > 0 && pg_cpuset_is_empty(sets[nsets - 1]))
		pg_cpuset_free(sets[--nsets]);

	/* Sanity check. */
	if (nsets == 0)
		elog(ERROR, "no CPUs found");

	/* Try to find a shift value that results in a perfect hash. */
	success = false;
	for (int shift = 0; shift < 8; shift++)
	{
		bool		conflict = false;
		bool		occupied[PG_CPUSET_TABLE_MAP_ENTRIES *
							 PG_CPUSET_TABLE_MAP_SUBENTRIES] = {0};

		/* Build candidate table. */
		table->format = PG_CPUSET_TABLE_FORMAT_MAP;
		table->sets = nsets;
		table->map_shift = shift;

		for (int i = 0; i < nsets && !conflict; ++i)
		{
			pg_cpuset_iterator iter;

			pg_cpuset_iterator_begin(sets[i], &iter);
			while (pg_cpuset_iterator_has_next(&iter))
			{
				pg_cpu_t	cpu;
				pg_cpuset_table_map_hash_t hash;

				cpu = pg_cpuset_iterator_next(&iter);
				hash = pg_cpuset_table_map_hash_cpu(table, cpu);
				if (occupied[hash])
				{
					if (pg_cpuset_table_map_get(table, cpu) != i)
					{
						conflict = true;
						break;
					}
				}
				else
				{
					pg_cpuset_table_map_set(table, cpu, i);
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
	{
		for (int i = 0; i < nsets; ++i)
		{
			StringInfoData s;

			initStringInfo(&s);
			pg_cpuset_to_stringinfo(sets[i], &s);
			elog(LOG, "CPU set %d: %s", i, s.data);
			resetStringInfo(&s);
		}
	}

	/* Clean up. */
	for (int i = 0; i < nsets; ++i)
		pg_cpuset_free(sets[i]);
	for (int i = 0; i < num_numa_nodes; ++i)
		pg_cpuset_free(numa_nodes[i]);

	if (success)
		return table->sets;

#if defined(PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT)
	if (num_numa_nodes > 1)
	{
		elog(LOG, "could not build CPU map, using top-level NUMA nodes");
		return pg_cpuset_table_build_numa_direct(table, num_numa_nodes);
	}
#endif

	elog(LOG, "could not build CPU map, using single CPU set");
	return pg_cpuset_table_build_empty(table);
}

int
pg_cpuset_table_count(const pg_cpuset_table *table)
{
	return table->sets;
}

int
pg_cpuset_table_numa_node(const pg_cpuset_table *table,
						  pg_cpuset_table_index_t index)
{
#if defined(PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT)
	if (table->format == PG_CPUSET_TABLE_FORMAT_NUMA_DIRECT)
		return index;
#endif

	if (table->format == PG_CPUSET_TABLE_FORMAT_MAP)
		return table->numa_nodes[index];

	Assert(table->format == PG_CPUSET_TABLE_FORMAT_EMPTY);

	return 0;
}

void
pg_cpuset_table_pin(const pg_cpuset_table *table,
					pg_cpuset_table_index_t index)
{
	if (table->format == PG_CPUSET_TABLE_FORMAT_MAP)
	{
		pg_cpuset_iterator iter;
		pg_cpuset_t *affinity;

		/* Assume current affinity is set of candidate CPUs. */
		affinity = pg_cpuset_get_affinity();
		if (!affinity)
			elog(ERROR, "pg_cpuset_get_affinity() failed: %m");

		/* Filter by table. */
		pg_cpuset_iterator_begin(affinity, &iter);
		while (pg_cpuset_iterator_has_next(&iter))
		{
			pg_cpu_t	cpu = pg_cpuset_iterator_next(&iter);

			if (pg_cpuset_table_map_get(table, cpu) != index)
				pg_cpuset_remove(affinity, cpu);
		}

		/* Sanity check. */
		if (pg_cpuset_is_empty(affinity))
			elog(ERROR, "cannot pin backend to empty CPU set");

		/* Apply the new affinity. */
		if (pg_cpuset_set_affinity(affinity) < 0)
			elog(ERROR, "pg_cpuset_set_affinity(): %m");
		pg_cpuset_free(affinity);
	}
#if defined(PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT)
	else if (table->format == PG_CPUSET_TABLE_FORMAT_NUMA_DIRECT)
	{
		pg_cpuset_t *affinity;
		pg_cpu_t	cpus[PG_CPUSET_MAX_CPUS_PER_NUMA_NODE];
		int			ncpus;

		/*
		 * This is effectively numa_run_on_node(), but Windows needs it to be
		 * done explicitly like this and it's simpler to share the code.
		 */
		ncpus = pg_numa_get_cpus_for_node(index, cpus, lengthof(cpus));
		if (ncpus < 0)
			elog(ERROR, "pg_numa_cpus_for_node(%d) failed: %m", index);
		affinity = pg_cpuset_make(cpus, ncpus);
		if (pg_cpuset_set_affinity(affinity) < 0)
			elog(ERROR, "pg_cpuset_set_affinity() failed: %m");
		pg_cpuset_free(affinity);
	}
#endif
	else
	{
		Assert(table->format == PG_CPUSET_TABLE_FORMAT_EMPTY);
	}
}
