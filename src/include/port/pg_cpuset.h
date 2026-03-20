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

#include <limits.h>
#ifdef HAVE_SCHED_GETCPU
#include <sched.h>
#endif

#include "port/pg_cpu.h"
#include "port/pg_numa.h"


/* A table of CPU sets. */
struct pg_cpuset_table;
typedef struct pg_cpuset_table pg_cpuset_table;

/* An index identifying a CPU set within a table. */
typedef uint8_t pg_cpuset_table_index_t;

/*
 * Populate a table of CPU sets in user-supplied memory.
 *
 * If sets_per_node is 0, only one set it created.
 *
 * If it is 1, one partition is created per NUMA node.  A larger number
 * subdivides each NUMA nodes, or no non-NUMA systems, all available CPUs, and
 * must be a power of two.  Subsets are currently created using monotonic CPU
 * IDs ranges.
 *
 * Returns the total number of sets in the table, and ereports on failure.
 */
extern PGDLLIMPORT int pg_cpuset_table_build(pg_cpuset_table *table,
											 int cpusets_per_node);

/* Retrieves the number of sets in the table. */
extern PGDLLIMPORT int pg_cpuset_table_count(const pg_cpuset_table *table);

/* Retrieves the NUMA node of a given set in the table. */
extern PGDLLIMPORT int pg_cpuset_table_numa_node(const pg_cpuset_table *table,
												 pg_cpuset_table_index_t index);

/*
 * Finds the index of the set of CPUs that the caller is currently running on.
 *
 * This can be used for purposes such as partitioning resource pools to reduce
 * lock contention, and routing messages to workers that have pinned
 * themselves to the same CPU set.
 */
static inline pg_cpuset_table_index_t pg_cpuset_table_current(const pg_cpuset_table *table);

/*
 * Set the CPU affinity of the calling backend so that it runs on the
 * requested CPU set.  This can be used to partition workers into dedicated
 * per-node pools.  Regular backends should not use this!
 *
 * XXX This can't be called more than once or undone, currently.
 *
 * XXX In the long term with multi-threading support, we might want a
 * different interface that can pin worker threads to individual CPUs.
 */
extern PGDLLIMPORT void pg_cpuset_table_pin(const pg_cpuset_table *table,
											pg_cpuset_table_index_t index);



/*-------------------------------------------------------------------------
 *
 * Private implementation code follow.  It is in the header to allow for
 * fast inlined CPU set lookups.
 *
 *-------------------------------------------------------------------------
 */

/*
 * If we have a fast way to know which node we're on and the user only wants
 * top-level NUMA node CPU sets, we can skip building a map.
 */
#if defined(PG_NUMA_HAVE_WORKING_GET_CURRENT_NODE)
#define PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT
#endif

enum pg_cpuset_table_format
{
	/* Single set of processors, no book-keeping required. */
	PG_CPUSET_TABLE_FORMAT_EMPTY,

	/*
	 * A map of processors to CPU sets indexes, for NUMA-nodes, with or
	 * without subsets, and for non-NUMA systems with subsets.
	 */
	PG_CPUSET_TABLE_FORMAT_MAP,

#ifdef PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT

	/*
	 * Sets matching NUMA nodes exactly with no subsets, using OS facilities
	 * to look up the current node so that we don't need a map.
	 */
	PG_CPUSET_TABLE_FORMAT_NUMA_DIRECT,
#endif
};

/*
 * PG_CPUSET_TABLE_FORMAT_MAP:
 *
 * We map pg_cpu_t -> pg_cpuset_index_t by using some of its bits to make an
 * extremely simplistic "perfect hash"-style key:
 *
 * 1.  The lower 3 bits are always included.  This should work for NUMA
 * systems that use interleaving CPU numbers.
 *
 * 2.  A further 4 more bits, shifted down by an amount that
 * pg_cpuset_table_build() determines.  This is good for typical NUMA systems
 * that use disjoint ranges of CPU numbers on each node, and also for futher
 * division into subsets.
 *
 * The table is packed into a single cache line with two entries per byte.
 * This allows up to 16 CPU sets to be configured in total.
 *
 * It might not be possible to find a successful shift value on very large
 * systems with non-power-of-two core counts per node.
 */

#define PG_CPUSET_TABLE_MAP_ENTRIES (64 / sizeof(pg_cpuset_table_map_entry_t))
#define PG_CPUSET_TABLE_MAP_SUBENTRIES 2
#define PG_CPUSET_TABLE_MAP_SUBENTRY_MASK ((1 << PG_CPUSET_TABLE_MAP_SUBENTRY_BITS) - 1)
#define PG_CPUSET_TABLE_MAP_SUBENTRY_BITS (sizeof(pg_cpuset_table_map_entry_t) * CHAR_BIT / \
										   PG_CPUSET_TABLE_MAP_SUBENTRIES)
#define PG_CPUSET_TABLE_MAP_MAX_CPUSETS (PG_CPUSET_TABLE_MAP_SUBENTRY_MASK + 1)

#define PG_CPUSET_TABLE_MAP_HASH_CPU1_BITS 3
#define PG_CPUSET_TABLE_MAP_HASH_CPU2_BITS 4
#define PG_CPUSET_TABLE_MAP_HASH_CPU_BITS (PG_CPUSET_TABLE_MAP_HASH_CPU1_BITS +	\
										   PG_CPUSET_TABLE_MAP_HASH_CPU2_BITS)
#define PG_CPUSET_TABLE_MAP_HASH_CPU1_MASK ((1 << PG_CPUSET_TABLE_MAP_HASH_CPU1_BITS) - 1)
#define PG_CPUSET_TABLE_MAP_HASH_CPU2_MASK								\
	(((1 << PG_CPUSET_TABLE_MAP_HASH_CPU_BITS) - 1) &					\
	 ~PG_CPUSET_TABLE_MAP_HASH_CPU1_MASK)

typedef uint8_t pg_cpuset_table_map_hash_t;
typedef uint8_t pg_cpuset_table_map_entry_t;

static_assert(PG_CPUSET_TABLE_MAP_ENTRIES * PG_CPUSET_TABLE_MAP_SUBENTRIES ==
			  (1 << (PG_CPUSET_TABLE_MAP_HASH_CPU_BITS)),
			  "table size must match hash size");

struct pg_cpuset_table
{
	uint8_t		format;
	uint8_t		sets;
	uint8_t		map_shift;
	uint8_t		numa_nodes[PG_CPUSET_TABLE_MAP_MAX_CPUSETS];
				alignas(64) pg_cpuset_table_map_entry_t map[PG_CPUSET_TABLE_MAP_ENTRIES];
};
typedef struct pg_cpuset_table pg_cpuset_table;


static_assert(sizeof(pg_cpuset_table_map_hash_t) * CHAR_BIT >=
			  PG_CPUSET_TABLE_MAP_HASH_CPU_BITS,
			  "hash type must be wide enough for maximum hash value");

static inline pg_cpuset_table_map_hash_t
pg_cpuset_table_map_hash_cpu(const pg_cpuset_table *table, pg_cpu_t cpu)
{
	int			value = cpu;

	return ((value & PG_CPUSET_TABLE_MAP_HASH_CPU1_MASK) |
			((value >> table->map_shift) & PG_CPUSET_TABLE_MAP_HASH_CPU2_MASK));
}

static inline pg_cpuset_table_index_t
pg_cpuset_table_map_get(const pg_cpuset_table *table, pg_cpu_t cpu)
{
	pg_cpuset_table_map_hash_t hash;
	pg_cpuset_table_map_entry_t nibbles;

	hash = pg_cpuset_table_map_hash_cpu(table, cpu);
	nibbles = table->map[hash / PG_CPUSET_TABLE_MAP_SUBENTRIES];
	if (hash % PG_CPUSET_TABLE_MAP_SUBENTRIES)
		nibbles >>= PG_CPUSET_TABLE_MAP_SUBENTRY_BITS;

	return nibbles & PG_CPUSET_TABLE_MAP_SUBENTRY_MASK;
}

static inline pg_cpuset_table_index_t
pg_cpuset_table_current(const pg_cpuset_table *table)
{
	if (table->format == PG_CPUSET_TABLE_FORMAT_EMPTY)
		return 0;

#if defined(PG_CPUSET_HAVE_FORMAT_NUMA_DIRECT)
	if (table->format == PG_CPUSET_TABLE_FORMAT_NUMA_DIRECT)
		return pg_numa_get_current_node();
#endif

	Assert(table->format == PG_CPUSET_TABLE_FORMAT_MAP);
	return pg_cpuset_table_map_get(table, pg_cpu_current());
}

#endif							/* PG_CPUSET_H */
