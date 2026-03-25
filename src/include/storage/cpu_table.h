/*-------------------------------------------------------------------------
 *
 * cpu_table.h
 *	  Routing mechanism that partitions CPUs for efficient IPC.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/cpu_table.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CPU_TABLE_H
#define CPU_TABLE_H

#include "port/pg_cpu.h"
#include "port/pg_numa.h"

#if defined(PG_NUMA_HAVE_WORKING_GET_CURRENT_NODE)
#define CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT
#endif

/* Constants used for CPU_TABLE_FORMAT_MAP. */
#define CPU_TABLE_ENTRIES			64
#define CPU_TABLE_SUBENTRIES		2
#define CPU_TABLE_SUBENTRY_MASK		0x0f
#define CPU_TABLE_SUBENTRY_BITS		4
#define CPU_TABLE_MAX_PARTITIONS   	16
#define CPU_TABLE_HASH_CPU1_BITS	3
#define CPU_TABLE_HASH_CPU2_BITS   	4
#define CPU_TABLE_HASH_CPU_BITS		(CPU_TABLE_HASH_CPU1_BITS + \
									 CPU_TABLE_HASH_CPU2_BITS)
#define CPU_TABLE_HASH_CPU1_MASK 	((1 << CPU_TABLE_HASH_CPU1_BITS) - 1)
#define CPU_TABLE_HASH_CPU2_MASK 	(((1 << CPU_TABLE_HASH_CPU_BITS) - 1) & \
									 ~CPU_TABLE_HASH_CPU1_MASK)

enum cpu_table_format
{
	/* Single set of processors, no book-keeping required. */
	CPU_TABLE_FORMAT_EMPTY,

#ifdef CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT

	/*
	 * Sets matching NUMA nodes exactly with no subsets.  Use fast system
	 * library facilities to look up the current node.  No map.
	 */
	CPU_TABLE_FORMAT_NUMA_DIRECT,
#endif

	/*
	 * A map of processors to CPU sets indexes, for NUMA-nodes, with or
	 * without subsets, and for non-NUMA systems with subsets.
	 */
	CPU_TABLE_FORMAT_MAP,
};

/* Immutable and copyable once constructed. */
typedef struct cpu_table
{
	enum cpu_table_format format;
	int			partitions;
	int			partitions_per_numa_node;

	/* Members used for CPU_TABLE_FORMAT_MAP. */
	int			partition_to_numa_node[CPU_TABLE_MAX_PARTITIONS];
	int			map_shift;
	uint8_t		map[CPU_TABLE_ENTRIES];

	pg_cpuset_t initial_affinity;
} cpu_table;

extern PGDLLIMPORT int cpu_table_build(cpu_table *table,
									   int partitions_per_numa_node);

extern PGDLLIMPORT int cpu_table_partitions(const cpu_table *table);
extern PGDLLIMPORT int cpu_table_partitions_per_numa_node(const cpu_table *table);

extern PGDLLIMPORT int cpu_table_partition_to_numa_node(const cpu_table *table,
														int partition);
extern PGDLLIMPORT int cpu_table_cpu_to_partition(const cpu_table *table,
												  pg_cpu_t cpu);
extern PGDLLIMPORT void cpu_table_run_on_partition(const cpu_table *table,
												   int partition);

static inline uint8_t
cpu_table_hash_cpu(const cpu_table *table, pg_cpu_t cpu)
{
	int			value = PG_CPU_AS_INT(cpu);

	return ((value & CPU_TABLE_HASH_CPU1_MASK) |
			((value >> table->map_shift) & CPU_TABLE_HASH_CPU2_MASK));
}

static inline int
cpu_table_get(const cpu_table *table, pg_cpu_t cpu)
{
	uint8_t		hash;
	uint8_t		nibbles;

	hash = cpu_table_hash_cpu(table, cpu);
	nibbles = table->map[hash / CPU_TABLE_SUBENTRIES];
	if (hash % CPU_TABLE_SUBENTRIES)
		nibbles >>= CPU_TABLE_SUBENTRY_BITS;

	return nibbles & CPU_TABLE_SUBENTRY_MASK;
}

static inline int
cpu_table_current_partition(const cpu_table *table)
{
	if (table->format == CPU_TABLE_FORMAT_EMPTY)
		return 0;

#if defined(CPU_TABLE_HAVE_FORMAT_NUMA_DIRECT)
	if (table->format == CPU_TABLE_FORMAT_NUMA_DIRECT)
		return pg_numa_get_current_node();
#endif

	Assert(table->format == CPU_TABLE_FORMAT_MAP);
	return cpu_table_get(table, pg_cpu_current());
}

#endif							/* CPU_TABLE_H */
