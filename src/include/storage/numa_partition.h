#ifndef NUMA_PARTITION_H
#define NUMA_PARTITION_H

#include "storage/cpu_table.h"

extern PGDLLIMPORT int numa_partitions;
extern PGDLLIMPORT cpu_table numa_partition_table;

#define MAX_NUMA_PARTITIONS CPU_TABLE_MAX_PARTITIONS

extern void numa_partition_initialize(void);
extern int	numa_partition_count(void);
extern int	numa_partition_count_per_numa_node(void);
extern int	numa_partition_to_numa_node(int partition);
extern int	numa_partition_for_cpu(pg_cpu_t cpu);
extern void numa_partition_pin_worker(int partition);

/* Which NUMA partition is the caller running on right now? */
static inline int
numa_partition_current(void)
{
	return cpu_table_current_partition(&numa_partition_table);
}

#endif
