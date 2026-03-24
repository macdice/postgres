#ifndef CPU_AFFINITY_H
#define CPU_AFFINITY_H

#include "storage/cpu_table.h"

extern PGDLLIMPORT int cpu_affinity;
extern PGDLLIMPORT cpu_table cpu_affinity_table;

#define CPU_AFFINITY_MAX_CPU_SETS CPU_TABLE_MAX_CPU_SETS

extern void cpu_affinity_initialize(void);
extern int	cpu_affinity_num_cpu_sets(void);
extern int	cpu_affinity_get_numa_node(int cpu_set_number);
extern int	cpu_affinity_get_cpu_set_for_cpu(pg_cpu_t cpu);
extern void cpu_affinity_run_on_cpu_set(int cpu_set_number);

/* Which CPU set is the caller running on right now? */
static inline int
cpu_affinity_current_cpu_set(void)
{
	return cpu_table_current_cpu_set(&cpu_affinity_table);
}

#endif
