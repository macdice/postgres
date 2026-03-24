#ifndef CPU_AFFINITY_H
#define CPU_AFFINITY_H

#include "port/pg_cpuset.h"

typedef struct CpuAffinityControl
{
	pg_cpuset_table table;
} CpuAffinityControl;

extern PGDLLIMPORT int cpu_affinity;

extern PGDLLIMPORT CpuAffinityControl *cpu_affinity_control;

/*
 * Which CPU set is the caller running on right now?  The result is greater
 * than zero and less than cpu_affinity_cpu_sets().
 */
static inline int
cpu_affinity_current_cpuset(void)
{
	return pg_cpuset_table_current(&cpu_affinity_control->table);
}

extern size_t CpuAffinityShmemSize(void);
extern void CpuAffinityShmemInit(void);

extern int	cpu_affinity_cpu_sets(void);
extern int	cpu_affinity_numa_node_for_cpu_set(int n);
extern void cpu_affinity_run_on_cpu_set(int n);

#endif
