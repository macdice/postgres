/*-------------------------------------------------------------------------
 *
 * cpu_funcs.c
 *    SQL interface for CPU information
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/ipc/cpu_funcs.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "port/pg_cpuset.h"
#include "port/pg_numa.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

Datum
pg_get_cpus(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int num_numa_nodes;

	num_numa_nodes = pg_numa_get_max_node() + 1;

	InitMaterializedSRF(fcinfo, 0);

	for (int node = 0; node < num_numa_nodes; ++node)
	{
		pg_cpuset_t node_cpus;
		pg_cpuset_iterator iter;

		/* Try to use the NUMA API to discover CPUs. */
		pg_cpuset_initialize(&node_cpus);
		if (pg_numa_get_cpus_for_node(node, &node_cpus) < 0)
			elog(ERROR, "pg_numa_get_cpus_for_node() failed: %m");
		/* No NUMA support?  Fall back to current affinity. */
		if (num_numa_nodes == 1 &&
			node == 0 &&
			pg_cpuset_is_empty(&node_cpus) &&
			pg_cpuset_get_process_affinity_self(&node_cpus) < 0 &&
			errno != ENOSYS)
			elog(ERROR, "pg_cpuset_get_process_affinity_self() failed: %m");

		pg_cpuset_iterator_begin(&node_cpus, &iter);
		while (pg_cpuset_iterator_has_next(&iter))
		{
			pg_cpu_t	cpu = pg_cpuset_iterator_next(&iter);
#define PG_GET_CPUS_COLS	3
			Datum		values[PG_GET_CPUS_COLS];
			bool		nulls[PG_GET_CPUS_COLS];

			values[0] = Int32GetDatum(PG_CPU_NUMBER(cpu));
			nulls[0] = false;

			values[1] = Int32GetDatum(PG_CPU_GROUP(cpu));
			nulls[1] = !PG_CPU_HAS_GROUP;

			values[2] = Int32GetDatum(node);
			nulls[2] = false;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	return (Datum) 0;
}
