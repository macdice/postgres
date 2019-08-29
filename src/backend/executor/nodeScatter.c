/*-------------------------------------------------------------------------
 *
 * nodeScatter.c
 *	  Support routines for distributing tuples among workers.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeScatter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/nodeScatter.h"
#include "storage/shm_mmq.h"

#define SCATTER_QUEUE_SIZE 32

static TupleTableSlot *
ExecScatter(PlanState *pstate)
{
	//ScatterState *node = castNode(ScatterState, pstate);

	return NULL;
}

ScatterState *
ExecInitScatter(Scatter *node, EState *estate, int eflags)
{   
	ScatterState *state;

	state = makeNode(ScatterState);
	state->ps.plan = (Plan *) node;
	state->ps.state = estate;
	state->ps.ExecProcNode = ExecScatter;

	return state;
}

void
ExecEndScatter(ScatterState *node)
{
}

void
ExecReScanScatter(ScatterState *node)
{
}

void
ExecScatterEstimate(ScatterState *node, ParallelContext *pcxt)
{
	//EState	   *estate = node->ps.state;

	node->mmq_size = shm_mmq_estimate(SCATTER_QUEUE_SIZE);
	shm_toc_estimate_chunk(&pcxt->estimator, node->mmq_size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecScatterInitializeDSM(ScatterState *node, ParallelContext *pcxt)
{
	//EState	   *estate = node->ps.state;
	shm_mmq	   *mmq;

	mmq = shm_toc_allocate(pcxt->toc, node->mmq_size);
	shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, mmq);

	node->mmq_handle = shm_mmq_init(mmq, SCATTER_QUEUE_SIZE);
}

void
ExecScatterReInitializeDSM(ScatterState *node,
						   ParallelContext *pcxt)
{
}

void
ExecScatterInitializeWorker(ScatterState *node,
							ParallelWorkerContext *pwcxt)
{
	shm_mmq *mmq;

	mmq = shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
	node->mmq_handle = shm_mmq_attach(mmq);
}
