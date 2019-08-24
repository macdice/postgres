/*-------------------------------------------------------------------------
 *
 * nodeScatter.h
 *	  prototypes for nodeScatter.c
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeScatter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESCATTER_H
#define NODESCATTER_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern ScatterState *ExecInitScatter(Scatter *node, EState *estate, int eflags);
extern void ExecEndScatter(ScatterState *node);
extern void ExecReScanScatter(ScatterState *node);

extern void ExecScatterEstimate(ScatterState *node, ParallelContext *pcxt);
extern void ExecScatterInitializeDSM(ScatterState *node,
									 ParallelContext *pcxt);
extern void ExecScatterReInitializeDSM(ScatterState *node,
									   ParallelContext *pcxt);
extern void ExecScatterInitializeWorker(ScatterState *node,
										ParallelWorkerContext *pwcxt);
extern void ExecShutdownScatter(ScatterState *node);

#endif							/* NODESCATTER_H */
