/*-------------------------------------------------------------------------
 *
 * barrier.h
 *	  Barriers for synchronizing workers.
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * src/include/storage/barrier.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BARRIER_H
#define BARRIER_H

/*
 * For the header previously known as "barrier.h", please include
 * "port/atomics.h", which deals with atomics, compiler barriers and memory
 * barriers.
 */

#include "storage/condition_variable.h"
#include "storage/spin.h"

typedef struct Barrier
{
	slock_t mutex;
	int phase;
	int participants;
	int arrived;
	ConditionVariable condition_variable;
} Barrier;

extern void BarrierInit(Barrier *barrier, int num_workers);
extern bool BarrierWait(Barrier *barrier, uint32 wait_event_info);
extern int BarrierAttach(Barrier *barrier);
extern bool BarrierDetach(Barrier *barrier);
extern int BarrierPhase(Barrier *barrier);
extern int BarrierParticipants(Barrier *barrier);

#endif   /* BARRIER_H */
