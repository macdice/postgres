/*-------------------------------------------------------------------------
 *
 * barrier.c
 *	  Barriers for synchronizing cooperating processes.
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * This implementation of barriers allows for static sets of participants
 * known up front, or dynamic sets of participants which processes can join
 * or leave at any time.  In the dynamic case, a phase number can be used to
 * track progress through a parallel algorithm; in the static case it isn't
 * needed.
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/barrier.c
 *
 *-------------------------------------------------------------------------
 */

#include "storage/barrier.h"
#include "utils/probes.h"

/*
 * Initialize this barrier, setting a static number of participants that we
 * will wait for at each computation phase.  To use a dynamic number of
 * participants, this number should be zero, and BarrierAttach and
 * BarrierDetach should be used to register and deregister participants.
 */
void
BarrierInit(Barrier *barrier, int participants)
{
	SpinLockInit(&barrier->mutex);
	barrier->participants = participants;
	barrier->arrived = 0;
	barrier->phase = 0;
	ConditionVariableInit(&barrier->condition_variable);
}

/*
 * Wait for all participants to arrive at this barrier, and then return in all
 * participants.  Sets the phase to the given phase, which must not be equal
 * to the current phase.  The caller must be attached to this barrier.
 *
 * While waiting, pg_stat_activity shows a wait_event_class and wait_event
 * controlled by the wait_event_info passed in, which should be a value from
 * from one of the WaitEventXXX enums defined in pgstat.h.
 *
 * Return true in one arbitrarily selected participant (currently the first
 * one to arrive).  Return false in all others.  The differing return code
 * can be used to coordinate a phase of work that must be done in only one
 * participant while the others wait.
 */
bool
BarrierWaitSet(Barrier *barrier, int new_phase, uint32 wait_event_info)
{
	bool first;
	bool release;
	int phase;

	TRACE_POSTGRESQL_BARRIER_WAIT_START(new_phase);

	SpinLockAcquire(&barrier->mutex);
	Assert(barrier->phase != new_phase);
	++barrier->arrived;
	first = barrier->arrived == 1;
	if (barrier->arrived == barrier->participants)
	{
		release = true;
		barrier->arrived = 0;
		barrier->phase = new_phase;
	}
	else
	{
		release = false;
		phase = barrier->phase;
	}
	SpinLockRelease(&barrier->mutex);

	/* Check if we can release our peers and return. */
	if (release)
	{
		ConditionVariableBroadcast(&barrier->condition_variable);
		TRACE_POSTGRESQL_BARRIER_WAIT_DONE(new_phase, first);

		return first;
	}

	/* Wait for phase to change. */
	ConditionVariablePrepareToSleep(&barrier->condition_variable);
	for (;;)
	{
		SpinLockAcquire(&barrier->mutex);
		release = barrier->phase != phase;
		SpinLockRelease(&barrier->mutex);
		if (release)
			break;
		ConditionVariableSleep(&barrier->condition_variable, wait_event_info);
	}
	ConditionVariableCancelSleep();

	TRACE_POSTGRESQL_BARRIER_WAIT_DONE(new_phase, first);

	/* The callers should all agree on the new phase. */
	Assert(barrier->phase == new_phase);

	return first;
}

/*
 * Wait for all participants to arrive at this barrier, and then return in all
 * participants.  Advance the phase by one.  The caller must be attached to
 * this barrier.
 *
 * While waiting, pg_stat_activity shows a wait_event_class and wait_event
 * controlled by the wait_event_info passed in, which should be a value from
 * from one of the WaitEventXXX enums defined in pgstat.h.
 *
 * Return true in one arbitrarily selected participant (currently the first
 * one to arrive).  Return false in all others.  The differing return code
 * can be used to coordinate a phase of work that must be done in only one
 * worker while the others wait.
 */
bool
BarrierWait(Barrier *barrier, uint32 wait_event_info)
{
	/* See BarrierPhase for why it is safe to read the phase without lock. */
	return BarrierWaitSet(barrier, barrier->phase + 1, wait_event_info);
}

/*
 * Attach to a barrier.  All waiting participants will now wait for this
 * participant to call BarrierWait or BarrierDetach.  Return the current
 * phase.
 */
int
BarrierAttach(Barrier *barrier)
{
	int phase;

	TRACE_POSTGRESQL_BARRIER_ATTACH();

	SpinLockAcquire(&barrier->mutex);
	++barrier->participants;
	phase = barrier->phase;
	SpinLockRelease(&barrier->mutex);

	return phase;
}

/*
 * Detach from a barrier.  This may release other waiters from BarrierWait and
 * advance the phase, if they were only waiting for this backend.  Return
 * true if this participant was the last to detach.
 */
bool
BarrierDetach(Barrier *barrier)
{
	bool release;
	bool last;

	TRACE_POSTGRESQL_BARRIER_DETACH();

	SpinLockAcquire(&barrier->mutex);
	Assert(barrier->participants > 0);
	--barrier->participants;

	/*
	 * If any other participants are waiting and we were the last participant
	 * waited for, release them.
	 */
	if (barrier->participants > 0 &&
		barrier->arrived == barrier->participants)
	{
		release = true;
		barrier->arrived = 0;
		barrier->phase++;
	}
	else
		release = false;

	last = barrier->participants == 0;
	SpinLockRelease(&barrier->mutex);

	if (release)
		ConditionVariableBroadcast(&barrier->condition_variable);

	return last;
}

/*
 * Return the current phase of a barrier.  The caller must be attached.
 */
int
BarrierPhase(Barrier *barrier)
{
	/*
	 * It is OK to read barrier->phase without locking, because it can't
	 * change without us (we are attached to it), and we executed a memory
	 * barrier when we either attached or participated in changing it last
	 * time.
	 */
	return barrier->phase;
}

/*
 * Return an instantaneous snapshot of the number of participants currently
 * attached to this barrier.  For debugging purposes only.
 */
int
BarrierParticipants(Barrier *barrier)
{
	int participants;

	SpinLockAcquire(&barrier->mutex);
	participants = barrier->participants;
	SpinLockRelease(&barrier->mutex);

	return participants;
}
