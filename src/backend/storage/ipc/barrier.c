/*-------------------------------------------------------------------------
 *
 * barrier.c
 *	  Barriers for synchronizing cooperating processes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This implementation of barriers allows for static sets of participants
 * known up front, or dynamic sets of participants which processes can join or
 * leave at any time.  In the dynamic case, a phase number can be used to
 * track progress through a parallel algorithm, and may be necessary to
 * synchronize with the current phase of a multi-phase algorithm when a new
 * participant joins.  In the static case, the phase number is used
 * internally, but it isn't strictly necessary for client code to access it
 * because the phase can only advance when the declared number of participants
 * reaches the barrier so client code should be in no doubt about the current
 * phase of computation at all times.
 *
 * Consider a parallel algorithm that involves separate phases of computation
 * A, B and C where the output of each phase is needed before the next phase
 * can begin.
 *
 * In the case of a static barrier initialized with 4 participants, each
 * participant works on phase A, then calls BarrierWait to wait until all 4
 * participants have reached that point.  When BarrierWait returns control,
 * each participant can work on B, and so on.  Because the barrier knows how
 * many participants to expect, the phases of computation don't need labels or
 * numbers, since each process's program counter implies the current phase.
 * Even if some of the processes are slow to start up and begin running phase
 * A, the other participants are expecting them and will patiently wait at the
 * barrier.  The code could be written as follows:
 *
 *     perform_a();
 *     BarrierWait(&barrier, ...);
 *     perform_b();
 *     BarrierWait(&barrier, ...);
 *     perform_c();
 *     BarrierWait(&barrier, ...);
 *
 * If the number of participants is not known up front, then a dynamic barrier
 * is needed and the number should be set to zero at initialization.  New
 * complications arise because the number necessarily changes over time as
 * participants attach and detach, and therefore phases B, C or even the end
 * of processing may be reached before any given participant has started
 * running and attached.  Therefore the client code must perform an initial
 * test of the phase number after attaching, because it needs to find out
 * which phase of the algorithm has been reached by any participants that are
 * already attached in order to synchronize with that work.  Once the program
 * counter or some other representation of current progress is synchronized
 * with the barrier's phase, normal control flow can be used just as in the
 * static case.  Our example could be written using a switch statement with
 * cases that fall-through, as follows:
 *
 *     phase = BarrierAttach(&barrier);
 *     switch (phase)
 *     {
 *     case PHASE_A:
 *         perform_a();
 *         BarrierWait(&barrier, ...);
 *     case PHASE_B:
 *         perform_b();
 *         BarrierWait(&barrier, ...);
 *     case PHASE_C:
 *         perform_c();
 *         BarrierWait(&barrier, ...);
 *     default:
 *     }
 *     BarrierDetach(&barrier);
 *
 * Static barriers behave similarly to POSIX's pthread_barrier_t.  Dynamic
 * barriers behave similarly to Java's java.util.concurrent.Phaser.
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/barrier.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/barrier.h"

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
 * participants.  Increments the current phase.  The caller must be attached
 * to this barrier.
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
BarrierWait(Barrier *barrier, uint32 wait_event_info)
{
	bool first;
	bool last;
	int start_phase;
	int next_phase;

	SpinLockAcquire(&barrier->mutex);
	start_phase = barrier->phase;
	next_phase = start_phase + 1;
	++barrier->arrived;
	if (barrier->arrived == 1)
		first = true;
	else
		first = false;
	if (barrier->arrived == barrier->participants)
	{
		last = true;
		barrier->arrived = 0;
		barrier->phase = next_phase;
	}
	else
		last = false;
	SpinLockRelease(&barrier->mutex);

	/*
	 * If we were the last expected participant to arrive, we can release our
	 * peers and return.
	 */
	if (last)
	{
		ConditionVariableBroadcast(&barrier->condition_variable);
		return first;
	}

	/*
	 * Otherwise we have to wait for the last participant to arrive and
	 * advance the phase.
	 */
	ConditionVariablePrepareToSleep(&barrier->condition_variable);
	for (;;)
	{
		bool advanced;

		/*
		 * We know that phase must either be start_phase, indicating that we
		 * need to keep waiting, or next_phase, indicating that the last
		 * participant that we were waiting for has either arrived or detached
		 * so that the next phase has begun.  The phase cannot advance any
		 * further than that without this backend's participation, because
		 * this backend is attached.
		 */
		SpinLockAcquire(&barrier->mutex);
		Assert(barrier->phase == start_phase || barrier->phase == next_phase);
		advanced = barrier->phase == next_phase;
		SpinLockRelease(&barrier->mutex);
		if (advanced)
			break;
		ConditionVariableSleep(&barrier->condition_variable, wait_event_info);
	}
	ConditionVariableCancelSleep();

	return first;
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
		++barrier->phase;
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
