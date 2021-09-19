/*-------------------------------------------------------------------------
 *
 * aio_exchange.c
 *	  Routines for coordinating which backend will complete an IO.
 *
 * This module is used by implementations where only the backend that submits
 * an IO is allowed to consume the result from the kernel.  In order to avoid
 * deadlocks, backends are allowed to interrupt others to ask for the result.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/aio_exchange.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/wait_event.h"

static volatile sig_atomic_t pgaio_exchange_interrupt_pending;
static volatile sig_atomic_t pgaio_exchange_interrupt_holdoff;

void
pgaio_exchange_shmem_init(void)
{
	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[i];

		pg_atomic_init_u32(&io->interlock.exchange.interruptible, false);
		io->interlock.exchange.result = INT_MIN;
	}
}

void
pgaio_exchange_submit_one(PgAioInProgress *io)
{
	for (PgAioInProgress * cur = io;;)
	{
		cur->interlock.exchange.head_idx = pgaio_io_id(io);
		if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
			break;
		cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
	}

	pg_atomic_write_u32(&io->interlock.exchange.interruptible, true);
	io->interlock.exchange.result = INT_MIN;
}

void
pgaio_exchange_process_completion(PgAioInProgress * io,
								  int result,
								  bool in_interrupt_handler)
{
	if (unlikely(in_interrupt_handler))
	{
		/*
		 * Can't process the completion from here, but we can make it
		 * available to other backends with operations that are safe from a
		 * signal handler.
		 */
		Assert(io->submitter_id == my_aio_id);
		Assert(io->interlock.exchange.result == INT_MIN);
		io->interlock.exchange.result = result;
		ConditionVariableSignalFromSignalHandler(&io->cv);
		return;
	}

	pgaio_process_io_completion(io, result);

	/*
	 * XXX It's a shame to broadcast on the CV here (because
	 * pgaio_exchange_wait_one() is waiting for INFLIGHT to clear), and then
	 * again in the shared callbacks!  This may be impetus to implement the
	 * empty CV optimisation XXX
	 */
	ConditionVariableBroadcast(&io->cv);
}

static void
pgaio_exchange_wait_one_local(PgAioInProgress * io, uint64 ref_generation, uint32 wait_event_info)
{
	PgAioInProgress *head_io;
	PgAioIPFlags flags;
	int			result;

	/* Prevent the interrupt handler from running. */
	pgaio_exchange_disable_interrupt();

	/* Find the head of the merge chain (could be out of date if gen advanced). */
	head_io = &aio_ctl->in_progress_io[io->interlock.exchange.head_idx];

	/*
	 * We might have consumed a result form the kernel while the interrupt was
	 * enabled.  In that case, we have to negotiate for the right to process
	 * completions, and wait for someone else to do so if we lose.
	 */
	if ((result = head_io->interlock.exchange.result) != INT_MIN)
	{
		if (!pg_atomic_exchange_u32(&head_io->interlock.exchange.interruptible, false))
		{
			/*
			 * Another backend has already cleared it.  Wait for it to clear the
			 * INFLIGHT flag, or turn the interruptible flag back on and wake us.
			 */
			ConditionVariablePrepareToSleep(&head_io->cv);
			if (!pgaio_io_recycled(io, ref_generation, &flags) &&
				(flags & PGAIOIP_INFLIGHT))
				ConditionVariableSleep(&head_io->cv, wait_event_info);
			ConditionVariableCancelSleep();
			goto out;
		}
		pgaio_exchange_process_completion(head_io, result, false);
		goto out;
	}

	/*
	 * Mark IO non-interruptible, to prevent useless interruptions while we're
	 * draining.  We don't care if someone else has already cleared it,
	 * because we'll be handling the completion directly (we won't write the
	 * result into shared memory).
	 */
	pg_atomic_write_u32(&head_io->interlock.exchange.interruptible, false);

	for (;;)
	{
		if (pgaio_io_recycled(io, ref_generation, &flags) ||
			(flags & PGAIOIP_INFLIGHT))
			break;
		pgaio_drain(NULL,
					/* block = */ true,
					/* call_shared = */ false,
					/* call_local = */ false);
	}

 out:
	pgaio_exchange_enable_interrupt();
}

static void
pgaio_exchange_wait_one_foreign(PgAioInProgress * io, uint64 ref_generation, uint32 wait_event_info)
{
	PgAioInProgress *head_io;
	PgAioIPFlags flags;

	/* Find the head of the merge chain (could be out of date if gen advanced). */
	head_io = &aio_ctl->in_progress_io[io->interlock.exchange.head_idx];

	/*
	 * Elect one backend to become the interruptor, if that turns out to be
	 * necessary.  That doesn't mean that we'll definitely process the
	 * completion (the submitter might decide to do it itself), but if the
	 * submitter decides to share the result then the winner of this election
	 * will do it.
	 */
	if (!pg_atomic_exchange_u32(&head_io->interlock.exchange.interruptible, false))
	{
		/*
		 * Another backend has already cleared it.  Wait for that to clear the
		 * INFLIGHT flag, or turn the interruptible flag back on and wake us.
		 */
		ConditionVariablePrepareToSleep(&head_io->cv);
		if (!pgaio_io_recycled(io, ref_generation, &flags) &&
			(flags & PGAIOIP_INFLIGHT))
			ConditionVariableSleep(&head_io->cv, wait_event_info);
		ConditionVariableCancelSleep();
		return;
	}

	/* I won, but are we looking at a later generation? */
	if (pgaio_io_recycled(io, ref_generation, &flags))
	{
		/*
		 * We don't have to wait any more, but we have to turn the
		 * interruptible flag back on and wake everyone up.
		 */
		pg_atomic_write_u32(&io->interlock.exchange.interruptible, true);
		ConditionVariableBroadcast(&io->cv);
		return;
	}

	/*
	 * Wait for the submitter to tell us the result, or to process completions
	 * itself and wake us up.  Interrupt it periodically, to make sure that it
	 * eventually does that even if it's blocked (perhaps on a lock we hold).
	 * Start with 10ms and back off until we reach 1 second.
	 */
	ConditionVariablePrepareToSleep(&io->cv);
	for (int backoff = 10;; backoff = Min(backoff * 2, 1000))
	{
		int			result;

		/* Has the submitter decided to do it itself? */
		if (pgaio_io_recycled(io, ref_generation, &flags) ||
			!(flags & PGAIOIP_INFLIGHT))
			break;

		/* Has the submitter given us the result? */
		result = head_io->interlock.exchange.result;
		if (result != INT_MIN)
		{
			pgaio_exchange_process_completion(head_io, result, false);
			break;
		}

		/* Interrupt the submitter. */
		SendProcSignal(ProcGlobal->allProcs[io->submitter_id].pid,
					   PROCSIG_AIO_INTERRUPT,
					   InvalidBackendId);

		ConditionVariableTimedSleep(&io->cv, backoff, wait_event_info);
	}
	ConditionVariableCancelSleep();
}

void
pgaio_exchange_wait_one(PgAioContext *context, PgAioInProgress * io, uint64 ref_generation, uint32 wait_event_info)
{
	/*
	 * XXX Would it be possible for us to see an out of date submitter_id,
	 * after the IO was retried by another backend?  Should we clear our own
	 * submitter_id when completing to avoid that?
	 */

	if (io->submitter_id == my_aio_id)
		pgaio_exchange_wait_one_local(io, ref_generation, wait_event_info);
	else
		pgaio_exchange_wait_one_foreign(io, ref_generation, wait_event_info);
}

/*
 * Collect results from any IOs that other backends are waiting for.
 *
 * Runs in signal handler context.  Also runs in user context.
 */
static void
pgaio_exchange_process_interrupt(bool in_signal_handler)
{
	/*
	 * XXX: Bogus pointer is a way to tell pgaio_{posix_aio,iocp}_drain()
	 * if we're in a signal handler!  FIXME, need extensible flags or
	 * something
	 */
	pgaio_drain(/* context = */ (void *) in_signal_handler,
				/* block = */ false,
				/* call_shared = */ false,
				/* call_local = */ false);
}

/*
 * Disable interrupt processing.
 */
void
pgaio_exchange_disable_interrupt(void)
{
	pgaio_exchange_interrupt_holdoff++;
}

/*
 * Re-enable interrupt processing, and handle any interrupts we missed.
 */
void
pgaio_exchange_enable_interrupt(void)
{
	Assert(pgaio_exchange_interrupt_holdoff > 0);
	if (--pgaio_exchange_interrupt_holdoff == 0)
	{
		while (pgaio_exchange_interrupt_pending)
		{
			pgaio_exchange_interrupt_pending = false;
			pgaio_exchange_interrupt_holdoff++;
			pgaio_exchange_process_interrupt(false);
			pgaio_exchange_interrupt_holdoff--;
		}
	}
}

/*
 * Handler for PROCSIG_AIO_INTERRUPT.  Called in signal handler when another
 * backend is blocked waiting for an IO that this backend submitted.
 */
void
HandleAioInterrupt(void)
{
	/*
	 * If exchange interrupts are currently disabled, just remember that the
	 * interrupt arrived.  It'll be processed when they're reenabled.
	 */
	if (pgaio_exchange_interrupt_holdoff > 0)
	{
		pgaio_exchange_interrupt_pending = true;
		return;
	}
	pgaio_exchange_process_interrupt(true);
}
