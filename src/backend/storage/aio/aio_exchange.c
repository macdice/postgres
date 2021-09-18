/*-------------------------------------------------------------------------
 *
 * aio_exchange.c
 *	  Routines for coordinating which backend will complete an IO.
 *
 * This module provides a common implementation of wait_one() current used by
 * "posix_aio" and "iocp".  With those kernel APIs, only the submitting
 * process can consume an IO's result, so we need a way to handle the
 * (hopefully rare) cases where a backend finishes up waiting for an IO that
 * another backend submitted.  This is accomplished by using an atomic
 * variable to "pass the exchange" from the submitter to the completer, with an
 * interrupt to make sure that progress can always be made.
 *
 * XXX TODO: Is there a better name than "exchange"?
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
		io->interlock.exchange.raw_result = INT_MIN;
	}
}

void
pgaio_exchange_submit_one(PgAioInProgress *io)
{
	pg_atomic_add_fetch_u32(&my_aio->inflight_count, 1);

	for (PgAioInProgress * cur = io;;)
	{
		cur->interlock.exchange.head_idx = pgaio_io_id(io);
		if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
			break;
		cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
	}

	io->interlock.exchange.raw_result = INT_MIN;
}

void
pgaio_exchange_process_completion(PgAioInProgress * io,
								  int raw_result,
								  bool in_interrupt_handler)
{
	Assert(io->submitter_id == my_aio_id);
	Assert(io->interlock.exchange.raw_result == INT_MIN);

	if (unlikely(in_interrupt_handler))
	{
		/*
		 * Can't process the completion from here, but we can make it
		 * available to other backends with operations that are safe from a
		 * signal handler.
		 */
		io->interlock.exchange.raw_result = raw_result;
		ConditionVariableSignalFromSignalHandler(&io->cv);
		return;
	}
	
	pgaio_process_io_completion(io, raw_result);

	/*
	 * XXX It's a shame to broadcast on the CV here (because
	 * pgaio_exchange_wait_one() is waiting for INFLIGHT to clear), and then
	 * again in the shared callbacks!  This may be impetus to implement the
	 * empty CV optimisation XXX
	 */
	ConditionVariableBroadcast(&io->cv);
}

void
pgaio_exchange_wait_one(PgAioContext *context, PgAioInProgress * io, uint64 ref_generation, uint32 wait_event_info)
{
	PgAioInProgress *head_io;
	PgAioIPFlags flags;
	int			raw_result;

	/* Find the head of the merge chain (could be out of date if gen advanced). */
	head_io = &aio_ctl->in_progress_io[io->interlock.exchange.head_idx];

	/* If I submitted, drain until it's no longer in flight. */
	if (io->submitter_id == my_aio_id)
	{
		/* Prevent the interrupt handler from running. */
		pgaio_exchange_disable_interrupt();

		/*
		 * It might have reaped a raw result while it was enabled, in which
		 * case we have to negotiate for the right to process completions.
		 */
		if ((raw_result = io->interlock.exchange.raw_result) != INT_MIN)
		{
			if (!pg_atomic_exchange_u32(&io->interlock.exchange.interruptible, false))
				goto out;
			if (pgaio_io_recycled(io, ref_generation, &flags) ||
				!(flags & PGAIOIP_INFLIGHT))
				goto out;
			pgaio_exchange_process_completion(io, raw_result, false);
			goto out;
		}

		/*
		 * Mark IO non-interruptible, to prevent useless interruptions while
		 * we're draining.
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
		return;
	}

	/*
	 * Elect one backend to become the interruptor, if that turns out to be
	 * necessary.
	 */
	if (!pg_atomic_exchange_u32(&head_io->interlock.exchange.interruptible, false))
	{
		/*
		 * Another backend had already cleared it.  Wait for it to clear the
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
	 */
	ConditionVariablePrepareToSleep(&io->cv);
	for (int backoff = 10;; backoff = Min(backoff * 2, 1000))
	{
		int			raw_result;

		/* Has the submitter done it? */
		if (pgaio_io_recycled(io, ref_generation, &flags) ||
			!(flags & PGAIOIP_INFLIGHT))
			break;
		
		/* Has the submitter given us the result? */
		raw_result = head_io->interlock.exchange.raw_result;
		if (raw_result != INT_MIN)
		{
			pgaio_exchange_process_completion(head_io, raw_result, false);
			break;
		}

		ConditionVariableTimedSleep(&io->cv, backoff, wait_event_info);
	}
	ConditionVariableCancelSleep();
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
 * Disable interrupt processing.  This avoids undefined behaviour in
 * aio_suspend() for IOs that have already returned, and problems with
 * implementations that are not as async signal safe as they should be.
 */
void
pgaio_exchange_disable_interrupt(void)
{
	pgaio_exchange_interrupt_holdoff++;
}

/*
 * Renable interrupt processing, and handle any interrupts we missed.
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
