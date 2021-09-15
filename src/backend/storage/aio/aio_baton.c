/*-------------------------------------------------------------------------
 *
 * aio_baton.c
 *	  Routines for coordinating which backend will complete an IO.
 *
 * This module provides a common implementation of wait_one() current used by
 * "posix_aio" and "iocp".  With those kernel APIs, only the submitting
 * process can consume an IO's result, so we need a way to handle the
 * (hopefully rare) cases where a backend finishes up waiting for an IO that
 * another backend submitted.  This is accomplished by using an atomic
 * variable to "pass the baton" from the submitter to the completer, with an
 * interrupt to make sure that progress can always be made.
 *
 * XXX TODO: Is there a better name than "baton"?
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/aio_baton.c
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

#define PGAIO_BATON_STATE_MASK			0x0f00000000000000
#define PGAIO_BATON_SUBMITTER_MASK		0x00ffffff00000000
#define PGAIO_BATON_COMPLETER_MASK		0x0000000000ffffff
#define PGAIO_BATON_SUBMITTER_SHIFT		32

#define PGAIO_BATON_INVALID_COMPLETER	0xffffffff

#define PGAIO_BATON_SUBMITTED			0x0100000000000000
#define PGAIO_BATON_HAS_WAITER			0x0200000000000000
#define PGAIO_BATON_COMPLETED			0x0300000000000000

static volatile sig_atomic_t pgaio_baton_interrupt_pending;
static volatile sig_atomic_t pgaio_baton_interrupt_holdoff;

static uint64
pgaio_make_baton(uint64 state, uint32 submitter_id, uint32 completer_id)
{
	return state |
		(((uint64) submitter_id) << PGAIO_BATON_SUBMITTER_SHIFT) |
		completer_id;
}

static uint32
pgaio_baton_submitter(uint64 baton)
{
	return (baton & PGAIO_BATON_SUBMITTER_MASK) >>
		PGAIO_BATON_SUBMITTER_SHIFT;
}

static uint32
pgaio_baton_completer(uint64 baton)
{
	return baton & PGAIO_BATON_COMPLETER_MASK;
}

static uint64
pgaio_baton_state(uint64 baton)
{
	return baton & PGAIO_BATON_STATE_MASK;
}

static uint64
pgaio_read_baton(PgAioInProgress *io)
{
	return pg_atomic_read_u64(&io->interlock.baton.baton);
}

static bool
pgaio_update_baton(PgAioInProgress * io,
				   uint64 baton,
				   uint64 state,
				   uint32 submitter_id,
				   uint32 completer_id)
{
	return pg_atomic_compare_exchange_u64(&io->interlock.baton.baton,
										  &baton,
										  pgaio_make_baton(state,
														   submitter_id,
														   completer_id));
}

void
pgaio_baton_shmem_init(void)
{
	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[i];

		pg_atomic_init_u64(&io->interlock.baton.baton,
						   pgaio_make_baton(0,
											0,
											PGAIO_BATON_INVALID_COMPLETER));
	}
}

void
pgaio_baton_submit_one(PgAioInProgress *io)
{
	pg_atomic_add_fetch_u32(&my_aio->inflight_count, 1);

	for (PgAioInProgress * cur = io;;)
	{
		cur->interlock.baton.head_idx = pgaio_io_id(io);
		if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
			break;
		cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
	}

	io->interlock.baton.raw_result = INT_MIN;
	pg_atomic_write_u64(&io->interlock.baton.baton,
						pgaio_make_baton(PGAIO_BATON_SUBMITTED,
										 my_aio_id,
										 -1));
}

static void
pgaio_baton_wake(PgAioInProgress *io, uint completer_id)
{
	pg_atomic_fetch_sub_u32(&my_aio->baton_waiter_count, 1);
	if (completer_id != -1)
		SetLatch(&ProcGlobal->allProcs[completer_id].procLatch);
}

/*
 * The kernel has provided the result for an IO.  Process completion callbacks
 * immediately if we're not in an interrupt handler; hopefully this is the
 * common case.
 *
 * It is called in four different scenarios.  Here XXX stands for
 * pgaio_{posix_aio,iocp}:
 *
 * 1.  A backend submits an IO and then XXX_drain() consumes the result from
 * the kernel some time later (because we opportunii...
 *
 * 2.  XXX_submit() fails to submit an IO, and immediately records the error.
 *
 * 3.  Another backend waits in pgaio_baton_wait_one() for an IO that was
 * submitted by this backend.  It signals When pgaio_baton_process_interrupt() handles a
 * request for
 *
 * If we're in an interrupt handler (because a blocked backend asked us to
 * collect the result from the kernel, it's not safe to run completion
 * callbacks, or do much else.  We can write to shared memory and set latches,
 * though.  If another backend is waiting on this IO, pass the result to it so
 * that it can make progress.  Otherwise, store the result for the next
 * backend to wait on the IO.
 *
 * XX Xblah blah
 */
void
pgaio_baton_process_completion(PgAioInProgress * io,
							   int raw_result,
							   bool in_interrupt_handler)
{
	uint64 baton;
	uint32 submitter_id;
	uint32 completer_id;
	uint64 state;

	/*
	 * If we're in a signal handler, we can't do very much at all.  Just write
	 * the result into shared memory and wake up the waiter, if there is one.
	 */
	if (unlikely(in_interrupt_handler))
	{
		uint32		waiter_id;

		io->interlock.baton.raw_result = raw_result;
		pg_memory_barrier();

		/*
		 * XXX Provide a function pg_atomic_read_u32_from_u64() that provided
		 * non-torn read of the lower half of a u64 even on platforms where
		 * atomic 64 bit is simulated
		 */
		waiter_id = pgaio_read_baton(io);

		if (waiter_id != PGAIO_BATON_INVALID_COMPLETER)
			pgaio_baton_wake(io, waiter_id);

		return;
	}

	/*
	 * Use CAS to serialize with any backend that registers as completer.  We
	 * need to be able to wake it up (it didn't get the job).
	 */
	do
	{
		baton = pgaio_read_baton(io);
		submitter_id = pgaio_baton_submitter(baton);
		completer_id = pgaio_baton_completer(baton);
		state = pgaio_baton_state(baton);

		/*
		 * This should only be called for IOs that this backend submitted, and
		 * we should definitely see a fresh enough view of that without memory
		 * barriers: this backend wrote the submitter ID.
		 */
		Assert(submitter_id == my_aio_id);
		Assert(state == PGAIO_BATON_SUBMITTED ||
			   state == PGAIO_BATON_HAS_WAITER);

	}
	while (!pgaio_update_baton(io,
							   baton,
							   PGAIO_BATON_COMPLETED,
							   submitter_id,
							   my_aio_id));

	/* Process completions. */
	pgaio_process_io_completion(io, raw_result);

	/* If there was a backend waiting, now wake it up. */
	if (state == PGAIO_BATON_HAS_WAITER && completer_id != my_aio_id)
		pgaio_baton_wake(io, completer_id);
}

/*

 */
void
pgaio_baton_wait_one(PgAioContext *context, PgAioInProgress * io, uint64 ref_generation, uint32 wait_event_info)
{
	PgAioInProgress *head_io;
	uint32		head_idx;
	uint32		submitter_id;
	uint32		completer_id;
	uint64		baton;
	uint64		state;
	int			raw_result;

	/*
	 * To wait for a ...
	 */
	head_idx = io->interlock.baton.head_idx;
	head_io = &aio_ctl->in_progress_io[head_idx];

	for (;;)
	{
		PgAioIPFlags flags;

		if (pgaio_io_recycled(io, ref_generation, &flags) ||
			!(flags & PGAIOIP_INFLIGHT))
			break;

		baton = pgaio_read_baton(head_io);
		submitter_id = pgaio_baton_submitter(baton);
		completer_id = pgaio_baton_completer(baton);
		state = pgaio_baton_state(baton);

		pg_read_barrier();

		raw_result = head_io->interlock.baton.raw_result;

		if (state == PGAIO_BATON_SUBMITTED)
		{
			if (raw_result != INT_MIN)
			{
				/*
				 * Result is here already, but there's no waiter.  It must
				 * have been drained by an interrupt handler than couldn't
				 * process it.  We can now do it immediately.
				 */
				if (!pgaio_update_baton(head_io,
										baton,
										PGAIO_BATON_COMPLETED,
										submitter_id,
										my_aio_id))
					continue;	/* lost race, try again */
				pgaio_process_io_completion(head_io, raw_result);
				pgaio_complete_ios(false);
				break;
			}

			/* Try to become the registered completer. */
			if (!pgaio_update_baton(head_io,
									baton,
									PGAIO_BATON_HAS_WAITER,
									submitter_id,
									my_aio_id))
				continue;	/* lost race, try again */

			/* If I submitted it, go around again so we can drain. */
			/* XXX restructure so we can fall through */
			if (submitter_id == my_aio_id)
				continue;

			/*
			 * Interrupt the submitter to tell it we are waiting.
			 *
			 * XXX Looking up the backendId would make this more
			 * efficient, but it seems to be borked for the startup
			 * process, which advertises a bogus backendId in its PGPROC.
			 * *FIXME*
			 */
			pg_atomic_fetch_add_u32(&aio_ctl->backend_state[submitter_id].baton_waiter_count,
									1);
			SendProcSignal(ProcGlobal->allProcs[submitter_id].pid,
						   PROCSIG_AIO_INTERRUPT,
						   InvalidBackendId);
		}
		else if (state == PGAIO_BATON_HAS_WAITER)
		{
			if (raw_result != INT_MIN)
			{
				/*
				 * There's already a waiter (possibly me).  Since the result
				 * is available and I'm on CPU right now, steal it.
				 */
				if (!pgaio_update_baton(head_io,
										baton,
										PGAIO_BATON_COMPLETED,
										submitter_id,
										my_aio_id))
					continue;	/* lost race, try again */

				pgaio_process_io_completion(head_io, raw_result);
				pgaio_complete_ios(false);

				/* Wake previous waiter, if not me. */
				if (completer_id != my_aio_id)
					pgaio_baton_wake(head_io, completer_id);

				break;
			}
			else if (submitter_id == my_aio_id)
			{
				/*
				 * I'm this IO's submitter.  It would probably save some
				 * ping-pong if the submitter does the completion work, so
				 * steal it.
				 */
				if (completer_id != my_aio_id)
				{
					if (!pgaio_update_baton(head_io,
											baton,
											PGAIO_BATON_HAS_WAITER,
											submitter_id,
											my_aio_id))
						continue;	/* lost race, try again */

					/* Wake previous waiter.  It'll wait on the CV. */
					pgaio_baton_wake(head_io, completer_id);
				}

				/* Since I submitted it, I need to drain to make progress. */
				pgaio_drain(NULL,
							/* block = */ true,
							/* call_shared = */ true,
							/* call_local = */ true);
				pgaio_complete_ios(false);
			}
			else if (completer_id == my_aio_id)
			{
				/*
				 * I'm registered as the completer, and I'm waiting for the
				 * submitter to either give me the result or process
				 * completions and wake me up.
				 */
				WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, 0);
				ResetLatch(MyLatch);
			}
			else
			{
				/*
				 * Someone else is already signed up and waiting to complete
				 * this IO.  We have to wait on the condition variable.
				 */
				ConditionVariableSleep(&io->cv,
									   WAIT_EVENT_AIO_IO_COMPLETE_ONE);
				break;
			}
		}
		else
		{
			/* Already completed, recycled, etc. */
			break;
		}
	}

	ConditionVariableCancelSleep();
}

/*
 * Collect results from any IOs that other backends are waiting for.
 *
 * Runs in signal handler context.  Also runs in user context.
 */
static void
pgaio_baton_process_interrupt(bool in_signal_handler)
{
	while (pg_atomic_read_u32(&my_aio->baton_waiter_count) > 0)
	{
		/*
		 * Wait for the kernel to tell us that one or more IO has returned.
		 * It's a shame to have temporarily given up up whatever we were doing
		 * in our user context to wait around for an IO to finish on behalf of
		 * another backend, but that backend can't do it, and we can't block
		 * others' progress or we might deadlock.  This problem would go away
		 * if we used threads instead of processes.
		 *
		 * XXX: Bogus pointer is a way to tell pgaio_{posix_aio,iocp}_drain()
		 * if we're in a signal handler!  FIXME, need extensible flags or
		 * something
		 */
		pgaio_drain(/* context = */ (void *) in_signal_handler,
					/* block = */ true,
					/* call_shared = */ false,
					/* call_local = */ false);
	}
}

/*
 * Disable interrupt processing.  This avoids undefined behaviour in
 * aio_suspend() for IOs that have already returned, and problems with
 * implementations that are not as async signal safe as they should be.
 */
void
pgaio_baton_disable_interrupt(void)
{
	pgaio_baton_interrupt_holdoff++;
}

/*
 * Renable interrupt processing, and handle any interrupts we missed.
 */
void
pgaio_baton_enable_interrupt(void)
{
	Assert(pgaio_baton_interrupt_holdoff > 0);
	if (--pgaio_baton_interrupt_holdoff == 0)
	{
		while (pgaio_baton_interrupt_pending)
		{
			pgaio_baton_interrupt_pending = false;
			pgaio_baton_interrupt_holdoff++;
			pgaio_baton_process_interrupt(false);
			pgaio_baton_interrupt_holdoff--;
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
	 * If baton interrupts are currently disabled, just remember that the
	 * interrupt arrived.  It'll be processed when they're reenabled.
	 */
	if (pgaio_baton_interrupt_holdoff > 0)
	{
		pgaio_baton_interrupt_pending = true;
		return;
	}
	pgaio_baton_process_interrupt(true);
}
