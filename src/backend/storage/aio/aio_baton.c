/*-------------------------------------------------------------------------
 *
 * aio_baton.c
 *	  Routines for coordinating which backend will complete an IO.
 *
 * This module provides a common implementation of wait_one() shared by
 * implementations where only the submitting process can consume the result
 * from the kernel.  We need a way to handle the (hopefully rare) cases where
 * a backend finishes up waiting for an IO that another backend submitted.
 * This is accomplished by using an atomic variable to "pass the baton" from
 * the submitter to the completer.
 *
 * This is shared infrastrucure used by the "posix_aio" and "iocp" [TODO!]
 * implementations.  (In contrast, the io_uring implementation coordinates
 * completion using 'contexts' with per-context completion locks, and the
 * worker implementation doesn't need to coordinate completion and worker
 * processes always do it, so these routines are not used by those IO
 * methods.)
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

#include "storage/aio_internal.h"

#define PGAIO_BATON_FLAGS_STATE_MASK		0x0f00000000000000
#define PGAIO_BATON_FLAGS_SUBMITTER_MASK	0x00ffffff00000000
#define PGAIO_BATON_FLAGS_COMPLETER_MASK	0x0000000000ffffff
#define PGAIO_BATON_FLAGS_SUBMITTER_SHIFT	32

#define PGAIO_BATON_FLAGS_SUBMITTED			0x0100000000000000
#define PGAIO_BATON_FLAGS_WAITER			0x0200000000000000
#define PGAIO_BATON_FLAGS_COMPLETED			0x0300000000000000

static uint64
pgaio_make_baton_flags(uint64 control_flags,
					   uint32 submitter_id,
					   uint32 completer_id)
{
	return control_flags |
		(((uint64) submitter_id) << PGAIO_BATON_FLAGS_SUBMITTER_SHIFT) |
		completer_id;
}

static uint32
pgaio_submitter_from_baton_flags(uint64 flags)
{
	return (flags & PGAIO_BATON_FLAGS_SUBMITTER_MASK) >>
		PGAIO_BATON_FLAGS_SUBMITTER_SHIFT;
}

static uint32
pgaio_completer_from_baton_flags(uint64 flags)
{
	return flags & PGAIO_BATON_FLAGS_COMPLETER_MASK;
}

static uint64
pgaio_state_from_baton_flags(uint64 flags)
{
	return flags & PGAIO_BATON_FLAGS_STATE_MASK;
}

static bool
pgaio_update_baton_flags(PgAioInProgress * io,
						 uint64 old_flags,
						 uint64 control_flags,
						 uint32 submitter_id,
						 uint32 completer_id)
{
	return pg_atomic_compare_exchange_u64(&io->io_method_data.posix_aio.flags,
										  &old_flags,
										  pgaio_make_baton_flags(control_flags,
																 submitter_id,
																 completer_id));
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

	pg_atomic_fetch_sub_u32(&my_aio->inflight_count, 1);

	/*
	 * Maintain the array of active iocbs.  If this was an IO that was actually
	 * submitted to the kernel, it should have been "activated", but if we
	 * failed to submit or it was a degenerate case like NOP then it's not
	 * "active".
	 */
	pgaio_posix_aio_disable_interrupt();
	pgaio_posix_aio_deactivate_io(io);
	pgaio_posix_aio_enable_interrupt();

	/*
	 * If we're in a signal handler, we can't do very much at all.  Just write
	 * the result into shared memory and wake up the waiter, if there is one.
	 */
	if (unlikely(in_interrupt_handler))
	{
		uint32		waiter_id;

		io->io_method_data.posix_aio.raw_result = result;
		pg_memory_barrier();
		waiter_id = io->io_method_data.posix_aio.waiter_id;
		if (waiter_id != -1)
			SetLatch(&ProcGlobal->allProcs[waiter_id].procLatch);
		return;
	}
	
	/*
	 * Use a CAS loop to serialize with any backend that might be concurrently
	 * signing up to be the completer.  We need to be able to wake it up.
	 */
	do
	{
		flags = pg_atomic_read_u64(&io->io_method_data.posix_aio.flags);
		submitter_id = pgaio_submitter_from_baton_flags(flags);
		completer_id = pgaio_completer_from_baton_flags(flags);
		state = pgaio_state_from_baton_flags(flags);

		/*
		 * This should only be called for IOs that this backend submitted, and
		 * we should definitely see a fresh enough view of that without memory
		 * barriers: this backend wrote the submitter ID.
		 */
		Assert(submitter_id == my_aio_id);
		Assert(state == PGAIO_BATON_FLAGS_SUBMITTED ||
			   state == PGAIO_BATON_FLAGS_WAITER);

	}
	while (!pgaio_update_baton_flags(io,
									 flags,
									 PGAIO_BATON_FLAGS_COMPLETED,
									 submitter_id,
									 my_aio_id));

	/* Process completions. */
	pgaio_process_io_completion(io, raw_result);
	
	/* If there was a backend waiting, wake it up. */
	if (flags == PGAIO_BATON_WAITING && completer_id != my_aio_id)
		SendProcSignal(ProcGlobal->allProcs[submitter_id].pid,
					   PROCSIG_POSIX_AIO,
					   InvalidBackendId);
}

/*

 */
static void
pgaio_baton_wait_one(PgAioContext *context, PgAioInProgress * io, uint64 ref_generation, uint32 wait_event_info)
{
	uint32		submitter_id;
	uint32		completer_id;
	uint64		flags;
	int			raw_result;

	for (;;)
	{
		flags = pg_atomic_read_u64(&io->io_method_data.posix_aio.flags);
		submitter_id = pgaio_submitter_from_baton_flags(flags);
		completer_id = pgaio_completer_from_baton_flags(flags);

		pg_read_barrier();

		raw_result = io->io_method_data.posix_aio.raw_result;

		switch (pgaio_state_from_baton_flags(flags))
		{
		case PGAIO_BATON_FLAGS_SUBMITTED:
			if (raw_result != INT_MIN)
			{
				/* Result is already here. */
				if (!pgaio_update_baton_flags(io,
											  flags,
											  PGAIO_BATON_FLAGS_COMPLETED,
											  submitter_id,
											  my_aio_id))
					continue;	/* lost race, try again */
				return true;
			}
			else
			{
				/* Wait for the result.  There can be only one direct waiter. */
				if (!pgaio_update_baton_flags(io,
											  flags,
											  PGAIO_BATON_FLAGS_WAITER,
											  submitter_id,
											  my_aio_id))
					continue;	/* lost race, try again */

				/* Tell the submitter we're waiting (if not this backend). */
				if (submitter_id != my_aio_id)
				{
					/*
					 * We're going to ask the submitter to help us by
					 * collecting the result and passing it to us.  Since it
					 * will run code in a signal handler, we'll give it a
					 * second copy of the waiter ID, so that we don't have to
					 * worry about whether it's safe to use 64 bit atomics in
					 * a signal handler.
					 */
					io->io_method_data.posix_aio.waiter_id = my_aio_id;

					/* Double-check for a result to close a race. XXX discuss */
					if (io->io_method_data.posix_aio.raw_result != INT_MIN)
						continue;	/* go around again, we'll try to grab it */

					/*
					 * Interrupt the submitter to tell it we are waiting.
					 *
					 * XXX Looking up the backendId would make this more
					 * efficient, but it seems to be borked for the startup
					 * process, which advertises a bogus backendId in its PGPROC.
					 * *FIXME*
					 */
					SendProcSignal(ProcGlobal->allProcs[submitter_id].pid,
								   PROCSIG_POSIX_AIO,
								   InvalidBackendId);
				}
				/* Go around again. */
			}
			break;
		case PGAIO_POSIX_AIO_FLAG_HAS_WAITER:
			if (raw_result != INT_MIN)
			{
				/*
				 * There's already a waiter (possibly me).  Since the result
				 * is available and I'm on CPU right now, steal it.
				 */
				if (!pgaio_update_baton_flags(io,
											  flags,
											  PGAIO_BATON_FLAGS_COMPLETED,
											  submitter_id,
											  my_aio_id))
					continue;	/* lost race, try again */

				/* Wake previous waiter, if not me. */
				if (completer_id != my_aio_id)
					SetLatch(&ProcGlobal->allProcs[completer_id].procLatch);

				return true;
			}
			else if (submitter_id == my_aio_id)
			{
				/*
				 * The submitter doesn't have the result yet, but is waiting
				 * for this IO.  It would probably save some ping-pong if the
				 * submitter eventually completes, so replace any other waiter.
				 */
				if (completer_id != my_aio_id)
				{
					if (!pgaio_update_baton_flags(io,
												  flags,
												  PGAIO_BATON_FLAGS_WAITER,
												  submitter_id,
												  my_aio_id))
						continue;	/* lost race, try again */

					/* Wake previous waiter.  Request denied. */
					SetLatch(&ProcGlobal->allProcs[completer_id].procLatch);
				}

				/* Since I submitted it, I need to drain to make progress. */
				pgaio_drain(NULL,
							/* block = */ true,
							/* call_shared = */ false,
							/* call_local = */ false);
			}
			else if (completer_id == my_aio_id)
			{
				/*
				 * We're waiting for the submitter to give us the baton, or
				 * deny it, and set our latch.  We'll keep sleeping until we
				 * see a new state.
				 */
				WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, 0);
				ResetLatch(MyLatch);
			}
			else
			{
				/*
				 * Someone else has is waiting. No point in trying to usurp it,
				 * we'd only have to wait too.  We'll have to wait on the IO
				 * CV.
				 */
				return false;
			}
			break;
		default:
			return false;
		}
	}
}




