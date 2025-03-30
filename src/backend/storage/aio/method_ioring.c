/*-------------------------------------------------------------------------
 *
 * method_ioring.c
 *    AIO - perform AIO using Windows IoRing
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/method_ioring.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* included early, for IOMETHOD_IORING_ENABLED */
#include "storage/aio.h"		/* IWYU pragma: keep */

#ifdef IOMETHOD_IORING_ENABLED

#include <windows.h>
#include <ioringapi.h>

#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "utils/wait_event.h"

/* Entry points for IoMethodOps. */
static void pgaio_ioring_init_backend(void);
static int	pgaio_ioring_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);
static void pgaio_ioring_wait_one(PgAioHandle *ioh, uint64 ref_generation);

const IoMethodOps pgaio_ioring_ops = {
	.wait_on_fd_before_close = true,	/* CloseHandle() cancels IOs ??? */

	.shmem_size = pgaio_cq_shmem_size,
	.shmem_init = pgaio_cq_shmem_init,
	.init_backend = pgaio_ioring_init_backend,

	.submit = pgaio_ioring_submit,
	.wait_one = pgaio_ioring_wait_one,
};

static HANDLE pgaio_ioring_handle;

static int	pgaio_ioring_drain_local(bool wait, bool ipc);
static void pgaio_ioring_drain_remote(PgAioHandle *ioh);
static void pgaio_ioring_drain_remote_handler(SIGNAL_ARGS);

static void
pgaio_ioring_init_backend(void)
{
	IORING_CREATE_FLAGS flags = {
		.Required = IORING_CREATE_REQUIRED_FLAGS_NONE,
		.Advisory = IORING_CREATE_ADVISORY_FLAGS_NONE
	};

	/*
	 * Version 2 had read, write.  Version 3 added flush.  Version 4 might
	 * have scatter/gather?  They already appear in the opcode list, there is
	 * just no BuildIoRingXXX functions...
	 */
	if (!CreateIoRing(IORING_VERSION_2,
					  flags,
					  io_max_concurrency,
					  io_max_concurrency * 2,
					  &pgaio_ioring_handle))
	{
		_dosmaperr(GetLastError());
		elog(ERROR, "could not create IoRing");
	}

	/* Support for handing raw completions to other backends on request. */
	pqsignal(SIGIO, pgaio_ioring_drain_remote_handler);
}

static inline IORING_HANDLE_REF
pgaio_ioring_fd(int fd)
{
	return IoRingHandleRefFromHandle((HANDLE) _get_osfhandle(fd));
}

static int
pgaio_ioring_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];
		struct iovec *iov;
		HRESULT		s = 0;

		pgaio_io_prepare_submit(ioh);
		pgaio_cq_prepare_submit(ioh);

		switch (ioh->op)
		{
			case PGAIO_OP_READV:
				iov = &pgaio_ctl->iovecs[ioh->iovec_off];

				if (ioh->op_data.read.iov_length == 1)
				{
					s = BuildIoRingReadFile(pgaio_ioring_handle,
											pgaio_ioring_fd(ioh->op_data.read.fd),
											IoRingBufferRefFromPointer(iov->iov_base),
											iov->iov_len,
											ioh->op_data.read.offset,
											(UINT_PTR) ioh,
											IOSQE_FLAGS_NONE);
				}
				else
				{
					/*
					 * XXX There is no BuildIoRingReadFileScatter()! But there
					 * is IORING_OP_READ_SCATTER so it must be coming!
					 *
					 * For now this will be a short read of just the first
					 * iovec...
					 */
					s = BuildIoRingReadFile(pgaio_ioring_handle,
											pgaio_ioring_fd(ioh->op_data.read.fd),
											IoRingBufferRefFromPointer(iov->iov_base),
											iov->iov_len,
											ioh->op_data.read.offset,
											(UINT_PTR) ioh,
											IOSQE_FLAGS_NONE);
				}
				break;

			case PGAIO_OP_WRITEV:
				iov = &pgaio_ctl->iovecs[ioh->iovec_off];

				if (ioh->op_data.read.iov_length == 1)
				{
					s = BuildIoRingWriteFile(pgaio_ioring_handle,
											 pgaio_ioring_fd(ioh->op_data.read.fd),
											 IoRingBufferRefFromPointer(iov->iov_base),
											 iov->iov_len,
											 ioh->op_data.read.offset,
											 (UINT_PTR) ioh,
											 IOSQE_FLAGS_NONE);
				}
				else
				{
					/* See above, no scatter yet... */
					s = BuildIoRingWriteFile(pgaio_ioring_handle,
											 pgaio_ioring_fd(ioh->op_data.read.fd),
											 IoRingBufferRefFromPointer(iov->iov_base),
											 iov->iov_len,
											 ioh->op_data.read.offset,
											 (UINT_PTR) ioh,
											 IOSQE_FLAGS_NONE);
				}
				break;

			case PGAIO_OP_INVALID:
				elog(ERROR, "trying to prepare invalid IO operation for execution");
		}

		if (s != S_OK)
		{
			_dosmaperr(GetLastError());
			pgaio_io_process_completion(ioh, -errno);
		}
	}

	if (SubmitIoRing(pgaio_ioring_handle, 0, 0, NULL) != S_OK)
		elog(ERROR, "SubmitIoRing failed");

	return num_staged_ios;
}

static void
pgaio_ioring_wait_one(PgAioHandle *ioh, uint64 ref_generation)
{
	ProcNumber	owner_procno = pgaio_io_get_owner(ioh);
	bool		am_owner = owner_procno == MyProcNumber;
	int			processed;
	PgAioHandleState state;

	/*
	 * Process the owner's completion queue.  Usually this backend is the
	 * owner and it's usually empty.  It can't fill up again because SIGIO
	 * won't be processed until we reach a pseudo-signal execution point, but
	 * we also have to process whatever is in there as it may be what we are
	 * waiting for.
	 */
	START_CRIT_SECTION();
	processed = pgaio_cq_try_process_completion(ioh);
	if (am_owner && pgaio_cq_in_progress(ioh))
		processed += pgaio_ioring_drain_local(processed == 0, false);
	END_CRIT_SECTION();

	if (processed > 0)
		return;

	/*
	 * We may need other processes' help.  We could be the owner, but if so we
	 * must have moved it to the shared completion queue (or we'd have used a
	 * blocking drain and returned above), in which case we try to process it
	 * and failing that, wait for someone else to finish doing so.  We could
	 * also need the owner to drain it to the cq first before we or someone
	 * else can process it.
	 */
	START_CRIT_SECTION();
	ConditionVariablePrepareToSleep(&ioh->cv);
	while (!pgaio_io_was_recycled(ioh, ref_generation, &state) &&
		   state == PGAIO_HS_SUBMITTED)
	{
		bool		in_progress = pgaio_cq_in_progress(ioh);

		if (in_progress)
			pgaio_ioring_drain_remote(ioh);
		else if (pgaio_cq_try_process_completion(ioh) > 0)
			break;

		/*
		 * Wait for the owner to insert it into the shared completion queue,
		 * or for any backend to complete it.  As long as it is still appears
		 * to be actually running, we periodically ask it to check.
		 */
		ConditionVariableOrLatchSleep(&ioh->cv,
									  in_progress ? 100 : -1,
									  in_progress ?
									  WAIT_EVENT_AIO_IO_IPC_EXECUTION :
									  WAIT_EVENT_AIO_IO_COMPLETION);
	}
	ConditionVariableCancelSleep();
	END_CRIT_SECTION();
}

/*
 * Drain completion events from the kernel.  Optionally process completions
 * immediately, but otherwise insert them into the shared completion queue.
 */
static int
pgaio_ioring_drain_local(bool wait, bool process)
{
	IORING_CQE	cqe;
	int			nevents = 0;

	if (process)
		Assert(CritSectionCount > 0);

	for (;;)
	{
		while (PopIoRingCompletion(pgaio_ioring_handle, &cqe) == S_OK)
		{
			int32		result;

			nevents++;
			ioh = (PgAioHandle *) cqe.UserData;
			if (cqe.Result !=S_OK)	/* ??? */
			{
				_dosmaperr(cqe.Result); /* ??? */
				result = -errno;
			}
			else
			{
				result = (int32) cqe.Information;
			}

			if (process)
				pgaio_io_process_completion(ioh, result);
			else
				pgaio_cq_insert(ioh, result, false);
		}
		if (nevents > 0 || !wait)
			break;
		if (SubmitIoRing(pgaio_ioring_handle, 1, INFINITE, NULL) != S_OK)
		{
			_dosmaperr(GetLastError());
			elog(ERROR, "wait for completions failed: %m");
		}
	}

	return nevents;
}

/*
 * Ask the owner of a handle to insert any new completions into the shared
 * completion queue.
 */
static void
pgaio_ioring_drain_remote(PgAioHandle *ioh)
{
	kill(GetPGProcByNumber(ioh->owner_procno)->pid, SIGIO);
}

/*
 * Drain on request, unless we're already draining.
 */
static void
pgaio_ioring_drain_remote_handler(SIGNAL_ARGS)
{
	pgaio_ioring_drain_local(false, false);
}

#endif							/* IOMETHOD_IORING_ENABLED */
