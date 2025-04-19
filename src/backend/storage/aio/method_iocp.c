/*-------------------------------------------------------------------------
 *
 * method_iocp.c
 *    AIO - perform AIO using Windows I/O Completion Ports
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/method_iocp.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* included early, for IOMETHOD_IOCP_ENABLED */
#include "storage/aio.h"		/* IWYU pragma: keep */

#ifdef IOMETHOD_IOCP_ENABLED

#include <windows.h>

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
static void pgaio_iocp_init_backend(void);
static void pgaio_iocp_opened_fd(int fd);
static int	pgaio_iocp_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);
static void pgaio_iocp_wait_one(PgAioHandle *ioh, uint64 ref_generation);

const IoMethodOps pgaio_iocp_ops = {
	.wait_on_fd_before_close = true,	/* CloseHandle() cancels IOs */

	.shmem_size = pgaio_cq_shmem_size,
	.shmem_init = pgaio_cq_shmem_init,
	.init_backend = pgaio_iocp_init_backend,

	.opened_fd = pgaio_iocp_opened_fd,

	.submit = pgaio_iocp_submit,
	.wait_one = pgaio_iocp_wait_one,
};

static FILE_SEGMENT_ELEMENT *pgaio_iocp_file_segments;
static int	pgaio_iocp_file_segments_per_io;
static OVERLAPPED *pgaio_iocp_overlapped;
static int	pgaio_iocp_noverlapped;
static HANDLE pgaio_iocp_completion_port;

static int	pgaio_iocp_drain_local(bool wait, bool ipc);
static void pgaio_iocp_drain_remote(PgAioHandle *ioh);
static void pgaio_iocp_drain_remote_handler(SIGNAL_ARGS);

static void
pgaio_iocp_init_backend(void)
{
	ULONG_PTR	CompletionKey = 0;

	/* Backend-private OVERLAPPED objects. */
	pgaio_iocp_noverlapped = dclist_count(&pgaio_my_backend->idle_ios);
	pgaio_iocp_overlapped = palloc(sizeof(OVERLAPPED) *
								   pgaio_iocp_noverlapped);

	/*
	 * XXX We could put a big array of FILE_SEGMENT_ELEMENT directly in shared
	 * memory *instead* of iovecs on Windows, and use unions in a few places
	 * (PgAioHandle and elsewhere) so that we never have to convert between
	 * the Unix and Windows vector format.  This is a waste of memory and CPU
	 * cycles.
	 */
	pgaio_iocp_file_segments_per_io =
		(io_max_combine_limit * BLCKSZ) / PG_WIN32_FILE_SEGMENT_SIZE;
	pgaio_iocp_file_segments = palloc(sizeof(FILE_SEGMENT_ELEMENT) *
									  pgaio_iocp_file_segments_per_io *
									  pgaio_iocp_noverlapped);

	/* Dedicated completion port for this backend's disk I/O. */
	pgaio_iocp_completion_port =
		CreateIoCompletionPort(INVALID_HANDLE_VALUE,
							   NULL,
							   CompletionKey,
							   1);
	if (pgaio_iocp_completion_port == NULL)
	{
		_dosmaperr(GetLastError());
		elog(ERROR, "could not create completion port");
	}

	/* Support for handing raw completions to other backends on request. */
	pqsignal(SIGIO, pgaio_iocp_drain_remote_handler);
}

static void
pgaio_iocp_opened_fd(int fd)
{
	ULONG_PTR	CompletionKey = 0;

	/* Associate fd's handle with IOCP for completion events. */
	if (CreateIoCompletionPort((HANDLE) _get_osfhandle(fd),
							   pgaio_iocp_completion_port,
							   CompletionKey,
							   0) != pgaio_iocp_completion_port)
	{
		_dosmaperr(GetLastError());
		elog(PANIC, "could not associate file handle with completion port: %m");
	}
}

static OVERLAPPED *
pgaio_iocp_overlapped_for_io(PgAioHandle *ioh)
{
	int			id;

	Assert(ioh->owner_procno == MyProcNumber);
	id = pgaio_io_get_id(ioh);

	if (ioh->owner_procno != MyProcNumber ||
		id < pgaio_my_backend->io_handle_off ||
		id >= pgaio_my_backend->io_handle_off + pgaio_iocp_noverlapped)
		return NULL;

	return &pgaio_iocp_overlapped[id - pgaio_my_backend->io_handle_off];
}

static FILE_SEGMENT_ELEMENT *
pgaio_iocp_file_segments_for_io(PgAioHandle *ioh)
{
	int			id;

	Assert(ioh->owner_procno == MyProcNumber);
	id = pgaio_io_get_id(ioh);

	return &pgaio_iocp_file_segments[(id - pgaio_my_backend->io_handle_off) *
									 pgaio_iocp_file_segments_per_io];
}

static PgAioHandle *
pgaio_iocp_io_for_id(int32 id)
{
	return &pgaio_ctl->io_handles[id];
}

static PgAioHandle *
pgaio_iocp_io_for_overlapped(OVERLAPPED *overlapped)
{
	return pgaio_iocp_io_for_id(pgaio_my_backend->io_handle_off +
								(overlapped - pgaio_iocp_overlapped));
}

static int
pgaio_iocp_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];
		OVERLAPPED *overlapped = pgaio_iocp_overlapped_for_io(ioh);
		struct iovec *iov;
		FILE_SEGMENT_ELEMENT *file_segments;
		DWORD		size;
		HANDLE		h;
		bool		success;

		pgaio_io_prepare_submit(ioh);
		pgaio_cq_prepare_submit(ioh);
		memset(overlapped, 0, sizeof(*overlapped));

		/*
		 * If a multi-iovec read/write arrives here and we're not using direct
		 * I/O, we can only process the first iovec (you get a short I/O).
		 *
		 * XXX Higher levels should be prevented from building such iovec
		 * lists in that case.
		 */

		switch (ioh->op)
		{
			case PGAIO_OP_READV:
				h = (HANDLE) _get_osfhandle(ioh->op_data.read.fd);
				iov = &pgaio_ctl->iovecs[ioh->iovec_off];
				overlapped->Offset = ioh->op_data.read.offset;

				if (ioh->op_data.read.iov_length == 1)
				{
					success = ReadFile(h,
									   iov->iov_base,
									   iov->iov_len,
									   NULL,
									   overlapped);
				}
				else
				{
					file_segments = pgaio_iocp_file_segments_for_io(ioh);
					size = pg_win32_iovec_to_file_segments(file_segments,
														   pgaio_iocp_file_segments_per_io,
														   iov,
														   ioh->op_data.read.iov_length);
					success = ReadFileScatter(h,
											  file_segments,
											  size,
											  NULL,
											  overlapped);
				}
				break;

			case PGAIO_OP_WRITEV:
				h = (HANDLE) _get_osfhandle(ioh->op_data.write.fd);
				iov = &pgaio_ctl->iovecs[ioh->iovec_off];
				overlapped->Offset = ioh->op_data.write.offset;

				if (ioh->op_data.write.iov_length == 1)
				{
					success = WriteFile(h,
										iov->iov_base,
										iov->iov_len,
										NULL,
										overlapped);
				}
				else
				{
					file_segments = pgaio_iocp_file_segments_for_io(ioh);
					size = pg_win32_iovec_to_file_segments(file_segments,
														   pgaio_iocp_file_segments_per_io,
														   iov,
														   ioh->op_data.write.iov_length);
					success = WriteFileGather(h,
											  file_segments,
											  size,
											  NULL,
											  overlapped);
				}
				break;

			case PGAIO_OP_INVALID:
				elog(ERROR, "trying to prepare invalid IO operation for execution");
		}

		if (success)
		{
			/* Processed synchronously. */
			pgaio_io_process_completion(ioh, size);
		}
		else if (GetLastError() == ERROR_HANDLE_EOF)
		{
			/* End of file processed synchonously. */
			Assert(ioh->op == PGAIO_OP_READV);
			pgaio_io_process_completion(ioh, 0);
		}
		else if (GetLastError() != ERROR_IO_PENDING)
		{
			/* Failed to start IO. */
			_dosmaperr(GetLastError());
			pgaio_io_process_completion(ioh, -errno);
		}
	}

	return num_staged_ios;
}

static void
pgaio_iocp_wait_one(PgAioHandle *ioh, uint64 ref_generation)
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
		processed += pgaio_iocp_drain_local(processed == 0, false);
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
			pgaio_iocp_drain_remote(ioh);
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
pgaio_iocp_drain_local(bool wait, bool process)
{
	OVERLAPPED_ENTRY events[32];
	ULONG		nevents;

	if (process)
		Assert(CritSectionCount > 0);

	if (!GetQueuedCompletionStatusEx(pgaio_iocp_completion_port,
									 events,
									 lengthof(events),
									 &nevents,
									 wait ? INFINITE : 0,
									 false))
		nevents = 0;

	for (int i = 0; i < nevents; i++)
	{
		OVERLAPPED *overlapped;
		DWORD		transferred;
		PgAioHandle *ioh;
		int32		result;

		overlapped = events[i].lpOverlapped;

		/*
		 * We might receive a notification about a pseudo-synchronous I/O from
		 * win32pread.c etc.  Skip it.
		 */
		if (overlapped->hEvent != NULL)
			continue;

		if (!GetOverlappedResult(NULL, overlapped, &transferred, false))
		{
			if (GetLastError() == ERROR_HANDLE_EOF)
			{
				result = 0;
			}
			else
			{
				_dosmaperr(GetLastError());
				result = -errno;
			}
		}
		else
		{
			result = transferred;
		}

		ioh = pgaio_iocp_io_for_overlapped(overlapped);
		if (process)
			pgaio_io_process_completion(ioh, result);
		else
			pgaio_cq_insert(ioh, result, false);
	}

	return nevents;
}

/*
 * Ask the owner of a handle to insert any new completions into the shared
 * completion queue.
 */
static void
pgaio_iocp_drain_remote(PgAioHandle *ioh)
{
	kill(GetPGProcByNumber(ioh->owner_procno)->pid, SIGIO);
}

/*
 * Drain on request, unless we're already draining.
 */
static void
pgaio_iocp_drain_remote_handler(SIGNAL_ARGS)
{
	pgaio_iocp_drain_local(false, false);
}

#endif							/* IOMETHOD_IOCP_ENABLED */
