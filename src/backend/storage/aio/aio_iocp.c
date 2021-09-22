/*-------------------------------------------------------------------------
 *
 * aio_iocp.c
 *	  Routines for Windows IOCP.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/aio_iocp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <windows.h>

#include "pgstat.h"
#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

/*
 * How much memory does each FILE_SEGMENT_ELEMENT cover?
 * XXX Should we call GetSystemInfo() to get this value at runtime?
 */
#define PGAIO_IOCP_IOV_SEG_SIZE 4096

static HANDLE pgaio_iocp_completion_port;

static PgAioInProgress * io_for_overlapped(OVERLAPPED *overlapped);
static OVERLAPPED *overlapped_for_io(PgAioInProgress *io);

static int	pgaio_iocp_start_rw(PgAioInProgress * io);
static void pgaio_iocp_process_completion(PgAioInProgress * io,
										  int result,
										  bool in_interrupt_handler);

static void pgaio_iocp_submit_internal(PgAioInProgress *ios[], int nios);
static int pgaio_iocp_drain_internal(bool block, bool in_interrupt_handler);

/* Module initialization. */

/*
 * Initialize shared memory data structures.
 */
static void
pgaio_iocp_shmem_init(bool first_time)
{
	if (!first_time)
		return;

	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[i];

		pg_atomic_init_u64(&io->io_method_data.iocp.flags, 0);
	}
}


/* Functions for submitting IOs to the kernel. */

/*
 * Submit a given number of pending IOs to the kernel, and optionally drain
 * any results that have arrived, without waiting.
 */
static int
pgaio_iocp_submit(int max_submit, bool drain)
{
	PgAioInProgress *ios[PGAIO_SUBMIT_BATCH_SIZE];
	int			nios = 0;

	START_CRIT_SECTION();
	while (!dlist_is_empty(&my_aio->pending))
	{
		dlist_node *node;
		PgAioInProgress *io;

		if (nios == max_submit)
			break;

		node = dlist_pop_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);

		pgaio_io_prepare_submit(io, 0);

		my_aio->submissions_total_count++;


		ios[nios] = io;
		++nios;
	}
	pgaio_iocp_submit_internal(ios, nios);
	END_CRIT_SECTION();

	/* XXXX copied from uring submit */

	/*
	 * Others might have been waiting for this IO. Because it wasn't marked as
	 * in-flight until now, they might be waiting for the CV. Wake'em up.
	 */
	pgaio_broadcast_ios(ios, nios);

	/* callbacks will be called later by pgaio_submit() */
	if (drain)
		pgaio_drain(NULL,
					 /* block = */ false,
					 /* call_shared = */ false,
					 /* call_local = */ false);

	return nios;
}

/*
 * Resubmit an IO that was only partially completed (for example, a short
 * read) or that the kernel told us to retry.
 */
static void
pgaio_iocp_io_retry(PgAioInProgress * io)
{
	WRITE_ONCE_F(io->flags) |= PGAIOIP_INFLIGHT;

	pgaio_iocp_submit_internal(&io, 1);

	pgaio_complete_ios(false);

	ConditionVariableBroadcast(&io->cv);
}

static void
pgaio_iocp_submit_one(PgAioInProgress *io)
{
	int rc;

	pg_atomic_add_fetch_u32(&my_aio->inflight_count, 1);

	pgaio_exchange_submit_one(io);

	switch (io->op)
	{
	case PGAIO_OP_READ:
	case PGAIO_OP_WRITE:
		rc = pgaio_iocp_start_rw(io);
		break;
	case PGAIO_OP_INVALID:
		rc = -1;
		errno = EOPNOTSUPP;
		break;
	default:
		rc = -1;
		elog(ERROR, "unexpected op");
	}

	if (rc < 0)
		pgaio_iocp_process_completion(io, -errno, false);
}

static void
pgaio_iocp_submit_internal(PgAioInProgress *ios[], int nios)
{
	PgAioInProgress *synchronous_ios[PGAIO_SUBMIT_BATCH_SIZE];
	int nsync = 0;

	Assert(nios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < nios; ++i)
	{
		PgAioInProgress *io = ios[i];

		switch (io->op)
		{
			case PGAIO_OP_FLUSH_RANGE:	/* XXX ignoring for now */
			case PGAIO_OP_NOP:
				pgaio_iocp_process_completion(io, 0, false);
				break;
			case PGAIO_OP_FSYNC:
				/*
				* XXX FileFlushBuffers() doesn't seem to have an asynchronous
				* version.  Handle synchronously, after starting others.
				*/
				synchronous_ios[nsync++] = ios[i];
				break;
			default:
				pgaio_iocp_submit_one(io);
				break;
			}
	}

	if (nsync > 0)
	{
		for (int i = 0; i < nsync; ++i)
			pgaio_do_synchronously(synchronous_ios[i]);
		pgaio_complete_ios(false);
	}
}

static FILE_SEGMENT_ELEMENT *segments_elements = NULL;
static int segment_element_size = 0;

/*
 * Convert Unix iovec array to Windows memory-page representation.  The
 * segments array must have space for PGAIO_IOCP_IOV_MAX_PAGES plus one
 * more for NULL termination.
 *
 * Returns the total number of bytes to transfer.
 */
static size_t
pgaio_iocp_iov_to_segments(FILE_SEGMENT_ELEMENT **segments,
							  const struct iovec *iov, int iovcnt)
{
	int count = 0;
	char *base;
	size_t total_len = 0;
	size_t len;
	size_t num_win_pages;

	for (int i = 0; i < iovcnt; i++)
		total_len += iov[i].iov_len;

	num_win_pages = total_len / PGAIO_IOCP_IOV_SEG_SIZE + 1;

	if (segments_elements == NULL)
	{
		segments_elements = malloc(sizeof(FILE_SEGMENT_ELEMENT) * num_win_pages);
		segment_element_size = num_win_pages;
	}
	else if (segment_element_size < num_win_pages)
	{
		segments_elements = realloc(segments_elements, sizeof(FILE_SEGMENT_ELEMENT) * num_win_pages);
		segment_element_size = num_win_pages;
	}

	for (int i = 0; i < iovcnt; ++i) {
		base = iov[i].iov_base;
		len = iov[i].iov_len;

		if (len % PGAIO_IOCP_IOV_SEG_SIZE != 0)
			elog(ERROR, "scatter/gather I/O not multiple of memory page size");

		/* Unpack this iovec into pages. */
		while (len > 0)
		{
		//	elog(LOG, "pgaio_iocp_iov_to_segments: %p %zu iovcnt = %d, count = %d, PGAIO_IOCP_IOV_MAX_PAGES = %d", base, len, iovcnt, count, PGAIO_IOCP_IOV_MAX_PAGES);
			segments_elements[count++].Buffer = base;
			base += PGAIO_IOCP_IOV_SEG_SIZE;
			len -= PGAIO_IOCP_IOV_SEG_SIZE;
		}
	}

	segments_elements[count].Buffer = NULL;
	*segments = segments_elements;

	return count * PGAIO_IOCP_IOV_SEG_SIZE;
}

/*
 * Start a read or write.
 */
static int
pgaio_iocp_start_rw(PgAioInProgress * io)
{
	OVERLAPPED *overlapped = overlapped_for_io(io);
	struct iovec iov[IOV_MAX];
	int			iovcnt;
	bool		result;

	/* Prepare the OVERLAPPED struct. */
	memset(overlapped, 0, sizeof(*overlapped));
	if (io->op == PGAIO_OP_READ)
		overlapped->Offset = io->op_data.read.offset +
			io->op_data.read.already_done;
	else
		overlapped->Offset = io->op_data.write.offset +
			io->op_data.write.already_done;

	/*
	 * Build a Unix iovec from the merged IO chain.  This produces a single
	 * iovec for the simple non-scatter/gather merge case.
	 */
	iovcnt = pgaio_fill_iov(iov, io);

	if (iovcnt > 1)
	{
		FILE_SEGMENT_ELEMENT *segments;
		size_t size;

		/* Windows can't do scatter/gather on buffered files. */
		if (!io_data_direct)
		{
			/* pgaio_can_scatter_gather() should not have allowed this. */
			elog(ERROR, "unexpected vector read/write");
		}

		/* Convert to the page-by-page format Windows requires. */
		size = pgaio_iocp_iov_to_segments(&segments, iov, iovcnt);

		if (io->op == PGAIO_OP_READ)
			result = ReadFileScatter((HANDLE) _get_osfhandle(io->op_data.read.fd), segments, size, NULL, overlapped);
		else
			result = WriteFileGather((HANDLE) _get_osfhandle(io->op_data.write.fd), segments, size, NULL, overlapped);
	}
	else
	{
		if (io->op == PGAIO_OP_READ)
			result = ReadFile((HANDLE) _get_osfhandle(io->op_data.read.fd), iov[0].iov_base, iov[0].iov_len, NULL,
								overlapped);
		else
			result = WriteFile((HANDLE) _get_osfhandle(io->op_data.write.fd), iov[0].iov_base, iov[0].iov_len, NULL,
								 overlapped);
		
	}

	if (!result)
	{
		DWORD err = GetLastError();

		if (err != ERROR_IO_PENDING)
		{
			elog(LOG, "pgaio_iocp_start_rw: %lu", err);
			_dosmaperr(err);
			return -1;
		}
	}
	return 0;
}


/* Functions for waiting for IOs to complete. */

static int
pgaio_iocp_drain(PgAioContext *context, bool block, bool call_shared)
{

	int			ndrained;

	ndrained = pgaio_iocp_drain_internal(block, false);

	if (call_shared)
		pgaio_complete_ios(false);

	return ndrained;
}

static int
pgaio_iocp_drain_internal(bool block, bool in_interrupt_handler)
{
	int			ndrained;

	for (;;)
	{
		PgAioInProgress *io;
		OVERLAPPED *overlapped;
		DWORD nbytes;
		ULONG_PTR completion_key = 0; /* not used */

		/*
		 * XXX Need to use GetQueuedCompletionStatusEx() to consume
		 * multiple results at once (hard to understand how to get errors...).
		 */

		if (!GetQueuedCompletionStatus(pgaio_iocp_completion_port,
									   &nbytes,
									   &completion_key,
									   &overlapped,
									   ndrained == 0 && block ? INFINITE : 0))
		{
			if (!overlapped)
				break;

			io = io_for_overlapped(overlapped);
			_dosmaperr(GetLastError());
			pgaio_iocp_process_completion(io, -errno, in_interrupt_handler);
		}
		else
		{
			io = io_for_overlapped(overlapped);
			pgaio_iocp_process_completion(io, nbytes, in_interrupt_handler);
		}

		ndrained++;
	}

	return ndrained;
}

/*
 * Given an aiocb, return the associated PgAioInProgress.
 */
static PgAioInProgress *
io_for_overlapped(OVERLAPPED *overlapped)
{
	return (PgAioInProgress *)
		(((char *) overlapped) - offsetof(PgAioInProgress,
								  io_method_data.iocp.overlapped));
}

static OVERLAPPED *
overlapped_for_io(PgAioInProgress * io)
{
	return &io->io_method_data.iocp.overlapped;
}

/*
 * The kernel has provided the result for an IO that we submitted.  This might
 * run in the main thread on failure to submit, but normally runs in the
 * completion thread so mustn't do anything but update atomics and set latches.
 */
static void
pgaio_iocp_process_completion(PgAioInProgress * io, int result, bool in_interrupt_handler)
{
	pg_atomic_fetch_sub_u32(&my_aio->inflight_count, 1);

    pgaio_exchange_process_completion(io, result, in_interrupt_handler);
}

/*
 * Drain all in progress IOs from a file descriptor, if necessary on this
 * platform.
 */
static void
pgaio_iocp_closing_fd(int fd)
{
	/*
	 * https://social.msdn.microsoft.com/Forums/SQLSERVER/en-US/5d67623b-fe3f-463e-950d-7af24e3243ca/safe-to-call-closehandle-when-an-overlapped-io-is-in-progress?forum=windowsgeneraldevelopmentissues
	 *
	 *
	 * XXX Should be handled by top level facility, shared with POSIX AIO.  For
	 * now, just wait for *everything* we submitted, which is pessimal, and
	 * broken (doesn't understand retries).
	 */
	pgaio_wait_for_issued();
}

static void
pgaio_iocp_postmaster_child_init_local(void)
{
	ULONG_PTR		CompletionKey = 0;

	/*
	 * Create an IO completion port that will be used to receive all I/O
	 * completions for this process.
	 */
	pgaio_iocp_completion_port =
		CreateIoCompletionPort(INVALID_HANDLE_VALUE,
							   NULL,
							   CompletionKey,
							   1);
	if (pgaio_iocp_completion_port == NULL)
	{
		_dosmaperr(GetLastError());
		elog(FATAL, "could not create completion port");
	}
}

/*
 * Register a file handle with our IOCP.  This has external linkage so that
 * fd.c can call it, to make sure that our completion thread will hear about
 * the completion of every I/O initiated on this file.
 */
void
pgaio_iocp_register_file_handle(HANDLE file_handle)
{
	ULONG_PTR		CompletionKey = 0;

	if (CreateIoCompletionPort(file_handle,
								pgaio_iocp_completion_port,
								CompletionKey,
								1) != pgaio_iocp_completion_port)
	{
		_dosmaperr(GetLastError());
		elog(PANIC, "could not associate file handle with completion port: %m");
	}
}

const IoMethodOps pgaio_iocp_ops = {
	.shmem_init = pgaio_iocp_shmem_init,
	.postmaster_child_init_local = pgaio_iocp_postmaster_child_init_local,
	.submit = pgaio_iocp_submit,
	.retry = pgaio_iocp_io_retry,
	.wait_one = pgaio_exchange_wait_one,
	.drain = pgaio_iocp_drain,
	.closing_fd = pgaio_iocp_closing_fd,

	/*
	 * Windows ReadFileScatter() and WriteFileGather() only work on direct IO
	 * files, so we can't set this to true for buffered mode.
	 */
	.can_scatter_gather_direct = true
};
