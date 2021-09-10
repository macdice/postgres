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

static bool	pgaio_iocp_take_baton(PgAioInProgress * io);
static bool pgaio_iocp_give_baton(PgAioInProgress * io, int result);

static PgAioInProgress * io_for_overlapped(OVERLAPPED *overlapped);
static OVERLAPPED *overlapped_for_io(PgAioInProgress *io);

static int	pgaio_iocp_start_rw(PgAioInProgress * io);
static void pgaio_iocp_kernel_io_done(PgAioInProgress * io,
										 int result);

static uint64 pgaio_iocp_make_flags(uint64 control_flags,
									   uint32 submitter_id,
									   uint32 completer_id);

/* Entry points for IoMethodOps. */
static void pgaio_iocp_shmem_init(void);
static int pgaio_iocp_submit(int max_submit, bool drain);
static void pgaio_iocp_wait_one(PgAioContext *context, PgAioInProgress *io, uint64 ref_generation, uint32 wait_event_info);
static void pgaio_iocp_io_retry(PgAioInProgress *io);
static int pgaio_iocp_drain(PgAioContext *context, bool block, bool call_shared);
static void pgaio_iocp_closing_fd(int fd);


/* XXX put these in the right order */
static void pgaio_iocp_submit_one(PgAioInProgress *io);
static void pgaio_iocp_submit_internal(PgAioInProgress *ios[], int nios);
static void pgaio_iocp_postmaster_child_init_local(void);


/*
 * XXX This baton stuff is similar to the POSIX AIO version, and could perhaps
 * be shared code.
 */

/* Bits and masks for determining the completer for an IO. */
#define PGAIO_IOCP_FLAG_AVAILABLE				0x0100000000000000
#define PGAIO_IOCP_AIO_FLAG_REQUESTED			0x0200000000000000
#define PGAIO_IOCP_AIO_FLAG_GRANTED				0x0400000000000000
#define PGAIO_IOCP_FLAG_DONE					0x0800000000000000
#define PGAIO_IOCP_FLAG_RESULT					0x1000000000000000
#define PGAIO_IOCP_FLAG_SUBMITTER_MASK			0x00ffffff00000000
#define PGAIO_IOCP_FLAG_COMPLETER_MASK			0x0000000000ffffff
#define PGAIO_IOCP_FLAG_SUBMITTER_SHIFT			32


const IoMethodOps pgaio_iocp_ops = {
	.shmem_init = pgaio_iocp_shmem_init,
	.postmaster_child_init_local = pgaio_iocp_postmaster_child_init_local,
	.submit = pgaio_iocp_submit,
	.retry = pgaio_iocp_io_retry,
	.wait_one = pgaio_iocp_wait_one,
	.drain = pgaio_iocp_drain,
	.closing_fd = pgaio_iocp_closing_fd,

	/*
	 * Windows ReadFileScatter() and WriteFileGather() only work on direct IO
	 * files, so we can't set this to true for buffered mode.
	 */
	.can_scatter_gather_direct = true
};

/* Module initialization. */

/*
 * Initialize shared memory data structures.
 */
static void
pgaio_iocp_shmem_init(void)
{
	/* XXX: don't re-initialize while already running */
	if (IsUnderPostmaster && IsPostmasterEnvironment)
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

	/* Set things up so that any backend can become the completer. */
	/* XXX PGAIO_IOCP_RESULT_INVALID is not defined. */
	io->io_method_data.iocp.raw_result = PGAIO_IOCP_RESULT_INVALID;
	pg_atomic_write_u64(&io->io_method_data.iocp.flags,
						pgaio_iocp_make_flags(PGAIO_IOCP_FLAG_AVAILABLE,
												 my_aio_id,
												 0));

	/*
	 * Every IO needs the index of the head IO in a merge chain, so that we
	 * can find the OVERLAPPED that the kernel knows about.
	 */
	for (PgAioInProgress * cur = io;;)
	{
		cur->io_method_data.iocp.merge_head_idx = pgaio_io_id(io);
		if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
			break;
		cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
	}

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
		pgaio_iocp_kernel_io_done(io, -errno);
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
				pgaio_iocp_kernel_io_done(io, 0);
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

/*
 * Wait for a given IO/generation to complete.
 */
static void
pgaio_iocp_wait_one(PgAioContext *context,
					   PgAioInProgress * io,
					   uint64 ref_generation,
					   uint32 wait_event_info)
{
	PgAioInProgress *merge_head_io;
	PgAioIPFlags flags;

	for (;;)
	{
		uint32		merge_head_idx;

		if (pgaio_io_recycled(io, ref_generation, &flags) ||
			!(flags & PGAIOIP_INFLIGHT))
			break;

		/*
		 * Find the IO that is the head of the merge chain.  This information
		 * may be arbitrarily out of date, but we'll cope with that.
		 */
		merge_head_idx = io->io_method_data.iocp.merge_head_idx;
		merge_head_io = &aio_ctl->in_progress_io[merge_head_idx];

		if (pgaio_iocp_take_baton(merge_head_io))
		{
			/*
			 * We're now the completer for the head of the merged IO
			 * chain. It's possibly a later generation than the one we're
			 * actually waiting for, but the result is available now so
			 * let's process it anyway and check the generation again.
			 */
			pgaio_process_io_completion(merge_head_io,
										merge_head_io->io_method_data.iocp.raw_result);
		}
		else
		{
			/*
			 * Someone else is already signed up to reap the merged IO chain,
			 * or this is a later generation and we'll detect that in the next
			 * loop.  Wait on the IO.
			 */
			ConditionVariablePrepareToSleep(&io->cv);
			if (pgaio_io_recycled(io, ref_generation, &flags) ||
				!(flags & PGAIOIP_INFLIGHT))
				break;
			ConditionVariableSleep(&io->cv,
								   WAIT_EVENT_AIO_IO_COMPLETE_ONE);
		}
	}
	pgaio_complete_ios(false);
	ConditionVariableCancelSleep();
}

int
pgaio_iocp_drain(PgAioContext *context, bool block, bool call_shared)
{
	/*
	 * XXX The problem with doing nothing here is that we don't find out about
	 * IOs completing opportunistically and start more IOs to keep the pipes
	 * full.  Perhaps the IOCP thread should also put IOs into a circular
	 * buffer that this would consult.
	 */
	if (call_shared)
		pgaio_complete_ios(false);

	return 0;
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
pgaio_iocp_kernel_io_done(PgAioInProgress * io, int result)
{
	pg_atomic_fetch_sub_u32(&my_aio->inflight_count, 1);

	/*
	 * Store the value for later, for whoever arrives first to take the baton.
	 * If someone is waiting already, give them the baton now.
	 */
	pgaio_iocp_give_baton(io, result);
}


/* Functions for negotiating who is allowed to complete an IO. */
static uint64
pgaio_iocp_make_flags(uint64 control_flags,
						 uint32 submitter_id,
						 uint32 completer_id)
{
	return control_flags |
		(((uint64) submitter_id) << PGAIO_IOCP_FLAG_SUBMITTER_SHIFT) |
		completer_id;
}

static uint32
pgaio_iocp_submitter_from_flags(uint64 flags)
{
	return (flags & PGAIO_IOCP_FLAG_SUBMITTER_MASK) >>
		PGAIO_IOCP_FLAG_SUBMITTER_SHIFT;
}

static uint32
pgaio_iocp_completer_from_flags(uint64 flags)
{
	return flags & PGAIO_IOCP_FLAG_COMPLETER_MASK;
}

static bool
pgaio_iocp_update_flags(PgAioInProgress * io,
						   uint64 old_flags,
						   uint64 control_flags,
						   uint32 submitter_id,
						   uint32 completer_id)
{
	return pg_atomic_compare_exchange_u64(&io->io_method_data.iocp.flags,
										  &old_flags,
										  pgaio_iocp_make_flags(control_flags,
																   submitter_id,
																   completer_id));
}

/*
 * Try to get permission to complete this IO.  Waits if no result available yet.
 */
static bool
pgaio_iocp_take_baton(PgAioInProgress * io)
{
	uint32		submitter_id;
	uint32		completer_id;
	uint64		flags;
	bool		waiting = false;

	for (;;)
	{
		flags = pg_atomic_read_u64(&io->io_method_data.iocp.flags);
		submitter_id = pgaio_iocp_submitter_from_flags(flags);
		completer_id = pgaio_iocp_completer_from_flags(flags);

		printf("[%x] pgaio_iocp_take_baton: flags = %llx\n", my_aio_id, flags);

		if (flags & PGAIO_IOCP_FLAG_AVAILABLE)
		{
			if ((flags & PGAIO_IOCP_FLAG_RESULT))
			{
				/*
				 * The raw result from the kernel is already, available, or
				 * the caller (submitter) has it.  Grant the baton
				 * immediately.
				 */
				if (!pgaio_iocp_update_flags(io,
												flags,
												PGAIO_IOCP_FLAG_DONE,
												submitter_id,
												my_aio_id))
					continue;	/* lost race, try again */
				return true;
			}
			else
			{
				/* Request the right to complete. */
				if (!pgaio_iocp_update_flags(io,
												flags,
												PGAIO_IOCP_AIO_FLAG_REQUESTED,
												submitter_id,
												my_aio_id))
					continue;	/* lost race, try again */

				/* Go around again. */
				waiting = true;
			}
		}
		else if (flags & PGAIO_IOCP_AIO_FLAG_REQUESTED)
		{

			if (waiting)
			{
				Assert(completer_id == my_aio_id);

				/*
				 * We're waiting for the IOCP thread to write the result and
				 * set our latch.
				 */
				WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, 0);
				ResetLatch(MyLatch);

				/* Go around again. */
			}
			else
			{
				/*
				 * Someone else has requested the baton and is waiting.  No
				 * point in trying to usurp it, we'd only have to wait too.
				 * We'll have to wait on the IO CV.
				 *
				 * XXX Should the submitter steal the IO back if it sees
				 * someone else waiting?  It can perhaps run the completions
				 * more efficiently.
				 */
				return false;
			}
		}
		else if (flags & PGAIO_IOCP_AIO_FLAG_GRANTED)
		{
			/* It was granted to someone.  Was it us? */
			if (completer_id == my_aio_id)
			{
				if (!pgaio_iocp_update_flags(io,
												flags,
												PGAIO_IOCP_FLAG_DONE,
												submitter_id,
												my_aio_id))
					continue;	/* lost race, try again */
				return true;
			}
			return false;
		}
		else
		{
			/* Initial or done state. */
			return false;
		}
	}
}

/*
 * Store the raw result.  Called by the IOCP thread.  Only allowed to touch
 * shared memory and set latches, can't run any normal PostgreSQL code.
 */
static bool
pgaio_iocp_give_baton(PgAioInProgress * io, int result)
{
	uint64		flags;
	uint32		submitter_id;
	uint32		completer_id;

	io->io_method_data.iocp.raw_result = result;


	for (;;)
	{
		flags = pg_atomic_read_u64(&io->io_method_data.iocp.flags);
		submitter_id = pgaio_iocp_submitter_from_flags(flags);
		completer_id = pgaio_iocp_completer_from_flags(flags);

		if (flags & PGAIO_IOCP_FLAG_AVAILABLE)
		{
			if (!pgaio_iocp_update_flags(io,
											flags,
											PGAIO_IOCP_FLAG_AVAILABLE |
											PGAIO_IOCP_FLAG_RESULT,
											submitter_id,
											completer_id))
				continue;		/* lost race, try again (not expected) */

			/*
			 * There's no one to grant the baton to, but the next backend to
			 * try to take the baton will succeed immediately.
			 */
			break;
		}
		else if (flags & PGAIO_IOCP_AIO_FLAG_REQUESTED)
		{
			if (!pgaio_iocp_update_flags(io,
											flags,
											PGAIO_IOCP_AIO_FLAG_GRANTED,
											submitter_id,
											completer_id))
				continue;		/* lost race, try again (not expected) */

			/* Wake the completer. */
			SetLatch(&ProcGlobal->allProcs[completer_id].procLatch);

			printf("[%x] pgaio_iocp_give_baton: waking backend %d %llu\n", my_aio_id, completer_id, flags);


			return true;
		}
		else
		{
			fprintf(stderr, "unreachable %llu\n", (long long unsigned) flags);
			abort();
		}
	}

	return false;
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
	 * XXX Oops, here we might want to call something that waits for all IOs
	 * on this fd.  POSIX AIO has something like that.  Previously I had
	 * something like that in aio.c but it was buggy so I took it out.  Part
	 * of the problem is that some other backend can complete an IO that you
	 * own any time, and in this WWindows IOCP-thread mockup it's ever harder:
	 * you'd need to go through everything on the issued and issued_abandoned
	 * lists, and wait for it to complete if we are the submitter, using the
	 * baton logic so that there's an atomic check that we're really still the
	 * submitter.  Or something.
	 *
	 * For now, just wait for *everything* we submitted, which is pessimal!
	 */
	pgaio_wait_for_issued();
}

/*
 * A thread that runs forever consuming IO completion notifications and
 * transferring the results to PostgreSQL's share memory structures.
 */
static DWORD WINAPI
pgaio_iocp_completion_thread(LPVOID param)
{
	for (;;)
	{
		PgAioInProgress *io;
		OVERLAPPED *overlapped;
		DWORD nbytes;
		ULONG_PTR completion_key = 0; /* not used */

		printf("pgaio_iocp_completion_thread:\n");
		/*
		 * XXX There is also GetQueuedCompletionStatusEx() that can dequeue
		 * multiple results at once, but I can't figure out how to get
		 * per I/O errors...
		 */

		if (!GetQueuedCompletionStatus(pgaio_iocp_completion_port,
									   &nbytes,
									   &completion_key,
									   &overlapped,
									   INFINITE))
		{
			if (overlapped)
			{
				io = io_for_overlapped(overlapped);
				_dosmaperr(GetLastError());
				pgaio_iocp_kernel_io_done(io, -errno);
			}
			else
			{
				/* Error trying to dequeue. */
				fprintf(stderr, "could not wait for completion events: %m\n");
				abort();
			}
		}
		else
		{
			io = io_for_overlapped(overlapped);
			pgaio_iocp_kernel_io_done(io, nbytes);
		}
	}
}

static void
pgaio_iocp_postmaster_child_init_local(void)
{
	HANDLE		thread_handle;
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

	/* Start the I/O completion thread. */
	thread_handle = CreateThread(NULL, 0, pgaio_iocp_completion_thread,
								 NULL, 0, NULL);
	if (thread_handle == NULL)
		elog(FATAL, "could not create completion port");
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

	/*
	 * XXX This is the way you register an existing file handle with an
	 * existing IOCP, right?
	 */
	if (CreateIoCompletionPort(file_handle,
								pgaio_iocp_completion_port,
								CompletionKey,
								1) != pgaio_iocp_completion_port)
	{
		_dosmaperr(GetLastError());
		elog(FATAL, "could not associate file handle with completion port");
	}
}
