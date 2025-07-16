/*-------------------------------------------------------------------------
 *
 * method_posix_aio.c
 *    AIO - POSIX AIO with FreeBSD extensions
 *
 * IMPLEMENTATION
 *
 * The "submit" entry point calls aio_read() etc or lio_listio() for many at
 * IOs at once.  If the kernel reports resource exhaustion with EAGAIN, it
 * falls back to synchronous execution.
 *
 * The "wait_one" entry point waits for completion notifications with kevent(),
 * using EVFILT_AIO events.
 *
 * The main complication is cross-process completion, which POSIX AIO doesn't
 * allow.  A signal handler moves raw results between processes if necessary.
 * This code is wrapped in #ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL,
 * and potentially removable in future work.
 *
 * PORTABILITY
 *
 * This module currently runs only on FreeBSD.
 *
 * 1.  We could support running on systems without vectored I/O, but in
 * practice FreeBSD's aio_readv()/aio_writev() and LIO_READV/LIO_WRITEV
 * extensions are needed practice to be competitive with synchronous
 * preadv()/pwritev() in io_method=worker, for buffer pool I/O.
 *
 * 2.  Although NetBSD and macOS have both POSIX AIO and kevent(), they are not
 * yet connected, and other available options are unsuitable.  pgaio requires
 * an efficient queue-like way of cohsuming completions that can't drop
 * notifications due to overlowing queues, which seems to exclude polling all
 * IOs with aio_error() and POSIX realtime signals.  (Other platform-specific
 * candidates might include SIGEV_IOCP and SIGEV_PORT.)
 *
 * POSIX doesn't specify whether .wait_on_fd_before_close is required, but
 * FreeBSD doesn't need it.
 *
 * FUTURE DIRECTIONS
 *
 *  * The cross-process coping functions pgaio_posix_aio_ipc_XXX() would not be
 *    needed in a future multi-threaded server or if FreeBSD supported
 *    cross-process completion draining.
 *
 *  * Future PGAIO_OP_XXX operations corresponding to fsync(), fdatasync(),
 *    send(), recv() can be implemented.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/method_posix_aio.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* included early, for IOMETHOD_POSIX_AIO_ENABLED */
#include "storage/aio.h"		/* IWYU pragma: keep */

#ifdef IOMETHOD_POSIX_AIO_ENABLED

#include <aio.h>
#include <signal.h>

#ifdef HAVE_SYS_EVENT_H
#include <sys/event.h>
#endif

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "utils/wait_event.h"

#ifndef SIGEV_KEVENT
#error "kevent() with EVFILT_AIO support is required"
#endif

/* Currently, backends can only drain their own completion queue directly. */
#define PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL

/* Entry points for IoMethodOps. */
static size_t pgaio_posix_aio_shmem_size(void);
static void pgaio_posix_aio_shmem_init(bool first_time);
static void pgaio_posix_aio_init_backend(void);
static int	pgaio_posix_aio_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);
static void pgaio_posix_aio_wait_one(PgAioHandle *ioh, uint64 ref_generation);

const IoMethodOps pgaio_posix_aio_ops = {
	.shmem_size = pgaio_posix_aio_shmem_size,
	.shmem_init = pgaio_posix_aio_shmem_init,
	.init_backend = pgaio_posix_aio_init_backend,

	.submit = pgaio_posix_aio_submit,
	.wait_one = pgaio_posix_aio_wait_one,
};

/* Per-backend state. */
typedef struct pg_attribute_aligned (PG_CACHE_LINE_SIZE)
PgAioPosixAioContext
{
	LWLock		completion_lock;
	int			completion_queue_fd;

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
	pg_atomic_uint32 ipc_procno;;
#endif
} PgAioPosixAioContext;

static PgAioPosixAioContext *pgaio_posix_aio_contexts;
static PgAioPosixAioContext *pgaio_my_posix_aio_context;
static struct aiocb *pgaio_posix_aio_aiocbs;

/* Completion queue support functions. */
static void pgaio_posix_aio_init_completion_queue(PgAioPosixAioContext *context);
static void pgaio_posix_aio_prepare_completion_queue(PgAioHandle *ioh, struct aiocb *aiocb);
static int	pgaio_posix_aio_drain_completion_queue(PgAioHandle *ioh);

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
/* Cross-process completion queue support functions. */
static void pgaio_posix_aio_ipc_init(void);
static void pgaio_posix_aio_ipc_init_context(PgAioPosixAioContext *context);
static int	pgaio_posix_aio_ipc_drain_completion_queue(PgAioHandle *ioh);
static void pgaio_posix_aio_ipc_acquire_own_completion_lock(PgAioPosixAioContext *context);
static void pgaio_posix_aio_ipc_handler(SIGNAL_ARGS);
#endif

static size_t
pgaio_posix_aio_backends(void)
{
	return MaxBackends + NUM_AUXILIARY_PROCS;
}

static size_t
pgaio_posix_aio_shmem_size(void)
{
	return mul_size(sizeof(PgAioPosixAioContext), pgaio_posix_aio_backends());
}

static void
pgaio_posix_aio_shmem_init(bool first_time)
{
	bool		found;

	pgaio_posix_aio_contexts = (PgAioPosixAioContext *)
		ShmemInitStruct("AioPosixAioContext", pgaio_posix_aio_shmem_size(), &found);
	if (!found)
	{
		for (int i = 0; i < pgaio_posix_aio_backends(); i++)
		{
			PgAioPosixAioContext *context = &pgaio_posix_aio_contexts[i];

			LWLockInitialize(&context->completion_lock,
							 LWTRANCHE_AIO_POSIX_AIO_COMPLETION);

			pgaio_posix_aio_ipc_init_context(context);
		}
	}
}

static void
pgaio_posix_aio_init_backend(void)
{
	pgaio_my_posix_aio_context = &pgaio_posix_aio_contexts[MyProcNumber];

	/* AIO control blocks for this backend's IOs. */
	pgaio_posix_aio_aiocbs = palloc(sizeof(struct aiocb) * io_max_concurrency);

	pgaio_posix_aio_init_completion_queue(pgaio_my_posix_aio_context);

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
	pgaio_posix_aio_ipc_init();
#endif
}


static PgAioPosixAioContext *
pgaio_posix_aio_get_context(PgAioHandle *ioh)
{
	return &pgaio_posix_aio_contexts[ioh->owner_procno];
}

static struct aiocb *
pgaio_posix_aio_get_aiocb(PgAioHandle *ioh)
{
	int			id;

	id = pgaio_io_get_id(ioh);

	Assert(ioh->owner_procno == MyProcNumber);
	Assert(id >= pgaio_my_backend->io_handle_off);
	Assert(id < pgaio_my_backend->io_handle_off + io_max_concurrency);

	return &pgaio_posix_aio_aiocbs[id - pgaio_my_backend->io_handle_off];
}

static PgAioHandle *
pgaio_posix_aio_get_io_by_id(int32 id)
{
	return &pgaio_ctl->io_handles[id];
}

static void
pgaio_posix_aio_prepare_aiocb(PgAioHandle *ioh, struct aiocb *aiocb)
{
	struct iovec *iov;

	memset(aiocb, 0, sizeof(*aiocb));

	switch (ioh->op)
	{
		case PGAIO_OP_READV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
			if (ioh->op_data.read.iov_length == 1)
			{
				aiocb->aio_fildes = ioh->op_data.read.fd;
				aiocb->aio_offset = ioh->op_data.read.offset;
				aiocb->aio_buf = iov->iov_base;
				aiocb->aio_nbytes = iov->iov_len;
				aiocb->aio_lio_opcode = LIO_READ;
			}
			else
			{
				aiocb->aio_fildes = ioh->op_data.read.fd;
				aiocb->aio_offset = ioh->op_data.read.offset;
				aiocb->aio_iov = iov;
				aiocb->aio_iovcnt = ioh->op_data.read.iov_length;
				aiocb->aio_lio_opcode = LIO_READV;
			}
			break;

		case PGAIO_OP_WRITEV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
			if (ioh->op_data.write.iov_length == 1)
			{
				aiocb->aio_fildes = ioh->op_data.write.fd;
				aiocb->aio_offset = ioh->op_data.write.offset;
				aiocb->aio_buf = iov->iov_base;
				aiocb->aio_nbytes = iov->iov_len;
				aiocb->aio_lio_opcode = LIO_WRITE;
			}
			else
			{
				aiocb->aio_fildes = ioh->op_data.write.fd;
				aiocb->aio_offset = ioh->op_data.write.offset;
				aiocb->aio_iov = iov;
				aiocb->aio_iovcnt = ioh->op_data.write.iov_length;
				aiocb->aio_lio_opcode = LIO_WRITEV;
			}
			break;

		case PGAIO_OP_INVALID:
			elog(ERROR, "trying to prepare invalid IO operation for execution");
	}
}

static bool
pgaio_posix_aio_submit_aiocb(struct aiocb *aiocb)
{
	int			ret;

	switch (aiocb->aio_lio_opcode)
	{
		case LIO_READ:
			ret = aio_read(aiocb);
			break;
		case LIO_WRITE:
			ret = aio_write(aiocb);
			break;
		case LIO_READV:
			ret = aio_readv(aiocb);
			break;
		case LIO_WRITEV:
			ret = aio_writev(aiocb);
			break;
		default:
			ret = -1;
			errno = ENOSYS;
	}

	if (ret < 0)
	{
		if (errno == EAGAIN)
			return false;
		elog(PANIC, "could not start POSIX AIO operation: %m");
	}
	return true;
}

static inline void
pgaio_posix_aio_submitted(PgAioHandle *ioh)
{
	/*
	 * Advance to PGAIO_HS_SUBMITTED state only after successful submission.
	 * If we advanced it sooner, wait_one() could be reached for an IO that is
	 * falling back to synchronous execution and hang.
	 */
	pgaio_io_prepare_submit(ioh);
}

static int
pgaio_posix_aio_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	struct aiocb *asynchronous_aiocbs[PGAIO_SUBMIT_BATCH_SIZE];
	struct PgAioHandle *synchronous_ios[PGAIO_SUBMIT_BATCH_SIZE];
	int			nsync = 0;

	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];
		struct aiocb *aiocb = pgaio_posix_aio_get_aiocb(ioh);

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL

		/*
		 * Store the special not-yet-drained value before submitting.
		 * Otherwise, it could clobber a result concurrently stored by
		 * pgaio_posix_aio_ipc_handler().
		 */
		ioh->result = -EINPROGRESS;
#endif

		asynchronous_aiocbs[i] = aiocb;
		pgaio_posix_aio_prepare_aiocb(ioh, aiocb);
		pgaio_posix_aio_prepare_completion_queue(ioh, aiocb);
	}

	pgstat_report_wait_start(WAIT_EVENT_AIO_POSIX_AIO_SUBMIT);
	if (num_staged_ios == 1)
	{
		/* Avoid lio_listio() for single IOs, saving a couple of cycles. */
		if (pgaio_posix_aio_submit_aiocb(asynchronous_aiocbs[0]))
			pgaio_posix_aio_submitted(staged_ios[0]);
		else
			synchronous_ios[nsync++] = staged_ios[0];
	}
	else
	{
		/* Batch submission. */
		if (lio_listio(LIO_NOWAIT,
					   asynchronous_aiocbs,
					   num_staged_ios,
					   NULL) == 0)
		{
			for (int i = 0; i < num_staged_ios; ++i)
				pgaio_posix_aio_submitted(staged_ios[i]);
		}
		else
		{
			if (errno != EINTR || errno != EIO || errno != EAGAIN)
				elog(PANIC, "could not start list of POSIX AIO operations: %m");

			for (int i = 0; i < num_staged_ios; ++i)
			{
				int			error = aio_error(asynchronous_aiocbs[i]);

				if (error < 0 || error == EINVAL || error == EAGAIN)
					synchronous_ios[nsync++] = staged_ios[i];
				else
					pgaio_posix_aio_submitted(staged_ios[i]);
			}
		}
	}
	pgstat_report_wait_end();

	/* For any EAGAIN failures, gracefully fall back to synchronous execution. */
	for (int i = 0; i < nsync; ++i)
	{
		pgaio_io_prepare_submit_synchronously(synchronous_ios[i]);
		pgaio_io_perform_synchronously(synchronous_ios[i]);
	}

	return num_staged_ios;
}

static void
pgaio_posix_aio_wait_one(PgAioHandle *ioh, uint64 ref_generation)
{
	ProcNumber	owner_procno = pgaio_io_get_owner(ioh);
	PgAioPosixAioContext *context = pgaio_posix_aio_get_context(ioh);
	PgAioHandleState state;

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
	if (owner_procno == MyProcNumber)
	{
		pgaio_posix_aio_ipc_acquire_own_completion_lock(context);
	}
	else
#endif
	{
		LWLockAcquire(&context->completion_lock, LW_EXCLUSIVE);
	}

	if (!pgaio_io_was_recycled(ioh, ref_generation, &state) &&
		state == PGAIO_HS_SUBMITTED)
	{
		START_CRIT_SECTION();
		pgaio_posix_aio_drain_completion_queue(ioh);
		END_CRIT_SECTION();
	}

	LWLockRelease(&context->completion_lock);
}




/*
 * Supported completion queue APIs providing the functions
 * pgaio_posix_aio_{init,prepare,drain}_completion_queue().  Currently the only
 * implementation uses kqueue.
 */

static void
pgaio_posix_aio_init_completion_queue(PgAioPosixAioContext *context)
{
	context->completion_queue_fd = kqueue();
	if (context->completion_queue_fd < 0)
		elog(ERROR, "kqueue() failed: %m");
}

static void
pgaio_posix_aio_prepare_completion_queue(PgAioHandle *ioh, struct aiocb *aiocb)
{
	aiocb->aio_sigevent.sigev_notify = SIGEV_KEVENT;
	aiocb->aio_sigevent.sigev_notify_kqueue =
		pgaio_my_posix_aio_context->completion_queue_fd;
}

static PgAioHandle *
pgaio_posix_aio_io_for_aiocb(struct aiocb *aiocb)
{
	int			id;

	id = pgaio_my_backend->io_handle_off + (aiocb - pgaio_posix_aio_aiocbs);

	return pgaio_posix_aio_get_io_by_id(id);
}

static int
pgaio_posix_aio_drain_completion_queue(PgAioHandle *ioh)
{
	PgAioPosixAioContext *context;
	const struct timespec *timeout = NULL;
	struct kevent events[PGAIO_SUBMIT_BATCH_SIZE];
	int			nevents;
	int			count = 0;
	bool		in_signal_handler = false;

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
	if (ioh == NULL)
	{
		const struct timespec zero_timeout = {0};

		/*
		 * Running on behalf of another process that holds the
		 * completion_lock. Fall through to fill in any result values that are
		 * available without waiting.
		 */
		in_signal_handler = true;
		timeout = &zero_timeout;
		context = pgaio_my_posix_aio_context;
	}
	else if (ioh->result != -EINPROGRESS)
	{
		/*
		 * An earlier signal handler filled this result in on behalf of
		 * someone else, but it wasn't the one it was waiting for so it hasn't
		 * been completed yet.
		 */
		pgaio_io_process_completion(ioh, ioh->result);
		return 1;
	}
	else
	{
		/*
		 * Need to drain.  If it's this backend's queue, fall through to do so
		 * directly.  Otherwise signal the owning backend.
		 */
		context = pgaio_posix_aio_get_context(ioh);
		if (context != pgaio_my_posix_aio_context)
			return pgaio_posix_aio_ipc_drain_completion_queue(ioh);
	}
#endif

retry:

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
	if (!in_signal_handler)
#endif
	{
		pgstat_report_wait_start(WAIT_EVENT_AIO_POSIX_AIO_EXECUTION);
	}

	nevents = kevent(context->completion_queue_fd, NULL, 0,
					 events, lengthof(events), timeout);

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
	if (!in_signal_handler)
#endif
	{
		pgstat_report_wait_end();
	}

	if (nevents < 0)
	{
		if (errno == EINTR)
			goto retry;
		elog(PANIC, "kevent() failed: %m");
	}

	for (int i = 0; i < nevents; i++)
	{
		struct aiocb *aiocb = (struct aiocb *) events[i].ident;
		int			error = events[i].data;
		int			result;
		PgAioHandle *ioh;

		if (events[i].filter != EVFILT_AIO)
			elog(PANIC, "unexpected kevent filter %d", events[i].filter);

		/* We know which IO completed and its error status. */
		result = aio_return(aiocb);
		if (error != 0)
			result = -error;

		ioh = pgaio_posix_aio_io_for_aiocb(aiocb);

#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL
		if (in_signal_handler)
		{
			/*
			 * Store result for processing by the requester.  This could be
			 * any IO, not necessarily the one it will process, so we always
			 * have to check for these incidental cases above.
			 */
			ioh->result = result;
		}
		else
#endif
		{
			pgaio_io_process_completion(ioh, result);
		}

		count++;
	}

	return count;
}




#ifdef PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL

static void
pgaio_posix_aio_ipc_init(void)
{
	pqsignal(SIGIO, pgaio_posix_aio_ipc_handler);
}

static void
pgaio_posix_aio_ipc_init_context(PgAioPosixAioContext *context)
{
	pg_atomic_init_u32(&context->ipc_procno, INVALID_PROC_NUMBER);
}

static int
pgaio_posix_aio_ipc_drain_completion_queue(PgAioHandle *ioh)
{
	PgAioPosixAioContext *context = pgaio_posix_aio_get_context(ioh);
	int			backoff_ms = 10;
	int			count = 0;

	Assert(LWLockHeldByMe(&context->completion_lock));
	Assert(context != pgaio_my_posix_aio_context);

	for (;;)
	{
		uint32		old_proc_number;
		int			event;

		/* Raw result available? */
		if (ioh->result != -EINPROGRESS)
		{
			pgaio_io_process_completion(ioh, ioh->result);
			count++;
			break;
		}

		/* Say who's calling. */
		old_proc_number = INVALID_PROC_NUMBER;
		if (!pg_atomic_compare_exchange_u32(&context->ipc_procno,
											&old_proc_number,
											MyProcNumber) &&
			old_proc_number != MyProcNumber)
			break;				/* yield lock to owner */

		/* Ask owner to drain completions without waiting. */
		if (old_proc_number != INVALID_PROC_NUMBER)
			kill(GetPGProcByNumber(ioh->owner_procno)->pid, SIGIO);

		event = WaitLatch(MyLatch,
						  WL_LATCH_SET |
						  WL_TIMEOUT |
						  WL_EXIT_ON_PM_DEATH,
						  backoff_ms,
						  WAIT_EVENT_AIO_POSIX_AIO_EXECUTION_IPC);
		if (event == WL_LATCH_SET)
			ResetLatch(MyLatch);

		/*
		 * Repeated polling with an exponential backoff, to guarantee
		 * progress.  Usually IOs are either finished on first poll, or
		 * there's a real IO stall and the owner begins to wait and takes
		 * over, but this last resort strategy can break a deadlock if
		 * necessary.
		 */
		if (backoff_ms < 1000)
			backoff_ms *= 2;
	}

	return count;
}

/*
 * Acquire this backend's own completion_lock, and if some other backend holds
 * it, also command pgaio_posix_aio_ipc_drain_completion_queue() to give up
 * early since this backend can process its own queue promptly and efficiently.
 */
static void
pgaio_posix_aio_ipc_acquire_own_completion_lock(PgAioPosixAioContext *context)
{
	Assert(context == pgaio_my_posix_aio_context);
	Assert(!LWLockHeldByMe(&context->completion_lock));

	if (!LWLockConditionalAcquire(&context->completion_lock, LW_EXCLUSIVE))
	{
		ProcNumber	procno;

		procno = pg_atomic_exchange_u32(&context->ipc_procno, MyProcNumber);
		if (procno != INVALID_PROC_NUMBER)
			SetLatch(&GetPGProcByNumber(procno)->procLatch);

		LWLockAcquire(&context->completion_lock, LW_EXCLUSIVE);
		pg_atomic_write_u32(&context->ipc_procno, INVALID_PROC_NUMBER);
	}
}

/*
 * Signal handler that fills available ioh->result values without waiting and
 * then wakes the requester, if at least one was filled.
 */
static void
pgaio_posix_aio_ipc_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	PgAioPosixAioContext *context = pgaio_my_posix_aio_context;
	uint32		procno;

	/*
	 * The signal was sent by a backend that (at that moment) held the
	 * completion_lock and was waiting for a response, but *this* backend
	 * might be partway through commandeering the lock.
	 */
	procno = pg_atomic_read_u32(&context->ipc_procno);
	if (procno == MyProcNumber)
		return;

	/*
	 * Mark the request as handled, which also checks if the requester is
	 * still interested as of this instant.  This is an indirect way of making
	 * sure that *this* backend doesn't hold the completion lock as of the
	 * point of interruption.  The drain function could become very confused
	 * if interrupted and reentered from here.
	 */
	if (!pg_atomic_compare_exchange_u32(&context->ipc_procno,
										&procno,
										INVALID_PROC_NUMBER))
		return;

	if (pgaio_posix_aio_drain_completion_queue(NULL) > 0)
		SetLatch(&GetPGProcByNumber(procno)->procLatch);

	errno = save_errno;
}

#endif							/* PGAIO_POSIX_AIO_NEED_CROSS_PROCESS_SIGNAL */

#endif							/* IOMETHOD_POSIX_AIO_ENABLED */
