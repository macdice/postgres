/*-------------------------------------------------------------------------
 *
 * method_posix_aio.c
 *    AIO - Standard POSIX AIO with optional FreeBSD extensions
 *
 * https://pubs.opengroup.org/onlinepubs/9799919799/basedefs/aio.h.html
 *
 * PORTABILITY
 *
 *  Working:                               FreeBSD
 *  Working but experimental:              NetBSD, macOS
 *  Worked in the past, might come back:   AIX
 *  Compiles and runs, not for end users:  Linux, Solaris/illumos
 *
 * The last group including Linux (really glibc and musl) implements POSIX AIO
 * in user space with a thread pool that we don't want, among other problems,
 * but it's useful to be able to maintain the code on common developer
 * platforms.  Linux users should choose io_method=io_uring instead.
 *
 * Vectored I/O is currently only available on FreeBSD.  Without it,
 * multi-vector operations result in short reads/writes and retries.
 *
 * IMPLEMENTATION
 *
 * The "submit" entry point calls aio_read() etc or lio_listio() for many at
 * once.  If the OS reports resource exhaustion with EAGAIN, it falls back to
 * synchronous execution.
 *
 * The "wait_one" entry point waits for completion notifications with kevent()
 * or sigwait().
 *
 * The main complication is cross-process completion, which POSIX AIO doesn't
 * allow.  A signal handler moves raw results between processes if necessary.
 *
 * FUTURE DIRECTIONS
 *
 *  * The cross-process coping functions pgaio_posix_aio_ipc_XXX() would not be
 *    needed in a future multi-threaded server.
 *
 *  * Higher levels should avoid sending vectored operations to I/O methods
 *    that can't handle them (a problem on Windows too).
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

/* Does this platform have vectored AIO operations? */
#if defined(LIO_READV) && defined(LIO_WRITEV)
#define PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
#endif

/* Which completion queue API? */
#ifdef HAVE_SYS_EVENT_H
#if !defined(SIGEV_KEVENT) || !defined(EVFILT_AIO)
#define PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
#endif
#define PGAIO_POSIX_AIO_USE_KEVENT
#else
#define PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
#define PGAIO_POSIX_AIO_USE_SIGWAIT
#endif

/* Platform quirks and characteristics. */
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__)
#define PGAIO_POSIX_AIO_CLOSE_HARMLESS
#endif
#ifdef __APPLE__
#define PGAIO_POSIX_AIO_LIO_NOTIFICATIONS_BROKEN
#endif
#ifdef __GLIBC__
#define PGAIO_POSIX_AIO_ASYNC_SIGNAL_SAFETY_BROKEN
#endif

/* Entry points for IoMethodOps. */
static size_t pgaio_posix_aio_shmem_size(void);
static void pgaio_posix_aio_shmem_init(bool first_time);
static void pgaio_posix_aio_init_backend(void);
static int	pgaio_posix_aio_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);
static void pgaio_posix_aio_wait_one(PgAioHandle *ioh, uint64 ref_generation);

const IoMethodOps pgaio_posix_aio_ops = {
#ifdef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
	.have_vectored_file_io_buffered = true,
	.have_vectored_file_io_direct = true,
#endif
#ifndef PGAIO_POSIX_AIO_CLOSE_HARMLESS

	/*
	 * POSIX allows queued AIOs to be canceled on close, and even worse, user
	 * space implementions can fail with EBADF or access the wrong file, so
	 * wait first unless this platform is known to let them run.
	 */
	.wait_on_fd_before_close = true,
#endif

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
	/* Serialize completion processing for each backend. */
	LWLock		completion_lock;

	/* Cross-process completion support. */
	pg_atomic_uint32 ipc_procno;;
	int			ipc_io_id;
} PgAioPosixAioContext;

static PgAioPosixAioContext *pgaio_posix_aio_contexts;
static PgAioPosixAioContext *pgaio_my_posix_aio_context;
static struct aiocb *pgaio_posix_aio_aiocbs;
static int	pgaio_posix_aio_naiocbs;

/* Completion queue support functions. */
static void pgaio_posix_aio_init_completion_queue(void);
static void pgaio_posix_aio_prepare_completion_queue(PgAioHandle *ioh, struct aiocb *aiocb);
static int	pgaio_posix_aio_drain_completion_queue(PgAioHandle *ioh, bool cross_process);

/* Cross-process completion queue support functions. */
static void pgaio_posix_aio_ipc_init(void);
static void pgaio_posix_aio_ipc_init_context(PgAioPosixAioContext *context);
static void pgaio_posix_aio_ipc_drain_completion_queue(PgAioHandle *ioh);
static void pgaio_posix_aio_ipc_acquire_own_completion_lock(PgAioPosixAioContext *context);
static void pgaio_posix_aio_ipc_begin_submit(void);
static void pgaio_posix_aio_ipc_end_submit(void);
static void pgaio_posix_aio_ipc_handler(SIGNAL_ARGS);

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
	pgaio_posix_aio_naiocbs = dclist_count(&pgaio_my_backend->idle_ios);
	pgaio_posix_aio_aiocbs = palloc(sizeof(struct aiocb) *
									pgaio_posix_aio_naiocbs);

	pgaio_posix_aio_init_completion_queue();
	pgaio_posix_aio_ipc_init();
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
	Assert(id < pgaio_my_backend->io_handle_off + pgaio_posix_aio_naiocbs);

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
#ifndef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
			/* Force short read, first vector only. */
			ioh->op_data.read.iov_length = 1;
#endif
			if (ioh->op_data.read.iov_length == 1)
			{
				aiocb->aio_fildes = ioh->op_data.read.fd;
				aiocb->aio_offset = ioh->op_data.read.offset;
				aiocb->aio_buf = iov->iov_base;
				aiocb->aio_nbytes = iov->iov_len;
				aiocb->aio_lio_opcode = LIO_READ;
			}
#ifdef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
			else
			{
				aiocb->aio_fildes = ioh->op_data.read.fd;
				aiocb->aio_offset = ioh->op_data.read.offset;
				aiocb->aio_iov = iov;
				aiocb->aio_iovcnt = ioh->op_data.read.iov_length;
				aiocb->aio_lio_opcode = LIO_READV;
			}
#endif
			break;

		case PGAIO_OP_WRITEV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
#ifndef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
			/* Force short write, first vector only. */
			ioh->op_data.write.iov_length = 1;
#endif
			if (ioh->op_data.write.iov_length == 1)
			{
				aiocb->aio_fildes = ioh->op_data.write.fd;
				aiocb->aio_offset = ioh->op_data.write.offset;
				aiocb->aio_buf = iov->iov_base;
				aiocb->aio_nbytes = iov->iov_len;
				aiocb->aio_lio_opcode = LIO_WRITE;
			}
#ifdef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
			else
			{
				aiocb->aio_fildes = ioh->op_data.write.fd;
				aiocb->aio_offset = ioh->op_data.write.offset;
				aiocb->aio_iov = iov;
				aiocb->aio_iovcnt = ioh->op_data.write.iov_length;
				aiocb->aio_lio_opcode = LIO_WRITEV;
			}
#endif
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
#ifdef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
		case LIO_READV:
			ret = aio_readv(aiocb);
			break;
		case LIO_WRITEV:
			ret = aio_writev(aiocb);
			break;
#endif
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
#ifdef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
	/*
	 * When using a merged signal, store the special not-yet-drained value
	 * after submitting but before advancing to PGAIO_HS_SUBMITTED.  Otherwise
	 * pgaio_posix_aio_ipc_handler() could reach aio_error() before the aiocb
	 * is known to the OS.
	 */
	ioh->result = -EINPROGRESS;
#endif

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
	bool		submit_individually;

	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];
		struct aiocb *aiocb = pgaio_posix_aio_get_aiocb(ioh);

#ifndef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL

		/*
		 * When using EVFILT_AIO, store the special not-yet-drained value
		 * before submitting.  Otherwise, it could clobber a result
		 * concurrently stored by pgaio_posix_aio_ipc_handler().
		 */
		ioh->result = -EINPROGRESS;
#endif

		asynchronous_aiocbs[i] = aiocb;
		pgaio_posix_aio_prepare_aiocb(ioh, aiocb);
		pgaio_posix_aio_prepare_completion_queue(ioh, aiocb);
	}

#ifdef PGAIO_POSIX_AIO_LIO_NOTIFICATIONS_BROKEN
	/* Dropped per-aiocb notifications would hang. */
	submit_individually = true;
#else
	/* Use lio_listio() only for multiple IOs, saving a couple of cycles. */
	submit_individually = num_staged_ios == 1;
#endif

	pgaio_posix_aio_ipc_begin_submit();
	pgstat_report_wait_start(WAIT_EVENT_AIO_POSIX_AIO_SUBMIT);
	if (submit_individually)
	{
		for (int i = 0; i < num_staged_ios; i++)
		{
			if (pgaio_posix_aio_submit_aiocb(asynchronous_aiocbs[i]))
				pgaio_posix_aio_submitted(staged_ios[i]);
			else
				synchronous_ios[nsync++] = staged_ios[i];
		}
	}
	else
	{
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
	pgaio_posix_aio_ipc_end_submit();

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

	if (owner_procno == MyProcNumber)
		pgaio_posix_aio_ipc_acquire_own_completion_lock(context);
	else
		LWLockAcquire(&context->completion_lock, LW_EXCLUSIVE);

	if (!pgaio_io_was_recycled(ioh, ref_generation, &state) &&
		state == PGAIO_HS_SUBMITTED)
	{
		int			result = ioh->result;

		START_CRIT_SECTION();

		if (result != -EINPROGRESS)
			pgaio_io_process_completion(ioh, result);
		else if (owner_procno == MyProcNumber)
			pgaio_posix_aio_drain_completion_queue(ioh, false);
		else
			pgaio_posix_aio_ipc_drain_completion_queue(ioh);

		END_CRIT_SECTION();
	}

	LWLockRelease(&context->completion_lock);
}




/*
 * Supported completion queue APIs providing the functions
 * pgaio_posix_aio_{init,prepare,drain}_completion_queue().
 */

#ifdef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
/*
 * POSIX intended realtime (unmerged, queued, payload-carrying) signals as the
 * completion queues for AIO, and they can tell you which IO completed, but
 * they are baroque and unportable: queues overflow, and macOS hasn't got them.
 *
 * Just use a plain old merged signal as a completion notification, and poll
 * the inflight IO queue to unmerge it:
 *
 *  * If using kevent(), EVFILT_SIGNAL has a merge count, so we can poll in
 *    submission order and terminate when we've found them all.  This avoids
 *    almost all spurious polling as long as completion order roughly matches.
 *
 *  * If using sigwait(), available on all POSIX systems, then we have to
 *    poll all inflight IOs.  That's terrible, but compiles and runs on
 *    Linux/glibc using the same code paths, for basic testing only.
 *
 * FreeBSD needs none of this and never polls.  It receives EVFILT_AIO
 * events containing the IO and error status.  Hopefully macOS and NetBSD will
 * connect their AIO to their kqueue one day.  (Commercial Unixen all had their
 * own kqueue-like thing for this, to review if AIX support is ever wanted.)
 */

#if defined(SIGPOLL)
#define PGAIO_POSIX_AIO_COMPLETION_SIGNO SIGPOLL
#elif defined(SIGIO)
#define PGAIO_POSIX_AIO_COMPLETION_SIGNO SIGIO
#else
#error "can't find a signal to use for I/O completion notification from kernel"
#endif

static bool
pgaio_posix_aio_poll_one(PgAioHandle *ioh, bool cross_process)
{
	int			error;
	int			result;

	error = aio_error(pgaio_posix_aio_get_aiocb(ioh));
	if (error < 0)
		elog(PANIC, "aio_error() failed: %m");

	if (error == EINPROGRESS)
		return false;

	result = aio_return(pgaio_posix_aio_get_aiocb(ioh));
	if (error != 0)
		result = -error;

	if (cross_process)
		ioh->result = result;
	else
		pgaio_io_process_completion(ioh, result);

	return true;
}

static PgAioHandle *
pgaio_posix_aio_next_inprogress(PgAioHandle *ioh)
{
	while (dclist_has_next(&pgaio_my_backend->in_flight_ios, &ioh->node))
	{
		ioh = dclist_container(PgAioHandle,
							   node,
							   dclist_next_node(&pgaio_my_backend->in_flight_ios,
												&ioh->node));
		if (ioh->result == -EINPROGRESS)
			return ioh;
	}

	return NULL;
}

static int
pgaio_posix_aio_poll_inflight_queue(int limit)
{
	PgAioHandle *ioh;
	int			count = 0;

	Assert(LWLockHeldByMe(&pgaio_my_posix_aio_context->completion_lock));

	/* Start from the oldest and most likely to be finished. */
	if (dclist_is_empty(&pgaio_my_backend->in_flight_ios))
		elog(ERROR, "waiting for IO while no IOs in flight");
	ioh = dclist_head_element(PgAioHandle,
							  node,
							  &pgaio_my_backend->in_flight_ios);
	if (ioh->result != -EINPROGRESS)
		ioh = pgaio_posix_aio_next_inprogress(ioh);

	while (ioh && count < limit)
	{
		PgAioHandle *ioh_next = pgaio_posix_aio_next_inprogress(ioh);

		if (pgaio_posix_aio_poll_one(ioh, false))
			count++;

		ioh = ioh_next;
	}

	return count;
}

#endif

#ifdef PGAIO_POSIX_AIO_USE_KEVENT

static int	pgaio_posix_aio_kevent_fd = -1;

static void
pgaio_posix_aio_init_completion_queue(void)
{
	pgaio_posix_aio_kevent_fd = kqueue();
	if (pgaio_posix_aio_kevent_fd < 0)
		elog(ERROR, "kqueue() failed: %m");

#ifdef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
	{
		struct kevent kev;

		/* Block completion signal and register it with the kqueue. */
		sigaddset(&UnBlockSig, PGAIO_POSIX_AIO_COMPLETION_SIGNO);
		EV_SET(&kev, PGAIO_POSIX_AIO_COMPLETION_SIGNO, EVFILT_SIGNAL, EV_ADD, 0, 0, 0);
		if (kevent(pgaio_posix_aio_kevent_fd, &kev, 1, NULL, 0, NULL) < 0)
			elog(ERROR, "kevent() failed: %m");
	}
#endif
}

static void
pgaio_posix_aio_prepare_completion_queue(PgAioHandle *ioh, struct aiocb *aiocb)
{
#ifdef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
	aiocb->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
	aiocb->aio_sigevent.sigev_signo = PGAIO_POSIX_AIO_COMPLETION_SIGNO;
#else
	aiocb->aio_sigevent.sigev_notify = SIGEV_KEVENT;
	aiocb->aio_sigevent.sigev_notify_kqueue = pgaio_posix_aio_kevent_fd;
#endif
}

#ifndef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
static PgAioHandle *
pgaio_posix_aio_io_for_aiocb(struct aiocb *aiocb)
{
	int			id;

	id = pgaio_my_backend->io_handle_off + (aiocb - pgaio_posix_aio_aiocbs);

	return pgaio_posix_aio_get_io_by_id(id);
}
#endif

static int
pgaio_posix_aio_drain_completion_queue(PgAioHandle *ioh, bool cross_process)
{
	const struct timespec zero_timeout = {0};
	struct kevent events[PGAIO_SUBMIT_BATCH_SIZE];
	int			nevents;
	int			count = 0;

#ifdef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
	/* When called by cross-process handler, do the minimum. */
	if (cross_process)
		return (ioh->result == -EINPROGRESS &&
				pgaio_posix_aio_poll_one(ioh, true)) ? 1 : 0;
#endif

retry:
	if (!cross_process)
		pgstat_report_wait_start(WAIT_EVENT_AIO_POSIX_AIO_EXECUTION);
	nevents = kevent(pgaio_posix_aio_kevent_fd,
					 NULL, 0,
					 events, lengthof(events),
					 cross_process ? &zero_timeout : NULL);
	if (!cross_process)
		pgstat_report_wait_end();

	if (nevents < 0)
	{
		if (errno == EINTR)
			goto retry;
		elog(PANIC, "kevent() failed: %m");
	}

	for (int i = 0; i < nevents; i++)
	{
#ifdef PGAIO_POSIX_AIO_USE_MERGED_COMPLETION_SIGNAL
		if (events[i].filter == EVFILT_SIGNAL)
		{
			int			num_notifications = events[i].data;

			/* We don't know which IOs completed, but we do know how many. */
			count += pgaio_posix_aio_poll_inflight_queue(num_notifications);
		}
#else
		if (events[i].filter == EVFILT_AIO)
		{
			struct aiocb *aiocb = (struct aiocb *) events[i].ident;
			int			error = events[i].data;
			int			result;
			PgAioHandle *ioh;

			/* We know which IO completed and its error status. */
			result = aio_return(aiocb);
			if (error != 0)
				result = -error;

			ioh = pgaio_posix_aio_io_for_aiocb(aiocb);
			if (cross_process)
				ioh->result = result;
			else
				pgaio_io_process_completion(ioh, result);
			count++;
		}
#endif
	}

	return count;
}

#endif							/* PGAIO_POSIX_AIO_USE_KEVENT */

#if defined(PGAIO_POSIX_AIO_USE_SIGWAIT)

static sigset_t pgaio_posix_aio_completion_sigmask;

static void
pgaio_posix_aio_dummy_signal_handler(SIGNAL_ARGS)
{
	Assert(false);
}

static void
pgaio_posix_aio_init_completion_queue(void)
{
	/* Need a blocked dummy signal handler for sigwait(). */
	pqsignal(PGAIO_POSIX_AIO_COMPLETION_SIGNO,
			 pgaio_posix_aio_dummy_signal_handler);
	sigaddset(&UnBlockSig, PGAIO_POSIX_AIO_COMPLETION_SIGNO);
	sigemptyset(&pgaio_posix_aio_completion_sigmask);
	sigaddset(&pgaio_posix_aio_completion_sigmask,
			  PGAIO_POSIX_AIO_COMPLETION_SIGNO);
}

static void
pgaio_posix_aio_prepare_completion_queue(PgAioHandle *ioh, struct aiocb *aiocb)
{
	aiocb->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
	aiocb->aio_sigevent.sigev_signo = PGAIO_POSIX_AIO_COMPLETION_SIGNO;
}

static int
pgaio_posix_aio_drain_completion_queue(PgAioHandle *ioh, bool cross_process)
{
	int			dummy;

	/* When called by cross-process handler, do the minimum. */
	if (cross_process)
		return (ioh->result == -EINPROGRESS &&
				pgaio_posix_aio_poll_one(ioh, true)) ? 1 : 0;

retry:
	pgstat_report_wait_start(WAIT_EVENT_AIO_POSIX_AIO_EXECUTION);
	if (sigwait(&pgaio_posix_aio_completion_sigmask, &dummy) < 0)
	{
		if (errno == EINTR)
			goto retry;
		elog(PANIC, "could not wait for signal: %m");
	}
	pgstat_report_wait_end();

	/* We don't know which IOs completed, or even how many. */
	return pgaio_posix_aio_poll_inflight_queue(INT_MAX);
}
#endif							/* PGAIO_POSIX_AIO_USE_SIGWAIT */




/* Cross-process I/O completion queue support. */

#define PGAIO_POSIX_AIO_IPC_SIGNO SIGXCPU

static void
pgaio_posix_aio_ipc_init(void)
{
	pqsignal(PGAIO_POSIX_AIO_IPC_SIGNO, pgaio_posix_aio_ipc_handler);
}

static void
pgaio_posix_aio_ipc_init_context(PgAioPosixAioContext *context)
{
	pg_atomic_init_u32(&context->ipc_procno, INVALID_PROC_NUMBER);
}

static void
pgaio_posix_aio_ipc_drain_completion_queue(PgAioHandle *ioh)
{
	PgAioPosixAioContext *context = pgaio_posix_aio_get_context(ioh);
	int			backoff_ms = 10;

	Assert(LWLockHeldByMe(&context->completion_lock));
	Assert(context != pgaio_my_posix_aio_context);

	context->ipc_io_id = pgaio_io_get_id(ioh);

	for (;;)
	{
		uint32		old_proc_number;
		int			event;

		/* Raw result available? */
		if (ioh->result != -EINPROGRESS)
		{
			pgaio_io_process_completion(ioh, ioh->result);
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
			kill(GetPGProcByNumber(ioh->owner_procno)->pid,
				 PGAIO_POSIX_AIO_IPC_SIGNO);

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

static void
pgaio_posix_aio_ipc_begin_submit(void)
{
#ifdef PGAIO_POSIX_AIO_ASYNC_SIGNAL_SAFETY_BROKEN
	sigset_t	mask;

	/*
	 * glibc's aio_error() function doesn't appear to be async-sync-safe,
	 * since it acquires a mutex also held by aio_read() etc.  Block
	 * pgaio_posix_aio_ipc_handler() while submitting.
	 */
	sigemptyset(&mask);
	sigaddset(&mask, PGAIO_POSIX_AIO_IPC_SIGNO);
	sigprocmask(SIG_BLOCK, &mask, NULL);
#endif
}

static void
pgaio_posix_aio_ipc_end_submit(void)
{
#ifdef PGAIO_POSIX_AIO_ASYNC_SIGNAL_SAFETY_BROKEN
	sigset_t	mask;

	sigemptyset(&mask);
	sigaddset(&mask, PGAIO_POSIX_AIO_IPC_SIGNO);
	sigprocmask(SIG_UNBLOCK, &mask, NULL);
#endif
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
	int			id;
	PgAioHandle *ioh;

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

	id = context->ipc_io_id;
	ioh = pgaio_posix_aio_get_io_by_id(id);
	if (pgaio_posix_aio_drain_completion_queue(ioh, true) > 0)
		SetLatch(&GetPGProcByNumber(procno)->procLatch);

	errno = save_errno;
}

#endif							/* IOMETHOD_POSIX_AIO_ENABLED */
