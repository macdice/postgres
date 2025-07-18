/*-------------------------------------------------------------------------
 *
 * method_posix_aio.c
 *    AIO - use POSIX AIO with realtime signals for completion notification
 *
 * This uses standard POSIX facilities for I/O and works on all supported
 * Unixen except macOS, but is currently only useful on FreeBSD.
 *
 * Much better APIs exist for consuming completions synchronously in batches
 * from a kernel queue, but they don't support cross-process
 * submit()/wait_one().  That is required by the pgaio architecture for
 * deadlock-free progress.  Signals offer a very simple way around that problem
 * while PostgreSQL remains multi-process.
 *
 * Portability notes:
 *
 * 1.  Working fully: FreeBSD.  Uses vectored I/O extensions as required for
 *     good buffer pool I/O performance.
 *
 * 2.  Working without vectored I/O: NetBSD (also AIX and HP-UX, which were
 *     supported platforms when this I/O method was developed).  Vectored
 *     operations are silently truncated at the first vector, eg only
 *     contiguous buffers can be read in at the same time.  In later work,
 *     higher levels performing I/O combining could be made aware of this to
 *     avoid retries, something that is also needed for Windows' buffered files.
 *
 * 3.  Working without vectored I/O, but emulated by libc with threads:
 *     Linux/glibc, Linux/musl, illumos, Solaris.  This is not a recommended
 *     configuration on those platforms.  It compiles and runs,, and is supported only to allow
 *	   maintenance of this module, ie to keep
 *     availability of better options, but it is useful that it compiles and
 *     works, for code maintenance purposes.  Consider it a developer feature.
 *
 * 4.  Not working: macOS.  macOS doesn't implement realtime signals.  (Could
 *     be made to work using aio_suspend() in a hypothetical multi-threaded
 *     future or on operations that are guaranteed not to cross process
 *     boundaries.)
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

#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/condition_variable.h"
#include "storage/eventflag.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "utils/wait_event.h"

/* Realtime signal for completion notifications from the kernel. */
#define PGAIO_POSIX_AIO_NOTIFICATION_SIGNO SIGRTMIN

/* Regular signal for requests to enable the above from other backends. */
#define PGAIO_POSIX_AIO_NOTIFICATION_CONTROL_SIGNO SIGIO

/* Does this platform have vectored AIO operations? */
#if defined(LIO_READV) && defined(LIO_WRITEV)
#define PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
#endif

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

typedef struct PgAioPosixAioBackend
{
	/* Number of backends waiting for an IO submitted by this backend. */
	pg_atomic_uint32 waiters;
} PgAioPosixAioBackend;

static EventFlag *pgaio_posix_aio_eventflags;
static PgAioPosixAioBackend *pgaio_posix_aio_backends;
static PgAioPosixAioBackend *my_pgaio_posix_aio_backend;
static struct aiocb *pgaio_posix_aio_aiocbs;
static int	pgaio_posix_aio_naiocbs;

static void pgaio_posix_aio_prepare_submit(PgAioHandle *ioh, struct aiocb *aiocb);
static PgAioHandle *pgaio_posix_aio_process_notification(siginfo_t *notification);
static void pgaio_posix_aio_notification_handler(int signo,
												 siginfo_t *notification,
												 void *context);
static void pgaio_posix_aio_notification_control_handler(int signo);

static size_t
pgaio_posix_aio_shmem_size(void)
{
	int io_handle_count;
	int backends;

	backends = MaxBackends + NUM_AUXILIARY_PROCS;
	io_handle_count = io_max_concurrency * backends;

	return add_size(mul_size(sizeof(PgAioPosixAioBackend), backends),
					mul_size(sizeof(EventFlag), io_handle_count));
}

static void
pgaio_posix_aio_shmem_init(bool first_time)
{
	bool		found;

	/* Set up the per-backend waiter count. */
	pgaio_posix_aio_backends = (PgAioPosixAioBackend *)
		ShmemInitStruct("AioPosixAioBackend", pgaio_posix_aio_shmem_size(), &found);
	if (!found)
		for (int i = 0; i < MaxBackends + NUM_AUXILIARY_PROCS; i++)
			pg_atomic_init_u32(&pgaio_posix_aio_backends[i].waiters, 0);
	my_pgaio_posix_aio_backend = &pgaio_posix_aio_backends[MyProcNumber];

	/* Allocate the per-IO EventFlag.  Initialized on use. */
	pgaio_posix_aio_eventflags = (EventFlag *)
		ShmemInitStruct("AioPosixAioEventFlag", pgaio_posix_aio_shmem_size(), &found);
}

static sigset_t pgaio_posix_aio_notification_sigset;

static void
pgaio_posix_aio_init_backend(void)
{
	struct sigaction sa;

	/* Set up backend-private aiocb objects for this backend's handles. */
	pgaio_posix_aio_naiocbs = dclist_count(&pgaio_my_backend->idle_ios);
	pgaio_posix_aio_aiocbs = palloc(sizeof(struct aiocb) *
									pgaio_posix_aio_naiocbs);

	/* Realtime signal handler for asynchonous completion notifications. */
	sa.sa_sigaction = pgaio_posix_aio_notification_handler;
	sa.sa_flags = SA_RESTART | SA_SIGINFO;
	if (sigaction(PGAIO_POSIX_AIO_NOTIFICATION_SIGNO, &sa, NULL) < 0)
		elog(ERROR, "could not install I/O completion notification handler");

	/* Regular signal handler for requests to unblock the above. */
	sa.sa_handler = pgaio_posix_aio_notification_control_handler;
	sa.sa_flags = SA_RESTART | SA_SIGINFO;
	if (sigaction(PGAIO_POSIX_AIO_NOTIFICATION_CONTROL_SIGNO, &sa, NULL) < 0)
		elog(ERROR, "could not install I/O completion notification control handler");

	/*
	 * The notification handler starts out blocked and notifications are
	 * consumed synchronously with sigwaitinfo().  It will be unblocked while
	 * other backends are actively waiting for I/Os submitted by this backend,
	 * to guarantee progress at the cost of a bit of extra interrupt traffic.
	 */
	sigemptyset(&pgaio_posix_aio_notification_sigset);
	sigaddset(&pgaio_posix_aio_notification_sigset,
			  PGAIO_POSIX_AIO_NOTIFICATION_SIGNO);
	sigprocmask(SIG_BLOCK, &pgaio_posix_aio_notification_sigset, NULL);
}

static struct aiocb *
pgaio_posix_aio_get_aiocb(PgAioHandle *ioh)
{
	int			id;

	Assert(ioh->owner_procno == MyProcNumber);
	id = pgaio_io_get_id(ioh);

	if (ioh->owner_procno != MyProcNumber ||
		id < pgaio_my_backend->io_handle_off ||
		id >= pgaio_my_backend->io_handle_off + pgaio_posix_aio_naiocbs)
		return NULL;

	return &pgaio_posix_aio_aiocbs[id - pgaio_my_backend->io_handle_off];
}

static EventFlag *
pgaio_posix_aio_get_eventflag(PgAioHandle *ioh)
{
	return &pgaio_posix_aio_eventflags[pgaio_io_get_id(ioh)];
}

static PgAioHandle *
pgaio_posix_aio_get_io_by_id(int32 id)
{
	return &pgaio_ctl->io_handles[id];
}

static int
pgaio_posix_aio_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	struct aiocb *aiocbs[PGAIO_SUBMIT_BATCH_SIZE];

	Assert(num_staged_ios <= lengthof(aiocbs));

	/* Set up the control blocks and event flags. */
	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];

		if ((aiocbs[i] = pgaio_posix_aio_get_aiocb(ioh)) == NULL)
			elog(ERROR, "cannot submit I/O for handle %u",
				 pgaio_io_get_id(ioh));

		/*
		 * This ordering is important, because it ensures the eventflag is
		 * initialized before PGAIO_HS_SUBMITTED is visible to any other
		 * backend.
		 */
		pgaio_posix_aio_prepare_submit(ioh, aiocbs[i]);
		pgaio_io_prepare_submit(ioh);
	}

	if (num_staged_ios == 1)
	{
		int			ret;

		/*
		 * The single-operation versions might save a few cycles compared to
		 * lio_listio() with a batch size of one, and are a little easier to
		 * trace while debugging...
		 */
		switch (aiocbs[0]->aio_lio_opcode)
		{
			case LIO_READ:
				ret = aio_read(aiocbs[0]);
				break;
			case LIO_WRITE:
				ret = aio_write(aiocbs[0]);
				break;
#ifdef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
			case LIO_READV:
				ret = aio_readv(aiocbs[0]);
				break;
			case LIO_WRITEV:
				ret = aio_writev(aiocbs[0]);
				break;
#endif
			default:
				errno = ENOSYS;
				ret = -1;
		}
		if (ret < 0)
			pgaio_io_process_completion(staged_ios[0], -errno);
	}
	else if (lio_listio(LIO_NOWAIT, aiocbs, num_staged_ios, NULL) < 0)
	{
		int			lio_errno = errno;

		if (lio_errno == EAGAIN || lio_errno == EINTR || lio_errno == EIO)
		{
			/* Per POSIX, these three errors require per-IO polling. */
			for (int i = 0; i < num_staged_ios; ++i)
			{
				int			io_errno = aio_error(aiocbs[i]);

				if (io_errno == EINPROGRESS || io_errno == 0)
					continue;	/* submitted or done, OK */
				if (io_errno < 0)	/* aio_error() failed? */
					io_errno = errno;
				if (io_errno == EINVAL) /* not even recognized? */
					io_errno = lio_errno;
				pgaio_io_process_completion(staged_ios[i], -io_errno);
			}
		}
		else
		{
			for (int i = 0; i < num_staged_ios; ++i)
				pgaio_io_process_completion(staged_ios[i], -lio_errno);
		}
	}

	return num_staged_ios;
}

static volatile sig_atomic_t pgaio_posix_aio_synchronous_notifications;

static void
pgaio_posix_aio_begin_synchronous_notifications(void)
{
	/*
	 * After this store, the asynchronous handler can't be unblocked by
	 * pgaio_posix_aio_notification_control_handler().
	 */
	pgaio_posix_aio_synchronous_notifications = true;

	/*
	 * If pgaio_posix_aio_notification_handler() was unblocked, block it.
	 */
	if (pg_atomic_read_u32(&my_pgaio_posix_aio_backend->waiters) > 0)
		sigprocmask(SIG_BLOCK, &pgaio_posix_aio_notification_sigset, NULL);
}

static void
pgaio_posix_aio_end_synchronous_notifications(void)
{
	/*
	 * After this store, pgaio_posix_aio_notification_control_handler() is free
	 * to unblock pgaio_posix_aio_synchronous_notifications().
	 */
	pgaio_posix_aio_synchronous_notifications = false;

	/*
	 * Unblock asynchronous notifications if there are any other backends
	 * waiting for an IO subnitted by this backend.
	 */
	if (pg_atomic_read_u32(&my_pgaio_posix_aio_backend->waiters) > 0)
{
		sigprocmask(SIG_UNBLOCK, &pgaio_posix_aio_notification_sigset, NULL);
fprintf(stderr, "ublocking per request\b");
}
}

static void
pgaio_posix_aio_notification_control_handler(int signo)
{
	/*
	 * Block or unblock the asynchronous notificaiton handler depending on
	 * whether any other backend is interested.  Deferrred if synchronous
	 * processing is currently underway (see above).
	 */
	if (!pgaio_posix_aio_synchronous_notifications)
	{
		if (pg_atomic_read_u32(&my_pgaio_posix_aio_backend->waiters) > 0)
			sigprocmask(SIG_UNBLOCK, &pgaio_posix_aio_notification_sigset, NULL);
		else
			sigprocmask(SIG_BLOCK, &pgaio_posix_aio_notification_sigset, NULL);
	}
}

static void
pgaio_posix_aio_begin_wait(PgAioHandle *ioh)
{
	int owner = pgaio_io_get_owner(ioh);

	/*
	 * If we're the first waiter, tell the submitter to start processing
	 * completion notifications asynchronously.
	 */
	if (pg_atomic_fetch_add_u32(&pgaio_posix_aio_backends[owner].waiters, 1) == 0)
		kill(SIGIO, GetPGProcByNumber(pgaio_io_get_owner(ioh))->pid);
}

static void
pgaio_posix_aio_end_wait(PgAioHandle *ioh)
{
	int owner = pgaio_io_get_owner(ioh);

	/*
	 * If we were the last waiter, tell the submitter to stop asynchronous
	 * completion notifications, so it can use the more efficient synchronous
	 * notification path.
	 */
	if (pg_atomic_fetch_sub_u32(&pgaio_posix_aio_backends[owner].waiters, 1) == 0)
		kill(SIGIO, GetPGProcByNumber(pgaio_io_get_owner(ioh))->pid);
}

static void
pgaio_posix_aio_wait_one(PgAioHandle *ioh, uint64 ref_generation)
{
	EventFlag *event = pgaio_posix_aio_get_eventflag(ioh);

	/*
	 * In the common case of waiting for an IO that this backend submitted, try
	 * to consume the completion notification synchronously.
	 */
	if (pgaio_io_get_owner(ioh) == MyProcNumber &&
		eventflag_begin_wait_exclusive(event))
	{
		START_CRIT_SECTION();

elog(LOG, "XXX i am the submitter and waiter");
		pgaio_posix_aio_begin_synchronous_notifications();
		if (eventflag_has_fired(event))
		{
			/*
			 * Asynchronous notifications are blocked now, but a notification
			 * has arrived concurrently, so process it immediately.
			 */
elog(LOG, "XXX has fired");
			Assert(ioh->result != -EINPROGRESS);
			pgaio_io_process_completion(ioh, ioh->result);
		}
		else
		{
			PgAioHandle *other_ioh;

elog(LOG, "XXX has not fired, so i will wait synchronously");
			/*
			 * Consume completions synchronously until we find the one we're
			 * waiting for.
			 */
			do
			{
				siginfo_t notification;

				if (sigwaitinfo(&pgaio_posix_aio_notification_sigset, &notification) < 0)
					elog(ERROR, "could not wait for queued signal: %m");
				other_ioh = pgaio_posix_aio_process_notification(&notification);
				if (!other_ioh)
					continue;
				Assert(ioh->result != -EINPROGRESS);
				pgaio_io_process_completion(other_ioh, other_ioh->result);
			} while (other_ioh != ioh);
		}
		pgaio_posix_aio_end_synchronous_notifications();

		END_CRIT_SECTION();

		return;
	}

	/*
	 * Otherwise ioh was submitted by another backend already has an exclusive
	 * waiter.
	 */
	START_CRIT_SECTION();

	pgaio_posix_aio_begin_wait(ioh);
	if (eventflag_wait_exclusive(pgaio_posix_aio_get_eventflag(ioh),
								 WAIT_EVENT_AIO_POSIX_AIO_EXECUTION))
	{
		pgaio_posix_aio_end_wait(ioh);

		/*
		 * The event has fired, either before we got here or after a sleep,
		 * and this backend has won the right to process completions.
		 */
		Assert(ioh->result != -EINPROGRESS);
		pgaio_io_process_completion(ioh, ioh->result);
	}
	else
	{
		PgAioHandleState state;

		/*
		 * Another backend is dealing with it.  Use the standard condition
		 * variable to wait for it to process completions.
		 */
		pgaio_posix_aio_end_wait(ioh);
		while (!pgaio_io_was_recycled(ioh, ref_generation, &state) &&
			   state == PGAIO_HS_SUBMITTED)
			ConditionVariableSleep(&ioh->cv, WAIT_EVENT_AIO_IO_COMPLETION);
		ConditionVariableCancelSleep();
	}
	END_CRIT_SECTION();
}

static void
pgaio_posix_aio_prepare_submit(PgAioHandle *ioh, struct aiocb *aiocb)
{
	struct iovec *iov;

	/* Used for assertions only. */
	ioh->result = -EINPROGRESS;

	/* This EventFlag will fired when the result is available. */
	eventflag_init(pgaio_posix_aio_get_eventflag(ioh));

	/* Ask for a realtime signal carrying the ID as payload. */
	memset(aiocb, 0, sizeof(*aiocb));
	aiocb->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
	aiocb->aio_sigevent.sigev_signo = PGAIO_POSIX_AIO_NOTIFICATION_SIGNO;
	aiocb->aio_sigevent.sigev_value.sival_int = pgaio_io_get_id(ioh);

	switch (ioh->op)
	{
		case PGAIO_OP_READV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
#ifndef PGAIO_POSIX_AIO_HAVE_VECTORED_OPS
			/* Force short read. */
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
			/* Force short write. */
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

/*
 * Process a completion notification from the kernel.
 */
static PgAioHandle *
pgaio_posix_aio_process_notification(siginfo_t *notification)
{
	int			result;
	PgAioHandle *ioh;

	if (notification->si_code != SI_ASYNCIO)
		return NULL;

	/* Find the referenced I/O handle. */
	ioh = pgaio_posix_aio_get_io_by_id(notification->si_value.sival_int);

	/*
	 * Retrieve the non-error result.
	 *
	 * Portability note:  POSIX requires aio_return() to be async-signal-safe.
	 * Though the glibc and musl implementations might not meet that
	 * requirement for some aio_XXX() functions due to dubious use of mutexes,
	 * aio_return() is not one of them: it simply returns a value from the
	 * struct.
	 */
	result = aio_return(pgaio_posix_aio_get_aiocb(ioh));
	Assert(ioh->state == PGAIO_HS_SUBMITTED);
	Assert(ioh->result == -EINPROGRESS);

	/* Replace with negative errno if the operation failed. */
	if (notification->si_errno != 0)
		result = -notification->si_errno;
	ioh->result = result;

	return ioh;
}

/*
 * Handle an asynchronous completion notification from the kernel.
 */
static void
pgaio_posix_aio_notification_handler(int signo, siginfo_t *notification,
									 void *context)
{
	int			save_errno = errno;
	PgAioHandle *ioh;

	ioh = pgaio_posix_aio_process_notification(notification);

	/*
	 * If a backend is waiting for the result, wake it up.  Has full barrier
	 * semantics, so the waiter sees ioh->result.
	 */
	if (ioh)
{
fprintf(stderr, "XXX fire!\n");
		eventflag_fire(pgaio_posix_aio_get_eventflag(ioh));
}

	errno = save_errno;
}

#endif							/* IOMETHOD_POSIX_AIO_ENABLED */
