/*-------------------------------------------------------------------------
 *
 * method_posix_aio.c
 *    AIO - perform AIO using POSIX AIO (FreeBSD only)
 *
 * POSIX AIO is less portable than it sounds, due to incomplete
 * implementations, sketchy user space emulations and incompatible extensions
 * for things we really need for database work.  This module works on FreeBSD
 * only for now: it supports vectored I/O as needed for smgr, and consuming
 * events with kqueue fits the pgaio model better than the standard's basic
 * options.
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
#include <sys/event.h>

#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "utils/wait_event.h"

/* Entry points for IoMethodOps. */
static void pgaio_posix_aio_init_backend(void);
static int	pgaio_posix_aio_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);
static void pgaio_posix_aio_wait_one(PgAioHandle *ioh, uint64 ref_generation);

const IoMethodOps pgaio_posix_aio_ops = {
	.shmem_size = pgaio_cq_shmem_size,
	.shmem_init = pgaio_cq_shmem_init,
	.init_backend = pgaio_posix_aio_init_backend,

	.submit = pgaio_posix_aio_submit,
	.wait_one = pgaio_posix_aio_wait_one,
};

static struct aiocb *pgaio_posix_aio_aiocbs;
static int	pgaio_posix_aio_naiocbs;
static int	pgaio_posix_aio_kqueue;
static volatile sig_atomic_t pgaio_posix_aio_draining;

static void pgaio_posix_aio_prepare_submit(PgAioHandle *ioh, struct aiocb *aiocb);
static int	pgaio_posix_aio_drain_local(bool wait, bool ipc);
static void pgaio_posix_aio_drain_remote(PgAioHandle *ioh);
static void pgaio_posix_aio_drain_remote_handler(SIGNAL_ARGS);

static void
pgaio_posix_aio_init_backend(void)
{
	/* Backend-private aiocb objects. */
	pgaio_posix_aio_naiocbs = dclist_count(&pgaio_my_backend->idle_ios);
	pgaio_posix_aio_aiocbs = palloc(sizeof(struct aiocb) *
									pgaio_posix_aio_naiocbs);

	/*
	 * Dedicated kqueue for disk I/O, separate from the one used for sockets
	 * and latches.  Since events are sometimes given to other backends for
	 * processing, it doesn't make sense to mix them.
	 */
	pgaio_posix_aio_kqueue = kqueue();
	if (pgaio_posix_aio_kqueue < 0)
		elog(ERROR, "kqueue failed: %m");

	/* Support for handing raw completions to other backends on request. */
	pqsignal(SIGIO, pgaio_posix_aio_drain_remote_handler);
}

static struct aiocb *
pgaio_posix_aio_aiocb_for_io(PgAioHandle *ioh)
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

static PgAioHandle *
pgaio_posix_aio_io_for_id(int32 id)
{
	return &pgaio_ctl->io_handles[id];
}

static PgAioHandle *
pgaio_posix_aio_io_for_aiocb(struct aiocb *aiocb)
{
	return pgaio_posix_aio_io_for_id(pgaio_my_backend->io_handle_off +
									 (aiocb - pgaio_posix_aio_aiocbs));
}

static int
pgaio_posix_aio_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	struct aiocb *aiocbs[PGAIO_SUBMIT_BATCH_SIZE];

	Assert(num_staged_ios <= lengthof(aiocbs));

	/* Set up the control blocks. */
	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];

		if ((aiocbs[i] = pgaio_posix_aio_aiocb_for_io(ioh)) == NULL)
			elog(ERROR, "cannot submit I/O for handle %u",
				 pgaio_io_get_id(ioh));
		pgaio_io_prepare_submit(ioh);
		pgaio_cq_prepare_submit(ioh);
		pgaio_posix_aio_prepare_submit(ioh, aiocbs[i]);
	}

	if (num_staged_ios == 1)
	{
		int			ret;

		/*
		 * The single-operation versions might save a few cycles compared to
		 * lio_listio() with only one operation, and are a little easier to
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
			case LIO_READV:
				ret = aio_readv(aiocbs[0]);
				break;
			case LIO_WRITEV:
				ret = aio_writev(aiocbs[0]);
				break;
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


static void
pgaio_posix_aio_wait_one(PgAioHandle *ioh, uint64 ref_generation)
{
	ProcNumber	owner_procno = pgaio_io_get_owner(ioh);
	bool		am_owner = owner_procno == MyProcNumber;
	int			processed;
	PgAioHandleState state;

	/*
	 * Process the owner's completion queue.  Usually this backend is the
	 * owner and it's usually empty.  It can't fill up again while we hold off
	 * IPC, but we also have to process whatever is in there as it may be what
	 * we are waiting for.
	 */
	START_CRIT_SECTION();
	pgaio_posix_aio_draining = true;
	processed = pgaio_cq_try_process_completion(ioh);
	if (am_owner && pgaio_cq_in_progress(ioh))
		processed += pgaio_posix_aio_drain_local(processed == 0, false);
	pgaio_posix_aio_draining = false;
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
		if (pgaio_cq_in_progress(ioh))
			pgaio_posix_aio_drain_remote(ioh);
		else if (pgaio_cq_try_process_completion(ioh) > 0)
			break;

		/*
		 * Wait for the owner to insert it into the shared completion queue,
		 * or for any backend to complete it.  As long as it is still appears
		 * to be actually running, we periodically ask it to check.
		 */
		ConditionVariableOrLatchSleep(&ioh->cv,
									  pgaio_cq_in_progress(ioh) ? 100 : -1,
									  WAIT_EVENT_AIO_POSIX_AIO_COMPLETION);
	}
	ConditionVariableCancelSleep();
	END_CRIT_SECTION();
}

static void
pgaio_posix_aio_prepare_submit(PgAioHandle *ioh, struct aiocb *aiocb)
{
	struct iovec *iov;

	memset(aiocb, 0, sizeof(*aiocb));
	aiocb->aio_sigevent.sigev_notify = SIGEV_KEVENT;
	aiocb->aio_sigevent.sigev_notify_kqueue = pgaio_posix_aio_kqueue;

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

/*
 * Drain completion events from the kernel.  Optionally process completions
 * immediately, but otherwise insert them into the shared completion queue.
 */
static int
pgaio_posix_aio_drain_local(bool wait, bool process)
{
	struct timespec nowait = {0};
	struct kevent events[32];
	int			rc;

	if (process)
	{
		Assert(CritSectionCount > 0);
		Assert(pgaio_posix_aio_draining);
	}

retry:
	rc = kevent(pgaio_posix_aio_kqueue,
				NULL, 0,
				events, lengthof(events),
				wait ? NULL : &nowait);
	if (rc < 0)
	{
		if (errno == EINTR)
			goto retry;
		elog(PANIC, "kevent failed: %m");
	}

	for (int i = 0; i < rc; i++)
	{
		struct aiocb *aiocb;
		PgAioHandle *ioh;
		int			error;
		int32		result;

		if (events[i].filter != EVFILT_AIO)
			elog(PANIC,
				 "unexpected kevent filter %hd in I/O kqueue",
				 events[i].filter);

		aiocb = (struct aiocb *) events[i].ident;
		error = events[i].data;
		result = aio_return(aiocb);
		if (error != 0)
			result = -result;
		ioh = pgaio_posix_aio_io_for_aiocb(aiocb);

		if (process)
			pgaio_io_process_completion(ioh, result);
		else
			pgaio_cq_insert(ioh, result, false);
	}

	return rc;
}

/*
 * Ask the owner of a handle to insert any new completions into the shared
 * completion queue.
 */
static void
pgaio_posix_aio_drain_remote(PgAioHandle *ioh)
{
	kill(GetPGProcByNumber(ioh->owner_procno)->pid, SIGIO);
}

/*
 * Drain on request, unless we're already draining.
 */
static void
pgaio_posix_aio_drain_remote_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	if (!pgaio_posix_aio_draining)
		pgaio_posix_aio_drain_local(false, false);

	errno = save_errno;
}

#endif							/* IOMETHOD_POSIX_AIO_ENABLED */
