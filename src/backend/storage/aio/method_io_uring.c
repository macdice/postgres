/*-------------------------------------------------------------------------
 *
 * method_io_uring.c
 *    AIO - perform AIO using Linux' io_uring
 *
 * XXX Write me
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/method_io_uring.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_LIBURING

#include <liburing.h>

#include "pgstat.h"
#include "port/pg_iovec.h"
#include "storage/aio_internal.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"


/* Entry points for IoMethodOps. */
static size_t pgaio_uring_shmem_size(void);
static void pgaio_uring_shmem_init(bool first_time);
static void pgaio_uring_init_backend(void);

static int	pgaio_uring_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);
static void pgaio_uring_wait_one(PgAioHandle *ioh, uint64 ref_generation);

static void pgaio_uring_sq_from_io(PgAioHandle *ioh, struct io_uring_sqe *sqe);


const IoMethodOps pgaio_uring_ops = {
	.shmem_size = pgaio_uring_shmem_size,
	.shmem_init = pgaio_uring_shmem_init,
	.init_backend = pgaio_uring_init_backend,

	.submit = pgaio_uring_submit,
	.wait_one = pgaio_uring_wait_one,
};

typedef struct PgAioUringContext
{
	LWLock		completion_lock;

	struct io_uring io_uring_ring;
	/* XXX: probably worth padding to a cacheline boundary here */
} PgAioUringContext;


static PgAioUringContext *pgaio_uring_contexts;
static PgAioUringContext *pgaio_my_uring_context;

/* io_uring local state */
static struct io_uring local_ring;



static Size
pgaio_uring_context_shmem_size(void)
{
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS - MAX_IO_WORKERS;

	return mul_size(TotalProcs, sizeof(PgAioUringContext));
}

static size_t
pgaio_uring_shmem_size(void)
{
	return pgaio_uring_context_shmem_size();
}

static void
pgaio_uring_shmem_init(bool first_time)
{
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS - MAX_IO_WORKERS;
	bool		found;

	pgaio_uring_contexts = (PgAioUringContext *)
		ShmemInitStruct("AioUring", pgaio_uring_shmem_size(), &found);

	if (found)
		return;

	for (int contextno = 0; contextno < TotalProcs; contextno++)
	{
		PgAioUringContext *context = &pgaio_uring_contexts[contextno];
		int			ret;

		/*
		 * XXX: Probably worth sharing the WQ between the different rings,
		 * when supported by the kernel. Could also cause additional
		 * contention, I guess?
		 */
#if 0
		if (!AcquireExternalFD())
			elog(ERROR, "No external FD available");
#endif
		ret = io_uring_queue_init(io_max_concurrency, &context->io_uring_ring, 0);
		if (ret < 0)
			elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));

		LWLockInitialize(&context->completion_lock, LWTRANCHE_AIO_URING_COMPLETION);
	}
}

static void
pgaio_uring_init_backend(void)
{
	int			ret;

	pgaio_my_uring_context = &pgaio_uring_contexts[MyProcNumber];

	ret = io_uring_queue_init(32, &local_ring, 0);
	if (ret < 0)
		elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
}

static int
pgaio_uring_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	struct io_uring *uring_instance = &pgaio_my_uring_context->io_uring_ring;
	int			in_flight_before = dclist_count(&pgaio_my_backend->in_flight_ios);

	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];
		struct io_uring_sqe *sqe;

		sqe = io_uring_get_sqe(uring_instance);

		if (!sqe)
			elog(ERROR, "io_uring submission queue is unexpectedly full");

		pgaio_io_prepare_submit(ioh);
		pgaio_uring_sq_from_io(ioh, sqe);

		/*
		 * io_uring executes IO in process context if possible. That's
		 * generally good, as it reduces context switching. When performing a
		 * lot of buffered IO that means that copying between page cache and
		 * userspace memory happens in the foreground, as it can't be
		 * offloaded to DMA hardware as is possible when using direct IO. When
		 * executing a lot of buffered IO this causes io_uring to be slower
		 * than worker mode, as worker mode parallelizes the copying. io_uring
		 * can be told to offload work to worker threads instead.
		 *
		 * If an IO is buffered IO and we already have IOs in flight or
		 * multiple IOs are being submitted, we thus tell io_uring to execute
		 * the IO in the background. We don't do so for the first few IOs
		 * being submitted as executing in this process' context has lower
		 * latency.
		 */
		if (in_flight_before > 4 && (ioh->flags & PGAIO_HF_BUFFERED))
			io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);

		in_flight_before++;
	}

	while (true)
	{
		int			ret;

		pgstat_report_wait_start(WAIT_EVENT_AIO_IO_URING_SUBMIT);
		ret = io_uring_submit(uring_instance);
		pgstat_report_wait_end();

		if (ret == -EINTR)
		{
			pgaio_debug(DEBUG3,
						"aio method uring: submit EINTR, nios: %d",
						num_staged_ios);
			continue;
		}
		if (ret < 0)
			elog(PANIC, "failed: %d/%s",
				 ret, strerror(-ret));
		else if (ret != num_staged_ios)
		{
			/* likely unreachable, but if it is, we would need to re-submit */
			elog(PANIC, "submitted only %d of %d",
				 ret, num_staged_ios);
		}
		else
		{
			pgaio_debug(DEBUG4,
						"aio method uring: submitted %d IOs",
						num_staged_ios);
		}
		break;
	}

	return num_staged_ios;
}


#define PGAIO_MAX_LOCAL_COMPLETED_IO 32

static void
pgaio_uring_drain_locked(PgAioUringContext *context)
{
	int			ready;
	int			orig_ready;

	/*
	 * Don't drain more events than available right now. Otherwise it's
	 * plausible that one backend could get stuck, for a while, receiving CQEs
	 * without actually processing them.
	 */
	orig_ready = ready = io_uring_cq_ready(&context->io_uring_ring);

	while (ready > 0)
	{
		struct io_uring_cqe *cqes[PGAIO_MAX_LOCAL_COMPLETED_IO];
		uint32		ncqes;

		START_CRIT_SECTION();
		ncqes =
			io_uring_peek_batch_cqe(&context->io_uring_ring,
									cqes,
									Min(PGAIO_MAX_LOCAL_COMPLETED_IO, ready));
		Assert(ncqes <= ready);

		ready -= ncqes;

		for (int i = 0; i < ncqes; i++)
		{
			struct io_uring_cqe *cqe = cqes[i];
			PgAioHandle *ioh;

			ioh = io_uring_cqe_get_data(cqe);
			io_uring_cqe_seen(&context->io_uring_ring, cqe);

			pgaio_io_process_completion(ioh, cqe->res);
		}

		END_CRIT_SECTION();

		pgaio_debug(DEBUG3,
					"drained %d/%d, now expecting %d",
					ncqes, orig_ready, io_uring_cq_ready(&context->io_uring_ring));
	}
}

static void
pgaio_uring_wait_one(PgAioHandle *ioh, uint64 ref_generation)
{
	PgAioHandleState state;
	ProcNumber	owner_procno = ioh->owner_procno;
	PgAioUringContext *owner_context = &pgaio_uring_contexts[owner_procno];
	bool		expect_cqe;
	int			waited = 0;

	/*
	 * We ought to have a smarter locking scheme, nearly all the time the
	 * backend owning the ring will consume the completions, making the
	 * locking unnecessarily expensive.
	 */
	LWLockAcquire(&owner_context->completion_lock, LW_EXCLUSIVE);

	while (true)
	{
		pgaio_debug_io(DEBUG3, ioh,
					   "wait_one io_gen: %llu, ref_gen: %llu, cycle %d",
					   (long long unsigned) ref_generation,
					   (long long unsigned) ioh->generation,
					   waited);

		if (pgaio_io_was_recycled(ioh, ref_generation, &state) ||
			state != PGAIO_HS_SUBMITTED)
		{
			break;
		}
		else if (io_uring_cq_ready(&owner_context->io_uring_ring))
		{
			expect_cqe = true;
		}
		else
		{
			int			ret;
			struct io_uring_cqe *cqes;

			pgstat_report_wait_start(WAIT_EVENT_AIO_IO_URING_COMPLETION);
			ret = io_uring_wait_cqes(&owner_context->io_uring_ring, &cqes, 1, NULL, NULL);
			pgstat_report_wait_end();

			if (ret == -EINTR)
			{
				continue;
			}
			else if (ret != 0)
			{
				elog(PANIC, "unexpected: %d/%s: %m", ret, strerror(-ret));
			}
			else
			{
				Assert(cqes != NULL);
				expect_cqe = true;
				waited++;
			}
		}

		if (expect_cqe)
		{
			pgaio_uring_drain_locked(owner_context);
		}
	}

	LWLockRelease(&owner_context->completion_lock);

	pgaio_debug(DEBUG3,
				"wait_one with %d sleeps",
				waited);
}

static void
pgaio_uring_sq_from_io(PgAioHandle *ioh, struct io_uring_sqe *sqe)
{
	struct iovec *iov;

	switch (ioh->op)
	{
		case PGAIO_OP_READV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
			if (ioh->op_data.read.iov_length == 1)
			{
				io_uring_prep_read(sqe,
								   ioh->op_data.read.fd,
								   iov->iov_base,
								   iov->iov_len,
								   ioh->op_data.read.offset);
			}
			else
			{
				io_uring_prep_readv(sqe,
									ioh->op_data.read.fd,
									iov,
									ioh->op_data.read.iov_length,
									ioh->op_data.read.offset);

			}
			break;

		case PGAIO_OP_WRITEV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
			if (ioh->op_data.write.iov_length == 1)
			{
				io_uring_prep_write(sqe,
									ioh->op_data.write.fd,
									iov->iov_base,
									iov->iov_len,
									ioh->op_data.write.offset);
			}
			else
			{
				io_uring_prep_writev(sqe,
									 ioh->op_data.write.fd,
									 iov,
									 ioh->op_data.write.iov_length,
									 ioh->op_data.write.offset);
			}
			break;

		case PGAIO_OP_INVALID:
			elog(ERROR, "trying to prepare invalid IO operation for execution");
	}

	io_uring_sqe_set_data(sqe, ioh);
}

#endif							/* USE_LIBURING */
