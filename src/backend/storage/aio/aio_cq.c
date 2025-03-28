/*-------------------------------------------------------------------------
 *
 * aio_cq.c
 *    AIO - shared completion queue infrastructure for I/O methods
 *
 * Some I/O APIs only allow the process that started an I/O to consume
 * completion notifications from the kernel.  This module allows I/O methods
 * to insert completed-but-not-yet-processed handles into pre-owner queues.  A
 * .wait_one() implementation can then perform the completion step in any
 * process.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/aio_cq.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "utils/wait_event.h"

/*
 * A queue of IDs of unprocessed but completed handles owned by one backend.
 * The lock always protects the tail, because any backends might call
 * pgaio_cq_process().  Whether it also protects the head depends on the I/O
 * method; see pgaio_cq_insert() for details.
 */
typedef struct PgAioCompletionQueue
{
	LWLock		lock;
	uint32		tail;
	uint32		head;
	uint32		size;
	uint32	   *cqes;
} PgAioCompletionQueue;

/* Array of completion queues indexed by ioh->owner_procno. */
static PgAioCompletionQueue *pgaio_cq_table;

static int
pgaio_cq_entries(void)
{
	/* Spare extra entry because head == tail means empty. */
	Assert(io_max_concurrency > 0);
	return io_max_concurrency + 1;
}

static int
pgaio_cq_backends(void)
{
	return MaxBackends + NUM_AUXILIARY_PROCS;
}

size_t
pgaio_cq_shmem_size(void)
{
	return add_size(mul_size(sizeof(PgAioCompletionQueue),
							 pgaio_cq_backends()),
					mul_size(sizeof(uint32),
							 mul_size(pgaio_cq_backends(),
									  pgaio_cq_entries())));
}

void
pgaio_cq_shmem_init(bool first_time)
{
	int			backends;
	int			size;
	uint32	   *cqes;
	bool		found;

	pgaio_cq_table = (PgAioCompletionQueue *)
		ShmemInitStruct("AioCompetionQueues", pgaio_cq_shmem_size(), &found);

	if (found)
		return;

	backends = pgaio_cq_backends();
	size = pgaio_cq_entries();

	/* Space for entries (ie handle IDs) follows final queue. */
	cqes = (uint32 *) &pgaio_cq_table[backends];

	for (int i = 0; i < backends; i++)
	{
		LWLockInitialize(&pgaio_cq_table[i].lock, LWTRANCHE_AIO_IPC_COMPLETION);
		pgaio_cq_table[i].head = 0;
		pgaio_cq_table[i].tail = 0;
		pgaio_cq_table[i].size = size;
		pgaio_cq_table[i].cqes = cqes;
#ifdef USE_ASSERT_CHECKING
		for (int j = 0; j < size; j++)
			cqes[j] = 0xdead;
#endif
		cqes += size;
	}
}

/*
 * Prepare to submit a handle.  Called by a ->submit() function.
 */
void
pgaio_cq_prepare_submit(PgAioHandle *ioh)
{
	ioh->result = -EINPROGRESS;
}

/*
 * Check if a handle is still in progress.  Returns false after
 * pgaio_cq_insert() or pgaio_io_process_completion(), but the circumstances
 * under which the result might be racy depend on the I/O method.
 */
bool
pgaio_cq_in_progress(PgAioHandle *ioh)
{
	return ioh->result == -EINPROGRESS;
}

/*
 * Insert a raw completion result into its owner's shared completion queue.
 * Called by an I/O method to allow pgaio_cq_try_process_completion() to be
 * run by any backend.
 *
 * If lock is false, this is async-signal-safe and the I/O method must
 * serialize insertions for each per-backend queue.
 */
void
pgaio_cq_insert(PgAioHandle *ioh, int32 result, bool lock)
{
	PgAioCompletionQueue *cq = &pgaio_cq_table[pgaio_io_get_owner(ioh)];
	uint32		head;

	Assert(ioh->state == PGAIO_HS_SUBMITTED);
	Assert(lock || pgaio_io_get_owner(ioh) == MyProcNumber);
	Assert(!lock || CritSectionCount > 0);

	/* Store the result directly in the result member. */
	Assert(result != -EINPROGRESS);
	Assert(ioh->result == -EINPROGRESS);
	ioh->result = result;

	if (lock)
		LWLockAcquire(&cq->lock, LW_EXCLUSIVE);

	head = cq->head;
	cq->cqes[head] = pgaio_io_get_id(ioh);
	pg_write_barrier();
	cq->head = (head + 1) % cq->size;

	if (lock)
		LWLockRelease(&cq->lock);

	/*
	 * Wake one waiter using the special async-signal-safe wake function if we
	 * are potentially in a signal handler, and otherwise just use a regular
	 * broadcast so the I/O method can use the regular CV sleep API.
	 */
	if (!lock)
		ConditionVariableWakeOne(&ioh->cv);
	else
		ConditionVariableBroadcast(&ioh->cv);
}

/*
 * Process completions for some of the handles in the per-backend completion
 * queue of the backend that owns a given handle, and return how many were
 * processed.
 */
int
pgaio_cq_try_process_completion(PgAioHandle *ioh)
{
	PgAioCompletionQueue *cq = &pgaio_cq_table[pgaio_io_get_owner(ioh)];
	int			dequeued = 0;
	int32		cqes[32];
	uint32		tail;
	uint32		head;

	Assert(CritSectionCount > 0);

	/* Hold the lock only while copying cqes out. */
	LWLockAcquire(&cq->lock, LW_EXCLUSIVE);
	tail = cq->tail;
	pg_read_barrier();
	head = cq->head;
	pg_read_barrier();
	while (tail != head && dequeued < lengthof(cqes))
	{
		cqes[dequeued++] = cq->cqes[tail++];
		if (tail == cq->size)
			tail = 0;
	}
#ifdef USE_ASSERT_CHECKING
	{
		uint32		i = cq->tail;

		while (i != tail)
		{
			cq->cqes[i] = 0xdead;
			if (++i == cq->size)
				i = 0;
		}
	}
#endif
	cq->tail = tail;
	LWLockRelease(&cq->lock);

	for (int i = 0; i < dequeued; i++)
	{
		PgAioHandle *other_ioh = &pgaio_ctl->io_handles[cqes[i]];

		/* pgaio_cq_insert() already stored result. */
		pgaio_io_process_completion(other_ioh, other_ioh->result);
	}

	return dequeued;
}
