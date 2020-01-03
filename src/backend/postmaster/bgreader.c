/*-------------------------------------------------------------------------
 *
 * bgreader.c
 *
 * Background reader processes perform prefetching work requested by other
 * processes through a shared-memory queue, by calling PrefetchBuffer().
 *
 * We try to reach a steady number of background reader processes by using
 * an idle timeout, and a maximum number.
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/bgreader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/relpath.h"
#include "datatype/timestamp.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgreader.h"
#include "postmaster/bgworker.h"
#include "storage/block.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/relfilenode.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

/* GUCs */
int			max_background_readers = 0;
int			max_background_reader_queue_depth = 10;
int			background_reader_idle_timeout = 1000;
int			background_reader_launch_delay = 1000;

typedef struct BackgroundReaderRequest
{
	RelFileNode		rnode;
	ForkNumber		forkNum;
	BlockNumber		blockNum;

#if 0
	Buffer			buffer;
	int			   *error;
	pid_t			completion_notify_pid;
#endif
} BackgroundReaderRequest;

/*
 * Shared state for background reader subsystem.  This is protected by
 * BackgroundReaderLock.  If that proves to be too contended, we could have N
 * separate queues with separate locks, and hash requests to figure out where
 * to put them.  For now, keep it simple and use a single lock.
 */
typedef struct BackgroundReaderShared
{
	ConditionVariable queue_not_empty;
	ConditionVariable queue_not_full;
	TimestampTz		next_launch_time;
	uint32			workers_launched;
	uint32			workers_terminated;
	int				head;
	int				tail;
	int				size;
	BackgroundReaderRequest queue[FLEXIBLE_ARRAY_MEMBER];
} BackgroundReaderShared;

static BackgroundReaderShared *Shared;

static void
HandleBackgroundReaderRequest(BackgroundReaderRequest *request)
{
	SMgrRelation	reln;
	char			dummy_buffer[BLCKSZ];
	char		   *destination;

#if 0
	Assert(*request->error == EINPROGRESS);
	if (request->buffer != InvalidBuffer)
		destination = (char *) BufferGetPage(request->buffer);
	else
#endif
		destination = dummy_buffer;

	PG_TRY();
	{
elog(LOG, "worker reading block %u", request->blockNum);
		reln = smgropen(request->rnode, InvalidBackendId);
		smgrread(reln, request->forkNum, request->blockNum, destination);
#if 0
		*request->error = 0;
#endif
	}
	PG_FINALLY();
	{
#if 0
		if (*request->error == EINPROGRESS)
			*request->error = EIO;
		if (request->completion_notify_pid != InvalidPid)
			kill(request->completion_notify_pid, SIGIO);
#endif
	}
	PG_END_TRY();
}

void
BackgroundReaderMain(Datum arg)
{
	BackgroundReaderRequest request;
	bool		done = false;
	bool		found;
	bool		was_full;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* We connect, so we can appear in pg_stat_activity. */
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	while (!done)
	{
		/* Can we consume a request? */
		LWLockAcquire(BackgroundReaderLock, LW_EXCLUSIVE);
		if (Shared->tail != Shared->head)
		{
			found = true;
			was_full = (Shared->head + 1) % Shared->size == Shared->tail;
			request = Shared->queue[Shared->tail];
			Shared->tail = (Shared->tail + 1) % Shared->size;
		}
		else
		{
			found = false;
			was_full = false;
		}
		LWLockRelease(BackgroundReaderLock);

		if (found)
		{
			if (was_full)
				ConditionVariableBroadcast(&Shared->queue_not_full);
			HandleBackgroundReaderRequest(&request);
		}
		else
			done = ConditionVariableTimedSleep(&Shared->queue_not_empty,
											   background_reader_idle_timeout,
											   WAIT_EVENT_BGREADER_MAIN);
	}
}

/*
 * Return the number of workers currently launching or running.
 */
static uint32
CurrentBackgroundReaders(void)
{
	Assert(LWLockHeldByMe(BackgroundReaderLock));

	/*
	 * We read workers_terminated without synchronization, because it's
	 * written to by the postmaster which can't use locking.  If we see a
	 * stale value it doesn't matter, we'll just be slightly too conservative
	 * about when we can launch new workers.
	 */
	return Shared->workers_launched - Shared->workers_terminated;
}

/*
 * Return false if there are too many readers launching or running already.
 * Otherwise return true, and the reader must call LaunchBackgroundReader().
 */
static bool
PrepareToLaunchBackgroundReader(void)
{
	Assert(LWLockHeldByMe(BackgroundReaderLock));

	/*
	 * We read workers_terminated without synchronization, because it's
	 * written to by the postmaster which can't use locking.  If we see a
	 * stale value it doesn't matter, we'll just be slightly too conservative
	 * about when we can launch new workers.
	 */
	if (CurrentBackgroundReaders() >= max_background_readers)
		return false;

	/*
	 * We've decided to launch.  The caller must call
	 * LaunchBackgroundReader().
	 */
	++Shared->workers_launched;

	return true;
}
static void
LaunchBackgroundReader(void)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;

	Assert(!LWLockHeldByMe(BackgroundReaderLock));

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "BackgroundReaderMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "background reader");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "background reader");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = Int32GetDatum(0);
	bgw.bgw_terminate_count = &Shared->workers_terminated;

	/* Try to launch the worker. */
	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		/* Undo -- we didn't launch anything. */
		LWLockAcquire(BackgroundReaderLock, LW_EXCLUSIVE);
		--Shared->workers_launched;
		LWLockRelease(BackgroundReaderLock);

		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase max_worker_processes or decrease max_background_readers")));
	}
}

/*
 * Enqueue a request for a block to be read, try to wake an existing worker,
 * and launch a new worker if that is appropriate.  If the queue is full, wait
 * unless 'nowait' is true.  Return true if the request was enqueued.
 */
bool
EnqueueBackgroundReaderRequest(RelFileNode rnode,
							   ForkNumber forkNum,
							   BlockNumber blockNum,
#if 0
							   Buffer buffer,
							   int *error,
							   pid_t completion_notify_pid,
#endif
							   bool nowait)
{
	BackgroundReaderRequest request;
	TimestampTz next_launch_time;
	int			new_head;
	bool		need_more_workers = false;

	request.rnode = rnode;
	request.forkNum = forkNum;
	request.blockNum = blockNum;
#if 0
	request.buffer = buffer
	request.error = error;
	request.compliation_notify_pid;
#endif

 retry:
	LWLockAcquire(BackgroundReaderLock, LW_EXCLUSIVE);

	/* Is the queue already full? */
	new_head = (Shared->head + 1) % Shared->size;
	if (new_head == Shared->tail)
	{
		bool launch;

		/*
		 * If there are no workers running because they exited on error, then
		 * we'd better try launching some more to drain the queue.
		 */
		launch = PrepareToLaunchBackgroundReader();

		if (!nowait)
			ConditionVariablePrepareToSleep(&Shared->queue_not_full);
		LWLockRelease(BackgroundReaderLock);

		if (launch)
			LaunchBackgroundReader();

		if (nowait)
			return false;

		ConditionVariableTimedSleep(&Shared->queue_not_full,
									background_reader_launch_delay,
									WAIT_EVENT_BGREADER_ENQUEUE);
		ConditionVariableCancelSleep();
		goto retry;
	}

	/*
	 * If there are no workers, or there was already something waiting in the
	 * queue before us, we'll consider adding a new worker.
	 */
	if (CurrentBackgroundReaders() == 0 ||
		(Shared->head != Shared->tail &&
		 CurrentBackgroundReaders() < max_background_readers))

	{
		need_more_workers = true;
		next_launch_time = Shared->next_launch_time;
	}

	/* Insert our new request at the head end of the queue. */
	Shared->queue[Shared->head] = request;
	Shared->head = new_head;
	LWLockRelease(BackgroundReaderLock);

	/* Signal a worker, if one is waiting. */
	ConditionVariableSignal(&Shared->queue_not_empty);

	if (need_more_workers)
	{
		TimestampTz		now = GetCurrentTimestamp();
		bool			launch = false;

		if (now >= next_launch_time)
		{
			LWLockAcquire(BackgroundReaderLock, LW_EXCLUSIVE);
			if (now > Shared->next_launch_time &&
				PrepareToLaunchBackgroundReader())
			{
				Shared->next_launch_time =
					TimestampTzPlusMilliseconds(now,
												background_reader_launch_delay);
				launch = true;
			}
			LWLockRelease(BackgroundReaderLock);

			if (launch)
				LaunchBackgroundReader();
		}
	}

	return true;
}

/*
 * Set up the shared memory state required for tracking background readers.
 */
void
BackgroundReaderInit(void)
{
	size_t		size = BackgroundReaderShmemSize();
	bool		found;

	Shared = (BackgroundReaderShared *)
		ShmemInitStruct("BackgroundReaderShared", size, &found);

	if (!found)
	{
		memset(Shared, 0, size);
		ConditionVariableInit(&Shared->queue_not_empty);
		ConditionVariableInit(&Shared->queue_not_full);
		Shared->size = max_background_reader_queue_depth;
	}
}

/*
 * Compute the amount of shared memory required to track background readers.
 */
size_t
BackgroundReaderShmemSize(void)
{
	size_t			size;

	size = offsetof(BackgroundReaderShared, queue);
	size = add_size(size, mul_size(max_background_reader_queue_depth,
								   sizeof(BackgroundReaderRequest)));

	return size;
}
