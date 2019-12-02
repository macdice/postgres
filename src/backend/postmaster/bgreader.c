/*-------------------------------------------------------------------------
 *
 * bgreader.c
 *
 * Background reader processes perform prefetching work requested by other
 * processes through a shared-memory queue, by calling
 * EnqueueBackgroundReaderRequest(), or indirectly via PrefetchBuffer().
 *
 * We try to reach a steady number of background reader processes by using
 * an idle timeout, and a maximum number.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
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
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

/* GUCs */
int			max_background_readers = 1;
int			max_background_reader_queue_depth = 10;
int			background_reader_idle_timeout = 1000;
int			background_reader_launch_delay = 1000;

/*
 * How far back should we look, when trying to merge requests into recent
 * requests that are waiting in the queue?
 */
#define BGREADER_MERGE_WINDOW 4

/* How big should we let requests get by merging? */
#define BGREADER_MAX_MERGE_BLOCKS 8

typedef struct BackgroundReaderRequest
{
	Oid				relid;
	RelFileNode		rnode;
	ForkNumber		forkNum;
	BlockNumber		blockNum;
	int				blocks;
	bool			recovery;
	bool			cancel;
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
	LockRelId relid;
	Buffer buffer;

	/* Canceled by sinval message while in the queue? */
	if (request->cancel)
		return;

	/*
	 * For online requests, we lock the logical relation.  In recovery, we
	 * lock the relfilenode, because wal prefetcher doesn't have logical OIDs
	 * to give us.
	 */
	if (request->relid == InvalidOid)
	{
		relid.dbId = request->rnode.dbNode;
		relid.relId = request->rnode.relNode;
	}
	else
	{
		relid.dbId = request->rnode.dbNode;
		relid.relId = request->relid;
	}

	/*
	 * We have to hold a share lock on the relation while we try to access its
	 * buffers, otherwise it might have been dropped.  Acquiring the lock will
	 * tell us if anything has happened to the relation, by checking the
	 * shared invalidation queue.  Unfortunately we have to expose that via a
	 * global variable.
	 */
	LockRelationIdForSession(&relid, AccessShareLock);
	AcceptInvalidationMessages();

	/* TODO: For online invalidation to work, we also need a new kind of cache
	 * invalidation a bit like RelCache but which doesn't get ignored just
	 * because you're in the wrong database */

	/* Canceled by a sinval message when we acquired the lock? */
	if (request->cancel)
	{
		UnlockRelationIdForSession(&relid, AccessShareLock);
		return;
	}

	/* Read the buffers. */
	for (int i = 0; i < request->blocks; ++i)
	{
		/*
		 * XXX HORRIBLE KLUDGE ALERT: Pretend we're in recovery, when the
		 * walreader is doing this to prefetch on behalf of recovery.
		 * Otherwise pages that will be truncated later by the WAL (crash
		 * recovery/catching up), or haven't been created yet (streaming
		 * replication) would cause errors for background reader, even the
		 * they would be zeroed when recovery reads them in (and md.c finds
		 * out the file is absent or too short).
		 */
		InRecovery = request->recovery;

		/*
		 * TODO: Ideally we would use a new ReadBuffer variant that doesn't
		 * increase usage count (otherwise it gets bumped once here and once
		 * when the ultimate user of the data reads it), and that gives up
		 * immediately if it can't get the buffer lock.  Not sure if that is
		 * best indated with RBM_PREFETCH, or with a special strategy passed
		 * in instead of the default.  Also, by not using a special strategy,
		 * we lose the scan resistence that the ultimate user of this buffer
		 * might want.  Also, it'd be nice to save cycled by not having it
		 * return the buffer pinned.
		 */
		buffer = ReadBufferWithoutRelcache(request->rnode,
										   request->forkNum,
										   request->blockNum + i,
										   RBM_NORMAL,
										   NULL);
		if (BufferIsValid(buffer))
			ReleaseBuffer(buffer);

		InRecovery = false;
	}

	UnlockRelationIdForSession(&relid, AccessShareLock);
}

static void
BackgroundReaderSmgrInvalCallback(Datum arg, RelFileNode rnode)
{
	BackgroundReaderRequest *request =
		(BackgroundReaderRequest *) DatumGetPointer(arg);

	/*
	 * If the request we're currently processing is for this relation, cancel
	 * it.  This is an item we've already removed from the queue, and haven't
	 * processed yet, straight after we acquire a relation lock.
	 */
	if (RelFileNodeEquals(request->rnode, rnode))
		request->cancel = true;

	/* We also have to nuke anything in the queue for this relation. */
	LWLockAcquire(BackgroundReaderLock, LW_EXCLUSIVE);
	for (int i = Shared->tail; i != Shared->head; i = (i + 1) % Shared->size)
	{
		request = &Shared->queue[i];
		if (RelFileNodeEquals(request->rnode, rnode))
			request->cancel = true;
	}
	LWLockRelease(BackgroundReaderLock);
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

	/* We connect, but not to a database, so we can use locks etc. */
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	/* We need a resource owner to interact with buffers. */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bgreader");

	/*
	 * We need to process sinval messages, so that we don't finish up trying
	 * to prefetch from dropped or truncated relfilenodes.
	 */
	CacheRegisterSmgrInvalCallback(BackgroundReaderSmgrInvalCallback,
								   PointerGetDatum(&request));

	PG_TRY();
	{
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
				HandleBackgroundReaderRequest(&request);
				if (was_full)
					ConditionVariableBroadcast(&Shared->queue_not_full);
			}
			else
				done = ConditionVariableTimedSleep(&Shared->queue_not_empty,
												   background_reader_idle_timeout,
												   WAIT_EVENT_BGREADER_MAIN);
			CHECK_FOR_INTERRUPTS();
		}
	}
	PG_FINALLY();
	{
		/*
		 * Most code in the tree does this via transactions.  We're not using
		 * transaction locks, so we have to remember to do this ourselves, to
		 * avoid leaking session locks, and also buffer locks and pins.
		 * XXX There is probably a better way to do this?
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		AbortBufferIO();
		UnlockBuffers();
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_LOCKS, false, true);
		LockReleaseAll(DEFAULT_LOCKMETHOD, true);
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_AFTER_LOCKS, false, true);
	}
	PG_END_TRY();
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
 * Can we extend an existing request to cover the new request parameters?
 */
static inline bool
MergeBackgroundReaderRequest(BackgroundReaderRequest *dst,
							 const BackgroundReaderRequest *src)
{
	BlockNumber begin, end;
	BlockNumber blocks;

	/* If canceled or not for matching relation and fork, give up. */
	if (dst->cancel ||
		dst->relid != src->relid ||
		!RelFileNodeEquals(dst->rnode, src->rnode) ||
		dst->forkNum != src->forkNum ||
		dst->recovery != src->recovery)
		return false;

	/* If neither adjacent nor overlapping, give up. */
	if ((src->blockNum > dst->blockNum + dst->blocks) ||
		(src->blockNum + src->blocks < dst->blockNum))
		return false;

	/* If merged request would be bigger than both and too large, give up. */
	begin = Min(src->blockNum, dst->blockNum);
	end = Max(src->blockNum + src->blocks, dst->blockNum + dst->blocks);
	Assert(end > begin);
	blocks = end - begin;
	if (blocks > src->blocks &&
		blocks > dst->blocks &&
		blocks > BGREADER_MAX_MERGE_BLOCKS)
		return false;

	/* Merge. */
	dst->blockNum = begin;
	dst->blocks = blocks;
	return true;
}

/*
 * Enqueue a request for a range of blocks to be prefetched, try to wake an
 * existing worker, and launch a new worker if that is appropriate.  If the
 * queue is full, then the behavior depends on 'wait_event'.  If it's 0, then
 * false indicates that the request couldn't be enqueued immediately.  If it's
 * a wait event value from pgstat.h, then false means that it was enqueued,
 * but we had to wait.
 */
bool
EnqueueBackgroundReaderRequest(Oid relid,
							   RelFileNode rnode,
							   ForkNumber forkNum,
							   BlockNumber blockNum,
							   int blocks,
							   bool recovery,
							   int wait_event)
{
	BackgroundReaderRequest request;
	TimestampTz next_launch_time;
	int			new_head;
	int			candidate;
	bool		need_more_workers = false;
	bool		had_to_wait = false;
	int			requests,
				readers;

	request.relid = relid;
	request.rnode = rnode;
	request.forkNum = forkNum;
	request.blockNum = blockNum;
	request.blocks = blocks;
	request.recovery = recovery;
	request.cancel = false;

 retry:
	LWLockAcquire(BackgroundReaderLock, LW_EXCLUSIVE);

	/* Can we merge it into a recent request? */
	candidate = Shared->head;
	for (int i = 0; i < BGREADER_MERGE_WINDOW; ++i)
	{
		if (candidate == Shared->tail)
			break;
		if (--candidate < 0)
			candidate = Shared->size - 1;
		if (MergeBackgroundReaderRequest(&Shared->queue[candidate],
										 &request))
		{
			LWLockRelease(BackgroundReaderLock);
			return true;
		}
	}

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

		if (wait_event != 0)
			ConditionVariablePrepareToSleep(&Shared->queue_not_full);
		LWLockRelease(BackgroundReaderLock);

		if (launch)
			LaunchBackgroundReader();

		if (wait_event != 0)
		{
			had_to_wait = true;
			ConditionVariableTimedSleep(&Shared->queue_not_full,
										background_reader_launch_delay,
										wait_event);
			ConditionVariableCancelSleep();
			goto retry;
		}
		return false;
	}

	/*
	 * If there are no workers, or there was already something waiting in the
	 * queue before us, we'll consider adding a new worker.
	 */
	readers = CurrentBackgroundReaders();
	requests = Shared->head - Shared->tail;
	if (requests < 0)
		requests += Shared->size;
	Assert(requests >= 0);
	if (readers == 0 || (requests > 0 && readers < max_background_readers))

	{
		need_more_workers = true;
		next_launch_time = Shared->next_launch_time;
	}

	/* Insert our new request at the head end of the queue. */
	Shared->queue[Shared->head] = request;
	Shared->head = new_head;
	LWLockRelease(BackgroundReaderLock);

	/* Signal a worker, if it looks like there may be an idle one. */
	/* XXX questionable */
	if (requests < readers)
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

	return !had_to_wait;
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
