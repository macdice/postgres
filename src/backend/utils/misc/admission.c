/*-------------------------------------------------------------------------
 *
 * admission.c
 *		  Admission control, to limit resource usage.
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		  src/backend/utils/misc/admission.c
 *
 *-------------------------------------------------------------------------
*/
#include "postgres.h"

#include "access/parallel.h"
#include "access/session.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "pgstat.h"
#include "utils/admission.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/spin.h"

typedef struct AdmissionControlSharedData
{
	size_t		mem_used;
	slock_t		mutex;
	ConditionVariable mem_freed_cv;
} AdmissionControlSharedData;

static AdmissionControlSharedData *AdmissionControlShared;

Size
AdmissionControlShmemSize(void)
{
	return sizeof(*AdmissionControlShared);
}

void
AdmissionControlShmemInit(void)
{
	bool		found;

	AdmissionControlShared = ShmemInitStruct("AdmissionControl",
											 AdmissionControlShmemSize(),
											 &found);
	if (!found)
	{
		AdmissionControlShared->mem_used = 0;
		SpinLockInit(&AdmissionControlShared->mutex);
		ConditionVariableInit(&AdmissionControlShared->mem_freed_cv);
	}
}

/*
 * Whenever raw memory is allocated, it should be reported here so that we can
 * try to track the total memory used by each session.
 */
static void
AdmissionControlTrackMemAllocated(size_t size)
{
	size_t		old_used;
	size_t		new_used;

	old_used = Max(CurrentSession->exec_mem_reserved,
				   CurrentSession->exec_mem_allocated);
	CurrentSession->exec_mem_allocated += size;
	new_used = Max(CurrentSession->exec_mem_reserved,
				   CurrentSession->exec_mem_allocated);

	/*
	 * We only need to touch shared memory if we exceeded the reserved.
	 * XXX figure out a way to report only eg 1MB changes to shm + pgstat.
	 */
	if (new_used > old_used)
	{
		SpinLockAcquire(&AdmissionControlShared->mutex);
		AdmissionControlShared->mem_used += (new_used - old_used);
		SpinLockRelease(&AdmissionControlShared->mutex);
	}

	pgstat_report_exec_mem_allocated(CurrentSession->exec_mem_allocated);
}

static void
AdmissionControlTrackMemFreed(size_t size)
{
	size_t		old_used;
	size_t		new_used;

	Assert(size <= CurrentSession->exec_mem_allocated);
	old_used = Max(CurrentSession->exec_mem_reserved,
				   CurrentSession->exec_mem_allocated);
	CurrentSession->exec_mem_allocated -= size;
	new_used = Max(CurrentSession->exec_mem_reserved,
				   CurrentSession->exec_mem_allocated);

	if (new_used < old_used)
	{
		SpinLockAcquire(&AdmissionControlShared->mutex);
		AdmissionControlShared->mem_used -= (old_used - new_used);
		SpinLockRelease(&AdmissionControlShared->mutex);

		/*
		 * XXX This is too primitive: we need a fair queue, more like
		 * heavyweight locks.  What we have here is a thundering herd of wakers
		 * who get the memory in whatever order.
		 */
		ConditionVariableBroadcast(&AdmissionControlShared->mem_freed_cv);
	}

	pgstat_report_exec_mem_allocated(CurrentSession->exec_mem_allocated);
}

/*
 * A callback suitable for use in a MemoryContext, to report allocated and
 * freed executor memory.
 */
void
AdmissionControlExecMemChanged(ssize_t delta, void *data)
{
	if (delta > 0)
		AdmissionControlTrackMemAllocated(delta);
	else
		AdmissionControlTrackMemFreed(-delta);
}

/*
 * Begin a query that anticipates that it will use a given amount of memory.
 * If cluster_work_mem_limit is set above zero and there is not enough head
 * room for this query to begin, then this call might block until the memory
 * can be reserved for this session.  If session_work_mem_limit is set above
 * zero and would be exceeded, any error is raised (possibly indicating that
 * work_mem was set too high).  Otherwise, the query can proceed and control
 * returns.
 *
 * The amount of memory actually reserved is returned.  This value *must* be
 * passed to AdmissionControlEndQuery() at the end of query processing.
 */
size_t
AdmissionControlBeginQuery(QueryDesc *queryDesc)
{
	size_t		estimate;
	size_t		using;
	size_t		want;
	Plan	   *plan;

	/* Fast return if the sky's the limit. */
	if (cluster_work_mem_limit < 0 && session_work_mem_limit < 0)
		return 0;

	/*
	 * If we're in a parallel worker, the leader's query was already allowed to
	 * proceed.
	 * XXX and already considered all workers?
	 */
	if (IsParallelWorker())
		return 0;

	/*
	 * Figure out how much memory the query thinks it needs.  The work_mem
	 * setting might have changed since planning, but in that case we'll
	 * effectively scale the estimate linearly.
	 */
	plan = queryDesc->plannedstmt->planTree;
	if (queryDesc->estate->es_top_eflags & EXEC_FLAG_REWIND)
		estimate = (plan->mem_random_freed + plan->mem_random_held) * work_mem * 1024;
	else
		estimate = (plan->mem_seq_freed + plan->mem_seq_held) * work_mem * 1024;

	/*
	 * Are we trying to ask for more memory than any one session is allowed?
	 * That would not be OK, and we can't just wait, because we might wait
	 * forever.
	 */
	using = Max(CurrentSession->exec_mem_reserved, CurrentSession->exec_mem_allocated);
	if (session_work_mem_limit >= 0 &&
		using + estimate > session_work_mem_limit * (size_t) 1024)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("query estimated to require %zu kB of executor memory but %zu kB is already in use by the session and session_work_mem_limit would be exceeded",
						estimate / 1024,
						using),
				 errhint("Consider reducing work_mem, increasing session_work_mem_limit or run fewer cursors concurrently.")));

	/*
	 * Does this session need to try to reserve more memory?  If some other
	 * query (portal) owned by this session has exceeded what it reserved,
	 * we'll reserve extra memory to cover it here; that is why we need to
	 * return the size we actually reserved to the caller, so that it can be
	 * released later.
	 */
	using = Max(CurrentSession->exec_mem_reserved, CurrentSession->exec_mem_allocated);
	want = (using + estimate) - CurrentSession->exec_mem_reserved;

	/*
	 * Is waiting not going to help?  XXX Should figure out how to subtract
	 * memory that other sessions are not going to give back because they
	 * reserved it at startup, so that we don't wait (in vain) for that.
	 */
	if (estimate > cluster_work_mem_limit * (size_t) 1024)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("query estimated to required %zu kB of memory but cluster_work_mem_limit would be exceeded",
						estimate / 1024),
				 errhint("Consider reducing work_mem, increasing cluster_work_mem_limit or running fewer queries concurrently.")));

	/*
	 * Register our new memory requirement, or wait until we can.  Another way
	 * out is by error (statement timeout or other interruption).
	 */
	for (;;)
	{
		bool		success = false;
		size_t		new_mem_used;

		SpinLockAcquire(&AdmissionControlShared->mutex);
		new_mem_used = AdmissionControlShared->mem_used + want;
		if (new_mem_used <= cluster_work_mem_limit * (size_t) 1024)
		{
			AdmissionControlShared->mem_used = new_mem_used;
			success = true;
		}
		SpinLockRelease(&AdmissionControlShared->mutex);

		if (success)
			break;

		ConditionVariableSleep(&AdmissionControlShared->mem_freed_cv,
							   WAIT_EVENT_ADMISSION_CONTROL);
	}
	ConditionVariableCancelSleep();

	CurrentSession->exec_mem_reserved += want;
	pgstat_report_exec_mem_reserved(CurrentSession->exec_mem_reserved);

	return want;
}

/*
 * Give back reserved memory.
 */
void
AdmissionControlEndQuery(size_t reserved)
{
	size_t		old_used;
	size_t		new_used;

	Assert(reserved >= CurrentSession->exec_mem_reserved);
	old_used = Max(CurrentSession->exec_mem_reserved,
				   CurrentSession->exec_mem_allocated);
	CurrentSession->exec_mem_reserved -= reserved;
	new_used = Max(CurrentSession->exec_mem_reserved,
				   CurrentSession->exec_mem_allocated);

	if (new_used < old_used)
	{
		SpinLockAcquire(&AdmissionControlShared->mutex);
		AdmissionControlShared->mem_used -= (old_used - new_used);
		SpinLockRelease(&AdmissionControlShared->mutex);

		/* XXX Need fair queue */
		ConditionVariableBroadcast(&AdmissionControlShared->mem_freed_cv);
	}

	pgstat_report_exec_mem_reserved(CurrentSession->exec_mem_reserved);
}

/*
 * At session start, we try to reserve the memory configured by
 * session_work_mem_reservation.  This will allow us to begin smaller queries
 * without having to reserve more memory.
 */
void
AdmissionControlBeginSession(void)
{
	size_t		new_used;

	if (session_work_mem_reservation <= 0)
		return;

	SpinLockAcquire(&AdmissionControlShared->mutex);
	new_used = AdmissionControlShared->mem_used +
		session_work_mem_reservation * (size_t) 1024;
	if (cluster_work_mem_limit < 0 ||
		new_used <= cluster_work_mem_limit * (size_t) 1024)
	{
		AdmissionControlShared->mem_used = new_used;
		CurrentSession->exec_mem_reserved +=
			session_work_mem_reservation * (size_t) 1024;
	}
	SpinLockRelease(&AdmissionControlShared->mutex);

	pgstat_report_exec_mem_reserved(CurrentSession->exec_mem_reserved);

	/* We'll need to release our reservation at exit. */
	before_shmem_exit(AdmissionControlEndSession, 0);
}

/*
 * Give back all reserved and allocated memory before process exit.
 */
void
AdmissionControlEndSession(int code, Datum datum)
{
	if (CurrentSession->exec_mem_allocated > 0)
		AdmissionControlTrackMemFreed(CurrentSession->exec_mem_allocated);
	if (CurrentSession->exec_mem_reserved > 0)
		AdmissionControlEndQuery(CurrentSession->exec_mem_reserved);
}
