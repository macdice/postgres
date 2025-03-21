/*-------------------------------------------------------------------------
 *
 * method_worker.c
 *    AIO - perform AIO using worker processes
 *
 * IO workers consume IOs from a shared memory submission queue, run
 * traditional synchronous system calls, and perform the shared completion
 * handling immediately.  Client code submits most requests by pushing IOs
 * into the submission queue, and waits (if necessary) using condition
 * variables.  Some IOs cannot be performed in another process due to lack of
 * infrastructure for reopening the file, and must processed synchronously by
 * the client code when submitted.
 *
 * The pool tries to stabilize at a size that can handle recently seen
 * variation in demand, within the configured limits.
 *
 * This method of AIO is available in all builds on all operating systems, and
 * is the default.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/method_worker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/aio_subsys.h"
#include "storage/io_worker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/injection_point.h"
#include "utils/memdebug.h"
#include "utils/ps_status.h"
#include "utils/wait_event.h"

/* Saturation for stats counters used to estimate wakeup:work ratio. */
#define PGAIO_WORKER_STATS_MAX 4

/* Debugging only: show activity and statistics in ps command line. */
/* #define PGAIO_WORKER_SHOW_PS_INFO */

typedef struct PgAioWorkerSubmissionQueue
{
	uint32		size;
	uint32		head;
	uint32		tail;
	int			sqes[FLEXIBLE_ARRAY_MEMBER];
} PgAioWorkerSubmissionQueue;

typedef struct PgAioWorkerSlot
{
	ProcNumber	proc_number;
} PgAioWorkerSlot;

/*
 * Sets of worker IDs are held in a simple bitmap, accessed through functions
 * that provide a more readable abstraction.  If we wanted to support more
 * workers than that, the contention on the single queue would surely get too
 * high, so we might want to consider multiple pools instead of widening this.
 */
typedef uint64 PgAioWorkerSet;

#define PGAIO_WORKER_SET_BITS (sizeof(PgAioWorkerSet) * CHAR_BIT)

static_assert(PGAIO_WORKER_SET_BITS >= MAX_IO_WORKERS, "too small");

typedef struct PgAioWorkerControl
{
	/* Seen by postmaster */
	volatile bool grow;

	/* Protected by AioWorkerSubmissionQueueLock. */
	PgAioWorkerSet idle_worker_set;

	/* Protected by AioWorkerControlLock. */
	PgAioWorkerSet worker_set;
	int			nworkers;

	/* Protected by AioWorkerControlLock. */
	PgAioWorkerSlot workers[FLEXIBLE_ARRAY_MEMBER];
} PgAioWorkerControl;

static size_t pgaio_worker_shmem_size(void);
static void pgaio_worker_shmem_init(bool first_time);

static bool pgaio_worker_needs_synchronous_execution(PgAioHandle *ioh);
static int	pgaio_worker_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);


const IoMethodOps pgaio_worker_ops = {
	.shmem_size = pgaio_worker_shmem_size,
	.shmem_init = pgaio_worker_shmem_init,

	.needs_synchronous_execution = pgaio_worker_needs_synchronous_execution,
	.submit = pgaio_worker_submit,
};


/* GUCs */
int			io_min_workers = 1;
int			io_max_workers = 8;
int			io_worker_idle_timeout = 60000;
int			io_worker_launch_interval = 100;


static int	io_worker_queue_size = 64;
static int	MyIoWorkerId = -1;
static PgAioWorkerSubmissionQueue *io_worker_submission_queue;
static PgAioWorkerControl *io_worker_control;


static void
pgaio_worker_set_initialize(PgAioWorkerSet *set)
{
	*set = 0;
}

static bool
pgaio_worker_set_is_empty(PgAioWorkerSet *set)
{
	return *set == 0;
}

static PgAioWorkerSet
pgaio_worker_set_singleton(int worker)
{
	return UINT64_C(1) << worker;
}

static void
pgaio_worker_set_fill(PgAioWorkerSet *set)
{
	*set = UINT64_MAX >> (PGAIO_WORKER_SET_BITS - MAX_IO_WORKERS);
}

static void
pgaio_worker_set_subtract(PgAioWorkerSet *set1, const PgAioWorkerSet *set2)
{
	*set1 &= ~*set2;
}

static void
pgaio_worker_set_insert(PgAioWorkerSet *set, int worker)
{
	*set |= pgaio_worker_set_singleton(worker);
}

static void
pgaio_worker_set_remove(PgAioWorkerSet *set, int worker)
{
	*set &= ~pgaio_worker_set_singleton(worker);
}

static void
pgaio_worker_set_remove_less_than(PgAioWorkerSet *set, int worker)
{
	*set &= ~(pgaio_worker_set_singleton(worker) - 1);
}

static int
pgaio_worker_set_get_highest(PgAioWorkerSet *set)
{
	Assert(!pgaio_worker_set_is_empty(set));
	return pg_leftmost_one_pos64(*set);
}

static int
pgaio_worker_set_get_lowest(PgAioWorkerSet *set)
{
	Assert(!pgaio_worker_set_is_empty(set));
	return pg_rightmost_one_pos64(*set);
}

static int
pgaio_worker_set_pop_lowest(PgAioWorkerSet *set)
{
	int			worker = pgaio_worker_set_get_lowest(set);

	pgaio_worker_set_remove(set, worker);
	return worker;
}

#ifdef USE_ASSERT_CHECKING
static bool
pgaio_worker_set_contains(PgAioWorkerSet *set, int worker)
{
	return (*set & pgaio_worker_set_singleton(worker)) != 0;
}

static int
pgaio_worker_set_count(PgAioWorkerSet *set)
{
	return pg_popcount64(*set);
}
#endif

static size_t
pgaio_worker_queue_shmem_size(int *queue_size)
{
	/* Round size up to next power of two so we can make a mask. */
	*queue_size = pg_nextpower2_32(io_worker_queue_size);

	return offsetof(PgAioWorkerSubmissionQueue, sqes) +
		sizeof(int) * *queue_size;
}

static size_t
pgaio_worker_control_shmem_size(void)
{
	return offsetof(PgAioWorkerControl, workers) +
		sizeof(PgAioWorkerSlot) * MAX_IO_WORKERS;
}

static size_t
pgaio_worker_shmem_size(void)
{
	size_t		sz;
	int			queue_size;

	sz = pgaio_worker_queue_shmem_size(&queue_size);
	sz = add_size(sz, pgaio_worker_control_shmem_size());

	return sz;
}

static void
pgaio_worker_shmem_init(bool first_time)
{
	bool		found;
	int			queue_size;

	io_worker_submission_queue =
		ShmemInitStruct("AioWorkerSubmissionQueue",
						pgaio_worker_queue_shmem_size(&queue_size),
						&found);
	if (!found)
	{
		io_worker_submission_queue->size = queue_size;
		io_worker_submission_queue->head = 0;
		io_worker_submission_queue->tail = 0;
	}

	io_worker_control =
		ShmemInitStruct("AioWorkerControl",
						pgaio_worker_control_shmem_size(),
						&found);
	if (!found)
	{
		io_worker_control->grow = false;
		pgaio_worker_set_initialize(&io_worker_control->worker_set);
		pgaio_worker_set_initialize(&io_worker_control->idle_worker_set);
		for (int i = 0; i < MAX_IO_WORKERS; ++i)
			io_worker_control->workers[i].proc_number = INVALID_PROC_NUMBER;
	}
}

static void
pgaio_worker_grow(bool grow)
{
	/*
	 * This is called from sites that don't hold AioWorkerControlLock, but
	 * these values change infrequently and an up-to-date value is not
	 * required for this heuristic purpose.
	 */
	if (!grow)
	{
		/* Avoid dirtying memory if not already set. */
		if (io_worker_control->grow)
			io_worker_control->grow = false;
	}
	else
	{
		/* Do nothing if request already pending. */
		if (!io_worker_control->grow)
		{
			io_worker_control->grow = true;
			SendPostmasterSignal(PMSIGNAL_IO_WORKER_GROW);
		}
	}
}

/*
 * Called by the postmaster to check if a new worker is needed.
 */
bool
pgaio_worker_test_grow(void)
{
	return io_worker_control && io_worker_control->grow;
}

/*
 * Called by the postmaster to check if a new worker is needed when it's ready
 * to launch one, and clear the flag.
 */
bool
pgaio_worker_test_and_clear_grow(void)
{
	bool		result;

	result = io_worker_control->grow;
	if (result)
		io_worker_control->grow = false;

	return result;
}

static int
pgaio_worker_choose_idle(int minimum_worker)
{
	PgAioWorkerSet worker_set;
	int			worker;

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	worker_set = io_worker_control->idle_worker_set;
	pgaio_worker_set_remove_less_than(&worker_set, minimum_worker);
	if (pgaio_worker_set_is_empty(&worker_set))
		return -1;

	/* Find the lowest numbered idle worker and mark it not idle. */
	worker = pgaio_worker_set_get_lowest(&worker_set);
	pgaio_worker_set_remove(&io_worker_control->idle_worker_set, worker);

	return worker;
}

/*
 * Try to wake a worker by setting its latch, to tell it there are IOs to
 * process in the submission queue.
 */
static void
pgaio_worker_wake(int worker)
{
	ProcNumber	proc_number;

	/*
	 * If the selected worker is concurrently exiting, then pgaio_worker_die()
	 * had not yet removed it as of when we saw it in idle_worker_set.  That's
	 * OK, because it will wake all remaining workers to close wakeup-vs-exit
	 * races: *someone* will see the queued IO.  If there are no workers
	 * running, the postmaster will start a new one.
	 */
	proc_number = io_worker_control->workers[worker].proc_number;
	if (proc_number != INVALID_PROC_NUMBER)
		SetLatch(&GetPGProcByNumber(proc_number)->procLatch);
}

static bool
pgaio_worker_submission_queue_insert(PgAioHandle *ioh)
{
	PgAioWorkerSubmissionQueue *queue;
	uint32		new_head;

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	queue = io_worker_submission_queue;
	new_head = (queue->head + 1) & (queue->size - 1);
	if (new_head == queue->tail)
	{
		pgaio_debug(DEBUG3, "io queue is full, at %u elements",
					io_worker_submission_queue->size);
		return false;			/* full */
	}

	queue->sqes[queue->head] = pgaio_io_get_id(ioh);
	queue->head = new_head;

	return true;
}

static int
pgaio_worker_submission_queue_consume(void)
{
	PgAioWorkerSubmissionQueue *queue;
	int			result;

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	queue = io_worker_submission_queue;
	if (queue->tail == queue->head)
		return -1;				/* empty */

	result = queue->sqes[queue->tail];
	queue->tail = (queue->tail + 1) & (queue->size - 1);

	return result;
}

static uint32
pgaio_worker_submission_queue_depth(void)
{
	uint32		head;
	uint32		tail;

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	head = io_worker_submission_queue->head;
	tail = io_worker_submission_queue->tail;

	if (tail > head)
		head += io_worker_submission_queue->size;

	Assert(head >= tail);

	return head - tail;
}

static bool
pgaio_worker_needs_synchronous_execution(PgAioHandle *ioh)
{
	return
		!IsUnderPostmaster
		|| ioh->flags & PGAIO_HF_REFERENCES_LOCAL
		|| !pgaio_io_can_reopen(ioh);
}

static int
pgaio_worker_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	PgAioHandle **synchronous_ios = NULL;
	int			nsync = 0;
	int			worker = -1;

	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < num_staged_ios; i++)
		pgaio_io_prepare_submit(staged_ios[i]);

	if (LWLockConditionalAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE))
	{
		for (int i = 0; i < num_staged_ios; ++i)
		{
			Assert(!pgaio_worker_needs_synchronous_execution(staged_ios[i]));
			if (!pgaio_worker_submission_queue_insert(staged_ios[i]))
			{
				/*
				 * Do the rest synchronously. If the queue is full, give up
				 * and do the rest synchronously. We're holding an exclusive
				 * lock on the queue so nothing can consume entries.
				 */
				synchronous_ios = &staged_ios[i];
				nsync = (num_staged_ios - i);

				break;
			}
		}

		if (worker == -1)
		{
			/* Choose an idle worker to wake up if we haven't already. */
			worker = pgaio_worker_choose_idle(0);
		}
		LWLockRelease(AioWorkerSubmissionQueueLock);
	}
	else
	{
		/* do everything synchronously, no wakeup needed */
		synchronous_ios = staged_ios;
		nsync = num_staged_ios;
	}

	/*
	 * If we didn't find a worker to wake up, the existing workers will
	 * determine whether the pool is too small.
	 */
	if (worker != -1)
		pgaio_worker_wake(worker);

	/* Run whatever is left synchronously. */
	while (nsync > 0)
	{
		pgaio_io_perform_synchronously(*synchronous_ios++);
		nsync--;

		/* Between synchronous operations, try to enqueue again. */
		if (nsync > 0)
		{
			worker = -1;
			if (LWLockConditionalAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE))
			{
				while (nsync > 0 &&
					   pgaio_worker_submission_queue_insert(*synchronous_ios))
				{
					synchronous_ios++;
					nsync--;
					if (worker == -1)
						worker = pgaio_worker_choose_idle(0);
				}
				LWLockRelease(AioWorkerSubmissionQueueLock);
			}
			if (worker != -1)
				pgaio_worker_wake(worker);
		}
	}

	return num_staged_ios;
}

/*
 * on_shmem_exit() callback that releases the worker's slot in
 * io_worker_control.
 */
static void
pgaio_worker_die(int code, Datum arg)
{
	PgAioWorkerSet notify_set;

	LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
	pgaio_worker_set_remove(&io_worker_control->idle_worker_set, MyIoWorkerId);
	LWLockRelease(AioWorkerSubmissionQueueLock);

	LWLockAcquire(AioWorkerControlLock, LW_EXCLUSIVE);
	Assert(io_worker_control->workers[MyIoWorkerId].proc_number == MyProcNumber);
	io_worker_control->workers[MyIoWorkerId].proc_number = INVALID_PROC_NUMBER;
	Assert(pgaio_worker_set_contains(&io_worker_control->worker_set, MyIoWorkerId));
	pgaio_worker_set_remove(&io_worker_control->worker_set, MyIoWorkerId);
	notify_set = io_worker_control->worker_set;
	Assert(io_worker_control->nworkers > 0);
	io_worker_control->nworkers--;
	Assert(pgaio_worker_set_count(&io_worker_control->worker_set) ==
		   io_worker_control->nworkers);
	LWLockRelease(AioWorkerControlLock);

	/* Notify other workers on pool change. */
	while (!pgaio_worker_set_is_empty(&notify_set))
		pgaio_worker_wake(pgaio_worker_set_pop_lowest(&notify_set));
}

/*
 * Register the worker in shared memory, assign MyIoWorkerId and register a
 * shutdown callback to release registration.
 */
static void
pgaio_worker_register(void)
{
	PgAioWorkerSet free_worker_set;
	PgAioWorkerSet old_worker_set;

	MyIoWorkerId = -1;

	LWLockAcquire(AioWorkerControlLock, LW_EXCLUSIVE);
	pgaio_worker_set_fill(&free_worker_set);
	pgaio_worker_set_subtract(&free_worker_set, &io_worker_control->worker_set);
	if (!pgaio_worker_set_is_empty(&free_worker_set))
		MyIoWorkerId = pgaio_worker_set_get_lowest(&free_worker_set);
	if (MyIoWorkerId == -1)
		elog(ERROR, "couldn't find a free worker ID");

	Assert(io_worker_control->workers[MyIoWorkerId].proc_number ==
		   INVALID_PROC_NUMBER);
	io_worker_control->workers[MyIoWorkerId].proc_number = MyProcNumber;

	old_worker_set = io_worker_control->worker_set;
	Assert(!pgaio_worker_set_contains(&old_worker_set, MyIoWorkerId));
	pgaio_worker_set_insert(&io_worker_control->worker_set, MyIoWorkerId);
	io_worker_control->nworkers++;
	Assert(pgaio_worker_set_count(&io_worker_control->worker_set) ==
		   io_worker_control->nworkers);
	LWLockRelease(AioWorkerControlLock);

	/* Notify other workers on pool change. */
	while (!pgaio_worker_set_is_empty(&old_worker_set))
		pgaio_worker_wake(pgaio_worker_set_pop_lowest(&old_worker_set));

	on_shmem_exit(pgaio_worker_die, 0);
}

static void
pgaio_worker_error_callback(void *arg)
{
	ProcNumber	owner;
	PGPROC	   *owner_proc;
	int32		owner_pid;
	PgAioHandle *ioh = arg;

	if (!ioh)
		return;

	Assert(ioh->owner_procno != MyProcNumber);
	Assert(MyBackendType == B_IO_WORKER);

	owner = ioh->owner_procno;
	owner_proc = GetPGProcByNumber(owner);
	owner_pid = owner_proc->pid;

	errcontext("I/O worker executing I/O on behalf of process %d", owner_pid);
}

/*
 * Check if this backend is allowed to time out, and thus should use a
 * non-infinite sleep time.  Only the highest-numbered worker is allowed to
 * time out, and only if the pool is above io_min_workers.  Serializing
 * timeouts keeps IDs in a range 0..N without gaps, and avoids undershooting
 * io_min_workers.
 *
 * The result is only instantaneously true and may be temporarily inconsistent
 * in different workers around transitions, but all workers are woken up on
 * pool size or GUC changes making the result eventually consistent.
 */
static bool
pgaio_worker_can_timeout(void)
{
	PgAioWorkerSet worker_set;

	/* Serialize against pool size changes. */
	LWLockAcquire(AioWorkerControlLock, LW_SHARED);
	worker_set = io_worker_control->worker_set;
	LWLockRelease(AioWorkerControlLock);

	if (MyIoWorkerId != pgaio_worker_set_get_highest(&worker_set))
		return false;
	if (MyIoWorkerId < io_min_workers)
		return false;

	return true;
}

void
IoWorkerMain(const void *startup_data, size_t startup_data_len)
{
	sigjmp_buf	local_sigjmp_buf;
	TimestampTz idle_timeout_abs = 0;
	int			timeout_guc_used = 0;
	PgAioHandle *volatile error_ioh = NULL;
	ErrorContextCallback errcallback = {0};
	volatile int error_errno = 0;
	char		cmd[128];
	int			ios = 0;
	int			wakeups = 0;

	AuxiliaryProcessMainCommon();

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, die);		/* to allow manually triggering worker restart */

	/*
	 * Ignore SIGTERM, will get explicit shutdown via SIGUSR2 later in the
	 * shutdown sequence, similar to checkpointer.
	 */
	pqsignal(SIGTERM, SIG_IGN);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SignalHandlerForShutdownRequest);

	/* also registers a shutdown callback to unregister */
	pgaio_worker_register();

	sprintf(cmd, "%d", MyIoWorkerId);
	set_ps_display(cmd);

	errcallback.callback = pgaio_worker_error_callback;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* see PostgresMain() */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		error_context_stack = NULL;
		HOLD_INTERRUPTS();

		EmitErrorReport();

		/*
		 * In the - very unlikely - case that the IO failed in a way that
		 * raises an error we need to mark the IO as failed.
		 *
		 * Need to do just enough error recovery so that we can mark the IO as
		 * failed and then exit (postmaster will start a new worker).
		 */
		LWLockReleaseAll();

		if (error_ioh != NULL)
		{
			/* should never fail without setting error_errno */
			Assert(error_errno != 0);

			errno = error_errno;

			START_CRIT_SECTION();
			pgaio_io_process_completion(error_ioh, -error_errno);
			END_CRIT_SECTION();
		}

		proc_exit(1);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	while (!ShutdownRequestPending)
	{
		uint32		io_index;
		int			worker = -1;
		int			queue_depth = 0;
		bool		grow = false;

		/*
		 * Try to get a job to do.
		 *
		 * The lwlock acquisition also provides the necessary memory barrier
		 * to ensure that we don't see an outdated data in the handle.
		 */
		LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
		if ((io_index = pgaio_worker_submission_queue_consume()) == -1)
		{
			/* Nothing to do.  Mark self idle. */
			pgaio_worker_set_insert(&io_worker_control->idle_worker_set,
									MyIoWorkerId);
		}
		else
		{
			/* Got one.  Clear idle flag. */
			pgaio_worker_set_remove(&io_worker_control->idle_worker_set,
									MyIoWorkerId);

			/*
			 * See if we should wake up a higher numbered peer.  Only do this
			 * if this worker is itself not receiving spurious wakeups.  This
			 * heuristic discovers the useful wakeup propagation chain length.
			 */
			if (wakeups <= ios)
			{
				queue_depth = pgaio_worker_submission_queue_depth();
				worker = pgaio_worker_choose_idle(MyIoWorkerId + 1);

				/*
				 * If there were no idle higher numbered peers and there are
				 * more than enough IOs queued for me and all lower numbered
				 * peers, then try to start a new worker.
				 */
				if (worker == -1 && queue_depth > MyIoWorkerId)
					grow = true;
			}
		}
		LWLockRelease(AioWorkerSubmissionQueueLock);

		/* Propagate wakeups. */
		if (worker != -1)
			pgaio_worker_wake(worker);
		else if (grow)
			pgaio_worker_grow(true);

		if (io_index != -1)
		{
			PgAioHandle *ioh = NULL;

			/* Cancel timeout and update wakeup:work ratio. */
			idle_timeout_abs = 0;
			if (++ios == PGAIO_WORKER_STATS_MAX)
			{
				ios /= 2;
				wakeups /= 2;
			}

			ioh = &pgaio_ctl->io_handles[io_index];
			error_ioh = ioh;
			errcallback.arg = ioh;

			pgaio_debug_io(DEBUG4, ioh,
						   "worker %d processing IO",
						   MyIoWorkerId);

			/*
			 * Prevent interrupts between pgaio_io_reopen() and
			 * pgaio_io_perform_synchronously() that otherwise could lead to
			 * the FD getting closed in that window.
			 */
			HOLD_INTERRUPTS();

			/*
			 * It's very unlikely, but possible, that reopen fails. E.g. due
			 * to memory allocations failing or file permissions changing or
			 * such.  In that case we need to fail the IO.
			 *
			 * There's not really a good errno we can report here.
			 */
			error_errno = ENOENT;
			pgaio_io_reopen(ioh);

			/*
			 * To be able to exercise the reopen-fails path, allow injection
			 * points to trigger a failure at this point.
			 */
			INJECTION_POINT("aio-worker-after-reopen", ioh);

			error_errno = 0;
			error_ioh = NULL;

			/*
			 * As part of IO completion the buffer will be marked as NOACCESS,
			 * until the buffer is pinned again - which never happens in io
			 * workers. Therefore the next time there is IO for the same
			 * buffer, the memory will be considered inaccessible. To avoid
			 * that, explicitly allow access to the memory before reading data
			 * into it.
			 */
#ifdef USE_VALGRIND
			{
				struct iovec *iov;
				uint16		iov_length = pgaio_io_get_iovec_length(ioh, &iov);

				for (int i = 0; i < iov_length; i++)
					VALGRIND_MAKE_MEM_UNDEFINED(iov[i].iov_base, iov[i].iov_len);
			}
#endif

#ifdef PGAIO_WORKER_SHOW_PS_INFO
			sprintf(cmd, "%d: [%s] %s",
					MyIoWorkerId,
					pgaio_io_get_op_name(ioh),
					pgaio_io_get_target_description(ioh));
			set_ps_display(cmd);
#endif

			/*
			 * We don't expect this to ever fail with ERROR or FATAL, no need
			 * to keep error_ioh set to the IO.
			 * pgaio_io_perform_synchronously() contains a critical section to
			 * ensure we don't accidentally fail.
			 */
			pgaio_io_perform_synchronously(ioh);

			RESUME_INTERRUPTS();
			errcallback.arg = NULL;
		}
		else
		{
			int			timeout_ms;

			/* Cancel new worker if pending. */
			pgaio_worker_grow(false);

			/* Compute the remaining allowed idle time. */
			if (io_worker_idle_timeout == -1)
			{
				/* Never time out. */
				timeout_ms = -1;
			}
			else
			{
				TimestampTz now = GetCurrentTimestamp();

				/* If the GUC changes, reset timer. */
				if (idle_timeout_abs != 0 &&
					io_worker_idle_timeout != timeout_guc_used)
					idle_timeout_abs = 0;

				/* On first sleep, compute absolute timeout. */
				if (idle_timeout_abs == 0)
				{
					idle_timeout_abs =
						TimestampTzPlusMilliseconds(now,
													io_worker_idle_timeout);
					timeout_guc_used = io_worker_idle_timeout;
				}

				/*
				 * All workers maintain the absolute timeout value, but only
				 * the highest worker can actually time out and only if
				 * io_min_workers is satisfied.  All others wait only for
				 * explicit wakeups caused by queue insertion, wakeup
				 * propagation, change of pool size (possibly promoting one to
				 * new highest) or GUC reload.
				 */
				if (pgaio_worker_can_timeout())
					timeout_ms =
						TimestampDifferenceMilliseconds(now,
														idle_timeout_abs);
				else
					timeout_ms = -1;
			}

#ifdef PGAIO_WORKER_SHOW_PS_INFO
			sprintf(cmd, "%d: idle, ios:wakeups = %d:%d",
					MyIoWorkerId, ios, wakeups);
			set_ps_display(cmd);
#endif

			if (WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
						  timeout_ms,
						  WAIT_EVENT_IO_WORKER_MAIN) == WL_TIMEOUT)
			{
				/* WL_TIMEOUT */
				if (pgaio_worker_can_timeout())
					if (GetCurrentTimestamp() >= idle_timeout_abs)
						break;
			}
			else
			{
				/* WL_LATCH_SET */
				if (++wakeups == PGAIO_WORKER_STATS_MAX)
				{
					ios /= 2;
					wakeups /= 2;
				}
			}
			ResetLatch(MyLatch);
		}

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/* If io_max_workers has been decreased, exit highest first. */
			if (MyIoWorkerId >= io_max_workers)
				break;
		}
	}

	error_context_stack = errcallback.previous;
	proc_exit(0);
}

bool
pgaio_workers_enabled(void)
{
	return io_method == IOMETHOD_WORKER;
}
