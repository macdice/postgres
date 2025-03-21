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
 * When a batch of IOs is submitted, the lowest numbered idle worker is woken
 * up.  If it sees more work in the queue it wakes a peer to help, and so on
 * in a chain.  When a backlog is detected, the pool size is increased.  When
 * the highest numbered worker times out after a period of inactivity.
 *
 * This method of AIO is available in all builds on all operating systems, and
 * is the default.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/method_worker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

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
#define PGAIO_WORKER_STATS_MAX 64

typedef struct PgAioWorkerSubmissionQueue
{
	uint32		size;
	uint32		mask;
	uint32		head;
	uint32		tail;
	uint32		sqes[FLEXIBLE_ARRAY_MEMBER];
} PgAioWorkerSubmissionQueue;

typedef struct PgAioWorkerSlot
{
	ProcNumber	proc_number;
} PgAioWorkerSlot;

typedef struct PgAioWorkerControl
{
	/* Seen by postmaster */
	volatile bool new_worker_needed;

	/* Potected by AioWorkerSubmissionQueueLock. */
	uint64		idle_worker_mask;

	/* Protected by AioWorkerControlLock. */
	uint64		worker_set;
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
int			io_worker_launch_interval = 500;


static int	io_worker_queue_size = 64;
static int	MyIoWorkerId = -1;
static PgAioWorkerSubmissionQueue *io_worker_submission_queue;
static PgAioWorkerControl *io_worker_control;


static size_t
pgaio_worker_queue_shmem_size(int *queue_size)
{
	/* Round size up to next power of two so we can make a mask. */
	*queue_size = pg_nextpower2_32(io_worker_queue_size);

	return offsetof(PgAioWorkerSubmissionQueue, sqes) +
		sizeof(uint32) * *queue_size;
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
		io_worker_control->new_worker_needed = false;
		io_worker_control->worker_set = 0;
		io_worker_control->idle_worker_mask = 0;
		for (int i = 0; i < MAX_IO_WORKERS; ++i)
			io_worker_control->workers[i].proc_number = INVALID_PROC_NUMBER;
	}
}

static void
pgaio_worker_consider_new_worker(uint32 queue_depth)
{
	/*
	 * This is called from sites that don't hold AioWorkerControlLock, but it
	 * changes infrequently and an up to date value is not required for this
	 * heuristic purpose.
	 */
	if (!io_worker_control->new_worker_needed &&
		queue_depth >= io_worker_control->nworkers)
	{
		io_worker_control->new_worker_needed = true;
		SendPostmasterSignal(PMSIGNAL_IO_WORKER_CHANGE);
	}
}

/*
 * Called by a worker when the queue is empty, to try to prevent a delayed
 * reaction to a brief burst.  This races against the postmaster acting on the
 * old value if it was recently set to true, but that's OK, the ordering would
 * be indeterminate anyway even if we could use locks in the postmaster.
 */
static void
pgaio_worker_cancel_new_worker(void)
{
	io_worker_control->new_worker_needed = false;
}

/*
 * Called by the postmaster to check if a new worker is needed.
 */
bool
pgaio_worker_test_new_worker_needed(void)
{
	return io_worker_control->new_worker_needed;
}

/*
 * Called by the postmaster to check if a new worker is needed when it's ready
 * to launch one, and clear the flag.
 */
bool
pgaio_worker_clear_new_worker_needed(void)
{
	bool		result;

	result = io_worker_control->new_worker_needed;
	if (result)
		io_worker_control->new_worker_needed = false;

	return result;
}

static uint64
pgaio_worker_mask(int worker)
{
	return UINT64_C(1) << worker;
}

static void
pgaio_worker_add(uint64 *set, int worker)
{
	*set |= pgaio_worker_mask(worker);
}

static void
pgaio_worker_remove(uint64 *set, int worker)
{
	*set &= ~pgaio_worker_mask(worker);
}

#ifdef USE_ASSERT_CHECKING
static bool
pgaio_worker_in(uint64 set, int worker)
{
	return (set & pgaio_worker_mask(worker)) != 0;
}
#endif

static uint64
pgaio_worker_highest(uint64 set)
{
	return pg_leftmost_one_pos64(set);
}

static uint64
pgaio_worker_lowest(uint64 set)
{
	return pg_rightmost_one_pos64(set);
}

static int
pgaio_worker_pop(uint64 *set)
{
	int			worker;

	Assert(set != 0);
	worker = pgaio_worker_lowest(*set);
	pgaio_worker_remove(set, worker);
	return worker;
}

static int
pgaio_worker_choose_idle(void)
{
	uint64		idle_worker_mask;
	int			worker;

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	/*
	 * Workers only wake higher numbered workers, to try to encourage an
	 * ordering of wakeup:work ratios, reducing spurious wakeups in lower
	 * numbered workers.
	 */
	idle_worker_mask = io_worker_control->idle_worker_mask;
	if (MyIoWorkerId != -1)
		idle_worker_mask &= ~(pgaio_worker_mask(MyIoWorkerId) - 1);

	if (idle_worker_mask == 0)
		return -1;

	/* Find the lowest bit position, and clear it. */
	worker = pgaio_worker_lowest(idle_worker_mask);
	pgaio_worker_remove(&io_worker_control->idle_worker_mask, worker);

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
	 * had not yet removed it as of when we saw it in idle_worker_mask. That's
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

static uint32
pgaio_worker_submission_queue_consume(void)
{
	PgAioWorkerSubmissionQueue *queue;
	uint32		result;

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	queue = io_worker_submission_queue;
	if (queue->tail == queue->head)
		return UINT32_MAX;		/* empty */

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

static void
pgaio_worker_submit_internal(int num_staged_ios, PgAioHandle **staged_ios)
{
	PgAioHandle *synchronous_ios[PGAIO_SUBMIT_BATCH_SIZE];
	uint32		queue_depth;
	int			worker = -1;
	int			nsync = 0;

	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
	for (int i = 0; i < num_staged_ios; ++i)
	{
		Assert(!pgaio_worker_needs_synchronous_execution(staged_ios[i]));
		if (!pgaio_worker_submission_queue_insert(staged_ios[i]))
		{
			/*
			 * We'll do it synchronously, but only after we've sent as many as
			 * we can to workers, to maximize concurrency.
			 */
			synchronous_ios[nsync++] = staged_ios[i];
		}
		else if (worker == -1)
		{
			/* Choose an idle worker to wake up if we haven't already. */
			worker = pgaio_worker_choose_idle();

			pgaio_debug_io(DEBUG4, staged_ios[i],
						   "choosing worker %d",
						   worker);
		}
	}
	queue_depth = pgaio_worker_submission_queue_depth();
	LWLockRelease(AioWorkerSubmissionQueueLock);

	if (worker != -1)
		pgaio_worker_wake(worker);
	else
		pgaio_worker_consider_new_worker(queue_depth);

	/* Run whatever is left synchronously. */
	for (int i = 0; i < nsync; ++i)
	{
		/*
		 * Between synchronous IO operations, try again to enqueue as many as
		 * we can.
		 */
		if (i > 0)
		{
			worker = -1;

			LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
			while (i < nsync &&
				   pgaio_worker_submission_queue_insert(synchronous_ios[i]))
			{
				if (worker == -1)
					worker = pgaio_worker_choose_idle();
				i++;
			}
			LWLockRelease(AioWorkerSubmissionQueueLock);

			if (worker != -1)
				pgaio_worker_wake(worker);

			if (i == nsync)
				break;
		}

		pgaio_io_perform_synchronously(synchronous_ios[i]);
	}
}

static int
pgaio_worker_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];

		pgaio_io_prepare_submit(ioh);
	}

	pgaio_worker_submit_internal(num_staged_ios, staged_ios);

	return num_staged_ios;
}

/*
 * on_shmem_exit() callback that releases the worker's slot in
 * io_worker_control.
 */
static void
pgaio_worker_die(int code, Datum arg)
{
	uint64		notify_set;

	LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
	pgaio_worker_remove(&io_worker_control->idle_worker_mask, MyIoWorkerId);
	LWLockRelease(AioWorkerSubmissionQueueLock);

	LWLockAcquire(AioWorkerControlLock, LW_EXCLUSIVE);
	Assert(io_worker_control->workers[MyIoWorkerId].proc_number == MyProcNumber);
	io_worker_control->workers[MyIoWorkerId].proc_number = INVALID_PROC_NUMBER;
	Assert(pgaio_worker_in(io_worker_control->worker_set, MyIoWorkerId));
	pgaio_worker_remove(&io_worker_control->worker_set, MyIoWorkerId);
	notify_set = io_worker_control->worker_set;
	Assert(io_worker_control->nworkers > 0);
	io_worker_control->nworkers--;
	Assert(pg_popcount64(io_worker_control->worker_set) ==
		   io_worker_control->nworkers);
	LWLockRelease(AioWorkerControlLock);

	/* Notify other workers on pool change. */
	while (notify_set != 0)
		pgaio_worker_wake(pgaio_worker_pop(&notify_set));
}

/*
 * Register the worker in shared memory, assign MyIoWorkerId and register a
 * shutdown callback to release registration.
 */
static void
pgaio_worker_register(void)
{
	uint64		worker_set_inverted;
	uint64		old_worker_set;

	MyIoWorkerId = -1;

	LWLockAcquire(AioWorkerControlLock, LW_EXCLUSIVE);
	worker_set_inverted = ~io_worker_control->worker_set;
	if (worker_set_inverted != 0)
	{
		MyIoWorkerId = pgaio_worker_lowest(worker_set_inverted);
		if (MyIoWorkerId >= MAX_IO_WORKERS)
			MyIoWorkerId = -1;
	}
	if (MyIoWorkerId == -1)
		elog(ERROR, "couldn't find a free worker slot");

	Assert(io_worker_control->workers[MyIoWorkerId].proc_number ==
		   INVALID_PROC_NUMBER);
	io_worker_control->workers[MyIoWorkerId].proc_number = MyProcNumber;

	old_worker_set = io_worker_control->worker_set;
	Assert(!pgaio_worker_in(old_worker_set, MyIoWorkerId));
	pgaio_worker_add(&io_worker_control->worker_set, MyIoWorkerId);
	io_worker_control->nworkers++;
	Assert(pg_popcount64(io_worker_control->worker_set) ==
		   io_worker_control->nworkers);
	LWLockRelease(AioWorkerControlLock);

	/* Notify other workers on pool change. */
	while (old_worker_set != 0)
		pgaio_worker_wake(pgaio_worker_pop(&old_worker_set));

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
	uint64		worker_set;

	/* Serialize against pool sized changes. */
	LWLockAcquire(AioWorkerControlLock, LW_SHARED);
	worker_set = io_worker_control->worker_set;
	LWLockRelease(AioWorkerControlLock);

	if (MyIoWorkerId != pgaio_worker_highest(worker_set))
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

	MyBackendType = B_IO_WORKER;
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
		uint32		queue_depth;
		int			worker = -1;

		/*
		 * Try to get a job to do.
		 *
		 * The lwlock acquisition also provides the necessary memory barrier
		 * to ensure that we don't see an outdated data in the handle.
		 */
		LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
		io_index = pgaio_worker_submission_queue_consume();
		queue_depth = pgaio_worker_submission_queue_depth();
		if (io_index == UINT32_MAX)
		{
			/* Nothing to do.  Mark self idle. */
			pgaio_worker_add(&io_worker_control->idle_worker_mask,
							 MyIoWorkerId);
		}
		else
		{
			/* Got one.  Clear idle flag. */
			pgaio_worker_remove(&io_worker_control->idle_worker_mask,
								MyIoWorkerId);

			/*
			 * See if we should wake up a peer.  Only do this if this worker
			 * is not experiencing spurious wakeups itself, to end a chain of
			 * wasted scheduling.
			 */
			if (queue_depth > 0 && wakeups <= ios)
				worker = pgaio_worker_choose_idle();
		}
		LWLockRelease(AioWorkerSubmissionQueueLock);

		/* Propagate wakeups. */
		if (worker != -1)
			pgaio_worker_wake(worker);
		else if (wakeups <= ios)
			pgaio_worker_consider_new_worker(queue_depth);

		if (io_index != UINT32_MAX)
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
			pgaio_worker_cancel_new_worker();

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
				 * io_min_workers is exceeded.  All others wait only for
				 * explicit wakeups caused by queue insertion, wakeup
				 * propagation, change of pool size (possibly making them
				 * highest), or GUC reload.
				 */
				if (pgaio_worker_can_timeout())
					timeout_ms =
						TimestampDifferenceMilliseconds(now,
														idle_timeout_abs);
				else
					timeout_ms = -1;
			}

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
