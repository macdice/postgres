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
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
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
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "portability/instr_time.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/aio_subsys.h"
#include "storage/condition_variable.h"
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
	ConditionVariable space_cv;
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

StaticAssertDecl(PGAIO_WORKER_SET_BITS >= MAX_IO_WORKERS,
				 "PgAioWorkerSet too small");

typedef struct PgAioWorkerDepthStats
{
	PgAioWorkerSet jj
//	uint8		busy_target;
} PgAioWorkerDepthStats;

typedef struct PgAioWorkerControl
{
	/* Developer-only simulation of slow storage. */
	pg_atomic_uint64 limit_op_next_ns;
	pg_atomic_uint64 limit_read_next_ns;
	pg_atomic_uint64 limit_write_next_ns;
	int			limit_op_ns;
	int			limit_read_block_ns;
	int			limit_write_block_ns;
	bool		limit_enabled;

	/* Seen by postmaster */
	volatile bool grow;

	/* Protected by AioWorkerControlLock. */
	PgAioWorkerSet worker_set;
	int			nworkers;	/* XXX not needed? */
	PgAioWorkerSlot workers[MAX_IO_WORKERS];

	/* Protected by AioWorkerSubmissionQueueLock. */
	PgAioWorkerSet idle_worker_set;
	uint32 depth_at_wakeup[MAX_IO_WORKERS];
	PgAioWorkerDepthStats depth_curve[FLEXIBLE_ARRAY_MEMBER];
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
int			io_max_workers = 32;
int			io_worker_idle_timeout = 60000;
int			io_worker_launch_interval = 100;

/* Developer-only GUCs accessed with "debug_" prefixes. */
int			io_worker_limit_iops = 0;
int			io_worker_limit_read = 0;
int			io_worker_limit_write = 0;
int			io_worker_queue_size = 64;
bool		io_worker_overflow_sync = true;

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

static int
pgaio_worker_queue_size(void)
{
	/* Round size up to next power of two so we can make a mask. */
	return pg_nextpower2_32(io_worker_queue_size);
}

static size_t
pgaio_worker_queue_shmem_size(void)
{
	return offsetof(PgAioWorkerSubmissionQueue, sqes) +
		sizeof(uint32) * pgaio_worker_queue_size();
}

static size_t
pgaio_worker_control_shmem_size(void)
{
	return offsetof(PgAioWorkerControl, depth_busy_curve) +
		sizeof(uint16) * pgaio_worker_queue_size();
}

static size_t
pgaio_worker_shmem_size(void)
{
	size_t		sz;

	sz = pgaio_worker_queue_shmem_size();
	sz = add_size(sz, pgaio_worker_control_shmem_size());

	return sz;
}

static void
pgaio_worker_shmem_init(bool first_time)
{
	bool		found;
	int			queue_size = pgaio_worker_queue_size();

	io_worker_submission_queue =
		ShmemInitStruct("AioWorkerSubmissionQueue",
						pgaio_worker_queue_shmem_size(),
						&found);
	if (!found)
	{
		ConditionVariableInit(&io_worker_submission_queue->space_cv);
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
		for (int i = 0; i < queue_size; ++i)
			io_worker_control->depth_busy_curve[i] = i;

		assign_debug_io_worker_limit_iops(io_worker_limit_iops, NULL);
		assign_debug_io_worker_limit_read(io_worker_limit_read, NULL);
		assign_debug_io_worker_limit_write(io_worker_limit_write, NULL);
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

static void
pgaio_worker_compute_busy_target(void)
{
	int			busy_target;

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	busy_target = io_worker_control->depth_curve[queue_depth].busy_target;
	if (excessivebusy_target > 1 &&
		busy_target == MyIoWorkerId + 1 &&
	
}

#define PGAIO_WORKER_NONE -1
#define PGAIO_WORKER_GROW INT_MAX

/*
 * Choose an idle worker to wake up.  Returns a worker ID, PGAIO_WORKER_NONE if
 * statistics indicated that enough are running already, or PGAIO_WORKER_GROW
 * if a new worker is needed.
 */
static int
pgaio_worker_compute_wakeup(void)
{
	PgAioWorkerSet worker_set;
	int			worker;
	int			

	Assert(LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE));

	busy_target = io_worker_control->depth_curve[queue_depth].busy_target;

	if (pgaio_worker_set_count(&io_worker_control->busy_worker_set) < busy_target)
	{
		/* If there are no idle workers, start one if possible. */
		if (pgaio_worker_set_is_empty(&io_worker_control->idle_worker_set))
			return PGAIO_WORKER_GROW;

		/* Select the lowest idle worker to wake up. */
		worker = pgaio_worker_set_pop_lowest(&io_worker_control->idle_worker_set);

		/*
		 * Tell the worker what the queue depth was when we made this decision,
		 * so that it can provide feedback about spurious wakeups due to work
		 * stealing.
		 */
		io_worker_control->wakeup_depth[worker] = queue_depth;

		return worker;
	}

	return PGAIO_WORKER_NONE;
}

static void
pgaio_worker_apply_busy_adjustment(int pool_change)
{
	Assert(!LWLockHeldByMeInMode(AioWorkerSubmissionQueueLock);

	if (adjustment == PGAIO_WORKER_NONE)
		;
	else if (adjustment == PGAIO_WORKER_GROW)
		pgaio_worker_grow(true);
	else
		pgaio_worker_wake(pool_change);
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

static void
pgaio_worker_submit_internal(int num_staged_ios, PgAioHandle **staged_ios)
{
	PgAioHandle *synchronous_ios[PGAIO_SUBMIT_BATCH_SIZE];
	int			busy_adjustment;
	int			nsync = 0;

	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
	for (int i = 0; i < num_staged_ios; ++i)
	{
		Assert(!pgaio_worker_needs_synchronous_execution(staged_ios[i]));
		if (!pgaio_worker_submission_queue_insert(staged_ios[i]))
		{
			if (!io_worker_overflow_sync)
			{
				/* Wait for at least one IO to be drained and try again. */
				ConditionVariablePrepareToSleep(&io_worker_submission_queue->space_cv);
				LWLockRelease(AioWorkerSubmissionQueueLock);
				ConditionVariableSleep(&io_worker_submission_queue->space_cv,
									   WAIT_EVENT_AIO_WORKER_SUBMISSION);
				LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
				ConditionVariableCancelSleep();
				continue;
			}

			/*
			 * We'll do it synchronously, but only after we've sent as many as
			 * we can to workers, to maximize concurrency.
			 */
			synchronous_ios[nsync++] = staged_ios[i];
		}
	}
	busy_adjustment =
		pgaio_worker_compute_busy_adjustment(pgaio_worker_submission_queue_depth());
	LWLockRelease(AioWorkerSubmissionQueueLock);

	pgaio_worker_apply_busy_adjustment(adjustment);

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
					worker = pgaio_worker_choose_idle(0);
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
	PgAioWorkerSet notify_set;

	LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
	pgaio_worker_set_remove(&io_worker_control->busy, MyIoWorkerId);
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

static BlockNumber
pgaio_worker_get_block_count(PgAioHandle *ioh)
{
	if (ioh->op == PGAIO_OP_READV ||
		ioh->op == PGAIO_OP_WRITEV)
	{
		struct iovec *iov;
		size_t		len = 0;
		int			iovcnt;

		iovcnt = pgaio_io_get_iovec_length(ioh, &iov);
		for (int i = 0; i < iovcnt; ++i)
			len += iov[i].iov_len;

		return len / BLCKSZ;
	}

	return 0;
}

static void
pgaio_worker_wait(pg_atomic_uint64 *next_ns_p,
				  int delay_ns,
				  uint32 wait_event_info)
{
	uint64		now_ns = INSTR_TIME_GET_NANOSEC(pg_clock_gettime_ns());
	uint64		next_ns = pg_atomic_read_u64(next_ns_p);

	for (;;)
	{
		if (next_ns >= now_ns)
		{
			/* Need to wait.  Delay the next op further. */
			next_ns = pg_atomic_fetch_add_u64(next_ns_p, delay_ns);

			/* Value shouldn't ever go down. */
			Assert(next_ns >= now_ns);

			/* Average rate maintained even with low-res sleep or EINTR. */
			pgstat_report_wait_start(wait_event_info);
			pg_usleep(((next_ns - now_ns) + 999) / 1000);
			pgstat_report_wait_end();
			break;
		}
		else
		{
			/* Don't need to wait.  New next_ns is relative to now. */
			if (pg_atomic_compare_exchange_u64(next_ns_p,
											   &next_ns,
											   now_ns + delay_ns))
				break;
		}
	}
}

static void
pgaio_worker_limit_io(PgAioHandle *ioh)
{
	int			op_ns = io_worker_control->limit_op_ns;
	int			read_block_ns = io_worker_control->limit_read_block_ns;
	int			write_block_ns = io_worker_control->limit_write_block_ns;

	if (op_ns)
		pgaio_worker_wait(&io_worker_control->limit_op_next_ns,
						  op_ns,
						  WAIT_EVENT_AIO_WORKER_LIMIT_IOPS);
	if (read_block_ns && ioh->op == PGAIO_OP_READV)
		pgaio_worker_wait(&io_worker_control->limit_read_next_ns,
						  pgaio_worker_get_block_count(ioh) * read_block_ns,
						  WAIT_EVENT_AIO_WORKER_LIMIT_READ);
	if (write_block_ns && ioh->op == PGAIO_OP_WRITEV)
		pgaio_worker_wait(&io_worker_control->limit_write_next_ns,
						  pgaio_worker_get_block_count(ioh) * write_block_ns,
						  WAIT_EVENT_AIO_WORKER_LIMIT_WRITE);
}

static inline void
pgaio_worker_mark_idle(int worker)
{
	Assert(!pgaio_worker_set_contains(&io_worker_control->idle_worker_set, worker));
	Assert(pgaio_worker_set_contains(&io_worker_control->busy_worker_set, worker));
	pgaio_worker_set_remove(&io_worker_control->busy_worker_set, worker);
	pgaio_worker_set_insert(&io_worker_control->idle_worker_set, worker);
}

static inline void
pgaio_worker_assert_idle(int worker)
{
	Assert(pgaio_worker_set_contains(&io_worker_control->idle_worker_set, worker));
	Assert(!pgaio_worker_set_contains(&io_worker_control->busy_worker_set, worker));
}

static inline void
pgaio_worker_mark_busy(int worker)
{
	Assert(pgaio_worker_set_contains(&io_worker_control->idle_worker_set, worker));
	Assert(!pgaio_worker_set_contains(&io_worker_control->busy_worker_set, worker));
	pgaio_worker_set_remove(&io_worker_control->idle_worker_set, worker);
	pgaio_worker_set_insert(&io_worker_control->busy_worker_set, worker);
}

static inline void
pgaio_worker_assert_busy(int worker)
{
	Assert(!pgaio_worker_set_contains(&io_worker_control->idle_worker_set, worker));
	Assert(pgaio_worker_set_contains(&io_worker_control->busy_worker_set, worker));
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
	bool		wakeup_received = false;
	int			ratio_spurious = 0;
	int			ratio_opportunistic = 0;

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
		int			highest_busy_worker = -1;
		int			adjustment = PGAIO_WORKER_ADJUST_NONE;
		uint32		pool_change_depth = 0;

		/*
		 * Try to get a job to do.
		 *
		 * The lwlock acquisition also provides the necessary memory barrier
		 * to ensure that we don't see an outdated data in the handle.
		 */
		LWLockAcquire(AioWorkerSubmissionQueueLock, LW_EXCLUSIVE);
		if (wakeup_received)
			wakeup_depth = pgaio_worker_consume_wakeup_depth();
		if ((io_index = pgaio_worker_submission_queue_consume()) == -1)
		{
			if (wakeup_received)
				pgaio_worker_mark_idle(MyIoWorkerId);
			else
				pgaio_worker_assert_busy(MyIoWorkerId);
		}
		else
		{
			if (wakeup_received)
				pgaio_worker_mark_busy(MyIoWorkerId);
			else
				pgaio_worker_assert_busy(MyIoWorkerId);
		}
		wakeup = pgaio_worker_compute_wakeup(io_index, wakeup_depth);
		LWLockRelease(AioWorkerSubmissionQueueLock);

		/* Update statistics, and decide if we want to change busy_target. */
		if (busy_change_depth)
		{
			if (io_index == -1)
			{
		}
		else
		{
			/* We found work without being woken up. */
			if (ratio_ios == PGAIO_WORKER_STATS_MAX)
			{
				if (ratio_wakeups == 0 &&
					busy_change == PGAIO_WORKER_BUSY_CHANGE_NONE &&
					highest_busy_worker == 
				{
					/* We 
				}
			}
		}

		/* Communicate any adjustment in desired number of busy workers. */
		pgaio_worker_perform_wakeup(wakeup);

		if (io_index != -1)
		{
			PgAioHandle *ioh = NULL;

			/* If the queue was previously full, wake potential inserters. */
			if (queue_depth == io_worker_submission_queue->size - 2)
				ConditionVariableBroadcast(&io_worker_submission_queue->space_cv);

			/* Cancel timeout. */
			idle_timeout_abs = 0;

			/* Is this an opportunistic extra work cycle? */
			if (!wakeup_received)
			{
				if (ratio_opportunistic == PGAIO_WORKER_STATS_MAX)
				{
					if (ratio_spurious == 0)
					{
						/* We're being fully saturated with opportunistic work. */
					}
					ratio_spurious /= 2;
					ratio_opportunistic /= 2;
				}
				else
				{
					ratio_spurious++;
				}
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

			/* Simulate slow storage, if configured. */
			if (io_worker_control->limit_enabled)
				pgaio_worker_limit_io(ioh);

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

static void
assign_debug_io_worker_limit(int *wait_ns, int per_second)
{
	/*
	 * Deferred when called before io_method=worker is configured, and ignored
	 * permanently otherwise.  The GUC variables will still be assigned, and
	 * pgaio_worker_shmem_init() will call again.
	 */
	if (!io_worker_control)
		return;

	LWLockAcquire(AioWorkerControlLock, LW_EXCLUSIVE);
	*wait_ns = per_second == 0 ? 0 : NS_PER_S / per_second;
	io_worker_control->limit_enabled =
		io_worker_control->limit_op_ns > 0 ||
		io_worker_control->limit_read_block_ns > 0 ||
		io_worker_control->limit_write_block_ns > 0;
	LWLockRelease(AioWorkerControlLock);
}

void
assign_debug_io_worker_limit_iops(int newval, void *extra)
{
	assign_debug_io_worker_limit(&io_worker_control->limit_op_ns, newval);
}

void
assign_debug_io_worker_limit_read(int newval, void *extra)
{
	assign_debug_io_worker_limit(&io_worker_control->limit_read_block_ns, newval);
}

void
assign_debug_io_worker_limit_write(int newval, void *extra)
{
	assign_debug_io_worker_limit(&io_worker_control->limit_write_block_ns, newval);
}

static const char *
show_debug_io_worker_limit(const int *wait_ns)
{
	int			per_second;

	/* Zero if io_method=worker is not configured. */
	if (!io_worker_control)
		return "0";

	LWLockAcquire(AioWorkerControlLock, LW_SHARED);
	per_second = *wait_ns == 0 ? 0 : NS_PER_S / *wait_ns;
	LWLockRelease(AioWorkerControlLock);

	return psprintf("%d", per_second);
}

const char *
show_debug_io_worker_limit_iops(void)
{
	return show_debug_io_worker_limit(&io_worker_control->limit_op_ns);
}

const char *
show_debug_io_worker_limit_read(void)
{
	return show_debug_io_worker_limit(&io_worker_control->limit_read_block_ns);
}

const char *
show_debug_io_worker_limit_write(void)
{
	return show_debug_io_worker_limit(&io_worker_control->limit_write_block_ns);
}
