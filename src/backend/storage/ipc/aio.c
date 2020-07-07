/*
 *
 * Should-be state:
 *
 * - PG IOs acquired using dedicated lock
 * - PG IOs get queued in local submission queues
 * - PG IOs converted into uring IOs, submitted to shared ring, under
 *   submission lock
 * - uring completions are reaped under completion lock, PG IOs extracted,
 *   uring IOs are recycled
 * - PG IO completions are run
 */
#include "postgres.h"

//#define ILIST_DEBUG 1

#include <fcntl.h>
#include <liburing.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/memnodes.h"
#include "pgstat.h"
#include "storage/aio.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"


#define PGAIO_VERBOSE


/*
 * FIXME: This is just so large because merging happens when submitting
 * pending requests, rather than when staging them.
 */
#define PGAIO_SUBMIT_BATCH_SIZE 256
#define PGAIO_BACKPRESSURE_LIMIT (max_aio_in_flight - 128)
#define PGAIO_MAX_LOCAL_REAPED 128
#define PGAIO_MAX_COMBINE 16


typedef enum PgAioAction
{
	/* intentionally the zero value, to help catch zeroed memory etc */
	PGAIO_INVALID = 0,

	PGAIO_NOP,
	PGAIO_FSYNC,
	PGAIO_FLUSH_RANGE,

	PGAIO_READ_BUFFER,
	PGAIO_WRITE_BUFFER,
	PGAIO_WRITE_WAL,
} PgAioAction;

typedef enum PgAioInProgressFlags
{
	/* request in the ->unused list */
	PGAIOIP_UNUSED = 1 << 0,

	/*  */
	PGAIOIP_IDLE = 1 << 1,

	/*  */
	PGAIOIP_IN_PROGRESS = 1 << 2,

	/* somewhere */
	PGAIOIP_PENDING = 1 << 3,

	/* request in kernel */
	PGAIOIP_INFLIGHT = 1 << 4,

	/* request reaped */
	PGAIOIP_REAPED = 1 << 5,

	/* shared completion callback was called */
	PGAIOIP_SHARED_CALLBACK_CALLED = 1 << 6,

	/* completed */
	PGAIOIP_DONE = 1 << 7,

	PGAIOIP_FOREIGN_DONE = 1 << 8,

	/* IO is merged with others */
	PGAIOIP_MERGE = 1 << 9,

	PGAIOIP_RETRY = 1 << 10,

	/* request failed completely */
	PGAIOIP_HARD_FAILURE = 1 << 11,

	/* request failed partly, e.g. a short write */
	PGAIOIP_SOFT_FAILURE = 1 << 12,

	PGAIOIP_SHARED_FAILED = 1 << 13,

	/* local completion callback was called */
	PGAIOIP_LOCAL_CALLBACK_CALLED = 1 << 14,

} PgAioInProgressFlags;

/* IO completion callback */
typedef bool (*PgAioCompletedCB)(PgAioInProgress *io);

typedef uint16 PgAioIPFlags;

struct PgAioInProgress
{
	/* PgAioAction, indexes PgAioCompletionCallbacks */
	PgAioAction type;

	PgAioIPFlags flags;

	bool user_referenced;
	bool system_referenced;

	/* which AIO ring is this entry active for */
	uint8 ring;

	/* index into allProcs, or PG_UINT32_MAX for process local IO */
	uint32 owner_id;

	/* the IOs result, depends on operation. E.g. the length of a read */
	int32 result;

	/*
	 * Single callback that can be registered on an IO to be called upon
	 * completion. Note that this is reset whenever an IO is recycled..
	 */
	PgAioOnCompletionLocalContext *on_completion_local;

	/*
	 * Membership in one of
	 * PgAioCtl->unused,
	 * PgAioPerBackend->unused,
	 * PgAioPerBackend->outstanding,
	 */
	dlist_node owner_node;

	/*
	 * Membership in
	 * PgAioPerBackend->pending,
	 * PgAioPerBackend->reaped,
	 * local_recycle_requests
	 * PgAioPerBackend->foreign_completed,
	 * PgAioPerBackend->local_completed
	 */
	dlist_node io_node;

	ConditionVariable cv;

	PgAioBounceBuffer *bb;

	PgAioInProgress *merge_with;

	uint64 generation;

	/*
	 * NB: Note that fds in here may *not* be relied upon for re-issuing
	 * requests (e.g. for partial reads/writes) - the fd might be from another
	 * process, or closed since. That's not a problem for IOs waiting to be
	 * issued only because the queue is flushed when closing an fd.
	 */
	union {
		struct
		{
			int fd;
			bool barrier;
			bool datasync;
		} fsync;

		struct
		{
			int fd;
			off_t nbytes;
			off_t offset;
		} flush_range;

		struct
		{
			uint32 offset;
			uint32 nbytes;
			uint32 already_done;
			int fd;
			char *bufdata;
			Buffer buf;
			AioBufferTag tag;
			int mode;
		} read_buffer;

		struct
		{
			uint32 offset;
			uint32 nbytes;
			uint32 already_done;
			int fd;
			char *bufdata;
			Buffer buf;
			AioBufferTag tag;
		} write_buffer;

		struct
		{
			int fd;
			uint32 offset;
			uint32 nbytes;
			uint32 already_done;
			char *bufdata;
			bool no_reorder;
		} write_wal;
	} d;
};

/* typedef in header */
struct PgAioBounceBuffer
{
	union
	{
		char buffer[BLCKSZ];
		dlist_node node;
	} d;
};

/*
 * XXX: Really want a proclist like structure that works with integer
 * offsets. Given the limited number of IOs ever existing, using full pointers
 * is completely unnecessary.
 */

typedef struct PgAioPerBackend
{
	/*
	 * Local unused IOs. There's only a limited number of these. Used to
	 * reduce overhead of the central unused list.
	 *
	 * FIXME: Actually use.
	 *
	 * Could be singly linked list.
	 *
	 * PgAioInProgress->owner_node
	 */
	dlist_head unused;
	uint32 unused_count;

	/*
	 * IOs handed out to code within the backend.
	 *
	 * PgAioInProgress->owner_node
	 */
	dlist_head outstanding;
	uint32 outstanding_count;

	/*
	 * Requests waiting to be issued to the kernel. They are submitted to the
	 * kernel in batches, for efficiency (local merging of IOs, and better
	 * kernel side queue processing).
	 *
	 * Could be singly linked list.
	 *
	 * PgAioInProgress->io_node
	 */
	dlist_head pending;
	uint32 pending_count;

	/*
	 * PgAioInProgress that are issued to the ringbuffer, and have not yet
	 * been processed (but they may have completed without the completions
	 * having been processed).
	 */
	pg_atomic_uint32 inflight;

	/*
	 * Requests where we've received a kernel completion, but haven't yet
	 * processed them.  This is needed to handle failing callbacks.
	 *
	 * Could be singly linked list.
	 *
	 * PgAioInProgress->io_node
	 */
	dlist_head reaped;

	/*
	 * IOs that were completed, but not yet recycled.
	 *
	 * PgAioInProgress->io_node
	 */
	dlist_head local_completed;
	uint32 local_completed_count;

	/*
	 * IOs where the completion was received in another backend.
	 *
	 * Could be singly linked list.
	 *
	 * PgAioInProgress->io_node
	 */
	slock_t foreign_completed_lock;
	uint32 foreign_completed_count;
	dlist_head foreign_completed;

	/*
	 * Stats.
	 */
	uint64 executed_total_count; /* un-merged */
	uint64 issued_total_count; /* merged */
	uint64 submissions_total_count; /* number of submission syscalls */
	uint64 foreign_completed_total_count;
	uint64 retry_total_count;

} PgAioPerBackend;

typedef struct PgAioCtl
{
	/* PgAioInProgress that are not used */
	dlist_head unused_ios;

	/*
	 * Number of PgAioInProgressIOs that are in use. This includes pending
	 * requests, as well as requests actually issues to the queue.
	 *
	 * Protected by SharedAIOCtlLock.
	 */
	uint32 used_count;

	dlist_head reaped_uncompleted;

	/*
	 * FIXME: there should be multiple rings, at least one for data integrity
	 * writes, allowing efficient interleaving with WAL writes, and one for
	 * the rest. But likely a small number of non integrity rings too.
	 *
	 * It could also make sense to have a / a few rings for specific purposes
	 * like prefetching, and vacuum too. Configuring different depths could be
	 * a nice tool to manage maximum overall system impact (in particular by
	 * limiting queue/inflight operations size).
	 */
	struct io_uring shared_ring;

	dlist_head bounce_buffers;

	int backend_state_count;
	PgAioPerBackend *backend_state;

	PgAioInProgress in_progress_io[FLEXIBLE_ARRAY_MEMBER];
} PgAioCtl;

/* general pgaio helper functions */
static void pgaio_complete_ios(bool in_error);
static void pgaio_backpressure(struct io_uring *ring, const char *loc);
static void pgaio_prepare_io(PgAioInProgress *io, PgAioAction action);
static void pgaio_finish_io(PgAioInProgress *io);
static void pgaio_bounce_buffer_release_locked(PgAioInProgress *io);

/* io_uring related functions */
static int pgaio_uring_submit(bool drain);
static int pgaio_uring_drain(struct io_uring *ring);
static void pgaio_uring_wait_one(struct io_uring *ring, PgAioInProgress *io, uint32 wait_event_info);

static int pgaio_uring_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe, struct iovec **iovs);
static void pgaio_uring_io_from_cqe(struct io_uring_cqe *cqe);

static int __sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
								unsigned flags, sigset_t *sig);

/* io completions */
static bool pgaio_complete_nop(PgAioInProgress *io);
static bool pgaio_complete_fsync(PgAioInProgress *io);
static bool pgaio_complete_flush_range(PgAioInProgress *io);
static bool pgaio_complete_read_buffer(PgAioInProgress *io);
static bool pgaio_complete_write_buffer(PgAioInProgress *io);
static bool pgaio_complete_write_wal(PgAioInProgress *io);


static MemoryContext aio_retry_context;

/*
 * To support EXEC_BACKEND environments, where we cannot rely on callback
 * addresses being equivalent across processes, completion actions are just
 * indices into a process local array of callbacks, indexed by the type of
 * action.  Also makes the shared memory entries a bit smaller, but that's not
 * a huge win.
 */
static const PgAioCompletedCB completion_callbacks[] =
{
	[PGAIO_NOP] = pgaio_complete_nop,
	[PGAIO_FSYNC] = pgaio_complete_fsync,
	[PGAIO_FLUSH_RANGE] = pgaio_complete_flush_range,
	[PGAIO_READ_BUFFER] = pgaio_complete_read_buffer,
	[PGAIO_WRITE_BUFFER] = pgaio_complete_write_buffer,
	[PGAIO_WRITE_WAL] = pgaio_complete_write_wal,
};


/* (future) GUC controlling global MAX number of in-progress IO entries */
extern int max_aio_in_progress;
extern int max_aio_in_flight;
extern int max_aio_bounce_buffers;
int max_aio_in_progress = 32768; /* XXX: Multiple of MaxBackends instead? */
int max_aio_in_flight = 4096;
int max_aio_bounce_buffers = 4096;

/* global list of in-progress IO */
static PgAioCtl *aio_ctl;

/* current backend's per-backend-state */
static PgAioPerBackend *my_aio;
static int my_aio_id;

/* FIXME: move into PgAioPerBackend / subsume into ->reaped */
static dlist_head local_recycle_requests;


/* io_uring local state */
struct io_uring local_ring;

#define PGAIO_URING_SUBMIT_MAX_IOVEC (PGAIO_SUBMIT_BATCH_SIZE * PGAIO_MAX_COMBINE)
struct iovec pgaio_uring_submit_iovecs[PGAIO_URING_SUBMIT_MAX_IOVEC];

static Size
AioCtlShmemSize(void)
{
	Size		sz;

	/* aio_ctl itself */
	sz = offsetof(PgAioCtl, in_progress_io);

	/* ios */
	sz = add_size(sz, mul_size(max_aio_in_progress, sizeof(PgAioInProgress)));

	return sz;
}

static Size
AioCtlBackendShmemSize(void)
{
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS;

	return mul_size(TotalProcs, sizeof(PgAioPerBackend));
}

static Size
AioBounceShmemSize(void)
{
	return add_size(BLCKSZ /* alignment padding */,
					mul_size(BLCKSZ, max_aio_bounce_buffers));
}

Size
AioShmemSize(void)
{
	return add_size(add_size(AioCtlShmemSize(), AioBounceShmemSize()),
					AioCtlBackendShmemSize());
}

void
AioShmemInit(void)
{
	bool		found;
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS;

	aio_ctl = (PgAioCtl *)
		ShmemInitStruct("PgAio", AioCtlShmemSize(), &found);

	if (!found)
	{
		memset(aio_ctl, 0, AioCtlShmemSize());

		dlist_init(&aio_ctl->unused_ios);
		dlist_init(&aio_ctl->reaped_uncompleted);

		for (int i = 0; i < max_aio_in_progress; i++)
		{
			PgAioInProgress *io = &aio_ctl->in_progress_io[i];

			ConditionVariableInit(&io->cv);
			dlist_push_tail(&aio_ctl->unused_ios, &io->owner_node);
			io->flags = PGAIOIP_UNUSED;
			io->system_referenced = true;
		}

		aio_ctl->backend_state_count = TotalProcs;
		aio_ctl->backend_state = (PgAioPerBackend *)
			ShmemInitStruct("PgAioBackend", AioCtlBackendShmemSize(), &found);
		memset(aio_ctl->backend_state, 0, AioCtlBackendShmemSize());

		for (int procno = 0; procno < TotalProcs; procno++)
		{
			PgAioPerBackend *bs = &aio_ctl->backend_state[procno];

			dlist_init(&bs->unused);
			dlist_init(&bs->outstanding);
			dlist_init(&bs->pending);
			pg_atomic_init_u32(&bs->inflight, 0);
			dlist_init(&bs->reaped);

			dlist_init(&bs->foreign_completed);
			SpinLockInit(&bs->foreign_completed_lock);
		}

		{
			char *p;
			PgAioBounceBuffer *buffers;

			dlist_init(&aio_ctl->bounce_buffers);
			p = ShmemInitStruct("PgAioBounceBuffers", AioBounceShmemSize(), &found);
			Assert(!found);
			buffers = (PgAioBounceBuffer *) TYPEALIGN(BLCKSZ, (uintptr_t) p);

			for (int i = 0; i < max_aio_bounce_buffers; i++)
			{
				PgAioBounceBuffer *bb = &buffers[i];

				memset(bb, 0, BLCKSZ);
				dlist_push_tail(&aio_ctl->bounce_buffers, &bb->d.node);
			}
		}

		{
			int ret;

			ret = io_uring_queue_init(max_aio_in_flight, &aio_ctl->shared_ring, 0);
			if (ret < 0)
				elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
		}

	}
}

void
pgaio_postmaster_init(void)
{
	/* FIXME: should also be allowed to use AIO */
	dlist_init(&local_recycle_requests);

	// XXX: could create a local queue here.

	/*
	 * Need to be allowed to re-open files during retries. Those can happen,
	 * e.g. when fsyncing WAL, within a critical section. Reopening files
	 * currently requires memory. So create a context with small reservation
	 * that's allowed to be used within a critical section.
	 */
	aio_retry_context = AllocSetContextCreate(TopMemoryContext,
											  "aio retry context",
											  1024,
											  1024,
											  1024);
	MemoryContextAllowInCriticalSection(aio_retry_context, true);
}

void
pgaio_postmaster_child_init_local(void)
{
	/*
	 *
	 */
	{
		int ret;

		ret = io_uring_queue_init(32, &local_ring, 0);
		if (ret < 0)
		{
			elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
		}
	}
}

static void
pgaio_postmaster_child_exit(int code, Datum arg)
{
	/* FIXME: handle unused */
	Assert(my_aio->outstanding_count == 0);
	Assert(dlist_is_empty(&my_aio->outstanding));

	Assert(my_aio->pending_count == 0);
	Assert(dlist_is_empty(&my_aio->pending));

	Assert(dlist_is_empty(&my_aio->reaped));

	Assert(my_aio->foreign_completed_count == 0);
	Assert(dlist_is_empty(&my_aio->foreign_completed));
}

void
pgaio_postmaster_child_init(void)
{
	/* no locking needed here, only affects this process */
	io_uring_ring_dontfork(&aio_ctl->shared_ring);

	my_aio_id = MyProc->pgprocno;
	my_aio = &aio_ctl->backend_state[my_aio_id];

	dlist_init(&local_recycle_requests);

	on_shmem_exit(pgaio_postmaster_child_exit, 0);

	Assert(my_aio->unused_count == 0);
	Assert(my_aio->outstanding_count == 0);
	Assert(my_aio->pending_count == 0);
	Assert(my_aio->local_completed_count == 0);
	Assert(my_aio->foreign_completed_count == 0);

	my_aio->executed_total_count = 0;
	my_aio->issued_total_count = 0;
	my_aio->submissions_total_count = 0;
	my_aio->foreign_completed_total_count = 0;
	my_aio->retry_total_count = 0;
}

void
pgaio_at_abort(void)
{
	pgaio_complete_ios(/* in_error = */ true);

	pgaio_submit_pending(true);

	while (!dlist_is_empty(&my_aio->outstanding))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->outstanding);

		if (!pgaio_io_done(io))
			pgaio_io_wait(io, true);
		pgaio_io_release(io);
	}
}

void
pgaio_at_commit(void)
{
	Assert(dlist_is_empty(&local_recycle_requests));

	while (!dlist_is_empty(&my_aio->outstanding))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->outstanding);

		elog(WARNING, "leaked io %zu", io - aio_ctl->in_progress_io);
		if (!pgaio_io_done(io))
			pgaio_io_wait(io, true);
		pgaio_io_release(io);
	}
}

static int
pgaio_uncombine_one(PgAioInProgress *io)
{
	int orig_result = io->result;
	int running_result = orig_result;
	PgAioInProgress *cur = io;
	PgAioInProgress *last = NULL;
	int extracted = 0;

	while (cur)
	{
		PgAioInProgress *next = cur->merge_with;

		Assert(!(cur->flags & PGAIOIP_SHARED_CALLBACK_CALLED));
		Assert(cur->merge_with || cur != io);

		switch (cur->type)
		{
			case PGAIO_READ_BUFFER:
				Assert(cur->d.read_buffer.already_done == 0);

				if (orig_result < 0)
				{
					cur->result = io->result;
				}
				else if (running_result >= cur->d.read_buffer.nbytes)
				{
					cur->result = cur->d.read_buffer.nbytes;
					running_result -= cur->result;
				}
				else if (running_result < cur->d.read_buffer.nbytes)
				{
					cur->result = running_result;
					running_result = 0;
				}

				break;

			case PGAIO_WRITE_BUFFER:
				Assert(cur->d.write_buffer.already_done == 0);

				if (orig_result < 0)
				{
					cur->result = io->result;
				}
				else if (running_result >= cur->d.write_buffer.nbytes)
				{
					cur->result = cur->d.write_buffer.nbytes;
					running_result -= cur->result;
				}
				else if (running_result < cur->d.write_buffer.nbytes)
				{
					cur->result = running_result;
					running_result = 0;
				}
				break;

			case PGAIO_WRITE_WAL:
				Assert(cur->d.write_wal.already_done == 0);

				if (orig_result < 0)
				{
					cur->result = io->result;
				}
				else if (running_result >= cur->d.write_wal.nbytes)
				{
					cur->result = cur->d.write_wal.nbytes;
					running_result -= cur->result;
				}
				else if (running_result < cur->d.write_wal.nbytes)
				{
					cur->result = running_result;
					running_result = 0;
				}
				break;

			default:
				elog(PANIC, "merge for %d not supported yet", cur->type);
		}

		cur->merge_with = NULL;

		if (last)
		{
			cur->flags =
				(cur->flags & ~(PGAIOIP_INFLIGHT |
								PGAIOIP_MERGE)) |
				PGAIOIP_REAPED;

			Assert(dlist_is_member(&my_aio->reaped, &last->io_node));
			dlist_insert_after(&last->io_node, &cur->io_node);
			extracted++;
		}
		else
		{
			cur->flags &= ~PGAIOIP_MERGE;
		}

		last = cur;
		cur = next;
	}

	return extracted;
}

static void
pgaio_uncombine(void)
{
	dlist_mutable_iter iter;

	/* "unmerge" merged IOs, so they can be treated uniformly */
	dlist_foreach_modify(iter, &my_aio->reaped)
	{
		PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, iter.cur);
		uint32 extracted = 0;

		if (io->flags & PGAIOIP_MERGE)
			extracted += pgaio_uncombine_one(io);

		/*
		 * FIXME: this should a) probably not be here, b) only once for all
		 * the received IOs.
		 */
		pg_atomic_fetch_sub_u32(&aio_ctl->backend_state[io->owner_id].inflight,
								extracted + 1);
	}
}

static void  __attribute__((noinline))
pgaio_complete_ios(bool in_error)
{
	int pending_count_before = my_aio->pending_count;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	/* call all callbacks, without holding lock */
	while (!dlist_is_empty(&my_aio->reaped))
	{
		dlist_node *node = dlist_head_node(&my_aio->reaped);
		PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

		Assert(dlist_is_member(&my_aio->reaped, &io->io_node));

		Assert(node != NULL);

		if (!(io->flags & PGAIOIP_SHARED_CALLBACK_CALLED))
		{
			PgAioCompletedCB cb;
			bool finished;

			*(volatile PgAioIPFlags*) &io->flags |= PGAIOIP_SHARED_CALLBACK_CALLED;

			cb = completion_callbacks[io->type];
			finished = cb(io);

			dlist_delete_from(&my_aio->reaped, node);

			if (finished)
			{
				dlist_push_tail(&local_recycle_requests, &io->io_node);
			}
			else
			{
				Assert((*(volatile PgAioIPFlags*) &io->flags) & (PGAIOIP_SOFT_FAILURE | PGAIOIP_HARD_FAILURE));

				LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);
				*(volatile PgAioIPFlags*) &io->flags =
					(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
					PGAIOIP_DONE |
					PGAIOIP_SHARED_FAILED;
				dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
				LWLockRelease(SharedAIOCompletionLock);
			}

			/* signal state change */
			if (IsUnderPostmaster)
				ConditionVariableBroadcast(&io->cv);
		}
		else
		{
			Assert(in_error);

			dlist_delete_from(&my_aio->reaped, node);

			LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);
			*(volatile PgAioIPFlags*) &io->flags =
				(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
				PGAIOIP_DONE |
				PGAIOIP_HARD_FAILURE |
				PGAIOIP_SHARED_FAILED;
			dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
			LWLockRelease(SharedAIOCompletionLock);
		}
	}

	/* if any IOs weren't fully done, re-submit them */
	if (pending_count_before != my_aio->pending_count)
		pgaio_submit_pending(false);

	/*
	 * Next, under lock, process all the still pending requests. This entails
	 * releasing the "system" reference on the IO and checking which callbacks
	 * need to be called.
	 */
	START_CRIT_SECTION();

	while (!dlist_is_empty(&local_recycle_requests))
	{
		dlist_mutable_iter iter;
		PgAioInProgress* signal_ios[32];
		int to_signal = 0;

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

		dlist_foreach_modify(iter, &local_recycle_requests)
		{
			PgAioInProgress *cur = dlist_container(PgAioInProgress, io_node, iter.cur);

			dlist_delete_from(&local_recycle_requests, iter.cur);
			signal_ios[to_signal++] = cur;

			Assert(cur->system_referenced);
			Assert(cur->flags & PGAIOIP_REAPED);
			Assert(!(cur->flags & PGAIOIP_DONE));
			Assert(!(cur->flags & PGAIOIP_INFLIGHT));
			Assert(!(cur->flags & PGAIOIP_MERGE));
			Assert(!(cur->flags & (PGAIOIP_SHARED_FAILED)));
			Assert(!(cur->flags & (PGAIOIP_SOFT_FAILURE)));
			Assert(cur->merge_with == NULL);

			if (cur->user_referenced)
			{
				cur->system_referenced = false;

				if (cur->owner_id != my_aio_id)
				{
					PgAioPerBackend *other = &aio_ctl->backend_state[cur->owner_id];

					SpinLockAcquire(&other->foreign_completed_lock);

					dlist_push_tail(&other->foreign_completed, &cur->io_node);
					other->foreign_completed_count++;
					other->foreign_completed_total_count++;

					pg_write_barrier();

					*(volatile PgAioIPFlags*) &cur->flags =
						(cur->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
						PGAIOIP_DONE |
						PGAIOIP_FOREIGN_DONE;

					SpinLockRelease(&other->foreign_completed_lock);
				}
				else
				{
					*(volatile PgAioIPFlags*) &cur->flags =
						(cur->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
						PGAIOIP_DONE;

					dlist_push_tail(&my_aio->local_completed, &cur->io_node);
					my_aio->local_completed_count++;
				}
			}
			else
			{
				cur->flags = PGAIOIP_UNUSED;

				pgaio_bounce_buffer_release_locked(cur);

				cur->type = 0;
				cur->owner_id = INVALID_PGPROCNO;
				cur->result = 0;
				cur->system_referenced = true;
				cur->on_completion_local = NULL;
				cur->generation++;

				dlist_push_tail(&aio_ctl->unused_ios, &cur->owner_node);
				aio_ctl->used_count--;
			}

			if (to_signal >= lengthof(signal_ios))
				break;
		}
		LWLockRelease(SharedAIOCtlLock);

		if (IsUnderPostmaster)
		{
			for (int i = 0; i < to_signal; i++)
			{
				ConditionVariableBroadcast(&signal_ios[i]->cv);
			}
		}
	}

	END_CRIT_SECTION();
}

/*
 * Receive completions in ring.
 */
static int  __attribute__((noinline))
pgaio_drain(struct io_uring *ring)
{
	int ndrained = 0;

	START_CRIT_SECTION();

	ndrained = pgaio_uring_drain(ring);

	if (ndrained > 0)
		pgaio_uncombine();

	END_CRIT_SECTION();

	pgaio_complete_ios(false);

	/*
	 * Transfer all the foreign completions into the local queue.
	 */
	if (my_aio->foreign_completed_count != 0)
	{
		SpinLockAcquire(&my_aio->foreign_completed_lock);

		while (!dlist_is_empty(&my_aio->foreign_completed))
		{
			dlist_node *node = dlist_pop_head_node(&my_aio->foreign_completed);
			PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

			Assert(!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED));

			dlist_push_tail(&my_aio->local_completed, &io->io_node);
			io->flags &= ~PGAIOIP_FOREIGN_DONE;
			my_aio->foreign_completed_count--;
			my_aio->local_completed_count++;
		}
		SpinLockRelease(&my_aio->foreign_completed_lock);
	}

	/*
	 * Call all pending local callbacks.
	 */
	if (my_aio->local_completed_count != 0)
	{
		static int recursion_count  = 0;

		if (recursion_count == 0)
		{
			recursion_count++;

			while (!dlist_is_empty(&my_aio->local_completed))
			{
				dlist_node *node = dlist_pop_head_node(&my_aio->local_completed);
				PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

				Assert(!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED));
				io->flags |= PGAIOIP_LOCAL_CALLBACK_CALLED;
				my_aio->local_completed_count--;

				if (!io->on_completion_local)
					continue;

				io->on_completion_local->callback(io->on_completion_local, io);
			}

			recursion_count--;
		}
	}

	return ndrained;
}

static bool
pgaio_can_be_combined(PgAioInProgress *last, PgAioInProgress *cur)
{
	if (last->type != cur->type)
		return false;

	if (last->flags & PGAIOIP_RETRY ||
		cur->flags & PGAIOIP_RETRY)
		return false;

	switch (last->type)
	{
		case PGAIO_INVALID:
			elog(ERROR, "unexpected");
			break;

		case PGAIO_READ_BUFFER:
			if (last->d.read_buffer.fd != cur->d.read_buffer.fd)
				return false;
			if ((last->d.read_buffer.offset + last->d.read_buffer.nbytes) != cur->d.read_buffer.offset)
				return false;
			if (last->d.read_buffer.mode != cur->d.read_buffer.mode)
				return false;
			if (last->d.read_buffer.already_done != 0 || cur->d.read_buffer.already_done != 0)
				return false;

			return true;

		case PGAIO_NOP:
		case PGAIO_FLUSH_RANGE:
		case PGAIO_FSYNC:
		case PGAIO_WRITE_BUFFER:
			if (last->d.write_buffer.fd != cur->d.write_buffer.fd)
				return false;
			if ((last->d.write_buffer.offset + last->d.write_buffer.nbytes) != cur->d.write_buffer.offset)
				return false;
			if (last->d.write_buffer.already_done != 0 || cur->d.write_buffer.already_done != 0)
				return false;
			return true;

		case PGAIO_WRITE_WAL:
			if (last->d.write_wal.fd != cur->d.write_wal.fd)
				return false;
			if ((last->d.write_wal.offset + last->d.write_wal.nbytes) != cur->d.write_wal.offset)
				return false;
			if (last->d.write_wal.already_done != 0 || cur->d.write_wal.already_done != 0)
				return false;
			if (last->d.write_wal.no_reorder || cur->d.write_wal.no_reorder)
				return false;
			return true;
	}

	pg_unreachable();
}

static void
pgaio_io_merge(PgAioInProgress *into, PgAioInProgress *tomerge)
{
	elog(DEBUG3, "merging %zu to %zu",
		 tomerge - aio_ctl->in_progress_io,
		 into - aio_ctl->in_progress_io);

	into->merge_with = tomerge;
	into->flags |= PGAIOIP_MERGE;
}

static void
pgaio_combine_pending(void)
{
	dlist_mutable_iter iter;
	PgAioInProgress *last = NULL;
	int combined = 1;

	Assert(my_aio->pending_count > 1);

	dlist_foreach_modify(iter, &my_aio->pending)
	{
		PgAioInProgress *cur = dlist_container(PgAioInProgress, io_node, iter.cur);

		/* can happen when failing partway through io submission */
		if (cur->merge_with)
		{
			elog(DEBUG1, "already merged request (%zu), giving up on merging",
				 cur - aio_ctl->in_progress_io);
			return;
		}

		Assert(cur->merge_with == NULL);
		Assert(!(cur->flags & PGAIOIP_MERGE));

		if (last == NULL)
		{
			last = cur;
			continue;
		}

		if (pgaio_can_be_combined(last, cur))
		{
			combined++;

			dlist_delete_from(&my_aio->pending, &cur->io_node);

			cur->flags &= ~PGAIOIP_PENDING;
			my_aio->pending_count--;

			pgaio_io_merge(last, cur);
		}
		else
		{
			combined = 1;
		}

		if (combined >= PGAIO_MAX_COMBINE)
		{
			elog(DEBUG3, "max combine at %d", combined);
			last = NULL;
			combined = 1;
		}
		else
			last = cur;
	}
}

void  __attribute__((noinline))
pgaio_submit_pending(bool drain)
{
	int total_submitted = 0;
	uint32 orig_total;

	if (!aio_ctl || !my_aio)
		return;

	if (my_aio->pending_count == 0)
	{
		Assert(dlist_is_empty(&my_aio->pending));
		return;
	}

	HOLD_INTERRUPTS();

	orig_total = my_aio->pending_count;

#define COMBINE_ENABLED

#ifdef COMBINE_ENABLED
#if 0
	ereport(LOG, errmsg("before combine"),
			errhidestmt(true),
			errhidecontext(true));
	pgaio_print_list(&my_aio->pending, NULL, offsetof(PgAioInProgress, io_node));
#endif
	if (my_aio->pending_count > 1)
		pgaio_combine_pending();

#if 0
	ereport(LOG, errmsg("after combine"),
			errhidestmt(true),
			errhidecontext(true));
	pgaio_print_list(&my_aio->pending, NULL, offsetof(PgAioInProgress, io_node));
#endif
#endif /* COMBINE_ENABLED */


	total_submitted = pgaio_uring_submit(drain);

	my_aio->executed_total_count += orig_total;
	my_aio->issued_total_count += total_submitted;

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "submitted %d (orig %d)", total_submitted, orig_total);
#endif

	RESUME_INTERRUPTS();
}

static void  __attribute__((noinline))
pgaio_backpressure(struct io_uring *ring, const char *loc)
{
	pgaio_drain(ring);

	while (true)
	{
		uint32 inflight_before = pg_atomic_read_u32(&my_aio->inflight);

		if (inflight_before < PGAIO_BACKPRESSURE_LIMIT)
			break;

		/* first drain */
		{
			int total;
			int cqr_before = io_uring_cq_ready(ring);
			int used_before = aio_ctl->used_count;

			total = pgaio_drain(ring);

			elog(DEBUG1, "backpressure drain at %s:"
				 " cqr before/after: %d/%d"
				 ", my inflight b/a: %d/%d"
				 ", used b/a: %d/%d, processed %d",
				 loc,
				 cqr_before, io_uring_cq_ready(ring),
				 inflight_before, pg_atomic_read_u32(&my_aio->inflight),
				 used_before, aio_ctl->used_count,
				 total);

		}

		/* recheck */
		inflight_before = pg_atomic_read_u32(&my_aio->inflight);
		if (inflight_before < PGAIO_BACKPRESSURE_LIMIT)
			break;
		{
			int ret;
			int waitfor;
			int cqr_after;
			int cqr_before = io_uring_cq_ready(ring);
			int used_before = aio_ctl->used_count;

			/*
			 * FIXME: This code likely has a race condition, where the queue
			 * might be emptied after our check, but before we wait for events
			 * to be completed.  Using a lock around this could fix that, but
			 * we dont want that (both because others should be able to add
			 * requests, and because we'd rather have the kernel wake everyone
			 * up, if there's some readiness - it's quite likely multiple
			 * backends may wait for the same IO).
			 *
			 * Possible fix: While holding lock, register for CV for one of
			 * the inflight requests. Then, using appropriate sigmask'ery,
			 * wait until either that request is processed by somebody else,
			 * or a new completion is ready. The latter is much more likely.
			 */
			cqr_before = io_uring_cq_ready(ring);

			//waitfor = inflight_before - PGAIO_BACKPRESSURE_LIMIT;
			waitfor = 1;

			pgstat_report_wait_start(WAIT_EVENT_AIO_BACKPRESSURE);
			ret = __sys_io_uring_enter(ring->ring_fd,
									   0, waitfor,
									   IORING_ENTER_GETEVENTS, NULL);
			pgstat_report_wait_end();
			if (ret < 0 && errno != EINTR)
				elog(WARNING, "enter failed: %d/%s", ret, strerror(-ret));

			cqr_after = io_uring_cq_ready(ring);
#if 1
			elog(DEBUG2, "backpressure wait at %s waited for %d, "
				 "for inflight b/a: %d/%d, used b/a: %d/%d, "
				 "cqr before %d after %d "
				 "space left: %d, sq ready: %d",
				 loc, waitfor,
				 inflight_before, pg_atomic_read_u32(&my_aio->inflight),
				 used_before, aio_ctl->used_count,
				 cqr_before, io_uring_cq_ready(ring),
				 io_uring_sq_space_left(ring),
				 io_uring_sq_ready(ring));
#endif
			if (cqr_after)
				pgaio_drain(ring);
		}
	}
}

/*
 * FIXME: The !holding_reference implementation is a really ugly crock. It's
 * needed for things like WaitIO(), where we can't ensure that the io hasn't
 * already been freed and recycled by the time we check. And locking
 * preventing that would be prohibitively expensive.
 *
 * It'd be much better to implement it as something like:
 *
 * aio = buf->io_in_progress;
 * if (buf->io_in_progress)
 * {
 *     pgaio_io_wait_prepare(io);
 *     if (!buf->io_in_progress)
 *         continue;
 *     pgaio_io_wait(io);
 * ...

 * where pgaio_io_wait_prepare would start to wait on the IO's condition
 * variable, and the subsequent check of buf->io_in_progress would ensure that
 * wait on the wrong buffer.
 */
void
pgaio_io_wait(PgAioInProgress *io, bool holding_reference)
{
	PgAioIPFlags init_flags;
	PgAioIPFlags flags;
	uint32 done_flags = PGAIOIP_DONE | PGAIOIP_IDLE;
	uint64 init_generation = *(volatile uint64*) &io->generation;
	uint64 current_generation;

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "waiting for %zu",
		 io - aio_ctl->in_progress_io);
#endif

	init_flags = *(volatile PgAioIPFlags*) &io->flags;

	/*
	 * If the IO we're waiting for is ours and not yet submitted, submit
	 * now.
	 */
	if ((init_flags & PGAIOIP_PENDING) &&
		(!IsUnderPostmaster || io->owner_id == my_aio_id))
	{
		pgaio_submit_pending(false);
	}

again:

	current_generation = *(volatile uint64*) &io->generation;

	flags = *(volatile PgAioIPFlags*) &io->flags;

	if (!holding_reference)
	{
		/*
		 * If we're not possibly trying to reuse the IO, it's sufficient to
		 * wait for the callback to have been called. But if the backend might
		 * want to recycle the IO, that's not good enough.
		 */
		done_flags |= PGAIOIP_SHARED_CALLBACK_CALLED;

		/* possible due to racyness */
		done_flags |= PGAIOIP_UNUSED;

		if (!(flags & (PGAIOIP_IN_PROGRESS)))
			goto out;
	}
	else
	{
		Assert(io->user_referenced);

		/*
		 * Shouldn't wait our own IO when it's not in progress (otherwise this
		 * pgaio_io_function shouldn't have been called). We can however not
		 * assert this for the current set of flags, because if the IO was
		 * retried, the local completion callback could have been called, and
		 * recycled the IO. Thus check that the initial set of flags didn't
		 * include IDLE and that either the IO is IDLE or was recycled.
		 */
		Assert(!(init_flags & PGAIOIP_IDLE));
		Assert(!(flags & PGAIOIP_IDLE) ||
			   current_generation != init_generation);
	}

	/*
	 * When holding a reference the IO can't become unused. And if we're not,
	 * we should have exited above.
	 */
	Assert(!(flags & PGAIOIP_UNUSED));

	if (flags & done_flags)
	{
		goto out;
	}

	while (true)
	{
		current_generation = *(volatile uint64*) &io->generation;
		flags = *(volatile PgAioIPFlags*) &io->flags;

		if (current_generation != init_generation ||
			(flags & done_flags))
			break;

		pgaio_drain(&aio_ctl->shared_ring);

		current_generation = *(volatile uint64*) &io->generation;
		flags = *(volatile PgAioIPFlags*) &io->flags;

		if (current_generation != init_generation ||
			(flags & done_flags))
			break;

		if (my_aio->pending_count > 0)
		{
			/*
			 * If we otherwise would have to sleep submit all pending
			 * requests, to avoid others having to wait for us to submit
			 * them. Don't want to so when not needing to sleep, as submitting
			 * IOs in smaller increments can be less efficient.
			 */
			pgaio_submit_pending(false);
		}
		else if (flags & PGAIOIP_INFLIGHT)
		{
			/* note that this is allowed to spuriously return */
			pgaio_uring_wait_one(&aio_ctl->shared_ring, io,
								 WAIT_EVENT_AIO_IO_COMPLETE_ANY);
		}
		else
		{
			/* shouldn't be reachable without concurrency */
			Assert(IsUnderPostmaster);

			/* ensure we're going to get woken up */
			if (IsUnderPostmaster)
				ConditionVariablePrepareToSleep(&io->cv);

			current_generation = *(volatile uint64*) &io->generation;
			flags = *(volatile PgAioIPFlags*) &io->flags;

			if (!(current_generation != init_generation ||
				  (flags & done_flags)))
				ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_IO_COMPLETE_ONE);

			if (IsUnderPostmaster)
				ConditionVariableCancelSleep();
		}
	}

	flags = *(volatile PgAioIPFlags*) &io->flags;
	current_generation = *(volatile uint64*) &io->generation;

	Assert(init_generation != current_generation ||
		   flags & done_flags);

out:
	if (init_generation == current_generation &&
		(flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_HARD_FAILURE)))
	{
		/* can retry soft failures, but not hard ones */
		/* FIXME: limit number of soft retries */
		if (flags & PGAIOIP_SOFT_FAILURE)
		{
			pgaio_io_retry(io);

			goto again;
		}
		else
		{
			elog(WARNING, "request %zd failed permanently",
				 io - aio_ctl->in_progress_io);
		}
	}

	flags = *(volatile PgAioIPFlags*) &io->flags;
	current_generation = *(volatile uint64*) &io->generation;

	if (init_generation == current_generation &&
		holding_reference &&
		!(flags & PGAIOIP_LOCAL_CALLBACK_CALLED) &&
		io->on_completion_local)
	{
		pgaio_drain(&aio_ctl->shared_ring);
	}
}

PgAioInProgress *
pgaio_io_get(void)
{
	dlist_node *elem;
	PgAioInProgress *io;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	// FIXME: relax?
	Assert(my_aio->pending_count < PGAIO_SUBMIT_BATCH_SIZE);

	/* FIXME: wait for an IO to complete if full */

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	while (unlikely(dlist_is_empty(&aio_ctl->unused_ios)))
	{
		LWLockRelease(SharedAIOCtlLock);
		elog(DEBUG1, "needed to drain while getting IO (used %d inflight %d)",
			 aio_ctl->used_count, pg_atomic_read_u32(&my_aio->inflight));

		/*
		 * FIXME: should we wait for IO instead?
		 *
		 * Also, need to protect against too many ios handed out but not used.
		 */
		pgaio_drain(&aio_ctl->shared_ring);

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	}

	elem = dlist_pop_head_node(&aio_ctl->unused_ios);
	aio_ctl->used_count++;

	LWLockRelease(SharedAIOCtlLock);

	io = dlist_container(PgAioInProgress, owner_node, elem);

	Assert(io->flags == PGAIOIP_UNUSED);
	Assert(io->system_referenced);
	io->user_referenced = true;
	io->system_referenced = false;

	*(volatile PgAioIPFlags*) &io->flags = PGAIOIP_IDLE;

	io->owner_id = my_aio_id;

	dlist_push_tail(&my_aio->outstanding, &io->owner_node);
	my_aio->outstanding_count++;

	return io;
}

bool
pgaio_io_success(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(io->flags & PGAIOIP_DONE);

	if (io->flags & (PGAIOIP_HARD_FAILURE | PGAIOIP_SOFT_FAILURE))
		return false;

	/* FIXME: is this possible? */
	if (!(io->flags & PGAIOIP_SHARED_CALLBACK_CALLED))
		return false;

	return true;
}

bool
pgaio_io_done(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!(io->flags & PGAIOIP_UNUSED));

	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (io->flags & (PGAIOIP_DONE | PGAIOIP_IDLE))
		return true;

	return false;
}

/*
 * Register a completion callback that is executed locally in the backend that
 * initiated the IO, even if the the completion of the IO has been reaped by
 * another process (which executed the shared callback, unlocking buffers
 * etc).  This is mainly useful for AIO using code to promptly react to
 * individual IOs finishing, without having to individually check each of the
 * IOs.
 */
void
pgaio_io_on_completion_local(PgAioInProgress *io, PgAioOnCompletionLocalContext *ocb)
{
	Assert(io->flags & PGAIOIP_IDLE);
	Assert(io->on_completion_local == NULL);

	io->on_completion_local = ocb;
}

static int
reopen_buffered(const AioBufferTag *tag)
{
	uint32 off;
	SMgrRelation reln = smgropen(tag->rnode.node, tag->rnode.backend);

	return smgrfd(reln, tag->forkNum, tag->blockNum, &off);
}

void
pgaio_io_retry(PgAioInProgress *io)
{
	bool retryable = false;
	bool need_retry;

	switch (io->type)
	{
		case PGAIO_READ_BUFFER:
			retryable = true;
			break;

		case PGAIO_WRITE_BUFFER:
			retryable = true;
			break;

		default:
			break;
	}

	if (!retryable)
	{
		elog(WARNING, "non-retryable aio being retried");
		return;
	}

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	/* could concurrently have been unset / retried */
	if (io->flags & PGAIOIP_SHARED_FAILED)
	{
		Assert(!(io->flags & PGAIOIP_FOREIGN_DONE));

		dlist_delete(&io->io_node);

		io->flags =
			(io->flags & ~(PGAIOIP_SHARED_FAILED |
						   PGAIOIP_DONE |
						   PGAIOIP_FOREIGN_DONE |
						   PGAIOIP_SHARED_CALLBACK_CALLED |
						   PGAIOIP_LOCAL_CALLBACK_CALLED |
						   PGAIOIP_HARD_FAILURE |
						   PGAIOIP_SOFT_FAILURE)) |
			PGAIOIP_IN_PROGRESS |
			PGAIOIP_PENDING |
			PGAIOIP_RETRY;

		need_retry = true;
	}
	else
	{
		need_retry = false;
	}
	LWLockRelease(SharedAIOCtlLock);

	if (!need_retry)
	{
		ereport(LOG, errmsg("was about to retry %zd, but somebody else did already",
							io - aio_ctl->in_progress_io),
				errhidestmt(true),
				errhidecontext(true));
		pgaio_io_print(io, NULL);
		return;
	}

	switch (io->type)
	{
		case PGAIO_READ_BUFFER:
			io->d.read_buffer.fd = reopen_buffered(&io->d.read_buffer.tag);
			break;

		case PGAIO_WRITE_BUFFER:
			io->d.write_buffer.fd = reopen_buffered(&io->d.write_buffer.tag);
			break;

		default:
			break;
	}

	dlist_push_tail(&my_aio->pending, &io->io_node);
	my_aio->pending_count++;
	my_aio->retry_total_count++;

	pgaio_submit_pending(true);
}

extern void
pgaio_io_recycle(PgAioInProgress *io)
{
	uint32 init_flags = *(volatile PgAioIPFlags*) &io->flags;

	Assert(init_flags & (PGAIOIP_IDLE | PGAIOIP_DONE));
	Assert(io->user_referenced);
	Assert(!io->system_referenced);
	Assert(io->merge_with == NULL);

	if (io->bb)
	{
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		pgaio_bounce_buffer_release_locked(io);
		LWLockRelease(SharedAIOCtlLock);
	}

	if (io->flags & PGAIOIP_DONE)
	{
		if (io->flags & PGAIOIP_FOREIGN_DONE)
		{
			SpinLockAcquire(&my_aio->foreign_completed_lock);
			dlist_delete_from(&my_aio->foreign_completed, &io->io_node);
			io->flags &= ~PGAIOIP_FOREIGN_DONE;
			my_aio->foreign_completed_count--;
			SpinLockRelease(&my_aio->foreign_completed_lock);
		}
		else if (!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED))
		{
			dlist_delete_from(&my_aio->local_completed, &io->io_node);
			my_aio->local_completed_count--;
		}
		io->flags &= ~PGAIOIP_DONE;
		io->flags |= PGAIOIP_IDLE;

		io->generation++;
	}

	io->flags &= ~(PGAIOIP_DONE |
				   PGAIOIP_MERGE |
				   PGAIOIP_SHARED_CALLBACK_CALLED |
				   PGAIOIP_LOCAL_CALLBACK_CALLED |
				   PGAIOIP_RETRY |
				   PGAIOIP_HARD_FAILURE |
				   PGAIOIP_SOFT_FAILURE);
	Assert(io->flags == PGAIOIP_IDLE);
	io->result = 0;
	io->on_completion_local = NULL;
}

static void  __attribute__((noinline))
pgaio_prepare_io(PgAioInProgress *io, PgAioAction action)
{
	/* true for now, but not necessarily in the future */
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->user_referenced);
	Assert(io->merge_with == NULL);

	Assert(my_aio->pending_count < PGAIO_SUBMIT_BATCH_SIZE);

	io->flags = (io->flags & ~PGAIOIP_IDLE) | PGAIOIP_IN_PROGRESS | PGAIOIP_PENDING;

	/* for this module */
	io->system_referenced = true;
	io->type = action;
	if (IsUnderPostmaster)
		io->owner_id = MyProc->pgprocno;

	// FIXME: should this be done in end_get_io?
	dlist_push_tail(&my_aio->pending, &io->io_node);
	my_aio->pending_count++;
}

static void  __attribute__((noinline))
pgaio_finish_io(PgAioInProgress *io)
{
	if (my_aio->pending_count >= PGAIO_SUBMIT_BATCH_SIZE)
		pgaio_submit_pending(true);
	else
		pgaio_backpressure(&aio_ctl->shared_ring, "get_io");
}


void
pgaio_io_release(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!IsUnderPostmaster || io->owner_id == MyProc->pgprocno);

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	io->user_referenced = false;

	dlist_delete_from(&my_aio->outstanding, &io->owner_node);
	my_aio->outstanding_count--;

	if (!io->system_referenced)
	{
		Assert(!(io->flags & PGAIOIP_INFLIGHT));
		Assert(!(io->flags & PGAIOIP_MERGE));
		Assert(io->flags & PGAIOIP_DONE ||
			   io->flags & PGAIOIP_IDLE);

		if (io->flags & PGAIOIP_DONE)
		{
			if (io->flags & PGAIOIP_FOREIGN_DONE)
			{
				SpinLockAcquire(&my_aio->foreign_completed_lock);
				Assert(io->flags & PGAIOIP_FOREIGN_DONE);
				dlist_delete_from(&my_aio->foreign_completed, &io->io_node);
				my_aio->foreign_completed_count--;
				SpinLockRelease(&my_aio->foreign_completed_lock);
			}
			else if (!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED))
			{
				dlist_delete_from(&my_aio->local_completed, &io->io_node);
				my_aio->local_completed_count--;
			}

			io->generation++;
		}

		io->flags = PGAIOIP_UNUSED;
		io->type = 0;
		io->owner_id = INVALID_PGPROCNO;
		io->result = 0;
		io->system_referenced = true;
		io->on_completion_local = NULL;

		Assert(io->merge_with == NULL);

		/* could do this earlier or conditionally */
		pgaio_bounce_buffer_release_locked(io);

		dlist_push_tail(&aio_ctl->unused_ios, &io->owner_node);
		aio_ctl->used_count--;
	}

	LWLockRelease(SharedAIOCtlLock);
}

void
pgaio_print_queues(void)
{
	uint32 inflight = 0;

	for (int procno = 0; procno < aio_ctl->backend_state_count; procno++)
	{
		PgAioPerBackend *bs = &aio_ctl->backend_state[procno];

		inflight += pg_atomic_read_u32(&bs->inflight);
	}

	ereport(LOG,
			errmsg("shared queue: space: %d ready: %d, we think: %u inflight",
				   io_uring_sq_space_left(&aio_ctl->shared_ring),
				   io_uring_cq_ready(&aio_ctl->shared_ring),
				   inflight),
			errhidestmt(true),
			errhidecontext(true)
		);
}

static const char *
pgaio_io_action_string(PgAioAction a)
{
	switch(a)
	{
		case PGAIO_INVALID:
			return "invalid";
		case PGAIO_NOP:
			return "nop";
		case PGAIO_FLUSH_RANGE:
			return "flush_range";
		case PGAIO_FSYNC:
			return "fsync";
		case PGAIO_READ_BUFFER:
			return "read_buffer";
		case PGAIO_WRITE_BUFFER:
			return "write_buffer";
		case PGAIO_WRITE_WAL:
			return "write_wal";
	}

	pg_unreachable();
}


static void
pgaio_io_flag_string(PgAioIPFlags flags, StringInfo s)
{
	bool first = true;

#define STRINGIFY_FLAG(f) if (flags & f) {  appendStringInfoString(s, first ? CppAsString(f) : " | " CppAsString(f)); first = false;}

	STRINGIFY_FLAG(PGAIOIP_UNUSED);
	STRINGIFY_FLAG(PGAIOIP_IDLE);
	STRINGIFY_FLAG(PGAIOIP_IN_PROGRESS);
	STRINGIFY_FLAG(PGAIOIP_PENDING);
	STRINGIFY_FLAG(PGAIOIP_INFLIGHT);
	STRINGIFY_FLAG(PGAIOIP_REAPED);
	STRINGIFY_FLAG(PGAIOIP_SHARED_CALLBACK_CALLED);
	STRINGIFY_FLAG(PGAIOIP_LOCAL_CALLBACK_CALLED);

	STRINGIFY_FLAG(PGAIOIP_DONE);
	STRINGIFY_FLAG(PGAIOIP_FOREIGN_DONE);

	STRINGIFY_FLAG(PGAIOIP_MERGE);
	STRINGIFY_FLAG(PGAIOIP_RETRY);
	STRINGIFY_FLAG(PGAIOIP_HARD_FAILURE);
	STRINGIFY_FLAG(PGAIOIP_SOFT_FAILURE);
	STRINGIFY_FLAG(PGAIOIP_SHARED_FAILED);

#undef STRINGIFY_FLAG
}

static void
pgaio_io_action_desc(PgAioInProgress *io, StringInfo s)
{
	switch (io->type)
	{
		case PGAIO_FSYNC:
			appendStringInfo(s, "fd: %d, datasync: %d, barrier: %d",
							 io->d.fsync.fd,
							 io->d.fsync.datasync,
							 io->d.fsync.barrier);
			break;
		case PGAIO_FLUSH_RANGE:
			appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %llu",
							 io->d.flush_range.fd,
							 (unsigned long long) io->d.flush_range.offset,
							 (unsigned long long) io->d.flush_range.nbytes);
			break;
		case PGAIO_READ_BUFFER:
			appendStringInfo(s, "fd: %d, mode: %d, offset: %d, nbytes: %d, already_done: %d, buf/data: %d/%p",
							 io->d.read_buffer.fd,
							 io->d.read_buffer.mode,
							 io->d.read_buffer.offset,
							 io->d.read_buffer.nbytes,
							 io->d.read_buffer.already_done,
							 io->d.read_buffer.buf,
							 io->d.read_buffer.bufdata);
			break;
		case PGAIO_WRITE_BUFFER:
			appendStringInfo(s, "fd: %d, offset: %d, nbytes: %d, already_done: %d, buf/data: %d/%p",
							 io->d.write_buffer.fd,
							 io->d.write_buffer.offset,
							 io->d.write_buffer.nbytes,
							 io->d.write_buffer.already_done,
							 io->d.read_buffer.buf,
							 io->d.write_buffer.bufdata);
			break;
		case PGAIO_WRITE_WAL:
			appendStringInfo(s, "fd: %d, offset: %d, nbytes: %d, already_done: %d, bufdata: %p, no-reorder: %d",
							 io->d.write_wal.fd,
							 (int) io->d.write_wal.offset,
							 (int) io->d.write_wal.nbytes,
							 io->d.write_wal.already_done,
							 io->d.write_wal.bufdata,
							 io->d.write_wal.no_reorder);
			break;
		default:
			break;
	}
}

static void
pgaio_io_print_one(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "aio %zu: ring: %d, init: %d, flags: ",
					 io - aio_ctl->in_progress_io,
					 io->ring,
					 io->owner_id);
	pgaio_io_flag_string(io->flags, s);
	appendStringInfo(s, ", result: %d, user/system_referenced: %d/%d, action: %s (",
					 io->result,
					 io->user_referenced,
					 io->system_referenced,
					 pgaio_io_action_string(io->type));
	pgaio_io_action_desc(io, s);
	appendStringInfoString(s, ")");
}

void
pgaio_io_print(PgAioInProgress *io, StringInfo s)
{
	bool alloc = false;
	MemoryContext old_context;

	if (s == NULL)
	{
		old_context = MemoryContextSwitchTo(ErrorContext);
		s = makeStringInfo();
		alloc = true;
	}

	pgaio_io_print_one(io, s);

	{
		PgAioInProgress *cur = io;
		int nummerge = 0;

		if (cur->merge_with)
			appendStringInfoString(s, "\n  merge with:");

		while (cur->merge_with)
		{
			nummerge++;
			appendStringInfo(s, "\n    %d: ", nummerge);
			pgaio_io_print_one(cur->merge_with, s);

			cur = cur->merge_with;
		}
	}

	if (alloc)
	{
		ereport(LOG,
				errmsg("%s", s->data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s->data);
		pfree(s);
		MemoryContextReset(ErrorContext);
		MemoryContextSwitchTo(old_context);
	}
}

void
pgaio_print_list(dlist_head *head, StringInfo s, size_t offset)
{
	bool alloc = false;
	dlist_iter iter;
	bool first = true;
	MemoryContext old_context;

	if (s == NULL)
	{
		old_context = MemoryContextSwitchTo(ErrorContext);
		s = makeStringInfo();
		alloc = true;
	}

	dlist_foreach(iter, head)
	{
		PgAioInProgress *io = ((PgAioInProgress *) ((char *) (iter.cur) - offset));

		if (!first)
			appendStringInfo(s, "\n");
		first = false;

		pgaio_io_print(io, s);
	}

	if (alloc)
	{
		ereport(LOG,
				errmsg("%s", s->data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s->data);
		pfree(s);
		MemoryContextSwitchTo(old_context);
		MemoryContextReset(ErrorContext);
	}
}

PgAioBounceBuffer *
pgaio_bounce_buffer_get(void)
{
	PgAioBounceBuffer *bb = NULL;

	while (true)
	{
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		if (!dlist_is_empty(&aio_ctl->bounce_buffers))
		{
			dlist_node *node = dlist_pop_head_node(&aio_ctl->bounce_buffers);

			bb = dlist_container(PgAioBounceBuffer, d.node, node);
		}
		LWLockRelease(SharedAIOCtlLock);

		if (!bb)
			pgaio_drain(&aio_ctl->shared_ring);
		else
			break;
	}

	return bb;
}

static void
pgaio_bounce_buffer_release_locked(PgAioInProgress *io)
{
	Assert(LWLockHeldByMe(SharedAIOCtlLock));

	if (!io->bb)
		return;

	dlist_push_tail(&aio_ctl->bounce_buffers, &io->bb->d.node);
	io->bb = NULL;
}

char *
pgaio_bounce_buffer_buffer(PgAioBounceBuffer *bb)
{
	return bb->d.buffer;
}

void
pgaio_assoc_bounce_buffer(PgAioInProgress *io, PgAioBounceBuffer *bb)
{
	Assert(bb != NULL);
	Assert(io->bb == NULL);
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->user_referenced);

	io->bb = bb;
}

/* --------------------------------------------------------------------------------
 * io_uring related code
 * --------------------------------------------------------------------------------
 */

static int
pgaio_uring_submit(bool drain)
{
	PgAioInProgress *ios[PGAIO_SUBMIT_BATCH_SIZE];
	struct io_uring_sqe *sqe[PGAIO_SUBMIT_BATCH_SIZE];
	int total_submitted = 0;

	while (!dlist_is_empty(&my_aio->pending))
	{
		int nios = 0;
		int nsubmit = Min(my_aio->pending_count, PGAIO_SUBMIT_BATCH_SIZE);
		int inflight_add = 0;
		struct iovec *iovs = pgaio_uring_submit_iovecs;

		Assert(nsubmit != 0 && nsubmit <= my_aio->pending_count);
		LWLockAcquire(SharedAIOSubmissionLock, LW_EXCLUSIVE);

		for (int i = 0; i < nsubmit; i++)
		{
			dlist_node *node;
			PgAioInProgress *io;

			sqe[nios] = io_uring_get_sqe(&aio_ctl->shared_ring);

			if (!sqe[nios])
				break;

			node = dlist_pop_head_node(&my_aio->pending);
			io = dlist_container(PgAioInProgress, io_node, node);

			Assert(io->flags & PGAIOIP_PENDING);

			ios[nios] = io;
			inflight_add += pgaio_uring_sq_from_io(ios[nios], sqe[nios], &iovs);

			*(volatile PgAioIPFlags*) &io->flags =
				(io->flags & ~PGAIOIP_PENDING) | PGAIOIP_INFLIGHT;
			my_aio->pending_count--;
			nios++;
			total_submitted++;
		}

		if (nios > 0)
		{
			int ret;

			pg_atomic_add_fetch_u32(&my_aio->inflight, inflight_add);
			my_aio->submissions_total_count++;

	again:
			pgstat_report_wait_start(WAIT_EVENT_AIO_SUBMIT);
			ret = io_uring_submit(&aio_ctl->shared_ring);
			pgstat_report_wait_end();

			if (ret == -EINTR)
				goto again;

			if (ret < 0)
				elog(PANIC, "failed: %d/%s",
					 ret, strerror(-ret));
		}

		LWLockRelease(SharedAIOSubmissionLock);

		/*
		 * Others might have been waiting for this IO. Because it wasn't
		 * marked as in-flight until now, they might be waiting for the
		 * CV. Wake'em up.
		 */
		for (int i = 0; i < nios; i++)
		{
			PgAioInProgress *cur = ios[i];

			while (cur)
			{
				ConditionVariableBroadcast(&cur->cv);
				cur = cur->merge_with;
			}
		}

		/* check if there are completions we could process */
		if (drain)
			pgaio_drain(&aio_ctl->shared_ring);
	}

	return total_submitted;
}

static int
pgaio_uring_drain(struct io_uring *ring)
{
	uint32 processed = 0;
	int ready;

	Assert(!LWLockHeldByMe(SharedAIOCompletionLock));

	if (!io_uring_cq_ready(ring))
		return 0;

	LWLockAcquire(SharedAIOCompletionLock, LW_EXCLUSIVE);

	/*
	 * Don't drain more events than available right now. Otherwise it's
	 * plausible that one backend could get stuck, for a while, receiving CQEs
	 * without actually processing them.
	 */
	ready = io_uring_cq_ready(ring);

	while (ready > 0)
	{
		struct io_uring_cqe *reaped_cqes[PGAIO_MAX_LOCAL_REAPED];
		uint32 processed_one;

		processed_one =
			io_uring_peek_batch_cqe(ring,
									reaped_cqes,
									Min(PGAIO_MAX_LOCAL_REAPED, ready));
		Assert(processed_one <= ready);

		ready -= processed_one;
		processed += processed_one;

		for (int i = 0; i < processed_one; i++)
		{
			struct io_uring_cqe *cqe = reaped_cqes[i];

			pgaio_uring_io_from_cqe(cqe);

			io_uring_cqe_seen(ring, cqe);
		}
	}

	LWLockRelease(SharedAIOCompletionLock);

	return processed;
}

static void
pgaio_uring_wait_one(struct io_uring *ring, PgAioInProgress *io, uint32 wait_event_info)
{
	PgAioIPFlags flags;
	int ret;

	/* ensure we're going to get woken up */
	if (IsUnderPostmaster)
	{
		ConditionVariablePrepareToSleep(&io->cv);
		ResetLatch(MyLatch);
		PG_SETMASK(&BlockSig);
	}

	/*
	 * If the IO is still in progress (could have finished concurrently by
	 * another backend), wait for it using io_uring_enter.
	 */
	flags = *(volatile PgAioIPFlags*) &io->flags;
	if (flags & PGAIOIP_INFLIGHT)
	{

#ifdef PGAIO_VERBOSE
		elog(DEBUG3, "sys enter %zu, ready %d ",
			 io - aio_ctl->in_progress_io, io_uring_cq_ready(ring));
#endif

		/* wait for one io to be completed */
		errno = 0;
		pgstat_report_wait_start(wait_event_info);
		ret = __sys_io_uring_enter(ring->ring_fd,
								   0, 1,
								   IORING_ENTER_GETEVENTS, &UnBlockSig);
		pgstat_report_wait_end();

		if (ret < 0 && errno == EINTR)
		{
			elog(DEBUG3, "got interrupted");
		}
		else if (ret != 0)
			elog(WARNING, "unexpected: %d/%s: %m", ret, strerror(-ret));
	}

	if (IsUnderPostmaster)
	{
		PG_SETMASK(&UnBlockSig);
		ConditionVariableCancelSleep();
	}
}

static void
pgaio_uring_io_from_cqe(struct io_uring_cqe *cqe)
{
	PgAioInProgress *io;

	io = io_uring_cqe_get_data(cqe);
	Assert(io != NULL);
	Assert(io->flags & PGAIOIP_INFLIGHT);
	Assert(io->system_referenced);

	*(volatile PgAioIPFlags*) &io->flags = (io->flags & ~PGAIOIP_INFLIGHT) | PGAIOIP_REAPED;
	io->result = cqe->res;

	// FIXME: can't do that currently, in other backends list
	dlist_push_tail(&my_aio->reaped, &io->io_node);

	/*
	 * FIXME: needs to be removed at some point, this is effectively a
	 * critical section.
	 */
	if (cqe->res < 0)
	{
		elog(WARNING, "cqe: u: %p s: %d/%s f: %u",
			 io_uring_cqe_get_data(cqe),
			 cqe->res,
			 cqe->res < 0 ? strerror(-cqe->res) : "",
			 cqe->flags);
	}
}

/*
 * FIXME: These need to be deduplicated.
 */
static int
prep_read_buffer_iov(PgAioInProgress *io, struct io_uring_sqe *sqe, struct iovec *iovs)
{
	PgAioInProgress *cur;
	uint32 offset = io->d.read_buffer.offset;
	int	niov = 0;

	cur = io;
	while (cur)
	{
		offset += cur->d.write_buffer.already_done;
		iovs[niov].iov_base = cur->d.read_buffer.bufdata + cur->d.read_buffer.already_done;
		iovs[niov].iov_len = cur->d.read_buffer.nbytes - cur->d.read_buffer.already_done;

		niov++;

		if (niov > 0)
			cur->flags |= PGAIOIP_INFLIGHT;

		cur = cur->merge_with;
	}

	io_uring_prep_readv(sqe,
						io->d.read_buffer.fd,
						iovs,
						niov,
						offset);

	return niov;
}

static int
prep_write_buffer_iov(PgAioInProgress *io, struct io_uring_sqe *sqe, struct iovec *iovs)
{
	PgAioInProgress *cur;
	uint32 offset = io->d.write_buffer.offset;
	int	niov = 0;

	cur = io;
	while (cur)
	{
		offset += cur->d.write_buffer.already_done;
		iovs[niov].iov_base = cur->d.write_buffer.bufdata + cur->d.write_buffer.already_done;
		iovs[niov].iov_len = cur->d.write_buffer.nbytes - cur->d.write_buffer.already_done;

		niov++;

		if (niov > 0)
			cur->flags |= PGAIOIP_INFLIGHT;

		cur = cur->merge_with;
	}

	io_uring_prep_writev(sqe,
						 io->d.write_buffer.fd,
						 iovs,
						 niov,
						 offset);
	return niov;
}

static int
prep_write_wal_iov(PgAioInProgress *io, struct io_uring_sqe *sqe, struct iovec *iovs)
{
	PgAioInProgress *cur;
	uint32 offset = io->d.write_wal.offset;
	int	niov = 0;

	cur = io;
	while (cur)
	{
		offset += cur->d.write_wal.already_done;
		iovs[niov].iov_base = cur->d.write_wal.bufdata + cur->d.write_wal.already_done;
		iovs[niov].iov_len = cur->d.write_wal.nbytes - cur->d.write_wal.already_done;

		niov++;

		if (niov > 0)
			cur->flags |= PGAIOIP_INFLIGHT;

		cur = cur->merge_with;
	}

	io_uring_prep_writev(sqe,
						io->d.write_wal.fd,
						iovs,
						niov,
						offset);
	return niov;
}

static int
pgaio_uring_sq_from_io(PgAioInProgress *io, struct io_uring_sqe *sqe, struct iovec **iovs)
{
	int submitted = 1;

	switch (io->type)
	{
		case PGAIO_FSYNC:
			io_uring_prep_fsync(sqe,
								io->d.fsync.fd,
								io->d.fsync.datasync ? IORING_FSYNC_DATASYNC : 0);
			if (io->d.fsync.barrier)
				sqe->flags |= IOSQE_IO_DRAIN;
			break;

		case PGAIO_READ_BUFFER:
			submitted = prep_read_buffer_iov(io, sqe, *iovs);
			*iovs += submitted;
			//sqe->flags |= IOSQE_ASYNC;
			break;

		case PGAIO_WRITE_BUFFER:
			submitted = prep_write_buffer_iov(io, sqe, *iovs);
			*iovs += submitted;
			break;

		case PGAIO_FLUSH_RANGE:
			io_uring_prep_rw(IORING_OP_SYNC_FILE_RANGE,
							 sqe,
							 io->d.flush_range.fd,
							 NULL,
							 io->d.flush_range.nbytes,
							 io->d.flush_range.offset);
			sqe->sync_range_flags = SYNC_FILE_RANGE_WRITE;
			break;

		case PGAIO_WRITE_WAL:
			submitted = prep_write_wal_iov(io, sqe, *iovs);
			*iovs += submitted;
			if (io->d.write_wal.no_reorder)
				sqe->flags = IOSQE_IO_DRAIN;
			break;

		case PGAIO_NOP:
			elog(ERROR, "not yet");
			break;

		case PGAIO_INVALID:
			elog(ERROR, "invalid");
	}

	io_uring_sqe_set_data(sqe, io);

	return submitted;
}

static int
__sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
			 unsigned flags, sigset_t *sig)
{

# ifndef __NR_io_uring_enter
#  define __NR_io_uring_enter		426
# endif

	return syscall(__NR_io_uring_enter, fd, to_submit, min_complete,
			flags, sig, _NSIG / 8);
}



/* --------------------------------------------------------------------------------
 * Code dealing with specific IO types
 * --------------------------------------------------------------------------------
 */

void
pgaio_io_start_flush_range(PgAioInProgress *io, int fd, off_t offset, off_t nbytes)
{
	pgaio_prepare_io(io, PGAIO_FLUSH_RANGE);

	io->d.flush_range.fd = fd;
	io->d.flush_range.offset = offset;
	io->d.flush_range.nbytes = nbytes;

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_flush_range %zu: %d, %llu, %llu",
		 io - aio_ctl->in_progress_io,
		 fd, (unsigned long long) offset, (unsigned long long) nbytes);
#endif
}

void
pgaio_io_start_read_buffer(PgAioInProgress *io, const AioBufferTag *tag, int fd, uint32 offset, uint32 nbytes, char *bufdata, int buffno, int mode)
{
	pgaio_prepare_io(io, PGAIO_READ_BUFFER);

	io->d.read_buffer.buf = buffno;
	io->d.read_buffer.mode = mode;
	io->d.read_buffer.fd = fd;
	io->d.read_buffer.offset = offset;
	io->d.read_buffer.nbytes = nbytes;
	io->d.read_buffer.bufdata = bufdata;
	io->d.read_buffer.already_done = 0;
	memcpy(&io->d.read_buffer.tag, tag, sizeof(io->d.read_buffer.tag));

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_buffer_read %zu: "
		 "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 buffno,
		 bufdata);
#endif
}

void
pgaio_io_start_write_buffer(PgAioInProgress *io, const AioBufferTag *tag, int fd, uint32 offset, uint32 nbytes, char *bufdata, int buffno)
{
	pgaio_prepare_io(io, PGAIO_WRITE_BUFFER);

	io->d.write_buffer.buf = buffno;
	io->d.write_buffer.fd = fd;
	io->d.write_buffer.offset = offset;
	io->d.write_buffer.nbytes = nbytes;
	io->d.write_buffer.bufdata = bufdata;
	io->d.write_buffer.already_done = 0;
	memcpy(&io->d.write_buffer.tag, tag, sizeof(io->d.write_buffer.tag));

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_buffer_write %zu: "
		 "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 buffno,
		 bufdata);
#endif
}

void
pgaio_io_start_write_wal(PgAioInProgress *io, int fd, uint32 offset, uint32 nbytes, char *bufdata, bool no_reorder)
{
	pgaio_prepare_io(io, PGAIO_WRITE_WAL);

	io->d.write_wal.fd = fd;
	io->d.write_wal.no_reorder = no_reorder;
	io->d.write_wal.offset = offset;
	io->d.write_wal.nbytes = nbytes;
	io->d.write_wal.bufdata = bufdata;
	io->d.write_wal.already_done = 0;

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_write_wal %zu:"
		 "fd %d, off: %llu, bytes: %llu, no_reorder: %d, data %p",
		 io - aio_ctl->in_progress_io,
		 fd,
		 (unsigned long long) offset,
		 (unsigned long long) nbytes,
		 no_reorder,
		 bufdata);
#endif
}

void
pgaio_io_start_nop(PgAioInProgress *io)
{
	pgaio_prepare_io(io, PGAIO_NOP);
	pgaio_finish_io(io);
}

void
pgaio_io_start_fsync(PgAioInProgress *io, int fd, bool barrier)
{
	pgaio_prepare_io(io, PGAIO_FSYNC);
	io->d.fsync.fd = fd;
	io->d.fsync.barrier = barrier;
	io->d.fsync.datasync = false;
	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_fsync %zu:"
		 "fd %d, is_barrier: %d, is_datasync: %d",
		 io - aio_ctl->in_progress_io,
		 fd,
		 barrier,
		 false);
#endif
}

void
pgaio_io_start_fdatasync(PgAioInProgress *io, int fd, bool barrier)
{
	pgaio_prepare_io(io, PGAIO_FSYNC);
	io->d.fsync.fd = fd;
	io->d.fsync.barrier = barrier;
	io->d.fsync.datasync = true;
	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_fsync %zu:"
		 "fd %d, is_barrier: %d, is_datasync: %d",
		 io - aio_ctl->in_progress_io,
		 fd,
		 barrier,
		 true);
#endif
}

static bool
pgaio_complete_nop(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed nop");
#endif

	return true;
}

static bool
pgaio_complete_fsync(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed fsync: %zu",
		 io - aio_ctl->in_progress_io);
#endif
	if (io->result != 0)
		elog(PANIC, "fsync needs better error handling");

	return true;
}

static bool
pgaio_complete_flush_range(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed flush_range: %zu, %s",
		 io - aio_ctl->in_progress_io,
		 io->result < 0 ? strerror(-io->result) : "ok");
#endif

	return true;
}

static bool
pgaio_complete_read_buffer(PgAioInProgress *io)
{
	Buffer		buffer = io->d.read_buffer.buf;

	bool		call_completion;
	bool		failed;
	bool		done;

	/* great for torturing error handling */
#if 0
	if (io->result > 4096)
		io->result = 4096;
#endif

#ifdef PGAIO_VERBOSE
	elog(io->flags & PGAIOIP_RETRY ? DEBUG1 : DEBUG3,
		 "completed read_buffer: %zu, %d/%s, buf %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 io->d.read_buffer.buf);
#endif

	if (io->result != (io->d.read_buffer.nbytes - io->d.read_buffer.already_done))
	{
		MemoryContext old_context = MemoryContextSwitchTo(aio_retry_context);

		failed = true;

		//pgaio_io_print(io, NULL);

		if (io->result < 0)
		{
			if (io->result == EAGAIN || io->result == EINTR)
			{
				elog(PANIC, "need to implement retries for failed requests");
			}
			else
			{
				ereport(WARNING,
						errcode_for_file_access(),
						errmsg("could not read block %u in file \"%s\": %s",
							   io->d.read_buffer.tag.blockNum,
							   relpath(io->d.read_buffer.tag.rnode,
									   io->d.read_buffer.tag.forkNum),
							   strerror(-io->result)));
			}

			call_completion = true;
			done = true;
		}
		else
		{
			io->flags |= PGAIOIP_SOFT_FAILURE;
			call_completion = false;
			done = false;
			io->d.read_buffer.already_done += io->result;

			/*
			 * This is actually pretty common and harmless, happens when part
			 * of the block is in the kernel page cache, but the other
			 * isn't. So don't issue WARNING/ERROR, but just retry.
			 *
			 * While it can happen with single BLCKSZ reads (since they're
			 * bigger than typical page sizes), it's made much more likely by
			 * us combining reads.
			 *
			 * XXX: Should we handle repeated failures for the same blocks
			 * differently?
			 */
			ereport(DEBUG1,
					errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("aio %zd: could not read block %u in file \"%s\": read only %d of %d bytes (init: %d, cur: %d)",
						   io - aio_ctl->in_progress_io,
						   io->d.read_buffer.tag.blockNum,
						   relpath(io->d.read_buffer.tag.rnode,
								   io->d.read_buffer.tag.forkNum),
						   io->result, BLCKSZ,
						   io->owner_id, MyProc ? MyProc->pgprocno : INVALID_PGPROCNO));
		}

		MemoryContextSwitchTo(old_context);
		MemoryContextReset(aio_retry_context);
	}
	else
	{
		io->d.read_buffer.already_done += io->result;
		Assert(io->d.read_buffer.already_done == BLCKSZ);

		call_completion = true;
		failed = false;
		done = true;
	}

	if (call_completion && BufferIsValid(buffer))
		ReadBufferCompleteRead(buffer, io->d.read_buffer.mode, failed);

	return done;
}

static bool
pgaio_complete_write_buffer(PgAioInProgress *io)
{
	Buffer		buffer = io->d.write_buffer.buf;

	bool		call_completion;
	bool		failed;
	bool		done;

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed write_buffer: %zu, %d/%s, buf %d",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok",
		 io->d.write_buffer.buf);
#endif

	if (io->result != (io->d.write_buffer.nbytes - io->d.write_buffer.already_done))
	{
		MemoryContext old_context = MemoryContextSwitchTo(aio_retry_context);

		failed = true;

		if (io->result < 0)
		{
			int elevel ;

			failed = true;

			if (io->result == -EAGAIN || io->result == -EINTR)
			{
				io->flags |= PGAIOIP_SOFT_FAILURE;

				call_completion = false;
				done = false;

				elevel = DEBUG1;
			}
			else
			{
				io->flags |= PGAIOIP_HARD_FAILURE;
				elevel = WARNING;

				call_completion = true;
				done = true;

				pgaio_io_print(io, NULL);
			}

			ereport(elevel,
					errcode_for_file_access(),
					errmsg("aio %zd: could not write block %u in file \"%s\": %s",
						   io - aio_ctl->in_progress_io,
						   io->d.write_buffer.tag.blockNum,
						   relpath(io->d.write_buffer.tag.rnode,
								   io->d.write_buffer.tag.forkNum),
						   strerror(-io->result)),
					errhint("Check free disk space."));
		}
		else
		{
			io->flags |= PGAIOIP_SOFT_FAILURE;
			io->d.write_buffer.already_done += io->result;

			call_completion = false;
			done = false;

			ereport(WARNING,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("aio %zd: could not write block %u in file \"%s\": wrote only %d of %d bytes (init: %d, cur: %d)",
							io - aio_ctl->in_progress_io,
							io->d.write_buffer.tag.blockNum,
							relpath(io->d.write_buffer.tag.rnode,
									io->d.write_buffer.tag.forkNum),
							io->result, (io->d.write_buffer.nbytes - io->d.write_buffer.already_done),
							io->owner_id, MyProc ? MyProc->pgprocno : INVALID_PGPROCNO)));
		}

		MemoryContextSwitchTo(old_context);
		MemoryContextReset(aio_retry_context);
	}
	else
	{
		io->d.write_buffer.already_done += io->result;
		Assert(io->d.write_buffer.already_done == BLCKSZ);

		call_completion = true;
		failed = false;
		done = true;
	}

	if (call_completion && BufferIsValid(buffer))
		ReadBufferCompleteWrite(buffer, failed);

	return done;
}

static bool
pgaio_complete_write_wal(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed write_wal: %zu, %d/%s",
		 io - aio_ctl->in_progress_io,
		 io->result,
		 io->result < 0 ? strerror(-io->result) : "ok");
#endif

	if (io->result < 0)
	{
		if (io->result == EAGAIN || io->result == EINTR)
		{
			elog(WARNING, "need to implement retries");
		}

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to log file: %s",
						strerror(-io->result))));
	}
	else if (io->result != (io->d.write_wal.nbytes - io->d.write_wal.already_done))
	{
		/* FIXME: implement retries for short writes */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to log file: wrote only %d of %d bytes",
						io->result, (io->d.write_wal.nbytes - io->d.write_wal.already_done))));
	}

	/* FIXME: update xlog.c state */

	return true;
}


/* --------------------------------------------------------------------------------
 * SQL interface functions
 * --------------------------------------------------------------------------------
 */

Datum
pg_stat_get_aio_backends(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_AIO_BACKEND_COLS	12

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (int i = 0; i < aio_ctl->backend_state_count; i++)
	{
		PgAioPerBackend *bs = &aio_ctl->backend_state[i];
		Datum		values[PG_STAT_GET_AIO_BACKEND_COLS];
		bool		nulls[PG_STAT_GET_AIO_BACKEND_COLS];
		int			pid = ProcGlobal->allProcs[i].pid;

		if (pid == 0)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[ 0] = Int32GetDatum(pid);
		values[ 1] = Int64GetDatum(bs->executed_total_count);
		values[ 2] = Int64GetDatum(bs->issued_total_count);
		values[ 3] = Int64GetDatum(bs->submissions_total_count);
		values[ 4] = Int64GetDatum(bs->foreign_completed_total_count);
		values[ 5] = Int64GetDatum(bs->retry_total_count);
		values[ 6] = Int64GetDatum(pg_atomic_read_u32(&bs->inflight));
		values[ 7] = Int32GetDatum(bs->unused_count);
		values[ 8] = Int32GetDatum(bs->outstanding_count);
		values[ 9] = Int32GetDatum(bs->pending_count);
		values[10] = Int32GetDatum(bs->local_completed_count);
		values[11] = Int32GetDatum(bs->foreign_completed_count);


		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

Datum
pg_stat_get_aios(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_AIOS_COLS	8

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	StringInfoData tmps;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	initStringInfo(&tmps);

	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[i];
		Datum		values[PG_STAT_GET_AIOS_COLS];
		bool		nulls[PG_STAT_GET_AIOS_COLS];
		int			owner_pid;
		PgAioInProgressFlags flags = io->flags;

		if (flags & PGAIOIP_UNUSED)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[ 0] = Int32GetDatum(i);
		values[ 1] = PointerGetDatum(cstring_to_text(pgaio_io_action_string(io->type)));

		pgaio_io_flag_string(flags, &tmps);
		values[ 2] = PointerGetDatum(cstring_to_text(tmps.data));
		resetStringInfo(&tmps);

		values[ 3] = Int32GetDatum(io->ring);

		owner_pid = ProcGlobal->allProcs[io->owner_id].pid;
		values[ 4] = Int32GetDatum(owner_pid);

		values[ 5] = Int64GetDatum(io->generation);

		values[ 6] = Int32GetDatum(io->result);

		pgaio_io_action_desc(io, &tmps);
		values[ 7] = PointerGetDatum(cstring_to_text(tmps.data));
		resetStringInfo(&tmps);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
