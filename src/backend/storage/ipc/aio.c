/*
 * Big picture changes:
 * - backend local recycleable IOs
 * - merging of IOs when submitting individual IOs, not when submitting all pending IOs
 * - reorganization of shared callback system, so there's an underlying
 *   "write" operation that's used both by WAL, generic, ...  writes.
 * - Consider not exposing PgAioInProgress* at all, instead expose a PgAioReference { uint32 io; uint64 generation; }
 *   which would make it a lot less problematic to immediate reuse IOs.
 * - Shrink size of PgAioInProgress
 * - refcount bounc buffers / redesign
 * - get rid of the current backpressure logic
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "access/xlog.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/squeue32.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/memnodes.h"
#include "pgstat.h"
#include "access/xlog_internal.h"
#include "postmaster/interrupt.h"
#include "storage/aio.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner_private.h"

#ifdef USE_LIBURING
#include <liburing.h>
#endif

#ifdef USE_POSIX_AIO
#include <aio.h>
#endif

#define PGAIO_VERBOSE


/*
 * FIXME: This is just so large because merging happens when submitting
 * pending requests, rather than when staging them.
 */
#define PGAIO_SUBMIT_BATCH_SIZE 256
#define PGAIO_MAX_LOCAL_REAPED 128
#define PGAIO_MAX_COMBINE 16

#define PGAIO_NUM_CONTEXTS 8

#ifdef __darwin__
/*
 * macOS includes an implementation of POSIX 1003.4 asynchronous I/O, without
 * the signal queueing mechanism that appeared in the same POSIX revision.
 * Bafflingly, it only supports signals as an I/O completion notification
 * mechanism.  That means you get signals but you have no way to know which I/O
 * completed.  The extra footwork required to figure that out by brute force is
 * controlled by this macro, so that those code paths can be easily tested on
 * non-macOS systems too.
 */
#define MISSING_SIGNAL_QUEUEING
#endif

/*
 * The type of AIO.
 *
 * To keep PgAioInProgress smaller try to tell the compiler to only use the
 * minimal space. We could alternatively just use a uint8, but then we'd need
 * casts in more places...
 */
typedef enum
#ifdef pg_attribute_packed
pg_attribute_packed()
#endif
	PgAioAction
{
	/* intentionally the zero value, to help catch zeroed memory etc */
	PGAIO_INVALID = 0,

	PGAIO_NOP,
	/* FIXME: unify */
	PGAIO_FSYNC,
	PGAIO_FSYNC_WAL,
	PGAIO_FLUSH_RANGE,

	PGAIO_READ_BUFFER,
	/* FIXME: unify */
	PGAIO_WRITE_BUFFER,
	PGAIO_WRITE_WAL,
	PGAIO_WRITE_GENERIC,
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

	/* can't use aio_suspend() to wait */
	PGAIOIP_POSIX_AIO_RETURNED = 1 << 15,

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
	 * PgAioPerBackend->issued,
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

	/* index into context->iovec, or -1 */
	int32 used_iovec;

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
			TimeLineID tli;
			XLogSegNo segno;
			bool barrier;
			bool datasync;
		} fsync;

		struct
		{
			int fd;
			TimeLineID tli;
			XLogSegNo segno;
			bool barrier;
			bool datasync;
			uint32 flush_no;
		} fsync_wal;

		struct
		{
			int fd;
			uint64 nbytes;
			uint32 offset;
			AioBufferTag tag;
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
			TimeLineID tli;
			XLogSegNo segno;
			uint32 offset;
			uint32 nbytes;
			uint32 already_done;
			char *bufdata;
			bool no_reorder;
			uint32 write_no;
		} write_wal;

		struct
		{
			int fd;
			uint64 offset;
			uint32 nbytes;
			uint32 already_done;
			char *bufdata;
			bool no_reorder;
		} write_generic;
	} d;

#ifdef USE_POSIX_AIO
	struct aiocb posix_aiocb;
	volatile sig_atomic_t posix_aio_nocheck;
	volatile sig_atomic_t posix_aio_returned;
#ifdef MISSING_SIGNAL_QUEUEING
	size_t inflight_slot;
#endif
#endif
};

/* typedef in header */
struct PgAioBounceBuffer
{
	pg_atomic_uint32 refcount;
	dlist_node node;
	char *buffer;
};

/*
 * An iovec that can represent the biggest possible iovec (due to combining)
 * we may need for a single IO submission.
 */
typedef struct PgAioIovec
{
	slist_node node;
	struct iovec iovec[PGAIO_MAX_COMBINE];
} PgAioIovec;


/*
 * XXX: Really want a proclist like structure that works with integer
 * offsets. Given the limited number of IOs ever existing, using full pointers
 * is completely unnecessary.
 */

typedef struct PgAioPerBackend
{
	uint32 last_context;

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
	 * Requests issued by backend that have not yet completed yet (but may be
	 * foreign_completed) and are still referenced by backend code (see
	 * issued_abandoned for those).
	 *
	 * PgAioInProgress->owner_node
	 */
	dlist_head issued;
	uint32 issued_count;

	/*
	 * Requests issued by backend that have not yet completed yet (but may be
	 * foreign_completed) and that are not referenced by backend code anymore (see
	 * issued for those).
	 *
	 * PgAioInProgress->owner_node
	 */
	dlist_head issued_abandoned;
	uint32 issued_abandoned_count;

	/*
	 * PgAioInProgress that are issued to the ringbuffer, and have not yet
	 * been processed (but they may have completed without the completions
	 * having been processed).
	 */
	pg_atomic_uint32 inflight_count;

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

typedef struct PgAioContext
{
#ifdef USE_LIBURING
	LWLock submission_lock;
	LWLock completion_lock;

	struct io_uring io_uring_ring;

	/*
	 * For many versions of io_uring iovecs need to be in shared memory. The
	 * lists of available iovecs are split to be under the submission /
	 * completion locks - that allows to avoid additional lock acquisitions in
	 * the common cases.
	 */
	PgAioIovec *iovecs;

	/* locked by submission lock */
	slist_head unused_iovecs;
	uint32 unused_iovecs_count;

	/* locked by completion lock */
	slist_head reaped_iovecs;
	uint32 reaped_iovecs_count;
#endif

	/* XXX: probably worth padding to a cacheline boundary here */
} PgAioContext;

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

	/*
	 * Protected by SharedAIOCtlLock.
	 */
	dlist_head reaped_uncompleted;

	PgAioBounceBuffer *bounce_buffers;
	dlist_head unused_bounce_buffers;
	uint32 unused_bounce_buffers_count;

	/*
	 * When using worker mode, these condition variables are used for sleeping
	 * on aio_submission_queue.
	 */
	ConditionVariable submission_queue_not_empty;
	ConditionVariable submission_queue_not_full;

	int backend_state_count;
	PgAioPerBackend *backend_state;

	uint32 num_contexts;
	PgAioContext *contexts;

	PgAioInProgress in_progress_io[FLEXIBLE_ARRAY_MEMBER];
} PgAioCtl;

/* Used for caching file descriptors between invocation. */
typedef struct PgAioWorkerState {
	int wal_fd;
	XLogSegNo wal_segno;
	TimeLineID wal_tli;
} PgAioWorkerState;

/* general pgaio helper functions */
static void pgaio_complete_ios(bool in_error);
static void pgaio_apply_backend_limit(void);
static void pgaio_prepare_io(PgAioInProgress *io, PgAioAction action);
static void pgaio_io_prepare_submit(PgAioInProgress *io, uint32 ring);
static void pgaio_finish_io(PgAioInProgress *io);
static void pgaio_bounce_buffer_release_internal(PgAioBounceBuffer *bb, bool holding_lock, bool release_resowner);
static void pgaio_io_ref_internal(PgAioInProgress *io, PgAioIoRef *ref);
static void pgaio_transfer_foreign_to_local(void);
static void pgaio_uncombine(void);
static int pgaio_uncombine_one(PgAioInProgress *io);
static void pgaio_call_local_callbacks(bool in_error);
static int pgaio_fill_iov(struct iovec *iovs, const PgAioInProgress *io);

/* aio worker related functions */
static int pgaio_worker_submit(bool drain, bool will_wait);
static void pgaio_worker_state_init(PgAioWorkerState *state);
static void pgaio_worker_do(PgAioWorkerState *state, PgAioInProgress *io);
static void pgaio_worker_state_close(PgAioWorkerState *state);

#ifdef USE_LIBURING
/* io_uring related functions */
static int pgaio_uring_submit(int max_submit, bool drain);
static int pgaio_uring_drain(PgAioContext *context);
static void pgaio_uring_wait_one(PgAioContext *context, PgAioInProgress *io, uint32 wait_event_info);

static void pgaio_uring_sq_from_io(PgAioContext *context, PgAioInProgress *io, struct io_uring_sqe *sqe);
static void pgaio_uring_io_from_cqe(PgAioContext *context, struct io_uring_cqe *cqe);
static void pgaio_uring_iovec_transfer(PgAioContext *context);

static int __sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete,
								unsigned flags, sigset_t *sig);
#endif

#ifdef USE_POSIX_AIO
/* POSIX AIO related functions */
static int pgaio_posix_aio_drain(PgAioContext *context, bool in_error);
static int pgaio_posix_submit(int max_submit, bool drain);
#endif

/* io completions */
static bool pgaio_complete_nop(PgAioInProgress *io);
static bool pgaio_complete_fsync(PgAioInProgress *io);
static bool pgaio_complete_fsync_wal(PgAioInProgress *io);
static bool pgaio_complete_flush_range(PgAioInProgress *io);
static bool pgaio_complete_read_buffer(PgAioInProgress *io);
static bool pgaio_complete_write_buffer(PgAioInProgress *io);
static bool pgaio_complete_write_wal(PgAioInProgress *io);
static bool pgaio_complete_write_generic(PgAioInProgress *io);

static const char * pgaio_io_action_string(PgAioAction a);

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
	[PGAIO_FSYNC_WAL] = pgaio_complete_fsync_wal,
	[PGAIO_FLUSH_RANGE] = pgaio_complete_flush_range,
	[PGAIO_READ_BUFFER] = pgaio_complete_read_buffer,
	[PGAIO_WRITE_BUFFER] = pgaio_complete_write_buffer,
	[PGAIO_WRITE_WAL] = pgaio_complete_write_wal,
	[PGAIO_WRITE_GENERIC] = pgaio_complete_write_generic,
};

/* GUCs */
int aio_type;
int aio_worker_queue_size;
int aio_workers;

/* (future) GUC controlling global MAX number of in-progress IO entries */
/* FIXME: find a good naming pattern */
extern int max_aio_in_progress;
/* FIXME: this is per context right now */
extern int max_aio_in_flight;
extern int max_aio_bounce_buffers;

/* max per backend concurrency */
extern int io_max_concurrency;

int max_aio_in_progress = 32768; /* XXX: Multiple of MaxBackends instead? */
int max_aio_in_flight = 4096;
int max_aio_bounce_buffers = 1024;
int io_max_concurrency = 128;

/* global list of in-progress IO */
static PgAioCtl *aio_ctl;

/* current backend's per-backend-state */
static PgAioPerBackend *my_aio;
static int my_aio_id;

/* FIXME: move into PgAioPerBackend / subsume into ->reaped */
static dlist_head local_recycle_requests;


#ifdef USE_LIBURING
/* io_uring local state */
struct io_uring local_ring;
#endif

/* Submission queue, used if aio_type is bgworker. */
squeue32 *aio_submission_queue;

#ifdef USE_POSIX_AIO
/* Shared completion queue, used if aio_type is posix. */
squeue32 *aio_completion_queue;

#ifdef MISSING_SIGNAL_QUEUEING
volatile sig_atomic_t *my_inflight_io;
volatile sig_atomic_t my_inflight_io_count;
#endif
#endif

/* Options for aio_type. */
const struct config_enum_entry aio_type_options[] = {
	{"worker", AIOTYPE_WORKER, false},
#ifdef USE_LIBURING
	{"io_uring", AIOTYPE_LIBURING, false},
#endif
#ifdef USE_POSIX_AIO
	{"posix", AIOTYPE_POSIX, false},
#endif
	{NULL, 0, false}
};

#ifdef USE_POSIX_AIO

/*
 * Maintain extra book keeping for macOS.
 */
static void
pgaio_posix_aio_preflight(PgAioInProgress *io)
{
#ifdef MISSING_SIGNAL_QUEUEING
	size_t io_index = io - aio_ctl->in_progress_io;

	/*
	 * We need a data structure that a signal handler can safely remove an
	 * element from.  For now, search for a free slot in an array of
	 * sig_atomic_t; it's a bit ugly, but we'll probably find a slot near the
	 * beginning.
	 */
	for (size_t slot = 0; slot < max_aio_in_flight; ++slot)
	{
		if (my_inflight_io[slot] == (sig_atomic_t) -1)
		{
			my_inflight_io[slot] = io_index;
			io->inflight_slot = slot;

			/* Track the upper bound to limit the signal handler's search. */
			if (slot >= my_inflight_io_count)
				my_inflight_io_count = slot + 1;

			return;
		}
	}

	elog(PANIC, "too many IOs in flight");
#endif
}

/*
 * Check if an IO that was initiated by us has completed, and if so, retrieve
 * the result and either put it on the shared completion queue for processing
 * by any process if requested, or handle completion immediately.
 *
 * When this runs in a signal handler, SIGIO is blocked (sigaction was called
 * without SA_NODEFER).  That shouldn't be necessary for correctness, since
 * squeue32 uses atomics in a way that works correctly in this case, but it's
 * probably better to prevent that anyway.  On macOS, it allows pending signals
 * delivered close together to be collapsed, clawing back some of the
 * performance lost due to the lack of signal queueing.  Even with signal
 * queuing, it's probably better to avoid the interrupts and retries.
 */
static void
pgaio_posix_aio_check(PgAioInProgress *io, bool to_shared_completion_queue)
{
	ssize_t	return_status;
	int		error_status;

	/* Spinlocks are not safe in this context. */
#ifdef PG_HAVE_ATOMIC_U64_SIMULATION
#error "Cannot use squeue32 from signal handler without atomics"
#endif

	Assert(io->owner_id == my_aio_id);

	/* Don't call aio_return() underneath aio_suspend()'s feet. */
	if (io->posix_aio_nocheck)
		return;

#ifdef MISSING_SIGNAL_QUEUEING
	/* Avoid some repeated polling on macOS by remembering we've done it. */
	if (io->posix_aio_returned)
		return;

	/*
	 * sig_atomic_t can be signed or unsigned, but it must at least big enough
	 * to hold indexes into in_progress_io.
	 */
	StaticAssertStmt(sizeof(sig_atomic_t) >= sizeof(int),
					 "sig_atomic_t too small to hold an AIO index");
#endif

	/* Check if the IO has completed and has an error status. */
	error_status = aio_error(&io->posix_aiocb);
	if (likely(error_status == 0))
	{
		/* Retrieve the return status and release kernel resource. */
		return_status = aio_return(&io->posix_aiocb);
		if (return_status < 0)
		{
			/* Concurrently handled by overlapping signal handler? */
			if (errno == EINVAL)
				return;
			elog(PANIC, "aio_return() failed after success: %m");
		}
		io->posix_aio_returned = true;
		io->result = return_status;
#ifdef MISSING_SIGNAL_QUEUEING
		my_inflight_io[io->inflight_slot] = (sig_atomic_t) -1;
#endif
	}
	else
	{
		/* Did aio_error() itself fail? */
		if (error_status < 0)
		{
			/*
			 * We were asked to check an IO that didn't actually have any IO
			 * outstanding?  Expected only on macOS.
			 */
			if (errno == EINVAL)
				return;
			elog(PANIC, "aio_error() failed: %m");
		}

		/*
		 * Still running?  Should only happen on macOS where we have to check
		 * every outstanding IO potentially repeatedly, or if a hand-crafted
		 * signal was sent, not from the kernel AIO system.
		 */
		if (error_status == EINPROGRESS)
			return;

		/* Release kernel resource. */
		return_status = aio_return(&io->posix_aiocb);
		if (return_status < 0)
		{
			/* Concurrently handled by overlapping signal handler? */
			if (errno == EINVAL)
				return;
			elog(PANIC, "aio_return() failed after error: %m");
		}

		/* Set the error using a negative result. */
		io->posix_aio_returned = true;
		io->result = -error_status;
#ifdef MISSING_SIGNAL_QUEUEING
		my_inflight_io[io->inflight_slot] = (sig_atomic_t) -1;
#endif
	}

	if (to_shared_completion_queue)
	{
		/*
		 * When running in a signal handler, we push this into the shared
		 * completion queue so that any backend can handle it.  This is also
		 * async-signal-safe, since it's based on atomic operations.
		 *
		 * XXX Explain theory about why the queue must have enough room for a
		 * newly completed IO.
		 */
		if (!squeue32_enqueue(aio_completion_queue,
							  io - &aio_ctl->in_progress_io[0]))
			elog(PANIC, "shared completion queue unexpectedly full");
	}
	else
	{
		/*
		 * When we've waited for an IO with aio_suspend(), we might as well put
		 * it on our own reaped list immediately to reduce contention on the
		 * shared memory queue.  And then we might as well run the completion
		 * callbacks too, just as a later call to pgaio_drain() would otherwise
		 * do.
		 */
		io->flags = (io->flags & ~PGAIOIP_INFLIGHT) | PGAIOIP_REAPED;
		dlist_push_tail(&my_aio->reaped, &io->io_node);
		pgaio_uncombine();
		pgaio_complete_ios(false);
		pgaio_transfer_foreign_to_local();
		pgaio_call_local_callbacks(false);
	}
}

/*
 * Whenever an IO initated by this process completes, we receive a signal.
 */
static void
pgaio_sigio_handler(int sig, siginfo_t *si, void *uap)
{
	int		save_errno = errno;

#ifndef MISSING_SIGNAL_QUEUEING
	PgAioInProgress *io = (PgAioInProgress *) si->si_value.sival_ptr;

	/*
	 * Sanity check on the pointer in the signal we received.  It had
	 * better be correctly aligned and in the expected range, and it had
	 * better point to an IO that this process initiated.  We don't
	 * actually expect spurious signals carrying bogus sigev_value data,
	 * but it doesn't cost much to check.
	 */
	if (((uintptr_t) io) % sizeof(io) == 0 &&
		io >= &aio_ctl->in_progress_io[0] &&
		io < &aio_ctl->in_progress_io[max_aio_in_progress] &&
		io->owner_id == my_aio_id)
		pgaio_posix_aio_check(io, true);
#else
	/*
	 * Without signal queuing, unhandled signals are merged and don't carry a
	 * user data pointer that points to our IO object.  This is going to
	 * require O(n^2) checks in the number of concurrent IOs.  Oh well.
	 */
	size_t count = my_inflight_io_count;

	for (size_t i = 0; i < count; ++i)
	{
		int index = my_inflight_io[i];
		PgAioInProgress *io;

		if (index != (sig_atomic_t) -1)
		{
			io = &aio_ctl->in_progress_io[index];
			pgaio_posix_aio_check(io, true);
		}
	}
#endif

	errno = save_errno;
}
#endif

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
	Size		sz;

	/* PgAioBounceBuffer itself */
	sz = mul_size(sizeof(PgAioBounceBuffer), max_aio_bounce_buffers);

	/* and the associated buffer */
	sz = add_size(sz,
				  mul_size(BLCKSZ, add_size(max_aio_bounce_buffers, 1)));

	return sz;
}

static Size
AioContextShmemSize(void)
{
	return mul_size(PGAIO_NUM_CONTEXTS,	sizeof(PgAioContext));
}

static Size
AioContextIovecsShmemSize(void)
{
	return mul_size(PGAIO_NUM_CONTEXTS,
					mul_size(sizeof(PgAioIovec), max_aio_in_flight));
}

static Size
AioSubmissionQueueShmemSize(void)
{
	/*
	 * For worker mode, we need a submission queue.  XXX We should probably
	 * have more than one.
	 */
	if (aio_type == AIOTYPE_WORKER)
		return squeue32_estimate(aio_worker_queue_size);
	else
		return 0;
}

#ifdef USE_POSIX_AIO
static Size
AioCompletionQueueShmemSize(void)
{
	return squeue32_estimate(max_aio_in_progress);
}
#endif

Size
AioShmemSize(void)
{
	Size		sz = 0;

	sz = add_size(sz, AioCtlShmemSize());
	sz = add_size(sz, AioCtlBackendShmemSize());
	sz = add_size(sz, AioSubmissionQueueShmemSize());
	sz = add_size(sz, AioBounceShmemSize());
	sz = add_size(sz, AioContextShmemSize());
	sz = add_size(sz, AioContextIovecsShmemSize());

#ifdef USE_POSIX_AIO
	sz = add_size(sz, AioCompletionQueueShmemSize());
#endif

	return sz;
}

void
AioShmemInit(void)
{
	bool		found;
	uint32		TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS;

#ifdef USE_POSIX_AIO
	aio_completion_queue = (squeue32 *)
		ShmemInitStruct("PgAioCompletionQueue", AioCompletionQueueShmemSize(), &found);
	if (!found)
		squeue32_init(aio_completion_queue, max_aio_in_progress);
#endif

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
			io->generation = 1;
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
			dlist_init(&bs->issued);
			dlist_init(&bs->issued_abandoned);
			pg_atomic_init_u32(&bs->inflight_count, 0);
			dlist_init(&bs->reaped);

			dlist_init(&bs->foreign_completed);
			SpinLockInit(&bs->foreign_completed_lock);
		}

		{
			char *p;
			char *blocks;

			dlist_init(&aio_ctl->unused_bounce_buffers);
			aio_ctl->bounce_buffers =
				ShmemInitStruct("PgAioBounceBuffers",
								sizeof(PgAioBounceBuffer) * max_aio_bounce_buffers,
								&found);
			Assert(!found);

			p = ShmemInitStruct("PgAioBounceBufferBlocks",
								BLCKSZ * (max_aio_bounce_buffers + 1),
								&found);
			Assert(!found);
			blocks = (char *) TYPEALIGN(BLCKSZ, (uintptr_t) p);

			for (int i = 0; i < max_aio_bounce_buffers; i++)
			{
				PgAioBounceBuffer *bb = &aio_ctl->bounce_buffers[i];

				bb->buffer = blocks + i * BLCKSZ;
				memset(bb->buffer, 0, BLCKSZ);
				pg_atomic_init_u32(&bb->refcount, 0);
				dlist_push_tail(&aio_ctl->unused_bounce_buffers, &bb->node);
				aio_ctl->unused_bounce_buffers_count++;
			}
		}

		if (aio_type == AIOTYPE_WORKER)
		{
			aio_submission_queue =
				ShmemInitStruct("aio submission queue",
								AioSubmissionQueueShmemSize(),
								&found);
			Assert(!found);
			squeue32_init(aio_submission_queue, aio_worker_queue_size);
			ConditionVariableInit(&aio_ctl->submission_queue_not_full);
			ConditionVariableInit(&aio_ctl->submission_queue_not_empty);
		}
#ifdef USE_LIBURING
		else if (aio_type == AIOTYPE_LIBURING)
		{
			PgAioIovec *iovecs;

			aio_ctl->num_contexts = PGAIO_NUM_CONTEXTS;
			aio_ctl->contexts = ShmemInitStruct("PgAioContexts", AioContextShmemSize(), &found);
			Assert(!found);

			iovecs = (PgAioIovec *)
			ShmemInitStruct("PgAioContextsIovecs", AioContextIovecsShmemSize(), &found);
			Assert(!found);
			memset(iovecs, 0, AioContextIovecsShmemSize());

			for (int contextno = 0; contextno < aio_ctl->num_contexts; contextno++)
			{
				PgAioContext *context = &aio_ctl->contexts[contextno];
				int ret;

				LWLockInitialize(&context->submission_lock, LWTRANCHE_AIO_CONTEXT_SUBMISSION);
				LWLockInitialize(&context->completion_lock, LWTRANCHE_AIO_CONTEXT_COMPLETION);

				slist_init(&context->unused_iovecs);
				slist_init(&context->reaped_iovecs);

				context->iovecs = iovecs;
				iovecs += max_aio_in_flight;

				for (uint32 i = 0; i < max_aio_in_flight; i++)
				{
					slist_push_head(&context->unused_iovecs, &context->iovecs[i].node);
					context->unused_iovecs_count++;
				}

				/*
				 * XXX: Probably worth sharing the WQ between the different
				 * rings, when supported by the kernel. Could also cause
				 * additional contention, I guess?
				 */
				if (!AcquireExternalFD())
					elog(ERROR, "io_uring_queue_init: %m");
				ret = io_uring_queue_init(max_aio_in_flight, &context->io_uring_ring, 0);
				if (ret < 0)
					elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
			}
		}
#endif

	}

#if USE_POSIX_AIO
	{
		struct sigaction sa;

		/*
		 * Any system that supports POSIX AIO should also support sigaction()
		 * with a three-argument signal handler.  (macOS doesn't exactly
		 * support it correctly, but it still compiles).
		 */
		sa.sa_sigaction = pgaio_sigio_handler;
		sa.sa_flags = SA_RESTART | SA_SIGINFO;
		sigemptyset(&sa.sa_mask);
		if (sigaction(SIGIO, &sa, NULL) < 0)
			elog(ERROR, "could not install signal handler: %m");
	}
#ifdef MISSING_SIGNAL_QUEUEING
	my_inflight_io = malloc(max_aio_in_flight * sizeof(sig_atomic_t));
	for (size_t i = 0; i < max_aio_in_flight; ++i)
		my_inflight_io[i] = (sig_atomic_t) -1;
	my_inflight_io_count = 0;
#endif
#endif
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
#ifdef USE_LIBURING
	/*
	 *
	 */
	if (aio_type == AIOTYPE_LIBURING) {
		int ret;

		ret = io_uring_queue_init(32, &local_ring, 0);
		if (ret < 0)
		{
			elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
		}
	}
#endif
}

static void
pgaio_postmaster_before_child_exit(int code, Datum arg)
{
	elog(DEBUG2, "aio before shmem exit: start");

	/*
	 * Need to wait for in-progress IOs initiated by this backend to
	 * finish. Some operating systems, like linux w/ io_uring, cancel IOs that
	 * are still in progress when exiting. Other's don't provide access to the
	 * results of such IOs.
	 */
	while (!dlist_is_empty(&my_aio->issued))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->issued);

		pgaio_io_release(io);
	}

	Assert(my_aio->issued_count == 0);
	Assert(dlist_is_empty(&my_aio->issued));

	while (!dlist_is_empty(&my_aio->issued_abandoned))
	{
		PgAioInProgress *io = NULL;
		PgAioIoRef ref;

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		if (!dlist_is_empty(&my_aio->issued_abandoned))
		{
			io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->issued_abandoned);
			pgaio_io_ref_internal(io, &ref);
		}
		LWLockRelease(SharedAIOCtlLock);

		if (!io)
		{
			elog(LOG, "skipped exit wait for abandoned IO %zu", io - aio_ctl->in_progress_io);
			break;
		}

		elog(LOG, "exit wait for abandoned IO %zu", io - aio_ctl->in_progress_io);
		pgaio_io_print(io, NULL);
		pgaio_io_wait_ref(&ref, false);
	}

	elog(DEBUG2, "aio before shmem exit: end");
}

static void
pgaio_postmaster_child_exit(int code, Datum arg)
{
	/* FIXME: handle unused */
	Assert(my_aio->outstanding_count == 0);
	Assert(dlist_is_empty(&my_aio->outstanding));

	Assert(my_aio->pending_count == 0);
	Assert(dlist_is_empty(&my_aio->pending));

	Assert(my_aio->issued_count == 0);
	Assert(dlist_is_empty(&my_aio->issued));

	Assert(my_aio->issued_abandoned_count == 0);
	Assert(dlist_is_empty(&my_aio->issued_abandoned));

	Assert(pg_atomic_read_u32(&my_aio->inflight_count) == 0);

	Assert(dlist_is_empty(&my_aio->reaped));

	Assert(my_aio->local_completed_count == 0);
	Assert(dlist_is_empty(&my_aio->local_completed));

	Assert(my_aio->foreign_completed_count == 0);
	Assert(dlist_is_empty(&my_aio->foreign_completed));
}

void
pgaio_postmaster_child_init(void)
{
#ifdef USE_LIBURING
	if (aio_type == AIOTYPE_LIBURING)
	{
		/* no locking needed here, only affects this process */
		for (int i = 0; i < aio_ctl->num_contexts; i++)
			io_uring_ring_dontfork(&aio_ctl->contexts[i].io_uring_ring);
	}
#endif

	my_aio_id = MyProc->pgprocno;
	my_aio = &aio_ctl->backend_state[my_aio_id];

	dlist_init(&local_recycle_requests);

	before_shmem_exit(pgaio_postmaster_before_child_exit, 0);
	on_shmem_exit(pgaio_postmaster_child_exit, 0);

	Assert(my_aio->unused_count == 0);
	Assert(my_aio->outstanding_count == 0);
	Assert(my_aio->issued_count == 0);
	Assert(my_aio->issued_abandoned_count == 0);
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

	pgaio_submit_pending(false);

	while (!dlist_is_empty(&my_aio->outstanding))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->outstanding);

		pgaio_io_release(io);
	}

	while (!dlist_is_empty(&my_aio->issued))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->issued);

		pgaio_io_release(io);
	}
}

void
pgaio_at_commit(void)
{
	Assert(dlist_is_empty(&local_recycle_requests));

	if (my_aio->pending_count != 0)
	{
		elog(WARNING, "unsubmitted IOs %d", my_aio->pending_count);
		pgaio_submit_pending(false);
	}

	while (!dlist_is_empty(&my_aio->outstanding))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->outstanding);

		elog(WARNING, "leaked outstanding io %zu", io - aio_ctl->in_progress_io);

		pgaio_io_release(io);
	}

	while (!dlist_is_empty(&my_aio->issued))
	{
		PgAioInProgress *io = dlist_head_element(PgAioInProgress, owner_node, &my_aio->issued);

		elog(WARNING, "leaked issued io %zu", io - aio_ctl->in_progress_io);

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

			case PGAIO_WRITE_GENERIC:
				Assert(cur->d.write_generic.already_done == 0);

				if (orig_result < 0)
				{
					cur->result = io->result;
				}
				else if (running_result >= cur->d.write_generic.nbytes)
				{
					cur->result = cur->d.write_generic.nbytes;
					running_result -= cur->result;
				}
				else if (running_result < cur->d.write_generic.nbytes)
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
		uint32 extracted = 1;

		if (io->flags & PGAIOIP_MERGE)
			extracted += pgaio_uncombine_one(io);

		pg_atomic_fetch_sub_u32(&aio_ctl->backend_state[io->owner_id].inflight_count, 1);
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

			/*
			 * Set flag before calling callback, otherwise we could easily end
			 * up looping forever.
			 */
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

				LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
				*(volatile PgAioIPFlags*) &io->flags =
					(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
					PGAIOIP_DONE |
					PGAIOIP_SHARED_FAILED;
				dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
				LWLockRelease(SharedAIOCtlLock);
			}

			/* signal state change */
			if (IsUnderPostmaster)
				ConditionVariableBroadcast(&io->cv);
		}
		else
		{
			Assert(in_error);

			dlist_delete_from(&my_aio->reaped, node);

			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			*(volatile PgAioIPFlags*) &io->flags =
				(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
				PGAIOIP_DONE |
				PGAIOIP_HARD_FAILURE |
				PGAIOIP_SHARED_FAILED;
			dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
			LWLockRelease(SharedAIOCtlLock);
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
				PgAioPerBackend *other = &aio_ctl->backend_state[cur->owner_id];

#ifdef PGAIO_VERBOSE
				ereport(DEBUG2,
						errmsg("removing aio %zu from issued_abandoned complete_ios",
							   cur - aio_ctl->in_progress_io),
						errhidecontext(1),
						errhidestmt(1));
#endif

				dlist_delete_from(&other->issued_abandoned, &cur->owner_node);
				Assert(other->issued_abandoned_count > 0);
				other->issued_abandoned_count--;

				cur->generation++;
				pg_write_barrier();

				cur->flags = PGAIOIP_UNUSED;

				if (cur->bb)
				{
					pgaio_bounce_buffer_release_internal(cur->bb,
														 /* holding_lock = */ true,
														 /* release_resowner = */ false);
					cur->bb = NULL;
				}

				cur->type = 0;
				cur->owner_id = INVALID_PGPROCNO;
				cur->result = 0;
				cur->system_referenced = true;
				cur->on_completion_local = NULL;

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

static void
pgaio_io_call_local_callback(PgAioInProgress *io, bool in_error)
{
	Assert(!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED));
	Assert(io->user_referenced);

	dlist_delete_from(&my_aio->issued, &io->owner_node);
	my_aio->issued_count--;
	dlist_push_tail(&my_aio->outstanding, &io->owner_node);
	my_aio->outstanding_count++;

	io->flags |= PGAIOIP_LOCAL_CALLBACK_CALLED;

	if (!io->on_completion_local)
		return;

	if (!in_error)
		io->on_completion_local->callback(io->on_completion_local, io);

}

/*
 * Call all pending local callbacks.
 */
static void
pgaio_call_local_callbacks(bool in_error)
{
	if (my_aio->local_completed_count != 0 &&
		CritSectionCount == 0)
	{
		/* FIXME: this isn't safe against errors */
		static int local_callback_depth = 0;

		if (local_callback_depth == 0)
		{
			local_callback_depth++;

			while (!dlist_is_empty(&my_aio->local_completed))
			{
				dlist_node *node = dlist_pop_head_node(&my_aio->local_completed);
				PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

				Assert(my_aio->local_completed_count > 0);
				my_aio->local_completed_count--;

				pgaio_io_call_local_callback(io, in_error);
			}

			local_callback_depth--;
		}
	}
}

/*
 * Receive completions in ring.
 */
static int  __attribute__((noinline))
pgaio_drain(PgAioContext *context, bool in_error, bool call_local)
{
	int ndrained = 0;

	START_CRIT_SECTION();

	if (aio_type == AIOTYPE_WORKER)
	{
		/*
		 * Worker mode has no completion queue, because the worker processes
		 * all completion work directly.
		 */
	}
#ifdef USE_LIBURING
	else if (aio_type == AIOTYPE_LIBURING)
		ndrained = pgaio_uring_drain(context);
#endif
#ifdef USE_POSIX_AIO
	else if (aio_type == AIOTYPE_POSIX)
		ndrained = pgaio_posix_aio_drain(context, in_error);
#endif

	if (ndrained > 0)
		pgaio_uncombine();

	END_CRIT_SECTION();

	pgaio_complete_ios(false);
	pgaio_transfer_foreign_to_local();
	pgaio_call_local_callbacks(in_error);

	return ndrained;
}

#ifdef USE_POSIX_AIO
static int
pgaio_posix_aio_drain(PgAioContext *context, bool in_error)
{
	uint32 io_index;
	int ndrained = 0;

	/* Reap as many completed IOs as we can without waiting. */
	while (squeue32_dequeue(aio_completion_queue, &io_index))
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[io_index];

		io->flags = (io->flags & ~PGAIOIP_INFLIGHT) | PGAIOIP_REAPED;
		dlist_push_tail(&my_aio->reaped, &io->io_node);
		++ndrained;
	}

	return ndrained;
}
#endif

static void
pgaio_transfer_foreign_to_local(void)
{
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
}

/*
 * Some AIO modes lack scatter/gather support, which limits I/O combining to
 * contiguous ranges of memory.
 */
static bool
pgaio_can_scatter_gather(void)
{
	if (aio_type == AIOTYPE_WORKER)
	{
		/*
		 * We may not have true scatter/gather on this platform (see fallback
		 * emulation in pg_preadv()/pg_pwritev()), but there may still be some
		 * advantage to keeping sequential regions within the same process so
		 * we'll say yes here.
		 */
		return true;
	}
#ifdef USE_LIBURING
	if (aio_type == AIOTYPE_LIBURING)
		return true;
#endif
#if defined(HAVE_AIO_READV) && defined(HAVE_AIO_WRITEV)
	if (aio_type == AIOTYPE_POSIX)
		return true;
#endif
	return false;
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
			if (!pgaio_can_scatter_gather() &&
				(last->d.read_buffer.buf + 1 != cur->d.read_buffer.buf))
				return false;
			if (last->d.read_buffer.mode != cur->d.read_buffer.mode)
				return false;
			if (last->d.read_buffer.already_done != 0 || cur->d.read_buffer.already_done != 0)
				return false;

			return true;

		case PGAIO_NOP:
		case PGAIO_FLUSH_RANGE:
		case PGAIO_FSYNC:
		case PGAIO_FSYNC_WAL:
			return false;

		case PGAIO_WRITE_BUFFER:
			if (last->d.write_buffer.fd != cur->d.write_buffer.fd)
				return false;
			if ((last->d.write_buffer.offset + last->d.write_buffer.nbytes) != cur->d.write_buffer.offset)
				return false;
			if (!pgaio_can_scatter_gather() &&
				(last->d.write_buffer.buf + 1 != cur->d.write_buffer.buf))
				return false;
			if (last->d.write_buffer.already_done != 0 || cur->d.write_buffer.already_done != 0)
				return false;
			return true;

		case PGAIO_WRITE_WAL:
			/* FIXME: XLOG sometimes intentionally does smaller writes - this would undo that */
			return false;
			if (last->d.write_wal.fd != cur->d.write_wal.fd)
				return false;
			if ((last->d.write_wal.offset + last->d.write_wal.nbytes) != cur->d.write_wal.offset)
				return false;
			if (!pgaio_can_scatter_gather() &&
				(last->d.write_wal.bufdata + last->d.write_wal.nbytes) != cur->d.write_wal.bufdata)
				return false;
			if (last->d.write_wal.already_done != 0 || cur->d.write_wal.already_done != 0)
				return false;
			if (last->d.write_wal.no_reorder || cur->d.write_wal.no_reorder)
				return false;
			return true;

		case PGAIO_WRITE_GENERIC:
			if (last->d.write_generic.fd != cur->d.write_generic.fd)
				return false;
			if ((last->d.write_generic.offset + last->d.write_generic.nbytes) != cur->d.write_generic.offset)
				return false;
			if (!pgaio_can_scatter_gather() &&
				(last->d.write_generic.bufdata + last->d.write_generic.nbytes) != cur->d.write_generic.bufdata)
				return false;
			if (last->d.write_generic.already_done != 0 || cur->d.write_generic.already_done != 0)
				return false;
			if (last->d.write_generic.no_reorder || cur->d.write_generic.no_reorder)
				return false;
			return true;
	}

	pg_unreachable();
}

static void
pgaio_io_merge(PgAioInProgress *into, PgAioInProgress *tomerge)
{
	ereport(DEBUG3,
			errmsg("merging %zu to %zu",
				   tomerge - aio_ctl->in_progress_io,
				   into - aio_ctl->in_progress_io),
			errhidestmt(true),
			errhidecontext(true));

	into->merge_with = tomerge;
	into->flags |= PGAIOIP_MERGE;
}

static void
pgaio_combine_pending(void)
{
	dlist_iter iter;
	PgAioInProgress *last = NULL;
	int combined = 1;

	Assert(my_aio->pending_count > 1);

	dlist_foreach(iter, &my_aio->pending)
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

			pgaio_io_merge(last, cur);
		}
		else
		{
			combined = 1;
		}

		if (combined >= PGAIO_MAX_COMBINE)
		{
			ereport(DEBUG3,
					errmsg("max combine at %d", combined),
					errhidestmt(true),
					errhidecontext(true));
			last = NULL;
			combined = 1;
		}
		else
			last = cur;
	}
}

static bool
pgaio_worker_need_synchronous(PgAioInProgress *io)
{
	char *buf;
	size_t size;

	/* Single user mode doesn't have any background workers. */
	if (!IsUnderPostmaster)
		return true;

	switch (io->type)
	{
	case PGAIO_FSYNC:
		/* We can't open WAL files that don't have a regular name yet. */
		return io->d.fsync.segno == -1;
	case PGAIO_WRITE_WAL:
		/* We can't open WAL files that don't have a regular name yet. */
		return io->d.write_wal.segno == -1;
	case PGAIO_READ_BUFFER:
		buf = io->d.read_buffer.bufdata;
		size = io->d.read_buffer.nbytes;
		break;
	case PGAIO_WRITE_BUFFER:
		buf = io->d.write_buffer.bufdata;
		size = io->d.write_buffer.nbytes;
		break;
	case PGAIO_WRITE_GENERIC:
		/* We don't know how to open the file. */
		return true;
	default:
		return false;
	}

	/*
	 * XXX Sometimes we receive pointers to private memory.  Oops.  There's
	 * probably some solution involving bounce buffers?  For now, range-check
	 * it and fall back to synchronous operation for that case.
	 */
	if (buf < BufferBlocks || (buf + size) > (BufferBlocks + BLCKSZ * NBuffers))
		return true;

	return false;
}

static int
pgaio_worker_submit(bool drain, bool will_wait)
{
	int nsubmitted = 0;
	bool force_synchronous = false;

	/*
	 * If we've received a hint that the caller intends to wait for completion
	 * immediately, and there is exactly one thing in the pending list, then we
	 * might as well perform the operation synchronously and skip all the
	 * interprocess overheads.
	 *
	 * XXX Huh, wouldn't we want to be able to do this for uring and posix
	 * modes too?!  How can we share the code?
	*/
	if (will_wait &&
		!dlist_is_empty(&my_aio->pending) &&
		dlist_head_node(&my_aio->pending) == dlist_tail_node(&my_aio->pending))
		force_synchronous = true;

	while (!dlist_is_empty(&my_aio->pending))
	{
		dlist_node *node;
		PgAioInProgress *io;
		uint32 io_index;

		node = dlist_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);
		io_index = io - aio_ctl->in_progress_io;

		pgaio_io_prepare_submit(io, 0);

		if (force_synchronous || pgaio_worker_need_synchronous(io))
		{
			PgAioWorkerState state;

			/* Perform the IO synchronously in this process. */
			pgaio_worker_state_init(&state);
			pgaio_worker_do(&state, io);
			pgaio_worker_state_close(&state);
		}
		else
		{
			/* Push it on the submission queue and wake a worker. */
			/* XXX Think about interruptions! */
			while (!squeue32_enqueue(aio_submission_queue, io_index))
				ConditionVariableSleep(&aio_ctl->submission_queue_not_full,
									   WAIT_EVENT_AIO_SUBMIT);
			ConditionVariableCancelSleep();
			ConditionVariableSignal(&aio_ctl->submission_queue_not_empty);
		}

		nsubmitted++;
	}

	return nsubmitted;
}

static void
pgaio_submit_pending_internal(bool drain, bool will_wait)
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

	/*
	 * Loop until all pending IOs are submitted. Throttle max in-flight before
	 * calling into the IO implementation specific routine, so this code can
	 * be shared.
	 */
	while (!dlist_is_empty(&my_aio->pending))
	{
		int max_submit;
		int did_submit;

		Assert(my_aio->pending_count > 0);
		pgaio_apply_backend_limit();

		Assert(my_aio->pending_count > 0);
		if (my_aio->pending_count == 0)
			break;

		max_submit = Min(my_aio->pending_count, PGAIO_SUBMIT_BATCH_SIZE);
		max_submit = Min(max_submit, io_max_concurrency - pg_atomic_read_u32(&my_aio->inflight_count));
		Assert(max_submit > 0);

		START_CRIT_SECTION();
		if (aio_type == AIOTYPE_WORKER)
			did_submit = pgaio_worker_submit(drain, will_wait);
#ifdef USE_LIBURING
		else if (aio_type == AIOTYPE_LIBURING)
			did_submit = pgaio_uring_submit(max_submit, drain);
#endif
#ifdef USE_POSIX_AIO
		else if (aio_type == AIOTYPE_POSIX)
			did_submit = pgaio_posix_submit(max_submit, drain);
#endif
		else
			elog(ERROR, "unexpected aio_type");
		total_submitted += did_submit;
		Assert(did_submit > 0 && did_submit <= max_submit);
		END_CRIT_SECTION();
	}

	my_aio->executed_total_count += orig_total;
	my_aio->issued_total_count += total_submitted;

#ifdef PGAIO_VERBOSE
	ereport(DEBUG3,
			errmsg("submitted %d (orig %d)", total_submitted, orig_total),
			errhidestmt(true),
			errhidecontext(true));
#endif

	RESUME_INTERRUPTS();

	if (drain)
		pgaio_call_local_callbacks(/* in_error = */ false);
}

void  __attribute__((noinline))
pgaio_submit_pending(bool drain)
{
	pgaio_submit_pending_internal(drain, false);
}

#if defined(USE_POSIX_AIO)

typedef struct pgaio_posix_listio_buffer
{
	int ncbs;
	struct aiocb *cbs[AIO_LISTIO_MAX];
} pgaio_posix_listio_buffer;

static int
pgaio_posix_flush_listio(pgaio_posix_listio_buffer *lb)
{
	int rc;

	if (lb->ncbs == 0)
		return 0;

	rc = lio_listio(LIO_NOWAIT, lb->cbs, lb->ncbs, NULL);

	if (rc < 0)
	{
		/*
		 * XXX Figure out how to tidy up
		 */
		elog(PANIC, "blah: %m");
	}

	lb->ncbs = 0;

	return 0;
}

static int
pgaio_posix_add_listio(pgaio_posix_listio_buffer *lb, struct aiocb *cb)
{
	if (lb->ncbs == AIO_LISTIO_MAX)
	{
		int rc;

		rc = pgaio_posix_flush_listio(lb);
		if (rc < 0)
			return rc;
	}
	lb->cbs[lb->ncbs++] = cb;

	return 0;
}

/*
 * Assumes that io->posix_aiocb is cleared, but has the aio_filedes and
 * aio_offset already set.
 */
static int
pgaio_posix_start_rw(pgaio_posix_listio_buffer *lb, PgAioInProgress *io,
					 int lio_opcode)
{
	struct aiocb *cb = &io->posix_aiocb;
	struct iovec iov[IOV_MAX];
	int iovcnt;

	iovcnt = pgaio_fill_iov(iov, io);

#if defined(HAVE_AIO_READV) && defined(HAVE_AIO_WRITEV)
	if (iovcnt > 1)
	{
		/*
		 * We can't do scatter/gather in a listio on any known OS, but it's
		 * better to use FreeBSD's nonstandard separate system calls than pass
		 * up the opportunity for scatter/gather IO.  Note that this case
		 * should only be reachable if pgaio_can_scatter_gather() returned
		 * true.
		 */
		cb->aio_iov = iov;
		cb->aio_iovcnt = iovcnt;

		return lio_opcode == LIO_WRITE ? aio_writev(cb) : aio_readv(cb);
	}
	else
#endif
	{
		Assert(iovcnt == 1);

		/*
		 * This might be a single PG IO, or a chain of reads into contiguous
		 * memory, so that it takes only a single iovec.  We'll batch it up
		 * with other such single iovec requests.
		 */
		cb->aio_buf = iov[0].iov_base;
		cb->aio_nbytes = iov[0].iov_len;
		cb->aio_lio_opcode = lio_opcode;

		return pgaio_posix_add_listio(lb, cb);
	}
}

static int
pgaio_posix_submit(int max_submit, bool drain)
{
	PgAioInProgress *ios[PGAIO_SUBMIT_BATCH_SIZE];
	int nios = 0;
	pgaio_posix_listio_buffer listio_buffer = {0};

	/*
	 * This implementation only supports combined IO for contiguous regions of
	 * memory, and pgaio_can_scatter_gather() knows that.  That's why the
	 * nbytes computation just has to sum up nbytes for merged requests.
	 */

	while (!dlist_is_empty(&my_aio->pending))
	{
		dlist_node *node;
		PgAioInProgress *io;
		int rc;

		if (nios == max_submit)
			break;

		node = dlist_pop_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);

		pgaio_io_prepare_submit(io, 0);

		io->posix_aio_returned = false;
		io->posix_aio_nocheck = false;
		pgaio_posix_aio_preflight(io);

		pg_atomic_add_fetch_u32(&my_aio->inflight_count, 1);
		my_aio->submissions_total_count++;

		/* Request a signal on completion.  */
		memset(&io->posix_aiocb, 0, sizeof(io->posix_aiocb));
		io->posix_aiocb.aio_sigevent.sigev_notify = SIGEV_SIGNAL;
		io->posix_aiocb.aio_sigevent.sigev_signo = SIGIO;

#ifndef MISSING_SIGNAL_QUEUEING
		/* Pass a pointer to our IO object to the signal handler. */
		io->posix_aiocb.aio_sigevent.sigev_value.sival_ptr = io;
#endif

		switch (io->type)
		{
			case PGAIO_INVALID:
			case PGAIO_NOP:
				rc = 0;
				break;
			case PGAIO_FLUSH_RANGE:
				/*
				 * This is supposed to represent Linux's sync_file_range(),
				 * which initiates writeback for only a certain range of a
				 * file.  On other systems, we could either initiate
				 * fdatasync() or do nothing.  Initiating fdatasync() seems
				 * closer to the intended behavior.  XXX review
				 */
#ifdef O_DSYNC
				rc = aio_fsync(O_DSYNC, &io->posix_aiocb);
#else
				rc = aio_fsync(O_SYNC, &io->posix_aiocb);
#endif
				break;
			case PGAIO_FSYNC:
			case PGAIO_FSYNC_WAL:
				/*
				 * XXX Are we assuming that writes on our own pending list must
				 * be included in any later fsync on our own pending list?  For
				 * now, this preceding flush assumes yes (POSIX fsync will
				 * include the effects of any writes initiated earlier, so we'd
				 * make sure that whatever's in our listio buffer is submitted
				 * first).  But perhaps that can be removed?
				 */
				rc = pgaio_posix_flush_listio(&listio_buffer);
				if (rc == 0)
				{
					io->posix_aiocb.aio_fildes = io->d.fsync.fd;
#ifdef O_DSYNC
					rc = aio_fsync(io->d.fsync.datasync ? O_DSYNC : O_SYNC,
								   &io->posix_aiocb);
#else
					rc = aio_fsync(O_SYNC, &io->posix_aiocb);
#endif
				}
				break;
			case PGAIO_READ_BUFFER:
				io->posix_aiocb.aio_fildes = io->d.read_buffer.fd;
				io->posix_aiocb.aio_offset = io->d.read_buffer.offset +
					io->d.read_buffer.already_done;
				rc = pgaio_posix_start_rw(&listio_buffer, io, LIO_READ);
				break;
			case PGAIO_WRITE_BUFFER:
				io->posix_aiocb.aio_fildes = io->d.write_buffer.fd;
				io->posix_aiocb.aio_offset = io->d.write_buffer.offset +
					io->d.write_buffer.already_done;
				rc = pgaio_posix_start_rw(&listio_buffer, io, LIO_WRITE);
				break;
			case PGAIO_WRITE_WAL:
				io->posix_aiocb.aio_fildes = io->d.write_wal.fd;
				io->posix_aiocb.aio_offset = io->d.write_wal.offset +
					io->d.write_wal.already_done;
				rc = pgaio_posix_start_rw(&listio_buffer, io, LIO_WRITE);
				break;
			case PGAIO_WRITE_GENERIC:
				io->posix_aiocb.aio_fildes = io->d.write_generic.fd;
				io->posix_aiocb.aio_offset = io->d.write_generic.offset +
					io->d.write_generic.already_done;
				rc = pgaio_posix_start_rw(&listio_buffer, io, LIO_WRITE);
				break;
		}

		ios[nios] = io;
		++nios;
	}
	pgaio_posix_flush_listio(&listio_buffer);

	/* XXXX copied from uring submit */
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

	/* callbacks will be called later by pgaio_submit() */
	if (drain)
		pgaio_drain(NULL, false, false);

	return nios;
}
#endif

static void
pgaio_io_prepare_submit(PgAioInProgress *io, uint32 ring)
{
	PgAioInProgress *cur;

	cur = io;

	while (cur)
	{
		Assert(cur->flags & PGAIOIP_PENDING);

		cur->ring = ring;

		*(volatile PgAioIPFlags*) &cur->flags =
			(cur->flags & ~PGAIOIP_PENDING) | PGAIOIP_INFLIGHT;

		dlist_delete_from(&my_aio->pending, &cur->io_node);
		my_aio->pending_count--;

		if (cur->flags & PGAIOIP_RETRY)
		{
			/* XXX: more error checks */
		}
		else if (cur->user_referenced)
		{
			Assert(my_aio_id == cur->owner_id);
			Assert(my_aio->outstanding_count > 0);
			dlist_delete_from(&my_aio->outstanding, &cur->owner_node);
			my_aio->outstanding_count--;

			dlist_push_tail(&my_aio->issued, &cur->owner_node);
			my_aio->issued_count++;
		}
		else
		{
#ifdef PGAIO_VERBOSE
			ereport(DEBUG2,
					errmsg("putting aio %zu onto issued_abandoned during submit",
						   cur - aio_ctl->in_progress_io),
					errhidecontext(1),
					errhidestmt(1));
#endif

			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			dlist_push_tail(&my_aio->issued_abandoned, &cur->owner_node);
			my_aio->issued_abandoned_count++;
			LWLockRelease(SharedAIOCtlLock);
		}

		cur = cur->merge_with;
	}
}

static void
pgaio_apply_backend_limit(void)
{
	uint32 current_inflight = pg_atomic_read_u32(&my_aio->inflight_count);

	while (current_inflight >= io_max_concurrency)
	{
		PgAioInProgress *io;

		/*
		 * XXX: Should we be a bit fairer and check the "oldest" in-flight IO
		 * between issued and issued_abandoned?
		 */

		if (my_aio->issued_count > 0)
		{
			dlist_iter iter;

			Assert(!dlist_is_empty(&my_aio->issued));

			dlist_foreach(iter, &my_aio->issued)
			{
				io = dlist_container(PgAioInProgress, owner_node, iter.cur);

				if (io->flags & PGAIOIP_INFLIGHT)
				{
					PgAioIoRef ref;

					ereport(DEBUG2,
							errmsg("applying per-backend limit to issued IO %zu/%llu (current %d in %d, target %d)",
								   io - aio_ctl->in_progress_io,
								   (unsigned long long) io->generation,
								   my_aio->issued_count + my_aio->issued_abandoned_count,
								   current_inflight,
								   io_max_concurrency),
							errhidestmt(true),
							errhidecontext(true));

					pgaio_io_ref(io, &ref);
					pgaio_io_wait_ref(&ref, /* call_local = */ false);
					current_inflight = pg_atomic_read_u32(&my_aio->inflight_count);
					break;
				}
			}
		}

		if (current_inflight < io_max_concurrency)
			break;

		if (my_aio->issued_abandoned_count > 0)
		{
			dlist_iter iter;
			PgAioIoRef ref;

			io = NULL;

			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			dlist_foreach(iter, &my_aio->issued_abandoned)
			{
				io = dlist_container(PgAioInProgress, owner_node, iter.cur);

				if (io->flags & PGAIOIP_INFLIGHT)
				{
					pgaio_io_ref_internal(io, &ref);
					break;
				}
				else
					io = NULL;
			}
			LWLockRelease(SharedAIOCtlLock);

			if (io == NULL)
				continue;

			ereport(DEBUG2,
					errmsg("applying per-backend limit to issued_abandoned IO %zu/%llu (current %d in %d, target %d)",
						   io - aio_ctl->in_progress_io,
						   (unsigned long long) io->generation,
						   my_aio->issued_count + my_aio->issued_abandoned_count,
						   current_inflight,
						   io_max_concurrency),
					errhidestmt(true),
					errhidecontext(true));

			pgaio_io_wait_ref(&ref, false);
		}

		current_inflight = pg_atomic_read_u32(&my_aio->inflight_count);
	}
}

#ifdef USE_POSIX_AIO
static void
pgaio_posix_aio_wait(PgAioInProgress *io, int wait_event)
{
	Assert(io->owner_id == my_aio_id);
	Assert(!(io->flags & PGAIOIP_POSIX_AIO_RETURNED));

	/*
	 * Tell pgaio_posix_aio_check() to skip this item just in case it's reached
	 * by the signal handler.  That's because aio_suspend()'s behavior is
	 * undefined by POSIX if aio_return() has already been called, so we can't
	 * allow that to happen behind our back.  On FreeBSD, it returns EINVAL in
	 * that case, which seems like a good idea, but unfortunately macOS
	 * sometimes sleeps forever.
	 */
	io->posix_aio_nocheck = true;
	if (io->posix_aio_returned)
	{
		/* aio_return() already ran in the signal handler.  We're done. */
		io->posix_aio_nocheck = false;
		io->flags |= PGAIOIP_POSIX_AIO_RETURNED;
		return;
	}

	/*
	 * Wait for the kernel to tell us that this IO has completed.  While
	 * waiting, completion signals for other IOs will still be handled via the
	 * usual signal handler route.
	 */
	pgstat_report_wait_start(WAIT_EVENT_AIO_IO_COMPLETE_ONE_LOCAL);
	for (;;)
	{
		const struct aiocb *aiocb = &io->posix_aiocb;

		/* Atomically check if it's done and begin sleeping if not. */
		if (aio_suspend(&aiocb, 1, NULL) == 0)
		{
			/*
			 * We have to run the check manually.  As a slight optimization, we
			 * tell pgaio_posix_aio_check() to skip the shared completion queue
			 * and process IO completion callbacks immediately.
			 */
			io->posix_aio_nocheck = false;
			pgaio_posix_aio_check(io, false);
			break;
		}
		else if (errno != EINTR)
			elog(PANIC, "aio_suspend failed for I/O %zu: %m",
				 io - aio_ctl->in_progress_io);
	}
	pgstat_report_wait_end();
}
#endif

void
pgaio_io_wait_ref(PgAioIoRef *ref, bool call_local)
{
	uint64 ref_generation;
	PgAioInProgress *io;
	uint32 done_flags = PGAIOIP_DONE;
	PgAioIPFlags flags;
	PgAioContext *context;
	bool am_owner;

	Assert(ref->aio_index < max_aio_in_progress);

	io = &aio_ctl->in_progress_io[ref->aio_index];
	ref_generation = ((uint64) ref->generation_upper) << 32 |
		ref->generation_lower;

	Assert(ref_generation != 0);

	am_owner = io->owner_id == my_aio_id;
	flags = io->flags;
	pg_read_barrier();

	if (io->generation != ref_generation)
		return;

	if (am_owner && (flags & PGAIOIP_PENDING))
		pgaio_submit_pending(false);

	context = &aio_ctl->contexts[io->ring];
	Assert(!(flags & (PGAIOIP_UNUSED)));

	while (true)
	{
		flags = io->flags;
		pg_read_barrier();

		if (io->generation != ref_generation)
			return;

		if (flags & done_flags)
			goto wait_ref_out;

		Assert(!(flags & (PGAIOIP_UNUSED)));

		pgaio_drain(context, false, call_local);

		flags = io->flags;
		pg_read_barrier();

		if (io->generation != ref_generation)
			return;

		if (flags & done_flags)
			goto wait_ref_out;

		if (my_aio->pending_count > 0 && call_local)
		{
			/* FIXME: we should call this in a larger number of cases */

			/*
			 * If we otherwise would have to sleep submit all pending
			 * requests, to avoid others having to wait for us to submit
			 * them. Don't want to do so when not needing to sleep, as
			 * submitting IOs in smaller increments can be less efficient.
			 */
			pgaio_submit_pending(false);
		}
		else if (flags & PGAIOIP_INFLIGHT)
		{
			/* note that this is allowed to spuriously return */
			if (aio_type == AIOTYPE_WORKER)
				ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_IO_COMPLETE_ONE);
#ifdef USE_LIBURING
			else if (aio_type == AIOTYPE_LIBURING)
				pgaio_uring_wait_one(context, io,
									 WAIT_EVENT_AIO_IO_COMPLETE_ANY);
#endif
#ifdef USE_POSIX_AIO
			else if (aio_type == AIOTYPE_POSIX)
			{
				if (io->owner_id == my_aio_id &&
					!(io->flags & PGAIOIP_POSIX_AIO_RETURNED))
				{
					/* Wait for notification from the kernel. */
					pgaio_posix_aio_wait(io, WAIT_EVENT_AIO_IO_COMPLETE_ONE_LOCAL);
				}
				else
				{
					/*
					 * We're waiting for the kernel to tell the initiating
					 * backend about completion, and then for that backend or
					 * any other to drain the queue and signal the condition
					 * variable.
					 *
					 * Although it's highly likely to happen soon after
					 * completion, there's no guarantee that another backend
					 * will ever drain the queue.  So, as a last resort, we'll
					 * also do it ourselves after 100ms.  XXX: How can we do
					 * better than this?!
					 */
					ConditionVariableTimedSleep(&io->cv, 100,
												WAIT_EVENT_AIO_IO_COMPLETE_ONE);
				}
			}
#endif
		}
		else
		{
			/* shouldn't be reachable without concurrency */
			Assert(IsUnderPostmaster);

			/* ensure we're going to get woken up */
			if (IsUnderPostmaster)
				ConditionVariablePrepareToSleep(&io->cv);

			flags = io->flags;
			pg_read_barrier();
			if (io->generation == ref_generation && !(flags & done_flags))
				ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_IO_COMPLETE_ONE);

			if (IsUnderPostmaster)
				ConditionVariableCancelSleep();
		}
	}

wait_ref_out:

	flags = io->flags;
	pg_read_barrier();
	if (io->generation != ref_generation)
		return;

	Assert(flags & PGAIOIP_DONE);

	if (unlikely(flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_HARD_FAILURE)))
	{
		/* can retry soft failures, but not hard ones */
		/* FIXME: limit number of soft retries */
		if (flags & PGAIOIP_SOFT_FAILURE)
		{
			pgaio_io_retry(io);
			pgaio_io_wait_ref(ref, call_local);
		}
		else
		{
			pgaio_io_print(io, NULL);
			elog(WARNING, "request %zd failed permanently",
				 io - aio_ctl->in_progress_io);
		}

		return;
	}

	if (am_owner && call_local && !(flags & PGAIOIP_LOCAL_CALLBACK_CALLED))
	{
		if (flags & PGAIOIP_FOREIGN_DONE)
		{
			SpinLockAcquire(&my_aio->foreign_completed_lock);
			dlist_delete_from(&my_aio->foreign_completed, &io->io_node);
			io->flags &= ~PGAIOIP_FOREIGN_DONE;
			my_aio->foreign_completed_count--;
			SpinLockRelease(&my_aio->foreign_completed_lock);
		}
		else
		{
			Assert(my_aio->local_completed_count > 0);
			dlist_delete_from(&my_aio->local_completed, &io->io_node);
			my_aio->local_completed_count--;
		}

		pgaio_io_call_local_callback(io, false);
	}

}

/*
 */
void
pgaio_io_wait(PgAioInProgress *io)
{
	PgAioIoRef ref;

	Assert(io->user_referenced && io->owner_id == my_aio_id);

	pgaio_io_ref(io, &ref);
	pgaio_io_wait_ref(&ref, /* call_local = */ true);
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
			 aio_ctl->used_count, pg_atomic_read_u32(&my_aio->inflight_count));

		/*
		 * FIXME: should we wait for IO instead?
		 *
		 * Also, need to protect against too many ios handed out but not used.
		 */
		for (int i = 0; i < aio_ctl->num_contexts; i++)
			pgaio_drain(&aio_ctl->contexts[i], false, true);

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	}

	elem = dlist_pop_head_node(&aio_ctl->unused_ios);
	aio_ctl->used_count++;

	LWLockRelease(SharedAIOCtlLock);

	io = dlist_container(PgAioInProgress, owner_node, elem);

	Assert(io->flags == PGAIOIP_UNUSED);
	Assert(io->system_referenced);
	Assert(io->on_completion_local == NULL);

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

	if (io->flags & (PGAIOIP_IDLE | PGAIOIP_HARD_FAILURE))
		return true;

	if (io->flags & PGAIOIP_DONE)
	{
		if (io->owner_id == my_aio_id &&
			!(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED))
			return false;
		return true;
	}

	return false;
}

static void
pgaio_io_ref_internal(PgAioInProgress *io, PgAioIoRef *ref)
{
	Assert(io->flags & (PGAIOIP_IDLE | PGAIOIP_IN_PROGRESS | PGAIOIP_DONE));

	ref->aio_index = io - aio_ctl->in_progress_io;
	ref->generation_upper = (uint32) (io->generation >> 32);
	ref->generation_lower = (uint32) io->generation;
}

void
pgaio_io_ref(PgAioInProgress *io, PgAioIoRef *ref)
{
	Assert(io->user_referenced);
	pgaio_io_ref_internal(io, ref);
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
	Assert(io->owner_id == my_aio_id);
	Assert(!io->system_referenced);
	Assert(io->merge_with == NULL);

	if (io->bb)
	{
		pgaio_bounce_buffer_release_internal(io->bb, false, false);
		io->bb = NULL;
	}

	if (io->flags & PGAIOIP_DONE)
	{
		/* request needs to actually be done, including local callbacks */
		Assert(!(io->flags & PGAIOIP_FOREIGN_DONE));
		Assert(io->flags & PGAIOIP_LOCAL_CALLBACK_CALLED);

		io->generation++;
		pg_write_barrier();

		io->flags &= ~PGAIOIP_DONE;
		io->flags |= PGAIOIP_IDLE;
	}

	io->flags &= ~(PGAIOIP_MERGE |
				   PGAIOIP_SHARED_CALLBACK_CALLED |
				   PGAIOIP_LOCAL_CALLBACK_CALLED |
				   PGAIOIP_RETRY |
				   PGAIOIP_HARD_FAILURE |
				   PGAIOIP_SOFT_FAILURE |
				   PGAIOIP_POSIX_AIO_RETURNED);
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
}

void
pgaio_io_release(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!IsUnderPostmaster || io->owner_id == MyProc->pgprocno);

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	io->user_referenced = false;

	if (io->flags & (PGAIOIP_IDLE |
					 PGAIOIP_PENDING |
					 PGAIOIP_LOCAL_CALLBACK_CALLED))
	{
		Assert(!(io->flags & PGAIOIP_INFLIGHT));

		Assert(my_aio->outstanding_count > 0);
		dlist_delete_from(&my_aio->outstanding, &io->owner_node);
		my_aio->outstanding_count--;

#ifdef PGAIO_VERBOSE
		ereport(DEBUG3, errmsg("releasing plain user reference to %zu",
							   io - aio_ctl->in_progress_io),
				errhidecontext(1),
				errhidestmt(1));
#endif
	}
	else
	{
		dlist_delete_from(&my_aio->issued, &io->owner_node);
		my_aio->issued_count--;

		if (io->system_referenced)
		{
#ifdef PGAIO_VERBOSE
			ereport(DEBUG2, errmsg("putting aio %zu onto issued_abandoned during release",
								   io - aio_ctl->in_progress_io),
					errhidecontext(1),
					errhidestmt(1));
#endif

			dlist_push_tail(&my_aio->issued_abandoned, &io->owner_node);
			my_aio->issued_abandoned_count++;
		}
		else
		{
			Assert(io->flags & (PGAIOIP_DONE | PGAIOIP_SHARED_CALLBACK_CALLED));

#ifdef PGAIO_VERBOSE
			ereport(DEBUG2, errmsg("not putting aio %zu onto issued_abandoned during release",
								   io - aio_ctl->in_progress_io),
					errhidecontext(1),
					errhidestmt(1));
#endif
		}
	}

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
				io->on_completion_local = NULL;
			}

		}

		io->generation++;
		pg_write_barrier();

		io->flags = PGAIOIP_UNUSED;
		io->type = 0;
		io->owner_id = INVALID_PGPROCNO;
		io->result = 0;
		io->system_referenced = true;
		io->on_completion_local = NULL;

		Assert(io->merge_with == NULL);

		/* could do this earlier or conditionally */
		if (io->bb)
		{
			pgaio_bounce_buffer_release_internal(io->bb,
												 /* holding_lock = */ true,
												 /* release_resowner = */ false);
			io->bb = NULL;
		}

		dlist_push_tail(&aio_ctl->unused_ios, &io->owner_node);
		aio_ctl->used_count--;
	}

	LWLockRelease(SharedAIOCtlLock);
}

void
pgaio_print_queues(void)
{
	StringInfoData s;
	uint32 inflight_backend = 0;
	uint32 *inflight_context;

	initStringInfo(&s);

	for (int procno = 0; procno < aio_ctl->backend_state_count; procno++)
	{
		PgAioPerBackend *bs = &aio_ctl->backend_state[procno];

		inflight_backend += pg_atomic_read_u32(&bs->inflight_count);
	}

	inflight_context = palloc0(sizeof(uint32) * aio_ctl->backend_state_count);
	for (int i = 0; i < max_aio_in_progress; i++)
	{
		PgAioInProgress *io = &aio_ctl->in_progress_io[i];

		if (!(io->flags & PGAIOIP_INFLIGHT))
			continue;
		inflight_context[io->ring]++;
	}

	appendStringInfo(&s, "inflight backend: %d", inflight_backend);

#ifdef USE_LIBURING
	for (int contextno = 0; contextno < aio_ctl->num_contexts; contextno++)
	{
		PgAioContext *context = &aio_ctl->contexts[contextno];

		appendStringInfo(&s, "\n\tqueue[%d]: space: %d, ready: %d, we think inflight: %d",
						 contextno,
						 io_uring_sq_space_left(&context->io_uring_ring),
						 io_uring_cq_ready(&context->io_uring_ring),
						 inflight_context[contextno]);
	}
#endif

	ereport(LOG, errmsg_internal("%s", s.data),
			errhidestmt(true),
			errhidecontext(true));
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
		case PGAIO_FSYNC_WAL:
			return "fsync_wal";
		case PGAIO_READ_BUFFER:
			return "read_buffer";
		case PGAIO_WRITE_BUFFER:
			return "write_buffer";
		case PGAIO_WRITE_WAL:
			return "write_wal";
		case PGAIO_WRITE_GENERIC:
			return "write_generic";
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
	STRINGIFY_FLAG(PGAIOIP_POSIX_AIO_RETURNED);

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
		case PGAIO_FSYNC_WAL:
			appendStringInfo(s, "fd: %d, datasync: %d, barrier: %d, flush_no: %d",
							 io->d.fsync_wal.fd,
							 io->d.fsync_wal.datasync,
							 io->d.fsync_wal.barrier,
							 io->d.fsync_wal.flush_no);
			break;
		case PGAIO_FLUSH_RANGE:
			appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %llu",
							 io->d.flush_range.fd,
							 (unsigned long long) io->d.flush_range.offset,
							 (unsigned long long) io->d.flush_range.nbytes);
			break;
		case PGAIO_READ_BUFFER:
			appendStringInfo(s, "fd: %d, mode: %d, offset: %u, nbytes: %u, already_done: %u, buf/data: %d/%p",
							 io->d.read_buffer.fd,
							 io->d.read_buffer.mode,
							 io->d.read_buffer.offset,
							 io->d.read_buffer.nbytes,
							 io->d.read_buffer.already_done,
							 io->d.read_buffer.buf,
							 io->d.read_buffer.bufdata);
			break;
		case PGAIO_WRITE_BUFFER:
			appendStringInfo(s, "fd: %d, offset: %u, nbytes: %u, already_done: %u, buf/data: %u/%p",
							 io->d.write_buffer.fd,
							 io->d.write_buffer.offset,
							 io->d.write_buffer.nbytes,
							 io->d.write_buffer.already_done,
							 io->d.read_buffer.buf,
							 io->d.write_buffer.bufdata);
			break;
		case PGAIO_WRITE_WAL:
			appendStringInfo(s, "fd: %d, offset: %u, nbytes: %u, already_done: %u, bufdata: %p, no-reorder: %d",
							 io->d.write_wal.fd,
							 io->d.write_wal.offset,
							 io->d.write_wal.nbytes,
							 io->d.write_wal.already_done,
							 io->d.write_wal.bufdata,
							 io->d.write_wal.no_reorder);
			break;
		case PGAIO_WRITE_GENERIC:
			appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, bufdata: %p, no-reorder: %d",
							 io->d.write_generic.fd,
							 (unsigned long long) io->d.write_generic.offset,
							 io->d.write_generic.nbytes,
							 io->d.write_generic.already_done,
							 io->d.write_generic.bufdata,
							 io->d.write_generic.no_reorder);
			break;
		default:
			break;
	}
}

static void
pgaio_io_print_one(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "aio %zu/"UINT64_FORMAT": action: %s, ring: %d, init: %d, flags: ",
					 io - aio_ctl->in_progress_io,
					 io->generation,
					 pgaio_io_action_string(io->type),
					 io->ring,
					 io->owner_id);
	pgaio_io_flag_string(io->flags, s);
	appendStringInfo(s, ", result: %d, user/system_referenced: %d/%d (",
					 io->result,
					 io->user_referenced,
					 io->system_referenced);
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

	ResourceOwnerEnlargeAioBB(CurrentResourceOwner);

	while (true)
	{
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
		if (!dlist_is_empty(&aio_ctl->unused_bounce_buffers))
		{
			dlist_node *node = dlist_pop_head_node(&aio_ctl->unused_bounce_buffers);

			aio_ctl->unused_bounce_buffers_count--;
			bb = dlist_container(PgAioBounceBuffer, node, node);

			Assert(pg_atomic_read_u32(&bb->refcount) == 0);
		}
		LWLockRelease(SharedAIOCtlLock);

		if (!bb)
		{
			for (int i = 0; i < aio_ctl->num_contexts; i++)
				pgaio_drain(&aio_ctl->contexts[i], false, true);
		}
		else
			break;
	}

	pg_atomic_write_u32(&bb->refcount, 1);

	ResourceOwnerRememberAioBB(CurrentResourceOwner, bb);

	return bb;
}

static void
pgaio_bounce_buffer_release_internal(PgAioBounceBuffer *bb, bool holding_lock, bool release_resowner)
{
	Assert(holding_lock == LWLockHeldByMe(SharedAIOCtlLock));
	Assert(bb != NULL);

	if (release_resowner)
		ResourceOwnerForgetAioBB(CurrentResourceOwner, bb);

	if (pg_atomic_sub_fetch_u32(&bb->refcount, 1) != 0)
		return;

	if (!holding_lock)
		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	dlist_push_tail(&aio_ctl->unused_bounce_buffers, &bb->node);
	aio_ctl->unused_bounce_buffers_count++;
	if (!holding_lock)
		LWLockRelease(SharedAIOCtlLock);
}

void
pgaio_bounce_buffer_release(PgAioBounceBuffer *bb)
{
	pgaio_bounce_buffer_release_internal(bb,
										 /* holding_lock = */ false,
										 /* release_resowner */ true);
}

char *
pgaio_bounce_buffer_buffer(PgAioBounceBuffer *bb)
{
	return bb->buffer;
}

void
pgaio_assoc_bounce_buffer(PgAioInProgress *io, PgAioBounceBuffer *bb)
{
	Assert(bb != NULL);
	Assert(io->bb == NULL);
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->user_referenced);
	Assert(pg_atomic_read_u32(&bb->refcount) > 0);

	io->bb = bb;
	pg_atomic_fetch_add_u32(&bb->refcount, 1);
}

#ifdef USE_LIBURING

/* --------------------------------------------------------------------------------
 * io_uring related code
 * --------------------------------------------------------------------------------
 */

static PgAioContext *
pgaio_acquire_context(void)
{
	PgAioContext *context;
	int init_last_context = my_aio->last_context;

	/*
	 * First try to acquire a context without blocking on the lock. We start
	 * with the last context we successfully used, which should lead to
	 * backends spreading to different contexts over time.
	 */
	for (int i = 0; i < aio_ctl->num_contexts; i++)
	{
		context = &aio_ctl->contexts[my_aio->last_context];

		if (LWLockConditionalAcquire(&context->submission_lock, LW_EXCLUSIVE))
		{
			return context;
		}

		if (++my_aio->last_context == aio_ctl->num_contexts)
			my_aio->last_context = 0;
	}

	/*
	 * Couldn't acquire any without blocking. Block on the last + 1.;
	 */
	if (++my_aio->last_context == aio_ctl->num_contexts)
		my_aio->last_context = 0;
	context = &aio_ctl->contexts[my_aio->last_context];

	elog(DEBUG2, "blocking acquiring io context %d, started on %d",
		 my_aio->last_context, init_last_context);

	LWLockAcquire(&context->submission_lock, LW_EXCLUSIVE);

	return context;
}

static int
pgaio_uring_submit(int max_submit, bool drain)
{
	PgAioInProgress *ios[PGAIO_SUBMIT_BATCH_SIZE];
	struct io_uring_sqe *sqe[PGAIO_SUBMIT_BATCH_SIZE];
	PgAioContext *context;
	int nios = 0;

	context = pgaio_acquire_context();

	Assert(max_submit != 0 && max_submit <= my_aio->pending_count);

	while (!dlist_is_empty(&my_aio->pending))
	{
		dlist_node *node;
		PgAioInProgress *io;

		if (nios == max_submit)
			break;

		/*
		 * XXX: Should there be a per-ring limit? If so, we'd probably best
		 * apply it here.
		 */

		sqe[nios] = io_uring_get_sqe(&context->io_uring_ring);

		if (!sqe[nios])
		{
			Assert(nios != 0);
			elog(WARNING, "io_uring_get_sqe() returned NULL?");
			break;
		}

		node = dlist_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);
		ios[nios] = io;

		pgaio_io_prepare_submit(io, context - aio_ctl->contexts);

		pgaio_uring_sq_from_io(context, ios[nios], sqe[nios]);

		nios++;
	}

	Assert(nios > 0);

	{
		int ret;

		pg_atomic_add_fetch_u32(&my_aio->inflight_count, nios);
		my_aio->submissions_total_count++;

again:
		pgstat_report_wait_start(WAIT_EVENT_AIO_SUBMIT);
		ret = io_uring_submit(&context->io_uring_ring);
		pgstat_report_wait_end();

		if (ret == -EINTR)
			goto again;

		if (ret < 0)
			elog(PANIC, "failed: %d/%s",
				 ret, strerror(-ret));
	}

	LWLockRelease(&context->submission_lock);

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

	/* callbacks will be called later by pgaio_submit() */
	if (drain)
		pgaio_drain(context, false, false);

	return nios;
}

static int
pgaio_uring_drain(PgAioContext *context)
{
	uint32 processed = 0;
	int ready;

	Assert(!LWLockHeldByMe(&context->completion_lock));

	if (!io_uring_cq_ready(&context->io_uring_ring))
		return 0;

	LWLockAcquire(&context->completion_lock, LW_EXCLUSIVE);

	/*
	 * Don't drain more events than available right now. Otherwise it's
	 * plausible that one backend could get stuck, for a while, receiving CQEs
	 * without actually processing them.
	 */
	ready = io_uring_cq_ready(&context->io_uring_ring);

	while (ready > 0)
	{
		struct io_uring_cqe *reaped_cqes[PGAIO_MAX_LOCAL_REAPED];
		uint32 processed_one;

		processed_one =
			io_uring_peek_batch_cqe(&context->io_uring_ring,
									reaped_cqes,
									Min(PGAIO_MAX_LOCAL_REAPED, ready));
		Assert(processed_one <= ready);

		ready -= processed_one;
		processed += processed_one;

		for (int i = 0; i < processed_one; i++)
		{
			struct io_uring_cqe *cqe = reaped_cqes[i];

			pgaio_uring_io_from_cqe(context, cqe);

			io_uring_cqe_seen(&context->io_uring_ring, cqe);
		}
	}

	if (context->reaped_iovecs_count > context->unused_iovecs_count &&
		LWLockConditionalAcquire(&context->submission_lock, LW_EXCLUSIVE))
	{
		ereport(DEBUG4,
				errmsg("plenty reaped iovecs (%d), transferring",
					   context->reaped_iovecs_count),
				errhidestmt(true),
				errhidecontext(true));

		pgaio_uring_iovec_transfer(context);
		LWLockRelease(&context->submission_lock);
	}

	LWLockRelease(&context->completion_lock);

	return processed;
}

static void
pgaio_uring_wait_one(PgAioContext *context, PgAioInProgress *io, uint32 wait_event_info)
{
	PgAioIPFlags flags;
	int ret;

	/* ensure we're going to get woken up */
	if (IsUnderPostmaster)
	{
		ConditionVariablePrepareToSleep(&io->cv);
		ResetLatch(MyLatch);
		MyLatch->maybe_sleeping = true;
		pg_memory_barrier();
	}

	/*
	 * If the IO is still in progress (could have finished concurrently by
	 * another backend), wait for it using io_uring_enter.
	 */
	flags = *(volatile PgAioIPFlags*) &io->flags;
	if (flags & PGAIOIP_INFLIGHT)
	{

#ifdef PGAIO_VERBOSE
		ereport(DEBUG3,
				errmsg("sys enter %zu, ready %d ",
					   io - aio_ctl->in_progress_io,
					   io_uring_cq_ready(&context->io_uring_ring)),
				errhidestmt(true),
				errhidecontext(true));
#endif

		/* wait for one io to be completed */
		errno = 0;
		pgstat_report_wait_start(wait_event_info);
		ret = __sys_io_uring_enter(context->io_uring_ring.ring_fd,
								   0, 1,
								   IORING_ENTER_GETEVENTS, &SleepSig);
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
		ConditionVariableCancelSleep();
		Assert(MyLatch->maybe_sleeping);
		MyLatch->maybe_sleeping = false;
	}
}

static void
pgaio_uring_io_from_cqe(PgAioContext *context, struct io_uring_cqe *cqe)
{
	PgAioInProgress *io;

	io = io_uring_cqe_get_data(cqe);
	Assert(io != NULL);
	Assert(io->flags & PGAIOIP_INFLIGHT);
	Assert(io->system_referenced);

	*(volatile PgAioIPFlags*) &io->flags = (io->flags & ~PGAIOIP_INFLIGHT) | PGAIOIP_REAPED;
	io->result = cqe->res;

	dlist_push_tail(&my_aio->reaped, &io->io_node);

	if (io->used_iovec != -1)
	{
		PgAioIovec *iovec = &context->iovecs[io->used_iovec];

		slist_push_head(&context->reaped_iovecs, &iovec->node);
		context->reaped_iovecs_count++;
	}

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
static void
prep_read_buffer_iov(PgAioInProgress *io, struct io_uring_sqe *sqe, struct iovec *iovs)
{
	PgAioInProgress *cur;
	uint32 offset = io->d.read_buffer.offset;
	int	niov = 0;

	cur = io;
	while (cur)
	{
		offset += cur->d.read_buffer.already_done;
		iovs[niov].iov_base = cur->d.read_buffer.bufdata + cur->d.read_buffer.already_done;
		iovs[niov].iov_len = cur->d.read_buffer.nbytes - cur->d.read_buffer.already_done;

		niov++;
		cur = cur->merge_with;
	}

	io_uring_prep_readv(sqe,
						io->d.read_buffer.fd,
						iovs,
						niov,
						offset);
}

static void
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
		cur = cur->merge_with;
	}

	io_uring_prep_writev(sqe,
						 io->d.write_buffer.fd,
						 iovs,
						 niov,
						 offset);
}

static void
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
		cur = cur->merge_with;
	}

	io_uring_prep_writev(sqe,
						io->d.write_wal.fd,
						iovs,
						niov,
						offset);
}

static void
prep_write_generic_iov(PgAioInProgress *io, struct io_uring_sqe *sqe, struct iovec *iovs)
{
	PgAioInProgress *cur;
	off_t offset = io->d.write_generic.offset;
	int	niov = 0;

	cur = io;
	while (cur)
	{
		offset += cur->d.write_generic.already_done;
		iovs[niov].iov_base = cur->d.write_generic.bufdata + cur->d.write_generic.already_done;
		iovs[niov].iov_len = cur->d.write_generic.nbytes - cur->d.write_generic.already_done;

		niov++;
		cur = cur->merge_with;
	}

	io_uring_prep_writev(sqe,
						io->d.write_generic.fd,
						iovs,
						niov,
						offset);
}

static void
pgaio_uring_iovec_transfer(PgAioContext *context)
{
	Assert(LWLockHeldByMe(&context->submission_lock));
	Assert(LWLockHeldByMe(&context->completion_lock));

	while (!slist_is_empty(&context->reaped_iovecs))
	{
		slist_push_head(&context->unused_iovecs, slist_pop_head_node(&context->reaped_iovecs));
	}

	context->unused_iovecs_count += context->reaped_iovecs_count;
	context->reaped_iovecs_count = 0;
}

static struct PgAioIovec *
pgaio_uring_iovec_get(PgAioContext *context, PgAioInProgress *io)
{
	slist_node *node;
	PgAioIovec *iov;

	if (context->unused_iovecs_count == 0)
	{
		ereport(DEBUG2,
				errmsg("out of unused iovecs, transferring %d reaped ones",
					   context->reaped_iovecs_count),
				errhidestmt(true),
				errhidecontext(true));
		LWLockAcquire(&context->completion_lock, LW_EXCLUSIVE);
		Assert(context->reaped_iovecs_count > 0);
		pgaio_uring_iovec_transfer(context);
		LWLockRelease(&context->completion_lock);
		Assert(context->unused_iovecs_count > 0);
	}

	context->unused_iovecs_count--;
	node = slist_pop_head_node(&context->unused_iovecs);
	iov = slist_container(PgAioIovec, node, node);

	io->used_iovec = iov - context->iovecs;

	return iov;
}

static void
pgaio_uring_sq_from_io(PgAioContext *context, PgAioInProgress *io, struct io_uring_sqe *sqe)
{
	PgAioIovec *iovec;

	io->used_iovec = -1;

	switch (io->type)
	{
		case PGAIO_FSYNC:
			io_uring_prep_fsync(sqe,
								io->d.fsync.fd,
								io->d.fsync.datasync ? IORING_FSYNC_DATASYNC : 0);
			if (io->d.fsync.barrier)
				sqe->flags |= IOSQE_IO_DRAIN;
			break;

		case PGAIO_FSYNC_WAL:
			io_uring_prep_fsync(sqe,
								io->d.fsync_wal.fd,
								io->d.fsync_wal.datasync ? IORING_FSYNC_DATASYNC : 0);
			if (io->d.fsync.barrier)
				sqe->flags |= IOSQE_IO_DRAIN;
			break;

		case PGAIO_READ_BUFFER:
			iovec = pgaio_uring_iovec_get(context, io);
			prep_read_buffer_iov(io, sqe, iovec->iovec);
			//sqe->flags |= IOSQE_ASYNC;
			break;

		case PGAIO_WRITE_BUFFER:
			iovec = pgaio_uring_iovec_get(context, io);
			prep_write_buffer_iov(io, sqe, iovec->iovec);
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
			iovec = pgaio_uring_iovec_get(context, io);
			prep_write_wal_iov(io, sqe, iovec->iovec);
			if (io->d.write_wal.no_reorder)
				sqe->flags = IOSQE_IO_DRAIN;
			break;

		case PGAIO_WRITE_GENERIC:
			iovec = pgaio_uring_iovec_get(context, io);
			prep_write_generic_iov(io, sqe, iovec->iovec);
			if (io->d.write_generic.no_reorder)
				sqe->flags = IOSQE_IO_DRAIN;
			break;

		case PGAIO_NOP:
			elog(ERROR, "not yet");
			break;

		case PGAIO_INVALID:
			elog(ERROR, "invalid");
	}

	io_uring_sqe_set_data(sqe, io);
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

#endif

/* --------------------------------------------------------------------------------
 * Code dealing with specific IO types
 * --------------------------------------------------------------------------------
 */

void
pgaio_io_start_flush_range(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes)
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
	Assert(ShmemAddrIsValid(bufdata));

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
	ereport(DEBUG3,
			errmsg("start_read_buffer %zu: "
				   "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
				   io - aio_ctl->in_progress_io,
				   fd,
				   (unsigned long long) offset,
				   (unsigned long long) nbytes,
				   buffno,
				   bufdata),
		 errhidestmt(true),
		 errhidecontext(true));
#endif
}

void
pgaio_io_start_write_buffer(PgAioInProgress *io, const AioBufferTag *tag, int fd, uint32 offset, uint32 nbytes, char *bufdata, int buffno)
{
	Assert(ShmemAddrIsValid(bufdata));

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
	ereport(DEBUG3,
			errmsg("start_write_buffer %zu: "
				   "fd %d, off: %llu, bytes: %llu, buff: %d, data %p",
				   io - aio_ctl->in_progress_io,
				   fd,
				   (unsigned long long) offset,
				   (unsigned long long) nbytes,
				   buffno,
				   bufdata),
			errhidestmt(true),
			errhidecontext(true));
#endif
}

void
pgaio_io_start_write_wal(PgAioInProgress *io, int fd, XLogSegNo segno, uint32 offset, uint32 nbytes, char *bufdata, bool no_reorder, uint32 write_no)
{
	Assert(ShmemAddrIsValid(bufdata));

	pgaio_prepare_io(io, PGAIO_WRITE_WAL);

	io->d.write_wal.fd = fd;
	io->d.write_wal.tli = ThisTimeLineID;
	io->d.write_wal.segno = segno;
	io->d.write_wal.no_reorder = no_reorder;
	io->d.write_wal.offset = offset;
	io->d.write_wal.nbytes = nbytes;
	io->d.write_wal.bufdata = bufdata;
	io->d.write_wal.already_done = 0;
	io->d.write_wal.write_no = write_no;

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	ereport(DEBUG3,
			errmsg("start_write_wal %zu:"
				   "fd %d, off: %llu, bytes: %llu, no_reorder: %d, data %p",
				   io - aio_ctl->in_progress_io,
				   fd,
				   (unsigned long long) offset,
				   (unsigned long long) nbytes,
				   no_reorder,
				   bufdata),
			errhidestmt(true),
			errhidecontext(true));
#endif
}

void
pgaio_io_start_write_generic(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes, char *bufdata, bool no_reorder)
{
	Assert(ShmemAddrIsValid(bufdata));

	pgaio_prepare_io(io, PGAIO_WRITE_GENERIC);

	io->d.write_generic.fd = fd;
	io->d.write_generic.no_reorder = no_reorder;
	io->d.write_generic.offset = offset;
	io->d.write_generic.nbytes = nbytes;
	io->d.write_generic.bufdata = bufdata;
	io->d.write_generic.already_done = 0;

	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	ereport(DEBUG3,
			errmsg("start_write_generic %zu:"
				   "fd %d, off: %llu, bytes: %llu, no_reorder: %d, data %p",
				   io - aio_ctl->in_progress_io,
				   fd,
				   (unsigned long long) offset,
				   (unsigned long long) nbytes,
				   no_reorder,
				   bufdata),
			errhidestmt(true),
			errhidecontext(true));
#endif
}

void
pgaio_io_start_nop(PgAioInProgress *io)
{
	pgaio_prepare_io(io, PGAIO_NOP);
	pgaio_finish_io(io);
}

void
pgaio_io_start_fsync(PgAioInProgress *io, int fd, XLogSegNo segno, bool barrier)
{
	pgaio_prepare_io(io, PGAIO_FSYNC);
	io->d.fsync.fd = fd;
	io->d.fsync.tli = ThisTimeLineID;
	io->d.fsync.segno = segno;
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
pgaio_io_start_fdatasync(PgAioInProgress *io, int fd, XLogSegNo segno, bool barrier)
{
	pgaio_prepare_io(io, PGAIO_FSYNC);
	io->d.fsync.fd = fd;
	io->d.fsync.tli = ThisTimeLineID;
	io->d.fsync.segno = segno;
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

void
pgaio_io_start_fsync_wal(PgAioInProgress *io, int fd, XLogSegNo segno, bool barrier, bool datasync_only, uint32 flush_no)
{
	pgaio_prepare_io(io, PGAIO_FSYNC_WAL);
	io->d.fsync_wal.fd = fd;
	io->d.fsync_wal.tli = ThisTimeLineID;
	io->d.fsync_wal.segno = segno;
	io->d.fsync_wal.barrier = barrier;
	io->d.fsync_wal.datasync = datasync_only;
	io->d.fsync_wal.flush_no = flush_no;
	pgaio_finish_io(io);

#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "start_fsync_wal %zu:"
		 "fd %d, is_barrier: %d, is_datasync: %d, flush_no: %d",
		 io - aio_ctl->in_progress_io,
		 fd,
		 barrier,
		 datasync_only,
		 flush_no);
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
pgaio_complete_fsync_wal(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed fsync_wal: %zu",
		 io - aio_ctl->in_progress_io);
#endif
	if (io->result != 0)
		elog(PANIC, "fsync_wal needs better error handling");

	XLogFlushComplete(io, io->d.fsync_wal.flush_no);
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
	ereport(io->flags & PGAIOIP_RETRY ? DEBUG1 : DEBUG3,
			errmsg("completed read_buffer: %zu, %d/%s, buf %d",
				   io - aio_ctl->in_progress_io,
				   io->result,
				   io->result < 0 ? strerror(-io->result) : "ok",
				   io->d.read_buffer.buf),
			errhidestmt(true),
			errhidecontext(true));
#endif

	if (io->result != (io->d.read_buffer.nbytes - io->d.read_buffer.already_done))
	{
		MemoryContext old_context = MemoryContextSwitchTo(aio_retry_context);

		failed = true;

		//pgaio_io_print(io, NULL);

		if (io->result < 0)
		{
			if (io->result == -EAGAIN || io->result == -EINTR)
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
	ereport(DEBUG3,
			errmsg("completed write_buffer: %zu, %d/%s, buf %d",
				   io - aio_ctl->in_progress_io,
				   io->result,
				   io->result < 0 ? strerror(-io->result) : "ok",
				   io->d.write_buffer.buf),
			errhidestmt(true),
			errhidecontext(true));
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
	ereport(DEBUG3,
			errmsg("completed write_wal: %zu, %d/%s",
				   io - aio_ctl->in_progress_io,
				   io->result,
				   io->result < 0 ? strerror(-io->result) : "ok"),
			errhidestmt(true),
			errhidecontext(true));
#endif

	if (io->result < 0)
	{
		if (io->result == -EAGAIN || io->result == -EINTR)
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

	XLogWriteComplete(io, io->d.write_wal.write_no);

	return true;
}


static bool
pgaio_complete_write_generic(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	ereport(DEBUG3,
			errmsg("completed write_generic: %zu, %d/%s",
				   io - aio_ctl->in_progress_io,
				   io->result,
				   io->result < 0 ? strerror(-io->result) : "ok"),
			errhidestmt(true),
			errhidecontext(true));
#endif

	if (io->result < 0)
	{
		if (io->result == -EAGAIN || io->result == -EINTR)
		{
			elog(WARNING, "need to implement retries");
		}

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to log file: %s",
						strerror(-io->result))));
	}
	else if (io->result != (io->d.write_generic.nbytes - io->d.write_generic.already_done))
	{
		/* FIXME: implement retries for short writes */
		/* FIXME: not WAL */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to log file: wrote only %d of %d bytes",
						io->result, (io->d.write_generic.nbytes - io->d.write_generic.already_done))));
	}

	return true;
}

static void
pgaio_worker_state_init(PgAioWorkerState *state)
{
	state->wal_fd = -1;
	state->wal_segno = -1;
	state->wal_tli = -1;
}

static void
pgaio_worker_state_close(PgAioWorkerState *state)
{
	if (state->wal_fd >= 0)
		close(state->wal_fd);
	pgaio_worker_state_init(state);
}

/*
 * Extract iov_base and iov_len from a single IO.
 */
static void
pgaio_fill_one_iov(struct iovec *iov, const PgAioInProgress *io)
{
	switch (io->type)
	{
	case PGAIO_WRITE_WAL:
		iov->iov_base = io->d.write_wal.bufdata + io->d.write_wal.already_done;
		iov->iov_len = io->d.write_wal.nbytes;
		break;
	case PGAIO_READ_BUFFER:
		iov->iov_base = io->d.read_buffer.bufdata + io->d.read_buffer.already_done;
		iov->iov_len = io->d.read_buffer.nbytes;
		break;
	case PGAIO_WRITE_BUFFER:
		iov->iov_base = io->d.write_buffer.bufdata + io->d.write_buffer.already_done;
		iov->iov_len = io->d.write_buffer.nbytes;
		break;
	case PGAIO_WRITE_GENERIC:
		iov->iov_base = io->d.write_generic.bufdata + io->d.write_generic.already_done;
		iov->iov_len = io->d.write_generic.nbytes;
		break;
	default:
		elog(ERROR, "unexpected IO type while populating iovec");
	}
}

/*
 * Populate an array of iovec objects with the address ranges from a chain of
 * merged IOs.  Return the number of iovecs (which may be smaller than the
 * number of IOs).
 */
static int
pgaio_fill_iov(struct iovec *iovs, const PgAioInProgress *io)
{
	struct iovec *iov;

	/* Fill in the first one. */
	iov = &iovs[0];
	pgaio_fill_one_iov(iov, io);
	
	/*
	 * We have a chain of IOs that were linked together because they access
	 * contiguous regions of a file.  As a micro-optimization we'll also
	 * consolidate iovecs that access contiguous memory.
	 */
	for (io = io->merge_with; io; io = io->merge_with)
	{
		struct iovec *next = iov + 1;

		pgaio_fill_one_iov(next, io);
		if ((char *) iov->iov_base + iov->iov_len == next->iov_base)
			iov->iov_len += next->iov_len;
		else
			++iov;
	}

	return iov + 1 - iovs;
}

static void
pgaio_worker_do(PgAioWorkerState *state, PgAioInProgress *io)
{
	SMgrRelation reln;
	AioBufferTag *tag = NULL;
	ssize_t result;
	size_t already_done;
	off_t offset;
	uint32 off;
	int fd = -1;
	XLogSegNo segno = -1;
	struct iovec iov[IOV_MAX];
	int iovcnt = 0;

	Assert(io->flags & PGAIOIP_INFLIGHT);

	/*
	 * Handle easy cases, and extract tag.  Also compute the total size of
	 * merged requests.  For now, pgaio_can_be_combined() only allows
	 * consecutive blocks to be merged for worker mode, so it's enough to sum
	 * up the size of merged requests.
	 */
	switch (io->type)
	{
	case PGAIO_NOP:
		break;
	case PGAIO_FLUSH_RANGE:
		/* XXX not supported yet */
		result = 0;
		already_done = 0;
		goto done;
	case PGAIO_READ_BUFFER:
		tag = &io->d.read_buffer.tag;
		fd = io->d.read_buffer.fd;
		offset = io->d.read_buffer.offset;
		already_done = io->d.read_buffer.already_done;
		break;
	case PGAIO_WRITE_BUFFER:
		tag = &io->d.write_buffer.tag;
		fd = io->d.write_buffer.fd;
		offset = io->d.write_buffer.offset;
		already_done = io->d.write_buffer.already_done;
		break;
	case PGAIO_FSYNC:
		/* TODO: derive fd from flush_no? */
		fd = io->d.fsync_wal.fd;
		segno = io->d.fsync_wal.segno;
		ThisTimeLineID = io->d.fsync_wal.tli;
		already_done = 0;
		break;
	case PGAIO_FSYNC_WAL:
		/* TODO: derive fd from flush_no? */
		fd = io->d.fsync.fd;
		segno = io->d.fsync.segno;
		ThisTimeLineID = io->d.fsync.tli;
		already_done = 0;
		break;
	case PGAIO_WRITE_WAL:
		fd = io->d.write_wal.fd;
		segno = io->d.write_wal.segno;
		ThisTimeLineID = io->d.write_wal.tli;
		offset = io->d.write_wal.offset;
		already_done = io->d.write_wal.already_done;
		break;
	case PGAIO_WRITE_GENERIC:
		fd = io->d.write_wal.fd;
		offset = io->d.write_generic.offset;
		already_done = io->d.write_generic.already_done;
		break;
	default:
		result = -1;
		errno = EOPNOTSUPP;
		already_done = 0;
		goto done;
	}

	/* Get our hands on the file descriptor. */
	switch (io->type)
	{
	case PGAIO_READ_BUFFER:
	case PGAIO_WRITE_BUFFER:
		if (IsUnderPostmaster)
		{
			/* Open the relation. */
			reln = smgropen(tag->rnode.node, tag->rnode.backend);
			fd = smgrfd(reln, tag->forkNum, tag->blockNum, &off);
			if (fd < 0)
			{
				errno = EBADF;		/* ??? */
				result = -1;
				goto done;
			}
		}
		else
		{
			/* Otherwise running synchronously, and we use the IO's fd. */
		}
		break;
	case PGAIO_FSYNC:
	case PGAIO_FSYNC_WAL:
	case PGAIO_WRITE_WAL:
		if (segno != -1)
		{
			/* Do we already have this WAL segment open? */
			if (state->wal_fd < 0 ||
				state->wal_tli != ThisTimeLineID ||
				state->wal_segno != segno)
			{
				if (state->wal_fd < 0)
				{
					close(state->wal_fd);
					state->wal_fd = -1;
				}
				/* XXX panics on failure! */
				state->wal_fd = XLogFileOpen(segno);
				Assert(state->wal_fd >= 0);
				state->wal_tli = ThisTimeLineID;
				state->wal_segno = segno;
			}
			fd = state->wal_fd;
		}
		else
		{
			/* Otherwise running synchronously, and we use the IO's fd. */
			Assert(MyBackendType != B_AIO_WORKER);
		}
		break;
	default:
		break;
	}

	/* Build array of iovec objects for scatter/gather I/O. */
	switch (io->type)
	{
	case PGAIO_READ_BUFFER:
	case PGAIO_WRITE_BUFFER:
	case PGAIO_WRITE_WAL:
	case PGAIO_WRITE_GENERIC:
		iovcnt = pgaio_fill_iov(iov, io);
		break;
	default:
		break;
	}

	/* Perform IO. */
	switch (io->type)
	{
	case PGAIO_FLUSH_RANGE:
		/* XXX not implemented */
		result = 0;
		break;
	case PGAIO_FSYNC:
	case PGAIO_FSYNC_WAL:
		pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);
		if (io->d.fsync.datasync)
			result = fdatasync(fd);
		else
			result = fsync(fd);
		pgstat_report_wait_end();
		break;
	case PGAIO_READ_BUFFER:
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
		result = pg_preadv(fd, iov, iovcnt, offset + already_done);
		pgstat_report_wait_end();
		break;
	case PGAIO_WRITE_BUFFER:
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
		result = pg_pwritev(fd, iov, iovcnt, offset + already_done);
		pgstat_report_wait_end();
		break;
	case PGAIO_WRITE_WAL:
		pgstat_report_wait_start(WAIT_EVENT_WAL_WRITE);
		result = pg_pwritev(fd, iov, iovcnt, offset + already_done);
		pgstat_report_wait_end();
		break;
	case PGAIO_WRITE_GENERIC:
		pgstat_report_wait_start(0); /* TODO: need a new wait event? */
		result = pg_pwritev(fd, iov, iovcnt, offset + already_done);
		pgstat_report_wait_end();
		break;
	default:
		result = -1;
		errno = EOPNOTSUPP;
		break;
	}

done:
	/* Encode result and error into io->result. */
	io->result = result < 0 ? -errno : result;

	/*
	 * We'll reap the IO immediately.  This might be running in a regular
	 * worker or a background worker, so we can't actually complete reaped IOs
	 * just yet, because a regular backend might not be in the right context
	 * for that.  (???)
	 */
	io->flags = (io->flags & ~PGAIOIP_INFLIGHT) | PGAIOIP_REAPED;
	dlist_push_tail(&my_aio->reaped, &io->io_node);

	/* It might need to be unmerged into multiple IOs. */
	if (io->flags & PGAIOIP_MERGE)
		pgaio_uncombine_one(io);
}

void
AioWorkerMain(void)
{
	PgAioWorkerState state;

	MyBackendType = B_AIO_WORKER;

	/* TODO review all signals */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, die);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, SignalHandlerForCrashExit);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, die);
	pqsignal(SIGURG, latch_sigurg_handler);
	PG_SETMASK(&UnBlockSig);

	/* XXX Install setjmp handler to log errors and exit */

	pgaio_worker_state_init(&state);
	for (;;)
	{
		uint32 io_index;

		if (squeue32_dequeue(aio_submission_queue, &io_index))
		{
			ConditionVariableCancelSleep();
			ConditionVariableBroadcast(&aio_ctl->submission_queue_not_full);

			/* Perform the IO. */
			pgaio_worker_do(&state, &aio_ctl->in_progress_io[io_index]);

			/*
			 * XXX We could theoretically push this into a shared queue for regular
			 * backends to drain, like the other AIO implementations, but for
			 * now we'll run the completion events directly in the AIO worker.
			 */
			pgaio_complete_ios(false);
		}
		else
		{
			ConditionVariableSleep(&aio_ctl->submission_queue_not_empty,
								   0 /* XXX wait event */);
		}
	}
	/* XXX not reached, but if we had nice shutdown signalling... */
	pgaio_worker_state_close(&state);
}

/* --------------------------------------------------------------------------------
 * SQL interface functions
 * --------------------------------------------------------------------------------
 */

Datum
pg_stat_get_aio_backends(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_AIO_BACKEND_COLS	13

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
		values[ 6] = Int64GetDatum(pg_atomic_read_u32(&bs->inflight_count));
		values[ 7] = Int32GetDatum(bs->unused_count);
		values[ 8] = Int32GetDatum(bs->outstanding_count);
		values[ 9] = Int32GetDatum(bs->pending_count);
		values[10] = Int32GetDatum(bs->local_completed_count);
		values[11] = Int32GetDatum(bs->foreign_completed_count);
		values[12] = Int32GetDatum(bs->last_context);


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
		uint32      owner_id;
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

		owner_id = io->owner_id; // XXX: READ_ONCE needed?
		if (owner_id != INVALID_PGPROCNO)
		{
			owner_pid = ProcGlobal->allProcs[owner_id].pid;
			values[ 4] = Int32GetDatum(owner_pid);
		}
		else
		{
			nulls[ 4] = true;
		}

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
