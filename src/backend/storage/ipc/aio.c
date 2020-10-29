/*
 * Big picture changes needed:
 * - backend local recycleable IOs
 * - retries for AIOs that cannot be retried shared (e.g. XLogFileInit())
 * - docs / comments / cleanup
 * - split aio.c in multiple files (aio.c, aio_uring.c, aio_worker.c, aio_cb.c)
 *   - introduce aio_internal.h for that purpose
 * - move more of the shared callback logic into bufmgr.c etc
 * - merging of IOs when submitting individual IOs, not when submitting all pending IOs
 * - Shrink size of PgAioInProgress
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
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
#include "port/pg_iovec.h"
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

#define PGAIO_VERBOSE


/*
 * FIXME: This is just so large because merging happens when submitting
 * pending requests, rather than when staging them.
 */
#define PGAIO_SUBMIT_BATCH_SIZE 256
#define PGAIO_MAX_LOCAL_REAPED 128
#define PGAIO_MAX_COMBINE 16

#define PGAIO_NUM_CONTEXTS 8

/*
 * The type of AIO.
 *
 * To keep PgAioInProgress smaller try to tell the compiler to only use the
 * minimal space. We could alternatively just use a uint8, but then we'd need
 * casts in more places...
 */
typedef enum pg_attribute_packed_desired() PgAioOp
{
	/* intentionally the zero value, to help catch zeroed memory etc */
	PGAIO_OP_INVALID = 0,

	PGAIO_OP_READ,
	PGAIO_OP_WRITE,

	PGAIO_OP_FSYNC,

	PGAIO_OP_FLUSH_RANGE,

	PGAIO_OP_NOP,
} PgAioOp;

typedef enum pg_attribute_packed_desired() PgAioSharedCallback
{
	/* intentionally the zero value, to help catch zeroed memory etc */
	PGAIO_SCB_INVALID = 0,

	PGAIO_SCB_READ_SB,
	PGAIO_SCB_READ_SMGR,

	PGAIO_SCB_WRITE_SB,
	PGAIO_SCB_WRITE_SMGR,
	PGAIO_SCB_WRITE_WAL,
	PGAIO_SCB_WRITE_GENERIC,

	PGAIO_SCB_FSYNC_RAW,
	PGAIO_SCB_FSYNC_WAL,

	PGAIO_SCB_FLUSH_RANGE_RAW,
	PGAIO_SCB_FLUSH_RANGE_SMGR,

	PGAIO_SCB_NOP,
} PgAioSharedCallback;


typedef enum PgAioInProgressFlags
{
	/* request in the ->unused list */
	PGAIOIP_UNUSED = 1 << 0,

	/*  */
	PGAIOIP_IDLE = 1 << 1,

	PGAIOIP_PREP = 1 << 2,

	/*  */
	PGAIOIP_IN_PROGRESS = 1 << 3,

	/* somewhere */
	PGAIOIP_PENDING = 1 << 4,

	/* request in kernel */
	PGAIOIP_INFLIGHT = 1 << 5,

	/* request reaped */
	PGAIOIP_REAPED = 1 << 6,

	/* shared completion callback was called */
	PGAIOIP_SHARED_CALLBACK_CALLED = 1 << 7,

	/* completed */
	PGAIOIP_DONE = 1 << 8,

	PGAIOIP_FOREIGN_DONE = 1 << 9,

	PGAIOIP_RETRY = 1 << 10,

	/* request failed completely */
	PGAIOIP_HARD_FAILURE = 1 << 11,

	/* request failed partly, e.g. a short write */
	PGAIOIP_SOFT_FAILURE = 1 << 12,

	PGAIOIP_SHARED_FAILED = 1 << 13,

	/* local completion callback was called */
	PGAIOIP_LOCAL_CALLBACK_CALLED = 1 << 14,

} PgAioInProgressFlags;

typedef uint16 PgAioIPFlags;

struct PgAioInProgress
{
	/* basic IO types supported by aio.c */
	PgAioOp op;

	/* shared callback for this IO, see io_action_cbs */
	PgAioSharedCallback scb;

	/* which AIO ring is this entry active for */
	uint8 ring;

	PgAioIPFlags flags;

	bool user_referenced;
	bool system_referenced;

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

	/*
	 * PGAIO_BB_INVALID, or index into aio_ctl->bounce_buffers
	 */
#define PGAIO_BB_INVALID PG_UINT32_MAX
	uint32 bb_idx;

	/*
	 * PGAIO_MERGE_INVALID if not merged, index into aio_ctl->in_progress_io
	 * otherwise.
	 */
#define PGAIO_MERGE_INVALID PG_UINT32_MAX
	uint32 merge_with_idx;

	uint64 generation;

	/*
	 * Data necessary for basic IO types (PgAioOp).
	 *
	 * NB: Note that fds in here may *not* be relied upon for re-issuing
	 * requests (e.g. for partial reads/writes) - the fd might be from another
	 * process, or closed since. That's not a problem for IOs waiting to be
	 * issued only because the queue is flushed when closing an fd.
	 */
	union {
		struct
		{
			int fd;
			uint32 nbytes;
			uint64 offset;
			uint32 already_done;
			char *bufdata;
		} read;

		struct
		{
			int fd;
			uint32 nbytes;
			uint64 offset;
			uint32 already_done;
			char *bufdata;
		} write;

		struct
		{
			int fd;
			bool datasync;
		} fsync;

		struct
		{
			int fd;
			uint32 nbytes;
			uint64 offset;
		} flush_range;
	} op_data;

	union {
		struct
		{
			Buffer buffid;
			BackendId backend;
			int mode;
		} read_sb;

		struct
		{
			AioBufferTag tag;
		} read_smgr;

		struct
		{
			Buffer buffid;
			BackendId backend;
			bool release_lock;
		} write_sb;

		struct
		{
			AioBufferTag tag;
		} write_smgr;

		struct
		{
			uint32 write_no;
		} write_wal;

		struct
		{
			uint32 flush_no;
		} fsync_wal;

		struct
		{
			AioBufferTag tag;
		} flush_range_smgr;
	} scb_data;
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
#else
	/* msvc doesn't like empty structs */
	int pro_forma_member;
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
	squeue32 *aio_submission_queue;


	int backend_state_count;
	PgAioPerBackend *backend_state;

	uint32 num_contexts;
	PgAioContext *contexts;

	PgAioInProgress in_progress_io[FLEXIBLE_ARRAY_MEMBER];
} PgAioCtl;

/* general pgaio helper functions */
static void pgaio_complete_ios(bool in_error);
static void pgaio_apply_backend_limit(void);
static void pgaio_io_prepare(PgAioInProgress *io, PgAioOp op);
static void pgaio_io_stage(PgAioInProgress *io, PgAioSharedCallback scb);
static void pgaio_io_prepare_submit(PgAioInProgress *io, uint32 ring);
static void pgaio_bounce_buffer_release_internal(PgAioBounceBuffer *bb, bool holding_lock, bool release_resowner);
static void pgaio_io_ref_internal(PgAioInProgress *io, PgAioIoRef *ref);
static void pgaio_transfer_foreign_to_local(void);
static void pgaio_call_local_callbacks(bool in_error);
static int pgaio_fill_iov(struct iovec *iovs, const PgAioInProgress *io);
static int pgaio_synchronous_submit(void);
static void pgaio_io_wait_ref_int(PgAioIoRef *ref, bool call_shared, bool call_local);

/* printing functions */
static const char *pgaio_io_shared_callback_string(PgAioSharedCallback a);
static const char *pgaio_io_operation_string(PgAioOp op);

/* aio worker related functions */
static int pgaio_worker_submit(int max_submit, bool drain);
static void pgaio_worker_do(PgAioInProgress *io);

#ifdef USE_LIBURING
/* io_uring related functions */
static int pgaio_uring_submit(int max_submit, bool drain);
static int pgaio_uring_drain(PgAioContext *context, bool call_shared);
static void pgaio_uring_wait_one(PgAioContext *context, PgAioInProgress *io, uint64 generation, uint32 wait_event_info);

static void pgaio_uring_sq_from_io(PgAioContext *context, PgAioInProgress *io, struct io_uring_sqe *sqe);
static void pgaio_uring_io_from_cqe(PgAioContext *context, struct io_uring_cqe *cqe);
static void pgaio_uring_iovec_transfer(PgAioContext *context);

#endif

/* IO callbacks */
static void pgaio_invalid_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_nop_complete(PgAioInProgress *io);
static void pgaio_nop_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_fsync_raw_complete(PgAioInProgress *io);
static void pgaio_fsync_raw_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_fsync_wal_complete(PgAioInProgress *io);
static void pgaio_fsync_wal_retry(PgAioInProgress *io);
static void pgaio_fsync_wal_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_flush_range_complete(PgAioInProgress *io);
static void pgaio_flush_range_desc(PgAioInProgress *io, StringInfo s);
static void pgaio_flush_range_smgr_retry(PgAioInProgress *io);

static bool pgaio_read_sb_complete(PgAioInProgress *io);
static void pgaio_read_sb_retry(PgAioInProgress *io);
static void pgaio_read_sb_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_read_smgr_complete(PgAioInProgress *io);
static void pgaio_read_smgr_retry(PgAioInProgress *io);
static void pgaio_read_smgr_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_sb_complete(PgAioInProgress *io);
static void pgaio_write_sb_retry(PgAioInProgress *io);
static void pgaio_write_sb_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_smgr_complete(PgAioInProgress *io);
static void pgaio_write_smgr_retry(PgAioInProgress *io);
static void pgaio_write_smgr_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_wal_complete(PgAioInProgress *io);
static void pgaio_write_wal_retry(PgAioInProgress *io);
static void pgaio_write_wal_desc(PgAioInProgress *io, StringInfo s);

static bool pgaio_write_generic_complete(PgAioInProgress *io);
static void pgaio_write_generic_desc(PgAioInProgress *io, StringInfo s);


/*
 * Implementation of different AIO actions.
 *
 * To support EXEC_BACKEND environments, where we cannot rely on callback
 * addresses being equivalent across processes, PgAioInProgress does not point
 * directly to the type's PgAioActionCBs, but contains an index instead.
 */


/*
 * IO completion callback.
 */
typedef bool (*PgAioCompletedCB)(PgAioInProgress *io);

/*
 * IO retry callback.
 */
typedef void (*PgAioRetryCB)(PgAioInProgress *io);

/*
 * IO desc callback.
 */
typedef void (*PgAioDescCB)(PgAioInProgress *io, StringInfo s);

typedef struct PgAioActionCBs
{
	PgAioOp op;
	const char *name;
	PgAioRetryCB retry;
	PgAioCompletedCB complete;
	PgAioDescCB desc;
} PgAioActionCBs;

static const PgAioActionCBs io_action_cbs[] =
{
	[PGAIO_SCB_INVALID] =
	{
		.op = PGAIO_OP_INVALID,
		.name = "invalid",
		.desc = pgaio_invalid_desc,
	},

	[PGAIO_SCB_READ_SB] =
	{
		.op = PGAIO_OP_READ,
		.name = "sb",
		.retry = pgaio_read_sb_retry,
		.complete = pgaio_read_sb_complete,
		.desc = pgaio_read_sb_desc,
	},

	[PGAIO_SCB_READ_SMGR] =
	{
		.op = PGAIO_OP_READ,
		.name = "smgr",
		.retry = pgaio_read_smgr_retry,
		.complete = pgaio_read_smgr_complete,
		.desc = pgaio_read_smgr_desc,
	},

	[PGAIO_SCB_WRITE_SB] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "sb",
		.retry = pgaio_write_sb_retry,
		.complete = pgaio_write_sb_complete,
		.desc = pgaio_write_sb_desc,
	},

	[PGAIO_SCB_WRITE_SMGR] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "smgr",
		.retry = pgaio_write_smgr_retry,
		.complete = pgaio_write_smgr_complete,
		.desc = pgaio_write_smgr_desc,
	},

	[PGAIO_SCB_WRITE_WAL] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "wal",
		.retry = pgaio_write_wal_retry,
		.complete = pgaio_write_wal_complete,
		.desc = pgaio_write_wal_desc,
	},

	[PGAIO_SCB_WRITE_GENERIC] =
	{
		.op = PGAIO_OP_WRITE,
		.name = "generic",
		.complete = pgaio_write_generic_complete,
		.desc = pgaio_write_generic_desc,
	},

	[PGAIO_SCB_FSYNC_RAW] =
	{
		.op = PGAIO_OP_FSYNC,
		.name = "raw",
		.complete = pgaio_fsync_raw_complete,
		.desc = pgaio_fsync_raw_desc,
	},

	[PGAIO_SCB_FSYNC_WAL] =
	{
		.op = PGAIO_OP_FSYNC,
		.name = "wal",
		.retry = pgaio_fsync_wal_retry,
		.complete = pgaio_fsync_wal_complete,
		.desc = pgaio_fsync_wal_desc,
	},

	[PGAIO_SCB_FLUSH_RANGE_RAW] =
	{
		.op = PGAIO_OP_FLUSH_RANGE,
		.name = "raw",
		.complete = pgaio_flush_range_complete,
		.desc = pgaio_flush_range_desc,
	},

	[PGAIO_SCB_FLUSH_RANGE_SMGR] =
	{
		.op = PGAIO_OP_FLUSH_RANGE,
		.name = "smgr",
		.retry = pgaio_flush_range_smgr_retry,
		.complete = pgaio_flush_range_complete,
		.desc = pgaio_flush_range_desc,
	},

	[PGAIO_SCB_NOP] =
	{
		.op = PGAIO_OP_NOP,
		.name = "nop",
		.complete = pgaio_nop_complete,
		.desc = pgaio_nop_desc,
	},
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

int MyAioWorkerId;

#ifdef USE_LIBURING
/* io_uring local state */
struct io_uring local_ring;
#endif

/* Options for aio_type. */
const struct config_enum_entry aio_type_options[] = {
	{"worker", AIOTYPE_WORKER, false},
#ifdef USE_LIBURING
	{"io_uring", AIOTYPE_LIBURING, false},
#endif
	{NULL, 0, false}
};


/*
 * Ensure that flags is written once, rather than potentially multiple times
 * (e.g. once and'ing it, and once or'ing it).
 */
#define WRITE_ONCE_F(flags) *(volatile PgAioIPFlags*) &(flags)


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

	return sz;
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
			io->generation = 1;
			io->bb_idx = PGAIO_BB_INVALID;
			io->merge_with_idx = PGAIO_MERGE_INVALID;
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
			aio_ctl->aio_submission_queue =
				ShmemInitStruct("aio submission queue",
								AioSubmissionQueueShmemSize(),
								&found);
			Assert(!found);
			squeue32_init(aio_ctl->aio_submission_queue, aio_worker_queue_size);
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
}

void
pgaio_postmaster_init(void)
{
	/* FIXME: should also be allowed to use AIO */
	dlist_init(&local_recycle_requests);

	// XXX: could create a local queue here.
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
	 * When exitinging in a normal backend there will be no pending IOs due to
	 * pgaio_at_commit()/pgaio_at_abort(). But for aux processes that won't be
	 * the case, so do so explicitly.
	 */
	pgaio_at_abort();


	/*
	 * Need to wait for in-progress IOs initiated by this backend to
	 * finish. Some operating systems, like linux w/ io_uring, cancel IOs that
	 * are still in progress when exiting. Other's don't provide access to the
	 * results of such IOs.
	 */

	/* move all requests onto issued_abandoned */
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

	my_aio = NULL;
}

void
pgaio_postmaster_child_init(void)
{
	Assert(!my_aio);

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

	/* try to spread out a bit from the start */
	my_aio->last_context = MyProcPid % PGAIO_NUM_CONTEXTS;

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

/* helper pgaio_process_io_completion combining the read/write cases */
static void
pgaio_process_io_rw_completion(PgAioInProgress *myio,
							   bool first,
							   uint32 nbytes,
							   uint64 offset,
							   uint32 *already_done,
							   int result,
							   int *running_result_p,
							   int *new_result,
							   PgAioIPFlags *new_flags)
{
	int running_result = *running_result_p;
	int remaining_bytes;

	Assert(first || *already_done == 0);
	Assert(*already_done < nbytes);

	remaining_bytes = nbytes - *already_done;

	if (result <= 0)
	{
		/* merged request failed, report failure on all */
		*new_result = result;

		if (result == -EAGAIN || result == -EINTR)
			*new_flags |= PGAIOIP_SOFT_FAILURE;
		else
			*new_flags |= PGAIOIP_HARD_FAILURE;

		ereport(DEBUG1,
				errcode_for_file_access(),
				errmsg("aio %zd: failed to %s:%s of %u-%u bytes at offset %llu+%u: %s",
					   myio - aio_ctl->in_progress_io,
					   pgaio_io_operation_string(myio->op),
					   pgaio_io_shared_callback_string(myio->scb),
					   nbytes, *already_done,
					   (long long unsigned) offset, *already_done,
					   strerror(-*new_result)));
	}
	else if (running_result == 0)
	{
		Assert(!first);
		*new_result = -EAGAIN;
		*new_flags |= PGAIOIP_SOFT_FAILURE;
	}
	else if (running_result >= remaining_bytes)
	{
		/* request completely satisfied */
		*already_done += remaining_bytes;
		running_result -= remaining_bytes;
		*new_result = *already_done;
	}
	else
	{
		/*
		 * This is actually pretty common and harmless, happens e.g. when one
		 * parts of the read are in the kernel page cache, but others
		 * aren't. So don't issue WARNING/ERROR, but just retry.
		 *
		 * While it can happen with single BLCKSZ reads (since they're bigger
		 * than typical page sizes), it's made much more likely by us
		 * combining reads.
		 *
		 * For writes it can happen e.g. in case of memory pressure.
		 */

		ereport(DEBUG3,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("aio %zd: partial %s:%s of %u-%u bytes at offset %llu+%u: read only %u of %u",
						myio - aio_ctl->in_progress_io,
						pgaio_io_operation_string(myio->op),
						pgaio_io_shared_callback_string(myio->scb),
						nbytes, *already_done,
						(long long unsigned) offset, *already_done,
						running_result,
						nbytes - *already_done)));

		Assert(running_result < remaining_bytes);
		*already_done += running_result;
		*new_result = *already_done;
		running_result = 0;

		*new_flags |= PGAIOIP_SOFT_FAILURE;
	}

	*running_result_p = running_result;
}

static void
pgaio_process_io_completion(PgAioInProgress *io, int result)
{
	int running_result;
	PgAioInProgress *cur = io;
	bool first = true;

	Assert(io->flags & PGAIOIP_INFLIGHT);
	Assert(io->system_referenced);
	Assert(io->result == 0 || io->merge_with_idx == PGAIO_MERGE_INVALID);

	/* very useful for testing the retry logic */
	/*
	 * XXX: a probabilistic mode would be useful, especially if it also
	 * injected occasional EINTR/EAGAINs.
	 */
#if 0
	if (io->op == PGAIO_OP_READ && result > 4096)
		result = 4096;
#endif
#if 0
	if (result > 4096 && (
			io->scb == PGAIO_SCB_WRITE_SMGR
			|| io->scb == PGAIO_SCB_WRITE_SB
			|| io->scb == PGAIO_SCB_WRITE_WAL
			))
		result = 4096;
#endif

	/* treat 0 length write as ENOSPC */
	if (result == 0 && io->op == PGAIO_OP_WRITE)
		running_result = result = -ENOSPC;
	else
		running_result = result;

	while (cur)
	{
		PgAioInProgress *next;
		PgAioIPFlags new_flags;

		new_flags = cur->flags;

		Assert(new_flags & PGAIOIP_INFLIGHT);
		Assert(cur->system_referenced);
		Assert(cur->op == io->op);
		Assert(cur->scb == io->scb);

		if (cur->merge_with_idx != PGAIO_MERGE_INVALID)
		{
			next = &aio_ctl->in_progress_io[cur->merge_with_idx];
			cur->merge_with_idx = PGAIO_MERGE_INVALID;
		}
		else
			next = NULL;

		switch (cur->op)
		{
			case PGAIO_OP_READ:
				pgaio_process_io_rw_completion(cur,
											   first,
											   cur->op_data.read.nbytes,
											   cur->op_data.read.offset,
											   &cur->op_data.read.already_done,
											   result,
											   &running_result,
											   &cur->result,
											   &new_flags);
				break;

			case PGAIO_OP_WRITE:
				pgaio_process_io_rw_completion(cur,
											   first,
											   cur->op_data.write.nbytes,
											   cur->op_data.write.offset,
											   &cur->op_data.write.already_done,
											   result,
											   &running_result,
											   &cur->result,
											   &new_flags);
				break;

			case PGAIO_OP_FSYNC:
			case PGAIO_OP_FLUSH_RANGE:
			case PGAIO_OP_NOP:
				/* XXX: should we treat EINTR/EAGAIN as retryable? */
				cur->result = result;
				if (result < 0)
					new_flags |= PGAIOIP_HARD_FAILURE;
				break;

			case PGAIO_OP_INVALID:
				pg_unreachable();
				elog(ERROR, "invalid");
				break;
		}

		new_flags &= ~PGAIOIP_INFLIGHT;
		new_flags |= PGAIOIP_REAPED;

		WRITE_ONCE_F(cur->flags) = new_flags;

		dlist_push_tail(&my_aio->reaped, &cur->io_node);

		cur = next;
		first = false;
	}

}

static bool
pgaio_io_call_shared_complete(PgAioInProgress *io)
{
	Assert(io_action_cbs[io->scb].op == io->op);

#ifdef PGAIO_VERBOSE
	if (message_level_is_interesting(DEBUG3))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
		StringInfoData s;

		initStringInfo(&s);

		pgaio_io_print(io, &s);

		ereport(DEBUG3,
				errmsg("completing %s",
					   s.data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s.data);
		MemoryContextSwitchTo(oldcontext);
	}
#endif

	return io_action_cbs[io->scb].complete(io);
}

static void pg_noinline
pgaio_complete_ios(bool in_error)
{
	int pending_count_before = my_aio->pending_count;

	Assert(!LWLockHeldByMe(SharedAIOCtlLock));

	/*
	 * Don't want to recurse into proc_exit() or such while calling callbacks
	 * - we need to process the shared (but not local) callbacks.
	 */
	HOLD_INTERRUPTS();

	/* call all callbacks, without holding lock */
	while (!dlist_is_empty(&my_aio->reaped))
	{
		dlist_node *node = dlist_head_node(&my_aio->reaped);
		PgAioInProgress *io = dlist_container(PgAioInProgress, io_node, node);

		Assert(dlist_is_member(&my_aio->reaped, &io->io_node));

		Assert(node != NULL);

		if (!(io->flags & PGAIOIP_SHARED_CALLBACK_CALLED))
		{
			bool finished;

			/*
			 * Set flag before calling callback, otherwise we could easily end
			 * up looping forever.
			 */
			WRITE_ONCE_F(io->flags) |= PGAIOIP_SHARED_CALLBACK_CALLED;

			finished = pgaio_io_call_shared_complete(io);

			dlist_delete_from(&my_aio->reaped, node);

			if (finished)
			{
				dlist_push_tail(&local_recycle_requests, &io->io_node);
			}
			else
			{
				Assert(io->flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_HARD_FAILURE));

				LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
				WRITE_ONCE_F(io->flags) =
					(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
					PGAIOIP_DONE |
					PGAIOIP_SHARED_FAILED;
				dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
				LWLockRelease(SharedAIOCtlLock);

				/* signal state change */
				if (IsUnderPostmaster)
					ConditionVariableBroadcast(&io->cv);
			}
		}
		else
		{
			Assert(in_error);

			dlist_delete_from(&my_aio->reaped, node);

			LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
			WRITE_ONCE_F(io->flags) =
				(io->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
				PGAIOIP_DONE |
				PGAIOIP_HARD_FAILURE |
				PGAIOIP_SHARED_FAILED;
			dlist_push_tail(&aio_ctl->reaped_uncompleted, &io->io_node);
			LWLockRelease(SharedAIOCtlLock);
		}
	}

	RESUME_INTERRUPTS();

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
			Assert(!(cur->flags & (PGAIOIP_SHARED_FAILED)));
			Assert(!(cur->flags & (PGAIOIP_SOFT_FAILURE)));
			Assert(cur->merge_with_idx == PGAIO_MERGE_INVALID);

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

					WRITE_ONCE_F(cur->flags) =
						(cur->flags & ~(PGAIOIP_REAPED | PGAIOIP_IN_PROGRESS)) |
						PGAIOIP_DONE |
						PGAIOIP_FOREIGN_DONE;

					SpinLockRelease(&other->foreign_completed_lock);
				}
				else
				{
					WRITE_ONCE_F(cur->flags) =
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
				ereport(DEBUG4,
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

				if (cur->bb_idx != PGAIO_BB_INVALID)
				{
					pgaio_bounce_buffer_release_internal(&aio_ctl->bounce_buffers[cur->bb_idx],
														 /* holding_lock = */ true,
														 /* release_resowner = */ false);
					cur->bb_idx = PGAIO_BB_INVALID;
				}

				cur->op = PGAIO_OP_INVALID;
				cur->scb = PGAIO_SCB_INVALID;
				cur->owner_id = INVALID_PGPROCNO;
				cur->result = 0;
				cur->system_referenced = true;
				cur->on_completion_local = NULL;

				dlist_push_head(&aio_ctl->unused_ios, &cur->owner_node);
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

	Assert(my_aio->local_completed_count > 0);
	dlist_delete_from(&my_aio->local_completed, &io->io_node);
	Assert(my_aio->local_completed_count > 0);
	my_aio->local_completed_count--;

	dlist_delete_from(&my_aio->issued, &io->owner_node);
	Assert(my_aio->issued_count > 0);
	my_aio->issued_count--;
	dlist_push_tail(&my_aio->outstanding, &io->owner_node);
	my_aio->outstanding_count++;

	io->flags |= PGAIOIP_LOCAL_CALLBACK_CALLED;

	if (!io->on_completion_local)
		return;

	if (!in_error)
	{
		check_stack_depth();
		io->on_completion_local->callback(io->on_completion_local, io);
	}
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

				pgaio_io_call_local_callback(io, in_error);
			}

			local_callback_depth--;
		}
	}
}

/*
 * Receive completions in ring.
 */
static int pg_noinline
pgaio_drain(PgAioContext *context, bool call_shared, bool call_local)
{
	int ndrained = 0;

	if (aio_type == AIOTYPE_WORKER)
	{
		/*
		 * Worker mode has no completion queue, because the worker processes
		 * all completion work directly.
		 */
	}
#ifdef USE_LIBURING
	else if (aio_type == AIOTYPE_LIBURING)
		ndrained = pgaio_uring_drain(context, call_shared);
#endif

	if (call_shared)
	{
		pgaio_complete_ios(false);
		pgaio_transfer_foreign_to_local();
	}
	if (call_local)
	{
		Assert(call_shared);
		pgaio_call_local_callbacks(false);
	}
	return ndrained;
}

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
	return false;
}

static bool
pgaio_can_be_combined(PgAioInProgress *last, PgAioInProgress *cur)
{
	if (last->op != cur->op)
		return false;

	/* could be relaxed, but unlikely to be ever useful? */
	if (last->scb != cur->scb)
		return false;

	if (last->flags & PGAIOIP_RETRY ||
		cur->flags & PGAIOIP_RETRY)
		return false;

	switch (last->op)
	{
		case PGAIO_OP_READ:
			if (last->op_data.read.fd != cur->op_data.read.fd)
				return false;
			if ((last->op_data.read.offset + last->op_data.read.nbytes) != cur->op_data.read.offset)
				return false;
			if (!pgaio_can_scatter_gather() &&
				((last->op_data.read.bufdata + last->op_data.read.nbytes) != cur->op_data.read.bufdata))
				return false;
			return true;

		case PGAIO_OP_WRITE:
			if (last->op_data.write.fd != cur->op_data.write.fd)
				return false;
			if ((last->op_data.write.offset + last->op_data.write.nbytes) != cur->op_data.write.offset)
				return false;
			if (!pgaio_can_scatter_gather() &&
				((last->op_data.write.bufdata + last->op_data.write.nbytes) != cur->op_data.write.bufdata))
				return false;
			return true;

		case PGAIO_OP_FSYNC:
		case PGAIO_OP_FLUSH_RANGE:
		case PGAIO_OP_NOP:
			return false;
			break;
		case PGAIO_OP_INVALID:
			elog(ERROR, "unexpected");
			break;
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

	into->merge_with_idx = tomerge - aio_ctl->in_progress_io;
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
		if (cur->merge_with_idx != PGAIO_MERGE_INVALID)
		{
			elog(DEBUG1, "already merged request (%zu), giving up on merging",
				 cur - aio_ctl->in_progress_io);
			return;
		}

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
	/* Single user mode doesn't have any AIO workers. */
	if (!IsUnderPostmaster)
		return true;

	/* We don't know how to open the file. */
	return io_action_cbs[io->scb].retry == NULL;
}

static int
pgaio_synchronous_submit(void)
{
	int nsubmitted = 0;

	while (!dlist_is_empty(&my_aio->pending))
	{
		dlist_node *node;
		PgAioInProgress *io;

		node = dlist_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);

		pgaio_io_prepare_submit(io, io->ring);
		pgaio_worker_do(io);

		++nsubmitted;
	}

	return nsubmitted;
}

static int
pgaio_worker_submit(int max_submit, bool drain)
{
	int nios = 0;

	while (!dlist_is_empty(&my_aio->pending) && nios < max_submit)
	{
		dlist_node *node;
		PgAioInProgress *io;
		uint32 io_index;

		node = dlist_head_node(&my_aio->pending);
		io = dlist_container(PgAioInProgress, io_node, node);
		io_index = io - aio_ctl->in_progress_io;

		pgaio_io_prepare_submit(io, io->ring);

		if (pgaio_worker_need_synchronous(io))
		{
			/* Perform the IO synchronously in this process. */
			pgaio_worker_do(io);
		}
		else
		{
			/*
			 * Push it on the submission queue and wake a worker, but if the
			 * queue is full then handle it synchronously rather than waiting.
			 * XXX Is this fair enough?
			 */
			if (squeue32_enqueue(aio_ctl->aio_submission_queue, io_index))
				ConditionVariableSignal(&aio_ctl->submission_queue_not_empty);
			else
				pgaio_worker_do(io);
		}
		pgaio_complete_ios(false);

		++nios;
	}

	return nios;
}

static void
pgaio_submit_pending_internal(bool drain, bool call_shared, bool call_local, bool will_wait)
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
	if (my_aio->pending_count > 1 && PGAIO_MAX_COMBINE > 1)
		pgaio_combine_pending();

#if 0
	ereport(LOG, errmsg("after combine"),
			errhidestmt(true),
			errhidecontext(true));
	pgaio_print_list(&my_aio->pending, NULL, offsetof(PgAioInProgress, io_node));
#endif
#endif /* COMBINE_ENABLED */

	/*
	 * FIXME: currently the pgaio_synchronous_submit() path is only compatible
	 * with AIOTYPE_WORKER: There's e.g. a danger we'd wait for io_uring
	 * events, which could stall (or trigger asserts), as
	 * pgaio_io_wait_ref_int() right now has no way of detecting that case.
	 */
	if (aio_type != AIOTYPE_WORKER)
		will_wait = false;

	/*
	 * Loop until all pending IOs are submitted. Throttle max in-flight before
	 * calling into the IO implementation specific routine, so this code can
	 * be shared.
	 */
	while (!dlist_is_empty(&my_aio->pending))
	{
		int max_submit;
		int did_submit;
		int inflight_limit;
		int pending_count;

		Assert(my_aio->pending_count > 0);
		pgaio_apply_backend_limit();

		pending_count = my_aio->pending_count;
		Assert(pending_count > 0);
		if (pending_count == 0)
			break;

		inflight_limit = io_max_concurrency - pg_atomic_read_u32(&my_aio->inflight_count);
		max_submit = Min(pending_count, PGAIO_SUBMIT_BATCH_SIZE);
		max_submit = Min(max_submit, inflight_limit);
		Assert(max_submit > 0 && max_submit <= pending_count);

		START_CRIT_SECTION();
		if (my_aio->pending_count == 1 && will_wait)
			did_submit = pgaio_synchronous_submit();
		else if (aio_type == AIOTYPE_WORKER)
			did_submit = pgaio_worker_submit(max_submit, drain);
#ifdef USE_LIBURING
		else if (aio_type == AIOTYPE_LIBURING)
			did_submit = pgaio_uring_submit(max_submit, drain);
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

	if (call_shared)
	{
		pgaio_complete_ios(false);
		pgaio_transfer_foreign_to_local();
		pgaio_call_local_callbacks(/* in_error = */ false);
	}
}

void pg_noinline
pgaio_submit_pending(bool drain)
{
	Assert(my_aio);

	pgaio_submit_pending_internal(drain,
								  /* call_shared */ drain,
								  /* call_local */ drain,
								  /* will_wait */ false);
}

void
pgaio_closing_possibly_referenced(void)
{
	if (!my_aio)
		return;

	pgaio_submit_pending(false);
}

static void
pgaio_io_prepare_submit(PgAioInProgress *io, uint32 ring)
{
	PgAioInProgress *cur;

	cur = io;

	while (true)
	{
		Assert(cur->flags & PGAIOIP_PENDING);
		Assert(!(cur->flags & PGAIOIP_PREP));
		Assert(!(cur->flags & PGAIOIP_IDLE));

		cur->ring = ring;

		pg_write_barrier();

		WRITE_ONCE_F(cur->flags) =
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
			ereport(DEBUG4,
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

		ereport(DEBUG5,
				errmsg("readied %zu/%llu for submit",
					   cur - aio_ctl->in_progress_io,
					   (unsigned long long ) cur->generation),
				errhidecontext(1),
				errhidestmt(1));

		if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
			break;
		cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
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
					pgaio_io_wait_ref_int(&ref, /* call_shared = */ false, /* call_local = */ false);
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

			/*
			 * ->issued_abandoned_count is only maintained once shared
			 * callbacks have been invoked. So do so, as otherwise we could
			 * end up looping here endlessly, as those IOs already finished.
			 */
			pgaio_complete_ios(false);
			pgaio_transfer_foreign_to_local();

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

			pgaio_io_wait_ref_int(&ref,  /* call_shared = */ false, /* call_local = */ false);
		}

		current_inflight = pg_atomic_read_u32(&my_aio->inflight_count);
	}
}

void
pgaio_io_wait_ref(PgAioIoRef *ref, bool call_local)
{
	pgaio_io_wait_ref_int(ref, /* call_shared = */ true, call_local);
}

static void
pgaio_io_wait_ref_int(PgAioIoRef *ref, bool call_shared, bool call_local)
{
	uint64 ref_generation;
	PgAioInProgress *io;
	uint32 done_flags = PGAIOIP_DONE;
	PgAioIPFlags flags;
	bool am_owner;

	/*
	 * If we just wait for the IO to finish, we can only wait for the
	 * corresponding state.
	 */
	if (!call_shared)
	{
		Assert(!call_local);
		done_flags |= PGAIOIP_REAPED;
	}

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
	{
		Assert(call_shared);
		pgaio_submit_pending_internal(true, call_shared, call_local,
									  /* will_wait = */ true);
	}

	Assert(!am_owner || !(flags & PGAIOIP_IDLE));

	Assert(!(flags & (PGAIOIP_UNUSED)));

wait_ref_again:

	while (true)
	{
		PgAioContext *context;

		flags = io->flags;
		context = &aio_ctl->contexts[io->ring];
		pg_read_barrier();

		if (io->generation != ref_generation)
			return;

		if (flags & done_flags)
			goto wait_ref_out;

		Assert(!(flags & (PGAIOIP_UNUSED)));

		if (flags & PGAIOIP_INFLIGHT)
		{
			pgaio_drain(context, call_shared, call_local);

			flags = io->flags;
			context = &aio_ctl->contexts[io->ring];
			pg_read_barrier();

			if (io->generation != ref_generation)
				return;

			if (flags & done_flags)
				goto wait_ref_out;
		}

		if (my_aio->pending_count > 0 && call_shared)
		{
			/*
			 * If we otherwise would have to sleep submit all pending
			 * requests, to avoid others having to wait for us to submit
			 * them. Don't want to do so when not needing to sleep, as
			 * submitting IOs in smaller increments can be less efficient.
			 */
			pgaio_submit_pending_internal(call_shared, call_shared, call_local,
										  /* will_wait = */ false);
		}
		else if (aio_type != AIOTYPE_WORKER && (flags & PGAIOIP_INFLIGHT))
		{
			/* note that this is allowed to spuriously return */
			if (aio_type == AIOTYPE_WORKER)
				ConditionVariableSleep(&io->cv, WAIT_EVENT_AIO_IO_COMPLETE_ONE);
#ifdef USE_LIBURING
			else if (aio_type == AIOTYPE_LIBURING)
				pgaio_uring_wait_one(context, io, ref_generation,
									 WAIT_EVENT_AIO_IO_COMPLETE_ANY);
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
				ConditionVariableCancelSleepEx(true);
		}
	}

wait_ref_out:

	flags = io->flags;
	pg_read_barrier();
	if (io->generation != ref_generation)
		return;

	/*
	 * If somebody else is retrying the IO, just wait from scratch.
	 */
	if (!(flags & done_flags))
	{
		Assert(flags & PGAIOIP_RETRY);
		goto wait_ref_again;
	}

	if (!call_shared)
	{
		Assert(flags & (PGAIOIP_REAPED | PGAIOIP_SHARED_CALLBACK_CALLED | PGAIOIP_DONE));
		return;
	}

	Assert(flags & PGAIOIP_DONE);

	if (unlikely(flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_HARD_FAILURE)))
	{
		/* can retry soft failures, but not hard ones */
		/* FIXME: limit number of soft retries */
		if (flags & PGAIOIP_SOFT_FAILURE)
		{
			pgaio_io_retry(io);
			pgaio_io_wait_ref_int(ref, call_shared, call_local);
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
			pgaio_transfer_foreign_to_local();

		Assert(!(io->flags & PGAIOIP_FOREIGN_DONE));

		pgaio_io_call_local_callback(io, false);
	}
}

bool
pgaio_io_check_ref(PgAioIoRef *ref)
{
	uint64 ref_generation;
	PgAioInProgress *io;
	PgAioIPFlags flags;
	PgAioContext *context;

	Assert(ref->aio_index < max_aio_in_progress);

	io = &aio_ctl->in_progress_io[ref->aio_index];
	ref_generation = ((uint64) ref->generation_upper) << 32 |
		ref->generation_lower;

	Assert(ref_generation != 0);

	flags = io->flags;
	pg_read_barrier();

	if (io->generation != ref_generation)
		return true;

	if (flags & PGAIOIP_PENDING)
		return false;

	if (flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (flags & PGAIOIP_DONE)
		return true;

	context = &aio_ctl->contexts[io->ring];
	Assert(!(flags & (PGAIOIP_UNUSED)));

	if (io->generation != ref_generation)
		return true;

	if (flags & PGAIOIP_INFLIGHT)
		pgaio_drain(context, /* call_shared = */ true, /* call_local = */ false);

	flags = io->flags;
	pg_read_barrier();

	if (io->generation != ref_generation)
		return true;

	if (flags & (PGAIOIP_SOFT_FAILURE | PGAIOIP_RETRY))
		return false;

	if (flags & PGAIOIP_DONE)
		return true;

	return false;
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
		elog(WARNING, "needed to drain while getting IO (used %d inflight %d)",
			 aio_ctl->used_count, pg_atomic_read_u32(&my_aio->inflight_count));

		/*
		 * FIXME: should we wait for IO instead?
		 *
		 * Also, need to protect against too many ios handed out but not used.
		 */
		for (int i = 0; i < aio_ctl->num_contexts; i++)
			pgaio_drain(&aio_ctl->contexts[i],
						/* call_shared = */ true, /* call_local = */ true);

		LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);
	}

	elem = dlist_pop_head_node(&aio_ctl->unused_ios);
	aio_ctl->used_count++;

	LWLockRelease(SharedAIOCtlLock);

	io = dlist_container(PgAioInProgress, owner_node, elem);

#ifdef PGAIO_VERBOSE
	ereport(DEBUG3, errmsg("acquired io %zu/%llu",
						   io - aio_ctl->in_progress_io,
						   (long long unsigned) io->generation),
			errhidecontext(1),
			errhidestmt(1));
#endif

	Assert(io->op == PGAIO_OP_INVALID);
	Assert(io->scb == PGAIO_SCB_INVALID);
	Assert(io->flags == PGAIOIP_UNUSED);
	Assert(io->system_referenced);
	Assert(io->on_completion_local == NULL);
	Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);

	io->user_referenced = true;
	io->system_referenced = false;

	WRITE_ONCE_F(io->flags) = PGAIOIP_IDLE;

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

int
pgaio_io_result(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(io->flags & PGAIOIP_DONE);

	/*
	 * FIXME: This will currently not return correct information for partial
	 * writes that were retried.
	 */
	return io->result;
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

bool
pgaio_io_pending(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(!(io->flags & PGAIOIP_UNUSED));

	return io->flags & PGAIOIP_PENDING;
}

uint32
pgaio_io_id(PgAioInProgress *io)
{
	return io - aio_ctl->in_progress_io;
}

uint64
pgaio_io_generation(PgAioInProgress *io)
{
	return io->generation;
}

static void
pgaio_io_ref_internal(PgAioInProgress *io, PgAioIoRef *ref)
{
	Assert(io->flags & (PGAIOIP_IDLE | PGAIOIP_PREP | PGAIOIP_IN_PROGRESS | PGAIOIP_DONE));

	ref->aio_index = io - aio_ctl->in_progress_io;
	ref->generation_upper = (uint32) (io->generation >> 32);
	ref->generation_lower = (uint32) io->generation;
	Assert(io->generation != 0);
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
	Assert(io->flags & (PGAIOIP_IDLE | PGAIOIP_PENDING));
	Assert(io->on_completion_local == NULL);

	io->on_completion_local = ocb;
}

void
pgaio_io_retry(PgAioInProgress *io)
{
	bool need_retry;
	PgAioRetryCB retry_cb = NULL;

	retry_cb = io_action_cbs[io->scb].retry;

	if (!retry_cb)
		elog(PANIC, "non-retryable aio being retried");

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	/* could concurrently have been unset / retried */
	if (io->flags & PGAIOIP_SHARED_FAILED)
	{
		Assert(!(io->flags & PGAIOIP_FOREIGN_DONE));

		dlist_delete(&io->io_node);

		WRITE_ONCE_F(io->flags) =
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
		return;
	}

	retry_cb(io);

	dlist_push_tail(&my_aio->pending, &io->io_node);
	my_aio->pending_count++;
	my_aio->retry_total_count++;

	pgaio_submit_pending(false);
}

void
pgaio_io_recycle(PgAioInProgress *io)
{
	Assert(io->flags & (PGAIOIP_IDLE | PGAIOIP_DONE));
	Assert(io->user_referenced);
	Assert(io->owner_id == my_aio_id);
	Assert(!io->system_referenced);
	Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);

	if (io->bb_idx != PGAIO_BB_INVALID)
	{
		pgaio_bounce_buffer_release_internal(&aio_ctl->bounce_buffers[io->bb_idx],
											 /* holding_lock = */ false,
											 /* release_resowner = */ false);
		io->bb_idx = PGAIO_BB_INVALID;
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

		io->op = PGAIO_OP_INVALID;
		io->scb = PGAIO_SCB_INVALID;
	}

	io->flags &= ~(PGAIOIP_SHARED_CALLBACK_CALLED |
				   PGAIOIP_LOCAL_CALLBACK_CALLED |
				   PGAIOIP_RETRY |
				   PGAIOIP_HARD_FAILURE |
				   PGAIOIP_SOFT_FAILURE);
	Assert(io->flags == PGAIOIP_IDLE);
	io->result = 0;
	io->on_completion_local = NULL;
}

static void pg_noinline
pgaio_io_prepare(PgAioInProgress *io, PgAioOp op)
{
	/* true for now, but not necessarily in the future */
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->user_referenced);
	Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);
	Assert(io->op == PGAIO_OP_INVALID);
	Assert(io->scb == PGAIO_SCB_INVALID);

	if (my_aio->pending_count + 1 >= PGAIO_SUBMIT_BATCH_SIZE)
	{
#ifdef PGAIO_VERBOSE
		ereport(DEBUG3,
				errmsg("submitting during prep for %zu due to %u inflight",
					   io - aio_ctl->in_progress_io,
					   my_aio->pending_count),
				errhidecontext(1),
				errhidestmt(1));
#endif
		pgaio_submit_pending(true);
	}

	Assert(my_aio->pending_count < PGAIO_SUBMIT_BATCH_SIZE);

	/* for this module */
	io->system_referenced = true;
	io->op = op;
	Assert(io->owner_id == my_aio_id);

	WRITE_ONCE_F(io->flags) = PGAIOIP_PREP;
}


static void pg_noinline
pgaio_io_stage(PgAioInProgress *io, PgAioSharedCallback scb)
{
	Assert(my_aio->pending_count < PGAIO_SUBMIT_BATCH_SIZE);
	Assert(io->flags == PGAIOIP_PREP);
	Assert(io->user_referenced);
	Assert(io->op != PGAIO_OP_INVALID);
	Assert(io->scb == PGAIO_SCB_INVALID);
	Assert(io_action_cbs[scb].op == io->op);

	io->scb = scb;

	WRITE_ONCE_F(io->flags) = (io->flags & ~PGAIOIP_PREP) | PGAIOIP_IN_PROGRESS | PGAIOIP_PENDING;
	dlist_push_tail(&my_aio->pending, &io->io_node);
	my_aio->pending_count++;

#ifdef PGAIO_VERBOSE
	if (message_level_is_interesting(DEBUG3))
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
		StringInfoData s;

		/*
		 * This code isn't ever allowed to error out, but the debugging
		 * output is super useful...
		 */
		START_CRIT_SECTION();

		initStringInfo(&s);

		pgaio_io_print(io, &s);

		ereport(DEBUG3,
				errmsg("staged %s",
					   s.data),
				errhidestmt(true),
				errhidecontext(true));
		pfree(s.data);
		MemoryContextSwitchTo(oldcontext);

		END_CRIT_SECTION();
	}
#endif
}

void
pgaio_io_release(PgAioInProgress *io)
{
	Assert(io->user_referenced);
	Assert(io->owner_id == my_aio_id);

	LWLockAcquire(SharedAIOCtlLock, LW_EXCLUSIVE);

	io->user_referenced = false;

	if (io->flags & (PGAIOIP_IDLE |
					 PGAIOIP_PREP |
					 PGAIOIP_PENDING |
					 PGAIOIP_LOCAL_CALLBACK_CALLED))
	{
		Assert(!(io->flags & PGAIOIP_INFLIGHT));

		Assert(my_aio->outstanding_count > 0);
		dlist_delete_from(&my_aio->outstanding, &io->owner_node);
		my_aio->outstanding_count--;

#ifdef PGAIO_VERBOSE
		ereport(DEBUG4, errmsg("releasing plain user reference to %zu",
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
			ereport(DEBUG4, errmsg("putting aio %zu onto issued_abandoned during release",
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
			ereport(DEBUG4, errmsg("not putting aio %zu onto issued_abandoned during release",
								   io - aio_ctl->in_progress_io),
					errhidecontext(1),
					errhidestmt(1));
#endif
		}
	}

	if (!io->system_referenced)
	{
		Assert(!(io->flags & PGAIOIP_INFLIGHT));
		Assert(io->flags & (PGAIOIP_IDLE |
							PGAIOIP_PREP |
							PGAIOIP_DONE));

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
		io->op = PGAIO_OP_INVALID;
		io->scb = PGAIO_SCB_INVALID;
		io->owner_id = INVALID_PGPROCNO;
		io->result = 0;
		io->system_referenced = true;
		io->on_completion_local = NULL;

		Assert(io->merge_with_idx == PGAIO_MERGE_INVALID);

		if (io->bb_idx != PGAIO_BB_INVALID)
		{
			pgaio_bounce_buffer_release_internal(&aio_ctl->bounce_buffers[io->bb_idx],
												 /* holding_lock = */ true,
												 /* release_resowner = */ false);
			io->bb_idx = PGAIO_BB_INVALID;
		}

		dlist_push_head(&aio_ctl->unused_ios, &io->owner_node);
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
pgaio_io_operation_string(PgAioOp op)
{
	switch (op)
	{
		case PGAIO_OP_INVALID:
			return "invalid";
		case PGAIO_OP_READ:
			return "read";
		case PGAIO_OP_WRITE:
			return "write";
		case PGAIO_OP_FSYNC:
			return "fsync";
		case PGAIO_OP_FLUSH_RANGE:
			return "flush_range";
		case PGAIO_OP_NOP:
			return "nop";
	}
	return "";
}

static const char *
pgaio_io_shared_callback_string(PgAioSharedCallback a)
{
	return io_action_cbs[a].name;
}

static void
pgaio_io_flag_string(PgAioIPFlags flags, StringInfo s)
{
	bool first = true;

#define STRINGIFY_FLAG(f) if (flags & f) {  appendStringInfoString(s, first ? CppAsString(f) : " | " CppAsString(f)); first = false;}

	STRINGIFY_FLAG(PGAIOIP_UNUSED);
	STRINGIFY_FLAG(PGAIOIP_IDLE);
	STRINGIFY_FLAG(PGAIOIP_PREP);
	STRINGIFY_FLAG(PGAIOIP_IN_PROGRESS);
	STRINGIFY_FLAG(PGAIOIP_PENDING);
	STRINGIFY_FLAG(PGAIOIP_INFLIGHT);
	STRINGIFY_FLAG(PGAIOIP_REAPED);
	STRINGIFY_FLAG(PGAIOIP_SHARED_CALLBACK_CALLED);
	STRINGIFY_FLAG(PGAIOIP_LOCAL_CALLBACK_CALLED);

	STRINGIFY_FLAG(PGAIOIP_DONE);
	STRINGIFY_FLAG(PGAIOIP_FOREIGN_DONE);

	STRINGIFY_FLAG(PGAIOIP_RETRY);
	STRINGIFY_FLAG(PGAIOIP_HARD_FAILURE);
	STRINGIFY_FLAG(PGAIOIP_SOFT_FAILURE);
	STRINGIFY_FLAG(PGAIOIP_SHARED_FAILED);

#undef STRINGIFY_FLAG
}

static void
pgaio_io_shared_desc(PgAioInProgress *io, StringInfo s)
{
	io_action_cbs[io->scb].desc(io, s);
}

static void
pgaio_io_print_one(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "aio %zu/%llu: op: %s, scb: %s, result: %d, ring: %d, owner: %d, flags: ",
					 io - aio_ctl->in_progress_io,
					 (long long unsigned) io->generation,
					 pgaio_io_operation_string(io->op),
					 pgaio_io_shared_callback_string(io->scb),
					 io->result,
					 io->ring,
					 io->owner_id);
	pgaio_io_flag_string(io->flags, s);
	appendStringInfo(s, ", user/system_referenced: %d/%d (",
					 io->user_referenced,
					 io->system_referenced);
	pgaio_io_shared_desc(io, s);
	appendStringInfoString(s, ")");
}

void
pgaio_io_ref_print(PgAioIoRef *ref, StringInfo s)
{
	PgAioInProgress *io;

	io = &aio_ctl->in_progress_io[ref->aio_index];

	pgaio_io_print(io, s);
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

	if (io->merge_with_idx != PGAIO_MERGE_INVALID)
	{
		PgAioInProgress *cur = io;
		int nummerge = 0;

		appendStringInfoString(s, "\n  merge with:");

		while (cur->merge_with_idx != PGAIO_MERGE_INVALID)
		{
			PgAioInProgress *next;

			nummerge++;
			appendStringInfo(s, "\n    %d: ", nummerge);

			next = &aio_ctl->in_progress_io[cur->merge_with_idx];
			pgaio_io_print_one(next, s);

			cur = next;
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
			elog(WARNING, "needed to drain while getting BB (inflight %d)",
				 pg_atomic_read_u32(&my_aio->inflight_count));

			for (int i = 0; i < aio_ctl->num_contexts; i++)
				pgaio_drain(&aio_ctl->contexts[i],
							/* call_shared = */ true,
							/* call_local = */ true);
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
	Assert(io->bb_idx == PGAIO_BB_INVALID);
	Assert(io->flags == PGAIOIP_IDLE);
	Assert(io->user_referenced);
	Assert(pg_atomic_read_u32(&bb->refcount) > 0);

	io->bb_idx = bb - aio_ctl->bounce_buffers;
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
	int our_nios = 0;

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
		/*
		 * Don't increase our inflight count for requests initiated by another
		 * backend.
		 */
		if (io->owner_id == my_aio_id)
			our_nios++;
		else
			pg_atomic_fetch_add_u32(&aio_ctl->backend_state[io->owner_id].inflight_count, 1);
	}

	Assert(nios > 0);

	{
		int ret;

		if (our_nios)
			pg_atomic_add_fetch_u32(&my_aio->inflight_count, our_nios);
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

		while (true)
		{
			ConditionVariableBroadcast(&cur->cv);
			if (cur->merge_with_idx == PGAIO_MERGE_INVALID)
				break;
			cur = &aio_ctl->in_progress_io[cur->merge_with_idx];
		}
	}

	/* callbacks will be called later, by pgaio_submit_pending_internal() */
	if (drain)
		pgaio_drain(context,
					/* call_shared = */ false,
					/* call_local = */ false);

	return nios;
}

static int
pgaio_uring_drain_locked(PgAioContext *context)
{
	uint32 processed = 0;
	int ready;

	Assert(LWLockHeldByMe(&context->completion_lock));

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

		START_CRIT_SECTION();

		ereport(DEBUG3,
				errmsg("context [%zu]: drain %d ready",
					   context - aio_ctl->contexts,
					   ready),
				errhidestmt(true),
				errhidecontext(true));

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

		END_CRIT_SECTION();
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

	return processed;
}

static int
pgaio_uring_drain(PgAioContext *context, bool call_shared)
{
	uint32 processed = 0;

	Assert(!LWLockHeldByMe(&context->completion_lock));

	while (io_uring_cq_ready(&context->io_uring_ring))
	{
		if (LWLockAcquireOrWait(&context->completion_lock, LW_EXCLUSIVE))
		{
			processed = pgaio_uring_drain_locked(context);

			/*
			 * If allowed, call shared callbacks under lock - that prevent
			 * other backends to first have to wait on the completion_lock for
			 * pgaio_uring_wait_one, then again below pgaio_drain(), and then
			 * again on the condition variable for the AIO.
			 */
			if (call_shared)
				pgaio_complete_ios(false);

			LWLockRelease(&context->completion_lock);
			break;
		}
	}

	return processed;
}

static void
pgaio_uring_wait_one(PgAioContext *context, PgAioInProgress *io, uint64 ref_generation, uint32 wait_event_info)
{
	PgAioIPFlags flags;

	if (LWLockAcquireOrWait(&context->completion_lock, LW_EXCLUSIVE))
	{
		ereport(DEBUG3,
				errmsg("[%d] got the lock, before waiting for %zu/%llu, %d ready",
					   (int)(context - aio_ctl->contexts),
					   io - aio_ctl->in_progress_io,
					   (long long unsigned) io->generation,
					   io_uring_cq_ready(&context->io_uring_ring)),
				errhidestmt(true),
				errhidecontext(true));

		flags = io->flags;

		pg_read_barrier();

		if (!io_uring_cq_ready(&context->io_uring_ring) &&
			io->generation == ref_generation &&
			(flags & PGAIOIP_INFLIGHT))
		{
			int ret;
			struct io_uring_cqe *cqes;

			/*
			 * XXX: Temporary, non-assert, sanity checks, some of these are
			 * hard to hit in assert builds and lead to rare and hard to debug
			 * hangs.
			 */
			if (io->generation != ref_generation ||
				&aio_ctl->contexts[io->ring] != context)
			{
				ereport(PANIC,
						errmsg("generation/context changed while locked: ref_gen: %llu, real_gen: %llu, orig ring: %d, cur ring: %d",
							   (long long unsigned) ref_generation,
							   (long long unsigned) io->generation,
							   (int)(context - aio_ctl->contexts),
							   io->ring),
						errhidestmt(true),
						errhidecontext(true));
			}

			pgstat_report_wait_start(wait_event_info);
			ret = io_uring_wait_cqes(&context->io_uring_ring, &cqes, 1, NULL, NULL);
			pgstat_report_wait_end();

			if (ret < 0 && errno == EINTR)
			{
				elog(DEBUG3, "got interrupted");
			}
			else if (ret != 0)
				elog(WARNING, "unexpected: %d/%s: %m", ret, strerror(-ret));

			/* see XXX above */
			if (io->generation != ref_generation ||
				&aio_ctl->contexts[io->ring] != context)
			{
				ereport(PANIC,
						errmsg("generation/context changed while locked, after wait: ref_gen: %llu, real_gen: %llu, orig ring: %d, cur ring: %d",
							   (long long unsigned) ref_generation,
							   (long long unsigned) io->generation,
							   (int)(context - aio_ctl->contexts),
							   io->ring),
						errhidestmt(true),
						errhidecontext(true));
			}

			ereport(DEBUG3,
					errmsg("[%d] after waiting for %zu, %d ready",
						   (int)(context - aio_ctl->contexts),
						   io - aio_ctl->in_progress_io,
						   io_uring_cq_ready(&context->io_uring_ring)),
					errhidestmt(true),
					errhidecontext(true));
		}
		else
		{
			ereport(DEBUG3,
					errmsg("[%d] got ready under lock, without wait %zu, %d ready",
						   (int)(context - aio_ctl->contexts),
						   io - aio_ctl->in_progress_io,
						   io_uring_cq_ready(&context->io_uring_ring)),
					errhidestmt(true),
					errhidecontext(true));
		}

		/*
		 * Drain and call shared callbacks under lock while we already have it
		 * - that avoids others to first have to wait on the completion_lock
		 * for pgaio_uring_wait_one, then again in pgaio_drain(), and then
		 * again on the condition variable for the AIO.
		 */
		if (io_uring_cq_ready(&context->io_uring_ring))
		{
			pgaio_uring_drain_locked(context);
			pgaio_complete_ios(false);
		}

		LWLockRelease(&context->completion_lock);
	}
	else
	{
		ereport(DEBUG3,
				errmsg("[%d] somebody else got the lock while waiting for %zu/%lu, %d ready",
					   (int)(context - aio_ctl->contexts),
					   io - aio_ctl->in_progress_io, io->generation,
					   io_uring_cq_ready(&context->io_uring_ring)),
				errhidestmt(true),
				errhidecontext(true));
	}
}

static void
pgaio_uring_io_from_cqe(PgAioContext *context, struct io_uring_cqe *cqe)
{
	uint32 prev_inflight_count PG_USED_FOR_ASSERTS_ONLY;
	PgAioInProgress *io;

	io = io_uring_cqe_get_data(cqe);
	Assert(io != NULL);

	if (io->used_iovec != -1)
	{
		PgAioIovec *iovec = &context->iovecs[io->used_iovec];

		io->used_iovec = -1;
		slist_push_head(&context->reaped_iovecs, &iovec->node);
		context->reaped_iovecs_count++;
	}

	/* XXX: this doesn't need to be under the lock */
	prev_inflight_count = pg_atomic_fetch_sub_u32(&aio_ctl->backend_state[io->owner_id].inflight_count, 1);
	Assert(prev_inflight_count > 0);

	pgaio_process_io_completion(io, cqe->res);
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
	int iovcnt = 0;

	io->used_iovec = -1;

	switch (io->op)
	{
		case PGAIO_OP_READ:
			iovec = pgaio_uring_iovec_get(context, io);
			iovcnt = pgaio_fill_iov(iovec->iovec, io);

			io_uring_prep_readv(sqe,
								io->op_data.read.fd,
								iovec->iovec,
								iovcnt,
								io->op_data.read.offset
								+ io->op_data.read.already_done);
			break;

		case PGAIO_OP_WRITE:
			iovec = pgaio_uring_iovec_get(context, io);
			iovcnt = pgaio_fill_iov(iovec->iovec, io);

			io_uring_prep_writev(sqe,
								 io->op_data.write.fd,
								 iovec->iovec,
								 iovcnt,
								 io->op_data.write.offset
								 + io->op_data.write.already_done);
			break;

		case PGAIO_OP_FSYNC:
			io_uring_prep_fsync(sqe,
								io->op_data.fsync.fd,
								io->op_data.fsync.datasync ? IORING_FSYNC_DATASYNC : 0);
			break;

		case PGAIO_OP_FLUSH_RANGE:
			io_uring_prep_rw(IORING_OP_SYNC_FILE_RANGE,
							 sqe,
							 io->op_data.flush_range.fd,
							 NULL,
							 io->op_data.flush_range.nbytes,
							 io->op_data.flush_range.offset);
			sqe->sync_range_flags = SYNC_FILE_RANGE_WRITE;
			break;

		case PGAIO_OP_NOP:
			elog(ERROR, "not yet");
			break;

		case PGAIO_OP_INVALID:
			elog(ERROR, "invalid");
	}

	io_uring_sqe_set_data(sqe, io);
}

#endif


/* --------------------------------------------------------------------------------
 * IO initialization routines for the basic IO types (see PgAioOp).
 * --------------------------------------------------------------------------------
 */


void
pgaio_io_prep_read(PgAioInProgress *io, int fd, char *bufdata, uint64 offset, uint32 nbytes)
{
	Assert(io->op == PGAIO_OP_READ);
	Assert(io->flags == PGAIOIP_PREP);
	Assert(ShmemAddrIsValid(bufdata));

	io->op_data.read.fd = fd;
	io->op_data.read.offset = offset;
	io->op_data.read.nbytes = nbytes;
	io->op_data.read.bufdata = bufdata;
	io->op_data.read.already_done = 0;
}

void
pgaio_io_prep_write(PgAioInProgress *io, int fd, char *bufdata, uint64 offset, uint32 nbytes)
{
	Assert(io->op == PGAIO_OP_WRITE);
	Assert(io->flags == PGAIOIP_PREP);
	Assert(ShmemAddrIsValid(bufdata));

	io->op_data.write.fd = fd;
	io->op_data.write.offset = offset;
	io->op_data.write.nbytes = nbytes;
	io->op_data.write.bufdata = bufdata;
	io->op_data.write.already_done = 0;
}

void
pgaio_io_prep_fsync(PgAioInProgress *io, int fd, bool datasync)
{
	Assert(io->op == PGAIO_OP_FSYNC);
	Assert(io->flags == PGAIOIP_PREP);

	io->op_data.fsync.fd = fd;
	io->op_data.fsync.datasync = datasync;
}

void
pgaio_io_prep_flush_range(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes)
{
	Assert(io->op == PGAIO_OP_FLUSH_RANGE);
	Assert(io->flags == PGAIOIP_PREP);

	io->op_data.flush_range.fd = fd;
	io->op_data.flush_range.offset = offset;
	io->op_data.flush_range.nbytes = nbytes;
}

void
pgaio_io_prep_nop(PgAioInProgress *io)
{
	Assert(io->op == PGAIO_OP_WRITE);
	Assert(io->flags == PGAIOIP_PREP);
}


/* --------------------------------------------------------------------------------
 * IO start routines (see PgAioSharedCallback for a list)
 * --------------------------------------------------------------------------------
 */

void
pgaio_io_start_read_sb(PgAioInProgress *io, struct SMgrRelationData* smgr, ForkNumber forknum,
					   BlockNumber blocknum, char *bufdata, int buffid, int mode)
{
	Assert(BufferIsValid(buffid));

	pgaio_io_prepare(io, PGAIO_OP_READ);

	smgrstartread(io, smgr, forknum, blocknum, bufdata);

	io->scb_data.read_sb.buffid = buffid;
	io->scb_data.read_sb.backend = smgr->smgr_rnode.backend;
	io->scb_data.read_sb.mode = mode;

	ReadBufferPrepRead(io, buffid);

	pgaio_io_stage(io, PGAIO_SCB_READ_SB);
}

void
pgaio_io_start_read_smgr(PgAioInProgress *io, struct SMgrRelationData* smgr, ForkNumber forknum,
						 BlockNumber blocknum, char *bufdata)
{
	pgaio_io_prepare(io, PGAIO_OP_READ);

	smgrstartread(io, smgr, forknum, blocknum, bufdata);

	io->scb_data.read_smgr.tag = (AioBufferTag){
		.rnode = smgr->smgr_rnode,
		.forkNum = forknum,
		.blockNum = blocknum
	};

	pgaio_io_stage(io, PGAIO_SCB_READ_SMGR);
}

void
pgaio_io_start_write_sb(PgAioInProgress *io,
						struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
						char *bufdata, int buffid, bool skipFsync, bool release_lock)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	smgrstartwrite(io, smgr, forknum, blocknum, bufdata, skipFsync);

	io->scb_data.write_sb.buffid = buffid;
	io->scb_data.write_sb.backend = smgr->smgr_rnode.backend;
	io->scb_data.write_sb.release_lock = release_lock;

	ReadBufferPrepWrite(io, buffid, release_lock);

	pgaio_io_stage(io, PGAIO_SCB_WRITE_SB);
}

void
pgaio_io_start_write_smgr(PgAioInProgress *io,
						  struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
						  char *bufdata, bool skipFsync)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	smgrstartwrite(io, smgr, forknum, blocknum, bufdata, skipFsync);

	io->scb_data.write_smgr.tag = (AioBufferTag){
		.rnode = smgr->smgr_rnode,
		.forkNum = forknum,
		.blockNum = blocknum
	};

	pgaio_io_stage(io, PGAIO_SCB_WRITE_SMGR);
}

void
pgaio_io_start_write_wal(PgAioInProgress *io, int fd, uint32 offset, uint32 nbytes, char *bufdata, uint32 write_no)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	pgaio_io_prep_write(io, fd, bufdata, offset, nbytes);

	io->scb_data.write_wal.write_no = write_no;

	pgaio_io_stage(io, PGAIO_SCB_WRITE_WAL);
}

void
pgaio_io_start_write_generic(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes, char *bufdata)
{
	pgaio_io_prepare(io, PGAIO_OP_WRITE);

	pgaio_io_prep_write(io, fd, bufdata, offset, nbytes);

	pgaio_io_stage(io, PGAIO_SCB_WRITE_GENERIC);
}

void
pgaio_io_start_fsync_raw(PgAioInProgress *io, int fd, bool datasync)
{
	pgaio_io_prepare(io, PGAIO_OP_FSYNC);

	pgaio_io_prep_fsync(io, fd, datasync);

	pgaio_io_stage(io, PGAIO_SCB_FSYNC_RAW);
}

void
pgaio_io_start_fsync_wal(PgAioInProgress *io, int fd, bool datasync_only, uint32 flush_no)
{
	pgaio_io_prepare(io, PGAIO_OP_FSYNC);

	pgaio_io_prep_fsync(io, fd, datasync_only);

	io->scb_data.fsync_wal.flush_no = flush_no;

	pgaio_io_stage(io, PGAIO_SCB_FSYNC_WAL);
}

void
pgaio_io_start_flush_range_raw(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes)
{
	pgaio_io_prepare(io, PGAIO_OP_FLUSH_RANGE);

	pgaio_io_prep_flush_range(io, fd, offset, nbytes);

	pgaio_io_stage(io, PGAIO_SCB_FLUSH_RANGE_RAW);
}

BlockNumber
pgaio_io_start_flush_range_smgr(PgAioInProgress *io, struct SMgrRelationData* smgr, ForkNumber forknum,
								BlockNumber blocknum, BlockNumber nblocks)
{
	BlockNumber ret;

	pgaio_io_prepare(io, PGAIO_OP_FLUSH_RANGE);

	ret = smgrstartwriteback(io, smgr, forknum, blocknum, nblocks);

	if (ret != InvalidBlockNumber)
	{
		io->scb_data.flush_range_smgr.tag = (AioBufferTag){
			.rnode = smgr->smgr_rnode,
			.forkNum = forknum,
			.blockNum = blocknum
		};
		pgaio_io_stage(io, PGAIO_SCB_FLUSH_RANGE_SMGR);
	}
	else
	{
		Assert(io->op == PGAIO_OP_FLUSH_RANGE);
		Assert(io->owner_id == my_aio_id);
		Assert(io->flags == PGAIOIP_PREP);
		Assert(io->system_referenced);

		io->op = PGAIO_OP_INVALID;
		io->system_referenced = false;

		WRITE_ONCE_F(io->flags) = PGAIOIP_IDLE;
	}

	return ret;
}

void
pgaio_io_start_nop(PgAioInProgress *io)
{
	pgaio_io_prepare(io, PGAIO_OP_NOP);

	pgaio_io_prep_nop(io);

	pgaio_io_stage(io, PGAIO_SCB_NOP);
}


/* --------------------------------------------------------------------------------
 * shared IO implementation (see PgAioSharedCallback for a list)
 * --------------------------------------------------------------------------------
 */

static void
pgaio_invalid_desc(PgAioInProgress *io, StringInfo s)
{
}

static bool
pgaio_nop_complete(PgAioInProgress *io)
{
#ifdef PGAIO_VERBOSE
	elog(DEBUG3, "completed nop");
#endif

	return true;
}

static void
pgaio_nop_desc(PgAioInProgress *io, StringInfo s)
{
}

static bool
pgaio_fsync_raw_complete(PgAioInProgress *io)
{
	if (io->result != 0)
		elog(PANIC, "fsync needs better error handling");

	return true;
}

static void
pgaio_fsync_raw_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, datasync: %d",
					 io->op_data.fsync.fd,
					 io->op_data.fsync.datasync);
}

static void
pgaio_fsync_wal_retry(PgAioInProgress *io)
{
	io->op_data.fsync.fd = XLogFileForFlushNo(io->scb_data.fsync_wal.flush_no);
}

static bool
pgaio_fsync_wal_complete(PgAioInProgress *io)
{
	if (io->result != 0)
		elog(PANIC, "fsync_wal needs better error handling");

	XLogFlushComplete(io, io->scb_data.fsync_wal.flush_no);
	return true;
}

static void
pgaio_fsync_wal_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "flush_no: %d, fd: %d, datasync: %d",
					 io->scb_data.fsync_wal.flush_no,
					 io->op_data.fsync.fd,
					 io->op_data.fsync.datasync);
}

static bool
pgaio_flush_range_complete(PgAioInProgress *io)
{
	return true;
}

static void
pgaio_flush_range_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %llu",
					 io->op_data.flush_range.fd,
					 (unsigned long long) io->op_data.flush_range.offset,
					 (unsigned long long) io->op_data.flush_range.nbytes);
}

static void
pgaio_flush_range_smgr_retry(PgAioInProgress *io)
{
	uint32 off;
	AioBufferTag *tag = &io->scb_data.flush_range_smgr.tag;
	SMgrRelation reln = smgropen(tag->rnode.node,
								 tag->rnode.backend);

	io->op_data.flush_range.fd = smgrfd(reln, tag->forkNum, tag->blockNum, &off);
	Assert(off == io->op_data.flush_range.offset);
}

static void
pgaio_read_sb_retry(PgAioInProgress *io)
{
	BufferDesc *bufHdr = NULL;
	bool		islocal;
	BufferTag	tag;
	Buffer		buffid = io->scb_data.read_sb.buffid;
	SMgrRelation reln;
	uint32		off;

	islocal = BufferIsLocal(buffid);

	if (islocal)
		bufHdr = GetLocalBufferDescriptor(-buffid - 1);
	else
		bufHdr = GetBufferDescriptor(buffid - 1);

	tag = bufHdr->tag;

	reln = smgropen(tag.rnode, io->scb_data.read_sb.backend);
	io->op_data.read.fd = smgrfd(reln, tag.forkNum, tag.blockNum, &off);

	Assert(off == io->op_data.read.offset);
}

static bool
pgaio_read_sb_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	ReadBufferCompleteRead(io->scb_data.read_sb.buffid,
						   io->op_data.read.bufdata,
						   io->scb_data.read_sb.mode,
						   io->flags & PGAIOIP_HARD_FAILURE);
	return true;
}

static void
pgaio_read_sb_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, mode: %d, offset: %llu, nbytes: %u, already_done: %u, buffid: %u",
					 io->op_data.read.fd,
					 io->scb_data.read_sb.mode,
					 (long long unsigned) io->op_data.read.offset,
					 io->op_data.read.nbytes,
					 io->op_data.read.already_done,
					 io->scb_data.read_sb.buffid);
}

static void
pgaio_read_smgr_retry(PgAioInProgress *io)
{
	uint32 off;
	AioBufferTag *tag = &io->scb_data.read_smgr.tag;
	SMgrRelation reln = smgropen(tag->rnode.node,
								 tag->rnode.backend);

	io->op_data.read.fd = smgrfd(reln, tag->forkNum, tag->blockNum, &off);
	Assert(off == io->op_data.read.offset);
}

static bool
pgaio_read_smgr_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	ReadBufferCompleteRawRead(&io->scb_data.read_smgr.tag,
							  io->op_data.read.bufdata,
							  io->flags & PGAIOIP_HARD_FAILURE);

	return true;
}

static void
pgaio_read_smgr_desc(PgAioInProgress *io, StringInfo s)
{
	/* FIXME: proper path, critical section safe */
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, rel: %u, buf: %p",
					 io->op_data.read.fd,
					 (long long unsigned) io->op_data.read.offset,
					 io->op_data.read.nbytes,
					 io->op_data.read.already_done,
					 io->scb_data.read_smgr.tag.rnode.node.relNode,
					 io->op_data.read.bufdata);
}

static void
pgaio_write_sb_retry(PgAioInProgress *io)
{
	BufferDesc *bufHdr = NULL;
	bool		islocal;
	BufferTag	tag;
	Buffer		buffid = io->scb_data.read_sb.buffid;
	SMgrRelation reln;
	uint32		off;

	islocal = BufferIsLocal(buffid);

	if (islocal)
		bufHdr = GetLocalBufferDescriptor(-buffid - 1);
	else
		bufHdr = GetBufferDescriptor(buffid - 1);

	tag = bufHdr->tag;
	reln = smgropen(tag.rnode, io->scb_data.read_sb.backend);
	io->op_data.write.fd = smgrfd(reln, tag.forkNum, tag.blockNum, &off);

	Assert(off == io->op_data.write.offset);
}

static bool
pgaio_write_sb_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	ReadBufferCompleteWrite(io->scb_data.write_sb.buffid,
							io->scb_data.write_sb.release_lock,
							io->flags & PGAIOIP_HARD_FAILURE);
	return true;
}

static void
pgaio_write_sb_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, release_lock: %d, buffid: %u",
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->scb_data.write_sb.release_lock,
					 io->scb_data.write_sb.buffid);
}


static void
pgaio_write_smgr_retry(PgAioInProgress *io)
{
	uint32 off;
	AioBufferTag *tag = &io->scb_data.write_smgr.tag;
	SMgrRelation reln = smgropen(tag->rnode.node,
								 tag->rnode.backend);

	io->op_data.read.fd = smgrfd(reln, tag->forkNum, tag->blockNum, &off);
	Assert(off == io->op_data.read.offset);
}

static bool
pgaio_write_smgr_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (!(io->flags & PGAIOIP_HARD_FAILURE))
		Assert(io->op_data.write.already_done == BLCKSZ);

	return true;
}

static void
pgaio_write_smgr_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, data: %p",
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->op_data.write.bufdata);
}


static void
pgaio_write_wal_retry(PgAioInProgress *io)
{
	io->op_data.write.fd = XLogFileForWriteNo(io->scb_data.write_wal.write_no);
}

static bool
pgaio_write_wal_complete(PgAioInProgress *io)
{
	if (io->flags & PGAIOIP_SOFT_FAILURE)
		return false;

	if (io->flags & PGAIOIP_HARD_FAILURE)
	{
		if (io->result <= 0)
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log file: %s",
							strerror(-io->result))));
		}
		else
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log file: wrote only %d of %d bytes",
							io->result, (io->op_data.write.nbytes - io->op_data.write.already_done))));
		}
	}


	XLogWriteComplete(io, io->scb_data.write_wal.write_no);

	return true;
}

static void
pgaio_write_wal_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "write_no: %d, fd: %d, offset: %llu, nbytes: %u, already_done: %u, bufdata: %p",
					 io->scb_data.write_wal.write_no,
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->op_data.write.bufdata);
}

static bool
pgaio_write_generic_complete(PgAioInProgress *io)
{
	/* FIXME: retries for partial writes */
	/* FIXME: this should just be in local completion callback */
	if (io->result < 0)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to generic file: %s",
						strerror(-io->result))));
	}
	else if (io->op_data.write.nbytes != io->op_data.write.already_done)
	{
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to generic file: wrote only %d of %d bytes",
						io->op_data.write.already_done, io->op_data.write.nbytes)));
	}

	return true;
}

static void
pgaio_write_generic_desc(PgAioInProgress *io, StringInfo s)
{
	appendStringInfo(s, "fd: %d, offset: %llu, nbytes: %u, already_done: %u, bufdata: %p",
					 io->op_data.write.fd,
					 (unsigned long long) io->op_data.write.offset,
					 io->op_data.write.nbytes,
					 io->op_data.write.already_done,
					 io->op_data.write.bufdata);
}

/*
 * Extract iov_base and iov_len from a single IO.
 */
static void
pgaio_fill_one_iov(struct iovec *iov, const PgAioInProgress *io, bool first)
{
	switch (io->op)
	{
		case PGAIO_OP_WRITE:
			Assert(first || io->op_data.write.already_done == 0);
			iov->iov_base = io->op_data.write.bufdata + io->op_data.write.already_done;
			iov->iov_len = io->op_data.write.nbytes - io->op_data.write.already_done;
			break;
		case PGAIO_OP_READ:
			Assert(first || io->op_data.read.already_done == 0);
			iov->iov_base = io->op_data.read.bufdata + io->op_data.read.already_done;
			iov->iov_len = io->op_data.read.nbytes - io->op_data.read.already_done;
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
	pgaio_fill_one_iov(iov, io, true);

	/*
	 * We have a chain of IOs that were linked together because they access
	 * contiguous regions of a file.  As a micro-optimization we'll also
	 * consolidate iovecs that access contiguous memory.
	 */
	while (io->merge_with_idx != PGAIO_MERGE_INVALID)
	{
		struct iovec *next = iov + 1;

		io = &aio_ctl->in_progress_io[io->merge_with_idx];

		pgaio_fill_one_iov(next, io, false);
		if ((char *) iov->iov_base + iov->iov_len == next->iov_base)
			iov->iov_len += next->iov_len;
		else
			++iov;
	}

	return iov + 1 - iovs;
}

static void
pgaio_worker_do(PgAioInProgress *io)
{
	ssize_t result = 0;
	struct iovec iov[IOV_MAX];
	int iovcnt = 0;
	bool fd_usable;

	Assert(io->flags & PGAIOIP_INFLIGHT);

	/*
	 * When running in a worker process, we can't use the file descriptor;
	 * instead we have to open the file, with appropriate caching.
	 */
	fd_usable = !AmAioWorkerProcess();

	/*
	 * If this is not the process originating the request (can happen via
	 * pgaio_synchronous_submit()), we need to open the files locally to rely
	 * on the fd.
	 */
	if (!fd_usable)
	{
		Assert(io_action_cbs[io->scb].retry != NULL);
		io_action_cbs[io->scb].retry(io);
	}

	/* Perform IO. */
	switch (io->op)
	{
		case PGAIO_OP_READ:
			iovcnt = pgaio_fill_iov(iov, io);
			pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
			result = pg_preadv(io->op_data.read.fd, iov, iovcnt,
							   io->op_data.read.offset + io->op_data.read.already_done);
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_WRITE:
			iovcnt = pgaio_fill_iov(iov, io);
			pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
			result = pg_pwritev(io->op_data.write.fd, iov, iovcnt,
								io->op_data.write.offset + io->op_data.write.already_done);
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_FSYNC:
			pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);
			if (io->op_data.fsync.datasync)
				result = pg_fdatasync(io->op_data.fsync.fd);
			else
				result = pg_fsync(io->op_data.fsync.fd);
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_FLUSH_RANGE:
			pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_FLUSH);
			pg_flush_data(io->op_data.flush_range.fd,
						  io->op_data.flush_range.offset,
						  io->op_data.flush_range.nbytes);
			/* never errors */
			result = 0;
			pgstat_report_wait_end();
			break;
		case PGAIO_OP_NOP:
			result = 0;
			break;
		case PGAIO_OP_INVALID:
			result = -1;
			errno = EOPNOTSUPP;
			break;
	}

	pgaio_process_io_completion(io, result < 0 ? -errno : result);
}

bool
IsAioWorker(void)
{
	return MyBackendType == B_AIO_WORKER;
}

void
AioWorkerMain(void)
{
	/* TODO review all signals */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, die); /* to allow manually triggering worker restart */
	/*
	 * Ignore SIGTERM, will get explicit shutdown via SIGUSR2 later in the
	 * shutdown sequence, similar to checkpointer.
	 */
	pqsignal(SIGTERM, SIG_IGN);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, die);
	PG_SETMASK(&UnBlockSig);

	for (;;)
	{
		uint32 io_index;

		if (squeue32_dequeue(aio_ctl->aio_submission_queue, &io_index))
		{
			/* Do IO and completions.  This'll signal anyone waiting. */
			/*
			 * XXX I think we might need this to be in a critical section, so
			 * that we can't lose track of an IO without taking the system
			 * down.  But we're not allowed to, because the IO handler might
			 * reach smgropen() which allocates.
			 */
			ConditionVariableCancelSleep();
			pgaio_worker_do(&aio_ctl->in_progress_io[io_index]);
			pgaio_complete_ios(false);
		}
		else
		{
			/* Nothing in the queue.  Go to sleep. */
			ConditionVariableSleep(&aio_ctl->submission_queue_not_empty,
								   WAIT_EVENT_AIO_WORKER_MAIN);
		}
	}
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
#define PG_STAT_GET_AIOS_COLS	9

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
		PgAioInProgress *raw_io = &aio_ctl->in_progress_io[i];
		PgAioInProgress io_copy;
		Datum		values[PG_STAT_GET_AIOS_COLS];
		bool		nulls[PG_STAT_GET_AIOS_COLS];
		uint64 ref_generation;
		PgAioInProgressFlags flags;

		flags = raw_io->flags;
		if (flags & PGAIOIP_UNUSED)
			continue;

		ref_generation = raw_io->generation;

		pg_read_barrier();
		io_copy = *raw_io;
		pg_read_barrier();

		flags = raw_io->flags;
		pg_read_barrier();

		if (raw_io->generation != ref_generation)
			continue;

		if (flags & PGAIOIP_UNUSED)
			continue;

		memset(nulls, 0, sizeof(nulls));

		values[ 0] = Int32GetDatum(i);
		values[ 1] = Int64GetDatum(io_copy.generation);
		values[ 2] = PointerGetDatum(cstring_to_text(pgaio_io_operation_string(io_copy.op)));
		values[ 3] = PointerGetDatum(cstring_to_text(pgaio_io_shared_callback_string(io_copy.scb)));

		pgaio_io_flag_string(flags, &tmps);
		values[ 4] = PointerGetDatum(cstring_to_text(tmps.data));
		resetStringInfo(&tmps);

		values[ 5] = Int32GetDatum(io_copy.ring);

		if (io_copy.owner_id != INVALID_PGPROCNO)
			values[ 6] = Int32GetDatum(ProcGlobal->allProcs[io_copy.owner_id].pid);
		else
			nulls[ 6] = true;

		values[ 7] = Int32GetDatum(io_copy.result);

		pgaio_io_shared_desc(&io_copy, &tmps);
		values[ 8] = PointerGetDatum(cstring_to_text(tmps.data));
		resetStringInfo(&tmps);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
