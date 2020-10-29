/*-------------------------------------------------------------------------
 *
 * aio.h
 *	  aio
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/aio.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AIO_H
#define AIO_H

#include "common/relpath.h"
#include "lib/ilist.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/relfilenode.h"


typedef struct PgAioInProgress PgAioInProgress;
typedef struct PgAioBounceBuffer PgAioBounceBuffer;

typedef struct PgAioIoRef
{
	uint32 aio_index;
	uint32 generation_upper;
	uint32 generation_lower;
} PgAioIoRef;

/* Enum for aio_type GUC. */
typedef enum AioType
{
	AIOTYPE_WORKER = 0,
	AIOTYPE_LIBURING,
} AioType;

/* We'll default to bgworker. */
#define DEFAULT_AIO_TYPE AIOTYPE_WORKER

/* GUCs */
extern int aio_type;
extern int aio_workers;
extern int aio_worker_queue_size;

extern int MyAioWorkerId;

/* initialization */
extern void pgaio_postmaster_init(void);
extern Size AioShmemSize(void);
extern void AioShmemInit(void);
extern void pgaio_postmaster_child_init_local(void);
extern void pgaio_postmaster_child_init(void);

extern void pgaio_at_abort(void);
extern void pgaio_at_commit(void);

/*
 * XXX: Add flags to the initiation functions that govern:
 * - whether PgAioInProgress will be released once the IO has successfully
 *   finished, even if done by another backend (e.g. useful when prefetching,
 *   without wanting to look at the concrete buffer, or when doing buffer
 *   writeback). In particular this'd also cause the buffer pin to be released
 *   upon completion.
 * - whether a failed request needs to be seen by the issuing backend. That's
 *   not needed e.g. for prefetches, but is for buffer writes by checkpointer.
 * - whether pending requests might be issued if sensible, or whether that's
 *   not allowed.
 *
 *
 * FIXME: Add indicator about which IO channel needs to be used (important
 * e.g. for buffer writes interleaved with WAL writes, for queue depth
 * management of checkpointer, for readahead)
 */

extern PgAioInProgress *pgaio_io_get(void);
extern void pgaio_io_release(PgAioInProgress *io);

typedef struct PgAioOnCompletionLocalContext PgAioOnCompletionLocalContext;
typedef void (*PgAioOnCompletionLocalCB)(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);

struct PgAioOnCompletionLocalContext
{
	PgAioOnCompletionLocalCB callback;
};

#define pgaio_ocb_container(type, membername, ptr)												\
	(AssertVariableIsOfTypeMacro(ptr, PgAioOnCompletionLocalContext *),							\
	 AssertVariableIsOfTypeMacro(((type *) NULL)->membername, PgAioOnCompletionLocalContext),	\
	 ((type *) ((char *) (ptr) - offsetof(type, membername))))

extern void pgaio_io_on_completion_local(PgAioInProgress *io, PgAioOnCompletionLocalContext *ocb);

extern void pgaio_io_wait(PgAioInProgress *io);
extern void pgaio_io_wait_ref(PgAioIoRef *ref, bool call_local);
extern bool pgaio_io_check_ref(PgAioIoRef *ref);

extern bool pgaio_io_success(PgAioInProgress *io);
extern int pgaio_io_result(PgAioInProgress *io);
extern bool pgaio_io_done(PgAioInProgress *io);
extern bool pgaio_io_pending(PgAioInProgress *io);
extern uint32 pgaio_io_id(PgAioInProgress *io);
extern uint64 pgaio_io_generation(PgAioInProgress *io);

extern void pgaio_io_recycle(PgAioInProgress *io);
extern void pgaio_io_retry(PgAioInProgress *io);

extern void pgaio_print_queues(void);
struct StringInfoData;
extern void pgaio_io_print(PgAioInProgress *io, struct StringInfoData *s);
extern void pgaio_io_ref_print(PgAioIoRef *ref, struct StringInfoData *s);

struct dlist_head;
extern void pgaio_print_list(struct dlist_head *head, struct StringInfoData *s, size_t offset);
extern void pgaio_submit_pending(bool drain);

extern void pgaio_closing_possibly_referenced(void);

extern void pgaio_io_ref(PgAioInProgress *io, PgAioIoRef *ref);


static inline bool
pgaio_io_ref_valid(const PgAioIoRef *ref)
{
	return ref->aio_index != PG_UINT32_MAX;
}

static inline void
pgaio_io_ref_clear(PgAioIoRef *ref)
{
	ref->aio_index = PG_UINT32_MAX;
}


typedef struct AioBufferTag
{
	RelFileNodeBackend rnode;			/* physical relation identifier */
	ForkNumber	forkNum;
	BlockNumber blockNum;		/* blknum relative to begin of reln */
} AioBufferTag;

struct SMgrRelationData;


/* --------------------------------------------------------------------------------
 * Low level IO preparation routines. These are called as part of
 * pgaio_io_start_*.
 * --------------------------------------------------------------------------------
 */

extern void pgaio_io_prep_read(PgAioInProgress *io, int fd, char *bufdata, uint64 offset, uint32 nbytes);
extern void pgaio_io_prep_write(PgAioInProgress *io, int fd, char *bufdata, uint64 offset, uint32 nbytes);
extern void pgaio_io_prep_fsync(PgAioInProgress *io, int fd, bool datasync);
extern void pgaio_io_prep_flush_range(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes);
extern void pgaio_io_prep_nop(PgAioInProgress *io);



/* --------------------------------------------------------------------------------
 * IO start routines
 * --------------------------------------------------------------------------------
 */

extern void pgaio_io_start_read_smgr(PgAioInProgress *io,
									 struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
									 char *bufdata);
extern void pgaio_io_start_read_sb(PgAioInProgress *io,
								   struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
								   char *bufdata, int buffid, int mode);

extern void pgaio_io_start_write_sb(PgAioInProgress *io,
									struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
									char *bufdata, int buffid, bool skipFsync, bool release_lock);
extern void pgaio_io_start_write_smgr(PgAioInProgress *io,
									  struct SMgrRelationData* smgr, ForkNumber forknum, BlockNumber blocknum,
									  char *bufdata, bool skipFsync);
extern void pgaio_io_start_write_wal(PgAioInProgress *io, int fd,
									 uint32 offset, uint32 nbytes,
									 char *bufdata,
									 uint32 write_no);
extern void pgaio_io_start_write_generic(PgAioInProgress *io, int fd,
										 uint64 offset, uint32 nbytes,
										 char *bufdata);

extern void pgaio_io_start_fsync_raw(PgAioInProgress *io, int fd,
									 bool datasync);
extern void pgaio_io_start_fsync_wal(PgAioInProgress *io, int fd,
									 bool datasync_only, uint32 sync_no);

extern void pgaio_io_start_flush_range_raw(PgAioInProgress *io, int fd, uint64 offset, uint32 nbytes);
extern BlockNumber pgaio_io_start_flush_range_smgr(PgAioInProgress *io,
												   struct SMgrRelationData* smgr, ForkNumber forknum,
												   BlockNumber blocknum, BlockNumber nblocks);

extern void pgaio_io_start_nop(PgAioInProgress *io);


/* --------------------------------------------------------------------------------
 * Bounce buffers for doing BLCKSZ sized asynchronous IO to/from outside of
 * shared bufers.
 * --------------------------------------------------------------------------------
 */

extern void pgaio_assoc_bounce_buffer(PgAioInProgress *io, PgAioBounceBuffer *bb);
extern PgAioBounceBuffer *pgaio_bounce_buffer_get(void);
extern void pgaio_bounce_buffer_release(PgAioBounceBuffer *bb);
extern char *pgaio_bounce_buffer_buffer(PgAioBounceBuffer *bb);


/* --------------------------------------------------------------------------------
 * IO backend implementation related functions
 * --------------------------------------------------------------------------------
 */

extern void AioWorkerMain(void);
extern bool IsAioWorker(void);


/* --------------------------------------------------------------------------------
 * Helpers. In aio_util.c.
 * --------------------------------------------------------------------------------
 */

/*
 * Helper to efficiently perform bulk writes.
 */
typedef struct pg_streaming_write pg_streaming_write;
typedef void (*pg_streaming_write_completed)(pg_streaming_write *pgsw, void *pgsw_private, int result, void *write_private);

extern pg_streaming_write *pg_streaming_write_alloc(uint32 iodepth, void *private);
extern PgAioInProgress *pg_streaming_write_get_io(pg_streaming_write *pgsw);
extern void pg_streaming_write_release_io(pg_streaming_write *pgsw, PgAioInProgress *io);
extern uint32 pg_streaming_write_inflight(pg_streaming_write *pgsw);
extern void pg_streaming_write_write(pg_streaming_write *pgsw, PgAioInProgress *io,
									 pg_streaming_write_completed on_completion,
									 void *private);
extern void pg_streaming_write_wait_all(pg_streaming_write *pgsw);
extern void pg_streaming_write_free(pg_streaming_write *pgsw);

/*
 * Helper for efficient reads (using readahead).
 */

typedef struct PgStreamingRead PgStreamingRead;
typedef enum PgStreamingReadNextStatus
{
	PGSR_NEXT_END,
	PGSR_NEXT_NO_IO,
	PGSR_NEXT_IO
} PgStreamingReadNextStatus;

typedef PgStreamingReadNextStatus (*PgStreamingReadDetermineNextCB)(uintptr_t pgsr_private, PgAioInProgress *aio, uintptr_t *read_private);
typedef void (*PgStreamingReadRelease)(uintptr_t pgsr_private, uintptr_t read_private);
extern PgStreamingRead *pg_streaming_read_alloc(uint32 iodepth, uintptr_t pgsr_private,
												PgStreamingReadDetermineNextCB determine_next_cb,
												PgStreamingReadRelease release_cb);
extern void pg_streaming_read_free(PgStreamingRead *pgsr);
extern uintptr_t pg_streaming_read_get_next(PgStreamingRead *pgsr);

#endif							/* AIO_H */
