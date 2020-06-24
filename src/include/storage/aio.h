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
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/relfilenode.h"
#include "lib/ilist.h"

typedef struct PgAioInProgress PgAioInProgress;
typedef struct PgAioBounceBuffer PgAioBounceBuffer;

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

extern dlist_node* pgaio_io_node(PgAioInProgress *io);

typedef struct PgAioOnCompletionLocalContext PgAioOnCompletionLocalContext;;
typedef void (*PgAioOnCompletionLocalCB)(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);
struct PgAioOnCompletionLocalContext
{
	PgAioOnCompletionLocalCB callback;
};
#define pgaio_ocb_container(type, membername, ptr)								\
	(AssertVariableIsOfTypeMacro(ptr, PgAioOnCompletionLocalContext *),			\
	 AssertVariableIsOfTypeMacro(((type *) NULL)->membername, PgAioOnCompletionLocalContext),	\
	 ((type *) ((char *) (ptr) - offsetof(type, membername))))
extern void pgaio_io_on_completion_local(PgAioInProgress *io, PgAioOnCompletionLocalContext *ocb);

extern void pgaio_io_wait(PgAioInProgress *io, bool holding_reference);
extern bool pgaio_io_success(PgAioInProgress *io);
extern bool pgaio_io_done(PgAioInProgress *io);
extern void pgaio_io_recycle(PgAioInProgress *io);


typedef struct AioBufferTag
{
	RelFileNodeBackend rnode;			/* physical relation identifier */
	ForkNumber	forkNum;
	BlockNumber blockNum;		/* blknum relative to begin of reln */
} AioBufferTag;
struct buftag;

extern void pgaio_io_start_flush_range(PgAioInProgress *io, int fd, off_t offset, off_t nbytes);
extern void pgaio_io_start_nop(PgAioInProgress *io);
extern void pgaio_io_start_fsync(PgAioInProgress *io, int fd, bool barrier);
extern void pgaio_io_start_fdatasync(PgAioInProgress *io, int fd, bool barrier);

extern void pgaio_io_start_read_buffer(PgAioInProgress *io, const AioBufferTag *tag, int fd, uint32 offset, uint32 nbytes,
									   char *bufdata, int buffno, int mode);
extern void pgaio_io_start_write_buffer(PgAioInProgress *io, const AioBufferTag *tag, int fd, uint32 offset, uint32 nbytes,
										char *bufdata, int buffno);
extern void pgaio_io_start_write_wal(PgAioInProgress *io, int fd,
									 uint32 offset, uint32 nbytes,
									 char *bufdata, bool no_reorder);

extern void pgaio_io_retry(PgAioInProgress *io);
extern void pgaio_submit_pending(bool drain);

extern void pgaio_drain_shared(void);

extern void pgaio_print_queues(void);
struct StringInfoData;
extern void pgaio_io_print(PgAioInProgress *io, struct StringInfoData *s);
struct dlist_head;
extern void pgaio_print_list(struct dlist_head *head, struct StringInfoData *s, size_t offset);


extern void pgaio_assoc_bounce_buffer(PgAioInProgress *io, PgAioBounceBuffer *bb);
extern PgAioBounceBuffer *pgaio_bounce_buffer_get(void);
extern char *pgaio_bounce_buffer_buffer(PgAioBounceBuffer *bb);


/*
 * Helpers. In aio_util.c.
 */

/*
 * Helper to efficiently perform bulk writes.
 */
typedef struct pg_streaming_write pg_streaming_write;
typedef void (*pg_streaming_write_completed)(void *pgsw_private, void *write_private);

extern pg_streaming_write *pg_streaming_write_alloc(uint32 iodepth, void *private,
													pg_streaming_write_completed on_completion);
extern PgAioInProgress *pg_streaming_write_get_io(pg_streaming_write *pgsw);
extern uint32 pg_streaming_write_inflight(pg_streaming_write *pgsw);
extern void pg_streaming_write_write(pg_streaming_write *pgsw, PgAioInProgress *io, void *private);
extern void pg_streaming_write_wait(pg_streaming_write *pgsw, uint32 count);
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
