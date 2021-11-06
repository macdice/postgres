/*-------------------------------------------------------------------------
 *
 * xlogprefetch.c
 *		Prefetching support for recovery.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		src/backend/access/transam/xlogprefetch.c
 *
 * The goal of this module is to read future WAL records and issue
 * PrefetchSharedBuffer() calls for referenced blocks, so that we avoid I/O
 * stalls in the main recovery loop.
 *
 * When examining a WAL record from the future, we need to consider that a
 * referenced block or segment file might not exist on disk until this record
 * or some earlier record has been replayed.  After a crash, a file might also
 * be missing because it was dropped by a later WAL record; in that case, it
 * will be recreated when this record is replayed.  These cases are handled by
 * recognizing them and adding a "filter" that prevents all prefetching of a
 * certain block range until the present WAL record has been replayed.  Blocks
 * skipped for these reasons are counted as "skip_new" (that is, cases where we
 * didn't try to prefetch "new" blocks).
 *
 * Blocks found in the buffer pool already are counted as "skip_hit".
 * Repeated access to the same buffer is detected and skipped, and this is
 * counted with "skip_seq".  Blocks that were logged with FPWs are skipped if
 * recovery_prefetch_fpw is off, since on most systems there will be no I/O
 * stall; this is counted with "skip_fpw".
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogprefetch.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "catalog/storage_xlog.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"
#include "funcapi.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/aio.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

/*
 * Sample the queue depth and distance every time we replay this much WAL.
 * This is used to compute avg_queue_depth and avg_distance for the log
 * message that appears at the end of crash recovery.
 */
#define XLOGPREFETCHER_SAMPLE_DISTANCE BLCKSZ

/*
 * Flag set in decoded block reference to indicate that the block has an
 * active streaming read, so we have to call pg_streaming_read_get_next() to
 * complete that read and stay in sync.
 */
#define XLOGPREFETCHER_MUST_CALL_GET_NEXT 0x01

/*
 * Flag set in decoded block reference to indicate that the block is in
 * "recent_buffer" and pinned.
 */
#define XLOGPREFETCHER_PINNED 0x02

/* GUCs */
bool		recovery_prefetch = false;
bool		recovery_prefetch_fpw = false;

static int	XLogPrefetchReconfigureCount = 0;

/*
 * A prefetcher.  This is a mechanism that wraps an XLogReader, prefetching
 * blocks that will be soon be referenced, to try to avoid IO stalls.
 *
 * XXX say something about GUCs and buffer pools
 */
struct XLogPrefetcher
{
	/* WAL reader and current reading state. */
	XLogReaderState *reader;
	DecodedXLogRecord *record;
	int			next_block_id;

	/* Details of last prefetch to skip repeats and seq scans. */
	SMgrRelation last_reln;
	RelFileNode last_rnode;
	BlockNumber last_blkno;

	/* Online averages. */
	uint64		samples;
	double		avg_queue_depth;
	double		avg_distance;
	XLogRecPtr	next_sample_lsn;

	/* Book-keeping required to avoid accessing non-existing blocks. */
	HTAB	   *filter_table;
	dlist_head	filter_queue;

	/* IO depth manager. */
	PgStreamingRead *streaming_read;
	int			reconfigure_count;
};

/*
 * A temporary filter used to track block ranges that haven't been created
 * yet, whole relations that haven't been created yet, and whole relations
 * that (we assume) have already been dropped, or will be created by bulk WAL
 * operators.
 */
typedef struct XLogPrefetcherFilter
{
	RelFileNode rnode;
	XLogRecPtr	filter_until_replayed;
	BlockNumber filter_from_block;
	dlist_node	link;
} XLogPrefetcherFilter;

/*
 * Counters exposed in shared memory for pg_stat_prefetch_recovery.
 */
typedef struct XLogPrefetchStats
{
	pg_atomic_uint64 reset_time;	/* Time of last reset. */
	pg_atomic_uint64 prefetch;	/* Prefetches initiated. */
	pg_atomic_uint64 skip_hit;	/* Blocks already buffered. */
	pg_atomic_uint64 skip_new;	/* New/missing blocks filtered. */
	pg_atomic_uint64 skip_fpw;	/* FPWs skipped. */
	pg_atomic_uint64 skip_seq;	/* Repeat blocks skipped. */
	float		avg_distance;
	float		avg_queue_depth;

	/* Reset counters */
	pg_atomic_uint32 reset_request;
	uint32		reset_handled;

	/* Dynamic values */
	int			distance;		/* Number of bytes ahead in the WAL. */
	int			queue_depth;	/* Number of I/Os in progress. */
} XLogPrefetchStats;

static inline void XLogPrefetcherAddFilter(XLogPrefetcher *prefetcher,
										   RelFileNode rnode,
										   BlockNumber blockno,
										   XLogRecPtr lsn);
static inline bool XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher,
											RelFileNode rnode,
											BlockNumber blockno);
static inline void XLogPrefetcherCompleteFilters(XLogPrefetcher *prefetcher,
												 XLogRecPtr replaying_lsn);
//static void XLogPrefetchSaveStats(void);
//static void XLogPrefetchRestoreStats(void);

static PgStreamingReadNextStatus XLogPrefetcherNextBlock(uintptr_t pgsr_private,
														 PgAioInProgress *aio,
														 uintptr_t *read_private);

static XLogPrefetchStats *SharedStats;

size_t
XLogPrefetchShmemSize(void)
{
	return sizeof(XLogPrefetchStats);
}

#if 0
static void
XLogPrefetchResetStats(void)
{
	pg_atomic_write_u64(&SharedStats->reset_time, GetCurrentTimestamp());
	pg_atomic_write_u64(&SharedStats->prefetch, 0);
	pg_atomic_write_u64(&SharedStats->skip_hit, 0);
	pg_atomic_write_u64(&SharedStats->skip_new, 0);
	pg_atomic_write_u64(&SharedStats->skip_fpw, 0);
	pg_atomic_write_u64(&SharedStats->skip_seq, 0);
	SharedStats->avg_distance = 0;
	SharedStats->avg_queue_depth = 0;
}
#endif

void
XLogPrefetchShmemInit(void)
{
	bool		found;

	SharedStats = (XLogPrefetchStats *)
		ShmemInitStruct("XLogPrefetchStats",
						sizeof(XLogPrefetchStats),
						&found);

	if (!found)
	{
		pg_atomic_init_u32(&SharedStats->reset_request, 0);
		SharedStats->reset_handled = 0;


		pg_atomic_init_u64(&SharedStats->reset_time, GetCurrentTimestamp());
		pg_atomic_init_u64(&SharedStats->prefetch, 0);
		pg_atomic_init_u64(&SharedStats->skip_hit, 0);
		pg_atomic_init_u64(&SharedStats->skip_new, 0);
		pg_atomic_init_u64(&SharedStats->skip_fpw, 0);
		pg_atomic_init_u64(&SharedStats->skip_seq, 0);
		SharedStats->avg_distance = 0;
		SharedStats->avg_queue_depth = 0;
		SharedStats->distance = 0;
		SharedStats->queue_depth = 0;
	}
}

/*
 * Called when any GUC is changed that affects prefetching.
 */
void
XLogPrefetchReconfigure(void)
{
	XLogPrefetchReconfigureCount++;
}

/*
 * Called by any backend to request that the stats be reset.
 */
void
XLogPrefetchRequestResetStats(void)
{
	pg_atomic_fetch_add_u32(&SharedStats->reset_request, 1);
}

#if 0
/*
 * Tell the stats collector to serialize the shared memory counters into the
 * stats file.
 */
static void
XLogPrefetchSaveStats(void)
{
	PgStat_RecoveryPrefetchStats serialized = {
		.prefetch = pg_atomic_read_u64(&SharedStats->prefetch),
		.skip_hit = pg_atomic_read_u64(&SharedStats->skip_hit),
		.skip_new = pg_atomic_read_u64(&SharedStats->skip_new),
		.skip_fpw = pg_atomic_read_u64(&SharedStats->skip_fpw),
		.skip_seq = pg_atomic_read_u64(&SharedStats->skip_seq),
		.stat_reset_timestamp = pg_atomic_read_u64(&SharedStats->reset_time)
	};

	pgstat_send_recoveryprefetch(&serialized);
}
#endif

#if 0
/*
 * Try to restore the shared memory counters from the stats file.
 */
static void
XLogPrefetchRestoreStats(void)
{
	PgStat_RecoveryPrefetchStats *serialized = pgstat_fetch_recoveryprefetch();

	if (serialized->stat_reset_timestamp != 0)
	{
		pg_atomic_write_u64(&SharedStats->prefetch, serialized->prefetch);
		pg_atomic_write_u64(&SharedStats->skip_hit, serialized->skip_hit);
		pg_atomic_write_u64(&SharedStats->skip_new, serialized->skip_new);
		pg_atomic_write_u64(&SharedStats->skip_fpw, serialized->skip_fpw);
		pg_atomic_write_u64(&SharedStats->skip_seq, serialized->skip_seq);
		pg_atomic_write_u64(&SharedStats->reset_time, serialized->stat_reset_timestamp);
	}
}
#endif

/*
 * Increment a counter in shared memory.  This is equivalent to *counter++ on a
 * plain uint64 without any memory barrier or locking, except on platforms
 * where readers can't read uint64 without possibly observing a torn value.
 */
static inline void
XLogPrefetchIncrement(pg_atomic_uint64 *counter)
{
	Assert(AmStartupProcess() || !IsUnderPostmaster);
	pg_atomic_write_u64(counter, pg_atomic_read_u64(counter) + 1);
}

#if 0
/*
 * Initialize an XLogPrefetchState object and restore the last saved
 * statistics from disk.
 */
void
XLogPrefetchBegin(XLogPrefetchState *state, XLogReaderState *reader)
{
	XLogPrefetchRestoreStats();

	/* We'll reconfigure on the first call to XLogPrefetch(). */
	state->reader = reader;
	state->prefetcher = NULL;
	state->reconfigure_count = XLogPrefetchReconfigureCount - 1;
}
#endif

#if 0
/*
 * Shut down the prefetching infrastructure, if configured.
 */
void
XLogPrefetchEnd(XLogPrefetchState *state)
{
	XLogPrefetchSaveStats();

	if (state->prefetcher)
		XLogPrefetcherFree(state->prefetcher);
	state->prefetcher = NULL;

	SharedStats->queue_depth = 0;
	SharedStats->distance = 0;
}
#endif

static void
XLogPrefetcherReleaseBlock(uintptr_t pgsr_private, uintptr_t read_private)
{
	DecodedBkpBlock *block = (DecodedBkpBlock *) read_private;

	if (block)
	{
		if (block->prefetch_flags & XLOGPREFETCHER_PINNED)
		{
			Assert(BufferIsValid(block->recent_buffer));
			ReleaseBuffer(block->recent_buffer);
			block->prefetch_flags &= ~XLOGPREFETCHER_PINNED;
		}
		block->prefetch_flags &= ~XLOGPREFETCHER_MUST_CALL_GET_NEXT;
	}
}

/*
 * Create a prefetcher that is ready to begin prefetching blocks referenced by
 * WAL records.
 */
XLogPrefetcher *
XLogPrefetcherAllocate(XLogReaderState *reader)
{
	XLogPrefetcher *prefetcher;
	static HASHCTL hash_table_ctl = {
		.keysize = sizeof(RelFileNode),
		.entrysize = sizeof(XLogPrefetcherFilter)
	};

	prefetcher = palloc0(sizeof(XLogPrefetcher));

	prefetcher->reader = reader;
	prefetcher->filter_table = hash_create("XLogPrefetcherFilterTable", 1024,
										   &hash_table_ctl,
										   HASH_ELEM | HASH_BLOBS);
	dlist_init(&prefetcher->filter_queue);

	SharedStats->queue_depth = 0;
	SharedStats->distance = 0;

	/*
	 * The allowed IO depth is based on the maintenance_io_concurrency setting.
	 * In theory we might have a separate limit for each tablespace, but it's
	 * not clear how that should work, so for now we'll just use the general
	 * GUC to rate-limit all prefetching.
	 */
	prefetcher->streaming_read =
		pg_streaming_read_alloc(maintenance_io_concurrency,
								(uintptr_t) prefetcher,
								XLogPrefetcherNextBlock,
								XLogPrefetcherReleaseBlock);

	prefetcher->reconfigure_count = XLogPrefetchReconfigureCount;

	return prefetcher;
}

/*
 * Destroy a prefetcher and release all resources.
 */
void
XLogPrefetcherFree(XLogPrefetcher *prefetcher)
{
	pg_streaming_read_free(prefetcher->streaming_read);

	/* Log final statistics. */
	ereport(LOG,
			(errmsg("recovery finished prefetching at %X/%X; "
					"prefetch = " UINT64_FORMAT ", "
					"skip_hit = " UINT64_FORMAT ", "
					"skip_new = " UINT64_FORMAT ", "
					"skip_fpw = " UINT64_FORMAT ", "
					"skip_seq = " UINT64_FORMAT ", "
					"avg_distance = %f, "
					"avg_queue_depth = %f",
					(uint32) (prefetcher->reader->EndRecPtr << 32),
					(uint32) (prefetcher->reader->EndRecPtr),
					pg_atomic_read_u64(&SharedStats->prefetch),
					pg_atomic_read_u64(&SharedStats->skip_hit),
					pg_atomic_read_u64(&SharedStats->skip_new),
					pg_atomic_read_u64(&SharedStats->skip_fpw),
					pg_atomic_read_u64(&SharedStats->skip_seq),
					SharedStats->avg_distance,
					SharedStats->avg_queue_depth)));
	hash_destroy(prefetcher->filter_table);
	pfree(prefetcher);
}

#ifdef UNUSED
static void
XLogPrefetcherPeriodic(XLogPrefetcher *prefetcher)
{
	uint32 io_depth;
	uint32 reset_request;
	int64 distance;

	/*
	 * Normally we initiate new IOs when others complete, but we need to kick
	 * the the streaming read object from time to time while it's idle, or
	 * it'd never get started.
	 */
	io_depth = pg_streaming_read_inflight(prefetcher->streaming_read);
#if 0
	if (io_depth == 0)
	{
		elog(LOG, "XXXXXX will call pg_streaming_read_prefetch");
		pg_streaming_read_prefetch(prefetcher->streaming_read);
		elog(LOG, "XXXXXX pg_stremaing_read_prefetch returned");
	}
#endif

	/* How far ahead of replay are we now? */
	if (prefetcher->record)
		distance = prefetcher->record->lsn - prefetcher->reader->record->lsn;
	else
		distance = 0;

	/* Update the instantaneous stats visible in pg_stat_prefetch_recovery. */
	SharedStats->queue_depth = io_depth;
	SharedStats->distance = distance;

	/*
	 * Have we been asked to reset our stats counters?  This is checked with
	 * an unsynchronized memory read, but we'll see it eventually and we'll be
	 * accessing that cache line anyway.
	 */
	reset_request = pg_atomic_read_u32(&SharedStats->reset_request);
	if (reset_request != SharedStats->reset_handled)
	{
		XLogPrefetchResetStats();
		SharedStats->reset_handled = reset_request;

		prefetcher->avg_distance = 0;
		prefetcher->avg_queue_depth = 0;
		prefetcher->samples = 0;
	}

	/* Compute online averages. */
	prefetcher->samples++;
	if (prefetcher->samples == 1)
	{
		prefetcher->avg_distance = SharedStats->distance;
		prefetcher->avg_queue_depth = SharedStats->queue_depth;
	}
	else
	{
		prefetcher->avg_distance +=
			(SharedStats->distance - prefetcher->avg_distance) /
			prefetcher->samples;
		prefetcher->avg_queue_depth +=
			(SharedStats->queue_depth - prefetcher->avg_queue_depth) /
			prefetcher->samples;
	}

	/* Expose it in shared memory. */
	SharedStats->avg_distance = prefetcher->avg_distance;
	SharedStats->avg_queue_depth = prefetcher->avg_queue_depth;

	prefetcher->next_sample_lsn =
		prefetcher->reader->record->lsn + XLOGPREFETCHER_SAMPLE_DISTANCE;
}
#endif

/*
 * A PgStreamingRead callback that reads ahead in the WAL and tries to
 * initiate one IO.
 */
static PgStreamingReadNextStatus
XLogPrefetcherNextBlock(uintptr_t pgsr_private,
						PgAioInProgress *aio,
						uintptr_t *read_private)
{
	XLogPrefetcher *prefetcher = (XLogPrefetcher *) pgsr_private;
	XLogReaderState *reader = prefetcher->reader;
	XLogRecPtr replaying_lsn = reader->ReadRecPtr;

	elog(LOG, "XLogPrefetcherNextBlock");
	/*
	 * We keep track of the record and block we're up to between calls with
	 * prefetcher->record and prefetcher->next_block_id.
	 */
	for (;;)
	{
		DecodedXLogRecord *record;

		/* Try to read a new future record, if we don't already have one. */
		if (prefetcher->record == NULL)
		{
			switch (XLogReadAhead(prefetcher->reader, &prefetcher->record))
			{
			case XLREAD_FAIL:
				/*
				 * We can't read any more.  The error will be returned via
				 * XLogNextRecord() after any other records that appear before
				 * it in the decoded record queue.
				 */
				return PGSR_NEXT_AGAIN;
			case XLREAD_WAIT:
				/*
				 * No future data available right now, and XLogReadPage()
				 * decided not to wait, because there are decoded records in
				 * the queue ready to be replayed first.
				 */
				return PGSR_NEXT_AGAIN;
			case XLREAD_SUCCESS:
				/* We have a new record to process. */
				prefetcher->next_block_id = 0;
				break;
			}
		}

		record = prefetcher->record;

		/*
		 * If this is a record that creates a new SMGR relation, we'll avoid
		 * prefetching anything from that rnode until it has been replayed.
		 */
		if (replaying_lsn < record->lsn &&
			record->header.xl_rmid == RM_SMGR_ID &&
			(record->header.xl_info & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE)
		{
			xl_smgr_create *xlrec = (xl_smgr_create *) record->main_data;

			XLogPrefetcherAddFilter(prefetcher, xlrec->rnode, 0, record->lsn);
		}

		/* Scan the block references, starting where we left off last time. */
		while (prefetcher->next_block_id <= record->max_block_id)
		{
			int block_id = prefetcher->next_block_id++;
			DecodedBkpBlock *block = &record->blocks[block_id];
			SMgrRelation reln;
			bool already_valid;

			elog(LOG, "XXXXXXXX LSN = %lX, block = %u", record->lsn, block->blkno);

			if (!block->in_use)
				continue;

			/*
			 * Record the location of the DecodedBkpBlock in the circular WAL
			 * buffer.  This is used for sanity checking in
			 * XLogPrefetcherReadAhead() [RENAME ME], to assert that
			 * prefetching is in sync with replay.
			 */
			block->prefetch_flags = 0;
			*read_private = (uintptr_t) block;
			elog(LOG, "read_private = %p", block);

			/* We don't try to prefetch anything but the main fork for now. */
			if (block->forknum != MAIN_FORKNUM)
			{
				elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (1)");
				block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;
				return PGSR_NEXT_NO_IO;
			}

			/*
			 * If there is a full page image attached, we won't be reading the
			 * page, so normally we'll skip prefetching.  However, if the
			 * underlying filesystem uses larger logical blocks than us, it
			 * might still need to perform a read-before-write some time
			 * later.  Therefore, skip prefetching unless configured to do so.
			 */
			if (block->has_image && !recovery_prefetch_fpw)
			{
				XLogPrefetchIncrement(&SharedStats->skip_fpw);
				elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (2)");
				block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;
				return PGSR_NEXT_NO_IO;
			}

			/*
			 * If this block will initialize a new page then it may be a
			 * relation extension.  Skip prefetching, and add a filter so that
			 * we'll keep doing that until this record is replayed.
			 */
			if (block->flags & BKPBLOCK_WILL_INIT)
			{
				XLogPrefetcherAddFilter(prefetcher, block->rnode, block->blkno,
										record->lsn);
				XLogPrefetchIncrement(&SharedStats->skip_new);
				elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (3)");
				block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;
				return PGSR_NEXT_NO_IO;
			}

			/* Should we skip prefetching this block due to a filter? */
			if (XLogPrefetcherIsFiltered(prefetcher, block->rnode, block->blkno))
			{
				XLogPrefetchIncrement(&SharedStats->skip_new);
				elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (4)");
				block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;
				return PGSR_NEXT_NO_IO;
			}

			/* Fast path for repeated references to the same relation. */
			if (RelFileNodeEquals(block->rnode, prefetcher->last_rnode))
			{
				/* For repeat access to the same block, skip prefetching. */
				if (block->blkno == prefetcher->last_blkno)
				{
					XLogPrefetchIncrement(&SharedStats->skip_seq);
					elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (5)");
					block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;
					return PGSR_NEXT_NO_IO;
				}

				/* We can avoid calling smgropen(). */
				/* XXX this is not cool if it's freed by sinval! */
				reln = prefetcher->last_reln;
			}
			else
			{
				/* Otherwise we have to open it. */
				reln = smgropen(block->rnode, InvalidBackendId);
				prefetcher->last_rnode = block->rnode;
				prefetcher->last_reln = reln;
			}
			prefetcher->last_blkno = block->blkno;

			/* Try to prefetch this block! */
//			elog(LOG, "inspecting prefetch_flags block_id = %d, address %p", block_id, block);
			Assert(block->prefetch_flags == 0);
			block->recent_buffer = ReadBufferAsyncSMgr(reln,
													   RELPERSISTENCE_PERMANENT,
													   block->forknum,
													   block->blkno,
													   RBM_NORMAL,
													   NULL,
													   &already_valid,
													   &aio);
			if (already_valid)
			{
				/*
				 * The block was already cached, no nothing to do.
				 *
				 * XXX Recovery will have to reacquire the pin (or re-read if
				 * unlucky), because we don't hold pins.
				 */
				ReleaseBuffer(block->recent_buffer);

				elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (6)");
				block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;

				return PGSR_NEXT_NO_IO;
			}
			else if (BufferIsValid(block->recent_buffer))
			{
				/*
				 * I/O initiated!
				 */
				XLogPrefetchIncrement(&SharedStats->prefetch);
				//block->prefetch_flags = XLOGPREFETCHER_IO;

				block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;
				block->prefetch_flags |= XLOGPREFETCHER_PINNED;
				elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (7), aio = %u, blkno = %u", pgaio_io_id(aio), block->blkno);
				return PGSR_NEXT_IO;
			}
			else
			{
				/*
				 * Neither cached nor initiated.  The underlying segment file
				 * doesn't exist.
				 *
				 * It might be missing becaused it was unlinked, we crashed,
				 * and now we're replaying WAL.  When recovery reads this
				 * block, it will use the EXTENSION_CREATE_RECOVERY.  We
				 * certainly don't want to do that sort of thing while merely
				 * prefetching, so let's just ignore references to this
				 * relation until this record is replayed, and let recovery
				 * create the dummy file or complain if something is wrong.
				 *
				 * It might be missing because we haven't yet replayed a
				 * record like XLOG_DBASE_CREATE that will create relfilenodes
				 * in bulk, but we're already trying to prefetch blocks from
				 * those files due to references later in the WAL.  In that
				 * case too, we'd just block processing of this relfilenode
				 * until this LSN is replayed, to avoid trouble, and let
				 * recovery complain if something is wrong.
				 */
				XLogPrefetcherAddFilter(prefetcher, block->rnode, 0,
										record->lsn);
				XLogPrefetchIncrement(&SharedStats->skip_new);

				/* XXX This can't actually happen yet, need to adjust ReadBufferAsyncSMgr()! */
				elog(LOG, "XLogPrefetcherNextBlock -> PGSR_NEXT_NO_IO (8)");
				block->prefetch_flags |= XLOGPREFETCHER_MUST_CALL_GET_NEXT;
				return PGSR_NEXT_NO_IO;
			}
		}

		/* Advance to the next record. */
		prefetcher->record = NULL;
	}
	pg_unreachable();
}

/*
 * Expose statistics about recovery prefetching.
 */
Datum
pg_stat_get_prefetch_recovery(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_PREFETCH_RECOVERY_COLS 10
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[PG_STAT_GET_PREFETCH_RECOVERY_COLS];
	bool		nulls[PG_STAT_GET_PREFETCH_RECOVERY_COLS];

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mod required, but it is not allowed in this context")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (pg_atomic_read_u32(&SharedStats->reset_request) != SharedStats->reset_handled)
	{
		/* There's an unhandled reset request, so just show NULLs */
		for (int i = 0; i < PG_STAT_GET_PREFETCH_RECOVERY_COLS; ++i)
			nulls[i] = true;
	}
	else
	{
		for (int i = 0; i < PG_STAT_GET_PREFETCH_RECOVERY_COLS; ++i)
			nulls[i] = false;
	}

	values[0] = TimestampTzGetDatum(pg_atomic_read_u64(&SharedStats->reset_time));
	values[1] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->prefetch));
	values[2] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_hit));
	values[3] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_new));
	values[4] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_fpw));
	values[5] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_seq));
	values[6] = Int32GetDatum(SharedStats->distance);
	values[7] = Int32GetDatum(SharedStats->queue_depth);
	values[8] = Float4GetDatum(SharedStats->avg_distance);
	values[9] = Float4GetDatum(SharedStats->avg_queue_depth);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Don't prefetch any blocks >= 'blockno' from a given 'rnode', until 'lsn'
 * has been replayed.
 */
static inline void
XLogPrefetcherAddFilter(XLogPrefetcher *prefetcher, RelFileNode rnode,
						BlockNumber blockno, XLogRecPtr lsn)
{
	XLogPrefetcherFilter *filter;
	bool		found;

	elog(LOG, "XLogPrefetcherAddFilter rel %u, block %u, lsn = %lx", rnode.relNode, blockno, lsn);
	filter = hash_search(prefetcher->filter_table, &rnode, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * Don't allow any prefetching of this block or higher until replayed.
		 */
		filter->filter_until_replayed = lsn;
		filter->filter_from_block = blockno;
		dlist_push_head(&prefetcher->filter_queue, &filter->link);
	}
	else
	{
		/*
		 * We were already filtering this rnode.  Extend the filter's lifetime
		 * to cover this WAL record, but leave the lower of the block numbers
		 * there because we don't want to have to track individual blocks.
		 */
		filter->filter_until_replayed = lsn;
		dlist_delete(&filter->link);
		dlist_push_head(&prefetcher->filter_queue, &filter->link);
		filter->filter_from_block = Min(filter->filter_from_block, blockno);
	}
}

/*
 * Have we replayed the records that caused us to begin filtering a block
 * range?  That means that relations should have been created, extended or
 * dropped as required, so we can drop relevant filters.
 */
static inline void
XLogPrefetcherCompleteFilters(XLogPrefetcher *prefetcher, XLogRecPtr replaying_lsn)
{
	while (unlikely(!dlist_is_empty(&prefetcher->filter_queue)))
	{
		XLogPrefetcherFilter *filter = dlist_tail_element(XLogPrefetcherFilter,
														  link,
														  &prefetcher->filter_queue);

		if (filter->filter_until_replayed >= replaying_lsn)
			break;
		elog(LOG, "XLogPrefetcherCompleteFilters drop filter rel = %u, block %u, filter_until = %lX because replaying %lX", filter->rnode.relNode, filter->filter_from_block, filter->filter_until_replayed, replaying_lsn);
		dlist_delete(&filter->link);
		hash_search(prefetcher->filter_table, filter, HASH_REMOVE, NULL);
	}
}

/*
 * Check if a given block should be skipped due to a filter.
 */
static inline bool
XLogPrefetcherIsFiltered(XLogPrefetcher *prefetcher, RelFileNode rnode,
						 BlockNumber blockno)
{
	/*
	 * Test for empty queue first, because we expect it to be empty most of
	 * the time and we can avoid the hash table lookup in that case.
	 */
	if (unlikely(!dlist_is_empty(&prefetcher->filter_queue)))
	{
		XLogPrefetcherFilter *filter = hash_search(prefetcher->filter_table, &rnode,
												   HASH_FIND, NULL);

		if (filter && filter->filter_from_block <= blockno) {
			elog(LOG, "XLogPrefetcherIsFiltered rel = %u, block %u <= %u, filtered", rnode.relNode, filter->filter_from_block, blockno);
			return true;
		}
		if (filter)
			elog(LOG, "XLogPrefetcherIsFiltered rel = %u, block %u > %u, not filtered", rnode.relNode, filter->filter_from_block, blockno);
	}

	return false;
}

/*
 * A wrapper for XLogReadRecord() that tries to initiate IO ahead of time.
 * When using a prefetcher, XLogReadRecord() should not be called directly, or
 * the two cursors through the WAL would become desynchronized.
 */
XLogReadRecordResult
XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher,
						 XLogRecord **out_record,
						 char **errmsg)
{
	XLogReadRecordResult result;
	DecodedXLogRecord *record;

	/*
	 * See if it's time to reconfigure the prefetching machinery, becauase a
	 * relevant GUC was changed.
	 */
	if (unlikely(XLogPrefetchReconfigureCount != prefetcher->reconfigure_count))
	{
		pg_streaming_read_free(prefetcher->streaming_read);

		prefetcher->streaming_read =
			pg_streaming_read_alloc(maintenance_io_concurrency,
									(uintptr_t) prefetcher,
									XLogPrefetcherNextBlock,
									XLogPrefetcherReleaseBlock);
		/* XXX als destroy any unconsumed records in wal decoding buffer? */

		prefetcher->reconfigure_count = XLogPrefetchReconfigureCount;
	}
	
	/*
	 * If there's nothing queued yet, then start prefetching.  Normally this
	 * happens automatically when we call pg_streaming_read_get_next() below
	 * to complete earlier IOs, but if we didn't have a special case here we'd
	 * never be able to get started.
	 */
	if (!XLogReaderHasQueuedRecordOrError(prefetcher->reader))
		pg_streaming_read_prefetch(prefetcher->streaming_read);

	/* Read the next record. */
	result = XLogNextRecord(prefetcher->reader, &record, errmsg, true);
	if (result != XLREAD_SUCCESS)
	{
		*out_record = NULL;
		return result;
	}

	/*
	 * The record we just got is the "current" one, for the benefit of the
	 * XLogRecXXX() mascros.
	 */
	Assert(record == prefetcher->reader->record);

	/*
	 * Can we drop any prefetch filters yet, given the record we're about to
	 * return?  This assumes that any records with earlier LSNs have been
	 * replayed, so if we were waiting for a relation to be created or
	 * extended, it is now OK to access blocks in the covered range.
	 */
	XLogPrefetcherCompleteFilters(prefetcher, record->lsn);

	/* Make sure that any IOs initiated due to this record are completed. */
	for (int block_id = 0; block_id <= record->max_block_id; ++block_id)
	{
		uintptr_t block_p PG_USED_FOR_ASSERTS_ONLY;

		if (!record->blocks[block_id].in_use)
			continue;

		/*
		 * If the streaming read has forgotten about this block, when we can't
		 * call pg_streaming_read_get_next().  This happens during release, if
		 * we reconfigure the streaming read's parameters.
		 */
		if ((record->blocks[block_id].prefetch_flags & XLOGPREFETCHER_MUST_CALL_GET_NEXT) == 0)
			continue;

		block_p = pg_streaming_read_get_next(prefetcher->streaming_read);
		
		record->blocks[block_id].prefetch_flags &= ~XLOGPREFETCHER_MUST_CALL_GET_NEXT;

		/*
		 * Assert that we're in sync with XLogPrefetcherNextBlock(), which is
		 * feeding blocks into the far end of the pipe.
		 */
		Assert(block_p == (uintptr_t) &record->blocks[block_id]);

		/*
		 * We could potentially leave it pinned, and teach
		 * XLogReadBufferForRedo() to recognize that case so it doesn't
		 * need to acquire a new pin.  For now, keep it simple.
		 */
		if (record->blocks[block_id].prefetch_flags & XLOGPREFETCHER_PINNED)
		{
			Assert(BufferIsValid(record->blocks[block_id].recent_buffer));
			record->blocks[block_id].prefetch_flags &= ~XLOGPREFETCHER_PINNED;
			ReleaseBuffer(record->blocks[block_id].recent_buffer);
		}
	}

	*out_record = &record->header;

	Assert(*out_record == &prefetcher->reader->record->header);

	return XLREAD_SUCCESS;
}

void
assign_recovery_prefetch(bool new_value, void *extra)
{
	/* Reconfigure prefetching, because a setting it depends on changed. */
	recovery_prefetch = new_value;
	if (AmStartupProcess())
		XLogPrefetchReconfigure();
}

void
assign_recovery_prefetch_fpw(bool new_value, void *extra)
{
	/* Reconfigure prefetching, because a setting it depends on changed. */
	recovery_prefetch_fpw = new_value;
	if (AmStartupProcess())
		XLogPrefetchReconfigure();
}
