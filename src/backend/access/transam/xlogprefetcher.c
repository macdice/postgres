/*-------------------------------------------------------------------------
 *
 * xlogprefetcher.c
 *		Prefetching support for recovery.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *		src/backend/access/transam/xlogprefetcher.c
 *
 * This module provides a drop-in replacement for an XLogReader that tries to
 * minimize I/O stalls by looking up future blocks in the buffer cache, and
 * initiating I/Os that might complete before the caller eventually needs the
 * data.  XLogRecBufferForRedo() cooperates uses information stored in the
 * decoded record to find buffers efficiently.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogprefetcher.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands_xlog.h"
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

/* Every time we process this much WAL, we update stats in shared memory. */
#define XLOGPREFETCHER_SAMPLE_DISTANCE BLCKSZ

/* GUCs */
bool		recovery_prefetch = false;

static int	XLogPrefetchReconfigureCount = 0;

/*
 * A prefetcher.  This is a mechanism that wraps an XLogReader, prefetching
 * blocks that will be soon be referenced, to try to avoid IO stalls.
 */
struct XLogPrefetcher
{
	/* WAL reader and current reading state. */
	XLogReaderState *reader;
	DecodedXLogRecord *record;
	int			next_block_id;

	/* When to next write stats to shared memory. */
	XLogRecPtr	next_stats_lsn;

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
	pg_atomic_uint64 hit;		/* Blocks already in cache. */
	pg_atomic_uint64 skip_init;	/* Zero-inited blocks skipped. */
	pg_atomic_uint64 skip_new;	/* New/missing blocks filtered. */
	pg_atomic_uint64 skip_fpw;	/* FPWs skipped. */

	/* Reset counters */
	pg_atomic_uint32 reset_request;
	uint32		reset_handled;

	/* Dynamic values */
	int			wal_distance;	/* Number of WAL bytes ahead. */
	int			block_distance;	/* Number of block references ahead. */
	int			io_depth;		/* Number of I/Os in progress. */
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

static void
XLogPrefetchResetStats(void)
{
	pg_atomic_write_u64(&SharedStats->reset_time, GetCurrentTimestamp());
	pg_atomic_write_u64(&SharedStats->prefetch, 0);
	pg_atomic_write_u64(&SharedStats->hit, 0);
	pg_atomic_write_u64(&SharedStats->skip_init, 0);
	pg_atomic_write_u64(&SharedStats->skip_new, 0);
	pg_atomic_write_u64(&SharedStats->skip_fpw, 0);
}

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
		pg_atomic_init_u64(&SharedStats->hit, 0);
		pg_atomic_init_u64(&SharedStats->skip_init, 0);
		pg_atomic_init_u64(&SharedStats->skip_new, 0);
		pg_atomic_init_u64(&SharedStats->skip_fpw, 0);
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
		.skip_init = pg_atomic_read_u64(&SharedStats->skip_init),
		.skip_new = pg_atomic_read_u64(&SharedStats->skip_new),
		.skip_fpw = pg_atomic_read_u64(&SharedStats->skip_fpw),
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
		pg_atomic_write_u64(&SharedStats->hit, serialized->hit);
		pg_atomic_write_u64(&SharedStats->skip_init, serialized->skip_init);
		pg_atomic_write_u64(&SharedStats->skip_new, serialized->skip_new);
		pg_atomic_write_u64(&SharedStats->skip_fpw, serialized->skip_fpw);
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

static void
XLogPrefetcherReleaseBlock(uintptr_t pgsr_private, uintptr_t read_private)
{
	DecodedBkpBlock *block = (DecodedBkpBlock *) read_private;

	if (block)
	{
		if (block->prefetch_buffer_pinned)
		{
			Assert(BufferIsValid(block->prefetch_buffer));
			ReleaseBuffer(block->prefetch_buffer);
			block->prefetch_buffer_pinned = false;
		}
		block->prefetch_get_next = false;
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

	SharedStats->wal_distance = 0;
	SharedStats->block_distance = 0;
	SharedStats->io_depth = 0;

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
	hash_destroy(prefetcher->filter_table);
	pfree(prefetcher);
}

static void
XLogPrefetcherComputeStats(XLogPrefetcher *prefetcher)
{
	uint32 io_depth;
	uint32 completed;
	uint32 reset_request;
	int64 wal_distance;


	/* How far ahead of replay are we now? */
	if (prefetcher->record)
		wal_distance = prefetcher->record->lsn - prefetcher->reader->record->lsn;
	else
		wal_distance = 0;

	/* How many IOs are currently in flight and completed? */
	io_depth = pg_streaming_read_inflight(prefetcher->streaming_read);
	completed = pg_streaming_read_completed(prefetcher->streaming_read);

	/* Update the instantaneous stats visible in pg_stat_prefetch_recovery. */
	SharedStats->io_depth = io_depth;
	SharedStats->block_distance = io_depth + completed;
	SharedStats->wal_distance = wal_distance;

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
	}

	prefetcher->next_stats_lsn =
		prefetcher->reader->record->lsn + XLOGPREFETCHER_SAMPLE_DISTANCE;
}

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
			int nonblocking;

			/*
			 * If there are already records or an error queued up that could
			 * be replayed, we don't want to block here.  Otherwise, it's OK
			 * to block waiting for more data: presumably the caller has
			 * nothing else to do.
			 */
			nonblocking = XLogReaderHasQueuedRecordOrError(reader);

			record = XLogReadAhead(prefetcher->reader, nonblocking);
			if (record == NULL)
			{
				/*
				 * We can't read any more, due to an error or lack of data in
				 * nonblocking mode.
				 */
				return PGSR_NEXT_AGAIN;
			}

			/* We have a new record to process. */
			prefetcher->record = record;
			prefetcher->next_block_id = 0;
		}
		else
		{
			/* Continue to process from last call, or last loop. */
			record = prefetcher->record;
		}

		/*
		 * Check for operations that change the identity of buffer tags.
		 * These must be treated as barriers that prevent prefetching for
		 * certain ranges of buffer tags, so that we can't be confused by OID
		 * wraparound, and can't hold pins on buffers that need to be dropped.
		 *
		 * XXX Perhaps this information could be derived automatically if we
		 * had some standardized header flags and fields for these fields,
		 * instead of special logic.
		 *
		 * XXX Are there other operations that need this treatment?
		 */
		if (replaying_lsn < record->lsn)
		{
			uint8 rmid = record->header.xl_rmid;
			uint8 record_type = record->header.xl_info & ~XLR_INFO_MASK;
			
			if (rmid == RM_DBASE_ID)
			{
				if (record_type == XLOG_DBASE_CREATE)
				{
					xl_dbase_create_rec *xlrec = (xl_dbase_create_rec *)
						record->main_data;
					RelFileNode rnode = {InvalidOid, xlrec->db_id, InvalidOid};
					
					/*
					 * Don't try to prefetch anything in this database until
					 * it has been created, or we might confuse blocks on OID
					 * wraparound.  (We could use XLOG_DBASE_DROP instead, but
					 * there shouldn't be any reference to blocks in a
					 * database between DROP and CREATE for the same OID, and
					 * doing it on CREATE avoids the more expensive
					 * ENOENT-handling if we didn't treat CREATE as a
					 * barrier).
					 */					
					XLogPrefetcherAddFilter(prefetcher, rnode, 0, record->lsn);
				}
			}
			else if (rmid == RM_SMGR_ID)
			{
				if (record_type == XLOG_SMGR_CREATE)
				{
					xl_smgr_create *xlrec = (xl_smgr_create *)
						record->main_data;

					/*
					 * Don't prefetch anything for this whole relation until
					 * it has been created, or we might confuse blocks on OID
					 * wraparound.
					 */
					XLogPrefetcherAddFilter(prefetcher, xlrec->rnode, 0,
											record->lsn);
				}
				else if (record_type == XLOG_SMGR_TRUNCATE)
				{
					xl_smgr_truncate *xlrec = (xl_smgr_truncate *)
						record->main_data;

					/*
					 * Don't prefetch anything in the truncated range until
					 * the truncation has been performed, or we might try to
					 * pin a buffer that is about to be truncated (becuase
					 * it's referenced again later), causing the truncation to
					 * fail.
					 */
					XLogPrefetcherAddFilter(prefetcher, xlrec->rnode,
											xlrec->blkno,
											record->lsn);
				}
			}
		}

		/* Scan the block references, starting where we left off last time. */
		while (prefetcher->next_block_id <= record->max_block_id)
		{
			int block_id = prefetcher->next_block_id++;
			DecodedBkpBlock *block = &record->blocks[block_id];
			SMgrRelation reln;
			bool already_valid;

			if (!block->in_use)
				continue;

			Assert(!BufferIsValid(block->prefetch_buffer));;
			Assert(!block->prefetch_buffer_pinned);
			Assert(!block->prefetch_get_next);

			/*
			 * Record the location of the DecodedBkpBlock in the circular WAL
			 * buffer.  This is used for sanity checking in
			 * XLogPrefetcherDe() [RENAME ME], to assert that
			 * prefetching is in sync with replay.
			 */
			*read_private = (uintptr_t) block;

			/* We don't try to prefetch anything but the main fork for now. */
			if (block->forknum != MAIN_FORKNUM)
			{
				block->prefetch_get_next = true;
				return PGSR_NEXT_NO_IO;
			}

			/*
			 * If there is a full page image attached, we won't be reading the
			 * page, so don't both trying to prefetch.
			 */
			if (block->has_image)
			{
				XLogPrefetchIncrement(&SharedStats->skip_fpw);
				block->prefetch_get_next = true;
				return PGSR_NEXT_NO_IO;
			}

			/* There is no point in reading a page that will be zeroed. */
			if (block->flags & BKPBLOCK_WILL_INIT)
			{
				XLogPrefetchIncrement(&SharedStats->skip_init);
				block->prefetch_get_next = true;
				return PGSR_NEXT_NO_IO;
			}

			/* Should we skip prefetching this block due to a filter? */
			if (XLogPrefetcherIsFiltered(prefetcher, block->rnode, block->blkno))
			{
				XLogPrefetchIncrement(&SharedStats->skip_new);
				block->prefetch_get_next = true;
				return PGSR_NEXT_NO_IO;
			}

			/*
			 * We could try to have a fast path for repeated references to the
			 * same relation (with some scheme to handle invalidations
			 * safely), but for now we'll call smgropen() every time.
			 */
			reln = smgropen(block->rnode, InvalidBackendId);

			/*
			 * If the block is past the end of the relation, filter out
			 * further accesses until this record is replayed.
			 */
			if (block->blkno >= smgrnblocks(reln, block->forknum))
			{
				XLogPrefetcherAddFilter(prefetcher, block->rnode, block->blkno,
										record->lsn);
				XLogPrefetchIncrement(&SharedStats->skip_new);
				block->prefetch_get_next = true;
				return PGSR_NEXT_NO_IO;
			}

			block->prefetch_buffer = ReadBufferAsyncSMgr(reln,
														 RELPERSISTENCE_PERMANENT,
														 block->forknum,
														 block->blkno,
														 RBM_NORMAL,
														 NULL,
														 &already_valid,
														 &aio);
			if (already_valid)
			{
				/* Cache hit, nothing to do. */
				XLogPrefetchIncrement(&SharedStats->hit);
				block->prefetch_buffer_pinned = true;
				block->prefetch_get_next = true;
				return PGSR_NEXT_NO_IO;
			}
			else if (BufferIsValid(block->prefetch_buffer))
			{
				/* Cache miss, I/O started. */
				XLogPrefetchIncrement(&SharedStats->prefetch);
				block->prefetch_buffer_pinned = true;
				block->prefetch_get_next = true;
				return PGSR_NEXT_IO;
			}
			else
			{
				/*
				 * Neither cached nor initiated.  The underlying segment file
				 * doesn't exist. (ENOENT)
				 *
				 * logXXX need to adjust ReadBufferAsyncSMgr() to produce this case!
				 *
				 * It might be missing becaused it was unlinked, we crashed,
				 * and now we're replaying WAL.  When recovery reads this
				 * block, it will use the EXTENSION_CREATE_RECOVERY, so by
				 * waiting until this

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
				block->prefetch_get_next = true;
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
	values[2] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->hit));
	values[3] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_init));
	values[4] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_new));
	values[5] = Int64GetDatum(pg_atomic_read_u64(&SharedStats->skip_fpw));
	values[6] = Int32GetDatum(SharedStats->wal_distance);
	values[7] = Int32GetDatum(SharedStats->block_distance);
	values[8] = Int32GetDatum(SharedStats->io_depth);
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
 * Have we replayed any records that caused us to begin filtering a block
 * range?  That means that relations should have been created, extended or
 * dropped as required, so we can stop filtering out accesses to a given
 * relfilenode.
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
		XLogPrefetcherFilter *filter;

		/* See if the block range is filtered. */
		filter = hash_search(prefetcher->filter_table, &rnode, HASH_FIND, NULL);
		if (filter && filter->filter_from_block <= blockno)
			return true;

		/* See if the whole database is filtered. */
		rnode.relNode = InvalidOid;
		filter = hash_search(prefetcher->filter_table, &rnode, HASH_FIND, NULL);
		if (filter && filter->filter_from_block <= blockno)
			return true;
	}

	return false;
}

/*
 * A wrapper for XLogBeginRead() that also resets the prefetcher.
 */
void
XLogPrefetcherBeginRead(XLogPrefetcher *prefetcher,
						XLogRecPtr recPtr)
{
	/*
	 * Recreate streaming_read, so that all IO resources and pins are
	 * released.
	 */
	prefetcher->record = NULL;
	pg_streaming_read_free(prefetcher->streaming_read);
	prefetcher->streaming_read =
		pg_streaming_read_alloc(maintenance_io_concurrency,
								(uintptr_t) prefetcher,
								XLogPrefetcherNextBlock,
								XLogPrefetcherReleaseBlock);

	/* This will forget about any queued up records in the decoder. */
	XLogBeginRead(prefetcher->reader, recPtr);
}

/*
 * A wrapper for XLogReadRecord() that provides the same interface, but also
 * tries to initiate IO ahead of time.
 */
XLogRecord *
XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher, char **errmsg)
{
	DecodedXLogRecord *record;

#ifdef USE_ASSERT_CHECKING
	if (prefetcher->reader->record)
	{
		DecodedXLogRecord *last_record = prefetcher->reader->record;

		for (int i = 0; i < last_record->max_block_id; ++i)
		{
			if (last_record->blocks[i].in_use &&
				last_record->blocks[i].prefetch_buffer_pinned)
				elog(ERROR,
					 "redo routine did not read buffer pinned by prefetcher");
		}
	}
#endif

	/*
	 * See if it's time to reset the prefetching machinery, because a
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

		prefetcher->reconfigure_count = XLogPrefetchReconfigureCount;
	}

	/*
	 * If there's nothing queued yet, then start prefetching.  Normally this
	 * happens automatically when we call pg_streaming_read_get_next() below
	 * to complete earlier IOs, but if we didn't have a special case for an
	 * empty queue we'd never be able to get started.
	 */
	if (!XLogReaderHasQueuedRecordOrError(prefetcher->reader))
		pg_streaming_read_prefetch(prefetcher->streaming_read);

	/* Read the next record. */
	record = XLogNextRecord(prefetcher->reader, errmsg);
	if (!record)
		return NULL;

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

	/*
	 * See if it's time to compute some statistics, because enough WAL has
	 * been processed.
	 */
	if (unlikely(record->lsn >= prefetcher->next_stats_lsn))
		XLogPrefetcherComputeStats(prefetcher);

	/*
	 * Make sure that any IOs we initiated earlier for this record are
	 * completed.
	 */
	for (int block_id = 0; block_id <= record->max_block_id; ++block_id)
	{
		uintptr_t block_p PG_USED_FOR_ASSERTS_ONLY;

		if (!record->blocks[block_id].in_use)
			continue;

		/*
		 * When streaming_read is freed due to a relevant GUC change or being
		 * repositioned with XLogPrefetcherBeginRead(), it clears this flag so
		 * that we know that we don't need to call
		 * pg_streaming_read_get_next().
		 */
		if (!record->blocks[block_id].prefetch_get_next)
			continue;

		/* Otherwise we have to call it to stay in sync. */
		block_p = pg_streaming_read_get_next(prefetcher->streaming_read);
		record->blocks[block_id].prefetch_get_next = false;

		/*
		 * Assert that we're in sync with XLogPrefetcherNextBlock(), which is
		 * feeding blocks into the far end of the pipe.
		 */
		Assert(block_p == (uintptr_t) &record->blocks[block_id]);
	}

	Assert(record == prefetcher->reader->record);

	return &record->header;
}

void
assign_recovery_prefetch(bool new_value, void *extra)
{
	/* Reconfigure prefetching, because a setting it depends on changed. */
	recovery_prefetch = new_value;
	if (AmStartupProcess())
		XLogPrefetchReconfigure();
}
