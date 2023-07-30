/*
 * XXX This is a quick and dirty hack to stand in for Andres's real
 * PGStreamingRead interface while I plumb some other stuff... -TM
 */

#ifndef STREAMING_READ_H
#define STREAMING_READ_H

#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/fd.h"

typedef enum PgStreamingReadNextStatus
{
	PGSR_NEXT_END,
	PGSR_NEXT_AGAIN,
	PGSR_NEXT_NO_IO,
	PGSR_NEXT_IO
}			PgStreamingReadNextStatus;

struct PgStreamingRead;

typedef PgStreamingReadNextStatus(*PgStreamingReadBufferDetermineNextCB) (
																		  struct PgStreamingRead *pgsr,
																		  uintptr_t pgsr_private,
																		  BufferTag *tag,
																		  ReadBufferMode *mode);

/*
 * We should be able to fit 6 recently used RelFileLocators in a cache
 * line for fast linear search.
 */
#define PG_STREAMING_READ_SMGR_CACHE_SIZE (64 / sizeof(RelFileLocator))

typedef struct PgStreamingRead
{
	int			max_ios_in_progress;
	int			ios_in_progress;
	int			ios_in_progress_trigger;
	int			max_pinned_buffers;
	int			pinned_buffers;
	int			pinned_buffers_trigger;
	int			next_tail_buffer;
	int			ops_size;
	int			ops_head;		/* newest op goes here */
	int			ops_tail;		/* oldest op here */
	bool		finished;
	uintptr_t	user_data;
	PgStreamingReadBufferDetermineNextCB next_cb;
	BufferAccessStrategy strategy;

	/* Range we are collecting for StartReadBuffers(). */
	BufferTag	range_tag;
	int			range_nblocks;
	BufferTag	sequential_tag;

	/* Cache of recently accessed SMgrRelations. */
	RelFileLocator cache_locator[PG_STREAMING_READ_SMGR_CACHE_SIZE];
	SMgrRelation  cache_smgr[PG_STREAMING_READ_SMGR_CACHE_SIZE];
	int			cache_second_chance;
	int			cache_hand;

	AsyncBufferOp ops[FLEXIBLE_ARRAY_MEMBER];
}			PgStreamingRead;


static inline PgStreamingRead *
pg_streaming_read_buffer_alloc(uint32 iodepth,
							   uintptr_t user_data,
							   BufferAccessStrategy strategy,
							   PgStreamingReadBufferDetermineNextCB determine_next_cb)
{
	PgStreamingRead *sr;
	int			ops_size;

	/*
	 * One extra 'ops' element, because it's a circular buffer and head==tail
	 * means empty.
	 */
	ops_size = iodepth * 2 + 1;

	sr = (PgStreamingRead *)
		palloc0(offsetof(PgStreamingRead, ops) +
				sizeof(AsyncBufferOp) * ops_size);

	sr->max_ios_in_progress = iodepth;
	sr->ios_in_progress_trigger = iodepth - (iodepth / 4);
	sr->max_pinned_buffers = iodepth * 2;
	sr->pinned_buffers_trigger =
		Max(iodepth, sr->max_pinned_buffers - MAX_BUFFERS_PER_TRANSFER);
	sr->pinned_buffers = 0;
	sr->ios_in_progress = 0;
	sr->ops_size = ops_size;
	sr->next_tail_buffer = 0;
	sr->ops_head = 0;
	sr->ops_tail = 0;
	sr->finished = false;
	sr->user_data = user_data;
	sr->strategy = strategy;
	sr->next_cb = determine_next_cb;
	sr->range_nblocks = 0;

	return sr;
}

static inline SMgrRelation
pg_streaming_read_buffer_get_smgr(PgStreamingRead *sr, const BufferTag *tag)
{
	RelFileLocator locator = {
		.spcOid = tag->spcOid,
		.dbOid = tag->dbOid,
		.relNumber = tag->relNumber
	};
	SMgrRelation smgr;
	int slot = -1;

	/*
	 * Is it in our recently-used SMgrRelation cached?  This is intended to be
	 * a search of about a cache line of data that might have a fast SIMD
	 * implementation.
	 */
	for (int i = 0; i < lengthof(sr->cache_locator); ++i)
	{
		if (RelFileLocatorEquals(sr->cache_locator[i], locator))
		{
			slot = i;
			break;
		}
	}

	/* If we found it, make it less likely to be evicted, and return it. */
	if (likely(slot >= 0 && sr->cache_smgr[slot]))
	{
		sr->cache_second_chance |= 1 << slot;
		return sr->cache_smgr[slot];
	}

	/*
	 * Find a slot that hasn't recently had its second_chance flag set,
	 * clearing them as we go (cf. Multics CLOCK).
	 */
	for (;;)
	{
		slot = sr->cache_hand++ % lengthof(sr->cache_locator);
		if ((sr->cache_second_chance & (1 << slot)) == 0)
			break;
		sr->cache_second_chance &= ~(1 << slot);
	}

	/* Evict current inhabitant of slot, if there is one. */
	if (sr->cache_smgr[slot])
	{
		smgrclearowner(&sr->cache_smgr[slot], sr->cache_smgr[slot]);
		sr->cache_smgr[slot] = NULL;
	}

	/*
	 * Open and remember the new entry.  It doesn't get a second chance yet,
	 * it'll have to earn it by being accessed again.
	 */
	smgr = smgropen(locator, InvalidBackendId);
	sr->cache_smgr[slot] = smgr;
	sr->cache_locator[slot] = locator;

	return smgr;
}

static inline void
pg_streaming_read_buffer_start_range(PgStreamingRead * sr)
{
	bool no_prefetch_hint;

	/*
	 * Issuing prefetch hints for strictly sequential access seems to perform
	 * worse than letting the kernel's readahead mechanism do it, at least on
	 * Linux.  Also disabled by direct I/O.
	 */
	no_prefetch_hint =
		(io_direct_flags & IO_DIRECT_DATA) ||
		BufferTagsEqual(&sr->range_tag, &sr->sequential_tag);
	StartReadBuffers(pg_streaming_read_buffer_get_smgr(sr, &sr->range_tag),
					 'p',	/* XXX */
					 sr->range_tag.forkNum,
					 sr->range_tag.blockNum,
					 sr->range_nblocks,
					 sr->strategy,
					 no_prefetch_hint,
					 &sr->ops[sr->ops_head]);
	sr->ops_head = (sr->ops_head + 1) % sr->ops_size;
	sr->pinned_buffers += sr->range_nblocks;
	sr->ios_in_progress++;

	/* Remember where the next block would be, for next time. */
	sr->sequential_tag = sr->range_tag;
	sr->sequential_tag.blockNum += sr->range_nblocks;

	/* Range is now empty, so we can start building a new one. */
	sr->range_nblocks = 0;
}

static inline void
pg_streaming_read_buffer_prefetch(PgStreamingRead * sr)
{
	/*
	 * Our trigger level for prefetching set low enough that we have a chance
	 * of building up our maximum iov count, but other than that we prefetch as
	 * much as possible.  A better implementation wouldn't be so aggressive,
	 * and should ramp up.
	 */
	while (!sr->finished &&
		   sr->pinned_buffers < sr->pinned_buffers_trigger)
	{
		PgStreamingReadNextStatus status;
		BufferTag	tag;
		ReadBufferMode mode;

		status = sr->next_cb(sr, sr->user_data, &tag, &mode);

		/* Callback says no new blocks can be fetched right now? */
		if (status == PGSR_NEXT_AGAIN)
			break;
		/* Callback says data has run out? */
		if (status == PGSR_NEXT_END)
		{
			sr->finished = true;
			if (sr->range_nblocks > 0)
				pg_streaming_read_buffer_start_range(sr);
			break;
		}
		/* Callback says it wants to read a buffer?  */
		if (status == PGSR_NEXT_IO)
		{
			/*
			 * There must be more space here or pinned_buffers would be greater
			 * than pin_buffers_trigger.
			 */
			Assert((sr->ops_head + 1) % sr->ops_size != sr->ops_tail);

			if (sr->range_nblocks > 0 &&
				sr->range_nblocks < MAX_BUFFERS_PER_TRANSFER &&
				tag.spcOid == sr->range_tag.spcOid &&
				tag.dbOid == sr->range_tag.dbOid &&
				tag.relNumber == sr->range_tag.relNumber &&
				tag.forkNum == sr->range_tag.forkNum &&
				tag.blockNum == sr->range_tag.blockNum + sr->range_nblocks)
			{
				/* Extend the existing range that we are building. */
				sr->range_nblocks++;
			}
			else
			{
				/*
				 * If we were building a range, it's time to start that one
				 * to get it out of the way, either because it's big enough
				 * or the next block is not consecutive.
				 */
				if (sr->range_nblocks > 0)
					pg_streaming_read_buffer_start_range(sr);

				/* Start building a new range. */
				sr->range_tag = tag;
				sr->range_nblocks = 1;
			}
		}
	}
}

static inline Buffer
pg_streaming_read_buffer_get_next(PgStreamingRead * sr)
{
	pg_streaming_read_buffer_prefetch(sr);

	/* See if we have one buffer to return. */
	for (;;)
	{
		while (sr->ops_head != sr->ops_tail)
		{
			/* Are there more buffers available in the tail ops element? */
			if (sr->next_tail_buffer < sr->ops[sr->ops_tail].nblocks)
			{
				if (sr->next_tail_buffer == 0)
				{
					/*
					 * We need to complete this read before the buffers it
					 * owns are ready to be returned.
					 */
					CompleteReadBuffers(&sr->ops[sr->ops_tail]);
					sr->ios_in_progress--;
				}
				/* We are giving away ownership of this pinned buffer. */
				sr->pinned_buffers--;
				return sr->ops[sr->ops_tail].buffers[sr->next_tail_buffer++];
			}

			/* Advance tail to next op, if there is one. */
			sr->ops_tail = (sr->ops_tail + 1) % sr->ops_size;
			sr->next_tail_buffer = 0;
		}

		if (sr->range_nblocks > 0)
		{
			/*
			 * Nothing left in sr->ops, but we can force the current range to
			 * be started to create an entry.
			 */
			pg_streaming_read_buffer_start_range(sr);
		}
		else
			break;
	}

	return InvalidBuffer;
}

static inline void
pg_streaming_read_free(PgStreamingRead * sr)
{
	Buffer buffer;

	/* Stop reading ahead, and unpin anything that wasn't consumed. */
	sr->finished = true;
	for (;;)
	{
		buffer = pg_streaming_read_buffer_get_next(sr);
		if (buffer == InvalidBuffer)
			break;
		ReleaseBuffer(buffer);
	}

	/* Clear our cache of SMgrObjects. */

	/*
	 * XXX This is only going to work if the PgStreamingRead has some kidn of
	 * automatic cleanup!  These would be dangling pointers on non-local exit
	 * of unregistered PgStreamingRead.
	 */
	for (int i = 0; i < lengthof(sr->cache_smgr); ++i)
		if (sr->cache_smgr[i])
			smgrclearowner(&sr->cache_smgr[i], sr->cache_smgr[i]);

	pfree(sr);
}

static inline int
pg_streaming_read_inflight(PgStreamingRead *sr)
{
	return sr->ios_in_progress;
}

static inline int
pg_streaming_read_completed(PgStreamingRead *sr)
{
	/*
	 * XXX this isn't quite right, because ios_in_progress is merged I/Os, but
	 * it'll do for now!
	 */
	return sr->pinned_buffers - sr->ios_in_progress;
}

#endif							/* STREAMING_READ_H */
