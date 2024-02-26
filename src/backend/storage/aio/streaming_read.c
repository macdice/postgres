#include "postgres.h"

#include "storage/streaming_read.h"
#include "utils/rel.h"

/*
 * Element type for PgStreamingRead's circular array of block ranges.
 */
typedef struct PgStreamingReadRange
{
	bool		need_wait;
	bool		advice_issued;
	BlockNumber blocknum;
	int			nblocks;
	int			per_buffer_data_index;
	Buffer		buffers[MAX_BUFFERS_PER_TRANSFER];
	ReadBuffersOperation operation;
} PgStreamingReadRange;

/*
 * Streaming read object.
 */
struct PgStreamingRead
{
	int			max_ios;
	int			ios_in_progress;
	int			max_pinned_buffers;
	int			pinned_buffers;
	int			pinned_buffers_trigger;
	int			next_tail_buffer;
	int			ramp_up_pin_limit;
	int			ramp_up_pin_stall;
	bool		finished;
	bool		advice_enabled;
	void	   *pgsr_private;
	PgStreamingReadBufferCB callback;

	BufferAccessStrategy strategy;
	BufferManagerRelation bmr;
	ForkNumber	forknum;

	/* Sometimes we need to buffer one block for flow control. */
	BlockNumber unget_blocknum;
	void	   *unget_per_buffer_data;

	/* Next expected block, for detecting sequential access. */
	BlockNumber seq_blocknum;

	/* Space for optional per-buffer private data. */
	size_t		per_buffer_data_size;
	void	   *per_buffer_data;

	/* Circular buffer of ranges. */
	int			size;
	int			head;
	int			tail;
	PgStreamingReadRange ranges[FLEXIBLE_ARRAY_MEMBER];
};

static PgStreamingRead *
pg_streaming_read_buffer_alloc_internal(int flags,
										void *pgsr_private,
										size_t per_buffer_data_size,
										BufferAccessStrategy strategy)
{
	PgStreamingRead *pgsr;
	int			size;
	int			max_ios;
	uint32		max_pinned_buffers;


	/*
	 * Decide how many assumed I/Os we will allow to run concurrently.  That
	 * is, advice to the kernel to tell it that we will soon read.  This
	 * number also affects how far we look ahead for opportunities to start
	 * more I/Os.
	 */
	if (flags & PGSR_FLAG_MAINTENANCE)
		max_ios = maintenance_io_concurrency;
	else
		max_ios = effective_io_concurrency;

	/*
	 * The desired level of I/O concurrency controls how far ahead we are
	 * willing to look ahead.  We also clamp it to at least
	 * MAX_BUFFER_PER_TRANFER so that we can have a chance to build up a full
	 * sized read, even when max_ios is zero.
	 */
	max_pinned_buffers = Max(max_ios * 4, MAX_BUFFERS_PER_TRANSFER);

	/*
	 * The *_io_concurrency GUCs might be set to 0, but we want to allow at
	 * least one, to keep our gating logic simple.
	 */
	max_ios = Max(max_ios, 1);

	/*
	 * Don't allow this backend to pin too many buffers.  For now we'll apply
	 * the limit for the shared buffer pool and the local buffer pool, without
	 * worrying which it is.
	 */
	LimitAdditionalPins(&max_pinned_buffers);
	LimitAdditionalLocalPins(&max_pinned_buffers);
	Assert(max_pinned_buffers > 0);

	/*
	 * pgsr->ranges is a circular buffer.  When it is empty, head == tail.
	 * When it is full, there is an empty element between head and tail.  Head
	 * can also be empty (nblocks == 0), therefore we need two extra elements
	 * for non-occupied ranges, on top of max_pinned_buffers to allow for the
	 * maxmimum possible number of occupied ranges of the smallest possible
	 * size of one.
	 */
	size = max_pinned_buffers + 2;

	pgsr = (PgStreamingRead *)
		palloc0(offsetof(PgStreamingRead, ranges) +
				sizeof(pgsr->ranges[0]) * size);

	pgsr->max_ios = max_ios;
	pgsr->per_buffer_data_size = per_buffer_data_size;
	pgsr->max_pinned_buffers = max_pinned_buffers;
	pgsr->pgsr_private = pgsr_private;
	pgsr->strategy = strategy;
	pgsr->size = size;

	pgsr->unget_blocknum = InvalidBlockNumber;

#ifdef USE_PREFETCH

	/*
	 * This system supports prefetching advice.  As long as direct I/O isn't
	 * enabled, and the caller hasn't promised sequential access, we can use
	 * it.
	 */
	if ((io_direct_flags & IO_DIRECT_DATA) == 0 &&
		(flags & PGSR_FLAG_SEQUENTIAL) == 0)
		pgsr->advice_enabled = true;
#endif

	/*
	 * We start off building small ranges, but double that quickly, for the
	 * benefit of users that don't know how far ahead they'll read.  This can
	 * be disabled by users that already know they'll read all the way.
	 */
	if (flags & PGSR_FLAG_FULL)
		pgsr->ramp_up_pin_limit = INT_MAX;
	else
		pgsr->ramp_up_pin_limit = 1;

	/*
	 * We want to avoid creating ranges that are smaller than they could be
	 * just because we hit max_pinned_buffers.  We only look ahead when the
	 * number of pinned buffers falls below this trigger number, or put
	 * another way, we stop looking ahead when we wouldn't be able to build a
	 * "full sized" range.
	 */
	pgsr->pinned_buffers_trigger =
		Max(1, (int) max_pinned_buffers - MAX_BUFFERS_PER_TRANSFER);

	/* Space for the callback to store extra data along with each block. */
	if (per_buffer_data_size)
		pgsr->per_buffer_data = palloc(per_buffer_data_size * max_pinned_buffers);

	return pgsr;
}

/*
 * Create a new streaming read object that can be used to perform the
 * equivalent of a series of ReadBuffer() calls for one fork of one relation.
 * Internally, it generates larger vectored reads where possible by looking
 * ahead.
 */
PgStreamingRead *
pg_streaming_read_buffer_alloc(int flags,
							   void *pgsr_private,
							   size_t per_buffer_data_size,
							   BufferAccessStrategy strategy,
							   BufferManagerRelation bmr,
							   ForkNumber forknum,
							   PgStreamingReadBufferCB next_block_cb)
{
	PgStreamingRead *result;

	result = pg_streaming_read_buffer_alloc_internal(flags,
													 pgsr_private,
													 per_buffer_data_size,
													 strategy);
	result->callback = next_block_cb;
	result->bmr = bmr;
	result->forknum = forknum;

	return result;
}

/*
 * Find the per-buffer data index for the Nth block of a range.
 */
static int
get_per_buffer_data_index(PgStreamingRead *pgsr, PgStreamingReadRange *range, int n)
{
	int			result;

	/*
	 * Find slot in the circular buffer of per-buffer data, without using the
	 * expensive % operator.
	 */
	result = range->per_buffer_data_index + n;
	if (result >= pgsr->max_pinned_buffers)
		result -= pgsr->max_pinned_buffers;
	Assert(result == (range->per_buffer_data_index + n) % pgsr->max_pinned_buffers);

	return result;
}

/*
 * Return a pointer to the per-buffer data by index.
 */
static void *
get_per_buffer_data_by_index(PgStreamingRead *pgsr, int per_buffer_data_index)
{
	return (char *) pgsr->per_buffer_data +
		pgsr->per_buffer_data_size * per_buffer_data_index;
}

/*
 * Return a pointer to the per-buffer data for the Nth block of a range.
 */
static void *
get_per_buffer_data(PgStreamingRead *pgsr, PgStreamingReadRange *range, int n)
{
	return get_per_buffer_data_by_index(pgsr,
										get_per_buffer_data_index(pgsr,
																  range,
																  n));
}

/*
 * Start reading the head range, and create a new head range.  The new head
 * range is returned.  It may not be empty, if StartReadBuffers() couldn't
 * start the entire range; in that case the returned range contains the
 * remaining portion of the range.
 */
static PgStreamingReadRange *
pg_streaming_read_start_head_range(PgStreamingRead *pgsr)
{
	PgStreamingReadRange *head_range;
	PgStreamingReadRange *new_head_range;
	int			nblocks_pinned;
	int			flags;

	/* Caller should make sure we never exceed max_ios. */
	Assert(pgsr->ios_in_progress < pgsr->max_ios);

	/* Should only call if the head range has some blocks to read. */
	head_range = &pgsr->ranges[pgsr->head];
	Assert(head_range->nblocks > 0);

	/*
	 * If advice hasn't been suppressed, and this system supports it, this
	 * isn't a strictly sequential pattern, then we'll issue advice.
	 */
	if (pgsr->advice_enabled && head_range->blocknum != pgsr->seq_blocknum)
		flags = READ_BUFFERS_ISSUE_ADVICE;
	else
		flags = 0;


	/* Start reading as many blocks as we can from the head range. */
	nblocks_pinned = head_range->nblocks;
	head_range->need_wait =
		StartReadBuffers(pgsr->bmr,
						 head_range->buffers,
						 pgsr->forknum,
						 head_range->blocknum,
						 &nblocks_pinned,
						 pgsr->strategy,
						 flags,
						 &head_range->operation);

	/* Did that start an I/O? */
	if (head_range->need_wait && (flags & READ_BUFFERS_ISSUE_ADVICE))
	{
		head_range->advice_issued = true;
		pgsr->ios_in_progress++;
		Assert(pgsr->ios_in_progress <= pgsr->max_ios);
	}

	/*
	 * StartReadBuffers() might have pinned fewer blocks than we asked it to,
	 * but always at least one.
	 */
	Assert(nblocks_pinned <= head_range->nblocks);
	Assert(nblocks_pinned >= 1);
	pgsr->pinned_buffers += nblocks_pinned;

	/*
	 * Remember where the next block would be after that, so we can detect
	 * sequential access next time.
	 */
	pgsr->seq_blocknum = head_range->blocknum + nblocks_pinned;

	/*
	 * Create a new head range.  There must be space, because we have enough
	 * elements for every range to hold just one block, up to the pin limit.
	 */
	Assert(pgsr->size > pgsr->max_pinned_buffers);
	Assert((pgsr->head + 1) % pgsr->size != pgsr->tail);
	if (++pgsr->head == pgsr->size)
		pgsr->head = 0;
	new_head_range = &pgsr->ranges[pgsr->head];
	new_head_range->nblocks = 0;
	new_head_range->advice_issued = false;

	/*
	 * If we didn't manage to start the whole read above, we split the range,
	 * moving the remainder into the new head range.
	 */
	if (nblocks_pinned < head_range->nblocks)
	{
		int			nblocks_remaining = head_range->nblocks - nblocks_pinned;

		head_range->nblocks = nblocks_pinned;

		new_head_range->blocknum = head_range->blocknum + nblocks_pinned;
		new_head_range->nblocks = nblocks_remaining;
	}

	/* The new range has per-buffer data starting after the previous range. */
	new_head_range->per_buffer_data_index =
		get_per_buffer_data_index(pgsr, head_range, nblocks_pinned);

	return new_head_range;
}

/*
 * Ask the callback which block it would like us to read next, with a small
 * buffer in front to allow pg_streaming_unget_block() to work.
 */
static BlockNumber
pg_streaming_get_block(PgStreamingRead *pgsr, void *per_buffer_data)
{
	BlockNumber result;

	if (unlikely(pgsr->unget_blocknum != InvalidBlockNumber))
	{
		/*
		 * If we had to unget a block, now it is time to return that one
		 * again.
		 */
		result = pgsr->unget_blocknum;
		pgsr->unget_blocknum = InvalidBlockNumber;

		/*
		 * The same per_buffer_data element must have been used, and still
		 * contains whatever data the callback wrote into it.  So we just
		 * sanity-check that we were called with the value that
		 * pg_streaming_unget_block() pushed back.
		 */
		Assert(per_buffer_data == pgsr->unget_per_buffer_data);
	}
	else
	{
		/* Use the installed callback directly. */
		result = pgsr->callback(pgsr, pgsr->pgsr_private, per_buffer_data);
	}

	return result;
}

/*
 * In order to deal with short reads in StartReadBuffers(), we sometimes need
 * to defer handling of a block until later.  This *must* be called with the
 * last value returned by pg_streaming_get_block().
 */
static void
pg_streaming_unget_block(PgStreamingRead *pgsr, BlockNumber blocknum, void *per_buffer_data)
{
	Assert(pgsr->unget_blocknum == InvalidBlockNumber);
	pgsr->unget_blocknum = blocknum;
	pgsr->unget_per_buffer_data = per_buffer_data;
}

static void
pg_streaming_read_look_ahead(PgStreamingRead *pgsr)
{
	PgStreamingReadRange *range;

	/*
	 * If we're still ramping up, we may have to stall to wait for buffers to
	 * be consumed first before we do any more prefetching.
	 */
	if (pgsr->ramp_up_pin_stall > 0)
	{
		Assert(pgsr->pinned_buffers > 0);
		return;
	}

	/*
	 * If we're finished or can't start more I/O, then don't look ahead.
	 */
	if (pgsr->finished || pgsr->ios_in_progress == pgsr->max_ios)
		return;

	/*
	 * We'll also wait until the number of pinned buffers falls below our
	 * trigger level, so that we have the chance to create a full range.
	 */
	if (pgsr->pinned_buffers >= pgsr->pinned_buffers_trigger)
		return;

	do
	{
		BlockNumber blocknum;
		void	   *per_buffer_data;

		/* Do we have a full-sized range? */
		range = &pgsr->ranges[pgsr->head];
		if (range->nblocks == lengthof(range->buffers))
		{
			/* Start as much of it as we can. */
			range = pg_streaming_read_start_head_range(pgsr);

			/* If we're now at the I/O limit, stop here. */
			if (pgsr->ios_in_progress == pgsr->max_ios)
				return;

			/*
			 * If we couldn't form a full range, then stop here to avoid
			 * creating small I/O.
			 */
			if (pgsr->pinned_buffers >= pgsr->pinned_buffers_trigger)
				return;

			/*
			 * That might have only been partially started, but always
			 * processes at least one so that'll do for now.
			 */
			Assert(range->nblocks < lengthof(range->buffers));
		}

		/* Find per-buffer data slot for the next block. */
		per_buffer_data = get_per_buffer_data(pgsr, range, range->nblocks);

		/* Find out which block the callback wants to read next. */
		blocknum = pg_streaming_get_block(pgsr, per_buffer_data);
		if (blocknum == InvalidBlockNumber)
		{
			/* End of stream. */
			pgsr->finished = true;
			break;
		}

		/*
		 * Is there a head range that we cannot extend, because the requested
		 * block is not consecutive?
		 */
		if (range->nblocks > 0 &&
			range->blocknum + range->nblocks != blocknum)
		{
			/* Yes.  Start it, so we can begin building a new one. */
			range = pg_streaming_read_start_head_range(pgsr);

			/*
			 * It's possible that it was only partially started, and we have a
			 * new range with the remainder.  Keep starting I/Os until we get
			 * it all out of the way, or we hit the I/O limit.
			 */
			while (range->nblocks > 0 && pgsr->ios_in_progress < pgsr->max_ios)
				range = pg_streaming_read_start_head_range(pgsr);

			/*
			 * We have to 'unget' the block returned by the callback if we
			 * don't have enough I/O capacity left to start something.
			 */
			if (pgsr->ios_in_progress == pgsr->max_ios)
			{
				pg_streaming_unget_block(pgsr, blocknum, per_buffer_data);
				return;
			}
		}

		/* If we have a new, empty range, initialize the start block. */
		if (range->nblocks == 0)
		{
			range->blocknum = blocknum;
		}

		/* This block extends the range by one. */
		Assert(range->blocknum + range->nblocks == blocknum);
		range->nblocks++;

	} while (pgsr->pinned_buffers + range->nblocks < pgsr->max_pinned_buffers &&
			 pgsr->pinned_buffers + range->nblocks < pgsr->ramp_up_pin_limit);

	/* If we've hit the ramp-up limit, insert a stall. */
	if (pgsr->pinned_buffers + range->nblocks >= pgsr->ramp_up_pin_limit)
	{
		/* Can't get here if an earlier stall hasn't finished. */
		Assert(pgsr->ramp_up_pin_stall == 0);
		/* Don't do any more prefetching until these buffers are consumed. */
		pgsr->ramp_up_pin_stall = pgsr->ramp_up_pin_limit;
		/* Double it.  It will soon be out of the way. */
		pgsr->ramp_up_pin_limit *= 2;
	}

	/* Start as much as we can. */
	while (range->nblocks > 0)
	{
		range = pg_streaming_read_start_head_range(pgsr);
		if (pgsr->ios_in_progress == pgsr->max_ios)
			break;
	}
}

Buffer
pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **per_buffer_data)
{
	pg_streaming_read_look_ahead(pgsr);

	/* See if we have one buffer to return. */
	while (pgsr->tail != pgsr->head)
	{
		PgStreamingReadRange *tail_range;

		tail_range = &pgsr->ranges[pgsr->tail];

		/*
		 * Do we need to perform an I/O before returning the buffers from this
		 * range?
		 */
		if (tail_range->need_wait)
		{
			WaitReadBuffers(&tail_range->operation);
			tail_range->need_wait = false;

			/*
			 * We don't really know if the kernel generated a physical I/O
			 * when we issued advice, let alone when it finished, but it has
			 * certainly finished now because we've performed the read.
			 */
			if (tail_range->advice_issued)
			{
				Assert(pgsr->ios_in_progress > 0);
				pgsr->ios_in_progress--;
			}
		}

		/* Are there more buffers available in this range? */
		if (pgsr->next_tail_buffer < tail_range->nblocks)
		{
			int			buffer_index;
			Buffer		buffer;

			buffer_index = pgsr->next_tail_buffer++;
			buffer = tail_range->buffers[buffer_index];

			Assert(BufferIsValid(buffer));

			/* We are giving away ownership of this pinned buffer. */
			Assert(pgsr->pinned_buffers > 0);
			pgsr->pinned_buffers--;

			if (pgsr->ramp_up_pin_stall > 0)
				pgsr->ramp_up_pin_stall--;

			if (per_buffer_data)
				*per_buffer_data = get_per_buffer_data(pgsr, tail_range, buffer_index);

			return buffer;
		}

		/* Advance tail to next range, if there is one. */
		if (++pgsr->tail == pgsr->size)
			pgsr->tail = 0;
		pgsr->next_tail_buffer = 0;

		/*
		 * If tail crashed into head, and head is not empty, then it is time
		 * to start that range.
		 */
		if (pgsr->tail == pgsr->head &&
			pgsr->ranges[pgsr->head].nblocks > 0)
			pg_streaming_read_start_head_range(pgsr);
	}

	Assert(pgsr->pinned_buffers == 0);

	return InvalidBuffer;
}

void
pg_streaming_read_free(PgStreamingRead *pgsr)
{
	Buffer		buffer;

	/* Stop looking ahead. */
	pgsr->finished = true;

	/* Unpin anything that wasn't consumed. */
	while ((buffer = pg_streaming_read_buffer_get_next(pgsr, NULL)) != InvalidBuffer)
		ReleaseBuffer(buffer);

	Assert(pgsr->pinned_buffers == 0);
	Assert(pgsr->ios_in_progress == 0);

	/* Release memory. */
	if (pgsr->per_buffer_data)
		pfree(pgsr->per_buffer_data);

	pfree(pgsr);
}
