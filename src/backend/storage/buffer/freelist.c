/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/write_stream.h"

#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))

/* GUCs */
int			max_bulk_scan_buffers_kb = 16 * 1024;

/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	/*
	 * Clock sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 */
	pg_atomic_uint32 nextVictimBuffer;

	int			firstFreeBuffer;	/* Head of list of unused buffers */
	int			lastFreeBuffer; /* Tail of list of unused buffers */

	/*
	 * NOTE: lastFreeBuffer is undefined when firstFreeBuffer is -1 (that is,
	 * when the list is empty)
	 */

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Bgworker process to be notified upon activity or -1 if none. See
	 * StrategyNotifyBgWriter.
	 */
	int			bgwprocno;
} BufferStrategyControl;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			nbuffers;
	/* Adaptive range of nbuffers array. */
	int			nbuffers_min;
	int			nbuffers_max;

	/* Counters used to implement adaptive downsizing. */
	int			cycle;
	int			cycle_preserve_ring_size;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * Index of the write-behind slot in the ring.  If the caller indicates
	 * that this buffer is dirty by calling StrategyReleaseBuffer(), we'll
	 * queue it up to be cleaned by write_stream.
	 */
	int			write_behind;

	/*
	 * For strategies with write-behind logic.  Handles correspond to buffer
	 * position in buffers[] array.
	 */
	WriteStream *write_stream;
	WriteStreamWriteHandle *write_stream_handles;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;


/* Prototypes for internal functions */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
									 uint32 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);
static BufferAccessStrategy
			GetAccessStrategyWithSizeRange(BufferAccessStrategyType btype,
										   int ring_max_size_kb,
										   int ring_min_size_kb);

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32
ClockSweepTick(void)
{
	uint32		victim;

	/*
	 * Atomically move hand ahead one buffer - if there's several processes
	 * doing this, this can lead to buffers being returned slightly out of
	 * apparent order.
	 */
	victim =
		pg_atomic_fetch_add_u32(&StrategyControl->nextVictimBuffer, 1);

	if (victim >= NBuffers)
	{
		uint32		originalVictim = victim;

		/* always wrap what we look up in BufferDescriptors */
		victim = victim % NBuffers;

		/*
		 * If we're the one that just caused a wraparound, force
		 * completePasses to be incremented while holding the spinlock. We
		 * need the spinlock so StrategySyncStart() can return a consistent
		 * value consisting of nextVictimBuffer and completePasses.
		 */
		if (victim == 0)
		{
			uint32		expected;
			uint32		wrapped;
			bool		success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				/*
				 * Acquire the spinlock while increasing completePasses. That
				 * allows other readers to read nextVictimBuffer and
				 * completePasses in a consistent manner which is required for
				 * StrategySyncStart().  In theory delaying the increment
				 * could lead to an overflow of nextVictimBuffers, but that's
				 * highly unlikely and wouldn't be particularly harmful.
				 */
				SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

				wrapped = expected % NBuffers;

				success = pg_atomic_compare_exchange_u32(&StrategyControl->nextVictimBuffer,
														 &expected, wrapped);
				if (success)
					StrategyControl->completePasses++;
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
			}
		}
	}
	return victim;
}

/*
 * have_free_buffer -- a lockless check to see if there is a free buffer in
 *					   buffer pool.
 *
 * If the result is true that will become stale once free buffers are moved out
 * by other operations, so the caller who strictly want to use a free buffer
 * should not call this.
 */
bool
have_free_buffer(void)
{
	if (StrategyControl->firstFreeBuffer >= 0)
		return true;
	else
		return false;
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */

	*from_ring = false;

	/*
	 * If given a strategy object, see whether it can select a buffer. We
	 * assume strategy objects don't need buffer_strategy_lock.
	 */
	if (strategy != NULL)
	{
		buf = GetBufferFromRing(strategy, buf_state);
		if (buf != NULL)
		{
			*from_ring = true;
			return buf;
		}
	}

	/*
	 * If asked, we need to waken the bgwriter. Since we don't want to rely on
	 * a spinlock for this we force a read from shared memory once, and then
	 * set the latch based on that value. We need to go through that length
	 * because otherwise bgwprocno might be reset while/after we check because
	 * the compiler might just reread from memory.
	 *
	 * This can possibly set the latch of the wrong process if the bgwriter
	 * dies in the wrong moment. But since PGPROC->procLatch is never
	 * deallocated the worst consequence of that is that we set the latch of
	 * some arbitrary process.
	 */
	bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		StrategyControl->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.  Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 */
	pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);

	/*
	 * First check, without acquiring the lock, whether there's buffers in the
	 * freelist. Since we otherwise don't require the spinlock in every
	 * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
	 * uselessly in most cases. That obviously leaves a race where a buffer is
	 * put on the freelist but we don't see the store yet - but that's pretty
	 * harmless, it'll just get used during the next buffer acquisition.
	 *
	 * If there's buffers on the freelist, acquire the spinlock to pop one
	 * buffer of the freelist. Then check whether that buffer is usable and
	 * repeat if not.
	 *
	 * Note that the freeNext fields are considered to be protected by the
	 * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
	 * manipulate them without holding the spinlock.
	 */
	if (StrategyControl->firstFreeBuffer >= 0)
	{
		while (true)
		{
			/* Acquire the spinlock to remove element from the freelist */
			SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

			if (StrategyControl->firstFreeBuffer < 0)
			{
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
				break;
			}

			buf = GetBufferDescriptor(StrategyControl->firstFreeBuffer);
			Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

			/* Unconditionally remove buffer from freelist */
			StrategyControl->firstFreeBuffer = buf->freeNext;
			buf->freeNext = FREENEXT_NOT_IN_LIST;

			/*
			 * Release the lock so someone else can access the freelist while
			 * we check out this buffer.
			 */
			SpinLockRelease(&StrategyControl->buffer_strategy_lock);

			/*
			 * If the buffer is pinned or has a nonzero usage_count, we cannot
			 * use it; discard it and retry.  (This can only happen if VACUUM
			 * put a valid buffer in the freelist and then someone else used
			 * it before we got to it.  It's probably impossible altogether as
			 * of 8.3, but we'd better check anyway.)
			 */
			local_buf_state = LockBufHdr(buf);
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
				&& BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0)
			{
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}

	/* Nothing on the freelist, so run the "clock sweep" algorithm */
	trycounter = NBuffers;
	for (;;)
	{
		buf = GetBufferDescriptor(ClockSweepTick());

		/*
		 * If the buffer is pinned or has a nonzero usage_count, we cannot use
		 * it; decrement the usage_count (unless pinned) and keep scanning.
		 */
		local_buf_state = LockBufHdr(buf);

		if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
		{
			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				local_buf_state -= BUF_USAGECOUNT_ONE;

				trycounter = NBuffers;
			}
			else
			{
				/* Found a usable buffer */
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
		}
		else if (--trycounter == 0)
		{
			/*
			 * We've scanned all the buffers without making any state changes,
			 * so all the buffers are pinned (or were when we looked at them).
			 * We could hope that someone will free one eventually, but it's
			 * probably better to fail than to risk getting stuck in an
			 * infinite loop.
			 */
			UnlockBufHdr(buf, local_buf_state);
			elog(ERROR, "no unpinned buffers available");
		}
		UnlockBufHdr(buf, local_buf_state);
	}
}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 */
void
StrategyFreeBuffer(BufferDesc *buf)
{
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */
	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		buf->freeNext = StrategyControl->firstFreeBuffer;
		if (buf->freeNext < 0)
			StrategyControl->lastFreeBuffer = buf->buf_id;
		StrategyControl->firstFreeBuffer = buf->buf_id;
	}

	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}

/*
 * StrategySyncStart -- tell BufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
	uint32		nextVictimBuffer;
	int			result;

	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	nextVictimBuffer = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
	result = nextVictimBuffer % NBuffers;

	if (complete_passes)
	{
		*complete_passes = StrategyControl->completePasses;

		/*
		 * Additionally add the number of wraparounds that happened before
		 * completePasses could be incremented. C.f. ClockSweepTick().
		 */
		*complete_passes += nextVictimBuffer / NBuffers;
	}

	if (num_buf_alloc)
	{
		*num_buf_alloc = pg_atomic_exchange_u32(&StrategyControl->numBufferAllocs, 0);
	}
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
StrategyNotifyBgWriter(int bgwprocno)
{
	/*
	 * We acquire buffer_strategy_lock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	StrategyControl->bgwprocno = bgwprocno;
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}


/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size
StrategyShmemSize(void)
{
	Size		size = 0;

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

	return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
StrategyInitialize(bool init)
{
	bool		found;

	/*
	 * Initialize the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

	/*
	 * Get or create the shared strategy control block
	 */
	StrategyControl = (BufferStrategyControl *)
		ShmemInitStruct("Buffer Strategy Status",
						sizeof(BufferStrategyControl),
						&found);

	if (!found)
	{
		/*
		 * Only done once, usually in postmaster
		 */
		Assert(init);

		SpinLockInit(&StrategyControl->buffer_strategy_lock);

		/*
		 * Grab the whole linked list of free buffers for our strategy. We
		 * assume it was previously set up by InitBufferPool().
		 */
		StrategyControl->firstFreeBuffer = 0;
		StrategyControl->lastFreeBuffer = NBuffers - 1;

		/* Initialize the clock sweep pointer */
		pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);

		/* Clear statistics */
		StrategyControl->completePasses = 0;
		pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);

		/* No pending notification */
		StrategyControl->bgwprocno = -1;
	}
	else
		Assert(!init);
}


/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	int			ring_max_size_kb;
	int			ring_min_size_kb;

	/*
	 * Select ring size to use.  See buffer/README for rationales.
	 *
	 * Note: if you change the ring size for BAS_BULKREAD, see also
	 * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
	 */
	switch (btype)
	{
		case BAS_NORMAL:
			/* if someone asks for NORMAL, just give 'em a "default" object */
			return NULL;

		case BAS_BULKREAD:
			ring_max_size_kb = max_bulk_scan_buffers_kb;
			ring_min_size_kb = 256;
			break;
		case BAS_BULKWRITE:
			ring_max_size_kb = max_bulk_scan_buffers_kb;
			ring_min_size_kb = max_bulk_scan_buffers_kb;
			break;
		case BAS_VACUUM:
			ring_max_size_kb = 2048;
			ring_min_size_kb = 2048;
			break;

		default:
			elog(ERROR, "unrecognized buffer access strategy: %d",
				 (int) btype);
			return NULL;		/* keep compiler quiet */
	}

	return GetAccessStrategyWithSizeRange(btype, ring_max_size_kb, ring_min_size_kb);
}

/*
 * GetAccessStrategyWithSize -- create a BufferAccessStrategy object with a
 *		number of buffers equivalent to the passed in size.
 *
 * If the given ring size is 0, no BufferAccessStrategy will be created and
 * the function will return NULL.  ring_size_kb must not be negative.
 */
BufferAccessStrategy
GetAccessStrategyWithSize(BufferAccessStrategyType btype, int ring_size_kb)
{
	return GetAccessStrategyWithSizeRange(btype, ring_size_kb, ring_size_kb);
}

/*
 * GetAccessStrategyWithSizeRange -- create an adaptively sized BAS.
 */
static BufferAccessStrategy
GetAccessStrategyWithSizeRange(BufferAccessStrategyType btype,
							   int ring_max_size_kb,
							   int ring_min_size_kb)
{
	int			ring_buffers;
	BufferAccessStrategy strategy;

	Assert(ring_max_size_kb >= 0);
	Assert(ring_min_size_kb >= 0);
	Assert(ring_max_size_kb >= ring_min_size_kb);

	/* Figure out how many buffers ring_max_size_kb is */
	ring_buffers = ring_max_size_kb / (BLCKSZ / 1024);

	/* 0 means unlimited, so no BufferAccessStrategy required */
	if (ring_buffers == 0)
		return NULL;

	/* Cap to 1/8th of shared_buffers */
	ring_buffers = Min(NBuffers / 8, ring_buffers);

	/* NBuffers should never be less than 16, so this shouldn't happen */
	Assert(ring_buffers > 0);

	/* Allocate the object and initialize all elements to zeroes */
	strategy = (BufferAccessStrategy)
		palloc0(offsetof(BufferAccessStrategyData, buffers) +
				ring_buffers * sizeof(Buffer));

	/* Set fields that don't start out zero */
	strategy->btype = btype;
	strategy->nbuffers_min = Min(ring_buffers, ring_min_size_kb / (BLCKSZ / 1024));
	strategy->nbuffers_max = ring_buffers;
	strategy->nbuffers = strategy->nbuffers_min;

	/*
	 * Set up a write stream for cleaning dirty buffers that are given back to
	 * use by StrategyReleaseBuffer().
	 *
	 * We don't want the write stream to be constrained to flush the WAL any
	 * more often than the worst case without the stream.  Allow it to defer
	 * enough writes for the whole ring to be dirty and need to be cleaned
	 * before recycling the oldest buffer.  The write stream will try to clean
	 * buffers sooner than that, though, if concurrent WAL activity allows
	 * earlier flushing.
	 *
	 * XXX allocate this and the handle array on demand in
	 * StrategyReleaseBuffer()?  This we skip creating it for purely read-only
	 * SELECT
	 */
	strategy->write_stream = write_stream_begin(0, NULL, ring_buffers);

	/*
	 * We need a way to make sure that write-behind has definitely finished
	 * cleaning a buffer before we recycle it.  Record a handle for each
	 * buffer, so we can wait for I/O to complete, if necessary.
	 */
	strategy->write_stream_handles =
		palloc0(sizeof(WriteStreamWriteHandle) * ring_buffers);

	return strategy;
}

/*
 * GetAccessStrategyBufferCount -- an accessor for the number of buffers in
 *		the ring
 *
 * Returns 0 on NULL input to match behavior of GetAccessStrategyWithSize()
 * returning NULL with 0 size.  Note that the value may change over time, for
 * strategies with a range.
 */
int
GetAccessStrategyBufferCount(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return 0;

	return strategy->nbuffers;
}

/*
 * GetAccessStrategyPinLimit -- get cap of number of buffers that should be pinned
 *
 * When pinning extra buffers to look ahead, users of a ring-based strategy are
 * in danger of pinning too much of the ring at once while performing look-ahead.
 * Unless the ring can grow, hitting dirty buffers while looking ahead could
 * increase the WAL flush rate.
 *
 * Since external code has no insight into any of that, allow individual
 * strategy types to expose a clamp that should be applied when deciding on a
 * maximum number of buffers to pin at once.
 *
 * Callers should combine this number with other relevant limits and take the
 * minimum.
 */
int
GetAccessStrategyPinLimit(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return NBuffers;

	/* The ring can grow.  Anything up to the initial size would be OK to pin. */
	if (strategy->nbuffers < strategy->nbuffers_max)
		return strategy->nbuffers;

	/* The ring can't grow.  Don't pin more than half of the ring. */
	return strategy->nbuffers_max / 2;
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
	{
		if (strategy->write_stream_handles)
			pfree(strategy->write_stream_handles);
		if (strategy->write_stream)
		{
			/*
			 * If someone uses a temporary strategy and throws it away without
			 * calling StrategyWaitAll(), we'll reset the write stream here so
			 * that we don't generate extra writes.  That way we avoid
			 * generating I/O when only a small amount of data is touched.
			 */
			write_stream_reset(strategy->write_stream);
			write_stream_end(strategy->write_stream);
		}
		pfree(strategy);
	}
}

/*
 * Increase the size of a strategy's ring buffer.
 */
static void
GrowStrategyRing(BufferAccessStrategy strategy)
{
	int			new_nbuffers;
	int			buffers_to_insert;
	int			buffers_to_move;

	Assert(strategy->nbuffers < strategy->nbuffers_max);

	/*
	 * Double the size of the ring by inserting empty slots at the current
	 * position.
	 */
	new_nbuffers = Min(strategy->nbuffers * 2,
					   strategy->nbuffers_max);
	buffers_to_insert = new_nbuffers - strategy->nbuffers;
	buffers_to_move = strategy->nbuffers - strategy->current;
	strategy->nbuffers = new_nbuffers;

	/*
	 * Slide the rest of the slots up to make space.  This might seem
	 * expensive, but it's amortized by geometric growth.  If we instead just
	 * pointed current to new additional space at the end, write-behind
	 * tracking would finish up out of order with negative consequences for
	 * performance.
	 */
	memmove(&strategy->buffers[strategy->current + buffers_to_insert],
			&strategy->buffers[strategy->current],
			buffers_to_move * sizeof(Buffer));

	/* Zero-initialize the inserted slots. */
	memset(&strategy->buffers[strategy->current],
		   0,
		   buffers_to_insert * sizeof(Buffer));
	memset(&strategy->write_stream_handles[strategy->current],
		   0,
		   buffers_to_insert * sizeof(WriteStreamWriteHandle));
}

/*
 * Initiate as much write-behind as we can.
 *
 * In the ideal case, we are called with a pinned buffer that the consumer of
 * buffers has just finished modifying.  Otherwise, we have a fallback
 * strategy that is more conservative, but tries to find dirty buffers to
 * stream out before they're re-used.
 */
static void
StrategyWriteBehindImpl(BufferAccessStrategy strategy, Buffer pinned_buffer)
{
	while (strategy->write_behind != strategy->current)
	{
		int distance;
		int write_behind;
		Buffer buffer;
		BufferDesc *desc;
		uint32 buf_state;

		/* Where is the next potential write-behind buffer? */
		write_behind = strategy->write_behind + 1;
		if (write_behind == strategy->nbuffers)
			write_behind = 0;
		buffer = strategy->buffers[write_behind];

		/* Empty slot? */
		if (buffer == InvalidBuffer)
		{
			strategy->write_behind = write_behind;
			continue;
		}

		/*
		 * Ideal case: the consumer of buffers is considerate and tells us
		 * when it has finished modifying a buffer, and gives it back to us
		 * still pinned in exactly the right order.  We can be agressive about
		 * write-behind, and skip some header manipulation.  Do we have that
		 * exact case?
		 */
		if (pinned_buffer != InvalidBuffer && pinned_buffer == buffer)
		{
			/*
			 * Peek at unlock buffer header bit without holding lock.  We
			 * can't use BufferIsDirty() here because we don't hold an
			 * exclusive content lock.  It's OK to do this directly though,
			 * because we only want to know if *this* backend called
			 * MarkBufferDirty() recently.  Then we should be able to see that
			 * flag without lock or memory barriers, and if someone cleared it
			 * in between then that's OK, we want to skip it here.  This test
			 * makes it OK for generic code paths to call
			 * StrategyReleaseBuffer() without having to keep track of whether
			 * they dirtied the buffer.
			 */
			desc = GetBufferDescriptor(buffer - 1);
			if (pg_atomic_read_u32(&desc->state) & BM_DIRTY)
			{
				/* Start writing this buffer out. */
				strategy->write_stream_handles[write_behind] =
					write_stream_write_buffer(strategy->write_stream, buffer);
				strategy->write_behind = write_behind;
			}
			break;
		}

		/*
		 * Otherwise we have to be a lot more conservative.  We stay a good
		 * distance behind the 'current' end of the ring, until it looks like
		 * the consumer of buffers is very likely to be finished with it.
		 */
		distance = strategy->current - write_behind;
		if (distance < 0)
			distance += strategy->nbuffers;
		Assert(distance >= 0);

		/* Lock the buffer header. */
		desc = GetBufferDescriptor(buffer - 1);
		buf_state = LockBufHdr(desc);

		/* Is it too soon? */
		if (BUF_STATE_GET_REFCOUNT(buf_state) > 0)
		{
			/*
			 * It is pinned, but we don't know by whom.  It could be in a read
			 * stream looking ahead using this very strategy, so the consumer
			 * hasn't had a chance to modify the page yet.  Wait until it is
			 * outside the window that we would recommend to a read stream,
			 * plus a few more.
			 *
			 * Note that if it's still pinned by the time GetBufferFromRing()
			 * looks at it, it'll be booted from ring anyway.
			 */
			if (distance < GetAccessStrategyPinLimit(strategy) + 3)
			{
				UnlockBufHdr(desc, buf_state);
				break;
			}
		}

		/* After this point we're definitely going to skip or write. */
		strategy->write_behind = write_behind;

		/* Is it too hot? */
		if (BUF_STATE_GET_USAGECOUNT(buf_state) > 1)
		{
			/*
			 * It has a usage count indicating someone else has been touching
			 * it, so it's too valuable to be in the ring.  Boot it out now
			 * (if we didn't, GetBufferFromRing() would probably do it soon
			 * anyway but we can save it the trouble; the only way it could
			 * decide not to is if CLOCK cools it down enough first, but that
			 * isn't a more correct outcome).
			 *
			 * This means that consumers that repin a page repeatedly can't
			 * use strategies effectively, and degrade to BAS_NORMAL behavior.
			 */
			UnlockBufHdr(desc, buf_state);
			strategy->buffers[write_behind] = InvalidBuffer;
			continue;
		}

		/* Is it too clean? */
		if (!(buf_state & BM_DIRTY))
		{
			/* It's not dirty, so there is no write-behind to do anyway. */
			UnlockBufHdr(desc, buf_state);
			continue;
		}

		/*
		 * We have a candidate buffer that GetBufferFromRing() would consider
		 * re-using, and it looks like the consumer of buffers has finished
		 * dirtying it.  Start writing it out so that it is hopefully clean by
		 * the time GetBufferFromRing() looks at it.
		 *
		 * XXX FIXME here we unlock and re-lock the header, because we don't
		 * have a direct way to acquire a pin from here!
		 */
		{
			RelFileLocator rlocator;
			ForkNumber forknumber;
			BlockNumber blocknumber;

			rlocator = BufTagGetRelFileLocator(&desc->tag);
			forknumber = BufTagGetForkNum(&desc->tag);
			blocknumber = desc->tag.blockNum;
			UnlockBufHdr(desc, buf_state);
			if (!ReadRecentBuffer(rlocator,
								  forknumber,
								  blocknumber,
								  strategy->buffers[write_behind]))
			{
				/* Someone evicted it between the two statements above! */
				continue;
			}
		}

		/* Start writing this buffer out. */
		strategy->write_stream_handles[write_behind] =
			write_stream_write_buffer(strategy->write_stream, buffer);
	}
}

void
StrategyWriteBehind(BufferAccessStrategy strategy)
{
	StrategyWriteBehindImpl(strategy, InvalidBuffer);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty / not usable.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */


	/* Advance to next ring slot */
	if (++strategy->current >= strategy->nbuffers)
	{
		if (strategy->nbuffers > strategy->nbuffers_min &&
			strategy->cycle_preserve_ring_size != strategy->cycle)
		{
			/*
			 * We made it through a whole tour of the ring without having to
			 * do any write-behind.  Time to down-size gradually.
			 */
			strategy->nbuffers--;
		}
		strategy->cycle++;
		strategy->current = 0;
	}

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation strategy.  He will then fill this
	 * slot by calling AddBufferToRing with the new buffer.
	 */
	bufnum = strategy->buffers[strategy->current];
	if (bufnum == InvalidBuffer)
		return NULL;

	/*
	 * If StrategyWriteBehind() started writing this buffer out, we now have
	 * to make sure that is finished and the buffer is unpinned, or expand the
	 * ring, that is, insert new empty slots at current, to avoid stalling.
	 */
	if (write_stream_handle_is_valid(strategy->write_stream_handles[strategy->current]))
	{
		/*
		 * Yes.  Since the consumer of buffers is apparently still generating
		 * dirty buffers, remember not to shrink the ring in this cycle.
		 */
		strategy->cycle_preserve_ring_size = strategy->cycle;

		if (strategy->nbuffers == strategy->nbuffers_max)
		{
			/*
			 * We are at maximum ring size, so if we have to flush the WAL to
			 * finish the write, then so be it.
			 */
			write_stream_wait(strategy->write_stream,
							  strategy->write_stream_handles[strategy->current]);
		}
		else
		{
			/*
			 * Try to complete the write without stalling, since we still have
			 * space to expand the ring instead.
			 */
			if (write_stream_wait_no_stall(strategy->write_stream,
										   strategy->write_stream_handles[strategy->current]))
			{
				GrowStrategyRing(strategy);
				return NULL;
			}
		}

		/*
		 * XXX THE PROBLEM is that the above code might have eaten the nice
		 * reserved private refcount that GetVictimBuffer() carefully set up.
		 * So then PinBuffer_Locked() dies.  Doh.
		 */

		/*
		 * Make the handle invalid, so that there is a chance of the ring
		 * doing down in size if the consumer of buffers begins to scan
		 * buffers without returning them marked dirty.
		 */
		memset(&strategy->write_stream_handles[strategy->current],
			   0,
			   sizeof(WriteStreamWriteHandle));
	}

	/*
	 * If the buffer is pinned we cannot use it under any circumstances.
	 *
	 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
	 * since our own previous usage of the ring element would have left it
	 * there, but it might've been decremented by clock sweep since then). A
	 * higher usage_count indicates someone else has touched the buffer, so we
	 * shouldn't re-use it.
	 */
	buf = GetBufferDescriptor(bufnum - 1);
	local_buf_state = LockBufHdr(buf);
	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
		&& BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1)
	{
		*buf_state = local_buf_state;
		return buf;
	}
	UnlockBufHdr(buf, local_buf_state);

	/*
	 * Tell caller to allocate a new buffer with the normal allocation
	 * strategy.  He'll then replace this ring element via AddBufferToRing.
	 */
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
{
	strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * Utility function returning the IOContext of a given BufferAccessStrategy's
 * strategy ring.
 */
IOContext
IOContextForStrategy(BufferAccessStrategy strategy)
{
	if (!strategy)
		return IOCONTEXT_NORMAL;

	switch (strategy->btype)
	{
		case BAS_NORMAL:

			/*
			 * Currently, GetAccessStrategy() returns NULL for
			 * BufferAccessStrategyType BAS_NORMAL, so this case is
			 * unreachable.
			 */
			pg_unreachable();
			return IOCONTEXT_NORMAL;
		case BAS_BULKREAD:
			return IOCONTEXT_BULKREAD;
		case BAS_BULKWRITE:
			return IOCONTEXT_BULKWRITE;
		case BAS_VACUUM:
			return IOCONTEXT_VACUUM;
	}

	elog(ERROR, "unrecognized BufferAccessStrategyType: %d", strategy->btype);
	pg_unreachable();
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.  This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf, bool from_ring)
{
	/*
	 * Even if we can't reject this due to being at maximum size, remember
	 * that we were asked to, to prevent the ring from shrinking.
	 */
	strategy->cycle_preserve_ring_size = strategy->cycle;

	/* If there's no chance of expanding the ring, give up. */
	if (strategy->nbuffers == strategy->nbuffers_max)
		return false;

	/* Don't muck with behavior of normal buffer-replacement strategy */
	if (!from_ring ||
		strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Increase the ring size, and reject this buffer.  This creates empty
	 * slots at 'current'.
	 */
	GrowStrategyRing(strategy);

	return true;
}

/*
 * Return a pinned buffer to a strategy.  Consumers of buffers don't have to
 * call this, but write-behind is more aggressive and efficient if they do
 * this for dirtied buffers.
 */
void
StrategyReleaseBuffer(BufferAccessStrategy strategy, Buffer buffer)
{
	/*
	 * If enabled for this strategy, and this isn't a repeated release of the
	 * same buffer, then we can try to implement write-behind.
	 */
	if (strategy &&
		strategy->buffers[strategy->write_behind] != buffer)
	{
		StrategyWriteBehindImpl(strategy, buffer);
		return;
	}

	/*
	 * Write-behind is not possible.  Just release the buffer immediately.
	 * (If we really are getting repeated calls for the same buffer, the
	 * caller has a non-ideal access pattern that will get buffers booted out
	 * of the ring for being too hot anyway, but at least we might stream data
	 * out more efficiently while also failing at scan resistance...)
	 */
	ReleaseBuffer(buffer);
}

/*
 * Unlock a buffer, then return it still pinned to a strategy.
 */
void
StrategyUnlockReleaseBuffer(BufferAccessStrategy strategy, Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	StrategyReleaseBuffer(strategy, buffer);
}

/*
 * Abandon any write-behind work, releasing any pins held by the strategy.
 * This is useful before vacuum drops buffers to truncate a relation, throwing
 * away dirty data.
 */
void
StrategyReset(BufferAccessStrategy strategy)
{
	if (strategy && strategy->write_stream)
		write_stream_reset(strategy->write_stream);
}

/*
 * Finish any write-behind work, releasing any pins held by the strategy.
 */
void
StrategyWaitAll(BufferAccessStrategy strategy)
{
	if (strategy && strategy->write_stream)
		write_stream_wait_all(strategy->write_stream);
}
