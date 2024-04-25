/*-------------------------------------------------------------------------
 *
 * write_stream.c
 *	  Mechanism for flushing buffered data efficiently
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/write_stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "lib/pairingheap.h"
#include "storage/buf_internals.h"
#include "storage/write_stream.h"


typedef struct WriteStreamWrite
{
	/*
	 * A WriteStreamWrite can be on the freelist, or in the queue, or the
	 * current pending_write.
	 */
	union
	{
		pairingheap_node queue_node;
		dlist_node	freelist_node;
	}			u;

	/*
	 * A generation counter for writes, so we can check if a write has
	 * finished even though it might have been recycled.
	 */
	uint64		counter;

	/*
	 * The last known high LSN for the buffers of this write.  This may be out
	 * of date. XXX
	 */
	XLogRecPtr	flush_lsn;

	/*
	 * If we have to unpin and later re-pin the buffers, we'll need the
	 * arguments for ReadRecentBuffer().  The block number is for the first
	 * buffer, the rest of the buffers follow.
	 */
	RelFileLocator rlocator;
	ForkNumber	forknumber;
	BlockNumber blocknumber;

	/* Buffers. */
	int			nblocks;
	bool		buffers_are_pinned;
	Buffer		buffers[MAX_IO_COMBINE_LIMIT];

	/* Cross-check for membership in different data structures. */
#ifdef USE_ASSERT_CHECKING
	enum
	{
		WSW_FREE,
		WSW_PENDING,
		WSW_QUEUED
	}			location;
#endif
} WriteStreamWrite;

struct WriteStream
{
	int16		queued_writes;
	int16		max_queued_writes;

	WritebackContext *wb_context;

	/* The write we are currently trying to grow. */
	WriteStreamWrite *pending_write;

	/*
	 * Known flushed threshold.  In recovery this set higher than any LSN,
	 * because recovery currently always logs before replaying and doesn't
	 * allow GetFlushRecPtr() to be called.
	 */
	XLogRecPtr	flush_lsn_limit;

	/* Writes prioritized by LSN. */
	pairingheap lsn_reorder_queue;

	/* Spare writes. */
	dlist_head	write_freelist;

	size_t		writes_array_size;
	WriteStreamWrite writes[FLEXIBLE_ARRAY_MEMBER];
};

static inline bool
pinned_buffers_are_consecutive(Buffer buf1, Buffer buf2)
{
	BufferDesc *hdr1;
	BufferDesc *hdr2;

	hdr1 = GetBufferDescriptor(buf1 - 1);
	hdr2 = GetBufferDescriptor(buf2 - 1);

	return BufferTagsConsecutive(&hdr1->tag, &hdr2->tag);
}

/*
 * Perform one write.  The write object is released to the freelist.
 * If the write fails and raises an error, the stream is invalid and can't be
 * used anymore.
 *
 * Returns true if we stalled.
 */
static bool
write_stream_do_write(WriteStream *stream, WriteStreamWrite *write)
{
	bool		stalled = false;
	Buffer	   *buffers;
	int			nblocks;
	BlockNumber blocknumber;

	/* Sanity checks. */
	Assert(write->nblocks > 0);
	Assert(write->location == WSW_QUEUED || write->location == WSW_PENDING);

	buffers = write->buffers;
	nblocks = write->nblocks;
	blocknumber = write->blocknumber;

	while (nblocks > 0)
	{
		bool		stalled_this_fragment;
		int			nblocks_this_fragment;

		if (write->buffers_are_pinned)
		{
			/* We have all the buffers pinned already. */
			nblocks_this_fragment = nblocks;
		}
		else
		{
			/* Skip any leading buffers have gone away. */
			if (!ReadRecentBuffer(write->rlocator,
								  write->forknumber,
								  blocknumber,
								  buffers[0]))
			{
				blocknumber++;
				buffers++;
				nblocks--;
				continue;
			}
			/* We have re-pinned one buffer.  How many more can we pin? */
			nblocks_this_fragment = 1;
			while (nblocks_this_fragment < nblocks &&
				   ReadRecentBuffer(write->rlocator,
									write->forknumber,
									blocknumber + nblocks_this_fragment,
									buffers[nblocks_this_fragment]))
				nblocks_this_fragment++;
		}

		/* Flush this fragment. */
		FlushPinnedBuffers(buffers,
						   nblocks_this_fragment,
						   stream->wb_context,
						   &stalled_this_fragment);
		buffers += nblocks_this_fragment;
		nblocks -= nblocks_this_fragment;
		stalled |= stalled_this_fragment;
	}

#ifdef USE_ASSERT_CHECKING
	for (int i = 0; i < write->nblocks; ++i)
		write->buffers[i] = InvalidBuffer;
#endif

	/*
	 * Allow write_stream_wait()'s fast path to see that all the buffers in
	 * this write are finished.
	 */
	write->counter++;

	/* This write object is now available for re-use. */
	dlist_push_head(&stream->write_freelist, &write->u.freelist_node);
#ifdef USE_ASSERT_CHECKING
	write->location = WSW_FREE;
#endif

	return stalled;
}

/*
 * Check if a given LSN is certainly flushed already.
 */
static inline bool
write_stream_check_flush_limit(WriteStream *stream, XLogRecPtr lsn)
{
	/* This is always true when in recovery. */
	if (likely(lsn <= stream->flush_lsn_limit))
		return true;

	/* Update our copy from shared memory. */
	stream->flush_lsn_limit = GetFlushRecPtr(NULL);
	return lsn <= stream->flush_lsn_limit;
}

/*
 * Make the write queue bigger.
 */
static void
write_stream_expand_queue(WriteStream *stream, int16 max_queued_writes)
{
	size_t		initialized_size;
	size_t		new_initialized_size;

	/*
	 * We need one extra element for the current pending write.  (If we had
	 * asynchronous I/O, we'd want more for the in writes in progress.)
	 */
	initialized_size = stream->max_queued_writes + 1;
	new_initialized_size = max_queued_writes + 1;
	Assert(new_initialized_size <= stream->writes_array_size);

	/* Initialize more writes, and put them on the freelist. */
	while (initialized_size < new_initialized_size)
	{
		WriteStreamWrite *write = &stream->writes[initialized_size++];

#ifdef USE_ASSERT_CHECKING
		write->location = WSW_FREE;
		for (int j = 0; j < lengthof(write->buffers); ++j)
			write->buffers[j] = InvalidBuffer;
#endif
		dlist_push_head(&stream->write_freelist, &write->u.freelist_node);
	}
	stream->max_queued_writes = initialized_size - 1;
	Assert(stream->max_queued_writes > 0);
}

/*
 * Perform at least the write with the highest priority (lowest flush LSN).
 * Also do any more writes can be done without flushing.
 *
 * If try_avoid_flush is set, then consider expanding the queue size instead
 * if a flush would be necessary.  Either way, when this function returns,
 * there is space for a new write to be insert into the queue.
 *
 * Returns true if we stalled.
 */
static bool
write_stream_do_highest_priority_write(WriteStream *stream, bool try_avoid_flush)
{
	WriteStreamWrite *write;
	bool		stalled;

	/* We should only be called if there is at least one queued. */
	Assert(stream->queued_writes > 0);
	Assert(!pairingheap_is_empty(&stream->lsn_reorder_queue));

	write = pairingheap_container(WriteStreamWrite,
								  u.queue_node,
								  pairingheap_first(&stream->lsn_reorder_queue));

	/*
	 * Consider making the reorder queue bigger instead, if we'd have to flush
	 * to preform this write.  That might give the WAL writer a better chance
	 * of keeping up.
	 */
	if (try_avoid_flush &&
		stream->max_queued_writes < stream->writes_array_size - 1 &&
		!write_stream_check_flush_limit(stream, write->flush_lsn))
	{
		write_stream_expand_queue(stream, stream->max_queued_writes + 1);
		return false;
	}

	/* Perform the highest priority write. */
	pairingheap_remove_first(&stream->lsn_reorder_queue);
	Assert(stream->queued_writes > 0);
	stream->queued_writes--;
	stalled = write_stream_do_write(stream, write);

	/*
	 * How many more writes can we do without flushing the WAL?  Usually all
	 * of them, if we had to flush above, because of XLogFlush()'s policy of
	 * flushing as much as possible, even past the requested LSN.  If we
	 * didn't have to flush, then someone else must have, and in that case the
	 * following loop might find only some.
	 */
	while (!pairingheap_is_empty(&stream->lsn_reorder_queue))
	{
		write = pairingheap_container(WriteStreamWrite,
									  u.queue_node,
									  pairingheap_first(&stream->lsn_reorder_queue));
		if (!write_stream_check_flush_limit(stream, write->flush_lsn))
			break;

		/* Perform the highest priority loop. */
		pairingheap_remove_first(&stream->lsn_reorder_queue);
		Assert(stream->queued_writes > 0);
		stream->queued_writes--;
		stalled |= write_stream_do_write(stream, write);
	}

	return stalled;
}

/*
 * Push the current pending write onto the priority queue.  It will wait there
 * until the queue is full and it has the lowest LSN.  Hopefully this means
 * that we'll have to flush the WAL less often.  If the queue is full, perform
 * the the highest priority write first.
 *
 * If we can already see that no WAL flush is required, then skip the queue and
 * do the write immediately.
 *
 * Returns true if we stalled.
 */
static bool
write_stream_enqueue_pending_write(WriteStream *stream)
{
	WriteStreamWrite *write = stream->pending_write;
	bool		stalled = false;

	Assert(write != NULL);
	Assert(write->location = WSW_PENDING);

	/*
	 * If no WAL flushing is required to write this data, we can skip the LSN
	 * queue and proceed directly to I/O while we still have it pinned.  This
	 * is likely to be true only for UNLOGGED relation data.
	 */
	if (write_stream_check_flush_limit(stream, write->flush_lsn))
	{
		stream->pending_write = NULL;
		return write_stream_do_write(stream, write);
	}

	/* Have we hit the queue limit? */
	if (stream->queued_writes == stream->max_queued_writes)
		stalled = write_stream_do_highest_priority_write(stream,
														 true /* try_avoid_flush */ );
	Assert(stream->queued_writes < stream->max_queued_writes);

	/*
	 * We don't want to hold lots of pins while we're waiting for the WAL to
	 * be flushed, so it's time to unpin them, after noting down the
	 * information we'll need to repin them later (unless someone else writes
	 * them out and evicts them first).
	 *
	 * XXX Should be prepared to hold some pins, up to a reasonable limit, so
	 * that system with fast WAL can avoid these extra pin management costs?
	 */
	BufferGetTag(write->buffers[0],
				 &write->rlocator,
				 &write->forknumber,
				 &write->blocknumber);
	for (int i = 0; i < write->nblocks; ++i)
		ReleaseBuffer(write->buffers[i]);
	write->buffers_are_pinned = false;

	pairingheap_add(&stream->lsn_reorder_queue, &write->u.queue_node);
	stream->queued_writes++;
#ifdef USE_ASSERT_CHECKING
	write->location = WSW_QUEUED;
#endif

	stream->pending_write = NULL;

	return stalled;
}

static int
write_stream_lsn_compare(const pairingheap_node *a,
						 const pairingheap_node *b,
						 void *arg)
{
	XLogRecPtr	a_lsn;
	XLogRecPtr	b_lsn;

	a_lsn = pairingheap_const_container(WriteStreamWrite, u.queue_node, a)->flush_lsn;
	b_lsn = pairingheap_const_container(WriteStreamWrite, u.queue_node, b)->flush_lsn;
	return a_lsn < b_lsn ? -1 : a_lsn > b_lsn ? 1 : 0;
}

/*
 * Begin a new write stream.
 *
 * Caller can specify the maximum number of written buffers that can be
 * deferred by internal queuing, or -1 for a reasonable default.  The queue is
 * initially small, but expands as required to try to avoid having to flush
 * the WAL.
 */
WriteStream *
write_stream_begin(int flags, WritebackContext *wb_context, int max_distance)
{
	WriteStream *stream;
	int16		max_queued_writes;
	size_t		writes_array_size;

	/* An arbitrary default. */
	if (max_distance <= 0)
		max_distance = 16 * io_combine_limit;

	/*
	 * Maximum possible number of deferred writes, when expanding the queue.
	 * We don't know how successful we'll be at write combining, so allow for
	 * average write size of half of io_combine_limit.  Round up to avoid
	 * zero.
	 */
	writes_array_size = max_distance;
	if (io_combine_limit > 1)
		writes_array_size /= io_combine_limit / 2;
	writes_array_size++;

	/* Add one for the current pending write, and cap for data type. */
	writes_array_size++;
	writes_array_size = Min(PG_INT16_MAX, writes_array_size);

	/* Initial size of the reorder queue. */
	max_queued_writes = 1;

	/* Allocate and initialize. */
	stream = palloc(offsetof(WriteStream, writes) +
					writes_array_size * sizeof(WriteStreamWrite));
	stream->max_queued_writes = -1; /* expanded below */
	stream->writes_array_size = writes_array_size;
	stream->queued_writes = 0;
	stream->pending_write = NULL;
	stream->wb_context = wb_context;
	dlist_init(&stream->write_freelist);
	stream->lsn_reorder_queue.ph_root = NULL;
	stream->lsn_reorder_queue.ph_arg = NULL;
	stream->lsn_reorder_queue.ph_compare = write_stream_lsn_compare;

	/* What LSN should we initially consider to be flushed? */
	if (RecoveryInProgress())
		stream->flush_lsn_limit = PG_UINT64_MAX;
	else
		stream->flush_lsn_limit = 0;

	write_stream_expand_queue(stream, max_queued_writes);

	return stream;
}


/*
 * Start writing out one buffer.  The write is finished either when the stream
 * is eventually flushed with write_stream_end(), but if the caller is
 * interested in waiting for individual buffers to be written out, the
 * returned handle can be used to wait.
 */
WriteStreamWriteHandle
write_stream_write_buffer(WriteStream *stream, Buffer buffer)
{
	WriteStreamWriteHandle handle;
	WriteStreamWrite *pending_write;

	/* Can we combine this buffer with the current pending write, if any? */
	pending_write = stream->pending_write;
	if (likely(pending_write))
	{
		if (likely(pending_write->nblocks < io_combine_limit &&
				   pinned_buffers_are_consecutive(pending_write->buffers[pending_write->nblocks - 1],
												  buffer)))
		{
			/*
			 * Yes!  Append, and adjust LSN high water mark for the combined
			 * write. It doesn't matter if the LSN changes later, we're only
			 * using it to guide prioritization, not for correctness.
			 */
			pending_write->buffers[pending_write->nblocks++] = buffer;
			pending_write->flush_lsn = Max(pending_write->flush_lsn,
										   BufferGetLSNAtomic(buffer));
			Assert(pending_write->location == WSW_PENDING);

			handle.p = &pending_write->counter;
			handle.c = pending_write->counter;
			return handle;
		}

		/* No.  Queue it up, to get it out of the way. */
		write_stream_enqueue_pending_write(stream);
	}

	/*
	 * There must be at least one write object in the freelist, because there
	 * isn't a pending write.
	 */
	Assert(stream->pending_write == NULL);
	Assert(!dlist_is_empty(&stream->write_freelist));

	/* Start forming a new write, with this buffer as the head. */
	pending_write = dlist_container(WriteStreamWrite,
									u.freelist_node,
									dlist_pop_head_node(&stream->write_freelist));
	Assert(pending_write->location == WSW_FREE);
	pending_write->buffers_are_pinned = true;
	pending_write->buffers[0] = buffer;
	pending_write->nblocks = 1;
	pending_write->flush_lsn = BufferGetLSNAtomic(buffer);
#ifdef USE_ASSERT_CHECKING
	pending_write->location = WSW_PENDING;
#endif
	stream->pending_write = pending_write;


	handle.p = &pending_write->counter;
	handle.c = pending_write->counter;
	return handle;
}

/*
 * Slow path for write_stream_wait() and write_stream_wait_no_stall().
 * Returns true if we stalled; see fast path functions in header for
 * definition of stalling and semantics.
 */
bool
write_stream_wait_internal(WriteStream *stream,
						   WriteStreamWriteHandle handle,
						   bool try_no_stall)
{
	WriteStreamWrite *write;
	bool		stalled = false;

	/* Convert handle back to a write object. */
	write = (WriteStreamWrite *)
		((char *) handle.p - offsetof(WriteStreamWrite, counter));

	/* The inline fast path shouldn't have sent us here if done already. */
	Assert(handle.c == write->counter);

	/*
	 * If the caller doesn't want to wait for the WAL to be flushed, give up
	 * if it looks like we'd need to do that.
	 */
	if (try_no_stall && !write_stream_check_flush_limit(stream, write->flush_lsn))
		return true;

	/* If it's the current pending write, enqueue it. */
	if (write == stream->pending_write)
	{
		Assert(write->location == WSW_PENDING);
		stalled = write_stream_enqueue_pending_write(stream);

		/* Finished yet? */
		if (handle.c != write->counter)
		{
			Assert(write->location == WSW_FREE);
			return stalled;
		}
	}

	/* Keep writing out queued writes until the handle is satisfied. */
	Assert(write->location == WSW_QUEUED);
	while (handle.c == write->counter)
	{
		/* There must be one! */
		Assert(stream->queued_writes > 0);
		stalled |= write_stream_do_highest_priority_write(stream, false);
	}

	return stalled;
}

/*
 * Release all pins and forget all queued writes.
 */
void
write_stream_reset(WriteStream *stream)
{
	WriteStreamWrite *write;

	if ((write = stream->pending_write))
	{
		stream->pending_write = NULL;
		write->counter++;
		for (int i = 0; i < write->nblocks; ++i)
			ReleaseBuffer(write->buffers[i]);
		dlist_push_head(&stream->write_freelist, &write->u.freelist_node);
#ifdef USE_ASSERT_CHECKING
		write->location = WSW_FREE;
#endif
	}

	while (!pairingheap_is_empty(&stream->lsn_reorder_queue))
	{
		write = pairingheap_container(WriteStreamWrite,
									  u.queue_node,
									  pairingheap_remove_first(&stream->lsn_reorder_queue));
		write->counter++;
		Assert(!write->buffers_are_pinned);
		dlist_push_head(&stream->write_freelist, &write->u.freelist_node);
#ifdef USE_ASSERT_CHECKING
		write->location = WSW_FREE;
#endif
		stream->queued_writes--;
	}

	Assert(stream->pending_write == NULL);
	Assert(stream->queued_writes == 0);
}

/*
 * Finish all queued writes.  Nothing is pinned on return.
 */
void
write_stream_wait_all(WriteStream *stream)
{
	/* Complete any queued up writes. */
	while (stream->queued_writes > 0 ||
		   stream->pending_write)
	{
		if (stream->pending_write &&
			stream->queued_writes < stream->max_queued_writes)
			write_stream_enqueue_pending_write(stream);
		if (stream->queued_writes > 0)
			write_stream_do_highest_priority_write(stream, false);
	}

	Assert(stream->pending_write == NULL);
	Assert(stream->queued_writes == 0);
}

/*
 * Finish all writes and release all resources.
 */
void
write_stream_end(WriteStream *stream)
{
	write_stream_wait_all(stream);
	pfree(stream);
}
