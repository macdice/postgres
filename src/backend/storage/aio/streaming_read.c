#include "postgres.h"

#include "storage/streaming_read.h"
#include "utils/rel.h"

/*
 * Element type for PgStreamingRead's circular array of clusters of buffers.
 *
 * For hits and RBM_WILL_ZERO, need_to_complete is false, we have just one
 * buffer in each cluster, already pinned and ready for use.
 *
 * For misses that require a physical read, need_to_complete is true, and
 * buffers[] holds a group of of neighboring blocks, so we can complete them
 * with a single call to CompleteReadBuffers().  We can also issue a single
 * prefetch for it as soon as it has grown to its largest possible size, if
 * our random access heuristics determine that is a good idea.
 */
typedef struct PgStreamingReadCluster
{
	bool		prefetch_issued;
	bool		need_to_complete;

	BufferManagerRelation bmr;
	ForkNumber	forknum;
	BlockNumber blocknum;
	int			nblocks;

	BufferAccessStrategy strategy;

	bool		prefetch_hint[MAX_BUFFERS_PER_TRANSFER];
	Buffer		buffers[MAX_BUFFERS_PER_TRANSFER];
} PgStreamingReadCluster;

struct PgStreamingRead
{
	int			max_ios;
	int			ios_in_progress;
	int			ios_in_progress_trigger;
	int			max_pinned_buffers;
	int			pinned_buffers;
	int			pinned_buffers_trigger;
	int			next_tail_buffer;
	bool		finished;
	uintptr_t	pgsr_private;
	PgStreamingReadBufferDetermineNextCB next_cb;
	BufferAccessStrategy strategy;

	/* Next expected prefetch, for sequential prefetch avoidance. */
	BufferManagerRelation seq_bmr;
	ForkNumber	seq_forknum;
	BlockNumber	seq_blocknum;

	/* Circular buffer of clusters. */
	int			size;
	int			head;
	int			tail;
	PgStreamingReadCluster clusters[FLEXIBLE_ARRAY_MEMBER];
};

PgStreamingRead *
pg_streaming_read_buffer_alloc(int max_ios,
							   uintptr_t pgsr_private,
							   BufferAccessStrategy strategy,
							   PgStreamingReadBufferDetermineNextCB determine_next_cb)
{
	PgStreamingRead *pgsr;
	int			size;
	int			max_pinned_buffers;

	Assert(max_ios > 0);

	/* XXX */
	max_pinned_buffers = max_ios * 2;

	size = Max(max_ios, max_pinned_buffers) + 1;

	pgsr = (PgStreamingRead *)
		palloc0(offsetof(PgStreamingRead, clusters) +
				sizeof(pgsr->clusters[0]) * size);

	pgsr->max_ios = max_ios;
	pgsr->max_pinned_buffers = max_pinned_buffers;
	pgsr->pgsr_private = pgsr_private;
	pgsr->strategy = strategy;
	pgsr->next_cb = determine_next_cb;
	pgsr->size = size;

	/*
	 * Also start prefetching when there is still space to fill up a full sized
	 * run of consecutive buffers at once.  (If we left it any later, we'd
	 * never be able to build up a range of MAX_BUFFERS_PER_TRANSFER at once).
	 */
	pgsr->pinned_buffers_trigger =
		Max(max_ios, max_pinned_buffers - MAX_BUFFERS_PER_TRANSFER);

	return pgsr;
}

void
pg_streaming_read_prefetch(PgStreamingRead *pgsr)
{
	/* If we're finished or can't start one more I/O, then no prefetching. */
	if (pgsr->finished || pgsr->ios_in_progress == pgsr->max_ios)
		return;

	/*
	 * We'll also wait until we have a chance to pin a lot of buffers at once,
	 * so that we have the chance to create a large scatter read.
	 */
	if (pgsr->pinned_buffers >= pgsr->pinned_buffers_trigger)
		return;

	do
	{
		BufferManagerRelation bmr;
		ForkNumber forknum;
		BlockNumber blocknum;
		ReadBufferMode mode;
		Buffer buffer;
		bool found;
		bool allocated;
		PgStreamingReadCluster *head_cluster;

		/*
		 * Try to find out which block the callback wants to read next.  False
		 * indicates end-of-stream (but the client can restart).
		 */
		if (!pgsr->next_cb(pgsr, pgsr->pgsr_private, &bmr, &forknum, &blocknum,
						   &mode))
		{
			pgsr->finished = true;
			return;
		}

		Assert(mode == RBM_NORMAL || mode == RBM_WILL_ZERO);

		buffer = PrepareReadBuffer(bmr,
								   forknum,
								   blocknum,
								   pgsr->strategy,
								   mode,
								   &found,
								   &allocated);
		pgsr->pinned_buffers++;

		/*
		 * See if we have to create a new cluster.  The point of a cluster is
		 * to coalesce neighboring reads, so if we don't need to read or it's
		 * not consecutive, we'll start a new one.
		 */
		head_cluster = &pgsr->clusters[pgsr->head];
		if (found ||
			mode == RBM_WILL_ZERO ||
			pgsr->head == pgsr->tail ||
			!head_cluster->need_to_complete ||
			head_cluster->nblocks == lengthof(head_cluster->buffers) ||
			head_cluster->bmr.smgr != bmr.smgr ||
			head_cluster->bmr.rel != bmr.rel ||
			head_cluster->forknum != forknum ||
			head_cluster->blocknum + head_cluster->nblocks != blocknum)
		{
#ifdef USE_PREFETCH
			/*
			 * Issue prefetch if using buffered I/O, we were the first to
			 * allocate a buffer, we expect to perform a read, and this
			 * doesn't seem to be sequential.
			 */
			if (pgsr->head != pgsr->tail &&
				head_cluster->need_to_complete &&
				(io_direct_flags & IO_DIRECT_DATA) == 0 &&
				(head_cluster->bmr.smgr != pgsr->seq_bmr.smgr ||
				 head_cluster->bmr.rel != pgsr->seq_bmr.rel ||
				 head_cluster->forknum != pgsr->seq_forknum ||
				 head_cluster->blocknum != pgsr->seq_blocknum))
			{
				SMgrRelation smgr =
					head_cluster->bmr.smgr ? head_cluster->bmr.smgr
					: RelationGetSmgr(head_cluster->bmr.rel);

				for (int i = 0; i < head_cluster->nblocks; i++)
				{
					if (head_cluster->prefetch_hint[i])
					{
						BlockNumber first_blocknum = head_cluster->blocknum + i;
						int nblocks = 1;

						/*
						 * How many adjacent blocks can we merge with to
						 * reduce system calls?  Usually this is all of them,
						 * unless there are overlapping streaming reads.
						 */
						while ((i + 1) < head_cluster->nblocks &&
							   head_cluster->prefetch_hint[i + 1])
						{
							nblocks++;
							i++;
						}

						smgrprefetch(smgr,
									 head_cluster->forknum,
									 first_blocknum,
									 nblocks);
					}

					/*
					 * Remember where the next block would be, so we can avoid
					 * issuing purely sequential prefetch hints.  We expect
					 * the kernel to do a better job at that than we can.
					 */
					pgsr->seq_bmr = head_cluster->bmr;
					pgsr->seq_forknum = head_cluster->forknum;
					pgsr->seq_blocknum = head_cluster->blocknum + head_cluster->nblocks;
				}

				/* Count this as an I/O that is concurrently in progress. */
				head_cluster->prefetch_issued = true;
				pgsr->ios_in_progress++;
			}
#endif

			/* Create a new head cluster.  There must be space. */
			Assert(pgsr->size > pgsr->max_pinned_buffers);
			Assert((pgsr->head + 1) % pgsr->size != pgsr->tail);
			pgsr->head = (pgsr->head + 1) % pgsr->size;
			head_cluster = &pgsr->clusters[pgsr->head];

			/* Initially it holds just this buffer. */
			head_cluster->bmr = bmr;
			head_cluster->forknum = forknum;
			head_cluster->blocknum = blocknum;
			head_cluster->nblocks = 1;
			head_cluster->prefetch_hint[0] = allocated;
			head_cluster->buffers[0] = buffer;
			head_cluster->prefetch_issued = false;

			/*
			 * Will we need to 'complete' this cluster before returning its
			 * buffers?
			 */
			if (mode != RBM_WILL_ZERO && found)
				head_cluster->need_to_complete = false;
			else
				head_cluster->need_to_complete = true;
		}
		else
		{
			/* Extend the existing cluster by one block. */
			head_cluster->prefetch_hint[head_cluster->nblocks] = allocated;
			head_cluster->buffers[head_cluster->nblocks++] = buffer;
		}
	} while (pgsr->ios_in_progress < pgsr->max_ios &&
			 pgsr->pinned_buffers < pgsr->max_pinned_buffers);
}

void
pg_streaming_read_reset(PgStreamingRead *pgsr)
{
	pgsr->finished = false;
}

uintptr_t
pg_streaming_read_get_next(PgStreamingRead *pgsr)
{
	pg_streaming_read_prefetch(pgsr);

	/* See if we have one buffer to return. */
	while (pgsr->tail != pgsr->head ||
		   pgsr->next_tail_buffer < pgsr->clusters[pgsr->tail].nblocks)
	{
		PgStreamingReadCluster *tail_cluster;

		tail_cluster = &pgsr->clusters[pgsr->tail];

		/*
		 * Do we need to perform an I/O before returning the buffers from this
		 * cluster?
		 */
		if (tail_cluster->need_to_complete)
		{
			CompleteReadBuffers(tail_cluster->bmr,
								tail_cluster->buffers,
								tail_cluster->forknum,
								tail_cluster->blocknum,
								tail_cluster->nblocks,
								tail_cluster->strategy);
			tail_cluster->need_to_complete = false;

			/* We only count I/O depth if we issue prefetch hints. */
			if (tail_cluster->prefetch_issued)
				pgsr->ios_in_progress--;
		}

		/* Are there more buffers available in this cluster? */
		if (pgsr->next_tail_buffer < tail_cluster->nblocks)
		{
			/* We are giving away ownership of this pinned buffer. */
			pgsr->pinned_buffers--;

			return tail_cluster->buffers[pgsr->next_tail_buffer++];
		}

		/* Advance tail to next cluster, if there is one. */
		if (pgsr->tail != pgsr->head)
		{
			pgsr->tail = (pgsr->tail + 1) % pgsr->size;
			pgsr->next_tail_buffer = 0;
		}
	}

	return InvalidBuffer;
}

void
pg_streaming_read_free(PgStreamingRead *pgsr)
{
	Buffer buffer;

	/* Stop reading ahead, and unpin anything that wasn't consumed. */
	pgsr->finished = true;
	for (;;)
	{
		buffer = pg_streaming_read_get_next(pgsr);
		if (buffer == InvalidBuffer)
			break;
		ReleaseBuffer(buffer);
	}

	pfree(pgsr);
}

int
pg_streaming_read_inflight(PgStreamingRead *pgsr)
{
	return pgsr->ios_in_progress;
}

int
pg_streaming_read_completed(PgStreamingRead *pgsr)
{
	/*
	 * XXX this isn't quite right, because ios_in_progress is merged I/Os, but
	 * it'll do for now!
	 */
	return pgsr->pinned_buffers - pgsr->ios_in_progress;
}
