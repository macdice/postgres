//#define ILIST_DEBUG 1

#include "postgres.h"

#include "storage/aio.h"
#include "miscadmin.h"

/* typedef is in header */
typedef struct oustanding_write
{
	PgAioInProgress *aio;
	bool in_progress;
	void *private_data;
} outstanding_write;

struct pg_streaming_write
{
	uint32 iodepth;

	uint32 head;
	uint32 inflight;

	void *private_data;
	//WritebackContext *wb_context;

	pg_streaming_write_completed on_completion;

	outstanding_write outstanding_writes[];
};

pg_streaming_write*
pg_streaming_write_alloc(uint32 iodepth, void *private_data, pg_streaming_write_completed on_completion)
{
	pg_streaming_write *psw;

	iodepth = Max(Min(iodepth, NBuffers / 128), 1);

	psw = palloc0(offsetof(pg_streaming_write, outstanding_writes)
				 + sizeof(outstanding_write) * iodepth);

	psw->iodepth = iodepth;
	psw->private_data = private_data;
	psw->on_completion = on_completion;

	return psw;
}

PgAioInProgress *
pg_streaming_write_get_io(pg_streaming_write *pgsw)
{
	uint32		off = (pgsw->head) % pgsw->iodepth;
	outstanding_write *this_write = &pgsw->outstanding_writes[off];

	if (this_write->in_progress)
	{
		pg_streaming_write_wait(pgsw, 1);
		pgaio_io_recycle(this_write->aio);
	}

	if (!this_write->aio)
	{
		this_write->aio = pgaio_io_get();
	}
	else
	{
		pgaio_io_recycle(this_write->aio);
	}

	Assert(!this_write->in_progress);

	return this_write->aio;
}

uint32
pg_streaming_write_inflight(pg_streaming_write *pgsw)
{
	return pgsw->inflight;
}

void
pg_streaming_write_write(pg_streaming_write *pgsw, PgAioInProgress *io, void *private_data)
{
	uint32		off = (pgsw->head++) % pgsw->iodepth;
	outstanding_write *this_write = &pgsw->outstanding_writes[off];

	Assert(this_write->aio && this_write->aio == io);
	Assert(!this_write->in_progress);
	Assert(!this_write->private_data);

	this_write->private_data = private_data;
	this_write->in_progress = true;

	pgsw->inflight++;

	/*
	 * XXX: It'd make sense to trigger io submission more often.
	 */
}

void
pg_streaming_write_wait(pg_streaming_write *pgsw, uint32 wait_for)
{
	Assert(wait_for <= pgsw->iodepth);

	for (uint32 i = 0; i < pgsw->iodepth; i++, wait_for--)
	{
		uint32 off = (i + pgsw->head) % pgsw->iodepth;
		outstanding_write *this_write = &pgsw->outstanding_writes[off];

		if (!this_write->in_progress)
			continue;

		if (wait_for > 0)
			pgaio_io_wait(this_write->aio, true);
		else if (!pgaio_io_done(this_write->aio))
			break;

		pgsw->inflight--;
		this_write->in_progress = false;
		pgaio_io_recycle(this_write->aio);

		pgsw->on_completion(pgsw->private_data, this_write->private_data);
		this_write->private_data = NULL;
	}
}

void
pg_streaming_write_wait_all(pg_streaming_write *pgsw)
{
	if (pgsw->inflight > 0)
		pg_streaming_write_wait(pgsw, pgsw->iodepth);
	Assert(pgsw->inflight == 0);
}

void
pg_streaming_write_free(pg_streaming_write *pgsw)
{
	for (uint32 off = 0; off < pgsw->iodepth; off++)
	{
		outstanding_write *this_write = &pgsw->outstanding_writes[off];

		Assert(!this_write->in_progress);
		if (this_write->aio)
			pgaio_io_release(this_write->aio);
		this_write->aio = NULL;
	}

	pfree(pgsw);
}

typedef struct PgStreamingReadItem
{
	/* membership in PgStreamingRead->issued, PgStreamingRead->available */
	dlist_node node;
	/* membership in PgStreamingRead->in_order */
	dlist_node sequence_node;

	PgAioOnCompletionLocalContext on_completion;
	PgStreamingRead *pgsr;
	PgAioInProgress *aio;

	/* is this IO still considered to be in progress */
	bool in_progress;
	/* is this item currently valid / used */
	bool valid;
	uintptr_t read_private;
} PgStreamingReadItem;

struct PgStreamingRead
{
	uint32 iodepth_max;
	uint32 distance_max;
	uint32 all_items_count;

	uintptr_t pgsr_private;
	PgStreamingReadDetermineNextCB determine_next_cb;
	PgStreamingReadRelease release_cb;

	uint32 current_window;

	/* number of requests issued */
	uint64 prefetched_total_count;
	/* number of requests submitted to kernel */
	uint64 submitted_total_count;
	/* number of current requests completed */
	uint32 completed_count;
	/* number of current requests in flight */
	int32 inflight_count;
	/* number of requests not yet submitted */
	int32 pending_count;
	/* number of requests that didn't require IO (debugging only) */
	int32 no_io_count;

	bool hit_end;

	/* submitted reads */
	dlist_head issued;

	/* available reads (unused or completed) */
	dlist_head available;

	/*
	 * IOs, be they completed or in progress, in the order that the callback
	 * returned them.
	 */
	dlist_head in_order;

	PgStreamingReadItem all_items[];
};

static void pg_streaming_read_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);
static void pg_streaming_read_prefetch(PgStreamingRead *pgsr);

PgStreamingRead *
pg_streaming_read_alloc(uint32 iodepth, uintptr_t pgsr_private,
						PgStreamingReadDetermineNextCB determine_next_cb,
						PgStreamingReadRelease release_cb)
{
	PgStreamingRead *pgsr;

	iodepth = Max(Min(iodepth, NBuffers / 128), 1);

	pgsr = palloc0(offsetof(PgStreamingRead, all_items) +
				   sizeof(PgStreamingReadItem) * iodepth * 2);

	pgsr->iodepth_max = iodepth;
	pgsr->distance_max = iodepth;
	pgsr->all_items_count = pgsr->iodepth_max + pgsr->distance_max;
	pgsr->pgsr_private = pgsr_private;
	pgsr->determine_next_cb = determine_next_cb;
	pgsr->release_cb = release_cb;

	pgsr->current_window = 0;

	dlist_init(&pgsr->available);
	dlist_init(&pgsr->in_order);
	dlist_init(&pgsr->issued);

	for (int i = 0; i < pgsr->all_items_count; i++)
	{
		PgStreamingReadItem *this_read = &pgsr->all_items[i];

		this_read->on_completion.callback = pg_streaming_read_complete;
		this_read->pgsr = pgsr;
		dlist_push_tail(&pgsr->available, &this_read->node);
	}

	return pgsr;
}

void
pg_streaming_read_free(PgStreamingRead *pgsr)
{
	for (int i = 0; i < pgsr->all_items_count; i++)
	{
		PgStreamingReadItem *this_read = &pgsr->all_items[i];

		if (this_read->in_progress)
		{
			Assert(this_read->valid);
			pgaio_io_wait(this_read->aio, true);
			Assert(!this_read->in_progress);
		}

		if (this_read->valid)
			pgsr->release_cb(pgsr->pgsr_private, this_read->read_private);

		if (this_read->aio)
		{
			pgaio_io_release(this_read->aio);
			this_read->aio = NULL;
		}
	}

	pfree(pgsr);
}

static void
pg_streaming_read_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io)
{
	PgStreamingReadItem *this_read = pgaio_ocb_container(PgStreamingReadItem, on_completion, ocb);
	PgStreamingRead *pgsr = this_read->pgsr;

#if 0
	if ((pgsr->prefetched_total_count % 10000) == 0)
		ereport(LOG, errmsg("pgsr read completed: qd %d completed: %d",
							pgsr->inflight_count, pgsr->completed_count),
				errhidestmt(true),
				errhidecontext(true));
#endif

	Assert(this_read->in_progress);
	Assert(this_read->valid);
	Assert(pgsr->inflight_count > 0);
	Assert(pgaio_io_done(io));
	Assert(pgaio_io_success(io));

	dlist_delete_from(&pgsr->issued, &this_read->node);
	pgsr->inflight_count--;
	pgsr->completed_count++;
	this_read->in_progress = false;
	pgaio_io_recycle(this_read->aio);

	pg_streaming_read_prefetch(pgsr);
}

static void
pg_streaming_read_prefetch_one(PgStreamingRead *pgsr)
{
	PgStreamingReadItem *this_read;
	PgStreamingReadNextStatus status;

	Assert(!dlist_is_empty(&pgsr->available));

	this_read = dlist_container(PgStreamingReadItem, node, dlist_pop_head_node(&pgsr->available));
	Assert(!this_read->valid);
	Assert(!this_read->in_progress);
	Assert(this_read->read_private == 0);

	if (this_read->aio == NULL)
	{
		this_read->aio = pgaio_io_get();
	}

	pgaio_io_on_completion_local(this_read->aio, &this_read->on_completion);
	this_read->in_progress = true;
	this_read->valid = true;
	dlist_push_tail(&pgsr->issued, &this_read->node);
	dlist_push_tail(&pgsr->in_order, &this_read->sequence_node);
	pgsr->pending_count++;
	pgsr->inflight_count++;
	pgsr->prefetched_total_count++;

	status = pgsr->determine_next_cb(pgsr->pgsr_private, this_read->aio, &this_read->read_private);

	if (status == PGSR_NEXT_END)
	{
		pgsr->pending_count--;
		pgsr->inflight_count--;
		pgsr->prefetched_total_count--;
		pgsr->hit_end = true;
		this_read->read_private = 0;
		this_read->valid = false;
		this_read->in_progress = false;
		pgaio_io_recycle(this_read->aio);
		dlist_delete_from(&pgsr->in_order, &this_read->sequence_node);
		dlist_push_tail(&pgsr->available, &this_read->node);
	}
	else if (status == PGSR_NEXT_NO_IO)
	{
		Assert(this_read->read_private != 0);
		pgsr->pending_count--;
		pgsr->inflight_count--;
		pgsr->no_io_count++;
		pgsr->completed_count++;
		this_read->in_progress = false;
		pgaio_io_recycle(this_read->aio);
		dlist_delete_from(&pgsr->issued, &this_read->node);
	}
	else
	{
		Assert(this_read->read_private != 0);
	}
}

static void
pg_streaming_read_prefetch(PgStreamingRead *pgsr)
{
	uint32 min_issue;

	if (pgsr->hit_end)
		return;

	Assert(pgsr->inflight_count <= pgsr->current_window);
	Assert(pgsr->completed_count <= (pgsr->iodepth_max + pgsr->distance_max));

	/*
	 * XXX: Some issues:
	 *
	 * - We should probably do the window calculation based on the number of
	 *   buffers the user actually requested, i.e. only recompute this
	 *   whenever pg_streaming_read_get_next() is called. Otherwise we will
	 *   always read the whole prefetch window.
	 *
	 * - The algorithm here is pretty stupid. Should take distance / iodepth
	 *   properly into account in a distitcn way. Grow slower.
	 *
	 * - It'd be good to have a usage dependent iodepth management. After an
	 *   initial increase, we should only increase further if the the user the
	 *   window proves too small (i.e. we don't manage to keep 'completed'
	 *   close to full).
	 *
	 * - If most requests don't trigger IO, we should probably reduce the
	 *   prefetch window.
	 */
	if (pgsr->current_window < pgsr->iodepth_max)
	{
		if (pgsr->current_window == 0)
			pgsr->current_window = 4;
		else
			pgsr->current_window *= 2;

		if (pgsr->current_window > pgsr->iodepth_max)
			pgsr->current_window = pgsr->iodepth_max;

		min_issue = 1;
	}
	else
	{
		min_issue = Min(pgsr->iodepth_max, pgsr->current_window / 4);
	}

	Assert(pgsr->inflight_count <= pgsr->current_window);
	Assert(pgsr->completed_count <= (pgsr->iodepth_max + pgsr->distance_max));

	if (pgsr->completed_count >= pgsr->current_window)
		return;

	if (pgsr->inflight_count >= pgsr->current_window)
		return;

	while (!pgsr->hit_end &&
		   (pgsr->inflight_count < pgsr->current_window) &&
		   (pgsr->completed_count < pgsr->current_window))
	{
		pg_streaming_read_prefetch_one(pgsr);

		if (pgsr->pending_count >= min_issue)
		{
			pgsr->submitted_total_count += pgsr->pending_count;
			pgsr->pending_count = 0;
			pgaio_submit_pending(true);
		}
	}

	if (pgsr->pending_count >= min_issue)
	{
		pgsr->submitted_total_count += pgsr->pending_count;
		pgsr->pending_count = 0;
		pgaio_submit_pending(true);
	}
}

uintptr_t
pg_streaming_read_get_next(PgStreamingRead *pgsr)
{
	if (pgsr->prefetched_total_count == 0)
	{
		pg_streaming_read_prefetch(pgsr);
		Assert(pgsr->hit_end || pgsr->prefetched_total_count > 0);
	}

	if (dlist_is_empty(&pgsr->in_order))
	{
		Assert(pgsr->hit_end);
		return 0;
	}
	else
	{
		PgStreamingReadItem *this_read;
		uint64_t ret;

		Assert(pgsr->prefetched_total_count > 0);

		this_read = dlist_container(PgStreamingReadItem, sequence_node,
									dlist_pop_head_node(&pgsr->in_order));
		Assert(this_read->valid);

		if (this_read->in_progress)
		{
			pgaio_io_wait(this_read->aio, true);
			/* callback should have updated */
			Assert(!this_read->in_progress);
			Assert(this_read->valid);
		}

		Assert(this_read->read_private != 0);
		ret = this_read->read_private;
		this_read->read_private = 0;
		this_read->valid = false;

		pgsr->completed_count--;
		dlist_push_tail(&pgsr->available, &this_read->node);
		pg_streaming_read_prefetch(pgsr);

		return ret;
	}
}
