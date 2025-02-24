/*-------------------------------------------------------------------------
 *
 * aio_init.c
 *    AIO - Subsystem Initialization
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/aio_init.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/aio_subsys.h"
#include "storage/io_worker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/guc.h"



static Size
AioCtlShmemSize(void)
{
	Size		sz;

	/* pgaio_ctl itself */
	sz = offsetof(PgAioCtl, io_handles);

	return sz;
}

static uint32
AioProcs(void)
{
	/*
	 * While AIO workers don't need their own AIO context, we can't currently
	 * guarantee nothing gets assigned to the a ProcNumber for an IO worker if
	 * we just subtracted MAX_IO_WORKERS.
	 */
	return MaxBackends + NUM_AUXILIARY_PROCS;
}

static Size
AioBackendShmemSize(void)
{
	return mul_size(AioProcs(), sizeof(PgAioBackend));
}

static Size
AioHandleShmemSize(void)
{
	Size		sz;

	/* ios */
	sz = mul_size(AioProcs(),
				  mul_size(io_max_concurrency, sizeof(PgAioHandle)));

	return sz;
}

static Size
AioHandleIOVShmemSize(void)
{
	return mul_size(sizeof(struct iovec),
					mul_size(mul_size(PG_IOV_MAX, AioProcs()),
							 io_max_concurrency));
}

static Size
AioHandleDataShmemSize(void)
{
	return mul_size(sizeof(uint64),
					mul_size(mul_size(PG_IOV_MAX, AioProcs()),
							 io_max_concurrency));
}

static Size
AioBounceBufferDescShmemSize(void)
{
	Size		sz;

	/* PgAioBounceBuffer itself */
	sz = mul_size(sizeof(PgAioBounceBuffer),
				  mul_size(AioProcs(), io_bounce_buffers));

	return sz;
}

static Size
AioBounceBufferDataShmemSize(void)
{
	Size		sz;

	/* and the associated buffer */
	sz = mul_size(BLCKSZ,
				  mul_size(io_bounce_buffers, AioProcs()));
	/* memory for alignment */
	sz += BLCKSZ;

	return sz;
}

/*
 * Choose a suitable value for io_max_concurrency.
 *
 * It's unlikely that we could have more IOs in flight than buffers that we
 * would be allowed to pin.
 *
 * On the upper end, apply a cap too - just because shared_buffers is large,
 * it doesn't make sense have millions of buffers undergo IO concurrently.
 */
static int
AioChooseMaxConcurrency(void)
{
	uint32		max_backends;
	int			max_proportional_pins;

	/* Similar logic to LimitAdditionalPins() */
	max_backends = MaxBackends + NUM_AUXILIARY_PROCS;
	max_proportional_pins = NBuffers / max_backends;

	max_proportional_pins = Max(max_proportional_pins, 1);

	/* apply upper limit */
	return Min(max_proportional_pins, 64);
}

/*
 * Choose a suitable value for io_bounce_buffers.
 *
 * It's very unlikely to be useful to allocate more bounce buffers for each
 * backend than the backend is allowed to pin. Additionally, bounce buffers
 * currently are used for writes, it seems very uncommon for more than 10% of
 * shared_buffers to be written out concurrently.
 *
 * XXX: This quickly can take up significant amounts of memory, the logic
 * should probably fine tuned.
 */
static int
AioChooseBounceBuffers(void)
{
	uint32		max_backends;
	int			max_proportional_pins;

	/* Similar logic to LimitAdditionalPins() */
	max_backends = MaxBackends + NUM_AUXILIARY_PROCS;
	max_proportional_pins = (NBuffers / 10) / max_backends;

	max_proportional_pins = Max(max_proportional_pins, 1);

	/* apply upper limit */
	return Min(max_proportional_pins, 256);
}

Size
AioShmemSize(void)
{
	Size		sz = 0;

	/*
	 * We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
	 * However, if the DBA explicitly set wal_buffers = -1 in the config file,
	 * then PGC_S_DYNAMIC_DEFAULT will fail to override that and we must force
	 *
	 */
	if (io_max_concurrency == -1)
	{
		char		buf[32];

		snprintf(buf, sizeof(buf), "%d", AioChooseMaxConcurrency());
		SetConfigOption("io_max_concurrency", buf, PGC_POSTMASTER,
						PGC_S_DYNAMIC_DEFAULT);
		if (io_max_concurrency == -1)	/* failed to apply it? */
			SetConfigOption("io_max_concurrency", buf, PGC_POSTMASTER,
							PGC_S_OVERRIDE);
	}


	/*
	 * If io_bounce_buffers is -1, we automatically choose a suitable value.
	 *
	 * See also comment above.
	 */
	if (io_bounce_buffers == -1)
	{
		char		buf[32];

		snprintf(buf, sizeof(buf), "%d", AioChooseBounceBuffers());
		SetConfigOption("io_bounce_buffers", buf, PGC_POSTMASTER,
						PGC_S_DYNAMIC_DEFAULT);
		if (io_bounce_buffers == -1)	/* failed to apply it? */
			SetConfigOption("io_bounce_buffers", buf, PGC_POSTMASTER,
							PGC_S_OVERRIDE);
	}

	sz = add_size(sz, AioCtlShmemSize());
	sz = add_size(sz, AioBackendShmemSize());
	sz = add_size(sz, AioHandleShmemSize());
	sz = add_size(sz, AioHandleIOVShmemSize());
	sz = add_size(sz, AioHandleDataShmemSize());
	sz = add_size(sz, AioBounceBufferDescShmemSize());
	sz = add_size(sz, AioBounceBufferDataShmemSize());

	if (pgaio_method_ops->shmem_size)
		sz = add_size(sz, pgaio_method_ops->shmem_size());

	return sz;
}

void
AioShmemInit(void)
{
	bool		found;
	uint32		io_handle_off = 0;
	uint32		iovec_off = 0;
	uint32		per_backend_iovecs = io_max_concurrency * PG_IOV_MAX;
	uint32		bounce_buffers_off = 0;
	uint32		per_backend_bb = io_bounce_buffers;
	char	   *bounce_buffers_data;

	pgaio_ctl = (PgAioCtl *)
		ShmemInitStruct("AioCtl", AioCtlShmemSize(), &found);

	if (found)
		goto out;

	memset(pgaio_ctl, 0, AioCtlShmemSize());

	pgaio_ctl->io_handle_count = AioProcs() * io_max_concurrency;
	pgaio_ctl->iovec_count = AioProcs() * per_backend_iovecs;
	pgaio_ctl->bounce_buffers_count = AioProcs() * per_backend_bb;

	pgaio_ctl->backend_state = (PgAioBackend *)
		ShmemInitStruct("AioBackend", AioBackendShmemSize(), &found);

	pgaio_ctl->io_handles = (PgAioHandle *)
		ShmemInitStruct("AioHandle", AioHandleShmemSize(), &found);

	pgaio_ctl->iovecs = (struct iovec *)
		ShmemInitStruct("AioHandleIOV", AioHandleIOVShmemSize(), &found);
	pgaio_ctl->handle_data = (uint64 *)
		ShmemInitStruct("AioHandleData", AioHandleDataShmemSize(), &found);

	pgaio_ctl->bounce_buffers = (PgAioBounceBuffer *)
		ShmemInitStruct("AioBounceBufferDesc", AioBounceBufferDescShmemSize(),
						&found);

	bounce_buffers_data =
		ShmemInitStruct("AioBounceBufferData", AioBounceBufferDataShmemSize(),
						&found);
	bounce_buffers_data =
		(char *) TYPEALIGN(BLCKSZ, (uintptr_t) bounce_buffers_data);
	pgaio_ctl->bounce_buffers_data = bounce_buffers_data;


	/* Initialize IO handles. */
	for (uint64 i = 0; i < pgaio_ctl->io_handle_count; i++)
	{
		PgAioHandle *ioh = &pgaio_ctl->io_handles[i];

		ioh->op = PGAIO_OP_INVALID;
		ioh->target = PGAIO_TID_INVALID;
		ioh->state = PGAIO_HS_IDLE;

		slist_init(&ioh->bounce_buffers);
	}

	/* Initialize Bounce Buffers. */
	for (uint64 i = 0; i < pgaio_ctl->bounce_buffers_count; i++)
	{
		PgAioBounceBuffer *bb = &pgaio_ctl->bounce_buffers[i];

		bb->buffer = bounce_buffers_data;
		bounce_buffers_data += BLCKSZ;
	}


	for (int procno = 0; procno < AioProcs(); procno++)
	{
		PgAioBackend *bs = &pgaio_ctl->backend_state[procno];

		bs->io_handle_off = io_handle_off;
		io_handle_off += io_max_concurrency;

		bs->bounce_buffers_off = bounce_buffers_off;
		bounce_buffers_off += per_backend_bb;

		dclist_init(&bs->idle_ios);
		memset(bs->staged_ios, 0, sizeof(PgAioHandle *) * PGAIO_SUBMIT_BATCH_SIZE);
		dclist_init(&bs->in_flight_ios);
		slist_init(&bs->idle_bbs);

		/* initialize per-backend IOs */
		for (int i = 0; i < io_max_concurrency; i++)
		{
			PgAioHandle *ioh = &pgaio_ctl->io_handles[bs->io_handle_off + i];

			ioh->generation = 1;
			ioh->owner_procno = procno;
			ioh->iovec_off = iovec_off;
			ioh->handle_data_len = 0;
			ioh->report_return = NULL;
			ioh->resowner = NULL;
			ioh->num_shared_callbacks = 0;
			ioh->distilled_result.status = ARS_UNKNOWN;
			ioh->flags = 0;

			ConditionVariableInit(&ioh->cv);

			dclist_push_tail(&bs->idle_ios, &ioh->node);
			iovec_off += PG_IOV_MAX;
		}

		/* initialize per-backend bounce buffers */
		for (int i = 0; i < per_backend_bb; i++)
		{
			PgAioBounceBuffer *bb = &pgaio_ctl->bounce_buffers[bs->bounce_buffers_off + i];

			slist_push_head(&bs->idle_bbs, &bb->node);
		}
	}

out:
	/* Initialize IO method specific resources. */
	if (pgaio_method_ops->shmem_init)
		pgaio_method_ops->shmem_init(!found);
}

void
pgaio_init_backend(void)
{
	/* shouldn't be initialized twice */
	Assert(!pgaio_my_backend);

	if (MyBackendType == B_IO_WORKER)
		return;

	if (MyProc == NULL || MyProcNumber >= AioProcs())
		elog(ERROR, "aio requires a normal PGPROC");

	pgaio_my_backend = &pgaio_ctl->backend_state[MyProcNumber];

	if (pgaio_method_ops->init_backend)
		pgaio_method_ops->init_backend();

	before_shmem_exit(pgaio_shutdown, 0);
}
