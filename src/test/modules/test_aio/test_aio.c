/*-------------------------------------------------------------------------
 *
 * delay_execution.c
 *		Test module to allow delay between parsing and execution of a query.
 *
 * The delay is implemented by taking and immediately releasing a specified
 * advisory lock.  If another process has previously taken that lock, the
 * current process will be blocked until the lock is released; otherwise,
 * there's no effect.  This allows an isolationtester script to reliably
 * test behaviors where some specified action happens in another backend
 * between parsing and execution of any desired query.
 *
 * Copyright (c) 2020-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/delay_execution/delay_execution.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "fmgr.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/rel.h"


PG_MODULE_MAGIC;


typedef struct InjIoErrorState
{
	bool		enabled_short_read;
	bool		enabled_reopen;

	bool		short_read_result_set;
	int			short_read_result;
}			InjIoErrorState;

static InjIoErrorState * inj_io_error_state;

/* Shared memory init callbacks */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;


static PgAioHandle *last_handle;



static void
test_aio_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(sizeof(InjIoErrorState));
}

static void
test_aio_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	inj_io_error_state = ShmemInitStruct("injection_points",
										 sizeof(InjIoErrorState),
										 &found);

	if (!found)
	{
		/*
		 * First time through, so initialize.  This is shared with the dynamic
		 * initialization using a DSM.
		 */
		inj_io_error_state->enabled_short_read = false;
		inj_io_error_state->enabled_reopen = false;

#ifdef USE_INJECTION_POINTS
		InjectionPointAttach("AIO_PROCESS_COMPLETION_BEFORE_SHARED",
							 "test_aio",
							 "inj_io_short_read",
							 NULL,
							 0);
		InjectionPointLoad("AIO_PROCESS_COMPLETION_BEFORE_SHARED");

		InjectionPointAttach("AIO_WORKER_AFTER_REOPEN",
							 "test_aio",
							 "inj_io_reopen",
							 NULL,
							 0);
		InjectionPointLoad("AIO_WORKER_AFTER_REOPEN");

#endif
	}
	else
	{
#ifdef USE_INJECTION_POINTS
		InjectionPointLoad("AIO_PROCESS_COMPLETION_BEFORE_SHARED");
		InjectionPointLoad("AIO_WORKER_AFTER_REOPEN");
		elog(LOG, "injection point loaded");
#endif
	}

	LWLockRelease(AddinShmemInitLock);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Shared memory initialization */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = test_aio_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = test_aio_shmem_startup;
}


PG_FUNCTION_INFO_V1(errno_from_string);
Datum
errno_from_string(PG_FUNCTION_ARGS)
{
	const char *sym = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (strcmp(sym, "EIO") == 0)
		PG_RETURN_INT32(EIO);
	else if (strcmp(sym, "EAGAIN") == 0)
		PG_RETURN_INT32(EAGAIN);
	else if (strcmp(sym, "EINTR") == 0)
		PG_RETURN_INT32(EINTR);
	else if (strcmp(sym, "ENOSPC") == 0)
		PG_RETURN_INT32(ENOSPC);
	else if (strcmp(sym, "EROFS") == 0)
		PG_RETURN_INT32(EROFS);

	ereport(ERROR,
			errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg_internal("%s is not a supported errno value", sym));
	PG_RETURN_INT32(0);
}


PG_FUNCTION_INFO_V1(grow_rel);
Datum
grow_rel(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	uint32		nblocks = PG_GETARG_UINT32(1);
	Relation	rel;
#define MAX_BUFFERS_TO_EXTEND_BY 64
	Buffer		victim_buffers[MAX_BUFFERS_TO_EXTEND_BY];

	rel = relation_open(relid, AccessExclusiveLock);

	while (nblocks > 0)
	{
		uint32		extend_by_pages;

		extend_by_pages = Min(nblocks, MAX_BUFFERS_TO_EXTEND_BY);

		ExtendBufferedRelBy(BMR_REL(rel),
							MAIN_FORKNUM,
							NULL,
							0,
							extend_by_pages,
							victim_buffers,
							&extend_by_pages);

		nblocks -= extend_by_pages;

		for (uint32 i = 0; i < extend_by_pages; i++)
		{
			ReleaseBuffer(victim_buffers[i]);
		}
	}

	relation_close(rel, NoLock);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(corrupt_rel_block);
Datum
corrupt_rel_block(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	uint32		block = PG_GETARG_UINT32(1);
	Relation	rel;
	Buffer		buf;
	Page		page;
	PageHeader	ph;

	rel = relation_open(relid, AccessExclusiveLock);

	buf = ReadBuffer(rel, block);
	page = BufferGetPage(buf);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	MarkBufferDirty(buf);

	PageInit(page, BufferGetPageSize(buf), 0);

	ph = (PageHeader) page;
	ph->pd_special = BLCKSZ + 1;

	FlushOneBuffer(buf);

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);

	ReleaseBuffer(buf);

	EvictUnpinnedBuffer(buf);

	relation_close(rel, NoLock);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(read_corrupt_rel_block);
Datum
read_corrupt_rel_block(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	uint32		block = PG_GETARG_UINT32(1);
	Relation	rel;
	Buffer		buf;
	BufferDesc *buf_hdr;
	Page		page;
	PgAioReturn ior;
	PgAioHandle *ioh;
	PgAioWaitRef iow;
	SMgrRelation smgr;
	uint32		buf_state;

	rel = relation_open(relid, AccessExclusiveLock);

	/* read buffer without erroring out */
	buf = ReadBufferExtended(rel, MAIN_FORKNUM, block, RBM_ZERO_AND_LOCK, NULL);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);

	page = BufferGetBlock(buf);

	ioh = pgaio_io_acquire(CurrentResourceOwner, &ior);
	pgaio_io_get_wref(ioh, &iow);

	buf_hdr = GetBufferDescriptor(buf - 1);
	smgr = RelationGetSmgr(rel);

	/* FIXME: even if just a test, we should verify nobody else uses this */
	buf_state = LockBufHdr(buf_hdr);
	buf_state &= ~(BM_VALID | BM_DIRTY);
	UnlockBufHdr(buf_hdr, buf_state);

	StartBufferIO(buf_hdr, true, false);

	pgaio_io_set_handle_data_32(ioh, (uint32 *) &buf, 1);
	pgaio_io_register_callbacks(ioh, PGAIO_HCB_SHARED_BUFFER_READV);

	smgrstartreadv(ioh, smgr, MAIN_FORKNUM, block,
				   (void *) &page, 1);

	ReleaseBuffer(buf);

	pgaio_wref_wait(&iow);

	if (ior.result.status != ARS_OK)
		pgaio_result_report(ior.result, &ior.target_data,
							ior.result.status == ARS_PARTIAL ? WARNING : ERROR);

	relation_close(rel, NoLock);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(invalidate_rel_block);
Datum
invalidate_rel_block(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	uint32		block = PG_GETARG_UINT32(1);
	Relation	rel;
	PrefetchBufferResult pr;
	Buffer		buf;

	rel = relation_open(relid, AccessExclusiveLock);

	/* this is a gross hack, but there's no good API exposed */
	pr = PrefetchBuffer(rel, MAIN_FORKNUM, block);
	buf = pr.recent_buffer;
	elog(LOG, "recent: %d", buf);
	if (BufferIsValid(buf))
	{
		/* if the buffer contents aren't valid, this'll return false */
		if (ReadRecentBuffer(rel->rd_locator, MAIN_FORKNUM, block, buf))
		{
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			FlushOneBuffer(buf);
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);

			if (!EvictUnpinnedBuffer(buf))
				elog(ERROR, "couldn't unpin");
		}
	}

	relation_close(rel, AccessExclusiveLock);

	PG_RETURN_VOID();
}

#if 0
PG_FUNCTION_INFO_V1(test_unsubmitted_vs_close);
Datum
test_unsubmitted_vs_close(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	uint32		block = PG_GETARG_UINT32(1);
	Relation	rel;
	Buffer		buf;
	Page		page;
	PageHeader	ph;

	rel = relation_open(relid, AccessExclusiveLock);

	buf = ReadBufferExtended(rel, MAIN_FORKNUM, block, RBM_ZERO_AND_LOCK, NULL);

	buf = ReadBuffer(rel, block);
	page = BufferGetPage(buf);

	EvictUnpinnedBuffer(buf);

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);


	MarkBufferDirty(buf);
	ph->pd_special = BLCKSZ + 1;

	/* last_handle = pgaio_io_acquire(); */

	PG_RETURN_VOID();
}
#endif

PG_FUNCTION_INFO_V1(handle_get);
Datum
handle_get(PG_FUNCTION_ARGS)
{
	last_handle = pgaio_io_acquire(CurrentResourceOwner, NULL);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(handle_release_last);
Datum
handle_release_last(PG_FUNCTION_ARGS)
{
	if (!last_handle)
		elog(ERROR, "no handle");

	pgaio_io_release(last_handle);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(handle_get_and_error);
Datum
handle_get_and_error(PG_FUNCTION_ARGS)
{
	pgaio_io_acquire(CurrentResourceOwner, NULL);

	elog(ERROR, "as you command");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(handle_get_twice);
Datum
handle_get_twice(PG_FUNCTION_ARGS)
{
	pgaio_io_acquire(CurrentResourceOwner, NULL);
	pgaio_io_acquire(CurrentResourceOwner, NULL);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(handle_get_release);
Datum
handle_get_release(PG_FUNCTION_ARGS)
{
	PgAioHandle *handle;

	handle = pgaio_io_acquire(CurrentResourceOwner, NULL);
	pgaio_io_release(handle);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(batch_start);
Datum
batch_start(PG_FUNCTION_ARGS)
{
	pgaio_enter_batchmode();
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(batch_end);
Datum
batch_end(PG_FUNCTION_ARGS)
{
	pgaio_exit_batchmode();
	PG_RETURN_VOID();
}

#ifdef USE_INJECTION_POINTS
extern PGDLLEXPORT void inj_io_short_read(const char *name, const void *private_data);
extern PGDLLEXPORT void inj_io_reopen(const char *name, const void *private_data);

void
inj_io_short_read(const char *name, const void *private_data)
{
	PgAioHandle *ioh;

	elog(LOG, "short read called: %d", inj_io_error_state->enabled_short_read);

	if (inj_io_error_state->enabled_short_read)
	{
		ioh = pgaio_inj_io_get();

		if (inj_io_error_state->short_read_result_set)
		{
			elog(LOG, "short read, changing result from %d to %d",
				 ioh->result, inj_io_error_state->short_read_result);

			ioh->result = inj_io_error_state->short_read_result;
		}
	}
}

void
inj_io_reopen(const char *name, const void *private_data)
{
	elog(LOG, "reopen called: %d", inj_io_error_state->enabled_reopen);

	if (inj_io_error_state->enabled_reopen)
	{
		elog(ERROR, "injection point triggering failure to reopen ");
	}
}
#endif

PG_FUNCTION_INFO_V1(inj_io_short_read_attach);
Datum
inj_io_short_read_attach(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	inj_io_error_state->enabled_short_read = true;
	inj_io_error_state->short_read_result_set = !PG_ARGISNULL(0);
	if (inj_io_error_state->short_read_result_set)
		inj_io_error_state->short_read_result = PG_GETARG_INT32(0);
#else
	elog(ERROR, "injection points not supported");
#endif

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(inj_io_short_read_detach);
Datum
inj_io_short_read_detach(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	inj_io_error_state->enabled_short_read = false;
#else
	elog(ERROR, "injection points not supported");
#endif
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(inj_io_reopen_attach);
Datum
inj_io_reopen_attach(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	inj_io_error_state->enabled_reopen = true;
#else
	elog(ERROR, "injection points not supported");
#endif

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(inj_io_reopen_detach);
Datum
inj_io_reopen_detach(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	inj_io_error_state->enabled_reopen = false;
#else
	elog(ERROR, "injection points not supported");
#endif
	PG_RETURN_VOID();
}
