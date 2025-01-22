/*-------------------------------------------------------------------------
 *
 * aio_callback.c
 *	  AIO - Functionality related to callbacks that can be registered on IO
 *	  Handles
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/aio_callback.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "storage/md.h"
#include "utils/memutils.h"


/* just to have something to put into the aio_handle_cbs */
static const struct PgAioHandleCallbacks aio_invalid_cb = {0};

typedef struct PgAioHandleCallbacksEntry
{
	const PgAioHandleCallbacks *const cb;
	const char *const name;
} PgAioHandleCallbacksEntry;

/*
 * Callback definition for the callbacks that can be registered on an IO
 * handle.  See PgAioHandleCallbackID's definition for an explanation for why
 * callbacks are not identified by a pointer.
 */
static const PgAioHandleCallbacksEntry aio_handle_cbs[] = {
#define CALLBACK_ENTRY(id, callback)  [id] = {.cb = &callback, .name = #callback}
	CALLBACK_ENTRY(PGAIO_HCB_INVALID, aio_invalid_cb),

	CALLBACK_ENTRY(PGAIO_HCB_MD_READV, aio_md_readv_cb),
	CALLBACK_ENTRY(PGAIO_HCB_MD_WRITEV, aio_md_writev_cb),
#undef CALLBACK_ENTRY
};



/*
 * Register callback for the IO handle.
 *
 * Only a limited number (PGAIO_HANDLE_MAX_CALLBACKS) of callbacks can be
 * registered for each IO.
 *
 * Callbacks need to be registered before [indirectly] calling
 * pgaio_io_prep_*(), as the IO may be executed immediately.
 *
 *
 * Note that callbacks are executed in critical sections.  This is necessary
 * to be able to execute IO in critical sections (consider e.g. WAL
 * logging). To perform AIO we first need to acquire a handle, which, if there
 * are no free handles, requires waiting for IOs to complete and to execute
 * their completion callbacks.
 *
 * Callbacks may be executed in the issuing backend but also in another
 * backend (because that backend is waiting for the IO) or in IO workers (if
 * io_method=worker is used).
 *
 *
 * See PgAioHandleCallbackID's definition for an explanation for why
 * callbacks are not identified by a pointer.
 */
void
pgaio_io_register_callbacks(PgAioHandle *ioh, PgAioHandleCallbackID cbid)
{
	const PgAioHandleCallbacksEntry *ce = &aio_handle_cbs[cbid];

	if (cbid >= lengthof(aio_handle_cbs))
		elog(ERROR, "callback %d is out of range", cbid);
	if (aio_handle_cbs[cbid].cb->complete_shared == NULL &&
		aio_handle_cbs[cbid].cb->complete_local == NULL)
		elog(ERROR, "callback %d does not have completion callback", cbid);
	if (ioh->num_shared_callbacks >= PGAIO_HANDLE_MAX_CALLBACKS)
		elog(PANIC, "too many callbacks, the max is %d", PGAIO_HANDLE_MAX_CALLBACKS);
	ioh->shared_callbacks[ioh->num_shared_callbacks] = cbid;

	pgaio_debug_io(DEBUG3, ioh,
				   "adding cb #%d, id %d/%s",
				   ioh->num_shared_callbacks + 1,
				   cbid, ce->name);

	ioh->num_shared_callbacks++;
}

/*
 * Associate an array of data with the Handle. This is e.g. useful to the
 * transport knowledge about which buffers a multi-block IO affects to
 * completion callbacks.
 *
 * Right now this can be done only once for each IO, even though multiple
 * callbacks can be registered. There aren't any known usecases requiring more
 * and the required amount of shared memory does add up, so it doesn't seem
 * worth multiplying memory usage by PGAIO_HANDLE_MAX_CALLBACKS.
 */
void
pgaio_io_set_handle_data_64(PgAioHandle *ioh, uint64 *data, uint8 len)
{
	Assert(ioh->state == PGAIO_HS_HANDED_OUT);
	Assert(ioh->handle_data_len == 0);
	Assert(len <= PG_IOV_MAX);

	for (int i = 0; i < len; i++)
		pgaio_ctl->handle_data[ioh->iovec_off + i] = data[i];
	ioh->handle_data_len = len;
}

/*
 * Convenience version of pgaio_io_set_handle_data_64() that converts a 32bit
 * array to a 64bit array. Without it callers would end up needing to
 * open-code equivalent code.
 */
void
pgaio_io_set_handle_data_32(PgAioHandle *ioh, uint32 *data, uint8 len)
{
	Assert(ioh->state == PGAIO_HS_HANDED_OUT);
	Assert(ioh->handle_data_len == 0);
	Assert(len <= PG_IOV_MAX);

	for (int i = 0; i < len; i++)
		pgaio_ctl->handle_data[ioh->iovec_off + i] = data[i];
	ioh->handle_data_len = len;
}

/*
 * Return data set with pgaio_io_set_handle_data_*().
 */
uint64 *
pgaio_io_get_handle_data(PgAioHandle *ioh, uint8 *len)
{
	Assert(ioh->handle_data_len > 0);

	*len = ioh->handle_data_len;

	return &pgaio_ctl->handle_data[ioh->iovec_off];
}

/*
 * Internal function which invokes ->stage for all the registered callbacks.
 */
void
pgaio_io_call_stage(PgAioHandle *ioh)
{
	Assert(ioh->target > PGAIO_TID_INVALID && ioh->target < PGAIO_TID_COUNT);
	Assert(ioh->op > PGAIO_OP_INVALID && ioh->op < PGAIO_OP_COUNT);

	for (int i = ioh->num_shared_callbacks; i > 0; i--)
	{
		PgAioHandleCallbackID cbid = ioh->shared_callbacks[i - 1];
		const PgAioHandleCallbacksEntry *ce = &aio_handle_cbs[cbid];

		if (!ce->cb->stage)
			continue;

		pgaio_debug_io(DEBUG3, ioh,
					   "calling cb #%d %d/%s->stage",
					   i, cbid, ce->name);
		ce->cb->stage(ioh);
	}
}

/*
 * Internal function which invokes ->complete_shared for all the registered
 * callbacks.
 */
void
pgaio_io_call_complete_shared(PgAioHandle *ioh)
{
	PgAioResult result;

	START_CRIT_SECTION();

	Assert(ioh->target > PGAIO_TID_INVALID && ioh->target < PGAIO_TID_COUNT);
	Assert(ioh->op > PGAIO_OP_INVALID && ioh->op < PGAIO_OP_COUNT);

	result.status = ARS_OK;		/* low level IO is always considered OK */
	result.result = ioh->result;
	result.id = PGAIO_HCB_INVALID;
	result.error_data = 0;

	/*
	 * Call callbacks with the last registered (innermost) callback first.
	 * Each callback can modify the result forwarded to the next callback.
	 */
	for (int i = ioh->num_shared_callbacks; i > 0; i--)
	{
		PgAioHandleCallbackID cbid = ioh->shared_callbacks[i - 1];
		const PgAioHandleCallbacksEntry *ce = &aio_handle_cbs[cbid];

		if (!ce->cb->complete_shared)
			continue;

		pgaio_debug_io(DEBUG4, ioh,
					   "calling cb #%d, id %d/%s->complete_shared with distilled result: (status %s, id %u, error_data %d, result %d)",
					   i, cbid, ce->name,
					   pgaio_result_status_string(result.status),
					   result.id, result.error_data, result.result);
		result = ce->cb->complete_shared(ioh, result);
	}

	ioh->distilled_result = result;

	pgaio_debug_io(DEBUG3, ioh,
				   "after shared completion: distilled result: (status %s, id %u, error_data: %d, result %d), raw_result: %d",
				   pgaio_result_status_string(result.status),
				   result.id, result.error_data, result.result,
				   ioh->result);

	END_CRIT_SECTION();
}


/*
 * Internal function which invokes ->complete_local for all the registered
 * callbacks.
 *
 * XXX: It'd be nice to deduplicate with pgaio_io_call_complete_shared().
 */
void
pgaio_io_call_complete_local(PgAioHandle *ioh)
{
	PgAioResult result;

	START_CRIT_SECTION();

	Assert(ioh->target > PGAIO_TID_INVALID && ioh->target < PGAIO_TID_COUNT);
	Assert(ioh->op > PGAIO_OP_INVALID && ioh->op < PGAIO_OP_COUNT);

	/* start with distilled result from shared callback */
	result = ioh->distilled_result;

	for (int i = ioh->num_shared_callbacks; i > 0; i--)
	{
		PgAioHandleCallbackID cbid = ioh->shared_callbacks[i - 1];
		const PgAioHandleCallbacksEntry *ce = &aio_handle_cbs[cbid];

		if (!ce->cb->complete_local)
			continue;

		pgaio_debug_io(DEBUG4, ioh,
					   "calling cb #%d, id %d/%s->complete_local with distilled result: status %s, id %u, error_data %d, result %d",
					   i, cbid, ce->name,
					   pgaio_result_status_string(result.status),
					   result.id, result.error_data, result.result);
		result = ce->cb->complete_local(ioh, result);
	}

	/*
	 * Note that we don't save the result in ioh->distilled_result, the local
	 * callback's result should not ever matter to other waiters.
	 */
	pgaio_debug_io(DEBUG3, ioh,
				   "after local completion: distilled result: (status %s, id %u, error_data %d, result %d), raw_result: %d",
				   pgaio_result_status_string(result.status),
				   result.id, result.error_data, result.result,
				   ioh->result);

	END_CRIT_SECTION();
}



/* --------------------------------------------------------------------------------
 * IO Result
 * --------------------------------------------------------------------------------
 */

void
pgaio_result_report(PgAioResult result, const PgAioTargetData *target_data, int elevel)
{
	PgAioHandleCallbackID cbid = result.id;
	const PgAioHandleCallbacksEntry *ce = &aio_handle_cbs[cbid];

	Assert(result.status != ARS_UNKNOWN);
	Assert(result.status != ARS_OK);

	if (ce->cb->report == NULL)
		elog(ERROR, "callback %d/%s does not have report callback",
			 result.id, ce->name);

	ce->cb->report(result, target_data, elevel);
}
