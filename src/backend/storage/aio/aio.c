/*-------------------------------------------------------------------------
 *
 * aio.c
 *    AIO - Core Logic
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/aio.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/ilist.h"
#include "storage/aio.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"


/* Options for io_method. */
const struct config_enum_entry io_method_options[] = {
	{"sync", IOMETHOD_SYNC, false},
	{NULL, 0, false}
};

/* GUCs */
int			io_method = DEFAULT_IO_METHOD;
int			io_max_concurrency = -1;



void
assign_io_method(int newval, void *extra)
{
}

bool
check_io_max_concurrency(int *newval, void **extra, GucSource source)
{
	if (*newval == -1)
	{
		/*
		 * Auto-tuning will be applied later during startup, as auto-tuning
		 * depends on the value of various GUCs.
		 */
		return true;
	}
	else if (*newval == 0)
	{
		GUC_check_errdetail("Only -1 or values bigger than 0 are valid.");
		return false;
	}

	return true;
}

/*
 * Release IO handle during resource owner cleanup.
 */
void
pgaio_io_release_resowner(dlist_node *ioh_node, bool on_error)
{
	/* placeholder for later */
}
