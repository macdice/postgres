/*--------------------------------------------------------------------------
 *
 * test_sts.c
 *		Test sharedtuplestore.c
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_sts/test_sts.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "storage/sharedfileset.h"
#include "utils/elog.h"
#include "utils/sharedtuplestore.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_sts);

Datum
test_sts(PG_FUNCTION_ARGS)
{
	int			tuples = PG_GETARG_INT32(0);
	int			tuple_len = PG_GETARG_INT32(1);
	SharedFileSet sfs;
	SharedTuplestore *sts;
	SharedTuplestoreAccessor *accessor;

	SharedFileSetInit(&sfs, NULL);

	sts = palloc(sts_estimate(1));
	accessor = sts_initialize(sts, 1, 0, sizeof(int), 0, &sfs, "test_sts");

	for (int i = 0; i < tuples; ++i)
	{
		MinimalTuple tuple;
		int			l;

		/* alternate between requested size and 100 */
		l = (i % 2 == 0) ? 100 : tuple_len;

		tuple = palloc(l);
		memset(tuple, i & 0xff, l);
		tuple->t_len = l;
		sts_puttuple(accessor, &i, tuple);
		pfree(tuple);
	}
	sts_end_write(accessor);

	sts_begin_parallel_scan(accessor);
	for (int i = 0; i < tuples; ++i)
	{
		MinimalTuple tuple;
		int			meta;
		int			l;

		l = (i % 2 == 0) ? 100 : tuple_len;

		tuple = sts_parallel_scan_next(accessor, &meta);
		if (!tuple)
			elog(ERROR, "expected more tuples");
		if (i != meta)
			elog(ERROR, "expected meta data %d, got meta data %d", i, meta);
		if (tuple->t_len != l)
			elog(ERROR, "expected tuple length %d, got %d", l, tuple->t_len);
		for (int j = sizeof(tuple->t_len); j < l; ++j)
			if (((uint8 *) tuple)[j] != (i & 0xff))
				elog(ERROR, "expected byte %x, got %x", (i & 0xff), ((uint8 *) tuple)[j]);
	}
	if (sts_parallel_scan_next(accessor, NULL) != NULL)
		elog(ERROR, "did not expect more tuples");
	sts_end_parallel_scan(accessor);

	SharedFileSetDeleteAll(&sfs);

	PG_RETURN_VOID();
}
