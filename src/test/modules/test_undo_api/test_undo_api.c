#include "postgres.h"

#include "access/transam.h"
#include "access/undoaccess.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"

#include <stdlib.h>
#include <unistd.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_undo_api);

static void
compare_undo_record(UnpackedUndoRecord *urp1, UnpackedUndoRecord *urp2)
{
	int	header_size = offsetof(UnpackedUndoRecord, uur_next) + sizeof(uint64);

	/* Compare undo record header. */
	if (strncmp((char *) urp1, (char *) urp2, header_size) != 0)
		elog(ERROR, "undo header did not match");

	/* Compare payload and tuple length. */
	if (urp1->uur_payload.len != urp2->uur_payload.len)
		elog(ERROR, "payload data length did not match");

	if (urp1->uur_tuple.len != urp2->uur_tuple.len)
		elog(ERROR, "tuple data length did not match");

	/* Compare undo record payload data. */
	if (strncmp(urp1->uur_payload.data, urp2->uur_payload.data, urp1->uur_tuple.len) != 0)
		elog(ERROR, "undo payload data did not match");

	/* Compare undo record tuple data. */
	if (strncmp(urp1->uur_tuple.data, urp2->uur_tuple.data, urp1->uur_tuple.len) != 0)
		elog(ERROR, "undo tuple data did not match");
}

/*
 * test_insert_and_fetch - test simple insert and fetch undo record API
 */
static void
test_insert_and_fetch()
{
	UndoRecordInsertContext context = {{0}};
	UndoPersistence persistence = UNDO_PERMANENT;
	char	data[5000];
	int		 len = 5000;
	UnpackedUndoRecord	undorecord = {0};
	UnpackedUndoRecord *undorecord_out;
	UndoRecPtr	undo_ptr;

	/* Prepare dummy undo record*/
	undorecord.uur_rmid = 1;
	undorecord.uur_type = 2;
	undorecord.uur_info = 0;
	undorecord.uur_xid = 100;
	undorecord.uur_cid = 1;
	undorecord.uur_fork = MAIN_FORKNUM;
	undorecord.uur_blkprev = 10;
	undorecord.uur_block = 1;
	undorecord.uur_offset = 10;

	/* Insert large data so that record get split across pages. */
	initStringInfo(&undorecord.uur_tuple);
	memset(data, 'a', 5000);
	appendBinaryStringInfo(&undorecord.uur_tuple,
						   (char *) data,
						   len);
	initStringInfo(&undorecord.uur_payload);
	appendBinaryStringInfo(&undorecord.uur_payload,
						   (char *) data,
						   len);
	/* Prepare undo record. */
	BeginUndoRecordInsert(&context, persistence, 2, NULL);
	undo_ptr = PrepareUndoInsert(&context, &undorecord, InvalidFullTransactionId);

	/* Insert prepared undo record under critical section. */
	START_CRIT_SECTION();
	InsertPreparedUndo(&context);
	END_CRIT_SECTION();

	/* Release undo buffers. */
	FinishUndoRecordInsert(&context);

	/* Fetch inserted undo record. */
	undorecord_out = UndoFetchRecord(undo_ptr, InvalidBlockNumber,
									 InvalidOffsetNumber,
									 InvalidTransactionId, NULL,
									 NULL);
	/* compare undo records. */
	compare_undo_record(&undorecord, undorecord_out);

	UndoRecordRelease(undorecord_out);
	pfree(undorecord.uur_tuple.data);
}

#define MAX_UNDO_RECORD 10
/*
 * test_bulk_fetch - test the bulk fetch API.
 */
static void
test_bulk_fetch()
{
	int i;
	UndoRecordInsertContext context = {{0}};
	UndoPersistence persistence = UNDO_PERMANENT;
	UndoRecInfo	urp_in_array[MAX_UNDO_RECORD];
	UndoRecInfo *urp_out_array;
	UnpackedUndoRecord	uur[MAX_UNDO_RECORD] = {{0}};
	UndoRecPtr	undo_ptr;
	int			nrecords = 0;

	for (i = 0; i < MAX_UNDO_RECORD; i++)
	{
		uur[i].uur_rmid = 1;
		uur[i].uur_type = 2;
		uur[i].uur_info = 0;
		uur[i].uur_xid = 100;
		uur[i].uur_cid = 1;
		uur[i].uur_fork = MAIN_FORKNUM;
		uur[i].uur_blkprev = 10;
		uur[i].uur_block = i;
		uur[i].uur_offset = i + 1;
		urp_in_array[i].uur = &uur[i];
	}

	/* Prepare multiple undo records. */
	BeginUndoRecordInsert(&context, persistence, MAX_UNDO_RECORD, NULL);
	for (i = 0; i < MAX_UNDO_RECORD; i++)
	{
		undo_ptr = PrepareUndoInsert(&context, &uur[i], InvalidFullTransactionId);
		urp_in_array[i].urp = undo_ptr;
	}

	/* Insert them all in one shot. */
	START_CRIT_SECTION();
	InsertPreparedUndo(&context);
	END_CRIT_SECTION();

	/* Release undo buffers. */
	FinishUndoRecordInsert(&context);

	undo_ptr = urp_in_array[MAX_UNDO_RECORD - 1].urp;

	/*
	 * Perform the bulk fetch. 2000 bytes are enough to hold 10 records.  Later
	 * we can enhance this to test the fetch in multi batch by increasing the
	 * record counts or reducing undo_apply_size to smaller value.
	 */
	urp_out_array = UndoBulkFetchRecord(&undo_ptr, urp_in_array[0].urp, 2000,
										&nrecords, false);
	/* Check whether we have got all the record we inserted. */
	if (nrecords != MAX_UNDO_RECORD)
		elog(ERROR, "undo record count did not match");

	/* Compare all records we have fetch using bulk fetch API*/
	for (i = 0; i < MAX_UNDO_RECORD; i++)
	{
		if (urp_in_array[i].urp != urp_out_array[MAX_UNDO_RECORD - 1 - i].urp)
			elog(ERROR, "undo record pointer did not match");
		compare_undo_record(urp_in_array[i].uur, urp_out_array[MAX_UNDO_RECORD - 1 - i].uur);
		UndoRecordRelease(urp_out_array[MAX_UNDO_RECORD - 1 - i].uur);
	}
}
/*
 * Undo API test module
 */
Datum
test_undo_api(PG_FUNCTION_ARGS)
{
	/* Test simple insert and fetch record. */
	test_insert_and_fetch();

	/* Test undo record bulk fetch API*/
	test_bulk_fetch();

	PG_RETURN_VOID();
}
