#include "postgres.h"

#include "access/undoaccess.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "storage/procarray.h"
#include "test.h"
#include "utils/array.h"

PG_FUNCTION_INFO_V1(test_multixact);

Datum
test_multixact(PG_FUNCTION_ARGS)
{
	ArrayType *a = PG_GETARG_ARRAYTYPE_P(0);
	int size = ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a));
	TransactionId *xids = (TransactionId *) ARR_DATA_PTR(a); /* yeah wrong type I know... */
	UnpackedUndoRecord undorecord = {0};
	UndoRecordInsertContext context;
	TransactionId terminator = InvalidTransactionId;

	undorecord.uur_rmid = RM_TEST_ID;
	undorecord.uur_type = 42;
	undorecord.uur_info = UREC_INFO_PAYLOAD;
	undorecord.uur_dbid = InvalidOid;
	undorecord.uur_xid = InvalidTransactionId;
	undorecord.uur_cid = InvalidCommandId;
	undorecord.uur_fork = InvalidForkNumber;
	undorecord.uur_block = InvalidBlockNumber;
	undorecord.uur_blkprev = InvalidBlockNumber;
	undorecord.uur_offset = 0;
	initStringInfo(&undorecord.uur_payload);
	appendBinaryStringInfo(&undorecord.uur_payload,
						   (const void *) xids,
						   sizeof(int) * size);
	appendBinaryStringInfo(&undorecord.uur_payload,
						   (const void *) &terminator,
						   sizeof(terminator));

	BeginUndoRecordInsert(&context, UNDO_SHARED, 1, NULL);
	PrepareUndoInsert(&context, &undorecord, GetCurrentFullTransactionId());
	START_CRIT_SECTION();
	InsertPreparedUndo(&context);
	END_CRIT_SECTION();
	FinishUndoRecordInsert(&context);

	PG_RETURN_NULL();
}

UndoStatus
test_undo_status(UnpackedUndoRecord *uur, TransactionId *xid)
{
	TransactionId *xids;

	Assert(uur->uur_rmid == RM_TEST_ID);
	Assert(uur->uur_type == 42);
	Assert(uur->uur_payload.data);

	xids = (TransactionId *) uur->uur_payload.data;

	/* If any of these xids are running, wait some more. */
	while (TransactionIdIsValid(*xids))
	{
		elog(LOG, "test_undo_status considering transaction ID %u...", *xids);
		if (TransactionIdIsInProgress(*xids))
		{
			elog(LOG, "test_undo_status says: WAIT!");
			*xid = *xids;
			return UNDO_STATUS_WAIT_XMIN;
		}
		++xids;
	}

	/* Otherwise, discard now. */
	elog(LOG, "test_undo_status says: DISCARD!");
	return UNDO_STATUS_DISCARD;
}

void
test_undo_desc(StringInfo buf, UnpackedUndoRecord *uur)
{
	TransactionId *xids;

	Assert(uur->uur_rmid == RM_TEST_ID);
	Assert(uur->uur_type == 42);
	Assert(uur->uur_payload.data);

	xids = (TransactionId *) uur->uur_payload.data;

	appendStringInfo(buf, "TestMultixact");
	while (TransactionIdIsValid(*xids))
		appendStringInfo(buf, " %u", *xids++);
}
