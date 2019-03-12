/*--------------------------------------------------------------------------
 *
 * test_undorecord.c
 *		Throw-away test code for undo records.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_undorecord.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/undoinsert.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/xlog_internal.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "storage/bufmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(dump_undo_records);

Datum
dump_undo_records(PG_FUNCTION_ARGS)
{
	int logno = PG_GETARG_INT32(0);
	UndoRecPtr record_ptr;
	UndoRecPtr oldest_record_ptr;
	UndoLogControl *log;
	bool empty;

	log = UndoLogGet(logno, false);

	/* Prevent any data from being discarded while we look at it. */
	LWLockAcquire(&log->discard_lock, LW_SHARED);

	/*
	 * We need a consistent snapshot of the range of records and the length
	 * of the final record.
	 */
	LWLockAcquire(&log->mutex, LW_SHARED);
	empty = log->meta.unlogged.insert == log->meta.discard;
	record_ptr = MakeUndoRecPtr(log->logno,
								log->meta.unlogged.insert - log->meta.unlogged.prevlen);
	oldest_record_ptr = MakeUndoRecPtr(log->logno, log->meta.discard);
	LWLockRelease(&log->mutex);

	/* Now walk back record-by-record dumping description data. */
	if (!empty)
	{
		UnpackedUndoRecord *record = palloc0(sizeof(UnpackedUndoRecord));
		RelFileNode rnode;
		StringInfoData buffer;

		UndoRecPtrAssignRelFileNode(rnode, record_ptr);
		initStringInfo(&buffer);
		while (record_ptr >= oldest_record_ptr)
		{
			resetStringInfo(&buffer);
			record = UndoGetOneRecord(record, record_ptr, rnode,
									  log->meta.persistence);
			if (RmgrTable[record->uur_rmid].rm_undo_desc)
				RmgrTable[record->uur_rmid].rm_undo_desc(&buffer, record);
			else
				appendStringInfo(&buffer, "<no undo desc function>");
			if (record->uur_info & UREC_INFO_TRANSACTION)
				appendStringInfo(&buffer, "; xid=%u, next xact=%zx",
								 record->uur_xid,
								 record->uur_next);
			elog(NOTICE, UndoRecPtrFormat ": %s: %s",
				 record_ptr,
				 RmgrTable[record->uur_rmid].rm_name,
				 buffer.data);
			if (BufferIsValid(record->uur_buffer))
			{
				ReleaseBuffer(record->uur_buffer);
				record->uur_buffer = InvalidBuffer;
			}
			if (record->uur_prevlen == 0)
				break;
			record_ptr -= record->uur_prevlen;
		}
		pfree(buffer.data);
	}

	LWLockRelease(&log->discard_lock);

	PG_RETURN_VOID();
}
