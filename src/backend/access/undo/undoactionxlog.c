/*-------------------------------------------------------------------------
 *
 * undoactionxlog.c
 *	  WAL replay logic for undo actions.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undoactionxlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undoaction_xlog.h"
#include "access/undoaccess.h"
#include "access/xlog.h"
#include "access/xlogutils.h"

/*
 * Replay of undo apply progress.
 */
static void
undo_xlog_apply_progress(XLogReaderState *record)
{
	xl_undoapply_progress *xlrec = (xl_undoapply_progress *) XLogRecGetData(record);
	UndoPersistence persistence;
	UndoRecordInsertContext context = {{0}};

	persistence =
		UndoLogNumberGetPersistence(UndoRecPtrGetLogNo(xlrec->urec_ptr));

	BeginUndoRecordInsert(&context, persistence, 1, record);

	/* Update the progress in the transaction header. */
	PrepareUpdateUndoActionProgress(&context, xlrec->urec_ptr, xlrec->progress);

	UndoRecordUpdateTransInfo(&context, 0);

	/* Release undo buffers. */
	FinishUndoRecordInsert(&context);
}

void
undoaction_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_UNDO_APPLY_PROGRESS:
			undo_xlog_apply_progress(record);
			break;
		default:
			elog(PANIC, "undoaction_redo: unknown op code %u", info);
	}
}
