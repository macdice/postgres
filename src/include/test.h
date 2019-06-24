/*
 * Throw away test RMGR functions.
 *
 * Since there is currently no way for an extension to register redo/undo
 * functions, this is part of a test patch that adds stuff to core.  Not for
 * commit!
 */

#ifndef TEST_H
#define TEST_H

#include "access/transam.h"
#include "access/undorecord.h"
#include "access/xlog_internal.h"
#include "lib/stringinfo.h"

extern UndoStatus test_undo_status(UnpackedUndoRecord *record, TransactionId *xid);
extern void test_undo_desc(StringInfo buf, UnpackedUndoRecord *record);

#endif
