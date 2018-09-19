/*-------------------------------------------------------------------------
 *
 * zheapam_undo.h
 *	  POSTGRES zheap access UNDO definitions.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/zheapam_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ZHEAPAM_UNDO_H
#define ZHEAPAM_UNDO_H

#include "postgres.h"

extern bool zheap_undo(List *luinfo, UndoRecPtr urec_ptr, Oid reloid,
					   TransactionId xid, BlockNumber blkno,
					   bool blk_chain_complete, bool rellock);
#endif
