/*-------------------------------------------------------------------------
 *
 * undoaction.c
 *	  execute undo actions
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undoaction.c
 *
 * To apply the undo actions, we collect the undo records in bulk and try to
 * process them together.  We ensure to update the transaction's progress at
 * regular intervals so that after a crash we can skip already applied undo.
 * The undo apply progress is updated in terms of the number of blocks
 * processed.  Undo apply progress value XACT_APPLY_PROGRESS_COMPLETED
 * indicates that all the undo is applied, XACT_APPLY_PROGRESS_NOT_STARTED
 * indicates that no undo action has been applied yet and any other value
 * indicates that we have applied undo partially and after crash recovery, we
 * need to start processing the undo from the same location.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "access/undoaction_xlog.h"
#include "access/undolog.h"
#include "access/undorequest.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "utils/relfilenodemap.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "storage/shmem.h"

static void UpdateUndoApplyProgress(UndoRecPtr last_log_start_urec_ptr,
						  BlockNumber block_num);
/*
 * undo_record_comparator
 *
 * qsort comparator to handle undo record for applying undo actions of the
 * transaction.
 */
static int
undo_record_comparator(const void *left, const void *right)
{
	UnpackedUndoRecord *luur = ((UndoRecInfo *) left)->uur;
	UnpackedUndoRecord *ruur = ((UndoRecInfo *) right)->uur;

	if (luur->uur_rmid < ruur->uur_rmid)
		return -1;
	else if (luur->uur_rmid > ruur->uur_rmid)
		return 1;
	else if (luur->uur_reloid < ruur->uur_reloid)
		return -1;
	else if (luur->uur_reloid > ruur->uur_reloid)
		return 1;
	else if (luur->uur_block == ruur->uur_block)
	{
		/*
		 * If records are for the same block then maintain their existing
		 * order by comparing their index in the array.  Because for single
		 * block we need to maintain the order for applying undo action.
		 */
		if (((UndoRecInfo *) left)->index < ((UndoRecInfo *) right)->index)
			return -1;
		else
			return 1;
	}
	else if (luur->uur_block < ruur->uur_block)
		return -1;
	else
		return 1;
}

/*
 * UpdateUndoApplyProgress - Updates how far undo actions from a particular
 * log have been applied while rolling back a transaction.  This progress is
 * measured in terms of undo block number of the undo log till which the
 * undo actions have been applied.
 */
static void
UpdateUndoApplyProgress(UndoRecPtr progress_urec_ptr,
						BlockNumber block_num)
{
	UndoLogCategory category;
	UndoRecordInsertContext context = {{0}};

	category =
		UndoLogNumberGetCategory(UndoRecPtrGetLogNo(progress_urec_ptr));

	BeginUndoRecordInsert(&context, category, 1, NULL);

	/*
	 * Prepare and update the progress of the undo action apply in the
	 * transaction header.
	 */
	PrepareUpdateUndoActionProgress(&context, progress_urec_ptr, block_num);

	START_CRIT_SECTION();

	/* Update the progress in the transaction header. */
	UndoRecordUpdateTransInfo(&context, 0);

	/* WAL log the undo apply progress. */
	{
		XLogRecPtr	lsn;
		xl_undoapply_progress xlrec;

		xlrec.urec_ptr = progress_urec_ptr;
		xlrec.progress = block_num;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, sizeof(xlrec));

		RegisterUndoLogBuffers(&context, 1);
		lsn = XLogInsert(RM_UNDOACTION_ID, XLOG_UNDO_APPLY_PROGRESS);
		UndoLogBuffersSetLSN(&context, lsn);
	}

	END_CRIT_SECTION();

	/* Release undo buffers. */
	FinishUndoRecordInsert(&context);
}

/*
 * execute_undo_actions - Execute the undo actions
 *
 * full_xid - Transaction id that is getting rolled back.
 * from_urecptr - undo record pointer from where to start applying undo
 *				actions.
 * to_urecptr	- undo record pointer up to which the undo actions need to be
 *				applied.
 * nopartial	- true if rollback is for complete transaction.
 */
void
execute_undo_actions(FullTransactionId full_xid, UndoRecPtr from_urecptr,
					 UndoRecPtr to_urecptr, bool nopartial)
{
	UnpackedUndoRecord *uur = NULL;
	UndoRecInfo *urp_array;
	UndoRecPtr	urec_ptr;
	ForkNumber	prev_fork = InvalidForkNumber;
	BlockNumber prev_block = InvalidBlockNumber;
	UndoRecPtr last_log_start_urec_ptr = to_urecptr;

	/*
	 * We choose maintenance_work_mem to collect the undo records for
	 * rollbacks as most of the large rollback requests are done by
	 * background worker which can be considered as maintainence operation.
	 * However, we can introduce a new guc for this as well.
	 */
	int			undo_apply_size = maintenance_work_mem * 1024L;
	TransactionId	xid PG_USED_FOR_ASSERTS_ONLY = XidFromFullTransactionId(full_xid);

	/* 'from' and 'to' pointers must be valid. */
	Assert(from_urecptr != InvalidUndoRecPtr);
	Assert(to_urecptr != InvalidUndoRecPtr);

	urec_ptr = from_urecptr;

	if (nopartial)
	{
		/*
		 * It is important here to fetch the latest undo record and validate if
		 * the actions are already executed.  The reason is that it is possible
		 * that discard worker or backend might try to execute the rollback
		 * request which is already executed.  For ex., after discard worker
		 * fetches the record and found that this transaction need to be
		 * rolledback, backend might concurrently execute the actions and
		 * remove the request from rollback hash table.
		 *
		 * The other case where this will be required is when the transactions
		 * records span across multiple logs.  Say, we register the
		 * transaction from the first log and then we encounter the same
		 * transaction in the second log where its status is still not marked
		 * as done.  Now, before we try to register the request for the second
		 * log, the undo worker came along rolled back the previous request
		 * and removed its hash entry.  In this case, we will successfully
		 * register the request from the second log and it should be detected
		 * here.
		 */
		uur = UndoFetchRecord(to_urecptr, InvalidBlockNumber, InvalidOffsetNumber,
							  InvalidTransactionId, NULL, NULL);

		/* already processed. */
		if (uur == NULL)
		{
			/*
			 * Undo action is already applied, so delete the hash table entry
			 * if exists.
			 */
			RollbackHTRemoveEntry(full_xid, to_urecptr);
			return;
		}

		/*
		 * We don't need to execute the undo actions if they are already
		 * executed.
		 */
		if (IsXactApplyProgressCompleted(uur->uur_progress))
		{
			/*
			 * Undo action is already applied, so delete the hash table entry
			 * if exists.
			 */
			RollbackHTRemoveEntry(full_xid, to_urecptr);
			UndoRecordRelease(uur);
			return;
		}

		Assert(xid == uur->uur_xid);

		UndoRecordRelease(uur);
		uur = NULL;
		last_log_start_urec_ptr =
			RollbackHTGetLastLogStartUrp(full_xid, to_urecptr);
	}

	/*
	 * Fetch the multiple undo records that can fit into undo_apply_size; sort
	 * them in order of reloid and block number and then apply them
	 * page-at-a-time.  Repeat this until we process all the records for the
	 * transaction being rolled back.
	 */
	do
	{
		int			prev_rmid = -1;
		Oid			prev_reloid = InvalidOid;
		bool		blk_chain_complete;
		int			i;
		int			nrecords;
		int			last_index = 0;

		/*
		 * Fetch multiple undo records at once.  This will return the array
		 * of undo records which holds undo record pointers and the pointers
		 * to the actual unpacked undo record.   This will also update the
		 * number of undo records it has copied in the urp_array.
		 *
		 * At a time, we only fetch undo records from a single undo log.  Once,
		 * we process all the undo records from one undo log, we update the
		 * last_log_start_urec_ptr and proceed to the previous undo log.
		 */
		urp_array = UndoBulkFetchRecord(&urec_ptr, last_log_start_urec_ptr,
										undo_apply_size,
										&nrecords, false);

		/*
		 * Since the rollback of this transaction is in-progress, there will be
		 * at least one undo record which is not yet discarded.
		 */
		Assert(nrecords > 0);

		/*
		 * The undo records must belong to the transaction that is being
		 * rolled back.
		 */
		Assert(TransactionIdEquals(xid, urp_array[0].uur->uur_xid));

		/* Sort the undo record array in order of target blocks. */
		qsort((void *) urp_array, nrecords, sizeof(UndoRecInfo),
			  undo_record_comparator);

		if (nopartial && !UndoRecPtrIsValid(urec_ptr))
			blk_chain_complete = true;
		else
			blk_chain_complete = false;

		/*
		 * Now we have urp_array which is sorted in the block order so
		 * traverse this array and apply the undo action block by block.
		 */
		for (i = last_index; i < nrecords; i++)
		{
			UnpackedUndoRecord *uur = urp_array[i].uur;

			/*
			 * If this undo is not for the same block then apply all undo
			 * actions for the previous block.
			 */
			if (prev_rmid >= 0 &&
				(prev_rmid != uur->uur_rmid ||
				 prev_reloid != uur->uur_reloid ||
				 prev_fork != uur->uur_fork ||
				 prev_block != uur->uur_block))
			{
				execute_undo_actions_page(urp_array, last_index, i - 1,
										  prev_reloid, full_xid, prev_block,
										  blk_chain_complete);
				last_index = i;
			}

			prev_rmid = uur->uur_rmid;
			prev_reloid = uur->uur_reloid;
			prev_fork = uur->uur_fork;
			prev_block = uur->uur_block;
		}

		/* Apply the last set of the actions. */
		execute_undo_actions_page(urp_array, last_index, i - 1,
								  prev_reloid, full_xid, prev_block,
								  blk_chain_complete);

		/*
		 * Set undo action apply progress as completed in the transaction header
		 * if this is a main transaction.
		 */
		if (nopartial)
		{
			if (urp_array[nrecords - 1].uur->uur_info & UREC_INFO_LOGSWITCH)
			{
				/*
				 * We have crossed the log boundary.  So, mark current log
				 * header as complete and set the next progress location in the
				 * previous log.
				 */
				UpdateUndoApplyProgress(last_log_start_urec_ptr,
										XACT_APPLY_PROGRESS_COMPLETED);
				last_log_start_urec_ptr = urp_array[nrecords - 1].uur->uur_prevlogstart;
			}
			else if (!UndoRecPtrIsValid(urec_ptr))
			{
				/*
				 * Invalid urec_ptr indicates that we have executed all the undo
				 * actions for this transaction.  So, mark current log header
				 * as complete.
				 */
				Assert(last_log_start_urec_ptr == to_urecptr);
				UpdateUndoApplyProgress(last_log_start_urec_ptr,
										XACT_APPLY_PROGRESS_COMPLETED);
			}
			else
			{
				BlockNumber	progress_block_num;

				/*
				 * There are still some undo actions pending in this log.  So,
				 * just update the progress block number.
				 */
				progress_block_num = UndoRecPtrGetBlockNum(urp_array[nrecords - 1].urp);

				/*
				 * If we've not fetched undo records for more than one undo
				 * block, we can't update the progress block number.  Because,
				 * there can still be undo records in this block that needs to
				 * be applied for rolling back this transaction.
				 */
				if (UndoRecPtrGetBlockNum(urp_array[0].urp) > progress_block_num)
				{
					/*
					 * Update the progress block number.  We increase the block
					 * number by one since the current block might have some undo
					 * records that are yet to be applied.  But, all undo records
					 * from the next block must have been applied.
					 */
					UpdateUndoApplyProgress(last_log_start_urec_ptr,
											progress_block_num + 1);
				}
			}
		}

		/* Free all undo records. */
		for (i = 0; i < nrecords; i++)
			UndoRecordRelease(urp_array[i].uur);

		/* Free urp array for the current batch of undo records. */
		pfree(urp_array);

		/*
		 * Invalid urec_ptr indicates that we have executed all the undo
		 * actions for this transaction.
		 */
		if (!UndoRecPtrIsValid(urec_ptr))
			break;
	} while (true);

	/*
	 * Undo action is applied so delete the hash table entry.
	 */
	Assert(TransactionIdIsValid(xid));
	RollbackHTRemoveEntry(full_xid, to_urecptr);
}

/*
 * execute_undo_actions_page - Execute the undo actions for a page
 *
 *	urp_array - array of undo records (along with their location) for which undo
 *				action needs to be applied.
 *	first_idx - index in the urp_array of the first undo action to be applied
 *	last_idx  - index in the urp_array of the last undo action to be applied
 *	reloid	- OID of relation on which undo actions needs to be applied.
 *	blkno	- block number on which undo actions needs to be applied.
 *	blk_chain_complete - indicates whether the undo chain for block is
 *						 complete.
 *
 *	returns true, if successfully applied the undo actions, otherwise, false.
 */
bool
execute_undo_actions_page(UndoRecInfo *urp_array, int first_idx, int last_idx,
						  Oid reloid, FullTransactionId full_xid, BlockNumber blkno,
						  bool blk_chain_complete)
{
	/*
	 * All records passed to us are for the same RMGR, so we just use the
	 * first record to dispatch.
	 */
	Assert(urp_array != NULL);

	return RmgrTable[urp_array[first_idx].uur->uur_rmid].rm_undo(urp_array, first_idx,
																 last_idx, reloid,
																 full_xid, blkno,
																 blk_chain_complete);
}
