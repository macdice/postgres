/*-------------------------------------------------------------------------
 *
 * undoaction.c
 *	  execute undo actions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undoaction.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/transam.h"
#include "access/undoaction.h"
#include "access/undoaction_xlog.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "postmaster/undoloop.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "access/undodiscard.h"

#define ROLLBACK_HT_SIZE	1024

static bool execute_undo_actions_page(List *luinfo, UndoRecPtr urec_ptr,
					 Oid reloid, TransactionId xid, BlockNumber blkno,
					 bool blk_chain_complete, bool norellock, int options);
static void RollbackHTRemoveEntry(UndoRecPtr start_urec_ptr);

/* This is the hash table to store all the rollabck requests. */
static HTAB *RollbackHT;

/*
 * execute_undo_actions - Execute the undo actions
 *
 * from_urecptr - undo record pointer from where to start applying undo action.
 * to_urecptr	- undo record pointer upto which point apply undo action.
 * nopartial	- true if rollback is for complete transaction.
 * rewind		- whether to rewind the insert location of the undo log or not.
 *				  Only the backend executed the transaction can rewind, but
 *				  any other process e.g. undo worker should not rewind it.
 *				  Because, if the backend have already inserted new undo records
 *				  for the next transaction and if we rewind then we will loose
 *				  the undo record inserted for the new transaction.
 * 	rellock	  -	  if the caller already has the lock on the required relation,
 *				  then this flag is false, i.e. we do not need to acquire any
 *				  lock here. If the flag is true then we need to acquire lock
 *				  here itself, because caller will not be having any lock.
 *				  When we are performing undo actions for prepared transactions,
 *			      or for rollback to savepoint, we need not to lock as we already
 *				  have the lock on the table. In cases like error or when
 *				  rollbacking from the undo worker we need to have proper locks.
 */
void
execute_undo_actions(UndoRecPtr from_urecptr, UndoRecPtr to_urecptr,
					 bool nopartial, bool rewind, bool rellock)
{
	UnpackedUndoRecord *uur = NULL;
	UndoRecPtr	urec_ptr, prev_urec_ptr;
	UndoRecPtr	save_urec_ptr;
	Oid			prev_reloid = InvalidOid;
	ForkNumber	prev_fork = InvalidForkNumber;
	BlockNumber	prev_block = InvalidBlockNumber;
	List	   *luinfo = NIL;
	bool		more_undo;
	int			options = 0;
	TransactionId xid = InvalidTransactionId;
	UndoRecInfo	*urec_info;

	Assert(from_urecptr != InvalidUndoRecPtr);
	Assert(UndoRecPtrGetLogNo(from_urecptr) != UndoRecPtrGetLogNo(to_urecptr) ||
		   from_urecptr >= to_urecptr);
	/*
	 * If the location upto which rollback need to be done is not provided,
	 * then rollback the complete transaction.
	 * FIXME: this won't work if undolog crossed the limit of 1TB, because
	 * then from_urecptr and to_urecptr will be from different lognos.
	 */
	if (to_urecptr == InvalidUndoRecPtr)
	{
		UndoLogNumber logno = UndoRecPtrGetLogNo(from_urecptr);
		to_urecptr = UndoLogGetLastXactStartPoint(logno);
	}

	save_urec_ptr = urec_ptr = from_urecptr;

	if (nopartial)
	{
		uur = UndoFetchRecord(urec_ptr, InvalidBlockNumber, InvalidOffsetNumber,
							  InvalidTransactionId, NULL, NULL);
		if (uur == NULL)
			return;

		xid = uur->uur_xid;
		UndoRecordRelease(uur);
		uur = NULL;

		/*
		 * Grab the undo action apply lock before start applying the undo action
		 * this will prevent applying undo actions concurrently.  If we do not
		 * get the lock that mean its already being applied concurrently or the
		 * discard worker might be pushing its request to the rollback hash
		 * table
		 */
		if (!ConditionTransactionUndoActionLock(xid))
			return;
	}

	prev_urec_ptr = InvalidUndoRecPtr;
	while (prev_urec_ptr != to_urecptr)
	{
		Oid			reloid = InvalidOid;
		uint16		urec_prevlen;
		UndoRecPtr	urec_prevurp = InvalidUndoRecPtr;
		bool		non_page;

		more_undo = true;

		prev_urec_ptr = urec_ptr;

		/* Fetch the undo record for given undo_recptr. */
		uur = UndoFetchRecord(urec_ptr, InvalidBlockNumber,
						 InvalidOffsetNumber, InvalidTransactionId, NULL, NULL);

		/* If there is no info block, this is not a page-based undo record. */
		non_page = uur && !(uur->uur_info & UREC_INFO_BLOCK);

		if (uur != NULL && !non_page)
			reloid = uur->uur_reloid;

		xid = uur->uur_xid;

		if (non_page)
		{
			prev_reloid = InvalidOid;
			urec_prevlen = uur->uur_prevlen;
			save_urec_ptr = uur->uur_blkprev;

			/*
			 * Execute individual undo actions not associated with a page
			 * immediately.
			 */
			urec_info = palloc(sizeof(UndoRecInfo));
			urec_info->uur = uur;
			urec_info->urp = urec_ptr;
			luinfo = lappend(luinfo, urec_info);
			execute_undo_actions_page(luinfo, urec_ptr, reloid, xid,
									  InvalidBlockNumber, false, rellock,
									  0);
			pfree(urec_info);
			urec_info = NULL;
			list_free(luinfo);
			luinfo = NIL;
			UndoRecordRelease(uur);

			/* Follow undo chain until to_urecptr. */
			if (urec_ptr != to_urecptr &&
				(urec_prevlen > 0 || UndoRecPtrIsValid(urec_prevurp)))
			{
				urec_ptr = UndoGetPrevUndoRecptr(urec_ptr, urec_prevlen);
				continue;
			}
			else
				more_undo = false;
		}
		/*
		 * If the record is already discarded by undo worker or if the relation
		 * is dropped or truncated, then we cannot fetch record successfully.
		 * Hence, skip quietly.
		 *
		 * Note: reloid remains InvalidOid for a discarded record.
		 */
		else if (!OidIsValid(reloid))
		{
			/* release the undo records for which action has been replayed */
			while (luinfo)
			{
				UndoRecInfo *urec_info = (UndoRecInfo *) linitial(luinfo);

				UndoRecordRelease(urec_info->uur);
				pfree(urec_info);
				luinfo = list_delete_first(luinfo);
			}

			urec_prevlen = uur->uur_prevlen;

			/* Release the just-fetched record */
			if (uur != NULL)
				UndoRecordRelease(uur);

			/* The undo chain must continue till we reach to_urecptr */
			if (urec_ptr != to_urecptr &&
				(urec_prevlen > 0 || UndoRecPtrIsValid(urec_prevurp)))
			{
				urec_ptr = UndoGetPrevUndoRecptr(urec_ptr, urec_prevlen);
				continue;
			}
			else
				more_undo = false;
		}
		else if (!OidIsValid(prev_reloid) ||
				 (prev_reloid == reloid &&
				  prev_fork == uur->uur_fork &&
				  prev_block == uur->uur_block))
		{
			/* Collect the undo records that belong to the same page. */
			prev_reloid = reloid;
			prev_fork = uur->uur_fork;
			prev_block = uur->uur_block;

			/* Prepare an undo record information element. */
			urec_info = palloc(sizeof(UndoRecInfo));
			urec_info->urp = urec_ptr;
			urec_info->uur = uur;

			luinfo = lappend(luinfo, urec_info);
			urec_prevlen = uur->uur_prevlen;
			urec_prevurp = uur->uur_prevurp;
			save_urec_ptr = uur->uur_blkprev;

			/* The undo chain must continue till we reach to_urecptr */
			if (urec_ptr != to_urecptr &&
				(urec_prevlen > 0 || UndoRecPtrIsValid(urec_prevurp)))
			{
				urec_ptr = UndoGetPrevUndoRecptr(urec_ptr, urec_prevlen);
				continue;
			}
			else
				more_undo = false;
		}
		else
		{
			more_undo = true;
		}

		/*
		 * If no more undo is left to be processed and we are rolling back the
		 * complete transaction, then we can consider that the undo chain for a
		 * block is complete.
		 * If the previous undo pointer in the page is invalid, then also the
		 * undo chain for the current block is completed.
		 */
		if (luinfo &&
			((!more_undo && nopartial) || !UndoRecPtrIsValid(save_urec_ptr)))
		{
			execute_undo_actions_page(luinfo, save_urec_ptr, prev_reloid,
									  xid, prev_block, true, rellock, options);
			/* Done with the page so reset the options. */
			options = 0;
		}
		else if (luinfo)
		{
			execute_undo_actions_page(luinfo, save_urec_ptr, prev_reloid,
									  xid, prev_block, false, rellock, options);
			/* Done with the page so reset the options. */
			options = 0;
		}

		/* release the undo records for which action has been replayed */
		while (luinfo)
		{
			UndoRecInfo *urec_info = (UndoRecInfo *) linitial(luinfo);

			UndoRecordRelease(urec_info->uur);
			pfree(urec_info);
			luinfo = list_delete_first(luinfo);
		}

		/*
		 * There are still more records to process, so keep moving backwards
		 * in the chain.
		 */
		if (more_undo)
		{
			/* Prepare an undo record information element. */
			urec_info = palloc(sizeof(UndoRecInfo));
			urec_info->urp = urec_ptr;
			urec_info->uur = uur;
			luinfo = lappend(luinfo, urec_info);

			prev_reloid = reloid;
			prev_fork = uur->uur_fork;
			prev_block = uur->uur_block;
			save_urec_ptr = uur->uur_blkprev;

			/*
			 * Continue to process the records if this is not the last undo
			 * record in chain.
			 */
			urec_prevlen = uur->uur_prevlen;
			urec_prevurp = uur->uur_prevurp;
			if (urec_ptr != to_urecptr &&
				(urec_prevlen > 0 || UndoRecPtrIsValid(urec_prevurp)))
				urec_ptr = UndoGetPrevUndoRecptr(urec_ptr, urec_prevlen);
			else
				break;
		}
		else
			break;
	}

	/* Apply the undo actions for the remaining records. */
	if (list_length(luinfo))
	{
		execute_undo_actions_page(luinfo, save_urec_ptr, prev_reloid,
								xid, prev_block, nopartial ? true : false,
								rellock, options);

		/* release the undo records for which action has been replayed */
		while (luinfo)
		{
			UndoRecInfo *urec_info = (UndoRecInfo *) linitial(luinfo);

			UndoRecordRelease(urec_info->uur);
			pfree(urec_info);
			luinfo = list_delete_first(luinfo);
		}
	}

	if (rewind)
	{
		/* Read the prevlen from the first record of this transaction. */
		uur = UndoFetchRecord(to_urecptr, InvalidBlockNumber,
							  InvalidOffsetNumber, InvalidTransactionId,
							  NULL, NULL);
		/*
		 * If undo is already discarded before we rewind, then do nothing.
		 */
		if (uur == NULL)
			return;


		/*
		* Rewind the insert location to start of this transaction.  This is
		* to avoid reapplying some intermediate undo. We do not need to wal
		* log this information here, because if the system crash before we
		* rewind the insert pointer then after recovery we can identify
		* whether the undo is already applied or not from the slot undo record
		* pointer. Also set the correct prevlen value (what we have fetched
		* from the undo).
		*/
		UndoLogRewind(to_urecptr, uur->uur_prevlen);

		UndoRecordRelease(uur);
	}

	if (nopartial)
	{
		/*
		 * Set undo action apply completed in the transaction header if this is
		 * a main transaction and we have not rewound its undo.
		 */
		if (!rewind)
		{
			/*
			 * Undo action is applied so delete the hash table entry and release
			 * the undo action lock.
			 */
			RollbackHTRemoveEntry(from_urecptr);

			/*
			 * Prepare and update the progress of the undo action apply in the
			 * transaction header.
			 */
			PrepareUpdateUndoActionProgress(to_urecptr, 1);

			START_CRIT_SECTION();

			/* Update the progress in the transaction header. */
			UndoRecordUpdateTransInfo(0);

			/* WAL log the undo apply progress. */
			{
				xl_undoapply_progress xlrec;

				xlrec.urec_ptr = to_urecptr;
				xlrec.progress = 1;

				/*
				 * FIXME : We need to register undo buffers and set LSN for them
				 * that will be required for FPW of the undo buffers.
				 */
				XLogBeginInsert();
				XLogRegisterData((char *) &xlrec, sizeof(xlrec));

				(void) XLogInsert(RM_UNDOACTION_ID, XLOG_UNDO_APPLY_PROGRESS);
			}

			END_CRIT_SECTION();
			UnlockReleaseUndoBuffers();
		}

		TransactionUndoActionLockRelease(xid);
	}
}

/*
 * execute_undo_actions_page - Execute the undo actions for a page
 *
 *	After applying all the undo actions for a page, we clear the transaction
 *	slot on a page if the undo chain for block is complete, otherwise just
 *	rewind the undo pointer to the last record for that block that precedes
 *	the last undo record for which action is replayed.
 *
 *	luinfo - list of undo records (along with their location) for which undo
 *			 action needs to be replayed.
 *	urec_ptr - undo record pointer to which we need to rewind.
 *	reloid	- OID of relation on which undo actions needs to be applied.
 *	blkno	- block number on which undo actions needs to be applied.
 *	blk_chain_complete - indicates whether the undo chain for block is
 *						 complete.
 *	nopartial - true if rollback is for complete transaction. If we are not
 *				rolling back the complete transaction then we need to apply the
 *				undo action for UNDO_INVALID_XACT_SLOT also because in such
 *				case we will rewind the insert undo location.
 *	rellock	  -	if the caller already has the lock on the required relation,
 *				then this flag is false, i.e. we do not need to acquire any
 *				lock here. If the flag is true then we need to acquire lock
 *				here itself, because caller will not be having any lock.
 *				When we are performing undo actions for prepared transactions,
 *				or for rollback to savepoint, we need not to lock as we already
 *				have the lock on the table. In cases like error or when
 *				rollbacking from the undo worker we need to have proper locks.
 * options	 -  options for executing undo actions.
 *
 *	returns true, if successfully applied the undo actions, otherwise, false.
 */
static bool
execute_undo_actions_page(List *luinfo, UndoRecPtr urec_ptr, Oid reloid,
						  TransactionId xid, BlockNumber blkno,
						  bool blk_chain_complete, bool rellock, int options)
{
	UndoRecInfo *first;

	/*
	 * All records passed to us are for the same RMGR, so we just use the
	 * first record to dispatch.
	 */
	Assert(luinfo != NIL);
	first = (UndoRecInfo *) linitial(luinfo);

	return RmgrTable[first->uur->uur_rmid].rm_undo(luinfo, urec_ptr, reloid,
												   xid, blkno,
												   blk_chain_complete, rellock,
												   options);
}

/*
 * To return the size of the hash-table for rollbacks.
 */
int
RollbackHTSize(void)
{
	return hash_estimate_size(ROLLBACK_HT_SIZE, sizeof(RollbackHashEntry));
}

/*
 * To initialize the hash-table for rollbacks in shared memory
 * for the given size.
 */
void
InitRollbackHashTable(void)
{
	HASHCTL info;
	MemSet(&info, 0, sizeof(info));

	info.keysize = sizeof(UndoRecPtr);
	info.entrysize = sizeof(RollbackHashEntry);
	info.hash = tag_hash;

	RollbackHT = ShmemInitHash("Undo actions Lookup Table",
								ROLLBACK_HT_SIZE, ROLLBACK_HT_SIZE, &info,
								HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
}

/*
 * To push the rollback requests from backend to the hash-table.
 * Return true if the request is successfully added, else false
 * and the caller may execute undo actions itself.
 */
bool
PushRollbackReq(UndoRecPtr start_urec_ptr, UndoRecPtr end_urec_ptr, Oid dbid)
{
	bool found = false;
	RollbackHashEntry *rh;

	Assert(UndoRecPtrGetLogNo(start_urec_ptr) != UndoRecPtrGetLogNo(end_urec_ptr) ||
		   start_urec_ptr >= end_urec_ptr);
	/*
	 * If the location upto which rollback need to be done is not provided,
	 * then rollback the complete transaction.
	 */
	if (start_urec_ptr == InvalidUndoRecPtr)
	{
		UndoLogNumber logno = UndoRecPtrGetLogNo(end_urec_ptr);
		start_urec_ptr = UndoLogGetLastXactStartPoint(logno);
	}

	Assert(UndoRecPtrIsValid(start_urec_ptr));

	/* If there is no space to accomodate new request, then we can't proceed. */
	if (RollbackHTIsFull())
		return false;

	if(!UndoRecPtrIsValid(end_urec_ptr))
	{
		UndoLogNumber logno = UndoRecPtrGetLogNo(start_urec_ptr);
		end_urec_ptr = UndoLogGetLastXactStartPoint(logno);
	}

	LWLockAcquire(RollbackHTLock, LW_EXCLUSIVE);

	rh = (RollbackHashEntry *) hash_search(RollbackHT, &start_urec_ptr,
										   HASH_ENTER_NULL, &found);
	if (!rh)
	{
		LWLockRelease(RollbackHTLock);
		return false;
	}
	/* We shouldn't try to push the same rollback request again. */
	if (!found)
	{
		rh->start_urec_ptr = start_urec_ptr;
		rh->end_urec_ptr = end_urec_ptr;
		rh->dbid = (dbid == InvalidOid) ? MyDatabaseId : dbid;
	}
	LWLockRelease(RollbackHTLock);

	return true;
}

/*
 * To perform the undo actions for the transactions whose rollback
 * requests are in hash table. Sequentially, scan the hash-table
 * and perform the undo-actions for the respective transactions.
 * Once, the undo-actions are applied, remove the entry from the
 * hash table.
 */
void
RollbackFromHT(Oid dbid)
{
	UndoRecPtr start[ROLLBACK_HT_SIZE];
	UndoRecPtr end[ROLLBACK_HT_SIZE];
	RollbackHashEntry *rh;
	HASH_SEQ_STATUS status;
	int i = 0;

	/* Fetch the rollback requests */
	LWLockAcquire(RollbackHTLock, LW_SHARED);

	Assert(hash_get_num_entries(RollbackHT) <= ROLLBACK_HT_SIZE);
	hash_seq_init(&status, RollbackHT);
	while (RollbackHT != NULL &&
		  (rh = (RollbackHashEntry *) hash_seq_search(&status)) != NULL)
	{
		if (rh->dbid == dbid)
		{
			start[i] = rh->start_urec_ptr;
			end[i] = rh->end_urec_ptr;
			i++;
		}
	}

	LWLockRelease(RollbackHTLock);

	/* Execute the rollback requests */
	while(--i >= 0)
	{
		Assert(UndoRecPtrIsValid(start[i]));
		Assert(UndoRecPtrIsValid(end[i]));

		StartTransactionCommand();
		execute_undo_actions(start[i], end[i], true, false, true);
		CommitTransactionCommand();
	}
}

/*
 * Remove the rollback request entry from the rollback hash table.
 */
static void
RollbackHTRemoveEntry(UndoRecPtr start_urec_ptr)
{
	LWLockAcquire(RollbackHTLock, LW_EXCLUSIVE);

	hash_search(RollbackHT, &start_urec_ptr, HASH_REMOVE, NULL);

	LWLockRelease(RollbackHTLock);
}

/*
 * To check if the rollback requests in the hash table are all
 * completed or not. This is required because we don't not want to
 * expose RollbackHT in xact.c, where it is required to ensure
 * that we push the resuests only when there is some space in
 * the hash-table.
 */
bool
RollbackHTIsFull(void)
{
	bool result = false;

	LWLockAcquire(RollbackHTLock, LW_SHARED);

	if (hash_get_num_entries(RollbackHT) >= ROLLBACK_HT_SIZE)
		result = true;

	LWLockRelease(RollbackHTLock);

	return result;
}

/*
 * Get database list from the rollback hash table.
 */
List *
RollbackHTGetDBList()
{
	HASH_SEQ_STATUS status;
	RollbackHashEntry	*rh;
	List	*dblist = NIL;

	/* Fetch the rollback requests */
	LWLockAcquire(RollbackHTLock, LW_SHARED);

	hash_seq_init(&status, RollbackHT);
	while (RollbackHT != NULL &&
		  (rh = (RollbackHashEntry *) hash_seq_search(&status)) != NULL)
		dblist = list_append_unique_oid(dblist, rh->dbid);

	LWLockRelease(RollbackHTLock);

	return dblist;
}

/*
 *		ConditionTransactionUndoActionLock
 *
 * Insert a lock showing that the undo action for given transaction is in
 * progress. This is only done for the main transaction not for the
 * sub-transaction.
 */
bool
ConditionTransactionUndoActionLock(TransactionId xid)
{
	LOCKTAG		tag;

	SET_LOCKTAG_TRANSACTION_UNDOACTION(tag, xid);

	if (LOCKACQUIRE_NOT_AVAIL == LockAcquire(&tag, ExclusiveLock, false, true))
		return false;
	else
		return true;
}

/*
 *		TransactionUndoActionLockRelease
 *
 * Delete the lock showing that the undo action given transaction ID is in
 * progress.
 */
void
TransactionUndoActionLockRelease(TransactionId xid)
{
	LOCKTAG		tag;

	SET_LOCKTAG_TRANSACTION_UNDOACTION(tag, xid);

	LockRelease(&tag, ExclusiveLock, false);
}
