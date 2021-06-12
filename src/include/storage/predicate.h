/*-------------------------------------------------------------------------
 *
 * predicate.h
 *	  POSTGRES public predicate locking definitions.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/predicate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREDICATE_H
#define PREDICATE_H

#include "storage/lock.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"


/*
 * GUC variables
 */
extern int	max_predicate_locks_per_xact;
extern int	max_predicate_locks_per_relation;
extern int	max_predicate_locks_per_page;


/* Number of SLRU buffers to use for Serial SLRU */
#define NUM_SERIAL_BUFFERS		16

/*
 * A handle used for sharing SERIALIZABLEXACT objects between the participants
 * in a parallel query.
 */
typedef void *SerializableXactHandle;

/*
 * function prototypes
 */

/* housekeeping for shared memory predicate lock structures */
extern void InitPredicateLocks(void);
extern Size PredicateLockShmemSize(void);

extern void CheckPointPredicate(void);

/* predicate lock reporting */
extern bool PageIsPredicateLocked(Relation relation, BlockNumber blkno);

/* predicate lock maintenance */
extern Snapshot GetSerializableTransactionSnapshot(Snapshot snapshot);
extern void SetSerializableTransactionSnapshot(Snapshot snapshot,
											   VirtualTransactionId *sourcevxid,
											   int sourcepid);
extern void RegisterPredicateLockingXid(TransactionId xid);
extern void PredicateLockRelationBody(Relation relation, Snapshot snapshot);
extern void PredicateLockPageBody(Relation relation, BlockNumber blkno,
								  Snapshot snapshot);
extern void PredicateLockTIDBody(Relation relation, ItemPointer tid,
								 Snapshot snapshot,
								 TransactionId insert_xid);
extern void PredicateLockPageSplit(Relation relation, BlockNumber oldblkno, BlockNumber newblkno);
extern void PredicateLockPageCombine(Relation relation, BlockNumber oldblkno, BlockNumber newblkno);
extern void TransferPredicateLocksToHeapRelation(Relation relation);
extern void ReleasePredicateLocks(bool isCommit, bool isReadOnlySafe);

/* conflict detection (may also trigger rollback) */
extern bool CheckForSerializableConflictOutNeededBody(Relation relation,
													  Snapshot snapshot);
extern void CheckForSerializableConflictOutBody(Relation relation,
												TransactionId xid,
												Snapshot snapshot);
extern void CheckForSerializableConflictInBody(Relation relation,
											   ItemPointer tid,
											   BlockNumber blkno);
extern void CheckTableForSerializableConflictInBody(Relation relation);

/* final rollback checking */
extern void PreCommit_CheckForSerializationFailure(void);

/* two-phase commit support */
extern void AtPrepare_PredicateLocks(void);
extern void PostPrepare_PredicateLocks(TransactionId xid);
extern void PredicateLockTwoPhaseFinish(TransactionId xid, bool isCommit);
extern void predicatelock_twophase_recover(TransactionId xid, uint16 info,
										   void *recdata, uint32 len);

/* parallel query support */
extern SerializableXactHandle ShareSerializableXact(void);
extern void AttachSerializableXact(SerializableXactHandle handle);

/* inline part of predicate and conflict checks */

/*
 * This is given external linkage only so that the following inline wrappers
 * can check it.
 */
struct SERIALIZABLEXACT;

extern PGDLLIMPORT struct SERIALIZABLEXACT *MySerializableXact;

static inline bool
InSerializableTransaction(void)
{
	return MySerializableXact != NULL;
}

static inline void
PredicateLockRelation(Relation relation, Snapshot snapshot)
{
	if (InSerializableTransaction())
		PredicateLockRelationBody(relation, snapshot);
}

static inline void
PredicateLockPage(Relation relation, BlockNumber blkno, Snapshot snapshot)
{
	if (InSerializableTransaction())
		PredicateLockPageBody(relation, blkno, snapshot);
}

static inline void
PredicateLockTID(Relation relation, ItemPointer tid, Snapshot snapshot,
				 TransactionId insert_xid)
{
	if (InSerializableTransaction())
		PredicateLockTIDBody(relation, tid, snapshot, insert_xid);
}

static inline bool
CheckForSerializableConflictOutNeeded(Relation relation, Snapshot snapshot)
{
	if (!InSerializableTransaction())
		return false;
	return CheckForSerializableConflictOutNeededBody(relation, snapshot);
}

static inline void
CheckForSerializableConflictOut(Relation relation, TransactionId xid, Snapshot snapshot)
{
	if (InSerializableTransaction())
		CheckForSerializableConflictOutBody(relation, xid, snapshot);
}

static inline void
CheckForSerializableConflictIn(Relation relation, ItemPointer tid, BlockNumber blkno)
{
	if (InSerializableTransaction())
		CheckForSerializableConflictInBody(relation, tid, blkno);
}

static inline void
CheckTableForSerializableConflictIn(Relation relation)
{
	if (InSerializableTransaction())
		CheckTableForSerializableConflictInBody(relation);
}

#endif							/* PREDICATE_H */
