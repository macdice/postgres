/*-------------------------------------------------------------------------
 *
 * snapmgr.h
 *	  POSTGRES snapshot manager
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/snapmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPMGR_H
#define SNAPMGR_H

#include "access/transam.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/snapshot.h"


/* GUC variables */
extern PGDLLIMPORT int old_snapshot_threshold;


extern PGDLLIMPORT bool FirstSnapshotSet;

extern PGDLLIMPORT TransactionId TransactionXmin;
extern PGDLLIMPORT TransactionId RecentXmin;

/* Variables representing various special snapshot semantics */
extern PGDLLIMPORT SnapshotData SnapshotSelfData;
extern PGDLLIMPORT SnapshotData SnapshotAnyData;
extern PGDLLIMPORT SnapshotData CatalogSnapshotData;

#define SnapshotSelf		(&SnapshotSelfData)
#define SnapshotAny			(&SnapshotAnyData)

/*
 * We don't provide a static SnapshotDirty variable because it would be
 * non-reentrant.  Instead, users of that snapshot type should declare a
 * local variable of type SnapshotData, and initialize it with this macro.
 */
#define InitDirtySnapshot(snapshotdata)  \
	((snapshotdata).snapshot_type = SNAPSHOT_DIRTY,	\
	 (snapshotdata).timeout_state = SNAPSHOT_TIMEOUT_NONE)

/*
 * Similarly, some initialization is required for a NonVacuumable snapshot.
 * The caller must supply the visibility cutoff state to use (c.f.
 * GlobalVisTestFor()).
 */
#define InitNonVacuumableSnapshot(snapshotdata, vistestp)  \
	((snapshotdata).snapshot_type = SNAPSHOT_NON_VACUUMABLE, \
	 (snapshotdata).vistest = (vistestp), \
	 (snapshotdata).timeout_state = SNAPSHOT_TIMEOUT_NONE)

/*
 * Similarly, some initialization is required for SnapshotToast.
 */
#define InitToastSnapshot(snapshotdata)  \
	((snapshotdata).snapshot_type = SNAPSHOT_TOAST, \
	 (snapshotdata).timeout_state = SNAPSHOT_TIMEOUT_NONE)

/* This macro encodes the knowledge of which snapshots are MVCC-safe */
#define IsMVCCSnapshot(snapshot)  \
	((snapshot)->snapshot_type == SNAPSHOT_MVCC || \
	 (snapshot)->snapshot_type == SNAPSHOT_HISTORIC_MVCC)

static inline bool
OldSnapshotThresholdActive(void)
{
	return old_snapshot_threshold >= 0;
}

extern Snapshot GetTransactionSnapshot(void);
extern Snapshot GetLatestSnapshot(void);
extern Snapshot GetOldestSnapshot(void);
extern void SnapshotSetCommandId(CommandId curcid);

extern void HandleSnapshotTimeout(void);

extern Snapshot GetCatalogSnapshot(Oid relid);
extern Snapshot GetNonHistoricCatalogSnapshot(Oid relid);
extern void InvalidateCatalogSnapshot(void);
extern void InvalidateCatalogSnapshotConditionally(void);

extern void PushActiveSnapshot(Snapshot snapshot);
extern void PushActiveSnapshotWithLevel(Snapshot snapshot, int snap_level);
extern void PushCopiedSnapshot(Snapshot snapshot);
extern void UpdateActiveSnapshotCommandId(void);
extern void PopActiveSnapshot(void);
extern Snapshot GetActiveSnapshot(void);
extern bool ActiveSnapshotSet(void);

extern Snapshot RegisterSnapshot(Snapshot snapshot);
extern void UnregisterSnapshot(Snapshot snapshot);
extern Snapshot RegisterSnapshotOnOwner(Snapshot snapshot, ResourceOwner owner);
extern void UnregisterSnapshotFromOwner(Snapshot snapshot, ResourceOwner owner);

extern void AtSubCommit_Snapshot(int level);
extern void AtSubAbort_Snapshot(int level);
extern void AtEOXact_Snapshot(bool isCommit, bool resetXmin);

extern void ImportSnapshot(const char *idstr);
extern bool XactHasExportedSnapshots(void);
extern void DeleteAllExportedSnapshotFiles(void);
extern void WaitForOlderSnapshots(TransactionId limitXmin, bool progress);
extern bool ThereAreNoPriorRegisteredSnapshots(void);
extern bool HaveRegisteredOrActiveSnapshot(void);
extern void SetOldSnapshotThresholdTimestamp(TimestampTz ts, TransactionId xlimit);

extern char *ExportSnapshot(Snapshot snapshot);

/*
 * These live in procarray.c because they're intimately linked to the
 * procarray contents, but thematically they better fit into snapmgr.h.
 */
typedef struct GlobalVisState GlobalVisState;
extern GlobalVisState *GlobalVisTestFor(Relation rel);
extern bool GlobalVisTestIsRemovableXid(GlobalVisState *state, TransactionId xid);
extern bool GlobalVisTestIsRemovableFullXid(GlobalVisState *state, FullTransactionId fxid);
extern FullTransactionId GlobalVisTestNonRemovableFullHorizon(GlobalVisState *state);
extern TransactionId GlobalVisTestNonRemovableHorizon(GlobalVisState *state);
extern bool GlobalVisCheckRemovableXid(Relation rel, TransactionId xid);
extern bool GlobalVisCheckRemovableFullXid(Relation rel, FullTransactionId fxid);

/*
 * Utility functions for implementing visibility routines in table AMs.
 */
extern bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);

/* Support for catalog timetravel for logical decoding */
struct HTAB;
extern struct HTAB *HistoricSnapshotGetTupleCids(void);
extern void SetupHistoricSnapshot(Snapshot snapshot_now, struct HTAB *tuplecids);
extern void TeardownHistoricSnapshot(bool is_error);
extern bool HistoricSnapshotActive(void);

extern Size EstimateSnapshotSpace(Snapshot snapshot);
extern void SerializeSnapshot(Snapshot snapshot, char *start_address);
extern Snapshot RestoreSnapshot(char *start_address);
extern void RestoreTransactionSnapshot(Snapshot snapshot, void *source_pgproc);

#endif							/* SNAPMGR_H */
