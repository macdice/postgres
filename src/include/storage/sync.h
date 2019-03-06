/*-------------------------------------------------------------------------
 *
 * sync.h
 *	  File synchronization management code.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/sync.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYNC_H
#define SYNC_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/segment.h"

/*
 * Caller specified type of sync request.
 *
 * SYNC_REQUESTs are issued to sync a particular file whose path is determined
 * by calling back the handler. A SYNC_FORGET_REQUEST instructs the sync
 * mechanism to cancel a previously submitted sync request.
 *
 * SYNC_FORGET_HIERARCHY_REQUEST is a special type of forget request that
 * involves scanning all pending sync requests and cancelling any entry that
 * matches. The entries are resolved by calling back the handler as the key is
 * opaque to the sync mechanism. Handling these types of requests are a tad slow
 * because we have to search all the requests linearly, but usage of this such
 * as dropping databases, is a pretty heavyweight operation anyhow, so we'll
 * live with it.
 *
 * SYNC_UNLINK_REQUEST is a request to delete the file after the next
 * checkpoint. The path is determined by calling back the handler.
 */
typedef enum syncrequesttype
{
	SYNC_REQUEST,
	SYNC_FORGET_REQUEST,
	SYNC_FORGET_HIERARCHY_REQUEST,
	SYNC_UNLINK_REQUEST
} SyncRequestType;

/*
 * Identifies the handler for the sync callbacks.
 *
 * These enums map back to entries in the callback function table. For
 * consistency, explicitly set the value to 0. See sync.c for more information.
 */
typedef enum syncrequesthandler
{
	SYNC_HANDLER_MD = 0,	/* md smgr */
	SYNC_HANDLER_UNDO = 1	/* undo smgr */
} SyncRequestHandler;

/*
 * Augmenting a relfilenode with the fork and segment number provides all
 * the information to locate the particular segment of interest for a relation.
 */
typedef struct filetag
{
	RelFileNode		rnode;
	ForkNumber		forknum;
	SegmentNumber	segno;
} FileTag;

#define INIT_FILETAG(a,xx_rnode,xx_forknum,xx_segno) \
( \
	(a).rnode = (xx_rnode), \
	(a).forknum = (xx_forknum), \
	(a).segno = (xx_segno) \
)

#define INIT_FILETAG2(a,xx_dbnode,xx_spcnode,xx_relnode,xx_forknum,xx_segno) \
( \
	(a).rnode.dbNode = (xx_dbnode), \
	(a).rnode.spcNode = (xx_spcnode), \
	(a).rnode.relNode = (xx_relnode), \
	(a).forknum = (xx_forknum), \
	(a).segno = (xx_segno) \
)

/* sync forward declarations */
extern void InitSync(void);
extern void SyncPreCheckpoint(void);
extern void SyncPostCheckpoint(void);
extern void ProcessSyncRequests(void);
extern void RememberSyncRequest(const FileTag *ftag, SyncRequestType type,
								 SyncRequestHandler handler);
extern void EnableSyncRequestForwarding(void);
extern bool RegisterSyncRequest(const FileTag *ftag, SyncRequestType type,
				SyncRequestHandler handler, bool retryOnError);

#endif							/* SYNC_H */
