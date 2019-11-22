/*-------------------------------------------------------------------------
 *
 * walreader.c
 *
 * A background process that reads WAL records and forwards block references
 * to the background reader infrastructure, in the hope of improving recovery
 * performance.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/walreader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/storage_xlog.h"
#include "lib/ilist.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgreader.h"
#include "postmaster/bgworker.h"
#include "postmaster/walreader.h"
#include "replication/walreceiver.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/hsearch.h"
#include "utils/ps_status.h"

/* GUCs */
int			min_wal_prefetch_distance = 0;
int			max_wal_prefetch_distance = 1024 * 10;

/*
 * When waiting for WAL to become available, we wait on a condition variable,
 * but when throttling our throughput we use small sleeps.
 */
#define WALREADER_NAP_TIME 100

void
StartWalReader(void)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "WalReaderMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "walreader");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "walreader");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = Int32GetDatum(0);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase max_worker_processes or decrease max_background_readers")));
	}
}

typedef struct NewRelFileQueueEntry
{
	RelFileNode rnode;
	XLogRecPtr lsn;
	dlist_node queue_node;
} NewRelFileQueueEntry;


void
WalReaderMain(Datum arg)
{
	XLogReaderState *reader;
	XLogRecPtr		read_location;
	XLogRecPtr		wait_location;
	XLogRecPtr		last_known_apply_location;
//	XLogRecPtr		last_known_write_location = 0;
	HTAB		   *new_relfile_table;
	dlist_head		new_relfile_queue;

	HASHCTL new_relfile_ctl = {
		.keysize = sizeof(RelFileNode),
		.entrysize = sizeof(NewRelFileQueueEntry)
	};

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Connect just so we show up in pg_stat_activity. */
	/*
	 * XXX This doesn't work for crash recovery.
	 */
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	/*
	 * To avoid accessing newly created relfiles before they creation has been
	 * replayed, we'll look out for XLOG_SMGR_CREATE records and track when
	 * they're replayed using a couple of data structures.
	 */
	new_relfile_table = hash_create("new relfile table", 1024, &new_relfile_ctl,
									HASH_ELEM | HASH_BLOBS);
	dlist_init(&new_relfile_queue);

	/*
	 * We'll use the standard callback for reading pages, but tell it not to
	 * bother wait for flushes, written data is fine for out purpose and we
	 * might as well start prefetching as soon as possible.
	 */
	reader = XLogReaderAllocate(wal_segment_size,
								DataDir,
								read_local_xlog_page,
								NULL);
	reader->durability = XLOGREADER_WRITE;

	/*
	 * TODO: We need to prevent WAL from being removed from underneath us
	 * (even though our goal is to read WAL *ahead* of recovery, in unlikely
	 * scenarios we could fall behind on a streaming replica and find that the
	 * WAL segment we want to read has been unlinked or renamed and
	 * overwritten).  Perhaps the way to do that is to create an ephemeral
	 * ReplicationSlot and update our slot's "replay_lsn" as we go.
	 */

	/*
	 * We'll start reading from the most recently replayed record, and hope to
	 * get ahead of recovery quickly.
	 */
	read_location = wait_location = last_known_apply_location =
		GetXLogReplayRecPtr(NULL);

	for (;;)
	{
		char *error;

		/* TODO: this only works for standbys, needs more work to work for
		 * crash recovery */

#if 0
		/*
		 * XXX This is unnecessary, because there is a similar wait inside
		 * (modified) read_local_xlog_page.  However, that (modified) code
		 * only works for walreceiver, it doesn't work for crash recovery.
		 * FIXME
		 */
		/*
		 * Wait for there to be a first record to read.  Unlike walsender and
		 * recovery, we only need to wait for it to be written, we don't need
		 * to wait for it to be flushed.  It's better to start prefetching as
		 * soon as possible.
		 */
		if (last_known_write_location <= wait_location)
			last_known_write_location =
				WaitForWalRcvWrite(read_location,
								   WAIT_EVENT_WAL_READER_WAIT_FOR_WAL);
#endif

		/* Read a record. */
		if (!XLogReadRecord(reader, read_location, &error))
			elog(ERROR, "%s", error ? error : "unknown error reading wal record");

		/* After the first record, just ask for the next record. */
		read_location = InvalidXLogRecPtr;
		wait_location = reader->EndRecPtr;

		/* Update the process title. */
		if (update_process_title)
		{
			char process_title[80];

			snprintf(process_title, sizeof(process_title),
					 "prefetching %X/%X",
					 (uint32) (wait_location >> 32),
					 (uint32) wait_location);
			set_ps_display(process_title, false);
		}

		/* Could we possibly be too far ahead of apply? */
		while (reader->ReadRecPtr > last_known_apply_location + max_wal_prefetch_distance)
		{
			/* Refresh our information from shared memory. */
			last_known_apply_location = GetXLogReplayRecPtr(NULL);

			/* Are we really too far ahead?  If so, take a nap. */
			if (reader->ReadRecPtr > last_known_apply_location + max_wal_prefetch_distance)
				WaitLatch(MyLatch,
						  WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						  WALREADER_NAP_TIME,
						  WAIT_EVENT_WAL_READER_WAIT_FOR_REPLAY);
		}

		/*
		 * Is this a record that creates a brand new relfile?  If so, we'll
		 * ignore references to it until the record is replayed.
		 */
		if (XLogRecGetRmid(reader) == RM_SMGR_ID &&
			(XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_SMGR_CREATE)
		{
			xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(reader);
			NewRelFileQueueEntry *entry;
			bool found;

			entry = hash_search(new_relfile_table,
								&xlrec->rnode,
								HASH_ENTER,
								&found);
			if (!found)
			{
				entry->lsn = reader->ReadRecPtr;
				dlist_push_head(&new_relfile_queue, &entry->queue_node);
			}
		}

		/*
		 * Have any new relfiles at the tail end of our queue now been
		 * replayed?
		 */
		while (!dlist_is_empty(&new_relfile_queue))
		{
			NewRelFileQueueEntry *entry =
				dlist_container(NewRelFileQueueEntry,
								queue_node,
								dlist_tail_node(&new_relfile_queue));

			/* If the oldest entry hasn't been replayed yet, give up. */
			if (entry->lsn >= last_known_apply_location)
				break;

			/* It's been replayed, so we can stop ignoring it. */
			dlist_delete(&entry->queue_node);
			hash_search(new_relfile_table, entry, HASH_REMOVE, NULL);
		}

		/*
		 * Are we not far enough ahead of recovery?  We don't really want to
		 * fight over locks with it, or even get behind it.
		 *
		 * XXX How bad is it that we use a stale value for this?  We're
		 * basically racing against recovery, trying to stay ahead, and that
		 * should always be the case.
		 */
		if (reader->EndRecPtr < last_known_apply_location + min_wal_prefetch_distance)
		{
			last_known_apply_location = GetXLogReplayRecPtr(NULL);
			continue;
		}

		/* Scan the record for block references. */
		for (int i = 0; i <= reader->max_block_id; ++i)
		{
			DecodedBkpBlock *block = &reader->blocks[i];

			/*
			 * If this is a block in a brand new relfile whose
			 * XLOG_SMGR_CREATE record isn't known to have been replayed yet,
			 * it's not safe to prefetch it yet.  InRecovery causes smgr to
			 * tolerate missing blocks, but not missing files.  So, skip.
			 */
			if (hash_search(new_relfile_table,
							&block->rnode,
							HASH_FIND,
							NULL))
				continue;

			/* Ignore everything but the main fork for now. */
			if (block->forknum != MAIN_FORKNUM)
				continue;

			/*
			 * Schedule a read of the block in the background.  If the
			 * background reader queue is full, just wait until it drains.
			 * Tell the background reader to process the request with the
			 * InRecovery flag set, so that blocks that don't exist yet are
			 * loaded as zeroes.  This covers the case of relations that were
			 * truncated later in the WAL (just like in regular recovery), and
			 * also relations that are being extended (which we don't want to
			 * attempt to do in the background).  One advantage of reading
			 * zeroed pages in the background in the extension case is that it
			 * causes write stalls due to evictions to be handled in the
			 * background.
			 *
			 * We pass in InvalidOid a relid, because we don't have access to
			 * the relation ID (we have only the rnode).  This will cause the
			 * background readers to take locks and process invalidations for
			 * the "fake" relation identified by the rnode.  The redo code for
			 * all WAL records that drop relfiles must lock (via the "fake"
			 * relid) and invalidate the smgr relation too.
			 */
			if (!EnqueueBackgroundReaderRequest(InvalidOid,
												block->rnode,
												block->forknum,
												block->blkno,
												1,
												true,		/* for recovery */
												WAIT_EVENT_WAL_READER_WAIT_FOR_BGREADER))
			{
				/*
				 * If we had to wait, then update our last known replay LSN,
				 * because there is a chance we've fallen behind recovery.
				 */
				last_known_apply_location = GetXLogReplayRecPtr(NULL);
			}
		}
	}

	hash_destroy(new_relfile_table);
	XLogReaderFree(reader);
}
