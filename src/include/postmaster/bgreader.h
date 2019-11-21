/*-------------------------------------------------------------------------
 *
 * bgreader.h
 *	  Exports from postmaster/bgreader.c.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/postmaster/bgreader.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BGREADER_H
#define BGREADER_H

#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilenode.h"

/* GUCs */
extern int max_background_readers;
extern int max_background_reader_queue_depth;
extern int background_reader_idle_timeout;
extern int background_reader_launch_delay;

extern void BackgroundReaderMain(Datum arg);
extern bool EnqueueBackgroundReaderRequest(Oid relid,
										   RelFileNode rnode,
										   ForkNumber forkNum,
										   BlockNumber blockNum,
										   int blocks,
										   bool recovery,
										   int wait_event);
extern void BackgroundReaderInit(void);
extern size_t BackgroundReaderShmemSize(void);

#endif							/* BGREADER_H */
