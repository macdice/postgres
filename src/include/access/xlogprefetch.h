/*-------------------------------------------------------------------------
 *
 * xlogprefetch.h
 *		Declarations for the recovery prefetching module.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/xlogprefetch.h
 *-------------------------------------------------------------------------
 */
#ifndef XLOGPREFETCH_H
#define XLOGPREFETCH_H

#include "access/xlogdefs.h"
#include "storage/aio.h"

/* GUCs */
extern bool recovery_prefetch;
extern bool recovery_prefetch_fpw;



struct XLogPrefetcher;
typedef struct XLogPrefetcher XLogPrefetcher;


extern void XLogPrefetchReconfigure(void);

extern size_t XLogPrefetchShmemSize(void);
extern void XLogPrefetchShmemInit(void);

extern void XLogPrefetchRequestResetStats(void);

extern XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader);
extern void XLogPrefetcherFree(XLogPrefetcher *prefetcher);

extern void XLogPrefetcherBeginRead(XLogPrefetcher *prefetcher,
									XLogRecPtr recPtr);

extern XLogPageReadResult XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher,
												   XLogRecord **out_record,
												   char **errmsg);

#endif
