/*-------------------------------------------------------------------------
 *
 * walreader.h
 *	  Exports from postmaster/walreader.c.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/postmaster/walreader.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WALREADER_H
#define WALREADER_H

/* GUCs */
extern int min_wal_prefetch_distance;
extern int max_wal_prefetch_distance;

extern void StartWalReader(void);
extern void WalReaderMain(Datum arg);

#endif							/* WALREADER_H */
