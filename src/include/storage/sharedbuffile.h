/*-------------------------------------------------------------------------
 *
 * sharedbuffile.h
 *	  Facilities for sharing temporary files between backends.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/sharedbuffile.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHAREDBUFFILE_H
#define SHAREDBUFFILE_H

struct SharedBufFileManager;
typedef struct SharedBufFileManager SharedBufFileManager;

extern void SharedBufFileManagerInitialize(SharedBufFileManager *manager,
										   int nparticipants,
										   dsm_segment *segment);
extern Size SharedBufFileManagerSize(int nparticipants);

extern BufFile *SharedBufFileCreate(SharedBufFileManger *manager,
									int participant,
									int number);
extern void SharedBufFileDestroy(SharedBufFileManager *manager,
								 int participant,
								 int number);
extern void SharedBufFileExport(SharedBufFileManager *manager,
								BufFile *file);
extern BufFile *SharedBufFileImport(SharedBufFileManager *manager,
									int participant,
									int number);

#endif   /* SHAREDBUFFILE_H */
