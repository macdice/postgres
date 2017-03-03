/*-------------------------------------------------------------------------
 *
 * sharedbuffile.h
 *	  Facilities for sharing temporary files between backends.
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

#include "storage/dsm.h"

struct SharedBufFileSet;
typedef struct SharedBufFileSet SharedBufFileSet;

extern void SharedBufFileSetInitialize(SharedBufFileSet *set,
									   int participants,
									   dsm_segment *segment);
extern Size SharedBufFileSetSize(int participants);
extern void SharedBufFileSetAttach(SharedBufFileSet *set,
								   dsm_segment *segment);

extern BufFile *SharedBufFileCreate(SharedBufFileSet *set,
									int partition,
									int participant);
extern void SharedBufFileDestroy(SharedBufFileSet *set,
								 int partition,
								 int participant);
extern void SharedBufFileExport(SharedBufFileSet *set,
								BufFile *file);
extern BufFile *SharedBufFileImport(SharedBufFileSet *set,
									int partition,
									int participant);

#endif   /* SHAREDBUFFILE_H */
