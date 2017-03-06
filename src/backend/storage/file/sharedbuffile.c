/*-------------------------------------------------------------------------
 *
 * sharedbuffile.c
 *	  Facilities for sharing temporary files between backends.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/sharedbuffile.c
 *
 * NOTES:
 *
 * BufFiles are used to spill temporary data to disk, but can normally only be
 * accessed by a single backend process.  This mechanism allows for a limited
 * form of BufFile sharing between backends, with shared ownership/cleanup.
 *
 * A single SharedBufFileSet can manage any number of shared BufFiles that are
 * shared between a fixed number of participating backends.  Each shared
 * BufFile can be written to by a single participant but can be read by any
 * backend after it has been 'exported'.  Once a given BufFile is exported, it
 * becomes read-only and cannot be extended.  To create a new shared BufFile,
 * a participant needs its own distinct participant number, and needs to
 * specify an arbitrary partition number for the file.  To make it available
 * to other backends, it must be explicitly exported, which flushes internal
 * buffers and renders it read-only.  To open a file that has been shared, a
 * backend needs to know the number of the participant that created the file,
 * and the partition number.  It is the responsibily of calling code to ensure
 * that files are not accessed before they have been shared.
 *
 * Each file is identified by a partition number and a participant number, so
 * that a SharedBufFileSet can be viewed as a 2D table of individual files.
 * To allow each backend to export exactly one file, partition number 0 can be
 * used; an example use case is a parallel tuple sort where each participant
 * exports a tape which a final merge will read.  To allow backends to export
 * multiple files each, a numbering scheme must be invented by the calling
 * code; an example use case is a parallel operation such as hash join which
 * needs to partition data into numbered batches.  The latter use case
 * combined with the desire to have a fixed sized shared memory footprint
 * while permitting dynamic expansion of the number of batches is what
 * motivates the [low_partition, high_partition) range tracking.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_tablespace.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "storage/fd.h"
#include "storage/sharedbuffile.h"
#include "storage/spin.h"

typedef struct SharedBufFileParticipant
{
	pid_t writer_pid;			/* PID of the participant (debug info only) */
	Oid tablespace;				/* tablespace for this participant's files */
	int low_partition;			/* lowest known partition */
	int high_partition;			/* one past the highest known patition */
} SharedBufFileParticipant;

struct SharedBufFileSet
{
	slock_t mutex;
	pid_t creator_pid;			/* PID of the creating backend */
	int set_number;				/* a unique identifier for this set of files */
	int refcount;				/* number of backends attached */
	int nparticipants;			/* number of backends expected */

	/* per-participant state */
	SharedBufFileParticipant participants[FLEXIBLE_ARRAY_MEMBER];
};

static void shared_buf_file_on_dsm_detach(dsm_segment *segment, Datum datum);

/*
 * Initialize an object in shared memory that can manage a group of shared
 * BufFiles.  'set' must point to an area of shared memory that has space for
 * SharedBufFileSetSize(nparticipants) bytes.  'nparticipants' is the number
 * of participants (backends) that can create and access shared BufFiles.
 * 'segment' should point to the DSM segment that holds this SharedBufFile.
 * Other backends should call SharedBufFileSetAttach.
 */
void
SharedBufFileSetInitialize(SharedBufFileSet *set,
						   int participants, dsm_segment *segment)
{
	int i;

	SpinLockInit(&set->mutex);
	set->refcount = 1;
	set->nparticipants = participants;
	set->creator_pid = MyProcPid;
	for (i = 0; i < participants; ++i)
	{
		SharedBufFileParticipant *p = &set->participants[i];

		/* No partitions yet. */
		p->low_partition = 0;
		p->high_partition = 0;

		/* Rotate through the configured tablespaces to spread files out. */
		p->tablespace = GetNextTempTableSpace();
		if (!OidIsValid(p->tablespace))
			p->tablespace = DEFAULTTABLESPACE_OID;
	}

	/* Register our callback to clean up if we are last to detach. */
	on_dsm_detach(segment, shared_buf_file_on_dsm_detach,
				  PointerGetDatum(set));
}

void
SharedBufFileSetAttach(SharedBufFileSet *set,
					   dsm_segment *segment)
{
	SpinLockAcquire(&set->mutex);
	++set->refcount;
	SpinLockRelease(&set->mutex);

	/* Register our callback to clean up if we are last to detach. */
	on_dsm_detach(segment, shared_buf_file_on_dsm_detach,
				  PointerGetDatum(set));
}

/*
 * The number of bytes of shared memory required to construct a
 * SharedBufFileSet.
 */
Size
SharedBufFileSetSize(int participants)
{
	return offsetof(SharedBufFileSet, participants) +
		sizeof(SharedBufFileParticipant) * participants;
}

/*
 * Create a new file suitable for sharing.  Each backend that calls this must
 * use a distinct participant number.  Behavior is undefined if a participant
 * calls this more than once for the same partition number.  Partitions should
 * ideally be numbered consecutively or in as small a range as possible,
 * because file cleanup will scan the range of known partitions looking for
 * files.
 */
BufFile *
SharedBufFileCreate(SharedBufFileSet *set,
					int partition, int participant)
{
	SharedBufFileParticipant *p = &set->participants[participant];

	SpinLockAcquire(&set->mutex);
	Assert(participant < set->nparticipants);
	Assert(participant >= 0);

	/* Must be unused so far or already used by this process. */
	if (p->writer_pid == InvalidPid)
		p->writer_pid = MyProcPid;
	else
		Assert(p->writer_pid == MyProcPid);

	/*
	 * Because we have a fixed space but want to allow variable numbers of
	 * files to be created per participant, we only keep track of the range of
	 * numbers that have been created by this participant and are not yet
	 * known to be destroyed.  We'll eventually clean up all file numbers in
	 * this range that we can find on disk.
	 */
	p->low_partition = Min(partition, p->low_partition);
	p->high_partition = Max(partition + 1, p->high_partition);
	SpinLockRelease(&set->mutex);

	return BufFileCreateShared(p->tablespace, set->creator_pid,
							   set->set_number,
							   partition, participant);
}

/*
 * Export a BufFile that was created with SharedBufFileCreate, so that other
 * backends can import it.
 */
void
SharedBufFileExport(SharedBufFileSet *set, BufFile *file)
{
	BufFileSetReadOnly(file);
}

/*
 * Import a BufFile that has been created and exported by another backend.
 * The calling code is responsible for coordinating with the creator of the
 * file so that it is known to have been been exported before any attempt to
 * import it.  Any number of backends may import the same file, as long as
 * they have attached to the SharedBufFileManager.  Return NULL if there is no
 * such file.
 */
BufFile *
SharedBufFileImport(SharedBufFileSet *set, int partition, int participant)
{
	SharedBufFileParticipant *p = &set->participants[participant];

	Assert(participant < set->nparticipants);
	Assert(participant >= 0);
	Assert(p->writer_pid != MyProcPid);

	/* Cheap check for non-existent file. */
	if (p->writer_pid == InvalidPid ||
		partition < p->low_partition ||
		partition >= p->high_partition)
		return NULL;

	/* Try to open the file.  May be NULL if it doesn't exist. */
	return BufFileOpenShared(p->tablespace, set->creator_pid,
							 set->set_number, partition, participant);
}

/*
 * Destroy a shared BufFile early.  Files are normally cleaned up
 * automatically when all participants detach, but it might be useful to
 * reclaim disk space sooner than that.  The caller asserts that no backends
 * will attempt to read from this file again and that only one backend will
 * destroy it.
 */
void
SharedBufFileDestroy(SharedBufFileSet *set, int partition, int participant)
{
	SharedBufFileParticipant *p = &set->participants[participant];

	Assert(participant < set->nparticipants);
	Assert(participant >= 0);

	/*
	 * Even though we perform no explicit locking or cache coherency here, the
	 * contract is that the caller must know that the described shared BufFile
	 * has been exported.  The only sensible ways we could know that involve a
	 * memory barrier.  (Is this stupid and unnecessary?  Just stick a mutex
	 * on it?)
	 */
	Assert(set->creator_pid != InvalidPid);
	Assert(p->low_partition <= partition || p->high_partition > partition);

	BufFileDeleteShared(p->tablespace, set->creator_pid,
						set->set_number, partition, participant);

	/*
	 * If this partition happens to be at the beginning or end of the range,
	 * we can adjust the range.  This optimization allows the final cleanup to
	 * avoid looking for files that have been explicitly destroyed in
	 * ascending file number order, as is the case for batched hash joins.
	 */
	if (partition == p->low_partition)
	   ++p->low_partition;
	if (partition + 1 == p->high_partition)
		--p->high_partition;
}

static void
shared_buf_file_on_dsm_detach(dsm_segment *segment, Datum datum)
{
	bool unlink_files = false;
	SharedBufFileSet *set = (SharedBufFileSet *) DatumGetPointer(datum);

	SpinLockAcquire(&set->mutex);
	Assert(set->refcount > 0);
	if (--set->refcount == 0)
		unlink_files = true;
	SpinLockRelease(&set->mutex);

	if (unlink_files)
	{
		int p;
		int n;

		/* Scan the list of participants. */
		for (p = 0; p < set->nparticipants; ++p)
		{
			SharedBufFileParticipant *participant = &set->participants[p];

			/* Scan the range of file numbers cleaning up. */
			for (n = participant->low_partition;
				 n < participant->high_partition;
				 ++n)
				BufFileDeleteShared(participant->tablespace,
									set->creator_pid,
									set->set_number,
									n, p);
		}
	}
}
