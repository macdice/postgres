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
 * A single SharedBufFileManager can manage any number of shared BufFiles that
 * are shared between a fixed number of participating backends.  Each shared
 * BufFile can be written to by a single participant but can be read by any
 * backend after it has been 'shared'.  Once a given BufFile is shared, it
 * becomes read-only and cannot be extended.  To create a new shared BufFile,
 * a participant needs its own distinct participant number, and needs to
 * specify an arbitrary index number for the file.  To make it available to
 * other backends, it must be explicitly 'shared', which flushes internal
 * buffers and renders it read-only.  To open a file that has been shared, a
 * backend needs to know the number of the participant that created the file,
 * and the index number.  It is the responsibilit of calling code to ensure
 * that files are not accessed before they have been shared.
 *
 * Each file is identified by a file number and a participant number, so that
 * a SharedBufFileManager manages a 2D table of individual files To allow each
 * backend to export exactly one file, only file number 0 need to be used; an
 * example use case is a parallel tuple sort where each participant exports a
 * tape which a final merge will read.  To allow backends to export multiple
 * files each, a numbering scheme must be invented by the calling code; an
 * example use case is a parallel operation such as hash join which needs to
 * partition data into numbered batches.  The latter use case combined with
 * the desire to have a fixed sized shared memory footprint while permitting
 * dynamic expansion of the number of batches is what motivates the [low_file,
 * high_file) range tracking.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/buffile.h"
#include "storage/sharedbuffile.h"
#include "storage/spin.h"

typedef struct SharedBufFileParticipant
{
	Oid tablespace;				/* tablespace for this participant's files */
	int low_file;				/* lowest known file number */
	int high_file;				/* one past the highest known file number */
} SharedBufFileParticipant;

struct SharedBufFileManager
{
	slock_t mutex;
	pid_t creator_pid;			/* PID of the creating backing */
	int set_number;				/* a unique identifier for this set of files */
	int refcount;				/* number of backends attached */
	int nparticipants;			/* number of backends expected */

	/* per-participant state */
	SharedBufFileParticipant participants[FLEXIBLE_ARRAY_MEMBER];
};

/*
 * Initialize an object in shared memory that can manage a group of shared
 * BufFiles.  'manager' must point to an area of shared memory that has space
 * for SharedBufFileManagerSize(nparticipants) bytes.  'nparticipants' is the
 * number of participants (backends) that can create and access shared
 * BufFiles.  'segment' should point to the DSM segment that holds this
 * SharedBufFile, or NULL if the object is being initialized in fixed shared
 * memory.  Other backends should call SharedBufFileManagerAttach.
 */
void
SharedBufFileManagerInitialize(SharedBufFileManager *manager,
							   int nparticipants, dsm_segment *segment)
{
	int i;

	SpinLockInit(&manager->mutex);
	manager->refcount = 1;
	manager->nparticipants = nparticipants;
	manager->creator_pid = MyProcPid;
	for (i = 0; i < nparticipants; ++i)
	{
		SharedBufFileParticipant *p = &manager->participants[participant];

		/* No files yet. */
		p->low_file = 0;
		p->high_file = 0;

		/* Rotate through the configured tablespaces to spread files out. */
		if (numTempTablespaces > 0)
			p->tablespace = tempTablespaces[i % numTempTablespaces];
		else
			p->tablespace = DEFAULTTABLESPACE_OID;
	}

	/* Register our detach callback. */
}

void
SharedBufFileManagerAttach(SharedBufFileManager *manager,
						   dsm_segment *segment)
{
	SpinLockAcquire(&manager->mutex);
	++manager->refcount;	
	SpinLockRelease(&manager->mutex);
}							   

/*
 * The number of bytes of shared memory required to construct a
 * SharedBufFileManager.
 */
Size
SharedBufFileManagerSize(int nparticipants)
{
	return offsetof(SharedBufFileManager, participants) +
		sizeof(SharedBufFileParticipant) * nparticipants;
}

/*
 * Create a new file suitable for sharing.  Each backend that calls this must
 * use a distinct participant number.  Behavior is undefined if a participant
 * calls this more than once for the same file number.  Files should ideally
 * be numbered consecutively or in as small a range as possible, because file
 * cleanup will scan this range looking for files.
 */
BufFile *
SharedBufFileCreate(SharedBufFileManager *manager,
					int participant,
					int number)
{
	SharedBufFileParticipant *p = &manager->participants[participant];

	SpinLockAcquire(&manager->mutex);
	Assert(participant < manager->nparticipants);
	Assert(participant >= 0);

	/* Must be unused so far or already used by this process. */
	if (p->pid == InvalidPid)
		p->pid = MyProcPid;
	else
		Assert(p->pid == MyProcPid);

	/*
	 * Because we have a fixed space but want to allow variable numbers of
	 * files to be created per participant, we only keep track of the range of
	 * numbers that have been created by this participant and are not yet
	 * known to be destroyed.  We'll eventually clean up all file numbers in
	 * this range that we can find on disk.
	 */
	p->low_file = Min(number, p->low_file);
	p->high_file = Max(number + 1, p->high_file);
	SpinLockRelease(&manager->mutex);
}

/*
 * Export a BufFile that was created with SharedBufFileCreate, so that other
 * backends can import it.
 */
void
SharedBufFileExport(SharedBufFileManager *manager, BufFile *file)
{
	BufFileFlush(file);
	BufFileSetReadOnly(file);
}

/*
 * Import a BufFile that has been created and exported by another backend.
 * The calling code is responsible for obtaining the participant number and
 * file number of such a file, and coordinating so that the file has been
 * exported before any attempt to import it.  Any number of backends may
 * import the same file, as long as they have attached to the
 * SharedBufFileManager.
 */
BufFile *
SharedBufFileImport(SharedBufFileManager *manager, int participant,
					int number)
{
	SharedBufFileParticipant *p = &manager->participants[participant];

	SpinLockAcquire(&manager->mutex);
	Assert(participant < manager->nparticipants);
	Assert(participant >= 0);
	Assert(p->pid != InvalidPid);
	Assert(p->pid != MyProcPid);
	Assert(p->key != 0);
	Assert(p->low_file <= number || p->high_file > number);
	SpinLockRelease(&manager->mutex);

	return BufFileOpenOtherPid(p->pid, p->key, number);
}

/*
 * Destroy a shared BufFile early.  Files are normally cleaned up
 * automatically when all participants detach, but it might be useful to
 * reclaim disk space sooner than that.  The caller asserts that no backends
 * will attempt to read from this file again and that only one backend will
 * destroy it.
 */
void
SharedBufFileDestroy(SharedBufFileManager *manager, int participant,
					 int number)
{
	SharedBufFileParticipant *p = &manager->participants[participant];
	int s;

	Assert(participant < manager->nparticipants);
	Assert(participant >= 0);
	
	/*
	 * Even though we perform no explicit locking or cache coherency here, the
	 * contract is that the caller must know that the described shared BufFile
	 * has been exported.  The only sensible ways we could know that involve a
	 * memory barrier.  (Is this stupid and unnecessary?  Just stick a mutex
	 * on it?)
	 */
	Assert(p->pid != InvalidPid);
	Assert(p->low_file <= number || p->high_file > number);

	/* Delete segment files until we run out. */
	s = 0;
	while (DeleteTemporaryFileInSet(participant->tablespace,
									manager->pid, manager->set,
									n, s))
		++s;
	
	/*
	 * If this file number happens to be at the beginning or end of the range,
	 * we can adjust the range.  This optimization allows the final cleanup to
	 * avoid looking for files that have been explicitly destroyed in
	 * ascending file number order, as is the case for batched hash joins.
	 */
	if (number == p->low_file)
	   ++p->low_file;
	if (number + 1 == p->high_file)
		--p->high_file;
}

static void
shared_buf_file_on_dsm_detach(dsm_segment *segment, Datum datum)
{
	bool unlink_files = false;
	SharedBufFileManager *manager = (SharedBufFileManager *)
		DatumGetPointer(datum);

	SpinLockAcquire(&manager->mutex);
	Assert(manager->refcount > 0);
	if (--manager->refcount == 0)
		unlink_files = true;
	SpinLockRelease(&manager->mutex);

	if (unlink_files)
	{
		int p;
		int n;
		int s;

		/* Scan the list of participants. */
		for (p = 0; p < manager->nparticipants; ++p)
		{
			SharedBufFileParticipant *participant = &manager->participants[p];

			/* Scan the range of file numbers. */
			for (n = participant->low_file; n < participant->high_file; ++n)
			{
				/* Delete segment files until we run out. */
				s = 0;
				while (DeleteTemporaryFileInSet(participant->tablespace,
												manager->pid,
												manager->set_number,
												n, s))
					++s;
			}
		}
	}
}
