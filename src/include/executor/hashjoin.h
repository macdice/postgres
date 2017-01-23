/*-------------------------------------------------------------------------
 *
 * hashjoin.h
 *	  internal structures for hash joins
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/hashjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HASHJOIN_H
#define HASHJOIN_H

#include "nodes/execnodes.h"
#include "storage/buffile.h"
#include "storage/barrier.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"

/* ----------------------------------------------------------------
 *				hash-join hash table structures
 *
 * Each active hashjoin has a HashJoinTable control block, which is
 * palloc'd in the executor's per-query context.  All other storage needed
 * for the hashjoin is kept in private memory contexts, two for each hashjoin.
 * This makes it easy and fast to release the storage when we don't need it
 * anymore.  (Exception: data associated with the temp files lives in the
 * per-query context too, since we always call buffile.c in that context.)
 *
 * The hashtable contexts are made children of the per-query context, ensuring
 * that they will be discarded at end of statement even if the join is
 * aborted early by an error.  (Likewise, any temporary files we make will
 * be cleaned up by the virtual file manager in event of an error.)
 *
 * Storage that should live through the entire join is allocated from the
 * "hashCxt", while storage that is only wanted for the current batch is
 * allocated in the "batchCxt".  By resetting the batchCxt at the end of
 * each batch, we free all the per-batch storage reliably and without tedium.
 *
 * During first scan of inner relation, we get its tuples from executor.
 * If nbatch > 1 then tuples that don't belong in first batch get saved
 * into inner-batch temp files. The same statements apply for the
 * first scan of the outer relation, except we write tuples to outer-batch
 * temp files.  After finishing the first scan, we do the following for
 * each remaining batch:
 *	1. Read tuples from inner batch file, load into hash buckets.
 *	2. Read tuples from outer batch file, match to hash buckets and output.
 *
 * It is possible to increase nbatch on the fly if the in-memory hash table
 * gets too big.  The hash-value-to-batch computation is arranged so that this
 * can only cause a tuple to go into a later batch than previously thought,
 * never into an earlier batch.  When we increase nbatch, we rescan the hash
 * table and dump out any tuples that are now of a later batch to the correct
 * inner batch file.  Subsequently, while reading either inner or outer batch
 * files, we might find tuples that no longer belong to the current batch;
 * if so, we just dump them out to the correct batch file.
 * ----------------------------------------------------------------
 */

/* these are in nodes/execnodes.h: */
/* typedef struct HashJoinTupleData *HashJoinTuple; */
/* typedef struct HashJoinTableData *HashJoinTable; */

typedef struct HashJoinTupleData
{
	/* link to next tuple in same bucket */
	union
	{
		dsa_pointer shared;
		struct HashJoinTupleData *unshared;
	} next;
	uint32		hashvalue;		/* tuple's hash code */
	/* Tuple data, in MinimalTuple format, follows on a MAXALIGN boundary */
}	HashJoinTupleData;

#define HJTUPLE_OVERHEAD  MAXALIGN(sizeof(HashJoinTupleData))
#define HJTUPLE_MINTUPLE(hjtup)  \
	((MinimalTuple) ((char *) (hjtup) + HJTUPLE_OVERHEAD))

/*
 * If the outer relation's distribution is sufficiently nonuniform, we attempt
 * to optimize the join by treating the hash values corresponding to the outer
 * relation's MCVs specially.  Inner relation tuples matching these hash
 * values go into the "skew" hashtable instead of the main hashtable, and
 * outer relation tuples with these hash values are matched against that
 * table instead of the main one.  Thus, tuples with these hash values are
 * effectively handled as part of the first batch and will never go to disk.
 * The skew hashtable is limited to SKEW_WORK_MEM_PERCENT of the total memory
 * allowed for the join; while building the hashtables, we decrease the number
 * of MCVs being specially treated if needed to stay under this limit.
 *
 * Note: you might wonder why we look at the outer relation stats for this,
 * rather than the inner.  One reason is that the outer relation is typically
 * bigger, so we get more I/O savings by optimizing for its most common values.
 * Also, for similarly-sized relations, the planner prefers to put the more
 * uniformly distributed relation on the inside, so we're more likely to find
 * interesting skew in the outer relation.
 */
typedef struct HashSkewBucket
{
	uint32		hashvalue;		/* common hash value */
	HashJoinTuple tuples;		/* linked list of inner-relation tuples */
} HashSkewBucket;

#define SKEW_BUCKET_OVERHEAD  MAXALIGN(sizeof(HashSkewBucket))
#define INVALID_SKEW_BUCKET_NO	(-1)
#define SKEW_WORK_MEM_PERCENT  2
#define SKEW_MIN_OUTER_FRACTION  0.01

/*
 * To reduce palloc/dsa_allocate overhead, the HashJoinTuples for the current
 * batch are packed in 32kB buffers instead of pallocing each tuple
 * individually.
 */
typedef struct HashMemoryChunkData
{
	int			ntuples;		/* number of tuples stored in this chunk */
	size_t		maxlen;			/* size of the buffer holding the tuples */
	size_t		used;			/* number of buffer bytes already used */

	/* pointer to the next chunk (linked list) */
	union
	{
		dsa_pointer shared;
		struct HashMemoryChunkData *unshared;
	} next;

	char		data[FLEXIBLE_ARRAY_MEMBER];	/* buffer allocated at the end */
}	HashMemoryChunkData;

typedef struct HashMemoryChunkData *HashMemoryChunk;

#define HASH_CHUNK_SIZE			(32 * 1024L)
#define HASH_CHUNK_HEADER_SIZE	(offsetof(HashMemoryChunkData, data))
#define HASH_CHUNK_THRESHOLD	(HASH_CHUNK_SIZE / 4)

/*
 * Read head position in a shared batch file.
 */
typedef struct HashJoinBatchPosition
{
	int fileno;
	off_t offset;
} HashJoinBatchPosition;

/*
 * The state exposed in shared memory by each participant to coordinate
 * reading of batch files that it wrote.
 */
typedef struct HashJoinSharedBatchReader
{
	int batchno;				/* the batch number we are currently reading */

	LWLock lock;				/* protects access to the members below */
	bool error;					/* has an IO error occurred? */
	HashJoinBatchPosition head;	/* shared read head for current batch */
} HashJoinSharedBatchReader;

/*
 * The state exposed in shared memory by each participant allowing its batch
 * files to be read by other participants.
 */
typedef struct HashJoinParticipantState
{
	/*
	 * To allow other participants to read from this participant's batch
	 * files, this participant publishes its batch descriptors (or invalid
	 * pointers) here.
	 */
	int inner_batchno;
	int outer_batchno;
	dsa_pointer inner_batch_descriptor;
	dsa_pointer outer_batch_descriptor;

	/*
	 * In the case of participants that exit early, they must publish all
	 * their future batches, rather than publishing them one by one above.
	 * These point to an array of dsa_pointers to BufFileDescriptor objects.
	 */
	int nbatch;
	dsa_pointer inner_batch_descriptors;
	dsa_pointer outer_batch_descriptors;

	/*
	 * The shared state used to coordinate reading from the current batch.  We
	 * need separate objects for the outer and inner side, because in the
	 * probing phase some participants can be reading from the outer batch,
	 * while others can be reading from the inner side to preload the next
	 * batch.
	 */
	HashJoinSharedBatchReader inner_batch_reader;
	HashJoinSharedBatchReader outer_batch_reader;
} HashJoinParticipantState;

/*
 * The state used by each backend to manage reading from batch files written
 * by all participants.
 */
typedef struct HashJoinBatchReader
{
	int participant_number;				/* read which participant's batch? */
	int batchno;						/* which batch are we reading? */
	bool inner;							/* inner or outer? */
	HashJoinSharedBatchReader *shared;	/* holder of the shared read head */
	BufFile *file;						/* the file opened in this backend */
	HashJoinBatchPosition head;			/* local read head position */
} HashJoinBatchReader;

/*
 * State for a shared hash join table.  Each backend participating in a hash
 * join with a shared hash table also has a HashJoinTableData object in
 * backend-private memory, which points to this shared state in the DSM
 * segment.
 */
typedef struct SharedHashJoinTableData
{
	Barrier barrier;				/* synchronization for the hash join */
	dsa_pointer buckets;			/* shared hash table buckets */
	int nbuckets;
	int log2_nbuckets;
	int planned_participants;		/* number of planned workers + leader */

	LWLock chunk_lock;				/* protects the following members */
	dsa_pointer chunks;				/* chunks loaded for the current batch */
	dsa_pointer chunk_work_queue;	/* next chunk for shared processing */
	Size size;						/* size of buckets + chunks */
	Size ntuples;
	
	/* state exposed by each participant for sharing batches */
	HashJoinParticipantState participants[FLEXIBLE_ARRAY_MEMBER];
} SharedHashJoinTableData;

/*
 * The head of the linked list of tuples in each bucket.  For shared hash
 * tables, it allows for tuples to be inserted into the bucket with an atomic
 * operation.  For unshared hash tables, it's a plain old pointer to the first
 * tuple.
 */
typedef union HashJoinBucketHead
{
	dsa_pointer_atomic shared;
	HashJoinTuple unshared;
} HashJoinBucketHead;

typedef struct HashJoinTableData
{
	int			nbuckets;		/* # buckets in the in-memory hash table */
	int			log2_nbuckets;	/* its log2 (nbuckets must be a power of 2) */

	int			nbuckets_original;		/* # buckets when starting the first
										 * hash */
	int			nbuckets_optimal;		/* optimal # buckets (per batch) */
	int			log2_nbuckets_optimal;	/* log2(nbuckets_optimal) */

	/* buckets[i] is head of list of tuples in i'th in-memory bucket */
	HashJoinBucketHead *buckets;
	/* buckets array is per-batch storage, as are all the tuples */

	bool		keepNulls;		/* true to store unmatchable NULL tuples */

	bool		skewEnabled;	/* are we using skew optimization? */
	HashSkewBucket **skewBucket;	/* hashtable of skew buckets */
	int			skewBucketLen;	/* size of skewBucket array (a power of 2!) */
	int			nSkewBuckets;	/* number of active skew buckets */
	int		   *skewBucketNums; /* array indexes of active skew buckets */

	int			nbatch;			/* number of batches */
	int			curbatch;		/* current batch #; 0 during 1st pass */

	int			nbatch_original;	/* nbatch when we started inner scan */
	int			nbatch_outstart;	/* nbatch when we started outer scan */

	bool		growEnabled;	/* flag to shut off nbatch increases */

	double		partialTuples;	/* # tuples obtained from inner plan by me */
	double		totalTuples;	/* # tuples obtained from inner plan by all */
	double		skewTuples;		/* # tuples inserted into skew tuples */

	/*
	 * These arrays are allocated for the life of the hash join, but only if
	 * nbatch > 1.  A file is opened only when we first write a tuple into it
	 * (otherwise its pointer remains NULL).  Note that the zero'th array
	 * elements never get used, since we will process rather than dump out any
	 * tuples of batch zero.
	 */
	BufFile   **innerBatchFile; /* buffered virtual temp file per batch */
	BufFile   **outerBatchFile; /* buffered virtual temp file per batch */

	/*
	 * Info about the datatype-specific hash functions for the datatypes being
	 * hashed. These are arrays of the same length as the number of hash join
	 * clauses (hash keys).
	 */
	FmgrInfo   *outer_hashfunctions;	/* lookup data for hash functions */
	FmgrInfo   *inner_hashfunctions;	/* lookup data for hash functions */
	bool	   *hashStrict;		/* is each hash join operator strict? */

	Size		spaceUsed;		/* memory space currently used by tuples */
	Size		spaceAllowed;	/* upper limit for space used */
	Size		spacePeak;		/* peak space used */
	Size		spaceUsedSkew;	/* skew hash table's current space usage */
	Size		spaceAllowedSkew;		/* upper limit for skew hashtable */

	MemoryContext hashCxt;		/* context for whole-hash-join storage */
	MemoryContext batchCxt;		/* context for this-batch-only storage */

	/* used for dense allocation of tuples (into linked chunks) */
	HashMemoryChunk chunks;		/* one list for the whole batch */

	/* used for scanning for unmatched tuples */
	HashMemoryChunk unmatched_chunks;
	HashMemoryChunk chunks_to_reinsert;

	HashMemoryChunk current_chunk;
	Size		current_chunk_index;

	/* State for coordinating shared hash tables. */
	dsa_area *area;
	SharedHashJoinTableData *shared;	/* the shared state */
	int attached_at_phase;				/* the phase this participant joined */
	bool detached_early;				/* did we decide to detach early? */
	HashJoinBatchReader batch_reader;	/* state for reading batches in */
	dsa_pointer current_chunk_shared;	/* DSA pointer to 'current_chunk' */

}	HashJoinTableData;

/* Check if a HashJoinTable is shared by parallel workers. */
#define HashJoinTableIsShared(table) ((table)->shared != NULL)

/* The phases of a parallel hash join. */
#define PHJ_PHASE_BEGINNING				0
#define PHJ_PHASE_CREATING				1
#define PHJ_PHASE_BUILDING				2
#define PHJ_PHASE_RESIZING				3
#define PHJ_PHASE_REINSERTING			4
#define PHJ_PHASE_PROBING				5
#define PHJ_PHASE_UNMATCHED				6

/* The subphases for batches. */
#define PHJ_SUBPHASE_PROMOTING			0
#define PHJ_SUBPHASE_LOADING			1
#define PHJ_SUBPHASE_PREPARING			2
#define PHJ_SUBPHASE_PROBING			3
#define PHJ_SUBPHASE_UNMATCHED			4

/* The phases of parallel processing for batch(n). */
#define PHJ_PHASE_PROMOTING_BATCH(n)	(PHJ_PHASE_UNMATCHED + (n) * 5 - 4)
#define PHJ_PHASE_LOADING_BATCH(n)		(PHJ_PHASE_UNMATCHED + (n) * 5 - 3)
#define PHJ_PHASE_PREPARING_BATCH(n)	(PHJ_PHASE_UNMATCHED + (n) * 5 - 2)
#define PHJ_PHASE_PROBING_BATCH(n)		(PHJ_PHASE_UNMATCHED + (n) * 5 - 1)
#define PHJ_PHASE_UNMATCHED_BATCH(n)	(PHJ_PHASE_UNMATCHED + (n) * 5 - 0)

/* Phase number -> sub-phase within a batch. */
#define PHJ_PHASE_TO_SUBPHASE(p)										\
	(((int)(p) - PHJ_PHASE_UNMATCHED + PHJ_SUBPHASE_UNMATCHED) % 5)

/* Phase number -> batch number. */
#define PHJ_PHASE_TO_BATCHNO(p)											\
	(((int)(p) - PHJ_PHASE_UNMATCHED + PHJ_SUBPHASE_UNMATCHED) / 5)

/* The phases of ExecHashShrink. */
#define PHJ_SHRINK_PHASE_BEGINNING		0
#define PHJ_SHRINK_PHASE_CLEARING		1
#define PHJ_SHRINK_PHASE_WORKING		2
#define PHJ_SHRINK_PHASE_DECIDING		3

/*
 * Return the 'participant number' for a process participating in a parallel
 * hash join.  We give a number < hashtable->shared->planned_participants
 * to each potential participant, including the leader.
 */
#define HashJoinParticipantNumber() \
	(IsParallelWorker() ? ParallelWorkerNumber + 1 : 0)

#endif   /* HASHJOIN_H */
