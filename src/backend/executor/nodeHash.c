/*-------------------------------------------------------------------------
 *
 * nodeHash.c
 *	  Routines to hash relations for hashjoin
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHash.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecHash	- generate an in-memory hash table of the relation
 *		ExecInitHash	- initialize node and subnodes
 *		ExecEndHash		- shutdown node and subnodes
 */

#include "postgres.h"

#include <math.h>
#include <limits.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "catalog/pg_statistic.h"
#include "commands/tablespace.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/barrier.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/probes.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static void ExecHashIncreaseNumBatches(HashJoinTable hashtable, int nbatch);
static void ExecHashIncreaseNumBuckets(HashJoinTable hashtable);
static void ExecHashReinsertAll(HashJoinTable hashtable);
static void ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node,
					  int mcvsToUse);
static void ExecHashSkewTableInsert(HashJoinTable hashtable,
						TupleTableSlot *slot,
						uint32 hashvalue,
						int bucketNumber);
static void ExecHashRemoveNextSkewBucket(HashJoinTable hashtable);

static HashMemoryChunk pop_chunk_queue(HashJoinTable table,
									   dsa_pointer *shared);
static HashMemoryChunk pop_chunk_queue_unlocked(HashJoinTable table,
												dsa_pointer *shared);
static HashJoinTuple next_tuple_in_bucket(HashJoinTable table,
										  HashJoinTuple tuple);

static void insert_tuple_into_bucket(HashJoinTable table, int bucketno,
									 HashJoinTuple tuple,
									 dsa_pointer tuple_pointer);
static HashJoinTuple first_tuple_in_bucket(HashJoinTable table, int bucketno);
static HashJoinTuple next_tuple_in_bucket(HashJoinTable table,
										  HashJoinTuple tuple);

static void *dense_alloc(HashJoinTable hashtable, Size size,
						 bool respect_work_mem);
static void *dense_alloc_shared(HashJoinTable hashtable, Size size,
								dsa_pointer *shared,
								bool respect_work_mem);
static void finish_loading(HashJoinTable hashtable);

/* ----------------------------------------------------------------
 *		ExecHash
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecHash(HashState *node)
{
	elog(ERROR, "Hash node does not support ExecProcNode call convention");
	return NULL;
}

 /* ----------------------------------------------------------------
  *		ExecHashCheckForEarlyExit
  *
  *		return true if this process needs to abandon work on the
  *		hash join to avoid a deadlock
  * ----------------------------------------------------------------
  */
bool
ExecHashCheckForEarlyExit(HashJoinTable hashtable)
{
	/*
	 * The golden rule of leader deadlock avoidance: since leader processes
	 * have two separate roles, namely reading from worker queues AND executing
	 * the same plan as workers, we must never allow a leader to wait for
	 * workers if there is any possibility those workers have emitted tuples.
	 * Otherwise we could get into a situation where a worker fills up its
	 * output tuple queue and begins waiting for the leader to read, while
	 * the leader is busy waiting for the worker.
	 *
	 * Parallel hash joins with shared tables are inherently susceptible to
	 * such deadlocks because there are points at which all participants must
	 * wait (you can't start check for unmatched tuples in the hash table until
	 * probing has completed in all workers, etc).
	 *
	 * So we follow these rules:
	 *
	 * 1.  If there are workers participating, the leader MUST NOT not
	 *     participate in any further work after probing the first batch, so
	 *     that it never has to wait for workers that might have emitted
	 *     tuples.
	 *
	 * 2.  If there are no workers participating, the leader MUST run all the
	 *     batches to completion, because that's the only way for the join
	 *     to complete.  There is no deadlock risk if there are no workers.
	 *
	 * 3.  Workers MUST NOT participate if the building phase has finished by
	 *     the time they have joined, so that the leader can reliably
	 *     determine whether there are any workers running when it comes to
	 *     the point where it must choose between 1 and 2.
	 *
	 * In other words, if the leader makes it all the way through building and
	 * probing before any workers show up, then the leader will run the whole
	 * hash join on its own.  If workers do show up any time before building
	 * is finished, the leader will stop executing the join after helping
	 * probe the first batch.  In the unlikely event of the first worker
	 * showing up after the leader has finished building, it will exit because
	 * it's too late, the leader has already decided to do all the work alone.
	 */

	if (!IsParallelWorker())
	{
		/* Running in the leader process. */
		if (BarrierPhase(&hashtable->shared->barrier) >= PHJ_PHASE_PROBING &&
			hashtable->shared->at_least_one_worker)
		{
			/* Abandon ship due to rule 1.  There are workers running. */
			TRACE_POSTGRESQL_HASH_LEADER_EARLY_EXIT();
			return true;
		}
		else
		{
			/*
			 * Continue processing due to rule 2.  There are no workers, and
			 * any workers that show up later will abandon ship.
			 */
		}
	}
	else
	{
		/* Running in a worker process. */
		if (hashtable->attached_at_phase < PHJ_PHASE_PROBING)
		{
			/*
			 * Advertise that there are workers, so that the leader can
			 * choose between rules 1 and 2.  It's OK that several workers can
			 * write to this variable without immediately memory
			 * synchronization, because the leader will only read it in a later
			 * phase (see above).
			 */
			hashtable->shared->at_least_one_worker = true;
		}
		else if (!hashtable->shared->at_least_one_worker)
		{
			/* Abandon ship due to rule 3. */
			TRACE_POSTGRESQL_HASH_WORKER_EARLY_EXIT();
			return true;
		}
	}

	return false;
}

/* ----------------------------------------------------------------
 *		MultiExecHash
 *
 *		build hash table for hashjoin, doing partitioning if more
 *		than one batch is required.
 * ----------------------------------------------------------------
 */
Node *
MultiExecHash(HashState *node)
{
	PlanState  *outerNode;
	List	   *hashkeys;
	HashJoinTable hashtable;
	TupleTableSlot *slot;
	ExprContext *econtext;
	uint32		hashvalue;
	Barrier	   *barrier;

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStartNode(node->ps.instrument);

	/*
	 * get state info from node
	 */
	outerNode = outerPlanState(node);
	hashtable = node->hashtable;

	if (HashJoinTableIsShared(hashtable))
	{
		/*
		 * Synchronize parallel hash table builds.  At this stage we know that
		 * the shared hash table has been created, but we don't know if our
		 * peers are still in MultiExecHash and if so how far through.  We use
		 * the phase to synchronize with them.
		 */
		barrier = &hashtable->shared->barrier;

		switch (BarrierPhase(barrier))
		{
		case PHJ_PHASE_BEGINNING:
			/* ExecHashTableCreate already handled this phase. */
			Assert(false);
		case PHJ_PHASE_CREATING:
			/* Wait for serial phase, and then either build or wait. */
			if (BarrierWait(barrier, WAIT_EVENT_HASH_CREATING))
				goto build;
			else if (node->ps.plan->parallel_aware)
				goto build;
			else
				goto post_build;
		case PHJ_PHASE_BUILDING:
			/* Building is already underway.  Can we join in? */
			if (node->ps.plan->parallel_aware)
				goto build;
			else
				goto post_build;
		case PHJ_PHASE_RESIZING:
			/* Can't help with serial phase. */
			goto post_resize;
		case PHJ_PHASE_REINSERTING:
			/* Reinserting is in progress after resizing.  Let's help. */
			goto reinsert;
		default:
			/* The hash table building work is already finished. */
			goto finish;
		}
	}

 build:
	if (HashJoinTableIsShared(hashtable))
	{
		/* Make sure our local state is up-to-date so we can build. */
		Assert(BarrierPhase(barrier) == PHJ_PHASE_BUILDING);
		ExecHashUpdate(hashtable);

		/* Coordinate shrinking while we build the hash table. */
		BarrierAttach(&hashtable->shared->shrink_barrier);
	}

	/*
	 * set expression context
	 */
	hashkeys = node->hashkeys;
	econtext = node->ps.ps_ExprContext;

	/*
	 * get all inner tuples and insert into the hash table (or temp files)
	 */
	TRACE_POSTGRESQL_HASH_BUILD_START();
	for (;;)
	{
		slot = ExecProcNode(outerNode);
		if (TupIsNull(slot))
			break;
		/* We have to compute the hash value */
		econtext->ecxt_innertuple = slot;
		if (ExecHashGetHashValue(hashtable, econtext, hashkeys,
								 false, hashtable->keepNulls,
								 &hashvalue))
		{
			int			bucketNumber;

			bucketNumber = ExecHashGetSkewBucket(hashtable, hashvalue);
			if (bucketNumber != INVALID_SKEW_BUCKET_NO)
			{
				/* It's a skew tuple, so put it into that hash table */
				ExecHashSkewTableInsert(hashtable, slot, hashvalue,
										bucketNumber);
				hashtable->skewTuples += 1;
			}
			else
			{
				/* Not subject to skew optimization, so insert normally */
				ExecHashTableInsert(hashtable, slot, hashvalue);
			}
			hashtable->partialTuples += 1;
			if (!HashJoinTableIsShared(hashtable))
				hashtable->totalTuples += 1;
		}
	}
	finish_loading(hashtable);
	TRACE_POSTGRESQL_HASH_BUILD_DONE((int) hashtable->partialTuples);

	if (HashJoinTableIsShared(hashtable))
		BarrierDetach(&hashtable->shared->shrink_barrier);

 post_build:
	if (HashJoinTableIsShared(hashtable))
	{
		bool elected_to_resize;

		/*
		 * Wait for all backends to finish building.  If only one worker is
		 * running the building phase because of a non-partial inner plan, the
		 * other workers will pile up here waiting.  If multiple worker are
		 * building, they should finish close to each other in time.
		 */
		Assert(BarrierPhase(barrier) == PHJ_PHASE_BUILDING);
		elected_to_resize = BarrierWait(barrier, WAIT_EVENT_HASH_BUILDING);
		/*
		 * Resizing is a serial phase.  All but one should skip ahead to
		 * reinserting phase, but all workers should update their copy of the
		 * shared tuple count with the final total first.
		 */
		if (!elected_to_resize)
			goto post_resize;
		Assert(BarrierPhase(barrier) == PHJ_PHASE_RESIZING);
	}

	/* resize the hash table if needed (NTUP_PER_BUCKET exceeded) */
	ExecHashUpdate(hashtable);
	ExecHashIncreaseNumBuckets(hashtable);

 post_resize:
	if (HashJoinTableIsShared(hashtable))
	{
		Assert(BarrierPhase(barrier) == PHJ_PHASE_RESIZING);
		BarrierWait(barrier, WAIT_EVENT_HASH_RESIZING);
		Assert(BarrierPhase(barrier) == PHJ_PHASE_REINSERTING);
	}

 reinsert:
	/* If the table was resized, insert tuples into the new buckets. */
	ExecHashUpdate(hashtable);
	ExecHashReinsertAll(hashtable);

	if (HashJoinTableIsShared(hashtable))
	{
		Assert(BarrierPhase(barrier) == PHJ_PHASE_REINSERTING);
		BarrierWait(barrier, WAIT_EVENT_HASH_REINSERTING);
		Assert(BarrierPhase(barrier) == PHJ_PHASE_PROBING);
	}

 finish:
	if (HashJoinTableIsShared(hashtable))
	{
		/*
		 * Building has finished.  The other workers may be probing or
		 * processing unmatched tuples for the initial batch, or dealing with
		 * later batches.  The next synchronization point is in ExecHashJoin's
		 * HJ_BUILD_HASHTABLE case, which will figure that out and synchronize
		 * this backend's local state machine with the group phase.
		 */
		Assert(BarrierPhase(barrier) >= PHJ_PHASE_PROBING);
		ExecHashUpdate(hashtable);
	}

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStopNode(node->ps.instrument, hashtable->partialTuples);

	/*
	 * We do not return the hash table directly because it's not a subtype of
	 * Node, and so would violate the MultiExecProcNode API.  Instead, our
	 * parent Hashjoin node is expected to know how to fish it out of our node
	 * state.  Ugly but not really worth cleaning up, since Hashjoin knows
	 * quite a bit more about Hash besides that.
	 */
	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for Hash node
 * ----------------------------------------------------------------
 */
HashState *
ExecInitHash(Hash *node, EState *estate, int eflags)
{
	HashState  *hashstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hashstate = makeNode(HashState);
	hashstate->ps.plan = (Plan *) node;
	hashstate->ps.state = estate;
	hashstate->hashtable = NULL;
	hashstate->hashkeys = NIL;	/* will be set by parent HashJoin */

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hashstate->ps);

	/*
	 * initialize our result slot
	 */
	ExecInitResultTupleSlot(estate, &hashstate->ps);

	/*
	 * initialize child expressions
	 */
	hashstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) hashstate);
	hashstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) hashstate);

	/*
	 * initialize child nodes
	 */
	outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize tuple type. no need to initialize projection info because
	 * this node doesn't do projections
	 */
	ExecAssignResultTypeFromTL(&hashstate->ps);
	hashstate->ps.ps_ProjInfo = NULL;

	return hashstate;
}

/* ---------------------------------------------------------------
 *		ExecEndHash
 *
 *		clean up routine for Hash node
 * ----------------------------------------------------------------
 */
void
ExecEndHash(HashState *node)
{
	PlanState  *outerPlan;

	/*
	 * free exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * shut down the subplan
	 */
	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}


/* ----------------------------------------------------------------
 *		ExecHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
HashJoinTable
ExecHashTableCreate(HashState *state, List *hashOperators, bool keepNulls)
{
	Hash	   *node;
	HashJoinTable hashtable;
	SharedHashJoinTable shared_hashtable;
	Plan	   *outerNode;
	size_t		space_allowed;
	int			nbuckets;
	int			nbatch;
	int			num_skew_mcvs;
	int			log2_nbuckets;
	int			nkeys;
	int			i;
	ListCell   *ho;
	MemoryContext oldcxt;

	/*
	 * Get information about the size of the relation to be hashed (it's the
	 * "outer" subtree of this node, but the inner relation of the hashjoin).
	 * Compute the appropriate size of the hash table.
	 */
	node = (Hash *) state->ps.plan;
	outerNode = outerPlan(node);

	shared_hashtable = state->shared_table_data;
	ExecChooseHashTableSize(outerNode->plan_rows, outerNode->plan_width,
							OidIsValid(node->skewTable),
							shared_hashtable != NULL,
							shared_hashtable != NULL ?
							shared_hashtable->planned_participants - 1 : 0,
							&space_allowed,
							&nbuckets, &nbatch, &num_skew_mcvs);

	/* nbuckets must be a power of 2 */
	log2_nbuckets = my_log2(nbuckets);
	Assert(nbuckets == (1 << log2_nbuckets));

	/*
	 * Initialize the hash table control block.
	 *
	 * The hashtable control block is just palloc'd from the executor's
	 * per-query memory context.
	 */
	hashtable = (HashJoinTable) palloc(sizeof(HashJoinTableData));
	hashtable->nbuckets = nbuckets;
	hashtable->nbuckets_original = nbuckets;
	hashtable->nbuckets_optimal = nbuckets;
	hashtable->log2_nbuckets = log2_nbuckets;
	hashtable->log2_nbuckets_optimal = log2_nbuckets;
	hashtable->buckets = NULL;
	hashtable->keepNulls = keepNulls;
	hashtable->skewEnabled = false;
	hashtable->skewBucket = NULL;
	hashtable->skewBucketLen = 0;
	hashtable->nSkewBuckets = 0;
	hashtable->skewBucketNums = NULL;
	hashtable->nbatch = nbatch;
	hashtable->curbatch = 0;
	hashtable->nbatch_original = nbatch;
	hashtable->nbatch_outstart = nbatch;
	hashtable->growEnabled = true;
	hashtable->partialTuples = 0;
	hashtable->totalTuples = 0;
	hashtable->skewTuples = 0;
	hashtable->innerBatchFile = NULL;
	hashtable->outerBatchFile = NULL;
	hashtable->spaceUsed = 0;
	hashtable->spacePeak = 0;
	hashtable->spaceAllowed = space_allowed;
	hashtable->spaceUsedSkew = 0;
	hashtable->spaceAllowedSkew =
		hashtable->spaceAllowed * SKEW_WORK_MEM_PERCENT / 100;
	hashtable->chunks = NULL;
	hashtable->unmatched_chunks = NULL;
	hashtable->chunks_to_reinsert = NULL;
	hashtable->current_chunk = NULL;
	hashtable->area = state->ps.state->es_query_dsa;
	hashtable->shared = state->shared_table_data;
	hashtable->detached_early = false;
	hashtable->batch_reader.batchno = 0;
	hashtable->batch_reader.inner = false;

#ifdef HJDEBUG
	printf("Hashjoin %p: initial nbatch = %d, nbuckets = %d\n",
		   hashtable, nbatch, nbuckets);
#endif

	/*
	 * Get info about the hash functions to be used for each hash key. Also
	 * remember whether the join operators are strict.
	 */
	nkeys = list_length(hashOperators);
	hashtable->outer_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->inner_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->hashStrict = (bool *) palloc(nkeys * sizeof(bool));
	i = 0;
	foreach(ho, hashOperators)
	{
		Oid			hashop = lfirst_oid(ho);
		Oid			left_hashfn;
		Oid			right_hashfn;

		if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
			elog(ERROR, "could not find hash function for hash operator %u",
				 hashop);
		fmgr_info(left_hashfn, &hashtable->outer_hashfunctions[i]);
		fmgr_info(right_hashfn, &hashtable->inner_hashfunctions[i]);
		hashtable->hashStrict[i] = op_strict(hashop);
		i++;
	}

	/*
	 * Create temporary memory contexts in which to keep the hashtable working
	 * storage if using private hash table.  See notes in executor/hashjoin.h.
	 */
	hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
											   "HashTableContext",
											   ALLOCSET_DEFAULT_SIZES);

	hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
												"HashBatchContext",
												ALLOCSET_DEFAULT_SIZES);

	/* Allocate data that will live for the life of the hashjoin */

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	if (nbatch > 1)
	{
		/*
		 * allocate and initialize the file arrays in hashCxt
		 */
		hashtable->innerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		/* The files will not be opened until needed... */
		/* ... but make sure we have temp tablespaces established for them */
		PrepareTempTablespaces();
	}

	MemoryContextSwitchTo(oldcxt);

	if (HashJoinTableIsShared(hashtable))
	{
		Barrier *barrier;

		/*
		 * Attach to the barrier.  The corresponding detach operation is in
		 * ExecHashTableDestroy.
		 */
		barrier = &hashtable->shared->barrier;
		hashtable->attached_at_phase = BarrierAttach(barrier);

		/*
		 * So far we have no idea whether there are any other participants, and
		 * if so, what phase they are working on.  The only thing we care about
		 * at this point is whether someone has already created the shared
		 * hash table yet.  If not, one backend will be elected to do that
		 * now.
		 */
		if (BarrierPhase(barrier) == PHJ_PHASE_BEGINNING)
		{
			if (BarrierWait(barrier, WAIT_EVENT_HASH_BEGINNING))
			{
				/* Serial phase: create the hash tables */
				Size bytes;
				HashJoinBucketHead *buckets;
				int i;
				SharedHashJoinTable shared;
				dsa_area *area;

				shared = hashtable->shared;
				area = hashtable->area;
				bytes = nbuckets * sizeof(HashJoinBucketHead);

				/* Allocate the hash table buckets. */
				shared->buckets = dsa_allocate(area, bytes);
				if (!DsaPointerIsValid(shared->buckets))
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("out of memory")));

				/* Initialize the hash table buckets to empty. */
				buckets = dsa_get_address(area, shared->buckets);
				for (i = 0; i < nbuckets; ++i)
					dsa_pointer_atomic_init(&buckets[i].shared,
											InvalidDsaPointer);

				/* Initialize the rest of parallel_state. */
				hashtable->shared->nbuckets = nbuckets;
				hashtable->shared->log2_nbuckets = log2_nbuckets;
				hashtable->shared->size = bytes;
				hashtable->shared->nbatch = hashtable->nbatch;

				/* TODO: ExecHashBuildSkewHash */

				/*
				 * The backend-local pointers in hashtable will be set up by
				 * ExecHashUpdate, at each point where they might have
				 * changed.
				 */
			}
			Assert(BarrierPhase(&hashtable->shared->barrier) ==
				   PHJ_PHASE_CREATING);
			/* The next synchronization point is in MultiExecHash. */
		}
	}
	else
	{
		/*
		 * Prepare context for the first-scan space allocations; allocate the
		 * hashbucket array therein, and set each bucket "empty".
		 */
		MemoryContextSwitchTo(hashtable->batchCxt);

		hashtable->buckets = (HashJoinBucketHead *)
			palloc0(nbuckets * sizeof(HashJoinBucketHead));

		MemoryContextSwitchTo(oldcxt);

		/*
		 * Set up for skew optimization, if possible and there's a need for
		 * more than one batch.  (In a one-batch join, there's no point in
		 * it.)
		 */
		if (nbatch > 1)
			ExecHashBuildSkewHash(hashtable, node, num_skew_mcvs);
	}

	return hashtable;
}


/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows and average row width).
 *
 * This is exported so that the planner's costsize.c can use it.
 */

/* Target bucket loading (tuples per bucket) */
#define NTUP_PER_BUCKET			1

void
ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
						bool shared, int parallel_workers,
						size_t *space_allowed,
						int *numbuckets,
						int *numbatches,
						int *num_skew_mcvs)
{
	int			tupsize;
	double		inner_rel_bytes;
	long		bucket_bytes;
	long		hash_table_bytes;
	long		skew_table_bytes;
	long		max_pointers;
	long		mppow2;
	int			nbatch = 1;
	int			nbuckets;
	double		dbuckets;

	/* Force a plausible relation size if no info */
	if (ntuples <= 0.0)
		ntuples = 1000.0;

	/* Estimate tupsize based on footprint of tuple in hashtable. */
	tupsize = HJTUPLE_OVERHEAD +
		MAXALIGN(SizeofMinimalTupleHeader) +
		MAXALIGN(tupwidth);

	/* Estimate total size including chunk overhead */
	if (tupsize > HASH_CHUNK_THRESHOLD)
	{
		/* Large tuples have a chunk each */
		inner_rel_bytes = ntuples * (tupsize + HASH_CHUNK_HEADER_SIZE);
	}
	else
	{
		int64 tuples;
		int tuples_per_chunk;
		int chunks;

		/* Small tuples get packed into fixed sized chunks */
		tuples_per_chunk = (HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE) / tupsize;
		tuples = (int64) ntuples;
		chunks = tuples / tuples_per_chunk + (tuples % tuples_per_chunk != 0);
		inner_rel_bytes = HASH_CHUNK_SIZE * chunks;
	}

	/*
	 * Target in-memory hashtable size is work_mem kilobytes.  Shared hash
	 * tables are allowed to multiply work_mem by the number of participants,
	 * since other non-shared memory based plans allow each participant to use
	 * work_mem for the same total.
	 */
	hash_table_bytes = work_mem * 1024L;
	if (shared && parallel_workers > 0)
		hash_table_bytes *= parallel_workers + 1;	/* one for the leader */
	*space_allowed = hash_table_bytes;

	/*
	 * If skew optimization is possible, estimate the number of skew buckets
	 * that will fit in the memory allowed, and decrement the assumed space
	 * available for the main hash table accordingly.
	 *
	 * We make the optimistic assumption that each skew bucket will contain
	 * one inner-relation tuple.  If that turns out to be low, we will recover
	 * at runtime by reducing the number of skew buckets.
	 *
	 * hashtable->skewBucket will have up to 8 times as many HashSkewBucket
	 * pointers as the number of MCVs we allow, since ExecHashBuildSkewHash
	 * will round up to the next power of 2 and then multiply by 4 to reduce
	 * collisions.
	 */
	if (useskew)
	{
		skew_table_bytes = hash_table_bytes * SKEW_WORK_MEM_PERCENT / 100;

		/*----------
		 * Divisor is:
		 * size of a hash tuple +
		 * worst-case size of skewBucket[] per MCV +
		 * size of skewBucketNums[] entry +
		 * size of skew bucket struct itself
		 *----------
		 */
		*num_skew_mcvs = skew_table_bytes / (tupsize +
											 (8 * sizeof(HashSkewBucket *)) +
											 sizeof(int) +
											 SKEW_BUCKET_OVERHEAD);
		if (*num_skew_mcvs > 0)
			hash_table_bytes -= skew_table_bytes;
	}
	else
		*num_skew_mcvs = 0;

	/*
	 * Set nbuckets to achieve an average bucket load of NTUP_PER_BUCKET when
	 * memory is filled, assuming a single batch; but limit the value so that
	 * the pointer arrays we'll try to allocate do not exceed work_mem nor
	 * MaxAllocSize.
	 *
	 * Note that both nbuckets and nbatch must be powers of 2 to make
	 * ExecHashGetBucketAndBatch fast.
	 */
	max_pointers = (work_mem * 1024L) / sizeof(HashJoinBucketHead);
	max_pointers = Min(max_pointers, MaxAllocSize / sizeof(HashJoinBucketHead));
	/* If max_pointers isn't a power of 2, must round it down to one */
	mppow2 = 1L << my_log2(max_pointers);
	if (max_pointers != mppow2)
		max_pointers = mppow2 / 2;

	/* Also ensure we avoid integer overflow in nbatch and nbuckets */
	/* (this step is redundant given the current value of MaxAllocSize) */
	max_pointers = Min(max_pointers, INT_MAX / 2);

	dbuckets = ceil(ntuples / NTUP_PER_BUCKET);
	dbuckets = Min(dbuckets, max_pointers);
	nbuckets = (int) dbuckets;
	/* don't let nbuckets be really small, though ... */
	nbuckets = Max(nbuckets, 1024);
	/* ... and force it to be a power of 2. */
	nbuckets = 1 << my_log2(nbuckets);

	/*
	 * If there's not enough space to store the projected number of tuples and
	 * the required bucket headers, we will need multiple batches.
	 */
	bucket_bytes = sizeof(HashJoinTuple) * nbuckets;
	if (inner_rel_bytes + bucket_bytes > hash_table_bytes)
	{
		/* We'll need multiple batches */
		long		lbuckets;
		double		dbatch;
		int			minbatch;
		long		bucket_size;

		/*
		 * Estimate the number of buckets we'll want to have when work_mem is
		 * entirely full.  Each bucket will contain a bucket pointer plus
		 * NTUP_PER_BUCKET tuples, whose projected size already includes
		 * overhead for the hash code, pointer to the next tuple, etc.
		 */
		bucket_size = (tupsize * NTUP_PER_BUCKET + sizeof(HashJoinTuple));
		lbuckets = 1L << my_log2(hash_table_bytes / bucket_size);
		lbuckets = Min(lbuckets, max_pointers);
		nbuckets = (int) lbuckets;
		nbuckets = 1 << my_log2(nbuckets);
		bucket_bytes = nbuckets * sizeof(HashJoinTuple);

		/*
		 * Buckets are simple pointers to hashjoin tuples, while tupsize
		 * includes the pointer, hash code, and MinimalTupleData.  So buckets
		 * should never really exceed 25% of work_mem (even for
		 * NTUP_PER_BUCKET=1); except maybe for work_mem values that are not
		 * 2^N bytes, where we might get more because of doubling. So let's
		 * look for 50% here.
		 */
		Assert(bucket_bytes <= hash_table_bytes / 2);

		/* Calculate required number of batches. */
		dbatch = ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes));
		dbatch = Min(dbatch, max_pointers);
		minbatch = (int) dbatch;
		nbatch = 2;
		while (nbatch < minbatch)
			nbatch <<= 1;
	}

	Assert(nbuckets > 0);
	Assert(nbatch > 0);

	*numbuckets = nbuckets;
	*numbatches = nbatch;
}

/*
 * Detach from the shared hash table, freeing all memory if we are the last to
 * detach.
 */
void
ExecHashTableDetach(HashJoinTable hashtable)
{
	if (HashJoinTableIsShared(hashtable) && !hashtable->detached_early)
	{
		Barrier *barrier = &hashtable->shared->barrier;

		/*
		 * Instead of waiting at the end of a hash join for all participants
		 * to finish, we detach and let the last to detach clean up the shared
		 * resources.  This avoids unnecessary waiting at the end of single
		 * batch probes.
		 */
		if (BarrierDetach(barrier))
		{
			/* Serial: free the buckets and chunks */
			if (DsaPointerIsValid(hashtable->shared->buckets))
			{
				dsa_pointer chunk_shared;

				/*
				 * We could just forget about the memory, since the whole area
				 * will be freed at the end of the query anyway.  But that
				 * wouldn't work for rescans, where we'll be allocating a
				 * whole hashtable again, creating a leak.  Perhaps we could
				 * consider moving all the chunks to a freelist here for reuse
				 * in the cast of a rescan, so that we can avoid the cost of
				 * one backend freeing all chunks in the common case.
				 */
				dsa_free(hashtable->area, hashtable->shared->buckets);
				hashtable->shared->buckets = InvalidDsaPointer;
				hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
				while (pop_chunk_queue(hashtable, &chunk_shared) != NULL)
					dsa_free(hashtable->area, chunk_shared);
			}
		}
		hashtable->shared = NULL;
	}
}

/* ----------------------------------------------------------------
 *		ExecHashTableDestroy
 *
 *		destroy a hash table
 * ----------------------------------------------------------------
 */
void
ExecHashTableDestroy(HashJoinTable hashtable)
{
	int			i;

	/* If shared, clean up shared memory and detach. */
	ExecHashTableDetach(hashtable);

	/*
	 * Make sure all the temp files are closed.  We skip batch 0, since it
	 * can't have any temp files (and the arrays might not even exist if
	 * nbatch is only 1).
	 */
	for (i = 1; i < hashtable->nbatch; i++)
	{
		if (hashtable->innerBatchFile[i])
			BufFileClose(hashtable->innerBatchFile[i]);
		if (hashtable->outerBatchFile[i])
			BufFileClose(hashtable->outerBatchFile[i]);
	}

	/* Release working memory (batchCxt is a child, so it goes away too) */
	MemoryContextDelete(hashtable->hashCxt);

	/* And drop the control block */
	pfree(hashtable);
}

/*
 * ExecHashIncreaseNumBatches
 *		increase the original number of batches in order to reduce
 *		current memory consumption
 */
static void
ExecHashIncreaseNumBatches(HashJoinTable hashtable, int nbatch)
{
	int			oldnbatch = hashtable->nbatch;
	MemoryContext oldcxt;

	/* safety check to avoid overflow */
	if (oldnbatch > Min(INT_MAX / 2, MaxAllocSize / (sizeof(void *) * 2)))
		return;

	Assert(nbatch > 1);

#ifdef HJDEBUG
	printf("Hashjoin %p: increasing nbatch to %d because space = %zu\n",
		   hashtable, nbatch, hashtable->spaceUsed);
#endif

	TRACE_POSTGRESQL_HASH_INCREASE_BATCHES(oldnbatch, nbatch);

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	if (hashtable->innerBatchFile == NULL)
	{
		/* we had no file arrays before */
		hashtable->innerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		/* time to establish the temp tablespaces, too */
		PrepareTempTablespaces();
	}
	else
	{
		/* enlarge arrays and zero out added entries */
		hashtable->innerBatchFile = (BufFile **)
			repalloc(hashtable->innerBatchFile, nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			repalloc(hashtable->outerBatchFile, nbatch * sizeof(BufFile *));
		MemSet(hashtable->innerBatchFile + oldnbatch, 0,
			   (nbatch - oldnbatch) * sizeof(BufFile *));
		MemSet(hashtable->outerBatchFile + oldnbatch, 0,
			   (nbatch - oldnbatch) * sizeof(BufFile *));
	}

	MemoryContextSwitchTo(oldcxt);

	hashtable->nbatch = nbatch;

	/* TODO: If know we need to resize nbuckets, we can do it while rebatching. */
}

/*
 * Process the queue of chunks whose tuples need to be redistributed into the
 * correct batches until it is empty.  In the best case this will shrink the
 * hash table, keeping about half of the tuples in memory and sending the rest
 * to a future batch.
 */
static void
ExecHashShrink(HashJoinTable hashtable)
{
	long		ninmemory;
	long		nfreed;
	dsa_pointer chunk_shared;
	HashMemoryChunk chunk;
#ifdef TRACE_POSTGRESQL_HASH_SHRINK_DONE
	int tuples_processed = 0;
	int chunks_processed = 0;
#endif

	TRACE_POSTGRESQL_HASH_SHRINK_START();

	if (HashJoinTableIsShared(hashtable))
	{
		/*
		 * Since a newly launched participant could arrive while shrinking is
		 * already underway, we need to be able to jump to the correct place
		 * in this function.
		 */
		switch (BarrierPhase(&hashtable->shared->shrink_barrier))
		{
		case PHJ_SHRINK_PHASE_BEGINNING: /* likely case */
			break;
		case PHJ_SHRINK_PHASE_CLEARING:
			goto clearing;
		case PHJ_SHRINK_PHASE_WORKING:
			goto working;
		case PHJ_SHRINK_PHASE_DECIDING:
			goto deciding;
		}

		/*
		 * We wait until all participants have reached this point.  We need to
		 * do that because we can't clear the hash table if any partipicant is
		 * still inserting tuples into it, and we can't modify chunks that any
		 * participant is still writing into.
		 */
		if (BarrierWait(&hashtable->shared->shrink_barrier,
						WAIT_EVENT_HASH_SHRINKING1))
		{
			/* Serial phase: one participant clears the hash table. */
			memset(hashtable->buckets, 0,
				   hashtable->nbuckets * sizeof(HashJoinBucketHead));
		}
	clearing:
		/* Wait until hash table is cleared. */
		BarrierWait(&hashtable->shared->shrink_barrier,
					WAIT_EVENT_HASH_SHRINKING2);

		Assert(hashtable->shared->nbatch == hashtable->nbatch);
	}
	else
	{
		/* Clear the hash table. */
		memset(hashtable->buckets, 0,
			   sizeof(HashJoinBucketHead) * hashtable->nbuckets);
	}

 working:
	/* Pop first chunk from the shrink queue. */
	if (HashJoinTableIsShared(hashtable))
		chunk = pop_chunk_queue(hashtable, &chunk_shared);
	else
	{
		chunk = hashtable->chunks;
		hashtable->chunks = NULL;
	}

	while (chunk != NULL)
	{
		/* position within the buffer (up to oldchunks->used) */
		size_t		idx = 0;

		/* process all tuples stored in this chunk (and then free it) */
		while (idx < chunk->used)
		{
			HashJoinTuple hashTuple = (HashJoinTuple) (chunk->data + idx);
			MinimalTuple tuple = HJTUPLE_MINTUPLE(hashTuple);
			int			hashTupleSize = (HJTUPLE_OVERHEAD + tuple->t_len);
			int			bucketno;
			int			batchno;

			ninmemory++;
			ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
									  &bucketno, &batchno);

			if (batchno == hashtable->curbatch)
			{
				/* keep tuple in memory - copy it into the new chunk */
				HashJoinTuple copyTuple;
				dsa_pointer copyShared = InvalidDsaPointer;

				if (HashJoinTableIsShared(hashtable))
					copyTuple = (HashJoinTuple)
						dense_alloc_shared(hashtable, hashTupleSize,
										   &copyShared, false);
				else
					copyTuple = (HashJoinTuple)
						dense_alloc(hashtable, hashTupleSize, false);
				memcpy(copyTuple, hashTuple, hashTupleSize);

				/* and add it back to the appropriate bucket */
				insert_tuple_into_bucket(hashtable, bucketno, copyTuple,
										 copyShared);
			}
			else
			{
				/* dump it out */
				Assert(batchno > hashtable->curbatch);
				ExecHashJoinSaveTuple(HJTUPLE_MINTUPLE(hashTuple),
									  hashTuple->hashvalue,
									  &hashtable->innerBatchFile[batchno]);

				nfreed++;
			}

			/* next tuple in this chunk */
			idx += MAXALIGN(hashTupleSize);

#ifdef TRACE_POSTGRESQL_HASH_SHRINK_DONE
			++tuples_processed;
#endif
		}

#ifdef TRACE_POSTGRESQL_HASH_SHRINK_DONE
		++chunks_processed;
#endif

		/* Free chunk and pop next from the queue. */
		if (HashJoinTableIsShared(hashtable))
		{
			Size size = chunk->maxlen + HASH_CHUNK_HEADER_SIZE;

			dsa_free(hashtable->area, chunk_shared);

			LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);
			Assert(hashtable->shared->size > size);
			hashtable->shared->size -= size;
			hashtable->shared->nfreed += nfreed;
			hashtable->shared->ninmemory += ninmemory;
			nfreed = 0;
			ninmemory = 0;
			chunk = pop_chunk_queue_unlocked(hashtable, &chunk_shared);
			hashtable->spaceUsed = hashtable->shared->size;
			LWLockRelease(&hashtable->shared->chunk_lock);
		}
		else
		{
			Size size = chunk->maxlen + HASH_CHUNK_HEADER_SIZE;
			HashMemoryChunk nextchunk = chunk->next.unshared;

			Assert(hashtable->spaceUsed > size);
			hashtable->spaceUsed -= size;
			pfree(chunk);
			chunk = nextchunk;
		}
	}
	TRACE_POSTGRESQL_HASH_SHRINK_DONE(tuples_processed, chunks_processed);

#ifdef HJDEBUG
	printf("Hashjoin %p: freed %ld of %ld tuples, space now %zu\n",
		   hashtable, nfreed, ninmemory, hashtable->spaceUsed);
#endif

	/*
	 * If we dumped out either all or none of the tuples in the table, disable
	 * further expansion of nbatch.  This situation implies that we have
	 * enough tuples of identical hashvalues to overflow spaceAllowed.
	 * Increasing nbatch will not fix it since there's no way to subdivide the
	 * group any more finely. We have to just gut it out and hope the server
	 * has enough RAM.
	 */
	if (HashJoinTableIsShared(hashtable))
	{
		/*
		 * Wait until all have finished shrinking chunks.  We need to do that
		 * because we need the total tuple counts before we can decide whether
		 * to prevent further attempts at shrinking.
		 */
		if (BarrierWait(&hashtable->shared->shrink_barrier,
						WAIT_EVENT_HASH_SHRINKING3))
		{
			/* Serial phase: one participant decides whether that paid off. */
			if (hashtable->shared->nfreed == 0 ||
				hashtable->shared->nfreed == hashtable->shared->ninmemory)
			{
				TRACE_POSTGRESQL_HASH_SHRINK_DISABLED();
				hashtable->shared->grow_enabled = false;
#ifdef HJDEBUG
			printf("Hashjoin %p: disabling further increase of nbatch\n",
				   hashtable);
#endif
			}
			hashtable->shared->shrink_needed = false;
		}
	deciding:
		/* Wait for above decision to be made. */
		BarrierWaitSet(&hashtable->shared->shrink_barrier,
					   PHJ_SHRINK_PHASE_BEGINNING,
					   WAIT_EVENT_HASH_SHRINKING4);
	}
	else
	{
		if (nfreed == 0 || nfreed == ninmemory)
		{
			TRACE_POSTGRESQL_HASH_SHRINK_DISABLED();
			hashtable->growEnabled = false;
#ifdef HJDEBUG
			printf("Hashjoin %p: disabling further increase of nbatch\n",
				   hashtable);
#endif
		}
	}
}

/*
 * Update the local hashtable with the current pointers and sizes from
 * hashtable->shared.
 */
void
ExecHashUpdate(HashJoinTable hashtable)
{
	if (!HashJoinTableIsShared(hashtable))
		return;

	/* The hash table. */
	hashtable->spaceUsed = hashtable->shared->size;
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;
	hashtable->nbuckets = hashtable->shared->nbuckets;
	hashtable->log2_nbuckets = my_log2(hashtable->nbuckets);
	hashtable->buckets = (HashJoinBucketHead *)
		dsa_get_address(hashtable->area, hashtable->shared->buckets);
	hashtable->curbatch =
		PHJ_PHASE_TO_BATCHNO(BarrierPhase(&hashtable->shared->barrier));
	if (hashtable->shared->nbatch > hashtable->nbatch)
		ExecHashIncreaseNumBatches(hashtable, hashtable->shared->nbatch);
}

/*
 * ExecHashIncreaseNumBuckets
 *		increase the original number of buckets in order to reduce
 *		number of tuples per bucket
 */
static void
ExecHashIncreaseNumBuckets(HashJoinTable hashtable)
{
	/* do nothing if not an increase (it's called increase for a reason) */
	if (hashtable->nbuckets >= hashtable->nbuckets_optimal)
		return;

#ifdef HJDEBUG
	printf("Hashjoin %p: increasing nbuckets %d => %d\n",
		   hashtable, hashtable->nbuckets, hashtable->nbuckets_optimal);
#endif

	TRACE_POSTGRESQL_HASH_INCREASE_BUCKETS(hashtable->nbuckets,
										   hashtable->nbuckets_optimal);

	/* account for the increase in space that will be used by buckets */
	hashtable->spaceUsed += sizeof(HashJoinBucketHead) *
		(hashtable->nbuckets_optimal - hashtable->nbuckets);
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;

	hashtable->nbuckets = hashtable->nbuckets_optimal;
	hashtable->log2_nbuckets = hashtable->log2_nbuckets_optimal;

	Assert(hashtable->nbuckets > 1);
	Assert(hashtable->nbuckets <= (INT_MAX / 2));
	Assert(hashtable->nbuckets == (1 << hashtable->log2_nbuckets));

	/*
	 * Just reallocate the proper number of buckets - we don't need to walk
	 * through them - we can walk the dense-allocated chunks (just like in
	 * ExecHashIncreaseNumBatches, but without all the copying into new
	 * chunks).  That happens in ExecHashReinsertAll.
	 */
	if (HashJoinTableIsShared(hashtable))
	{
		HashJoinBucketHead *buckets;
		int i;

		Assert(BarrierPhase(&hashtable->shared->barrier) == PHJ_PHASE_RESIZING);

		/* Free the existing bucket array. */
		dsa_free(hashtable->area, hashtable->shared->buckets);

		/*
		 * Share the bucket array and size information, which all backends
		 * will pick up when they run ExecHashUpdate.
		 */
		hashtable->shared->size = hashtable->spaceUsed;
		hashtable->shared->nbuckets = hashtable->nbuckets;
		hashtable->shared->log2_nbuckets = hashtable->log2_nbuckets;
		hashtable->shared->buckets =
			dsa_allocate(hashtable->area,
						 hashtable->nbuckets * sizeof(HashJoinBucketHead));
		if (!DsaPointerIsValid(hashtable->shared->buckets))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("out of memory")));

		/* Initialize the new buckets. */
		buckets = dsa_get_address(hashtable->area,
								  hashtable->shared->buckets);
		for (i = 0; i < hashtable->nbuckets; ++i)
			dsa_pointer_atomic_init(&buckets[i].shared,
									InvalidDsaPointer);

		/* ExecHashReinsert needs to process all chunks. */
		hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
	}
	else
	{
		hashtable->buckets = (HashJoinBucketHead *)
			repalloc(hashtable->buckets,
					 hashtable->nbuckets * sizeof(HashJoinBucketHead));
		hashtable->chunks_to_reinsert = hashtable->chunks;
		memset(hashtable->buckets, 0,
			   hashtable->nbuckets * sizeof(HashJoinBucketHead));
	}
}

/*
 * ExecHashReinsert
 *		reinsert the tuples from all chunks into the hashtable after increasing
 *		the number of buckets
 */
static void
ExecHashReinsertAll(HashJoinTable hashtable)
{
	HashMemoryChunk chunk;
	dsa_pointer chunk_shared;
#ifdef TRACE_POSTGRESQL_HASH_REINSERT_DONE
	int tuples_processed = 0;
	int chunks_processed = 0;
#endif

	/* scan through all tuples in all chunks to rebuild the hash table */
	TRACE_POSTGRESQL_HASH_REINSERT_START();
	if (HashJoinTableIsShared(hashtable))
		chunk = pop_chunk_queue(hashtable, &chunk_shared);
	else
		chunk = hashtable->chunks_to_reinsert;

	while (chunk != NULL)
	{
		/* process all tuples stored in this chunk */
		size_t		idx = 0;

		while (idx < chunk->used)
		{
			dsa_pointer hashTuple_shared = InvalidDsaPointer;
			HashJoinTuple hashTuple = (HashJoinTuple) (chunk->data + idx);
			int			bucketno;
			int			batchno;

			ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue,
									  &bucketno, &batchno);

			/* add the tuple to the proper bucket */
			if (HashJoinTableIsShared(hashtable))
				hashTuple_shared = chunk_shared + HASH_CHUNK_HEADER_SIZE + idx;
			insert_tuple_into_bucket(hashtable, bucketno, hashTuple,
									 hashTuple_shared);

			/* advance index past the tuple */
			idx += MAXALIGN(HJTUPLE_OVERHEAD +
							HJTUPLE_MINTUPLE(hashTuple)->t_len);

#ifdef TRACE_POSTGRESQL_HASH_REINSERT_DONE
			++tuples_processed;
#endif
		}

#ifdef TRACE_POSTGRESQL_HASH_REINSERT_DONE
		++chunks_processed;
#endif

		/* advance to the next chunk */
		if (HashJoinTableIsShared(hashtable))
			chunk = pop_chunk_queue(hashtable, &chunk_shared);
		else
			chunk = chunk->next.unshared;
	}
	TRACE_POSTGRESQL_HASH_REINSERT_DONE(tuples_processed, chunks_processed);
}


/*
 * ExecHashTableInsert
 *		insert a tuple into the hash table depending on the hash value
 *		it may just go to a temp file for later batches
 *
 * Note: the passed TupleTableSlot may contain a regular, minimal, or virtual
 * tuple; the minimal case in particular is certain to happen while reloading
 * tuples from batch files.  We could save some cycles in the regular-tuple
 * case by not forcing the slot contents into minimal form; not clear if it's
 * worth the messiness required.
 */
void
ExecHashTableInsert(HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue)
{
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
	int			bucketno;
	int			batchno;

	ExecHashGetBucketAndBatch(hashtable, hashvalue,
							  &bucketno, &batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if (batchno == hashtable->curbatch)
	{
		/*
		 * put the tuple in hash table
		 */
		HashJoinTuple hashTuple;
		dsa_pointer hashTuple_shared = InvalidDsaPointer;
		int			hashTupleSize;
		double		ntuples = (hashtable->totalTuples - hashtable->skewTuples);

		/* Create the HashJoinTuple */
		hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
		if (HashJoinTableIsShared(hashtable))
			hashTuple = (HashJoinTuple)
				dense_alloc_shared(hashtable, hashTupleSize,
								   &hashTuple_shared, true);
		else
			hashTuple = (HashJoinTuple)
				dense_alloc(hashtable, hashTupleSize, true);

		hashTuple->hashvalue = hashvalue;
		memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);

		/*
		 * We always reset the tuple-matched flag on insertion.  This is okay
		 * even when reloading a tuple from a batch file, since the tuple
		 * could not possibly have been matched to an outer tuple before it
		 * went into the batch file.
		 */
		HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

		/* Push it onto the front of the bucket's list */
		insert_tuple_into_bucket(hashtable, bucketno, hashTuple,
								 hashTuple_shared);

		/*
		 * Increase the (optimal) number of buckets if we just exceeded the
		 * NTUP_PER_BUCKET threshold, but only when there's still a single
		 * batch.
		 */
		if (hashtable->nbatch == 1 &&
			ntuples > (hashtable->nbuckets_optimal * NTUP_PER_BUCKET))
		{
			/* Guard against integer overflow and alloc size overflow */
			if (hashtable->nbuckets_optimal <= INT_MAX / 2 &&
				hashtable->nbuckets_optimal * 2 <= MaxAllocSize / sizeof(HashJoinTuple))
			{
				hashtable->nbuckets_optimal *= 2;
				hashtable->log2_nbuckets_optimal += 1;
			}
		}
	}
	else
	{
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert(batchno > hashtable->curbatch);
		ExecHashJoinSaveTuple(tuple,
							  hashvalue,
							  &hashtable->innerBatchFile[batchno]);
	}
}

/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in either econtext->ecxt_outertuple or
 * econtext->ecxt_innertuple.  Vars in the hashkeys expressions should have
 * varno either OUTER_VAR or INNER_VAR.
 *
 * A TRUE result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A FALSE result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then FALSE is never returned.)
 */
bool
ExecHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	if (outer_tuple)
		hashfunctions = hashtable->outer_hashfunctions;
	else
		hashfunctions = hashtable->inner_hashfunctions;

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (hashtable->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashfunctions[i], keyval));
			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecHashGetBucketAndBatch
 *		Determine the bucket number and batch number for a hash value
 *
 * Note: on-the-fly increases of nbatch must not change the bucket number
 * for a given hash code (since we don't move tuples to different hash
 * chains), and must only cause the batch number to remain the same or
 * increase.  Our algorithm is
 *		bucketno = hashvalue MOD nbuckets
 *		batchno = (hashvalue DIV nbuckets) MOD nbatch
 * where nbuckets and nbatch are both expected to be powers of 2, so we can
 * do the computations by shifting and masking.  (This assumes that all hash
 * functions are good about randomizing all their output bits, else we are
 * likely to have very skewed bucket or batch occupancy.)
 *
 * nbuckets and log2_nbuckets may change while nbatch == 1 because of dynamic
 * bucket count growth.  Once we start batching, the value is fixed and does
 * not change over the course of the join (making it possible to compute batch
 * number the way we do here).
 *
 * nbatch is always a power of 2; we increase it only by doubling it.  This
 * effectively adds one more bit to the top of the batchno.
 */
void
ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno)
{
	uint32		nbuckets = (uint32) hashtable->nbuckets;
	uint32		nbatch = (uint32) hashtable->nbatch;

	if (nbatch > 1)
	{
		/* we can do MOD by masking, DIV by shifting */
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = (hashvalue >> hashtable->log2_nbuckets) & (nbatch - 1);
	}
	else
	{
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = 0;
	}
}

/*
 * ExecScanHashBucket
 *		scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool
ExecScanHashBucket(HashJoinState *hjstate,
				   ExprContext *econtext)
{
	List	   *hjclauses = hjstate->hashclauses;
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple = hjstate->hj_CurTuple;
	uint32		hashvalue = hjstate->hj_CurHashValue;

	/*
	 * hj_CurTuple is the address of the tuple last returned from the current
	 * bucket, or NULL if it's time to start scanning a new bucket.
	 *
	 * If the tuple hashed to a skew bucket then scan the skew bucket
	 * otherwise scan the standard hashtable bucket.
	 */
	if (hashTuple != NULL)
		hashTuple = next_tuple_in_bucket(hashtable, hashTuple);
	else if (hjstate->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
		hashTuple = hashtable->skewBucket[hjstate->hj_CurSkewBucketNo]->tuples;
	else
		hashTuple = first_tuple_in_bucket(hashtable, hjstate->hj_CurBucketNo);

	while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue)
		{
			TupleTableSlot *inntuple;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
											 hjstate->hj_HashTupleSlot,
											 false);	/* do not pfree */
			econtext->ecxt_innertuple = inntuple;

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext, false))
			{
				hjstate->hj_CurTuple = hashTuple;
				return true;
			}
		}

		hashTuple = next_tuple_in_bucket(hashtable, hashTuple);
	}

	/*
	 * no match
	 */
	return false;
}

/*
 * ExecPrepHashTableForUnmatched
 *		set up for a series of ExecScanHashTableForUnmatched calls
 */
void
ExecPrepHashTableForUnmatched(HashJoinState *hjstate)
{
	/*----------
	 * During this scan we use the HashJoinState fields as follows:
	 *
	 * hj_HashTable->unmatched_chunks: the queue of chunks to scan
	 * hj_HashTable->current_chunk: chunk being scanned currently
	 * hj_HashTable->current_chunk_index: position within chunk
	 * hj_CurSkewBucketNo: next skew bucket (an index into skewBucketNums)
	 * hj_CurTuple: last skew tuple returned, or NULL to start next bucket
	 *----------
	 */
	hjstate->hj_HashTable->unmatched_chunks = hjstate->hj_HashTable->chunks;
	hjstate->hj_HashTable->current_chunk = NULL;
	hjstate->hj_CurSkewBucketNo = 0;
	hjstate->hj_CurTuple = NULL;

	TRACE_POSTGRESQL_HASH_UNMATCHED_START();
}

/*
 * ExecScanHashTableForUnmatched
 *		scan the hash table for unmatched inner tuples
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool
ExecScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple;
	MinimalTuple tuple;

	/*
	 * First, process the queue of chunks holding tuples that are in regular
	 * (non-skew) buckets.
	 */
	for (;;)
	{
		/* Do we need a new chunk to scan? */
		if (hashtable->current_chunk == NULL)
		{
			/* Pop the next chunk from the front of the queue. */
			if (HashJoinTableIsShared(hashtable))
			{
				hashtable->current_chunk =
					pop_chunk_queue(hashtable,
									&hashtable->current_chunk_shared);
			}
			else if (hashtable->unmatched_chunks != NULL)
			{
				hashtable->current_chunk = hashtable->unmatched_chunks;
				hashtable->unmatched_chunks =
					hashtable->current_chunk->next.unshared;
			}
			hashtable->current_chunk_index = 0;
		}

		/* Have we run out of chunks to scan? */
		if (hashtable->current_chunk == NULL)
			break;

		/* Have we reached the end of this chunk yet? */
		if (hashtable->current_chunk_index >= hashtable->current_chunk->used)
		{
			/* Go around again to get the next chunk from the queue. */
			hashtable->current_chunk = NULL;
			continue;
		}

		/* Take the next tuple from this chunk. */
		hashTuple = (HashJoinTuple)
			(hashtable->current_chunk->data + hashtable->current_chunk_index);
		tuple = HJTUPLE_MINTUPLE(hashTuple);
		hashtable->current_chunk_index +=
			MAXALIGN(HJTUPLE_OVERHEAD + tuple->t_len);

		/* Is it unmatched? */
		if (!HeapTupleHeaderHasMatch(tuple))
		{
			TupleTableSlot *inntuple;

			/* insert hashtable's tuple into exec slot */
			inntuple = ExecStoreMinimalTuple(tuple,
											 hjstate->hj_HashTupleSlot,
											 false); /* do not pfree */
			econtext->ecxt_innertuple = inntuple;

			/* reset context each time (see below for explanation) */
			ResetExprContext(econtext);
			return true;
		}
	}

	/*
	 * Next, scan all skew buckets, since those tuples are not stored in
	 * chunks.
	 */
	hashTuple = hjstate->hj_CurTuple;
	for (;;)
	{
		/*
		 * hj_CurTuple is the address of the tuple last returned from the
		 * current bucket, or NULL if it's time to start scanning a new
		 * bucket.
		 */
		if (hashTuple != NULL)
			hashTuple = hashTuple->next.unshared;
		else if (hjstate->hj_CurSkewBucketNo < hashtable->nSkewBuckets)
		{
			int			j = hashtable->skewBucketNums[hjstate->hj_CurSkewBucketNo];

			hashTuple = hashtable->skewBucket[j]->tuples;
			hjstate->hj_CurSkewBucketNo++;
		}
		else
			break;				/* finished all buckets */

		while (hashTuple != NULL)
		{
			if (!HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple)))
			{
				TupleTableSlot *inntuple;

				/* insert hashtable's tuple into exec slot */
				inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
												 hjstate->hj_HashTupleSlot,
												 false);		/* do not pfree */
				econtext->ecxt_innertuple = inntuple;

				/*
				 * Reset temp memory each time; although this function doesn't
				 * do any qual eval, the caller will, so let's keep it
				 * parallel to ExecScanHashBucket.
				 */
				ResetExprContext(econtext);

				hjstate->hj_CurTuple = hashTuple;
				return true;
			}

			hashTuple = hashTuple->next.unshared;
		}
	}

	/*
	 * no more unmatched tuples
	 */
	TRACE_POSTGRESQL_HASH_UNMATCHED_DONE();
	return false;
}

/*
 * ExecHashTableReset
 *
 *		reset hash table header for new batch
 */
void
ExecHashTableReset(HashJoinTable hashtable)
{
	MemoryContext oldcxt;
	int			nbuckets = hashtable->nbuckets;

	if (HashJoinTableIsShared(hashtable))
	{
		/*
		 * Wait for all workers to finish accessing the hash table for the
		 * previous batch.
		 */
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_UNMATCHED_BATCH(hashtable->curbatch - 1));
		if (BarrierWait(&hashtable->shared->barrier, WAIT_EVENT_HASH_UNMATCHED))
		{
			/* Serial phase: prepare shared state for new batch. */
			dsa_pointer chunk_shared;

			Assert(BarrierPhase(&hashtable->shared->barrier) ==
				   PHJ_PHASE_RESETTING_BATCH(hashtable->curbatch));

			/* Clear the hash table. */
			memset(hashtable->buckets, 0,
				   sizeof(HashJoinBucketHead) * hashtable->nbuckets);

			/* Free all the chunks. */
			hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
			hashtable->shared->chunks = InvalidDsaPointer;
			while (pop_chunk_queue(hashtable, &chunk_shared) != NULL)
				dsa_free(hashtable->area, chunk_shared);

			/* Reset the hash table size. */
			hashtable->shared->size =
				(hashtable->nbuckets * sizeof(HashJoinBucketHead));

			/* Rewind the shared read heads for this batch, inner and outer. */
			ExecHashJoinRewindBatches(hashtable, hashtable->curbatch);
		}

		/*
		 * Export this participant's inner and outer batch files, because they
		 * are now read-only.
		 */
		ExecHashJoinExportBatch(hashtable, hashtable->curbatch, true);
		ExecHashJoinExportBatch(hashtable, hashtable->curbatch, false);

		/*
		 * Wait again, so that all workers see the new hash table and can
		 * safely read from batch files from any participant.
		 */
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_RESETTING_BATCH(hashtable->curbatch));
		BarrierWait(&hashtable->shared->barrier, WAIT_EVENT_HASH_RESETTING);
		Assert(BarrierPhase(&hashtable->shared->barrier) ==
			   PHJ_PHASE_LOADING_BATCH(hashtable->curbatch));
		ExecHashUpdate(hashtable);

		/* Forget the current chunks. */
		hashtable->current_chunk = NULL;
		return;
	}

	/*
	 * Release all the hash buckets and tuples acquired in the prior pass, and
	 * reinitialize the context for a new pass.
	 */
	MemoryContextReset(hashtable->batchCxt);
	oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

	/* Reallocate and reinitialize the hash bucket headers. */
	hashtable->buckets = (HashJoinBucketHead *)
		palloc0(nbuckets * sizeof(HashJoinBucketHead));

	hashtable->spaceUsed = nbuckets * sizeof(HashJoinTuple);

	MemoryContextSwitchTo(oldcxt);

	/* Forget the chunks (the memory was freed by the context reset above). */
	hashtable->chunks = NULL;
}

/*
 * ExecHashTableResetMatchFlags
 *		Clear all the HeapTupleHeaderHasMatch flags in the table
 */
void
ExecHashTableResetMatchFlags(HashJoinTable hashtable)
{
	dsa_pointer chunk_shared = InvalidDsaPointer;
	HashMemoryChunk chunk;
	HashJoinTuple tuple;
	int			i;

	/* Reset all flags in the main table ... */
	TRACE_POSTGRESQL_HASH_RESET_MATCH_START();

	if (HashJoinTableIsShared(hashtable))
	{
		/* This only runs in the leader during rescan initialization. */
		Assert(!IsParallelWorker());
		hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
		chunk = pop_chunk_queue(hashtable, &chunk_shared);
	}
	else
		chunk = hashtable->chunks;

	while (chunk != NULL)
	{
		Size index = 0;

		/* Clear the flag for all tuples in this chunk. */
		while (index < chunk->used)
		{
			tuple = (HashJoinTuple) (chunk->data + index);
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
			index += MAXALIGN(HJTUPLE_OVERHEAD +
							  HJTUPLE_MINTUPLE(tuple)->t_len);
		}
		if (HashJoinTableIsShared(hashtable))
			chunk = pop_chunk_queue(hashtable, &chunk_shared);
		else
			chunk = chunk->next.unshared;
	}

	/* ... and the same for the skew buckets, if any */
	for (i = 0; i < hashtable->nSkewBuckets; i++)
	{
		int			j = hashtable->skewBucketNums[i];
		HashSkewBucket *skewBucket = hashtable->skewBucket[j];

		for (tuple = skewBucket->tuples; tuple != NULL;
			 tuple = tuple->next.unshared)
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
	}
	TRACE_POSTGRESQL_HASH_RESET_MATCH_DONE();
}


void
ExecReScanHash(HashState *node)
{
	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}


/*
 * ExecHashBuildSkewHash
 *
 *		Set up for skew optimization if we can identify the most common values
 *		(MCVs) of the outer relation's join key.  We make a skew hash bucket
 *		for the hash value of each MCV, up to the number of slots allowed
 *		based on available memory.
 */
static void
ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node, int mcvsToUse)
{
	HeapTupleData *statsTuple;
	Datum	   *values;
	int			nvalues;
	float4	   *numbers;
	int			nnumbers;

	/* Do nothing if planner didn't identify the outer relation's join key */
	if (!OidIsValid(node->skewTable))
		return;
	/* Also, do nothing if we don't have room for at least one skew bucket */
	if (mcvsToUse <= 0)
		return;

	/*
	 * Try to find the MCV statistics for the outer relation's join key.
	 */
	statsTuple = SearchSysCache3(STATRELATTINH,
								 ObjectIdGetDatum(node->skewTable),
								 Int16GetDatum(node->skewColumn),
								 BoolGetDatum(node->skewInherit));
	if (!HeapTupleIsValid(statsTuple))
		return;

	if (get_attstatsslot(statsTuple, node->skewColType, node->skewColTypmod,
						 STATISTIC_KIND_MCV, InvalidOid,
						 NULL,
						 &values, &nvalues,
						 &numbers, &nnumbers))
	{
		double		frac;
		int			nbuckets;
		FmgrInfo   *hashfunctions;
		int			i;

		if (mcvsToUse > nvalues)
			mcvsToUse = nvalues;

		/*
		 * Calculate the expected fraction of outer relation that will
		 * participate in the skew optimization.  If this isn't at least
		 * SKEW_MIN_OUTER_FRACTION, don't use skew optimization.
		 */
		frac = 0;
		for (i = 0; i < mcvsToUse; i++)
			frac += numbers[i];
		if (frac < SKEW_MIN_OUTER_FRACTION)
		{
			free_attstatsslot(node->skewColType,
							  values, nvalues, numbers, nnumbers);
			ReleaseSysCache(statsTuple);
			return;
		}

		/*
		 * Okay, set up the skew hashtable.
		 *
		 * skewBucket[] is an open addressing hashtable with a power of 2 size
		 * that is greater than the number of MCV values.  (This ensures there
		 * will be at least one null entry, so searches will always
		 * terminate.)
		 *
		 * Note: this code could fail if mcvsToUse exceeds INT_MAX/8 or
		 * MaxAllocSize/sizeof(void *)/8, but that is not currently possible
		 * since we limit pg_statistic entries to much less than that.
		 */
		nbuckets = 2;
		while (nbuckets <= mcvsToUse)
			nbuckets <<= 1;
		/* use two more bits just to help avoid collisions */
		nbuckets <<= 2;

		hashtable->skewEnabled = true;
		hashtable->skewBucketLen = nbuckets;

		/*
		 * We allocate the bucket memory in the hashtable's batch context. It
		 * is only needed during the first batch, and this ensures it will be
		 * automatically removed once the first batch is done.
		 */
		hashtable->skewBucket = (HashSkewBucket **)
			MemoryContextAllocZero(hashtable->batchCxt,
								   nbuckets * sizeof(HashSkewBucket *));
		hashtable->skewBucketNums = (int *)
			MemoryContextAllocZero(hashtable->batchCxt,
								   mcvsToUse * sizeof(int));

		hashtable->spaceUsed += nbuckets * sizeof(HashSkewBucket *)
			+ mcvsToUse * sizeof(int);
		hashtable->spaceUsedSkew += nbuckets * sizeof(HashSkewBucket *)
			+ mcvsToUse * sizeof(int);
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;

		/*
		 * Create a skew bucket for each MCV hash value.
		 *
		 * Note: it is very important that we create the buckets in order of
		 * decreasing MCV frequency.  If we have to remove some buckets, they
		 * must be removed in reverse order of creation (see notes in
		 * ExecHashRemoveNextSkewBucket) and we want the least common MCVs to
		 * be removed first.
		 */
		hashfunctions = hashtable->outer_hashfunctions;

		for (i = 0; i < mcvsToUse; i++)
		{
			uint32		hashvalue;
			int			bucket;

			hashvalue = DatumGetUInt32(FunctionCall1(&hashfunctions[0],
													 values[i]));

			/*
			 * While we have not hit a hole in the hashtable and have not hit
			 * the desired bucket, we have collided with some previous hash
			 * value, so try the next bucket location.  NB: this code must
			 * match ExecHashGetSkewBucket.
			 */
			bucket = hashvalue & (nbuckets - 1);
			while (hashtable->skewBucket[bucket] != NULL &&
				   hashtable->skewBucket[bucket]->hashvalue != hashvalue)
				bucket = (bucket + 1) & (nbuckets - 1);

			/*
			 * If we found an existing bucket with the same hashvalue, leave
			 * it alone.  It's okay for two MCVs to share a hashvalue.
			 */
			if (hashtable->skewBucket[bucket] != NULL)
				continue;

			/* Okay, create a new skew bucket for this hashvalue. */
			hashtable->skewBucket[bucket] = (HashSkewBucket *)
				MemoryContextAlloc(hashtable->batchCxt,
								   sizeof(HashSkewBucket));
			hashtable->skewBucket[bucket]->hashvalue = hashvalue;
			hashtable->skewBucket[bucket]->tuples = NULL;
			hashtable->skewBucketNums[hashtable->nSkewBuckets] = bucket;
			hashtable->nSkewBuckets++;
			hashtable->spaceUsed += SKEW_BUCKET_OVERHEAD;
			hashtable->spaceUsedSkew += SKEW_BUCKET_OVERHEAD;
			if (hashtable->spaceUsed > hashtable->spacePeak)
				hashtable->spacePeak = hashtable->spaceUsed;
		}

		free_attstatsslot(node->skewColType,
						  values, nvalues, numbers, nnumbers);
	}

	ReleaseSysCache(statsTuple);
}

/*
 * ExecHashGetSkewBucket
 *
 *		Returns the index of the skew bucket for this hashvalue,
 *		or INVALID_SKEW_BUCKET_NO if the hashvalue is not
 *		associated with any active skew bucket.
 */
int
ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue)
{
	int			bucket;

	/*
	 * Always return INVALID_SKEW_BUCKET_NO if not doing skew optimization (in
	 * particular, this happens after the initial batch is done).
	 */
	if (!hashtable->skewEnabled)
		return INVALID_SKEW_BUCKET_NO;

	/*
	 * Since skewBucketLen is a power of 2, we can do a modulo by ANDing.
	 */
	bucket = hashvalue & (hashtable->skewBucketLen - 1);

	/*
	 * While we have not hit a hole in the hashtable and have not hit the
	 * desired bucket, we have collided with some other hash value, so try the
	 * next bucket location.
	 */
	while (hashtable->skewBucket[bucket] != NULL &&
		   hashtable->skewBucket[bucket]->hashvalue != hashvalue)
		bucket = (bucket + 1) & (hashtable->skewBucketLen - 1);

	/*
	 * Found the desired bucket?
	 */
	if (hashtable->skewBucket[bucket] != NULL)
		return bucket;

	/*
	 * There must not be any hashtable entry for this hash value.
	 */
	return INVALID_SKEW_BUCKET_NO;
}

/*
 * ExecHashSkewTableInsert
 *
 *		Insert a tuple into the skew hashtable.
 *
 * This should generally match up with the current-batch case in
 * ExecHashTableInsert.
 */
static void
ExecHashSkewTableInsert(HashJoinTable hashtable,
						TupleTableSlot *slot,
						uint32 hashvalue,
						int bucketNumber)
{
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
	HashJoinTuple hashTuple;
	int			hashTupleSize;

	Assert(!HashJoinTableIsShared(hashtable));

	/* Create the HashJoinTuple */
	hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
	hashTuple = (HashJoinTuple) MemoryContextAlloc(hashtable->batchCxt,
												   hashTupleSize);
	hashTuple->hashvalue = hashvalue;
	memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);
	HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

	/* Push it onto the front of the skew bucket's list */
	hashTuple->next.unshared = hashtable->skewBucket[bucketNumber]->tuples;
	hashtable->skewBucket[bucketNumber]->tuples = hashTuple;

	/* Account for space used, and back off if we've used too much */
	hashtable->spaceUsed += hashTupleSize;
	hashtable->spaceUsedSkew += hashTupleSize;
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;
	while (hashtable->spaceUsedSkew > hashtable->spaceAllowedSkew)
		ExecHashRemoveNextSkewBucket(hashtable);

	/* Check we are not over the total spaceAllowed, either */
	if (hashtable->spaceUsed > hashtable->spaceAllowed &&
		hashtable->growEnabled)
		ExecHashIncreaseNumBatches(hashtable, hashtable->nbatch * 2);
}

/*
 *		ExecHashRemoveNextSkewBucket
 *
 *		Remove the least valuable skew bucket by pushing its tuples into
 *		the main hash table.
 */
static void
ExecHashRemoveNextSkewBucket(HashJoinTable hashtable)
{
	int			bucketToRemove;
	HashSkewBucket *bucket;
	uint32		hashvalue;
	int			bucketno;
	int			batchno;
	HashJoinTuple hashTuple;

	Assert(!HashJoinTableIsShared(hashtable));

	/* Locate the bucket to remove */
	bucketToRemove = hashtable->skewBucketNums[hashtable->nSkewBuckets - 1];
	bucket = hashtable->skewBucket[bucketToRemove];

	/*
	 * Calculate which bucket and batch the tuples belong to in the main
	 * hashtable.  They all have the same hash value, so it's the same for all
	 * of them.  Also note that it's not possible for nbatch to increase while
	 * we are processing the tuples.
	 */
	hashvalue = bucket->hashvalue;
	ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

	/* Process all tuples in the bucket */
	hashTuple = bucket->tuples;
	while (hashTuple != NULL)
	{
		HashJoinTuple nextHashTuple =
			next_tuple_in_bucket(hashtable, hashTuple);
		MinimalTuple tuple;
		Size		tupleSize;

		/*
		 * This code must agree with ExecHashTableInsert.  We do not use
		 * ExecHashTableInsert directly as ExecHashTableInsert expects a
		 * TupleTableSlot while we already have HashJoinTuples.
		 */
		tuple = HJTUPLE_MINTUPLE(hashTuple);
		tupleSize = HJTUPLE_OVERHEAD + tuple->t_len;

		/* Decide whether to put the tuple in the hash table or a temp file */
		if (batchno == hashtable->curbatch)
		{
			/* Move the tuple to the main hash table */
			HashJoinTuple copyTuple;

			/*
			 * We must copy the tuple into the dense storage, else it will not
			 * be found by, eg, ExecHashIncreaseNumBatches.
			 */
			copyTuple = (HashJoinTuple)
				dense_alloc(hashtable, tupleSize, false);
			memcpy(copyTuple, hashTuple, tupleSize);
			pfree(hashTuple);

			insert_tuple_into_bucket(hashtable, bucketno, copyTuple,
									 InvalidDsaPointer);

			/* We have reduced skew space, but overall space doesn't change */
			hashtable->spaceUsedSkew -= tupleSize;
		}
		else
		{
			/* Put the tuple into a temp file for later batches */
			Assert(batchno > hashtable->curbatch);
			ExecHashJoinSaveTuple(tuple, hashvalue,
								  &hashtable->innerBatchFile[batchno]);
			pfree(hashTuple);
			hashtable->spaceUsed -= tupleSize;
			hashtable->spaceUsedSkew -= tupleSize;
		}

		hashTuple = nextHashTuple;
	}

	/*
	 * Free the bucket struct itself and reset the hashtable entry to NULL.
	 *
	 * NOTE: this is not nearly as simple as it looks on the surface, because
	 * of the possibility of collisions in the hashtable.  Suppose that hash
	 * values A and B collide at a particular hashtable entry, and that A was
	 * entered first so B gets shifted to a different table entry.  If we were
	 * to remove A first then ExecHashGetSkewBucket would mistakenly start
	 * reporting that B is not in the hashtable, because it would hit the NULL
	 * before finding B.  However, we always remove entries in the reverse
	 * order of creation, so this failure cannot happen.
	 */
	hashtable->skewBucket[bucketToRemove] = NULL;
	hashtable->nSkewBuckets--;
	pfree(bucket);
	hashtable->spaceUsed -= SKEW_BUCKET_OVERHEAD;
	hashtable->spaceUsedSkew -= SKEW_BUCKET_OVERHEAD;

	/*
	 * If we have removed all skew buckets then give up on skew optimization.
	 * Release the arrays since they aren't useful any more.
	 */
	if (hashtable->nSkewBuckets == 0)
	{
		hashtable->skewEnabled = false;
		pfree(hashtable->skewBucket);
		pfree(hashtable->skewBucketNums);
		hashtable->skewBucket = NULL;
		hashtable->skewBucketNums = NULL;
		hashtable->spaceUsed -= hashtable->spaceUsedSkew;
		hashtable->spaceUsedSkew = 0;
	}
}

/*
 * Allocate 'size' bytes from the currently active HashMemoryChunk.  If
 * 'respect_work_mem' is true, this may cause the number of batches to be
 * increased in an attempt to shrink the hash table.
 */
static void *
dense_alloc(HashJoinTable hashtable, Size size, bool respect_work_mem)
{
	HashMemoryChunk newChunk;
	char	   *ptr;

	/* just in case the size is not already aligned properly */
	size = MAXALIGN(size);

	/*
	 * If tuple size is larger than of 1/4 of chunk size, allocate a separate
	 * chunk.
	 */
	if (size > HASH_CHUNK_THRESHOLD)
	{
		if (respect_work_mem &&
			hashtable->growEnabled &&
			hashtable->spaceUsed + HASH_CHUNK_HEADER_SIZE + size >
			hashtable->spaceAllowed)
		{
			/* work_mem would be exceeded: try to shrink hash table */
			ExecHashIncreaseNumBatches(hashtable, hashtable->nbatch * 2);
		}

		/* allocate new chunk and put it at the beginning of the list */
		newChunk = (HashMemoryChunk) MemoryContextAlloc(hashtable->batchCxt,
								 offsetof(HashMemoryChunkData, data) + size);
		newChunk->maxlen = size;
		newChunk->used = 0;
		newChunk->ntuples = 0;

		/*
		 * Add this chunk to the list after the first existing chunk, so that
		 * we don't lose the remaining space in the "current" chunk.
		 */
		if (hashtable->chunks != NULL)
		{
			newChunk->next.unshared = hashtable->chunks->next.unshared;
			hashtable->chunks->next.unshared = newChunk;
		}
		else
		{
			newChunk->next.unshared = hashtable->chunks;
			hashtable->chunks = newChunk;
		}

		newChunk->used += size;
		newChunk->ntuples += 1;

		/* count this single-tuple chunk as space used */
		hashtable->spaceUsed += HASH_CHUNK_HEADER_SIZE + size;
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;

		return newChunk->data;
	}

	/*
	 * See if we have enough space for it in the current chunk (if any). If
	 * not, allocate a fresh chunk.
	 */
	if ((hashtable->chunks == NULL) ||
		(hashtable->chunks->maxlen - hashtable->chunks->used) < size)
	{
		if (respect_work_mem &&
			hashtable->growEnabled &&
			hashtable->spaceUsed + HASH_CHUNK_SIZE > hashtable->spaceAllowed)
		{
			/* work_mem would be exceeded: try to shrink hash table */
			ExecHashIncreaseNumBatches(hashtable, hashtable->nbatch * 2);
		}

		/* allocate new chunk and put it at the beginning of the list */
		newChunk = (HashMemoryChunk) MemoryContextAlloc(hashtable->batchCxt,
					  HASH_CHUNK_SIZE);

		newChunk->maxlen = HASH_CHUNK_SIZE - HASH_CHUNK_HEADER_SIZE;
		newChunk->used = size;
		newChunk->ntuples = 1;

		newChunk->next.unshared = hashtable->chunks;
		hashtable->chunks = newChunk;

		/* count this whole mostly-empty chunk as space used */
		hashtable->spaceUsed += HASH_CHUNK_SIZE;
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;

		return newChunk->data;
	}

	/* There is enough space in the current chunk, let's add the tuple */
	ptr = hashtable->chunks->data + hashtable->chunks->used;
	hashtable->chunks->used += size;
	hashtable->chunks->ntuples += 1;

	/* return pointer to the start of the tuple memory */
	return ptr;
}

 /*
 * Allocate 'size' bytes from the currently active shared HashMemoryChunk, or
 * create a new chunk if necessary.  This is similar to the private memory
 * version, but coordinates memory accounting with other participants whenever
 * new chunks are needed.
 */
static void *
dense_alloc_shared(HashJoinTable hashtable,
				   Size size,
				   dsa_pointer *shared,
				   bool respect_work_mem)
{
	dsa_pointer chunk_shared;
	HashMemoryChunk chunk;
	Size chunk_size;

	/* just in case the size is not already aligned properly */
	size = MAXALIGN(size);

	/*
	 * Fast path: if there is enough space in this backend's current chunk,
	 * then we can allocate without any locking.
	 */
	chunk = hashtable->current_chunk;
	if (chunk != NULL &&
		size < HASH_CHUNK_THRESHOLD &&
		chunk->maxlen - chunk->used >= size)
	{
		void *result;

		chunk_shared = hashtable->current_chunk_shared;
		Assert(chunk == dsa_get_address(hashtable->area, chunk_shared));
		*shared = chunk_shared + HASH_CHUNK_HEADER_SIZE + chunk->used;
		result = chunk->data + chunk->used;
		chunk->used += size;
		chunk->ntuples += 1;

		Assert(chunk->used <= chunk->maxlen);
		Assert(result == dsa_get_address(hashtable->area, *shared));

		return result;
	}

 retry:
	/*
	 * Slow path: try to allocate a new chunk.
	 */
	LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);

	/* Check if some other participant has increased nbatch. */
	if (hashtable->shared->nbatch > hashtable->nbatch)
	{
		Assert(respect_work_mem);
		ExecHashIncreaseNumBatches(hashtable, hashtable->shared->nbatch);
	}

	/* Check if we need to help shrinking. */
	if (hashtable->shared->shrink_needed)
	{
		hashtable->current_chunk = NULL;
		LWLockRelease(&hashtable->shared->chunk_lock);
		ExecHashShrink(hashtable);
		goto retry;
	}

	/* Oversized tuples get their own chunk. */
	if (size > HASH_CHUNK_THRESHOLD)
		chunk_size = size + HASH_CHUNK_HEADER_SIZE;
	else
		chunk_size = HASH_CHUNK_SIZE;

	/* If appropriate, check if work_mem would be exceeded by a new chunk. */
	if (respect_work_mem &&
		hashtable->shared->grow_enabled &&
		(hashtable->shared->size +
		 chunk_size) > (work_mem * 1024L))
	{
		/*
		 * It would be exceeded.  Let's increase the number of batches, so we
		 * can try to shrink the hash table.
		 */
		hashtable->shared->nbatch *= 2;
		ExecHashIncreaseNumBatches(hashtable, hashtable->shared->nbatch);
		hashtable->shared->chunk_work_queue = hashtable->shared->chunks;
		hashtable->shared->chunks = InvalidDsaPointer;
		hashtable->shared->shrink_needed = true;
		hashtable->current_chunk = NULL;
		LWLockRelease(&hashtable->shared->chunk_lock);

		/* Begin shrinking.  Other participants will help. */
		ExecHashShrink(hashtable);
		goto retry;
	}

	/*
	 * If there was a chunk already, then add its tuple count to the shared
	 * total now.  The final chunk's count will be handled in finish_loading.
	 */
	if (chunk != NULL)
		hashtable->shared->ntuples += chunk->ntuples;

	/* We are cleared to allocate a new chunk. */
	chunk_shared = dsa_allocate(hashtable->area, chunk_size);
	if (!DsaPointerIsValid(chunk_shared))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of memory")));

	hashtable->shared->size += chunk_size;

	/* Set up the chunk. */
	chunk = (HashMemoryChunk) dsa_get_address(hashtable->area, chunk_shared);
	*shared = chunk_shared + HASH_CHUNK_HEADER_SIZE;
	chunk->maxlen = chunk_size - offsetof(HashMemoryChunkData, data);
	chunk->used = size;
	chunk->ntuples = 1;

	/*
	 * Push it onto the list of chunks, so that it can be found if we need to
	 * increase the number of buckets or batches and process all tuples.
	 */
	chunk->next.shared = hashtable->shared->chunks;
	hashtable->shared->chunks = chunk_shared;

	if (size > HASH_CHUNK_THRESHOLD)
	{
		/*
		 * Count oversized tuples immediately, but don't bother making this
		 * chunk the 'current' chunk because it has no more space in it for
		 * next time.
		 */
		++hashtable->shared->ntuples;
	}
	else
	{
		/*
		 * Make this the current chunk so that we can use the fast path to
		 * fill the rest of it up in future called.  We will count this tuple
		 * later, when the chunk is full.
		 */
		hashtable->current_chunk = chunk;
		hashtable->current_chunk_shared = chunk_shared;
	}
	/*
	 * Update local copy of total tuples so it can be used to compute the load
	 * factor and trigger bucket growth.
	 */
	hashtable->totalTuples = hashtable->shared->ntuples;
	LWLockRelease(&hashtable->shared->chunk_lock);

	Assert(chunk->data == dsa_get_address(hashtable->area, *shared));

	return chunk->data;
}

/*
 * Add the tuple count from the current chunk to the shared tuple count.  This
 * is necessary because dense_alloc_shared only updates the shared counter
 * when a new chunk is allocated, leaving the final chunk unaccounted for.
 */
static void
finish_loading(HashJoinTable hashtable)
{
	if (HashJoinTableIsShared(hashtable))
	{
		LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);
		if (hashtable->current_chunk != NULL)
			hashtable->shared->ntuples += hashtable->current_chunk->ntuples;
		hashtable->totalTuples = hashtable->shared->ntuples;
		LWLockRelease(&hashtable->shared->chunk_lock);
	}
}

/*
 * Insert a tuple at the front of a given bucket identified by number.  For
 * shared hash joins, tuple_shared must be provided, pointing to the tuple in
 * the dsa_area backing the table.  For private hash joins, it should be
 * InvalidDsaPointer.
 */
static void
insert_tuple_into_bucket(HashJoinTable table, int bucketno,
						 HashJoinTuple tuple, dsa_pointer tuple_shared)
{
	if (HashJoinTableIsShared(table))
	{
		Assert(tuple == dsa_get_address(table->area, tuple_shared));
		for (;;)
		{
			tuple->next.shared =
				dsa_pointer_atomic_read(&table->buckets[bucketno].shared);
			if (dsa_pointer_atomic_compare_exchange(&table->buckets[bucketno].shared,
													&tuple->next.shared,
													tuple_shared))
				break;
		}
	}
	else
	{
		tuple->next.unshared = table->buckets[bucketno].unshared;
		table->buckets[bucketno].unshared = tuple;
	}
}

/*
 * Get the first tuple in a given bucket identified by number.
 */
static HashJoinTuple
first_tuple_in_bucket(HashJoinTable table, int bucketno)
{
	if (HashJoinTableIsShared(table))
	{
		dsa_pointer p =
			dsa_pointer_atomic_read(&table->buckets[bucketno].shared);
		return (HashJoinTuple) dsa_get_address(table->area, p);
	}
	else
		return table->buckets[bucketno].unshared;
}

/*
 * Get the next tuple in the same bucket as 'tuple'.
 */
static HashJoinTuple
next_tuple_in_bucket(HashJoinTable table, HashJoinTuple tuple)
{
	if (HashJoinTableIsShared(table))
		return (HashJoinTuple)
			dsa_get_address(table->area, tuple->next.shared);
	else
		return tuple->next.unshared;
}

/*
 * Take the next available chunk from the queue of chunks being worked on in
 * parallel.  Return NULL if there are none left.  Otherwise return a pointer
 * to the chunk, and set *shared to the DSA pointer to the chunk.
 */
static HashMemoryChunk
pop_chunk_queue_unlocked(HashJoinTable hashtable, dsa_pointer *shared)
{
	HashMemoryChunk chunk;

	Assert(HashJoinTableIsShared(hashtable));
	Assert(LWLockHeldByMe(&hashtable->shared->chunk_lock));

	if (!DsaPointerIsValid(hashtable->shared->chunk_work_queue))
		return NULL;

	*shared = hashtable->shared->chunk_work_queue;
	chunk = (HashMemoryChunk)
		dsa_get_address(hashtable->area, *shared);
	hashtable->shared->chunk_work_queue = chunk->next.shared;

	return chunk;
}

/*
 * See pop_chunk_unlocked.
 */
static HashMemoryChunk
pop_chunk_queue(HashJoinTable hashtable, dsa_pointer *shared)
{
	HashMemoryChunk chunk;

	LWLockAcquire(&hashtable->shared->chunk_lock, LW_EXCLUSIVE);
	chunk = pop_chunk_queue_unlocked(hashtable, shared);
	LWLockRelease(&hashtable->shared->chunk_lock);

	return chunk;
}
