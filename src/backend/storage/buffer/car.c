/*
 * A reusable implementation of the algorithm "CLOCK with Adaptive
 * Replacement"[1] (CAR), with modifications to allow for concurrency.
 *
 * In the CAR model, there is a cache that can hold c objects (eg buffers),
 * but a "cache directory" with 2c entries, so that we have a memory of the
 * relative value of twice as many objects as we can hold.
 *
 * This module doesn't look after looking objects up itself.  The client of
 * this module must maintain a mapping table large enough for 2c objects, and
 * look after the set of car_mapping object.
 *
 * As in the paper, the car_access() routine can be run concurrently, but
 * calls to car_allocate() must be serialized.  In order to avoid serializing
 * faulting work, a single call to the CAR(x) routine from the paper's figure
 * 2 may correspond to multiple calls to this implementation's car_allocate(),
 * because we want the work of faulting objects out of the cache not to run
 * under a global lock.  Where the paper's replace() function "demotes the
 * head page in Tx", we run car_begin_replace() and return control, so the
 * caller can performing any I/O and then later call car_complete_replace().
 *
 * In order to support concurrent replacement, we introduce two extra lists,
 * F1 and F2, on which cached objects live while they are in transit between
 * Tn and Bn ("faulting").  Furthermore, in order to support the concept of
 * 'pinned' objects which cannot currently be replaced even if they are
 * selected by CAR, the calling code can reject the selection by calling
 * car_complete_replace(..., false).  In that case, the...
 *
 * [1] https://www.usenix.org/legacy/publications/library/proceedings/fast04/tech/full_papers/bansal/bansal.pdf
 */

#include "postgres.h"
#include "storage/car.h"
#include "storage/lwlock.h"
#include "lib/ilist.h"

/*
 * The main management object in shared memory.
 */
struct car_control
{
	int			c;				/* cache size */
	int			p;				/* target size for T1 */
	dclist_head lists[6];		/* T1, T2, F1, F2, B1, B2 */
	bool		need_cache_directory_replacement;
	int			freelist_size;
	int			freelist[FLEXIBLE_ARRAY_MEMBER];
};

typedef enum car_list_id
{
	T1,							/* cached objects seen recently */
	F1,							/* cached objects being faulted out of T1 */
	B1,							/* uncached objects seen recently */
	T2,							/* cached objects seen frequently */
	F2,							/* cached objects being faulted out of T2 */
	B2,							/* uncached objects seen frequently */
	CAR_INVALID
} car_list_id;

/* Push a mapping to the tail of a given list. */
static inline void
car_push_tail(car_control *car, car_list_id list, car_mapping *mapping)
{
	dclist_push_tail(&car->lists[list], &mapping->node);
	mapping->list = list;
}

/* Push a mapping to the head of a given list. */
static inline void
car_push_head(car_control *car, car_list_id list, car_mapping *mapping)
{
	dclist_push_head(&car->lists[list], &mapping->node);
	mapping->list = list;
}

/* Remove a mapping from its current list, making it invalid. */
static inline void
car_delete(car_control *car, car_mapping *mapping)
{
	Assert(mapping->list >= 0 && mapping->list < CAR_INVALID);
	dclist_delete_from(&car->lists[mapping->list], &mapping->node);
	mapping->list = CAR_INVALID;
}

/* Move a mapping from its current list to the tail of another list. */
static inline void
car_move_to_tail(car_control *car, car_list_id list, car_mapping *mapping)
{
	car_delete(car, mapping);
	car_push_tail(car, list, mapping);
}

/* Move a mapping from its current list to the head of another list. */
static inline void
car_move_to_head(car_control *car, car_list_id list, car_mapping *mapping)
{
	car_delete(car, mapping);
	car_push_head(car, list, mapping);
}

/* Check the size of a list. */
static inline int
car_size(car_control *car, car_list_id list)
{
	Assert(list >= 0 && list < CAR_INVALID);
	return dclist_count(&car->lists[list]);
}

/* Peek at the head element of a list, which must not be empty. */
static inline car_mapping *
car_head(car_control *car, car_list_id list)
{
	Assert(car_size(car, list) > 0);
	return dclist_head_element(car_mapping, node, &car->lists[list]);
}

/* Peek at the tail element of a list, which must not be empty. */
static inline car_mapping *
car_tail(car_control *car, car_list_id list)
{
	Assert(car_size(car, list) > 0);
	return dclist_tail_element(car_mapping, node, &car->lists[list]);
}

void
car_mapping_init(car_mapping *mapping)
{
	memset(mapping, 0, sizeof(*mapping));
	mapping->list = CAR_INVALID;
}

size_t
car_estimate_size(int objects)
{
	return offsetof(car_control, freelist) + sizeof(car_index) * objects;
}

void
car_init(car_control *car, int objects)
{
	car->c = objects;
	car->p = 0;
	for (int i = 0; i < lengthof(car->lists); ++i)
		dclist_init(&car->lists[i]);
	car->need_cache_directory_replacement = false;
	car->freelist_size = objects;
	for (int i = 0; i < objects; ++i)
		car->freelist[i] = objects - i - 1;
}

/*
 * Report how many mappings the caller is responsible for tracking in its
 * mapping table.  This is fixed by the published algorithm at twice the
 * maximum number number of cached objects, but we'll provide a function to
 * compute that rather than requiring the caller to know that.
 */
int
car_mappings(const car_control *ctl)
{
	return ctl->c * 2;
}

/*
 * If the ...
 *
 * Must be serialized with respect to car_allocate().
 */
void
car_forget(car_control *car, car_mapping *mapping)
{
	Assert(mapping->list == T1 || mapping->list == T2 ||
		   mapping->list == B1 || mapping->list == B2);

	/* XXX what if it's in F1,F2? */

	/* Remember the cache slot (eg buffer) that is now free. */
	if (mapping->list == T1 || mapping->list == T2)
	{
		Assert(mapping->index >= 0);
		car->freelist[car->freelist_size++] = mapping->index;
		Assert(car->freelist_size <= car->c);
	}
	else
		Assert(mapping->index == -1);


	/* Remove it from its list. */
	car_delete(car, mapping);
}

/*
 * Choose an object to evict from the cache.  Moves an object from a T list
 * (objects currently in cache) to the corresponding R list (objects still in
 * cache, but currently being moved out).  Decide whether to pick on T1 or T2
 * by considering the current target size for T1.
 *
 * Return NULL if all objects are concurrently being replaced, so that we
 * can't initiate any further replacement.
 */
static car_mapping *
car_begin_replace(car_control *car)
{
	for (;;)
	{
		car_mapping *mapping;

		/*
		 * In the paper, line 24 compares the the size of T1 to p (the target
		 * size for T1), but here we must also consider F1, because otherwise
		 * we'd penalize T1 for objects that are already in the process of
		 * being faulted out.  We also need to consider that there might be
		 * nothing left in T1 (because everything's in F1), and fall back to
		 * T2 to be able to make progress.
		 */
		if (car_size(car, T1) + car_size(car, F1) > Max(1, car->p) &&
			car_size(car, T1) > 0)
		{
			/* Take the head object from T1... */
			mapping = car_head(car, T1);

			if (!mapping->reference)
			{
				/* ... and move to F1, to initiate replacement. */
				car_move_to_tail(car, F1, mapping);

				return mapping;
			}
			else
			{
				/* ... clear reference bit, and move to tail of T2. */
				mapping->reference = false;
				car_move_to_tail(car, T2, mapping);
			}
		}
		else
		{
			/*
			 * In the paper, line 32 assumes that T2 must be non-empty, but we
			 * have to consider that pages might already be in F2 due to
			 * concurrent faulting activity.  In that case, your cache simply
			 * isn't large enough, and we have no choice but to fail.
			 */
			if (car_size(car, T2) == 0)
				return NULL;

			/* Take the head object from T2, ... */
			mapping = car_head(car, T2);

			if (!mapping->reference)
			{
				/* ... and move to F2, to initiate replacement. */
				car_move_to_tail(car, F2, mapping);

				return mapping;
			}
			else
			{
				/* ... set reference bit, and move to tail of T2. */
				mapping->reference = true;
				car_move_to_tail(car, T2, mapping);
			}
		}
	}
}

/*
 * If the caller has a mapping already but car_access() returned -1, the
 * existing car_mapping object should be passed in.  If the caller is creating
 * a new mapping, then a pointer to a newly initialized car_mapping associated
 * with the new mapping should be passed in.  Either way, the car_mapping must
 * remain at that address until a later call to this routine eventually
 * indicates that it should dropped, or car_forget() is used to forget it.
 *
 * Return CAR_ALLOCATE_OK on success, and then car_mapping_index(mapping)
 * gives the index of the newly allocated cache object.
 *
 * Return CAR_ALLOCATE_FORGET if a non-cached mapping has been selected to be
 * dropped.  The caller should remove the associated entry from its mapping
 * table, and then try again.
 *
 * Return CAR_ALLOCATE_REPLACE if a cached mapping has been selected for
 * replacement.  The mapping should remain, but the object at cache index
 * car_mapping_index(*replace) should be removed from the cache (ie faulted
 * out, possibly writing a buffer page if it is dirty).  On determining that
 * that is not possible (for example, because it is a pinned buffer), the
 * caller should call car_complete_replace(..., false).  Otherwise
 * car_complete_replace(..., true) should be called.
 *
 * Return CAR_ALLOCATE_FAIL if it is not possible to allocate a cache object,
 * because every single cache object is concurrently being replaced (this
 * indicates that the cache is too small).
 */
car_allocate_result
car_allocate(car_control *car, car_mapping *mapping, car_mapping **replace)
{
	/* Every possible cache index is either in use, being replaced, or free. */
	Assert(car_size(car, T1) +
		   car_size(car, F1) +
		   car_size(car, T2) +
		   car_size(car, F2) +
		   car->freelist_size == car->c);
	elog(LOG, "car_allocate T1=%d, F1=%d, B1=%d, T2=%d, F2=%d, B2=%d", car_size(car, T1), car_size(car, F1), car_size(car, B1), car_size(car, T2), car_size(car, F2), car_size(car, B2));

	/*
	 * In the paper, lines 6-10 perform cache directory replacement
	 * immediately after replace(), but we defer that work until after
	 * car_complete_replace().
	 */
	if (car->need_cache_directory_replacement)
	{
		/*
		 * An earlier call to car_complete_replace() kicked something out of
		 * Fx into Bx.  Now we have to decide whether to prefer B1 or B2.
		 * Unlike the paper, we also have to consider the F lists.
		 */
		car->need_cache_directory_replacement = false;
		if (mapping->list != B1 &&
			mapping->list != B2)
		{
			if (car_size(car, T1) +
				car_size(car, F1) +
				car_size(car, B1) == car->c)
			{
				/* Forget the LRU object in B1. */
				*replace = car_tail(car, B1);
				car_delete(car, *replace);

				return CAR_ALLOCATE_FORGET;
			}
			else if (car_size(car, T1) +
					 car_size(car, F1) +
					 car_size(car, B1) +
					 car_size(car, T2) +
					 car_size(car, F2) +
					 car_size(car, B2) == car->c * 2)
			{
				/* Forget the LRU object in B2. */
				*replace = car_tail(car, B2);
				car_delete(car, *replace);

				return CAR_ALLOCATE_FORGET;
			}
		}
	}

	/*
	 * In the paper, line 4 considers only T1 and T2, but we also have to
	 * consider objects that are concurrently being replaced (they still
	 * occupy cache space that can't be reused yet).
	 */
	if (car_size(car, T1) +
		car_size(car, F1) +
		car_size(car, T2) +
		car_size(car, F2) == car->c)
	{
		car_mapping *victim;

		/* Cache full.  Choose an object to kick out of the cache. */
		victim = car_begin_replace(car);

		/*
		 * It's theoretically possibly for every single cache object to be
		 * concurrently in the process of being replaced already.
		 */
		if (victim == NULL)
			return CAR_ALLOCATE_FAIL;

		/* The caller must call car_complete_replace() to finish the job. */
		*replace = victim;

		return CAR_ALLOCATE_REPLACE;
	}

	/* There must now be free cache space. */
	Assert(car_size(car, T1) + car_size(car, T2) < car->c);
	Assert(car->freelist_size > 0);

	/* Clear the reference bit. */
	mapping->reference = false;

	/* Cache directory miss?  (Caller created a brand new mapping.) */
	if (mapping->list == CAR_INVALID)
	{
		mapping->index = car->freelist[--car->freelist_size];
		car_push_tail(car, T1, mapping);

		return CAR_ALLOCATE_OK;
	}

	/* Cache directory hit.  Adapt target, and move to tail of T2. */
	if (mapping->list == B1)
	{
		/* Increase the target size for the list T1. */
		car->p = Min(car->p + Max(1, car_size(car, B2) / car_size(car, B1)), car->c);
	}
	else
	{
		Assert(mapping->list == B2);
		/* Decrease the target size for the list T1. */
		car->p = Max(car->p - Max(1, car_size(car, B1) / car_size(car, B2)), 0);
	}
	car_move_to_tail(car, T2, mapping);

	return CAR_ALLOCATE_OK;
}

/*
 */
void
car_complete_replace(car_control *car, car_mapping *replace, bool accepted)
{
	/* This must be a cached object selected by car_begin_replace(). */
	Assert(replace->list == F1 || replace->list == F2);
	Assert(replace->index >= 0);

	/* Does the caller refuse to fault this object out? */
	if (!accepted)
	{
		/*
		 * Put the object back in tail position on the list it came from and
		 * also set the referenced flag, so we don't try to replace it again
		 * soon.  This logic does not exist in the paper.  XXX May contain
		 * nuts
		 */
		replace->reference = true;
		car_move_to_tail(car, replace->list - 1, replace);
	}
	else
	{
		/* Put the object in MRU position on the corresponding B list. */
		car_move_to_head(car, replace->list + 1, replace);

		/* Next call to car_allocate() may need to eject an old Bx object. */
		car->need_cache_directory_replacement = true;
	}
}
