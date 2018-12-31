/*-------------------------------------------------------------------------
 *
 * sort_utils.h
 *
 *	  Simple sorting-related algorithms specialized for arrays of
 *	  paramaterized type, using inlined comparators.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * Usage notes:
 *
 *	  To generate functions specialized for a type, the following parameter
 *	  macros should be #define'd before this file is included.
 *
 *	  - SA_PREFIX - prefix for all symbol names generated.
 *	  - SA_ELEMENT_TYPE - type of the referenced elements
 *	  - SA_DECLARE - if defined the functions and types are declared
 *	  - SA_DEFINE - if defined the functions and types are defined
 *	  - SA_SCOPE - scope (e.g. extern, static inline) for functions
 *
 *	  The following are relevant only when SA_DEFINE is defined:
 *
 *	  - SA_COMPARE(a, b) - an expression to compare pointers to two values
 *
 * IDENTIFICATION
 *		src/include/lib/sort_utils.h
 *
 *-------------------------------------------------------------------------
 */

#define SA_MAKE_PREFIX(a) CppConcat(a,_)
#define SA_MAKE_NAME(name) SA_MAKE_NAME_(SA_MAKE_PREFIX(SA_PREFIX),name)
#define SA_MAKE_NAME_(a,b) CppConcat(a,b)

/* function declarations */
#define SA_SORT SA_MAKE_NAME(sort)
#define SA_UNIQUE SA_MAKE_NAME(unique)
#define SA_BINARY_SEARCH SA_MAKE_NAME(binary_search)
#define SA_LOWER_BOUND SA_MAKE_NAME(lower_bound)

#ifdef SA_DECLARE

SA_SCOPE void SA_SORT(SA_ELEMENT_TYPE *first, SA_ELEMENT_TYPE *last);
SA_SCOPE SA_ELEMENT_TYPE *SA_UNIQUE(SA_ELEMENT_TYPE *first,
									SA_ELEMENT_TYPE *last);
SA_SCOPE bool SA_BINARY_SEARCH(SA_ELEMENT_TYPE *first,
							   SA_ELEMENT_TYPE *last,
							   SA_ELEMENT_TYPE *value);
SA_SCOPE SA_ELEMENT_TYPE *SA_LOWER_BOUND(SA_ELEMENT_TYPE *first,
										 SA_ELEMENT_TYPE *last,
										 SA_ELEMENT_TYPE *value);

#endif

#ifdef SA_DEFINE

/* helper functions */
#define SA_QSORT_COMPARATOR SA_MAKE_NAME(qsort_comparator)

/*
 * Function wrapper for comparator expression.
 */
static inline int
SA_QSORT_COMPARATOR(const void *a, const void *b)
{
	return SA_COMPARE((SA_ELEMENT_TYPE *) a, (SA_ELEMENT_TYPE *) b);
}

/*
 * Sort an array [first, last) in place.  For now, just calls out to qsort,
 * but a quicksort with inlined comparators is known to be faster so we could
 * consider that here in future.
 */
SA_SCOPE void
SA_SORT(SA_ELEMENT_TYPE *first, SA_ELEMENT_TYPE *last)
{
	qsort(first, last - first, sizeof(SA_ELEMENT_TYPE), SA_QSORT_COMPARATOR);
}

/*
 * Remove duplicates from an array [first, last).  Return the new last pointer
 * (ie one past the new end).
 */
SA_SCOPE SA_ELEMENT_TYPE *
SA_UNIQUE(SA_ELEMENT_TYPE *first, SA_ELEMENT_TYPE *last)
{
	SA_ELEMENT_TYPE *write_head;
	SA_ELEMENT_TYPE *read_head;

	if (last - first <= 1)
		return last;

	write_head = first;
	read_head = first + 1;

	while (read_head < last)
	{
		if (SA_COMPARE(read_head, write_head) != 0)
			*++write_head = *read_head;
		++read_head;
	}
	return write_head + 1;
}

/*
 * Check if a sorted array [first, last) contains a value.
 */
SA_SCOPE bool
SA_BINARY_SEARCH(SA_ELEMENT_TYPE *first,
				 SA_ELEMENT_TYPE *last,
				 SA_ELEMENT_TYPE *value)
{
	SA_ELEMENT_TYPE *lower = first;
	SA_ELEMENT_TYPE *upper = last - 1;

	while (lower <= upper)
	{
		SA_ELEMENT_TYPE *mid;
		int			cmp;

		mid = lower + (upper - lower) / 2;
		cmp = SA_COMPARE(mid, value);
		if (cmp < 0)
			lower = mid + 1;
		else if (cmp > 0)
			upper = mid - 1;
		else
			return true;
	}

	return false;
}

/*
 * Find the first element in the range [first, last) that is not less than
 * value, in a sorted array.
 */
SA_SCOPE SA_ELEMENT_TYPE *
SA_LOWER_BOUND(SA_ELEMENT_TYPE *first,
			   SA_ELEMENT_TYPE *last,
			   SA_ELEMENT_TYPE *value)
{
	SA_ELEMENT_TYPE *lower = first;
	SA_ELEMENT_TYPE *upper = last - 1;
	SA_ELEMENT_TYPE *mid = first;

	while (lower <= upper)
	{
		int			cmp;

		mid = lower + (upper - lower) / 2;
		cmp = SA_COMPARE(mid, value);
		if (cmp < 0)
			lower = mid + 1;
		else if (cmp > 0)
			upper = mid - 1;
		else
			break;
	}

	return mid;
}

#endif

#undef SA_MAKE_PREFIX
#undef SA_MAKE_NAME
#undef SA_MAKE_NAME_
#undef SA_SORT
#undef SA_UNIQUE
#undef SA_BINARY_SEARCH
#undef SA_LOWER_BOUND
#undef SA_DECLARE
#undef SA_DEFINE
