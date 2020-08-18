/*
 *	qsort.c: standard quicksort algorithm
 */

#include "c.h"

#include "lib/gen_sort.h"

void
qsort_8(void *a, size_t n, int (*cmp) (const void *, const void *))
{
	pg_sort_cmp(a, n, 8, cmp, qsort_8);
}

void
qsort_4(void *a, size_t n, int (*cmp) (const void *, const void *))
{
	pg_sort_cmp(a, n, 4, cmp, qsort_4);
}

void
qsort_x(void *a, size_t n, size_t es,
		   int (*cmp) (const void *, const void *))
{
	pg_sort_sizecmp(a, n, es, cmp, qsort_x);
}

/*
 * qsort comparator wrapper for strcmp.
 */
int
pg_qsort_strcmp(const void *a, const void *b)
{
	return strcmp(*(const char *const *) a, *(const char *const *) b);
}
