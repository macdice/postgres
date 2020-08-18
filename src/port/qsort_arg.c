/*
 *	qsort_arg.c: qsort with a passthrough "void *" argument
 */

#include "c.h"

#include "lib/gen_sort.h"

void
qsort_8_arg(void *a, size_t n, qsort_arg_comparator cmp, void *arg)
{
	pg_sort_cmparg(a, n, 8, cmp, qsort_8_arg, arg);
}

void
qsort_4_arg(void *a, size_t n, qsort_arg_comparator cmp, void *arg)
{
	pg_sort_cmparg(a, n, 4, cmp, qsort_4_arg, arg);
}

void
qsort_x_arg(void *a, size_t n, size_t es, qsort_arg_comparator cmp, void *arg)
{
	pg_sort_sizecmparg(a, n, es, cmp, qsort_x_arg, arg);
}
