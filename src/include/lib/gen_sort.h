/*-------------------------------------------------------------------------
 *
 * gen_sort.h
 *
 *	  Generic sorting infrastructure.
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/lib/gen_sort.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SORT_UTILS_H
#define SORT_UTILS_H

/*
 * Qsort routine based on J. L. Bentley and M. D. McIlroy,
 * "Engineering a sort function",
 * Software--Practice and Experience 23 (1993) 1249-1265.
 *
 * We have modified their original by adding a check for already-sorted
 * input, which seems to be a win per discussions on pgsql-hackers around
 * 2006-03-21.
 *
 * Also, we recurse on the smaller partition and iterate on the larger one,
 * which ensures we cannot recurse more than log(N) levels (since the
 * partition recursed to is surely no more than half of the input).  Bentley
 * and McIlroy explicitly rejected doing this on the grounds that it's "not
 * worth the effort", but we have seen crashes in the field due to stack
 * overrun, so that judgment seems wrong.
 *
 *     Modifications from vanilla NetBSD source:
 *       Add do ... while() macro fix
 *       Remove __inline, _DIAGASSERTs, __P
 *       Remove ill-considered "swap_cnt" switch to insertion sort,
 *       in favor of a simple check for presorted input.
 *       Take care to recurse on the smaller partition, to bound stack usage.
 *
 */

/*     $NetBSD: qsort.c,v 1.13 2003/08/07 16:43:42 agc Exp $   */

/*-
 * Copyright (c) 1992, 1993
 *     The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in the
 *	  documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *	  may be used to endorse or promote products derived from this software
 *	  without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/* Helper functions. */

static inline void *
pg_sort_med3(void *a, void *b, void *c,
			 int (*cmp) (const void *, const void *))
{
	return cmp(a, b) < 0 ?
		(cmp(b, c) < 0 ? b : (cmp(a, c) < 0 ? c : a))
		: (cmp(b, c) > 0 ? b : (cmp(a, c) < 0 ? a : c));
}

static inline void *
pg_sort_med3_arg(void *a, void *b, void *c,
				 int (*cmp_arg) (const void *, const void *, void *arg),
				 void *arg)
{
	return cmp_arg(a, b, arg) < 0 ?
		(cmp_arg(b, c, arg) < 0 ? b : (cmp_arg(a, c, arg) < 0 ? c : a))
		: (cmp_arg(b, c, arg) > 0 ? b : (cmp_arg(a, c, arg) < 0 ? a : c));
}

static inline void
pg_swap_mem(void *a, void *b, size_t size)
{
	char	   *a_begin = a;
	char	   *a_end = a_begin + size;
	char	   *b_begin = b;

	/* We want the compiler to unroll and vectorize this loop. */
	while (a_begin < a_end)
	{
		char		tmp = *a_begin;

		*a_begin++ = *b_begin;
		*b_begin++ = tmp;
	}
}

/* Types for comparator functions. */
typedef int (*compare_fun)(const void *a, const void *b);
typedef int (*compare_arg_fun)(const void *a, const void *b, void *arg);

/* Sort functions with varying degrees of specialization. */
typedef void (*sort_sizecmp_fun)(void *data, size_t n, size_t es,
								 compare_fun cmp);
typedef void (*sort_cmp_fun)(void *data, size_t n, compare_fun cmp);
typedef void (*sort_fun)(void *data, size_t n);

/* Equivalents for comparators that take an argument. */
typedef void (*sort_sizecmparg_fun)(void *data, size_t n, size_t es,
									compare_arg_fun cmp_arg, void *arg);
typedef void (*sort_cmparg_fun)(void *data, size_t n,
								compare_arg_fun cmp_arg, void *arg);
typedef void (*sort_arg_fun)(void *data, size_t n, void *arg);

/*
 * Definition of our sort algorithm.  This is not intended to be used directly;
 * it should be wrapped in a function that passes in constant values so that
 * we can instantiate specialized variants of the code while maintaining only
 * one copy of the algorithm.
 *
 * In order to be able to recurse without falling back to a non-specialized
 * version, the caller (a specialized wrapper) should pass in a pointer to
 * itself.
 */
static pg_attribute_always_inline void
pg_sort_impl(void *data, size_t n, size_t size,
			 compare_fun cmp,
			 compare_arg_fun cmp_arg,
			 sort_sizecmp_fun recurse_sizecmp,
			 sort_cmp_fun recurse_cmp,
			 sort_fun recurse,
			 sort_sizecmparg_fun recurse_sizecmparg,
			 sort_cmparg_fun recurse_cmparg,
			 sort_arg_fun recurse_arg,
			 void *arg)
{
	char *a = data,
			   *pa,
			   *pb,
			   *pc,
			   *pd,
			   *pl,
			   *pm,
			   *pn;
	size_t		d1,
				d2;
	int			r,
				presorted;

	/* Avoid some repetition in the code below with some macros. */
#define DO_CMP(a_, b_)												\
	(cmp_arg ? cmp_arg((a_), (b_), arg) : cmp((a_), (b_)))
#define DO_MED3(a_, b_, c_)											\
	(cmp_arg ? pg_sort_med3_arg((a_), (b_), (c_), cmp_arg, arg) 	\
			 : pg_sort_med3((a_), (b_), (c_), cmp))
#define DO_SWAPN(a_, b_, n_) pg_swap_mem((a_), (b_), size * (n_))
#define DO_SWAP(a_, b_) DO_SWAPN((a_), (b_), 1)
#define DO_RECURSE(a_, n_) 											\
	do																\
	{																\
		if (recurse_sizecmp)										\
			recurse_sizecmp((a_), (n_), size, cmp);					\
		else if (recurse_cmp)										\
			recurse_cmp((a_), (n_), cmp);							\
		else if (recurse)											\
			recurse((a_), (n_));									\
		else if (recurse_sizecmparg)								\
			recurse_sizecmparg((a_), (n_), size, cmp_arg, arg);		\
		else if (recurse_cmparg)									\
			recurse_cmparg((a_), (n_), cmp_arg, arg);				\
		else if (recurse_arg)										\
			recurse_arg((a_), (n_), arg);							\
	} while (0)

loop:
	if (n < 7)
	{
		for (pm = a + size; pm < a + n * size; pm += size)
			for (pl = pm; pl > a && DO_CMP(pl - size, pl) > 0; pl -= size)
				DO_SWAP(pl, pl - size);
		return;
	}
	presorted = 1;
	for (pm = a + size; pm < a + n * size; pm += size)
	{
		if (DO_CMP(pm - size, pm) > 0)
		{
			presorted = 0;
			break;
		}
	}
	if (presorted)
		return;
	pm = a + (n / 2) * size;
	if (n > 7)
	{
		pl = a;
		pn = a + (n - 1) * size;
		if (n > 40)
		{
			size_t		d = (n / 8) * size;

			pl = DO_MED3(pl, pl + d, pl + 2 * d);
			pm = DO_MED3(pm - d, pm, pm + d);
			pn = DO_MED3(pn - 2 * d, pn - d, pn);
		}
		pm = DO_MED3(pl, pm, pn);
	}
	DO_SWAP(a, pm);
	pa = pb = a + size;
	pc = pd = a + (n - 1) * size;
	for (;;)
	{
		while (pb <= pc && (r = DO_CMP(pb, a)) <= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pa, pb);
				pa += size;
			}
			pb += size;
		}
		while (pb <= pc && (r = DO_CMP(pc, a)) >= 0)
		{
			if (r == 0)
			{
				DO_SWAP(pc, pd);
				pd -= size;
			}
			pc -= size;
		}
		if (pb > pc)
			break;
		DO_SWAP(pb, pc);
		pb += size;
		pc -= size;
	}
	pn = a + n * size;
	d1 = Min(pa - a, pb - pa);
	DO_SWAPN(a, pb - d1, d1 / size);
	d1 = Min(pd - pc, pn - pd - size);
	DO_SWAPN(pb, pn - d1, d1 / size);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > size)
			DO_RECURSE(a, d1 / size);
		if (d2 > size)
		{
			/* Iterate rather than recurse to save stack space */
			/* DO_RECURSE(pn - d2, d2 / size) */
			a = pn - d2;
			n = d2 / size;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > size)
			DO_RECURSE(pn - d2, d2 / size);
		if (d1 > size)
		{
			/* Iterate rather than recurse to save stack space */
			/* DO_RECURSE(a, d1 / size) */
			n = d1 / size;
			goto loop;
		}
	}
#undef DO_CMP
#undef DO_MED3
#undef DO_SWAP
#undef DO_SWAPN
#undef DO_SORT
}

/*
 * Some intermediate wrappers to make it a bit easier to make specialized
 * wrapper functions, and insulate them from changes to pg_sort_impl's
 * argument list.  The names of these functions indicate which arguments the
 * recursive wrapper takes, besides data and n.
 */

static pg_attribute_always_inline void
pg_sort(void *data, size_t n, size_t size, compare_fun cmp, sort_fun recurse)
{
	pg_sort_impl(data, n, size,
				 cmp, NULL,
				 NULL, NULL, recurse, NULL, NULL, NULL,
				 NULL);
}

static pg_attribute_always_inline void
pg_sort_sizecmp(void *data, size_t n, size_t size, compare_fun cmp,
				sort_sizecmp_fun recurse_sizecmp)
{
	pg_sort_impl(data, n, size,
				 cmp, NULL,
				 recurse_sizecmp, NULL, NULL, NULL, NULL, NULL,
				 NULL);
}

static pg_attribute_always_inline void
pg_sort_cmp(void *data, size_t n, size_t size, compare_fun cmp,
			sort_cmp_fun recurse_cmp)
{
	pg_sort_impl(data, n, size,
				 cmp, NULL,
				 NULL, recurse_cmp, NULL, NULL, NULL, NULL,
				 NULL);
}

static pg_attribute_always_inline void
pg_sort_arg(void *data, size_t n, size_t size,
			compare_arg_fun cmp_arg,
			sort_arg_fun recurse_arg,
			void *arg)
{
	pg_sort_impl(data, n, size,
				 NULL, cmp_arg,
				 NULL, NULL, NULL, NULL, NULL, recurse_arg,
				 arg);
}

static pg_attribute_always_inline void
pg_sort_cmparg(void *data, size_t n, size_t size,
			   compare_arg_fun cmp_arg,
			   sort_cmparg_fun recurse_cmparg,
			   void *arg)
{
	pg_sort_impl(data, n, size,
				 NULL, cmp_arg,
				 NULL, NULL, NULL, NULL, recurse_cmparg, NULL,
				 arg);
}

static pg_attribute_always_inline void
pg_sort_sizecmparg(void *data, size_t n, size_t size,
				   compare_arg_fun cmp_arg,
				   sort_sizecmparg_fun recurse_sizecmparg,
				   void *arg)
{
	pg_sort_impl(data, n, size,
				 NULL, cmp_arg,
				 NULL, NULL, NULL, recurse_sizecmparg, NULL, NULL,
				 arg);
}

#endif
