/*-------------------------------------------------------------------------
 *
 * pg_cpuset.c
 *	  Portable interface for controlling CPU affinity.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/port/pg_cpuset.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#ifdef HAVE_PTHREAD_GETAFFINITY_NP
#include <pthread.h>
#ifdef HAVE_PTHREAD_NP_H
#include <pthread_np.h>
#else
#endif
#endif

#ifdef WIN32
#include <windows.h>
#endif

#include "port/pg_bitutils.h"
#include "port/pg_cpuset.h"
#include "port/pg_numa.h"

void
pg_cpuset_initialize(pg_cpuset_t *cpuset)
{
#ifdef CPU_SETSIZE
	CPU_ZERO(cpuset);
#elif defined(WIN32)
	cpuset->count = 0;
#endif
}

bool
pg_cpuset_contains(const pg_cpuset_t *cpuset, pg_cpu_t cpu)
{
#ifdef CPU_SETSIZE
	return CPU_ISSET(cpu, cpuset);
#elif defined(WIN32)
	for (int i = 0; i < cpuset->count; ++i)
		if (cpuset->masks[i].Group == cpu.Group)
			return cpuset->masks[i].Mask & (1 << cpu.Number);
	return false;
#else
	return false;
#endif
}

void
pg_cpuset_and(pg_cpuset_t *a, const pg_cpuset_t *b)
{
#ifdef CPU_SETSIZE
	CPU_AND(a, a, b);
#elif defined(WIN32)
	for (int i = 0; i < a->count; ++i)
	{
		for (int j = 0; j < b->count; ++j)
		{
			if (a->masks[i].Group == b->masks[j].Group)
			{
				a->masks[i].Mask &= b->masks[j].Mask;
				break;
			}
		}
	}
#endif
}

void
pg_cpuset_add(pg_cpuset_t *cpuset, pg_cpu_t cpu)
{
#ifdef CPU_SETSIZE
	Assert(cpu >= 0 && cpu < CPU_SETSIZE);
	CPU_SET(cpu, cpuset);
#elif defined(WIN32)
	int			index = -1;

	for (int i = 0; i < cpuset->count; ++i)
	{
		if (cpuset->masks[i].Group == cpu.Group)
		{
			index = i;
			break;
		}
	}
	if (index == -1)
	{
		if (cpuset->count < lengthof(cpuset->masks))
		{
			memset(&cpuset->masks[cpuset->count],
				   0,
				   sizeof(cpuset->masks[cpuset->count]));
			cpuset->masks[cpuset->count].Group = cpu.Group;
			index = cpuset->count++;
		}
		else
		{
			Assert(false && "masks[] too small");
			return;
		}
	}
	cpuset->masks[index].Mask |= 1 << cpu.Number;
#endif
}

void
pg_cpuset_remove(pg_cpuset_t *cpuset, pg_cpu_t cpu)
{
#ifdef CPU_SETSIZE
	Assert(cpu >= 0 && cpu < CPU_SETSIZE);
	CPU_CLR(cpu, cpuset);
#elif defined(WIN32)
	int			index = -1;

	for (int i = 0; i < cpuset->count; ++i)
	{
		if (cpuset->masks[i].Group == cpu.Group)
		{
			index = i;
			break;
		}
	}
	if (index != -1)
		cpuset->masks[index].Mask &= 1 << cpu.Number;
#endif
}

int
pg_cpuset_count(const pg_cpuset_t *cpuset)
{
#ifdef CPU_SETSIZE
	return CPU_COUNT(cpuset);
#elif defined(WIN32)
	int			result = 0;

	for (int i = 0; i < cpuset->count; ++i)
		result += pg_popcount64(cpuset->masks[i].Mask);
	return result;
#else
	return 0;
#endif
}

bool
pg_cpuset_is_empty(const pg_cpuset_t *cpuset)
{
#ifdef CPU_SETSIZE
	return CPU_COUNT(cpuset) == 0;
#elif defined(WIN32)
	for (int i = 0; i < cpuset->count; ++i)
		if (cpuset->masks[i].Mask != 0)
			return false;
	return true;
#else
	return true;
#endif
}

void
pg_cpuset_iterator_begin(const pg_cpuset_t *cpuset, pg_cpuset_iterator *iter)
{
#ifdef CPU_SETSIZE
	CPU_ZERO(&iter->empty);
	iter->remaining = *cpuset;
	iter->next = 0;
#elif defined(WIN32)
	iter->cpuset = cpuset;
	iter->index = 0;
	iter->next_processor = 0;
	/* Find the first non-empty mask. */
	while (iter->index < cpuset->count &&
		   cpuset->masks[iter->index].Mask == 0)
		iter->index++;
	/* If we succeeded, find the lowest processor bit. */
	if (iter->index < cpuset->count)
		iter->next_processor =
			pg_rightmost_one_pos64(cpuset->masks[iter->index].Mask);
#endif
}

bool
pg_cpuset_iterator_has_next(const pg_cpuset_iterator *iter)
{
#ifdef CPU_SETSIZE
	return !CPU_EQUAL(&iter->empty, &iter->remaining);
#elif defined(WIN32)
	return iter->index < iter->cpuset->count;
#else
	return false;
#endif
}

pg_cpu_t
pg_cpuset_iterator_next(pg_cpuset_iterator *iter)
{
#ifdef CPU_SETSIZE
	Assert(pg_cpuset_iterator_has_next(iter));
	while (!CPU_ISSET(iter->next, &iter->remaining))
		iter->next++;
	CPU_CLR(iter->next, &iter->remaining);
	return iter->next++;
#elif defined(WIN32)
	const GROUP_AFFINITY *mask;
	pg_cpu_t	result = {0};
	uint64		rest;

	/* We are pointing at the CPU to return already. */
	Assert(pg_cpuset_iterator_has_next(iter));
	mask = &iter->cpuset->masks[iter->index];
	Assert(mask->Mask & (1 << iter->next_processor));
	result.Group = mask->Group;
	result.Number = iter->next_processor;

	/* Mask off that processor and all lower processors. */
	rest = mask->Mask & ~((1 << (iter->next_processor + 1)) - 1);
	if (rest != 0)
	{
		/* The lowest remaining processor is next. */
		iter->next_processor = pg_rightmost_one_pos64(rest);
	}
	else
	{
		/* Advance index until we find a non-empty mask. */
		do
		{
			iter->index++;
		}
		while (iter->index < iter->cpuset->count &&
			   iter->cpuset->masks[iter->index].Mask == 0);
		/* If we didn't run out of entries, the lowest bit is next. */
		if (iter->index < iter->cpuset->count)
			iter->next_processor =
				pg_rightmost_one_pos64(iter->cpuset->masks[iter->index].Mask);
	}
	return result;
#else
	return 0;
#endif
}

#ifdef WIN32
static int
pg_cpuset_get_all_windows(pg_cpuset_t *cpuset)
{
	typedef SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX info_type;
	char		buffer[4096];
	unsigned long buffer_size = sizeof(buffer);
	char	   *end;
	char	   *p;

	pg_cpuset_initialize(cpuset);
	if (!GetLogicalProcessorInformationEx(RelationProcessorCore,
										  (info_type *) buffer,
										  &buffer_size))
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	p = buffer;
	end = p + buffer_size;
	while (p < end)
	{
		info_type  *info = (info_type *) p;

		if (info->Relationship == RelationProcessorCore)
		{
			PROCESSOR_RELATIONSHIP *pr = &info->Processor;

			/*
			 * PROCESSOR_RELATIONSHIP has an array of GROUP_AFFINITY, just
			 * like pg_cpuset_t.  They contain a process group number and a
			 * 64-bit mask of processors in the group.
			 */
			for (int i = 0; i < pr->GroupCount; ++i)
			{
				bool		found = false;

				/* If a group is repeated, merge the bitmaps. */
				for (int j = 0; j < cpuset->count; ++j)
				{
					if (cpuset->masks[j].Group == pr->GroupMask[i].Group)
					{
						cpuset->masks[j].Mask |= pr->GroupMask[i].Mask;
						found = true;
						break;
					}
				}
				/* Otherwise append. */
				if (!found)
				{
					if (cpuset->count == lengthof(cpuset->masks))
					{
						errno = ENOBUFS;
						return -1;
					}
					cpuset->masks[cpuset->count++] = pr->GroupMask[i];
				}
			}
		}
		p += info->Size;
	}
	return 0;
}
#endif

int
pg_cpuset_get_thread_affinity_self(pg_cpuset_t *cpuset)
{
#if defined(HAVE_PTHREAD_GETAFFINITY_NP)
	return pthread_getaffinity_np(pthread_self(), sizeof(*cpuset), cpuset);
#elif defined(WIN32)
	if (GetThreadSelectedCpuSetMasks(GetCurrentThread(),
									 &cpuset->masks[0],
									 lengthof(cpuset->masks),
									 &cpuset->count))
	{
		if (cpuset->count > 0)
			return 0;
	}
	else
	{
		_dosmaperr(GetLastError());
		return -1;
	}
	return pg_cpuset_get_all_windows(cpuset);
#else
	errno = ENOSYS;
	return -1;
#endif
}

int
pg_cpuset_set_thread_affinity_self(const pg_cpuset_t *cpuset)
{
#if defined(HAVE_PTHREAD_GETAFFINITY_NP)
	return pthread_setaffinity_np(pthread_self(), sizeof(*cpuset), cpuset);
#elif defined(WIN32)
	if (SetThreadSelectedCpuSetMasks(GetCurrentThread(),
									 unconstify(GROUP_AFFINITY *,
												&cpuset->masks[0]),
									 cpuset->count))
		return 0;
	_dosmaperr(GetLastError());
	return -1;
#else
	errno = ENOSYS;
	return -1;
#endif
}

int
pg_cpuset_get_process_affinity_self(pg_cpuset_t *cpuset)
{
#if defined(HAVE_SCHED_GETAFFINITY)
	return sched_getaffinity(0, sizeof(*cpuset), cpuset);
#elif defined(HAVE_CPUSET_GETAFFINITY)
	return cpuset_getaffinity(CPU_LEVEL_WHICH,
							  CPU_WHICH_PID,
							  -1,
							  sizeof(*cpuset),
							  cpuset);
#elif defined(WIN32)
	if (GetProcessDefaultCpuSetMasks(GetCurrentProcess(),
									 &cpuset->masks[0],
									 lengthof(cpuset->masks),
									 &cpuset->count))
	{
		if (cpuset->count > 0)
			return 0;
	}
	else
	{
		_dosmaperr(GetLastError());
		return -1;
	}
	return pg_cpuset_get_all_windows(cpuset);
#else
	errno = ENOSYS;
	return -1;
#endif
}

int
pg_cpuset_set_process_affinity_self(const pg_cpuset_t *set)
{
#if defined(HAVE_SCHED_GETAFFINITY)
	return sched_setaffinity(0, sizeof(*set), set);
#elif defined(HAVE_CPUSET_GETAFFINITY)
	return cpuset_setaffinity(CPU_LEVEL_WHICH,
							  CPU_WHICH_PID,
							  -1,
							  sizeof(*set),
							  set);
#elif defined(WIN32)
	if (SetProcessDefaultCpuSetMasks(GetCurrentProcess(),
									 unconstify(GROUP_AFFINITY *,
												&set->masks[0]),
									 set->count))
		return 0;
	_dosmaperr(GetLastError());
	return -1;
#else
	errno = ENOSYS;
	return -1;
#endif
}
