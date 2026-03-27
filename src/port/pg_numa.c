/*-------------------------------------------------------------------------
 *
 * pg_numa.c
 * 		Basic NUMA portability routines
 *
 *
 * Copyright (c) 2025-2026, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/port/pg_numa.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"
#include <unistd.h>

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "port/pg_numa.h"

/*
 * At this point we provide support only for Linux thanks to libnuma, but in
 * future support for other platforms e.g. Win32 or FreeBSD might be possible
 * too. For Win32 NUMA APIs see
 * https://learn.microsoft.com/en-us/windows/win32/procthread/numa-support
 */
#ifdef USE_LIBNUMA

#include <limits.h>
#include <numa.h>
#include <numaif.h>

/*
 * numa_move_pages() chunk size, has to be <= 16 to work around a kernel bug
 * in do_pages_stat() (chunked by DO_PAGES_STAT_CHUNK_NR). By using the same
 * chunk size, we make it work even on unfixed kernels.
 *
 * 64-bit system are not affected by the bug, and so use much larger chunks.
 */
#if SIZEOF_SIZE_T == 4
#define NUMA_QUERY_CHUNK_SIZE 16
#else
#define NUMA_QUERY_CHUNK_SIZE 1024
#endif

/* libnuma requires initialization as per numa(3) on Linux */
int
pg_numa_init(void)
{
	int			r;

	/*
	 * XXX libnuma versions before 2.0.19 don't handle EPERM by disabling
	 * NUMA, which then leads to unexpected failures later. This affects
	 * containers that disable get_mempolicy by a seccomp profile.
	 */
	if (get_mempolicy(NULL, NULL, 0, 0, 0) < 0 && (errno == EPERM))
		r = -1;
	else
		r = numa_available();

	return r;
}

/*
 * We use move_pages(2) syscall here - instead of get_mempolicy(2) - as the
 * first one allows us to batch and query about many memory pages in one single
 * giant system call that is way faster.
 *
 * We call numa_move_pages() for smaller chunks of the whole array. The first
 * reason is to work around a kernel bug, but also to allow interrupting the
 * query between the calls (for many pointers processing the whole array can
 * take a lot of time).
 */
int
pg_numa_query_pages(int pid, unsigned long count, void **pages, int *status)
{
	unsigned long next = 0;
	int			ret = 0;

	/*
	 * Chunk pointers passed to numa_move_pages to NUMA_QUERY_CHUNK_SIZE
	 * items, to work around a kernel bug in do_pages_stat().
	 */
	while (next < count)
	{
		unsigned long count_chunk = Min(count - next,
										NUMA_QUERY_CHUNK_SIZE);

#ifndef FRONTEND
		CHECK_FOR_INTERRUPTS();
#endif

		/*
		 * Bail out if any of the chunks errors out (ret<0). We ignore (ret>0)
		 * which is used to return number of nonmigrated pages, but we're not
		 * migrating any pages here.
		 */
		ret = numa_move_pages(pid, count_chunk, &pages[next], NULL, &status[next], 0);
		if (ret < 0)
		{
			/* plain error, return as is */
			return ret;
		}

		next += count_chunk;
	}

	/* should have consumed the input array exactly */
	Assert(next == count);

	return 0;
}

int
pg_numa_get_max_node(void)
{
	return numa_max_node();
}

int
pg_numa_get_node_for_cpu(pg_cpu_t cpu)
{
	return numa_node_of_cpu(cpu);
}

int
pg_numa_get_cpus_for_node(int node, pg_cpuset_t *cpuset)
{
	struct bitmask *mask;
	int			possible_cpus;

	pg_cpuset_initialize(cpuset);

	if (numa_available() < 0)
		return 0;

	possible_cpus = numa_num_possible_cpus();
	mask = numa_allocate_cpumask();
	if (mask == NULL)
		return -1;
	if (numa_node_to_cpus(node, mask) < 0)
	{
		numa_free_cpumask(mask);
		return -1;
	}
	for (int i = 0; i < possible_cpus; ++i)
		if (numa_bitmask_isbitset(mask, i))
			pg_cpuset_add(cpuset, i);
	numa_free_cpumask(mask);

	return 0;
}

#define PG_NUMA_MAX_NODE ((sizeof(unsigned long) * CHAR_BIT) - 1)

static bool
pg_numa_node_value_ok(int node)
{
	if (node < 0 ||
		node > PG_NUMA_MAX_NODE ||
		pg_numa_get_max_node() > PG_NUMA_MAX_NODE)
	{
		errno = EOVERFLOW;
		return false;
	}
	return true;
}

int
pg_numa_set_policy_prefer(int node)
{
	unsigned long bitmap = 1 << node;

	if (!pg_numa_node_value_ok(node))
		return -1;

	return set_mempolicy(MPOL_PREFERRED, &bitmap, PG_NUMA_MAX_NODE);
}

int
pg_numa_set_policy_interleave(void)
{
	unsigned long bitmap = ((1 << pg_numa_get_max_node()) + 1) - 1;

	if (!pg_numa_node_value_ok(0))
		return -1;

	return set_mempolicy(MPOL_INTERLEAVE, &bitmap, PG_NUMA_MAX_NODE);
}

int
pg_numa_set_policy_local(void)
{
	unsigned long bitmap = 0;

	return set_mempolicy(MPOL_LOCAL, &bitmap, PG_NUMA_MAX_NODE);
}

typedef struct pg_numa_policy_holder
{
	int			mode;
	unsigned long nodemask;
} pg_numa_policy_holder;

int
pg_numa_save_policy(pg_numa_opaque_policy *policy)
{
	struct pg_numa_policy_holder *p = (pg_numa_policy_holder *) policy;

	if (!pg_numa_node_value_ok(0))
		return -1;

	return get_mempolicy(&p->mode,
						 &p->nodemask,
						 PG_NUMA_MAX_NODE,
						 NULL,
						 0);
}

int
pg_numa_restore_policy(const pg_numa_opaque_policy *policy)
{
	const struct pg_numa_policy_holder *p = (const pg_numa_policy_holder *) policy;

	return set_mempolicy(p->mode,
						 &p->nodemask,
						 PG_NUMA_MAX_NODE);
}

int
pg_numa_tonode_memory(void *mem, size_t size, int node)
{
	numa_tonode_memory(mem, size, node);
	return 0;
}

int
pg_numa_run_on_node(int node)
{
	return numa_run_on_node(node);
}


#elif defined(__FreeBSD__)

#include <sys/param.h>
#include <sys/domainset.h>
#include <sys/sysctl.h>

int
pg_numa_init(void)
{
	return -1;
}

int
pg_numa_query_pages(int pid, unsigned long count, void **pages, int *status)
{
	return 0;
}

int
pg_numa_get_max_node(void)
{
	int			ndomains;
	size_t		size = sizeof(ndomains);

	if (sysctlbyname("vm.ndomains", &ndomains, &size, NULL, 0) < 0)
		return 0;

	return ndomains > 0 ? ndomains - 1 : 0;
}

int
pg_numa_get_node_for_cpu(pg_cpu_t cpu)
{
	return 0;
}

int
pg_numa_get_cpus_for_node(int node, pg_cpuset_t *cpuset)
{
	pg_cpuset_initialize(cpuset);
	for (int i = 0;; ++i)
	{
		char		name[80];
		int			domain;
		size_t		size = sizeof(domain);

		snprintf(name, sizeof(name), "dev.cpu.%d.%%domain", i);
		size = sizeof(int);
		if (sysctlbyname(name, &domain, &size, NULL, 0) < 0)
		{
			if (errno != ENOENT)
				return -1;
			break;
		}
		if (node == domain)
			pg_cpuset_add(cpuset, i);
	}
	return 0;
}

int
pg_numa_set_policy_prefer(int node)
{
	domainset_t domains;

	DOMAINSET_ZERO(&domains);
	DOMAINSET_SET(node, &domains);
	return cpuset_setdomain(CPU_LEVEL_WHICH,
							CPU_WHICH_PID,
							-1,
							sizeof(domains),
							&domains,
							DOMAINSET_POLICY_PREFER);
}

int
pg_numa_set_policy_interleave(void)
{
	int			num_nodes = pg_numa_get_max_node() + 1;
	domainset_t domains;

	DOMAINSET_ZERO(&domains);
	for (int i = 0; i < num_nodes; ++i)
		DOMAINSET_SET(i, &domains);
	return cpuset_setdomain(CPU_LEVEL_WHICH,
							CPU_WHICH_PID,
							-1,
							sizeof(domains),
							&domains,
							DOMAINSET_POLICY_INTERLEAVE);
}

int
pg_numa_set_policy_local(void)
{
	int			num_nodes = pg_numa_get_max_node() + 1;
	domainset_t domains;

	DOMAINSET_ZERO(&domains);
	for (int i = 0; i < num_nodes; ++i)
		DOMAINSET_SET(i, &domains);
	return cpuset_setdomain(CPU_LEVEL_WHICH,
							CPU_WHICH_PID,
							-1,
							sizeof(domains),
							&domains,
							DOMAINSET_POLICY_FIRSTTOUCH);
}

typedef struct pg_numa_policy_holder
{
	domainset_t domains;
	int			policy;
} pg_numa_policy_holder;

int
pg_numa_save_policy(pg_numa_opaque_policy *policy)
{
	struct pg_numa_policy_holder *p = (pg_numa_policy_holder *) policy;

	static_assert(sizeof(*policy) >= sizeof(*p),
				  "pg_numa_opaque_policy too small");

	return cpuset_getdomain(CPU_LEVEL_WHICH,
							CPU_WHICH_PID,
							-1,
							sizeof(p->domains),
							&p->domains,
							&p->policy);
}

int
pg_numa_restore_policy(const pg_numa_opaque_policy *policy)
{
	const struct pg_numa_policy_holder *p = (const pg_numa_policy_holder *) policy;

	return cpuset_setdomain(CPU_LEVEL_WHICH,
							CPU_WHICH_PID,
							-1,
							sizeof(p->domains),
							&p->domains,
							p->policy);
}

#elif defined(WIN32)

#include <windows.h>

int
pg_numa_init(void)
{
	return -1;
}

int
pg_numa_query_pages(int pid, unsigned long count, void **pages, int *status)
{
	return 0;
}

int
pg_numa_get_max_node(void)
{
	ULONG		node;

	return GetNumaHighestNodeNumber(&node) ? node : 0;
}

int
pg_numa_get_node_for_cpu(pg_cpu_t cpu)
{
	USHORT		node;

	return GetNumaProcessorNodeEx(&cpu, &node) ? node : 0;
}

typedef BOOL (WINAPI * GetNumaNodeProcessorMask2_t) (USHORT,
													 PGROUP_AFFINITY,
													 USHORT,
													 PUSHORT);

int
pg_numa_get_cpus_for_node(int node, pg_cpuset_t *cpuset)
{
	HMODULE		kernel32;
	void	   *func;
	GetNumaNodeProcessorMask2_t GetNumaNodeProcessorMask2_func;

	/*
	 * There is a newer function that works with systems that have more than
	 * 64 CPUs per NUMA node, available since Windows Server 2022/Windows 11.
	 * MinGW doesn't seem to know about it, so let's grovel it out of
	 * kernel32.dll and provide a fallback.
	 */
	kernel32 = GetModuleHandle(TEXT("kernel32.dll"));
	if (kernel32 && (func = GetProcAddress(kernel32, "GetNumaNodeProcessorMask2")))
	{
		GetNumaNodeProcessorMask2_func = (GetNumaNodeProcessorMask2_t) func;

		if (GetNumaNodeProcessorMask2_func(node,
										   &cpuset->masks[0],
										   lengthof(cpuset->masks),
										   &cpuset->count))
			return 0;
		_dosmaperr(GetLastError());
		return -1;
	}
	else
	{
		/* The old version can only handle 64 CPUs per NUMA node. */
		cpuset->count = 1;
		if (GetNumaNodeProcessorMaskEx(node, &cpuset->masks[0]))
			return 0;
		_dosmaperr(GetLastError());
		return -1;
	}
}

#else

/* Empty wrappers */
int
pg_numa_init(void)
{
	/* We state that NUMA is not available */
	return -1;
}

int
pg_numa_query_pages(int pid, unsigned long count, void **pages, int *status)
{
	return 0;
}

int
pg_numa_get_max_node(void)
{
	return 0;
}

int
pg_numa_get_node_for_cpu(pg_cpu_t cpu)
{
	return 0;
}

int
pg_numa_get_cpus_for_node(int node, pg_cpuset_t *cpuset)
{
	return 0;
}

#endif
