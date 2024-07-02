/*-------------------------------------------------------------------------
 *
 * atomics.h
 *	  Atomic operations.
 *
 * These interfaces are for manipulating memory atomically and dealing with
 * cache coherency. They can be used to implement locking facilities and
 * lockless algorithms/data structures.
 *
 * This is mostly a renaming of <stdatomic.h>, with some differences:
 *
 * * read/write barriers (weaker than acquire/release)
 * * they affect even non-atomic access without dependency on atomic access
 * * pg_atomic_flag semantics don't allow mapping to atomic_flag
 *
 * PostgreSQL atomic type width assumptions:
 *
 * 8, 16:  Required to be lock-free (even though implemented with wider
 *         instructions by the compiler on some RISC-V systems).
 *
 * 32:     Required to be lock-free.  No known modern system lacks them.
 *
 * 64:     These can reasonably be expected to be lock-free and fast on
 *         all modern-ish systems.  For one known low-end system  they are
 *         emulated by the compiler/runtime with locks (armv7).
 *
 * In all cases values must be well aligned or undefined behavior results.
 *
 * To bring up postgres on a compiler, provide:
 * * pg_compiler_barrier()
 * Optionally also:
 * * pg_memory_barrier(), pg_write_barrier(), pg_read_barrier()
 *
 * Use higher level functionality (lwlocks, spinlocks, heavyweight locks)
 * whenever possible. Writing correct code using these facilities is hard.
 *
 * For an introduction to using memory barriers within the PostgreSQL backend,
 * see src/backend/storage/lmgr/README.barrier
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/atomics.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ATOMICS_H
#define ATOMICS_H

#define INSIDE_ATOMICS_H

#if defined(__cplusplus) && __cplusplus < 202302L
/* C++11 can't include C's <stdatomic.h>.  Use approach from C++23 33.5.12. */
extern "C++"
{
#include <atomic>
#define pg_atomic(T) std::atomic<T>
	using		std::memory_order_relaxed;
	using		std::memory_order_acquire;
	using		std::memory_order_release;
	using		std::memory_order_acq_rel;
	using		std::memory_order_seq_cst;
}
#else
#include <stdatomic.h>
#define pg_atomic(T) _Atomic(T)
#endif

/*
 * Visual Studio 2022 doesn't seem to define these in <stdatomic.h> yet, but
 * does define them to 2 in <atomic>.  They must hold in C too though, since
 * they interoperate.
 */
#if !defined(_MSC_VER) || defined(__cplusplus)
/* Assumptions about lock-free access. */
StaticAssertDecl(ATOMIC_CHAR_LOCK_FREE >= 2, "need lock-free 8-bit atomics");
StaticAssertDecl(ATOMIC_SHORT_LOCK_FREE >= 2, "need lock-free 16-bit atomics");
StaticAssertDecl(ATOMIC_INT_LOCK_FREE >= 2, "need lock-free 32-bit atomics");
#endif

/* Can't use standard atomic_flag due to extended semantics, see below. */
typedef pg_atomic(uint8)
pg_atomic_flag;

/* Common supported atomic integer types. */
typedef pg_atomic(uint8)
pg_atomic_uint8;
typedef pg_atomic(uint16)
pg_atomic_uint16;
typedef pg_atomic(uint32)
pg_atomic_uint32;
typedef pg_atomic(uint64)
pg_atomic_uint64;

/*
 * First a set of architecture specific files is included.
 *
 * It will often make sense to define memory barrier semantics here, since
 * e.g. generic compiler intrinsics for x86 memory barriers can't know that
 * postgres doesn't need x86 read/write barriers do anything more than a
 * compiler barrier.
 */
#if defined(__i386__) || defined(__i386) || defined(__x86_64__)
#include "port/atomics/arch-x86.h"
#endif

/*
 * Compiler specific, but architecture independent implementations.
 * This defines compiler barriers.
 */
/*
 * gcc or compatible, including clang and icc.
 */
#if defined(__GNUC__) || defined(__INTEL_COMPILER)
#include "port/atomics/generic-gcc.h"
#elif defined(_MSC_VER)
#include "port/atomics/generic-msvc.h"
#else
/* Unknown compiler. */
#endif

/* Fail if we couldn't find implementations of required facilities. */
#if !defined(pg_compiler_barrier_impl)
#error "could not find an implementation of pg_compiler_barrier"
#endif

/*
 * pg_compiler_barrier - prevent the compiler from moving code across
 *
 * A compiler barrier need not (and preferably should not) emit any actual
 * machine code, but must act as an optimization fence: the compiler must not
 * reorder loads or stores to main memory around the barrier.  However, the
 * CPU may still reorder loads or stores at runtime, if the architecture's
 * memory model permits this.
 *
 * Unlike standard atomic_signal_fence(), pg_compiler_barrier() prevents
 * reordering of non-atomic, non-volatile accesses, so a compiler-specific
 * definition is required.
 */
#define pg_compiler_barrier()	pg_compiler_barrier_impl()

/*
 * pg_memory_barrier - prevent the CPU from reordering memory access
 *
 * A memory barrier must act as a compiler barrier, and in addition must
 * guarantee that all loads and stores issued prior to the barrier are
 * completed before any loads or stores issued after the barrier.  Unless
 * loads and stores are totally ordered (which is not the case on most
 * architectures) this requires issuing some sort of memory fencing
 * instruction.
 *
 * Unlike standard atomic_thread_fence(), pg_{read,write}_barrier() affects
 * non-atomic, non-volatile accesses.
 */
static inline void
pg_memory_barrier(void)
{
#ifdef pg_memory_barrier_impl
	pg_memory_barrier_impl();
#else
	pg_compiler_barrier();
	atomic_thread_fence(memory_order_seq_cst);
#endif
}

/*
 * pg_(read|write)_barrier - prevent the CPU from reordering memory access
 *
 * A read barrier must act as a compiler barrier, and in addition must
 * guarantee that any loads issued prior to the barrier are completed before
 * any loads issued after the barrier.  Similarly, a write barrier acts
 * as a compiler barrier, and also orders stores.  Read and write barriers
 * are thus weaker than a full memory barrier, but stronger than a compiler
 * barrier.  In practice, on machines with strong memory ordering, read and
 * write barriers may require nothing more than a compiler barrier.
 *
 * Unlike standard atomic_thread_fence(), pg_{read,write}_barrier() affects
 * non-atomic, non-volatile accesses.  The default implementation uses
 * acquire/release fences, but these are strictly stronger than read/write.
 */

static inline void
pg_read_barrier(void)
{
#ifdef pg_read_barrier_impl
	pg_read_barrier_impl();
#else
	pg_compiler_barrier();
	atomic_thread_fence(memory_order_acquire);
#endif
}

static inline void
pg_write_barrier(void)
{
#ifdef pg_write_barrier_impl
	pg_write_barrier_impl();
#else
	pg_compiler_barrier();
	atomic_thread_fence(memory_order_release);
#endif
}

/*
 * Operations corresponding to standard atomic_flag.  We have to use an integer
 * instead of mapping directly to the standard names, to support our relaxed
 * check function.
 *
 * Acquire and release fences, which are respectively stronger than read and
 * write barriers.
 */
#define pg_atomic_init_flag(p) atomic_init((p), 1)
#define pg_atomic_test_set_flag(p) \
	atomic_fetch_and_explicit((p), 0, memory_order_acquire)
#define pg_atomic_clear_flag(p) \
	atomic_store_explicit((p), 1, memory_order_release)
#define pg_atomic_unlocked_test_flag(p) \
	atomic_load_explicit((p), memory_order_relaxed)

/*
 * Local convention for load/store, relaxed AKA no barrier.  These are the only
 * "generic" functions provided with pg_ prefixes.  It seems pointless to
 * rename everything in <stdatomic.h>, but these two are PostgreSQL's
 * established way of representing relaxed access, and a lot shorter.
 */
#define pg_atomic_read(p) \
	atomic_load_explicit((p), memory_order_relaxed)
#define pg_atomic_write(p, v) \
	atomic_store_explicit((p), (v), memory_order_relaxed)

/*
 * Backward-compatible type-specific function names.  Only the historical _u32
 * and _64 names are provided.  New code and code using other atomic types
 * should use the generic functions from the standard directly, and
 * pg_atomic_{read,write}() for relaxed access.
 *
 * All of these except pg_atomic_(unlocked_){read,write}_{32,64} have seq cst
 * AKA full barrier semantics.
 */
#define pg_atomic_init_u32 atomic_init
#define pg_atomic_init_u64 atomic_init
#define pg_atomic_read_u32 pg_atomic_read
#define pg_atomic_read_u64 pg_atomic_read
#define pg_atomic_write_u32 pg_atomic_write
#define pg_atomic_write_u64 pg_atomic_write
#define pg_atomic_unlocked_read_u32 pg_atomic_read
#define pg_atomic_unlocked_read_u64 pg_atomic_read
#define pg_atomic_unlocked_write_u32 pg_atomic_write
#define pg_atomic_unlocked_write_u64 pg_atomic_write
#define pg_atomic_read_membarrier_u32 atomic_load
#define pg_atomic_read_membarrier_u64 atomic_load
#define pg_atomic_write_membarrier_u32 atomic_store
#define pg_atomic_write_membarrier_u64 atomic_store
#define pg_atomic_exchange_u32 atomic_exchange
#define pg_atomic_exchange_u64 atomic_exchange
#define pg_atomic_compare_exchange_u32 atomic_compare_exchange_strong
#define pg_atomic_compare_exchange_u64 atomic_compare_exchange_strong
#define pg_atomic_fetch_add_u32 atomic_fetch_add
#define pg_atomic_fetch_add_u64 atomic_fetch_add
#define pg_atomic_fetch_sub_u32 atomic_fetch_sub
#define pg_atomic_fetch_sub_u64 atomic_fetch_sub
#define pg_atomic_fetch_and_u32 atomic_fetch_and
#define pg_atomic_fetch_and_u64 atomic_fetch_and
#define pg_atomic_fetch_or_u32 atomic_fetch_or
#define pg_atomic_fetch_or_u64 atomic_fetch_or

/*
 * The rest of this file defines non-fundamental convenience functions with no
 * counterpart in <stdatomic.h>.
 */

/*
 * Fetch and add/subtract wrappers that return the new value instead of the old
 * value.  We need functions to avoid double evaluation of v.
 */
#define PG_ATOMIC_GEN_REVERSE_FETCH(size, name, op) \
static inline uint##size \
pg_atomic_##name##_fetch_u##size(volatile pg_atomic_uint##size *p, uint##size v) \
{ \
	return atomic_fetch_##name(p, v) op v; \
}
PG_ATOMIC_GEN_REVERSE_FETCH(32, add, +);
PG_ATOMIC_GEN_REVERSE_FETCH(64, add, +);
PG_ATOMIC_GEN_REVERSE_FETCH(32, sub, -);
PG_ATOMIC_GEN_REVERSE_FETCH(64, sub, -);

/*
 * Monotonically advance the given variable using only atomic operations until
 * it's at least the target value.  Returns the latest value observed, which
 * may or may not be the target value.
 *
 * Full barrier semantics (even when value is unchanged).
 */
static inline uint64
pg_atomic_monotonic_advance_u64(volatile pg_atomic_uint64 *ptr, uint64 target)
{
	uint64		currval;

	currval = pg_atomic_read_u64(ptr);
	if (currval >= target)
	{
		pg_memory_barrier();
		return currval;
	}

	while (currval < target)
	{
		if (pg_atomic_compare_exchange_u64(ptr, &currval, target))
			return target;
	}

	return currval;
}

#undef INSIDE_ATOMICS_H

#endif							/* ATOMICS_H */
