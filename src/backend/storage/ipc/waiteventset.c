/*-------------------------------------------------------------------------
 *
 * waiteventset.c
 *	  ppoll()/pselect() like abstraction
 *
 * WaitEvents are an abstraction for waiting for one or more events at a time.
 * The waiting can be done in a race free fashion, similar ppoll() or
 * pselect() (as opposed to plain poll()/select()).
 *
 * You can wait for:
 * - a latch being set from another process or from signal handler in the same
 *   process (WL_LATCH_SET)
 * - data to become readable or writeable on a socket (WL_SOCKET_*)
 * - postmaster death (WL_POSTMASTER_DEATH or WL_EXIT_ON_PM_DEATH)
 * - timeout (WL_TIMEOUT)
 *
 * Implementation
 * --------------
 *
 * The poll() implementation uses the so-called self-pipe trick to overcome the
 * race condition involved with poll() and setting a global flag in the signal
 * handler. When a latch is set and the current process is waiting for it, the
 * signal handler wakes up the poll() in WaitLatch by writing a byte to a pipe.
 * A signal by itself doesn't interrupt poll() on all platforms, and even on
 * platforms where it does, a signal that arrives just before the poll() call
 * does not prevent poll() from entering sleep. An incoming byte on a pipe
 * however reliably interrupts the sleep, and causes poll() to return
 * immediately even if the signal arrives before poll() begins.
 *
 * The epoll() implementation overcomes the race with a different technique: it
 * keeps SIGURG blocked and consumes from a signalfd() descriptor instead.  We
 * don't need to register a signal handler or create our own self-pipe.  We
 * assume that any system that has Linux epoll() also has Linux signalfd().
 *
 * The kqueue() implementation waits for SIGURG with EVFILT_SIGNAL.
 *
 * The Windows implementation uses Windows events that are inherited by all
 * postmaster child processes. There's no need for the self-pipe trick there.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/waiteventset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#ifdef HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#endif
#ifdef HAVE_SYS_EVENT_H
#include <sys/event.h>
#endif
#ifdef HAVE_SYS_SIGNALFD_H
#include <sys/signalfd.h>
#endif
#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#include "common/hashfn.h"
#include "lib/ilist.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "portability/instr_time.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/latch.h"
#include "storage/waiteventset.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"

/*
 * Select the fd readiness primitive to use. Normally the "most modern"
 * primitive supported by the OS will be used, but for testing it can be
 * useful to manually specify the used primitive.  If desired, just add a
 * define somewhere before this block.
 */
#if defined(WAIT_USE_EPOLL) || defined(WAIT_USE_POLL) || \
	defined(WAIT_USE_KQUEUE) || defined(WAIT_USE_WIN32)
/* don't overwrite manual choice */
#elif defined(HAVE_SYS_EPOLL_H)
#define WAIT_USE_EPOLL
#elif defined(HAVE_KQUEUE)
#define WAIT_USE_KQUEUE
#elif defined(HAVE_POLL)
#define WAIT_USE_POLL
#elif WIN32
#define WAIT_USE_WIN32
#else
#error "no wait set implementation available"
#endif

/*
 * By default, we use a self-pipe with poll() and a signalfd with epoll(), if
 * available.  For testing the choice can also be manually specified.
 */
#if defined(WAIT_USE_POLL) || defined(WAIT_USE_EPOLL)
#if defined(WAIT_USE_SELF_PIPE) || defined(WAIT_USE_SIGNALFD)
/* don't overwrite manual choice */
#elif defined(WAIT_USE_EPOLL) && defined(HAVE_SYS_SIGNALFD_H)
#define WAIT_USE_SIGNALFD
#else
#define WAIT_USE_SELF_PIPE
#endif
#endif

typedef union WaitEventSetHandleImpl
{
	WaitEventSetHandle opaque;
#if defined(WAIT_USE_WIN32)
	HANDLE		wakeup;
#else
	pid_t		pid;
#endif
}			WaitEventSetHandleImpl;

static_assert(sizeof(WaitEventSetHandleImpl) == sizeof(WaitEventSetHandle),
			  "WL_HANDLE_SIZE is too small");

struct WaitEventRegistration
{
	WaitEvent	event;

	union
	{
		/* Members used in a physical WaitEventSet. */
		struct
		{
			/* Physical registrations bind to 0..N logical registrations. */
			dlist_head	bindings;
		}			physical;

		/* Members used in a logical WaitEventSet. */
		struct
		{
			/* Logical registrations bind to 0..1 physical registrations. */
			struct WaitEventRegistration *binding;
			/* Node in the physical registration's list of bindings. */
			dlist_node	bindings_node;
			/* Set that this registration belongs to. */
			WaitEventSet *set;
			/* Node in log_set's queue of bindings to create/modify. */
			dlist_node	dirty_registrations_node;
		}			logical;
	};

	/* Nodes for membership of WaitEventSet lists of registrations. */
	dlist_node	type_table_node;
	dlist_node	id_table_node;
};

typedef struct WaitEventRegistration WaitEventRegistration;

/* typedef in waiteventset.h */
struct WaitEventSet
{
	ResourceOwner owner;

	/*
	 * Underlying physical WaitEventSet, if this is a logical WaitEventSet.
	 * NULL if this is a physical WaitEventSet.
	 */
	WaitEventSet *underlying_set;

	union
	{
		struct
		{
			/*
			 * Number of linked logical sets and space required to avoid
			 * allocation failure in the underlying set if they don't exceed
			 * their requested nevents.
			 */
			int			reference_count;
			int			nevents_space_sum;
		}			physical;
		struct
		{
			/*
			 * List of logical registrations that are either not bound to a
			 * physical registration, or bound to a physical registration that
			 * might have a different event mask.  These must be resolved
			 * before waiting.
			 */
			dlist_head	dirty_registrations;
		}			logical;
	};

	int			nevents_space;	/* maximum number of events in this set */

	/*
	 * If nevents_space has been expanded beyond the value requested at
	 * creation time, this is non-NULL and must be freed later.
	 */
	void	   *expansion_mem;

	/*
	 * Array of words with at least nevents_space bits, for fast and compact
	 * allocation of elements in the events array.
	 */
	uint64_t   *events_bitmap;
	int			events_bitmap_size;

	/*
	 * Array, of nevents_space length, storing the definition of events this
	 * set is waiting for.
	 */
	WaitEventRegistration *events;

	/*
	 * Hash table of WaitEventRegistration objects by {type, id} using of hash
	 * chains linked with id_table_node.
	 */
	dlist_head *id_table;

	/*
	 * Lists of WaitEventRegistration objects for each type, linked with
	 * type_node.
	 */
	dlist_head	type_table[WL_TYPE_LAST + 1];

	/*
	 * WL_EXIT_ON_PM_DEATH is converted to WL_POSTMASTER_DEATH, but this flag
	 * is set so that we'll exit immediately if postmaster death is detected,
	 * instead of returning.
	 */
	bool		exit_on_postmaster_death;

#if defined(WAIT_USE_EPOLL)
	int			epoll_fd;
	/* epoll_wait returns events in a user provided arrays, allocate once */
	struct epoll_event *epoll_ret_events;
#elif defined(WAIT_USE_KQUEUE)
	int			kqueue_fd;
	/* kevent returns events in a user provided arrays, allocate once */
	struct kevent *kqueue_ret_events;
	bool		report_postmaster_not_running;
#elif defined(WAIT_USE_POLL)
	/* poll expects events to be waited on every poll() call, prepare once */
	struct pollfd *pollfds;
#elif defined(WAIT_USE_WIN32)

	/*
	 * Array of windows events. The first element always contains
	 * pgwin32_signal_event, so the remaining elements are offset by one (i.e.
	 * event->pos + 1).
	 */
	HANDLE	   *handles;
#endif
};

static const WaitEventMask wes_valid_masks[WL_TYPE_LAST + 1] = {
	[WL_TYPE_POSTMASTER] = WL_POSTMASTER_DEATH | WL_EXIT_ON_PM_DEATH,
	[WL_TYPE_LATCH] = WL_LATCH_SET,
	[WL_TYPE_SOCKET] = WL_SOCKET_MASK,
	[WL_TYPE_WAKEUP] = WL_WAKEUP_MASK,
};

static const char *wes_type_names[WL_TYPE_LAST + 1] = {
	[WL_TYPE_INVALID] = "WL_TYPE_INVALID",
	[WL_TYPE_POSTMASTER] = "WL_TYPE_POSTMASTER",
	[WL_TYPE_LATCH] = "WL_TYPE_LATCH",
	[WL_TYPE_SOCKET] = "WL_TYPE_SOCKET",
	[WL_TYPE_WAKEUP] = "WL_TYPE_WAKEUP",
};

#ifndef WIN32
/* Are we currently in WaitLatch? The signal handler would like to know. */
static volatile sig_atomic_t waiting = false;
#endif

#ifdef WAIT_USE_SIGNALFD
/* On Linux, we'll receive SIGURG via a signalfd file descriptor. */
static int	signal_fd = -1;
#endif

#ifdef WAIT_USE_SELF_PIPE
/* Read and write ends of the self-pipe */
static int	selfpipe_readfd = -1;
static int	selfpipe_writefd = -1;

/* Process owning the self-pipe --- needed for checking purposes */
static int	selfpipe_owner_pid = 0;

/* Private function prototypes */
static void latch_sigurg_handler(SIGNAL_ARGS);
static void sendSelfPipeByte(void);
#endif

#if defined(WAIT_USE_SELF_PIPE) || defined(WAIT_USE_SIGNALFD)
static void drain(void);
#endif

/* Platform-specific implementation functions. */
static void wes_adjust_physical(WaitEventSet *set,
								WaitEventRegistration *reg,
								WaitEventType type,
								WaitEventId id,
								WaitEventMask old_events,
								WaitEventMask new_events);
static inline int wes_wait_physical(WaitEventSet *set, int cur_timeout,
									WaitEvent *occurred_events, int nevents);

/* ResourceOwner support to hold WaitEventSets */
static void ResOwnerReleaseWaitEventSet(Datum res);

static const ResourceOwnerDesc wait_event_set_resowner_desc =
{
	.name = "WaitEventSet",
	.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
	.release_priority = RELEASE_PRIO_WAITEVENTSETS,
	.ReleaseResource = ResOwnerReleaseWaitEventSet,
	.DebugPrint = NULL
};

/* Convenience wrappers over ResourceOwnerRemember/Forget */
static inline void
ResourceOwnerRememberWaitEventSet(ResourceOwner owner, WaitEventSet *set)
{
	ResourceOwnerRemember(owner, PointerGetDatum(set), &wait_event_set_resowner_desc);
}
static inline void
ResourceOwnerForgetWaitEventSet(ResourceOwner owner, WaitEventSet *set)
{
	ResourceOwnerForget(owner, PointerGetDatum(set), &wait_event_set_resowner_desc);
}

#define sizeof_member(S, M) sizeof(((S *) NULL)->M)

static int
wes_lengthof_id_table(int nevents_space)
{
	return pg_nextpower2_32(nevents_space);
}

static size_t
wes_sizeof_id_table(int nevents_space)
{
	return sizeof_member(WaitEventSet, id_table[0]) *
		wes_lengthof_id_table(nevents_space);
}

static size_t
wes_sizeof_events(int nevents_space)
{
	return sizeof_member(WaitEventSet, events[0]) * nevents_space;
}

static size_t
wes_lengthof_events_bitmap(int nevents_space)
{
	return (nevents_space + 63) / 64;
}

static size_t
wes_sizeof_events_bitmap(int nevents_space)
{
	return sizeof_member(WaitEventSet, events_bitmap[0]) *
		wes_lengthof_events_bitmap(nevents_space);
}

static bool
wes_is_physical(WaitEventSet *set)
{
	return set->underlying_set == NULL;
}

static bool
wes_is_logical(WaitEventSet *set)
{
	return !wes_is_physical(set);
}

static void
wes_init_registration(WaitEventSet *set, WaitEventRegistration *reg)
{
	memset(reg, 0, sizeof(*reg));

	if (wes_is_physical(set))
	{
		dlist_init(&reg->physical.bindings);
	}
	else
	{
		reg->logical.set = set;
	}
}

static void
wes_id_table_init(WaitEventSet *set)
{
	for (int i = 0; i < wes_lengthof_id_table(set->nevents_space); ++i)
		dlist_init(&set->id_table[i]);
}

static dlist_head *
wes_id_table_bucket(WaitEventSet *set, WaitEventType type, WaitEventId id)
{
	uint64_t	hash = hash_combine64(murmurhash64(type), murmurhash64(id));
	size_t		buckets = wes_lengthof_id_table(set->nevents_space);

	return &set->id_table[hash & (buckets - 1)];
}

static WaitEventRegistration *
wes_id_table_find(WaitEventSet *set, WaitEventType type, WaitEventId id)
{
	WaitEventRegistration *reg;
	dlist_iter	iter;

	dlist_foreach(iter, wes_id_table_bucket(set, type, id))
	{
		reg = dlist_container(WaitEventRegistration, id_table_node, iter.cur);

		if (reg->event.type == type && reg->event.id == id)
			return reg;
	}

	return NULL;
}

static void
wes_id_table_insert(WaitEventSet *set, WaitEventRegistration *reg)
{
	Assert(reg->event.type != WL_TYPE_INVALID);
	Assert(wes_id_table_find(set, reg->event.type, reg->event.id) == NULL);
	dlist_push_tail(wes_id_table_bucket(set, reg->event.type, reg->event.id),
					&reg->id_table_node);
}

static void
wes_id_table_remove(WaitEventSet *set, WaitEventRegistration *reg)
{
	Assert(wes_id_table_find(set, reg->event.type, reg->event.id) == reg);
	dlist_delete(&reg->id_table_node);
}

static void
wes_type_table_init(WaitEventSet *set)
{
	for (int i = 0; i < lengthof(set->type_table); ++i)
		dlist_init(&set->type_table[i]);
}

static void
wes_type_table_insert(WaitEventSet *set, WaitEventRegistration *reg)
{
	Assert(reg->event.type != WL_TYPE_INVALID);
	dlist_push_tail(&set->type_table[reg->event.type],
					&reg->type_table_node);
}

static void
wes_type_list_remove(WaitEventSet *set, WaitEventRegistration *reg)
{
	dlist_delete(&reg->type_list_node);
}

static bool
wes_has_type(WaitEventSet *set, WaitEventType type)
{
	return !dlist_is_empty(&set->type_list[type]);
}

static int
wes_count_used(WaitEventSet *set)
{
	int			count = 0;

	for (int i = 0; i < wes_lengthof_events_bitmap(i); ++i)
		count += pg_popcount64(set->events_bitmap[i]);
	return count;
}

static bool
wes_is_used(WaitEventSet *set, WaitEventRegistration *reg)
{
	int			pos = reg - set->events;

	return set->events_bitmap[pos / 64] & (UINT64_C(1) << (pos % 64));
}

static void
wes_set_used(WaitEventSet *set, WaitEventRegistration *reg)
{
	int			pos = reg - set->events;

	Assert(reg->event.type != WL_TYPE_INVALID);
	Assert(!wes_is_used(set, reg));
	set->events_bitmap[pos / 64] |= UINT64_C(1) << (pos % 64);
}

static void
wes_clear_used(WaitEventSet *set, WaitEventRegistration *reg)
{
	int			pos = reg - set->events;

	Assert(reg->event.type == WL_TYPE_INVALID);
	Assert(wes_is_used(set, reg));
	set->events_bitmap[pos / 64] &= ~UINT64_C(1) << (pos % 64);
}

static WaitEventRegistration *
wes_find_index(WaitEventSet *set, WaitEventIndex index)
{
	WaitEventRegistration *reg;

	if (index < 0 || index >= set->nevents_space)
		elog(ERROR, "WaitEventSet index %d out of range", index);

	reg = &set->events[index];
	if (!wes_is_used(set, reg))
		elog(ERROR, "WaitEventSet index %d not in use", index);

	return reg;
}

static bool
wes_logical_registration_is_dirty(WaitEventRegistration *log_reg)
{
	Assert(wes_is_logical(log_reg->logical.set));

	/* Note that this thest requires deleting "thoroughly". */
	return !dlist_node_is_detached(&log_reg->logical.dirty_registrations_node);
}

static void
wes_logical_registration_set_dirty(WaitEventRegistration *log_reg)
{
	Assert(wes_is_logical(log_reg->logical.set));
	Assert(!wes_logical_registration_is_dirty(log_reg));
	dlist_push_tail(&log_reg->logical.set->logical.dirty_registrations,
					&log_reg->logical.dirty_registrations_node);
}

static void
wes_logical_registration_clear_dirty(WaitEventRegistration *log_reg)
{
	Assert(wes_is_logical(log_reg->logical.set));
	Assert(wes_logical_registration_is_dirty(log_reg));
	dlist_delete_thoroughly(&log_reg->logical.dirty_registrations_node);
}

static void
wes_propagate_logical_registration_change(WaitEventSet *log_set,
										  WaitEventRegistration *log_reg)
{
	WaitEventRegistration *phy_reg;

	if (wes_logical_registration_is_dirty(log_reg))
		return;

	phy_reg = log_reg->logical.binding;
	if (!phy_reg)
		return;

	if (log_reg->event.events != phy_reg->event.events)
		wes_logical_registration_set_dirty(log_reg);
}

static void
wes_propagate_physical_registration_change(WaitEventSet *phy_set,
										   WaitEventRegistration *phy_reg)
{
	dlist_mutable_iter iter;

	/*
	 * All logical registrations that are bound to this physical registration
	 * are marked dirty.  WaitEventSetWaitLogical() will make any necessary
	 * adjustments.
	 */
	dlist_foreach_modify(iter, &phy_reg->physical.bindings)
	{
		WaitEventRegistration *log_reg;

		log_reg = dlist_container(WaitEventRegistration,
								  id_table_node,
								  iter.cur);

		if (!wes_logical_registration_is_dirty(log_reg))
			wes_logical_registration_set_dirty(log_reg);
	}
}

static inline WaitEventRegistration *
wes_find_logical_registration_for_set(WaitEventRegistration *phy_reg,
									  WaitEventSet *log_set)
{
	WaitEventRegistration *log_reg;
	dlist_iter	iter;

	dlist_foreach(iter, &phy_reg->physical.bindings)
	{
		log_reg = dlist_container(WaitEventRegistration,
								  id_table_node,
								  iter.cur);
		if (log_reg->logical.set == log_set)
			return log_reg;
	}
	return NULL;
}

static bool
wes_type_uses_wakeup(WaitEventType type)
{
	switch (type)
	{
			/*
			 * List of waitable object types that can be handled by a logical
			 * WaitEventSet using WL_WAKEUP_RAW, and thus don't need to be
			 * "bound" to a physical registration.  This avoids book-keeping
			 * and adjustments for latches and condition variables.
			 */
		case WL_TYPE_LATCH:
			return false;
		default:
			return true;
	}
}

static bool
wes_physical_registration_has_bindings(WaitEventRegistration *phy_reg)
{
	return !dlist_is_empty(&phy_reg->physical.bindings);
}

static bool
wes_logical_registration_is_bound(WaitEventRegistration *logical)
{
	return logical->logical.binding != NULL;
}

static void
wes_bind_logical_registration(WaitEventRegistration *log_reg,
							  WaitEventRegistration *phy_reg)
{
	/* Shouldn't be binding registrations that don't need it. */
	Assert(!wes_type_uses_wakeup(log_reg->event.type));

	/* Shouldn't be already bound. */
	Assert(!wes_logical_registration_is_bound(log_reg));
	Assert(wes_find_logical_registration_for_set(phy_reg,
												 log_reg->logical.set) == NULL);

	dlist_push_tail(&phy_reg->physical.bindings,
					&log_reg->logical.bindings_node);
	log_reg->logical.binding = phy_reg;
}

static void
wes_unbind_logical_registration(WaitEventRegistration *log_reg)
{
	WaitEventRegistration *phy_reg;

	phy_reg = log_reg->logical.binding;

	Assert(phy_reg);
	Assert(wes_find_logical_registration_for_set(phy_reg,
												 log_reg->logical.set) ==
		   log_reg);

	/*
	 * It's important to delete thoroughly when reached from
	 * ReserveWaitEventSetSpace(), so we don't retain internal pointers to
	 * phy_reg->physical.bindings when rebinding.
	 */
	dlist_delete_thoroughly(&log_reg->logical.bindings_node);
	log_reg->logical.binding = NULL;
}

static void
wes_rebind_logical_registration(WaitEventRegistration *old_log_reg,
								WaitEventRegistration *new_log_reg)
{
	WaitEventRegistration *phy_reg = old_log_reg->logical.binding;

	wes_unbind_logical_registration(old_log_reg);
	wes_bind_logical_registration(new_log_reg, phy_reg);
}

static void
wes_unbind_physical_registration(WaitEventRegistration *phy_reg)
{
	dlist_head *bindings = &phy_reg->physical.bindings;

	while (!dlist_is_empty(bindings))
	{
		WaitEventRegistration *log_reg;

		log_reg = dlist_container(WaitEventRegistration,
								  logical.bindings_node,
								  dlist_pop_head_node(bindings));
		wes_unbind_logical_registration(log_reg);

		/*
		 * When one logical WaitEventSet deletes a physical event to silence
		 * it, the other logical WaitEventSets that were bound to it will need
		 * to add it before waiting.
		 */
		if (!wes_logical_registration_is_dirty(log_reg))
			wes_logical_registration_set_dirty(log_reg);
	}
}

static void
wes_rebind_physical_registration(WaitEventRegistration *old_phy_reg,
								 WaitEventRegistration *new_phy_reg)
{
	dlist_head *old_bindings = &old_phy_reg->physical.bindings;

	while (!dlist_is_empty(old_bindings))
	{
		WaitEventRegistration *log_reg;

		log_reg = dlist_container(WaitEventRegistration,
								  logical.bindings_node,
								  dlist_pop_head_node(old_bindings));
		wes_unbind_logical_registration(log_reg);
		wes_bind_logical_registration(log_reg, new_phy_reg);
	}
}

static WaitEventRegistration *
wes_find_free_registration(WaitEventSet *set)
{
	int			words = wes_lengthof_events_bitmap(set->nevents_space);
	WaitEventIndex index = -1;

	for (int i = 0; i < words; ++i)
	{
		uint64		word = ~set->events_bitmap[i];

		if (word != 0)
		{
			index = i * 64 + pg_rightmost_one_pos64(word);
			if (index >= set->nevents_space)
				index = -1;
			break;
		}
	}

	if (index == -1)
		return NULL;

	Assert(set->events[index].event.type == WL_TYPE_INVALID);
	return &set->events[index];
}

static void
wes_validate_add(WaitEventType type,
				 WaitEventId id,
				 uint32 events)
{
	if (type <= WL_TYPE_INVALID || type > WL_TYPE_LAST)
		elog(ERROR, "invalid type %d", type);

	if ((events & wes_valid_masks[type]) == 0 ||
		(events & ~wes_valid_masks[type]) != 0)
		elog(ERROR, "invalid events %u for type %s", events,
			 wes_type_names[type]);

	switch (type)
	{
		case WL_TYPE_POSTMASTER:
		case WL_TYPE_WAKEUP:
			if (id != 0)
				elog(ERROR, "invalid non-zero id %" PRIdPTR " for type %s",
					 id, wes_type_names[type]);
			break;

		case WL_TYPE_LATCH:
			if (id != 0)
			{
				Latch	   *latch = (Latch *) id;

				if (latch->owner_pid != MyProcPid)
					elog(ERROR, "cannot wait on a latch owned by another process");
			}
			else
			{
				elog(ERROR,
					 "invalid id for WL_TYPE_LATCH");
			}
			break;
		default:
			break;
	}
}

static Latch *
wes_get_latch(WaitEventRegistration *reg)
{
	Assert(reg->event.type == WL_TYPE_LATCH);
	return (Latch *) reg->event.id;
}

static void wes_end_wait_latches(WaitEventSet *set);

static int
wes_check_latches(WaitEventSet *set,
				  WaitEvent *occurred_events,
				  int nevents)
{
	WaitEventRegistration *reg;
	dlist_iter	iter;
	int			count = 0;

	dlist_foreach(iter, &set->type_list[WL_TYPE_LATCH])
	{
		reg = dlist_container(WaitEventRegistration, id_table_node, iter.cur);
		if (wes_get_latch(reg)->is_set)
		{
			*occurred_events = reg->event;
			if (++count == nevents)
				break;
		}
	}

	return count;
}

static int
wes_begin_wait_latches(WaitEventSet *set,
					   WaitEvent *occurred_events,
					   int nevents)
{
	WaitEventRegistration *reg;
	dlist_iter	iter;
	int			count;

	/* Initial check for already set latches. */
	count = wes_check_latches(set, occurred_events, nevents);
	if (count > 0)
		return count;

	/* Tell SetLatch() to wake this backend with WL_TYPE_WAKEUP. */
	dlist_foreach(iter, &set->type_list[WL_TYPE_LATCH])
	{
		reg = dlist_container(WaitEventRegistration, id_table_node, iter.cur);
		wes_get_latch(reg)->maybe_sleeping = true;
	}

	/* Pairs with barrier in SetLatch(). */
	pg_memory_barrier();

	/* Check again. */
	count = wes_check_latches(set, occurred_events, nevents);
	if (count > 0)
		wes_end_wait_latches(set);

	return count;
}

static void
wes_end_wait_latches(WaitEventSet *set)
{
	WaitEventRegistration *reg;
	dlist_iter	iter;

	dlist_foreach(iter, &set->type_list[WL_TYPE_LATCH])
	{
		reg = dlist_container(WaitEventRegistration, id_table_node, iter.cur);
		if (wes_get_latch(reg)->maybe_sleeping)
			wes_get_latch(reg)->maybe_sleeping = false;
	}
}

static int
wes_process_wakeup(WaitEventSet *set, WaitEvent *occurred_events, int nevents)
{
	if (nevents == 0)
		return 0;

	/*
	 * For now the only thing built on top of WL_TYPE_WAKEUP is WL_TYPE_LATCH.
	 */
	return wes_check_latches(set, occurred_events, nevents);
}

static inline bool
wes_has_dirty_registrations(WaitEventSet *log_set)
{
	Assert(wes_is_logical(log_set));
	return !dlist_is_empty(&log_set->logical.dirty_registrations);
}

static void
wes_modify_registration(WaitEventSet *set,
						WaitEventRegistration *reg,
						WaitEventMask events)
{
	if (reg->event.events == events)
		return;

	if (!wes_type_uses_wakeup(reg->event.type) &&
		!wes_is_logical(set))
		wes_adjust_physical(set,
							reg,
							reg->event.type,
							reg->event.id,
							reg->event.events,
							events);

	/* Exception safety: udpate state after syscall. */
	reg->event.events = events;

	if (wes_type_uses_wakeup(reg->event.type))
	{
		if (wes_is_physical(set))
			wes_propagate_physical_registration_change(set, reg);
		else
			wes_propagate_logical_registration_change(set, reg);
	}
}

static void
wes_delete_registration(WaitEventSet *set, WaitEventRegistration *reg)
{
	if (wes_is_logical(set))
	{
		WaitEventRegistration *phy_reg = reg->logical.binding;

		if (phy_reg)
		{
			wes_unbind_logical_registration(reg);

			/*
			 * If the physical registration has no remaining bindings (ie
			 * other logical WaitEventSets interested in it), it's time to
			 * delete it.  Leaving a stale descriptor in an OS-level set would
			 * be asking for trouble, as the user might close it.
			 */
			if (!wes_physical_registration_has_bindings(reg))
				wes_delete_registration(set->underlying_set, phy_reg);
		}
	}
	else
	{
		wes_adjust_physical(set,
							reg,
							reg->event.type,
							reg->event.id,
							reg->event.events,
							0);

		/*
		 * This is reached when one logical WaitEventSet suppresses
		 * uninteresting events that it doesn't want.  Some other logical
		 * WaitEventSet must be interested in it, and will need to re-add it
		 * before waiting.
		 */
		wes_unbind_physical_registration(reg);
	}

	/* Remove from lookup tables and make available for re-use. */
	wes_type_list_remove(set, reg);
	wes_id_table_remove(set, reg);
	reg->event.type = WL_TYPE_INVALID;
	wes_clear_used(set, reg);
}

static void
wes_resolve_dirty_registrations(WaitEventSet *set)
{
	dlist_mutable_iter iter;

	Assert(wes_is_logical(set));

	dlist_foreach_modify(iter, &set->logical.dirty_registrations)
	{
		WaitEventRegistration *log_reg;
		WaitEventRegistration *phy_reg;

		log_reg = dlist_container(WaitEventRegistration,
								  logical.dirty_registrations_node,
								  iter.cur);

		/*
		 * Events handled by logical WaitEventSet using WL_TYPE_WAKEUP should
		 * never appear in the dirty list as they don't need a binding.
		 */
		Assert(!wes_type_uses_wakeup(log_reg->event.type));

		if (wes_logical_registration_is_bound(log_reg))
		{
			phy_reg = log_reg->logical.binding;
			Assert(phy_reg->event.type == log_reg->event.type &&
				   phy_reg->event.id == log_reg->event.id);

			/* Make sure event mask matches (no-op if it does). */
			wes_modify_registration(set->underlying_set,
									phy_reg,
									log_reg->event.events);
		}
		else
		{
			WaitEventIndex phy_index;

			/* Add physical registration and bind them. */
			phy_index = AddWaitEventSetObject(set->underlying_set,
											  log_reg->event.type,
											  log_reg->event.id,
											  log_reg->event.events,
											  NULL);
			phy_reg = &set->underlying_set->events[phy_index];
			wes_bind_logical_registration(log_reg, phy_reg);
		}

		wes_logical_registration_clear_dirty(log_reg);
	}
}

/*
 * Initialize the process-local wait event infrastructure.
 *
 * This must be called once during startup of any process that can wait on
 * latches, before it issues any InitLatch() or OwnLatch() calls.
 */
void
InitializeWaitEventSupport(void)
{
#if defined(WAIT_USE_SELF_PIPE)
	int			pipefd[2];

	if (IsUnderPostmaster)
	{
		/*
		 * We might have inherited connections to a self-pipe created by the
		 * postmaster.  It's critical that child processes create their own
		 * self-pipes, of course, and we really want them to close the
		 * inherited FDs for safety's sake.
		 */
		if (selfpipe_owner_pid != 0)
		{
			/* Assert we go through here but once in a child process */
			Assert(selfpipe_owner_pid != MyProcPid);
			/* Release postmaster's pipe FDs; ignore any error */
			(void) close(selfpipe_readfd);
			(void) close(selfpipe_writefd);
			/* Clean up, just for safety's sake; we'll set these below */
			selfpipe_readfd = selfpipe_writefd = -1;
			selfpipe_owner_pid = 0;
			/* Keep fd.c's accounting straight */
			ReleaseExternalFD();
			ReleaseExternalFD();
		}
		else
		{
			/*
			 * Postmaster didn't create a self-pipe ... or else we're in an
			 * EXEC_BACKEND build, in which case it doesn't matter since the
			 * postmaster's pipe FDs were closed by the action of FD_CLOEXEC.
			 * fd.c won't have state to clean up, either.
			 */
			Assert(selfpipe_readfd == -1);
		}
	}
	else
	{
		/* In postmaster or standalone backend, assert we do this but once */
		Assert(selfpipe_readfd == -1);
		Assert(selfpipe_owner_pid == 0);
	}

	/*
	 * Set up the self-pipe that allows a signal handler to wake up the
	 * poll()/epoll_wait() in WaitLatch. Make the write-end non-blocking, so
	 * that SetLatch won't block if the event has already been set many times
	 * filling the kernel buffer. Make the read-end non-blocking too, so that
	 * we can easily clear the pipe by reading until EAGAIN or EWOULDBLOCK.
	 * Also, make both FDs close-on-exec, since we surely do not want any
	 * child processes messing with them.
	 */
	if (pipe(pipefd) < 0)
		elog(FATAL, "pipe() failed: %m");
	if (fcntl(pipefd[0], F_SETFL, O_NONBLOCK) == -1)
		elog(FATAL, "fcntl(F_SETFL) failed on read-end of self-pipe: %m");
	if (fcntl(pipefd[1], F_SETFL, O_NONBLOCK) == -1)
		elog(FATAL, "fcntl(F_SETFL) failed on write-end of self-pipe: %m");
	if (fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) == -1)
		elog(FATAL, "fcntl(F_SETFD) failed on read-end of self-pipe: %m");
	if (fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) == -1)
		elog(FATAL, "fcntl(F_SETFD) failed on write-end of self-pipe: %m");

	selfpipe_readfd = pipefd[0];
	selfpipe_writefd = pipefd[1];
	selfpipe_owner_pid = MyProcPid;

	/* Tell fd.c about these two long-lived FDs */
	ReserveExternalFD();
	ReserveExternalFD();

	pqsignal(SIGURG, latch_sigurg_handler);
#endif

#ifdef WAIT_USE_SIGNALFD
	sigset_t	signalfd_mask;

	if (IsUnderPostmaster)
	{
		/*
		 * It would probably be safe to re-use the inherited signalfd since
		 * signalfds only see the current process's pending signals, but it
		 * seems less surprising to close it and create our own.
		 */
		if (signal_fd != -1)
		{
			/* Release postmaster's signal FD; ignore any error */
			(void) close(signal_fd);
			signal_fd = -1;
			ReleaseExternalFD();
		}
	}

	/* Block SIGURG, because we'll receive it through a signalfd. */
	sigaddset(&UnBlockSig, SIGURG);

	/* Set up the signalfd to receive SIGURG notifications. */
	sigemptyset(&signalfd_mask);
	sigaddset(&signalfd_mask, SIGURG);
	signal_fd = signalfd(-1, &signalfd_mask, SFD_NONBLOCK | SFD_CLOEXEC);
	if (signal_fd < 0)
		elog(FATAL, "signalfd() failed");
	ReserveExternalFD();
#endif

#ifdef WAIT_USE_KQUEUE
	/* Ignore SIGURG, because we'll receive it via kqueue. */
	pqsignal(SIGURG, PG_SIG_IGN);
#endif
}


/*
 * Create physical or logical WaitEventSet.
 */
static WaitEventSet *
CreateWaitEventSetImpl(ResourceOwner resowner,
					   WaitEventSet *underlying_set,
					   int nevents)
{
	WaitEventSet *set;
	char	   *data;
	Size		sz = 0;

	/*
	 * Use MAXALIGN size/alignment to guarantee that later uses of memory are
	 * aligned correctly. E.g. epoll_event might need 8 byte alignment on some
	 * platforms, but earlier allocations like WaitEventSet and WaitEvent
	 * might not be sized to guarantee that when purely using sizeof().
	 */
	sz += MAXALIGN(sizeof(WaitEventSet));
	sz += MAXALIGN(wes_sizeof_id_table(nevents));
	sz += MAXALIGN(wes_sizeof_events_bitmap(nevents));
	sz += MAXALIGN(wes_sizeof_events(nevents));

	if (underlying_set == NULL)
	{
#if defined(WAIT_USE_EPOLL)
		sz += MAXALIGN(sizeof(struct epoll_event) * nevents);
#elif defined(WAIT_USE_KQUEUE)
		sz += MAXALIGN(sizeof(struct kevent) * nevents);
#elif defined(WAIT_USE_POLL)
		sz += MAXALIGN(sizeof(struct pollfd) * nevents);
#elif defined(WAIT_USE_WIN32)
		/* need space for the pgwin32_signal_event */
		sz += MAXALIGN(sizeof(HANDLE) * (nevents + 1));
#endif
	}

	if (resowner != NULL)
		ResourceOwnerEnlarge(resowner);

	data = (char *) MemoryContextAllocZero(TopMemoryContext, sz);

	set = (WaitEventSet *) data;
	data += MAXALIGN(sizeof(WaitEventSet));

	set->id_table = (dlist_head *) data;
	data += MAXALIGN(wes_sizeof_id_table(nevents));

	set->events_bitmap = (uint64_t *) data;
	data += MAXALIGN(wes_sizeof_events_bitmap(nevents));

	set->events = (WaitEventRegistration *) data;
	data += MAXALIGN(wes_sizeof_events(nevents));

	set->nevents_space = nevents;
	set->underlying_set = underlying_set;
	wes_id_table_init(set);
	wes_type_list_init(set);

	if (resowner != NULL)
	{
		ResourceOwnerRememberWaitEventSet(resowner, set);
		set->owner = resowner;
	}

	/* If logical, that's all we need. */
	if (underlying_set)
		return set;

#if defined(WAIT_USE_EPOLL)
	set->epoll_ret_events = (struct epoll_event *) data;
	data += MAXALIGN(sizeof(struct epoll_event) * nevents);
#elif defined(WAIT_USE_KQUEUE)
	set->kqueue_ret_events = (struct kevent *) data;
	data += MAXALIGN(sizeof(struct kevent) * nevents);
#elif defined(WAIT_USE_POLL)
	set->pollfds = (struct pollfd *) data;
	data += MAXALIGN(sizeof(struct pollfd) * nevents);
#elif defined(WAIT_USE_WIN32)
	set->handles = (HANDLE) data;
	data += MAXALIGN(sizeof(HANDLE) * nevents);
#endif

	/* Initialize physical implementations. */
#if defined(WAIT_USE_EPOLL)
	if (!AcquireExternalFD())
		elog(ERROR, "AcquireExternalFD, for epoll_create1, failed: %m");
	set->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	if (set->epoll_fd < 0)
	{
		ReleaseExternalFD();
		elog(ERROR, "epoll_create1 failed: %m");
	}
#elif defined(WAIT_USE_KQUEUE)
	if (!AcquireExternalFD())
		elog(ERROR, "AcquireExternalFD, for kqueue, failed: %m");
	set->kqueue_fd = kqueue();
	if (set->kqueue_fd < 0)
	{
		ReleaseExternalFD();
		elog(ERROR, "kqueue failed: %m");
	}
	if (fcntl(set->kqueue_fd, F_SETFD, FD_CLOEXEC) == -1)
	{
		int			save_errno = errno;

		close(set->kqueue_fd);
		ReleaseExternalFD();
		errno = save_errno;
		elog(ERROR, "fcntl(F_SETFD) failed on kqueue descriptor: %m");
	}
	set->report_postmaster_not_running = false;
#elif defined(WAIT_USE_WIN32)

	/*
	 * To handle signals while waiting, we need to add a win32 specific event.
	 * We accounted for the additional event at the top of this routine. See
	 * port/win32/signal.c for more details.
	 *
	 * Note: pgwin32_signal_event should be first to ensure that it will be
	 * reported when multiple events are set.  We want to guarantee that
	 * pending signals are serviced.
	 */
	set->handles[0] = pgwin32_signal_event;
#endif

	return set;
}

/*
 * Create a physical WaitEventSet using the selected operating system
 * facility.
 */
WaitEventSet *
CreatePhysicalWaitEventSet(ResourceOwner resowner, int nevents)
{
	return CreateWaitEventSetImpl(resowner, NULL, nevents);
}

/*
 * Create a lightweight "logical" WaitEventSet that shares an underlying
 * physical WaitEventSet.
 *
 * The physical WaitEventSet should not be used directly if there are active
 * logical WaitEventSets, since they modify it as required during their
 * operations, making it difficult to know what state it's in.
 */
WaitEventSet *
CreateLogicalWaitEventSet(ResourceOwner resowner,
						  WaitEventSet *underlying_set,
						  int nevents)
{
	if (!underlying_set || wes_is_logical(underlying_set))
		elog(ERROR, "underlying physical WaitEventSet required");

	/*
	 * Ask for low-level inter-backend wakeups to be reported.  The logical
	 * WaitEventSet will process them itself, so that it doesn't have to tell
	 * the physical WaitEventSet about higher-level objects.
	 */
	AddWaitEventSetObject(underlying_set,
						  WL_TYPE_WAKEUP,
						  0,
						  WL_WAKEUP_RAW,
						  NULL);

	return CreateWaitEventSetImpl(resowner, underlying_set, nevents);
}

static WaitEventSet *backend_physical_wait_event_set;

/*
 * Create a logical WaitEventSet using the per-backend WaitEventSet as the
 * underlying implementation.  It is created on first usage.
 */
WaitEventSet *
CreateWaitEventSet(ResourceOwner resowner, int nevents)
{
	if (backend_physical_wait_event_set == NULL)
		backend_physical_wait_event_set = CreatePhysicalWaitEventSet(NULL, 1);

	return CreateLogicalWaitEventSet(resowner,
									 backend_physical_wait_event_set,
									 nevents);
}


void
ReserveWaitEventSetSpace(WaitEventSet *set, int nevents_space)
{
	char	   *data;
	size_t		sz = 0;
	dlist_head *new_id_table;
	uint64_t   *new_events_bitmap;
	WaitEventRegistration *new_events;

	if (set->nevents_space >= nevents_space)
		return;

	sz += MAXALIGN(wes_sizeof_id_table(nevents_space));
	sz += MAXALIGN(wes_sizeof_events_bitmap(nevents_space));
	sz += MAXALIGN(wes_sizeof_events(nevents_space));

	data = (char *) MemoryContextAllocZero(TopMemoryContext, sz);

	new_id_table = (dlist_head *) data;
	data += MAXALIGN(wes_sizeof_id_table(nevents_space));

	new_events_bitmap = (uint64_t *) data;
	data += MAXALIGN(wes_sizeof_events_bitmap(nevents_space));

	new_events = (WaitEventRegistration *) data;
	data += MAXALIGN(wes_sizeof_events(nevents_space));

	for (int i = 0; i < set->nevents_space; ++i)
	{
		WaitEventRegistration *src = &set->events[i];
		WaitEventRegistration *dst = &new_events[i];

		if (src->event.type != WL_TYPE_INVALID)
		{
			Assert(wes_is_used(set, src));

			/* Copy the WaitEvent definition. */
			dst->event = src->event;

			/* Move all bindings from src to dst. */
			if (wes_is_physical(set))
				wes_rebind_physical_registration(src, dst);
			else if (wes_logical_registration_is_bound(src))
				wes_rebind_logical_registration(src, dst);

			/* If src is in dirty queue, replace it with dst. */
			if (wes_is_logical(set) &&
				wes_logical_registration_is_dirty(src))
			{
				wes_logical_registration_clear_dirty(src);
				wes_logical_registration_set_dirty(dst);
			}

			/* Remove src from lookup tables. */
			wes_type_list_remove(set, src);
			wes_id_table_remove(set, src);
		}
		else
		{
			Assert(!wes_is_used(set, src));
		}
	}

	/* The lists have been cleaned out. */
	for (int i = 0; i < lengthof(set->type_list); ++i)
		Assert(dlist_is_empty(&set->type_list[i]));
	for (int i = 0; i < wes_lengthof_id_table(set->nevents_space); ++i)
		Assert(dlist_is_empty(&set->id_table[i]));

	/* Free previous expansion memory if necessary. */
	if (set->expansion_mem)
		pfree(set->expansion_mem);

	/* Switch the new memory into place. */
	set->expansion_mem = data;
	set->id_table = new_id_table;
	set->events_bitmap = new_events_bitmap;
	set->events = new_events;

	/* Rebuild the lists and bitmap. */
	for (int i = 0; i < set->nevents_space; ++i)
	{
		WaitEventRegistration *reg = &set->events[i];

		if (reg->event.type != WL_TYPE_INVALID)
		{
			wes_type_list_insert(set, reg);
			wes_id_table_insert(set, reg);
			wes_set_used(set, reg);
		}
	}
}

/*
 * Free a previously created WaitEventSet.
 *
 * Note: preferably, this shouldn't have to free any resources that could be
 * inherited across an exec().  If it did, we'd likely leak those resources in
 * many scenarios.  For the epoll case, we ensure that by setting EPOLL_CLOEXEC
 * when the FD is created.  For the Windows case, we assume that the handles
 * involved are non-inheritable.
 */
void
FreeWaitEventSet(WaitEventSet *set)
{
	if (set->owner)
	{
		ResourceOwnerForgetWaitEventSet(set->owner, set);
		set->owner = NULL;
	}

	/* Logical WaitEventSet: just unlink everything and free memory. */
	if (wes_is_logical(set))
	{
		WaitEventSet *phy_set = set->underlying_set;

		Assert(phy_set->physical.nevents_space_sum >= set->nevents_space);
		phy_set->physical.nevents_space_sum -= set->nevents_space;

		Assert(phy_set->physical.reference_count > 0);
		phy_set->physical.reference_count--;

		for (int i = 0; i < set->nevents_space; ++i)
			if (set->events[i].event.type != WL_TYPE_INVALID &&
				wes_logical_registration_is_bound(&set->events[i]))
				wes_unbind_logical_registration(&set->events[i]);

		if (set->expansion_mem)
			pfree(set->expansion_mem);

		pfree(set);
		return;
	}

	if (set->physical.reference_count > 0)
		elog(ERROR,
			 "cannot free physical WaitEventSet that has dependent logical WaitEventSet");

#if defined(WAIT_USE_EPOLL)
	close(set->epoll_fd);
	ReleaseExternalFD();
#elif defined(WAIT_USE_KQUEUE)
	close(set->kqueue_fd);
	ReleaseExternalFD();
#elif defined(WAIT_USE_WIN32)
	for (WaitEvent *cur_event = set->events;
		 cur_event < (set->events + set->nevents);
		 cur_event++)
	{
		if (cur_event->events & WL_LATCH_SET)
		{
			/* uses the latch's HANDLE */
		}
		else if (cur_event->events & WL_POSTMASTER_DEATH)
		{
			/* uses PostmasterHandle */
		}
		else
		{
			/* Clean up the event object we created for the socket */
			WSAEventSelect(cur_event->fd, NULL, 0);
			WSACloseEvent(set->handles[cur_event->pos + 1]);
		}
	}
#endif

	if (set->expansion_mem)
		pfree(set->expansion_mem);
	pfree(set);
}

/*
 * Free a previously created WaitEventSet in a child process after a fork().
 */
void
FreeWaitEventSetAfterFork(WaitEventSet *set)
{
#if defined(WAIT_USE_EPOLL)
	close(set->epoll_fd);
	ReleaseExternalFD();
#elif defined(WAIT_USE_KQUEUE)
	/* kqueues are not normally inherited by child processes */
	ReleaseExternalFD();
#endif

	pfree(set);
}

/*
 * Add a waitable object to a WaitEventSet.  Returns an index that can be used
 * to modify the event mask slightly more efficiently.
 */
WaitEventIndex
AddWaitEventSetObject(WaitEventSet *set,
					  WaitEventType type,
					  WaitEventId id,
					  uint32 events,
					  void *user_data)
{
	WaitEventRegistration *reg;

	if ((reg = wes_id_table_find(set, type, id)))
	{
		/* Silently modify if already present. */
		wes_modify_registration(set, reg, events);
		return reg->event.index;
	}

	wes_validate_add(type, id, events);

	reg = wes_find_free_registration(set);
	if (reg == NULL)
	{
		/*
		 * You can avoid allocation failure risk here by providing a
		 * sufficient size value at creation time, or calling
		 * ReserveWaitEventSetSpace() youself.
		 */
		ReserveWaitEventSetSpace(set, set->nevents_space * 2);
		reg = wes_find_free_registration(set);
	}
	wes_init_registration(set, reg);

	if (!wes_type_uses_wakeup(type))
	{
		if (wes_is_physical(set))
			wes_adjust_physical(set, reg, type, id, 0, events);
		else
			wes_logical_registration_set_dirty(reg);
	}

	reg->event.type = type;
	reg->event.id = id;
	reg->event.events = events;
	reg->event.user_data = user_data;
	reg->event.index = reg - set->events;

	wes_id_table_insert(set, reg);
	wes_type_list_insert(set, reg);
	wes_set_used(set, reg);

	return reg->event.index;
}

/*
 * Modify the event mask for an object in a WaitEventSet.  Returns false if
 * not found.
 */
bool
ModifyWaitEventSetObject(WaitEventSet *set,
						 WaitEventType type,
						 WaitEventId id,
						 WaitEventMask events)
{
	WaitEventRegistration *reg = wes_id_table_find(set, type, id);

	if (!reg)
		return false;

	wes_modify_registration(set, reg, events);
	return true;
}

/*
 * Modify the event mask for an object in a WaitEventSet using a
 * WaitEventIndex value returned by AddWaitEventSetObject() or
 * WaitEventSetWait().
 */
void
ModifyWaitEventSetIndex(WaitEventSet *set,
						WaitEventIndex index,
						WaitEventMask events)
{
	wes_modify_registration(set, wes_find_index(set, index), events);
}

/*
 * Remove a waitable object from a WaitEventSet.  Returns true if it was found
 * and has been removed, and false if it wasn't present.
 */
bool
DeleteWaitEventSetObject(WaitEventSet *set,
						 WaitEventType type,
						 WaitEventId id)
{
	WaitEventRegistration *reg = wes_id_table_find(set, type, id);

	if (!reg)
		return false;

	wes_delete_registration(set, reg);
	return true;
}

/*
 * Remove a waitable object from a WaitEventSet using an index returned when
 * adding it.
 */
void
DeleteWaitEventSetIndex(WaitEventSet *set, WaitEventIndex index)
{
	wes_delete_registration(set, wes_find_index(set, index));
}


/*
 * Delete all objects of a given type and return the number deleted.
 */
int
DeleteWaitEventSetObjects(WaitEventSet *set, WaitEventType type)
{
	dlist_head *type_list;
	int			count = 0;

	Assert(type > WL_TYPE_INVALID && type <= WL_TYPE_LAST);

	type_list = &set->type_list[type];
	while (!dlist_is_empty(type_list))
	{
		WaitEventRegistration *reg;

		reg = dlist_container(WaitEventRegistration,
							  type_list_node,
							  dlist_pop_head_node(type_list));
		wes_delete_registration(set, reg);
		count++;
	}

	return count;
}

#if defined(WAIT_USE_EPOLL)
static void
wes_adjust_physical(WaitEventSet *set,
					WaitEventRegistration *reg,
					WaitEventType type,
					WaitEventId id,
					WaitEventMask old_events,
					WaitEventMask new_events)
{
	struct epoll_event epoll_ev;
	int			action;
	int			fd;
	int			rc;

	if (old_events == 0)
		action = EPOLL_CTL_ADD;
	else if (new_events == 0)
		action = EPOLL_CTL_DEL;
	else
		action = EPOLL_CTL_MOD;

	/* pointer to our event, returned by epoll_wait */
	epoll_ev.data.ptr = reg;
	/* always wait for errors */
	epoll_ev.events = EPOLLERR | EPOLLHUP;

	/* prepare pollfd entry once */
	if (new_events == WL_WAKEUP_RAW ||
		new_events == WL_WAKEUP_PROCESSED)
	{
#ifdef WAIT_USE_SIGNALFD
		fd = signal_fd;
#endif
#ifdef WAIT_USE_SELF_PIPE
		fd = selfpipe_readfd;
#endif
		epoll_ev.events |= EPOLLIN;
	}
	else if (new_events == WL_POSTMASTER_DEATH)
	{
		fd = postmaster_alive_fds[POSTMASTER_FD_WATCH];
		epoll_ev.events |= EPOLLIN;
	}
	else
	{
		fd = id;
		Assert(fd != PGINVALID_SOCKET);
		Assert(new_events & (WL_SOCKET_READABLE |
							 WL_SOCKET_WRITEABLE |
							 WL_SOCKET_CLOSED));

		if (new_events & WL_SOCKET_READABLE)
			epoll_ev.events |= EPOLLIN;
		if (new_events & WL_SOCKET_WRITEABLE)
			epoll_ev.events |= EPOLLOUT;
		if (new_events & WL_SOCKET_CLOSED)
			epoll_ev.events |= EPOLLRDHUP;
	}

	/*
	 * Even though unused, we also pass epoll_ev as the data argument if
	 * EPOLL_CTL_DEL is passed as action.  There used to be an epoll bug
	 * requiring that, and actually it makes the code simpler...
	 */
	rc = epoll_ctl(set->epoll_fd, action, fd, &epoll_ev);

	if (rc < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("%s() failed: %m",
						"epoll_ctl")));
}
#endif

#if defined(WAIT_USE_POLL)
static void
WaitEventSetAdjust(WaitEventSet *set, WaitEventRegistration *reg,
				   uint32 old_events, uint32 new_events)
{
	struct pollfd *pollfd = &set->pollfds[reg->event.pos];

	pollfd->revents = 0;
	pollfd->fd = event->fd;

	/* prepare pollfd entry once */
	if (event->events == WL_LATCH_SET)
	{
		Assert(set->latch != NULL);
		pollfd->events = POLLIN;
	}
	else if (event->events == WL_POSTMASTER_DEATH)
	{
		pollfd->events = POLLIN;
	}
	else
	{
		Assert(event->events & (WL_SOCKET_READABLE |
								WL_SOCKET_WRITEABLE |
								WL_SOCKET_CLOSED));
		pollfd->events = 0;
		if (event->events & WL_SOCKET_READABLE)
			pollfd->events |= POLLIN;
		if (event->events & WL_SOCKET_WRITEABLE)
			pollfd->events |= POLLOUT;
#ifdef POLLRDHUP
		if (event->events & WL_SOCKET_CLOSED)
			pollfd->events |= POLLRDHUP;
#endif
	}

	Assert(event->fd != PGINVALID_SOCKET);
}
#endif

#if defined(WAIT_USE_KQUEUE)

/*
 * On most BSD family systems, the udata member of struct kevent is of type
 * void *, so we could directly convert to/from WaitEvent *.  Unfortunately,
 * NetBSD has it as intptr_t, so here we wallpaper over that difference with
 * an lvalue cast.
 */
#define AccessWaitEvent(k_ev) (*((WaitEvent **)(&(k_ev)->udata)))

static inline void
WaitEventAdjustKqueueAdd(struct kevent *k_ev, int filter, int action,
						 WaitEvent *event)
{
	k_ev->ident = event->id;
	k_ev->filter = filter;
	k_ev->flags = action;
	k_ev->fflags = 0;
	k_ev->data = 0;
	AccessWaitEvent(k_ev) = event;
}

static inline void
WaitEventAdjustKqueueAddPostmaster(struct kevent *k_ev, WaitEvent *event)
{
	/* For now postmaster death can only be added, not removed. */
	k_ev->ident = PostmasterPid;
	k_ev->filter = EVFILT_PROC;
	k_ev->flags = EV_ADD;
	k_ev->fflags = NOTE_EXIT;
	k_ev->data = 0;
	AccessWaitEvent(k_ev) = event;
}

static inline void
WaitEventAdjustKqueueAddLatch(struct kevent *k_ev, WaitEvent *event)
{
	/* For now latch can only be added, not removed. */
	k_ev->ident = SIGURG;
	k_ev->filter = EVFILT_SIGNAL;
	k_ev->flags = EV_ADD;
	k_ev->fflags = 0;
	k_ev->data = 0;
	AccessWaitEvent(k_ev) = event;
}

/*
 * old_events is the previous event mask, used to compute what has changed.
 */
static void
WaitEventSetAdjust(WaitEventSet *set,
				   WaitEventRegistration *reg,
				   WaitEventType type,
				   WaitEventId id,
				   uint32 old_events,
				   uint32 new_events)
{
	int			rc;
	struct kevent k_ev[2];
	int			count = 0;
	bool		new_filt_read = false;
	bool		old_filt_read = false;
	bool		new_filt_write = false;
	bool		old_filt_write = false;

	if (old_events == event->events)
		return;

	Assert(event->events != WL_WAKE || set->wake_event != NULL);
	Assert(event->events == WL_LATCH_SET ||
		   event->events == WL_POSTMASTER_DEATH ||
		   (event->events & (WL_SOCKET_READABLE |
							 WL_SOCKET_WRITEABLE |
							 WL_SOCKET_CLOSED)));

	if (event->events == WL_POSTMASTER_DEATH)
	{
		/*
		 * Unlike all the other implementations, we detect postmaster death
		 * using process notification instead of waiting on the postmaster
		 * alive pipe.
		 */
		WaitEventAdjustKqueueAddPostmaster(&k_ev[count++], event);
	}
	else if (event->events == WL_LATCH_SET)
	{
		/* We detect latch wakeup using a signal event. */
		WaitEventAdjustKqueueAddLatch(&k_ev[count++], event);
	}
	else
	{
		/*
		 * We need to compute the adds and deletes required to get from the
		 * old event mask to the new event mask, since kevent treats readable
		 * and writable as separate events.
		 */
		if (old_events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED))
			old_filt_read = true;
		if (event->events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED))
			new_filt_read = true;
		if (old_events & WL_SOCKET_WRITEABLE)
			old_filt_write = true;
		if (event->events & WL_SOCKET_WRITEABLE)
			new_filt_write = true;
		if (old_filt_read && !new_filt_read)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_READ, EV_DELETE,
									 event);
		else if (!old_filt_read && new_filt_read)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_READ, EV_ADD,
									 event);
		if (old_filt_write && !new_filt_write)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_WRITE, EV_DELETE,
									 event);
		else if (!old_filt_write && new_filt_write)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_WRITE, EV_ADD,
									 event);
	}

	/* For WL_SOCKET_READ -> WL_SOCKET_CLOSED, no change needed. */
	if (count == 0)
		return;

	Assert(count <= 2);

	rc = kevent(set->kqueue_fd, &k_ev[0], count, NULL, 0, NULL);

	/*
	 * When adding the postmaster's pid, we have to consider that it might
	 * already have exited and perhaps even been replaced by another process
	 * with the same pid.  If so, we have to defer reporting this as an event
	 * until the next call.
	 */

	if (rc < 0)
	{
		if (event->events == WL_POSTMASTER_DEATH &&
			(errno == ESRCH || errno == EACCES))
			set->report_postmaster_not_running = true;
		else
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"kevent")));
	}
	else if (event->events == WL_POSTMASTER_DEATH &&
			 PostmasterPid != getppid() &&
			 !PostmasterIsAlive())
	{
		/*
		 * The extra PostmasterIsAliveInternal() check prevents false alarms
		 * on systems that give a different value for getppid() while being
		 * traced by a debugger.
		 */
		set->report_postmaster_not_running = true;
	}
}

#endif

#if defined(WAIT_USE_WIN32)
StaticAssertDecl(WSA_INVALID_EVENT == NULL, "");

static void
WaitEventAdjustWin32(WaitEventSet *set, WaitEvent *event)
{
	HANDLE	   *handle = &set->handles[event->pos + 1];

	if (event->events == WL_LATCH_SET)
	{
		Assert(set->latch != NULL);
		*handle = set->latch->event;
	}
	else if (event->events == WL_POSTMASTER_DEATH)
	{
		*handle = PostmasterHandle;
	}
	else
	{
		int			flags = FD_CLOSE;	/* always check for errors/EOF */

		if (event->events & WL_SOCKET_READABLE)
			flags |= FD_READ;
		if (event->events & WL_SOCKET_WRITEABLE)
			flags |= FD_WRITE;
		if (event->events & WL_SOCKET_CONNECTED)
			flags |= FD_CONNECT;
		if (event->events & WL_SOCKET_ACCEPT)
			flags |= FD_ACCEPT;

		if (*handle == WSA_INVALID_EVENT)
		{
			*handle = WSACreateEvent();
			if (*handle == WSA_INVALID_EVENT)
				elog(ERROR, "failed to create event for socket: error code %d",
					 WSAGetLastError());
		}
		if (WSAEventSelect(event->fd, *handle, flags) != 0)
			elog(ERROR, "failed to set up event for socket: error code %d",
				 WSAGetLastError());

		Assert(event->fd != PGINVALID_SOCKET);
	}
}
#endif

static int
wes_wait_logical(WaitEventSet *log_set,
				 int cur_timeout,
				 WaitEvent *occurred_events,
				 int nevents,
				 uint32 wait_event_info)
{
	bool		got_wakeup = false;
	int			phy_count;
	int			log_count;

	Assert(wes_is_logical(log_set));

	if (wes_has_dirty_registrations(log_set))
		wes_resolve_dirty_registrations(log_set);

	Assert(!wes_has_dirty_registrations(log_set));

#ifdef USE_ASSERT_CHECKING
	/* Sanity checks. */
	for (int i = 0; i < log_set->nevents_space; ++i)
	{
		WaitEventRegistration *log_reg = &log_set->events[i];
		WaitEventRegistration *phy_reg = log_reg->logical.binding;

		/* Usage bitmap and type are in sync. */
		if (!wes_is_used(log_set, log_reg))
		{
			Assert(log_reg->event.type == WL_TYPE_INVALID);
			Assert(phy_reg == NULL);
			continue;
		}
		Assert(log_reg->event.type != WL_TYPE_INVALID);

		/* Some events don't need binding because they share WL_WAKEUP_RAW. */
		if (wes_type_uses_wakeup(log_reg->event.type))
		{
			Assert(phy_reg == NULL);
			continue;
		}

		/* Everything else has a matching underlying registration. */
		Assert(phy_reg);
		Assert(phy_reg->event.type == log_reg->event.type);
		Assert(phy_reg->event.id == log_reg->event.id);
		Assert(phy_reg->event.events == log_reg->event.events);
	}
#endif

	/* Wait on underlying set. */
	phy_count = WaitEventSetWait(log_set->underlying_set,
								 cur_timeout,
								 occurred_events,
								 nevents,
								 wait_event_info);

	/* Timed out or interrupted? */
	if (phy_count <= 0)
		return phy_count;

	/* Filter and translate underlying events. */
	log_count = 0;
	for (int i = 0; i < phy_count; ++i)
	{
		WaitEventRegistration *phy_reg;
		WaitEventRegistration *log_reg;
		WaitEventMask events;

		phy_reg = &log_set->underlying_set->events[occurred_events[i].index];
		if (phy_reg->event.type == WL_TYPE_WAKEUP)
		{
			/* Need to poll latches and condition variables. */
			got_wakeup = true;
			continue;
		}

		log_reg = wes_find_logical_registration_for_set(phy_reg, log_set);
		if (!log_reg)
		{
			/*
			 * Lazily suppress events that are not interesting to this logical
			 * set.  They'll be re-added later if required.  (If we filter
			 * everything out here, we'll return a count of 0 and be called
			 * again with an updated timeout.)
			 */
			wes_delete_registration(log_set->underlying_set, phy_reg);
			continue;
		}

		/*
		 * We want the pos and user_data from the logical event, but the
		 * events from the physical event that fired.
		 */
		events = occurred_events[i].events;
		occurred_events[log_count] = log_reg->event;
		occurred_events[log_count].events = events;
		log_count++;
	}

	if (got_wakeup)
		log_count += wes_process_wakeup(log_set,
										occurred_events + log_count,
										nevents - log_count);

	return log_count;
}

/*
 * Wait for events added to the set to happen, or until the timeout is
 * reached.  At most nevents occurred events are returned.
 *
 * If timeout = -1, block until an event occurs; if 0, check sockets for
 * readiness, but don't block; if > 0, block for at most timeout milliseconds.
 *
 * Returns the number of events occurred, or 0 if the timeout was reached.
 *
 * Returned events will have the fd, pos, user_data fields set to the
 * values associated with the registered event.
 */
int
WaitEventSetWait(WaitEventSet *set,
				 int timeout,
				 WaitEvent *occurred_events,
				 int nevents,
				 uint32 wait_event_info)
{
	int			returned_events = 0;
	instr_time	start_time;
	instr_time	cur_time;
	int			cur_timeout = -1;

	Assert(nevents > 0);

	/*
	 * Initialize timeout if requested.  We must record the current time so
	 * that we can determine the remaining timeout if interrupted.
	 */
	if (timeout >= 0)
	{
		INSTR_TIME_SET_CURRENT(start_time);
		Assert(timeout >= 0 && timeout <= INT_MAX);
		cur_timeout = timeout;
	}
	else
		INSTR_TIME_SET_ZERO(start_time);

	pgstat_report_wait_start(wait_event_info);

#ifndef WIN32
	waiting = true;
#else
	/* Ensure that signals are serviced even if latch is already set */
	pgwin32_dispatch_queued_signals();
#endif

	/* Prepare to wait on latches. */
	if (wes_has_type(set, WL_TYPE_LATCH))
		returned_events += wes_begin_wait_latches(set, occurred_events, nevents);

	/*
	 * If we already have immediate events, set the timeout to zero so that we
	 * can poll the kernel for more, if the called asked for more events.
	 */
	if (returned_events > 0)
		cur_timeout = 0;

	while (returned_events < nevents)
	{
		int			rc;

		/*
		 * Wait for events using the readiness primitive chosen at the top of
		 * this file. If -1 is returned, a timeout has occurred, if 0 we have
		 * to retry, everything >= 1 is the number of returned events.
		 */
		if (wes_is_logical(set))
			rc = wes_wait_logical(set,
								  cur_timeout,
								  occurred_events + returned_events,
								  nevents - returned_events,
								  wait_event_info);
		else
			rc = wes_wait_physical(set,
								   cur_timeout,
								   occurred_events + returned_events,
								   nevents - returned_events);

		if (rc == -1)
			break;				/* timeout occurred */
		else
			returned_events += rc;

		/* If we're not done, update cur_timeout for next iteration */
		if (returned_events == 0 && timeout >= 0)
		{
			INSTR_TIME_SET_CURRENT(cur_time);
			INSTR_TIME_SUBTRACT(cur_time, start_time);
			cur_timeout = timeout - (long) INSTR_TIME_GET_MILLISEC(cur_time);
			if (cur_timeout <= 0)
				break;
		}
	}

	if (wes_has_type(set, WL_TYPE_LATCH))
		wes_end_wait_latches(set);

#ifndef WIN32
	waiting = false;
#endif

	pgstat_report_wait_end();

	return returned_events;
}



#if defined(WAIT_USE_EPOLL)

/*
 * Wait using linux's epoll_wait(2).
 *
 * This is the preferable wait method, as several readiness notifications are
 * delivered, without having to iterate through all of set->events. The return
 * epoll_event struct contain a pointer to our events, making association
 * easy.
 */
static inline int
wes_wait_physical(WaitEventSet *set, int cur_timeout,
				  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	int			rc;
	WaitEvent  *cur_event;
	struct epoll_event *cur_epoll_event;

	/* Sleep */
	rc = epoll_wait(set->epoll_fd, set->epoll_ret_events,
					Min(nevents, set->nevents_space), cur_timeout);

	/* Check return code */
	if (rc < 0)
	{
		/* EINTR is okay, otherwise complain */
		if (errno != EINTR)
		{
			waiting = false;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"epoll_wait")));
		}
		return 0;
	}
	else if (rc == 0)
	{
		/* timeout exceeded */
		return -1;
	}

	/*
	 * At least one event occurred, iterate over the returned epoll events
	 * until they're either all processed, or we've returned all the events
	 * the caller desired.
	 */
	for (cur_epoll_event = set->epoll_ret_events;
		 cur_epoll_event < (set->epoll_ret_events + rc) &&
		 returned_events < nevents;
		 cur_epoll_event++)
	{
		WaitEventRegistration *reg;

		reg = (WaitEventRegistration *) cur_epoll_event->data.ptr;
		cur_event = &reg->event;

		*occurred_events = *cur_event;
		occurred_events->events = 0;

		if (cur_event->events & WL_WAKEUP_MASK &&
			cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP))
		{
			/* Drain the signalfd. */
			drain();

			if (cur_event->events & WL_WAKEUP_PROCESSED)
			{
				int			generated = wes_process_wakeup(set,
														   occurred_events,
														   nevents - returned_events);

				occurred_events += generated;
				returned_events += generated;
			}
			else
			{
				occurred_events->events = WL_WAKEUP_RAW;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH &&
				 cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP))
		{
			/*
			 * We expect an EPOLLHUP when the remote end is closed, but
			 * because we don't expect the pipe to become readable or to have
			 * any errors either, treat those cases as postmaster death, too.
			 *
			 * Be paranoid about a spurious event signaling the postmaster as
			 * being dead.  There have been reports about that happening with
			 * older primitives (select(2) to be specific), and a spurious
			 * WL_POSTMASTER_DEATH event would be painful. Re-checking doesn't
			 * cost much.
			 */
			if (!PostmasterIsAliveInternal())
			{
				if (set->exit_on_postmaster_death)
					proc_exit(1);
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_POSTMASTER_DEATH;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events & (WL_SOCKET_READABLE |
									  WL_SOCKET_WRITEABLE |
									  WL_SOCKET_CLOSED))
		{
			Assert(cur_event->fd != PGINVALID_SOCKET);

			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP)))
			{
				/* data available in socket, or EOF */
				occurred_events->events |= WL_SOCKET_READABLE;
			}

			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(cur_epoll_event->events & (EPOLLOUT | EPOLLERR | EPOLLHUP)))
			{
				/* writable, or EOF */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}

			if ((cur_event->events & WL_SOCKET_CLOSED) &&
				(cur_epoll_event->events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)))
			{
				/* remote peer shut down, or error */
				occurred_events->events |= WL_SOCKET_CLOSED;
			}

			if (occurred_events->events != 0)
			{
				occurred_events->fd = cur_event->fd;
				occurred_events++;
				returned_events++;
			}
		}
	}

	return returned_events;
}

#elif defined(WAIT_USE_KQUEUE)

/*
 * Wait using kevent(2) on BSD-family systems and macOS.
 *
 * For now this mirrors the epoll code, but in future it could modify the fd
 * set in the same call to kevent as it uses for waiting instead of doing that
 * with separate system calls.
 */
static int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	int			rc;
	WaitEvent  *cur_event;
	struct kevent *cur_kqueue_event;
	struct timespec timeout;
	struct timespec *timeout_p;

	if (cur_timeout < 0)
		timeout_p = NULL;
	else
	{
		timeout.tv_sec = cur_timeout / 1000;
		timeout.tv_nsec = (cur_timeout % 1000) * 1000000;
		timeout_p = &timeout;
	}

	/*
	 * Report postmaster events discovered by WaitEventAdjustKqueue() or an
	 * earlier call to WaitEventSetWait().
	 */
	if (unlikely(set->report_postmaster_not_running))
	{
		if (set->exit_on_postmaster_death)
			proc_exit(1);
		occurred_events->fd = PGINVALID_SOCKET;
		occurred_events->events = WL_POSTMASTER_DEATH;
		return 1;
	}

	/* Sleep */
	rc = kevent(set->kqueue_fd, NULL, 0,
				set->kqueue_ret_events,
				Min(nevents, set->nevents_space),
				timeout_p);

	/* Check return code */
	if (rc < 0)
	{
		/* EINTR is okay, otherwise complain */
		if (errno != EINTR)
		{
			waiting = false;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"kevent")));
		}
		return 0;
	}
	else if (rc == 0)
	{
		/* timeout exceeded */
		return -1;
	}

	/*
	 * At least one event occurred, iterate over the returned kqueue events
	 * until they're either all processed, or we've returned all the events
	 * the caller desired.
	 */
	for (cur_kqueue_event = set->kqueue_ret_events;
		 cur_kqueue_event < (set->kqueue_ret_events + rc) &&
		 returned_events < nevents;
		 cur_kqueue_event++)
	{
		/* kevent's udata points to the associated WaitEvent */
		cur_event = AccessWaitEvent(cur_kqueue_event);

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET &&
			cur_kqueue_event->filter == EVFILT_SIGNAL)
		{
			if (set->latch && set->latch->maybe_sleeping && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH &&
				 cur_kqueue_event->filter == EVFILT_PROC &&
				 (cur_kqueue_event->fflags & NOTE_EXIT) != 0)
		{
			/*
			 * The kernel will tell this kqueue object only once about the
			 * exit of the postmaster, so let's remember that for next time so
			 * that we provide level-triggered semantics.
			 */
			set->report_postmaster_not_running = true;

			if (set->exit_on_postmaster_death)
				proc_exit(1);
			occurred_events->fd = PGINVALID_SOCKET;
			occurred_events->events = WL_POSTMASTER_DEATH;
			occurred_events++;
			returned_events++;
		}
		else if (cur_event->events & (WL_SOCKET_READABLE |
									  WL_SOCKET_WRITEABLE |
									  WL_SOCKET_CLOSED))
		{
			Assert(cur_event->fd >= 0);

			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(cur_kqueue_event->filter == EVFILT_READ))
			{
				/* readable, or EOF */
				occurred_events->events |= WL_SOCKET_READABLE;
			}

			if ((cur_event->events & WL_SOCKET_CLOSED) &&
				(cur_kqueue_event->filter == EVFILT_READ) &&
				(cur_kqueue_event->flags & EV_EOF))
			{
				/* the remote peer has shut down */
				occurred_events->events |= WL_SOCKET_CLOSED;
			}

			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(cur_kqueue_event->filter == EVFILT_WRITE))
			{
				/* writable, or EOF */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}

			if (occurred_events->events != 0)
			{
				occurred_events->fd = cur_event->fd;
				occurred_events++;
				returned_events++;
			}
		}
	}

	return returned_events;
}

#elif defined(WAIT_USE_POLL)

/*
 * Wait using poll(2).
 *
 * This allows to receive readiness notifications for several events at once,
 * but requires iterating through all of set->pollfds.
 */
static inline int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	int			rc;
	WaitEvent  *cur_event;
	struct pollfd *cur_pollfd;

	/* Sleep */
	rc = poll(set->pollfds, set->nevents, cur_timeout);

	/* Check return code */
	if (rc < 0)
	{
		/* EINTR is okay, otherwise complain */
		if (errno != EINTR)
		{
			waiting = false;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"poll")));
		}
		return 0;
	}
	else if (rc == 0)
	{
		/* timeout exceeded */
		return -1;
	}

	for (cur_event = set->events, cur_pollfd = set->pollfds;
		 cur_event < (set->events + set->nevents) &&
		 returned_events < nevents;
		 cur_event++, cur_pollfd++)
	{
		/* no activity on this FD, skip */
		if (cur_pollfd->revents == 0)
			continue;

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET &&
			(cur_pollfd->revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)))
		{
			/* There's data in the self-pipe, clear it. */
			drain();

			if (set->latch && set->latch->maybe_sleeping && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH &&
				 (cur_pollfd->revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)))
		{
			/*
			 * We expect a POLLHUP when the remote end is closed, but because
			 * we don't expect the pipe to become readable or to have any
			 * errors either, treat those cases as postmaster death, too.
			 *
			 * Be paranoid about a spurious event signaling the postmaster as
			 * being dead.  There have been reports about that happening with
			 * older primitives (select(2) to be specific), and a spurious
			 * WL_POSTMASTER_DEATH event would be painful.  Re-checking
			 * doesn't cost much.
			 */
			if (!PostmasterIsAliveInternal())
			{
				if (set->exit_on_postmaster_death)
					proc_exit(1);
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_POSTMASTER_DEATH;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events & (WL_SOCKET_READABLE |
									  WL_SOCKET_WRITEABLE |
									  WL_SOCKET_CLOSED))
		{
			int			errflags = POLLHUP | POLLERR | POLLNVAL;

			Assert(cur_event->fd >= PGINVALID_SOCKET);

			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(cur_pollfd->revents & (POLLIN | errflags)))
			{
				/* data available in socket, or EOF */
				occurred_events->events |= WL_SOCKET_READABLE;
			}

			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(cur_pollfd->revents & (POLLOUT | errflags)))
			{
				/* writeable, or EOF */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}

#ifdef POLLRDHUP
			if ((cur_event->events & WL_SOCKET_CLOSED) &&
				(cur_pollfd->revents & (POLLRDHUP | errflags)))
			{
				/* remote peer closed, or error */
				occurred_events->events |= WL_SOCKET_CLOSED;
			}
#endif

			if (occurred_events->events != 0)
			{
				occurred_events->fd = cur_event->fd;
				occurred_events++;
				returned_events++;
			}
		}
	}
	return returned_events;
}

#elif defined(WAIT_USE_WIN32)

/*
 * Wait using Windows' WaitForMultipleObjects().  Each call only "consumes" one
 * event, so we keep calling until we've filled up our output buffer to match
 * the behavior of the other implementations.
 *
 * https://blogs.msdn.microsoft.com/oldnewthing/20150409-00/?p=44273
 */
static inline int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	DWORD		rc;
	WaitEvent  *cur_event;

	/* Reset any wait events that need it */
	for (cur_event = set->events;
		 cur_event < (set->events + set->nevents);
		 cur_event++)
	{
		if (cur_event->reset)
		{
			WaitEventAdjustWin32(set, cur_event);
			cur_event->reset = false;
		}

		/*
		 * We associate the socket with a new event handle for each
		 * WaitEventSet.  FD_CLOSE is only generated once if the other end
		 * closes gracefully.  Therefore we might miss the FD_CLOSE
		 * notification, if it was delivered to another event after we stopped
		 * waiting for it.  Close that race by peeking for EOF after setting
		 * up this handle to receive notifications, and before entering the
		 * sleep.
		 *
		 * XXX If we had one event handle for the lifetime of a socket, we
		 * wouldn't need this.
		 */
		if (cur_event->events & WL_SOCKET_READABLE)
		{
			char		c;
			WSABUF		buf;
			DWORD		received;
			DWORD		flags;

			buf.buf = &c;
			buf.len = 1;
			flags = MSG_PEEK;
			if (WSARecv(cur_event->fd, &buf, 1, &received, &flags, NULL, NULL) == 0)
			{
				occurred_events->pos = cur_event->pos;
				occurred_events->user_data = cur_event->user_data;
				occurred_events->events = WL_SOCKET_READABLE;
				occurred_events->fd = cur_event->fd;
				return 1;
			}
		}

		/*
		 * Windows does not guarantee to log an FD_WRITE network event
		 * indicating that more data can be sent unless the previous send()
		 * failed with WSAEWOULDBLOCK.  While our caller might well have made
		 * such a call, we cannot assume that here.  Therefore, if waiting for
		 * write-ready, force the issue by doing a dummy send().  If the dummy
		 * send() succeeds, assume that the socket is in fact write-ready, and
		 * return immediately.  Also, if it fails with something other than
		 * WSAEWOULDBLOCK, return a write-ready indication to let our caller
		 * deal with the error condition.
		 */
		if (cur_event->events & WL_SOCKET_WRITEABLE)
		{
			char		c;
			WSABUF		buf;
			DWORD		sent;
			int			r;

			buf.buf = &c;
			buf.len = 0;

			r = WSASend(cur_event->fd, &buf, 1, &sent, 0, NULL, NULL);
			if (r == 0 || WSAGetLastError() != WSAEWOULDBLOCK)
			{
				occurred_events->pos = cur_event->pos;
				occurred_events->user_data = cur_event->user_data;
				occurred_events->events = WL_SOCKET_WRITEABLE;
				occurred_events->fd = cur_event->fd;
				return 1;
			}
		}
	}

	/*
	 * Sleep.
	 *
	 * Need to wait for ->nevents + 1, because signal handle is in [0].
	 */
	rc = WaitForMultipleObjects(set->nevents + 1, set->handles, FALSE,
								cur_timeout);

	/* Check return code */
	if (rc == WAIT_FAILED)
		elog(ERROR, "WaitForMultipleObjects() failed: error code %lu",
			 GetLastError());
	else if (rc == WAIT_TIMEOUT)
	{
		/* timeout exceeded */
		return -1;
	}

	if (rc == WAIT_OBJECT_0)
	{
		/* Service newly-arrived signals */
		pgwin32_dispatch_queued_signals();
		return 0;				/* retry */
	}

	/*
	 * With an offset of one, due to the always present pgwin32_signal_event,
	 * the handle offset directly corresponds to a wait event.
	 */
	cur_event = (WaitEvent *) &set->events[rc - WAIT_OBJECT_0 - 1];

	for (;;)
	{
		int			next_pos;
		int			count;

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET)
		{
			/*
			 * We cannot use set->latch->event to reset the fired event if we
			 * aren't waiting on this latch now.
			 */
			if (!ResetEvent(set->handles[cur_event->pos + 1]))
				elog(ERROR, "ResetEvent failed: error code %lu", GetLastError());

			if (set->latch && set->latch->maybe_sleeping && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH)
		{
			/*
			 * Postmaster apparently died.  Since the consequences of falsely
			 * returning WL_POSTMASTER_DEATH could be pretty unpleasant, we
			 * take the trouble to positively verify this with
			 * PostmasterIsAlive(), even though there is no known reason to
			 * think that the event could be falsely set on Windows.
			 */
			if (!PostmasterIsAliveInternal())
			{
				if (set->exit_on_postmaster_death)
					proc_exit(1);
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_POSTMASTER_DEATH;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events & WL_SOCKET_MASK)
		{
			WSANETWORKEVENTS resEvents;
			HANDLE		handle = set->handles[cur_event->pos + 1];

			Assert(cur_event->fd);

			occurred_events->fd = cur_event->fd;

			ZeroMemory(&resEvents, sizeof(resEvents));
			if (WSAEnumNetworkEvents(cur_event->fd, handle, &resEvents) != 0)
				elog(ERROR, "failed to enumerate network events: error code %d",
					 WSAGetLastError());
			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(resEvents.lNetworkEvents & FD_READ))
			{
				/* data available in socket */
				occurred_events->events |= WL_SOCKET_READABLE;

				/*------
				 * WaitForMultipleObjects doesn't guarantee that a read event
				 * will be returned if the latch is set at the same time.  Even
				 * if it did, the caller might drop that event expecting it to
				 * reoccur on next call.  So, we must force the event to be
				 * reset if this WaitEventSet is used again in order to avoid
				 * an indefinite hang.
				 *
				 * Refer
				 * https://msdn.microsoft.com/en-us/library/windows/desktop/ms741576(v=vs.85).aspx
				 * for the behavior of socket events.
				 *------
				 */
				cur_event->reset = true;
			}
			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(resEvents.lNetworkEvents & FD_WRITE))
			{
				/* writeable */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}
			if ((cur_event->events & WL_SOCKET_CONNECTED) &&
				(resEvents.lNetworkEvents & FD_CONNECT))
			{
				/* connected */
				occurred_events->events |= WL_SOCKET_CONNECTED;
			}
			if ((cur_event->events & WL_SOCKET_ACCEPT) &&
				(resEvents.lNetworkEvents & FD_ACCEPT))
			{
				/* incoming connection could be accepted */
				occurred_events->events |= WL_SOCKET_ACCEPT;
			}
			if (resEvents.lNetworkEvents & FD_CLOSE)
			{
				/* EOF/error, so signal all caller-requested socket flags */
				occurred_events->events |= (cur_event->events & WL_SOCKET_MASK);
			}

			if (occurred_events->events != 0)
			{
				occurred_events++;
				returned_events++;
			}
		}

		/* Is the output buffer full? */
		if (returned_events == nevents)
			break;

		/* Have we run out of possible events? */
		next_pos = cur_event->pos + 1;
		if (next_pos == set->nevents)
			break;

		/*
		 * Poll the rest of the event handles in the array starting at
		 * next_pos being careful to skip over the initial signal handle too.
		 * This time we use a zero timeout.
		 */
		count = set->nevents - next_pos;
		rc = WaitForMultipleObjects(count,
									set->handles + 1 + next_pos,
									false,
									0);

		/*
		 * We don't distinguish between errors and WAIT_TIMEOUT here because
		 * we already have events to report.
		 */
		if (rc < WAIT_OBJECT_0 || rc >= WAIT_OBJECT_0 + count)
			break;

		/* We have another event to decode. */
		cur_event = &set->events[next_pos + (rc - WAIT_OBJECT_0)];
	}

	return returned_events;
}
#endif

/*
 * Return whether the current build options can report WL_SOCKET_CLOSED.
 */
bool
WaitEventSetCanReportClosed(void)
{
#if (defined(WAIT_USE_POLL) && defined(POLLRDHUP)) || \
	defined(WAIT_USE_EPOLL) || \
	defined(WAIT_USE_KQUEUE)
	return true;
#else
	return false;
#endif
}

/*
 * Get the number of wait events registered in a given WaitEventSet.
 */
int
GetNumRegisteredWaitEvents(WaitEventSet *set)
{

	return wes_count_used(set);
}

#if defined(WAIT_USE_SELF_PIPE)

/*
 * SetLatch uses SIGURG to wake up the process waiting on the latch.
 *
 * Wake up WaitLatch, if we're waiting.
 */
static void
latch_sigurg_handler(SIGNAL_ARGS)
{
	if (waiting)
		sendSelfPipeByte();
}

/* Send one byte to the self-pipe, to wake up WaitLatch */
static void
sendSelfPipeByte(void)
{
	ssize_t		rc;
	char		dummy = 0;

retry:
	rc = write(selfpipe_writefd, &dummy, 1);
	if (rc < 0)
	{
		/* If interrupted by signal, just retry */
		if (errno == EINTR)
			goto retry;

		/*
		 * If the pipe is full, we don't need to retry, the data that's there
		 * already is enough to wake up WaitLatch.
		 */
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return;

		/*
		 * Oops, the write() failed for some other reason. We might be in a
		 * signal handler, so it's not safe to elog(). We have no choice but
		 * silently ignore the error.
		 */
		return;
	}
}

#endif

#if defined(WAIT_USE_SELF_PIPE) || defined(WAIT_USE_SIGNALFD)

/*
 * Read all available data from self-pipe or signalfd.
 *
 * Note: this is only called when waiting = true.  If it fails and doesn't
 * return, it must reset that flag first (though ideally, this will never
 * happen).
 */
static void
drain(void)
{
	char		buf[1024];
	ssize_t		rc;
	int			fd;

#ifdef WAIT_USE_SELF_PIPE
	fd = selfpipe_readfd;
#else
	fd = signal_fd;
#endif

	for (;;)
	{
		rc = read(fd, buf, sizeof(buf));
		if (rc < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;			/* the descriptor is empty */
			else if (errno == EINTR)
				continue;		/* retry */
			else
			{
				waiting = false;
#ifdef WAIT_USE_SELF_PIPE
				elog(ERROR, "read() on self-pipe failed: %m");
#else
				elog(ERROR, "read() on signalfd failed: %m");
#endif
			}
		}
		else if (rc == 0)
		{
			waiting = false;
#ifdef WAIT_USE_SELF_PIPE
			elog(ERROR, "unexpected EOF on self-pipe");
#else
			elog(ERROR, "unexpected EOF on signalfd");
#endif
		}
		else if (rc < sizeof(buf))
		{
			/* we successfully drained the pipe; no need to read() again */
			break;
		}
		/* else buffer wasn't big enough, so read again */
	}
}

#endif

static void
ResOwnerReleaseWaitEventSet(Datum res)
{
	WaitEventSet *set = (WaitEventSet *) DatumGetPointer(res);

	Assert(set->owner != NULL);
	set->owner = NULL;
	FreeWaitEventSet(set);
}

#ifndef WIN32
/*
 * Wake up my process if it's currently sleeping in WaitEventSetWaitBlock()
 *
 * NB: be sure to save and restore errno around it.  (That's standard practice
 * in most signal handlers, of course, but we used to omit it in handlers that
 * only set a flag.) XXX
 *
 * NB: this function is called from critical sections and signal handlers so
 * throwing an error is not a good idea.
 *
 * On Windows, Latch uses SetEvent directly and this is not used.
 */
void
WakeupMyProc(void)
{
#if defined(WAIT_USE_SELF_PIPE)
	if (waiting)
		sendSelfPipeByte();
#else
	if (waiting)
		kill(MyProcPid, SIGURG);
#endif
}

/* Similar to WakeupMyProc, but wake up another process */
void
WakeupOtherProc(int pid)
{
	kill(pid, SIGURG);
}
#endif

WaitEventIndex
AddWaitEventSetLatch(WaitEventSet *set, struct Latch *latch)
{
	return AddWaitEventSetObject(set,
								 WL_TYPE_LATCH, (WaitEventId) latch,
								 WL_LATCH_SET,
								 NULL);
}

bool
DeleteWaitEventSetLatch(WaitEventSet *set, struct Latch *latch)
{
	return DeleteWaitEventSetObject(set, WL_TYPE_LATCH, (WaitEventId) latch);
}

int
DeleteWaitEventSetLatches(WaitEventSet *set)
{
	return DeleteWaitEventSetObjects(set, WL_TYPE_LATCH);
}

WaitEventIndex
AddWaitEventSetSocket(WaitEventSet *set,
					  pgsocket socket,
					  WaitEventMask events,
					  void *user_data)
{
	return AddWaitEventSetObject(set,
								 WL_TYPE_SOCKET,
								 socket,
								 events,
								 user_data);
}

bool
ModifyWaitEventSetSocket(WaitEventSet *set, pgsocket socket, WaitEventMask events)
{
	return ModifyWaitEventSetObject(set, WL_TYPE_SOCKET, socket, events);
}

WaitEventIndex
AddWaitEventSetPostmaster(WaitEventSet *set, WaitEventMask events)
{
	return AddWaitEventSetObject(set, WL_TYPE_POSTMASTER, 0, events, NULL);
}

void
ModifyWaitEventSetPostmaster(WaitEventSet *set, WaitEventMask events)
{
	ModifyWaitEventSetObject(set, WL_TYPE_POSTMASTER, 0, events);
}
