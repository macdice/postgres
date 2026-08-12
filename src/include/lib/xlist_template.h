/*-------------------------------------------------------------------------
 *
 * xlist_template.h
 *		Template for specialized linked lists implementations.
 *
 * This is a template that must be parameterized with macros before inclusion.
 * If the declarations and definitions are being split between headers, then
 * one of the following should be used:
 *
 *		XLIST_DECLARE_ONLY:	define types and declare functions only
 *		XLIST_DEFINE_ONLY:	define functions only
 *
 * Alternatively, both can be generated at the same time with:
 *
 * 		XLIST_DECLARE:		define types only
 * 		XLIST_DEFINE:		define functions (may be deferred to second inclusion)
 *
 * It is necessary to include this template twice, with XLIST_DEFINE defined
 * the second time if using XLIST_OBJECT_T to break a circularity.
 *
 * The main macros are:
 *
 *		XLIST_PREFIX:		replaces XLIST in type and function names (required)
 *
 *		XLIST_SLIST:		singly-linked list (one of these is required)
 * 		XLIST_DLIST:		doubly-linked list
 *
 * 		XLIST_COUNTED:		enable efficient _count() (default: uint32_t)
 * 		XLIST_TAILED:		enable efficient _push_tail() for SLIST
 *		XLIST_LINEAR:		enable inefficient O(n) functions for SLIST
 *
 * The pointers between nodes have the following representation options:
 *
 * 		XLIST_PTR:			nodes linked by plain pointers
 *		XLIST_PTRDIFF:		nodes linked relative pointers
 *		XLIST_INDEX:		nodes linked by array index
 *
 * XLIST_LINK_INDEX adds an "array" argument to most functions, and also
 * reveals a set of function XLIST_*_index() that can optionally be used
 * instead of the node-based functions.
 *
 *										PTR		PTRDIFF	INDEX
 *		API requires extra argument		no		no		yes
 *		Needs XLIST_OBJECT_T			no		no		yes
 *		Optional index-based API		no		no		yes
 *		Relocatable						no		yes		yes
 *		Branch-free	(XLIST_EMPTY_SELF)	yes		yes		no
 *		Zero-initialization				yes 	yes		no
 *		Node size						biggest	medium?	smallest?
 *
 * XLIST_PTRDIFF is a straighforward replacement for pointers, but XLIST_INDEX
 * brings many branches into the code, gated on XLIST_EMPTY_NIL.
 *
 * The type used for indexes or relative pointers can be controlled with:
 *
 *		XLIST_LINK_T:		override link type (not for XLIST_PTR)
 *		XLIST_COUNT_T:		override count type (default: uint32_t)
 *		XLIST_NIL:			override NIL value (for XLIST_INDEX)
 *
 * Care must be taken not to overflow the link type.  In practice this means
 * that container objects must be in an array of known maximal size even when
 * using XLIST_PTRDIFF, or similarly constrained.
 *
 *		XLIST_OBJECT_T:		provide containing-option functions
 *		XLIST_OBJECT_MEMBER
 *
 * Specializing for object type reveals the XLIST_*_object() family of
 * functions, an alternative to the node-based API.
 *
 * Portions Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/lib/xlist.h
 *-------------------------------------------------------------------------
 */

/*
 * This is not a normal include guard!  The template may be included multiple
 * times, and sometimes twice for the same parameters + XLIST_DEFINE the
 * second time.
 */
#ifndef XLIST_MACROS
#define XLIST_MACROS

/* Check basic required parameters. */
#if !defined(XLIST_PREFIX)
#error "XLIST_PREFIX must be defined"
#endif
#if defined(XLIST_SLIST) + defined(XLIST_DLIST) != 1
#error "one of XLIST_SLIST, XLIST_DLIST must be defined"
#endif
#if !defined(XLIST_DECLARE) && !defined(XLIST_DEFINE)
#error "one or both of XLIST_DECLARE, XLIST_DEFINE must be defined"
#endif
#if defined(XLIST_PTR) + defined(XLIST_PTRDIFF) + defined(XLIST_INDEX) != 1
#error "one of XLIST_PTR, XLIST_PTRDIFF, XLIST_INDEX must be defined"
#endif
#if defined(XLIST_OBJECT_T) != defined(XLIST_OBJECT_MEMBER)
#error "both or neither of XLIST_OBJECT_T, XLIST_OBJECT_MEMBER must be defined"
#endif
#if defined(XLIST_EMPTY_NIL) + defined(XLIST_EMPTY_SELF) + defined(XLIST_EMPTY_LAZY) > 1
#error "at most one of XLIST_EMPTY_NIL, XLIST_EMPTY_SELF, XLIST_EMPTY_LAZY must be defined"
#endif
#if defined(XLIST_TAILED) && !defined(XLIST_SLIST)
#error "XLIST_TAILED is only allowed for XLIST_SLIST"
#endif

/* Checks and defaults for XLIST_PTR. */
#if defined(XLIST_PTR)
#define XLIST_NIL NULL
#if !defined(XLIST_EMPTY_NIL) && !defined(XLIST_EMPTY_SELF) && !defined(XLIST_EMPTY_LAZY)
#define XLIST_EMPTY_LAZY
#endif
#define XLIST_CONTEXT_ARG
#define XLIST_CONTEXT
#endif

/* Checks and defaults for relative pointers. */
#if defined(XLIST_PTRDIFF)
#if !defined(XLIST_LINK_T)
#define XLIST_LINK_T ptrdiff_t
#endif
static_assert(pg_type_is_signed(XLIST_LINK_T), "signed type required");
#define XLIST_NIL ((XLIST_LINK_T) 0)
#if defined(XLIST_EMPTY_NIL) || defined(XLIST_EMPTY_LAZY)
#error "only XLIST_EMPTY_SELF is allowed for XLIST_PTRDIFF"
#endif
#if !defined(XLIST_EMPTY_SELF)
#define XLIST_EMPTY_SELF
#endif
#define XLIST_CONTEXT_ARG
#define XLIST_CONTEXT
#endif

/* Checks and defaults for array indexes. */
#if defined(XLIST_INDEX)
#if !defined(XLIST_OBJECT_T)
#error "XLIST_OBJECT_T must be defined if using arrays"
#endif
#if !defined(XLIST_LINK_T)
#define XLIST_LINK_T int
#endif
#if !defined(XLIST_NIL)
#define XLIST_NIL (-1)
#endif
#if !defined(XLIST_EMPTY_NIL)
#define XLIST_EMPTY_NIL
#endif
/* Most functions gain an array argument. */
#define XLIST_CONTEXT_ARG , XLIST_OBJECT_T *array
#define XLIST_CONTEXT , array
#endif

/* Other defaults. */
#if !defined(XLIST_COUNT_T)
#define XLIST_COUNT_T uint32_t
#endif

/* Macros to generate a name with the requested prefix. */
#define XLIST_MAKE_PREFIX(a) CppConcat(a,_)
#define XLIST_MAKE_NAME_(a,b) CppConcat(a,b)
#define XLIST_MAKE_NAME(name) XLIST_MAKE_NAME_(XLIST_MAKE_PREFIX(XLIST_PREFIX), \
											   name)

/* Generate type names. */
#define XLIST_count_t XLIST_MAKE_NAME(count_t)
#define XLIST_head XLIST_MAKE_NAME(head)
#define XLIST_link_t XLIST_MAKE_NAME(link_t)
#define XLIST_node XLIST_MAKE_NAME(node)

/* Generate public function names. */
#define XLIST_count XLIST_MAKE_NAME(count)
#define XLIST_delete XLIST_MAKE_NAME(delete)
#define XLIST_delete_from XLIST_MAKE_NAME(delete_from)
#define XLIST_delete_from_thoroughly XLIST_MAKE_NAME(delete_from_thoroughly)
#define XLIST_delete_thoroughly XLIST_MAKE_NAME(delete_thoroughly)
#define XLIST_init XLIST_MAKE_NAME(init)
#define XLIST_insert_after XLIST_MAKE_NAME(insert_after)
#define XLIST_insert_before XLIST_MAKE_NAME(insert_before)
#define XLIST_insert_into_after XLIST_MAKE_NAME(insert_into_after)
#define XLIST_insert_into_before XLIST_MAKE_NAME(insert_into_before)
#define XLIST_is_empty XLIST_MAKE_NAME(is_empty)
#define XLIST_has_next XLIST_MAKE_NAME(has_next)
#define XLIST_has_prev XLIST_MAKE_NAME(has_prev)
#define XLIST_head_node XLIST_MAKE_NAME(head_node)
#define XLIST_move_head XLIST_MAKE_NAME(move_head)
#define XLIST_move_tail XLIST_MAKE_NAME(move_tail)
#define XLIST_next_node XLIST_MAKE_NAME(next_node)
#define XLIST_node_init XLIST_MAKE_NAME(node_init)
#define XLIST_node_is_detached XLIST_MAKE_NAME(node_is_detached)
#define XLIST_pop_head_node XLIST_MAKE_NAME(pop_head_node)
#define XLIST_pop_tail_node XLIST_MAKE_NAME(pop_tail_node)
#define XLIST_prev_node XLIST_MAKE_NAME(prev_node)
#define XLIST_push_head XLIST_MAKE_NAME(push_head)
#define XLIST_push_tail XLIST_MAKE_NAME(push_tail)
#define XLIST_tail_node XLIST_MAKE_NAME(tail_node)

/* Generate private function names. */
#define XLIST_check XLIST_MAKE_NAME(check)
#define XLIST_decrement XLIST_MAKE_NAME(decrement)
#define XLIST_delete_next XLIST_MAKE_NAME(delete_next)
#define XLIST_get_next XLIST_MAKE_NAME(get_next)
#define XLIST_get_next_or_head XLIST_MAKE_NAME(get_next_or_head)
#define XLIST_get_prev XLIST_MAKE_NAME(get_prev)
#define XLIST_get_prev_or_head XLIST_MAKE_NAME(get_prev_or_head)
#define XLIST_get_ptrdiff XLIST_MAKE_NAME(get_ptrdiff)
#define XLIST_increment XLIST_MAKE_NAME(inc)
#define XLIST_member_check XLIST_MAKE_NAME(member_check)
#define XLIST_next_is_nil XLIST_MAKE_NAME(next_is_nil)
#define XLIST_node_to_container XLIST_MAKE_NAME(node_to_container)
#define XLIST_node_to_container_offset XLIST_MAKE_NAME(node_to_container_offset)
#define XLIST_prev_is_nil XLIST_MAKE_NAME(prev_is_nil)
#define XLIST_push_common XLIST_MAKE_NAME(push_common)
#define XLIST_set_next XLIST_MAKE_NAME(set_next)
#define XLIST_set_next_to_next_of XLIST_MAKE_NAME(set_next_to_next_of)
#define XLIST_set_next_to_terminator XLIST_MAKE_NAME(set_next_to_terminator)
#define XLIST_set_prev XLIST_MAKE_NAME(set_prev)
#define XLIST_set_prev_to_prev_of XLIST_MAKE_NAME(set_prev_to_prev_of)
#define XLIST_set_prev_to_terminator XLIST_MAKE_NAME(set_prev_to_terminator)

#endif							/* !XLIST_MACROS */

/* If included a second time, don't emit the declarations again. */
#if !defined(XLIST_DECLARED) && \
	(defined(XLIST_DECLARE) || defined(XLIST_DECLARE_ONLY))
#define XLIST_DECLARED

/* Type used for counting elements in the list .*/
typedef XLIST_COUNT_T XLIST_count_t;

/* Internal type used for linking to next and previous nodes. */
#if defined(XLIST_PTR)
typedef struct XLIST_node *XLIST_link_t;
#else
static_assert(pg_type_is_integral(XLIST_LINK_T), "integer type required");
typedef XLIST_LINK_T XLIST_link_t;
#endif

/*
 * Node of a linked list.
 *
 * Embed this in structs that need to be part of a linked list.
 */
typedef struct XLIST_node
{
	XLIST_link_t next;
#if defined(XLIST_DLIST)
	XLIST_link_t prev;
#endif
} XLIST_node;

/*
 * Head of a linked list.
 */
typedef struct XLIST_head
{
	XLIST_node	head;
#if defined(XLIST_TAILED)
	XLIST_node	tail;
#endif
#if defined(XLIST_COUNTED)
	XLIST_count_t count;
#endif
} XLIST_head;

#endif							/* XLIST_ */



#if defined(XLIST_DEFINE) || defined(XLIST_DEFINE_ONLY)

/*-------------------------------------------------------------------------
 *
 * Internal support functions.
 *
 *-------------------------------------------------------------------------
 */

#ifdef XLIST_OBJECT_T
static inline XLIST_OBJECT_T *
XLIST_node_to_container_offset(XLIST_node *node, size_t offset)
{
	return (XLIST_OBJECT_T *) ((char *) node - offset);
};

static inline XLIST_OBJECT_T *
XLIST_node_to_container(XLIST_node *node)
{
	return XLIST_node_to_container_offset(node,
										  offsetof(XLIST_OBJECT_T,
												   XLIST_OBJECT_MEMBER));
};
#endif

static inline bool
XLIST_next_is_nil(XLIST_node *node)
{
	return node->next == XLIST_NIL;
}

#if defined(XLIST_DLIST)
static inline bool
XLIST_prev_is_nil(XLIST_node *node)
{
	return node->prev == XLIST_NIL;
}
#endif

#if defined(XLIST_PTRDIFF) && defined(USE_ASSERT_CHECKING)
static inline void
XLIST_check_node_distance(XLIST_node *node1, XLIST_node *node2)
{
	uintptr_t	abs_difference;

	/*
	 * TYPE_MAX <= abs(TYPE_MIN) in the integer representations required by
	 * C11, so it's a safe ceiling to use in both directions.  All modern
	 * computers use two's complement (the only representation allowed by
	 * POSIX:2001 and C23), where abs(TYPE_MIN) is higher by one.
	 */
	static_assert(-pg_type_numeric_limits_max(XLIST_link_t) < 0,
				  "expected to be able to negate the maximum value of "
				  "XLIST_LINK_T without wrapping around");

	if (node2 >= node1)
		abs_difference = (uintptr_t) node2 - (uintptr_t) node1;
	else
		abs_difference = (uintptr_t) node1 - (uintptr_t) node2;

	Assert(abs_difference <= pg_type_numeric_limits_max(XLIST_link_t));
}
#endif

#ifdef XLIST_PTRDIFF
static inline XLIST_link_t
XLIST_get_ptrdiff(XLIST_node *base, XLIST_node *target)
{
#ifdef USE_ASSERT_CHECKING
	XLIST_check_node_distance(base, target);
#endif

	return (char *) target - (char *) base;
}

static inline XLIST_link_t
XLIST_move_ptrdiff(XLIST_link_t link, XLIST_node *old_base, XLIST_node *new_base)
{
#ifdef USE_ASSERT_CHECKING
	XLIST_node *target = (XLIST_node *) (old_base + link);

	XLIST_check_node_distance(old_base, target);
	XLIST_check_node_distance(new_base, target);
#endif

	return link + ((char *) old_base - (char *) new_base);
}
#endif

/* Internal function to get next node. */
static inline void
XLIST_set_next(XLIST_node *node, XLIST_node *next XLIST_CONTEXT_ARG)
{
#if defined(XLIST_INDEX)
	node->next = XLIST_node_to_container(next) - &array[0];
#elif defined(XLIST_PTRDIFF)
	node->next = XLIST_get_ptrdiff(node, next);
#elif defined(XLIST_PTR)
	node->next = next;
#endif
}

/* Internal function to get next node. */
static inline XLIST_node *
XLIST_get_next(XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_INDEX)
	return &array[node->next].XLIST_OBJECT_MEMBER;
#elif defined(XLIST_PTRDIFF)
	return (XLIST_node *) ((char *) node + node->next);
#elif defined(XLIST_PTR)
	return node->next;
#endif
}

#if defined(XLIST_DLIST)
/* Internal function to set previous node. */
static inline void
XLIST_set_prev(XLIST_node *node, XLIST_node *prev XLIST_CONTEXT_ARG)
{
#if defined(XLIST_INDEX)
	node->prev = XLIST_node_to_container(prev) - &array[0];
#elif defined(XLIST_PTRDIFF)
	node->prev = XLIST_get_ptrdiff(node, prev);
#elif defined(XLIST_PTR)
	node->prev = prev;
#endif
}
#endif

#if defined(XLIST_DLIST)
/* Internal function to get previous node. */
static inline XLIST_node *
XLIST_get_prev(XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_INDEX)
	return &array[node->prev].XLIST_OBJECT_MEMBER;
#elif defined(XLIST_PTRDIFF)
	return (XLIST_node *) ((char *) node + node->prev);
#elif defined(XLIST_PTR)
	return node->prev;
#endif
}
#endif

#if defined(XLIST_DLIST) || defined(XLIST_TAILED)
/* Internal function to get next node or head if XLIST_EMPTY_NIL. */
static inline XLIST_node *
XLIST_get_next_or_head(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_EMPTY_NIL)
	if (node->next == XLIST_NIL)
		return &list->head;
#endif
	return XLIST_get_next(node XLIST_CONTEXT);
}
#endif

#if defined(XLIST_DLIST)
/* Internal function to get previous node or head if XLIST_EMPTY_NIL. */
static inline XLIST_node *
XLIST_get_prev_or_head(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_EMPTY_NIL)
	if (node->prev == XLIST_NIL)
		return &list->head;
#endif
	return XLIST_get_prev(node XLIST_CONTEXT);
}
#endif

/* Inernal shortcut for set_next(dst, get_next(src)). */
static inline void
XLIST_set_next_to_next_of(XLIST_node *dst, XLIST_node *src)
{
#if defined(XLIST_PTRDIFF)
	dst->next = XLIST_move_ptrdiff(src->next, src, dst);
#else
	dst->next = src->next;
#endif
}

#if defined(XLIST_DLIST)
/* Internal shortcut for set_prev(dst, get_prev(src)). */
static inline void
XLIST_set_prev_to_prev_of(XLIST_node *dst, XLIST_node *src)
{
#if defined(XLIST_PTRDIFF)
	dst->prev = XLIST_move_ptrdiff(src->prev, src, dst);
#else
	dst->prev = src->prev;
#endif
}
#endif

#if defined(XLIST_DLIST)
/* Internal function to avoid repeating ifdef. */
static inline void
XLIST_set_prev_to_terminator(XLIST_node *node, XLIST_head *list)
{
#ifdef XLIST_EMPTY_NIL
	node->prev = XLIST_NIL;
#else
	XLIST_set_prev(node, &list->head);
#endif
}
#endif

/* Internal function to avoid repeating ifdef. */
static inline void
XLIST_set_next_to_terminator(XLIST_node *node, XLIST_head *list)
{
#ifdef XLIST_EMPTY_NIL
	node->next = XLIST_NIL;
#else
	XLIST_set_next(node, &list->head);
#endif
}

#if defined(XLIST_COUNTED)
/* Internal function to maintain count. */
static inline void
XLIST_increment(XLIST_head *list)
{
	list->count++;
	Assert(list->count > 0);	/* overflow? */
}
#endif

#if defined(XLIST_COUNTED)
/* Internal function to maintain count. */
static inline void
XLIST_decrement(XLIST_head *list)
{
	Assert(list->count > 0);	/* underflow? */
	list->count--;
}
#endif

#if defined(XLIST_SLIST)
/*
 * Internal helper function.
 */
static inline void
XLIST_delete_next(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	XLIST_node *next = XLIST_get_next(node XLIST_CONTEXT);

	XLIST_set_next_to_next_of(node, next);

#if defined(XLIST_TAILED)
	if (XLIST_get_next(&list->tail XLIST_CONTEXT) == next)
	{
#if defined(XLIST_EMPTY_NIL)
		if (node == &list->head)
			list->tail.next = XLIST_NIL;
		else
			XLIST_set_next(&list->tail, node XLIST_CONTEXT);
#else
		XLIST_set_next(&list->tail, node XLIST_CONTEXT);
#endif
	}
#endif

#if defined(XLIST_COUNTED)
	XLIST_decrement(list);
#endif
}
#endif

static inline void XLIST_init(XLIST_head *list);
static inline bool XLIST_is_empty(const XLIST_head *list);

/*
 * Internal routine used by push_head() and push_tail().
 */
static inline bool
XLIST_push_common(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_EMPTY_LAZY)
	if (list->head.next == XLIST_NIL)
	{
		/* First insertion into a lazy self-initializing list. */
		XLIST_init(list);
	}
#endif

#if defined(XLIST_EMPTY_SELF) && !defined(XLIST_PTRDIFF)

	/*
	 * List must have been initialized.  We can't make this assertion for
	 * XLIST_PTRDIFF because a pointer to self is indisinguishible from
	 * zero-initialization.
	 */
	Assert(list->head.next != XLIST_NIL);
#endif

#if defined(XLIST_COUNTED)
	XLIST_increment(list);
#endif

#if defined(XLIST_EMPTY_NIL)
	/* Insertion into empty NIL-list require a special branch. */
	if (XLIST_is_empty(list))
	{
		node->next = XLIST_NIL;
		XLIST_set_next(&list->head, node XLIST_CONTEXT);
#if defined(XLIST_DLIST)
		node->prev = XLIST_NIL;
		XLIST_set_prev(&list->head, node XLIST_CONTEXT);
#elif defined(XLIST_TAILED)
		XLIST_set_next(&list->tail, node XLIST_CONTEXT);
#endif
		return true;
	}
#endif

	return false;
}


/* Internal consistency check. */
static inline void
XLIST_check(XLIST_head *list)
{
#ifdef XLIST_DEBUG
	XLIST_node *cur;
	XLIST_node *tail;
	XLIST_count_t count = 0;

	if (list == NULL)
		elog(ERROR, "linked list head address is NULL");

#if defined(XLIST_EMPTY_LAZY) || defined(XLIST_EMPTY_NIL)
	if (head->head.next == XLIST_NIL && head->head.prev == XLIST_NIL)
		return;					/* OK, initialized as NIL (usually zeroes) */
#endif

	/* iterate in forward direction */
	for (cur = head->head.next; cur != &head->head; cur = cur->next)
	{
		if (cur == NULL ||
			cur->next == NULL ||
			cur->prev == NULL ||
			cur->prev->next != cur ||
			cur->next->prev != cur)
			elog(ERROR, "doubly linked list is corrupted");
	}

	/* iterate in backward direction */
	for (cur = head->head.prev; cur != &head->head; cur = cur->prev)
	{
		if (cur == NULL ||
			cur->next == NULL ||
			cur->prev == NULL ||
			cur->prev->next != cur ||
			cur->next->prev != cur)
			elog(ERROR, "doubly linked list is corrupted");
	}
#endif
}

/* Insternal consistency check. */
static inline void
XLIST_member_check(const XLIST_head *list, const XLIST_node *node)
{
#ifdef XLIST_DEBUG
	XLIST_node *cur = &list->head;

	while (XLIST_has_next(list, node))
	{
		cur = XLIST_get_net(node);
		if (cur == node)
			return;
	}
	elog(PANIC, "linked list member check failure");
#endif
}



/*-------------------------------------------------------------------------
 *
 * Public functions.
 *
 *-------------------------------------------------------------------------
 */

/*
 * Initialize a list.
 * Previous state will be thrown away without any cleanup.
 */
static inline void
XLIST_init(XLIST_head *list)
{
	XLIST_set_next_to_terminator(&list->head, list);
#if defined(XLIST_DLIST)
	XLIST_set_prev_to_terminator(&list->head, list);
#endif
#if defined(XLIST_TAILED)
	XLIST_set_next_to_terminator(&list->tail, list);
#endif
#if defined(XLIST_COUNTED)
	list->count = 0;
#endif
}

/*
 * Initialize a list element.
 *
 * This is only needed when XLIST_node_is_detached() may be needed.
 */
static inline void
XLIST_node_init(XLIST_node *node)
{
	node->next = 0;
#if defined(XLIST_DLIST)
	node->prev = 0;
#endif
}

/*
 * Is the list empty?
 */
static inline bool
XLIST_is_empty(const XLIST_head *list)
{
#if defined(XLIST_EMPTY_LAZY) || defined(XLIST_EMPTY_NIL)
	if (list->head.next == XLIST_NIL)
		return true;
#endif

#if defined(XLIST_EMPTY_LAZY) || defined(XLIST_EMPTY_SELF)
	if (XLIST_get_next(unconstify(XLIST_node *, &list->head)) == &list->head)
		return true;
#endif
	return false;
}

/*
 * Check whether 'node' has a following node.
 * Caution: unreliable if 'node' is not in the list.
 */
static inline bool
XLIST_has_next(const XLIST_head *list, const XLIST_node *node)
{
#if defined(XLIST_EMPTY_NIL)
	return node->next != XLIST_NIL;
#else
	return XLIST_get_next(unconstify(XLIST_node *, node)) != &list->head;
#endif
}

static inline XLIST_node *XLIST_prev_node(XLIST_head *list,
										  XLIST_node *node XLIST_CONTEXT_ARG);

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Check whether 'node' has a preceding node.
 * Caution: unreliable if 'node' is not in the list.
 */
static inline bool
XLIST_has_prev(const XLIST_head *list, const XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_DLIST)
#if defined(XLIST_EMPTY_NIL)
	return node->prev != XLIST_NIL;
#else
	return XLIST_get_prev(unconstify(XLIST_node *, node)) != &list->head;
#endif
#elif defined(XLIST_LINEAR)
	XLIST_node *prev = XLIST_prev_node(unconstify(XLIST_head *, list),
									   unconstify(XLIST_node *, node) XLIST_CONTEXT);

	return prev && prev != &list->head;
#endif
}
#endif

/*
 * Check if node is detached. A node is only detached if it either has been
 * initialized with XLIST_node_init(), or deleted with
 * XLIST_delete_thoroughly() / XLIST_delete_from_thoroughly().
 */
static inline bool
XLIST_node_is_detached(const XLIST_node *node)
{
#if defined(XLIST_DLIST)
	Assert((node->next == XLIST_NIL && node->prev == XLIST_NIL) ||
		   (node->next != XLIST_NIL && node->prev != XLIST_NIL));
#endif

	return node->next == XLIST_NIL;
}

/*
 * Return the next node in the list (there must be one).
 */
static inline XLIST_node *
XLIST_next_node(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	Assert(XLIST_has_next(list, node));
	return XLIST_get_next(node XLIST_CONTEXT);
}

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Return previous node in the list (there must be one).
 */
static inline XLIST_node *
XLIST_prev_node(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_XLIST)
	Assert(XLIST_has_prev(list, node XLIST_CONTEXT));
	return XLIST_get_prev(node XLIST_CONTEXT);
#else
	Assert(!XLIST_is_empty(list));
	for (XLIST_node *cur = &list->head;
		 XLIST_has_next(list, cur);)
	{
		XLIST_node *next = XLIST_get_next(cur XLIST_CONTEXT);

		if (next == node)
			return cur;
		cur = next;
	}
	Assert(false);
	return NULL;
#endif
}
#endif

/*
 * Return the first node in the list (there must be one).
 */
static inline XLIST_node *
XLIST_head_node(XLIST_head *list XLIST_CONTEXT_ARG)
{
	Assert(!XLIST_is_empty(list));
	return XLIST_get_next(&list->head XLIST_CONTEXT);
}

#if defined(XLIST_DLIST) || defined(XLIST_TAILED) || defined(XLIST_LINEAR)
/*
 * Return the last node in the list (there must be one).
 */
static inline XLIST_node *
XLIST_tail_node(XLIST_head *list XLIST_CONTEXT_ARG)
{
	Assert(!XLIST_is_empty(list));
#if defined(XLIST_DLIST)
	return XLIST_get_prev(&list->head XLIST_CONTEXT);
#elif defined(XLIST_TAILED)
	return XLIST_get_next(&list->tail XLIST_CONTEXT);
#elif defined(XLIST_LINEAR)
	for (XLIST_node *node = XLIST_get_next(&list->head XLIST_CONTEXT);;)
	{
		if (!XLIST_has_next(list, node))
			return node;
		node = XLIST_get_next(node XLIST_CONTEXT);
	}
	Assert(false);
	return NULL;
#endif
}
#endif

/*
 * Insert a node at the beginning of the list.
 */
static inline void
XLIST_push_head(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	if (!XLIST_push_common(list, node XLIST_CONTEXT))
	{
#if defined(XLIST_TAILED)
		if (XLIST_is_empty(list))
			XLIST_set_next(&list->tail, node XLIST_CONTEXT);
#endif

		XLIST_set_next_to_next_of(node, &list->head);
#if defined(XLIST_DLIST)
		XLIST_set_prev_to_terminator(node, list);
		XLIST_set_prev(XLIST_get_next(node XLIST_CONTEXT), node XLIST_CONTEXT);
#endif
		XLIST_set_next(&list->head, node XLIST_CONTEXT);
	}
}

#if defined(XLIST_DLIST) || defined(XLIST_TAILED) || defined(XLIST_LINEAR)
/*
 * Insert a node at the end of the list.
 */
static inline void
XLIST_push_tail(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	if (!XLIST_push_common(list, node XLIST_CONTEXT))
	{
		XLIST_set_next_to_terminator(node, list);

#if defined(XLIST_DLIST)
		/* Branchless. */
		XLIST_set_prev_to_prev_of(node, &list->head);
		XLIST_set_next(XLIST_get_prev(node XLIST_CONTEXT), node XLIST_CONTEXT);
		XLIST_set_prev(&list->head, node XLIST_CONTEXT);
#elif defined(XLIST_TAILED)
		/* Branchless unless using XLIST_EMPTY_NIL. */
		XLIST_set_next(XLIST_get_next_or_head(list, &list->tail XLIST_CONTEXT),
					   node XLIST_CONTEXT);
		XLIST_set_next(&list->tail, node XLIST_CONTEXT);
#elif defined(XLIST_LINEAR)
		XLIST_set_next(XLIST_is_empty(list) ?
					   &list->head :
					   XLIST_tail_node(list XLIST_CONTEXT),
					   node XLIST_CONTEXT);
#endif
	}
}
#endif

/*
 * Insert a node after another *in the same list*.
 */
static inline void
XLIST_insert_into_after(XLIST_head *list,
						XLIST_node *after,
						XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_DLIST)
	XLIST_set_prev(node, after XLIST_CONTEXT);
#endif

	XLIST_set_next_to_next_of(node, after);
	XLIST_set_next(after, node XLIST_CONTEXT);

#if defined(XLIST_DLIST)
	/* This branches only for XLIST_EMPTY_NIL. */
	XLIST_set_prev(XLIST_get_next_or_head(list, node XLIST_CONTEXT),
				   node XLIST_CONTEXT);
#endif

#if defined(XLIST_TAILED)
	if (XLIST_get_next(&list->tail XLIST_CONTEXT) == after)
		XLIST_set_next(&list->tail, node XLIST_CONTEXT);
#endif

#if defined(XLIST_COUNTED)
	XLIST_increment(list);
#endif
}

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Insert a node before another *in the same list*.
 */
static inline void
XLIST_insert_into_before(XLIST_head *list,
						 XLIST_node *before,
						 XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_DLIST)
	XLIST_set_prev_to_prev_of(node, before);
	XLIST_set_next(node, before XLIST_CONTEXT);
	XLIST_set_prev(before, node XLIST_CONTEXT);
	XLIST_set_next(XLIST_get_prev_or_head(list, node XLIST_CONTEXT),
				   node XLIST_CONTEXT);

#if defined(XLIST_COUNTED)
	XLIST_increment(list);
#endif

#elif defined(XLIST_LINEAR)
	if (before == XLIST_head_node(list XLIST_CONTEXT))
		XLIST_push_head(list, node XLIST_CONTEXT);
	else
		XLIST_insert_into_after(list,
								XLIST_prev_node(list, before XLIST_CONTEXT),
								node XLIST_CONTEXT);
#endif
}
#endif

#ifdef XLIST_COUNTED
static inline void
XLIST_insert_before(XLIST_head *list,
					XLIST_node *before,
					XLIST_node *node XLIST_CONTEXT_ARG)
{
	XLIST_insert_into_before(list, before, node XLIST_CONTEXT);
}
#else
static inline void
XLIST_insert_before(XLIST_node *before,
					XLIST_node *node XLIST_CONTEXT_ARG)
{
	XLIST_insert_into_before(NULL, before, node XLIST_CONTEXT);
}
#endif

#if defined(XLIST_DLIST) && !defined(XLIST_EMPTY_NIL) && !defined(XLIST_COUNTED)
/*
 * Delete 'node' from its list (it must be in one).
 */
static inline void
XLIST_delete(XLIST_node *node XLIST_CONTEXT_ARG)
{
	XLIST_set_next_to_next_of(XLIST_get_prev(node XLIST_CONTEXT), node);
	XLIST_set_prev_to_prev_of(XLIST_get_next(node XLIST_CONTEXT), node);
}
#endif

#if defined(XLIST_DLIST) && !defined(XLIST_EMPTY_NIL) && !defined(XLIST_COUNTED)
/*
 * Like XLIST_delete(), but also sets next/prev to NULL to signal not being in
 * a list.
 */
static inline void
XLIST_delete_thoroughly(XLIST_node *node XLIST_CONTEXT_ARG)
{
	XLIST_delete(node XLIST_CONTEXT);
#ifdef XLIST_DLIST
	node->next = XLIST_NIL;
	node->prev = XLIST_NIL;
#endif
}
#endif

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Same as XLIST_delete, but performs checks in XLIST_DEBUG builds to ensure
 * that 'node' belongs to 'head'.
 */
static inline void
XLIST_delete_from(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	XLIST_member_check(list, node);

#if defined(XLIST_DLIST)
	XLIST_set_next_to_next_of(XLIST_get_prev_or_head(list, node XLIST_CONTEXT), node);
	XLIST_set_prev_to_prev_of(XLIST_get_next_or_head(list, node XLIST_CONTEXT), node);
#if defined(XLIST_COUNTED)
	XLIST_decrement(list);
#endif
#else
	XLIST_delete_next(list,
					  XLIST_prev_node(list, node XLIST_CONTEXT) XLIST_CONTEXT);
#endif
}
#endif

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Like XLIST_delete_from, but also sets next/prev to NIL to signal not being
 * in a list.
 */
static inline void
XLIST_delete_from_thoroughly(XLIST_head *list,
							 XLIST_node *node XLIST_CONTEXT_ARG)
{
	XLIST_delete_from(list, node XLIST_CONTEXT);
	node->next = XLIST_NIL;
#if defined(XLIST_DLIST)
	node->prev = XLIST_NIL;
#endif
}
#endif

/*
 * Remove and return the first node from a list (there must be one).
 */
static inline XLIST_node *
XLIST_pop_head_node(XLIST_head *list XLIST_CONTEXT_ARG)
{
	XLIST_node *node;

	Assert(!XLIST_is_empty(list));
	node = XLIST_head_node(list XLIST_CONTEXT);

#if defined(XLIST_DLIST)
	XLIST_delete_from(list, node XLIST_CONTEXT);
#else
	XLIST_delete_next(list, &list->head XLIST_CONTEXT);
#endif

	return node;
}

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Remove and return the last node from a list (there must be one).
 */
static inline XLIST_node *
XLIST_pop_tail_node(XLIST_head *list XLIST_CONTEXT_ARG)
{
	XLIST_node *node;

	Assert(!XLIST_is_empty(list));

	node = XLIST_tail_node(list XLIST_CONTEXT);
	XLIST_delete_from(list, node XLIST_CONTEXT);

	return node;
}
#endif

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Move element from its current position in the list to the head position in
 * the same list.
 *
 * Undefined behavior if 'node' is not already part of the list.
 */
static inline void
XLIST_move_head(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	/* fast path if it's already at the head */
	if (XLIST_head_node(list XLIST_CONTEXT) == node)
		return;

	XLIST_delete_from(list, node XLIST_CONTEXT);
	XLIST_push_head(list, node XLIST_CONTEXT);

	XLIST_check(list);
}
#endif

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Move element from its current position in the list to the tail position in
 * the same list.
 *
 * Undefined behavior if 'node' is not already part of the list.
 */
static inline void
XLIST_move_tail(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	/* fast path if it's already at the tail */
	if (XLIST_tail_node(list XLIST_CONTEXT) == node)
		return;

	XLIST_delete_from(list, node XLIST_CONTEXT);
	XLIST_push_tail(list, node XLIST_CONTEXT);

	XLIST_check(list);
}
#endif

/*
 * Return the number of entries in the list.
 */
static inline XLIST_count_t
XLIST_count(const XLIST_head *list XLIST_CONTEXT_ARG)
{
	XLIST_count_t count;

#ifdef XLIST_COUNT
	count = head->count;
#else
	count = 0;
	for (const XLIST_node *node = &list->head;
		 XLIST_has_next(list, node);
		 node = XLIST_get_next(unconstify(XLIST_node *, node) XLIST_CONTEXT))
		count++;
#endif

	Assert(XLIST_is_empty(list) == (count == 0));

	return count;
}

#endif							/* XLIST_DEFINE */


/*-------------------------------------------------------------------------
 *
 * Cleanup.
 *
 *-------------------------------------------------------------------------
 */

/*
 * Clean up the macros, unless we expect to be included a second time with the
 * existing parameters + XLIST_DEFINE.
 */

#if defined(XLIST_DECLARE_ONLY) ||							\
	defined(XLIST_DEFINE_ONLY) ||							\
	defined(XLIST_DEFINE)

/* Undefine include guard-like macros. */
#undef XLIST_DECLARED
#undef XLIST_MACROS

/* Undefine template parameter macros. */
#undef XLIST_DECLARE
#undef XLIST_DECLARE_ONLY
#undef XLIST_DEFINE
#undef XLIST_DEFINE_ONLY
#undef XLIST_DLIST
#undef XLIST_INDEX
#undef XLIST_LINK_T
#undef XLIST_NIL
#undef XLIST_OBJECT_MEMBER
#undef XLIST_OBJECT_T
#undef XLIST_PREFIX
#undef XLIST_PTR
#undef XLIST_PTRDIFF
#undef XLIST_SLIST
#undef XLIST_TAILED

/* Undefine internal macros. */
#undef XLIST_CONTEXT
#undef XLIST_CONTEXT_ARG
#undef XLIST_EMPTY_LAZY
#undef XLIST_EMPTY_NIL
#undef XLIST_EMPTY_SELF
#undef XLIST_MAKE_NAME
#undef XLIST_MAKE_NAME_
#undef XLIST_MAX_PREFIX
#undef XLIST_check
#undef XLIST_count
#undef XLIST_count_t
#undef XLIST_decrement
#undef XLIST_delete
#undef XLIST_delete_from
#undef XLIST_delete_from_thoroughly
#undef XLIST_delete_thoroughly
#undef XLIST_delete_next
#undef XLIST_get_next
#undef XLIST_get_next_or_head
#undef XLIST_get_prev
#undef XLIST_get_prev_or_head
#undef XLIST_get_ptrdiff
#undef XLIST_has_next
#undef XLIST_has_prev
#undef XLIST_head
#undef XLIST_head_node
#undef XLIST_increment
#undef XLIST_init
#undef XLIST_insert_after
#undef XLIST_insert_before
#undef XLIST_insert_into_after
#undef XLIST_insert_into_before
#undef XLIST_is_empty
#undef XLIST_link_t
#undef XLIST_member_check
#undef XLIST_move_head
#undef XLIST_move_tail
#undef XLIST_next_is_nil
#undef XLIST_next_node
#undef XLIST_node
#undef XLIST_node_init
#undef XLIST_node_is_detached
#undef XLIST_node_to_container
#undef XLIST_node_to_container_offset
#undef XLIST_pop_head_node
#undef XLIST_pop_tail_node
#undef XLIST_prev_is_nil
#undef XLIST_prev_node
#undef XLIST_push_common
#undef XLIST_push_head
#undef XLIST_push_tail
#undef XLIST_set_next
#undef XLIST_set_next_to_next_of
#undef XLIST_set_prev
#undef XLIST_set_prev_to_prev_of
#undef XLIST_tail_node

#endif
