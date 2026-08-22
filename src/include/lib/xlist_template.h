/*-------------------------------------------------------------------------
 *
 * xlist_template.h
 *		Template for linked list types and functions.
 *
 * This is a template that must be parameterized with macros before inclusion.
 * If the type declarations and function definitions are being split between
 * headers, then one of the following should be used:
 *
 *		XLIST_DECLARE_ONLY:		define types only
 *		XLIST_DEFINE_ONLY:		define functions only
 *
 * Alternatively, both can be generated at the same time with:
 *
 * 		XLIST_DECLARE:			define types only }-- one or both
 * 		XLIST_DEFINE:			define functions  }
 *
 * The main parameters are:
 *
 *		XLIST_PREFIX:			replaces XLIST in all identifiers (required)
 *
 *		XLIST_SLIST:			singly-linked list }-- choose one
 * 		XLIST_DLIST:			doubly-linked list }
 *
 * 		XLIST_COUNTED:			enable efficient _count() (default: uint32_t)
 * 		XLIST_TAILED:			enable efficient _push_tail() for SLIST
 *		XLIST_LINEAR:			enable inefficient O(n) functions for SLIST
 *
 * The links between nodes have the following options:
 *
 * 		XLIST_PTR:				nodes linked by plain pointers
 *		XLIST_PTRDIFF:			nodes linked relative pointers
 *		XLIST_INDEX:			nodes linked by array index
 *
 * XLIST_PTR lists are straightforward but have a fixed size and can't be
 * relocated or used in DSM segments.
 *
 * XLIST_PTRDIFF lists are only safe when all the nodes including the list
 * head are in one contiguous chunk of memory with a maximum span
 * representable with the selected link type.  That chunk can be reallocated,
 * bitwise copied to some other single chunk, written to disk and read back
 * in, or remapped at a different address, as long as there are never links
 * between objects that the compiler considers to be part of separate
 * allocations.  See comments in XLIST_link().
 *
 * XLIST_INDEX can be relocated, and the list head can be in a separate
 * allocation from the array of objects holding the nodes, but have a higher
 * runtime cost due branching required by the XLIST_EMPTY_NIL policy (see
 * below).
 *
 * The types and values used for indexes and relative pointers can be
 * controlled with:
 *
 *		XLIST_LINK_T:			override link type (not for XLIST_PTR)
 *		XLIST_PTRDIFF_SHIFT:	override scaling (default based on alignment)
 *		XLIST_COUNT_T:			override count type (default: uint32_t)
 *		XLIST_NIL:				override NIL value (for XLIST_INDEX)
 *
 * By default, XLIST_LINK_INDEX adds context arguments "first_node" and
 * "object_size" to most function so that links can be followed (see
 * XLIST_CONTEXT_ARG in argument lists).
 *
 * Alternatively, to specialize for a single use case, a pair of function-like
 * macros can be defined to show how to convert between indexes and nodes:
 *
 *		XLIST_INDEX_TO_NODE		#define XLIST_INDEX_TO_NODE(i) &array[i].node
 *		XLIST_NODE_TO_INDEX		#define XLIST_NODE_TO_INDEX(n) ...
 *
 * Additionally, if the following macro is defined then most list functions
 * gain a void *user_data argument (see XLIST_CONTEXT_ARG) that is passed
 * through to the above function-like macros as a second argument:
 *
 *		XLIST_INDEX_USER_DATA	pass a user_data argument to the above
 *
 * The representation of empty lists can be controlled explicitly, but
 * by default NIL is used only for XLIST_INDEX:
 *
 *		XLIST_EMPTY_SELF:		empty lists have head pointing to itself
 *		XLIST_EMPTY_NIL:		empty lists have NIL (less efficient)
 *
 * Whether or not zero-initialized list heads are allowed is controlled with:
 *
 *		XLIST_INIT_ON_ZERO_MEM:	zero-initialized memory is a valid list
 *		XLIST_INIT_REQUIRED:	the list must be initialized (more efficient)
 *
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
#if !defined(XLIST_DECLARE) && !defined(XLIST_DEFINE)
#error "one or both of XLIST_DECLARE, XLIST_DEFINE must be defined"
#endif
#if !defined(XLIST_PREFIX)
#error "XLIST_PREFIX must be defined"
#endif
#if defined(XLIST_SLIST) + defined(XLIST_DLIST) != 1
#error "one of XLIST_SLIST, XLIST_DLIST must be defined"
#endif
#if defined(XLIST_TAILED) && !defined(XLIST_SLIST)
#error "XLIST_TAILED is only allowed for XLIST_SLIST"
#endif
#if defined(XLIST_PTR) + defined(XLIST_PTRDIFF) + defined(XLIST_INDEX) != 1
#error "exactly one of XLIST_PTR, XLIST_PTRDIFF, XLIST_INDEX must be defined"
#endif
#if !defined(XLIST_INDEX) && (defined(XLIST_INDEX_TO_NODE) ||	\
							  defined(XLIST_NODE_TO_INDEX) ||	\
							  defined(XLIST_INDEX_USER_DATA))
#error "XLIST_INDEX_TO_NODE, XLIST_NODE_TO_INDEX and XLIST_INDEX_USER_DATA are only allowed if XLIST_INDEX is defined"
#endif
#if defined(XLIST_INDEX_TO_NODE) + defined(XLIST_NODE_TO_INDEX) == 1
#error "XLIST_INDEX_TO_NODE and XLIST_NODE_TO_INDEX must both be defined if one is"
#endif
#if defined (XLIST_INDEX_USER_DATA) && !defined(XLIST_INDEX_TO_NODE)
#error "XLIST_INDEX_USER_DATA requires XLIST_INDEX_TO_NODE and XLIST_NODE_TO_INDEX"
#endif
#if defined(XLIST_EMPTY_NIL) + defined(XLIST_EMPTY_SELF) > 1
#error "at most one of XLIST_EMPTY_NIL, XLIST_EMPTY_SELF can be defined"
#endif
#if defined(XLIST_INIT_ON_ZERO_MEM) + defined(XLIST_INIT_REQUIRED) > 1
#error "at most one of XLIST_INIT_ON_ZERO_MEM, XLIST_INIT_REQUIRED can be defined"
#endif
#if defined(XLIST_OBJECT_T) != defined(XLIST_OBJECT_MEMBER)
#error "both or neither of XLIST_OBJECT_T, XLIST_OBJECT_MEMBER must be defined"
#endif

/* Checks and defaults for XLIST_PTR. */
#if defined(XLIST_PTR)
/*
 * XLIST_EMPTY_NIL is unlikely to be useful with XLIST_PTR, but we still need
 * a NIL value to use for XLIST_delete_thoroughly().
 */
#define XLIST_NIL NULL
/* Defaults match the traditional dlist API. */
#if !defined(XLIST_EMPTY_SELF) && !defined(XLIST_EMPTY_NIL)
#define XLIST_EMPTY_SELF
#endif
#if !defined(XLIST_INIT_ON_ZERO_MEM) && !defined(XLIST_INIT_REQUIRED)
#define XLIST_INIT_ON_ZERO_MEM
#endif
#define XLIST_CONTEXT_ARG
#define XLIST_CONTEXT
#endif							/* XLIST_PTR */

/* Checks and defaults for relative pointers. */
#if defined(XLIST_PTRDIFF)
#if !defined(XLIST_LINK_T)
#define XLIST_LINK_T ptrdiff_t
#endif
static_assert(pg_type_is_signed(XLIST_LINK_T), "signed type required");
/* XLIST_NIL must be zero for XLIST_PTRDIFF. */
#define XLIST_NIL ((XLIST_LINK_T) 0)
#if !defined(XLIST_PTRDIFF_SHIFT)
/* Usurp invariable low-end bits for extra range unless set to 0. */
#define XLIST_PTRDIFF_SHIFT (alignof(XLIST_link_t) == 2 ? 1 :			\
							 alignof(XLIST_link_t) == 4 ? 2 :			\
							 alignof(XLIST_link_t) == 8 ? 3 : 0)
#endif
/* Defaults match the traditional dlist API, but see XLIST_push_common(). */
#if !defined(XLIST_EMPTY_SELF) && !defined(XLIST_EMPTY_NIL)
#define XLIST_EMPTY_SELF
#endif
#if !defined(XLIST_INIT_ON_ZERO_MEM) && !defined(XLIST_INIT_REQUIRED)
#define XLIST_INIT_ON_ZERO_MEM
#endif
#define XLIST_CONTEXT_ARG
#define XLIST_CONTEXT
#endif							/* XLIST_PTRDIFF */

/* Checks and defaults for array indexes. */
#if defined(XLIST_INDEX)
#if !defined(XLIST_LINK_T)
#define XLIST_LINK_T int
#endif
#if !defined(XLIST_NIL)
#define XLIST_NIL (-1)
#endif
#if !defined(XLIST_EMPTY_SELF) && !defined(XLIST_EMPTY_NIL)
#define XLIST_EMPTY_NIL
#endif
#if defined(XLIST_EMPTY_SELF)
/* The list head isn't in the array and doesn't have an index. */
#error "XLIST_EMPTY_SELF cannot be used with XLIST_INDEX"
#endif
#if !defined(XLIST_INIT_ON_ZERO_MEM) && !defined(XLIST_INIT_REQUIRED)
#define XLIST_INIT_REQUIRED
#endif
#if defined(XLIST_INIT_ON_ZERO_MEM)
#if defined(XLIST_SLIST)
/*
 * A singly-linked list beginning with index zero is indistinguishable from
 * zeroed memory, so XLIST_INIT_REQUIRED must be used.
 */
#error "XLIST_INDEX + XLIST_INIT_ON_ZERO_MEM incompatible with XLIST_SLIST"
#elif defined(XLIST_DLIST)
/* Likewise for doubly-linked lists if XLIST_NIL is 0. */
static_assert((XLIST_LINK_T) (XLIST_NIL) != 0,
			  "XLIST_INDEX + XLIST_INIT_ON_ZERO_MEM incompatible with zero as XLIST_NIL");
#endif
#endif
#if defined(XLIST_INDEX_USER_DATA)
#define XLIST_CONTEXT_ARG , void *user_data
#define XLIST_CONTEXT , user_data
#elif defined(XLIST_INDEX_TO_NODE)
#define XLIST_CONTEXT_ARG
#define XLIST_CONTEXT
#else
#define XLIST_CONTEXT_ARG , XLIST_node *first_node, size_t object_size
#define XLIST_CONTEXT , first_node, object_size
#endif
#endif							/* XLIST_INDEX */

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
#define XLIST_check_contents XLIST_MAKE_NAME(check_contents)
#define XLIST_check_links XLIST_MAKE_NAME(check_links)
#define XLIST_check_next_link XLIST_MAKE_NAME(check_next_link)
#define XLIST_check_next_nil XLIST_MAKE_NAME(check_next_nil)
#define XLIST_check_prev_link XLIST_MAKE_NAME(check_prev_link)
#define XLIST_check_prev_nil XLIST_MAKE_NAME(check_prev_nil)
#define XLIST_decrement XLIST_MAKE_NAME(decrement)
#define XLIST_delete_next XLIST_MAKE_NAME(delete_next)
#define XLIST_follow XLIST_MAKE_NAME(follow_link)
#define XLIST_get_next XLIST_MAKE_NAME(get_next)
#define XLIST_get_next_or_head XLIST_MAKE_NAME(get_next_or_head)
#define XLIST_get_prev XLIST_MAKE_NAME(get_prev)
#define XLIST_get_prev_or_head XLIST_MAKE_NAME(get_prev_or_head)
#define XLIST_increment XLIST_MAKE_NAME(inc)
#define XLIST_is_zero_mem XLIST_MAKE_NAME(is_zero_mem)
#define XLIST_link XLIST_MAKE_NAME(get_ptrdiff)
#define XLIST_member_check XLIST_MAKE_NAME(member_check)
#define XLIST_next_is_nil XLIST_MAKE_NAME(next_is_nil)
#define XLIST_node_to_container XLIST_MAKE_NAME(node_to_container)
#define XLIST_node_to_container_offset XLIST_MAKE_NAME(node_to_container_offset)
#define XLIST_prev_is_nil XLIST_MAKE_NAME(prev_is_nil)
#define XLIST_push_common XLIST_MAKE_NAME(push_common)
#define XLIST_relink XLIST_MAKE_NAME(relink)
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
typedef XLIST_LINK_T XLIST_link_t;
static_assert(pg_type_is_integral(XLIST_link_t), "integer type required");
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
	/*
	 * head.next and (if doubly-linked) head.prev point to the head and tail
	 * nodes of the list.  In an empty list, they either point to head itself
	 * (XLIST_EMPTY_SELF) or hold NIL (XLIST_EMPTY_NIL), and may optionally
	 * hold zero (XLIST_INIT_ON_ZERO_MEM).
	 */
	XLIST_node	head;

#if defined(XLIST_TAILED)

	/*
	 * If XLIST_SLIST + XLIST_TAILED are requested for O(1) _tail_node() and
	 * _push_tail() operations, tail.next points to the tail node.
	 *
	 * In empty lists, tail.next points points to head (XLIST_EMPTY_SELF),
	 * holds NIL (XLIST_EMPTY_NIL) or is zeroed memory before the first push
	 * (XLIST_INIT_ON_ZERO_MEM).
	 */
	XLIST_node	tail;
#endif

#if defined(XLIST_COUNTED)
	XLIST_count_t count;
#endif
} XLIST_head;

#endif							/* XLIST_DECLARED */



#if defined(XLIST_DEFINE) || defined(XLIST_DEFINE_ONLY)

/*-------------------------------------------------------------------------
 *
 * Internal support functions.
 *
 *-------------------------------------------------------------------------
 */

static inline XLIST_node *XLIST_head_node(const XLIST_head *list XLIST_CONTEXT_ARG);
static inline XLIST_node *XLIST_next_node(const XLIST_head *list,
										  const XLIST_node *node XLIST_CONTEXT_ARG);
static inline bool XLIST_has_next(const XLIST_head *list, const XLIST_node *node);
static inline XLIST_count_t XLIST_count(const XLIST_head *list XLIST_CONTEXT_ARG);

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
XLIST_next_is_nil(const XLIST_node *node)
{
	return node->next == XLIST_NIL;
}

#if defined(XLIST_DLIST)
static inline bool
XLIST_prev_is_nil(const XLIST_node *node)
{
	return node->prev == XLIST_NIL;
}
#endif

#if defined(XLIST_PTRDIFF)
static inline void
XLIST_check_node_distance(const XLIST_node *node1, const XLIST_node *node2)
{
#if defined(USE_ASSERT_CHECKING)
	uintptr_t	abs_difference;

	/*
	 * TYPE_MAX <= abs(TYPE_MIN), consider only the absolute difference to
	 * avoid mistakes in the rules of signed arithmetic.
	 */
	if ((uint64_t) node2 >= (uint64_t) node1)
		abs_difference = (uintptr_t) node2 - (uintptr_t) node1;
	else
		abs_difference = (uintptr_t) node1 - (uintptr_t) node2;

	abs_difference >>= XLIST_PTRDIFF_SHIFT;

	Assert(abs_difference <= pg_type_numeric_limits_max(XLIST_link_t));
#endif
}
#endif

static inline XLIST_node *
XLIST_follow(const XLIST_node *base, XLIST_link_t link XLIST_CONTEXT_ARG)
{
#if defined(XLIST_PTR)
	return link;
#elif defined(XLIST_PTRDIFF)
	return (XLIST_node *) ((uintptr_t) base + (link << XLIST_PTRDIFF_SHIFT));
#elif defined(XLIST_INDEX)
#if defined(XLIST_INDEX_USER_DATA)
	return XLIST_INDEX_TO_NODE(link, user_data);
#elif defined(XLIST_INDEX_TO_NODE)
	return XLIST_INDEX_TO_NODE(link);
#else
	return (XLIST_node *) ((char *) first_node + (link * object_size));
#endif
#endif
}

static inline XLIST_link_t
XLIST_link(const XLIST_node *base, XLIST_node *target XLIST_CONTEXT_ARG)
{
#if defined(XLIST_PTR)
	return target;
#elif defined(XLIST_PTRDIFF)
	intptr_t	difference;

	/* Assert that the result fits. */
	XLIST_check_node_distance(base, target);

	/*
	 * Assumptions:
	 *
	 * 1.  base and target must have the same provenance (they reside inside
	 * the same variable, malloc(), mmap() etc).  XLIST_follow() will be able
	 * to synthesize the pointer with the same provenance as the target
	 * argument, or if the whole list is relocated by (say) reallocation, the
	 * same provenance as any other pointers into the new copy.
	 *
	 * 2.  Casting the pointers to integers before subtraction causes them to
	 * be consider to be "exposed" (ISO TS 6010's formal way of saying that
	 * the address has been taken and now potentially escaped into the wild)
	 * for the purposes of alias analysis, unlike char pointer arithmetic.
	 * Without that, the compiler can and will consider 'target' to have no
	 * potential aliases and then reorder accesses inappropriately in an
	 * optimized build.  It also avoids the problem of pointer arithmetic
	 * being undefined except for top-level array elements and members within
	 * one object or array element.
	 *
	 * 3.  Pointer-to-integer conversion gives us the actual address, and
	 * memory is linear and flat.  (Pointer/integer conversion is
	 * implement-defined and only guaranteed to survive a round trip; a
	 * Deathstation 9000 or a segmented memory system could conform but also
	 * scramble/unscramble the bits, which would break XLIST_PTRDIFF_SHIFT > 0
	 * and defeat our reasoning about the safe range of XLIST_link_t, but also
	 * other parts of our source tree, for example stack depth checks and
	 * dsa_pointer.)
	 *
	 * 4.  The following should compile to two instructions: subtract and
	 * shift.  Using unsigned arithmetic is a simple way to avoid undefined
	 * behavior on signed overflow.  Using shift instead of division avoids a
	 * couple of instructions that deal with the rounding direction of
	 * negative numbers, at the cost of requiring two's complement to give
	 * correct results.  We already require two's complement elsewhere, as do
	 * POSIX:2001, C23 and C++20.
	 */
	if ((uintptr_t) target >= (uintptr_t) base)
		difference = (uintptr_t) target - (uintptr_t) base;
	else
		difference = -((uintptr_t) base - (uintptr_t) target);

	return difference >> XLIST_PTRDIFF_SHIFT;
#elif defined(XLIST_INDEX)
#if defined(XLIST_INDEX_USER_DATA)
	return XLIST_NODE_TO_INDEX(target, user_data);
#elif defined(XLIST_INDEX_TO_NODE)
	return XLIST_NODE_TO_INDEX(target);
#else
	Assert(target >= first_node);
	return ((char *) target - (char *) first_node) / object_size;
#endif
#endif
}

static inline XLIST_link_t
XLIST_relink(XLIST_link_t link,
			 const XLIST_node *old_base,
			 const XLIST_node *new_base)
{
#if defined(XLIST_PTRDIFF)
	int64_t		difference;

	XLIST_check_node_distance(new_base, XLIST_follow(old_base, link));

	if ((uintptr_t) old_base >= (uintptr_t) new_base)
		difference = (uintptr_t) old_base - (uintptr_t) new_base;
	else
		difference = -((uintptr_t) new_base - (uintptr_t) old_base);

	return link + (difference >> XLIST_PTRDIFF_SHIFT);
#else
	return link;
#endif
}

/* Internal function to get next node. */
static inline void
XLIST_set_next(XLIST_node *node, XLIST_node *next XLIST_CONTEXT_ARG)
{
	node->next = XLIST_link(node, next XLIST_CONTEXT);
}

/* Internal function to get next node. */
static inline XLIST_node *
XLIST_get_next(const XLIST_node *node XLIST_CONTEXT_ARG)
{
	return XLIST_follow(node, node->next XLIST_CONTEXT);
}

#if defined(XLIST_DLIST)
/* Internal function to set previous node. */
static inline void
XLIST_set_prev(XLIST_node *node, XLIST_node *prev XLIST_CONTEXT_ARG)
{
	node->prev = XLIST_link(node, prev XLIST_CONTEXT);
}
#endif

#if defined(XLIST_DLIST)
/* Internal function to get previous node. */
static inline XLIST_node *
XLIST_get_prev(const XLIST_node *node XLIST_CONTEXT_ARG)
{
	return XLIST_follow(node, node->prev XLIST_CONTEXT);
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
	dst->next = XLIST_relink(src->next, src, dst);
}

#if defined(XLIST_DLIST)
/* Internal shortcut for set_prev(dst, get_prev(src)). */
static inline void
XLIST_set_prev_to_prev_of(XLIST_node *dst, XLIST_node *src)
{
	dst->prev = XLIST_relink(src->prev, src, dst);
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

static inline bool
XLIST_is_zero_mem(const XLIST_head *list)
{
	if (list->head.next == 0)
	{
#if defined(XLIST_DLIST)
#if defined(XLIST_INDEX)
		/*
		 * It might be a list with index zero in head position, so also check
		 * the tail pointer.  (XLIST_INDEX + XLIST_SLIST was excluded at the
		 * top of the file.)
		 */
		if (list->head.prev != 0)
			return false;
#endif

		/*
		 * If next is 0, prev should be zero too.  (XLIST_INDEX with XLIST_NIL
		 * == 0 was excluded at the top of the file, see top.)
		 */
		Assert(list->head.prev == 0);
#endif

		return true;
	}
	return false;
}

/*
 * Internal routine used by push_head() and push_tail().
 */
static inline bool
XLIST_push_common(XLIST_head *list, XLIST_node *node XLIST_CONTEXT_ARG)
{
	/*
	 * Lazy initialization of zero-initialized object, if requested.
	 *
	 * We skip this for XLIST_PTRDIFF, because it initialized to zeroes for
	 * both XLIST_EMPTY_NIL and XLIST_EMPTY_SELF so we can save an branch.
	 */
#if defined(XLIST_INIT_ON_ZERO_MEM) && !defined(XLIST_PTRDIFF)
	if (unlikely(XLIST_is_zero_mem(list)))
		XLIST_init(list);
#endif

#if defined(XLIST_EMPTY_SELF) && !defined(XLIST_PTRDIFF)

	/*
	 * List must have been initialized by XLIST_INIT_ON_ZERO_MEM or explicitly
	 * if using XLIST_INIT_REQUIRED.  XLIST_PTRDIFF gets a free pass.
	 */
	Assert(list->head.next != XLIST_NIL);
#endif

#if defined(XLIST_COUNTED)
	XLIST_increment(list);
#endif

#if defined(XLIST_EMPTY_NIL)
	/* Insertion into empty NIL-list specially. */
	if (unlikely(XLIST_is_empty(list)))
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

	/* Otherwise leave it to the caller to insert at the appropriate end. */
	return false;
}

#if defined(XLIST_REGRESS)
static inline void
XLIST_check_next_nil(const XLIST_node *node,
					 const char *name,
					 const char *debug_context XLIST_CONTEXT_ARG)
{
	if (node->next != XLIST_NIL)
		elog(PANIC,
			 "expected %s node at %p to have next == NIL, but it has %" PRIxPTR
			 "; context: %s",
			 name,
			 node,
			 (uintptr_t) node->next,
			 debug_context);
}

#if defined(XLIST_DLIST)
static inline void
XLIST_check_prev_nil(const XLIST_node *node,
					 const char *name,
					 const char *debug_context XLIST_CONTEXT_ARG)
{
	if (node->prev != XLIST_NIL)
		elog(PANIC,
			 "expected %s node at %p to have prev == NIL, but it has %" PRIxPTR
			 "; context: %s",
			 name,
			 node,
			 (uintptr_t) node->prev,
			 debug_context);
}
#endif

static inline void
XLIST_check_next_link(const XLIST_node *before,
					  const XLIST_node *after,
					  const char *before_name,
					  const char *after_name,
					  const char *debug_context XLIST_CONTEXT_ARG)
{
	if (XLIST_get_next(before XLIST_CONTEXT) != after)
		elog(PANIC,
			 "expected %s node at %p to have next link to %s node at %p, but it has %"
			 PRIxPTR
			 "; context: %s",
			 before_name,
			 before,
			 after_name,
			 after,
			 (uintptr_t) before->next,
			 debug_context);
}

#if defined(XLIST_DLIST)
static inline void
XLIST_check_prev_link(const XLIST_node *before,
					  const XLIST_node *after,
					  const char *before_name,
					  const char *after_name,
					  const char *debug_context XLIST_CONTEXT_ARG)
{
	if (XLIST_get_prev(after XLIST_CONTEXT) != before)
		elog(PANIC,
			 "expected %s node at %p to have prev link to %s node at %p, but it has %"
			 PRIxPTR
			 "; context: %s",
			 after_name,
			 after,
			 before_name,
			 before,
			 (uintptr_t) after->prev,
			 debug_context);
}
#endif

static inline void
XLIST_check_links(const XLIST_head *list,
				  const XLIST_node *before,
				  const XLIST_node *after,
				  const char *debug_context XLIST_CONTEXT_ARG)
{
	if (before == NULL && after == NULL)
	{
		/* Empty list representation. */
#if defined(XLIST_EMPTY_NIL)
		XLIST_check_next_nil(&list->head,
							 "head",
							 debug_context XLIST_CONTEXT);
#elif defined(XLIST_EMPTY_SELF)
		XLIST_check_next_link(&list->head,
							  &list->head,
							  "head",
							  "head",
							  debug_context XLIST_CONTEXT);
#endif

#if defined(XLIST_TAILED)
#if defined(XLIST_EMPTY_NIL)
		XLIST_check_next_nil(&list->tail,
							 "tail",
							 debug_context XLIST_CONTEXT);
#elif defined(XLIST_EMPTY_SELF)
		XLIST_check_next_link(&list->tail,
							  &list->head,
							  "tail",
							  "head",
							  debug_context XLIST_CONTEXT);
#endif
#endif

#if defined(XLIST_DLIST)
#if defined(XLIST_EMPTY_NIL)
		XLIST_check_prev_nil(&list->head,
							 "head",
							 debug_context XLIST_CONTEXT);
#elif defined(XLIST_EMPTY_SELF)
		XLIST_check_prev_link(&list->head,
							  &list->head,
							  "head",
							  "head",
							  debug_context XLIST_CONTEXT);
#endif
#endif
	}
	else if (before == NULL)
	{
		/* Head to first node. */
		XLIST_check_next_link(&list->head,
							  after,
							  "head",
							  "first",
							  debug_context XLIST_CONTEXT);

#if defined(XLIST_DLIST)
#if defined(XLIST_EMPTY_NIL)
		XLIST_check_prev_nil(after,
							 "first",
							 debug_context XLIST_CONTEXT);
#else
		XLIST_check_prev_link(&list->head,
							  after,
							  "head",
							  "first",
							  debug_context XLIST_CONTEXT);
#endif
#endif
	}
	else if (after != NULL)
	{
		/* Tested with each node as before except the final one. */
		XLIST_check_next_link(before,
							  after,
							  "internal",
							  "internal",
							  debug_context XLIST_CONTEXT);

#if defined(XLIST_DLIST)
		XLIST_check_prev_link(before,
							  after,
							  "internal",
							  "internal",
							  debug_context XLIST_CONTEXT);
#endif
	}
	else
	{
		/* Final node. */
#if defined(XLIST_EMPTY_NIL)
		XLIST_check_next_nil(before,
							 "final",
							 debug_context XLIST_CONTEXT);
#else
		XLIST_check_next_link(before,
							  &list->head,
							  "final",
							  "head",
							  debug_context XLIST_CONTEXT);
#endif

#if defined(XLIST_TAILED)
		XLIST_check_next_link(&list->tail,
							  before,
							  "tail",
							  "final",
							  debug_context XLIST_CONTEXT);
#endif

#if defined(XLIST_DLIST)
		XLIST_check_prev_link(before,
							  &list->head,
							  "final",
							  "head",
							  debug_context XLIST_CONTEXT);
#endif
	}
}

/* Internal consistency check. */
static inline void
XLIST_check_contents(const XLIST_head *list,
					 const XLIST_count_t count,
					 const XLIST_node *expect[],
					 const char *debug_message XLIST_CONTEXT_ARG)
{
	if (count == 0)
	{
#if defined(XLIST_INIT_ON_ZERO_MEM)
		if (XLIST_is_zero_mem(list))
			return;
#endif
		XLIST_check_links(list,
						  NULL,
						  NULL,
						  debug_message XLIST_CONTEXT);
		return;
	}
	else
	{
		XLIST_check_links(list,
						  NULL,
						  expect[0],
						  debug_message XLIST_CONTEXT);
		for (int i = 0; i < count - 1; ++i)
			XLIST_check_links(list,
							  expect[i],
							  expect[i + 1],
							  debug_message XLIST_CONTEXT);
		XLIST_check_links(list,
						  expect[count - 1],
						  NULL,
						  debug_message XLIST_CONTEXT);
	}

#if defined(XLIST_COUNTED)
	if (count != XLIST_count(list XLIST_CONTEXT))
		elog(PANIC,
			 CppAsString(XLIST_check_contents) ": expected %" PRIu64
			 " nodes but counter has %" PRIu64,
			 (uint64_t) count,
			 (uint64_t) XLIST_count(list XLIST_CONTEXT));
#endif
}
#endif							/* XLIST_REGRESS */

static inline void
XLIST_check(XLIST_head *list XLIST_CONTEXT_ARG)
{
#ifdef XLIST_DEBUG
	XLIST_count_t count = 0;
	const char *debug_context = CppAsString(XLIST_check);

	if (list == NULL)
		elog(ERROR, "linked list head address is NULL");

#if defined(XLIST_INIT_ON_ZERO_MEM)
	if (XLIST_is_zero_mem(list))
		return;					/* OK, initialized as zeroes */
#endif

	if (XLIST_is_empty(list))
	{
		XLIST_check_links(list, NULL, NULL, debug_context XLIST_CONTEXT);
	}
	else
	{
		XLIST_node *cur = XLIST_head_node(list XLIST_CONTEXT);

		XLIST_check_links(list, NULL, cur, debug_context XLIST_CONTEXT);
		while (XLIST_has_next(list, cur))
		{
			XLIST_node *next = XLIST_next_node(list, cur XLIST_CONTEXT);

			XLIST_check_links(list, cur, next, debug_context XLIST_CONTEXT);
			cur = next;
			count++;
		}
		count++;
		XLIST_check_links(list, cur, NULL, debug_context XLIST_CONTEXT);
	}

#if defined(XLIST_COUNTED)
	if (count != XLIST_count(list XLIST_CONTEXT))
		elog(PANIC,
			 debug_context ": counted %" PRIu64 " nodes but list counter has %"
			 PRIu64,
			 (uint64_t) count,
			 XLIST_count(list XLIST_CONTEXT));
#endif
#endif
}

/* Insternal consistency check. */
static inline void
XLIST_member_check(const XLIST_head *list,
				   const XLIST_node *node XLIST_CONTEXT_ARG)
{
#ifdef XLIST_DEBUG
	const XLIST_node *cur = &list->head;

	while (XLIST_has_next(list, cur))
	{
		cur = XLIST_next_node(list, cur XLIST_CONTEXT);
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
	node->next = XLIST_NIL;
#if defined(XLIST_DLIST)
	node->prev = XLIST_NIL;
#endif
}

/*
 * Is the list empty?
 */
static inline bool
XLIST_is_empty(const XLIST_head *list)
{
#if defined(XLIST_INIT_ON_ZERO_MEM)
	if (XLIST_is_zero_mem(list))
		return true;
#endif

#if defined(XLIST_EMPTY_NIL)
	if (list->head.next == XLIST_NIL)
		return true;
#elif defined(XLIST_EMPTY_SELF)
	if (XLIST_get_next(&list->head) == &list->head)
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
	return XLIST_get_next(node) != &list->head;
#endif
}

static inline XLIST_node *XLIST_prev_node(const XLIST_head *list,
										  const XLIST_node *node XLIST_CONTEXT_ARG);

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
XLIST_next_node(const XLIST_head *list, const XLIST_node *node XLIST_CONTEXT_ARG)
{
	Assert(XLIST_has_next(list, node));
	return XLIST_get_next(node XLIST_CONTEXT);
}

#if defined(XLIST_DLIST) || defined(XLIST_LINEAR)
/*
 * Return previous node in the list (there must be one).
 */
static inline XLIST_node *
XLIST_prev_node(const XLIST_head *list, const XLIST_node *node XLIST_CONTEXT_ARG)
{
#if defined(XLIST_XLIST)
	Assert(XLIST_has_prev(list, node XLIST_CONTEXT));
	return XLIST_get_prev(node XLIST_CONTEXT);
#else
	Assert(!XLIST_is_empty(list));
	for (const XLIST_node *cur = &list->head;
		 XLIST_has_next(list, cur);)
	{
		XLIST_node *next = XLIST_get_next(cur XLIST_CONTEXT);

		if (next == node)
			return unconstify(XLIST_node *, cur);
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
XLIST_head_node(const XLIST_head *list XLIST_CONTEXT_ARG)
{
	Assert(!XLIST_is_empty(list));
	return XLIST_get_next(&list->head XLIST_CONTEXT);
}

#if defined(XLIST_DLIST) || defined(XLIST_TAILED) || defined(XLIST_LINEAR)
/*
 * Return the last node in the list (there must be one).
 */
static inline XLIST_node *
XLIST_tail_node(const XLIST_head *list XLIST_CONTEXT_ARG)
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
	XLIST_member_check(list, node XLIST_CONTEXT);

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

	XLIST_check(list XLIST_CONTEXT);
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

	XLIST_check(list XLIST_CONTEXT);
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
#undef XLIST_DEBUG
#undef XLIST_DECLARE
#undef XLIST_DECLARE_ONLY
#undef XLIST_DEFINE
#undef XLIST_DEFINE_ONLY
#undef XLIST_DLIST
#undef XLIST_INDEX
#undef XLIST_INDEX_TO_NODE
#undef XLIST_INDEX_TO_NODE_EX
#undef XLIST_INIT_ON_ZERO_MEM
#undef XLIST_INIT_REQUIRED
#undef XLIST_LINK_T
#undef XLIST_NIL
#undef XLIST_NODE_TO_INDEX
#undef XLIST_NODE_TO_INDEX_EX
#undef XLIST_OBJECT_MEMBER
#undef XLIST_OBJECT_T
#undef XLIST_PREFIX
#undef XLIST_PTR
#undef XLIST_PTRDIFF
#undef XLIST_REGRESS
#undef XLIST_SLIST
#undef XLIST_TAILED

/* Undefine internal macros. */
#undef XLIST_CONTEXT
#undef XLIST_CONTEXT_ARG
#undef XLIST_EMPTY_NIL
#undef XLIST_EMPTY_SELF
#undef XLIST_MAKE_NAME
#undef XLIST_MAKE_NAME_
#undef XLIST_MAX_PREFIX
#undef XLIST_PTRDIFF_SCALE
#undef XLIST_check
#undef XLIST_check_contents
#undef XLIST_check_links
#undef XLIST_check_next_link
#undef XLIST_check_next_nil
#undef XLIST_check_prev_link
#undef XLIST_check_prev_nil
#undef XLIST_count
#undef XLIST_count_t
#undef XLIST_decrement
#undef XLIST_delete
#undef XLIST_delete_from
#undef XLIST_delete_from_thoroughly
#undef XLIST_delete_thoroughly
#undef XLIST_delete_next
#undef XLIST_follow_link
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
#undef XLIST_is_zero_mem
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
#undef XLIST_read_ptrdiff
#undef XLIST_tail_node

#endif
