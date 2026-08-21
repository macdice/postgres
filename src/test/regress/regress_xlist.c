#include "postgres.h"

#include "regress_xlist.h"
#include "executor/functions.h"

/*
 * Main test schedule.  Every configuration of xlist can run this, as long as
 * XLIST_LINEAR or XLIST_DLIST is enabled.
 */
static const struct test_step common_schedule[] = {
	TEST(init, (list), LIST_EMPTY()),
	TEST(is_empty, (list), RETURNS(true)),
	TEST(count, (list), RETURNS(0)),
	TEST(push_head, (list, 0), LIST(0)),
	TEST(init, (list), LIST_EMPTY()),
	TEST(is_empty, (list), RETURNS(true)),
	TEST(count, (list), RETURNS(0)),
	TEST(push_head, (list, 0), LIST(0)),
	TEST(is_empty, (list), RETURNS(false)),
	TEST(tail_node, (list), RETURNS_NODE(0)),
	TEST(push_head, (list, 1), LIST(1, 0)),
	TEST(insert_into_before, (list, 0, 2), LIST(1, 2, 0)),
	TEST(insert_into_before, (list, 1, 3), LIST(3, 1, 2, 0)),
	TEST(insert_into_after, (list, 1, 4), LIST(3, 1, 4, 2, 0)),
	TEST(insert_into_after, (list, 0, 5), LIST(3, 1, 4, 2, 0, 5)),
	TEST(tail_node, (list), RETURNS_NODE(5)),
	TEST(count, (list), RETURNS(6)),
	TEST(has_prev, (list, 3), RETURNS(false)),
	TEST(has_next, (list, 3), RETURNS(true)),
	TEST(has_prev, (list, 1), RETURNS(true)),
	TEST(has_next, (list, 1), RETURNS(true)),
	TEST(has_prev, (list, 5), RETURNS(true)),
	TEST(has_next, (list, 5), RETURNS(false)),
	TEST(head_node, (list), RETURNS_NODE(3)),
	TEST(tail_node, (list), RETURNS_NODE(5)),
	TEST(delete_from, (list, 4), LIST(3, 1, 2, 0, 5)),
	TEST(tail_node, (list), RETURNS_NODE(5)),
	TEST(count, (list), RETURNS(5)),
	TEST(delete_from_thoroughly, (list, 5), LIST(3, 1, 2, 0)),
	TEST(tail_node, (list), RETURNS_NODE(0)),
	TEST(count, (list), RETURNS(4)),
	TEST(node_is_detached, (5), RETURNS(true)),
	TEST(node_is_detached, (2), RETURNS(false)),
	TEST(delete_from_thoroughly, (list, 3), LIST(1, 2, 0)),
	TEST(node_is_detached, (3), RETURNS(true)),
	TEST(delete_from, (list, 2), LIST(1, 0)),
	TEST(delete_from_thoroughly, (list, 1), LIST(0)),
	TEST(node_is_detached, (1), RETURNS(true)),
	TEST(push_tail, (list, 3), LIST(0, 3)),
	TEST(delete_from, (list, 3), LIST(0)),
	TEST(delete_from, (list, 0), LIST_EMPTY()),
	TEST(push_tail, (list, 2), LIST(2)),
	TEST(push_tail, (list, 3), LIST(2, 3)),
	TEST(push_head, (list, 1), LIST(1, 2, 3)),
	TEST(push_head, (list, 0), LIST(0, 1, 2, 3)),
	TEST(move_head, (list, 1), LIST(1, 0, 2, 3)),
	TEST(move_tail, (list, 2), LIST(1, 0, 3, 2)),
	TEST(pop_tail_node, (list), RETURNS_NODE(2), LIST(1, 0, 3)),
	TEST(pop_tail_node, (list), RETURNS_NODE(3), LIST(1, 0)),
	TEST(pop_head_node, (list), RETURNS_NODE(1), LIST(0)),
	TEST(pop_head_node, (list), RETURNS_NODE(0), LIST_EMPTY()),
	TEST(count, (list), RETURNS(0)),
};


#define XLIST_PREFIX dlist_ptr
#define XLIST_DLIST
#define XLIST_PTR
#define XLIST_DECLARE
#define XLIST_DEFINE
#include "lib/xlist_template.h"

struct dlist_ptr_cont
{
	int			dummy;
	dlist_ptr_node node;
};

static void
test_dlist_ptr(void)
{
	struct dlist_ptr_cont array[8] = {0};
	dlist_ptr_head list = {0};

	Assert(dlist_ptr_is_empty(&list));

	RUN_TESTS(dlist_ptr,
			  CHECK_NEXT_AND_PREV,
			  NO_COUNT,
			  CONTEXT_NONE,
			  common_schedule,
			  COMMON);
}


#define XLIST_PREFIX dclist_ptr
#define XLIST_DLIST
#define XLIST_COUNTED
#define XLIST_PTR
#define XLIST_DECLARE
#define XLIST_DEFINE
#include "lib/xlist_template.h"

struct dclist_ptr_cont
{
	int			dummy;
	dclist_ptr_node node;
};

static void
test_dclist_ptr(void)
{
	struct dclist_ptr_cont array[8] = {0};
	dclist_ptr_head list = {0};

	RUN_TESTS(dclist_ptr,
			  CHECK_NEXT_AND_PREV,
			  CHECK_COUNT,
			  CONTEXT_NONE,
			  common_schedule,
			  COMMON);
}


#define XLIST_PREFIX dlist_index
#define XLIST_DLIST
#define XLIST_INDEX
#define XLIST_OBJECT_T struct dlist_index_cont
#define XLIST_OBJECT_MEMBER node
#define XLIST_DECLARE
#include "lib/xlist_template.h"

struct dlist_index_cont
{
	int			dummy;
	dlist_index_node node;
};

#define XLIST_DEFINE
#include "lib/xlist_template.h"

/* Functions declared to take "opt_context" will receive this. */
#define DLIST_INDEX_CONTEXT() , &array[0].node, sizeof(array[0])

static void
test_dlist_index(void)
{
	struct dlist_index_cont array[8] = {0};
	dlist_index_head list = {0};

	RUN_TESTS(dlist_index,
			  CHECK_NEXT_AND_PREV_NIL,
			  NO_COUNT,
			  CONTEXT_ARRAY_AND_SIZE,
			  common_schedule,
			  COMMON);
}


#define XLIST_PREFIX dlist_ptrdiff
#define XLIST_DLIST
#define XLIST_PTRDIFF
#define XLIST_DECLARE
#define XLIST_DEFINE
#include "lib/xlist_template.h"

struct dlist_ptrdiff_cont
{
	int			dummy;
	dlist_ptrdiff_node node;
};

static void
test_dlist_ptrdiff_arithmetic(void)
{
#ifdef USE_ASSERT_CHECKING
	size_t scale = alignof(dlist_ptrdiff_node);
#endif
	struct dlist_ptrdiff_cont array[8] = {0};

	/* Pointing next to the node itself stores 0. */
	dlist_ptrdiff_set_next(&array[3].node, &array[3].node);
	Assert(array[3].node.next == 0);
	/* ... and same for prev. */
	dlist_ptrdiff_set_prev(&array[3].node, &array[3].node);
	Assert(array[3].node.prev == 0);

	/* Retrieving next/prev holding 0 returns self. */
	Assert(dlist_ptrdiff_get_next(&array[3].node) == &array[3].node);
	Assert(dlist_ptrdiff_get_prev(&array[3].node) == &array[3].node);

	/* Pointing next to adjacent object stores object size delta. */
	dlist_ptrdiff_set_next(&array[3].node, &array[4].node);
	Assert(array[3].node.next == sizeof(array[3]) / scale);
	Assert(dlist_ptrdiff_get_next(&array[3].node) == &array[4].node);
	/* ... and for prev. */
	dlist_ptrdiff_set_prev(&array[3].node, &array[2].node);
	Assert(array[3].node.prev == -(sizeof(array[3]) / scale));
	Assert(dlist_ptrdiff_get_prev(&array[3].node) == &array[2].node);

	/* Pointing next to next-of(node) adjusts the delta correctly. */
	dlist_ptrdiff_set_next_to_next_of(&array[5].node, &array[3].node);
	Assert(array[5].node.next == -(sizeof(array[5]) / scale));
	Assert(dlist_ptrdiff_get_next(&array[5].node) == &array[4].node);
	/* ... and for prev-of(node). */
	dlist_ptrdiff_set_prev_to_prev_of(&array[5].node, &array[3].node);
	Assert(array[5].node.prev == -((sizeof(array[5]) * (5 - 2)) / scale));
	Assert(dlist_ptrdiff_get_prev(&array[5].node) == &array[2].node);
}

static void
test_dlist_ptrdiff(void)
{
	struct dlist_ptrdiff_cont array[8] = {0};
	dlist_ptrdiff_head list = {0};

	RUN_TESTS(dlist_ptrdiff,
			  CHECK_NEXT_AND_PREV,
			  NO_COUNT,
			  CONTEXT_NONE,
			  common_schedule,
			  COMMON);
}

#define XLIST_PREFIX slist_ptr
#define XLIST_SLIST
#define XLIST_LINEAR
#define XLIST_PTR
#define XLIST_DECLARE
#define XLIST_DEFINE
#include "lib/xlist_template.h"

struct slist_ptr_cont
{
	int			dummy;
	slist_ptr_node node;
};

static void
test_slist_ptr(void)
{
	struct slist_ptr_cont array[8] = {0};
	slist_ptr_head list = {0};

	Assert(slist_ptr_is_empty(&list));

	RUN_TESTS(slist_ptr,
			  CHECK_NEXT,
			  NO_COUNT,
			  CONTEXT_NONE,
			  common_schedule,
			  COMMON);
}

#define XLIST_PREFIX stlist_ptr
#define XLIST_SLIST
#define XLIST_TAILED
#define XLIST_LINEAR
#define XLIST_PTR
#define XLIST_DECLARE
#define XLIST_DEFINE
#include "lib/xlist_template.h"

struct stlist_ptr_cont
{
	int			dummy;
	stlist_ptr_node node;
};

static void
test_stlist_ptr(void)
{
	struct stlist_ptr_cont array[8] = {0};
	stlist_ptr_head list = {0};

	Assert(stlist_ptr_is_empty(&list));

	RUN_TESTS(stlist_ptr,
			  CHECK_NEXT_AND_TAIL,
			  NO_COUNT,
			  CONTEXT_NONE,
			  common_schedule,
			  COMMON);
}

#define XLIST_PREFIX sclist_ptr
#define XLIST_SLIST
#define XLIST_COUNTED
#define XLIST_LINEAR
#define XLIST_PTR
#define XLIST_DECLARE
#define XLIST_DEFINE
#include "lib/xlist_template.h"

struct sclist_ptr_cont
{
	int			dummy;
	sclist_ptr_node node;
};

static void
test_sclist_ptr(void)
{
	struct sclist_ptr_cont array[8] = {0};
	sclist_ptr_head list = {0};

	Assert(sclist_ptr_is_empty(&list));

	RUN_TESTS(sclist_ptr,
			  CHECK_NEXT,
			  NO_COUNT,
			  CONTEXT_NONE,
			  common_schedule,
			  COMMON);
}

#define XLIST_PREFIX slist_index
#define XLIST_SLIST
#define XLIST_INDEX
#define XLIST_OBJECT_T struct slist_index_cont
#define XLIST_OBJECT_MEMBER node
#define XLIST_DECLARE
#include "lib/xlist_template.h"

struct slist_index_cont
{
	int			dummy;
	slist_index_node node;
};

#define XLIST_DEFINE
#include "lib/xlist_template.h"

static void
test_slist_index(void)
{
	struct slist_index_cont array[8] = {0};
	slist_index_head list = {0};

	RUN_TESTS(slist_index,
			  CHECK_NEXT_NIL,
			  NO_COUNT,
			  CONTEXT_ARRAY_AND_SIZE,
			  common_schedule,
			  COMMON);
}

#define XLIST_PREFIX stlist_index
#define XLIST_SLIST
#define XLIST_TAILED
#define XLIST_INDEX
#define XLIST_OBJECT_T struct stlist_index_cont
#define XLIST_OBJECT_MEMBER node
#define XLIST_DECLARE
#include "lib/xlist_template.h"

struct stlist_index_cont
{
	int			dummy;
	stlist_index_node node;
};

#define XLIST_DEFINE
#include "lib/xlist_template.h"

static void
test_stlist_index(void)
{
	struct stlist_index_cont array[8] = {0};
	stlist_index_head list = {0};

	RUN_TESTS(stlist_index,
			  CHECK_NEXT_AND_TAIL_NIL,
			  NO_COUNT,
			  CONTEXT_ARRAY_AND_SIZE,
			  common_schedule,
			  COMMON);
}

PG_FUNCTION_INFO_V1(test_xlist);

Datum
test_xlist(PG_FUNCTION_ARGS)
{
	test_dlist_ptrdiff_arithmetic();

	test_dlist_ptr();
	test_dlist_index();
	test_dlist_ptrdiff();

	test_dclist_ptr();

	test_slist_ptr();
	test_stlist_ptr();
	test_sclist_ptr();

	test_slist_index();
	test_stlist_index();

	PG_RETURN_NULL();
}
