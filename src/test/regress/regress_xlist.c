#include "postgres.h"

#include "regress_xlist.h"
#include "executor/functions.h"

/*
 * Main test schedule.  Every configuration of xlist can run this, as long as
 * XLIST_LINEAR or XLIST_DLIST is enabled.
 */
static const struct test_step common_schedule[] = {
	STEP(init, (head), CHECK_LIST_EMPTY()),
	STEP(is_empty, (head), EXPECT(true)),
	STEP(count, (head), EXPECT(0)),
	STEP(push_head, (head, 0), CHECK_LIST(0)),
	STEP(init, (head), CHECK_LIST_EMPTY()),
	STEP(is_empty, (head), EXPECT(true)),
	STEP(count, (head), EXPECT(0)),
	STEP(push_head, (head, 0), CHECK_LIST(0)),
	STEP(is_empty, (head), EXPECT(false)),
	STEP(tail_node, (head), EXPECT_NODE(0)),
	STEP(push_head, (head, 1), CHECK_LIST(1, 0)),
	STEP(insert_into_before, (head, 0, 2), CHECK_LIST(1, 2, 0)),
	STEP(insert_into_before, (head, 1, 3), CHECK_LIST(3, 1, 2, 0)),
	STEP(insert_into_after, (head, 1, 4), CHECK_LIST(3, 1, 4, 2, 0)),
	STEP(insert_into_after, (head, 0, 5), CHECK_LIST(3, 1, 4, 2, 0, 5)),
	STEP(tail_node, (head), EXPECT_NODE(5)),
	STEP(count, (head), EXPECT(6)),
	STEP(has_prev, (head, 3), EXPECT(false)),
	STEP(has_next, (head, 3), EXPECT(true)),
	STEP(has_prev, (head, 1), EXPECT(true)),
	STEP(has_next, (head, 1), EXPECT(true)),
	STEP(has_prev, (head, 5), EXPECT(true)),
	STEP(has_next, (head, 5), EXPECT(false)),
	STEP(head_node, (head), EXPECT_NODE(3)),
	STEP(tail_node, (head), EXPECT_NODE(5)),
	STEP(delete_from, (head, 4), CHECK_LIST(3, 1, 2, 0, 5)),
	STEP(tail_node, (head), EXPECT_NODE(5)),
	STEP(count, (head), EXPECT(5)),
	STEP(delete_from_thoroughly, (head, 5), CHECK_LIST(3, 1, 2, 0)),
	STEP(tail_node, (head), EXPECT_NODE(0)),
	STEP(count, (head), EXPECT(4)),
	STEP(node_is_detached, (5), EXPECT(true)),
	STEP(node_is_detached, (2), EXPECT(false)),
	STEP(delete_from_thoroughly, (head, 3), CHECK_LIST(1, 2, 0)),
	STEP(node_is_detached, (3), EXPECT(true)),
	STEP(delete_from, (head, 2), CHECK_LIST(1, 0)),
	STEP(delete_from_thoroughly, (head, 1), CHECK_LIST(0)),
	STEP(node_is_detached, (1), EXPECT(true)),
	STEP(push_tail, (head, 3), CHECK_LIST(0, 3)),
	STEP(delete_from, (head, 3), CHECK_LIST(0)),
	STEP(delete_from, (head, 0), CHECK_LIST_EMPTY()),
	STEP(push_tail, (head, 2), CHECK_LIST(2)),
	STEP(push_tail, (head, 3), CHECK_LIST(2, 3)),
	STEP(push_head, (head, 1), CHECK_LIST(1, 2, 3)),
	STEP(push_head, (head, 0), CHECK_LIST(0, 1, 2, 3)),
	STEP(move_head, (head, 1), CHECK_LIST(1, 0, 2, 3)),
	STEP(move_tail, (head, 2), CHECK_LIST(1, 0, 3, 2)),
	STEP(pop_tail_node, (head), EXPECT_NODE(2), CHECK_LIST(1, 0, 3)),
	STEP(pop_tail_node, (head), EXPECT_NODE(3), CHECK_LIST(1, 0)),
	STEP(pop_head_node, (head), EXPECT_NODE(1), CHECK_LIST(0)),
	STEP(pop_head_node, (head), EXPECT_NODE(0), CHECK_LIST_EMPTY()),
	STEP(count, (head), EXPECT(0)),
};

#if 0
static const struct test_step slist_schedule[] = {
	{INIT, NOARGS, EXPECT_EMPTY_LIST},
	{IS_EMPTY, NOARGS, EXPECT_TRUE},
	{COUNT, NOARGS, EXPECT_INTEGER(0)},
	{PUSH_HEAD, ARG(0), EXPECT_LIST(0)},
	{IS_EMPTY, NOARGS, EXPECT_FALSE},
	{PUSH_HEAD, ARG(1), EXPECT_LIST(1, 0)},
	{INSERT_INTO_AFTER, ARGS(0, 2), EXPECT_LIST(1, 0, 2)},
	{INSERT_AFTER, ARGS(0, 3), EXPECT_LIST(1, 0, 3, 2)},
	{INSERT_AFTER, ARGS(1, 4), EXPECT_LIST(1, 4, 0, 3, 2)},
	{COUNT, NOARGS, EXPECT_INTEGER(5)},
	{HAS_NEXT, ARG(1), EXPECT_TRUE},
	{HAS_NEXT, ARG(3), EXPECT_TRUE},
	{HAS_NEXT, ARG(2), EXPECT_FALSE},
	{HEAD_NODE, NOARGS, EXPECT_NODE(1)},
	{TAIL_NODE, NOARGS, EXPECT_NODE(2)},
	{PUSH_TAIL, ARG(2), EXPECT_LIST(2)},
	{PUSH_TAIL, ARG(3), EXPECT_LIST(2, 3)},
	{PUSH_HEAD, ARG(1), EXPECT_LIST(1, 2, 3)},
	{PUSH_HEAD, ARG(0), EXPECT_LIST(0, 1, 2, 3)},
	{MOVE_HEAD, ARG(1), EXPECT_LIST(1, 0, 2, 3)},
	{MOVE_TAIL, ARG(2), EXPECT_LIST(1, 0, 3, 2)},
	{POP_TAIL_NODE, NOARGS, EXPECT_NODE_AND_LIST(2, 1, 0, 3)},
	{POP_TAIL_NODE, NOARGS, EXPECT_NODE_AND_LIST(3, 1, 0)},
	{POP_HEAD_NODE, NOARGS, EXPECT_NODE_AND_LIST(1, 0)},
	{POP_HEAD_NODE, NOARGS, EXPECT_NODE_AND_EMPTY_LIST(0)},
	{COUNT, NOARGS, EXPECT_INTEGER(0)},
};
#endif


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
	dlist_ptr_head head = {0};

	Assert(dlist_ptr_is_empty(&head));

	RUN_TESTS(dlist_ptr,
			  CHECK_NEXT_AND_PREV,
			  NO_COUNT,
			  0,
			  0,
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
	dclist_ptr_head head = {0};

	RUN_TESTS(dclist_ptr,
			  CHECK_NEXT_AND_PREV,
			  CHECK_COUNT,
			  0,
			  0,
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

static void
test_dlist_index(void)
{
	struct dlist_index_cont array[8] = {0};
	dlist_index_head head = {0};

	RUN_TESTS(dlist_index,
			  CHECK_NEXT_AND_PREV_NIL,
			  NO_COUNT,
			  1,
			  array,
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
	Assert(array[3].node.next == sizeof(array[3]));
	Assert(dlist_ptrdiff_get_next(&array[3].node) == &array[4].node);
	/* ... and for prev. */
	dlist_ptrdiff_set_prev(&array[3].node, &array[2].node);
	Assert(array[3].node.prev == -sizeof(array[3]));
	Assert(dlist_ptrdiff_get_prev(&array[3].node) == &array[2].node);

	/* Pointing next to next-of(node) adjusts the delta correctly. */
	dlist_ptrdiff_set_next_to_next_of(&array[5].node, &array[3].node);
	Assert(array[5].node.next == -sizeof(array[5]));
	Assert(dlist_ptrdiff_get_next(&array[5].node) == &array[4].node);
	/* ... and for prev-of(node). */
	dlist_ptrdiff_set_prev_to_prev_of(&array[5].node, &array[3].node);
	Assert(array[5].node.prev == -sizeof(array[5]) * (5 - 2));
	Assert(dlist_ptrdiff_get_prev(&array[5].node) == &array[2].node);
}

static void
test_dlist_ptrdiff(void)
{
	struct dlist_ptrdiff_cont array[8] = {0};
	dlist_ptrdiff_head head = {0};

	RUN_TESTS(dlist_ptrdiff,
			  CHECK_NEXT_AND_PREV,
			  NO_COUNT,
			  0,
			  0,
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
	slist_ptr_head head = {0};

	Assert(slist_ptr_is_empty(&head));

	RUN_TESTS(slist_ptr,
			  CHECK_NEXT,
			  NO_COUNT,
			  0,
			  0,
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
	stlist_ptr_head head = {0};

	Assert(stlist_ptr_is_empty(&head));

	RUN_TESTS(stlist_ptr,
			  CHECK_NEXT_AND_TAIL,
			  NO_COUNT,
			  0,
			  0,
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
	sclist_ptr_head head = {0};

	Assert(sclist_ptr_is_empty(&head));

	RUN_TESTS(sclist_ptr,
			  CHECK_NEXT,
			  NO_COUNT,
			  0,
			  0,
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
	slist_index_head head = {0};

	RUN_TESTS(slist_index,
			  CHECK_NEXT_NIL,
			  NO_COUNT,
			  1,
			  array,
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
	stlist_index_head head = {0};

	RUN_TESTS(stlist_index,
			  CHECK_NEXT_AND_TAIL_NIL,
			  NO_COUNT,
			  1,
			  array,
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
