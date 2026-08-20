#ifndef REGRESS_XLIST_H
#define REGRESS_XLIST_H

/* X-macro for ops always available assuming XLIST_LINEAR fallbacks. */
#define FOR_EACH_COMMON_OP(do, ...)								\
	do(count, int (head, opt_context), __VA_ARGS__)					\
	do(delete_from, void (head, node, opt_context), __VA_ARGS__)	\
	do(delete_from_thoroughly, void (head, node, opt_context), __VA_ARGS__) \
	do(has_next, int (head, node), __VA_ARGS__)					\
	do(has_prev, int (head, node, opt_context), __VA_ARGS__)					\
	do(head_node, node (head, opt_context), __VA_ARGS__)					\
	do(init, void (head), __VA_ARGS__)							\
	do(insert_into_after, void (head, node, node, opt_context), __VA_ARGS__)	\
	do(insert_into_before, void (head, node, node, opt_context), __VA_ARGS__)	\
	do(is_empty, int (head), __VA_ARGS__)				\
	do(move_head, void (head, node, opt_context), __VA_ARGS__)			\
	do(move_tail, void (head, node, opt_context), __VA_ARGS__)	\
	do(node_is_detached, int (node), __VA_ARGS__)									\
	do(pop_head_node, node (head, opt_context), __VA_ARGS__)	\
	do(pop_tail_node, node (head, opt_context), __VA_ARGS__) \
	do(push_head, void (head, node, opt_context), __VA_ARGS__) \
	do(push_tail, void (head, node, opt_context), __VA_ARGS__) \
	do(tail_node, node (head, opt_context), __VA_ARGS__)

/* X-macro for all ops. */
#define FOR_EACH_OP(do, ...)			\
	FOR_EACH_COMMON_OP(do, __VA_ARGS__)

/* All ops. */
typedef enum op_name
{
#define EXPAND_ENUM(name, ...) OP_##name,
	FOR_EACH_OP(EXPAND_ENUM)
}			op_name;

/* Convert enum to string for debug messages. */
static inline const char *
op_name_to_string(op_name op)
{
	switch (op)
	{
#define EXPAND_OP_NAME_STRING_CASE(f_name, ...) case OP_##f_name: return #f_name;
			FOR_EACH_OP(EXPAND_OP_NAME_STRING_CASE);
	}
}

/* Argument list magic. */
#define CAT(a, b) CppConcat(a, b)
#define GET1(_1, ...) _1
#define GET2(_1, _2, ...) _2
#define GET3(_1, _2, _3, ...) _3
#define DROP(n, ...) CAT(DROP_, n)(__VA_ARGS__)
#define DROP_1(_1, ...) __VA_ARGS__
#define DROP_2(_1, _2, ...) __VA_ARGS__
#define DROP_3(_1, _2, _3, ...) __VA_ARGS__
#define DROP_4(_1, _2, _3, _4, ...) __VA_ARGS__
#define LAST(...) DROP(VA_ARGS_NARGS(__VA_ARGS__), dummy, __VA_ARGS__)
#define DROP_LAST(...) CAT(DROP_LAST_, VA_ARGS_NARGS(__VA_ARGS__))(__VA_ARGS__)
#define DROP_LAST_1(_1)
#define DROP_LAST_2(_1, _2) _1
#define DROP_LAST_3(_1, _2, _3) _1, _2
#define DROP_LAST_4(_1, _2, _3, _4) _1, _2, _3

/*
 * Expand function type to comma-sepraated list of the form:
 *
 * takes_opt_context, return_type, nargs, arg_type...
 *
 * Example: void(head, node, opt_context) -> 1, void, 2, head, node
 *
 * Example: int(node) -> 0, int, 1, node
 */
#define DECODE_F_TYPE(f_type) DECODE_F_TYPE_##f_type
#define DECODE_F_TYPE_void(...) DECODE_F_TYPE_(void, __VA_ARGS__)
#define DECODE_F_TYPE_int(...) DECODE_F_TYPE_(int, __VA_ARGS__)
#define DECODE_F_TYPE_node(...) DECODE_F_TYPE_(node, __VA_ARGS__)
#define DECODE_F_TYPE_(return_type, ...)								\
	CAT(DECODE_F_TYPE__, LAST(__VA_ARGS__))(return_type, __VA_ARGS__)
#define DECODE_F_TYPE__opt_context(return_type, ...)			\
	DECODE_F_TYPE___(1, return_type, DROP_LAST(__VA_ARGS__))
#define DECODE_F_TYPE__head(return_type, ...)		\
	DECODE_F_TYPE___(0, return_type, __VA_ARGS__)
#define DECODE_F_TYPE__node(return_type, ...)		\
	DECODE_F_TYPE___(0, return_type, __VA_ARGS__)
#define DECODE_F_TYPE___(have_opt_context, return_type, ...)			\
	have_opt_context, return_type, VA_ARGS_NARGS(__VA_ARGS__), __VA_ARGS__

/* One step in a test schedule. */
typedef struct test_step
{
	struct
	{
		op_name		name;
		int			nargs;
		int			args[3];
	}			op;
	struct
	{
		bool		check;
		bool		is_node;
		int			value;
	}			result;
	struct
	{
		bool		check;
		int			count;
		int			order[8];
	}			list_contents;
}			test_step;

/* Macros for writing the schedule of tests. */
#define STEP(f_name, args, ...) \
	{CALL(f_name, REMOVE_PARENS(args)), __VA_ARGS__}
#define REMOVE_PARENS(arg) REMOVE_PARENS_ arg
#define REMOVE_PARENS_(...) __VA_ARGS__
#define CALL(f_name, ...) .op = {										\
		.name = OP_##f_name,											\
		.nargs = VA_ARGS_NARGS(__VA_ARGS__),							\
		.args = CALL_ARGS(__VA_ARGS__)									\
	}
#define CALL_ARGS(...) CAT(CALL_ARGS_, VA_ARGS_NARGS(__VA_ARGS__))(__VA_ARGS__)
#define CALL_ARGS_1(_1) { CALL_ARG(_1) }
#define CALL_ARGS_2(_1, _2) { CALL_ARG(_1), CALL_ARG(_2) }
#define CALL_ARGS_3(_1, _2, _3) { CALL_ARG(_1), CALL_ARG(_2), CALL_ARG(_3) }
#define CALL_ARG(arg) CALL_ARG__(arg, CALL_ARG__##arg)
#define CALL_ARG__head ,		/* matching "head" gets you a longer arg
								 * list... */
#define CALL_ARG__(...) CAT(CALL_ARG___, VA_ARGS_NARGS(__VA_ARGS__))(__VA_ARGS__)
#define CALL_ARG___3(arg, ...) -1	/* saw "head", replace with -1 */
#define CALL_ARG___2(arg, ...) arg	/* otherwise node index */
#define EXPECT(v) .result = {.check = true, .value = v}
#define EXPECT_NODE(v) .result = {.check = true, .is_node = true, .value = v}
#define CHECK_LIST(...) .list_contents = {.check = true,				\
										  .count = VA_ARGS_NARGS(__VA_ARGS__), \
										  .order = {__VA_ARGS__}}
#define CHECK_LIST_EMPTY() .list_contents = {.check = true, .count = 0}

/* Counted lists can check the _count() function when CHECK_LIST() is present. */
#define CHECK_COUNT(count, expected_count) Assert((count) == (expected_count))
#define NO_COUNT(...)

static inline void
check_args(const char *f_name, int nargs, const test_step * step)
{
	if (step->op.nargs != nargs)
		elog(PANIC,
			 "expected %d arguments but got %d for function %s",
			 nargs, step->op.nargs, f_name);
}

/* Explan X-macro to a switch case that calls the function. */
#define EXPAND_CASE(f_name, f_type, prefix, have_context, context)	\
	EXPAND_CASE_(f_name, prefix, have_context, context, DECODE_F_TYPE(f_type))
#define EXPAND_CASE_(...) EXPAND_CASE__(__VA_ARGS__)
#define EXPAND_CASE__(f_name,											\
					  prefix,											\
					  have_context,										\
					  context,											\
					  takes_opt_context,								\
					  return_type,										\
					  nargs,											\
					  ...)												\
	case OP_##f_name:													\
	check_args(#f_name, nargs, step);									\
		EXPAND_CASE_RETURN_TYPE_##return_type							\
			prefix##_##f_name(EXPAND_CASE_ARGS(nargs, __VA_ARGS__)		\
							  EXPAND_CASE_ARG_OPT_CONTEXT(takes_opt_context, \
														  have_context,	\
														  context));	\
		break;
#define EXPAND_CASE_RETURN_TYPE_void
#define EXPAND_CASE_RETURN_TYPE_int result =
#define EXPAND_CASE_RETURN_TYPE_node node =
#define EXPAND_CASE_ARGS(nargs, ...) EXPAND_CASE_ARGS_##nargs(__VA_ARGS__)
#define EXPAND_CASE_ARGS_1(_1) EXPAND_CASE_ARG(_1, 0)
#define EXPAND_CASE_ARGS_2(_1, _2) EXPAND_CASE_ARG(_1, 1),	\
		EXPAND_CASE_ARG(_2, 1)
#define EXPAND_CASE_ARGS_3(_1, _2, _3) EXPAND_CASE_ARG(_1, 0),	\
		EXPAND_CASE_ARG(_2, 1), EXPAND_CASE_ARG(_3, 2)
#define EXPAND_CASE_ARG(type, pos) EXPAND_CASE_ARG_TYPE_##type(pos)
#define EXPAND_CASE_ARG_TYPE_head(pos) &head
#define EXPAND_CASE_ARG_TYPE_node(pos) &array[args[pos]].node
#define EXPAND_CASE_ARG_OPT_CONTEXT(takes_opt_context,					\
									have_context,						\
									context)							\
	EXPAND_CASE_ARG_OPT_CONTEXT_##takes_opt_context##_##have_context(context)
#define EXPAND_CASE_ARG_OPT_CONTEXT_0_0(context)
#define EXPAND_CASE_ARG_OPT_CONTEXT_0_1(context)
#define EXPAND_CASE_ARG_OPT_CONTEXT_1_0(context)
#define EXPAND_CASE_ARG_OPT_CONTEXT_1_1(context) , (context)

/* Special node indexes used in this test code. */
#define LIST_NIL -1
#define LIST_HEAD -2
#define LIST_TERMINATOR -3		/* NIL or HEAD as appropriate */
#define LIST_TAIL -4

#define RUN_TESTS(prefix,												\
				  check_link,											\
				  check_count,											\
				  have_context,											\
				  context,												\
				  steps,												\
				  ops)													\
	for (int step_number = 0;											\
		 step_number < lengthof(steps);									\
		 ++step_number)													\
	{																	\
		const test_step *step = &steps[step_number];					\
		const int *args = step->op.args;								\
		prefix##_node *node = NULL;										\
		int result = 0;													\
																		\
		switch (step->op.name)											\
		{																\
			FOR_EACH_##ops##_OP(EXPAND_CASE,							\
								prefix,									\
								have_context,							\
								context);								\
		default:														\
			elog(PANIC, "unknown op");									\
		}																\
																		\
		if (step->result.check)											\
		{																\
			if (step->result.is_node)									\
			{															\
				prefix##_node *expected =								\
					&array[step->result.value].node;					\
																		\
				if (node != expected)									\
					elog(PANIC,											\
						 "step %d with prefix %s: expected node #%d but got node #%d", \
						 step_number,									\
						 #prefix,										\
						 step->result.value,							\
						 (int) (((char *) node -						\
								 (char *) &array[0].node) /				\
								sizeof(array[0])));						\
			}															\
			else														\
			{															\
				if (result != step->result.value)						\
					elog(PANIC,											\
						 "step %d with prefix %s: expected %d but got %d", \
						 step_number,									\
						 #prefix,										\
						 step->result.value,							\
						 result);										\
			}															\
		}																\
																		\
		if (step->list_contents.check)									\
		{																\
			check_count(prefix##_count(&head),							\
						step->list_contents.count);						\
																		\
			if (step->list_contents.count == 0)							\
			{															\
				/* Empty. */											\
				check_link(prefix,										\
						   step_number,									\
						   LIST_TERMINATOR,								\
						   LIST_TERMINATOR,								\
						   have_context,								\
						   context);									\
			}															\
			else														\
			{															\
				int count = step->list_contents.count;					\
				const int *order = step->list_contents.order;			\
																		\
				/* First link. */										\
				check_link(prefix,										\
						   step_number,									\
						   LIST_TERMINATOR,								\
						   order[0],									\
						   have_context,								\
						   context);									\
																		\
				/* Internal links. */									\
				for (int i = 0; i < count - 1; ++i)						\
					check_link(prefix,									\
							   step_number,								\
							   order[i],								\
							   order[i + 1],							\
							   have_context,							\
							   context);								\
																		\
				/* Last link. */										\
				check_link(prefix,										\
						   step_number,									\
						   order[count - 1],							\
						   LIST_TERMINATOR,								\
						   have_context,								\
						   context);									\
			}															\
		}																\
	}

#define MAYBE_CONTEXT(have_context, context)	\
	MAYBE_CONTEXT_##have_context(context)
#define MAYBE_CONTEXT_0(context)
#define MAYBE_CONTEXT_1(context) , (context)

#define GET_NODE(node_index)											\
	((node_index) < LIST_NIL ? &head.head : &array[(node_index)].node)
#define GET_NEXT(prefix, node_index, have_context, context)			\
	prefix##_get_next(GET_NODE(node_index)							\
					  MAYBE_CONTEXT(have_context, context))
#define GET_PREV(prefix, node_index, have_context, context)			\
	prefix##_get_prev(GET_NODE(node_index)							\
					  MAYBE_CONTEXT(have_context, context))
#define GET_INDEX(prefix, node_p)										\
	(node_p == &head.head ?												\
	 LIST_HEAD :														\
	 (((char *) (node_p) - (char *) &array[0].node) / sizeof(array[0])))

static inline const char *
describe_node(char *buffer, size_t size, int node_index)
{
	if (node_index == LIST_HEAD)
		return "head node";

	if (node_index == LIST_NIL)
		return "NIL";

	if (node_index == LIST_TAIL)
		return "slist tail";

	if (node_index == LIST_TERMINATOR)
		return "terminator";

	snprintf(buffer, size, "node %d", node_index);
	return buffer;
}

static inline void
report_bad_link(const char *prefix,
				int step,
				const char *link,
				int node1,
				int expect_node2,
				int got_node2)
{
	char		node1_buf[16];
	char		expect_node2_buf[16];
	char		got_node2_buf[16];
	const char *node1_str;
	const char *expect_node2_str;
	const char *got_node2_str;

	node1_str = describe_node(node1_buf, sizeof(node1_buf), node1);
	expect_node2_str = describe_node(expect_node2_buf,
									 sizeof(expect_node2_buf),
									 expect_node2);
	got_node2_str = describe_node(got_node2_buf,
								  sizeof(got_node2_buf),
								  got_node2);

	elog(PANIC,
		 "prefix %s, step %d: expected %s's %s to point to %s, not %s",
		 prefix,
		 step,
		 node1_str,
		 link,
		 expect_node2_str,
		 got_node2_str);
}

#define CHECK_NEXT(prefix,												\
				   step,												\
				   node1,												\
				   node2,												\
				   have_context,										\
				   context)												\
	if (GET_NEXT(prefix, (node1), have_context, context) !=				\
		GET_NODE(node2))												\
		report_bad_link(#prefix,										\
						(step),											\
						"next",											\
						(node1),										\
						(node2),										\
						GET_INDEX(prefix,								\
								  GET_NEXT(prefix,						\
										   (node1),						\
										   have_context,				\
										   context)))

#define CHECK_PREV(prefix,												\
				   step,												\
				   node1,												\
				   node2,												\
				   have_context,										\
				   context)												\
	if (GET_PREV(prefix, (node2), have_context, context) !=				\
		GET_NODE(node1))												\
		report_bad_link(#prefix,										\
						(step),											\
						"prev",											\
						(node2),										\
						(node1),										\
						GET_INDEX(prefix,								\
								  GET_NEXT(prefix,						\
										   (node2),						\
										   have_context,				\
										   context)))

#define CHECK_NEXT_NIL(prefix,											\
					   step,											\
					   node1,											\
					   node2,											\
					   have_context,									\
					   context)											\
	if ((node1) == LIST_TERMINATOR && (node2) == LIST_TERMINATOR)		\
	{																	\
		/* Expect empty head with NIL. */								\
		if (!prefix##_next_is_nil(&head.head))							\
			report_bad_link(#prefix,									\
							(step),										\
							"next",										\
							LIST_HEAD,									\
							LIST_NIL,									\
							head.head.next);							\
	}																	\
	else if ((node2) == LIST_TERMINATOR)								\
	{																	\
		/* Expect last node with NIL as next. */						\
		if (!prefix##_next_is_nil(&array[(node1)].node))				\
			report_bad_link(#prefix,									\
							(step),										\
							"next",										\
							(node1),									\
							LIST_NIL,									\
							array[(node1)].node.next);					\
	}																	\
	else if ((node1) == LIST_TERMINATOR)								\
	{																	\
		/* Expect head to point to first node. */						\
		if (prefix##_next_is_nil(&head.head))							\
			report_bad_link(#prefix,									\
							(step),										\
							"next",										\
							LIST_HEAD,									\
							(node2),									\
							LIST_NIL);									\
	}																	\
	else																\
	{																	\
		/* Expect internal next link. */								\
		CHECK_NEXT(prefix,												\
				   (step),												\
				   (node1),												\
				   (node2),												\
				   have_context,										\
				   context);											\
	}																	\

#define CHECK_PREV_NIL(prefix,											\
					   step,											\
					   node1,											\
					   node2,											\
					   have_context,									\
					   context)											\
	if ((node1) == LIST_TERMINATOR && (node2) == LIST_TERMINATOR)		\
	{																	\
		/* Expect empty head with NIL. */								\
		if (!prefix##_prev_is_nil(&head.head))							\
			report_bad_link(#prefix,									\
							(step),										\
							"prev",										\
							LIST_HEAD,									\
							LIST_NIL,									\
							head.head.prev);							\
	}																	\
	else if ((node1) == LIST_TERMINATOR)								\
	{																	\
		/* Expect first node with NIL as previous. */					\
		if (!prefix##_prev_is_nil(&array[(node2)].node))				\
			report_bad_link(#prefix,									\
							(step),										\
							"prev",										\
							(node2),									\
							LIST_NIL,									\
							array[(node2)].node.prev);					\
	}																	\
	else if ((node2) == LIST_TERMINATOR)								\
	{																	\
		/* Expect tail to point to last node. */						\
		if (prefix##_prev_is_nil(&head.head))							\
			report_bad_link(#prefix,									\
							(step),										\
							"prev",										\
							LIST_HEAD,									\
							(node1),									\
							LIST_NIL);									\
	}																	\
	else																\
	{																	\
		/* Expect internal prev link. */								\
		CHECK_PREV(prefix,												\
				   (step),												\
				   (node1),												\
				   (node2),												\
				   have_context,										\
				   context);											\
	}																	\

#define CHECK_NEXT_AND_PREV(prefix,										\
							step,										\
							node1,										\
							node2,										\
							have_context,								\
							context)									\
	do																	\
	{																	\
		CHECK_NEXT(prefix,												\
				   (step),												\
				   (node1),												\
				   (node2),												\
				   have_context,										\
				   context);											\
		CHECK_PREV(prefix,												\
				   (step),												\
				   (node1),												\
				   (node2),												\
				   have_context,										\
				   context);											\
	}																	\
	while (0)

#define CHECK_NEXT_AND_TAIL(prefix,										\
							step,										\
							node1,										\
							node2,										\
							have_context,								\
							context)									\
	do																	\
	{																	\
		CHECK_NEXT(prefix,												\
				   (step),												\
				   (node1),												\
				   (node2),												\
				   have_context,										\
				   context);											\
		if ((node2) == LIST_TERMINATOR)									\
		{																\
			if ((node1) == LIST_TERMINATOR)								\
			{															\
				if (prefix##_get_next(&head.tail						\
									  MAYBE_CONTEXT(have_context,		\
													context)) !=		\
					&head.head)											\
					report_bad_link(#prefix,							\
									(step),								\
									"next",								\
									LIST_TAIL,							\
									LIST_HEAD,							\
									GET_INDEX(prefix, head.tail.next));	\
			}															\
			else														\
			{															\
				if (prefix##_get_next(&head.tail						\
									  MAYBE_CONTEXT(have_context,		\
													context)) !=		\
					GET_NODE(node1))									\
					report_bad_link(#prefix,							\
									(step),								\
									"next",								\
									LIST_TAIL,							\
									(node1),							\
									GET_INDEX(prefix, head.tail.next));	\
			}															\
		}																\
	}																	\
	while (0)

#define CHECK_NEXT_AND_TAIL_NIL(prefix,									\
								step,									\
								node1,									\
								node2,									\
								have_context,							\
								context)								\
	do																	\
	{																	\
		CHECK_NEXT_NIL(prefix,											\
					   (step),											\
					   (node1),											\
					   (node2),											\
					   have_context,									\
					   context);										\
		if ((node2) == LIST_TERMINATOR)									\
		{																\
			if ((node1) == LIST_TERMINATOR)								\
			{															\
				if (!prefix##_next_is_nil(&head.tail))					\
					report_bad_link(#prefix,							\
									(step),								\
									"next",								\
									LIST_TAIL,							\
									LIST_NIL,							\
									head.tail.next);					\
			}															\
			else														\
			{															\
				if (prefix##_get_next(&head.tail						\
									  MAYBE_CONTEXT(have_context,		\
													context)) !=		\
					GET_NODE(node1))									\
					report_bad_link(#prefix,							\
									(step),								\
									"next",								\
									LIST_TAIL,							\
									(node1),							\
									head.tail.next);					\
			}															\
		}																\
	}																	\
	while (0)

#define CHECK_NEXT_AND_PREV_NIL(prefix,									\
								step,									\
								node1,									\
								node2,									\
								have_context,							\
								context)								\
	do																	\
	{																	\
		CHECK_NEXT_NIL(prefix,											\
					   (step),											\
					   (node1),											\
					   (node2),											\
					   have_context,									\
					   context);										\
		CHECK_PREV_NIL(prefix,											\
					   (step),											\
					   (node1),											\
					   (node2),											\
					   have_context,									\
					   context);										\
	}																	\
	while (0)

#endif
