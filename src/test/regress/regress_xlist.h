#ifndef REGRESS_XLIST_H
#define REGRESS_XLIST_H

/* X-macro for ops available to all configuration (assuming XLIST_LINEAR fallbacks). */
#define FOR_EACH_COMMON_OP(do, ...)												\
	do(count, int (list, opt_context), __VA_ARGS__)								\
	do(delete_from, void (list, node, opt_context), __VA_ARGS__)				\
	do(delete_from_thoroughly, void (list, node, opt_context), __VA_ARGS__) 	\
	do(has_next, int (list, node), __VA_ARGS__)									\
	do(has_prev, int (list, node, opt_context), __VA_ARGS__)					\
	do(head_node, node (list, opt_context), __VA_ARGS__)						\
	do(init, void (list), __VA_ARGS__)											\
	do(insert_into_after, void (list, node, node, opt_context), __VA_ARGS__) 	\
	do(insert_into_before, void (list, node, node, opt_context), __VA_ARGS__)	\
	do(is_empty, int (list), __VA_ARGS__)										\
	do(move_head, void (list, node, opt_context), __VA_ARGS__)					\
	do(move_tail, void (list, node, opt_context), __VA_ARGS__)					\
	do(node_is_detached, int (node), __VA_ARGS__)								\
	do(pop_head_node, node (list, opt_context), __VA_ARGS__)					\
	do(pop_tail_node, node (list, opt_context), __VA_ARGS__)					\
	do(push_head, void (list, node, opt_context), __VA_ARGS__) 					\
	do(push_tail, void (list, node, opt_context), __VA_ARGS__) 					\
	do(tail_node, node (list, opt_context), __VA_ARGS__)

/* X-macro for all ops. */
#define FOR_EACH_OP(do, ...)					\
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
 * Example: void(list, node, opt_context) -> 1, void, 2, list, node
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
#define DECODE_F_TYPE__list(return_type, ...)		\
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
#define TEST(f_name, args, ...) \
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
#define CALL_ARG__list ,		/* matched "list"? make __VA_ARGS__ longer */
#define CALL_ARG__(...) CAT(CALL_ARG___, VA_ARGS_NARGS(__VA_ARGS__))(__VA_ARGS__)
#define CALL_ARG___3(arg, ...) -1	/* replace "list" with -1 */
#define CALL_ARG___2(arg, ...) arg	/* otherwise it's a node index */

/* Macros for the expected result of function call. */
#define RETURNS(v) .result = {.check = true, .value = v}
#define RETURNS_NODE(v) .result = {.check = true, .is_node = true, .value = v}
#define LIST(...) .list_contents = {.check = true,				\
									.count = VA_ARGS_NARGS(__VA_ARGS__), \
									.order = {__VA_ARGS__}}
#define LIST_EMPTY() .list_contents = {.check = true, .count = 0}

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

/* Expand X-macro to switch cases to call all the functions. */
#define EXPAND_CASE(f_name, f_type, prefix, context)	\
	EXPAND_CASE_(f_name, prefix, context, DECODE_F_TYPE(f_type))
#define EXPAND_CASE_(...) EXPAND_CASE__(__VA_ARGS__)
#define EXPAND_CASE__(f_name,											\
					  prefix,											\
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
#define EXPAND_CASE_ARG_TYPE_list(pos) &list
#define EXPAND_CASE_ARG_TYPE_node(pos) &array[args[pos]].node
#define EXPAND_CASE_ARG_OPT_CONTEXT(takes_opt_context,					\
									context)							\
	EXPAND_CASE_ARG_OPT_CONTEXT_##takes_opt_context(context)
#define EXPAND_CASE_ARG_OPT_CONTEXT_0(context)
#define EXPAND_CASE_ARG_OPT_CONTEXT_1(context) context()

/*
 * When using XLIST_INDEX, some functions need access to the array and the
 * array element size.  Other link types don't.
 */
#define CONTEXT_NONE()
#define CONTEXT_ARRAY_AND_SIZE() , &array[0].node, sizeof(array[0])
#define CONTEXT_ARRAY() , array

/* Special node indexes used in this test code. */
#define LIST_NIL -1
#define LIST_HEAD -2
#define LIST_TERMINATOR -3		/* NIL or HEAD as appropriate */
#define LIST_TAIL -4

#define RUN_TESTS(prefix, check_link, check_count, context, steps, ops)	\
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
						 0); /*GET_INDEX(node));*/								\
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
			char debug_context[80];										\
			const prefix##_node *										\
				expected[lengthof(step->list_contents.order)] = {0};	\
																		\
			for (int i = 0; i < step->list_contents.count; ++i)			\
				expected[i] =											\
					&array[step->list_contents.order[i]].node;			\
																		\
			snprintf(debug_context,										\
					 sizeof(debug_context),								\
					 "step %d of test schedule %s with prefix %s",		\
					 step_number,										\
					 #steps,											\
					 #prefix);											\
																		\
			prefix##_check_contents(&list,								\
									step->list_contents.count,			\
									expected,							\
									debug_context context());			\
		}																\
	}

#endif
