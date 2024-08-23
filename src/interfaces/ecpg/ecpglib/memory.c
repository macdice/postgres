/* src/interfaces/ecpg/ecpglib/memory.c */

#define POSTGRES_ECPG_INTERNAL
#include "postgres_fe.h"

#include "ecpgerrno.h"
#include "ecpglib.h"
#include "ecpglib_extern.h"
#include "ecpgtype.h"
#include "port/pg_threads.h"

void
ecpg_free(void *ptr)
{
fprintf(stderr, "XXX %d:%d ecpg_free %p\n", GetCurrentProcessId(), GetCurrentThreadId(), ptr);
	free(ptr);
}

char *
xxx_ecpg_alloc(long size, int lineno, int xxline, const char *xxfile)
{
	char	   *new = (char *) calloc(1L, size);

	if (!new)
	{
		ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
		return NULL;
	}

fprintf(stderr, "XXX %d:%d ecpg_alloc %p (%s:%d)\n", GetCurrentProcessId(), GetCurrentThreadId(), new, xxfile, xxline);

	return new;
}

char *
ecpg_realloc(void *ptr, long size, int lineno)
{
	char	   *new = (char *) realloc(ptr, size);

	if (!new)
	{
		ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
		return NULL;
	}

fprintf(stderr, "XXX %d:%d ecpg_realloc %p -> %p\n", GetCurrentProcessId(), GetCurrentThreadId(), ptr, new);
	return new;
}

char *
ecpg_strdup(const char *string, int lineno)
{
	char	   *new;

	if (string == NULL)
		return NULL;

	new = strdup(string);
	if (!new)
	{
		ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
		return NULL;
	}

fprintf(stderr, "XXX %d:%d ecpg_strdup %p\n", GetCurrentProcessId(), GetCurrentThreadId(), new);
	return new;
}

/* keep a list of memory we allocated for the user */
struct auto_mem
{
	void	   *pointer;
	struct auto_mem *next;
};

static pg_tss_t auto_mem_key;
static pg_once_flag auto_mem_once = PG_ONCE_FLAG_INIT;

static void pg_tss_dtor_calling_convention
auto_mem_destructor(void *arg)
{
	(void) arg;					/* keep the compiler quiet */
	ECPGfree_auto_mem();
}

static void
auto_mem_key_init(void)
{
	pg_tss_create(&auto_mem_key, auto_mem_destructor);
}

static struct auto_mem *
get_auto_allocs(void)
{
	pg_call_once(&auto_mem_once, auto_mem_key_init);
	return (struct auto_mem *) pg_tss_get(auto_mem_key);
}

static void
set_auto_allocs(struct auto_mem *am)
{
	pg_tss_set(auto_mem_key, am);
}

char *
xxx_ecpg_auto_alloc(long size, int lineno, int xxline, const char *xxfile)
{
	void	   *ptr = (void *) ecpg_alloc(size, lineno);

	if (!ptr)
		return NULL;

fprintf(stderr, "XXX %d:%d ecpg_auto_alloc %p (%s:%d)\n", GetCurrentProcessId(), GetCurrentThreadId(), ptr, xxfile, xxline);

	if (!ecpg_add_mem(ptr, lineno))
	{
		ecpg_free(ptr);
		return NULL;
	}
	return ptr;
}

bool
ecpg_add_mem(void *ptr, int lineno)
{
	struct auto_mem *am = (struct auto_mem *) ecpg_alloc(sizeof(struct auto_mem), lineno);

	if (!am)
		return false;

#ifdef USE_ASSERTION_CHECKING
	/* We shouldn't be adding something that was already added. */
	for (struct auto_mem *p = get_auto_allocs(); p; p = p->next)
		Assert(p != am->pointer);
#endif
fprintf(stderr, "XXX %d:%d ecpg_add_mem allocated node %p to store ptr %p\n", GetCurrentProcessId(), GetCurrentThreadId(), am, ptr);

	am->pointer = ptr;
	am->next = get_auto_allocs();
	set_auto_allocs(am);
	return true;
}

void
ECPGfree_auto_mem(void)
{
	struct auto_mem *am = get_auto_allocs();

	/* free all memory we have allocated for the user */
	if (am)
	{
		do
		{
			struct auto_mem *act = am;

			am = am->next;
fprintf(stderr, "XXX %d:%d ECPGfree_auto_mem will free act->pointer %p (in node %p)\n", GetCurrentProcessId(), GetCurrentThreadId(), act->pointer, act);
			ecpg_free(act->pointer);
fprintf(stderr, "XXX %d:%d ECPGfree_auto_mem will free act %p\n", GetCurrentProcessId(), GetCurrentThreadId(), act);
			ecpg_free(act);
		} while (am);
		set_auto_allocs(NULL);
	}
}

void
ecpg_clear_auto_mem(void)
{
	struct auto_mem *am = get_auto_allocs();

	/* only free our own structure */
	if (am)
	{
		do
		{
			struct auto_mem *act = am;

fprintf(stderr, "XXX %d:%d ECPGfree_clear_auto_mem freeing node %p (won't free ptr %p)\n", GetCurrentProcessId(), GetCurrentThreadId(), am, am->pointer);
			am = am->next;
			ecpg_free(act);
		} while (am);
		set_auto_allocs(NULL);
	}
}
