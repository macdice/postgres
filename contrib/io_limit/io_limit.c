#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "storage/aio_internal.h"
#include "storage/io_worker.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

/* GUCs. */
static int	io_limit_ios_per_second = 0;
static int	io_limit_read_per_second = 0;
static int	io_limit_write_per_second = 0;

typedef struct io_limit_control_data
{
	/* Whether any GUC is set to a non-zero value. */
	bool		enabled;

	/* Absolute time to wait until. */
	pg_atomic_uint64 op_next_ns;
	pg_atomic_uint64 read_next_ns;
	pg_atomic_uint64 write_next_ns;

	/* Limits expressed as delay intervals. */
	LWLock		lock;
	int			op_ns;
	int			read_block_ns;
	int			write_block_ns;
}			io_limit_control_data;

static io_limit_control_data * io_limit_control;

static void io_limit_shmem_request(void *arg);
static void io_limit_shmem_init(void *arg);

static void assign_io_limit_ios_per_second(int newval, void *extra);
static void assign_io_limit_read_per_second(int newval, void *extra);
static void assign_io_limit_write_per_second(int newval, void *extra);
static const char *show_io_limit_ios_per_second(void);
static const char *show_io_limit_read_per_second(void);
static const char *show_io_limit_write_per_second(void);

static void io_limit_on_perform(PgAioHandle *ioh);

static const ShmemCallbacks io_limit_shmem_callbacks = {
	.request_fn = io_limit_shmem_request,
	.init_fn = io_limit_shmem_init,
};

PG_MODULE_MAGIC_EXT(
					.name = "io_limit",
					.version = PG_VERSION
);

void
_PG_init(void)
{
	/* Bail out if not configured in shared_preload_libraries. */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("io_limit.ios_per_second",
							"Limits IOs per second.",
							"If set to zero, there is no limit.",
							&io_limit_ios_per_second,
							0,
							0, INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							assign_io_limit_ios_per_second,
							show_io_limit_ios_per_second);
	DefineCustomIntVariable("io_limit.read_per_second",
							"Limits read bandwidth.",
							"If set to zero, there is no limit.",
							&io_limit_read_per_second,
							0,
							0, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_BLOCKS,
							NULL,
							assign_io_limit_read_per_second,
							show_io_limit_read_per_second);
	DefineCustomIntVariable("io_limit.write_per_second",
							"Limits write bandwidth.",
							"If set to zero, there is no limit.",
							&io_limit_write_per_second,
							0,
							0, INT_MAX,
							PGC_USERSET,
							GUC_UNIT_BLOCKS,
							NULL,
							assign_io_limit_write_per_second,
							show_io_limit_write_per_second);

	MarkGUCPrefixReserved("io_limit");
	RegisterShmemCallbacks(&io_limit_shmem_callbacks);
	pgaio_worker_set_on_perform_hook(io_limit_on_perform);
}

static void
io_limit_shmem_request(void *arg)
{
	ShmemRequestStruct(.name = "io_limit",
					   .size = sizeof(io_limit_control_data),
					   .ptr = (void **) &io_limit_control);
}

static void
io_limit_shmem_init(void *arg)
{
	memset(io_limit_control, 0, sizeof(*io_limit_control));
	pg_atomic_init_u64(&io_limit_control->op_next_ns, 0);
	pg_atomic_init_u64(&io_limit_control->read_next_ns, 0);
	pg_atomic_init_u64(&io_limit_control->write_next_ns, 0);
	LWLockInitialize(&io_limit_control->lock, LWLockNewTrancheId("io_limit"));

	/* Assign initial values. */
	assign_io_limit_ios_per_second(io_limit_ios_per_second, NULL);
	assign_io_limit_read_per_second(io_limit_read_per_second, NULL);
	assign_io_limit_write_per_second(io_limit_write_per_second, NULL);
}

static void
assign_io_limit(int *wait_ns, int per_second)
{
	/* Ignore call from _PG_init() before ready. */
	if (!io_limit_control)
		return;

	LWLockAcquire(&io_limit_control->lock, LW_EXCLUSIVE);
	*wait_ns = per_second == 0 ? 0 : NS_PER_S / per_second;
	io_limit_control->enabled =
		io_limit_control->op_ns > 0 ||
		io_limit_control->read_block_ns > 0 ||
		io_limit_control->write_block_ns > 0;
	LWLockRelease(&io_limit_control->lock);
}

static void
assign_io_limit_ios_per_second(int newval, void *extra)
{
	assign_io_limit(&io_limit_control->op_ns, newval);
}

static void
assign_io_limit_read_per_second(int newval, void *extra)
{
	assign_io_limit(&io_limit_control->read_block_ns, newval);
}

static void
assign_io_limit_write_per_second(int newval, void *extra)
{
	assign_io_limit(&io_limit_control->write_block_ns, newval);
}

static const char *
show_io_limit(const int *wait_ns)
{
	int			per_second;

	LWLockAcquire(&io_limit_control->lock, LW_SHARED);
	per_second = *wait_ns == 0 ? 0 : NS_PER_S / *wait_ns;
	LWLockRelease(&io_limit_control->lock);

	return psprintf("%d", per_second);
}

static const char *
show_io_limit_ios_per_second(void)
{
	return show_io_limit(&io_limit_control->op_ns);
}

static const char *
show_io_limit_read_per_second(void)
{
	return show_io_limit(&io_limit_control->read_block_ns);
}

static const char *
show_io_limit_write_per_second(void)
{
	return show_io_limit(&io_limit_control->write_block_ns);
}

static BlockNumber
io_limit_get_block_count(PgAioHandle *ioh)
{
	if (ioh->op == PGAIO_OP_READV ||
		ioh->op == PGAIO_OP_WRITEV)
	{
		struct iovec *iov;
		size_t		size;
		int			iovcnt;

		size = 0;
		iovcnt = pgaio_io_get_iovec_length(ioh, &iov);
		for (int i = 0; i < iovcnt; ++i)
			size += iov[i].iov_len;

		return size / BLCKSZ;
	}

	return 0;
}

/*
 * Wait until *next_ns_p and advance *next_ns_p by delay_ns.
 */
static void
io_limit_wait(pg_atomic_uint64 *next_ns_p, int delay_ns)
{
	instr_time	now;
	uint64		now_ns;
	uint64		next_ns;

	INSTR_TIME_SET_CURRENT(now);
	now_ns = INSTR_TIME_GET_NANOSEC(now);
	next_ns = pg_atomic_read_u64(next_ns_p);

	for (;;)
	{
		if (next_ns > now_ns)
		{
			/* Need to wait.  Delay the next op further. */
			next_ns = pg_atomic_fetch_add_u64(next_ns_p, delay_ns);

			/* Average rate maintained even with low-res sleep or EINTR. */
			pg_usleep(((next_ns - now_ns) + 999) / 1000);
			break;
		}
		else
		{
			/* Don't need to wait.  New next_ns is relative to now. */
			if (pg_atomic_compare_exchange_u64(next_ns_p,
											   &next_ns,
											   now_ns + delay_ns))
				break;
		}
	}
}

static void
io_limit_on_perform(PgAioHandle *ioh)
{
	int			op_ns;
	int			read_block_ns;
	int			write_block_ns;

	if (!io_limit_control->enabled)
		return;

	op_ns = io_limit_control->op_ns;
	if (op_ns)
		io_limit_wait(&io_limit_control->op_next_ns, op_ns);

	if (ioh->op == PGAIO_OP_READV)
	{
		read_block_ns = io_limit_control->read_block_ns;
		if (read_block_ns)
			io_limit_wait(&io_limit_control->read_next_ns,
						  io_limit_get_block_count(ioh) * read_block_ns);
	}
	else if (ioh->op == PGAIO_OP_WRITEV)
	{
		write_block_ns = io_limit_control->write_block_ns;
		io_limit_wait(&io_limit_control->write_next_ns,
					  io_limit_get_block_count(ioh) * write_block_ns);
	}
}
