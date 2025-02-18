/*-------------------------------------------------------------------------
 *
 * aio_internal.h
 *    AIO related declarations that shoul only be used by the AIO subsystem
 *    internally.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/aio_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AIO_INTERNAL_H
#define AIO_INTERNAL_H


#include "lib/ilist.h"
#include "port/pg_iovec.h"
#include "storage/aio.h"
#include "storage/condition_variable.h"


/*
 * The maximum number of IOs that can be batch submitted at once.
 */
#define PGAIO_SUBMIT_BATCH_SIZE 32



typedef enum PgAioHandleState
{
	/* not in use */
	PGAIO_HS_IDLE = 0,

	/* returned by pgaio_io_acquire() */
	PGAIO_HS_HANDED_OUT,

	/* pgaio_io_prep_*() has been called, but IO hasn't been submitted yet */
	PGAIO_HS_DEFINED,

	/* target's stage() callback has been called, ready to be submitted */
	PGAIO_HS_STAGED,

	/* IO has been submitted and is being executed */
	PGAIO_HS_SUBMITTED,

	/* IO finished, but result has not yet been processed */
	PGAIO_HS_COMPLETED_IO,

	/* IO completed, shared completion has been called */
	PGAIO_HS_COMPLETED_SHARED,

	/* IO completed, local completion has been called */
	PGAIO_HS_COMPLETED_LOCAL,
} PgAioHandleState;


struct ResourceOwnerData;

/* typedef is in public header */
struct PgAioHandle
{
	/* all state updates should go through pgaio_io_update_state() */
	PgAioHandleState state:8;

	/* what are we operating on */
	PgAioTargetID target:8;

	/* which IO operation */
	PgAioOp		op:8;

	/* bitfield of PgAioHandleFlags */
	uint8		flags;

	uint8		num_shared_callbacks;

	/* using the proper type here would use more space */
	uint8		shared_callbacks[PGAIO_HANDLE_MAX_CALLBACKS];

	/*
	 * Length of data associated with handle using
	 * pgaio_io_set_handle_data_*().
	 */
	uint8		handle_data_len;

	/* XXX: could be optimized out with some pointer math */
	int32		owner_procno;

	/* raw result of the IO operation */
	int32		result;

	/*
	 * Index into PgAioCtl->iovecs and PgAioCtl->handle_data.
	 *
	 * At the moment there's no need to differentiate between the two, but
	 * that won't necessarily stay that way.
	 */
	uint32		iovec_off;

	/*
	 * List of bounce_buffers owned by IO. It would suffice to use an index
	 * based linked list here.
	 */
	slist_head	bounce_buffers;

	/**
	 * In which list the handle is registered, depends on the state:
	 * - IDLE, in per-backend list
	 * - HANDED_OUT - not in a list
	 * - DEFINED - in per-backend staged list
	 * - STAGED - in per-backend staged list
	 * - SUBMITTED - in issuer's in_flight list
	 * - COMPLETED_IO - in issuer's in_flight list
	 * - COMPLETED_SHARED - in issuer's in_flight list
	 **/
	dlist_node	node;

	struct ResourceOwnerData *resowner;
	dlist_node	resowner_node;

	/* incremented every time the IO handle is reused */
	uint64		generation;

	ConditionVariable cv;

	/* result of shared callback, passed to issuer callback */
	PgAioResult distilled_result;

	PgAioReturn *report_return;

	PgAioOpData op_data;

	/*
	 * Data necessary to identify the object undergoing IO to higher-level
	 * code. Needs to be sufficient to allow another backend to reopen the
	 * file.
	 */
	PgAioTargetData target_data;
};


struct PgAioBounceBuffer
{
	slist_node	node;
	struct ResourceOwnerData *resowner;
	dlist_node	resowner_node;
	char	   *buffer;
};


typedef struct PgAioBackend
{
	/* index into PgAioCtl->io_handles */
	uint32		io_handle_off;

	/* index into PgAioCtl->bounce_buffers */
	uint32		bounce_buffers_off;

	/* IO Handles that currently are not used */
	dclist_head idle_ios;

	/*
	 * Only one IO may be returned by pgaio_io_acquire()/pgaio_io_acquire()
	 * without having been either defined (by actually associating it with IO)
	 * or by released (with pgaio_io_release()). This restriction is necessary
	 * to guarantee that we always can acquire an IO. ->handed_out_io is used
	 * to enforce that rule.
	 */
	PgAioHandle *handed_out_io;

	/* Are we currently in batchmode? See pgaio_enter_batchmode(). */
	bool		in_batchmode;

	/*
	 * IOs that are defined, but not yet submitted.
	 */
	uint16		num_staged_ios;
	PgAioHandle *staged_ios[PGAIO_SUBMIT_BATCH_SIZE];

	/*
	 * List of in-flight IOs. Also contains IOs that aren't strict speaking
	 * in-flight anymore, but have been waited-for and completed by another
	 * backend. Once this backend sees such an IO it'll be reclaimed.
	 *
	 * The list is ordered by submission time, with more recently submitted
	 * IOs being appended at the end.
	 */
	dclist_head in_flight_ios;

	/* Bounce Buffers that currently are not used */
	slist_head	idle_bbs;

	/* see handed_out_io */
	PgAioBounceBuffer *handed_out_bb;
} PgAioBackend;


typedef struct PgAioCtl
{
	int			backend_state_count;
	PgAioBackend *backend_state;

	/*
	 * Array of iovec structs. Each iovec is owned by a specific backend. The
	 * allocation is in PgAioCtl to allow the maximum number of iovecs for
	 * individual IOs to be configurable with PGC_POSTMASTER GUC.
	 */
	uint64		iovec_count;
	struct iovec *iovecs;

	/*
	 * For, e.g., an IO covering multiple buffers in shared / temp buffers, we
	 * need to get Buffer IDs during completion to be able to change the
	 * BufferDesc state accordingly. This space can be used to store e.g.
	 * Buffer IDs.  Note that the actual iovec might be shorter than this,
	 * because we combine neighboring pages into one larger iovec entry.
	 */
	uint64	   *handle_data;

	/*
	 * To perform AIO on buffers that are not located in shared memory (either
	 * because they are not in shared memory or because we need to operate on
	 * a copy, as e.g. the case for writes when checksums are in use)
	 */
	uint64		bounce_buffers_count;
	PgAioBounceBuffer *bounce_buffers;
	char	   *bounce_buffers_data;

	uint64		io_handle_count;
	PgAioHandle *io_handles;
} PgAioCtl;



/*
 * Callbacks used to implement an IO method.
 */
typedef struct IoMethodOps
{
	/* global initialization */

	/*
	 * Amount of additional shared memory to reserve for the io_method. Called
	 * just like a normal ipci.c style *Size() function. Optional.
	 */
	size_t		(*shmem_size) (void);

	/*
	 * Initialize shared memory. First time is true if AIO's shared memory was
	 * just initialized, false otherwise. Optional.
	 */
	void		(*shmem_init) (bool first_time);

	/*
	 * Per-backend initialization. Optional.
	 */
	void		(*init_backend) (void);


	/* handling of IOs */

	/* optional */
	bool		(*needs_synchronous_execution) (PgAioHandle *ioh);

	/*
	 * Start executing passed in IOs.
	 *
	 * Will not be called if ->needs_synchronous_execution() returned true.
	 *
	 * num_staged_ios is <= PGAIO_SUBMIT_BATCH_SIZE.
	 *
	 */
	int			(*submit) (uint16 num_staged_ios, PgAioHandle **staged_ios);

	/*
	 * Wait for the IO to complete. Optional.
	 *
	 * If not provided, it needs to be guaranteed that the IO method calls
	 * pgaio_io_process_completion() without further interaction by the
	 * issuing backend.
	 */
	void		(*wait_one) (PgAioHandle *ioh,
							 uint64 ref_generation);
} IoMethodOps;


/* aio.c */
extern bool pgaio_io_was_recycled(PgAioHandle *ioh, uint64 ref_generation, PgAioHandleState *state);
extern void pgaio_io_stage(PgAioHandle *ioh, PgAioOp op);
extern void pgaio_io_process_completion(PgAioHandle *ioh, int result);
extern void pgaio_io_prepare_submit(PgAioHandle *ioh);
extern bool pgaio_io_needs_synchronous_execution(PgAioHandle *ioh);
extern const char *pgaio_io_get_state_name(PgAioHandle *ioh);
const char *pgaio_result_status_string(PgAioResultStatus rs);
extern void pgaio_shutdown(int code, Datum arg);

/* aio_callback.c */
extern void pgaio_io_call_stage(PgAioHandle *ioh);
extern void pgaio_io_call_complete_shared(PgAioHandle *ioh);
extern void pgaio_io_call_complete_local(PgAioHandle *ioh);

/* aio_io.c */
extern void pgaio_io_perform_synchronously(PgAioHandle *ioh);
extern const char *pgaio_io_get_op_name(PgAioHandle *ioh);

/* aio_target.c */
extern bool pgaio_io_can_reopen(PgAioHandle *ioh);
extern void pgaio_io_reopen(PgAioHandle *ioh);
extern const char *pgaio_io_get_target_name(PgAioHandle *ioh);


/*
 * The AIO subsystem has fairly verbose debug logging support. This can be
 * enabled/disabled at buildtime. The reason for this is that
 * a) the verbosity can make debugging things on higher levels hard
 * b) even if logging can be skipped due to elevel checks, it still causes a
 *    measurable slowdown
 */
#define PGAIO_VERBOSE		1

/*
 * Simple ereport() wrapper that only logs if PGAIO_VERBOSE is defined.
 *
 * This intentionally still compiles the code, guarded by a constant if (0),
 * if verbose logging is disabled, to make it less likely that debug logging
 * is silently broken.
 *
 * The current definition requires passing at least one argument.
 */
#define pgaio_debug(elevel, msg, ...)  \
	do { \
		if (PGAIO_VERBOSE) \
			ereport(elevel, \
					errhidestmt(true), errhidecontext(true), \
					errmsg_internal(msg, \
									__VA_ARGS__)); \
	} while(0)

/*
 * Simple ereport() wrapper. Note that the definition requires passing at
 * least one argument.
 */
#define pgaio_debug_io(elevel, ioh, msg, ...)  \
	pgaio_debug(elevel, "io %-10d|op %-5s|target %-4s|state %-16s: " msg, \
				pgaio_io_get_id(ioh), \
				pgaio_io_get_op_name(ioh), \
				pgaio_io_get_target_name(ioh), \
				pgaio_io_get_state_name(ioh), \
				__VA_ARGS__)


#ifdef USE_INJECTION_POINTS

extern void pgaio_io_call_inj(PgAioHandle *ioh, const char *injection_point);

/* just for use in tests, from within injection points */
extern PgAioHandle *pgaio_inj_io_get(void);

#else

#define pgaio_io_call_inj(ioh, injection_point) (void) 0
/*
 * no fallback for pgaio_inj_io_get, all code using injection points better be
 * guarded by USE_INJECTION_POINTS.
 */

#endif


/* Declarations for the tables of function pointers exposed by each IO method. */
extern PGDLLIMPORT const IoMethodOps pgaio_sync_ops;
extern PGDLLIMPORT const IoMethodOps pgaio_worker_ops;
#ifdef USE_LIBURING
extern PGDLLIMPORT const IoMethodOps pgaio_uring_ops;
#endif

extern PGDLLIMPORT const IoMethodOps *pgaio_method_ops;
extern PGDLLIMPORT PgAioCtl *pgaio_ctl;
extern PGDLLIMPORT PgAioBackend *pgaio_my_backend;



#endif							/* AIO_INTERNAL_H */
