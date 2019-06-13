/*-------------------------------------------------------------------------
 *
 * undorecord.h
 *	  encode and decode undo records
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undorecord.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDORECORD_H
#define UNDORECORD_H

#include "access/undolog.h"
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "storage/bufpage.h"
#include "storage/buf.h"
#include "storage/off.h"

/*
 * Every undo record begins with an UndoRecordHeader structure, which is
 * followed by the additional structures indicated by the contents of
 * urec_info.  All structures are packed into the alignment without padding
 * bytes, and the undo record itself need not be aligned either, so care
 * must be taken when reading the header.
 */
typedef struct UndoRecordHeader
{
	RmgrId		urec_rmid;		/* RMGR [XXX:TODO: this creates an alignment
								 * hole?] */
	uint8		urec_type;		/* record type code */
	uint8		urec_info;		/* flag bits */
	Oid			urec_reloid;	/* relation OID */

	/*
	 * Transaction id that has modified the tuple for which this undo record
	 * is written.  We use this to skip the undo records.  See comments atop
	 * function UndoFetchRecord.
	 */
	TransactionId urec_xid;		/* Transaction id */
	CommandId	urec_cid;		/* command id */
} UndoRecordHeader;

#define SizeOfUndoRecordHeader	\
	(offsetof(UndoRecordHeader, urec_cid) + sizeof(CommandId))

/*
 * If UREC_INFO_RELATION_DETAILS is set, an UndoRecordRelationDetails structure
 * follows.
 *
 * If UREC_INFO_BLOCK is set, an UndoRecordBlock structure follows.
 *
 * If UREC_INFO_TRANSACTION is set, an UndoRecordTransaction structure
 * follows.
 *
 * If UREC_INFO_BLKPREV is set, an UndoRecordBlockPrev structure follows.
 *
 * If UREC_INFO_LOGSWITCH is set, an UndoRecordLogSwitch structure follows.
 *
 * If UREC_INFO_PAYLOAD is set, an UndoRecordPayload structure follows.
 *
 * When (as will often be the case) multiple structures are present, they
 * appear in the same order in which the constants are defined here.  That is,
 * UndoRecordRelationDetails appears first.
 */
#define UREC_INFO_TRANSACTION				0x01
#define UREC_INFO_FORK						0x02
#define UREC_INFO_BLOCK						0x04
#define UREC_INFO_BLKPREV					0x08
#define UREC_INFO_LOGSWITCH					0x10
#define UREC_INFO_PAYLOAD					0x20

/*
 * Information for a transaction to which this undo belongs.  This
 * also stores the dbid and the progress of the undo apply during rollback.
 */
typedef struct UndoRecordTransaction
{
	/*
	 * Undo block number where we need to start reading the undo for applying
	 * the undo action.   InvalidBlockNumber means undo applying hasn't started
	 * for the transaction and MaxBlockNumber mean undo completely applied. And,
	 * any other block number means we have applied partial undo so next we can
	 * start from this block.
	 */
	BlockNumber	urec_progress;
	uint32		urec_xidepoch;	/* epoch of the current transaction */
	Oid			urec_dbid;		/* database id */

	/*
	 * Transaction's previous undo record pointer when a transaction spans
	 * across undo logs.  The first undo record in the new log stores the
	 * previous undo record pointer in the previous log as we can't calculate
	 * that directly using prevlen during rollback.
	 *
	 * TODO: instead of keeping in transaction header we can have new log
	 * switch header.
	 */
	UndoRecPtr	urec_next;		/* urec pointer of the next transaction */
} UndoRecordTransaction;

#define SizeOfUndoRecordTransaction \
	(offsetof(UndoRecordTransaction, urec_next) + sizeof(UndoRecPtr))

/*
 *  Information for a block to which this record pertains.
 */
typedef struct UndoRecordBlock
{
	BlockNumber urec_block;		/* block number */
	OffsetNumber urec_offset;	/* offset number */
} UndoRecordBlock;

#define SizeOfUndoRecordBlock \
	(offsetof(UndoRecordBlock, urec_offset) + sizeof(OffsetNumber))

/*
 * Information of the transaction's undo in the previous log.  If a transaction
 * is split across the undo logs then this header will be included in the first
 * undo record of the transaction in next log.
 */
typedef struct UndoRecordLogSwitch
{
	UndoRecPtr	urec_prevurp;		/* Transaction's last undo record pointer in
									 * the previous undo log. */
	UndoRecPtr	urec_prevlogstart;	/* Transaction's first undo record pointer
									 * in previous undo log. */
} UndoRecordLogSwitch;

#define SizeOfUndoRecordLogSwitch \
	(offsetof(UndoRecordLogSwitch, urec_prevlogstart) + sizeof(UndoRecPtr))

/*
 * Information about the amount of payload data and tuple data present
 * in this record.  The payload bytes immediately follow the structures
 * specified by flag bits in urec_info, and the tuple bytes follow the
 * payload bytes.
 */
typedef struct UndoRecordPayload
{
	uint16		urec_payload_len;	/* # of payload bytes */
	uint16		urec_tuple_len; /* # of tuple bytes */
} UndoRecordPayload;

#define SizeOfUndoRecordPayload \
	(offsetof(UndoRecordPayload, urec_tuple_len) + sizeof(uint16))

/*
 * Information that can be used to create an undo record or that can be
 * extracted from one previously created.  The raw undo record format is
 * difficult to manage, so this structure provides a convenient intermediate
 * form that is easier for callers to manage.
 *
 * When creating an undo record from an UnpackedUndoRecord, caller should
 * set uur_info to 0.  It will be initialized by the first call to
 * UndoRecordSetInfo or InsertUndoRecord.  We do set it in
 * UndoRecordAllocate for transaction specific header information.
 *
 * When an undo record is decoded into an UnpackedUndoRecord, all fields
 * will be initialized, but those for which no information is available
 * will be set to invalid or default values, as appropriate.
 */
typedef struct UnpackedUndoRecord
{
	RmgrId		uur_rmid;		/* rmgr ID */
	uint8		uur_type;		/* record type code */
	uint8		uur_info;		/* flag bits */
	Oid			uur_reloid;		/* relation OID */
	TransactionId uur_xid;		/* transaction id */
	CommandId	uur_cid;		/* command id */
	ForkNumber	uur_fork;		/* fork number */
	UndoRecPtr	uur_blkprev;	/* byte offset of previous undo for block */
	BlockNumber uur_block;		/* block number */
	OffsetNumber uur_offset;	/* offset number */
	uint32		uur_xidepoch;	/* epoch of the inserting transaction. */
	BlockNumber	uur_progress;	/* undo applying progress, see detail comment in
								 * UndoRecordTransaction */
	Oid			uur_dbid;		/* database id */
	UndoRecPtr	uur_next;		/* urec pointer of the next transaction */
	UndoRecPtr	uur_prevlogstart; /* urec pointer to the first record in the
									* previous log. */
	UndoRecPtr	uur_prevurp;	/* urec pointer to the previous record in the
								 * different log */
	StringInfoData uur_payload; /* payload bytes */
	StringInfoData uur_tuple;	/* tuple bytes */
} UnpackedUndoRecord;

typedef enum UndoPackStage
{
	UNDO_PACK_STAGE_HEADER,		/* We have not yet processed even the record
								 * header; we need to do that next. */
	UNDO_PACK_STAGE_TRANSACTION,	/* The next thing to be processed is the
									 * transaction details, if present. */
	UNDO_PACK_STAGE_FORKNUM,	/* The next thing to be processed is the
								 * relation fork number, if present. */
	UNDO_PACK_STAGE_BLOCK,		/* The next thing to be processed is the block
								 * details, if present. */
	UNDO_PACK_STAGE_BLOCKPREV,	/* The next thing to be processed is the block
								 * prev info. */
	UNDO_PACK_STAGE_LOGSWITCH,	/* The next thing to be processed is the log
								 * switch details. */
	UNDO_PACK_STAGE_PAYLOAD,	/* The next thing to be processed is the
								 * payload details, if present */
	UNDO_PACK_STAGE_PAYLOAD_DATA,	/* The next thing to be processed is the
									 * payload data */
	UNDO_PACK_STAGE_TUPLE_DATA, /* The next thing to be processed is the tuple
								 * data */
	UNDO_PACK_STAGE_UNDO_LENGTH,	/* Next thing to processed is undo length. */
	UNDO_PACK_STAGE_DONE		/* complete */
} UndoPackStage;

/*
 * Undo record context for inserting/unpacking undo record.  This will hold
 * intermediate state of undo record processed so far.
 */
typedef struct UndoPackContext
{
	UndoRecordHeader urec_hd;	/* Main header */
	UndoRecordTransaction urec_txn; /* Transaction header */
	ForkNumber	urec_fork;		/* Relation fork number */
	UndoRecordBlock urec_blk;	/* Block header */
	UndoRecPtr	urec_blkprev;	/* Block prev */
	UndoRecordLogSwitch urec_logswitch; /* Log switch header */
	UndoRecordPayload urec_payload; /* Payload data */
	char	   *urec_payloaddata;
	char	   *urec_tupledata;
	uint16		undo_len;		/* Length of the undo record. */
	int			already_processed;	/* Number of bytes read/written so far */
	int			partial_bytes;	/* Number of partial bytes read/written */
	UndoPackStage stage;		/* Undo pack stage */
} UndoPackContext;

extern void UndoRecordSetInfo(UnpackedUndoRecord *uur);
extern Size UndoRecordHeaderSize(uint8 uur_info);
extern Size UndoRecordExpectedSize(UnpackedUndoRecord *uur);
extern Size UnpackedUndoRecordSize(UnpackedUndoRecord *uur);
extern bool InsertUndoRecord(UnpackedUndoRecord *uur, Page page,
				 int starting_byte, int *already_written,
				 int remaining_bytes, uint16 undo_len, bool header_only);
extern void BeginUnpackUndo(UndoPackContext *ucontext);
extern void UnpackUndoData(UndoPackContext *ucontext, Page page,
			   int starting_byte);
extern void FinishUnpackUndo(UndoPackContext *ucontext,
				 UnpackedUndoRecord *uur);
extern void BeginInsertUndo(UndoPackContext *ucontext,
				UnpackedUndoRecord *uur);
extern void InsertUndoData(UndoPackContext *ucontext, Page page,
			   int starting_byte);
extern void SkipInsertingUndoData(UndoPackContext *ucontext,
					  int bytes_to_skip);
extern void mask_undo_page(char *pagedata);

#endif							/* UNDORECORD_H */
