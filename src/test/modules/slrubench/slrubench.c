#include "postgres.h"

#include "fmgr.h"
#include "access/clog.h"
#include "access/xlogdefs.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_clog_fetch);

Datum
test_clog_fetch(PG_FUNCTION_ARGS)
{
	TransactionId x1 = PG_GETARG_TRANSACTIONID(0);
	TransactionId x2 = PG_GETARG_TRANSACTIONID(1);
	int loops = PG_GETARG_INT32(2);

	elog(NOTICE, "xid range %u, %u; loop = %d", x1, x2, loops);
	for (int i = 0; i < loops; ++i)
	{
		for (TransactionId xid = x1; xid < x2; ++xid)
		{
			XLogRecPtr lsn;
			TransactionIdGetStatus(xid, &lsn);
		}
		CHECK_FOR_INTERRUPTS();
	}

	PG_RETURN_VOID();
}
