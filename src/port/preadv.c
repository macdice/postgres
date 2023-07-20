/*-------------------------------------------------------------------------
 *
 * preadv.c
 *	  Implementation of preadv(2) for platforms that lack one.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/preadv.c
 *
 *-------------------------------------------------------------------------
 */


#include "c.h"

#include <unistd.h>

#include "port/pg_iovec.h"

ssize_t
pg_preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset)
{
	ssize_t		sum = 0;
	ssize_t		part;

	for (int i = 0; i < iovcnt; ++i)
	{
		char	   *iov_base = iov[i].iov_base;
		size_t		iov_len = iov[i].iov_len;

		/* Try to handle contiguous buffers with one system call. */
		while ((i + 1) < iovcnt &&
			   iov_base + iov_len == iov[i + 1].iov_base)
			iov_len += iov[++i].iov_len;

		part = pg_pread(fd, iov_base, iov_len, offset);
		if (part < 0)
		{
			if (sum == 0)
				return -1;
			else
				return sum;
		}
		sum += part;
		offset += part;
		if (part < iov_len)
			return sum;
	}
	return sum;
}
