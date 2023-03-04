/*-------------------------------------------------------------------------
 *
 * win32ftruncate.c
 *	   Win32 ftruncate() replacement
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * src/port/win32ftruncate.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

int
ftruncate(int fd, pgoff_t length)
{
	HANDLE		handle;
	pgoff_t		save_position;

	/*
	 * We can't use chsize() because it works with 32 bit off_t.  We can't use
	 * _chsize_s() because it isn't available in MinGW.  So we have to use
	 * SetEndOfFile(), but that works with the current position.  So we save
	 * and restore it.
	 */

	handle = (HANDLE) _get_osfhandle(fd);
	if (handle == INVALID_HANDLE_VALUE)
	{
		errno = EBADF;
		return -1;
	}

	save_position = lseek(fd, 0, SEEK_CUR);
	if (save_position < 0)
		return -1;

	if (lseek(fd, length, SEEK_SET) < 0)
	{
		int			save_errno = errno;
		lseek(fd, save_position, SEEK_SET);
		errno = save_errno;
		return -1;
	}

	if (!SetEndOfFile(handle))
	{
		int			save_errno;

		_dosmaperr(GetLastError());
		save_errno = errno;
		lseek(fd, save_position, SEEK_SET);
		errno = save_errno;
		return -1;
	}
	lseek(fd, save_position, SEEK_SET);

	return 0;
}
