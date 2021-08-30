/*-------------------------------------------------------------------------
 *
 * pwrite.c
 *	  Implementation of pwrite(2) for platforms that lack one.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/pwrite.c
 *
 * Note that this implementation changes the current file position, unlike
 * the POSIX function, so we use the name pg_pwrite().
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#ifdef WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

ssize_t
pg_pwrite(int fd, const void *buf, size_t size, off_t offset)
{
#ifdef WIN32
	OVERLAPPED	overlapped = {0};
	HANDLE		handle;
	DWORD		result;

#ifndef FRONTEND
	static HANDLE completion_event = INVALID_HANDLE_VALUE;

	if (completion_event == INVALID_HANDLE_VALUE)
		completion_event = CreateEvent(NULL, TRUE, FALSE, NULL);
#endif

	handle = (HANDLE) _get_osfhandle(fd);
	if (handle == INVALID_HANDLE_VALUE)
	{
		errno = EBADF;
		return -1;
	}
	overlapped.Offset = offset;
#ifndef FRONTEND
	overlapped.hEvent = (HANDLE) (((uintptr_t)completion_event) | 1);
#endif
	if (!WriteFile(handle, buf, size, &result, &overlapped))
	{
		if (GetLastError() == ERROR_IO_PENDING)
		{
			if (!GetOverlappedResult(overlapped.hEvent, &overlapped, &result, TRUE))
			{
				_dosmaperr(GetLastError());
				return -1;
			}
		}
		else
		{
			_dosmaperr(GetLastError());
			return -1;
		}
	}

	return result;
#else
	if (lseek(fd, offset, SEEK_SET) < 0)
		return -1;

	return write(fd, buf, size);
#endif
}
