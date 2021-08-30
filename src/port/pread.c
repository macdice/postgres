/*-------------------------------------------------------------------------
 *
 * pread.c
 *	  Implementation of pread(2) for platforms that lack one.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/port/pread.c
 *
 * Note that this implementation changes the current file position, unlike
 * the POSIX function, so we use the name pg_pread().
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
pg_pread(int fd, void *buf, size_t size, off_t offset)
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
	overlapped.hEvent = completion_event;
#endif
	if (!ReadFile(handle, buf, size, &result, &overlapped))
	{
		if (GetLastError() == ERROR_HANDLE_EOF)
			return 0;

		_dosmaperr(GetLastError());
		return -1;
	}

#ifndef FRONTEND
	if (WaitForSingleObject(overlapped.hEvent, INFINITE) != WAIT_OBJECT_0)
	{
		_dosmaperr(GetLastError());
		return -1;
	}
#endif

	return result;
#else
	if (lseek(fd, offset, SEEK_SET) < 0)
		return -1;

	return read(fd, buf, size);
#endif
}
