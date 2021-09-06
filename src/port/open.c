/*-------------------------------------------------------------------------
 *
 * open.c
 *	   Win32 open() replacement
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/port/open.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef WIN32

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "port/win32ntdll.h"

#include <fcntl.h>
#include <assert.h>
#include <sys/stat.h>

/*
 * Internal function used by pgwin32_open() and _pgstat64().  When
 * backup_semantics is true, directories may be opened (for limited uses).  On
 * failure, INVALID_HANDLE_VALUE is returned and errno is set.
 */
HANDLE
pgwin32_open_handle(const char *fileName, int fileFlags, bool backup_semantics)
{
	HANDLE		h = INVALID_HANDLE_VALUE;
	int			loops = 0;
	wchar_t		wideFileName[MAX_PATH];
	size_t		wideFileNameSize;
	wchar_t		buffer[MAX_PATH];
	UNICODE_STRING ntFileName;
	ACCESS_MASK	desiredAccess;
	OBJECT_ATTRIBUTES objectAttributes;
	IO_STATUS_BLOCK ioStatusBlock;
	ULONG		fileAttributes;
	ULONG		shareAccess;
	ULONG		createDisposition;
	ULONG		createOptions;
	NTSTATUS	status;
	DWORD		err;

	if (initialize_ntdll() < 0)
		return INVALID_HANDLE_VALUE;

	/* Check that we can handle the request */
	assert((fileFlags & ((O_RDONLY | O_WRONLY | O_RDWR) | O_APPEND |
						 (O_RANDOM | O_SEQUENTIAL | O_TEMPORARY) |
						 _O_SHORT_LIVED | O_DSYNC | O_DIRECT |
						 (O_CREAT | O_TRUNC | O_EXCL) | (O_TEXT | O_BINARY))) == fileFlags);
#ifndef FRONTEND
	/* XXX When called by stat very early on, this fails! */
	//Assert(pgwin32_signal_event != NULL);	/* small chance of pg_usleep() */
#endif

	/* Convert char string to wchar_t string. */
	wideFileNameSize = MultiByteToWideChar(CP_ACP, 0, fileName, -1,
										   wideFileName,
										   lengthof(wideFileName));
	if (wideFileNameSize == 0)
	{
		_dosmaperr(GetLastError());
		return INVALID_HANDLE_VALUE;
	}

	/* Convert DOS/Win32 path to fully qualified NT path. */
	ntFileName.Length = 0;
	ntFileName.MaximumLength = sizeof(buffer);
	ntFileName.Buffer = buffer;
	if (!pg_RtlDosPathNameToNtPathName_U(wideFileName,
										 &ntFileName,
										 NULL,
										 NULL))
	{
		/* XXX does GetLastError() work for this function?  need the
		 * _WithStatus version maybe? */
		_dosmaperr(GetLastError());
		return INVALID_HANDLE_VALUE;
	}

	/* Convert other parameters. */
	desiredAccess =
		FILE_READ_ATTRIBUTES |		/* allow fstat() even if O_WRONLY */
		((fileFlags & O_TEMPORARY) ? DELETE : 0) |
		((fileFlags & O_RDWR) ? FILE_GENERIC_WRITE | FILE_GENERIC_READ :
		 (fileFlags & O_WRONLY) ? FILE_GENERIC_WRITE : FILE_GENERIC_READ);
	shareAccess = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
	InitializeObjectAttributes(&objectAttributes, &ntFileName,
							   OBJ_CASE_INSENSITIVE | OBJ_INHERIT, NULL, NULL);
	fileAttributes =
		((fileFlags & _O_SHORT_LIVED) ? FILE_ATTRIBUTE_TEMPORARY : 0);
	if (fileAttributes == 0)
		fileAttributes = FILE_ATTRIBUTE_NORMAL;
	if (fileFlags & O_CREAT)
	{
		if (fileFlags & O_EXCL)
			createDisposition = FILE_CREATE;
		else if (fileFlags & O_TRUNC)
			createDisposition = FILE_OVERWRITE_IF;
		else
			createDisposition = FILE_OPEN_IF;
	}
	else if (fileFlags & O_TRUNC)
		createDisposition = FILE_OVERWRITE;
	else
		createDisposition = FILE_OPEN;
	createOptions =
		FILE_SYNCHRONOUS_IO_NONALERT |
		(backup_semantics ? FILE_OPEN_FOR_BACKUP_INTENT : 0) |
		((fileFlags & O_RANDOM) ? FILE_RANDOM_ACCESS : 0) |
		((fileFlags & O_SEQUENTIAL) ? FILE_SEQUENTIAL_ONLY : 0) |
		((fileFlags & O_DIRECT) ? FILE_NO_INTERMEDIATE_BUFFERING : 0) |
		((fileFlags & O_DSYNC) ? FILE_WRITE_THROUGH : 0) |
		((fileFlags & O_TEMPORARY) ? FILE_DELETE_ON_CLOSE : 0);

	for (;;)
	{
		status = pg_NtCreateFile(&h,
								 desiredAccess,
								 &objectAttributes,
								 &ioStatusBlock,
								 NULL,
								 fileAttributes,
								 shareAccess,
								 createDisposition,
								 createOptions,
								 NULL,
								 0);

		if (NT_SUCCESS(status))
			break;

		/*
		 * Translate STATUS_DELETE_PENDING to a more Unix-like error.
		 *
		 * If there's no O_CREAT flag, then we'll pretend the file is
		 * invisible.  With O_CREAT, we have no choice but to report that
		 * there's a file in the way (which wouldn't happen on Unix).
		 */
		if (status == STATUS_DELETE_PENDING)
		{
			if (fileFlags & O_CREAT)
				err = ERROR_FILE_EXISTS;
			else
				err = ERROR_FILE_NOT_FOUND;
			_dosmaperr(err);
			return INVALID_HANDLE_VALUE;
		}

		/*
		 * For everything else, convert the NT error into a DOS error.  This
		 * is where STATUS_DELETE_PENDING would normally be mapped to
		 * ERROR_ACCESS_DENIED, and lost to us.
		 */
		err = pg_RtlNtStatusToDosError(status);

		/*
		 * Sharing violation or locking error can indicate antivirus, backup
		 * or similar software that's locking the file.  Wait a bit and try
		 * again, giving up after 30 seconds.
		 */
		if (err == ERROR_SHARING_VIOLATION ||
			err == ERROR_LOCK_VIOLATION)
		{
#ifndef FRONTEND
			if (loops == 50)
				ereport(LOG,
						(errmsg("could not open file \"%s\": %s", fileName,
								(err == ERROR_SHARING_VIOLATION) ? _("sharing violation") : _("lock violation")),
						 errdetail("Continuing to retry for 30 seconds."),
						 errhint("You might have antivirus, backup, or similar software interfering with the database system.")));
#endif

			if (loops < 300)
			{
				pg_usleep(100000);
				loops++;
				continue;
			}
		}

		_dosmaperr(err);
		return INVALID_HANDLE_VALUE;
	}

	return h;
}

int
pgwin32_open(const char *fileName, int fileFlags,...)
{
	HANDLE h;
	int fd;

	h = pgwin32_open_handle(fileName, fileFlags, false);
	if (h == INVALID_HANDLE_VALUE)
		return -1;

#ifdef FRONTEND

	/*
	 * Since PostgreSQL 12, those concurrent-safe versions of open() and
	 * fopen() can be used by frontends, having as side-effect to switch the
	 * file-translation mode from O_TEXT to O_BINARY if none is specified.
	 * Caller may want to enforce the binary or text mode, but if nothing is
	 * defined make sure that the default mode maps with what versions older
	 * than 12 have been doing.
	 */
	if ((fileFlags & O_BINARY) == 0)
		fileFlags |= O_TEXT;
#endif

	/* _open_osfhandle will, on error, set errno accordingly */
	if ((fd = _open_osfhandle((intptr_t) h, fileFlags & O_APPEND)) < 0)
		CloseHandle(h);			/* will not affect errno */
	else if (fileFlags & (O_TEXT | O_BINARY) &&
			 _setmode(fd, fileFlags & (O_TEXT | O_BINARY)) < 0)
	{
		_close(fd);
		return -1;
	}

	return fd;
}

FILE *
pgwin32_fopen(const char *fileName, const char *mode)
{
	int			openmode = 0;
	int			fd;

	if (strstr(mode, "r+"))
		openmode |= O_RDWR;
	else if (strchr(mode, 'r'))
		openmode |= O_RDONLY;
	if (strstr(mode, "w+"))
		openmode |= O_RDWR | O_CREAT | O_TRUNC;
	else if (strchr(mode, 'w'))
		openmode |= O_WRONLY | O_CREAT | O_TRUNC;
	if (strchr(mode, 'a'))
		openmode |= O_WRONLY | O_CREAT | O_APPEND;

	if (strchr(mode, 'b'))
		openmode |= O_BINARY;
	if (strchr(mode, 't'))
		openmode |= O_TEXT;

	fd = pgwin32_open(fileName, openmode);
	if (fd == -1)
		return NULL;
	return _fdopen(fd, mode);
}

#endif
