/*-------------------------------------------------------------------------
 *
 * rmtree.c
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/rmtree.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <unistd.h>
#include <sys/stat.h>

#include "common/file_utils.h"

#ifndef FRONTEND
#define pg_log_warning(...) elog(WARNING, __VA_ARGS__)
#define LOG_LEVEL WARNING
#else
#include "common/logging.h"
#define LOG_LEVEL PG_LOG_WARNING
#endif

/*
 *	rmtree
 *
 *	Delete a directory tree recursively.
 *	Assumes path points to a valid directory.
 *	Deletes everything under path.
 *	If rmtopdir is true deletes the directory too.
 *	Returns true if successful, false if there was any problem.
 *	(The details of the problem are reported already, so caller
 *	doesn't really have to say anything more, but most do.)
 */
bool
rmtree(const char *path, bool rmtopdir)
{
	char		pathbuf[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	bool		result = true;

	dir = opendir(path);
	if (dir == NULL)
	{
		pg_log_warning("could not open directory \"%s\": %m", path);
		return false;
	}

	while (errno = 0, (de = readdir(dir)))
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;
		snprintf(pathbuf, sizeof(pathbuf), "%s/%s", path, de->d_name);
		switch (get_dirent_type(pathbuf, de, false, LOG_LEVEL))
		{
			case PGFILETYPE_ERROR:
				/* already logged, press on */
				break;
			case PGFILETYPE_DIR:
				if (!rmtree(pathbuf, true))
					result = false;
				break;
			default:
				if (unlink(pathbuf) != 0 && errno != ENOENT)
				{
					pg_log_warning("could not unlink file \"%s\": %m", pathbuf);
					result = false;
				}
				break;
		}
	}

	if (errno != 0)
	{
		pg_log_warning("could not read directory \"%s\": %m", path);
		result = false;
	}

	closedir(dir);

	if (rmtopdir)
	{
		if (rmdir(path) != 0)
		{
			pg_log_warning("could not remove directory \"%s\": %m", path);
			result = false;
		}
	}

	return result;
}
