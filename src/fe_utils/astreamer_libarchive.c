/*-------------------------------------------------------------------------
 *
 * astreamer_libarchive.c
 *
 * This module reads from archives using https://www.libarchive.org/.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/fe_utils/astreamer_libarchive.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <archive.h>
#include <archive_entry.h>

#include "common/logging.h"
#include "fe_utils/astreamer.h"

/* This is the data size we'll try to stream at once. */
#define ASTREAMER_LIBARCHIVE_READER_BUFFER_SIZE (128 * 1024)

typedef struct astreamer_libarchive_reader
{
	astreamer	base;
	astreamer_member member;
	struct archive *archive;
	bool		end_of_file;
	bool		end_of_archive;
	pgoff_t		offset;
	char		zeroes[8192];
} astreamer_libarchive_reader;

static bool astreamer_libarchive_reader_pull_content(astreamer *streamer);
static void astreamer_libarchive_reader_finalize(astreamer *streamer);
static void astreamer_libarchive_reader_free(astreamer *streamer);

static const astreamer_ops astreamer_libarchive_reader_ops = {
	.pull_content = astreamer_libarchive_reader_pull_content,
	.finalize = astreamer_libarchive_reader_finalize,
	.free = astreamer_libarchive_reader_free
};

/*
 * Create an astreamer that decodes 'pathname' with libarchive and feeds its
 * contents to 'next'.  This streamer is a source that must be the first in
 * the chain, and content should be produced by calling
 * astreamer_pull_content(streamer).
 */
astreamer *
astreamer_libarchive_reader_new(astreamer *next, const char *pathname)
{
	astreamer_libarchive_reader *streamer;
	int			r;

	streamer = palloc0_object(astreamer_libarchive_reader);
	*((const astreamer_ops **) &streamer->base.bbs_ops) =
		&astreamer_libarchive_reader_ops;
	streamer->base.bbs_next = next;

	/* Prepare to read tar archives with any known compression filter. */
	streamer->archive = archive_read_new();
	if (streamer->archive == NULL)
		pg_fatal("out of memory");
	if (archive_read_support_format_tar(streamer->archive) != ARCHIVE_OK)
		pg_fatal("libarchive: could not initialize tar format: %s",
				 archive_error_string(streamer->archive));
	if (archive_read_support_filter_all(streamer->archive) != ARCHIVE_OK)
		pg_fatal("libarchive: could not initialize tar filter: %s",
				 archive_error_string(streamer->archive));

	/* Open file. */
	r = archive_read_open_filename(streamer->archive,
								   pathname,
								   ASTREAMER_LIBARCHIVE_READER_BUFFER_SIZE);
	if (r != ARCHIVE_OK)
		pg_fatal("libarchive: could not open \"%s\": %s",
				 pathname,
				 archive_error_string(streamer->archive));

	/* Start by wanting a new file. */
	streamer->end_of_file = true;
	streamer->end_of_archive = false;

	return &streamer->base;
}

/* Fill in an astreamer member given a libarchive entry. */
static void
astreamer_libarchive_reader_fill_member(astreamer_member *member,
										struct archive_entry *entry)
{
	strlcpy(member->pathname,
			archive_entry_pathname(entry),
			sizeof(member->pathname));
	member->size = archive_entry_size(entry);
	member->mode = archive_entry_mode(entry);
	member->uid = archive_entry_uid(entry);
	member->gid = archive_entry_gid(entry);
	switch (archive_entry_filetype(entry))
	{
		case AE_IFREG:
			member->is_regular = true;
			break;
		case AE_IFDIR:
			member->is_directory = true;
			break;
		case AE_IFLNK:
			member->is_symlink = true;
			strlcpy(member->linktarget,
					archive_entry_symlink(entry),
					sizeof(member->linktarget));
			break;
		default:
			break;
	}
}

/* Emit zeroes up to offset. */
static void
astreamer_libarchive_reader_expand_sparse(astreamer_libarchive_reader *mystreamer,
										  pgoff_t offset)
{
	size_t		size;

	while (mystreamer->offset < offset)
	{
		size = offset - mystreamer->offset;
		size = Min(size, sizeof(mystreamer->zeroes));
		astreamer_content(mystreamer->base.bbs_next,
						  &mystreamer->member,
						  mystreamer->zeroes,
						  size,
						  ASTREAMER_MEMBER_CONTENTS);
		mystreamer->offset += size;
	}
}

static bool
astreamer_libarchive_reader_pull_content(astreamer *streamer)
{
	astreamer_libarchive_reader *mystreamer;
	const void *data;
	size_t		size;
	pgoff_t		offset;

	mystreamer = (astreamer_libarchive_reader *) streamer;

	while (!mystreamer->end_of_archive)
	{
		/* Do we need a new file? */
		if (mystreamer->end_of_file)
		{
			struct archive_entry *entry;

			/* Start next file, or discover end of archive. */
			switch (archive_read_next_header(mystreamer->archive, &entry))
			{
				case ARCHIVE_RETRY:
					continue;
				case ARCHIVE_FATAL:
					pg_fatal("libarchive: %s",
							 archive_error_string(mystreamer->archive));
					break;
				case ARCHIVE_WARN:
					pg_log_warning("libarchive: %s",
								   archive_error_string(mystreamer->archive));
					pg_fallthrough;
				case ARCHIVE_OK:
					/* Send file header, then fall through to send one chunk. */
					mystreamer->end_of_file = false;
					mystreamer->offset = 0;
					astreamer_libarchive_reader_fill_member(&mystreamer->member,
															entry);
					astreamer_content(mystreamer->base.bbs_next,
									  &mystreamer->member,
									  NULL,
									  0,
									  ASTREAMER_MEMBER_HEADER);
					break;
				case ARCHIVE_EOF:
					/* End of archive. */
					mystreamer->end_of_archive = true;
					astreamer_content(mystreamer->base.bbs_next,
									  NULL,
									  NULL,
									  0,
									  ASTREAMER_ARCHIVE_TRAILER);
					break;
				default:
					pg_fatal("unexpected result from archive_read_next_header()");
					break;
			}
		}

		/*
		 * Stream a chunk of data, or discover end of file.
		 *
		 * It would be a bit simpler to use archive_read_data(), but this
		 * interface removes the need for copying to an output buffer.  In
		 * exchange for that, we have to deal with expanding (rare) sparse
		 * file zeroes.
		 */
		Assert(!mystreamer->end_of_file);
		switch (archive_read_data_block(mystreamer->archive,
										&data,
										&size,
										&offset))
		{
			case ARCHIVE_RETRY:
				continue;
			case ARCHIVE_FATAL:
				pg_fatal("libarchive: %s",
						 archive_error_string(mystreamer->archive));
				pg_unreachable();
			case ARCHIVE_WARN:
				pg_log_warning("libarchive: %s",
							   archive_error_string(mystreamer->archive));
				break;
			case ARCHIVE_EOF:
				size = 0;
				break;
			case ARCHIVE_OK:
				break;
			default:
				pg_fatal("unexpected result from archive_read_next_data_block()");
				break;
		}

		/* Expand any intervening sparse region. */
		astreamer_libarchive_reader_expand_sparse(mystreamer, offset);

		if (size == 0)
		{
			/* Send trailer, and go around to start another file. */
			mystreamer->end_of_file = true;
			astreamer_content(mystreamer->base.bbs_next,
							  &mystreamer->member,
							  NULL,
							  0,
							  ASTREAMER_MEMBER_TRAILER);
			continue;
		}

		/* Stream large chunk directly from libarchive's buffer and return. */
		Assert(mystreamer->offset == offset);
		astreamer_content(mystreamer->base.bbs_next,
						  &mystreamer->member,
						  data,
						  size,
						  ASTREAMER_MEMBER_CONTENTS);
		mystreamer->offset += size;
		return true;
	}
	return false;
}

static void
astreamer_libarchive_reader_finalize(astreamer *streamer)
{
	astreamer_finalize(streamer->bbs_next);
}

static void
astreamer_libarchive_reader_free(astreamer *streamer)
{
	astreamer_libarchive_reader *mystreamer;

	mystreamer = (astreamer_libarchive_reader *) streamer;
	archive_free(mystreamer->archive);
	pfree(streamer);
}
