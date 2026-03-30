/*-------------------------------------------------------------------------
 *
 * astreamer_tar.c
 *
 * This module implements three types of tar processing. A tar parser
 * expects unlabelled chunks of data (e.g. ASTREAMER_UNKNOWN) and splits
 * it into labelled chunks (any other value of astreamer_archive_context).
 * A tar archiver does the reverse: it takes a bunch of labelled chunks
 * and produces a tarfile, optionally replacing member headers and trailers
 * so that upstream astreamer objects can perform surgery on the tarfile
 * contents without knowing the details of the tar format. A tar terminator
 * just adds two blocks of NUL bytes to the end of the file, since older
 * server versions produce files with this terminator omitted.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/fe_utils/astreamer_tar.c
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <time.h>

#include "common/logging.h"
#include "fe_utils/astreamer.h"
#include "pgtar.h"

#define PAX_HEADERS_NAME "PaxHeaders"
#define SPARSE_PREFIX "GNUSparseFile."

typedef struct astreamer_tar_sparse_map_entry
{
	uint64		offset;
	uint64		length;
}			astreamer_tar_sparse_map_entry;

/*
 * Information collected from a pax 'x' pseudo-file that concerns the
 * following file.
 */
typedef struct astreamer_tar_pax_extended
{
	bool		apply_next;

	struct
	{
		int			major;
		int			minor;
		uint64		realsize;
		char		name[MAXPGPATH];
	} gnu_sparse;
}			astreamer_tar_pax_extended;

typedef struct astreamer_tar_sparse_map
{
	/* Sparse map parser state. */
	size_t		input_expected;
	char		input_buffer[TAR_BLOCK_SIZE * 2];
	size_t		input_size;
	size_t		input_position;
	size_t		input_ingested;
	enum
	{
		/* States used while parsing a GNU PAX 1.0 sparse map. */
		SPARSE_MAP_EXPECT_COUNT,
		SPARSE_MAP_EXPECT_OFFSET,
		SPARSE_MAP_EXPECT_LENGTH,
		SPARSE_MAP_SYNTAX_ERROR,
		SPARSE_MAP_SKIP_PADDING,
		SPARSE_MAP_EXPECT_FILE_DATA
	}			input_state;

	/* Table of data ranges. Holes exist in between. */
	uint64		nentries;
	uint64		nfilled;
	astreamer_tar_sparse_map_entry *entries;

	/* File reconstruction state. */
	uint64		input_stream_size;
	uint64		output_offset;
	uint64		output_entry;
}			astreamer_tar_sparse_map;

typedef struct astreamer_tar_parser
{
	astreamer	base;
	astreamer_archive_context next_context;
	astreamer_member member;
	size_t		file_bytes_sent;
	size_t		pad_bytes_expected;

	enum
	{
		/* Regular files, links, directories. */
		ASTREAMER_TAR_CONTENT_PLAIN = 1 << 0,

		/* PAX 'x' headers for the following file, not forwarded. */
		ASTREAMER_TAR_CONTENT_PAX_HEADERS = 1 << 1,

		/* GNU PAX 1.0 sparse files, to be expanded on the fly. */
		ASTREAMER_TAR_CONTENT_GNU_SPARSE = 1 << 2,

		/* Skipped because we don't understand the format. */
		ASTREAMER_TAR_CONTENT_UNSUPPORTED = 1 << 3
	}			content_type;

	astreamer_tar_pax_extended pax_extended;
	astreamer_tar_sparse_map sparse_map;
} astreamer_tar_parser;

/* Content types that we forward to the next astreamer. */
#define ASTREAMER_TAR_CONTENT_FORWARD_MASK \
	(ASTREAMER_TAR_CONTENT_PLAIN | \
	 ASTREAMER_TAR_CONTENT_GNU_SPARSE)

typedef struct astreamer_tar_archiver
{
	astreamer	base;
	bool		rearchive_member;
} astreamer_tar_archiver;

static void astreamer_tar_parser_content(astreamer *streamer,
										 astreamer_member *member,
										 const char *data, int len,
										 astreamer_archive_context context);
static void astreamer_tar_parser_finalize(astreamer *streamer);
static void astreamer_tar_parser_free(astreamer *streamer);
static bool astreamer_tar_header(astreamer_tar_parser *mystreamer);

static const astreamer_ops astreamer_tar_parser_ops = {
	.content = astreamer_tar_parser_content,
	.finalize = astreamer_tar_parser_finalize,
	.free = astreamer_tar_parser_free
};

static void astreamer_tar_archiver_content(astreamer *streamer,
										   astreamer_member *member,
										   const char *data, int len,
										   astreamer_archive_context context);
static void astreamer_tar_archiver_finalize(astreamer *streamer);
static void astreamer_tar_archiver_free(astreamer *streamer);

static const astreamer_ops astreamer_tar_archiver_ops = {
	.content = astreamer_tar_archiver_content,
	.finalize = astreamer_tar_archiver_finalize,
	.free = astreamer_tar_archiver_free
};

static void astreamer_tar_terminator_content(astreamer *streamer,
											 astreamer_member *member,
											 const char *data, int len,
											 astreamer_archive_context context);
static void astreamer_tar_terminator_finalize(astreamer *streamer);
static void astreamer_tar_terminator_free(astreamer *streamer);

static const astreamer_ops astreamer_tar_terminator_ops = {
	.content = astreamer_tar_terminator_content,
	.finalize = astreamer_tar_terminator_finalize,
	.free = astreamer_tar_terminator_free
};

/*
 * Create a astreamer that can parse a stream of content as tar data.
 *
 * The input should be a series of ASTREAMER_UNKNOWN chunks; the astreamer
 * specified by 'next' will receive a series of typed chunks, as per the
 * conventions described in astreamer.h.
 */
astreamer *
astreamer_tar_parser_new(astreamer *next)
{
	astreamer_tar_parser *streamer;

	streamer = palloc0_object(astreamer_tar_parser);
	*((const astreamer_ops **) &streamer->base.bbs_ops) =
		&astreamer_tar_parser_ops;
	streamer->base.bbs_next = next;
	initStringInfo(&streamer->base.bbs_buffer);
	streamer->next_context = ASTREAMER_MEMBER_HEADER;

	return &streamer->base;
}

/*
 * Parse a PAX 'x' file.  This is a special file that precedes a regular file,
 * allowing the standard header fields to be overriden, all of which we ignore:
 *
 * https://pubs.opengroup.org/onlinepubs/9699919799/utilities/pax.html#tag_20_92_13_03
 *
 * The only attributes we look for currently are GNU's sparse file ones:
 *
 * https://www.gnu.org/software/tar/manual/html_node/PAX-1.html
 */
static void
astreamer_tar_parse_pax_extended(astreamer_tar_parser *mystreamer)
{
	astreamer_tar_pax_extended *pax = &mystreamer->pax_extended;
	char *name;
	char *value;
	char *end_line;
	char *end;
	char *p;

	memset(&pax->gnu_sparse, 0, sizeof(pax->gnu_sparse));
	pax->gnu_sparse.major = -1;
	pax->gnu_sparse.minor = -1;
	pax->gnu_sparse.realsize = -1;

	p = mystreamer->base.bbs_buffer.data;
	end = p + mystreamer->base.bbs_buffer.len;

	fprintf(stderr, "XXXXXXXXXX got PAX data: %s\n", p);
	while (p < end &&
		   (end_line = memchr(p, '\n', end - p)) &&
		   (value = strchr(p, '=')) &&
		   (name = strchr(p, ' ')))
	{
		*end_line = '\0';
		*value++ = '\0';
		*name++ = '\0';

		fprintf(stderr, "[%s] = [%s]\n", name, value);
		if (strcmp(name, "GNU.sparse.major") == 0)
			pax->gnu_sparse.major = atoi(value);
		else if (strcmp(name, "GNU.sparse.minor") == 0)
			pax->gnu_sparse.minor = atoi(value);
		else if (strcmp(name, "GNU.sparse.realsize") == 0)
			pax->gnu_sparse.realsize = strtou64(value, NULL, 10);
		else if (strcmp(name, "GNU.sparse.name") == 0)
			strlcpy(pax->gnu_sparse.name, value, lengthof(pax->gnu_sparse.name));
	}

	/* Applies to the following file. */
	pax->apply_next = true;
}

/*
 * Handle the extended header fields that we understand.
 */
static void
astreamer_tar_apply_pax_extended(astreamer_tar_parser *mystreamer,
								 astreamer_member *member)
{
	const astreamer_tar_pax_extended *pax = &mystreamer->pax_extended;
	astreamer_tar_sparse_map *map = &mystreamer->sparse_map;
	char *p;

	/* Extended headers apply to exactly one following file. */
	Assert(mystreamer->pax_extended.apply_next);
	mystreamer->pax_extended.apply_next = false;

	/*
	 * PAX extended attributes that don't include GNU sparse information can
	 * be ignored.  High resolution mtime etc.
	 */
	if (pax->gnu_sparse.major == -1 &&
		pax->gnu_sparse.minor == -1)
		return;

	/*
	 * GNU sparse of an unexpected version, or missing required properties?
	 * Skip the file, we can't decode it.  It will appear that the file is
	 * missing.
	 *
	 * XXX Should this be an error?
	 */
	if (pax->gnu_sparse.major != 1 ||
		pax->gnu_sparse.minor != 0 ||
		pax->gnu_sparse.realsize == -1 ||
		pax->gnu_sparse.name[0] == '\0')
		mystreamer->content_type = ASTREAMER_TAR_CONTENT_UNSUPPORTED;

	/* Remember the input and output file sizes. */
	map->input_stream_size = member->size;
	member->size = pax->gnu_sparse.realsize;

	/* Replace the filename. */
	p = strrchr(member->pathname, '/');
	if (!p)
		p = member->pathname;	
	strlcpy(p,
			pax->gnu_sparse.name,
			sizeof(member->pathname) - (p - member->pathname));

	/* We are ready to decode a sparse file. */
	map->input_state = SPARSE_MAP_EXPECT_COUNT;
	map->input_size = 0;
	map->input_position = 0;
	map->input_expected = TAR_BLOCK_SIZE;
	map->output_offset = 0;
	map->output_entry = 0;
	mystreamer->content_type = ASTREAMER_TAR_CONTENT_GNU_SPARSE;
}

static void
astreamer_tar_sparse_map_expand_hole(astreamer_tar_parser *mystreamer,
									 uint64 hole_end)
{
	astreamer_tar_sparse_map *map = &mystreamer->sparse_map;

	while (hole_end > map->output_offset)
	{
		static const char zeroes[TAR_BLOCK_SIZE] = {0};
		size_t		size = hole_end - map->output_offset;

		if (size > sizeof(zeroes))
			size = sizeof(zeroes);

		fprintf(stderr, "XXX expanding hole %zu + %zu = %zu\n",
				map->output_offset,
				size,
				map->output_offset + size);
		astreamer_content(mystreamer->base.bbs_next,
						  &mystreamer->member,
						  zeroes,
						  size,
						  ASTREAMER_MEMBER_CONTENTS);

		map->output_offset += size;
	}
}

static size_t
astreamer_tar_sparse_map_expand(astreamer_tar_parser *mystreamer,
								const char *data,
								size_t size)
{
	astreamer_tar_sparse_map *map = &mystreamer->sparse_map;
	astreamer_member *member = &mystreamer->member;
	size_t		output_file_size = member->size;

	if (map->output_entry < map->nentries)
	{
		astreamer_tar_sparse_map_entry *head_data;
		size_t		head_remaining;

		head_data = &map->entries[map->output_entry];

		/* If there is a hole before this entry, expand it first. */
		if (map->output_offset < head_data->offset)
			astreamer_tar_sparse_map_expand_hole(mystreamer,
												 head_data->offset);

		/* Send remaining data, stopping at the next hole. */
		Assert(map->output_offset >= head_data->offset);
		head_remaining = head_data->length -
			(head_data->offset - map->output_offset);

		/* But not more data than we have received. */
		if (size > head_remaining)
			size = head_remaining;

		fprintf(stderr, "XXX expanding data %zu + %zu = %zu\n",
				map->output_offset,
				size,
				map->output_offset + size);
		astreamer_content(mystreamer->base.bbs_next,
						  &mystreamer->member,
						  data, size,
						  ASTREAMER_MEMBER_CONTENTS);
		map->output_offset += size;

		/* Have we exhausted this data entry? */
		if (map->output_offset >= head_data->offset + head_data->length)
		{
			map->output_entry++;

			/* If that was the last one, there might be a final hole. */
			if (map->output_entry == map->nentries &&
				map->output_offset < output_file_size)
				astreamer_tar_sparse_map_expand_hole(mystreamer,
													 output_file_size);
		}
	}
	else
	{
		/*
		 * There is more data in the file that the sparse map told us. Discard
		 * the rest.
		 */
	}
	return size;
}

static void
astreamer_tar_sparse_map_end(astreamer_tar_sparse_map * map)
{
	if (map->entries)
	{
		pfree(map->entries);
		map->entries = NULL;
	}
}

/*
 * Try to read a line from map->buffer and advance map->position to the next
 * line, asking for more data if appropriate.
 */
static bool
astreamer_tar_sparse_map_lex(astreamer_tar_sparse_map * map,
							 uint64 *number)
{
	const char *p;
	char	   *end;
	size_t		length;
	size_t		remaining;
	char	   *newline;

	Assert(map->input_size >= map->input_position);
	remaining = map->input_size - map->input_position;

	p = &map->input_buffer[map->input_position];
	newline = memchr(p, '\n', remaining);
	if (newline)
	{
		/* Replace newline with NUL terminator. */
		*newline = '\0';
		*number = strtou64(p, &end, 10);
		/* fprintf(stderr, "GOT %zu\n", *number); */
		if (*number == UINT64_MAX || end == p)
		{
			map->input_state = SPARSE_MAP_SYNTAX_ERROR;
			return false;
		}

		/* Step over the number + terminator. */
		length = end - p;
		length += 1;
		map->input_position += length;

		/* Recycle buffer if we happen to hit end. */
		if (map->input_position == map->input_size)
		{
			map->input_position = 0;
			map->input_size = 0;
		}
		return true;
	}
	else
	{
		fprintf(stderr, "NO NEWLINE\n");
		/* Need more data.  Keep partial number, and recycle buffer. */
		if (remaining > 0)
			memmove(&map->input_buffer[0], p, remaining);
		map->input_position = 0;
		map->input_size = remaining;

		/* Do we need a new block from the source? */
		if (map->input_expected == 0)
			map->input_expected = TAR_BLOCK_SIZE;
		return false;
	}
}

/*
 * Parse more of the sparse map.  sparse_map.bytes expected should be non-zero
 * on entry, and will be non-zero on exit if more data is expected. Returns
 * the number of bytes consumed by the sparse map.  If it is less than the
 * size passed in, then some data was not consumed because it is data from the
 * file.
 *
 * https://www.gnu.org/software/tar/manual/html_node/PAX-1.html
 */
static size_t
astreamer_tar_sparse_map_parse(astreamer_tar_parser *mystreamer,
							   const char *data,
							   size_t size)
{
	astreamer_tar_sparse_map *map = &mystreamer->sparse_map;
	size_t		ingested_sum = 0;

	for (int i = 0; i < Min(64, size); ++i)
		fprintf(stderr, "XXX data[%d] = %02x [%c]\n", i, data[i], data[i]);
	Assert(map->input_expected > 0);
	Assert(map->input_state != SPARSE_MAP_EXPECT_FILE_DATA);
	Assert(map->input_state != SPARSE_MAP_SYNTAX_ERROR);

	for (;;)
	{
		uint64	   *expect;

		/* Do we need to copy more data into map->buffer? */
		if (map->input_expected > 0)
		{
			size_t		ingested;

			/* Do we need to wait to be called again to do that? */
			if (size == 0)
				break;

			/* Ingest more data. */
			fprintf(stderr, "XXX bytes_expected = %zu, input_size = %zu, input_position = %zu\n", map->input_expected, map->input_size, map->input_position);

			ingested = Min(size, map->input_expected);
			if (!(ingested <= sizeof(map->input_buffer) - map->input_size))
				fprintf(stderr, "ingested = %zu, input_size = %zu", ingested, map->input_size);

			/*
			 * The maximum we could go over TAR_BLOCK_SIZE is the length of an
			 * integer in ASCII + newline, but we have space for
			 * TAR_BLOCK_SIZE * 2.
			 */
			Assert(ingested <= TAR_BLOCK_SIZE);
			Assert(ingested <= sizeof(map->input_buffer) - map->input_size);
			memcpy(&map->input_buffer[map->input_size], data, ingested);

			map->input_size += ingested;
			map->input_expected -= ingested;

			data += ingested;
			size -= ingested;
			ingested_sum += ingested;
			map->input_ingested += ingested;
		}

		switch (map->input_state)
		{
			case SPARSE_MAP_EXPECT_COUNT:
				expect = &map->nentries;
				if (!astreamer_tar_sparse_map_lex(map, expect))
					continue;
				Assert(map->entries == NULL);
				map->entries = palloc_array(astreamer_tar_sparse_map_entry,
											map->nentries);
				map->nfilled = 0;
				map->input_state = SPARSE_MAP_EXPECT_OFFSET;
				fprintf(stderr, "XXX got count = %zu\n", map->nentries);
				continue;

			case SPARSE_MAP_EXPECT_OFFSET:
				expect = &map->entries[map->nfilled].offset;
				if (!astreamer_tar_sparse_map_lex(map, expect))
					continue;
				map->input_state = SPARSE_MAP_EXPECT_LENGTH;
				fprintf(stderr, "XXX got offset[%zu] = %zu\n", map->nfilled, map->entries[map->nfilled].offset);
				continue;

			case SPARSE_MAP_EXPECT_LENGTH:
				expect = &map->entries[map->nfilled].length;
				if (!astreamer_tar_sparse_map_lex(map, expect))
					continue;
				fprintf(stderr, "XXX got length[%zu] = %zu\n", map->nfilled, map->entries[map->nfilled].length);
				if (map->nfilled > 0 &&
					map->entries[map->nfilled - 1].offset + map->entries[map->nfilled - 1].length != map->entries[map->nfilled].offset)
					fprintf(stderr, "XXX XXXXXXXX THERE IS A HOLE\n");
				if (++map->nfilled == map->nentries)
				{
					fprintf(stderr, "XXXXXXX going to skip.  expected = %zu, pos = %zu, size = %zu\n", map->input_expected, map->input_position, map->input_size);
					map->input_state = SPARSE_MAP_SKIP_PADDING;
				}
				else
					map->input_state = SPARSE_MAP_EXPECT_OFFSET;
				continue;

			case SPARSE_MAP_SKIP_PADDING:
				fprintf(stderr, "XXX SKIP PADDING %zu\n", map->input_expected);
				if (map->input_expected == 0)
				{
					map->input_state = SPARSE_MAP_EXPECT_FILE_DATA;
					Assert(map->input_ingested % TAR_BLOCK_SIZE == 0);
					return ingested_sum;
				}
				continue;

			case SPARSE_MAP_SYNTAX_ERROR:
			case SPARSE_MAP_EXPECT_FILE_DATA:
				return ingested_sum;
		}
	}

	return ingested_sum;
}

/*
 * Parse unknown content as tar data.
 */
static void
astreamer_tar_parser_content(astreamer *streamer, astreamer_member *member,
							 const char *data, int len,
							 astreamer_archive_context context)
{
	astreamer_tar_parser *mystreamer = (astreamer_tar_parser *) streamer;
	size_t		nbytes;

	/* Expect unparsed input. */
	Assert(member == NULL);
	Assert(context == ASTREAMER_UNKNOWN);

	while (len > 0)
	{
		switch (mystreamer->next_context)
		{
			case ASTREAMER_MEMBER_HEADER:

				/*
				 * If we're expecting an archive member header, accumulate a
				 * full block of data before doing anything further.
				 */
				if (!astreamer_buffer_until(streamer, &data, &len,
											TAR_BLOCK_SIZE))
					return;

				/*
				 * Now we can process the header and get ready to process the
				 * file contents; however, we might find out that what we
				 * thought was the next file header is actually the start of
				 * the archive trailer. Switch modes accordingly.
				 */
				if (astreamer_tar_header(mystreamer))
				{
					if (mystreamer->member.size == 0)
					{
						/* No content; trailer is zero-length. */
						astreamer_content(mystreamer->base.bbs_next,
										  &mystreamer->member,
										  NULL, 0,
										  ASTREAMER_MEMBER_TRAILER);

						/* Expect next header. */
						mystreamer->next_context = ASTREAMER_MEMBER_HEADER;
					}
					else
					{
						/* Expect contents. */
						mystreamer->next_context = ASTREAMER_MEMBER_CONTENTS;
					}
					mystreamer->base.bbs_buffer.len = 0;
					mystreamer->file_bytes_sent = 0;
				}
				else
					mystreamer->next_context = ASTREAMER_ARCHIVE_TRAILER;
				break;

			case ASTREAMER_MEMBER_CONTENTS:

				fprintf(stderr, "XXX ASTREAMER_MEMBER_CONTENTS, content type %d\n", mystreamer->content_type);
				switch (mystreamer->content_type)
				{
					case ASTREAMER_TAR_CONTENT_PAX_HEADERS:						
						/* Buffer the whole pseudo-file. */
						if (!astreamer_buffer_until(streamer, &data, &len,
													mystreamer->member.size))
							return;
						astreamer_tar_parse_pax_extended(mystreamer);
						mystreamer->next_context = ASTREAMER_MEMBER_TRAILER;
						return;

					case ASTREAMER_TAR_CONTENT_UNSUPPORTED:
						/* XXX astreamer_discard_until()? */
						fprintf(stderr, "XXXX i will discard unsupported, the size is %zu\n", member->size);
						if (!astreamer_buffer_until(streamer, &data, &len,
													mystreamer->member.size))
							return;
						mystreamer->next_context = ASTREAMER_MEMBER_TRAILER;
						return;
						
					case ASTREAMER_TAR_CONTENT_GNU_SPARSE:
						fprintf(stderr, "XXXX i will expand stuff\n");
						if (mystreamer->sparse_map.input_expected > 0)
						{
							size_t		sparse_map_bytes;

							/*
							 * Divert data to the sparse map until it is
							 * ready.
							 */
							sparse_map_bytes =
								astreamer_tar_sparse_map_parse(mystreamer,
															   data,
															   len);
							data += sparse_map_bytes;
							len -= sparse_map_bytes;
						}

						/* Expand any remaining data. */
						while (len > 0)
						{
							nbytes = astreamer_tar_sparse_map_expand(mystreamer,
																	 data,
																	 len);
							data += nbytes;
							len -= nbytes;
						}
						break;

					case ASTREAMER_TAR_CONTENT_PLAIN:

						/*
						 * Send as much content as we have, but not more than
						 * the remaining file length.
						 */
						Assert(mystreamer->file_bytes_sent < mystreamer->member.size);
						nbytes = mystreamer->member.size - mystreamer->file_bytes_sent;
						nbytes = Min(nbytes, len);

						Assert(nbytes > 0);
						astreamer_content(mystreamer->base.bbs_next,
										  &mystreamer->member,
										  data, nbytes,
										  ASTREAMER_MEMBER_CONTENTS);
						mystreamer->file_bytes_sent += nbytes;
						data += nbytes;
						len -= nbytes;
						break;
				}
				fprintf(stderr, "XXX sanity check ASTREAMER_MEMBER_CONTENTS, content type %d\n", mystreamer->content_type);

				/*
				 * If we've not yet sent the whole file, then there's more
				 * content to come; otherwise, it's time to expect the file
				 * trailer.
				 */
				Assert(mystreamer->file_bytes_sent <= mystreamer->member.size);
				if (mystreamer->file_bytes_sent == mystreamer->member.size)
				{
					if (mystreamer->pad_bytes_expected == 0)
					{
						/* Trailer is zero-length. */
						astreamer_content(mystreamer->base.bbs_next,
										  &mystreamer->member,
										  NULL, 0,
										  ASTREAMER_MEMBER_TRAILER);

						/* Expect next header. */
						mystreamer->next_context = ASTREAMER_MEMBER_HEADER;
					}
					else
					{
						/* Trailer is not zero-length. */
						mystreamer->next_context = ASTREAMER_MEMBER_TRAILER;
					}
					mystreamer->base.bbs_buffer.len = 0;
				}
				break;

			case ASTREAMER_MEMBER_TRAILER:

				/*
				 * If we're expecting an archive member trailer, accumulate
				 * the expected number of padding bytes before sending
				 * anything onward.
				 */
				if (!astreamer_buffer_until(streamer, &data, &len,
											mystreamer->pad_bytes_expected))
					return;

				/* OK, now we can send it. */
				if (mystreamer->content_type & ASTREAMER_TAR_CONTENT_FORWARD_MASK)
					astreamer_content(mystreamer->base.bbs_next,
									  &mystreamer->member,
									  data, mystreamer->pad_bytes_expected,
									  ASTREAMER_MEMBER_TRAILER);

				/* Expect next file header. */
				mystreamer->next_context = ASTREAMER_MEMBER_HEADER;
				mystreamer->base.bbs_buffer.len = 0;
				break;

			case ASTREAMER_ARCHIVE_TRAILER:

				/*
				 * We've seen an end-of-archive indicator, so anything more is
				 * buffered and sent as part of the archive trailer.
				 *
				 * Per POSIX, the last physical block of a tar archive is
				 * always full-sized, so there may be undefined data after the
				 * two zero blocks that mark end-of-archive.  GNU tar, for
				 * example, zero-pads to a 10kB boundary by default.  We just
				 * buffer whatever we receive and pass it along at finalize
				 * time.
				 */
				astreamer_buffer_bytes(streamer, &data, &len, len);
				return;

			default:
				/* Shouldn't happen. */
				pg_fatal("unexpected state while parsing tar archive");
		}
	}
}

/*
 * Parse a file header within a tar stream.
 *
 * The return value is true if we found a file header and passed it on to the
 * next astreamer; it is false if we have reached the archive trailer.
 */
static bool
astreamer_tar_header(astreamer_tar_parser *mystreamer)
{
	bool		has_nonzero_byte = false;
	int			i;
	astreamer_member *member = &mystreamer->member;
	char	   *buffer = mystreamer->base.bbs_buffer.data;

	Assert(mystreamer->base.bbs_buffer.len == TAR_BLOCK_SIZE);

	/* Check whether we've got a block of all zero bytes. */
	for (i = 0; i < TAR_BLOCK_SIZE; ++i)
	{
		if (buffer[i] != '\0')
		{
			has_nonzero_byte = true;
			break;
		}
	}

	/*
	 * If the entire block was zeros, this is the end of the archive, not the
	 * start of the next file.
	 */
	if (!has_nonzero_byte)
		return false;

	/*
	 * PAX extended headers are psueudo-files whose body describes the
	 * following file.  We don't want to forward it to the next astreamer as
	 * it's not logically a file, but we also don't want to end the stream.
	 */
	if (buffer[TAR_OFFSET_TYPEFLAG] == TAR_FILETYPE_PAX_HEADER)
	{
		mystreamer->content_type = ASTREAMER_TAR_CONTENT_PAX_HEADERS;
		strlcpy(member->pathname, &buffer[TAR_OFFSET_NAME], MAXPGPATH);
		member->size = read_tar_number(&buffer[TAR_OFFSET_SIZE], 12);
		mystreamer->pad_bytes_expected = tarPaddingBytesRequired(member->size);
		fprintf(stderr, "XXXXX I got a PAX header! size is %zu\n", member->size);
		return true;
	}

	/*
	 * Parse key fields out of the header.
	 */
	strlcpy(member->pathname, &buffer[TAR_OFFSET_NAME], MAXPGPATH);
	if (member->pathname[0] == '\0')
		pg_fatal("tar member has empty name");


	/* Default assumptions that might be modified by a PAX header below. */
	mystreamer->content_type = ASTREAMER_TAR_CONTENT_PLAIN;
	member->size = read_tar_number(&buffer[TAR_OFFSET_SIZE], 12);
	member->mode = read_tar_number(&buffer[TAR_OFFSET_MODE], 8);
	member->uid = read_tar_number(&buffer[TAR_OFFSET_UID], 8);
	member->gid = read_tar_number(&buffer[TAR_OFFSET_GID], 8);
	member->is_directory =
		(buffer[TAR_OFFSET_TYPEFLAG] == TAR_FILETYPE_DIRECTORY);
	member->is_link =
		(buffer[TAR_OFFSET_TYPEFLAG] == TAR_FILETYPE_SYMLINK);
	if (member->is_link)
		strlcpy(member->linktarget, &buffer[TAR_OFFSET_LINKNAME], 100);

	/* Compute number of padding bytes. */
	mystreamer->pad_bytes_expected = tarPaddingBytesRequired(member->size);

	/*
	 * Make any adjustments to the above based on the contents of a preceding
	 * PAX extended header.
	 */
	if (mystreamer->pax_extended.apply_next)
		astreamer_tar_apply_pax_extended(mystreamer, member);

	/* Forward the entire header to the next astreamer. */
	if (mystreamer->content_type & ASTREAMER_TAR_CONTENT_FORWARD_MASK)
		astreamer_content(mystreamer->base.bbs_next, member,
						  buffer, TAR_BLOCK_SIZE,
						  ASTREAMER_MEMBER_HEADER);

	return true;
}

/*
 * End-of-stream processing for a tar parser.
 */
static void
astreamer_tar_parser_finalize(astreamer *streamer)
{
	astreamer_tar_parser *mystreamer = (astreamer_tar_parser *) streamer;

	if (mystreamer->next_context != ASTREAMER_ARCHIVE_TRAILER &&
		(mystreamer->next_context != ASTREAMER_MEMBER_HEADER ||
		 mystreamer->base.bbs_buffer.len > 0))
		pg_fatal("COPY stream ended before last file was finished");

	/* Send the archive trailer, even if empty. */
	astreamer_content(streamer->bbs_next, NULL,
					  streamer->bbs_buffer.data, streamer->bbs_buffer.len,
					  ASTREAMER_ARCHIVE_TRAILER);

	/* Now finalize successor. */
	astreamer_finalize(streamer->bbs_next);
}

/*
 * Free memory associated with a tar parser.
 */
static void
astreamer_tar_parser_free(astreamer *streamer)
{
	pfree(streamer->bbs_buffer.data);
	astreamer_free(streamer->bbs_next);
}

/*
 * Create a astreamer that can generate a tar archive.
 *
 * This is intended to be usable either for generating a brand-new tar archive
 * or for modifying one on the fly. The input should be a series of typed
 * chunks (i.e. not ASTREAMER_UNKNOWN). See also the comments for
 * astreamer_tar_parser_content.
 */
astreamer *
astreamer_tar_archiver_new(astreamer *next)
{
	astreamer_tar_archiver *streamer;

	streamer = palloc0_object(astreamer_tar_archiver);
	*((const astreamer_ops **) &streamer->base.bbs_ops) =
		&astreamer_tar_archiver_ops;
	streamer->base.bbs_next = next;

	return &streamer->base;
}

/*
 * Fix up the stream of input chunks to create a valid tar file.
 *
 * If a ASTREAMER_MEMBER_HEADER chunk is of size 0, it is replaced with a
 * newly-constructed tar header. If it is of size TAR_BLOCK_SIZE, it is
 * passed through without change. Any other size is a fatal error (and
 * indicates a bug).
 *
 * Whenever a new ASTREAMER_MEMBER_HEADER chunk is constructed, the
 * corresponding ASTREAMER_MEMBER_TRAILER chunk is also constructed from
 * scratch. Specifically, we construct a block of zero bytes sufficient to
 * pad out to a block boundary, as required by the tar format. Other
 * ASTREAMER_MEMBER_TRAILER chunks are passed through without change.
 *
 * Any ASTREAMER_MEMBER_CONTENTS chunks are passed through without change.
 *
 * The ASTREAMER_ARCHIVE_TRAILER chunk is replaced with two
 * blocks of zero bytes. Not all tar programs require this, but apparently
 * some do. The server does not supply this trailer. If no archive trailer is
 * present, one will be added by astreamer_tar_parser_finalize.
 */
static void
astreamer_tar_archiver_content(astreamer *streamer,
							   astreamer_member *member,
							   const char *data, int len,
							   astreamer_archive_context context)
{
	astreamer_tar_archiver *mystreamer = (astreamer_tar_archiver *) streamer;
	char		buffer[2 * TAR_BLOCK_SIZE];

	Assert(context != ASTREAMER_UNKNOWN);

	if (context == ASTREAMER_MEMBER_HEADER && len != TAR_BLOCK_SIZE)
	{
		Assert(len == 0);

		/* Replace zero-length tar header with a newly constructed one. */
		tarCreateHeader(buffer, member->pathname, NULL,
						member->size, member->mode, member->uid, member->gid,
						time(NULL));
		data = buffer;
		len = TAR_BLOCK_SIZE;

		/* Also make a note to replace padding, in case size changed. */
		mystreamer->rearchive_member = true;
	}
	else if (context == ASTREAMER_MEMBER_TRAILER &&
			 mystreamer->rearchive_member)
	{
		int			pad_bytes = tarPaddingBytesRequired(member->size);

		/* Also replace padding, if we regenerated the header. */
		memset(buffer, 0, pad_bytes);
		data = buffer;
		len = pad_bytes;

		/* Don't do this again unless we replace another header. */
		mystreamer->rearchive_member = false;
	}
	else if (context == ASTREAMER_ARCHIVE_TRAILER)
	{
		/* Trailer should always be two blocks of zero bytes. */
		memset(buffer, 0, 2 * TAR_BLOCK_SIZE);
		data = buffer;
		len = 2 * TAR_BLOCK_SIZE;
	}

	astreamer_content(streamer->bbs_next, member, data, len, context);
}

/*
 * End-of-stream processing for a tar archiver.
 */
static void
astreamer_tar_archiver_finalize(astreamer *streamer)
{
	astreamer_finalize(streamer->bbs_next);
}

/*
 * Free memory associated with a tar archiver.
 */
static void
astreamer_tar_archiver_free(astreamer *streamer)
{
	astreamer_free(streamer->bbs_next);
	pfree(streamer);
}

/*
 * Create a astreamer that blindly adds two blocks of NUL bytes to the
 * end of an incomplete tarfile that the server might send us.
 */
astreamer *
astreamer_tar_terminator_new(astreamer *next)
{
	astreamer  *streamer;

	streamer = palloc0_object(astreamer);
	*((const astreamer_ops **) &streamer->bbs_ops) =
		&astreamer_tar_terminator_ops;
	streamer->bbs_next = next;

	return streamer;
}

/*
 * Pass all the content through without change.
 */
static void
astreamer_tar_terminator_content(astreamer *streamer,
								 astreamer_member *member,
								 const char *data, int len,
								 astreamer_archive_context context)
{
	/* Expect unparsed input. */
	Assert(member == NULL);
	Assert(context == ASTREAMER_UNKNOWN);

	/* Just forward it. */
	astreamer_content(streamer->bbs_next, member, data, len, context);
}

/*
 * At the end, blindly add the two blocks of NUL bytes which the server fails
 * to supply.
 */
static void
astreamer_tar_terminator_finalize(astreamer *streamer)
{
	char		buffer[2 * TAR_BLOCK_SIZE];

	memset(buffer, 0, 2 * TAR_BLOCK_SIZE);
	astreamer_content(streamer->bbs_next, NULL, buffer,
					  2 * TAR_BLOCK_SIZE, ASTREAMER_UNKNOWN);
	astreamer_finalize(streamer->bbs_next);
}

/*
 * Free memory associated with a tar terminator.
 */
static void
astreamer_tar_terminator_free(astreamer *streamer)
{
	astreamer_free(streamer->bbs_next);
	pfree(streamer);
}
