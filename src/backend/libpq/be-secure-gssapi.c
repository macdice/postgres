/*-------------------------------------------------------------------------
 *
 * be-secure-gssapi.c
 *  GSSAPI encryption support
 *
 * Portions Copyright (c) 2018-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *  src/backend/libpq/be-secure-gssapi.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "libpq/auth.h"
#include "libpq/be-gssapi-common.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

/*
 * Handle the encryption/decryption of data using GSSAPI.
 *
 * In the encrypted data stream on the wire, we break up the data
 * into packets where each packet starts with a uint32-size length
 * word (in network byte order), then encrypted data of that length
 * immediately following.  Decryption yields the same data stream
 * that would appear when not using encryption.
 *
 * NOTE: The client and server have to agree on the max packet size,
 * because we have to pass an entire packet to GSSAPI at a time and we
 * don't want the other side to send arbitrarily huge packets as we
 * would have to allocate memory for them to then pass them to GSSAPI.
 *
 * Therefore, this #define is effectively part of the protocol spec and can't
 * ever be changed.  It doesn't limit the size of our network I/Os buffers
 * though, as we try to fit messages into a single buffer as long as it
 * is at least this size.
 */
#define PQ_GSS_MAX_MESSAGE 16384

/*
 * The maximum number of buffers a message could occupy is currently 3, given
 * the minimum buffer size and maximum message size.
 */
#define PQ_GSS_MAX_BUFFERS_PER_MESSAGE \
	(1 + PQ_GSS_MAX_MESSAGE / MIN_SOCKET_BUFFER_SIZE)

/*
 * The segmentation scheme that allow in place encryption/decryption without
 * copying works like this:
 *
 * Outgoing data
 *
 * Outgoing messages are not allowed to span buffer boundaries.  Depending on
 * socket_buffer_size, a buffer can hold some number of messages of size
 * PQ_GSS_MAX_MESSAGE, and possibly a final message of smaller size.  Outbound
 * efficiency is therefore marginally better if socket_buffer_size is set to a
 * multiple of PQ_GSS_MAX_MESSAGE.  This is a simplification, not a
 * fundamental limitation, and with a bit more work we could build message
 * that span multiple buffers.
 *
 * be_gssapi_initialize_cleartext_buffer() sets up the segments leaving holes
 * in the right places for encryption framing, and then port_send_impl()
 * stores cleartext in those segments, ready for be_gssapi_encrypt() to
 * perform encryption in place.  The segment layout information is stored in
 * the holes themselves (since they are otherwise unused while the buffer
 * contains cleartext), and buf->segment_next points to the next one, allowing
 * be_gssapi_segment_next() (called by port_segment_next()) to adjust begin,
 * end, max_end whenever port_send_impl() needs to advance to the next one.
 * This arrangement allows strictly zero copying for encryption.
 *
 * Neither socket_buffer_size nor PQ_GSS_MAX_MESSAGE limits the size of
 * network transfer system calls, since buffers are grouped by up to
 * socket_combine_limit when they move to send.io_buffers, assuming
 * socket_buffers is set high enough.
  *
 * Incoming data
 *
 * We are not in control of the layout of messages in buffers arriving from
 * the wire, so we have to do a bit more work to tolerate messages with any
 * buffer alignment.  Encrypted buffers arrive from the network with nsegment
 * == 1 and whole or partial raw encrypted messages in the range [begin, end),
 * and be_gssapi_decrypt() figures out where the messages are, decrypts them
 * in place, and sets up the segmentation so that port_recv_impl() can find
 * all the ranges of of cleartext.
 *
 * Complete messages are decrypted in place and the containing buffers are
 * moved to recv.clear_buffers, but we might have to deal with a trailing
 * incomplete message that occupies the same buffer as a number of complete
 * messages.  In that case, we copy the incomplete message into a new buffer
 * in recv.crypt_buffers, in order to be able to guarantee progress in finite
 * space.
 *
 * If we waited for more buffers to arrive instead, a chain of buffers with
 * unfortunate layout could require waiting for an unbounded number of buffers
 * and never make progress.  Copying trailing data to a new buffer requires at
 * most PQ_GSS_MAX_MESSAGE / socket_buffer_size spare buffers, which we are
 * sure to be able to get, eventually (see pqcomm.c for details).  When this
 * happens, we also try to resynchronize message alignment with future
 * incoming network buffers, by starting an odd-size recv operation.  The goal
 * of this strategy is to make sure that no copying is required in the common
 * case, and in particular bulk data streams are decypted entirely in place in
 * their original network buffers.
 *
 * Segment framing data
 *
 * If buf->segment_next is zero, there is no framing data, the available space
 * is in the range [begin, max_end), the populated space is in the range
 * [begin, end), and port_buffer_next_segment() returns false.  This is the
 * case with non-GSSAPI buffers, encrypted GSSAPI buffers, and also GSSAPI
 * cleartext buffers holding zero or one remaining message.
 *
 * If buf->segment_next is non-zero, [begin, end) and [begin, max_end) contain
 * the values for the current segment as above, and buf->segment_next holds an
 * offset interpreted by be_gssapi_next_segment() to advance begin, end and
 * max_end to cover the next segment.
 *
 * We can safely store struct be_gssapi_segment_info in the GSSAPI header
 * space for the next message fragment (in outgoing messages: whole message,
 * in incoming message: possibly partial message), because RFC 1964 section
 * 1.2.2 says that GSS wrap tokens are larger than
 * sizeof(be_gssapi_segment_info).  In general we use gss_wrap_iov_length() to
 * tell us about header sizes rather than trying to second-guess its
 * implementation, but we do assume and assert that it's big enough for this
 * object.
 *
 * If a GSSAPI message's header wraps over a buffer boundary in an incoming
 * message, we don't need to store segment info.  The next cleartext segment
 * will begin in the next buffer, and the first segment's info doesn't need to
 * be stored, as it is instead stored in the initial values [begin, end)
 * values when the buffer is decrypted.  So be_gssapi_next_segment_info
 * objects are never split.
 *
 * An alternative approach to segment framing data would be to include space
 * for an iov list in struct PqBuffer, while this approach reuses space inside
 * the cleartext buffer itself temporarily, and avoids complicating other code
 * paths with iov considerations.
 */
typedef struct be_gssapi_next_segment_info
{
	uint32		next;
	uint32		begin;
	uint32		end;
	uint32		max_end;
} be_gssapi_next_segment_info;

/*
 * Initialize an empty multi-segment cleartext buffer ready to be filled up by
 * port_send_impl(), leaving holes for encryption framing.
 */
void
be_gssapi_initialize_cleartext_buffer(Port *port, PqBuffer *buf)
{
	uint32		offset;
	uint32		remaining;
	uint32		message_size;

	/*
	 * The space reserved for our packet length and GSSAPI's header is big
	 * enough to store a be_gssapi_segment_info object.
	 */
	Assert(sizeof(be_gssapi_next_segment_info) <=
		   sizeof(uint32) + port->gss->header_size);

	/*
	 * Outgoing messages are also well aligned for a be_gssapi_segment_info
	 * pointer, because socket_buffer_size is always a multiple of
	 * PG_IO_ALIGN.
	 */
	Assert(Min(PQ_GSS_MAX_MESSAGE, socket_buffer_size) % sizeof(uint32) == 0);
	

	/*
	 * Compute the boundaries of the first segment, and write them directly
	 * into buf to make the first segment active.
	 */
	offset = 0;
	remaining = Min(PQ_GSS_MAX_MESSAGE, socket_buffer_size);
	message_size = Min(PQ_GSS_MAX_MESSAGE, remaining);
	remaining -= message_size;
	buf->next_segment = remaining > 0 ? message_size : 0;
	buf->begin = sizeof(uint32) + port->gss->header_size;
	buf->end = buf->begin;
	buf->max_end = message_size - port->gss->trailer_size;

	/*
	 * Fill in the chain of be_gssapi_next_segment_info objects for the rest
	 * of the segments.
	 */
	while (remaining > 0)
	{
		be_gssapi_next_segment_info *next;

		/*
		 * Find the boundaries of the next message, and the offset of the next
		 * one after that.
		 */
		message_size = Min(PQ_GSS_MAX_MESSAGE, remaining);
		offset += message_size;
		remaining -= message_size;
		next = (be_gssapi_next_segment_info *) (buf->data + offset);
		next->next = remaining > 0 ? offset + message_size : 0;
		next->begin = offset + sizeof(uint32) + port->gss->header_size;
		next->end = next->begin;
		next->max_end = offset + message_size - port->gss->trailer_size;
	}
}

/*  
 * Advance begin, end, max_end to the values for the next segment.  This is
 * used only for cleartext buffers, to select between fragments of cleartext
 * while hiding the spaces reserved for in place GSS encryption.
 */
bool
be_gssapi_next_segment(PqBuffer *buf)
{
	be_gssapi_next_segment_info next;

	/* Is there another segment? */
	if (buf->next_segment == 0)
		return false;

	/*
	 * We may not be able to assume that message from the network are aligned
	 * suitably for uint32 access, so copy it out to a temporary variable.
	 * (Right?  Or perhaps GSSAPI messages are always aligned on at least 4,
	 * given their padding scheme?)
	 */
	Assert(buf->next_segment < socket_buffer_size);
	memcpy(&next, buf->data + buf->next_segment, sizeof(next));

	/* Reveal the next segment. */
	buf->next_segment = next.next;
	buf->begin = next.begin;
	buf->end = next.end;
	buf->max_end = next.max_end;
	return true;
}

/*
 * Encrypts the buffers in send.clear_buffers in place and moves them to
 * send.crypt_buffers.
 *
 * On entry, the ranges [begin, end) for all populated segments contain
 * cleartext data.  Multi-segment layout was configured by
 * be_gssapi_initialize_cleartext_buffer(), and port_send_impl() filled them
 * up.
 *
 * On exit, they have been encrypted in place and there is one segment [begin,
 * end) holding the concatenated messages in wire format.
 *
 * This operation doesn't require any free buffers, so it can always make
 * progress, as long as the GSSAPI library doesn't fail.
 */
int
be_gssapi_encrypt(Port *port)
{
	elog(LOG, "be_gssapi_encrypt_buffer");
	
	while (!bufq_empty(&port->send.clear_buffers))
	{
		PqBuffer *buf;
		uint32 start_of_message;
		uint32 encrypted_end;

		buf = bufq_head(&port->send.clear_buffers);

		/*
		 * We know how that_gssapi_encrypt_buffer() lays the segments out on
		 * PQ_GSS_MAX_MESSAGE boundaries, and we know that they are all full
		 * except for the final filled one whose end offset is in buf->end.
		 */
		start_of_message = 0;
		encrypted_end = 0;
		while (start_of_message < buf->end)
		{
			OM_uint32 major, minor;
			gss_iov_buffer_desc iov[4];
			uint32 size;
			uint32 header;
			uint32 begin;
			uint32 end;

			header = start_of_message + sizeof(size);
			begin = header + port->gss->header_size;
			end = start_of_message + PQ_GSS_MAX_MESSAGE - port->gss->header_size;
			if (end > buf->end)
				end = buf->end;		/* final populated message */
				
			/* An empty segment means no more segments. */
			if (end == begin)
				break;
			Assert(end > begin);
		
			/*
			 * Ask GSSAPI for the layout of the padding and trailer, and
			 * assert that everything else is as we expected.
			 */
			iov[0].type = GSS_IOV_BUFFER_TYPE_HEADER;
			iov[0].buffer.value = buf->data + header;
			iov[1].type = GSS_IOV_BUFFER_TYPE_DATA;
			iov[1].buffer.value = buf->data + begin;
			iov[1].buffer.length = end - begin;
			iov[2].type = GSS_IOV_BUFFER_TYPE_PADDING;
			iov[3].type = GSS_IOV_BUFFER_TYPE_TRAILER;
			major = gss_wrap_iov_length(&minor, port->gss->ctx, 1,
										GSS_C_QOP_DEFAULT, NULL,
										iov, lengthof(iov));
			if (GSS_ERROR(major))
			{
				/* XXX throw away buffer, store error */
				pg_GSS_error(_("gss_wrap_iov_length error"), major, minor);
				errno = ECONNRESET;
				return -1;
			}
			Assert(iov[0].buffer.length == port->gss->header_size);
			Assert(iov[1].buffer.length == end - begin);
			Assert(iov[3].buffer.length == port->gss->trailer_size);
				   
			/* Now we know where to put the padding and trailer. */
			iov[2].buffer.value =
				(char *) iov[1].buffer.value + iov[1].buffer.length;
			iov[3].buffer.value =
				(char *) iov[2].buffer.value + iov[2].buffer.length;

			/* How big will the size + encrypted message be? */
			size = sizeof(size) +
				iov[0].buffer.length +
				iov[1].buffer.length +
				iov[2].buffer.length +
				iov[3].buffer.length;
			Assert(size <= Min(PQ_GSS_MAX_MESSAGE, socket_buffer_size));
	
			/*
			 * If this is the final message, we know know the total size of
			 * the full network-ready buffer region.
			 */
			if (end == buf->end)
				encrypted_end = start_of_message + size;

			/*
			 * Encrypt the message.  Cleartext is replaced with ciphertext,
			 * and header, padding and trailer are populated.
			 */
			major = gss_wrap_iov(&minor, port->gss->ctx, 1, GSS_C_QOP_DEFAULT,
								 NULL, iov, lengthof(iov));
			if (GSS_ERROR(major))
			{
				pg_GSS_error(_("gss_wrap_iov error"), major, minor);
				errno = ECONNRESET;
				return -1;
			}

			/* Store initial size. */
			*(uint32 *) (buf->data + start_of_message) = pg_hton32(size);

			/* Skip to next message, if there is one. */
			start_of_message += PQ_GSS_MAX_MESSAGE;
		}

		/* This is now a non-segmented encrypted buffer. */
		Assert(encrypted_end != 0);
		buf->next_segment = 0;
		buf->begin = 0;
		buf->end = encrypted_end;		
		bufq_pop_head(&port->send.clear_buffers);
		bufq_push_tail(&port->send.crypt_buffers, buf);
	}

	return 0;
}

/*
 * Populate 'buffers' with the set of buffers containing an incoming encrypted
 * message beginning at 'offset' in buffers[0] extending for 'size' bytes.  On
 * entry, *nbuffers says how many elements of buffers[] are populated already,
 * and on exit it is updated with the new number.  Returns true if all the
 * required buffers were found, and false if there aren't enough buffers in
 * the recv.crypt_buffers queue yet and the caller will need to receive more
 * buffers from the network.
 */
static bool
find_buffers_for_message(Port *port,
						 PqBuffer *buffers,
						 int *nbuffers,
						 uint32 offset,
						 uint32 size)
{
	/* How many buffers will it take to gather 'size' bytes? */
	i = 0;
	offset = start_of_message;
	remaining = size;
	while (remaining > 0)
	{
		size_this_buffer = Min(buffer[i].end - offset, remaining);
		remaining -= size_this_buffer;
		if (i + 1 > nbuffers)
		{
			if (!bufq_has_next(buffer[i]))
				return false;
			buffer[++i] = bufq_next(buffer[i]);
			nbuffers++;
		}
		else
			++i;
		offset = buffer[i].begin;
	}
	return true;
}

/*
 * Decrypt a message.  On successful return, *cleartext and *cleartext_size
 * point to a subregion of the original message.  The encrypted message should
 * not include the size prefix.
 *
 * Unfortunately the gss_unwrap_iov() API doesn't seem to allow decrypting
 * from an IOV list unless perhaps you know the size of the decrypted message
 * already (?), but we have only the encrypted size.  Therefore the caller
 * currently has to reassemble a complete encrypted message if it didn't
 * arrive in one buffer.
 */
static int
be_gssapi_decrypt_message(Port *port,
						  uint8 *encrypted_message,
						  uint32 encrypted_size,
						  uint8 **cleartext,
						  uint32 *cleartext_size)
{
	gss_iov_buffer_desc iov[2];
	int conf_state;
		
	iov[0].type = GSS_IOV_BUFFER_TYPE_STREAM;
	iov[0].buffer.value = encrypted_message;
	iov[0].buffer.length = encrypted_size;
	iov[1].type = GSS_IOV_BUFFER_TYPE_DATA;
	major = gss_unwrap_iov(&minor, port->gss->ctx, &conf_state, NULL,
						   iov, lengthof(iov));
	if (GSS_ERROR(major))
	{
		pg_GSS_error(_("GSS unwrap error"), major, minor);
		errno = ECONNRESET;
		return -1;
	}
	if (conf_state != 1)
	{
		ereport(COMMERROR,
				(errmsg("GSS message is not confidential")));
		errno = ECONNRESET;
		return -1;
	}

	*cleartext = iov[1].buffer.value;
	*cleartext_size = iov[1].buffer.length;

	return 0;
}

/*
 * Copy a contiguous bytes to 'copy' from the memory beginning at 'offset' in
 * buffers[0] and extending into as many other buffers as needed according to
 * the buffers' "end" offsets.
 */
static void
be_gssapi_gather_bytes(uint8 *copy,
					   PqBuffer *buffers,
					   uint32 offset,
					   uint32 size)
{
	while (size > 0)
	{
		uint32 size_this_buffer;

		Assert(offset <= buffers->end);
		size_this_buffer = Min(size, buffers->end - offset);
		memcpy(copy, buffers->data + offset, size_this_buffer);
		size -= size_this_buffer;
		offset = 0;
		buffers++;
	}
}

/*
 * Copy contiguous bytes from 'copy' to the memory beginning at 'offset' in
 * buffers[0] and extending into as many other buffers as needed according to
 * the buffers' "end" offsets.
 */
static void
be_gssapi_scatter_bytes(const uint8 *copy,
						PqBuffer *buffers,
						uint32 offset,
						uint32 size)
{
	while (size > 0)
	{
		uint32 size_this_buffer;

		Assert(offset <= buffers->end);
		size_this_buffer = Min(size, buffers->end - offset);
		memcpy(buffers->data + offset, copy, size_this_buffer);
		size -= size_this_buffer;
		offset = 0;
		buffers++;
	}
}

/*
 * Decrypts as many buffers from recv.crypt_buffers as possible trying to do
 * it in place where possible.
 */
int
be_gssapi_decrypt(Port *port)
{
	PqBuffer *overflow;

	elog(LOG, "be_gssapi_decrypt_buffer");

	/* XXX */
	overflow = port_get_free_buffer(port);
	
	while (!bufq_empty(&port->recv.crypt_buffers))
	{
		uint8 copy[PQ_GSS_MAX_MESSAGE];
		uint8 *encrypted_message;
		PqBuffer buffers[PQ_GSS_MAX_BUFFERS_PER_MESSAGE];
		int nbuffers;
		uint32 size;

		nbuffers = 1;
		buffer[0] = bufq_head(&port->recv.crypt_buffers);
		Assert(buffer[0].begin < buffer[0].end);
		Assert(buffer[0].next_segment == 0);
		start_of_message = buf->begin;
		
		while (start_of_message < buf->end)
		{
			/* Decode size, possibly split across a buffer boundary. */
			if (start_of_message <= buffer[0].end - sizeof(uint32))
			{
				memcpy(&size, buffer[0].data + start_of_message, sizeof(uint32));
				size = pg_ntoh32(size);
			}
			else if (bufq_has_next(buffer[0]))
			{
				uint32 size_buffer0 = buffer[0].end - start_of_message;
				uint32 size_buffer1 = sizeof(size) - size_buffer0;
				
				buffers[1] = bufq_next(buffer[0]);
				nbuffers = 2;
				/* XXX check size_buffer1 bytes are available! */
				memcpy(&size,
					   buffer[0].data + start_of_message,
					   size_buffer0);
				memcpy((uint8 *) &size + size_buffer0,
					   buffer[1].data + buffer[1].begin,
					   size_buffer1);
			}
			else
			{
				/* if already decrypted messages, requeue to cleartext and
				 * copy size_buffer0 to overflow */
			}

			if (!find_buffers_for_message(port, buffers, &nbuffers,
										  start_of_message,
										  size - sizeof(size)))
			{
				/* We don't have all the buffers yet.  Requeue and copy! */
			}

			if (nbuffers == 1)
			{
				/* We can decrypt in place in the buffer. */
				encrypted_message =
					buffer[0].data +
					start_of_message +
					sizeof(size);
			}
			else
			{
				/* Gather message into our stack space. */
				be_gssapi_gather_bytes(copy,
									   buffers,
									   start_of_message,
									   size - sizeof(uint32));
				encrypted_message = copy;
			}			
			if (be_gssapi_decrypt_message(port,
										  message,
										  size - sizeof(uint32),
										  &cleartext,
										  &cleartext_size) < 0)
			{
			}

			/*
			 * Offset of the first byte of cleartext.  Note that it might be
			 * past the end of buffers[0]!
			 */
			start_of_cleartext = start_of_message +
				(cleartext - encrypted_message);
			
			if (nbuffers > 1)
			{
				/* Scatter cleartext back into buffers. */
				be_gssapi_scatter_bytes(copy,
										buffers,
										start_of_cleartext,
										cleartext_size);				
			}

#if 0
			if (start_of_cleartext >= buffers[0].end)
			{
				/*
				 * The first buffer must have contained only size/header
				 * information, so we can recycle it now.
				 */
				start_of_cleartext -= buffers[0].end;
				bufq_pop_head(&port->recv.crypt_buffers);
				port_put_free_buffer(port, buffers[0]);
				for (int i = 1; i < nbuffers; ++i)
					buffers[i - 1] = buffers[i];
				nbuffers--;
			}
#endif
			
			/*
			 * Set up the segment data for the cleartext, so that
			 * port_recv_impl() can find it.
			 */
			if (nsegments == 0)
			{
				buffers[0].next_segment = 0;
				buffers[0].begin = cle
			}
		}
	}
	
	return 0;
}

/*
 * Start up a GSSAPI-encrypted connection.  This performs GSSAPI
 * authentication; after this function completes, it is safe to call
 * be_gssapi_encrypt() and be_gssapi_decrypt().  Returns -1 and logs on
 * failure; otherwise, returns 0 and marks the connection as ready for GSSAPI
 * encryption.
 *
 * This function WILL block on port_send_all() and port_recv_all(), as
 * appropriate while establishing the GSSAPI session.  Note that those
 * functions go directly to the network at this point, but after we've
 * established port->gss->env they'll start going through port_encrypt() and
 * port_decrypt().
 */
ssize_t
secure_open_gssapi(Port *port)
{
	bool		complete_next = false;
	OM_uint32	major,
				minor;
	gss_cred_id_t delegated_creds;
	gss_iov_buffer_desc iov[4];

	INJECTION_POINT("backend-gssapi-startup");

	elog(LOG, "secure_open_gssapi");
	/*
	 * Allocate subsidiary Port data for GSSAPI operations.
	 */
	port->gss = (pg_gssinfo *)
		MemoryContextAllocZero(TopMemoryContext, sizeof(pg_gssinfo));

	delegated_creds = GSS_C_NO_CREDENTIAL;
	port->gss->delegated_creds = false;

	/*
	 * Use the configured keytab, if there is one.  As we now require MIT
	 * Kerberos, we might consider using the credential store extensions in
	 * the future instead of the environment variable.
	 */
	if (pg_krb_server_keyfile != NULL && pg_krb_server_keyfile[0] != '\0')
	{
		if (setenv("KRB5_KTNAME", pg_krb_server_keyfile, 1) != 0)
		{
			/* The only likely failure cause is OOM, so use that errcode */
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("could not set environment: %m")));
		}
	}

	while (true)
	{		
		uint32		netlen;
		gss_buffer_desc input,
					output = GSS_C_EMPTY_BUFFER;
		uint8		buffer[PQ_GSS_MAX_MESSAGE];

		/*
		 * The client always sends first, so try to go ahead and read the
		 * length and wait on the socket to be readable again if that fails.
		 */
		if (port_recv_all(port, &netlen, sizeof(netlen),
						  WAIT_EVENT_GSS_OPEN_SERVER) != 0)
			return -1;

		/*
		 * Get the length for this packet from the length header.
		 */
		input.length = pg_ntoh32(netlen);
		input.value = buffer;

		/*
		 * During initialization, packets are always fully consumed and
		 * shouldn't ever be over PQ_GSS_MAX_MESSAGE in length.
		 *
		 * Verify on our side that the client doesn't do something funny.
		 */
		if (input.length > lengthof(buffer))
		{
			ereport(COMMERROR,
					(errmsg("oversize GSSAPI packet sent by the client (%zu > %zu)",
							(size_t) input.length,
							lengthof(buffer))));
			return -1;
		}

		/*
		 * Get the rest of the packet so we can pass it to GSSAPI to accept
		 * the context.
		 */
		if (port_recv_all(port, input.value, input.length,
						  WAIT_EVENT_GSS_OPEN_SERVER) != 0)
			return -1;

		/* Process incoming data.  (The client sends first.) */
		major = gss_accept_sec_context(&minor, &port->gss->ctx,
									   GSS_C_NO_CREDENTIAL, &input,
									   GSS_C_NO_CHANNEL_BINDINGS,
									   &port->gss->name, NULL, &output, NULL,
									   NULL, pg_gss_accept_delegation ? &delegated_creds : NULL);

		if (GSS_ERROR(major))
		{
			pg_GSS_error(_("could not accept GSSAPI security context"),
						 major, minor);
			gss_release_buffer(&minor, &output);
			return -1;
		}
		else if (!(major & GSS_S_CONTINUE_NEEDED))
		{
			/*
			 * rfc2744 technically permits context negotiation to be complete
			 * both with and without a packet to be sent.
			 */
			complete_next = true;
		}

		if (delegated_creds != GSS_C_NO_CREDENTIAL)
		{
			pg_store_delegated_credential(delegated_creds);
			port->gss->delegated_creds = true;
		}

		/*
		 * Check if we have data to send and, if we do, make sure to send it
		 * all
		 */
		if (output.length > 0)
		{
			netlen = pg_hton32(output.length);

			if (output.length > PQ_GSS_MAX_MESSAGE - sizeof(uint32))
			{
				ereport(COMMERROR,
						(errmsg("server tried to send oversize GSSAPI packet (%zu > %zu)",
								(size_t) output.length,
								PQ_GSS_MAX_MESSAGE - sizeof(uint32))));
				gss_release_buffer(&minor, &output);
				return -1;
			}

			if (port_send_all(port, output.value, output.length,
							  WAIT_EVENT_GSS_OPEN_SERVER) != 0)
			{
				gss_release_buffer(&minor, &output);
				return -1;
			}
		}

		/*
		 * If we got back that the connection is finished being set up, now
		 * that we've sent the last packet, exit our loop.
		 */
		if (complete_next)
			break;
	}

	/*
	 * Determine the maximum cleartext message that can be encrypted and fit
	 * in leading and final messages, given the way
	 * be_gssapi_initialize_cleartext_buffer() lays out segments.  The reason
	 * we have to ask this instead of working backwards from
	 * GSS_IOV_BUFFER_TYPE_TRAILER is that the RFC... XXX
	 */
	major = gss_wrap_size_limit(&minor, port->gss->ctx, GSS_C_QOP_DEFAULT,
								Min(PQ_GSS_MAX_MESSAGE, socket_buffer_size) -
								sizeof(uint32),
								&port->gss->max_leading_cleartext_message);
	if (GSS_ERROR(major))
	{
		pg_GSS_error(_("gss_wrap_size_limit error"), major, minor);
		return -1;
	}
	if (socket_buffer_size % PQ_GSS_MAX_MESSAGE == 0)
	{
		port->gss->max_final_cleartext_message =
			port->gss->max_leading_cleartext_message;
	}
	else
	{
		major = gss_wrap_size_limit(&minor, port->gss->ctx, GSS_C_QOP_DEFAULT,
									(socket_buffer_size % PQ_GSS_MAX_MESSAGE) -
									sizeof(uint32),
									&port->gss->max_final_cleartext_message);
		if (GSS_ERROR(major))
		{
			pg_GSS_error(_("gss_wrap_size_limit error"), major, minor);
			return -1;
		}
	}
	
	/*
	 * Determine the size of the encryption framing, needed to set up the
	 * 'segments' that allow encryption/decryption in place.
	 */
	iov[0].type = GSS_IOV_BUFFER_TYPE_HEADER;
	iov[1].type = GSS_IOV_BUFFER_TYPE_DATA;
	iov[1].buffer.value = NULL;
	iov[1].buffer.length = 42;	/* arbitrary length, only affects padding */
	iov[2].type = GSS_IOV_BUFFER_TYPE_PADDING;
	iov[3].type = GSS_IOV_BUFFER_TYPE_TRAILER;
	major = gss_wrap_iov_length(&minor, port->gss->ctx, 1, GSS_C_QOP_DEFAULT,
								NULL, iov, lengthof(iov));
	if (GSS_ERROR(major))
	{
		pg_GSS_error(_("gss_wrap_iov_length error"), major, minor);
		return -1;
	}
	port->gss->header_size = iov[0].buffer.length;
	port->gss->trailer_size = iov[3].buffer.length;

	port->gss->enc = true;

	return 0;
}

/*
 * Return if GSSAPI authentication was used on this connection.
 */
bool
be_gssapi_get_auth(Port *port)
{
	if (!port || !port->gss)
		return false;

	return port->gss->auth;
}

/*
 * Return if GSSAPI encryption is enabled and being used on this connection.
 */
bool
be_gssapi_get_enc(Port *port)
{
	if (!port || !port->gss)
		return false;

	return port->gss->enc;
}

/*
 * Return the GSSAPI principal used for authentication on this connection
 * (NULL if we did not perform GSSAPI authentication).
 */
const char *
be_gssapi_get_princ(Port *port)
{
	if (!port || !port->gss)
		return NULL;

	return port->gss->princ;
}

/*
 * Return if GSSAPI delegated credentials were included on this
 * connection.
 */
bool
be_gssapi_get_delegation(Port *port)
{
	if (!port || !port->gss)
		return false;

	return port->gss->delegated_creds;
}
