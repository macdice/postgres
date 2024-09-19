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
 * When a buffer holds encrypted data, the first word of each message is the
 * size.  When it holds cleartext, it is part of the unused space so we steal
 * it to hold the size of the cleartext message.
 */
static uint32 *
be_gssapi_buffer_segment_end(PqBuffer *buf, int segment)
{
	return (uint32 *)(buf->data + segment * PQ_GSS_MAX_MESSAGE);
}

/*
 * Initialize an empty cleartext buffer with multiple segments, so that it can
 * be filled up with multiple cleartext messages leaving space in between for
 * encryption framing.
 */
void
be_gssapi_buffer_init(Port *port, PqBuffer *buf)
{	
	buf->nsegments = socket_buffer_size / PQ_GSS_MAX_MESSAGE;
	buf->segment = 0;
	for (int i = 0; i < buf->nsegments; ++i)
		*be_gssapi_buffer_segment_end(buf, i) = 0;
}

/*  
 * Set begin, end, max_end to the values for a given segment.  This is used to
 * select between multiple cleartext message bodies decrypted from a single
 * large network buffer, while hiding the space in between that is used for
 * encryption framing.
 */
be_gssapi_buffer_select_segment(Port *port, PqBuffer *buf, int segment)
{
	Assert(segment < buf->nsegments);

	buf->begin =
		PQ_GSS_MAX_MESSAGE * segment + sizeof(uint32) + port->gss->header_size;
	buf->end = *be_gssapi_buffer_segment_end(buf, segment);
	buf->max_end = PQ_GSS_MAX_MESSAGE - port->gss->trailer_size;
	buf->segment = segment;
}

/*
 * Encrypt a clear-text buffer in place.  On entry, the ranges [begin, end)
 * for all segments contain a cleartext data.  On successful exit, there is
 * only one segment containing an encrypted message [begin, end) ready for the wire.
 */
int
be_gssapi_encrypt_buffer(PqBuffer *buf)
{
	gss_iov_buffer_desc iov[4];
	uint32 size;

	for (int i = 0; i < buf->nsegments; ++i)
	{
		uint32 start_of_message;
		
		/* An empty segment means were finished. */
		be_gssapi_buffer_select_segment(buf, i);
		if (buf->end == buf_begin)
			break;
		
		/* GSS header will go after the size, which we will store below.. */
		start_of_message = i * PQ_GSS_MAX_MESSAGE;
		iov[0].type = GSS_IOV_BUFFER_TYPE_HEADER;
		iov[0].buffer.value = buf->data + start_of_message + sizeof(size);

		/* The data to be encrypted is at 'begin', which we will adjust below. */
		iov[1].type = GSS_IOV_BUFFER_TYPE_DATA;
		iov[1].buffer.value = buf->data + buf->begin;
		iov[1].buffer.length = buf->end - buf->begin;

		/* Some amount of padding and a trailer will follow, and we ask where. */
	iov[2].type = GSS_IOV_BUFFER_TYPE_PADDING;
	iov[3].type = GSS_IOV_BUFFER_TYPE_TRAILER;

	/* Query padding and trailing locations and sizes. */
	major = gss_wrap_iov_length(&minor, port->gss->ctx, 1, GSS_C_QOP_DEFAULT,
								NULL, iov, lengthof(iov));
	if (GSS_ERROR(major))
	{
		pg_GSS_error(_("gss_wrap_iov_length error"), major, minor);
		errno = ECONNRESET;
		return -1;
	}

	/* Cross-check the header size against secure_open_gssapi(). */
	if (iov[0].buffer.length + sizeof(uint32) != port->send_clear_min_begin)
	{
		ereport(COMERROR,
				(errmsg("GSS header size changed")));
		errno = ECONNRESET;
		return -1;		
	}

	/* How big will the outgoing message be? */
	size = sizeof(uint32) +
		iov[0].buffer.length +
		iov[1].buffer.length +
		iov[2].buffer.length +
		iov[3].buffer.length;

	if (size > Min(PQ_GSS_MAX_MESSAGE, socket_buffer_size))
	{
		ereport(COMERROR,
				(errmsg("GSS message oversized")));
		errno = ECONNRESET;
		return -1;		
	}
	
	/* GSS padding and trailer will follow the message. */
	iov[2].buffer = iov[1].buffer.value + iov[1].buffer.length;
	iov[3].buffer = iov[2].buffer.value + iov[2].buffer.length;

	/*
	 * Encrypt the message.  Cleartext is replaced with ciphertext, and
	 * header, padding and trailer are populated.
	 */
	major = gss_wrap_iov(&minor, port->gss->ctx, 1, GSS_C_QOP_DEFAULT,
						 NULL, iov, lengthof(iov));
	if (GSS_ERROR(major))
	{
		pg_GSS_error(_("gss_wrap_iov error"), major, minor);
		errno = ECONNRESET;
		return -1;
	}

	/* Store initial size and adjust range to include framing. */
	*(uint32 *) buf->data = pg_hton32(size);
	buf->begin = 0;
	buf->end = size;

	return 0;
}

/*
 * Decrypt a buffer in place.  On entry, the range [begin, end) contains an
 * encrypt message and possibly some trailing data.  On successful exit,
 * [begin, end) contains the cleartext message of one message, and any
 * trailing data is copied to the given overflow buffer.
 *
 * The overflow buffer is not expected to be initalized, and is initialized by
 * this function.  If overflow->end is zero on return, then there is no
 * overflow data.  This is expected to be common when receiving a stream of
 * full-sized messages.
 *
 * If an incomplete message is received, then this fails with EWOULDBLOCK and
 * the caller is responsible for appending more data when it arrives.
 */
int
be_gssapi_decrypt_buffer(PqBuffer *buf, PqBuffer *overflow)
{
	gss_iov_buffer_desc iov[2];
	uint32 size;

	/* We expect a left-justified message in the buffer. */
	if (buf->begin != 0 || buf->end < sizeof(size))
	{
		ereport(COMERROR,
				(errmsg("GSS packet not correctly aligned")));
		errno = ECONNRESET;
		return -1;				
	}

	/* Decode and sanity-check the size. */
	if (buf->end < sizeof(size))
	{
		/* Incomplete message. */
		errno = EWOULDBLOCK;
		return -1;
	}
	size = pg_ntoh32(*(uint32 *) buf->data);
	if (size > PQ_GSS_MAX_MESSAGE)
	{
		ereport(COMERROR,
				(errmsg("GSS packet has unsupported size %u", size)));
		errno = ECONNRESET;
		return -1;				
	}
	if (buf->end < size)
	{
		/* Incomplete message. */
		errno = EWOULDBLOCK;
		return -1;
	}

	/*
	 * Report the range of data that follows this message, for the caller to
	 * deal with.
	 */
	*trailing_offset = size;
	*trailing_size = buf->end - size;

	/*
	 * Decrypt the GSS message that begins after size.  Ciphertext is replaced
	 * with cleartext, and its location is report to us in iov[1].
	 */
	iov[0].type = GSS_IOV_BUFFER_TYPE_STREAM;
	iov[1].buffer.value = buf->data + sizeof(size);
	iov[1].buffer.length = size - sizeof(size);
	major = gss_unwrap_iov(&minor, port->gss->ctx, NULL, NULL,
						   iov, lengthof(iov));
	if (GSS_ERROR(major))
	{
		pg_GSS_error(_("gss_unwrap_iov error"), major, minor);
		errno = ECONNRESET;
		return -1;
	}

	/* Adjust [begin, end) to point to the cleartext. */
	Assert(iov[1].buffer.value >= buf->data);
	Assert(iov[1].buffer.value < buf->data + buf->end);
	Assert(iov[1].buffer.length < buf->end - buf->begin);
	buf->begin = iov[1].buffer.value - buf->data;
	buf->end = buf->begin + iov[1].buffer.length;

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
 * established port->gss->env they'll go through port_encrypt() and
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
	uint32		max_text_size;

	INJECTION_POINT("backend-gssapi-startup");

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
						  WAIT_EVENT_FSS_OPEN_SERVER) != 0)
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
		if (port_recv_all(port, input.buffer, input.length,
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
			uint32		netlen = pg_hton32(output.length);

			if (output.length > PQ_GSS_MAX_MESSAGE - sizeof(uint32))
			{
				ereport(COMMERROR,
						(errmsg("server tried to send oversize GSSAPI packet (%zu > %zu)",
								(size_t) output.length,
								PQ_GSS_MAX_MESSAGE - sizeof(uint32))));
				gss_release_buffer(&minor, &output);
				return -1;
			}

			if (port_send_all(port, output.buffer, output.length,
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
