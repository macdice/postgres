/*-------------------------------------------------------------------------
 *
 * be-secure.c
 *	  functions related to setting up a secure connection to the frontend.
 *	  Secure connections are expected to provide confidentiality,
 *	  message integrity and endpoint authentication.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/libpq/be-secure.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "libpq/libpq.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"

char	   *ssl_library;
char	   *ssl_cert_file;
char	   *ssl_key_file;
char	   *ssl_ca_file;
char	   *ssl_crl_file;
char	   *ssl_crl_dir;
char	   *ssl_dh_params_file;
char	   *ssl_passphrase_command;
bool		ssl_passphrase_command_supports_reload;

#ifdef USE_SSL
bool		ssl_loaded_verify_locations = false;
#endif

/* GUC variable controlling SSL cipher list */
char	   *SSLCipherSuites = NULL;

/* GUC variable for default ECHD curve. */
char	   *SSLECDHCurve;

/* GUC variable: if false, prefer client ciphers */
bool		SSLPreferServerCiphers;

int			ssl_min_protocol_version = PG_TLS1_2_VERSION;
int			ssl_max_protocol_version = PG_TLS_ANY;

/* Helper function declarations. */
static int	port_start_send(Port *port, PqBufferQueue *queue);
static int	port_start_recv(Port *port, PqBufferQueue *queue);
static void port_complete_send(Port *port, PqBufferQueue *queue,
							   ssize_t transferred, int error);
static void port_complete_recv(Port *port, PqBufferQueue *queue,
							   ssize_t transferred, int error);
static int	port_encrypt(Port *port);
static int	port_decrypt(Port *port);


/* ------------------------------------------------------------ */
/*			 Buffer freelist management                         */
/* ------------------------------------------------------------ */

static inline bool
port_has_free_buffer(Port *port)
{
	return !bufq_empty(&port->free_buffers);
}

static inline PqBuffer *
port_get_free_buffer(Port *port)
{
	/* XXX heap sorted by buf->data so we can merge iovecs? */
	return bufq_pop_head(&port->free_buffers);
}

static inline void
port_put_free_buffer(Port *port, PqBuffer *buf)
{
	bufq_push_tail(&port->free_buffers, buf);
}

int
port_free_buffer_count(Port *port)
{
	return bufq_size(&port->free_buffers);
}

/* ------------------------------------------------------------ */
/*			 Procedures common to all secure sessions			*/
/* ------------------------------------------------------------ */

/*
 *	Initialize global context.
 *
 * If isServerStart is true, report any errors as FATAL (so we don't return).
 * Otherwise, log errors at LOG level and return -1 to indicate trouble,
 * preserving the old SSL state if any.  Returns 0 if OK.
 */
int
secure_initialize(bool isServerStart)
{
#ifdef USE_SSL
	return be_tls_init(isServerStart);
#else
	return 0;
#endif
}

/*
 *	Destroy global context, if any.
 */
void
secure_destroy(void)
{
#ifdef USE_SSL
	be_tls_destroy();
#endif
}

/*
 * Indicate if we have loaded the root CA store to verify certificates
 */
bool
secure_loaded_verify_locations(void)
{
#ifdef USE_SSL
	return ssl_loaded_verify_locations;
#else
	return false;
#endif
}

/*
 *	Attempt to negotiate secure session.
 */
int
secure_open_server(Port *port)
{
#ifdef USE_SSL
	int			r = 0;

	/* push buffered data back through SSL setup */
	Assert(bufq_empty(&port->recv.crypt_buffers));
	while (!bufq_empty(&port->recv.clear_buffers))
		bufq_push_tail(&port->recv.crypt_buffers,
					   bufq_pop_head(&port->recv.clear_buffers));

	INJECTION_POINT("backend-ssl-startup");

	r = be_tls_open_server(port);

	if (port_recv_pending_encrypted(port) > 0)
	{
		/*
		 * This shouldn't be possible -- it would mean the client sent
		 * encrypted data before we established a session key...
		 */
		elog(LOG, "buffered unencrypted data remains after negotiating SSL connection");
		return STATUS_ERROR;
	}

	ereport(DEBUG2,
			(errmsg_internal("SSL connection from DN:\"%s\" CN:\"%s\"",
							 port->peer_dn ? port->peer_dn : "(anonymous)",
							 port->peer_cn ? port->peer_cn : "(anonymous)")));
	return r;
#else
	return 0;
#endif
}

/*
 *	Close secure session.
 */
void
secure_close(Port *port)
{
#ifdef USE_SSL
	if (port->ssl_in_use)
		be_tls_close(port);
#endif
}

static int
port_start_send(Port *port, PqBufferQueue *queue)
{
	ssize_t		transferred;
	int			iovcnt;
	PqBuffer   *buf;

	/* Only one I/O allowed per channel for now. */
	if (!bufq_empty(&port->send.io_buffers))
	{
		errno = EBUSY;
		return -1;
	}

	/* Prepare send->io_buffers and send->iov. */
	iovcnt = 0;
	do
	{
		buf = bufq_pop_head(queue);
		port->send.iov[iovcnt].iov_base = buf->data + buf->begin;
		port->send.iov[iovcnt].iov_len = buf->end - buf->begin;
		iovcnt++;
		bufq_push_tail(&port->send.io_buffers, buf);
	} while (!bufq_empty(queue) &&
			 iovcnt < socket_combine_limit &&
			 port_has_free_buffer(port));

	/* Try to send whole io_buffers queue at once. */
	transferred = WaitEventSetSend(port->wes,
								   port->sock_pos,
								   port->send.iov,
								   iovcnt);

	/* Being processed asynchronously? */
	if (transferred == -1 && errno == EINPROGRESS)
		return 0;

	/* Processed immedately. */
	port_complete_send(port, queue, transferred, errno);
	return transferred;
}

static int
port_start_recv(Port *port, PqBufferQueue *queue)
{
	ssize_t		transferred;
	int			iovcnt;
	int			nbuffers;

	/* Only one I/O allowed per channel for now. */
	if (!bufq_empty(&port->recv.io_buffers))
	{
		errno = EBUSY;
		return -1;
	}

	/* Return queued EOF. */
	if (port->recv.eof)
		return 0;

	/*
	 * If the last recv operation filled its final buffer, then try to
	 * maximize this read.
	 */
	if (port->last_recv_buffer_full)
		nbuffers = lengthof(port->recv.iov);
	else
		nbuffers = 1;

	/*
	 * XXX explain
	 */
	Assert(port_has_free_buffer(port));

	/* Prepare recv->io_buffers and recv->iov. */
	iovcnt = 0;
	do
	{
		PqBuffer   *buf;

		buf = port_get_free_buffer(port);
		buf->begin = 0;
		buf->end = 0;
		port->recv.iov[iovcnt].iov_base = buf->data;
		port->recv.iov[iovcnt].iov_len = socket_buffer_size;
		bufq_push_tail(&port->recv.io_buffers, buf);
		iovcnt++;
	}
	while (iovcnt < nbuffers && port_has_free_buffer(port));

	/* Start receiving. */
	transferred = WaitEventSetRecv(port->wes,
								   port->sock_pos,
								   port->recv.iov,
								   iovcnt);

	/* Being processed asynchronously? */
	if (transferred == -1 && errno == EINPROGRESS)
		return -1;

	/* Processed immediately. */
	port_complete_recv(port, queue, transferred, errno);
	return transferred;
}

static void
port_complete_send(Port *port,
				   PqBufferQueue *queue,
				   ssize_t transferred,
				   int error)
{
	PqBuffer   *insert;
	size_t		bytes_remaining;

	if (transferred < 0)
	{
		Assert(error != 0);
		Assert(error != EINPROGRESS);
		Assert(error != EBUSY);
		bytes_remaining = 0;
		port->send.error = error;
	}
	else
	{
		error = 0;
		bytes_remaining = transferred;
	}

	insert = NULL;
	do
	{
		PqBuffer   *buf;
		size_t		transferred_this_buffer;
		size_t		size_this_buffer;

		/* Pop head buffer. */
		buf = bufq_pop_head(&port->send.io_buffers);

		/* How much of this buffer was sent? */
		size_this_buffer = buf->end - buf->begin;
		transferred_this_buffer = Min(size_this_buffer, bytes_remaining);
		bytes_remaining -= transferred_this_buffer;

		if (transferred_this_buffer == size_this_buffer)
		{
			/* All data sent, so recycle it. */
			port_put_free_buffer(port, buf);
		}
		else
		{
			/*
			 * Some or all of this buffer remains unsent, so move it to the
			 * head of the queue it originally came from, where it can be
			 * retried later.
			 */
			buf->begin += transferred_this_buffer;
			if (insert)
			{
				/* Preserve order. */
				bufq_insert_after(queue, insert, buf);
				insert = buf;
			}
			else
			{
				/* But insert before anything else that was already there. */
				bufq_push_head(queue, buf);
				insert = buf;
			}
		}
	}
	while (!bufq_empty(&port->send.io_buffers));
}

static void
port_complete_recv(Port *port,
				   PqBufferQueue *queue,
				   ssize_t transferred,
				   int error)
{
	size_t		bytes_remaining;

	if (transferred < 0)
	{
		Assert(error != 0);
		Assert(error != EINPROGRESS);
		Assert(error != EBUSY);
		bytes_remaining = 0;
		port->recv.error = error;
	}
	else
	{
		error = 0;
		bytes_remaining = transferred;
		if (transferred == 0)
			port->recv.eof = true;
	}

	do
	{
		PqBuffer   *buf;
		size_t		transferred_this_buffer;

		/* Pop head buffer. */
		buf = bufq_pop_head(&port->recv.io_buffers);

		/* How much of this buffer is full? */
		transferred_this_buffer = Min(socket_buffer_size, bytes_remaining);
		bytes_remaining -= transferred_this_buffer;

		if (transferred_this_buffer == 0)
		{
			/* Nothing received into this buffer, so recycle it. */
			port_put_free_buffer(port, buf);
		}
		else
		{
			/*
			 * Partial or whole buffer filled, so move it to the destination
			 * queue where the data can be consumed, after anything else that
			 * was already there.
			 */
			buf->end = transferred_this_buffer;
			bufq_push_tail(queue, buf);
		}

		/*
		 * Influence the size of the next read.  This might cause a spurious
		 * future large read attempt if a message with nothing following it
		 * happens to fill exactly one buffer, but that should be rare and
		 * mistakes aren't terribly expensive.
		 */
		port->last_recv_buffer_full =
			transferred_this_buffer == socket_buffer_size;
	}
	while (!bufq_empty(&port->recv.io_buffers));
}

/*
 * Waits for at least one in-progress send or receive to complete, or the
 * latch to be set.  Returns 0 for timeout or I/O completion, and WL_LATCH_SET
 * if the latch was set and the caller must deal with that.
 *
 * This is much like calling WaitLatch(), except that we need to intercept
 * completing I/Os and progress the relevant I/O queues.
 */
int
port_wait_io(Port *port, int timeout, int wait_event)
{
	int			result;
	int			nevents;
	int			wait_for;
	WaitEvent	events[3];
	PqBufferQueue *queue;

	/*
	 * Which channels have I/O in progress, if any?  This is a no-op for true
	 * AIO, but is needed for readiness-based simulation.
	 */
	wait_for = 0;
	if (!bufq_empty(&port->recv.io_buffers))
		wait_for |= WL_SOCKET_RECV;
	if (!bufq_empty(&port->send.io_buffers))
		wait_for |= WL_SOCKET_SEND;
	ModifyWaitEvent(port->wes, port->sock_pos, wait_for, NULL);

	result = 0;
	nevents = WaitEventSetWait(port->wes,
							   -1,
							   events,
							   lengthof(events),
							   wait_event);

	for (int i = 0; i < nevents; ++i)
	{
		if (events[i].events == WL_SOCKET_SEND)
		{
			/* Where should unsent data be re-queued for retrying? */
			if (port->ssl_in_use || (port->gss && port->gss->enc))
				queue = &port->send.crypt_buffers;
			else
				queue = &port->send.clear_buffers;
			port_complete_send(port,
							   queue,
							   events[i].send_op.result.transferred,
							   events[i].send_op.result.error);
		}
		else if (events[i].events == WL_SOCKET_RECV)
		{
			/* Where should received data be appended? */
			if (port->ssl_in_use || (port->gss && port->gss->enc))
				queue = &port->recv.crypt_buffers;
			else
				queue = &port->recv.clear_buffers;
			port_complete_recv(port,
							   queue,
							   events[i].recv_op.result.transferred,
							   events[i].recv_op.result.error);
		}
		else if (events[i].events == WL_LATCH_SET)
			result = WL_LATCH_SET;
	}

	return result;
}

/*
 * Send data to network buffers, draining as required but not waiting.
 *
 * Fails with errno == EINPROGRESS if an asynchronous network transfer is in
 * progress that must complete before more progress can be made.
 */
static ssize_t
port_send_impl(Port *port, bool encryption_lib, const void *data, size_t size)

{
	ssize_t		transferred;
	PqBufferQueue *append_queue;
	PqBufferQueue *drain_queue;

	/* Return queued error. */
	if (port->send.error)
	{
		errno = port->send.error;
		port->send.error = 0;
		return -1;
	}

	if (encryption_lib)
	{
		/* Moves from caller (an encryption library) to network. */
		append_queue = drain_queue = &port->send.crypt_buffers;
	}
	else if (port->ssl_in_use || port->gss)
	{
		/* Data must be encrypted between caller and network. */
		append_queue = &port->send.clear_buffers;
		drain_queue = &port->send.crypt_buffers;
	}
	else
	{
		/* Data moves from caller to network. */
		append_queue = drain_queue = &port->send.clear_buffers;
	}

	transferred = 0;
	while (size > 0)
	{
		PqBuffer   *buf;
		size_t		size_this_buffer;
		ssize_t		r;

		/* Find tail buffer. */
		if (!bufq_empty(append_queue))
			buf = bufq_tail(append_queue);
		else
			buf = NULL;

		/* Find a new buffer to append to, if necessary. */
		if (!buf || buf->end == socket_buffer_size)
		{
			if (bufq_size(append_queue) < socket_combine_limit &&
				port_has_free_buffer(port))
			{
				/* Nope, but we can add a new one. */
				buf = port_get_free_buffer(port);
				buf->begin = 0;
				buf->end = 0;
				bufq_push_tail(append_queue, buf);
			}
			else if (!bufq_empty(&port->send.io_buffers))
			{
				/*
				 * There is already a send in progress so we can't start
				 * draining what we have to the network yet.
				 */
				errno = EINPROGRESS;
				return transferred > 0 ? transferred : -1;
			}
			else
			{
				/* Nope, but we can try to drain to the network. */
				if (append_queue != drain_queue)
				{
					/* Fill crypt_buffers first. */
					r = port_encrypt(port);
					if (r < 0)
					{
						/* XXX store error */
						return transferred > 0 ? transferred : -1;
					}
				}
				Assert(bufq_empty(&port->send.io_buffers));
				Assert(!bufq_empty(drain_queue));
				r = port_start_send(port, drain_queue);
				if (r < 0)
				{
					/* XXX store error */
					return transferred > 0 ? transferred : -1;
				}
				/* Sending immediately frees up buffers. */
				Assert(port_has_free_buffer(port));
				buf = port_get_free_buffer(port);
				buf->begin = 0;
				buf->end = 0;
				bufq_push_tail(append_queue, buf);
			}
		}

		/* Copy data in. */
		size_this_buffer = Min(size, socket_buffer_size - buf->end);
		memcpy(buf->data + buf->end, (char *) data, size_this_buffer);
		buf->end += size_this_buffer;
		data = (char *) data + size_this_buffer;
		size -= size_this_buffer;
		transferred += size_this_buffer;
	}

	return transferred;
}

/*
 * Receive up to 'len' bytes from queued buffers, replenishing them from the
 * network if possible without waiting.
 *
 * Fails with errno == EINPROGRESS if an asynchronous network transfer is in
 * progress that must complete before more progress can be made.
 */
static ssize_t
port_recv_impl(Port *port, bool encryption_lib, void *data, size_t size)
{
	ssize_t		transferred;
	PqBufferQueue *consume_queue;
	PqBufferQueue *fill_queue;

	/* Return queued error. */
	if (port->recv.error)
	{
		errno = port->recv.error;
		port->recv.error = 0;
		return -1;
	}

	if (encryption_lib)
	{
		/* Data moves from network to caller (an encryption library). */
		consume_queue = fill_queue = &port->recv.crypt_buffers;
	}
	else if (port->ssl_in_use || port->gss)
	{
		/* Data must be decrypted in between network and caller. */
		consume_queue = &port->recv.clear_buffers;
		fill_queue = &port->recv.crypt_buffers;
	}
	else
	{
		/* Data moves from network to caller. */
		consume_queue = fill_queue = &port->recv.clear_buffers;
	}

	transferred = 0;
	while (size > 0)
	{
		PqBuffer   *buf;
		size_t		size_this_buffer;
		ssize_t		r;

		if (!bufq_empty(consume_queue))
		{
			/* Does the head buffer have any data left? */
			buf = bufq_head(consume_queue);
			if (buf->begin == buf->end)
			{
				/* Nope, recycle. */
				bufq_pop_head(consume_queue);
				port_put_free_buffer(port, buf);
				continue;
			}
		}
		else if (!bufq_empty(fill_queue))
		{
			/* Try to read through the encyption library */
			Assert(fill_queue != consume_queue);
			Assert(port->ssl_in_use || port->gss);
			r = port_decrypt(port);
			if (r < 0)
			{
				/* XXX store error */
				elog(LOG, "port_decrypt said %d: %m", (int) r);
				return transferred > 0 ? transferred : 1;
			}
			continue;
		}
		else if (bufq_empty(&port->recv.io_buffers))
		{
			/* No recv in progress yet, so start one. */
			r = port_start_recv(port, fill_queue);
			if (r < 0)
			{
				/* If we already made progress, report that. */
				/* XXX if no EINPROGRESS, store error */
				return transferred > 0 ? transferred : -1;
			}
			continue;
		}
		else
		{
			/* A recv was already in progress, so keep reporting that. */
			errno = EINPROGRESS;
			return transferred > 0 ? transferred : -1;
		}

		/* Copy data out. */
		size_this_buffer = Min(size, buf->end - buf->begin);
		if (size_this_buffer > 0)
			memcpy(data, (char *) buf->data + buf->begin, size_this_buffer);
		buf->begin += size_this_buffer;
		data = (char *) data + size_this_buffer;
		size -= size_this_buffer;
		transferred += size_this_buffer;
	}

	return transferred;
}

/*
 * Transfer data to port->recv.crypt_buffers from a destination buffer.
 * Called by encryption library BIO routines, which must be ready for
 * EINPROGRESS.
 */
ssize_t
port_send_encrypted(Port *port, const void *data, size_t size)
{
	elog(LOG, "XXX port_send_encrypted %zu", size);
	return port_send_impl(port, true, data, size);
}

/*
 * Transfer data from port->recv.crypt_buffers to a destination buffer.
 * Called by encryption library BIO routines, which must be ready for
 * EINPROGRESS.
 */
ssize_t
port_recv_encrypted(Port *port, void *data, size_t size)
{
	elog(LOG, "XXX port_recv_encrypted %zu", size);
	return port_recv_impl(port, true, data, size);
}

/*
 * Ask the encryption library to consume and encrypt as much clear text as
 * possible from port->send.clear_buffers and append it to
 * port->send.crypt_buffers.
 */
static int
port_encrypt(Port *port)
{
	ssize_t		r;

	Assert(port->ssl_in_use || (port->gss && port->gss->enc));

#if defined(USE_SSL)
	if (port->ssl_in_use)
	{
		/*
		 * We write data "through" OpenSSL, and it appends encrypted data to
		 * new buffers in send.crypt_buffers with port_send_encrypted().
		 */
		while (!bufq_empty(&port->send.clear_buffers))
		{
			PqBuffer   *buf;
			void	   *data;
			size_t		size;

			buf = bufq_head(&port->send.clear_buffers);
			data = buf->data + buf->begin;
			size = buf->end - buf->begin;

			if (port->ssl_in_use)
				r = be_tls_write(port, data, size);

			if (r >= 0)
			{
				buf->begin += r;
				if (buf->begin == buf->end)
				{
					/* Recycle buffer that has been wholly encrypted. */
					bufq_pop_head(&port->send.clear_buffers);
					port_put_free_buffer(port, buf);
				}
			}
			else
			{
				/* XXX store error */
				return -1;
			}
		}
	}
#endif

#if defined(ENABLE_GSS)
	if (port->gss && port->gss->enc)
	{
		/*
		 * We ask GSSAPI to encrypt cleartext buffers in place so we can just
		 * re-queue them immediately to send.crypt_buffers, ready for the
		 * network.
		 */
		while (!bufq_empty(&port->send.clear_buffers))
		{
			PqBuffer	   *buf;

			buf = bufq_head(&port->send.clear_buffers);
			if (be_gssapi_encrypt_buffer(port, buf) < 0)
			{
				/* XXX */
				return -1;
			}

			bufq_pop_head(&port->send.clear_buffers);
			bufq_push_tail(&port->send.crypt_buffers, buf);
		}
	}
#endif
	
	return 0;
}

/*
 * Ask the encryption library to consume and decrypt as much data as possible
 * from port->recv.crypt_buffers, and append clear text to
 * port->recv.clear_buffers.
 */
static int
port_decrypt(Port *port)
{
	ssize_t		r;

	Assert(port->ssl_in_use || (port->gss && port->gss->enc));

#if defined(USE_SSL)
	if (port->ssl_in_use)
	{
		while (!bufq_empty(&port->recv.crypt_buffers))
		{
			PqBuffer   *buf;
			void	   *data;
			size_t		size;

			/* Find a clear text buffer to append to. */
			if (bufq_empty(&port->recv.clear_buffers))
				buf = NULL;
			else
			{
				/* Got one, but is it already full? */
				buf = bufq_tail(&port->recv.clear_buffers);
				if (buf->end == socket_buffer_size)
					buf = NULL;
			}

			/* Do we need a new one? */
			if (!buf)
			{
				/* XXX figure out flow control */
				if (!port_has_free_buffer(port))
					break;
				buf = port_get_free_buffer(port);
				buf->begin = 0;
				buf->end = 0;
				bufq_push_tail(&port->recv.clear_buffers, buf);
			}

			/* Read through the encryption library. */
			data = buf->data + buf->end;
			size = socket_buffer_size - buf->end;
			elog(LOG, "port_decrypt will try to decrypt %d bytes", (int) size);

			if (port->ssl_in_use)
				r = be_tls_read(port, data, size);

			if (r >= 0)
			{
				buf->end += r;
			}
			else
			{
				if (errno == EAGAIN || errno == EWOULDBLOCK)
					errno = EINPROGRESS;
				/* XXX store error */
				return -1;
			}
		}
	}
#endif

#if defined(ENABLE_GSS)
	if (port->gss && port->gss->enc)
	{
	}
#endif

	return 0;
}

/*
 * Called by pq_putXXX functions to send data.
 */
ssize_t
port_send(Port *port, const void *data, size_t size, int wait_event)
{
	ssize_t		transferred;

again:
	transferred = port_send_impl(port, false, data, size);
	if (transferred < 0 && errno == EINPROGRESS && wait_event != 0)
	{
		if (port_wait_io(port, -1, wait_event) == WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			ProcessClientWriteInterrupt(true);
		}
		goto again;
	}

	return transferred;
}

/*
 * Called by pq_getXXX functions to receive data, possibly via an encryption
 * library.  If wait_event is non-zero, will wait for network data, and
 * service client read interrupts.
 */
ssize_t
port_recv(Port *port, void *data, size_t size, int wait_event)
{
	ssize_t		transferred;

again:
	transferred = port_recv_impl(port, false, data, size);
	if (transferred < 0 && errno == EINPROGRESS && wait_event != 0)
	{
		if (port_wait_io(port, -1, WAIT_EVENT_CLIENT_READ) == WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			ProcessClientReadInterrupt(true);
		}
		goto again;
	}

	return transferred;
}

/*
 * Like port_send(), but with internal retry on short transfer.  Returns 0 on
 * success, EOF on failure.
 */
int
port_send_all(Port *port, const void *data, size_t size, int wait_event)
{
	while (size > 0)
	{
		ssize_t		transferred;

		transferred = port_send(port, data, size, wait_event);
		if (transferred < 0)
			return EOF;
		data = (char *) data + transferred;
		size -= transferred;
	}

	return 0;
}

/*
 * Like port_recv(), but with internal retry on short transfer.  Returns 0 on
 * success, EOF on failure.
 */
int
port_recv_all(Port *port, void *data, size_t size, int wait_event)
{
	ssize_t		transferred;

	Assert(wait_event != 0);
	while (size > 0)
	{
		transferred = port_recv(port, data, size, true);
		if (transferred < 0)
			return EOF;
		data = (char *) data + transferred;
		size -= transferred;
	}

	return 0;
}

/*
 * Send all buffered data to the network, encrypting if required.  If
 * wait_event is 0, does not wait for completion of network send.
 */
int
port_flush(Port *port, int wait_event)
{
	for (;;)
	{
		if (!bufq_empty(&port->send.io_buffers) && wait_event != 0)
		{
			/* Wait for in-progress send to complete. */
			/*
			 * XXX re-order, it would be better to run encryption code while
			 * waiting!
			 */
			if (port_wait_io(port, -1, wait_event) == WL_LATCH_SET)
			{
				ResetLatch(MyLatch);
				ProcessClientWriteInterrupt(true);
			}
		}
		else if (!bufq_empty(&port->send.crypt_buffers))
		{
			/* Start sending buffered encrypted data. */
			Assert(port->ssl_in_use || port->gss);
			if (port_start_send(port, &port->send.crypt_buffers) < 0 &&
				errno != EINPROGRESS)
				return -1;
		}
		else if (!bufq_empty(&port->send.clear_buffers))
		{
			if (port->ssl_in_use || port->gss)
			{
				/* Encrypt buffered clear text, to fill crypt_buffers. */
				if (port_encrypt(port) < 0 && errno != EWOULDBLOCK)
					return -1;
			}
			else
			{
				/* No encryption, so start sending buffered clear text. */
				if (port_start_send(port, &port->send.clear_buffers) < 0 &&
					errno != EINPROGRESS)
					return -1;
			}
		}
		else
		{
			/* All buffered data has been processed. */
			break;
		}
	}
	return 0;
}

/*
 * Like port_flush(), but only flushes encrypted data.  If wait_event is zero,
 * does not wait, but might fail with EINPROGRESS.
 */
int
port_flush_encrypted(Port *port, int wait_event)
{
	for (;;)
	{
		if (!bufq_empty(&port->send.io_buffers))
		{
			if (wait_event == 0)
			{
				/*
				 * Caller only wants to make as much progress as possible
				 * without waiting, so report I/O in progress.
				 */
				errno = EINPROGRESS;
				return -1;							 
			}
			/* Otherwise, caller wants to wait for all I/O to complete. */
			if (port_wait_io(port, -1, wait_event) == WL_LATCH_SET)
			{
				ResetLatch(MyLatch);
				ProcessClientWriteInterrupt(true);
			}			
		}
		else if (!bufq_empty(&port->send.crypt_buffers))
		{
			if (port_start_send(port, &port->send.crypt_buffers) < 0 &&
				errno != EINPROGRESS)
				return -1;
		}
		else
		{
			/* There is no send.crypt_buffers or send.io_buffers. */
			break;
		}
	}
	return 0;
}

static size_t
port_pending(PqBufferQueue *queue)
{
	PqBuffer   *buf;
	size_t		sum;

	sum = 0;
	if (!bufq_empty(queue))
	{
		buf = bufq_head(queue);
		sum += buf->end - buf->begin;
		while (bufq_has_next(queue, buf))
		{
			buf = bufq_next(queue, buf);
			sum += buf->end - buf->begin;
		}
	}

	return sum;
}

size_t
port_send_pending(Port *port)
{
	return port_pending(&port->send.clear_buffers);
}

size_t
port_recv_pending(Port *port)
{
	return port_pending(&port->recv.clear_buffers);
}

size_t
port_recv_pending_encrypted(Port *port)
{
	return port_pending(&port->recv.crypt_buffers);
}

/*
 * Peek at the next clear text byte to be received.
 */
int
port_peek(Port *port)
{
	uint8		byte;

	if (port_recv(port, &byte, 1, true) < 1)
		return EOF;

	/*
	 * Receiving always leaves the most recently returned byte in the head
	 * cleartext buffer.
	 */
	Assert(!bufq_empty(&port->recv.clear_buffers));
	Assert(bufq_head(&port->recv.clear_buffers)->begin > 0);
	Assert(bufq_head(&port->recv.clear_buffers)->begin <
		   bufq_head(&port->recv.clear_buffers)->end);

	/* Rewind by one byte. */
	bufq_head(&port->recv.clear_buffers)->begin--;

	return byte;
}
