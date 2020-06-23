# Async IO for PostgreSQL, porting branch

This is Thomas Munro's experimental branch of [Andres Freund's PostgreSQL AIO
prototype](https://github.com/anarazel/postgres/tree/aio).  Andres's branch
supplies the core architecture for asynchronous buffer and WAL I/O, but
requires Linux 5.1 or later because it uses
[io\_uring](https://lwn.net/Articles/810414/).  This branch attempts to add
support for other operating system using the POSIX AIO standard.

Five target operating systems:

 * FreeBSD: [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-posix&task=FreeBSD)](https://cirrus-ci.com/github/macdice/postgres?branch=aio-posix)
 * macOS: [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-posix&task=macOS)](https://cirrus-ci.com/github/macdice/postgres?branch=aio-posix)
 * NetBSD: make check passed when I tried it once on NetBSD 9.0
 * HPUX: untested, but likely to work
 * AIX: untested, but likely to need a bit of extra work due to note about [mmap memory](https://www.ibm.com/support/knowledgecenter/en/ssw_aix_72/kernelextension/async_io_subsys.html)

Two non-target operating systems -- they have some kind of POSIX AIO support,
but it's not suitable because it creates threads in every process:

 * Solaris/illumos: userland implementation in libc
 * GNU/Linux: userland implementation in libc

A separate project will add Windows support.

A separate project will add an optional background-worker mode available on all operating systems.
