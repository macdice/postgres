# Async IO for PostgreSQL, porting branch

This is Thomas Munro's experimental branch of [Andres Freund's PostgreSQL AIO
prototype](https://github.com/anarazel/postgres/tree/aio).  Andres's branch
supplies the core architecture for asynchronous buffer and WAL I/O, but
requires Linux 5.1 or later because it uses
[io\_uring](https://lwn.net/Articles/810414/).  This branch attempts to add
support for other operating system using worker processes and POSIX AIO, where
available.

| OS | Worker | Worker+DIO | Native AIO | Native AIO+DIO |
|----|--------|------------|-----|---------|
| Linux:4.19 | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=Linux&script=linux_worker_buf)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=Linux&script=linux_worker_dio)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | N/A | N/A |
| Linux:5.4 | [![Build Status](https://ci.appveyor.com/api/projects/status/github/macdice/postgres?branch=aio-porting&svg=true)](https://ci.appveyor.com/project/macdice/postgres) | [![Build Status](https://ci.appveyor.com/api/projects/status/github/macdice/postgres?branch=aio-porting&svg=true)](https://ci.appveyor.com/project/macdice/postgres)| [![Build Status](https://ci.appveyor.com/api/projects/status/github/macdice/postgres?branch=aio-porting&svg=true)](https://ci.appveyor.com/project/macdice/postgres)| [![Build Status](https://ci.appveyor.com/api/projects/status/github/macdice/postgres?branch=aio-porting&svg=true)](https://ci.appveyor.com/project/macdice/postgres) |
| FreeBSD | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=FreeBSD&script=freebsd_worker_buf)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=FreeBSD&script=freebsd_worker_dio)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=FreeBSD&script=freebsd_posix_buf)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=FreeBSD&script=freebsd_posix_dio)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) |
| macOS | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=macOS&script=macos_worker_buf)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=macOS&script=macos_worker_dio)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=macOS&script=macos_posix_buf)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=macOS&script=macos_posix_dio)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) |
| NetBSD | | | | |
| OpenBSD | | | | |
| illumos | | | | |
| Solaris | | | | |
| HPUX | | | | |
| AIX | | | | |
| Windows | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=Windows&script=windows_worker_buf)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | [![Build Status](https://api.cirrus-ci.com/github/macdice/postgres.svg?branch=aio-porting&task=Windows&script=windows_worker_dio)](https://cirrus-ci.com/github/macdice/postgres/aio-porting) | | |
