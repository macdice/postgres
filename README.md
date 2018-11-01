Undo logs for smgr file cleanup

The commits here are automatically extracted from the zheap-tmunro
branch at
https://github.com/EnterpriseDB/zheap.

There are seven patches in this patch set:

* [0001-Add-undo-log-manager.patch](../../commit/74ec23cfe07567565b1b7fecc9b1206b9e220821)
* [0002-Provide-access-to-undo-log-data-via-the-buffer-manager.patch](../../commit/9172991104f0471e7877688aafc27aa8d2056d31)
* [0003-Add-developer-documentation-for-the-undo-log-manager.patch](../../commit/26626245371e6b1144094c7cde0f4671cfc61e46)
* [0004-Add-tests-for-the-undo-log-manager.patch](../../commit/25197d739ae404ec0669c676ea8c94cd0000f26a)
* [0005-Add-user-facing-documentation-for-undo-logs.patch](../../commit/af04da90506cf460537001932f1159b0833732e7)
* [0006-Add-undo-records-workers-and-xact-integration.patch](../../commit/5cb30548eef039c52e0ce7ee42e09549b1dbe984)
* [0007-Use-undo-based-rollback-to-clean-up-files-on-abort.patch](../../commit/d7fb69dfa32d96298629bfd35675b7893cf62898)

This branch is automatically tested by:

* Ubuntu build bot over at Travis CI:  [<img src="https://travis-ci.org/macdice/postgres.svg?branch=undo-smgr"/>](https://travis-ci.org/macdice/postgres/branches)
* Windows build bot over at AppVeyor: [<img src="https://ci.appveyor.com/api/projects/status/github/macdice/postgres?branch=undo-smgr&svg=true"/>](https://ci.appveyor.com/project/macdice/postgres/branch/undo-smgr)

