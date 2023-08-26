#!/bin/sh

set -e
set -x

# Set up a ZFS partition that is tuned to minimize I/O.
#
# XXX: It'd probably better to fix this in the image, using something like
# https://people.freebsd.org/~lidl/blog/re-root.html

# fix backup partition table after resize
gpart recover da0
gpart show da0
# kill swap, so we can delete a partition
swapoff -a || true
# (apparently we can only have 4!?)
gpart delete -i 3 da0
gpart add -t freebsd-zfs -l data8k -a 4096 da0
gpart show da0
zpool create tank /dev/da0p3
zfs create tank/work
zfs set atime=off tank/work
zfs set compression=off tank/work
zfs set recordsize=8192 tank/work
sysctl vfs.zfs.nocacheflush=1
#sysctl vfs.zfs.txg.timeout=1000

# Migrate working directory
du -hs $CIRRUS_WORKING_DIR
mv $CIRRUS_WORKING_DIR $CIRRUS_WORKING_DIR.orig
ln -s /tank/work $CIRRUS_WORKING_DIR
cp -r $CIRRUS_WORKING_DIR.orig/* $CIRRUS_WORKING_DIR/
