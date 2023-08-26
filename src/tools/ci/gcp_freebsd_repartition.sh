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

# delete and re-add swap partition with expanded size
swapoff -a
gpart delete -i 3 da0
gpart add -t freebsd-swap -l swapfs -a 4096 da0
gpart show da0
swapon -a

# create a file system on a memory disk backed by swap, resulting in an ffs
# file system that writes back more lazily
mdconfig -a -t swap -s20G -u md1
newfs -b 8192 -U /dev/md1

# Migrate working directory
du -hs $CIRRUS_WORKING_DIR
mv $CIRRUS_WORKING_DIR $CIRRUS_WORKING_DIR.orig
mkdir $CIRRUS_WORKING_DIR
mount -o noatime /dev/md1 $CIRRUS_WORKING_DIR
cp -r $CIRRUS_WORKING_DIR.orig/* $CIRRUS_WORKING_DIR/
