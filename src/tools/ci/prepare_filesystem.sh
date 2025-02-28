#!/bin/sh

set -e
set -x

# migrate working directory to tmpfs
du -hs $CIRRUS_WORKING_DIR
mv $CIRRUS_WORKING_DIR $CIRRUS_WORKING_DIR.orig
mkdir $CIRRUS_WORKING_DIR
case "`uname`" in

  FreeBSD|NetBSD)
    mount -t tmpfs tmpfs $CIRRUS_WORKING_DIR
    ;;

  OpenBSD)
    # tmpfs is broken on OpenBSD.  mfs is similar but you need a size up front
    # and that needs to be backed by swap, so we'll somehow need to add swap,
    # either here or on the image.

    #du -h
    #umount /usr/obj  # unused 5G on /dev/sd0j
    #swapon /dev/sd0j # fails with ENXIO... what am I missing?

    # XXX As a temporary hack, mount a 4GB file as swap.
    dd if=/dev/zero of=/usr/obj/big.swap bs=1M count=4096
    swapon /usr/obj/big.swap

    mount -t mfs -o rw,noatime,nodev,-s=8000000 swap $CIRRUS_WORKING_DIR
    ;;

  *)
    echo "I don't know how to create a RAM disk on this computer"
    exit 1
esac
cp -a $CIRRUS_WORKING_DIR.orig/. $CIRRUS_WORKING_DIR/
