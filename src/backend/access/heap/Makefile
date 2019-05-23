#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/heap
#
#-------------------------------------------------------------------------

subdir = src/backend/access/heap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = heapam.o heapam_handler.o heapam_visibility.o hio.o pruneheap.o rewriteheap.o \
	syncscan.o tuptoaster.o vacuumlazy.o visibilitymap.o

include $(top_srcdir)/src/backend/common.mk
