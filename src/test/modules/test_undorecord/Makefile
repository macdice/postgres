# src/test/modules/test_undorecord/Makefile

MODULE_big = test_undorecord
OBJS = test_undorecord.o
PGFILEDESC = "test_undorecord - a test module for undo records"

EXTENSION = test_undorecord
DATA = test_undorecord--1.0.sql

REGRESS = test_undorecord

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_undo
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
