# src/test/modules/test_sts/Makefile

MODULE_big = test_sts
OBJS = \
	$(WIN32RES) \
	test_sts.o
PGFILEDESC = "test_sts - test code for sharedtuplestore.c"

EXTENSION = test_sts
DATA = test_sts--1.0.sql

REGRESS = test_sts

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_sts
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
