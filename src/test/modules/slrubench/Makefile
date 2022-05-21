# src/test/modules/slrubench/Makefile

MODULE_big = slrubench
OBJS = \
	$(WIN32RES) \
	slrubench.o
PGFILEDESC = "slrubench"

EXTENSION = slrubench
DATA = slrubench--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/slrubench
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
