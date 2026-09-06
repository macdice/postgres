# src/test/modules/test_simplehash/Makefile

MODULE_big = test_simplehash
OBJS = \
	$(WIN32RES) \
	test_simplehash.o
PGFILEDESC = "test_simplehash - test code for simplehash"

EXTENSION = test_simplehash
DATA = test_simplehash--1.0.sql

REGRESS = test_simplehash

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_simplehash
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
