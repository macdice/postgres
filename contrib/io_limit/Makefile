# contrib/io_limit/Makefile

MODULE_big = io_limit
OBJS = \
	$(WIN32RES) \
	io_limit.o

EXTENSION = io_limit
PGFILEDESC = "io_limit - io_limit - artificially limit asynchronous I/O for tesing"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_prewarm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
