# src/test/modules/encoding/Makefile

REGRESS = encoding

NO_INSTALLCHECK = 1
ENCODING = UTF8
NO_LOCALE = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/encoding
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
endif
