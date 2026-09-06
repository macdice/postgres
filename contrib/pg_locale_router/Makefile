# contrib/pg_locale_router/Makefile

MODULE_big = pg_locale_router
OBJS = \
	$(WIN32RES) \
	pg_locale_router.o \
	pg_locale_router_icu.o \
	pg_locale_router_libc.o \

EXTENSION = pg_locale_router
DATA = pg_locale_router--1.0.sql
PGFILEDESC = "pg_locale_router - version-aware locales"

#REGRESS = heap_locale_router

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_locale_router
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
