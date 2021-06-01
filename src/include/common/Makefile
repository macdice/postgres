subdir = src/include/common
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

all: thread_local_vars.h

thread_local_vars.h: generate-thread-local-vars.pl
	$(PERL) $(srcdir)/generate-thread-local-vars.pl > $@

clean:
	rm thread_local_vars.h
