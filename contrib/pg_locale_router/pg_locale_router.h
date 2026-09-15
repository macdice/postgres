#ifndef PG_LOCALE_ROUTER_H
#define PG_LOCALE_ROUTER_H

extern pg_locale_t pg_locale_router_newlocale_libc(const locale_descriptor *descriptor,
												   int flags,
												   MemoryContext context,
												   pg_newlocale_function std_newlocale);
#ifdef USE_ICU
extern pg_locale_t pg_locale_router_newlocale_icu(const locale_descriptor *descriptor,
												  int flags,
												  MemoryContext context,
												  pg_newlocale_function std_newlocale);
#endif

#endif
