#ifndef PG_LOCALE_ROUTER_H
#define PG_LOCALE_ROUTER_H

extern pg_locale_t pg_locale_router_libc_newlocale(const locale_descriptor *descriptor,
												   int flags,
												   MemoryContext context,
												   pg_newlocale_function std_newlocale);
extern void pg_locale_router_libc_init(void);

#ifdef USE_ICU
extern void pg_locale_router_icu_init(void);
extern pg_locale_t pg_locale_router_icu_newlocale(const locale_descriptor *descriptor,
												  int flags,
												  MemoryContext context,
												  pg_newlocale_function std_newlocale);
#endif

#endif
