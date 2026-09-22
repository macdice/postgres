#ifndef PG_LOCALE_INTERNAL_H
#define PG_LOCALE_INTERNAL_H

extern size_t size_locale_descriptor(const locale_descriptor *src);
extern void copy_locale_descriptor(locale_descriptor *dst,
								   char *string_space,
								   const locale_descriptor *src);
extern void pg_locale_set_descriptor(pg_locale_t locale,
									 const locale_descriptor *src);

extern void pg_freelocale(pg_locale_t locale);

#endif
