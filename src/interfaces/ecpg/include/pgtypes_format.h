/*
 * We would like to be able to convert between strings and doubles using the C
 * locale.  We can set the thread's current locale with uselocale() and use
 * snprintf() and strtod() according to the POSIX standard, but some systems
 * haven't implemented uselocale() yet.  We can use snprintf_l() and
 * strtod_l() on some systems, but POSIX hasn't standardized those yet.  All
 * systems we target can do one or the other, so provide a simple abstraction.
 */

#ifndef PGTYPES_FORMAT_H
#define PGTYPES_FORMAT_H

#if defined(LOCALE_T_IN_XLOCALE)
#include <xlocale.h>
#else
#include <locale.h>
#endif

extern int PGTYPESbegin_clocale(locale_t *old_locale);
extern void PGTYPESend_clocale(locale_t old_locale);

extern double PGTYPESstrtod(const char *str, char **endptr);
extern int PGTYPESsprintf(char *str, const char *format, ...) pg_attribute_printf(2, 3);
extern int PGTYPESsnprintf(char *str, size_t size, const char *format, ...) pg_attribute_printf(3, 4);

#endif
