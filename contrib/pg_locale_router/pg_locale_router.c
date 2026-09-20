#include "postgres.h"

#include "catalog/pg_collation.h"
#include "fmgr.h"
#include "utils/pg_locale.h"
#include "utils/pg_locale_internal.h"

#include "pg_locale_router.h"

PG_MODULE_MAGIC_EXT(.name = "pg_locale_router",
                    .version = PG_VERSION);



static pg_locale_t
pg_locale_router_newlocale(const locale_descriptor *descriptor,
						   int flags,
						   MemoryContext context,
						   pg_newlocale_function std_newlocale)
{
	
	switch (descriptor->provider)
	{
	case COLLPROVIDER_BUILTIN:
		/*
		 * Can't intercept builtin provider.  A plausible reason to do so
		 * would be to use ctype from an older Unicode version, but that seems
		 * like a job for a different extension.  pg_locale_router doesn't
		 * support ctype versioning for libc or ICU either so this isn't
		 * currently done.
		 */
		return std_newlocale(descriptor, flags, context);
		
	case COLLPROVIDER_LIBC:
		/* Intercept libc locales. */
		return pg_locale_router_newlocale_libc(descriptor,
											   flags,
											   context,
											   std_newlocale);
#ifdef USE_ICU
	case COLLPROVIDER_ICU:
		/* Intercept ICU locales. */
		return pg_locale_router_newlocale_icu(descriptor,
											  flags,
											  context,
											  std_newlocale);
#endif

	default:
		elog(ERROR, "pg_locale_router: unhandled collation provider for OID %u",
			 descriptor->id);
	}
}

void
_PG_init(void)
{
	if (pg_newlocale_hook)
		elog(ERROR, "pg_locale_router: pg_newlocale_hook already set");

	pg_newlocale_hook = pg_locale_router_newlocale;
}
