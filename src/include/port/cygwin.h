/* src/include/port/cygwin.h */

/*
 * Variables declared in the core backend and referenced by loadable
 * modules need to be marked "dllimport" in the core build, but
 * "dllexport" when the declaration is read in a loadable module.
 * No special markings should be used when compiling frontend code.
 */
#ifndef FRONTEND
#ifdef BUILDING_DLL
#define PGDLLIMPORT __declspec (dllexport)
#else
#define PGDLLIMPORT __declspec (dllimport)
#endif
#endif
