/* src/interfaces/ecpg/include/pgtypes.h */

#ifndef PGTYPES_H
#define PGTYPES_H

#ifdef __cplusplus
extern "C"
{
#endif

extern void PGTYPESchar_free(char *ptr);

/*
 * Initialize.  Not thread-safe and should only be called in one thread at a
 * time, but idempotent.  Returns 0 on success, -1 on failure and sets errno.
 */
extern int	PGTYPESinit(void);

#ifdef __cplusplus
}
#endif

#endif							/* PGTYPES_H */
