/*-------------------------------------------------------------------------
 *
 * combocid.h
 *	  Combo command ID support routines
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/combocid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMBOCID_H
#define COMBOCID_H

#include "storage/dsm.h"
#include "utils/dsa.h"

/*
 * HeapTupleHeaderGetCmin and HeapTupleHeaderGetCmax function prototypes
 * are in access/htup.h, because that's where the macro definitions that
 * those functions replaced used to be.
 */

extern void AtEOXact_ComboCid(void);
extern void SharedComboCidRegistryInit(dsm_segment *seg, dsa_area *area);
extern void SharedComboCidRegistryAttach(void);

#endif							/* COMBOCID_H */
