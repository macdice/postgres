/*-------------------------------------------------------------------------
 *
 * admission.h
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * src/include/utils/admission.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ADMISSION_H
#define ADMISSION_H

#include "executor/execdesc.h"

extern void AdmissionControlExecMemChanged(ssize_t delta, void *data);

extern size_t AdmissionControlBeginQuery(QueryDesc *queryDesc);
extern void AdmissionControlEndQuery(size_t size);

extern void AdmissionControlBeginSession(void);
extern void AdmissionControlEndSession(int, Datum);

extern Size AdmissionControlShmemSize(void);
extern void AdmissionControlShmemInit(void);

#endif							/* ADMISSION_H */
