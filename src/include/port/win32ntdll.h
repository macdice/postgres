/*-------------------------------------------------------------------------
 *
 * win32_ntdll.h
 *	  Dynamically loaded Windows NT functions.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/win32ntdll.h
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include <ntstatus.h>
#include <winternl.h>

typedef NTSTATUS (__stdcall *NtCreateFile_t)(PHANDLE FileHandle,
											 ACCESS_MASK DesiredAccess,
											 POBJECT_ATTRIBUTES ObjectAttributes,
											 PIO_STATUS_BLOCK IoStatusBlock,
											 PLARGE_INTEGER AllocationSize,
											 ULONG FileAttributes,
											 ULONG ShareAccess,
											 ULONG CreateDisposition,
											 ULONG CreateOptions,
											 PVOID EaBuffer,
											 ULONG EaLength);

typedef BOOLEAN (__stdcall *RtlDosPathNameToNtPathName_U_t)(PCWSTR DosName,
															PUNICODE_STRING NtName,
															PCWSTR *PartName,
															void *RelativeName);

typedef ULONG (__stdcall *RtlNtStatusToDosError_t)(NTSTATUS Status);

extern NtCreateFile_t pg_NtCreateFile;
extern RtlDosPathNameToNtPathName_U_t pg_RtlDosPathNameToNtPathName_U;
extern RtlNtStatusToDosError_t pg_RtlNtStatusToDosError;

extern int initialize_ntdll(void);
