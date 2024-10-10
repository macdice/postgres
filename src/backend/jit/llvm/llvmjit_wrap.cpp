/*-------------------------------------------------------------------------
 *
 * llvmjit_wrap.cpp
 *	  Parts of the LLVM interface not (yet) exposed to C.
 *
 * Copyright (c) 2016-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/llvm/llvmjit_wrap.cpp
 *
 *-------------------------------------------------------------------------
 */

extern "C"
{
#include "postgres.h"
}

#include <llvm-c/Core.h>
#include <llvm/IR/Function.h>

#include "jit/llvmjit.h"
#include "jit/llvmjit_backport.h"

#ifdef USE_LLVM_JITLINK
#include <llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h>
#endif

#ifdef USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include "jit/SectionMemoryManager.h"
#include <llvm/Support/CBindingWrapping.h>
#endif


/*
 * C-API extensions.
 */

LLVMTypeRef
LLVMGetFunctionReturnType(LLVMValueRef r)
{
	return llvm::wrap(llvm::unwrap<llvm::Function>(r)->getReturnType());
}

LLVMTypeRef
LLVMGetFunctionType(LLVMValueRef r)
{
	return llvm::wrap(llvm::unwrap<llvm::Function>(r)->getFunctionType());
}

#if defined(USE_LLVM_JITLINK) || defined(USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER)
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ExecutionSession, LLVMOrcExecutionSessionRef)
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ObjectLayer, LLVMOrcObjectLayerRef)
#endif

#if defined(USE_LLVM_JITLINK)

LLVMOrcObjectLayerRef
LLVMOrcCreateJITLinkObjectLinkingLayer(LLVMOrcExecutionSessionRef ES)
{
	auto expected_mm = llvm::jitlink::InProcessMemoryManager::Create();

	/* XXX how should this error be reported? */
	if (!expected_mm)
		elog(ERROR,
			 "could not create JITLink memory manager: %s",
			 llvm::toString(expected_mm.takeError()).c_str());

	return wrap(new llvm::orc::ObjectLinkingLayer(*unwrap(ES), std::move(*expected_mm)));
}

#elif defined(USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER)

/*
 * Using the older RuntimeDyld API, but with our backported memory manager.
 */
LLVMOrcObjectLayerRef
LLVMOrcCreateRTDyldObjectLinkingLayerWithSafeSectionMemoryManager(LLVMOrcExecutionSessionRef ES)
{
	return wrap(new llvm::orc::RTDyldObjectLinkingLayer(
		*unwrap(ES), [] { return std::make_unique<llvm::backport::SectionMemoryManager>(nullptr, true); }));
}

#endif
