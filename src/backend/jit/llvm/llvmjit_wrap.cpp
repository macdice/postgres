/*-------------------------------------------------------------------------
 *
 * llvmjit_wrap.cpp
 *	  Parts of the LLVM interface not (yet) exposed to C.
 *
 * Copyright (c) 2016-2024, PostgreSQL Global Development Group
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
#include <llvm-c/Types.h>
#include <llvm/IR/Function.h>
#if LLVM_VERSION_MAJOR >= 14
#include <llvm-c/OrcEE.h>
#include <llvm/ExecutionEngine/JITLink/EHFrameSupport.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h>
#endif

#include "jit/llvmjit.h"


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

#if LLVM_VERSION_MAJOR >= 14
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ExecutionSession, LLVMOrcExecutionSessionRef)
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ObjectLayer, LLVMOrcObjectLayerRef)

/*
 * Like LLVMOrcCreateRTDyldObjectLinkingLayer(), but for JITLink.
 * OrcEE.h does not yet offer a function like this for the C API, so
 * we supply our own.
 */
LLVMOrcObjectLayerRef
LLVMOrcCreateJITLinkObjectLinkingLayer(LLVMOrcExecutionSessionRef ES)
{
	Assert(ES);

	using namespace llvm::orc;
	using namespace llvm::jitlink;

	std::unique_ptr<ObjectLinkingLayer>
		oll(new ObjectLinkingLayer(*unwrap(ES)));
	std::unique_ptr<InProcessEHFrameRegistrar>
		fr(new InProcessEHFrameRegistrar());
	std::unique_ptr<EHFrameRegistrationPlugin>
		plugin(new EHFrameRegistrationPlugin(*unwrap(ES), std::move(fr)));

	oll->addPlugin(std::move(plugin));

	return wrap(oll.release());
}
#endif
