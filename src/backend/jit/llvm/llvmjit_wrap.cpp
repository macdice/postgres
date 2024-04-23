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
#include <llvm/IR/Function.h>

#if LLVM_VERSION_MAJOR < 17
#undef PIC		/* an unfortunately chosen identifier */
#include <llvm/Passes/PassBuilder.h>
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

#if LLVM_VERSION_MAJOR < 17
void
LLVMPassBuilderOptionsSetInlinerThreshold(LLVMPassBuilderOptionsRef Options,
										  int Threshold)
{
	/*
	 * A struct that is layout compatible with:
	 *
	 * https://github.com/llvm/llvm-project/blob/release/14.x/llvm/lib/Passes/PassBuilderBindings.cpp#L26
	 * https://github.com/llvm/llvm-project/blob/release/15.x/llvm/lib/Passes/PassBuilderBindings.cpp#L26
	 * https://github.com/llvm/llvm-project/blob/release/16.x/llvm/lib/Passes/PassBuilderBindings.cpp#L26
	 *
	 * This function that we needed was upstreamed in LLVM 17, but
	 * unfortunately we can't see the class definition from here.
	 *
	 * https://github.com/llvm/llvm-project/commit/c20a9bb001855da5d14721ce2894e3be77a999fe
	 */
	struct FakePassBuilderOptions {
		bool DebugLogging;
		bool VerifyEach;
		llvm::PipelineTuningOptions PTO;
	};

	reinterpret_cast<FakePassBuilderOptions *>(Options)->PTO.InlinerThreshold = Threshold;
}
#endif
