#include "moderndbs/error.h"
#include "moderndbs/codegen/expression.h"
#include <llvm/IR/TypeBuilder.h>
#include <iostream>

using JIT = moderndbs::JIT;
using Expression = moderndbs::Expression;
using ExpressionCompiler = moderndbs::ExpressionCompiler;
using NotImplementedException = moderndbs::NotImplementedException;
using data64_t = moderndbs::data64_t;

/// Evaluate the expresion.
data64_t Expression::evaluate(const data64_t* args) {
    return this->evaluate(args);
}

/// Build the expression code.
llvm::Value* Expression::build(llvm::IRBuilder<>& builder, llvm::Value* args) {
   return this->build(builder, args);
}

/// Constructor.
ExpressionCompiler::ExpressionCompiler(llvm::LLVMContext& context)
    : context(context), module(std::make_shared<llvm::Module>("meaningful_module_name", context)), jit(), fnPtr(nullptr) {
}

/// Compile an expression.
void ExpressionCompiler::compile(Expression& expression) {

    // declare i32 @printf(i8*, ...)
    auto expressionTypeBuilder = llvm::TypeBuilder<data64_t(data64_t *), false>::get(context);
    auto expressionFunction = llvm::cast<llvm::Function>(module->getOrInsertFunction("evaluation", expressionTypeBuilder));

    // entry:
    // %tmp = mul i32 %x, %y
    llvm::BasicBlock *expressionFunctionEntryBlock = llvm::BasicBlock::Create(context, "entry", expressionFunction);
    llvm::IRBuilder<> builder(expressionFunctionEntryBlock);
    builder.SetInsertPoint(expressionFunctionEntryBlock);

    builder.CreateRet( builder.CreateBitCast(expression.build(builder, expressionFunction->arg_begin()), llvm::Type::getInt64Ty(builder.getContext())));

   auto handle = jit.addModule(module);

}

/// Compile an expression.
data64_t ExpressionCompiler::run(data64_t* args) {

    fnPtr = reinterpret_cast<data64_t (*)(data64_t *)>(jit.getPointerToFunction("evaluation"));

    return fnPtr(args);
}

/// Dump the llvm module.
void ExpressionCompiler::dump() {
    module->print(llvm::errs(), nullptr);
}
