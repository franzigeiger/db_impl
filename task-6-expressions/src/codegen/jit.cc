#include "moderndbs/codegen/jit.h"
#include "moderndbs/error.h"

#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>

using JIT = moderndbs::JIT;

namespace {

std::shared_ptr<llvm::Module> optimizeModule(std::shared_ptr<llvm::Module> module) {
    // Create a function pass manager
    auto passManager = llvm::make_unique<llvm::legacy::FunctionPassManager>(module.get());

    // Add passes
    passManager->add(llvm::createInstructionCombiningPass());
    passManager->add(llvm::createReassociatePass());
    passManager->add(llvm::createGVNPass());
    passManager->add(llvm::createCFGSimplificationPass());
    passManager->doInitialization();

    // Run the optimizations
    for (auto& fn: *module) {
        passManager->run(fn);
    }
    return module;
}

}  // namespace

JIT::JIT()
    : target_machine(llvm::EngineBuilder().selectTarget()),
    data_layout(target_machine->createDataLayout()),
    object_layer([]() { return std::make_shared<llvm::SectionMemoryManager>(); }),
    compile_layer(object_layer, llvm::orc::SimpleCompiler(*target_machine)),
    optimize_layer(compile_layer, &optimizeModule) {
    // Load exported symbols of host process.
    // This is needed for the symbol resolver in addModule
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

llvm::Expected<JIT::ModuleHandle> JIT::addModule(std::shared_ptr<llvm::Module> module) {
    // Build the symbol resolver.
    // Lambda 1: Look back into the JIT itself.
    // Lambda 2: Search for external symbols in the host process.
    auto resolver = llvm::orc::createLambdaResolver(
        [&](const std::string& name) {
            if (auto symbol = optimize_layer.findSymbol(name, false)) {
                return symbol;
            } else {
                return llvm::JITSymbol(nullptr);
            }
        },
        [](const std::string& name) {
            if (auto symbolAddr = llvm::RTDyldMemoryManager::getSymbolAddressInProcess(name)) {
                return llvm::JITSymbol(symbolAddr, llvm::JITSymbolFlags::Exported);
            } else {
                return llvm::JITSymbol(nullptr);
            }
        }
    );

    // Add the module to the optimization layer.
    return optimize_layer.addModule(std::move(module), std::move(resolver));
}
llvm::JITSymbol JIT::findSymbol(const std::string& name) {
    std::string mangledName;
    llvm::raw_string_ostream mangledNameStream(mangledName);
    llvm::Mangler::getNameWithPrefix(mangledNameStream, name, data_layout);
    return optimize_layer.findSymbol(mangledNameStream.str(), true);
}

void JIT::removeModule(ModuleHandle handle) {
    llvm::cantFail(optimize_layer.removeModule(handle));
}

void* JIT::getPointerToFunction(const std::string& name) {
    auto symbol = findSymbol(name);
    if (!symbol) {
        throw std::runtime_error("symbol not found");
    }
    return reinterpret_cast<void*>(static_cast<uintptr_t>(*symbol.getAddress()));
}
