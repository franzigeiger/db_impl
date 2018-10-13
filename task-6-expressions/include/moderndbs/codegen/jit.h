#ifndef INCLUDE_MODERNDBS_CODEGEN_JIT_H
#define INCLUDE_MODERNDBS_CODEGEN_JIT_H

#include <llvm/ADT/STLExtras.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/IRTransformLayer.h>
#include <llvm/ExecutionEngine/Orc/LambdaResolver.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/Mangler.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace moderndbs {

    class JIT {
        private:
        /// The target machine.
        std::unique_ptr<llvm::TargetMachine> target_machine;
        /// The data layout.
        const llvm::DataLayout data_layout;

        // Optimization function
        using TransformFtor = std::function<std::shared_ptr<llvm::Module>(std::shared_ptr<llvm::Module>)>;

        /// The object layer.
        llvm::orc::RTDyldObjectLinkingLayer object_layer;
        /// The compile layer.
        llvm::orc::IRCompileLayer<decltype(object_layer), llvm::orc::SimpleCompiler> compile_layer;
        /// The optimize layer.
        llvm::orc::IRTransformLayer<decltype(compile_layer), TransformFtor> optimize_layer;

        public:
        /// The module handle.
        using ModuleHandle = decltype(compile_layer)::ModuleHandleT;

        /// The constructor.
        JIT();

        /// Get the target machine.
        auto& getTargetMachine() { return *target_machine; }
        /// Add a module.
        llvm::Expected<JIT::ModuleHandle> addModule(std::shared_ptr<llvm::Module> module);

        /// Search for JIT symbols.
        llvm::JITSymbol findSymbol(const std::string& name);
        /// Remove a module from the JIT.
        void removeModule(ModuleHandle module);

        /// Get pointer to function.
        void* getPointerToFunction(const std::string& name);
    };

}  // namespace moderndbs

#endif
