// ---------------------------------------------------------------------------
// MODERNDBS
// ---------------------------------------------------------------------------
#include <gtest/gtest.h>
#include <llvm/Support/TargetSelect.h>
// ---------------------------------------------------------------------------
int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    testing::GTEST_FLAG(filter) = "*Expression*";
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
// ---------------------------------------------------------------------------
