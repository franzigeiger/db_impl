#include <random>
#include <gtest/gtest.h>
#include "llvm/ExecutionEngine/MCJIT.h"
#include "llvm/IR/LLVMContext.h"
#include "moderndbs/codegen/expression.h"

using JIT = moderndbs::JIT;
using Argument = moderndbs::Argument;
using Cast = moderndbs::Cast;
using Constant = moderndbs::Constant;
using Expression = moderndbs::Expression;
using AddExpression = moderndbs::AddExpression;
using BinaryExpression = moderndbs::BinaryExpression;
using DivExpression = moderndbs::DivExpression;
using ExpressionCompiler = moderndbs::ExpressionCompiler;
using MulExpression = moderndbs::MulExpression;
using SubExpression = moderndbs::SubExpression;
using data64_t = moderndbs::data64_t;

namespace {

// ----------------------------------
// INTEGER
// ----------------------------------
// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledFPAddition) {
        llvm::LLVMContext context;
        Constant l(42.0);
        Constant r(21.0);
        AddExpression add(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(add);
        auto result = compiler.run(nullptr);
         EXPECT_NEAR(*reinterpret_cast<double*>(&result), 63.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledIntegerConstant) {
        llvm::LLVMContext context;
        Constant constant(static_cast<int64_t>(42));
        ExpressionCompiler compiler(context);
        compiler.compile(constant);
        auto result = compiler.run(nullptr);
        EXPECT_EQ(result, 42);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledIntegerArgument) {
        llvm::LLVMContext context;
        Argument arg(1, Expression::ValueType::INT64);
        ExpressionCompiler compiler(context);
        compiler.compile(arg);
        std::vector<int64_t> args { 21, 42 };
        auto result = compiler.run(reinterpret_cast<data64_t*>(args.data()));
        EXPECT_EQ(result, 42);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledIntegerAddition) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(42));
        Constant r(static_cast<int64_t>(21));
        AddExpression add(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(add);
        auto result = compiler.run(nullptr);
        EXPECT_EQ(result, 63);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledIntegerSubtraction) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(21));
        Constant r(static_cast<int64_t>(42));
        SubExpression sub(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(sub);
        auto result = compiler.run(nullptr);
        EXPECT_EQ(result, -21);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledIntegerMultiplication) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(42));
        Constant r(static_cast<int64_t>(21));
        MulExpression mul(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(mul);
        auto result = compiler.run(nullptr);
        EXPECT_EQ(result, 882);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledIntegerDivision) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(42));
        Constant r(static_cast<int64_t>(21));
        DivExpression div(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(div);
        auto result = compiler.run(nullptr);
        EXPECT_EQ(result, 2);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedIntegerConstant) {
        llvm::LLVMContext context;
        Constant constant(static_cast<int64_t>(42));
        auto result = constant.evaluate(nullptr);
        EXPECT_EQ(result, 42);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedIntegerArgument) {
        llvm::LLVMContext context;
        Argument arg(1, Expression::ValueType::INT64);
        std::vector<int64_t> args { 21, 42 };
        auto result = arg.evaluate(reinterpret_cast<data64_t*>(args.data()));
        EXPECT_EQ(result, 42);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedIntegerAddition) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(42));
        Constant r(static_cast<int64_t>(21));
        AddExpression add(l, r);
        auto result = add.evaluate(nullptr);
        EXPECT_EQ(result, 63);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedIntegerSubtraction) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(21));
        Constant r(static_cast<int64_t>(42));
        SubExpression sub(l, r);
        auto result = sub.evaluate(nullptr);
        EXPECT_EQ(result, -21);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedIntegerMultiplication) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(42));
        Constant r(static_cast<int64_t>(21));
        MulExpression mul(l, r);
        auto result = mul.evaluate(nullptr);
        EXPECT_EQ(result, 882);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedIntegerDivision) {
        llvm::LLVMContext context;
        Constant l(static_cast<int64_t>(42));
        Constant r(static_cast<int64_t>(21));
        DivExpression div(l, r);
        auto result = div.evaluate(nullptr);
        EXPECT_EQ(result, 2);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledComplexIntegerExpression) {
        llvm::LLVMContext context;

        // (((42 + a1) * (21 - a0) * a2) + 100) / a3

        Constant c1(static_cast<int64_t>(42));
        Constant c2(static_cast<int64_t>(21));
        Constant c3(static_cast<int64_t>(100));

        Argument a0(0, Expression::ValueType::INT64);
        Argument a1(1, Expression::ValueType::INT64);
        Argument a2(2, Expression::ValueType::INT64);
        Argument a3(3, Expression::ValueType::INT64);

        AddExpression v1(c1, a1);
        SubExpression v2(c2, a0);
        MulExpression v3(v1, v2);
        MulExpression v4(v3, a2);
        AddExpression v5(v4, c3);
        DivExpression v6(v5, a3);

        Expression& root = v6;

        ExpressionCompiler compiler(context);
        compiler.compile(root);
        compiler.dump();

        std::mt19937_64 rng(0);
        std::uniform_int_distribution<std::mt19937_64::result_type> dis;

        std::vector<int64_t> args(4);

        for (int i = 0; i < 1000; ++i) {
            args[0] = dis(rng);
            args[1] = dis(rng);
            args[2] = dis(rng);
            args[3] = dis(rng);

            auto expected = (((42 + args[1]) * (21 - args[0]) * args[2]) + 100) / args[3];
            auto result = compiler.run(reinterpret_cast<data64_t*>(args.data()));

            size_t doSomething =5;
            ASSERT_EQ(result, expected);
        }
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedComplexIntegerExpression) {
        llvm::LLVMContext context;

        // (((42 + a1) * (21 - a0) * a2) + 100) / a3

        Constant c1(static_cast<int64_t>(42));
        Constant c2(static_cast<int64_t>(21));
        Constant c3(static_cast<int64_t>(100));

        Argument a0(0, Expression::ValueType::INT64);
        Argument a1(1, Expression::ValueType::INT64);
        Argument a2(2, Expression::ValueType::INT64);
        Argument a3(3, Expression::ValueType::INT64);

        AddExpression v1(c1, a1);
        SubExpression v2(c2, a0);
        MulExpression v3(v1, v2);
        MulExpression v4(v3, a2);
        AddExpression v5(v4, c3);
        DivExpression v6(v5, a3);

        Expression& root = v6;

        std::mt19937_64 rng(0);
        std::uniform_int_distribution<std::mt19937_64::result_type> dis;

        std::vector<int64_t> args(4);

        for (int i = 0; i < 1000; ++i) {
            args[0] = dis(rng);
            args[1] = dis(rng);
            args[2] = dis(rng);
            args[3] = dis(rng);

            auto expected = (((42 + args[1]) * (21 - args[0]) * args[2]) + 100) / args[3];
            auto result = root.evaluate(reinterpret_cast<data64_t*>(args.data()));

            ASSERT_EQ(result, expected);
        }
    }

// ----------------------------------
// FLOATS
// ----------------------------------


// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledFPConstant) {
        llvm::LLVMContext context;
        Constant constant(42.0);
        ExpressionCompiler compiler(context);
        compiler.compile(constant);
        auto result = compiler.run(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 42.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledFPArgument) {
        llvm::LLVMContext context;
        Argument arg(1, Expression::ValueType::DOUBLE);
        ExpressionCompiler compiler(context);
        compiler.compile(arg);
        std::vector<double> args { 21.0, 42.0 };
        auto result = compiler.run(reinterpret_cast<data64_t*>(args.data()));
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 42.0, 0.1);
    }



// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledFPSubtraction) {
        llvm::LLVMContext context;
        Constant l(21.0);
        Constant r(42.0);
        SubExpression sub(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(sub);
        auto result = compiler.run(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), -21.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledFPMultiplication) {
        llvm::LLVMContext context;
        Constant l(42.0);
        Constant r(21.0);
        MulExpression mul(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(mul);
        auto result = compiler.run(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 882.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledFPDivision) {
        llvm::LLVMContext context;
        Constant l(42.0);
        Constant r(21.0);
        DivExpression div(l, r);
        ExpressionCompiler compiler(context);
        compiler.compile(div);
        auto result = compiler.run(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 2.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedFPConstant) {
        llvm::LLVMContext context;
        Constant constant(42.0);
        auto result = constant.evaluate(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 42.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedFPArgument) {
        llvm::LLVMContext context;
        Argument arg(1, Expression::ValueType::DOUBLE);
        std::vector<double> args { 21, 42 };
        auto result = arg.evaluate(reinterpret_cast<data64_t*>(args.data()));
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 42.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedFPAddition) {
        llvm::LLVMContext context;
        Constant l(42.0);
        Constant r(21.0);
        AddExpression add(l, r);
        auto result = add.evaluate(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 63.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedFPSubtraction) {
        llvm::LLVMContext context;
        Constant l(21.0);
        Constant r(42.0);
        SubExpression sub(l, r);
        auto result = sub.evaluate(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), -21.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedFPMultiplication) {
        llvm::LLVMContext context;
        Constant l(42.0);
        Constant r(21.0);
        MulExpression mul(l, r);
        auto result = mul.evaluate(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 882.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedFPDivision) {
        llvm::LLVMContext context;
        Constant l(42.0);
        Constant r(21.0);
        DivExpression div(l, r);
        auto result = div.evaluate(nullptr);
        EXPECT_NEAR(*reinterpret_cast<double*>(&result), 2.0, 0.1);
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, CompiledComplexFPExpression) {
        llvm::LLVMContext context;

        // (((42 + a1) - (21 - a0)) + a2) + 100.0

        Constant c1(42.0);
        Constant c2(21.0);
        Constant c3(100.0);

        Argument a0(0, Expression::ValueType::DOUBLE);
        Argument a1(1, Expression::ValueType::DOUBLE);
        Argument a2(2, Expression::ValueType::DOUBLE);

        AddExpression v1(c1, a1);
        SubExpression v2(c2, a0);
        SubExpression v3(v1, v2);
        AddExpression v4(v3, a2);
        AddExpression v5(v4, c3);

        Expression& root = v5;

        ExpressionCompiler compiler(context);
        compiler.compile(root);
        compiler.dump();

        std::mt19937_64 rng(0);
        std::uniform_real_distribution<double> dis(-10, +10);

        std::vector<double> args(3);

        for (int i = 0; i < 1000; ++i) {
            args[0] = dis(rng);
            args[1] = dis(rng);
            args[2] = dis(rng);

            auto expected = (((42.0 + args[1]) - (21.0 - args[0]) + args[2]) + 100.0);
            auto result = compiler.run(reinterpret_cast<data64_t*>(args.data()));

            ASSERT_NEAR(*reinterpret_cast<double*>(&result), expected, 1.0);
        }
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, InterpretedComplexFPExpression) {
        llvm::LLVMContext context;

        // (((42 + a1) - (21 - a0)) + a2) + 100.0

        Constant c1(42.0);
        Constant c2(21.0);
        Constant c3(100.0);

        Argument a0(0, Expression::ValueType::DOUBLE);
        Argument a1(1, Expression::ValueType::DOUBLE);
        Argument a2(2, Expression::ValueType::DOUBLE);

        AddExpression v1(c1, a1);
        SubExpression v2(c2, a0);
        SubExpression v3(v1, v2);
        AddExpression v4(v3, a2);
        AddExpression v5(v4, c3);

        Expression& root = v5;

        std::mt19937_64 rng(0);
        std::uniform_real_distribution<double> dis(-10, +10);

        std::vector<double> args(3);

        for (int i = 0; i < 1000; ++i) {
            args[0] = dis(rng);
            args[1] = dis(rng);
            args[2] = dis(rng);

            auto expected = (((42.0 + args[1]) - (21.0 - args[0]) + args[2]) + 100.0);
            auto result = root.evaluate(reinterpret_cast<data64_t*>(args.data()));

            ASSERT_NEAR(*reinterpret_cast<double*>(&result), expected, 1.0);
        }
    }

// NOLINTNEXTLINE
    TEST(ExpressionTest, ComiledCastingExpression) {
        llvm::LLVMContext context;

        // (((42 + a1) - (21 - a0)) + a2) + 100

        Constant c1(42.0);
        Constant c2(21.0);
        Constant c3(static_cast<int64_t>(100));

        Argument a0(0, Expression::ValueType::DOUBLE);
        Argument a1(1, Expression::ValueType::DOUBLE);
        Argument a2(2, Expression::ValueType::INT64);

        AddExpression v1(c1, a1);
        SubExpression v2(c2, a0);
        SubExpression v3(v1, v2);

        Cast v4(v3, Expression::ValueType::INT64);
        AddExpression v5(v4, a2);
        AddExpression v6(v5, c3);

        Expression& root = v6;

        ExpressionCompiler compiler(context);
        compiler.compile(root);
        compiler.dump();

        std::mt19937_64 rng(0);
        std::uniform_real_distribution<double> dis_double(-10, +10);
        std::uniform_int_distribution<std::mt19937_64::result_type> dis_int(0, 100);

        std::vector<data64_t> args(3);

        for (int i = 0; i < 1000; ++i) {
            double arg0 = dis_double(rng);
            double arg1 = dis_double(rng);
            int64_t arg2 = dis_int(rng);
            args[0] = *reinterpret_cast<data64_t*>(&arg0);
            args[1] = *reinterpret_cast<data64_t*>(&arg1);
            args[2] = *reinterpret_cast<data64_t*>(&arg2);

            auto expected = ((static_cast<int64_t>((42.0 + arg1) - (21.0 - arg0)) + arg2) + 100);
            auto result = compiler.run(args.data());

            ASSERT_EQ(*reinterpret_cast<int64_t*>(&result), expected);
        }
    }

}  // namespace
