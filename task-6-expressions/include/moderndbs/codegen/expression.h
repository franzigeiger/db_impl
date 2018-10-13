#ifndef INCLUDE_MODERNDBS_CODEGEN_EXPRESSION_H
#define INCLUDE_MODERNDBS_CODEGEN_EXPRESSION_H

#include <memory>
#include "moderndbs/codegen/jit.h"
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>

namespace moderndbs {
    /// A value (can either be a signed (!) 64 bit integer or a double).
    using data64_t = uint64_t;

    struct Expression {

        /// A value type.
        enum class ValueType {
            INT64,
            DOUBLE
        };

        /// The constant type.
        ValueType type;

        /// Constructor.
        explicit Expression(ValueType type): type(type) {}

        /// Get the expression type.
        ValueType getType() { return type; }
        /// Evaluate the expression.
        virtual data64_t evaluate(const data64_t* args);
        /// Build the expression code.
        virtual llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args);
    };

    struct Constant: public Expression {
        /// The constant value.
        data64_t value;

        /// Constructor.
        explicit Constant(int64_t value)
            : Expression(ValueType::INT64), value(*reinterpret_cast<data64_t*>(&value)) {}
        /// Constructor.
        explicit Constant(double value)
            : Expression(ValueType::DOUBLE), value(*reinterpret_cast<data64_t*>(&value)) {}

        /// Evaluate the expression.
         data64_t evaluate(const data64_t* args) override{
            return value;
        };
        /// Build the expression code.

         llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args) override{
             if(type == ValueType::INT64){
                  return llvm::ConstantInt::getSigned(llvm::Type::getInt64Ty(builder.getContext()) , static_cast<int64_t >(value));
             } else {
                 return llvm::ConstantFP::get(builder.getContext(), llvm::APFloat(* reinterpret_cast<double *> (&value)));
             }
         };
    };

    struct Argument: public Expression {
        /// The argument index.
        uint64_t index;

        /// Constructor.
        explicit Argument(uint64_t index, ValueType type)
            : Expression(type), index(index) {}

        /// Evaluate the expression.
        data64_t evaluate(const data64_t* args) override{
            return args[index];
        };
        /// Build the expression code.
        llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args) override{
            if(Expression::ValueType::INT64 == this->type){
                llvm::Value *i = llvm::ConstantInt::getSigned(llvm::Type::getInt64Ty(builder.getContext()) , static_cast<int64_t >(index));
                llvm::Value* arg = builder.CreateGEP(args, i);
                return builder.CreateLoad(arg);
            } else{
                llvm::Value *i = llvm::ConstantInt::getSigned(llvm::Type::getInt64Ty(builder.getContext()) , static_cast<int64_t >(index));
                llvm::Value* arg = builder.CreateGEP(args, i);
                auto loaded =  builder.CreateLoad(arg);
                return builder.CreateFPCast(loaded, llvm::Type::getDoubleTy(builder.getContext()) );
            }


        };
    };

    struct Cast: public Expression {
        /// The child.
        Expression& child;
        /// The child type.
        ValueType childType;

        /// Constructor.
        Cast(Expression& child, ValueType type)
            : Expression(type), child(child) {
            childType = child.getType();
        }

        /// Evaluate the expression.
        data64_t evaluate(const data64_t* args) override{
            switch(childType){
                case ValueType::INT64:
                    return static_cast<uint64_t >(child.evaluate(args));
                case ValueType ::DOUBLE:
                    return *reinterpret_cast<double*>(child.evaluate(args));
            }
        };
        /// Build the expression code.
        llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args) override{
            if(type == ValueType::INT64){
                return   builder.CreateFPToSI(child.build(builder, args), llvm::Type::getInt64Ty(builder.getContext()));
            } else {
                return  builder.CreateSIToFP(child.build(builder, args), llvm::Type::getDoubleTy(builder.getContext()) );
            }
        };
    };

    struct BinaryExpression: public Expression {
        /// The left child.
        Expression& left;
        /// The right child.
        Expression& right;

        /// Constructor.
        explicit BinaryExpression(Expression& left, Expression& right)
            : Expression(ValueType::INT64), left(left), right(right) {
            assert(left.getType() == right.getType() && "the left and right type must equal");
            type = left.getType();
        }

        llvm::Value* impliciteCast(Expression& toCast, llvm::IRBuilder<>& builder, llvm::Value* args ){
            if(toCast.type != type){
                    if(type == ValueType::INT64){
                        return builder.CreateIntCast(toCast.build(builder, args), llvm::Type::getInt64Ty(builder.getContext()), true);
                    } else {
                        return builder.CreateFPCast(toCast.build(builder, args), llvm::Type::getDoubleTy(builder.getContext()) );
                    }
                }else{
                    return toCast.build(builder, args);
                }
        }
    };

    struct AddExpression: public BinaryExpression {
        /// Constructor
        AddExpression(Expression& left, Expression& right)
            : BinaryExpression(left, right) {}

        /// Evaluate the expression.
        data64_t evaluate(const data64_t* args) override{
            if(type == ValueType::INT64){
                return left.evaluate(args) + right.evaluate(args);
            }else {
                auto left_value = left.evaluate(args);
                auto right_value = right.evaluate(args);
                double left = *reinterpret_cast <double * > (&left_value);
                double right  = *reinterpret_cast <double * > (&right_value);
                double res = left + right;
                return *reinterpret_cast<data64_t *>(&res);
            }

        };
        /// Build the expression code.
        llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args) override{
            auto leftValue = impliciteCast(left, builder, args);
            auto rightValue =  impliciteCast(right, builder, args);
            if(Expression::ValueType::INT64 == this->type){
                return builder.CreateAdd( leftValue, rightValue, "add");
            }else {
                return builder.CreateFAdd( leftValue, rightValue, "fadd");
            }

        };
    };

    struct SubExpression: public BinaryExpression {
        /// Constructor
        SubExpression(Expression& left, Expression& right)
            : BinaryExpression(left, right) {}

        /// Evaluate the expression.
        data64_t evaluate(const data64_t* args) override{
            if(type == ValueType::INT64){
                return left.evaluate(args) - right.evaluate(args);
            }else {
                auto left_value = left.evaluate(args);
                auto right_value = right.evaluate(args);
                double left = *reinterpret_cast <double * > (&left_value);
                double right  = *reinterpret_cast <double * > (&right_value);
                double res = left - right;
                return *reinterpret_cast<data64_t *>(&res);
            }

        };
        /// Build the expression code.
        llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args) override{
            auto leftValue = impliciteCast(left, builder, args);
            auto rightValue =  impliciteCast(right, builder, args);
            if(Expression::ValueType::INT64 == this->type){
                return builder.CreateBinOp(llvm::Instruction::Sub, leftValue, rightValue, "sub");
            }else {
                return builder.CreateFSub(leftValue, rightValue, "sub");
            }
        };
    };

    struct MulExpression: public BinaryExpression {
        /// Constructor
        MulExpression(Expression& left, Expression& right)
            : BinaryExpression(left, right) {}

        /// Evaluate the expression.
        data64_t evaluate(const data64_t* args) override{
            if(type == ValueType::INT64){
                return left.evaluate(args) * right.evaluate(args);
            }else {
                auto left_value = left.evaluate(args);
                auto right_value = right.evaluate(args);
                double left = *reinterpret_cast <double * > (&left_value);
                double right  = *reinterpret_cast <double * > (&right_value);
                double res = left * right;
                return *reinterpret_cast<data64_t *>(&res);
            }

        };
        /// Build the expression code.
        llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args) override{
            auto leftValue = impliciteCast(left, builder, args);
            auto rightValue =  impliciteCast(right, builder, args);
            if(Expression::ValueType::INT64 == this->type){
                return builder.CreateBinOp(llvm::Instruction::Mul, leftValue, rightValue, "mul");
            }else {
                return builder.CreateFMul(leftValue, rightValue, "fMul");
            }
        };
    };

    struct DivExpression: public BinaryExpression {
        /// Constructor
        DivExpression(Expression& left, Expression& right)
            : BinaryExpression(left, right) {}

        /// Evaluate the expression.
        data64_t evaluate(const data64_t* args) override{

            if(type == ValueType::INT64){
                auto left_value = left.evaluate(args);
                auto right_value = right.evaluate(args);
                int64_t left = *reinterpret_cast <int64_t * > (&left_value);
                int64_t right  = *reinterpret_cast <int64_t * > (&right_value);
                int64_t res = left / right;
                return *reinterpret_cast<data64_t *>(&res);
            }else {
                auto left_value = left.evaluate(args);
                auto right_value = right.evaluate(args);
                double left = *reinterpret_cast <double * > (&left_value);
                double right  = *reinterpret_cast <double * > (&right_value);
                double res = left / right;
                return *reinterpret_cast<data64_t *>(&res);
            }

        };
        /// Build the expression code.
        llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args) override{
            auto leftValue = impliciteCast(left, builder, args);
            auto rightValue =  impliciteCast(right, builder, args);
            if(Expression::ValueType::INT64 == this->type){
                return builder.CreateBinOp(llvm::Instruction::SDiv, leftValue, rightValue, "div");
            }else {
                return builder.CreateFDiv(leftValue, rightValue, "fdiv");
            }
        };
    };

    struct ExpressionCompiler {
        /// The llvm context.
        llvm::LLVMContext& context;
        /// The llvm module.
        std::shared_ptr<llvm::Module> module;
        /// The jit.
        JIT jit;
        /// The compiled function.
        data64_t (*fnPtr)(data64_t* args);

        /// Constructor.
        explicit ExpressionCompiler(llvm::LLVMContext& context);

        /// Compile an expression.
        void compile(Expression& expression);
        /// Run a previously compiled expression
        data64_t run(data64_t* args);

        /// Dump the llvm module
        void dump();
    };

}  // namespace moderndbs

#endif
