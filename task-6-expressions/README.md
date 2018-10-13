# Expressions

Implement an expression interpreter and an expression compiler for your database.
Your implementation should support the following type of expressions:

* Constants: Constants are either 8 byte float values or 8 byte signed integers.
* Arguments: Refers to a fixed index of an argument array storing 8 byte values (either floats or signed integers).
* Cast: Cast an expression from one type to another.
* Addition: Add two expressions.
* Subtraction: Subtract two expressions.
* Multiplication: Multiply two expressions.
* Division: Divide two expressions.

Add your implementation to the files `include/moderndbs/codegen/expression.h` and `src/moderndbs/codegen/expression.cc`.

For the expression interpreter, each expression type should implement the method `virtual data64_t evaluate(const data64_t* args)` which evaluates the expression tree given an argument array.

For the expression compiler, each expression type should implement the method `virtual llvm::Value* build(llvm::IRBuilder<>& builder, llvm::Value* args)` which generates code for the expression using the LLVM framework.
The expression compiler should then implement the method `void compile(Expression& expression)` that compiles a function `data64_t (*fnPtr)(data64_t* args)` for the expression tree.
Use the class `include/moderndbs/codegen/jit.h` to simplify the LLVM module compilation significantly.
Refer to the simple [printf example](src/codegen/printf.cc) for information on how to generate a LLVM module.

You don't need to implement implicit type casts.
The subtrees of a binary expression (Add, Sub, Mul, Div) are guaranteed to have the same type, which could either be a float or an integer.

Your implementation should pass all tests in `test/expression_test.cc`.

Run the benchmark `bench/bm_expression.cc` to compare the efficiency of both approaches.
