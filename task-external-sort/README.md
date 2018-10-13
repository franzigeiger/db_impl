# External Sort

Implement the external sort algorithm. It should take an input file that
contains unsigned 64 bit integers, the number of values the input file
contains, an output file, and a memory size value. In the end the output file
should contain all values from the input file in ascending order. Your
implementation must not use more bytes of heap memory than the number given as
memory size. You should use a k-way-merge that merges k individual runs which
each fit into memory. To sort runs that fit into memory, you can use
`std::sort()`. For the k-way-merge it may be useful to use
`std::priority_queue` or the heap functions `std::make_heap()`,
`std::push_heap()`, and `std::pop_heap()`.

Add your implementation to the file `src/external_sort.cc`. The signature for
the `external_sort()` function is given, do not change it. It may be useful to
look into `include/moderndbs/file.h` to understand the file API.

You can test your implementation by using the `external_sort` tool (see
`external_sort --help` for more information) or the tests in
`test/external_sort_test.cc`. To run them, compile the project and then execute
the `tester` binary.

Your implementation should pass all of the tests starting with
`ExternalSortTest`. Tests starting with `AdvancedExternalSortTest` are optional
and are not required to pass to get full points for this task. We will of
course check your code for correctness. Passing the normal tests is a
requirement but does not automatically mean that you will get full points.

Additionally, your code will be checked for code quality. You can run the
checks for that by using the `lint` CMake target (e.g. by running `make lint`).
When your code does not pass those checks, we may deduct (a small amount of)
points.


Name: Franziska Geiger
Matriculation number: 03703434