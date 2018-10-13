# Buffer Manager

Implement a buffer manager that uses the 2Q replacement strategy. The
implementation should take 64 bit page ids and return pages that can be locked
(shared or exclusive), read and written. The 64 bits of a page id are divided
into a segment id and a segment page id. The most significant 16 bits are the
segment id and the least significant 48 bits are the segment page id. When a
page is written to disk, it must be stored in a file whose filename is the
segment id. Your implementation should support different page and buffer sizes
and be able to fix and unfix pages concurrently from different threads. It may
be useful to use the class `moderndbs::File` from `include/moderndbs/file.h`.
Note that only `File.read_block()` and `File.write_block()` are thread-safe.
Also, using multiple `File` objects that refer to the same file concurrently
also leads to unexpected results. Also, to ensure thread-safety you can use
`std::mutex` and `std::shared_mutex`.

Add your implementation to the files `include/moderndbs/buffer_manager.h` and
`src/buffer_manager.cc`. There you can find the classes `BufferFrame` which
represents a page and `BufferManager` which represents the buffer manager. All
definitions for the methods that you should implement are provided. You can add
new member functions and member variables to either class as needed.

Your implementation should pass all of tests starting with `BufferManagerTest`.
Tests whose name contains `Multithread` are executed with a timeout of 30s.
This is done so that a deadlock does not block the tester. Additionally, we
will of course check your code for correctness. Passing the tests is a
requirement but does not automatically mean that you will get full points.

We will also grade your implementation based on its efficiency. You don't have
to optimize on assembly level or try to improve the runtime by microseconds.
Instead, we want you to especially take care when holding latches. They should
be held as short as possible to enable better concurrency. Most importantly,
latches should not be held while reading from or writing to disk as this
essentially blocks all other threads for the duration of the I/O operation
which can be many milliseconds.

Additionally, your code will be checked for code quality. You can run the
checks for that by using the `lint` CMake target (e.g. by running `make lint`).
When your code does not pass those checks, we may deduct (a small amount of)
points.


Name: Franziska Geiger
Matriculation number: 03703434