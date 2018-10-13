# ---------------------------------------------------------------------------
# MODERNDBS
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

set(TEST_CC
    test/btree_test.cc
    test/buffer_manager_test.cc
    test/external_sort_test.cc
)

# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

add_executable(tester test/tester.cc ${TEST_CC})
target_link_libraries(tester moderndbs gtest gmock Threads::Threads)

enable_testing()
add_test(moderndbs tester)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_clang_tidy_target(lint_test "${TEST_CC}")
add_dependencies(lint_test gtest)
list(APPEND lint_targets lint_test)
