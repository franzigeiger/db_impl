#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <utility>
#include <random>
#include <vector>
#include <gtest/gtest.h>
#include "moderndbs/external_sort.h"
#include "moderndbs/file.h"


namespace moderndbs {

class TestFileError
: public std::exception {
private:
    const char* message;

public:
    explicit TestFileError(const char* message) : message(message) {}

    ~TestFileError() override = default;

    const char* what() const noexcept override {
        return message;
    }
};


class TestFile
: public File {
private:
    Mode mode;
    std::vector<char> file_content;

public:
    explicit TestFile(Mode mode = WRITE) : mode(mode) {}

    explicit TestFile(std::vector<char>&& file_content, Mode mode = READ)
    : mode(mode), file_content(std::move(file_content)) {}

    TestFile(const TestFile&) = default;
    TestFile(TestFile&&) = default;

    ~TestFile() override = default;

    TestFile& operator=(const TestFile&) = default;
    TestFile& operator=(TestFile&&) = default;

    std::vector<char>& get_content() {
        return file_content;
    }

    Mode get_mode() const override {
        return mode;
    }

    size_t size() const override {
        return file_content.size();
    }

    void resize(size_t new_size) override {
        if (mode == READ) {
            throw TestFileError{"trying to resize a read only file"};
        }
        file_content.resize(new_size);
    }

    void read_block(size_t offset, size_t size, char* block) override {
        if (offset + size > file_content.size()) {
            throw TestFileError{"trying to read past end of file"};
        }
        std::memcpy(block, file_content.data() + offset, size);
    }

    void write_block(const char* block, size_t offset, size_t size) override {
        if (mode == READ) {
            throw TestFileError{"trying to write to a read only file"};
        }
        if (offset + size > file_content.size()) {
            throw TestFileError{"trying to write past end of file"};
        }
        std::memcpy(file_content.data() + offset, block, size);
    }
};

}  // namespace moderndbs


namespace {

static_assert(sizeof(uint64_t) == 8, "sizeof(uint64_t) must be 8");


constexpr size_t MEM_1KiB = 1ul << 10;
constexpr size_t MEM_1MiB = 1ul << 20;


// NOLINTNEXTLINE
TEST(ExternalSortTest, EmptyFile) {
    moderndbs::TestFile input{moderndbs::File::READ};
    moderndbs::TestFile output;
    moderndbs::external_sort(input, 0, output, MEM_1MiB);
    ASSERT_EQ(0, output.size());
}


// NOLINTNEXTLINE
TEST(ExternalSortTest, OneValue) {
    std::vector<char> file_content(8);
    uint64_t input_value = 0xabab42f00f00;
    std::memcpy(file_content.data(), &input_value, 8);
    moderndbs::TestFile input{std::move(file_content)};
    moderndbs::TestFile output;

    moderndbs::external_sort(input, 1, output, MEM_1MiB);

    ASSERT_EQ(8, output.size());
    uint64_t output_value = 0;
    std::memcpy(&output_value, output.get_content().data(), 8);
    ASSERT_EQ(input_value, output_value);
}


std::pair<std::vector<uint64_t>, moderndbs::TestFile> make_random_numbers(
    size_t count
) {
    std::mt19937_64 engine{0};
    std::uniform_int_distribution<uint64_t> distr;
    std::vector<uint64_t> values;
    values.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        uint64_t value = distr(engine);
        values.push_back(value);
    }
    std::vector<char> file_content(count * 8);
    std::memcpy(file_content.data(), values.data(), count * 8);
    return std::make_pair(
        std::move(values),
        moderndbs::TestFile{std::move(file_content)}
    );
}


std::vector<uint64_t> get_file_values(moderndbs::TestFile file) {
    auto& content = file.get_content();
    std::vector<uint64_t> values(content.size() / 8);
    std::memcpy(values.data(), content.data(), content.size());
    return values;
}


class ExternalSortParametrizedTest
: public ::testing::TestWithParam<std::pair<size_t, size_t>> {
};


// NOLINTNEXTLINE
TEST_P(ExternalSortParametrizedTest, SortDescendingNumbers) {
    auto [mem_size, num_values] = GetParam();
    std::vector<uint64_t> expected_values(num_values);
    std::vector<char> file_content(num_values * 8);
    {
        std::vector<uint64_t> file_values(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            expected_values[i] = i + 1;
            file_values[i] = num_values - i;
        }
        std::memcpy(file_content.data(), file_values.data(), num_values * 8);
    }
    moderndbs::TestFile input{std::move(file_content)};
    moderndbs::TestFile output;

    moderndbs::external_sort(input, num_values, output, mem_size);

    ASSERT_EQ(num_values * 8, output.size());
    auto output_values = get_file_values(output);
    ASSERT_EQ(expected_values, output_values);
}


// NOLINTNEXTLINE
TEST_P(ExternalSortParametrizedTest, SortRandomNumbers) {
    auto [mem_size, num_values] = GetParam();
    auto [expected_values, input] = make_random_numbers(num_values);
    std::sort(expected_values.begin(), expected_values.end());
    moderndbs::TestFile output;
    moderndbs::external_sort(input, num_values, output, mem_size);
    ASSERT_EQ(num_values * 8, output.size());
    auto output_values = get_file_values(output);
    ASSERT_EQ(expected_values, output_values);
}


// NOLINTNEXTLINE
TEST_P(ExternalSortParametrizedTest, SortEqualNumbers) {
    auto [mem_size, num_values] = GetParam();
    std::vector<uint64_t> expected_values(num_values, 0xabab42f00f00);
    std::vector<char> file_content(num_values * 8);
    std::memcpy(file_content.data(), expected_values.data(), num_values * 8);
    moderndbs::TestFile input{std::move(file_content)};
    moderndbs::TestFile output;

    moderndbs::external_sort(input, num_values, output, mem_size);

    ASSERT_EQ(num_values * 8, output.size());
    auto output_values = get_file_values(output);
    ASSERT_EQ(expected_values, output_values);
}


INSTANTIATE_TEST_CASE_P(
    ExternalSortTest,
    ExternalSortParametrizedTest,
    ::testing::Values(
        // All values fit in memory:
        std::make_pair(MEM_1KiB, 3),
        std::make_pair(MEM_1KiB, 40),
        std::make_pair(MEM_1KiB, 128),
        std::make_pair(MEM_1MiB, 100000),
        // n-way merge required:
        std::make_pair(MEM_1KiB, 129),
        std::make_pair(MEM_1KiB, 997),
        std::make_pair(MEM_1KiB, 1024),
        std::make_pair(MEM_1MiB, 200000)
    )
);


INSTANTIATE_TEST_CASE_P(
    AdvancedExternalSortTest,
    ExternalSortParametrizedTest,
    ::testing::Values(
        // In these test cases the number of runs for an n-way merge is larger
        // than the number of values that fit into memory, so another strategy
        // is needed.
        std::make_pair(MEM_1KiB, 128 * MEM_1KiB + 1),
        std::make_pair(MEM_1KiB, MEM_1MiB)
    )
);

}  // namespace
