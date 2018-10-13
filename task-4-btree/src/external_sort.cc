#include "moderndbs/external_sort.h"
#include <algorithm>
#include <cassert>
#include <vector>
#include "moderndbs/file.h"


namespace moderndbs {

namespace {

/// Reads `count` values from the file at the given offset.
void read_values(File& file, size_t offset, size_t count, uint64_t* values) {
    file.read_block(
        offset,
        count * sizeof(uint64_t),
        reinterpret_cast<char*>(values)
    );
}


/// Writes all values of an array of uint64_t values to the file at the given
/// offset.
void write_values(size_t count, const uint64_t* values, File& file, size_t offset) {
    file.write_block(
        reinterpret_cast<const char*>(values),
        offset,
        count * sizeof(uint64_t)
    );
}


/// Moves count values in the file starting at from_offset to the area starting
/// at to_offset. Uses an array of uint64_t values with buffer_size elements as
/// buffer.
void move_values(
    size_t count,
    uint64_t* buffer,
    size_t buffer_size,
    File& file,
    size_t from_offset,
    size_t to_offset
) {
    size_t i;
    for (i = 0; i + buffer_size <= count; i += buffer_size) {
        read_values(file, from_offset + i * sizeof(uint64_t), buffer_size, buffer);
        write_values(buffer_size, buffer, file, to_offset + i * sizeof(uint64_t));
    }
    if (i < count) {
        size_t num_values = count - i;
        read_values(file, from_offset + i * sizeof(uint64_t), num_values, buffer);
        write_values(num_values, buffer, file, to_offset + i * sizeof(uint64_t));
    }
}


/// Stores all information needed to track a run in an n-way merge.
struct MergeRun {
    /// Offset at which the data for this run starts.
    size_t offset;
    /// Offset at which the data for this run ends.
    size_t offset_end;
    /// Runs are only partially loaded into memory. This variable tracks the
    /// offset at which values should be loaded next.
    size_t next_offset;
    /// Pointer to an (in-memory) array that holds the values of the partial
    /// run.
    uint64_t* data;
    /// Index into the data array for the value that is currently being
    /// considered in the merge phase.
    size_t data_index;
    /// Size of the data array.
    size_t data_size;

    /// Returns the actual value that should be used for comparison in the
    /// merge phase.
    uint64_t get_value() const {
        assert(data_index < data_size);
        return data[data_index];
    }
};


/// This is the comparator function used for the make/push/pop_heap functions
/// of the standard library.
bool heap_cmp(const MergeRun& r1, const MergeRun& r2) {
    // > must be used here instead of < because the heap functions model a
    // max-heap. So when we use >, the smallest value will be at the top of the
    // heap.
    return r1.get_value() > r2.get_value();
}

}  // namespace


void external_sort(File& input, size_t num_values, File& output, size_t mem_size) {
    if (num_values == 0) {
        output.resize(0);
        return;
    }
    size_t num_mem_values = mem_size / sizeof(uint64_t);
    if (num_values <= num_mem_values) {
        // We can sort the entire file in memory.
        std::vector<uint64_t> values(num_values);
        read_values(input, 0, num_values, values.data());
        std::sort(values.begin(), values.end());
        output.resize(num_values * sizeof(uint64_t));
        write_values(num_values, values.data(), output, 0);
        return;
    }

    // First sort all individual runs, i.e. load num_mem_values from the input
    // file, sort them, and append them to the output file.
    size_t num_runs = 0;
    size_t remaining_values = num_mem_values;
    size_t output_size = num_values * sizeof(uint64_t);
    // We will use parts of the output file as temporary storage of the merge
    // runs. The actual output will be written at the beginning of the file
    // and needs output_size bytes. To store the merge runs we need output_size
    // bytes more so the output file needs to be 2*output_size bytes large.
    output.resize(2 * output_size);
    {
        std::vector<uint64_t> values(num_mem_values);
        // Sort blocks that contain exactly num_mem_values
        size_t values_index;
        for (
            values_index = 0;
            values_index + num_mem_values <= num_values;
            values_index += num_mem_values
        ) {
            read_values(input, values_index * sizeof(uint64_t), num_mem_values, values.data());
            std::sort(values.data(), values.data() + num_mem_values);
            write_values(
                num_mem_values,
                values.data(),
                output,
                output_size + values_index * sizeof(uint64_t)
            );
            ++num_runs;
        }
        // Sort trailing block that can have less than num_mem_values
        if (values_index < num_values) {
            remaining_values = num_values - values_index;
            read_values(input, values_index * sizeof(uint64_t), remaining_values, values.data());
            std::sort(values.data(), values.data() + remaining_values);
            write_values(
                remaining_values,
                values.data(),
                output,
                output_size + values_index * sizeof(uint64_t)
            );
            ++num_runs;
        }
    }
    // If we only had one run, we would have sorted it in-place already.
    assert(num_runs > 1);

    // Now do an n-way merge over all blocks. We want to ensure that we don't
    // use more than mem_size bytes of memory to merge the runs. We will use a
    // vector of MergeRun objects to track the indexes into the runs. The
    // remaining space will be used to store the actual elements. We know the
    // number of runs, it is is stored in the variable num_runs. So for this we
    // need
    //          num_runs * sizeof(MergeRun)
    // space. Each run should have an equal number of values stored in memory.
    // So we introduce a variable elements_per_run. To store the values for all
    // runs we then need
    //          num_runs * elements_per_run * sizeof(uint64_t)
    // space. Additionally we need space for the merged values as we don't want
    // to write them individually to the file. For this we will also use
    //          elements_per_run * sizeof(uint64_t)
    // bytes. So in total we have:
    //          mem_size = num_runs * sizeof(MergeRun) +
    //                     num_runs * elements_per_run * sizeof(uint64_t) +
    //                     elements_per_run * sizeof(uint64_t)
    // All variables but elements_per_run are known so we calculate it here:
    //          elements_per_run =
    //                  (mem_size - num_runs * sizeof(MergeRun)) /
    //                  ((num_runs + 1) * sizeof(uint64_t))
    size_t elements_per_run;
    size_t elements_per_input_run = num_mem_values;
    // ########################################################################
    // # The entire following while loop only deals with the edge cases where #
    // # mem_size is too small to efficiently do an n-way merge. It is ok to  #
    // # omit this as mem_size usually is large enough.                       #
    // ########################################################################
    while (true) {
        // It can happen that we did a 2-way merge often enough so that only
        // one run is left. In that case all values are sorted, so just copy
        // them to the beginning of the output file and return.
        if (num_runs == 1) {
            std::vector<uint64_t> values(num_mem_values);
            move_values(num_values, values.data(), num_mem_values, output, output_size, 0);
            output.resize(output_size);
            return;
        }
        if (num_runs * sizeof(MergeRun) < mem_size) {
            elements_per_run = (
                (mem_size - num_runs * sizeof(MergeRun)) / ((num_runs + 1) * sizeof(uint64_t))
            );
            // As we want the n-way merge to be efficient, each run should have
            // at least 64 elements as otherwise we would have to read small
            // blocks very often. The exact value of 64 is chosen because disks
            // typically can't read blocks smaller than 512 bytes.
            if (elements_per_run >= 64) {
                break;
            }
        }
        // We can't even store all MergeRun objects in memory, or
        // elements_per_run is smaller than 20 so we need to reduce the number
        // of runs first. For this we do a 2-way merge on all adjacent runs and
        // repeat this until elements_per_run is larger than or equal to 20.
        size_t new_num_runs = 0;
        // We will split the available memory into three equally sized parts:
        // Storage for the merged values and storage for the values of the
        // first and second run.
        size_t elements_per_2way_run = num_mem_values / 3;
        // This assertion only fails if mem_size is so small that we can't even
        // store three values.
        assert(elements_per_2way_run > 0);
        std::vector<uint64_t> values_storage(num_mem_values);
        uint64_t* first_run_values = &values_storage[0];
        uint64_t* second_run_values = &values_storage[elements_per_2way_run];
        uint64_t* output_values = &values_storage[2 * elements_per_2way_run];
        for (size_t i = 0; i + 1 < num_runs; i += 2) {
            // The first output_size bytes of the output file are currently
            // unused so we can use them as temporary storage for the 2-way
            // merge.
            size_t first_run_offset = (
                output_size + i * elements_per_input_run * sizeof(uint64_t)
            );
            size_t second_run_offset = (
                first_run_offset + elements_per_input_run * sizeof(uint64_t)
            );
            size_t offset_end;
            size_t num_values_in_second_run;
            if (i + 2 == num_runs) {
                // We have an even number of runs with the last run potentially
                // having less than elements_per_input_run values. After
                // merging the last two runs we will have a new last run with a
                // different amount of values so we need to update
                // remaining_values here.
                offset_end = second_run_offset + remaining_values * sizeof(uint64_t);
                num_values_in_second_run = remaining_values;
                remaining_values = elements_per_input_run + remaining_values;
            } else {
                offset_end = second_run_offset + elements_per_input_run * sizeof(uint64_t);
                num_values_in_second_run = elements_per_2way_run;
            }
            read_values(
                output,
                first_run_offset,
                elements_per_2way_run,
                first_run_values
            );
            size_t read_values_from_second_run = std::min(
                elements_per_2way_run, num_values_in_second_run
            );
            read_values(
                output,
                second_run_offset,
                read_values_from_second_run,
                second_run_values
            );
            size_t next_first_offset = (
                first_run_offset + elements_per_2way_run * sizeof(uint64_t)
            );
            size_t next_second_offset = (
                second_run_offset + read_values_from_second_run * sizeof(uint64_t)
            );
            size_t first_index = 0;
            size_t first_size = elements_per_2way_run;
            size_t second_index = 0;
            size_t second_size = read_values_from_second_run;
            size_t values_index = 0;
            size_t output_offset = 0;
            while (true) {
                if (first_index >= first_size && second_index >= second_size) {
                    // When there are no values left in the first or second
                    // run, we are done. Write the remaining values to the
                    // output if there are any.
                    if (values_index > 0) {
                        write_values(
                            values_index,
                            output_values,
                            output,
                            output_offset
                        );
                        output_offset += values_index * sizeof(uint64_t);
                        values_index = 0; // NOLINT
                    }
                    break;
                }
                if (first_index >= first_size) {
                    // This only happens when there are no values left in the
                    // first run. So unconditionally take the next value from
                    // the second run.
                    output_values[values_index] = second_run_values[second_index];
                    ++second_index;
                } else if (second_index >= second_size) {
                    // Vice-versa for the second run.
                    output_values[values_index] = first_run_values[first_index];
                    ++first_index;
                } else {
                    // The actual comparing and merging is done here.
                    uint64_t first_value = first_run_values[first_index];
                    uint64_t second_value = second_run_values[second_index];
                    if (first_value < second_value) {
                        output_values[values_index] = first_value;
                        ++first_index;
                    } else {
                        output_values[values_index] = second_value;
                        ++second_index;
                    }
                }
                ++values_index;
                // Now check if any of the indexes (first_index, second_index,
                // values_index) are at the end.
                if (values_index >= elements_per_2way_run) {
                    // Write merged elements to output.
                    write_values(
                        elements_per_2way_run,
                        output_values,
                        output,
                        output_offset
                    );
                    output_offset += elements_per_2way_run * sizeof(uint64_t);
                    values_index = 0;
                }
                if (
                    first_index >= first_size &&
                    // When this condition is false, there are no values
                    // remaining in the first run, so do nothing.
                    next_first_offset < second_run_offset
                ) {
                    // Load more values from the first run.
                    size_t next_offset = std::min(
                        next_first_offset + elements_per_2way_run * sizeof(uint64_t),
                        second_run_offset
                    );
                    first_size = (next_offset - next_first_offset) / sizeof(uint64_t);
                    read_values(
                        output,
                        next_first_offset,
                        first_size,
                        first_run_values
                    );
                    next_first_offset = next_offset;
                    first_index = 0;
                }
                if (
                    second_index >= second_size &&
                    // Same as above but for the second run.
                    next_second_offset < offset_end
                ) {
                    // Load more values from the second run.
                    size_t next_offset = std::min(
                        next_second_offset + elements_per_2way_run * sizeof(uint64_t),
                        offset_end
                    );
                    second_size = (next_offset - next_second_offset) / sizeof(uint64_t);
                    read_values(
                        output,
                        next_second_offset,
                        second_size,
                        second_run_values
                    );
                    next_second_offset = next_offset;
                    second_index = 0;
                }
            }
            ++new_num_runs;
            // The merged run currently lies at the beginning of the file. Copy
            // it back to the location of the two runs we merged.
            move_values(
                output_offset / sizeof(uint64_t),
                values_storage.data(),
                num_mem_values,
                output,
                0,
                first_run_offset
            );
        }
        if (num_runs % 2 != 0) {
            // We did a 2-way merge on an odd number of runs so there is still
            // a single run left which will be left untouched.
            ++new_num_runs;
        }
        // After every 2-way merge the runs double in size.
        elements_per_input_run *= 2;
        num_runs = new_num_runs;
    }
    // ########################################################################
    // # End of handling edge cases. The more interesting part of the         #
    // # external sort starts below.                                          #
    // ########################################################################

    std::vector<MergeRun> runs;
    runs.reserve(num_runs);
    std::vector<uint64_t> values(num_runs * elements_per_run);
    // Load all runs but the last one. They always have elements_per_input_run
    // elements.
    for (size_t i = 0; i < num_runs - 1; ++i) {
        uint64_t* data_ptr = &values[i * elements_per_run];
        size_t offset = output_size + i * elements_per_input_run * sizeof(uint64_t);
        read_values(output, offset, elements_per_run, data_ptr);
        runs.push_back({
            offset,
            offset + elements_per_input_run * sizeof(uint64_t),
            offset + elements_per_run * sizeof(uint64_t),
            data_ptr,
            0,
            elements_per_run
        });
    }
    // The last run has only remaining_values elements, so write it out
    // explicitly instead of including it into the for loop above.
    {
        uint64_t* data_ptr = &values[(num_runs - 1) * elements_per_run];
        size_t offset = output_size + (num_runs - 1) * elements_per_input_run * sizeof(uint64_t);
        size_t num_elements = std::min(remaining_values, elements_per_run);
        read_values(output, offset, num_elements, data_ptr);
        runs.push_back({
            offset,
            offset + remaining_values * sizeof(uint64_t),
            offset + num_elements * sizeof(uint64_t),
            data_ptr,
            0,
            num_elements
        });
    }

    size_t output_offset = 0;
    std::vector<uint64_t> output_block(elements_per_run);
    size_t output_block_index = 0;
    std::make_heap(runs.begin(), runs.end(), heap_cmp);
    while (true) {
        std::pop_heap(runs.begin(), runs.end(), heap_cmp);
        auto& min_run = runs.back();
        output_block[output_block_index] = min_run.get_value();
        ++output_block_index;
        if (output_block_index >= elements_per_run) {
            // The output block is full so write it to the output file.
            write_values(elements_per_run, output_block.data(), output, output_offset);
            output_offset += elements_per_run * sizeof(uint64_t);
            output_block_index = 0;
        }
        ++min_run.data_index;
        if (min_run.data_index >= min_run.data_size) {
            // Load more data from the input.
            if (min_run.next_offset >= min_run.offset_end) {
                // There is no more data, so don't reinsert this run into the
                // heap.
                runs.pop_back();
                // If there are no runs left, we are done. Write the remaining
                // elements to the output.
                if (runs.empty()) {
                    if (output_block_index > 0) {
                        write_values(
                            output_block_index, output_block.data(), output, output_offset
                        );
                    }
                    break;
                }
            } else {
                size_t num_elements = std::min(
                    elements_per_run, (min_run.offset_end - min_run.next_offset) / sizeof(uint64_t)
                );
                read_values(output, min_run.next_offset, num_elements, min_run.data);
                min_run.next_offset += num_elements * sizeof(uint64_t);
                min_run.data_index = 0;
                min_run.data_size = num_elements;
                // We loaded more data for the run and now the run has to be
                // inserted back into the heap.
                std::push_heap(runs.begin(), runs.end(), heap_cmp);
            }
        } else {
            // By increasing min_run.data_index the value for the run changed,
            // so it has to be inserted into the heap again.
            std::push_heap(runs.begin(), runs.end(), heap_cmp);
        }
    }

    // Resize the file so that it includes only the actual output. This
    // discards the temporary space that was used to store the merge runs.
    output.resize(output_size);
}

}  // namespace moderndbs
