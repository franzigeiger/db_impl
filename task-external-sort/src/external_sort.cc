#include "moderndbs/external_sort.h"
#include "moderndbs/file.h"
#include <iostream>
#include <algorithm>
#include "file/posix_file.cc"
#include <queue>
#include <vector>
#include <cmath>

namespace moderndbs {

struct element{
    uint64_t  value;
    size_t run;
};

void external_sort(File& input, size_t num_values, File& output, size_t mem_size) {
    if(num_values > 0 && mem_size >0) {
        uint64_t k = ceil((static_cast<double>(num_values) * static_cast<double>(sizeof(uint64_t))) / static_cast<double>(mem_size));
        if(k == 0){
            k=1;
        }
        size_t nums_per_run = mem_size / sizeof(uint64_t);
        size_t read_offset = 0;
        auto block = std::make_unique<uint64_t []>(nums_per_run);
        size_t left_num_values = num_values;

        auto *fileRegistry = new std::unique_ptr<File>[k];

        //sort k partitions
        for (size_t i = 0; i < k; i++) {
            fileRegistry[i] = PosixFile::make_temporary_file();

            size_t to_load_in_this_run = 0;
            if (left_num_values < nums_per_run) {
                to_load_in_this_run = left_num_values;
                left_num_values = 0;
            } else {
                to_load_in_this_run = nums_per_run;
                left_num_values -= nums_per_run;
            }

            input.read_block(read_offset, to_load_in_this_run * sizeof(uint64_t),
                             reinterpret_cast<char * >(block.get()));

            if(to_load_in_this_run>1) {
                std::sort(block.get(), block.get() + to_load_in_this_run );
            }

            fileRegistry[i]->resize(to_load_in_this_run * sizeof(uint64_t));
            fileRegistry[i]->write_block( reinterpret_cast<char * >(block.get()), 0,
                                         to_load_in_this_run * sizeof(uint64_t));
            read_offset += to_load_in_this_run * sizeof(uint64_t);

        }

        block.release();

        size_t loadSize = (nums_per_run / 2 / k) ;
        size_t itemsLeft [k];
        size_t elementsFromBlock[k];

        auto cmp = [](element left, element right) {
            return left.value > right.value;
        };

        std::priority_queue<element, std::vector<element>, decltype(cmp)> pq(cmp);
        auto loadBlock = std::make_unique<uint64_t []>(nums_per_run);
        //initially load priority queue
        for (size_t i = 0; i < k; i++) {
            size_t fileSize = fileRegistry[i]->size() / sizeof(uint64_t);
            size_t sizeToUse = fileSize < loadSize ? fileSize : loadSize;

            fileRegistry[i]->read_block(0, sizeToUse * sizeof(uint64_t), reinterpret_cast<char * >(loadBlock.get()));

            itemsLeft[i] = fileSize - sizeToUse;
            elementsFromBlock[i] = sizeToUse;
            for (size_t m = 0; m < sizeToUse; m++) {
                struct element e = {.value = loadBlock.get()[m], .run = i};
                pq.push(e);
            }
        }

        size_t vectorMax = nums_per_run / 2;
        std::vector<uint64_t> outputBlock;
        //save elements to output file blockwise with automatically load from file registry
        while (!pq.empty()) {

            struct element e = pq.top();
            outputBlock.push_back(e.value);
            elementsFromBlock[e.run]--;

            //load next range from temp file
            if (elementsFromBlock[e.run] == 0 && itemsLeft[e.run] != 0) {
                size_t fileSize = fileRegistry[e.run]->size() / sizeof(uint64_t);
                size_t startItem = fileSize - itemsLeft[e.run];
                size_t itemSize = loadSize > itemsLeft[e.run] ? itemsLeft[e.run] : loadSize;


                fileRegistry[e.run]->read_block( startItem * sizeof(uint64_t), itemSize * sizeof(uint64_t), reinterpret_cast<char * >(loadBlock.get()));

                for (size_t n = 0; n < itemSize; n++) {
                    struct element e1 = {.value = loadBlock.get()[n], .run = e.run};
                    pq.push(e1);
                }
                elementsFromBlock[e.run] = itemSize;
                itemsLeft[e.run] = itemsLeft[e.run] - itemSize;
            }

            //write to outputfile
            if (outputBlock.size() == vectorMax) {
                uint64_t *outputArray = &outputBlock[0];
                size_t oldSize= output.size();
                output.resize(oldSize + vectorMax * sizeof(uint64_t));
                output.write_block(reinterpret_cast<char *>(outputArray), oldSize ,
                                   vectorMax * sizeof(uint64_t));
                outputBlock.clear();
            }

            pq.pop();

        }

        //write the last not complete block to output file
        uint64_t *outputArray = &outputBlock[0];
        size_t oldSize= output.size();
        output.resize(oldSize + (outputBlock.size() * sizeof(uint64_t)));
        output.write_block(reinterpret_cast<char *>(outputArray),oldSize,
                           outputBlock.size() * sizeof(uint64_t));
        delete[] fileRegistry;
    }
}

}  // namespace moderndbs
