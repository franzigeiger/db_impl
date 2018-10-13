#include "moderndbs/buffer_manager.h"
#include "file/posix_file.cc"
#include <thread>
#include <iostream>

namespace moderndbs {

    char* BufferFrame::get_data() {
        return reinterpret_cast<char*>(data.data());
    }

    BufferManager::BufferManager(size_t page_size, size_t page_count) {
        maxPage= page_count ;
        pageCounter = 0;
        pageSize = page_size;
    }


    BufferManager::~BufferManager() {
        for(auto & i : fifoQueue) {
            if(i->dirty==true){
                saveFrame(*i);
            }
            // delete fifoQueue[i];
        }
        fifoQueue.clear();
        for(auto & i : lruQueue) {
            if(i->dirty==true){
                saveFrame(*i);
            }
            //delete fifoQueue[i];
        }
        lruQueue.clear();

    }

    BufferFrame::BufferFrame(size_t pageSize) {
        data = std::vector<uint64_t>(pageSize);

    }

    void BufferManager::lockFrame(BufferFrame* frame, bool exclusive){
        if(!exclusive){
            frame->mutex_.lock_shared();
        } else {
            frame->mutex_.lock();
        }
        frame->exclusive = exclusive;
    }

    void BufferManager::unlockFrame(BufferFrame* frame, bool exclusive){
        frame->useCounter--;
        if(!exclusive){
            frame->mutex_.unlock_shared();
        } else {
            frame->mutex_.unlock();
        }
    }

    BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
        fifoMutex.lock();
        lruMutex.lock();
        //first check if page is in lru queue, then we don't have to do anything else.
        BufferFrame* frame = nullptr;
        if(pageInLruQueue(&frame, page_id, exclusive)){
            fifoMutex.unlock();
            lruMutex.unlock();
            return *frame;
        }

        //search for pageid in fifoQueue, if it is found, save page in lruQueue and delete from fifo.
        if(pageInFifoQueue(&frame, page_id, exclusive)){
            fifoMutex.unlock();
            lruMutex.unlock();
            return *frame;
        }

        BufferFrame* toDelete = nullptr;

        if(pageCounter == maxPage){

            bool couldDelete = false;
            for(size_t i =0; i< fifoQueue.size() ; i++) {
                if(fifoQueue[i]->useCounter == 0){
                    couldDelete = true;
                    if(fifoQueue[i]->dirty==true){
                        toDelete =fifoQueue[i];
                    }
                    fifoQueue.erase(fifoQueue.begin() + i);
                    break;
                }
            }
            if(!couldDelete){
                for(size_t i =0; i< lruQueue.size() ; i++) {
                    if(lruQueue[i]->useCounter == 0){
                        couldDelete = true;
                        if(lruQueue[i]->dirty==true){
                            toDelete = lruQueue[i];
                        }
                        lruQueue.erase(lruQueue.begin() + i);
                        break;
                    }
                }
                if(!couldDelete){
                    fifoMutex.unlock();
                    lruMutex.unlock();
                    throw buffer_full_error{};
                }
            }

        } else {
            pageCounter++;
        }

        auto* newFrame = new BufferFrame(pageSize);

        newFrame->pageid=page_id;
        newFrame->useCounter=2;
        newFrame->dirty=false;
        newFrame->exclusive=exclusive;
        lockFrame(newFrame, true);

        fifoQueue.push_back(newFrame);

        fifoMutex.unlock();
        lruMutex.unlock();

        if(toDelete != nullptr){
            saveFrame(*toDelete);
        }

        createFrame(*newFrame);

        unlockFrame(newFrame, true);
        lockFrame(newFrame, exclusive);

        return *newFrame;
    }

    bool BufferManager::pageInFifoQueue(BufferFrame **frame, uint64_t page_id, bool exclusive) {
        for(auto & i : fifoQueue){
            if(i->pageid == page_id){
                *frame = i;
                (*frame)->useCounter++;
                fifoMutex.unlock();
                lruMutex.unlock();
                //first try to lock the page
                lockFrame(*frame, exclusive);
                break;
            }
        }

        //if the page is locked, we want to have the queues now and put the page at the end of queue
        if(*frame!= nullptr){
            fifoMutex.lock();
            lruMutex.lock();
            for(size_t i =0; i< fifoQueue.size() ; i++){
                if(fifoQueue[i]->pageid == page_id) {
                    fifoQueue.erase(fifoQueue.begin() + i);
                    lruQueue.push_back(*frame);
                    return true;
                }
            }
            for(size_t i =0; i< lruQueue.size() ; i++){
                if(lruQueue[i]->pageid == page_id) {
                    lruQueue.erase(lruQueue.begin() + i);
                    lruQueue.push_back(*frame);
                    return true;
                }
            }

            (*frame)->useCounter--;

        }
        return false;
    }

    size_t BufferManager::get_page_size(){
        return pageSize;
    }

    void BufferManager::createFrame(BufferFrame& frame){
        fileUseMutex.lock();
        auto load= PosixFile::open_file(std::to_string(get_segment_id(frame.pageid)).c_str(), File::WRITE);
        size_t start = (frame.pageid << 16 >> 16) * pageSize;
        load->read_block(start,pageSize, frame.get_data());
        fileUseMutex.unlock();
    }

    void BufferManager::saveFrame(BufferFrame& frame){
        fileUseMutex.lock();
        auto store= PosixFile::open_file(std::to_string(get_segment_id(frame.pageid)).c_str(), File::WRITE);
        size_t start =(frame.pageid << 16 >> 16) * pageSize;
        std::cout <<"write at page id:" << (frame.pageid << 16 >> 16) << std::endl;
        store->write_block(frame.get_data() , start , pageSize);
        fileUseMutex.unlock();
    }

    bool BufferManager::pageInLruQueue(BufferFrame** frame, uint64_t pageid, bool exclusive){
        //search for pageid in lruQueue, if it is found add the page at the end of queue
        for(auto & i : lruQueue){
            if(i->pageid == pageid){
                *frame= i;
                //increment the useCounter before unlocking the queue to avoid deletion of page
                (*frame)->useCounter++;
                fifoMutex.unlock();
                lruMutex.unlock();
                //first try to lock the page
                lockFrame(i, exclusive);
                break;
            }
        }
        if(*frame != nullptr) {
            fifoMutex.lock();
            lruMutex.lock();
            for (size_t i = 0; i < lruQueue.size(); i++) {
                if (lruQueue[i]->pageid == pageid) {
                    lruQueue.erase(lruQueue.begin() + i);
                    lruQueue.push_back(*frame);
                    return true;
                }
            }
            (*frame)->useCounter--;
        }

        return false;
    }

    void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {

        if(!page.dirty){
            page.dirty=is_dirty;
        }

        unlockFrame(&page, page.exclusive);
    }


    std::vector<uint64_t> BufferManager::get_fifo_list() const {
        std::vector<uint64_t> fifo;
        for(auto i : fifoQueue){
            fifo.push_back(i->pageid);
        }
        return fifo;
    }


    std::vector<uint64_t> BufferManager::get_lru_list() const {
        std::vector<uint64_t> lru;
        for(auto i : lruQueue){
            lru.push_back(i->pageid);
        }
        return lru;
    }





}  // namespace moderndbs

