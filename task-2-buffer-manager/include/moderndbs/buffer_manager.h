#ifndef INCLUDE_MODERNDBS_BUFFER_MANAGER_H
#define INCLUDE_MODERNDBS_BUFFER_MANAGER_H

#include <cstddef>
#include <cstdint>
#include <exception>
#include <vector>
#include "moderndbs/file.h"
#include <shared_mutex>


namespace moderndbs {

class BufferFrame {
private:
    friend class BufferManager;

    std::vector<uint64_t> data;
    int useCounter;

public:
    bool dirty;
    bool exclusive;
    uint64_t pageid;
    mutable std::shared_mutex mutex_;

    /// Returns a pointer to this page's data.
    char* get_data();
    explicit BufferFrame(size_t pageSize);

    ~BufferFrame() = default;
};


class buffer_full_error
: public std::exception {
public:
    const char* what() const noexcept override {
        return "buffer is full";
    }
};


class BufferManager {
private:
    std::vector<BufferFrame*> fifoQueue;
    std:: vector<BufferFrame*> lruQueue;
    int maxPage;
    size_t pageSize;
    mutable std::mutex lruMutex;
    mutable std::mutex fifoMutex;
    mutable std::mutex fileUseMutex;

public:
    int pageCounter ;
    /// Constructor.
    /// @param[in] page_size  Size in bytes that all pages will have.
    /// @param[in] page_count Maximum number of pages that should reside in
    //                        memory at the same time.
    BufferManager(size_t page_size, size_t page_count);

    /// Destructor. Writes all dirty pages to disk.
    ~BufferManager();

    /// Returns a reference to a `BufferFrame` object for a given page id. When
    /// the page is not loaded into memory, it is read from disk. Otherwise the
    /// loaded page is used.
    /// When the page cannot be loaded because the buffer is full, throws the
    /// exception `buffer_full_error`.
    /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
    /// `unfix_page()`.
    /// @param[in] page_id   Page id of the page that should be loaded.
    /// @param[in] exclusive If `exclusive` is true, the page is locked
    ///                      exclusively. Otherwise it is locked
    ///                      non-exclusively (shared).
    BufferFrame & fix_page(uint64_t page_id, bool exclusive);

    /// Takes a `BufferFrame` reference that was returned by an earlier call to
    /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
    /// written back to disk eventually.
    void unfix_page(BufferFrame& page, bool is_dirty);

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// FIFO list in FIFO order.
    /// Is not thread-safe.
    std::vector<uint64_t> get_fifo_list() const;

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// LRU list in LRU order.
    /// Is not thread-safe.
    std::vector<uint64_t> get_lru_list() const;

    /// Returns the segment id for a given page id which is contained in the 16
    /// most significant bits of the page id.
    static constexpr uint16_t get_segment_id(uint64_t page_id) {
        return page_id >> 48;
    }

    /// Returns the page id within its segment for a given page id. This
    /// corresponds to the 48 least significant bits of the page id.
    static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
        return page_id & ((1ull << 48) - 1);
    }
    //BufferFrame createFrame(uint64_t pageId, bool exclusive);

    void saveFrame(BufferFrame& frame);

    void createFrame(BufferFrame &frame);

    void lockFrame(BufferFrame *frame, bool exclusive);

    void unlockFrame(BufferFrame *frame, bool exclusive);

    bool pageInLruQueue(BufferFrame **frame, uint64_t pageid, bool exclusive);

    bool pageInFifoQueue(BufferFrame **frame, uint64_t page_id, bool exclusive);
};


}  // namespace moderndbs

#endif
