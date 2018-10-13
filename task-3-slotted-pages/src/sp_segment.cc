#include "moderndbs/slotted_page.h"
#include "moderndbs/segment.h"
#include <cassert>
#include <cstring>
#include <algorithm>
#include <bitset>


using moderndbs::SPSegment;
using moderndbs::Segment;
using moderndbs::TID;
using moderndbs::SlottedPage;

SPSegment::SPSegment(uint16_t segment_id, BufferManager& buffer_manager, SchemaSegment &schema, FSISegment &fsi)
    : Segment(segment_id, buffer_manager), schema(schema), fsi(fsi) {
    this->segment_id=segment_id;
    this->buffer_manager=&buffer_manager;
    this->schema.set_sp_segment(segment_id);
    //this->fsi = &fsi;
    //this->schema = schema;

}

TID SPSegment::allocate(uint32_t size) {
    std::pair<bool, uint64_t > pair = fsi.find(size);
    if(pair.first){
        BufferFrame& frame=buffer_manager->fix_page((segment_id << 48) + pair.second , true);
        SlottedPage * page = reinterpret_cast<SlottedPage *>(frame.get_data());
        std:: cout << pair.second <<std::endl;
        TID id= page->addNewEntry(size);
        buffer_manager->unfix_page(frame, true);
        fsi.update(pair.second, page->header.free_space);
        return id;
    }else {
        uint64_t pageId = schema.increment_sp_count() ;
        BufferFrame& frame=buffer_manager->fix_page((segment_id << 48) + pageId , true);

        SlottedPage * page = new (frame.get_data()) SlottedPage(buffer_manager->get_page_size());
        page->pageId=pageId;
        //fsi.addNewPage(pageId);
        TID id=page->addNewEntry(size);


        std:: cout << pageId << std::endl;
        buffer_manager->unfix_page(frame, true);
        fsi.update(pageId, page->header.free_space);
        return id;
    }
}

uint32_t SPSegment::read(TID tid, std::byte *record, uint32_t capacity) const {
    uint64_t pageId = tid.value >> 16;
    BufferFrame& frame=buffer_manager->fix_page((segment_id << 48) + pageId , false);
    SlottedPage * page = reinterpret_cast<SlottedPage *>(frame.get_data());

    uint16_t slotId= tid.value << 48 >> 48;
    moderndbs::SlottedPage::Slot* slot= page->getSlot(slotId);
    uint64_t value = slot->value;
    uint8_t t = value >> 56;
    if(t != ~0){
        uint32_t offset = value << 16 >> 40;
        value = reinterpret_cast<uint64_t >(&frame.get_data()[offset]);
        TID newId = TID(value);
        return read(newId, record, capacity);
    }

    uint32_t length = value << 40 >> 40;
    uint32_t offset = value << 16 >> 40;
    if(length <= capacity){
        memcpy(record, &frame.get_data()[offset],length);
    }

    buffer_manager->unfix_page(frame, false);
    //no idea what the return type should be and what to do with capacity
    return length;
}

uint32_t SPSegment::write(TID tid, std::byte *record, uint32_t record_size) {
    this->resize(tid, record_size);

    uint64_t* tidValue = &tid.value;

    uint64_t pageId = *tidValue >> 16;
    BufferFrame& frame=buffer_manager->fix_page((segment_id << 48) + pageId , true);
    SlottedPage * page = reinterpret_cast<SlottedPage *>(frame.get_data());

    uint16_t slotId= tid.value << 48 >> 48;
    moderndbs::SlottedPage::Slot* slot= page->getSlot(slotId);
    uint64_t value = slot->value;
    uint64_t t = value >> 56;
    uint64_t s = value  << 8 >> 56;
    uint64_t offset = value << 16 >> 40;


    if(t != ~0){
        buffer_manager->unfix_page(frame, false);
        tidValue = reinterpret_cast<uint64_t *>(&frame.get_data()[offset]);
        TID newTid = TID(*tidValue);

        uint64_t pageId = *tidValue >> 16;
        BufferFrame & frame2=buffer_manager->fix_page((segment_id << 48) + pageId , true);
        page = reinterpret_cast<SlottedPage *>(frame2.get_data());
        slotId= tid.value << 48 >> 48;
        slot= page->getSlot(slotId);
        value = slot->value;
        t = value >> 56;
        s = value  << 8 >> 56;
        offset = value << 16 >> 40;

    }

    uint32_t length = value << 40 >> 40;

    if(s != 0){
        assert(length == record_size + sizeof(uint64_t));
        memcpy(&frame.get_data()[offset+ sizeof(uint64_t)],record, record_size + sizeof(uint64_t) );
    }else {
        assert(length== record_size);
        memcpy(&frame.get_data()[offset+ sizeof(uint64_t)],record, record_size );
    }
    buffer_manager->unfix_page(frame, true);

    return 0;
}

void SPSegment::resize(TID tid, uint32_t new_size) {
    uint64_t * tidValue1 = &tid.value;
    uint64_t pageId = *tidValue1 >> 16;
    BufferFrame& frame=buffer_manager->fix_page((segment_id << 48) + pageId , true);
    SlottedPage * page = reinterpret_cast<SlottedPage *>(frame.get_data());
    uint16_t slotId= tid.value << 48 >> 48;
    moderndbs::SlottedPage::Slot* slot= page->getSlot(slotId);
    uint64_t value = slot->value;

    uint64_t t = value >> 56;
    std::bitset<64> bitset2{value};

    std::cout << bitset2 <<std::endl;
    uint64_t length = value << 40 >> 40;
    std::bitset<64> bitset3{value};

    std::cout << bitset3 <<std::endl;
    uint64_t offset = value << 16 >> 40;
    std::bitset<64> bitset{value};

    std::cout << bitset <<std::endl;
    //slot is not redirected and size fits, in this case we clean the data(?)
    if(new_size <= length &&  t == ~0){
        memcpy(&frame.get_data()[offset], 0 , length);
        buffer_manager->unfix_page(frame, true);
        return;
    }

    //slot does not point to another record but space don't fit
    if(new_size > length && t == ~0){
        //we must allocate a redirection
        //therefore first change old slot and record to redirection
        t= ~0;
        value = value << 8 >> 8 + t;
        slot->value = value;

        TID newID = allocate(new_size + sizeof(uint64_t));

        uint64_t * tidValue2 = &newID.value;
        uint64_t pageId = *tidValue2 >> 16;

        //clean data and add new tid
        memcpy(&frame.get_data()[offset], 0 , length );
        memcpy(&frame.get_data()[offset], tidValue2 , sizeof(uint64_t) );

        buffer_manager->unfix_page(frame, true);

        //old page is done, change new page
        BufferFrame& frame2=buffer_manager->fix_page((segment_id << 48) + pageId , true);
        SlottedPage * page = reinterpret_cast<SlottedPage *>(frame2.get_data());
        uint16_t slotId= tid.value << 48 >> 48;
        moderndbs::SlottedPage::Slot* slot= page->getSlot(slotId);
        uint64_t * value = &slot->value;
        //stuff which changes s and t
        uint8_t sNew= ~0;
        uint8_t tNew =0;
        offset = *value << 16 >> 40;
        length =  *value << 40 >> 40;
        value =0;
        *value = (tNew << 56);
        *value += sNew << 48;
        *value += offset << 40;
        *value += length;

        memcpy(&frame2.get_data()[offset], tidValue1 , sizeof(uint64_t) );

        buffer_manager->unfix_page(frame2, true);
        return;
    }

    //slot points to another record and space fits
    if(t != ~0 && new_size <= length ){
        //i'm not sure what to do exactly in that case, except cleaning the data in redirected frame
        buffer_manager->unfix_page(frame, false);
       uint64_t * tidValue = reinterpret_cast<uint64_t *>(&frame.get_data()[offset]);

       //redirected frame
        pageId = *tidValue >> 16;
        BufferFrame & frame2=buffer_manager->fix_page((segment_id << 48) + pageId , true);
        page = reinterpret_cast<SlottedPage *>(frame2.get_data());
        slotId= tid.value << 48 >> 48;
        slot= page->getSlot(slotId);
        value = slot->value;

        length = value << 40 >> 40;
        offset = value << 16 >> 40;

        memcpy(&frame2.get_data()[offset], 0 , length);
        buffer_manager->unfix_page(frame2, true);
        return;
    }

    //now only the case is left, when size dones't fit and slot is already redirected. In this case we would have to do a second redirect but we needn't handle that.

}

void SPSegment::erase(TID tid) {
    // TODO: add your implementation here
}
