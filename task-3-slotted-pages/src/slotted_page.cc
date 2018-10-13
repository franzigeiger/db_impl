#include "moderndbs/slotted_page.h"
#include <cassert>
#include <cstring>
#include <vector>
#include <algorithm>
#include <list>
#include <bitset>
#include "moderndbs/buffer_manager.h"

using SlottedPage = moderndbs::SlottedPage;
using TID = moderndbs::TID;

TID::TID(uint64_t raw_value) {
    this->value = raw_value;
}

TID::TID(uint64_t page, uint16_t slot) {

    this->value =  (page << 16) + slot;
}

SlottedPage::Header::Header(uint32_t page_size) {
    this->first_free_slot = 1;
    this->data_start = page_size;
    this->free_space = page_size - sizeof(header);
    this->slot_count = 0;
    this->pageSize = page_size;
}

SlottedPage::Slot::Slot() {
}

SlottedPage::SlottedPage(uint32_t page_size) : header(page_size) {
    this->header = Header(page_size);

}

void SlottedPage::compactify(uint32_t page_size) {

}

moderndbs::SlottedPage::Slot* SlottedPage::getSlot(uint16_t slotId){
    std::list<Slot *>::iterator it = slots.begin();
    std::advance(it, slotId-1);
   return *it;
}

TID SlottedPage::addNewEntry(uint32_t size){
    uint64_t t= ~0 ;
    uint64_t s = 0;
    uint64_t offset = header.data_start - size;
    uint64_t length = size;
    header.data_start = offset;
    header.slot_count++;


    uint64_t slotValue = 0;
    slotValue += t << 56;
    slotValue += s << 56 >> 8;
    slotValue += (offset << 40 >> 16);
    slotValue += (length <<40 >>40);

    Slot newSlot=Slot();
    newSlot.value = slotValue;

    if(header.first_free_slot == header.slot_count){
        slots.push_back(&newSlot);
    }else {
        std::list<Slot *>::iterator it = slots.begin();
        std::advance(it, header.first_free_slot);
        slots.insert(it, &newSlot);
    }

    header.free_space = header.data_start - sizeof(slots) -sizeof(header);

    TID newId = TID(pageId , header.first_free_slot);
    bool foundEmpty = false;
    size_t freeSLot =0 ;
    for(Slot * l : slots){
      if(l == nullptr){
          freeSLot++;
          foundEmpty = true;
          header.first_free_slot=freeSLot;
      }
    }

    if(!foundEmpty){
        header.first_free_slot= slots.size();
    }

    return newId;
}
