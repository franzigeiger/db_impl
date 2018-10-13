#include <limits>
#include "moderndbs/segment.h"
#include <math.h>

using Segment = moderndbs::Segment;
using FSISegment = moderndbs::FSISegment;

FSISegment::FSISegment(uint16_t segment_id, BufferManager& buffer_manager, SchemaSegment &schema)
    : Segment(segment_id, buffer_manager) {
    this->segment_id = segment_id;
    this->buffer_manager = &buffer_manager;
    this->schema=&schema;
    this->schema->set_fsi_segment(segment_id);
    bitSize = 4;

    //size_t numberOfItems = schema.get_sp_count()/2 ;
    //BufferFrame* frame = &(buffer_manager.fix_page(segment_id , true));
    //schema.get_sp_count();
    //*(&reinterpret_cast<uint8_t *>(frame->get_data())[10]) = 99999999;

    //fsiList = reinterpret_cast<std::vector<uint8_t>>(frame.get_data());
}


void FSISegment::update(uint64_t target_page, uint32_t free_space) {
        uint8_t calcSpace = free_space /(buffer_manager->get_page_size() / ((2^bitSize)-1)); //aufrunden
        size_t targetEntry = target_page/2;
        if(target_page%2 == 0){
            targetEntry--;
        }

        size_t itemsPerPage= (buffer_manager->get_page_size()/8);
        size_t pageNumber = (targetEntry/ itemsPerPage);


        BufferFrame& frame=buffer_manager->fix_page((segment_id << 48) + pageNumber, true );

        size_t offset = targetEntry % itemsPerPage;

    uint8_t item=*(&reinterpret_cast<uint8_t *>(frame.get_data())[offset]);

    if(target_page%2 == 0){
        item= (item >> 4 << 4 ) + calcSpace;
    } else{
        item= (item << 4 >> 4 ) + (calcSpace << 4);
    }

    *(&reinterpret_cast<uint8_t *>(frame.get_data())[offset]) = item;

    buffer_manager->unfix_page(frame, true);
}

std::pair<bool, uint64_t> FSISegment::find(uint32_t required_space) {
    size_t itemsPerPage= (buffer_manager->get_page_size());
    size_t spCount= schema->get_sp_count();
    size_t neededFsiPages = spCount!= 0? (spCount / itemsPerPage) + 1 : 0;
    uint8_t calcSpace = std::ceil(required_space / ceil((buffer_manager->get_page_size() / ((2^bitSize)-1))));

    for(int i=0; i< neededFsiPages ; i++){
        BufferFrame& frame = buffer_manager->fix_page((segment_id << 48) + i , false);
        for(int j =0; j < std::ceil(schema->get_sp_count()/2) ; j++ ){
            uint8_t item=*(&reinterpret_cast<uint8_t *>(frame.get_data())[j]);

            uint8_t lowerItem = item >> 4;
            if(calcSpace <= lowerItem){
                uint64_t pageId = (i*itemsPerPage ) + (2* j) +1;
                buffer_manager->unfix_page(frame, false);
                return std::pair(true, pageId);
            }
            uint8_t higherItem = item << 4 >> 4;
            if(calcSpace <= higherItem){
                uint64_t pageId = (i*itemsPerPage ) + (2* j) + 2;
                 buffer_manager->unfix_page(frame, false);
                return std::pair(true, pageId);
            }
        }
        buffer_manager->unfix_page(frame, false);
    }

    return std::pair( false, 0);

}

void FSISegment::addNewPage(uint64_t target_page){
    size_t itemsPerPage =  buffer_manager->get_page_size();
    size_t pageNumber = target_page/ itemsPerPage;
    BufferFrame & frame= buffer_manager->fix_page((segment_id << 48) + pageNumber , true);

    size_t offset = (target_page % itemsPerPage) -1;

    uint8_t item=*(&reinterpret_cast<uint8_t *>(frame.get_data())[offset]);

    uint8_t full = ~0 <<4;

    if(pageNumber%2 == 0){
        item= (item >> 4 << 4 ) + full;
    } else{
        item= (item << 4 >> 4 ) + (full <<4 );
    }

    *(&reinterpret_cast<uint8_t *>(frame.get_data())[offset]) = item;

    buffer_manager->unfix_page(frame, true);
}
