#include "moderndbs/schema.h"
#include "moderndbs/segment.h"
#include "moderndbs/error.h"
#include <limits>
#include <cstring>
#include <sstream>
#include <vector>
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"

using Segment = moderndbs::Segment;
using SchemaSegment = moderndbs::SchemaSegment;
using Schema = moderndbs::schema::Schema;
using Type = moderndbs::schema::Type;
using Table = moderndbs::schema::Table;
using Column = moderndbs::schema::Column;


SchemaSegment::SchemaSegment(uint16_t segment_id, BufferManager &buffer_manager)
        : Segment(segment_id, buffer_manager) {
    this->segment_id = segment_id;
    this->buffer_manager = &buffer_manager;
    spSegment=0;
    fsiSegment=0;
    spCount =0;

}

void SchemaSegment::set_schema(std::unique_ptr<Schema> new_schema) {
    schema = std::move(new_schema);
}

Schema *SchemaSegment::get_schema() {
    return schema.get();
}

uint64_t SchemaSegment::get_sp_count() {
    return spCount;
}

uint64_t SchemaSegment::increment_sp_count() {
    return ++spCount;
}

void SchemaSegment::set_fsi_segment(uint16_t segment) {
    fsiSegment = segment;
}

uint16_t SchemaSegment::get_fsi_segment() {
    return fsiSegment;
}

void SchemaSegment::set_sp_segment(uint16_t segment) {
    spSegment = segment;
}

uint16_t SchemaSegment::get_sp_segment() {
    return spSegment;
}

void SchemaSegment::read() {

    BufferFrame& frame = buffer_manager->fix_page((segment_id << 48) , false);
    size_t pageSize = buffer_manager->get_page_size();

    char* value = frame.get_data();
    size_t resultLength = *reinterpret_cast<size_t *>(value);
    value += sizeof(resultLength);
    this->spSegment = *reinterpret_cast<uint16_t *>(value);
    value += sizeof(spSegment);
    this->fsiSegment = *reinterpret_cast<uint16_t *>(value);
    value += sizeof(fsiSegment);
    this->spCount = *reinterpret_cast<uint16_t *>(value);
    value += sizeof(spCount);

    size_t occupied = sizeof(size_t) + sizeof(spSegment) + sizeof(fsiSegment) + sizeof(spCount);

    auto result = std::make_unique<char []>(resultLength-1);

    if(resultLength < pageSize){
        std::memcpy(result.get() , frame.get_data()+ occupied, resultLength);
        buffer_manager->unfix_page(frame, false);
    }else {
        memcpy(result.get() , value, pageSize- occupied);
        buffer_manager->unfix_page(frame, false);
        size_t counter =pageSize - occupied;
        size_t segmentCounter = 1;
        while (counter <= resultLength) {
            BufferFrame& frame = buffer_manager->fix_page((segment_id << 48) + segmentCounter  , false);
            if(counter + pageSize < resultLength){
               memcpy(result.get()+counter , frame.get_data(), pageSize);
            } else {
                memcpy(result.get()+ counter, frame.get_data(),resultLength - counter);
            }
            buffer_manager->unfix_page(frame, false);
            counter += pageSize;
            segmentCounter++;
        }
    }

    for(int i= resultLength ; i > 0; i--){
        if(result[i]!= '}'){
            result[i]= '\0';
        } else{
            break;
        }
    }

    rapidjson::Document d;
    std::cout << " json = " << result.get() << "end " << std::endl;
    d.Parse(result.get());

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    d.Accept(writer);

    std::vector<schema::Table> tables;
    rapidjson::Value &array = d["tables"];

        for (size_t i = 0; i < array.Size(); i++) {
            rapidjson::Value &object = array[i];
            std::string id = object["name"].GetString();

            rapidjson::Value &columns = object["columns"];
            std::vector<schema::Column> columnObjects;
            for (size_t i = 0; i < columns.Size(); i++) {
                rapidjson::Value& columnJson = columns[i];
                std::string id = columnJson["id"].GetString();

                rapidjson::Value& columnType = columnJson["type"];
                schema::Type type;
                std::string name = columnType["name"].GetString();

                if (name == "numeric" || name == "char" || name== "varchar") {
                    uint32_t length = columnType["length"].GetInt();
                    if (name== "numeric") {
                        uint32_t precision = columnType["precision"].GetInt();
                        type = schema::Type::Numeric(length, precision);
                    } else if (name =="char") {
                        type = schema::Type::Char(length);
                    } else {
                        type = schema::Type::Varchar(length);
                    }

                } else if (name == "integer") {
                    type = schema::Type::Integer();
                } else {
                    type = schema::Type::Timestamp();
                }


                schema::Column * c = new schema::Column(id, type);
                columnObjects.push_back(*c);
            }

            std::vector<std::string> pks;
            rapidjson::Value& pksJson = object["pk"];
            for (size_t i = 0; i < pksJson.Size(); i++) {
                pks.push_back(pksJson[i].GetString());
            }

            schema::Table *t = new schema::Table(id, columnObjects, pks);
            tables.push_back(*t);



    }
    schema =std::move(std::make_unique<schema::Schema>(schema::Schema(tables)));
}

void SchemaSegment::write() {
    rapidjson::Document d;

    d.SetObject();

    rapidjson::Document::AllocatorType &allocator = d.GetAllocator();
    rapidjson::Value array(rapidjson::kArrayType);

    for (schema::Table table : schema->tables) {

        rapidjson::Value object(rapidjson::kObjectType);
        rapidjson::Value name;
        name.SetString(table.id.c_str(), allocator);
        object.AddMember("name", rapidjson::Value(table.id.c_str() , allocator), allocator);

        rapidjson::Value columns(rapidjson::kArrayType);
        for (schema::Column column : table.columns) {
            rapidjson::Value columnJson(rapidjson::kObjectType);

            columnJson.AddMember("id", rapidjson::Value(column.id.c_str(), allocator), allocator);
            rapidjson::Value columnType(rapidjson::kObjectType);

            columnType.AddMember("name", rapidjson::Value(column.type.name() , allocator), allocator);
            columnType.AddMember("length", column.type.length, allocator);
            columnType.AddMember("precision", column.type.precision, allocator);

            columnJson.AddMember("type", columnType, allocator);
            columns.PushBack(columnJson, allocator);
        }

        object.AddMember("columns", columns, allocator);

        rapidjson::Value pks(rapidjson::kArrayType);
        for (std::string column : table.primary_key) {
            pks.PushBack(rapidjson::Value(column.c_str(), allocator), allocator);
        }

        object.AddMember("pk", pks, allocator);
        array.PushBack(object, allocator);
    }

    d.AddMember("tables", array, allocator);


    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    d.Accept(writer);

    std::string const_result =strbuf.GetString();
    size_t size =const_result.length();
    size_t pageSize = buffer_manager->get_page_size();


    std::cout << " json = " << const_result.c_str() << std::endl;
    BufferFrame &frame = buffer_manager->fix_page((segment_id << 48), true);

    char * value = frame.get_data();
    size_t& temp = *reinterpret_cast<size_t*>(value);
    temp = size;
    value += sizeof(size_t);
    *value = this->spSegment;
    value += sizeof(this->spSegment);
    *value = this->fsiSegment;
    value += sizeof(this->fsiSegment);
    *value = this->spCount;
    value += sizeof(this->spCount);

    size_t occupied = sizeof(size_t) + sizeof(spSegment) + sizeof(fsiSegment) + sizeof( this->spCount);
    if (size + occupied <= buffer_manager->get_page_size()) {
        std::memcpy(frame.get_data() + occupied, const_result.c_str(), size);
        buffer_manager->unfix_page(frame, true);
    } else {
        size_t pageNumber = ((size +occupied ) / pageSize) + 1;
        std::memcpy(frame.get_data()+occupied ,const_result.c_str(), pageSize- occupied);

        //*(&reinterpret_cast<size_t *>(frame.get_data())[1]) = *part;
        buffer_manager->unfix_page(frame, true);
        size_t itemCount = pageSize- occupied;
        for (size_t i = 1; i < pageNumber; i++) {
            BufferFrame &frame = buffer_manager->fix_page((segment_id << 48) + i, true);

            if (size - itemCount > pageSize) {
                char next[pageSize];

                std::memcpy(frame.get_data(),  const_result.c_str()+itemCount, pageSize);
            }else {
                char next[ size -itemCount];

                std::memcpy(frame.get_data(),  const_result.c_str() + itemCount, size- itemCount);
            }
            buffer_manager->unfix_page(frame, true);
            itemCount += pageSize;
        }

    }


}


