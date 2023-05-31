#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
    uint32_t size=0;
    MACH_WRITE_INT32(buf,columns.size());
    size+=sizeof(int32_t);
    for(int i=0;i<columns.size();i++){
        size+=columns[i]->SerializeTo(buf+size);
    }
    MACH_WRITE_TO(int8_t,buf+size,is_manage_);
    size+=sizeof(int8_t);
    return size;
}

uint32_t Schema::GetSerializedSize() const {
    uint32_t size=0;
    size+=sizeof(int32_t);
    for(int i=0;i<columns_.size();i++){
        size+=columns[i]->GetSerializedSize();
    }
    size+=sizeof(int8_t);
    return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    if (schema != nullptr) {
        LOG(WARNING) << "Pointer to schema is not null in schema deserialize."<<std::endl;
    }
    uint32_t size=0;
    int32_t sum=MACH_READ_INT32(buf);
    size+=sizeof(int32_t);
    std::vector<Column *> columns;
    for(int i=0;i<sum;i++){
        Column col=new Column("id", TypeId::kTypeInt, 0, false, false);
        size+=Column::DeserializeFrom(buf+size,&col);
        columns.push_back(col);
    }
    int8_t is=MACH_READ_FROM(int8_t,buf+size);
    size+=sizeof(int8_t);
    schema = new Schema(columns,static_cast<bool>is);
    return size;
}