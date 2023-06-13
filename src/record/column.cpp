#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
    uint32_t size=0;
    MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
    size+=sizeof(uint32_t);
    MACH_WRITE_INT32(buf+size,name_.length());
    MACH_WRITE_STRING(buf+size+4, name_);
    size+=MACH_STR_SERIALIZED_SIZE(name_);
    MACH_WRITE_TO(TypeId,buf+size,type_);
    size+=sizeof(TypeId);
    if(type_ == TypeId::kTypeChar){
        MACH_WRITE_UINT32(buf+size, len_);
        size+=sizeof(uint32_t);
    }
    MACH_WRITE_UINT32(buf+size, table_ind_);
    size+=sizeof(uint32_t);
    MACH_WRITE_TO(int8_t,buf+size,nullable_);
    size+=sizeof(int8_t);
    MACH_WRITE_TO(int8_t,buf+size,unique_);
    size+=sizeof(int8_t);
    return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
    uint32_t size=0;
    size+=sizeof(uint32_t);
    size+=MACH_STR_SERIALIZED_SIZE(name_);
    size+=sizeof(TypeId);
    if(type_ == TypeId::kTypeChar)size+=sizeof(uint32_t);
    size+=sizeof(uint32_t);
    size+=sizeof(int8_t);
    size+=sizeof(int8_t);
    return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
    if (column != nullptr) {
        LOG(WARNING) << "Pointer to column is not null in column deserialize."<<std::endl;
    }
    uint32_t size=0;
    uint32_t check=MACH_READ_UINT32(buf);
    if(check!=210928)return 0;
    size+=sizeof(uint32_t);
    int32_t length=MACH_READ_INT32(buf+size);
    size+=4;
    std::string name_={};
    for(int i=0;i<length;i++)name_.push_back(MACH_READ_FROM(int8_t,buf+size++));
    name_.push_back('\0');
    TypeId id=MACH_READ_FROM(TypeId,buf+size);
    size+=sizeof(TypeId);
    int32_t len=0;
    if(id==TypeId::kTypeChar){
        len=MACH_READ_INT32(buf+size);
        size+=sizeof(int32_t);
    }
    int32_t index=MACH_READ_INT32(buf+size);
    size+=sizeof(int32_t);
    int8_t nu=MACH_READ_FROM(int8_t,buf+size);
    size+=sizeof(int8_t);
    int8_t un=MACH_READ_FROM(int8_t,buf+size);
    size+=sizeof(int8_t);
    if(len)column = new Column(name_,id,len,index,nu,un);
    else column = new Column(name_,id,index,nu,un);
    return size;
}
