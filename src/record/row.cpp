#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size=0;
  int32_t num=fields_.size();
  MACH_WRITE_INT32(buf,num);
  size+=sizeof(int32_t);
  for(int i=0;i<num;i++){
      if(fields_[i]->is_null_ == true)
      size+=fields_[i]->SerializeTo(buf+size);
  }
  MACH_WRITE_INT32(buf+size,rid_.GetPageID());
  size+=sizeof(int32_t);
  MACH_WRITE_UINT32(buf+size,rid_.GetSlotNum());
  size+=sizeof(uint32_t);
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t size=0;
  int32_t num=MACH_READ_INT32(buf);
  for(int i=0;i<num;i++){
      size+=fields_[i]->DeserializeFrom(buf,&fields_[i],0);
  }
  MACH_READ_INT32(buf+size,rid_.GetPageID());
  size+=sizeof(int32_t);
  MACH_READ_UINT32(buf+size,rid_.GetSlotNum());
  size+=sizeof(uint32_t);
  return 0;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size=0;
  size+=sizeof(int32_t);
  for(int i=0;i<field_.size();i++)size+=fields_[i]->GetSerializedSize();
  size+=sizeof(int32_t);
  size+=sizeof(uint32_t);
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
