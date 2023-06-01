#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size=0;
  uint32_t num=fields_.size();
  MACH_WRITE_UINT32(buf,num);
  size+=sizeof(uint32_t);
  uint32_t bit=0;
  for(int i=0;i<num;i++){
    if(!fields_[i]->IsNull())bit^=1;
    bit<<=1;
  }
  for(int i=0;i<num/32+1;i++) {
    MACH_WRITE_UINT32(buf+size,bit);
    size+=sizeof(uint32_t);
  }
  for(int i=0;i<num;i++){
    size+=fields_[i]->SerializeTo(buf+size);
  }
  MACH_WRITE_INT32(buf+size,rid_.GetPageId());
  size+=sizeof(int32_t);
  MACH_WRITE_UINT32(buf+size,rid_.GetSlotNum());
  size+=sizeof(uint32_t);
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t size=0;
  uint32_t num=MACH_READ_UINT32(buf);
  size+=sizeof(uint32_t);
  uint32_t bit;
    bit= MACH_READ_UINT32(buf+size);
    size+=sizeof(uint32_t);
    fields_.clear();
    for (int j = 0;  j < num; j++) {
      Field *ff= nullptr;
      bit >>= 1;
      if (bit & 1) {
        size += Field::DeserializeFrom(buf + size,schema->GetColumn(j)->GetType() , &ff, false);
      }
      else size += Field::DeserializeFrom(buf + size,schema->GetColumn(j)->GetType() , &ff, true);
      fields_.push_back(ff);

    }
  page_id_t pageId= MACH_READ_FROM(page_id_t,buf+size);
  size+=sizeof(int32_t);
  uint32_t slot= MACH_READ_UINT32(buf+size);
  size+=sizeof(uint32_t);
  RowId rid(pageId,slot);
  SetRowId(rid);
  return size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size=0;
  size+=sizeof(uint32_t);
  size+=sizeof(uint32_t)*(fields_.size()/32+1);
  for(int i=0;i<fields_.size();i++){
      if(fields_[i]->IsNull())continue;
      size+=fields_[i]->GetSerializedSize();
  }
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
