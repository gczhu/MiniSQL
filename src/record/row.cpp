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
  uint32_t bit[num/32+1];
  for(int i=0;i<num/32+1;i++)bit[i]=0;
  for(int i=0;i<num;i++){
    if(!fields_[i]->IsNull())bit[i/32]^=1;
    bit[i/32]<<=1;
  }
  for(int i=0;i<num/32+1;i++) {
    MACH_WRITE_UINT32(buf+size,bit[i]);
    size+=sizeof(uint32_t);
  }
  for(int i=0;i<num;i++){
    if(fields_[i]->IsNull())continue;
    MACH_WRITE_TO(TypeId,buf+size, fields_[i]->GetTypeId());
    size+=sizeof(TypeId);
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
  uint32_t bit[num/32+1];
  for(int i=0;i<num/32+1;i++){
    bit[i]= MACH_READ_UINT32(buf+size);
  }
  for(int i=0;i<num/32+1;i++) {
    for (int j = 0; 32 * i + j < num && j < 32; j++) {
      Field *ff;
      if (bit[i] & 1) {
        TypeId t = MACH_READ_FROM(TypeId, buf + size);
        size += sizeof(TypeId);
        size += Field::DeserializeFrom(buf + size, t, &ff, false);
        fields_.push_back(new Field(*ff));
      }
      bit[i] >>= 1;
    }
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
      size+=sizeof(TypeId);
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
