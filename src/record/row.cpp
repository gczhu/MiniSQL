#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
<<<<<<< HEAD
  uint32_t size=0;
  uint32_t num=fields_.size();
  MACH_WRITE_UINT32(buf,num);
  size+=sizeof(uint32_t);
  uint64_t bit=0,tot=1;
  for(int i=0;i<num;i++){
    if(!fields_[i]->IsNull())bit^=tot;
    tot<<=1;
  }
  MACH_WRITE_TO(uint64_t,buf+size,bit);
  size+=sizeof(uint64_t);
  for(int i=0;i<num;i++){
    size+=fields_[i]->SerializeTo(buf+size);
  }
  MACH_WRITE_INT32(buf+size,rid_.GetPageId());
  size+=sizeof(int32_t);
  MACH_WRITE_UINT32(buf+size,rid_.GetSlotNum());
  size+=sizeof(uint32_t);
  return size;
=======
  //RowId rid = rid_;
  //MACH_WRITE_TO(RowId, buf, rid);
  //buf += 8;
  size_t field_num = schema->GetColumnCount();


  MACH_WRITE_TO(size_t, buf, field_num);
  buf += 4;
  uint32_t null_num = 0;
  for (int i = 0; i < field_num; ++i) {
    if( fields_[i]->IsNull() ) {
      null_num++;
    } 
  }
  MACH_WRITE_TO(uint32_t, buf, null_num);
  buf += 4;
  /*
  for (int i = 0; i < field_num; ++i) {
    // NULL?
    //MACH_WRITE_TO(TypeId, buf, fields_[i]->GetTypeId());
    //buf += 4;
    //MACH_WRITE_TO(bool, buf, fields_[i]->IsNull());
    //buf += 4;
    buf += fields_[i]->SerializeTo(buf);
  }
  */
  for (int i = 0; i < field_num; ++i) {
    if( fields_[i]->IsNull() ) {
      MACH_WRITE_TO(uint32_t, buf, i);
      buf += 4;
    } 
  }
  for (auto &itr : fields_) {
    if (!itr->IsNull()) {
      buf += itr->SerializeTo(buf);
    } 
  }
  //return 8 + 4 * field_num;
  return GetSerializedSize(schema);
>>>>>>> d8dd1c9 (commit b_plus_tree)
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
<<<<<<< HEAD

  uint32_t size=0;
  uint32_t num=MACH_READ_UINT32(buf);
  size+=sizeof(uint32_t);
  uint64_t bit;
  bit= MACH_READ_FROM(uint64_t ,buf+size);
    size+=sizeof(uint64_t);
    fields_.clear();
    for (int j = 0;  j < num; j++) {
      Field *ff= nullptr;
      if (bit & 1) {
        size += Field::DeserializeFrom(buf + size,schema->GetColumn(j)->GetType() , &ff, false);
      }
      else size += Field::DeserializeFrom(buf + size,schema->GetColumn(j)->GetType() , &ff, true);
      fields_.push_back(ff);
      bit>>=1;
    }
  page_id_t pageId= MACH_READ_FROM(page_id_t,buf+size);
  size+=sizeof(int32_t);
  uint32_t slot= MACH_READ_UINT32(buf+size);
  size+=sizeof(uint32_t);
  RowId rid(pageId,slot);
  SetRowId(rid);
  return size;
=======
  size_t field_num = MACH_READ_FROM(size_t, buf);
  field_num = schema->GetColumnCount();
  buf += 4;
  int null_num = MACH_READ_FROM(uint32_t, buf);
  buf += 4;
  std::vector<Field *> fields;
  std::vector<uint32_t> null_bitmap(field_num, 0);


  //std::cout << "Lai 1" << std::endl;

  for (int i = 0; i < null_num; i++) {
    null_bitmap[MACH_READ_FROM(uint32_t, buf)] = 1;
    buf += 4;
  }

  
  //std::cout << "Lai 2:" << field_num <<std::endl;
  //std::cout << "Lai 2:" << null_num <<std::endl;
  for (int i = 0; i < field_num; ++i) {
   // std::cout << "Hao Lai zi" << std::endl;
    Field *thefield = new Field(schema->GetColumn(i)->GetType());
    fields.push_back(thefield);
  }


  //std::cout << "Lai 3" << std::endl;
  for (int i = 0; i < field_num; ++i) {
    if(!null_bitmap[i]) {
      Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &(fields[i]), false);
      buf += fields[i]->GetSerializedSize();
    }
    // type and isnull
    //size_t  = MACH_READ_FROM(size_t, buf);
    //buf += 4;
    //bool is_null = MACH_READ_FROM(bool, buf);
    //buf += 4;
    //std::cout << "field_num: " << field_num << std::endl;
    //Field::DeserializeFrom(buf, , &(fields[i]), schema->GetColumn(i)->IsNullable());
  }

  //std::cout << "Lai 4" << std::endl;
  for (int i = 0; i < field_num; ++i) { 
    fields_.push_back(fields[i]);
  }

  // schema = ?
  //uint32_t SerializeedSize = 0;
  //for (int i = 0; i < field_num; ++i) {
    //SerializeedSize += 8;
    //SerializeedSize += fields[i]->GetSerializedSize();
  //}
  //std::cout << "Lai 5" << std::endl;
  return GetSerializedSize(schema);
>>>>>>> d8dd1c9 (commit b_plus_tree)
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
<<<<<<< HEAD
  uint32_t size=0;
  size+=sizeof(uint32_t);
  size+=sizeof(uint64_t);
  for(int i=0;i<fields_.size();i++){
      if(fields_[i]->IsNull())continue;
      size+=fields_[i]->GetSerializedSize();
  }
  size+=sizeof(int32_t);
  size+=sizeof(uint32_t);
  return size;
=======
  if(fields_.empty()) {
    return 0;
  }
  int field_num = fields_.size();
  uint32_t null_num = 0;
  for (int i = 0; i < field_num; ++i) {
    if( fields_[i]->IsNull() ) {
      null_num++;
    } 
  }
  uint32_t SerializeedSize = 0;
  
  for (int i = 0; i < field_num; ++i) {
    if(!fields_[i]->IsNull()) {
      SerializeedSize += fields_[i]->GetSerializedSize();
    }
  }

  return 8 + SerializeedSize + 4 * null_num;
>>>>>>> d8dd1c9 (commit b_plus_tree)
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
