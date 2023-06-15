#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 * finished
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t result = 0;
  /*magic num + sizeof table_meta_pages_ + sizeof index_meta_pages_ + table_meta_pages_ + index_meta_pages_*/
  result = 3 * sizeof(uint32_t) + table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) + index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
  return result;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  next_table_id_.store(0);
  next_index_id_.store(0);

  if (init == true) {
    catalog_meta_ = new CatalogMeta();
  } else {
    // Fetch the catalog meta page
    Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char* buffer = meta_page->GetData();
    catalog_meta_ = CatalogMeta::DeserializeFrom(buffer);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

    for (const auto &entry : catalog_meta_->table_meta_pages_) {
      // Fetch each table meta page
      page_id_t table_page_id = entry.second;
      Page *table_meta_page = buffer_pool_manager_->FetchPage(table_page_id);
      TableMetadata *table_meta;
      char * buf = table_meta_page->GetData();
      TableMetadata::DeserializeFrom(buf, table_meta);

      // Create and initialize table heap
      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(),
                                                table_meta->GetSchema(), log_manager_, lock_manager_);
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();

      TableInfo *table_info = TableInfo::Create();
      table_info->Init(table_meta, table_heap);
      tables_[table_meta->GetTableId()] = table_info;

      buffer_pool_manager_->UnpinPage(table_page_id, false);
    }

    for (const auto &entry : catalog_meta_->index_meta_pages_) {
      // Fetch each index meta page
      page_id_t index_page_id = entry.second;
      Page *index_meta_page = buffer_pool_manager_->FetchPage(index_page_id);
      IndexMetadata *index_meta;
      char * buf2 = index_meta_page->GetData();
      IndexMetadata::DeserializeFrom(buf2, index_meta);

      std::string table_name = tables_[index_meta->GetTableId()]->GetTableName();
      std::string index_name = index_meta->GetIndexName();
      index_names_[table_name][index_name] = index_meta->GetIndexId();

      // Create and initialize index info
      IndexInfo *index_info = IndexInfo::Create();
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      indexes_[index_meta->GetIndexId()] = index_info;

      buffer_pool_manager_->UnpinPage(index_page_id, false);
    }
  }
}


CatalogManager::~CatalogManager() {
  /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test
 *  模块3的同学注意,这里可能会影响test
   */
  FlushCatalogMetaPage();
  delete catalog_meta_;
  /*注释这一部分的代码以避免double free报错*/
  /*for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }*/
}

/**
* TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  // Check if the table already exists
  if (table_names_.count(table_name) > 0) {
    return DB_TABLE_ALREADY_EXIST;
  }
  Schema* schema_ = TableSchema ::DeepCopySchema(schema);
  // Allocate a new page
  page_id_t new_page_id;
  Page *table_meta_page = buffer_pool_manager_->NewPage(new_page_id);

  // Allocate a new table ID
  table_id_t table_id = catalog_meta_->GetNextTableId()+1;
  //std::cout<<table_id<<std::endl;
  table_names_.emplace(table_name, table_id);
  catalog_meta_->table_meta_pages_.emplace(table_id, new_page_id);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema_, txn, log_manager_, lock_manager_);

  // Serialize and store table metadata in the table heap
  page_id_t first_page = table_heap->GetFirstPageId();
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, first_page, schema_);
  char* buf = table_meta_page->GetData();
  table_meta->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(new_page_id, true);

  // Create table information
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_.emplace(table_id, table_info);
  //for(auto i:tables_)std::cout<<i.second->GetTableName()<<std::endl;
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  /*check if the table name exists*/
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  /*if exists*/
  table_info = tables_[it->second];
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.empty()) {
    return DB_FAILED;
  }

  /* reserve space in the vector for efficiency*/
  tables.reserve(tables_.size());

  /*then we retrieve all tables */
  for (const auto &[table_id, table_info] : tables_) {
    tables.push_back(table_info);
  }
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // Check if the table exists and check if the index already exists
  if (table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].count(index_name) > 0) {
    return DB_INDEX_ALREADY_EXIST;
  }

  // Get table info and schema
  auto table_info = tables_.at(table_names_.at(table_name));
  auto schema = table_info->GetSchema();

  // Construct key map
  std::vector<uint32_t> key_map;
  for (const auto &key : index_keys) {
    uint32_t col_idx;
    if (schema->GetColumnIndex(key, col_idx) != DB_COLUMN_NAME_NOT_EXIST) {
      key_map.push_back(col_idx);
    } else {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }

  index_id_t index_id = catalog_meta_->GetNextIndexId()+1;
  index_names_[table_name][index_name] = index_id;

  // Allocate a new page for index meta page
  page_id_t page_id;
  Page *index_meta_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_.insert({index_id, page_id});
  buffer_pool_manager_->UnpinPage(page_id, true);

  // Create index meta data and serialize it to the index meta page
  table_id_t id = table_names_[table_name];
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, id, key_map);
  char* buf = index_meta_page->GetData();
  index_meta->SerializeTo(buf);

  // Create index info
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  indexes_.insert({index_id, index_info});

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  /*firstly we check if the table name exists*/
  auto table_iter = table_names_.find(table_name);
  if (table_iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  /*then we check if the index exists*/
  auto index_iter = index_names_.at(table_name).find(index_name);
  if (index_iter == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_info = indexes_.at(index_iter->second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  /*firstly we check if the table name exists*/
  auto table_iter = table_names_.find(table_name);
  if (table_iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_iter->second;

  /*get the table indexes*/
  const std::unordered_map<std::string, index_id_t> &index_map = index_names_.at(table_name);
  for (const auto &index_entry : index_map) {
    index_id_t index_id = index_entry.second;
    indexes.push_back(indexes_.at(index_id));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto table_iter = table_names_.find(table_name);
  if (table_iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  /*fetch the table id and page id*/
  table_id_t table_id = table_iter->second;
  page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];

  /*delete the indexes of the table*/
  std::vector<IndexInfo*> indexes;
  if (GetTableIndexes(table_name, indexes) == DB_SUCCESS) {
    for (auto index_information : indexes) {
      DropIndex(table_name, index_information->GetIndexName());
    }
  }
  /*delete from table names*/
  table_names_.erase(table_iter);

  /*delete from tables_*/
  tables_.erase(table_id);
  /*delete meta data*/
  catalog_meta_->table_meta_pages_.erase(table_id);

  /*delete from buffer_pool_manager_*/
  buffer_pool_manager_->DeletePage(page_id);
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::DropIndex(const string& table_name, const string& index_name) {
  /*check if the table name exists*/
  auto table_iter = table_names_.find(table_name);
  if (table_iter == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  /*check if the index exists*/
  auto& index_map = index_names_[table_name];
  auto index_iter = index_map.find(index_name);
  if (index_iter == index_map.end()) {
    return DB_INDEX_NOT_FOUND;
  }

  /*fetch index_id*/
  index_id_t index_id = index_iter->second;

  /*delete index in the mapping*/
  index_map.erase(index_iter);

  /*delete from meta*/
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);

  /*delete others*/
  buffer_pool_manager_->DeletePage(page_id);
  indexes_.erase(index_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page* MetaPage = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (MetaPage == nullptr)
  {
    return DB_FAILED;
  }
  // 将元数据序列化到缓冲页的数据缓冲区中
  char * data = MetaPage->GetData();
  catalog_meta_->SerializeTo(data);

  // 刷新缓冲页的数据到磁盘
  if (!buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
    return DB_FAILED;  // 刷新失败，返回失败状态
  }
  // 释放元数据页的缓冲页对象
  if (!buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true)) {
    return DB_FAILED;  // 释放失败，返回失败状态
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // Check if the table already exists
  if (tables_.count(table_id) > 0) {
    return DB_TABLE_ALREADY_EXIST;
  }

  // Add the table metadata page to the catalog meta
  catalog_meta_->table_meta_pages_.emplace(table_id, page_id);
  Page* temp = buffer_pool_manager_->FetchPage(page_id);

  // Deserialize the table metadata from the page data
  TableMetadata* loaded_meta;
  char * buf = temp->GetData();
  TableMetadata::DeserializeFrom(buf, loaded_meta);

  // Add the table name and id to the table names map
  string table_name = loaded_meta->GetTableName();
  table_names_[table_name] = table_id;

  // Allocate space for table heap and table info from the system heap
  TableInfo* table_info = TableInfo::Create();
  Schema* s = loaded_meta->GetSchema();
  TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, page_id, s, log_manager_, lock_manager_);
  table_info->Init(loaded_meta, table_heap);

  // Keep tables_ and catalog_meta_->table_meta_pages_ consistent
  tables_[table_id] = table_info;

  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  /*check if the index exists*/
  if (indexes_.count(index_id) > 0) {
    return DB_INDEX_ALREADY_EXIST;
  }

  /*Add the table metadata page to the catalog meta*/
  catalog_meta_->index_meta_pages_.insert({index_id, page_id});
  auto temp = buffer_pool_manager_->FetchPage(page_id);

  /*read and deserialize the data*/
  IndexMetadata *index_meta;
  char * buf = temp->GetData();
  IndexMetadata::DeserializeFrom(buf, index_meta);
  table_id_t table_id = index_meta->GetTableId();
  std::string table_name = tables_[table_id]->GetTableName();
  index_names_[table_name][index_meta->GetIndexName()] = index_id;

  /*allocate space*/
  IndexInfo* index_info = IndexInfo::Create();
  index_info->Init(index_meta, tables_[table_id], buffer_pool_manager_);
  indexes_[index_id] = index_info;

  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 * finished
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto iter = tables_.find(table_id);
  /*check if the table already exists*/
  if (iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  } else {
    /*the table exists, we can use iter->second to get its table_info*/
    table_info = iter->second;
    return DB_SUCCESS;
  }
}