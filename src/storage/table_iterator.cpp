#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(BufferPoolManager *buffer_pool_manager, Schema *schema,RowId rowId)
    :row_(rowId),buffer_(buffer_pool_manager),schema_(schema){}

TableIterator::TableIterator(const TableIterator &other) {
  row_=other.row_;
  buffer_=other.buffer_;
  schema_=other.schema_;
}

TableIterator::~TableIterator() {
  delete buffer_;
  delete schema_;
  if(row!=nullptr)delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(row_==itr.row_)return true;
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if(row_==itr.row_)return false;
  return true;
}

const Row &TableIterator::operator*() {
  Page *page = buffer_->FetchPage(row_.GetPageId());
  row = new Row(row_);
  reinterpret_cast<TablePage *>(page)->GetTuple(row,schema_, nullptr, nullptr);
  buffer_->UnpinPage(page->GetPageId(),page->IsDirty());
  return *row;
}

Row *TableIterator::operator->() {
  Page *page = buffer_->FetchPage(row_.GetPageId());
  row = new Row(row_);
  reinterpret_cast<TablePage *>(page)->GetTuple(row,schema_, nullptr, nullptr);
  buffer_->UnpinPage(page->GetPageId(),page->IsDirty());
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  row_=itr.row_;
  buffer_=itr.buffer_;
  schema_=itr.schema_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  Page *page = buffer_->FetchPage(row_.GetPageId());
  RowId nxt;
  while(!reinterpret_cast<TablePage *>(page)->GetNextTupleRid(row_,&nxt)){
    page_id_t nxt_page=reinterpret_cast<TablePage *>(page)->GetNextPageId();
    if(nxt_page==INVALID_PAGE_ID)break;
    buffer_->UnpinPage(page->GetPageId(),page->IsDirty());
    page=buffer_->FetchPage(nxt_page);
  }
  row_=nxt;
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  RowId now=row_;
  *this=++*this;
  return TableIterator(buffer_,schema_,now);
}
