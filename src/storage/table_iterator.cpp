#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

TableIterator::TableIterator(const TableIterator &other) {
    row_=other.row_;
    buffer_=other.buffer_;
    schema_=other.schema_;
}

TableIterator::~TableIterator() {
    delete buffer_;
    delete schema_;
    if(Row!=nullptr)delete row;
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
    Page *page = buffer_>FetchPage(row_.GetPageId());
    row = new Row(row_);
    reinterpret_cast<TablePage *>(page)->GetTuple(row,schema_, nullptr, nullptr);
    return *row;
}

Row *TableIterator::operator->() {
    Page *page = buffer_pool_manager_->FetchPage(row_id.GetPageId());
    row = new Row(row_);
    reinterpret_cast<TablePage *>(page)->GetTuple(row,schema_, nullptr, nullptr);
    return *row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    row_=itr.row_;
    buffer_=itr.buffer_;
    schema_=itr.schema_;
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  return TableIterator();
}
