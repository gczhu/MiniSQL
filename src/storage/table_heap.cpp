#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  Page *page = buffer_pool_manager_->FetchPage(first_page_id_);
  while(!reinterpret_cast<TablePage *>(page)->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    if(reinterpret_cast<TablePage *>(page)->GetNextPageId()!=INVALID_PAGE_ID){
      buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
      page = buffer_pool_manager_->FetchPage(reinterpret_cast<TablePage *>(page)->GetNextPageId());
    }
    else{
      page_id_t nxt;
      Page *new_page = buffer_pool_manager_->NewPage(nxt);
      reinterpret_cast<TablePage *>(new_page)->Init(nxt,page->GetPageId(),log_manager_,txn);
      reinterpret_cast<TablePage *>(page)->SetNextPageId(nxt);
      if(!reinterpret_cast<TablePage *>(new_page)->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
        return false;
      }
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(),new_page->IsDirty());
      break;
    }
  }
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  int signal;
  Row old_row(rid);
  if(GetTuple(&old_row,txn)){
    return false;
  }
  if(!page->UpdateTuple(row,&old_row,schema_,txn,nullptr,nullptr)){
    MarkDelete(rid, nullptr);
    InsertTuple(row,txn);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
  return true;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->ApplyDelete(rid,txn,nullptr);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  Page *page = buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId());
  bool ok=reinterpret_cast<TablePage *>(page)->GetTuple(row,schema_, nullptr, nullptr);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
  return ok;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  Page *page = buffer_pool_manager_->FetchPage(first_page_id_);
  RowId *row_id = new RowId();
  while(!reinterpret_cast<TablePage *>(page)->GetFirstTupleRid(row_id)){
    page_id_t nxt = reinterpret_cast<TablePage *>(page)->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page->GetPageId(),page->IsDirty());
    if(nxt== INVALID_PAGE_ID){
      break;
    }
    page = buffer_pool_manager_->FetchPage(nxt);
  }
  return TableIterator(buffer_pool_manager_,schema_,*row_id);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(buffer_pool_manager_,schema_,INVALID_ROWID);
}
