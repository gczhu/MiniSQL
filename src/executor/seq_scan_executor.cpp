//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"
#include "storage/table_iterator.h"

/**
* TODO: Student Implement
*/

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  TableInfo* table_;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_);
  it=table_->GetTableHeap()->Begin(nullptr);
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  TableInfo* table_;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_);
  //std::cout<<it->GetRowId().GetPageId()<<" "<<it->GetRowId().GetSlotNum()<<std::endl;
  while(it != table_->GetTableHeap()->End()){
    if(plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(&(*it)).CompareEquals(Field(kTypeInt,1))==kTrue){
      *row=*it;
      *rid=it->GetRowId();
      ++it;
      return true;
    }
    ++it;
  }
  return false;
}
