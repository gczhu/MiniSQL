//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}



void InsertExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_);
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),index_);
  child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row ins;
  RowId insr;
  if(child_executor_->Next(&ins, &insr)){
    for(int i=0;i<table_->GetSchema()->GetColumnCount();i++){
      if(table_->GetSchema()->GetColumn(i)->IsUnique()){
        for(int j=0;j<index_.size();j++){
          if(i==index_[j]->GetIndexKeySchema()->GetColumn(0)->GetTableInd()){
            Row row_;
            std::vector<RowId> mul;
            ins.GetKeyFromRow(table_->GetSchema(),index_[j]->GetIndexKeySchema(),row_);
            index_[j]->GetIndex()->ScanKey(row_,mul,nullptr);
            if(!mul.empty())return false;
          }
        }
      }
    }
  }
  for(int i=0;i<index_.size();i++){
    Row row_;
    ins.GetKeyFromRow(table_->GetSchema(),index_[i]->GetIndexKeySchema(),row_);
    index_[i]->GetIndex()->InsertEntry(row_,insr, nullptr);
  }
  table_->GetTableHeap()->InsertTuple(ins, nullptr);
  return false;
}