//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}



void DeleteExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_);
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),index_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row del_;
  RowId deli_;
  if(child_executor_->Next(&del_, &deli_)){
    for(int i=0;i<index_.size();i++){
      Row row_;
      del_.GetKeyFromRow(table_->GetSchema(),index_[i]->GetIndexKeySchema(),row_);
      index_[i]->GetIndex()->RemoveEntry(row_,deli_, nullptr);
    }
    table_->GetTableHeap()->MarkDelete(deli_, nullptr);
  }
  else return false;
}