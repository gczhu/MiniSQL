//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}


/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_);
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),index_info_);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row* row_;
  while(child_executor_->Next(row_, nullptr)){
    bool ok=0;
    for(int i=0;i<index_info_.size();i++){
      if(index_info_[i]->GetIndex()->RemoveEntry(*row_,row_->GetRowId(),nullptr)!=DB_SUCCESS)
        ok=1;
    }
    if(!ok){
      table_->GetTableHeap()->MarkDelete(row_->GetRowId(),nullptr);
      for(int i=0;i<index_info_.size();i++){
        if(index_info_[i]->GetIndex()->InsertEntry(*row_,row_->GetRowId(),nullptr)!=DB_SUCCESS)
          ok=1;
      }
      if(!ok){
        table_->GetTableHeap()->InsertTuple(*row_,nullptr);
        return true;
      }

    }
  }
  return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  return Row(src_row);
}