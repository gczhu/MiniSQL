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
  Row del_;
  RowId deli_;
  if(child_executor_->Next(&del_, &deli_)){
    for(int i=0;i<index_info_.size();i++){
      Row row_;
      del_.GetKeyFromRow(table_->GetSchema(),index_info_[i]->GetIndexKeySchema(),row_);
      index_info_[i]->GetIndex()->RemoveEntry(row_,deli_, nullptr);
    }
    table_->GetTableHeap()->MarkDelete(deli_, nullptr);
    Row upd_= GenerateUpdatedTuple(del_);
    for(int i=0;i<table_->GetSchema()->GetColumnCount();i++){
      if(table_->GetSchema()->GetColumn(i)->IsUnique()){
        for(int j=0;j<index_info_.size();j++){
          if(i==index_info_[j]->GetIndexKeySchema()->GetColumn(0)->GetTableInd()){
            Row row_;
            std::vector<RowId> mul;
            upd_.GetKeyFromRow(table_->GetSchema(),index_info_[j]->GetIndexKeySchema(),row_);
            index_info_[j]->GetIndex()->ScanKey(row_,mul,nullptr);
            if(!mul.empty())return false;
          }
        }
      }
    }
    for(int i=0;i<index_info_.size();i++){
      Row row_;
      upd_.GetKeyFromRow(table_->GetSchema(),index_info_[i]->GetIndexKeySchema(),row_);
      index_info_[i]->GetIndex()->InsertEntry(row_,deli_, nullptr);
    }
    table_->GetTableHeap()->InsertTuple(upd_, nullptr);
  }
  else return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  Row upd_ = src_row;
  for(auto i:plan_->GetUpdateAttr()){
    Field f=i.second->Evaluate(&upd_);
    *(upd_.fields_[i.first]) = f;
  }
  return upd_;
}