#include "executor/executors/index_scan_executor.h"
#include <algorithm>
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto predicate = plan_->GetPredicate();
  while(predicate->GetChildAt(0)->GetType() == ExpressionType::LogicExpression || predicate->GetChildAt(0)->GetType() == ExpressionType::ComparisonExpression){
    auto temp_predicate = predicate->GetChildAt(1);
    auto col_idx = dynamic_pointer_cast<ColumnValueExpression>(temp_predicate->GetChildAt(0))->GetColIdx();
    for(auto it : plan_->indexes_){
      if(it->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == col_idx){
        std::vector<RowId> temp_result;
        std::vector<Field> Fields;
        Fields.push_back(temp_predicate->GetChildAt(1)->Evaluate(nullptr));
        Row _row(Fields);
        it->GetIndex()->ScanKey(_row,temp_result, nullptr,dynamic_pointer_cast<ComparisonExpression>(temp_predicate)->GetComparisonType());
        if(result.empty()){
          result.assign(temp_result.begin(),temp_result.end());
        }
        else {
          std::vector<RowId> temp;
          temp.assign(result.begin(),result.end());
          result.clear();
          std::set_intersection(temp_result.begin(),temp_result.end(),temp.begin(),temp.end(),inserter(result,result.begin()));
        }
      }
    }
    predicate = predicate->GetChildAt(0);
  }
  auto col_idx = dynamic_pointer_cast<ColumnValueExpression>(predicate->GetChildAt(0))->GetColIdx();
  for(auto it : plan_->indexes_){
    if(it->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == col_idx){
      std::vector<RowId> temp_result;
      std::vector<Field> Fields;
      Fields.push_back(predicate->GetChildAt(1)->Evaluate(nullptr));
      Row _row(Fields);
      it->GetIndex()->ScanKey(_row,temp_result, nullptr,dynamic_pointer_cast<ComparisonExpression>(predicate)->GetComparisonType());
      std::sort(temp_result.begin(), temp_result.end());
      if(result.empty()){
        result.assign(temp_result.begin(),temp_result.end());
      }
      else {
        std::vector<RowId> temp;
        temp.assign(result.begin(),result.end());
        result.clear();
        std::set_intersection(temp_result.begin(),temp_result.end(),temp.begin(),temp.end(),inserter(result,result.begin()));
      }
    }
  }
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  TableInfo* table_;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_);
  for(auto i=result.begin();i!=result.end();i++){
    if(plan_->need_filter_){
      Row row_(*i);
      table_->GetTableHeap()->GetTuple(&row_,nullptr);
      if(plan_->GetPredicate()->Evaluate(&row_).CompareEquals(Field(kTypeInt,kTrue)) == kTrue){
        *row = row_;
        *rid = row_.GetRowId();
        return true;
      }
    }
    else{
      Row row_(*i);
      table_->GetTableHeap()->GetTuple(&row_,nullptr);
      *row = row_;
      *rid = row_.GetRowId();
      return true;
    }
  }
  return false;
}
