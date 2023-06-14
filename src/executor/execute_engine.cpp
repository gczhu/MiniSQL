#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "parser/syntax_tree_printer.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  /** After you finish the code for the CatalogManager section,
   *  you can uncomment the commented code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  if(ast == nullptr||ast->child_ == nullptr)
    return DB_FAILED;
  std::string name_(ast->child_->val_);
  auto got=dbs_.find(name_);
  if(got!=dbs_.end())
    return DB_ALREADY_EXIST;
  DBStorageEngine *Cdata= new DBStorageEngine(name_);
  for(auto i:dbs_)std::cout<<i.first<<std::endl;
  dbs_.emplace(name_,Cdata);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  if(ast == nullptr||ast->child_ == nullptr)
    return DB_FAILED;
  std::string name_(ast->child_->val_);
  auto got=dbs_.find(name_);
  if(got==dbs_.end())
    return DB_NOT_EXIST;
  if(current_db_ == name_)
    current_db_ = "";
  dbs_[name_]->~DBStorageEngine();
  dbs_.erase(name_);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if(ast == nullptr)
    return DB_FAILED;
  for(std::unordered_map<std::string, DBStorageEngine *>::iterator it=dbs_.begin();it!=dbs_.end();it++)
    std::cout<<it->first<<std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  if(ast == nullptr||ast->child_ == nullptr)
    return DB_FAILED;
  std::string name_(ast->child_->val_);
  if(dbs_[name_] == nullptr)
    return DB_NOT_EXIST;
  current_db_ = name_;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(ast == nullptr || dbs_[current_db_] == nullptr)
    return DB_FAILED;
  std::vector<TableInfo *> tables;
  if(dbs_[current_db_]->catalog_mgr_->GetTables(tables)!=DB_SUCCESS)
    return DB_FAILED;
  std::cout<<tables.size()<<std::endl;
  for(int i=0;i<tables.size();i++)
    std::cout<<tables[i]->GetTableName()<<std::endl;
  if(!tables.size())std::cout<<"There is no table in "<<current_db_<<"."<<std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(ast == nullptr || current_db_ == "")
    return DB_FAILED;
  DBStorageEngine* current_ = dbs_[current_db_];
  if(current_ == nullptr)
    return DB_FAILED;
  string new_name(ast->child_->val_);
  TableInfo* new_info;
  if(current_->catalog_mgr_->GetTable(new_name, new_info) == DB_SUCCESS)
  {
    std::cout << "Table has already existed" << std::endl;
    return DB_FAILED;
  }
  TableInfo* tmp_table_info;
  pSyntaxNode kNCD_node = ast->child_->next_->child_;
  vector<Column*> tmp_column_vec;
  vector<string> column_names;
  unordered_map<string, bool> if_unique;
  unordered_map<string, bool> if_primary_key;
  unordered_map<string, string> type_of_column;
  unordered_map<string, int> char_size;
  vector<string> uni_keys;
  vector<string> pri_keys;
  while(kNCD_node != nullptr && kNCD_node->type_ == kNodeColumnDefinition)
  {
    std::string kNCD_ifunique;
    if(kNCD_node->val_ == nullptr)
      kNCD_ifunique = "";
    else
      kNCD_ifunique = kNCD_node->val_;
    string kNCD_columname(kNCD_node->child_->val_);
    string kNCD_typename(kNCD_node->child_->next_->val_);
    column_names.push_back(kNCD_columname);
    type_of_column[kNCD_columname] = kNCD_typename;
    if_primary_key[kNCD_columname] = false;
    if(kNCD_ifunique == "unique")
    {
      if_unique[kNCD_columname] = true;
      uni_keys.push_back(kNCD_columname);
    }
    else
      if_unique[kNCD_columname] = false;
    if(kNCD_typename == "char")
    {
      int i=0;
      char *num=kNCD_node->child_->next_->child_->val_;
      while(num[i]!='\0'){
        if(num[i++]=='.'){
          std::cout<<"char size shouldn't be double."<<std::endl;
          return DB_FAILED;
        }
      }
      char_size[kNCD_columname] = stoi(num);
      if(char_size[kNCD_columname] <= 0)
      {
        std::cout << "char size < 0 !" << endl;
        return DB_FAILED;
      }
    }
    kNCD_node = kNCD_node->next_;
  }
  if(kNCD_node != nullptr)
  {
    pSyntaxNode primary_keys_node = kNCD_node->child_;
    while(primary_keys_node)
    {
      string primary_key_name(primary_keys_node->val_);
      if_primary_key[primary_key_name] = true;
      pri_keys.push_back(primary_key_name);
      uni_keys.push_back(primary_key_name);
      primary_keys_node = primary_keys_node->next_;
    }
  }
  int column_index_counter = 0;
  for(int i=0;i<column_names.size();i++)
  {
    Column* new_column;
    if(type_of_column[column_names[i]] == "int")
    {
      if(if_unique[column_names[i]] || if_primary_key[column_names[i]])
      {
        new_column = new Column(column_names[i], TypeId::kTypeInt, column_index_counter,
                                false, true);
      }
      else
      {
        new_column = new Column(column_names[i], TypeId::kTypeInt, column_index_counter,
                                false, false);
      }
    }
    else if(type_of_column[column_names[i]] == "float")
    {
      if(if_unique[column_names[i]] || if_primary_key[column_names[i]])
      {
        new_column = new Column(column_names[i], TypeId::kTypeFloat, column_index_counter,
                                false, true);
      }
      else
      {
        new_column = new Column(column_names[i], TypeId::kTypeFloat, column_index_counter,
                                false, false);
      }
    }
    else if(type_of_column[column_names[i]] == "char") {
      if (if_unique[column_names[i]] || if_primary_key[column_names[i]]) {
        new_column = new Column(column_names[i], TypeId::kTypeChar, char_size[column_names[i]], column_index_counter,
                                false, true);
      } else {
        new_column = new Column(column_names[i], TypeId::kTypeChar, char_size[column_names[i]], column_index_counter,
                                false, false);
      }
    }
    else
    {
      std::cout << "Unknown Typename" << type_of_column[column_names[i]] << endl;
      return DB_FAILED;
    }
    column_index_counter++;
    tmp_column_vec.push_back(new_column);
  }
  Schema* new_schema = new Schema(tmp_column_vec);

  dberr_t message;
  message = current_->catalog_mgr_->CreateTable(new_name,new_schema, nullptr, tmp_table_info);
  if(message != DB_SUCCESS)
    return message;
  CatalogManager* mgr_ = current_->catalog_mgr_;
  for(int i=0;i<column_names.size();i++)
  {
    if(if_primary_key[column_names[i]])
    {
      string stp_index_name = type_of_column[column_names[i]] + "_index";
      vector<string> index_columns_stp = {column_names[i]};
      IndexInfo* stp_index_info;
      message = mgr_->CreateIndex(new_name, stp_index_name, index_columns_stp,nullptr, stp_index_info,"bptree");
      if(message != DB_SUCCESS)
        return message;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr)
    return DB_FAILED;
  std::string name_(ast->child_->val_);
  return dbs_[current_db_]->catalog_mgr_->DropTable(name_);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  std::vector<TableInfo *> tables;
  CatalogManager* mgr_= dbs_[current_db_]->catalog_mgr_;
  if(mgr_->GetTables(tables)!=DB_SUCCESS)
    return DB_FAILED;
  for(int i=0;i<tables.size();i++){
    std::vector<IndexInfo *> indexs;
    mgr_->GetTableIndexes(tables[i]->GetTableName(),indexs);
    for(int j=0;j<indexs.size();j++)
      std::cout<<tables[i]->GetTableName()<<"."<<indexs[j]->GetIndexName()<<std::endl;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(ast == nullptr || current_db_ == "")
    return DB_FAILED;
  CatalogManager* mgr = dbs_[current_db_]->catalog_mgr_;
  std::string tname_(ast->child_->next_->val_);
  std::string iname_(ast->child_->val_);
  TableInfo* table_;
  dberr_t message = mgr->GetTable(tname_, table_);
  if(message != DB_SUCCESS)
    return message;
  vector<std::string> vec_index_colum_lists;
  pSyntaxNode pSnode_ = ast->child_->next_->next_->child_;
  while(pSnode_)
  {
    vec_index_colum_lists.push_back(string(pSnode_->val_));
    pSnode_ = pSnode_->next_;
  }
  //暂时没考虑index所列的列里没有primary和unique的情况，真出事了再回来改
  Schema* target_schema = table_->GetSchema();
  for(string tmp_colum_name: vec_index_colum_lists)
  {
    uint32_t tmp_index;
    message = target_schema->GetColumnIndex(tmp_colum_name, tmp_index);
    if(message != DB_SUCCESS)
      return message;
  }
  IndexInfo* new_indexinfo;
  message = mgr->CreateIndex(tname_,iname_, vec_index_colum_lists, nullptr, new_indexinfo,"bptree");
  if(message != DB_SUCCESS)
    return message;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr ||dbs_[current_db_] == nullptr)
    return DB_FAILED;
  std::vector<TableInfo *> tables;
  CatalogManager* mgr_= dbs_[current_db_]->catalog_mgr_;
  std::string name_(ast->child_->val_);
  if(mgr_->GetTables(tables)!=DB_SUCCESS)
    return DB_FAILED;
  bool ok=0;
  for(int i=0;i<tables.size();i++){
    if(mgr_->DropIndex(tables[i]->GetTableName(),name_)==DB_SUCCESS)ok=1;
  }
  if(!ok)return DB_FAILED;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr)
    return DB_FAILED;
  std::string name_(ast->child_->val_);
  fstream exe(name_);
  if(!exe.is_open()){
    std::cout<<"open "<<name_<<" "<<"failedly"<<std::endl;
    return DB_FAILED;
  }
  int max_size=1024;
  char sql_[max_size];
  while(1){
    int num_=0;
    char tmp;
    do {
      if (exe.eof()) return DB_SUCCESS;
      tmp = exe.get();
      sql_[num_++] = tmp;
      if(num_>max_size){
        std::cout<<"Too long for one command."<<std::endl;
        return DB_FAILED;
      }
    }while(tmp!=';');
    sql_[num_]='\0';
    YY_BUFFER_STATE bp= yy_scan_string(sql_);
    if (bp == nullptr)
    {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);
    MinisqlParserInit();
    yyparse();
    if (MinisqlParserGetError())
      std::cout<<MinisqlParserGetErrorMessage();
    auto result = this->Execute(MinisqlGetParserRootNode());
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    this->ExecuteInformation(result);
    if (result == DB_QUIT) {
      break;
    }
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  if(ast == nullptr)
    return DB_FAILED;
  return DB_QUIT;
}
