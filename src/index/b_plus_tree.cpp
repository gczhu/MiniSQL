#include "index/b_plus_tree.h"

#include <string>
#include <exception>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
  internal_max_size_(internal_max_size) {

  // IndexRootsPage类用于查找index对应的page
  IndexRootsPage* indexRootsPage = reinterpret_cast<IndexRootsPage*>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));
  // 查找该index是否存在
  bool isExist = indexRootsPage->GetRootId(index_id_, &root_page_id_);
  // 如果该index不存在，则将root_page_id赋值为INVALID_PAGE_ID，否则index对应的page_id就会被保存在root_page_id中
  if (!isExist) {
    root_page_id_ = INVALID_PAGE_ID;
  }
  // 解除固定
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  buffer_pool_manager->UnpinPage(root_page_id_, true);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  // 如果该B+树为空，则直接返回false
  if (this->IsEmpty()) {
    return false;
  }
  
  // 调用FindLeafPage查找包含键key的叶节点
  Page* page = this->FindLeafPage(key);
  LeafPage* node = reinterpret_cast<LeafPage*>(page);
  
  // 在叶节点上查找键key对应的记录
  RowId value;    // value对应记录的行号
  bool isExist = node->Lookup(key, value, processor_);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);    // 解除固定
  if (!isExist) {   // 没找到
    return false;
  } else {    // 找到
    result.push_back(value);    // 将value放入result中返回
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if (this->IsEmpty()) {    // 如果当前树为空，则创建一棵新树
    StartNewTree(key, value);
    return true;
  }
  // 否则就将该键值对插入叶节点中
  // 如果已有该键，则插入失败，flag为false
  // 如果没有该键，则插入成功，flag为true
  bool flag = InsertIntoLeaf(key, value, transaction);
  return flag;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // 向内存池申请一个新的数据页
  Page* page = buffer_pool_manager_->NewPage(root_page_id_);
  // 初始化根节点（也是叶节点）
  LeafPage* node = reinterpret_cast<LeafPage*>(page);
  node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  node->SetNextPageId(INVALID_PAGE_ID);
  // 插入键值对信息
  node->Insert(key, value, processor_);
  // 更新root page id
  this->UpdateRootPageId(1);
  // 操作完毕，解除固定
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  // 调用FindLeafPage查找包含键key的叶节点
  Page* page = this->FindLeafPage(key);
  LeafPage* node = reinterpret_cast<LeafPage *>(page);

  // 判断该key是否已经存在于叶节点中，若存在则不允许插入
  RowId value1;
  if (node->Lookup(key, value1, processor_)) {
      buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
      return false;
  }

  // 否则将键值对插入到叶节点上
  node->Insert(key, value, processor_);
  if(node->GetSize() > node->GetMaxSize()) {
      LeafPage* new_node = Split(node, transaction);
      // 将分裂出的叶节点插入到父节点中
      InsertIntoParent(node, new_node->KeyAt(0), new_node, transaction);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  // 向内存池申请一个新的数据页
  page_id_t new_page_id;
  Page* page = buffer_pool_manager_->NewPage(new_page_id);
  // 如果返回值为nullptr，则抛出"out of memory"异常
  if (page == nullptr) {
    throw std::bad_alloc();
  }
  
  // 初始化新内部节点
  InternalPage* new_node = reinterpret_cast<InternalPage*>(page);
  new_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  // 将原节点一半移到新节点
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  // 向内存池申请一个新的数据页
  page_id_t new_page_id;
  Page* page = buffer_pool_manager_->NewPage(new_page_id);
  // 如果返回值为nullptr，则抛出"out of memory"异常
  if (page == nullptr) {
    throw std::bad_alloc();
  }
  
  // 初始化新叶节点
  LeafPage* newNode = reinterpret_cast<LeafPage*>(page);
  newNode->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  // 将原节点一半移到新节点
  node->MoveHalfTo(newNode);
  // 更新叶节点链表
  newNode->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);
  return newNode;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
  Transaction *transaction) {
  // 如果是根节点split，那么就需要新创建一个根节点
  if (old_node->IsRootPage()) {
    // 向内存池申请一个新的数据页
    Page* page = buffer_pool_manager_->NewPage(root_page_id_);
    // 初始化新根节点
    InternalPage* new_root = reinterpret_cast<InternalPage*>(page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    // 连接新根节点与两个孩子节点
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    // 更新root page id
    this->UpdateRootPageId(0);
    // 操作完毕，解除固定
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
  } else {    // 如果不是根节点
    // 根据old_node获取父节点
    Page* page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    InternalPage* parent = reinterpret_cast<InternalPage*>(page->GetData());
    // 插入split得到的新节点的pair信息
    int size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // 如果父节点插入新pair后超出size限制，则递归调用Split & InsertIntoParent
    if (size > internal_max_size_) {
      InternalPage* parent_sibling = Split(parent, transaction);
      this->InsertIntoParent(parent, parent_sibling->KeyAt(0), parent_sibling, transaction);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_sibling->GetPageId(), true);
    } else {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  /* 如果B+树为空，直接返回 */
  if (this->IsEmpty()) {
    return;
  }
  
  /* 调用FindLeafPage查找包含键key的叶节点 */
  Page* page = this->FindLeafPage(key);
  LeafPage* node = reinterpret_cast<LeafPage *>(page->GetData());
  int size = node->GetSize();
  
  /* 调用叶节点的RemoveAndDeleteRecord函数尝试删除键值对，若返回的删除后的size没有变，则说明该键值对不存在，无需删除 */
  if (node->RemoveAndDeleteRecord(key, processor_) == size) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return;
  }
  
  /* 否则删除成功，我们需要判断删除该键值对后是否满足半满条件，若不满足则需要调整 */
  bool need_delete = CoalesceOrRedistribute(node, transaction);
  if (need_delete) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(node->GetPageId());
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  /* 如果是根节点，需要根据删除后的数量决定是否删除该根节点 */
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  
  /* 否则先根据节点内的pair数是否满足半满条件决定是否需要调整 */
  if (node->GetSize() >= node->GetMinSize()) {    // 满足半满条件，无需调整
    return false;
  }

  /* 不满足半满条件，则先得到该节点的兄弟节点，再判断是重新分配还是合并 */
  // 找到父亲节点
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent = reinterpret_cast<InternalPage*>(parent_page->GetData());
  // 找到node下标，进而确定兄弟节点下标
  int index = parent->ValueIndex(node->GetPageId());
  int sibling_index = (index == 0) ? 1 : index - 1;
  // 找到兄弟节点
  Page* sibling_page = buffer_pool_manager_->FetchPage(parent->ValueAt(sibling_index));
  N* sibling = reinterpret_cast<N*>(sibling_page);

  // 根据node及其兄弟节点的size确定是重新分配还是合并
  if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) {   // 合并
    bool parent_need_delete = this->Coalesce(sibling, node, parent, index, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    if (parent_need_delete) {   // 调整父节点
      CoalesceOrRedistribute(parent, transaction);
    }
    return true;
  } else {    // 重新分配
    Redistribute(sibling, node, index);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    return false;
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
  Transaction *transaction) {
  // 当index等于0时neighbor_node是node的右兄弟，其余情况都是左兄弟
  // 为了方便处理，我们将index等于0时的两个指针调换位置，将原来的右兄弟转变成左兄弟
  if (index == 0) {
    swap(node, neighbor_node);
    index = 1;
  }
  // 进行合并
  node->MoveAllTo(neighbor_node);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  // 将node从父节点中移除
  parent->Remove(index);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  // 判断父节点是否需要合并或重调整
  if(parent->GetSize() < parent->GetMinSize()) {
      return true;
  }
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
  Transaction *transaction) {
  // 当index等于0时neighbor_node是node的右兄弟，其余情况都是左兄弟
  // 为了方便处理，我们将index等于0时的两个指针调换位置，将原来的右兄弟转变成左兄弟
  if (index == 0) {
    swap(node, neighbor_node);
    index = 1;
  }
  // 进行合并
  GenericKey* middle_key = parent->KeyAt(index);
  node->MoveAllTo(neighbor_node, middle_key, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  // 将node从父节点中移除
  parent->Remove(index);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  // 判断父节点是否需要合并或重调整
  if(parent->GetSize() < parent->GetMinSize()) {
      return true;
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  // 获取父节点
  Page* page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent = reinterpret_cast<InternalPage*>(page);
  // 如果index等于0，就把兄弟节点的第一个键值对移到node末尾
  // 否则，就把兄弟节点的最后一个键值对移到node开头
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  // 获取父节点
  Page* page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent = reinterpret_cast<InternalPage*>(page);
  // 如果index等于0，就把兄弟节点的第一个键值对移到node末尾
  // 否则，就把兄弟节点的最后一个键值对移到node开头
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // case1：将仅剩的一个儿子调整为根
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    // 移除旧根，得到新根
    InternalPage* root = reinterpret_cast<InternalPage*>(old_root_node);
    page_id_t new_root_id = root->RemoveAndReturnOnlyChild();
    Page* new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
    BPlusTreePage* new_root = reinterpret_cast<BPlusTreePage*>(new_root_page);
    new_root->SetParentPageId(INVALID_PAGE_ID);
    // 更新B+树的根信息
    this->root_page_id_ = new_root->GetPageId();
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    return true;
  } else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    // case2：删掉仅剩的最后一个节点
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page* leftmost_leaf_page = this->FindLeafPage(nullptr, 0, true);
  return IndexIterator(leftmost_leaf_page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page* page = this->FindLeafPage(key, 0, false);
  LeafPage* node = reinterpret_cast<LeafPage*>(page);
  int index = node->KeyIndex(key, processor_);
  return IndexIterator(node->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  Page* rightmost_leaf_page = this->FindLeafPage(nullptr, 1, false);
  LeafPage* node = reinterpret_cast<LeafPage*>(rightmost_leaf_page);
  int index = node->GetSize();
  return IndexIterator(node->GetPageId(), buffer_pool_manager_, index);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  // 从根节点开始查找包含键值key的叶节点
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page);
  while (!node->IsLeafPage()) {
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(node);
    page_id_t next_page_id;
    if (leftMost) { // 如果要找最左侧的节点
      next_page_id = internal_node->ValueAt(0);
    } else if (page_id == 1) {    // page_id等于1表示要找最右侧的节点
      next_page_id = internal_node->ValueAt(internal_node->GetSize() - 1);
    } else {
      next_page_id = internal_node->Lookup(key, processor_);
    }
    Page* next_page = buffer_pool_manager_->FetchPage(next_page_id);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage*>(next_page);
  }
  return reinterpret_cast<Page*>(node);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  IndexRootsPage* index_root_page = reinterpret_cast<IndexRootsPage*>
    (buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record == 0) {
    index_root_page->Update(index_id_, root_page_id_);
  } else {
    index_root_page->Insert(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}