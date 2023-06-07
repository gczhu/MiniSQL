#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
 /*****************************************************************************
  * HELPER METHODS AND UTILITIES
  *****************************************************************************/
  /*
   * Init method after creating a new internal page
   * Including set page type, set current size, set page id, set parent id, set
   * max page size and set key size
   */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetKeySize(key_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
 /*
  * Find and return the child pointer(page_id) which points to the child page
  * that contains input "key"
  * Start the search from the second key(the first key should always be invalid)
  * 用了二分查找
  */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1, right = this->GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int compareRes = KM.CompareKeys(KeyAt(mid), key);
    if (compareRes == 0) {    // mid的key等于要查找的key
      return this->ValueAt(mid);
    } else if (compareRes > 0) {    // mid的key大于要查找的key
      // 如果mid等于1，或者mid-1的key小于等于要查找的key，则返回mid-1的值
      if (mid == 1 || KM.CompareKeys(KeyAt(mid - 1), key) <= 0) {
        return this->ValueAt(mid - 1);
      }
      // 否则缩小范围继续查找
      right = mid - 1;
    } else {    // mid的key小于要查找的key
      // 如果mid等于size-1，或者mid+1的key大于要查找的key，则返回mid的值
      if (mid == GetSize() - 1 || KM.CompareKeys(KeyAt(mid + 1), key) > 0) {
        return this->ValueAt(mid);
      }
      // 否则缩小范围继续查找
      left = mid + 1;
    }
  }
  return INVALID_PAGE_ID;   // 未找到
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
 /*
  * Populate new root page with old_value + new_key & new_value
  * When the insertion cause overflow from leaf page all the way upto the root
  * page, you should create a new root page and populate its elements.
  * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
  */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  this->SetValueAt(0, old_value);
  this->SetKeyAt(1, new_key);
  this->SetValueAt(1, new_value);
  this->SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // 首先找到old_value对应的节点，然后把它之后的所有节点向后移动
  int i, old_value_index = this->ValueIndex(old_value), old_size = this->GetSize();
  for (i = old_size; i > old_value_index + 1; i--) {
    GenericKey* key = this->KeyAt(i - 1);
    page_id_t value = this->ValueAt(i - 1);
    this->SetKeyAt(i, key);
    this->SetValueAt(i, value);
  }
  // 插入new_key和new_value
  this->SetKeyAt(i, new_key);
  this->SetValueAt(i, new_value);
  this->SetSize(old_size + 1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
 /*
  * Remove half of key & value pairs from this page to "recipient" page
  */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int total_size = this->GetSize();   // 当前节点的数目
  int rec_size = recipient->GetSize();    // 目标节点的数目
  int size_after_move = total_size / 2;   // 移动后当前节点剩余的数目
  int size_moved = total_size - size_after_move;    // 移动的数目
  int cur_index = size_after_move;
  
  for (int i = rec_size; cur_index < GetSize(); i++, cur_index++) {
    // 移动
    recipient->SetKeyAt(i, this->KeyAt(cur_index));
    recipient->SetValueAt(i, this->ValueAt(cur_index));
    // 更新孩子节点的指针
    Page* page = buffer_pool_manager->FetchPage(this->ValueAt(cur_index));
    BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page);
    node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(node->GetPageId(), true);
  }
  
  // 更新size
  this->IncreaseSize(-size_moved);
  recipient->IncreaseSize(size_moved);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 * 由于MoveHalfTo和MoveAllTo都在内部实现了，因此就不需要用到该功能函数了
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
 /*
  * Remove the key & value pair in internal page according to input index(a.k.a
  * array offset)
  * NOTE: store key&value pair continuously after deletion
  */
void InternalPage::Remove(int index) {
  for (int i = index; i < this->GetSize() - 1; i++) {
    this->SetKeyAt(i, this->KeyAt(i + 1));
    this->SetValueAt(i, this->ValueAt(i + 1));
  }
  this->IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  this->IncreaseSize(-1);
  assert(this->GetSize() == 0);
  return this->ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
 /*
  * Remove all key & value pairs from this page to "recipient" page.
  * The middle_key is the separation key you should get from the parent. You need
  * to make sure the middle key is added to the recipient to maintain the invariant.
  * You also need to use BufferPoolManager to persist changes to the parent page id for those
  * pages that are moved to the recipient
  */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int rec_size = recipient->GetSize();    // 目标节点的数目
  for (int i = 0; i < this->GetSize(); i++) {
    // 移动
    recipient->SetKeyAt(rec_size + i, this->KeyAt(i));
    recipient->SetValueAt(rec_size + i, this->ValueAt(i));
    // 更新孩子节点的指针
    Page* page = buffer_pool_manager->FetchPage(this->ValueAt(i));
    BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page);
    node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(node->GetPageId(), true);
  }
  
  // 更新size
  recipient->IncreaseSize(this->GetSize());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
 /*
  * Remove the first key & value pair from this page to tail of "recipient" page.
  *
  * The middle_key is the separation key you should get from the parent. You need
  * to make sure the middle key is added to the recipient to maintain the invariant.
  * You also need to use BufferPoolManager to persist changes to the parent page id for those
  * pages that are moved to the recipient
  */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
  BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(this->KeyAt(0), this->ValueAt(0), buffer_pool_manager);
  this->Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 在末尾插入新pair
  int size = this->GetSize();
  this->SetKeyAt(size, key);
  this->SetValueAt(size, value);
  this->IncreaseSize(1);
  // 修改孩子节点的父指针
  Page* page = buffer_pool_manager->FetchPage(value);   // 取出value对应的page
  BPlusTreePage* child = reinterpret_cast<BPlusTreePage *>(page->GetData());    // 取出page的内容，即该pair的value对应的孩子节点信息
  child->SetParentPageId(this->GetPageId());    // 更新孩子节点的父亲指针
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);    // 由于fecth page时将这个数据页固定了，现在处理完了应该解除固定
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
  BufferPoolManager *buffer_pool_manager) {
  // 目标节点pair后移
  recipient->SetKeyAt(0, middle_key);
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->SetKeyAt(i, recipient->KeyAt(i - 1));
    recipient->SetValueAt(i, recipient->ValueAt(i - 1));
  }
  // 将当前节点的最后一个节点放到目标节点头
  recipient->SetKeyAt(0, this->KeyAt(this->GetSize() - 1));
  recipient->SetValueAt(0, this->ValueAt(this->GetSize() - 1));
  // 更新孩子节点父指针
  Page* page = buffer_pool_manager->FetchPage(this->ValueAt(this->GetSize() - 1));
  BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page);
  node->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(ValueAt(GetSize() - 1), true);
  // 更新size
  this->IncreaseSize(-1);
  recipient->IncreaseSize(1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  // 获取middle_key
  Page* parent_page = buffer_pool_manager->FetchPage(this->GetParentPageId()); 
  InternalPage* parent = reinterpret_cast<InternalPage*>(parent);
  int index = parent->ValueIndex(this->GetPageId());
  GenericKey* middle_key = parent->KeyAt(index);
  buffer_pool_manager->UnpinPage(parent->GetPageId(), false);
  // 当前节点pair后移
  this->SetKeyAt(0, middle_key);
  for (int i = this->GetSize(); i > 0; i--) {
    this->SetKeyAt(i, this->KeyAt(i - 1));
    this->SetValueAt(i, this->ValueAt(i - 1));
  }
  // 在当前节点的开头插入新pair
  this->SetValueAt(0, value);
  this->IncreaseSize(1);
  // 更新孩子节点父指针
  Page* page = buffer_pool_manager->FetchPage(value);
  BPlusTreePage* node = reinterpret_cast<BPlusTreePage*>(page);
  node->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}