#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetKeySize(key_size);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int left = 0, right = this->GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int compareRes = KM.CompareKeys(KeyAt(mid), key);
    if (compareRes == 0) {    // mid的key等于要查找的key
      return mid;
    } else if (compareRes > 0) {    // mid的key大于要查找的key
      // 如果mid等于0，或者mid-1的key小于要查找的key，则返回mid的值
      if (mid == 0 || KM.CompareKeys(KeyAt(mid - 1), key) < 0) {
        return mid;
      }
      // 否则缩小范围继续查找
      right = mid - 1;
    } else {    // mid的key小于要查找的key
      // 缩小范围继续查找
      left = mid + 1;
    }
  }
  // 不存在pairs_[i].first>=key，则返回size方便插入
  return this->GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair(this->KeyAt(index), this->ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = this->KeyIndex(key, KM);
  if (index == this->GetSize()) {   // 如果找不到pairs_[i].first>=key的pair，就插入到最后
    this->SetKeyAt(index, key);
    this->SetValueAt(index, value);
    this->IncreaseSize(1);
  } else {    // 如果找到第一个满足pairs_[i].first>=的pair
    if (KM.CompareKeys(KeyAt(index), key) == 0) {    // 如果该键已经存在，就无需插入直接返回
      return this->GetSize();
    }
    // 否则就将index处的pair与其之后的pair往后挪，把新pair插到这里
    int i;
    for (i = this->GetSize(); i > index; i--) {
      this->SetKeyAt(i, this->KeyAt(i - 1));
      this->SetValueAt(i, this->ValueAt(i - 1));
    }
    this->SetKeyAt(i, key);
    this->SetValueAt(i, value);
    this->IncreaseSize(1);
  }
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int total_size = this->GetSize();   // 当前节点的数目
  int rec_size = recipient->GetSize();    // 目标节点的数目
  int size_after_move = total_size / 2;   // 移动后当前节点剩余的数目
  int size_moved = total_size - size_after_move;    // 移动的数目
  int cur_index = size_after_move;

  for (int i = rec_size; cur_index < this->GetSize(); i++, cur_index++) {
    recipient->SetKeyAt(i, this->KeyAt(cur_index));
    recipient->SetValueAt(i, this->ValueAt(cur_index));
  }
  
  // 更新size
  this->IncreaseSize(-size_moved);
  recipient->IncreaseSize(size_moved);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  // 将src的内容复制到该节点后面的可用空间
  int old_size = this->GetSize();   // 没复制之前该节点内的节点个数
  void* dest = pairs_off + pair_size * old_size;    // 该节点可用空间的起始地址
  PairCopy(dest, src, size);    // 将size个pair从src复制到dest
  this->IncreaseSize(size);   // 更新size
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index == this->GetSize() || KM.CompareKeys(key, this->KeyAt(index)) != 0) {    // 没找到
    return false;
  }
  // 找到
  value = this->ValueAt(index);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index == this->GetSize() || KM.CompareKeys(key, this->KeyAt(index)) != 0) {    // 没找到
    return this->GetSize();
  }
  // 找到
  // 将该pair从对应的节点中删掉，后续pair前移
  for (int i = index; i < this->GetSize() - 1; i++) {
    this->SetKeyAt(i, this->KeyAt(i + 1));
    this->SetValueAt(i, this->ValueAt(i + 1));
  }
  this->IncreaseSize(-1);
  return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(pairs_off, this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  // 将第一个pair添加到另一个节点的末尾
  recipient->SetKeyAt(recipient->GetSize(), KeyAt(0));
  recipient->SetValueAt(recipient->GetSize(), ValueAt(0));
  recipient->IncreaseSize(1);
  // 后续节点前移
  for(int i = 0; i < this->GetSize() - 1; i++) {
      SetKeyAt(i, KeyAt(i + 1));
      SetValueAt(i, ValueAt(i + 1));
  }
  this->IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size = this->GetSize();
  this->SetKeyAt(size, key);
  this->SetValueAt(size, value);
  this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  // 将节点中的pair后移
  for (int i = recipient->GetSize() - 1; i >= 0; i--) {
      recipient->SetKeyAt(i + 1, recipient->KeyAt(i));
      recipient->SetValueAt(i + 1, recipient->ValueAt(i));
  }
  // 将该节点的最后一个pair插入到目标节点头
  recipient->SetValueAt(0, ValueAt(this->GetSize() - 1));
  recipient->SetKeyAt(0, KeyAt(GetSize() - 1));
  // 更新size
  recipient->IncreaseSize(1);
  this->IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  // 将节点中的pair后移
  for (int i = this->GetSize() - 1; i >= 0; i--) {
      this->SetKeyAt(i + 1, this->KeyAt(i));
      this->SetValueAt(i + 1, this->ValueAt(i));
  }
  // 插入到节点头
  this->SetKeyAt(0, key);
  this->SetValueAt(0, value);
  IncreaseSize(1);
}