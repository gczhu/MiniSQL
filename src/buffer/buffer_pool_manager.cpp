#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = { 0 };

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
  : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  frame_id_t frame_id;    // 要fetch的数据页在缓冲区中的id
  // 1.获取要fetch的数据页从磁盘到缓冲区的映射
  auto page2frame = page_table_.find(page_id);
  if (page2frame != page_table_.end()) {    // 1.1 如果该映射存在，则表明该数据页在缓冲区中存在
    frame_id = page2frame->second;   // 获取该数据页在缓冲区中的id
    pages_[frame_id].pin_count_++;    // 数据页固定
    replacer_->Pin(frame_id);
    return &pages_[frame_id];   // 直接返回
  } else {    // 1.2 如果该映射不存在，则表明该数据页在缓冲区中不存在，需要把数据页先从磁盘取到缓冲区中
    if (!free_list_.empty()) {    // 先从free_list_表示的缓冲区空闲空间中找一个用来写入的空间
      frame_id = free_list_.front();   // 获取在free_list_表头的空闲空间id
      free_list_.pop_front();   // 将该空闲空间从free_list_中删掉
    } else {    // 如果缓冲区没有空闲空间，就从relacer_中寻找用来替换的一块空间
      bool isVictim = replacer_->Victim(&frame_id);   // 采用LRU策略寻找最近最少使用的数据页用来替换
      if (!isVictim) {    // 如果没有数据页可以被替换，则返回nullptr
        return nullptr;
      }
    }

    // 2.如果被替换的空间中有脏数据，就先把数据写入到磁盘中
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }

    // 3.在page_table_的映射关系中删除原有的关系，插入新的关系
    page_table_.erase(pages_[frame_id].page_id_);
    page_table_.insert(pair<page_id_t, frame_id_t>(page_id, frame_id));

    // 4.更新缓冲区对应空间的数据
    pages_[frame_id].page_id_ = page_id;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);   // 从磁盘读入数据
    pages_[frame_id].pin_count_++;    // 数据页固定
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }
  return nullptr;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // 1.判断缓冲区是否所有的数据页都被固定
  size_t i;
  for (i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ == 0) {
      break;
    }
  }
  if (i == pool_size_) {    // 如果缓冲区中所有的数据页都被固定，则返回 nullptr
    return nullptr;
  }

  // 2.从缓冲区中找一个用来替换的数据页
  frame_id_t frame_id;    // 要被替换的数据页在缓冲区中的id
  if (!free_list_.empty()) {    // 先从free_list_表示的缓冲区空闲空间中找一个用来写入的空间
    frame_id = free_list_.front();   // 获取在free_list_表头的空闲空间id
    free_list_.pop_front();   // 将该空闲空间从free_list_中删掉
  } else {    // 如果缓冲区没有空闲空间，就从relacer_中寻找用来替换的一块空间
    bool isVictim = replacer_->Victim(&frame_id);   // 采用LRU策略寻找最近最少使用的数据页用来替换
    if (!isVictim) {    // 如果没有数据页可以被替换，则返回nullptr
      return nullptr;
    }
  }
  // 3.申请新的数据页，更新缓冲区数据以及page_table_的映射关系
  // 如果被替换的空间中有脏数据，就先把数据写入到磁盘中
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }
  // 从磁盘中申请新的数据页
  page_id = AllocatePage();
  // 在page_table_的映射关系中删除原有的关系，插入新的关系
  page_table_.erase(pages_[frame_id].page_id_);
  page_table_.insert(pair<page_id_t, frame_id_t>(page_id, frame_id));
  // 更新缓冲区对应空间的数据
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_++;    // 数据页固定
  replacer_->Pin(frame_id);
  return &pages_[frame_id];
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 2.   If P does not exist, return true.
  // 3.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 4.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  // 1.获取要delete的数据页从磁盘到缓冲区的映射
  auto page2frame = page_table_.find(page_id);
  if (page2frame != page_table_.end()) {    // 3.如果该映射存在，则表明该数据页在缓冲区中存在
    frame_id_t frame_id = page2frame->second;   // 获取该数据页在缓冲区中的id
    if (pages_[frame_id].pin_count_ > 0) {    // 如果有人在使用该数据页，则无法删除，返回false
      return false;
    } else {    // 4.否则，将数据页删除，并更新page table和free list
      // 如果被删除的空间中有脏数据，就先把数据写入到磁盘中
      if (pages_[frame_id].is_dirty_) {
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      }
      // 删除数据页
      DeallocatePage(pages_[frame_id].page_id_);
      // 更新page table和free list
      page_table_.erase(pages_[frame_id].page_id_);
      free_list_.emplace_back(frame_id);
      // 更新pages数组
      pages_[frame_id].ResetMemory();
      pages_[frame_id].page_id_ = INVALID_PAGE_ID;
      pages_[frame_id].pin_count_ = 0;
      pages_[frame_id].is_dirty_ = false;
      return true;
    }
  } else {    // 2. 如果该映射不存在，则表明该数据页在缓冲区中不存在，直接返回true
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // 获取要unpin的数据页从磁盘到缓冲区的映射
  auto page2frame = page_table_.find(page_id);
  if (page2frame != page_table_.end()) {    // 如果该映射存在，则表明该数据页在缓冲区中存在
    frame_id_t frame_id = page2frame->second;   // 获取该数据页在缓冲区中的id
    pages_[frame_id].is_dirty_ = is_dirty;
    if (pages_[frame_id].pin_count_ == 0) {   // 如果pin count为0，则不应该unpin，返回false
      return false;
    } else {
      pages_[frame_id].pin_count_--;    // 将该数据页在缓冲区中unpin
      if (pages_[frame_id].pin_count_ == 0) {   // 如果缓冲区中unpin后pin count为0，则还需要在replacer中unpin
        replacer_->Unpin(frame_id);
      }
      return true;
    }
  } else {    // 如果该映射不存在，则表明该数据页在缓冲区中不存在，直接返回true
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  // 获取要flush的数据页从磁盘到缓冲区的映射
  auto page2frame = page_table_.find(page_id);
  if (page2frame != page_table_.end()) {    // 如果该映射存在，则表明该数据页在缓冲区中存在
    frame_id_t frame_id = page2frame->second;   // 获取该数据页在缓冲区中的id
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);    // 写入磁盘
    pages_[frame_id].is_dirty_ = false;   // 由于转储到磁盘中，脏数据会被写入，因此要把is_dirty置为false
    return true;
  } else {    // 如果该映射不存在，则表明该数据页在缓冲区中不存在，直接返回false
    return false;
  }
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}