#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  this->num_pages = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 如果lru_list_为空，则没有可以替换的数据页
  if (lru_list_.empty()) {
    return false;
  }
  // lru_list_中表头元素即为最近最少被访问的数据页，将其删除并更新locate
  *frame_id = lru_list_.front();
  lru_list_.pop_front();
  locate.erase(*frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto locate_iter = locate.find(frame_id);   // 根据frame_id寻找对应的locate元素
  if (locate_iter != locate.end()) {    // 如果frame_id对应的元素在locate中
    auto list_iter = locate_iter->second;   // 获取数据页在lru_list中的位置
    lru_list_.erase(list_iter);    // 从lru_list中删除对应的数据页
    locate.erase(locate_iter);    // 从locate中删除对应的元素
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_list_.size() >= num_pages) {    // 如果超出LRUReplacer能管理页数的上限
    return;
  }
  auto locate_iter = locate.find(frame_id);   // 根据frame_id寻找对应的locate元素
  if (locate_iter == locate.end()) {    // 如果frame_id对应的元素不在locate中
    lru_list_.push_back(frame_id);    // 将对应的数据页插入lru_list的末尾
    locate.insert(pair<frame_id_t, list<frame_id_t>::iterator>(frame_id, prev(lru_list_.end(), 1)));   // 向locate中插入相关的映射信息
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}