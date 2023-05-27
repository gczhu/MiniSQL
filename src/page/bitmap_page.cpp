#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  page_offset = 0;
  uint32_t byte_index = page_offset / 8;  // 对应所在字节
  uint8_t  bit_index = page_offset % 8;   // 字节里对应的位

  while ( (bytes[byte_index] & (1 << bit_index)) != 0 )
  {
    page_offset++;
    if (page_offset >= 8 * MAX_CHARS)
    {
      return false;
    }
    byte_index = page_offset / 8;
    bit_index = page_offset % 8;
  }

  bytes[byte_index] |= (1 << bit_index);
  return true;
}


/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint32_t byte_index = page_offset / 8;  // 对应所在字节
  uint8_t bit_index = page_offset % 8;   // 字节里对应的位

  if ((bytes[byte_index] & (1 << bit_index)) != 0) {
    bytes[byte_index] = bytes[byte_index] & (~(1 << bit_index));
    next_free_page_ = page_offset;
    page_allocated_--;
    return true;
  } else {
    return false; // 没有分配，不用回收
  }
}


/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;  // 对应所在字节
  uint8_t  bit_index = page_offset % 8;   // 字节里对应的位

  if ( (bytes[byte_index] & (1 << bit_index)) == 0)
  {
    return true;
  } else {
    return false;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
