#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  char* meta = GetMetaData();
  DiskFileMetaPage *MetaPage = reinterpret_cast<DiskFileMetaPage *>(meta);
  uint32_t number_extents = MetaPage->GetExtentNums();
  uint32_t MAX_VALID_EXTEND_NUM = (PAGE_SIZE - 8) / 4;

  uint32_t i;
  for (i = 0; i < number_extents ; i++) {
    if (MetaPage->GetExtentUsedPage(i) < BITMAP_SIZE) {
      break ;
    }
  }

  /* the existed extents are full */
  if (i == number_extents) {
    if (i < MAX_VALID_EXTEND_NUM) {   // create a new extent to allocate the page
      std::unique_ptr<BitmapPage<PAGE_SIZE>> bitmap = std::make_unique<BitmapPage<PAGE_SIZE>>();
      uint32_t ofs;
      bitmap->AllocatePage(ofs);
      page_id_t bitmap_location = 1 + number_extents * (BITMAP_SIZE + 1);
      char* temp = reinterpret_cast<char*>(bitmap.get());
      WritePhysicalPage(bitmap_location, temp);

      MetaPage->num_allocated_pages_++;
      MetaPage->extent_used_page_[i] = 1;
      MetaPage->num_extents_++;

      return (MetaPage->num_allocated_pages_ - 1);
    } else {  // all the extents are full
      return INVALID_PAGE_ID;
    }
  }

  /* the loop broke, and we found a suitable extent to allocate a page in the existing extents */
  page_id_t bitmap_location = 1 + i * (BITMAP_SIZE + 1);
  char temp[PAGE_SIZE];
  ReadPhysicalPage(bitmap_location, temp); // read out the content in the Bitmap
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(temp);
  uint32_t page_offset;
  bitmap->AllocatePage(page_offset);
  WritePhysicalPage(bitmap_location, temp);
  page_id_t logical_page_id = page_offset + i * BITMAP_SIZE;

  MetaPage->num_allocated_pages_++;
  MetaPage->extent_used_page_[i] += 1;
  return logical_page_id;
}


/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  /*use IsPageFree function to test the condition*/
  bool free = IsPageFree(logical_page_id);
  if (free)
  {
    return ;
  } else {
    uint32_t page_offset = logical_page_id % BITMAP_SIZE;
    page_id_t extent_id = logical_page_id / BITMAP_SIZE;  // find the specific extent
    page_id_t Bitmap_location = 1 + extent_id * (BITMAP_SIZE + 1);
    char temp[PAGE_SIZE];
    ReadPhysicalPage(Bitmap_location,temp); // read out the content in the BitMap
    BitmapPage<PAGE_SIZE> *Bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(temp);
    Bitmap->DeAllocatePage(page_offset);
    WritePhysicalPage(Bitmap_location,temp);  // write the Bitmap back

    /*Don't forget to change the meta-data*/
    char* meta = GetMetaData();
    DiskFileMetaPage *MetaPage = reinterpret_cast<DiskFileMetaPage *>(meta);
    MetaPage->num_allocated_pages_ --;
    MetaPage->extent_used_page_[extent_id] -= 1;
    return ;
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  page_id_t extent_id = logical_page_id / BITMAP_SIZE;  // find the specific extent
  page_id_t Bitmap_location = 1 + extent_id * (BITMAP_SIZE + 1);
  /*As the bitmap doesn't have a logical page_id, we can only read it through physical page_id*/
  /* and use the function ReadPhysicalPage() */
  char temp[PAGE_SIZE];
  ReadPhysicalPage(Bitmap_location,temp); // read out the content in the BitMap
  BitmapPage<PAGE_SIZE> *Bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(temp);
  /* calculate the page_offset in the BitMap*/
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;
  return Bitmap->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t physical_page_id;
  physical_page_id = logical_page_id + 2 + logical_page_id / BITMAP_SIZE;   //一开始差距1位图页1元数据页，之后每一个BITMAP_SIZE再加1
  return physical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}