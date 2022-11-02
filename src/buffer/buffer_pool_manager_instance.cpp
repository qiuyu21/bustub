//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  latch_.lock();

  frame_id_t frame_id = -1;
  
  if (free_list_.size()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    auto b = replacer_->Evict(&frame_id);
    if (!b) {
      frame_id = -1;
    } else {
      auto &p = pages_[frame_id];
      auto pid = p.GetPageId();
      if (p.IsDirty()) {
        disk_manager_->WritePage(pid, p.GetData());
      }
      page_table_->Remove(pid);
    }
  }

  if (frame_id != -1) {
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    
    Page *p = &pages_[frame_id];
    p->ResetMemory();
    p->page_id_ = *page_id;
    p->is_dirty_ = false;
    p->pin_count_ = 1;

    latch_.unlock();
    return p;
  }

  latch_.unlock();
  return nullptr; 
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  latch_.lock();

  frame_id_t frame_id;

  auto found = page_table_->Find(page_id, frame_id);
  
  if (found) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    latch_.unlock();
    return &pages_[frame_id];
  }

  if (free_list_.size()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    auto b = replacer_->Evict(&frame_id);

    if (!b) {
      frame_id = -1;
    } else {
      auto &p = pages_[frame_id];
      if (p.IsDirty()) {
        auto pid = p.GetPageId();
        disk_manager_->WritePage(pid, p.GetData());
      }
      page_table_->Remove(p.GetPageId());
    }
  }

  if (frame_id != -1) {
    page_table_->Insert(page_id, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    Page *p = &pages_[frame_id];
    p->ResetMemory();
    p->page_id_ = page_id;
    p->is_dirty_ = false;
    p->pin_count_ = 1;

    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

    latch_.unlock();
    return &pages_[frame_id];
  }

  latch_.unlock();
  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();

  frame_id_t frame_id;
  auto found = page_table_->Find(page_id, frame_id);
  if (!found || !pages_[frame_id].GetPinCount()) {
    latch_.unlock();
    return false;
  }

  pages_[frame_id].pin_count_--;
  if (!pages_[frame_id].GetPinCount()) {
    replacer_->SetEvictable(frame_id, true);
  }

  pages_[frame_id].is_dirty_ |= is_dirty;

  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  latch_.lock();
  frame_id_t frame_id;
  auto found = page_table_->Find(page_id, frame_id);
  if (!found) {
    latch_.unlock();
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;

  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    auto &page = pages_[i];
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
  latch_.unlock();
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  latch_.lock();
  frame_id_t frame_id;
  auto found = page_table_->Find(page_id, frame_id);
  if (!found) {
    latch_.unlock();
    return true;
  }

  auto &p = pages_[frame_id];
  if (p.GetPinCount()) {
    latch_.unlock();
    return false;
  }

  page_table_->Remove(p.GetPageId());
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  p.ResetMemory();
  p.page_id_ = INVALID_PAGE_ID;
  p.pin_count_ = 0;

  DeallocatePage(page_id);

  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

}  // namespace bustub