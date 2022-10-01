//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), frames_(replacer_size_+1) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    latch_.lock();

    if (!curr_size_) {  // no evitable frame
        latch_.unlock();
        return false;
    }

    std::shared_ptr<Frame> f_ptr = pop();
    Frame *f = f_ptr.get();
    auto id = f->getFrameId();
    *frame_id = id;
    
    frames_.at(id) = nullptr;
    curr_size_--;
    latch_.unlock();
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id >= 0 && size_t(frame_id) <= replacer_size_, "frame id out of range");
    latch_.lock();
    current_timestamp_++;

    auto f_ptr = frames_.at(frame_id);
    if (!f_ptr) {
        f_ptr = std::make_shared<Frame>(frame_id, k_);
        frames_.at(frame_id) = f_ptr;
    }

    addTimeStamp(f_ptr, current_timestamp_);

    latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(frame_id >= 0 && size_t(frame_id) <= replacer_size_, "frame id out of range.");
    latch_.lock();

    auto f_ptr = frames_.at(frame_id);
    Frame *f = f_ptr.get();
    if (f->getEvitable() == set_evictable) {
        latch_.unlock();
        return;
    }

    f->setEvitable(set_evictable);
    if(set_evictable) {
        curr_size_++;
        push(frame_id);
    } else {
        curr_size_--;
        auto b = f->getTimeStamps().size() < k_;
        remove(b, f->getIndex());
    }

    latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id >= 0 && size_t(frame_id) <= replacer_size_, "frame id out of range.");
    latch_.lock();

    auto f_ptr = frames_.at(frame_id);
    if (!f_ptr) {
        latch_.unlock();
        return;
    }

    Frame *f = f_ptr.get();
    if (!f->getEvitable()) {
        BUSTUB_ASSERT(true, "frame is non-evitable.");
    }

    auto b = f->getTimeStamps().size() < k_;
    remove(b, f->getIndex());

    latch_.unlock();
}

void LRUKReplacer::addTimeStamp(std::shared_ptr<Frame> f_ptr, size_t timestamp) {
    Frame *f = f_ptr.get();

    auto &timestamps = f->getTimeStamps();
    auto n1 = timestamps.size();
    f->addTimeStamp(timestamp);
    auto n2 = timestamps.size();
    if (!f->getEvitable()) return;

    auto i = f->getIndex();
    if (n1 == k_) {
        down(false, i);
    } else if (n2 < k_) {
        down(true, i);
    } else {
        remove(true, i);
        push(f->getFrameId());
    }
}

// 0, 1, 2, 3, 4

void LRUKReplacer::down(bool b, size_t i) {
    std::vector<std::shared_ptr<Frame>> &vec = b ? minheap_L_ : minheap_G_;
    auto n = vec.size();
    while(1) {
        auto j = i * 2 + 1;
        if (j >= n) break;
        if (j + 1 < n && less(b, j + 1, j)) j = j + 1;
        if (less(b, i, j)) break;
        vec.at(i).get()->setIndex(j);
        vec.at(j).get()->setIndex(i);
        std::swap(vec[i], vec[j]);
        i = j;
    }
}

auto LRUKReplacer::remove(bool b, size_t i) -> std::shared_ptr<Frame> {
    std::vector<std::shared_ptr<Frame>> &vec = b ? minheap_L_ : minheap_G_;
    auto n = vec.size();
    auto f_ptr = vec.at(i);    
    vec.at(n-1).get()->setIndex(i);
    std::swap(vec[i], vec[n-1]);
    vec.pop_back();
    down(b, i);
    return f_ptr;
}

auto LRUKReplacer::less(bool b, size_t i, size_t j) -> bool {
    std::vector<std::shared_ptr<Frame>> &vec = b ? minheap_L_ : minheap_G_;
    return vec.at(i).get()->getTimeStamp() < vec.at(j).get()->getTimeStamp();
}

auto LRUKReplacer::pop() -> std::shared_ptr<Frame> {
    if (minheap_L_.size()) return remove(true, 0);
    return remove(false, 0);
}

void LRUKReplacer::push(frame_id_t frame_id) {
    auto f_ptr = frames_.at(frame_id);
    Frame *f = f_ptr.get();
    auto b = f->getTimeStamps().size() < k_;
    size_t i;
    if (b) {
        minheap_L_.push_back(f_ptr);
        i = minheap_L_.size() - 1;
    } else {
        minheap_G_.push_back(f_ptr);
        i = minheap_G_.size() - 1;
    }
    f->setIndex(i);
    up(b, i);
}

void LRUKReplacer::up(bool b, size_t i)  {
    std::vector<std::shared_ptr<Frame>> &vec = b ? minheap_L_ : minheap_G_;
    while(1) {
        auto parent = i / 2;
        if (less(b, parent, i)) break;
        vec.at(parent).get()->setIndex(i);
        vec.at(i).get()->setIndex(parent);
        std::swap(vec[i], vec[parent]);
        if (parent == 0) break;
        i = parent;
    }
}

//===--------------------------------------------------------------------===//
// Frame
//===--------------------------------------------------------------------===//
LRUKReplacer::Frame::Frame(frame_id_t id, size_t k_) : id_(id), evitable_(false), k_(k_) {}

void LRUKReplacer::Frame::addTimeStamp(size_t timestamp) {
    timestamps_.push_back(timestamp);
    if (timestamps_.size() > k_) timestamps_.erase(timestamps_.begin());
}

}  // namespace bustub
