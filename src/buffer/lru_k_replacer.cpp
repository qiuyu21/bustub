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

#include <iostream>
#include "buffer/lru_k_replacer.h"


namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), frames_(num_frames) {
    for (int i = 0; i < static_cast<int>(replacer_size_); i++) {
        frames_.at(i) = std::make_shared<Frame>(k_);
    }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    if (!curr_size_) return false;
    auto &vec = less_than_k_.size() ? less_than_k_ : equal_to_k_;
    assert(vec.size());
    int i = 0;
    for (int j = 1; j < static_cast<int>(vec.size()); j++) {
        if (frames_.at(vec.at(j)).get()->GetTimeStamp() < frames_.at(vec.at(i)).get()->GetTimeStamp()) {
            i = j;
        }
    }
    *frame_id = vec.at(i);
    Frame *frame = frames_.at(*frame_id).get();
    assert(frame->GetEvictable());
    vec.erase(vec.begin() + i);
    curr_size_--;
    frame->ClearTimeStamps();
    frame->SetEvictable(false);
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    Frame *frame = frames_.at(frame_id).get();
    auto &timestamps = frame->GetTimeStamps();
    auto bef = timestamps.size();
    frame->AddTimeStamp(current_timestamp_);
    auto aft = timestamps.size();
    current_timestamp_++;
    if (!frame->GetEvictable()) return;
    if (bef < k_ && aft == k_) {
        auto found = false;
        for (auto itr = less_than_k_.begin();itr != less_than_k_.end(); itr++) {
            if (*itr == frame_id) {
                found = true;
                less_than_k_.erase(itr);
                break;
            }
        }
        assert(found);
        equal_to_k_.push_back(frame_id);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    Frame *frame = frames_.at(frame_id).get();
    if (frame->GetEvictable() == set_evictable) return;
    frame->SetEvictable(set_evictable);
    if (set_evictable) {
        curr_size_++;
        frame->GetTimeStamps().size() < k_ ? less_than_k_.push_back(frame_id) : equal_to_k_.push_back(frame_id);
    } else {
        curr_size_--;
        auto &vec = frame->GetTimeStamps().size() < k_ ? less_than_k_ : equal_to_k_;
        auto found = false;
        for (auto itr = vec.begin(); itr != vec.end(); itr++) {
            if (*itr == frame_id) {
                found = true;
                vec.erase(itr);
                break;
            }
        }
        assert(found);
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    Frame *frame = frames_.at(frame_id).get();
    if (frame->GetEvictable()) {
        curr_size_--;
        auto &vec = frame->GetTimeStamps().size() < k_ ? less_than_k_ : equal_to_k_;
        auto found = false;
        for (auto itr = vec.begin(); itr != vec.end(); itr++) {
            if (*itr == frame_id) {
                found = true;
                vec.erase(itr);
                break;
            }
        }
        assert(found);
    }
    frame->ClearTimeStamps();
    frame->SetEvictable(false);
}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_; 
}

//===--------------------------------------------------------------------===//
// Frame
//===--------------------------------------------------------------------===//
LRUKReplacer::Frame::Frame(size_t k) : k_(k), evictable_(false) {}

void LRUKReplacer::Frame::AddTimeStamp(size_t timestamp) {
    timestamps_.push_back(timestamp);
    if (timestamps_.size() > k_) timestamps_.erase(timestamps_.begin());
}

}  // namespace bustub

