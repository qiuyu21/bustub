//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1), num_dirs_(1) {
  auto b = std::make_shared<Bucket>(bucket_size_, 0);
  dirToBucket_.insert(std::pair<int, std::shared_ptr<Bucket>>(0, b));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  Bucket *b = dirToBucket_.at(dir_index).get();
  return b->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  // std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetBucket(const K &key) -> std::shared_ptr<Bucket> & {
  auto dir_index = IndexOf(key);
  return dirToBucket_.at(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  Bucket *b = GetBucket(key).get();
  return b->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  Bucket *b = GetBucket(key).get();
  return b->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto b_ptr = GetBucket(key);
  Bucket *b = b_ptr.get();
  auto isFull = b->IsFull();
  auto inserted = b->Insert(key, value);
  if (!inserted) return;
  if (isFull) {
    if (b->GetDepth() == GetGlobalDepth()) {
      // increase the directories
      for (auto i = std::pow(2, GetGlobalDepth()); i < 2 * std::pow(2, GetGlobalDepth()); i++) {
        auto existing_bucket = dirToBucket_.at(((1 << GetGlobalDepth()) - 1) & int(i));
        dirToBucket_.insert(std::pair<int, std::shared_ptr<Bucket>>(i, existing_bucket));
      }
      IncrementGlobalDepth();
    }
    // Create a new bucket
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, b->GetDepth() + 1);
    auto dir_index = 1 << b->GetDepth() | IndexOf(key);
    dirToBucket_.at(dir_index) = new_bucket;
    b->IncrementDepth();
    RedistributeBucket(b_ptr);
  }

  // std::cout << "Added key ";
  // std::cout << key << std::endl;
  // for (auto it = dirToBucket_.begin(); it != dirToBucket_.end(); it++) {
  //   Bucket *b = it->second.get();
  //   std::cout << "Array ";
  //   std::cout << it->first;
  //   std::cout << ":";
  //   for (auto const &i: b->GetItems()) {
  //       std::cout << i.first;
  //       std::cout << " ";
  //   }
  //   std::cout << "\n";
  // }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  Bucket *b = bucket.get();
  auto &item_list = b->GetItems();
  auto n = item_list.size();
  auto it = item_list.begin();
  for (unsigned long i = 0; i < n; i++) {
    auto next_bucket = GetBucket(it->first);
    if (bucket != next_bucket) {
      next_bucket.get()->Insert(it->first, it->second);
      item_list.erase(it++);
    } else {
      it++;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto item_list = GetItems();
  for (auto it = item_list.begin(); it != item_list.end(); it++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto &item_list = GetItems();
  for (auto it = item_list.begin(); it != item_list.end(); it++) {
    if (it->first == key) {
      item_list.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // if (IsFull()) return false;
  auto item_list = GetItems();
  for (auto it = item_list.begin(); it != item_list.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return false;
    }
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
