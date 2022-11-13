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
  auto bkt = std::make_shared<Bucket>(bucket_size_, 0);
  buckets_.push_back(bkt);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int index) const -> int {
  return buckets_[index].get()->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return buckets_[IndexOf(key)].get()->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return buckets_[IndexOf(key)].get()->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t i = IndexOf(key);
  Bucket *b = buckets_[i].get();
  V ph;
  if (b->Find(key, ph)) return;
  if (!b->IsFull()) {
    b->Insert(key, value);
    return;
  }
  HandleFullBucket(i, key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::HandleFullBucket(size_t index, const K &key, const V &value) {
  auto bkt = buckets_[index];
  Bucket *b = bkt.get();
  int local_depth = b->GetDepth();
  int high_bit = 1 << local_depth;
  b->Insert(key, value);
  b->IncrementDepth();

  if (local_depth == GetGlobalDepth()) {
    IncreseDirectory();
    IncrementGlobalDepth();
  }

  auto bkt_new = std::make_shared<Bucket>(bucket_size_, local_depth+1);
  Bucket *nb = bkt_new.get();

  auto &items = b->GetItems();

  for (auto itr = items.begin(); itr != items.end();) { // redistribute
    if ((high_bit & IndexOf(itr->first))) {
      nb->Insert(itr->first, itr->second);
      itr = items.erase(itr);
    } else {
      itr++;
    }
  }

  for (int i = (high_bit-1) & index; i < num_dirs_; i += high_bit) {
    if (i & high_bit) {
      buckets_[i] = bkt_new;
    } else {
      buckets_[i] = bkt;
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncreseDirectory() -> void {
  auto cur_size = num_dirs_;
  auto next_size = cur_size * 2;
  num_dirs_ = next_size;
  buckets_.reserve(next_size);
  for (size_t i = 0; i < size_t(cur_size); i++) buckets_.push_back(buckets_[i]);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto &items = GetItems();
  for (auto it = items.begin(); it != items.end(); it++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto &items = GetItems();
  for (auto it = items.begin(); it != items.end(); it++) {
    if (it->first == key) {
      items.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto &items = GetItems();
  for (auto it = items.begin(); it != items.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return false;
    }
  }
  items.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
