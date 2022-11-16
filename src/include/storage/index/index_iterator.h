//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {

  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  
  public:
    IndexIterator(std::list<MappingType> *lst, BufferPoolManager *buffer_pool_manager, page_id_t pid);

    IndexIterator(std::list<MappingType> *lst, BufferPoolManager *buffer_pool_manager, page_id_t pid, KeyComparator &comparator, const KeyType &key);

    IndexIterator(std::list<MappingType> *lst);

    ~IndexIterator();  // NOLINT

    auto IsEnd() -> bool;

    auto operator*() -> const MappingType &;

    auto operator++() -> IndexIterator &;

    auto operator==(const IndexIterator &itr) const -> bool;

    auto operator!=(const IndexIterator &itr) const -> bool;

  private:
    std::list<MappingType> *lst_;

    BufferPoolManager *buffer_pool_manager_;

    page_id_t pid_;

    page_id_t next_pid_;

    typename std::list<MappingType>::iterator itr_;

    auto ReadPage(page_id_t pid) -> void;
};

}  // namespace bustub
