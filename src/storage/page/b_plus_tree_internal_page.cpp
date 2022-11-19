//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */ 
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto n = GetSize();
  for (auto i = 0; i < n; i++) {
    if (array_[i].second == value) return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto n = GetSize();
  for (auto i = 1; i < n; i++) {
    if (comparator(key, array_[i].first) < 0) return array_[i-1].second;
  }
  return array_[n-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) -> int {
  auto n = GetSize();
  for (auto i = 0; i < n; i++) {
    if (array_[i].second == old_value) {
      for (auto j = n - 1; j > i; j--) array_[j+1] = array_[j];
      array_[i+1].first = new_key;
      array_[i+1].second = new_value;
      break;
    }
  }
  SetSize(n+1);
  return n+1;
}


/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  auto n = GetSize();
  auto i = n / 2;
  auto size = n - i;
  recipient->CopyNFrom(&array_[i], size, buffer_pool_manager);
  SetSize(i);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  auto pid = GetPageId();
  auto n = GetSize();
  for (auto i = 0; i < size; i++) {
    array_[n+i] = items[i];
    auto child_page_id = items[i].second;
    Page *page = buffer_pool_manager->FetchPage(child_page_id);
    BPlusTreeInternalPage *child_page = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    child_page->SetParentPageId(pid);
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  auto n = GetSize();
  assert(index >= 0 && index < n);
  for (auto i = index; i < n - 1; i++) array_[i] = array_[i+1];
  SetSize(n-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  if (IsRootPage()) {
    recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
    return;
  }
  assert(false);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  auto pid = GetPageId();
  auto p_pid = GetParentPageId();

  Page *p_page = buffer_pool_manager->FetchPage(p_pid);
  assert(p_page != nullptr);
  auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(p_page->GetData());
  auto i = parent->ValueIndex(pid);

  // Change the dummy varible to the middle key
  array_[0].first = middle_key;
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);

  // Remove the first key&value pair
  Remove(0);

  // Set the key in the parent to the dummy variable
  parent->SetKeyAt(i, KeyAt(0));
  buffer_pool_manager->UnpinPage(p_pid, true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto n = GetSize();
  array_[n] = pair;
  auto pid = pair.second;
  Page *page = buffer_pool_manager->FetchPage(pid);
  assert(page != nullptr);
  reinterpret_cast<BPlusTreeInternalPage *>(page->GetData())->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pid, true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  auto r_pid = recipient->GetPageId();
  auto p_pid = GetParentPageId();
  auto n = GetSize();
  auto last = array_[n-1];

  Page *page;
  
  // re-vitalize the dummy variable with the middle key
  recipient->SetKeyAt(0, middle_key);

  // Update the middle key in the parent page to the last key 
  page = buffer_pool_manager->FetchPage(p_pid);
  assert(page != nullptr);
  auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  auto i = parent->ValueIndex(r_pid);
  parent->SetKeyAt(i, last.first);
  buffer_pool_manager->UnpinPage(p_pid, true);

  recipient->CopyFirstFrom(last, buffer_pool_manager);

  SetSize(n-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto n = GetSize();
  for (auto i = n - 1; i >= 0; i--) array_[i+1] = array_[i];
  array_[0] = pair;
  auto pid = pair.second;
  Page *page = buffer_pool_manager->FetchPage(pid);
  assert(page != nullptr);
  reinterpret_cast<BPlusTreeInternalPage *>(page->GetData())->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pid, true);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
