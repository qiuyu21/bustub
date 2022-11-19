#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      size_(0)
{ 
  // Create a root page
  // auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
  // assert(page != nullptr);
  // reinterpret_cast<LeafPage *>(page->GetData())->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  // buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return size_ == 0; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSize() const -> int { return size_; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  Page *parent = nullptr;
  Page *cur;
  page_id_t pid = root_page_id_;

  while(true) {
    cur = buffer_pool_manager_->FetchPage(pid);
    assert(cur != nullptr);
    cur->RLatch();

    if (parent != nullptr) {
      parent->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    }

    auto *leaf_page = reinterpret_cast<LeafPage *>(cur->GetData());
    if (leaf_page->IsLeafPage()) {
      ValueType val;
      auto found = leaf_page->Lookup(key, &val, comparator_);
      if (found) result->push_back(val);
      cur->RUnlatch();
      buffer_pool_manager_->UnpinPage(pid, false);
      return found;
    }

    parent = cur;
    auto *internal_page = reinterpret_cast<InternalPage *>(cur->GetData());
    pid = internal_page->Lookup(key, comparator_);
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {

  bool isBtreeLocked = LockInsert(transaction, key);

  auto *lock_queue = transaction->GetPageSet().get();
  assert(lock_queue->size() > 0);
  Page *leaf_page = lock_queue->back();
  page_id_t leaf_page_id = leaf_page->GetPageId();
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  assert(leaf->IsLeafPage());

  ValueType placeholder;
  if (leaf->Lookup(key, &placeholder, comparator_)) {
    if (isBtreeLocked) latch_.unlock();
    ReleaseTLocks(transaction);
    return false;
  }

  if (leaf->GetSize() < leaf->GetMaxSize()) {
    assert(leaf->Insert(key, value, comparator_));
    assert(!isBtreeLocked);
    size_++;
    leaf_page->WUnlatch();
    lock_queue->pop_back();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    ReleaseTLocks(transaction);
    return true;
  }

  // Full leaf, create a new leaf
  std::cout << "Leaf is full, creating a new leaf node..." << std::endl;
  page_id_t new_leaf_page_id;
  Page *new_leaf_disk_page = buffer_pool_manager_->NewPage(&new_leaf_page_id);
  assert(new_leaf_disk_page != nullptr);
  LeafPage *new_leaf = reinterpret_cast<LeafPage *>(new_leaf_disk_page->GetData());
  new_leaf->Init(new_leaf_page_id, leaf->GetParentPageId(), leaf_max_size_);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_page_id);
  leaf->MoveHalfTo(new_leaf);
  assert(comparator_(key, new_leaf->KeyAt(0)) > 0 ? new_leaf->Insert(key, value, comparator_) : leaf->Insert(key, value, comparator_));
  size_++;

  auto insert_key = new_leaf->KeyAt(0);
  auto insert_val = new_leaf_page_id;
  page_id_t insert_into = leaf->GetParentPageId();
  page_id_t insert_after = leaf_page_id;

  buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);

  if (insert_into != INVALID_PAGE_ID) { // Done with the leaf page
    leaf_page->WUnlatch();
    lock_queue->pop_back();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  }

  for(;;) {
    if (insert_into == INVALID_PAGE_ID) { // Create a new root
      assert(lock_queue->size() == 1);
      assert(isBtreeLocked);

      page_id_t new_root_page_id;
      Page *new_root = buffer_pool_manager_->NewPage(&new_root_page_id);
      assert(new_root != nullptr);
      auto *internal_root = reinterpret_cast<InternalPage *>(new_root->GetData());
      internal_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
      internal_root->PopulateNewRoot(insert_after, insert_key, insert_val);

      UpdateParent(insert_after, new_root_page_id);
      UpdateParent(insert_val, new_root_page_id);
      root_page_id_ = new_root_page_id;
      
      lock_queue->front()->WUnlatch();
      lock_queue->pop_back();
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
      buffer_pool_manager_->UnpinPage(insert_after, true);
      break;
    }
    
    Page *page = lock_queue->back();
    assert(page != nullptr);
    auto *inner = reinterpret_cast<InternalPage *>(page->GetData());
    if (inner->GetSize() < inner->GetMaxSize()) {
      inner->InsertNodeAfter(insert_after, insert_key, insert_val);
      page->WUnlatch();
      lock_queue->pop_back();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      break;
    }

    std::cout << "Internal node is full, creating a new internal node..." << std::endl;
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    assert(new_page != nullptr);
    auto *new_inner = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_inner->Init(new_page_id, inner->GetParentPageId(), internal_max_size_);

    inner->MoveHalfTo(new_inner, buffer_pool_manager_);
    if (inner->ValueIndex(insert_after) != -1) {
      inner->InsertNodeAfter(insert_after, insert_key, insert_val);
    } else {
      new_inner->InsertNodeAfter(insert_after, insert_key, insert_val);
      UpdateParent(insert_val, new_page_id);
    }
    if (new_inner->GetSize() - inner->GetSize() > 1) {
      inner->InsertNodeAfter(inner->ValueAt(inner->GetSize()-1), new_inner->KeyAt(0), new_inner->ValueAt(0));
      UpdateParent(new_inner->ValueAt(0), inner->GetPageId());
      new_inner->Remove(0);
    }

    insert_key = new_inner->KeyAt(0);
    insert_val = new_page_id;
    insert_into = inner->GetParentPageId();
    insert_after = inner->GetPageId();
  
    if (insert_into != INVALID_PAGE_ID) {
      page->WUnlatch();
      lock_queue->pop_back();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(new_page_id, true);
  }

  if (isBtreeLocked) latch_.unlock();
  ReleaseTLocks(transaction);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  
  Page *leaf_page = GetLeafPage(transaction, key, false);
  assert(leaf_page != nullptr);

  auto leaf_page_id = leaf_page->GetPageId();
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
 
  auto sizeBefore = leaf->GetSize();
  auto sizeAfter = leaf->RemoveAndDeleteRecord(key, comparator_);

  if (sizeBefore == sizeAfter) { // Key not found
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return;
  }

  if (sizeAfter >= leaf->GetMaxSize()/2 || leaf->IsRootPage()) { // It is still at least half-full or the leaf is a root page
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    return;
  }

  for (Page *page = leaf_page;;) {
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    auto p_pid = tree_page->GetParentPageId();


    std::cout << "brrowing from sibling" << std::endl;
    if (BorrowFromSibling(page)) {
      // Try borrow a key&value from the sibling
      std::cout << "brrowing succeeded" << std::endl;
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      return;
    }

    std::cout << "brrowing failed" << std::endl;
    std::cout << "merging" << std::endl;

    // Siblings don't have a spare
    // Now merge with one of the sibling
    // After merging, the page will be destroyed by buffer_pool_manager_
    // No need to un-pin
    MergeWithSibling(page);

    
    // Now parent page will have one less key&value, which might violate the invariant
    Page *p_page = buffer_pool_manager_->FetchPage(p_pid);
    assert(p_page != nullptr);
    auto *parent = reinterpret_cast<BPlusTreePage *>(p_page->GetData());
    if (parent->GetSize() > parent->GetMaxSize() / 2) {
      // If the parent page has more than half the size, we are done
      buffer_pool_manager_->UnpinPage(p_pid, false);
      return;
    } else if (parent->IsRootPage()) {
      // Or we have reached the root page
      if (parent->GetSize() == 1) {
        // If the root page has only 1 child, the root page will be removed and the child will become the new root
        root_page_id_ = reinterpret_cast<InternalPage *>(p_page->GetData())->ValueAt(0);
        buffer_pool_manager_->DeletePage(p_pid);
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData())->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);

      } else {
        buffer_pool_manager_->UnpinPage(p_pid, false);
      }
      return;
    }

    page = p_page;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(&i_data_, buffer_pool_manager_, GetMinMaxLeafPageId(true));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(&i_data_, buffer_pool_manager_, GetLeafPageId(key), comparator_, key);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(&i_data_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParent(const page_id_t pid, const page_id_t p_pid) {
  Page *p = buffer_pool_manager_->FetchPage(pid);
  assert(p != nullptr);
  reinterpret_cast<BPlusTreePage *>(p->GetData())->SetParentPageId(p_pid);
  buffer_pool_manager_->UnpinPage(pid, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromSibling(Page *page) -> bool {
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  return tree_page->IsLeafPage() ? BorrowFromLeaf(reinterpret_cast<LeafPage *>(page->GetData())) : 
                                   BorrowFromInternal(reinterpret_cast<InternalPage *>(page->GetData()));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromLeaf(LeafPage *leaf) -> bool {
  auto success = false;
  auto pid = leaf->GetPageId();
  auto p_pid = leaf->GetParentPageId();

  Page *p_page = buffer_pool_manager_->FetchPage(p_pid);
  assert(p_page != nullptr);
  auto *parent = reinterpret_cast<InternalPage *>(p_page->GetData());

  auto i = parent->ValueIndex(pid);
  assert(i != -1);

  if (i > 0) {
    // Left sibling
    page_id_t s_pid = parent->ValueAt(i-1);
    Page *s_page = buffer_pool_manager_->FetchPage(s_pid);
    assert(s_page != nullptr);
    auto *sibling = reinterpret_cast<LeafPage *>(s_page->GetData());
    if (sibling->GetSize() > sibling->GetMaxSize() / 2) {
      std::cout << "Borrowing from left sibling" << std::endl;
      sibling->MoveLastToFrontOf(leaf);
      parent->SetKeyAt(i, leaf->KeyAt(0));
      success = true;
    }
    buffer_pool_manager_->UnpinPage(s_pid, success);
  }
  
  if (!success && i+1 < parent->GetSize()) {
    // Right sibling
    page_id_t s_pid = parent->ValueAt(i+1);
    Page *s_page = buffer_pool_manager_->FetchPage(s_pid);
    assert(s_page != nullptr);
    auto *sibling = reinterpret_cast<LeafPage *>(s_page->GetData());
    if (sibling->GetSize() > sibling->GetMaxSize() / 2) {
      std::cout << "Borrowing from right sibling" << std::endl;
      sibling->MoveFirstToEndOf(leaf);
      parent->SetKeyAt(i+1, sibling->KeyAt(0));
      success = true;
    }
    buffer_pool_manager_->UnpinPage(s_pid, success);
  }

  buffer_pool_manager_->UnpinPage(p_pid, success);
  return success;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BorrowFromInternal(InternalPage *inner) -> bool {
  auto success = false;
  page_id_t pid = inner->GetPageId();
  page_id_t p_pid = inner->GetParentPageId();

  Page *p_page = buffer_pool_manager_->FetchPage(p_pid);
  assert(p_page != nullptr);
  auto *parent = reinterpret_cast<InternalPage *>(p_page->GetData());

  auto i = parent->ValueIndex(pid);
  assert(i != -1);

  if (i > 0) {
    // Left sibling
    page_id_t s_pid = parent->ValueAt(i-1);
    Page *s_page = buffer_pool_manager_->FetchPage(s_pid);
    assert(s_page != nullptr);
    auto *sibling = reinterpret_cast<InternalPage *>(s_page->GetData());
    if (sibling->GetSize() > sibling->GetMaxSize() / 2 + 1) {
      sibling->MoveLastToFrontOf(inner, parent->KeyAt(i), buffer_pool_manager_);
      success = true;
    }
    buffer_pool_manager_->UnpinPage(s_pid, success);
  }

  if (!success && i+1 < parent->GetSize()) {
    // Right sibling
    page_id_t s_pid = parent->ValueAt(i+1);
    Page *s_page = buffer_pool_manager_->FetchPage(s_pid);
    assert(s_page != nullptr);
    auto *sibling = reinterpret_cast<InternalPage *>(s_page->GetData());
    if (sibling->GetSize() > sibling->GetMaxSize() / 2 + 1) {
      sibling->MoveFirstToEndOf(inner, parent->KeyAt(i+1), buffer_pool_manager_);
      success = true;
    }
    buffer_pool_manager_->UnpinPage(s_pid, success);
  }

  buffer_pool_manager_->UnpinPage(p_pid, success);
  return success;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithSibling(Page *page) {
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  tree_page->IsLeafPage() ? MergeLeaf(reinterpret_cast<LeafPage *>(page->GetData())) :
                            MergeInternal(reinterpret_cast<InternalPage *>(page->GetData()));
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeaf(LeafPage *leaf) {
  page_id_t pid = leaf->GetPageId();
  page_id_t p_pid = leaf->GetParentPageId();

  Page *p_page = buffer_pool_manager_->FetchPage(p_pid);
  assert(p_page != nullptr);
  auto *parent = reinterpret_cast<InternalPage *>(p_page->GetData());

  auto i = parent->ValueIndex(pid);
  assert(i != -1);

  page_id_t s_pid = i > 0 ? parent->ValueAt(i-1) : parent->ValueAt(i+1);
  Page *s_page = buffer_pool_manager_->FetchPage(s_pid);
  assert(s_page != nullptr);
  auto *sibling = reinterpret_cast<LeafPage *>(s_page->GetData());

  while (leaf->GetSize()) {
    // Move all the keys to the sibling
    i > 0 ? leaf->MoveFirstToEndOf(sibling) : leaf->MoveLastToFrontOf(sibling);
  }

  // Remove the key/value from the parent
  parent->Remove(i);
  buffer_pool_manager_->UnpinPage(pid, false);
  buffer_pool_manager_->DeletePage(pid);
  buffer_pool_manager_->UnpinPage(s_pid, true);
  buffer_pool_manager_->UnpinPage(p_pid, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternal(InternalPage *inner) {
  page_id_t pid = inner->GetPageId();
  page_id_t p_pid = inner->GetParentPageId();

  Page *p_page = buffer_pool_manager_->FetchPage(p_pid);
  assert(p_page != nullptr);
  auto *parent = reinterpret_cast<InternalPage *>(p_page->GetData());

  auto i = parent->ValueIndex(pid);
  assert(i != -1);

  page_id_t s_pid = i > 0 ? parent->ValueAt(i-1) : parent->ValueAt(i+1);
  if (i > 0) {
    s_pid = parent->ValueAt(i-1);
  } else {
    s_pid = parent->ValueAt(i+1);
  }

  std::cout << "sibling:" << s_pid << std::endl;

  Page *s_page = buffer_pool_manager_->FetchPage(s_pid);
  assert(s_page != nullptr);
  auto *sibling = reinterpret_cast<InternalPage *>(s_page->GetData());

  std::cout << "size:" << sibling->GetSize() << std::endl;

  while(inner->GetSize()) {
    if (i > 0) {
      inner->MoveFirstToEndOf(sibling, parent->KeyAt(i), buffer_pool_manager_);
    } else {
      inner->MoveLastToFrontOf(sibling, parent->KeyAt(i+1), buffer_pool_manager_);
    }
    // i > 0 ? inner->MoveFirstToEndOf(sibling, parent->KeyAt(i), buffer_pool_manager_) : 
    //         inner->MoveLastToFrontOf(sibling, parent->KeyAt(i+1), buffer_pool_manager_);
  }

  parent->Remove(i);
  buffer_pool_manager_->UnpinPage(pid, false);
  buffer_pool_manager_->DeletePage(pid);
  buffer_pool_manager_->UnpinPage(s_pid, true);
  buffer_pool_manager_->UnpinPage(p_pid, true);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMinMaxLeafPageId(bool min) -> page_id_t {
  if (root_page_id_ == INVALID_PAGE_ID) return INVALID_PAGE_ID;
  page_id_t pid = root_page_id_;
  while(1) {
    Page *page = buffer_pool_manager_->FetchPage(pid);
    assert(page != nullptr);
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (tree_page->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(pid, false);
      break;
    }
    auto *internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_pid = min ? internal_page->ValueAt(0) : internal_page->ValueAt(internal_page->GetSize()-1);
    buffer_pool_manager_->UnpinPage(pid, false);
    pid = next_pid;
  }
  return pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageId(const KeyType &key) -> page_id_t {
  if (root_page_id_ == INVALID_PAGE_ID) return INVALID_PAGE_ID;

  page_id_t pid = root_page_id_;
  while(1) {
    Page *page = buffer_pool_manager_->FetchPage(pid);
    assert(page != nullptr);
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (tree_page->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(pid, false);
      break;
    }
    auto *internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    auto next_pid = internal_page->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(pid, false);
    pid = next_pid;
  }
  return pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReleaseTLocks(Transaction *transaction) -> void {
  auto *lock_queue = transaction->GetPageSet().get();
  while (lock_queue->size()) {
    auto *front_page = lock_queue->front();
    front_page->WUnlatch();
    lock_queue->pop_front();
    buffer_pool_manager_->UnpinPage(front_page->GetPageId(), false);
  }
}


// TODO: remove return value
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(Transaction *transaction, const KeyType &key, bool isInsert) -> Page * {
  Page *page;
  char *data;
  InternalPage *internal_page;
  auto *lock_queue = transaction->GetPageSet().get();
  page_id_t pid = root_page_id_;
  bool released = false;

  latch_.lock();
  if (pid == INVALID_PAGE_ID) {
    if (isInsert) {
      auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
      assert(page != nullptr);
      page->WLatch();
      reinterpret_cast<LeafPage *>(page->GetData())->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      lock_queue->push_back(page);
      latch_.unlock();
      return page;
    } else {
      latch_.unlock();
      return nullptr;
    }
  }

  while(1) {
    page = buffer_pool_manager_->FetchPage(pid);
    assert(page != nullptr);
    page->WLatch();
    data = page->GetData();
    internal_page = reinterpret_cast<InternalPage *>(data);
    if (isInsert) { // Insertion
      if (internal_page->GetSize() < internal_page->GetMaxSize()) {
        if (!released) {
          latch_.unlock();
          released = true;
        }
        ReleaseTLocks(transaction);
      }
    } else {
      // TODO: implement safe protocol for deletion
      assert(false);
    }
    lock_queue->push_back(page);
    if (internal_page->IsLeafPage()) return page;
    pid = internal_page->Lookup(key, comparator_);
  }

  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LockInsert(Transaction *transaction, const KeyType &key) -> bool {
  
  Page *page;
  char *data;
  InternalPage *internal_page;
  auto *lock_queue = transaction->GetPageSet().get();
  
  latch_.lock();
  page_id_t pid = root_page_id_;
  bool released = false;
  
  if (pid == INVALID_PAGE_ID) {
    auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(page != nullptr);
    page->WLatch();
    reinterpret_cast<LeafPage *>(page->GetData())->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    lock_queue->push_back(page);
    latch_.unlock();
    return false;
  }

  while(1) {
    page = buffer_pool_manager_->FetchPage(pid);
    assert(page != nullptr);
    page->WLatch();
    data = page->GetData();
    internal_page = reinterpret_cast<InternalPage *>(data);
    if (internal_page->GetSize() < internal_page->GetMaxSize()) {
      if (!released) {
        latch_.unlock();
        released = true;
      }
      ReleaseTLocks(transaction);
    }
    lock_queue->push_back(page);
    if (internal_page->IsLeafPage()) return !released;
    pid = internal_page->Lookup(key, comparator_);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub