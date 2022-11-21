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
  // std::cout << "Leaf is full, creating a new leaf node..." << std::endl;
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

    // std::cout << "Internal node is full, creating a new internal node..." << std::endl;
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
  
  bool isBtreeLocked = LockDelete(transaction, key);
  auto *lock_queue = transaction->GetPageSet().get();
  if (!lock_queue->size()) return;  // BTree has no page
  Page *leaf_page = lock_queue->back();
  auto leaf_page_id = leaf_page->GetPageId();
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  assert(leaf->IsLeafPage());
 
  auto sizeBefore = leaf->GetSize();
  auto sizeAfter = leaf->RemoveAndDeleteRecord(key, comparator_);
  if (sizeBefore == sizeAfter) {
    if (isBtreeLocked) latch_.unlock();
    ReleaseTLocks(transaction);
    return;
  }

  size_--;

  if (sizeAfter >= leaf->GetMaxSize()/2 || leaf->IsRootPage()) {
    leaf_page->WUnlatch();
    lock_queue->pop_back();
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    assert(!isBtreeLocked);
    ReleaseTLocks(transaction);
    return;
  }

  for (;;) {
    std::cout << "Borrowing..." << std::endl;
    if (Borrow(transaction)) {
      std::cout << "Borrowing succeeded." << std::endl;
      break;
    }
    std::cout << "Borrowing failed. Merging..." << std::endl;
    auto bef = lock_queue->size();
    Merge(transaction);
    auto aft = lock_queue->size();
    assert(lock_queue->size() > 0 && aft == bef - 1);


    Page *parent = lock_queue->back(); // the back of lock_queue should contain the parent's page
    auto *parent_tp = reinterpret_cast<BPlusTreePage *>(parent->GetData());
    auto parent_pid = parent_tp->GetPageId();

    if (parent_tp->GetSize() >= parent_tp->GetMaxSize() / 2) {
      parent->WUnlatch();
      lock_queue->pop_back();
      buffer_pool_manager_->UnpinPage(parent_pid, true);  // Since one pair of key&value was deleted during Merge(), need to flag it dirty
      break;
    } else if (parent_tp->IsRootPage()) {
      // TODO:
      // assert(isBtreeLocked);
      if (parent_tp->GetSize() == 1) {
        root_page_id_ = reinterpret_cast<InternalPage *>(parent->GetData())->ValueAt(0);
        parent->WUnlatch();
        lock_queue->pop_back();
        buffer_pool_manager_->UnpinPage(parent_pid, false);
        buffer_pool_manager_->DeletePage(parent_pid);
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData())->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
      } else {
        parent->WUnlatch();
        lock_queue->pop_back();
        buffer_pool_manager_->UnpinPage(parent_pid, true);
      }
      break;
    }
  }

  if (isBtreeLocked) latch_.unlock();
  ReleaseTLocks(transaction);
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
  return INDEXITERATOR_TYPE(&i_data_, buffer_pool_manager_, GetMin());
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(&i_data_, buffer_pool_manager_, Get(key), comparator_, key);
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
auto BPLUSTREE_TYPE::Borrow(Transaction *transaction) -> bool {
  auto *lock_queue = transaction->GetPageSet().get();
  assert(lock_queue->size() >= 2);
  

  // temporarily pop the last two pages, if the borrow failed, will push them back to the queue
  Page *child = lock_queue->back();
  lock_queue->pop_back();
  Page *parent = lock_queue->back();
  lock_queue->pop_back();
  
  auto *parent_internal = reinterpret_cast<InternalPage *>(parent->GetData());

  auto index = parent_internal->ValueIndex(child->GetPageId());
  assert(index != -1);
  int iSib[2] = {-1, -1}; // left, right sibling index
  if (index > 0) iSib[0] = index - 1;
  if (index + 1 < parent_internal->GetSize()) iSib[1] = index + 1;
  bool borrowed = false;
  for (auto i = 0; i < 2; i++) {
    if (iSib[i] == -1) continue;
    Page *sibling = buffer_pool_manager_->FetchPage(parent_internal->ValueAt(iSib[i]));
    assert(sibling != nullptr);
    auto *sibling_tp = reinterpret_cast<BPlusTreePage *>(sibling->GetData());
    if (sibling_tp->IsLeafPage()) {
      if (sibling_tp->GetSize() > sibling_tp->GetMaxSize()/2) {
        if (i == 0) {
          reinterpret_cast<LeafPage *>(sibling->GetData())->MoveLastToFrontOf(reinterpret_cast<LeafPage *>(child->GetData()));
          parent_internal->SetKeyAt(index, reinterpret_cast<LeafPage *>(child->GetData())->KeyAt(0));
        } else {
          reinterpret_cast<LeafPage *>(sibling->GetData())->MoveFirstToEndOf(reinterpret_cast<LeafPage *>(child->GetData()));
          parent_internal->SetKeyAt(index+1, reinterpret_cast<LeafPage *>(sibling->GetData())->KeyAt(0));
        }
        borrowed = true;
      }
    } else {  // borrowing between internal pages
      if (sibling_tp->GetSize() > (sibling_tp->GetMaxSize()+1)/2) {
        if (i == 0) {
          reinterpret_cast<InternalPage *>(sibling->GetData())->MoveLastToFrontOf(reinterpret_cast<InternalPage *>(child->GetData()), parent_internal->KeyAt(index), buffer_pool_manager_);
        } else {
          reinterpret_cast<InternalPage *>(sibling->GetData())->MoveFirstToEndOf(reinterpret_cast<InternalPage *>(child->GetData()), parent_internal->KeyAt(index+1), buffer_pool_manager_);
        }
        borrowed = true;
      }
    }

    if (borrowed) {
      buffer_pool_manager_->UnpinPage(sibling_tp->GetPageId(), true);
      break;
    } else {
      buffer_pool_manager_->UnpinPage(sibling_tp->GetPageId(), false);
    }
  }

  if(borrowed) {
    child->WUnlatch();
    parent->WUnlatch();
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    lock_queue->push_back(parent);
    lock_queue->push_back(child);
  }

  return borrowed;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(Transaction *transaction) {
  auto *lock_queue = transaction->GetPageSet().get();
  assert(lock_queue->size() >= 2);
  
  Page *child = lock_queue->back();
  lock_queue->pop_back();
  Page *parent = lock_queue->back();

  auto child_pid = child->GetPageId();

  auto *parent_internal = reinterpret_cast<InternalPage *>(parent->GetData());
  auto index = parent_internal->ValueIndex(child_pid);
  assert(index != -1);

  auto sibling_pid = index > 0 ? parent_internal->ValueAt(index-1) : parent_internal->ValueAt(index+1); // Prioritize left sibling
  Page *sibling = buffer_pool_manager_->FetchPage(sibling_pid);
  assert(sibling != nullptr);
  auto *sibling_tp = reinterpret_cast<BPlusTreePage *>(sibling->GetData());

  if (sibling_tp->IsLeafPage()) {
    std::cout << "Merging leaf..." << std::endl;
    index > 0 ? reinterpret_cast<LeafPage *>(child->GetData())->MoveAllTo(reinterpret_cast<LeafPage *>(sibling->GetData())) :
                  reinterpret_cast<LeafPage *>(sibling->GetData())->MoveAllTo(reinterpret_cast<LeafPage *>(child->GetData()));
  } else {
    std::cout << "Merging internal..." << std::endl;
    index > 0 ? reinterpret_cast<InternalPage *>(child->GetData())->MoveAllTo(reinterpret_cast<InternalPage *>(sibling->GetData()), parent_internal->KeyAt(index), buffer_pool_manager_) :
                  reinterpret_cast<InternalPage *>(sibling->GetData())->MoveAllTo(reinterpret_cast<InternalPage *>(child->GetData()), parent_internal->KeyAt(index+1), buffer_pool_manager_);
  }

  if (index > 0) {
    parent_internal->Remove(index);
    child->WUnlatch();
    buffer_pool_manager_->UnpinPage(child_pid, false);
    buffer_pool_manager_->DeletePage(child_pid);
    buffer_pool_manager_->UnpinPage(sibling_pid, true);
  } else {
    parent_internal->Remove(index+1);
    child->WUnlatch();
    buffer_pool_manager_->UnpinPage(child_pid, true);
    buffer_pool_manager_->UnpinPage(sibling_pid, false);
    buffer_pool_manager_->DeletePage(sibling_pid);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Get(const KeyType &key) -> page_id_t {
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

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LockInsert(Transaction *transaction, const KeyType &key) -> bool {
  
  Page *page;
  InternalPage *internal_page;
  auto *lock_queue = transaction->GetPageSet().get();
  
  latch_.lock();
  bool hold = true;
  page_id_t pid = root_page_id_;
  
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
    internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    if (internal_page->GetSize() < internal_page->GetMaxSize()) {
      if (hold) {
        latch_.unlock();
        hold = false;
      }
      ReleaseTLocks(transaction);
    }
    lock_queue->push_back(page);
    if (internal_page->IsLeafPage()) return hold;
    pid = internal_page->Lookup(key, comparator_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LockDelete(Transaction *transaction, const KeyType &key) -> bool {
  Page *page;
  InternalPage *internal_page;
  auto *lock_queue = transaction->GetPageSet().get();
  
  latch_.lock();
  bool hold = true;
  page_id_t pid = root_page_id_;

  if (pid == INVALID_PAGE_ID) {
    latch_.unlock();
    return false;
  }

  while(1) {
    page = buffer_pool_manager_->FetchPage(pid);
    assert(page != nullptr);
    page->WLatch();
    internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    if (internal_page->GetSize() > (internal_page->GetMaxSize() + 1) / 2) {
      if (hold) {
        latch_.unlock();
        hold = false;
      }
      ReleaseTLocks(transaction);
    }
    lock_queue->push_back(page);
    if (internal_page->IsLeafPage()) {
      if (lock_queue->size() == 1) {
        latch_.unlock();
        hold = false;
      }
      return hold;
    }
    pid = internal_page->Lookup(key, comparator_);
  }
}

// Not safe
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMin() -> page_id_t {
  // Not concurrent safe
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
    page_id_t next_pid = internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(pid, false);
    pid = next_pid;
  }
  return pid;
}

// Not safe
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMax() -> page_id_t {
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
    page_id_t next_pid = internal_page->ValueAt(internal_page->GetSize()-1);
    buffer_pool_manager_->UnpinPage(pid, false);
    pid = next_pid;
  }
  return pid;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub