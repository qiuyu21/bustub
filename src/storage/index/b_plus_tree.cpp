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
  page_id_t pid;
  char *data;
  Page *disk_page;
  LeafPage *leaf_page;
  InternalPage *internal_page;

  if (root_page_id_ == INVALID_PAGE_ID) return false;

  pid = root_page_id_;
  while(true) {
    disk_page = buffer_pool_manager_->FetchPage(pid);
    assert(disk_page != nullptr);
    data = disk_page->GetData();
    leaf_page = reinterpret_cast<LeafPage *>(data);
    if (leaf_page->IsLeafPage()) {
      ValueType val;
      auto found = leaf_page->Lookup(key, &val, comparator_);
      if (found) result->push_back(val);
      buffer_pool_manager_->UnpinPage(pid, false);
      return found;
    } else {
      internal_page = reinterpret_cast<InternalPage *>(data);
      auto next_pid = internal_page->Lookup(key, comparator_);
      buffer_pool_manager_->UnpinPage(pid, false);
      pid = next_pid;
    }
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
  Page *leaf_page;
  if (root_page_id_ == INVALID_PAGE_ID) {
    leaf_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(leaf_page != nullptr);
    reinterpret_cast<LeafPage *>(leaf_page->GetData())->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    buffer_pool_manager_->FlushPage(root_page_id_);
  } else {
    leaf_page = GetLeafPage(key);
    assert(leaf_page != nullptr);
  }
  
  // REMEMBER to unpin after usage
  page_id_t leaf_page_id = leaf_page->GetPageId();
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType placeholder;
  if (leaf->Lookup(key, &placeholder, comparator_)) { // Check if the key already exists
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return false;
  }

  int leaf_size = leaf->GetSize();
  if (leaf_size < leaf->GetMaxSize()) {
    assert(leaf->Insert(key, value, comparator_));
    buffer_pool_manager_->UnpinPage(leaf_page_id, true);
    size_++;
    return true;
  }

  // Full leaf, create a new leaf
  page_id_t new_leaf_page_id;
  Page *new_leaf_disk_page = buffer_pool_manager_->NewPage(&new_leaf_page_id);
  assert(new_leaf_disk_page != nullptr);
  LeafPage *new_leaf = reinterpret_cast<LeafPage *>(new_leaf_disk_page->GetData());
  new_leaf->Init(new_leaf_page_id, leaf->GetParentPageId(), leaf_max_size_);
  // Update the next page pointers
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_page_id);

  // Move half the key to the new leaf page
  leaf->MoveHalfTo(new_leaf);
  // Insert the new key&value pair
  assert(comparator_(key, new_leaf->KeyAt(0)) > 0 ? new_leaf->Insert(key, value, comparator_) : leaf->Insert(key, value, comparator_));
  // Resize if the new leaf has 2 keys more than the old leaf
  if (new_leaf->GetSize() - leaf->GetSize() > 1) new_leaf->MoveFirstToEndOf(leaf);
  size_++;


  // Push up the pair, which contains the middle key and the new page id
  auto pair = std::make_pair(new_leaf->KeyAt(0), new_leaf_page_id);
  page_id_t insert_into = new_leaf->GetParentPageId();
  page_id_t insert_after = leaf_page_id;
  // Unpin leaves' pages
  buffer_pool_manager_->UnpinPage(leaf_page_id, true);
  buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);

  while(1) {
    if (insert_into == INVALID_PAGE_ID) {
      // Reach the parent of root, create a new root page
      page_id_t new_root_page_id;
      Page *p = buffer_pool_manager_->NewPage(&new_root_page_id);
      assert(p != nullptr);
      auto *root_page = reinterpret_cast<InternalPage *>(p->GetData());
      root_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
      root_page->PopulateNewRoot(insert_after, pair.first, pair.second);
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
      root_page_id_ = new_root_page_id;
      // update the parent of insert_after, and pair.second
      UpdateParent(insert_after, new_root_page_id);
      UpdateParent(pair.second, new_root_page_id);
      return true;
    }

    Page *dpage = buffer_pool_manager_->FetchPage(insert_into);
    assert(dpage != nullptr);
    auto *inner = reinterpret_cast<InternalPage *>(dpage->GetData());
    auto n = inner->GetSize();
    if (n < inner->GetMaxSize()) {
      inner->InsertNodeAfter(insert_after, pair.first, pair.second);
      buffer_pool_manager_->UnpinPage(insert_into, true);
      return true;
    }

    // The internal page is full, need to split the internal page
    page_id_t new_page_id;
    Page *new_dpage = buffer_pool_manager_->NewPage(&new_page_id);
    assert(new_dpage != nullptr);
    auto *new_inner = reinterpret_cast<InternalPage *>(new_dpage->GetData());
    new_inner->Init(new_page_id, inner->GetParentPageId(), internal_max_size_);

    //
    inner->InsertNodeAfter(insert_after, pair.first, pair.second);
    // Move half the keys into new_inner.
    inner->MoveHalfTo(new_inner, buffer_pool_manager_);
    

    buffer_pool_manager_->UnpinPage(insert_into, true);
    buffer_pool_manager_->UnpinPage(new_page_id, true);

    pair = std::make_pair(new_inner->KeyAt(0), new_page_id);
    insert_into = new_inner->GetParentPageId();
    insert_after = inner->GetPageId();
  }

  return false;
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
  return INDEXITERATOR_TYPE(); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  return INDEXITERATOR_TYPE(); 
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  return INDEXITERATOR_TYPE(); 
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  return root_page_id_;
}

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


/**
 * This function locates the leaf page that might contains the key
 * Caller needs to UnpinPage when it finishes using it.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) -> Page * {
  Page *page;
  char *data;
  InternalPage *internal_page;
  page_id_t pid;

  pid = root_page_id_;
 
  while(1) {
    page = buffer_pool_manager_->FetchPage(pid);
    data = page->GetData();
    internal_page = reinterpret_cast<InternalPage *>(data);
    if (internal_page->IsLeafPage()) return page;
    auto next_pid = internal_page->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(pid, false);
    pid = next_pid;
  }

  return nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParent(const page_id_t pid, const page_id_t p_pid) {
  Page *p = buffer_pool_manager_->FetchPage(pid);
  assert(p != nullptr);
  auto *inner = reinterpret_cast<InternalPage *>(p->GetData());
  inner->SetParentPageId(p_pid);
  buffer_pool_manager_->UnpinPage(pid, true);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
