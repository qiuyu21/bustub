/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::list<MappingType> *lst, BufferPoolManager *buffer_pool_manager, page_id_t pid): lst_(lst), buffer_pool_manager_(buffer_pool_manager){
    lst->clear();
    if (pid == INVALID_PAGE_ID) return;
    ReadPage(pid);
    itr_ = lst->begin();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::list<MappingType> *lst, BufferPoolManager *buffer_pool_manager, page_id_t pid, KeyComparator &comparator, const KeyType &key): lst_(lst), buffer_pool_manager_(buffer_pool_manager) {
    lst->clear();
    if (pid == INVALID_PAGE_ID) return;
    ReadPage(pid);
    itr_ = lst->begin();
    for (;itr_ != lst->end(); itr_++) {
        if (comparator((*itr_).first, key) == 0) break;
    }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::list<MappingType> *lst): lst_(lst) { itr_ = lst_->end(); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return *itr_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    if (lst_->size() == 0) return *this;
    auto itr_copy_ = itr_;
    itr_++;
    if (itr_ == lst_->end()) {
        if (next_pid_ != INVALID_PAGE_ID) {
            ReadPage(next_pid_);
            itr_ = itr_copy_;
            itr_++;
        }
    }
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool { return itr_ == itr.itr_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return itr_ != itr.itr_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::ReadPage(page_id_t pid) -> void  {
    assert(pid != INVALID_PAGE_ID);
    Page *page = buffer_pool_manager_->FetchPage(pid);
    assert(page != nullptr);
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

    pid_ = pid;
    next_pid_ = leaf_page->GetNextPageId();

    for (auto i = 0; i < leaf_page->GetSize(); i++) lst_->push_back(leaf_page->GetItem(i));
    buffer_pool_manager_->UnpinPage(pid_, false);
}


template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
