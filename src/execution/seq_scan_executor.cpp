//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {
    auto oid = plan_->GetTableOid();
    auto *catalog = exec_ctx_->GetCatalog();
    t_info_ = catalog->GetTable(oid);
    assert(t_info_ != nullptr);
}

void SeqScanExecutor::Init() {
    auto *table_heap = t_info_->table_.get();
    assert(table_heap != nullptr);
    auto itr = table_heap->Begin(exec_ctx_->GetTransaction());
    itr_ptr_ = std::make_unique<TableIterator>(itr);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    auto *table_heap = t_info_->table_.get();
    assert(table_heap != nullptr);
    TableIterator *itr = itr_ptr_.get();
    if (*itr == table_heap->End()) return false;
    *tuple = itr->operator*();
    *rid = tuple->GetRid();
    itr->operator++();
    return true;
}

}  // namespace bustub
