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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    auto oid = plan_->GetTableOid();
    auto *catalog = exec_ctx_->GetCatalog();
    auto *table_info = catalog->GetTable(oid);
    auto *table_heap = table_info->table_.get();
    assert(table_heap != nullptr);
    auto itr = table_heap->Begin(exec_ctx_->GetTransaction());
    itr_ptr_ = std::make_unique<TableIterator>(itr);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    TableIterator *itr = itr_ptr_.get();
    auto oid = plan_->GetTableOid();
    auto *catalog = exec_ctx_->GetCatalog();
    auto *table_info = catalog->GetTable(oid);
    auto *table_heap = table_info->table_.get();
    assert(table_heap != nullptr);
    if (*itr == table_heap->End()) return false;
    *tuple = itr->operator*();
    *rid = tuple->GetRid();
    itr->operator++();
    return true;
}

}  // namespace bustub
