//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) 
{
    auto *i_info = exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid());

    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(i_info->index_.get());

    t_info_ = exec_ctx->GetCatalog()->GetTable(i_info->table_name_);
}

void IndexScanExecutor::Init() {
    rids_.clear();
    i_ = 0;
    for (auto itr = tree_->GetBeginIterator(); itr != tree_->GetEndIterator(); ++itr) {
        rids_.push_back((*itr).second);
    }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (i_ >= rids_.size()) return false;
    assert(t_info_->table_->GetTuple(rids_.at(i_), tuple, exec_ctx_->GetTransaction()));
    i_++;
    return true;
}

}  // namespace bustub
