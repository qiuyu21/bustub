//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "execution/plans/values_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor) 
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
    auto oid = plan_->GetTableOid();
    auto *catalog = exec_ctx_->GetCatalog();
    t_info_ = catalog->GetTable(oid);
    assert(t_info_ != nullptr);
}

void InsertExecutor::Init() {
    child_executor_.get()->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    auto n = 0;
    Tuple t;
    RID r;
    while (child_executor_.get()->Next(&t, &r)) {
        if (t_info_->table_->InsertTuple(t, &r, exec_ctx_->GetTransaction())) {
            auto idx_info = exec_ctx_->GetCatalog()->GetTableIndexes(t_info_->name_);
            for (auto itr = idx_info.begin(); itr != idx_info.end(); itr++) {
                (*itr)->index_->InsertEntry(t, r, exec_ctx_->GetTransaction());
            }
            n++;
        }
    }
    
    Value v = Value(TypeId::INTEGER, n);
    std::vector<Value> values;
    values.emplace_back(v);
    *tuple = {values, &GetOutputSchema()};

    return n > 0;
}

}  // namespace bustub
