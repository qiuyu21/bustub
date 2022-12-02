//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor) 
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
    auto oid = plan_->TableOid();
    auto *catalog = exec_ctx_->GetCatalog();
    t_info_ = catalog->GetTable(oid);
    assert(t_info_ != nullptr);
}

void DeleteExecutor::Init() {
    child_executor_->Init();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    auto n = 0;
    Tuple t;
    RID r;
    while (child_executor_.get()->Next(&t, &r)) {
        if (t_info_->table_->MarkDelete(r, exec_ctx_->GetTransaction())) {
            auto idx_info = exec_ctx_->GetCatalog()->GetTableIndexes(t_info_->name_);
            for (auto itr = idx_info.begin(); itr != idx_info.end(); itr++) {
                (*itr)->index_->DeleteEntry(t, r, exec_ctx_->GetTransaction());
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
