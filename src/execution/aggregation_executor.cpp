//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child) 
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()), aht_iterator_(aht_.Begin())
{
}

void AggregationExecutor::Init() {
    child_.get()->Init();
    aht_.Clear();
    Tuple t;
    RID r;
    int n = 0;
    while (child_.get()->Next(&t, &r)) {
        auto agg_key = MakeAggregateKey(&t);
        auto agg_val = MakeAggregateValue(&t);
        aht_.InsertCombine(agg_key, agg_val);
        n++;
    }
    if (n == 0 && plan_->agg_types_.size() == 1 && plan_->group_bys_.size() == 0) {
        aht_.InsertCombine(AggregateKey{}, aht_.GenerateInitialAggregateValue());
    }
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (aht_iterator_ == aht_.End()) return false;
    std::vector<Value> values;
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    *tuple = Tuple(values, &GetOutputSchema());
    ++aht_iterator_;
    return true;
}

}  // namespace bustub
