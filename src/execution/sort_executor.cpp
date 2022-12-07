#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
}

void SortExecutor::Init() {
    tuples_.clear();
    i_ = 0;
    child_executor_.get()->Init();
    Tuple t;
    RID r;
    
    while (child_executor_.get()->Next(&t, &r)) tuples_.push_back(t);
    for (auto &pair:plan_->GetOrderBy()) {
        assert(pair.first != OrderByType::INVALID);
        auto sort = [&](Tuple t1, Tuple t2) -> bool
        {
            Value v1 = pair.second.get()->Evaluate(&t1, GetOutputSchema());
            Value v2 = pair.second.get()->Evaluate(&t2, GetOutputSchema());
            if (pair.first == OrderByType::DEFAULT || pair.first == OrderByType::ASC) {
                return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
            } else {
                return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
            }
        };
        std::sort(tuples_.begin(), tuples_.end(), sort);
    }       
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (i_ == tuples_.size()) return false;
    *tuple = tuples_.at(i_);
    i_++;
    return true;
}

}  // namespace bustub
