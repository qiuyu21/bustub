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

    auto &order_bys = plan_->GetOrderBy();

    for (size_t j = 0; j < order_bys.size(); j++) {
        auto pair = order_bys.at(j);
        assert(pair.first != OrderByType::INVALID);
        auto sort = [&](Tuple t1, Tuple t2) -> bool {
            Value v1 = pair.second.get()->Evaluate(&t1, GetOutputSchema());
            Value v2 = pair.second.get()->Evaluate(&t2, GetOutputSchema());
            if (pair.first == OrderByType::DEFAULT || pair.first == OrderByType::ASC) {
                return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
            } else {
                return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
            }
        };
        if (j == 0) {
            std::sort(tuples_.begin(), tuples_.end(), sort);
            continue;
        }
        auto cur = tuples_.begin();
        while(cur != tuples_.end()) {
            auto next = cur+1;
            while (next != tuples_.end()) {
                auto prev = order_bys.at(j-1);
                Value v1 = prev.second.get()->Evaluate(&(*cur), GetOutputSchema());
                Value v2 = prev.second.get()->Evaluate(&(*next), GetOutputSchema());
                if (v1.CompareNotEquals(v2) == CmpBool::CmpTrue) break;
                next++;
            }
            std::sort(cur, next, sort);
            cur = next;
        }
    }
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (i_ == tuples_.size()) return false;
    *tuple = tuples_.at(i_);
    i_++;
    return true;
}

}  // namespace bustub
