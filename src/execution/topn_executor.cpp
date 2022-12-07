#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
}

auto TopNExecutor::GetComparator() {
    return [&](Tuple t1, Tuple t2) -> bool {
        for (size_t i = 0; i < plan_->GetOrderBy().size(); i++) {
            auto order_by = plan_->GetOrderBy().at(i);
            assert(order_by.first != OrderByType::INVALID);
            Value v1 = order_by.second.get()->Evaluate(&t1, GetOutputSchema());
            Value v2 = order_by.second.get()->Evaluate(&t2, GetOutputSchema());
            if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
                continue;
            } else if (order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC) {
                return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
            } else {
                return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
            }
        }
        return false;
    };
}

void TopNExecutor::Init() {
    child_executor_.get()->Init();
    i_ = 0;
    heap_.clear();
    Tuple t;
    RID r;

    auto cmp = GetComparator();
    while (child_executor_.get()->Next(&t, &r)) {
        if (heap_.size() < plan_->GetN()) {
            heap_.push_back(t);
            std::push_heap(heap_.begin(), heap_.end(), cmp);
        } else {
            Tuple t1 = heap_.at(0);
            if (cmp(t, t1)) {
                std::pop_heap(heap_.begin(), heap_.end(), cmp);
                heap_.pop_back();
                heap_.push_back(t);
                std::push_heap(heap_.begin(), heap_.end(), cmp);
            }
        }
    }

    std::vector<Tuple> tmp;
    while (heap_.size()) {
        tmp.push_back(heap_.front());
        std::pop_heap(heap_.begin(), heap_.end(), cmp);
        heap_.pop_back();
    }
    heap_ = tmp;
    std::reverse(heap_.begin(), heap_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (i_ == heap_.size()) return false;
    *tuple = heap_.at(i_);
    i_++;
    return true;
}

}  // namespace bustub
