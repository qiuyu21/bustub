//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)), 
      schema_outer_(left_executor_.get()->GetOutputSchema()), schema_inner_(right_executor_.get()->GetOutputSchema())
{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_.get()->Init();
  right_executor_.get()->Init();
  RID rid;
  match_ = 0;
  has_next_ = left_executor_.get()->Next(&t_outer_, &rid);  
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(has_next_) {
    Tuple t_inner;
    if (right_executor_.get()->Next(&t_inner, rid)) {
      Value val = plan_->predicate_->EvaluateJoin(&t_outer_, schema_outer_, 
                                                  &t_inner, schema_inner_);
      if (!val.IsNull() && val.GetAs<bool>()) {
        std::vector<Value> vals;
        int out_n = schema_outer_.GetColumnCount();
        int inn_n = schema_inner_.GetColumnCount();
        for (int i = 0; i < out_n; i++) vals.push_back(t_outer_.GetValue(&schema_outer_, i));
        for (int i = 0; i < inn_n; i++) vals.push_back(t_inner.GetValue(&schema_inner_, i));
        *tuple = Tuple(vals, &GetOutputSchema());
        match_++;
        return true;
      }
    } else {
      auto b = false;
      if (!match_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> vals;
        int out_n = schema_outer_.GetColumnCount();
        int inn_n = schema_inner_.GetColumnCount();
        for (int i = 0; i < out_n; i++) vals.push_back(t_outer_.GetValue(&schema_outer_, i));
        for (int i = 0; i < inn_n; i++) vals.push_back(ValueFactory::GetNullValueByType(schema_inner_.GetColumn(i).GetType()));
        *tuple = Tuple(vals, &GetOutputSchema());
        b = true;
      }
      has_next_ = left_executor_.get()->Next(&t_outer_, rid);
      right_executor_.get()->Init();
      match_ = 0;
      if (b) return true;
    }
  }
  return false;
}

}  // namespace bustub
