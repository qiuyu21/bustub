//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))
{
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  auto *catalog = exec_ctx_->GetCatalog();
  idx_ =catalog->GetIndex(plan_->GetIndexOid());
  t_inner_ = catalog->GetTable(plan_->GetInnerTableOid());
  std::cout << plan_->InnerTableSchema().ToString() << std::endl;
}

void NestIndexJoinExecutor::Init() {
  child_executor_.get()->Init();
  //
  RID rid;
  has_next_ = child_executor_.get()->Next(&t_outer_, &rid);
  //
  rids_.clear();
  i_ = 0;
  //
  if (has_next_) {
    Value val = plan_->KeyPredicate().get()->Evaluate(&t_outer_, child_executor_.get()->GetOutputSchema());
    std::vector<Column> cols = {{"Dumb", TypeId::INTEGER}};
    Schema sch = Schema(cols);
    Tuple key = {{val}, &sch};
    idx_->index_->ScanKey(key, &rids_, exec_ctx_->GetTransaction());
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(has_next_) {
    if (i_ == rids_.size()) {
      auto b = false;
      if (!rids_.size() && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> vals;
        int col_out = child_executor_.get()->GetOutputSchema().GetColumnCount();
        int col_in = t_inner_->schema_.GetColumnCount();
        for (int i = 0; i < col_out; i++) vals.push_back(t_outer_.GetValue(&child_executor_.get()->GetOutputSchema(), i));
        for (int i = 0; i < col_in; i++) vals.push_back(ValueFactory::GetNullValueByType(t_inner_->schema_.GetColumn(i).GetType()));
        *tuple = {vals, &GetOutputSchema()};
        b = true;
      }
      has_next_ = child_executor_.get()->Next(&t_outer_, rid);
      rids_.clear();
      i_ = 0;
      if (has_next_) {
        Value val = plan_->KeyPredicate().get()->Evaluate(&t_outer_, child_executor_.get()->GetOutputSchema());
        std::vector<Column> cols = {{"Dumb", TypeId::INTEGER}};
        Schema sch = Schema(cols);
        Tuple key = {{val}, &sch};
        idx_->index_->ScanKey(key, &rids_, exec_ctx_->GetTransaction());
      }
      if (b) return true;
    } else {
      Tuple t;
      assert(t_inner_->table_->GetTuple(rids_.at(i_), &t, exec_ctx_->GetTransaction()));
      std::vector<Value> vals;
      int col_out = child_executor_.get()->GetOutputSchema().GetColumnCount();
      int col_in = t_inner_->schema_.GetColumnCount();
      for (int i = 0; i < col_out; i++) vals.push_back(t_outer_.GetValue(&child_executor_.get()->GetOutputSchema(), i));
      for (int i = 0; i < col_in; i++) vals.push_back(t.GetValue(&t_inner_->schema_, i));
      *tuple = {vals, &GetOutputSchema()};
      i_++;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
