//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  IsValidLockMode(txn, lock_mode, false);

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) 
    table_lock_map_.insert(std::make_pair(oid, std::make_shared<LockRequestQueue>()));
  table_lock_map_latch_.unlock();

  LockRequestQueue *lrq = table_lock_map_.at(oid).get();
  std::unique_lock<std::mutex> lock(lrq->latch_);
  auto &queue = lrq->request_queue_;
  LockRequest *lr;

  for (auto req_itr = queue.begin();;req_itr++) {
    if (req_itr == queue.end()) {
      lr = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
      queue.emplace_back(lr);
      break;
    } else if ((*req_itr)->txn_id_ == txn->GetTransactionId()) {
      lr = *req_itr;
      assert(lr->granted_);
      if (lr->lock_mode_ == lock_mode) {
        lock.unlock();
        return true;
      } else if (lrq->upgrading_ != INVALID_TXN_ID) {
        lock.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      } else if (!IsValidUpgrade(lr->lock_mode_, lock_mode)) {
        lock.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      } else {
        lrq->upgrading_ = txn->GetTransactionId();
        break;
      }
    }
  }

  while(1) {
    if (lrq->upgrading_ != INVALID_TXN_ID) {   // Prioritize upgrading transaction
      if (lrq->upgrading_ == txn->GetTransactionId()) {
        if (!IsBlocking(lrq, txn, lock_mode)) {
          GetTableLockSet(txn, lr->lock_mode_)->erase(oid);
          lr->lock_mode_ = lock_mode;          
          lrq->upgrading_ = INVALID_TXN_ID;
          break;
        }
      }
    } else {
      if (!IsBlocking(lrq, txn, lock_mode)) break;
    }
    lrq->cv_.wait(lock);
  }

  GetTableLockSet(txn, lock_mode)->emplace(oid);
  lr->granted_ = true;
  lock.unlock();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {

  if (!IsTableExist(oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  LockRequestQueue *lrq = table_lock_map_.at(oid).get();
  lrq->latch_.lock();
  auto &queue = lrq->request_queue_;
  LockRequest *lr;

  for (auto req_itr = queue.begin();; req_itr++) {
    if (req_itr == queue.end()) {
      txn->SetState(TransactionState::ABORTED);
      lrq->latch_.unlock();
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    } else if ((*req_itr)->txn_id_ == txn->GetTransactionId()) {
      lr = *req_itr;
      if (!lr->granted_) {
        txn->SetState(TransactionState::ABORTED);
        lrq->latch_.unlock();
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      } else if (IsTableRowLocked(txn, oid)) {
        txn->SetState(TransactionState::ABORTED);
        lrq->latch_.unlock();
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      }
      queue.erase(req_itr);
      auto *lockset = GetTableLockSet(txn, lr->lock_mode_);
      auto itr = lockset->find(oid);
      assert(itr != lockset->end());
      lockset->erase(itr);
      UpdateTransactionStateOnUnlock(txn, lr->lock_mode_);
      break;
    }
  }

  lrq->cv_.notify_one();
  lrq->latch_.unlock();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  
  IsValidLockMode(txn, lock_mode, true);
  
  if(!IsTableExist(oid) || !IsValidRowLock(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) 
    row_lock_map_.insert(std::make_pair(rid, std::make_shared<LockRequestQueue>()));
  row_lock_map_latch_.unlock();

  LockRequestQueue *lrq = row_lock_map_.at(rid).get();
  std::unique_lock<std::mutex> lock(lrq->latch_);
  auto &queue = lrq->request_queue_;
  LockRequest *lr;

  for (auto req_itr = queue.begin();;req_itr++) {
    if (req_itr == queue.end()) {
      lr = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
      queue.emplace_back(lr);
      break;
    } else if ((*req_itr)->txn_id_ == txn->GetTransactionId()) {
      lr = *req_itr;
      assert(lr->granted_);
      if (lr->lock_mode_ == lock_mode) {
        lock.unlock();
        return true;
      } else if (lrq->upgrading_ != INVALID_TXN_ID) {
        lock.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      } else if (lr->lock_mode_ != LockMode::SHARED) {
        lock.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      } else {
        lrq->upgrading_ = txn->GetTransactionId();
        break;
      }
    }
  }

  while(1) {
    if (lrq->upgrading_ != INVALID_TXN_ID) {  // Prioritize upgrading transaction
      if (lrq->upgrading_ == txn->GetTransactionId()) {
        if (!IsBlocking(lrq, txn, lock_mode)) {
          GetRowLockSet(txn, lr->lock_mode_)->at(oid).erase(rid);
          lr->lock_mode_ = lock_mode;          
          lrq->upgrading_ = INVALID_TXN_ID;
          break;
        }
      }
    } else {
      if (!IsBlocking(lrq, txn, lock_mode)) break;
    }
    lrq->cv_.wait(lock);
  }

  auto *oid2set = GetRowLockSet(txn, lock_mode);
  if (oid2set->find(oid) == oid2set->end()) oid2set->emplace(oid, std::unordered_set<RID>());
  oid2set->at(oid).emplace(rid);
  lr->granted_ = true;
  lock.unlock();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {

  if (!IsRIDExist(rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  LockRequestQueue *lrq = row_lock_map_.at(rid).get();
  lrq->latch_.lock();
  auto &queue = lrq->request_queue_;
  LockRequest *lr;

  for (auto req_itr = queue.begin();;req_itr++) {
    if (req_itr == queue.end()) {
      lrq->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    } else if ((*req_itr)->txn_id_ == txn->GetTransactionId()) {
      lr = *req_itr;
      if (!lr->granted_) {
        lrq->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      }
      queue.erase(req_itr);
      auto *oid2rid = lr->lock_mode_ == LockMode::SHARED ? txn->GetSharedRowLockSet().get() 
                                                            : txn->GetExclusiveRowLockSet().get();
      oid2rid->at(oid).erase(rid);
      UpdateTransactionStateOnUnlock(txn, lr->lock_mode_);
      break;
    }
  }

  lrq->cv_.notify_one();
  lrq->latch_.unlock();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

void LockManager::IsValidLockMode(Transaction *txn, LockMode lock_mode, bool isRow) {
  if (isRow && lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  auto t_isolation = txn->GetIsolationLevel();
  if (t_isolation == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (t_isolation == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED){
        txn->SetState(TransactionState::ABORTED);
        throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  } else if (t_isolation == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
}

auto LockManager::IsValidUpgrade(LockMode from, LockMode to) -> bool {
  /* Only the following upgrades are allowed:
   * IS  -> [S, X, SIX]
   *  S  -> [X, SIX]
   * IX  -> [X, SIX]
   * SIX -> [X]
  */
  switch(from) {
    case LockMode::INTENTION_SHARED:
      if (to != LockMode::SHARED && to != LockMode::EXCLUSIVE && to != LockMode::SHARED_INTENTION_EXCLUSIVE) return false;
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (to != LockMode::EXCLUSIVE && to != LockMode::SHARED_INTENTION_EXCLUSIVE) return false;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (to != LockMode::EXCLUSIVE) return false;
      break;
    default:
      return false;      
  }
  return true;
}

auto LockManager::IsCompatible(LockMode a, LockMode b) -> bool {
  // Implementaiton of compatibility matrix
  switch(a) {
    case LockMode::SHARED:
      if (b == LockMode::EXCLUSIVE || b == LockMode::INTENTION_EXCLUSIVE || b == LockMode::SHARED_INTENTION_EXCLUSIVE) return false;
      break;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      if (b == LockMode::EXCLUSIVE) return false;
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (b == LockMode::SHARED || b == LockMode::EXCLUSIVE || b == LockMode::SHARED_INTENTION_EXCLUSIVE) return false;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (b != LockMode::INTENTION_SHARED) return false;
      break;
  }
  return true;
}

auto LockManager::IsBlocking(LockRequestQueue *lrq, Transaction *txn, LockMode lock_mode) -> bool {
  if (lrq->upgrading_ != INVALID_PAGE_ID && lrq->upgrading_ != txn->GetTransactionId()) return true;
  auto &queue = lrq->request_queue_;
  for (auto itr = queue.begin(); itr != queue.end(); itr++) {
    LockRequest *lr = (*itr);
    if (lr->granted_ && lr->txn_id_ != txn->GetTransactionId()) {
      if (!IsCompatible(lr->lock_mode_, lock_mode)) return true;
    }
  }
  return false;
}

auto LockManager::GetTableLockSet(Transaction *txn, LockMode lock_mode) -> std::unordered_set<table_oid_t> * {
  switch(lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedTableLockSet().get();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet().get();
    case LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet().get();
    case LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet().get();
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet().get();
  }
  throw bustub::Exception("This should not happen!");
}

auto LockManager::GetRowLockSet(Transaction *txn, LockMode lock_mode) -> std::unordered_map<table_oid_t, std::unordered_set<RID>> * {
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedRowLockSet().get();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveRowLockSet().get();
    default:
      break;
  }
  throw bustub::Exception("This should not happen!");
}

void LockManager::UpdateTransactionStateOnUnlock(Transaction *txn, LockMode lock_mode) {
  /*
  * TRANSACTION STATE UPDATE
  *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
  *    Only unlocking S or X locks changes transaction state.
  *
  *    REPEATABLE_READ:
  *        Unlocking S/X locks should set the transaction state to SHRINKING
  *
  *    READ_COMMITTED:
  *        Unlocking X locks should set the transaction state to SHRINKING.
  *        Unlocking S locks does not affect transaction state.
  *
  *   READ_UNCOMMITTED:
  *        Unlocking X locks should set the transaction state to SHRINKING.
  *        S locks are not permitted under READ_UNCOMMITTED.
  *            The behaviour upon unlocking an S lock under this isolation level is undefined.
  */
  switch(txn->GetState()) {
    case TransactionState::COMMITTED:
    case TransactionState::ABORTED:
      return;
    default:
      break;
  }
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) return;
  auto t_isolation = txn->GetIsolationLevel();
  switch(t_isolation) {
    case IsolationLevel::REPEATABLE_READ:
      txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_COMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) txn->SetState(TransactionState::SHRINKING);
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      } else {
        throw bustub::Exception("This should not happen!");
      }
  }
}

auto LockManager::IsTableRowLocked(Transaction *txn, const table_oid_t &oid) -> bool {
  auto *s_oid2rid = txn->GetSharedRowLockSet().get();
  auto s_row_lock_set = s_oid2rid->find(oid);
  if (s_row_lock_set != s_oid2rid->end()) if (s_row_lock_set->second.size()) return true;
  auto *x_oid2rid = txn->GetExclusiveRowLockSet().get();
  auto x_row_lock_set = x_oid2rid->find(oid);
  if (x_row_lock_set != x_oid2rid->end()) if (x_row_lock_set->second.size()) return true;
  return false;
}

auto LockManager::IsTableExist(const table_oid_t &oid) -> bool {
  std::scoped_lock<std::mutex> lock(table_lock_map_latch_);
  return table_lock_map_.find(oid) != table_lock_map_.end();
}

auto LockManager::IsRIDExist(const RID &rid) -> bool {
  std::scoped_lock<std::mutex> lock(row_lock_map_latch_);
  return row_lock_map_.find(rid) != row_lock_map_.end();
}

auto LockManager::IsValidRowLock(Transaction *txn, const table_oid_t &oid, const LockMode lock_mode) -> bool {
  LockRequestQueue *lrq = table_lock_map_.at(oid).get();
  std::scoped_lock<std::mutex> lock(lrq->latch_);
  for (auto itr = lrq->request_queue_.begin();;itr++) {
    if (itr == lrq->request_queue_.end()) {
      return false;
    } else if ((*itr)->txn_id_ == txn->GetTransactionId()) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        if ((*itr)->lock_mode_ == LockMode::SHARED || (*itr)->lock_mode_ == LockMode::INTENTION_SHARED) 
          return false;
      }
      return true;
    }
  }
}

}  // namespace bustub