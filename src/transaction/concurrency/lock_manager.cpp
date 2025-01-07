// /* Copyright (c) 2023 Renmin University of China
// RMDB is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 检查是否可以申请锁
 * @return {void} 
 * @param {Transaction*} txn 要申请锁的事务对象指针
 */
bool ensure_txn_can_lock(Transaction* txn) {
    // Abort or committed means no more locking
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    // Shrinking not allowed to acquire
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    // Move to GROWING if default
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
        return true;
    }
    if (txn->get_state() == TransactionState::GROWING) {
        return true;
    }
}

void LockManager::check_lock_conflict(Transaction *txn, GroupLockMode group_lock_mode, LockMode lock_mode) {
    switch (lock_mode) {
        case LockMode::SHARED:
            if (group_lock_mode == GroupLockMode::IX || 
                group_lock_mode == GroupLockMode::X || 
                group_lock_mode == GroupLockMode::SIX) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
            break;
        case LockMode::EXCLUSIVE:
            if (group_lock_mode != GroupLockMode::NON_LOCK) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
            break;
        case LockMode::INTENTION_SHARED:
            if (group_lock_mode == GroupLockMode::X) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
            break;
        case LockMode::INTENTION_EXCLUSIVE:
            if (group_lock_mode == GroupLockMode::S || group_lock_mode == GroupLockMode::X || group_lock_mode == GroupLockMode::SIX) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
            break;
        case LockMode::S_IX:
            if (group_lock_mode == GroupLockMode::S || group_lock_mode == GroupLockMode::X || group_lock_mode == GroupLockMode::IX || group_lock_mode == GroupLockMode::SIX) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
            break;
        default:
            break;
    }
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto &lock_request_queue = lock_table_.at(lock_data_id);
    _lock_IS_on_table(txn, tab_fd);
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    // 加锁队列的锁模式为IX、X、SIX时，事务无法申请共享锁，直接回滚
    // 已经有这个事务的锁请求时，直接返回
    for (auto &lock_request : request_queue) {
        // S锁是元组上最弱的锁，如果存在这个事务申请的锁，那么它一定大于等于S
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            return true;
        }
    }
    check_lock_conflict(txn, group_lock_mode, LockMode::SHARED);
    group_lock_mode = GroupLockMode::S;
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["S"]++;
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}


/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto &lock_request_queue = lock_table_[lock_data_id];
    _lock_IX_on_table(txn, tab_fd);
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    for (auto &lock_request : request_queue) {
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            if (lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
                return true;
            }
            if (lock_request.lock_mode_ == LockMode::SHARED) {
                lock_request.lock_mode_ == LockMode::EXCLUSIVE;
                group_lock_mode = GroupLockMode::X;
                lock_request_queue.lock_mode_count_["S"]--;
                lock_request_queue.lock_mode_count_["X"]++;
                return true;
            }
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::UPGRADE_CONFLICT);
        }
    }
    // 加锁队列有其他锁存在时时，事务无法申请排他锁，直接回滚
    check_lock_conflict(txn, group_lock_mode, LockMode::EXCLUSIVE);
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXCLUSIVE);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["X"]++;
    txn->append_lock_set(lock_data_id);
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto &lock_request_queue = lock_table_[lock_data_id];
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    for (auto &lock_request : request_queue) {
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            // 不需要锁升级
            if (lock_request.lock_mode_ == LockMode::SHARED || 
                lock_request.lock_mode_ == LockMode::EXCLUSIVE || 
                lock_request.lock_mode_ == LockMode::S_IX) {
                return true;
            }
            // IX锁升级为SIX锁 要考虑相容矩阵
            if (lock_request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE && 
                       lock_request_queue.lock_mode_count_["IX"] == 1) {
                lock_request.lock_mode_ == LockMode::S_IX;
                lock_request_queue.lock_mode_count_["IX"]--;
                lock_request_queue.lock_mode_count_["SIX"]++;
                lock_request_queue.lock_mode_count_["S"]++;
                group_lock_mode = GroupLockMode::SIX;
                return true;
            }
            // IS锁升级为S锁 要考虑相容矩阵
            if (lock_request.lock_mode_ == LockMode::INTENTION_SHARED && 
                       lock_request_queue.lock_mode_count_["IX"] == 0 &&
                       lock_request_queue.lock_mode_count_["SIX"] == 0) {
                lock_request.lock_mode_ == LockMode::SHARED;
                lock_request_queue.lock_mode_count_["IS"]--;
                lock_request_queue.lock_mode_count_["S"]++;
                group_lock_mode = GroupLockMode::S;
                return true;
            }
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }
    check_lock_conflict(txn, group_lock_mode, LockMode::SHARED);
    group_lock_mode = GroupLockMode::S;
    LockRequest lock_request(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["S"]++;
    txn->append_lock_set(lock_data_id);
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto &lock_request_queue = lock_table_[lock_data_id];
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    for (auto &lock_request : request_queue) {
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            // 不需要锁升级
            if (lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
                return true;
            }
            // 全局锁表上不能有其他锁，否则要回滚
            if (group_lock_mode == GroupLockMode::NON_LOCK) {
                lock_request.lock_mode_ == LockMode::EXCLUSIVE;
                lock_request_queue.lock_mode_count_["X"]++;
                group_lock_mode = GroupLockMode::X;
                return true;
            }
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }
    check_lock_conflict(txn, group_lock_mode, LockMode::EXCLUSIVE);
    group_lock_mode = GroupLockMode::X;
    LockRequest lock_request(txn->get_transaction_id(), LockMode::EXCLUSIVE);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["X"]++;
    txn->append_lock_set(lock_data_id);
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto &lock_request_queue = lock_table_[lock_data_id];
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    for (auto &lock_request : request_queue) {
        // IS锁是表上最弱的锁，如果队列中已经存在了这个事务申请的锁，那么锁等级一定大于等于IS
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            return true;
        }
    }
    check_lock_conflict(txn, group_lock_mode, LockMode::INTENTION_SHARED);
    if (group_lock_mode == GroupLockMode::NON_LOCK) {
        group_lock_mode = GroupLockMode::IS;
    }
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["IS"]++;
    txn->append_lock_set(lock_data_id);
    return true;
}

bool LockManager::_lock_IS_on_table(Transaction* txn, int tab_fd) {
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto &lock_request_queue = lock_table_[lock_data_id];
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    for (auto &lock_request : request_queue) {
        // IS锁是表上最弱的锁，如果队列中已经存在了这个事务申请的锁，那么锁等级一定大于等于IS
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            return true;
        }
    }
    check_lock_conflict(txn, group_lock_mode, LockMode::INTENTION_SHARED);
    if (group_lock_mode == GroupLockMode::NON_LOCK) {
        group_lock_mode = GroupLockMode::IS;
    }
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["IS"]++;
    txn->append_lock_set(lock_data_id);
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto &lock_request_queue = lock_table_[lock_data_id];
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    for (auto &lock_request : request_queue) {
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            // 不需要锁升级
            if (lock_request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                lock_request.lock_mode_ == LockMode::S_IX ||
                lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
                    return true;
            }
            // S锁升级为IX锁 要考虑相容矩阵
            if (lock_request.lock_mode_ == LockMode::SHARED && 
                lock_request_queue.lock_mode_count_["S"] == 1) {
                lock_request.lock_mode_ = LockMode::S_IX;
                lock_request_queue.lock_mode_count_["S"]--;
                lock_request_queue.lock_mode_count_["IX"]++;
                group_lock_mode = GroupLockMode::SIX;
                return true;
            }
            // IS锁升级为IX锁 要考虑相容矩阵
            if (lock_request.lock_mode_ == LockMode::INTENTION_SHARED && 
                lock_request_queue.lock_mode_count_["S"] == 0 &&
                lock_request_queue.lock_mode_count_["SIX"] == 0) {
                lock_request.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                lock_request_queue.lock_mode_count_["IS"]--;
                lock_request_queue.lock_mode_count_["IX"]++;
                group_lock_mode = GroupLockMode::IX;
                return true;
            }
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }
    check_lock_conflict(txn, group_lock_mode, LockMode::INTENTION_EXCLUSIVE);
    group_lock_mode = GroupLockMode::IX;
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["IX"]++;
    txn->append_lock_set(lock_data_id);
    return true;
}

bool LockManager::_lock_IX_on_table(Transaction* txn, int tab_fd) {
    ensure_txn_can_lock(txn);
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto &lock_request_queue = lock_table_[lock_data_id];
    auto &group_lock_mode = lock_request_queue.group_lock_mode_;
    auto &request_queue = lock_request_queue.request_queue_;
    for (auto &lock_request : request_queue) {
        if (lock_request.txn_id_ == txn->get_transaction_id()) {
            // 不需要锁升级
            if (lock_request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                lock_request.lock_mode_ == LockMode::S_IX ||
                lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
                    return true;
            }
            // S锁升级为IX锁 要考虑相容矩阵
            if (lock_request.lock_mode_ == LockMode::SHARED && 
                lock_request_queue.lock_mode_count_["S"] == 1) {
                lock_request.lock_mode_ = LockMode::S_IX;
                lock_request_queue.lock_mode_count_["S"]--;
                lock_request_queue.lock_mode_count_["IX"]++;
                group_lock_mode = GroupLockMode::SIX;
                return true;
            }
            // IS锁升级为IX锁 要考虑相容矩阵
            if (lock_request.lock_mode_ == LockMode::INTENTION_SHARED && 
                lock_request_queue.lock_mode_count_["S"] == 0 &&
                lock_request_queue.lock_mode_count_["SIX"] == 0) {
                lock_request.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                lock_request_queue.lock_mode_count_["IS"]--;
                lock_request_queue.lock_mode_count_["IX"]++;
                group_lock_mode = GroupLockMode::IX;
                return true;
            }
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }
    check_lock_conflict(txn, group_lock_mode, LockMode::INTENTION_EXCLUSIVE);
    group_lock_mode = GroupLockMode::IX;
    LockRequest lock_request(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    lock_request.granted_ = true;
    request_queue.emplace_back(lock_request);
    lock_request_queue.lock_mode_count_["IX"]++;
    txn->append_lock_set(lock_data_id);
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    // 解锁逻辑
    std::unique_lock<std::mutex> lock(latch_);

    if (txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }
    if (!lock_table_.count(lock_data_id)) {
        return true;
    }
    auto &lock_request_queue = lock_table_.at(lock_data_id);
    auto &requests = lock_request_queue.request_queue_;
    if (requests.empty()) {
        return true;
    }
    bool flag = false;
    for (auto request = requests.begin(); request != requests.end(); request++) {
        if (request->txn_id_ == txn->get_transaction_id()) {
            flag = true;
            if (request->lock_mode_ == LockMode::SHARED) {
                lock_request_queue.lock_mode_count_["S"]--;
            } else if (request->lock_mode_ == LockMode::EXCLUSIVE) {
                lock_request_queue.lock_mode_count_["X"]--;
            } else if (request->lock_mode_ == LockMode::INTENTION_SHARED) {
                lock_request_queue.lock_mode_count_["IS"]--;
            } else if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
                lock_request_queue.lock_mode_count_["IX"]--;
            } else if (request->lock_mode_ == LockMode::S_IX) {
                lock_request_queue.lock_mode_count_["SIX"]--;
            }
            requests.erase(request);
            if (requests.empty()) {
                lock_request_queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
                return true;
            }
            break;
        }
    }
    if (!flag) {
        return true;
    }
    GroupLockMode group_lock_mode = GroupLockMode::NON_LOCK;
    for (auto &request : requests) {
        if (request.lock_mode_ == LockMode::SHARED) {
            if (group_lock_mode == GroupLockMode::NON_LOCK) {
                group_lock_mode = GroupLockMode::S;
            }
        } else if (request.lock_mode_ == LockMode::EXCLUSIVE) {
            group_lock_mode = GroupLockMode::X;
            break;
        } else if (request.lock_mode_ == LockMode::INTENTION_SHARED) {
            if (group_lock_mode == GroupLockMode::NON_LOCK) {
                group_lock_mode = GroupLockMode::IS;
            }
        } else if (request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
            group_lock_mode = GroupLockMode::IX;
        } else if (request.lock_mode_ == LockMode::S_IX) {
            group_lock_mode = GroupLockMode::SIX;
        }
    }
    lock_request_queue.group_lock_mode_ = group_lock_mode;
    return true;
}