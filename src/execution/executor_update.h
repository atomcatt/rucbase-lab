/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        /*
            该方法需要把算子成员(由构造函数赋值)
            std::vector<Rid> rids_;
            标记的所有id的记录进行修改（与删除算子行为相似）。
        */
        for (auto &rid : rids_) {
            auto record = fh_->get_record(rid, context_);
            // 更新记录
            for (auto& set_clause : set_clauses_) {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                memcpy(record->data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
            }
            fh_->update_record(rid, record->data, context_);
            // 更新索引
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ix_manager = sm_manager_->get_ix_manager();
                auto ih = sm_manager_->ihs_.at(ix_manager->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    auto &col = index.cols[j];
                    memcpy(key + offset, record->data + col.offset, col.len);
                    offset += col.len;
                }
                // 删除旧的索引
                ih->delete_entry(key, context_->txn_);
                // 插入新的索引
                ih->insert_entry(key, rid, context_->txn_);
            }
        }
        return nullptr;
    }
    Rid& rid() override { return _abstract_rid; }
};