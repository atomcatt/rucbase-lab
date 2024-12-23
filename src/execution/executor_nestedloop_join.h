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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    void beginTuple() override {
        // 从左边表的第一行开始，遍历右边的每一行
        left_->beginTuple();
        // 如果已经到最后一行了，就返回
        if (left_->is_end()) {
            return;
        }
        right_->beginTuple();
        while(!left_->is_end()) {
            std::cout << "fed_conds_.size: " << fed_conds_.size() << std::endl;
            if (fed_conds_.empty()) {
                break;
            }
            bool flag = false;
            auto left_record = left_->Next();
            auto right_record = right_->Next();
            for (auto &fed_cond : fed_conds_) {
                auto lhs_col_meta = get_col(cols_, fed_cond.lhs_col);
                char *lhs_data = left_record->data + lhs_col_meta->offset;
                char *rhs_data = nullptr;
                ColType rhs_type;
                if (fed_cond.is_rhs_val) {
                    rhs_data = fed_cond.rhs_val.raw->data;
                    rhs_type = fed_cond.rhs_val.type;
                } else {
                    auto rhs_col_meta = get_col(cols_, fed_cond.rhs_col);
                    /*
                        注意：这里的offset是相对于整个record的，而不是相对于右边表的
                        for (auto &col : right_cols) {
                            col.offset += left_->tupleLen();
                        }
                    */
                    rhs_data = right_record->data + rhs_col_meta->offset - left_->tupleLen();
                    rhs_type = rhs_col_meta->type;
                }
                if (lhs_col_meta->type == TYPE_INT) {
                    printf("TYPE: INT lhs_data: %d, rhs_data: %d, len: %d\n", *(int*)(lhs_data), *(int*)rhs_data, lhs_col_meta->len);
                } else if (lhs_col_meta->type == TYPE_FLOAT) {
                    printf("TYPE: INT lhs_data: %f, rhs_data: %f, len: %d\n", *(float*)(lhs_data), *(float*)rhs_data, lhs_col_meta->len);
                } else if (lhs_col_meta->type == TYPE_STRING) {
                    printf("TYPE: STRING lhs_data: %s, rhs_data: %s, len: %d\n", lhs_data, rhs_data, lhs_col_meta->len);
                }
                int cmp = ix_compare(lhs_data, rhs_data, rhs_type, lhs_col_meta->len);
                if (fed_cond.op == OP_EQ) {
                    if (cmp == 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_NE) {
                    if (cmp != 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_LT) {
                    if (cmp < 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_GT) {
                    if (cmp > 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_LE) {
                    if (cmp <= 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_GE) {
                    if (cmp >= 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else {
                    throw InternalError("Unexpected comparison operator");
                }
            }
            if (flag) {
                break;
            }
            right_->nextTuple();
            // 如果右边表已经遍历完了，就从左边表的下一行开始，右边表重新开始遍历
            if (right_->is_end()) {
                left_->nextTuple();
                right_->beginTuple();
            }
        }
    }

    void nextTuple() override {
        right_->nextTuple();
        if (right_->is_end()) {
            left_->nextTuple();
            right_->beginTuple();
        }
        while(!left_->is_end()) {
            if (fed_conds_.empty()) {
                break;
            }
            bool flag = false;
            auto left_record = left_->Next();
            auto right_record = right_->Next();
            for (auto &fed_cond : fed_conds_) {
                auto lhs_col_meta = get_col(cols_, fed_cond.lhs_col);
                char *lhs_data = left_record->data + lhs_col_meta->offset;
                char *rhs_data = nullptr;
                ColType rhs_type;
                if (fed_cond.is_rhs_val) {
                    rhs_data = fed_cond.rhs_val.raw->data;
                    rhs_type = fed_cond.rhs_val.type;
                } else {
                    auto rhs_col_meta = get_col(cols_, fed_cond.rhs_col);
                    rhs_data = right_record->data + rhs_col_meta->offset - left_->tupleLen();
                    rhs_type = rhs_col_meta->type;
                }
                if (lhs_col_meta->type == TYPE_INT) {
                    printf("TYPE: INT lhs_data: %d, rhs_data: %d, len: %d\n", *(int*)(lhs_data), *(int*)rhs_data, lhs_col_meta->len);
                } else if (lhs_col_meta->type == TYPE_FLOAT) {
                    printf("TYPE: INT lhs_data: %f, rhs_data: %f, len: %d\n", *(float*)(lhs_data), *(float*)rhs_data, lhs_col_meta->len);
                } else if (lhs_col_meta->type == TYPE_STRING) {
                    printf("TYPE: STRING lhs_data: %s, rhs_data: %s, len: %d\n", lhs_data, rhs_data, lhs_col_meta->len);
                }
                int cmp = ix_compare(lhs_data, rhs_data, rhs_type, lhs_col_meta->len);
                if (fed_cond.op == OP_EQ) {
                    if (cmp == 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_NE) {
                    if (cmp != 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_LT) {
                    if (cmp < 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_GT) {
                    if (cmp > 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_LE) {
                    if (cmp <= 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else if (fed_cond.op == OP_GE) {
                    if (cmp >= 0) {
                        flag = true;
                    } else {
                        flag = false;
                        break;
                    }
                } else {
                    throw InternalError("Unexpected comparison operator");
                }
            }
            if (flag) {
                break;
            }
            right_->nextTuple();
            // 如果右边表已经遍历完了，就从左边表的下一行开始，右边表重新开始遍历
            if (right_->is_end()) {
                left_->nextTuple();
                right_->beginTuple();
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        auto record = std::make_unique<RmRecord>(len_);
        auto left_record = left_->Next();
        auto right_record = right_->Next();
        memcpy(record->data, left_record->data, left_->tupleLen());
        memcpy(record->data + left_->tupleLen(), right_record->data, right_->tupleLen());
        return record;
    }
    
    bool is_end() const override { return left_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    Rid &rid() override { return _abstract_rid; }
};

