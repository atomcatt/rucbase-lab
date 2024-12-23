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
#include <fstream>

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        std::cout << "In seq scan beginTuple()" << std::endl;
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto record = fh_->get_record(rid_, context_);
            bool flag = false;
            std::cout << "fed_conds_.size(): " << fed_conds_.size() << std::endl;
            if (fed_conds_.size() == 0) {
                flag = true;
            } else {
                for (auto &cond : fed_conds_) {
                    auto lhs_col_meta = get_col(cols_, cond.lhs_col);
                    char *lhs_data = record->data + lhs_col_meta->offset;
                    // char *lhs_data = record->data + 4;
                    char *rhs_data = nullptr;
                    ColType rhs_type;
                    if (cond.is_rhs_val) {
                        rhs_data = cond.rhs_val.raw->data;
                        rhs_type = cond.rhs_val.type;
                    } else {
                        auto rhs_col_meta = get_col(cols_, cond.rhs_col);
                        rhs_data = record->data + rhs_col_meta->offset;
                        rhs_type = rhs_col_meta->type;
                    }
                    if (lhs_col_meta->type == TYPE_INT) {
                        printf("TYPE: INT lhs_data: %d, rhs_data: %d, len: %d\n", *(int*)(lhs_data), *(int*)rhs_data, lhs_col_meta->len);
                    } else if (lhs_col_meta->type == TYPE_FLOAT) {
                        printf("TYPE: INT lhs_data: %f, rhs_data: %f, len: %d\n", *(float*)(lhs_data), *(float*)rhs_data, lhs_col_meta->len);printf("TYPE: lhs_data: %f\n", *(float*)(record->data + lhs_col_meta->offset));
                    } else if (lhs_col_meta->type == TYPE_STRING) {
                        printf("TYPE: STRING lhs_data: %s, rhs_data: %s, len: %d\n", lhs_data, rhs_data, lhs_col_meta->len);
                    }
                    int cmp = ix_compare(lhs_data, rhs_data, rhs_type, lhs_col_meta->len);
                    if (cond.op == OP_EQ) {
                        if (cmp == 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_NE) {
                        if (cmp != 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_LT) {
                        if (cmp < 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_GT) {
                        if (cmp > 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_LE) {
                        if (cmp <= 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_GE) {
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
            }
            printf("flag: %d\n", flag);
            if (flag) {
                break;
            }
            scan_->next();
        }
        if (scan_->is_end()) {
            std::cout << "In SeqScanExecutor::beginTuple(), scan_->is_end()" << std::endl;
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        std::cout << "In seq scan nextTuple()" << std::endl;
        scan_->next();
        while(!scan_->is_end()) {
            rid_ = scan_->rid();
            auto record = fh_->get_record(rid_, context_);
            bool flag = false;
            std::cout << "fed_conds_.size(): " << fed_conds_.size() << std::endl;
            if (fed_conds_.size() == 0) {
                flag = true;
            } else {
                for (auto &cond : fed_conds_) {
                    auto lhs_col_meta = get_col(cols_, cond.lhs_col);
                    char *lhs_data = record->data + lhs_col_meta->offset;
                    char *rhs_data = nullptr;
                    ColType rhs_type;
                    if (cond.is_rhs_val) {
                        rhs_data = cond.rhs_val.raw->data;
                        rhs_type = cond.rhs_val.type;
                    } else {
                        auto rhs_col_meta = get_col(cols_, cond.rhs_col);
                        rhs_data = record->data + rhs_col_meta->offset;
                        rhs_type = rhs_col_meta->type;
                    }
                    if (lhs_col_meta->type == TYPE_INT) {
                        printf("TYPE: INT lhs_data: %d, rhs_data: %d, len: %d\n", *(int*)(lhs_data), *(int*)rhs_data, lhs_col_meta->len);
                    } else if (lhs_col_meta->type == TYPE_FLOAT) {
                        printf("TYPE: INT lhs_data: %f, rhs_data: %f, len: %d\n", *(float*)(lhs_data), *(float*)rhs_data, lhs_col_meta->len);printf("TYPE: lhs_data: %f\n", *(float*)(record->data + lhs_col_meta->offset));
                    } else if (lhs_col_meta->type == TYPE_STRING) {
                        printf("TYPE: STRING lhs_data: %s, rhs_data: %s, len: %d\n", lhs_data, rhs_data, lhs_col_meta->len);
                    }
                    int cmp = ix_compare(lhs_data, rhs_data, rhs_type, lhs_col_meta->len);
                    if (cond.op == OP_EQ) {
                        if (cmp == 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_NE) {
                        if (cmp != 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_LT) {
                        if (cmp < 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_GT) {
                        if (cmp > 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_LE) {
                        if (cmp <= 0) {
                            flag = true;
                        } else {
                            flag = false;
                            break;
                        }
                    } else if (cond.op == OP_GE) {
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
            }
            if (flag) {
                break;
            }
            scan_->next();
        }
        if (scan_->is_end()) {
            std::cout << "In SeqScanExecutor::nextTuple(), scan_->is_end()" << std::endl;
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        std::cout << "In seq scan Next()" << std::endl;
        return fh_->get_record(rid_, context_);
    }
    size_t tupleLen() const override { return len_; }
    Rid &rid() override { return rid_; }
    const std::vector<ColMeta> &cols() const override { return cols_; };
    bool is_end() const override { 
        return scan_->is_end();
     }
};