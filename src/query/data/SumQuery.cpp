//
// Created by camelboat on 18-11-1.
//

#include "SumQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#ifdef TIMER
#include <iostream>

#endif
#include <numeric>
#include <algorithm>

constexpr const char *SumQuery::qname;

QueryResult::Ptr SumQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    Database &db = Database::getInstance();
    if (this->operands.empty()) {
        db.queries.erase(this->id);
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand (? operands)."_f % operands.size()
        );
    }
    try {
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];
        this->field_sum.reserve(this->operands.size());
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for SUM.)"_f
                );
            } else {
                this->field_sum.emplace_back(std::make_pair(table.getFieldIndex(*it),0));
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    for (auto fieldit = this->field_sum.begin(); fieldit!=this->field_sum.end();++fieldit) {
                        (*fieldit).second += (*it)[(*fieldit).first];
                    }
                }
            }
        }
        vector<Table::ValueType> sum_result;
        for (unsigned int i=0;i<this->field_sum.size();i++){
            sum_result.emplace_back(this->field_sum.at(i).second);
        }
        // lcx
//        int *sum = new int [this->operands.size()];
////        cout << "operands size is: " << this->operands.size() << endl;
//        for (unsigned int i = 0; i < this->operands.size(); i++) {
//            int sum_tmp = 0;
//            for (auto it = table.begin(); it != table.end(); it++) {
//                unsigned long field_tmp = table.getFieldIndex(operands[i]);
//                sum_tmp+=it->get(field_tmp);
//            }
//            sum[i] = sum_tmp;
////            int sum_tmp = accumulate(table.field().data()->begin(), table.field().data()->end(), 0);
////            cout << "sum is: " << sum_tmp << endl;
//        }
//        cout << "ANSWER = ( ";
//        for (unsigned int i = 0; i < this->operands.size() ; i++) {
//            cout << sum[i] << " ";
//        }
//        cout << ")" << '\n';
//        free(sum);
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"SUM takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        db.addresult(this->id,make_unique<AnswerMsgResult>(sum_result));
        db.table_locks[this->targetTable]->unlock();
        db.queries.erase(this->id);
        return std::make_unique<AnswerMsgResult>(sum_result);
    }
    catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }

}

std::string SumQuery::toString() {
    return "QUERY = SUM " + this->targetTable + "\"";
}