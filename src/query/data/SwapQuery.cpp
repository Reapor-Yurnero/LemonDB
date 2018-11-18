//
// Created by Reapor Yurnero on 31/10/2018.
//

#include "SwapQuery.h"

#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

#ifdef TIMER
#include <iostream>

#endif

constexpr const char *SwapQuery::qname;

QueryResult::Ptr SwapQuery::execute() {
    using namespace std;
#ifdef TIMER
    struct timespec ts1, ts2;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
#endif
    if (this->operands.size() != 2)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    Table::SizeType counter = 0;
    try {
        db.table_locks[this->targetTable]->lock();
        auto &table = db[this->targetTable];
        int mark = 0;
        for ( auto it = this->operands.begin();it!=this->operands.end();++it) {
            if (*it == "KEY") {
                throw invalid_argument(
                        R"(Can not input KEY for SWAP.)"_f
                );
            }
            else {
                if (mark == 0) swapA = table.getFieldIndex(*it);
                else swapB = table.getFieldIndex(*it);
                mark++;
            }
        }
        auto result = initCondition(table);
        if (result.second) {
            for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                    Table::ValueType temp = (*it)[swapA];
                    (*it)[swapA] = (*it)[swapB];
                    (*it)[swapB] = temp;
                    ++counter;
                }
            }
        }else
            db.table_locks[this->targetTable]->unlock();
#ifdef TIMER
        clock_gettime(CLOCK_MONOTONIC, &ts2);
        cerr<<"SWAP takes "<<(1000.0*ts2.tv_sec + 1e-6*ts2.tv_nsec
                             - (1000.0*ts1.tv_sec + 1e-6*ts1.tv_nsec))<<"ms in all\n";
#endif
        db.addresult(this->id,make_unique<RecordCountResult>(counter));
        db.table_locks[this->targetTable]->unlock();
        db.queries.erase(id);
        return make_unique<RecordCountResult>(counter);
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

std::string SwapQuery::toString() {
    return "QUERY = SWAP " + this->targetTable + "\"";
}